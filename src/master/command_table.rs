//! CRDMasterCommandTable: every command that was sent and has not finished.
//!
//! # Speed design
//!
//! - **Sharded.** 16 independent maps, each behind its own mutex. A command id
//!   selects its shard with `id & 15`. Ids are sequential, so load spreads
//!   evenly and threads touching different commands almost never contend.
//! - **Cache-line aligned shards.** Each shard is 64-byte aligned so two
//!   shards never share a cache line (no false sharing).
//! - **Integer hasher.** `CRDIdBuildHasher` replaces SipHash.
//! - **Short critical sections.** Locks cover one map operation. Nothing is
//!   formatted or allocated beyond the report summary while holding one.
//! - **Lock-free counters.** The id generator and the pending count are
//!   atomics.
//!
//! The table is type-erased: it records ids, status, timing and messages,
//! never job-specific payloads, so one table serves every job type.
//!
//! # Lifecycle
//!
//! `register` (before the command is sent), then any number of `Working`
//! reports, then one terminal report, which removes the entry and returns it.
//! A report for an id that is not pending is a protocol violation and comes
//! back as `CRDReportOutcome::Unknown`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::interfaces::crd_job::CRDReportStatus;

use super::ids::{crd_lock, CRDCommandKey, CRDIdBuildHasher, CRDJobKey};

/// Number of shards. Must be a power of two.
pub const CRD_COMMAND_TABLE_SHARDS: usize = 16;

/// Most recent report summaries kept per pending command.
/// `report_count` still counts every report.
pub const CRD_MAX_STORED_REPORTS: usize = 32;

/// True for `Success` and `Error`: the last report a command may produce.
#[inline]
pub fn crd_is_terminal(status: CRDReportStatus) -> bool {
    matches!(status, CRDReportStatus::Success | CRDReportStatus::Error)
}

/// Type-erased summary of one report.
#[derive(Debug, Clone)]
pub struct CRDReportSummary {
    pub status: CRDReportStatus,
    pub message: Option<String>,
    /// Time between sending the command and this report.
    pub elapsed: Duration,
}

/// Everything the master knows about one in-flight command.
#[derive(Debug, Clone)]
pub struct CRDCommandRecord {
    pub id: CRDCommandKey,
    /// The job instance the command was dispatched to.
    pub job: CRDJobKey,
    pub job_name: &'static str,
    pub sent_at: Instant,
    /// Total reports received, including ones no longer stored.
    pub report_count: u64,
    /// The newest reports, oldest first (at most `CRD_MAX_STORED_REPORTS`).
    pub recent_reports: VecDeque<CRDReportSummary>,
}

impl CRDCommandRecord {
    /// Status of the newest report, if any report has arrived.
    pub fn last_status(&self) -> Option<CRDReportStatus> {
        self.recent_reports.back().map(|summary| summary.status)
    }

    fn push_report(&mut self, status: CRDReportStatus, message: Option<&str>) {
        self.report_count += 1;

        if self.recent_reports.len() == CRD_MAX_STORED_REPORTS {
            self.recent_reports.pop_front();
        }

        self.recent_reports.push_back(CRDReportSummary {
            status,
            message: message.map(str::to_owned),
            elapsed: self.sent_at.elapsed(),
        });
    }
}

/// Result of recording a report.
#[derive(Debug)]
pub enum CRDReportOutcome {
    /// The command is still pending (non-terminal report recorded).
    Updated,
    /// The command finished and was removed. Carries its final record.
    Finished(CRDCommandRecord),
    /// No such pending command: unknown id, or a report after the terminal one.
    Unknown,
}

type CRDCommandMap = HashMap<CRDCommandKey, CRDCommandRecord, CRDIdBuildHasher>;

#[repr(align(64))]
struct CRDCommandShard(Mutex<CRDCommandMap>);

/// Table of commands that were sent and have not reached a terminal report.
pub struct CRDMasterCommandTable {
    shards: Box<[CRDCommandShard]>,
    mask: u64,
    next_id: AtomicU64,
    pending: AtomicUsize,
}

impl CRDMasterCommandTable {
    pub fn new() -> Self {
        let shards = (0..CRD_COMMAND_TABLE_SHARDS)
            .map(|_| CRDCommandShard(Mutex::new(HashMap::with_hasher(CRDIdBuildHasher))))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            shards,
            mask: (CRD_COMMAND_TABLE_SHARDS as u64) - 1,
            next_id: AtomicU64::new(1),
            pending: AtomicUsize::new(0),
        }
    }

    /// Returns a new unique command id (monotonic, lock-free).
    #[inline]
    pub fn next_id(&self) -> CRDCommandKey {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    fn shard(&self, id: CRDCommandKey) -> MutexGuard<'_, CRDCommandMap> {
        crd_lock(&self.shards[(id & self.mask) as usize].0)
    }

    /// Registers a command as pending. Call this BEFORE sending it, so a fast
    /// job cannot report on a command the table has not seen yet.
    pub fn register(&self, id: CRDCommandKey, job: CRDJobKey, job_name: &'static str) {
        let record = CRDCommandRecord {
            id,
            job,
            job_name,
            sent_at: Instant::now(),
            report_count: 0,
            recent_reports: VecDeque::new(),
        };

        if self.shard(id).insert(id, record).is_none() {
            self.pending.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Removes a pending command without a report (e.g. delivery failed).
    pub fn unregister(&self, id: CRDCommandKey) -> Option<CRDCommandRecord> {
        let removed = self.shard(id).remove(&id);
        if removed.is_some() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }

    /// Records a report. A terminal status removes the command.
    pub fn record_report(
        &self,
        id: CRDCommandKey,
        status: CRDReportStatus,
        message: Option<&str>,
    ) -> CRDReportOutcome {
        let mut shard = self.shard(id);

        if crd_is_terminal(status) {
            match shard.remove(&id) {
                Some(mut record) => {
                    drop(shard);
                    record.push_report(status, message);
                    self.pending.fetch_sub(1, Ordering::Relaxed);
                    CRDReportOutcome::Finished(record)
                }
                None => CRDReportOutcome::Unknown,
            }
        } else {
            match shard.get_mut(&id) {
                Some(record) => {
                    record.push_report(status, message);
                    CRDReportOutcome::Updated
                }
                None => CRDReportOutcome::Unknown,
            }
        }
    }

    /// Removes and returns every pending command of one job instance.
    /// Used when the instance ended without finishing them.
    pub fn abandon_job(&self, job: CRDJobKey) -> Vec<CRDCommandRecord> {
        let mut abandoned = Vec::new();

        for shard in self.shards.iter() {
            let mut map = crd_lock(&shard.0);
            let ids: Vec<CRDCommandKey> = map
                .iter()
                .filter(|(_, record)| record.job == job)
                .map(|(id, _)| *id)
                .collect();

            for id in ids {
                if let Some(record) = map.remove(&id) {
                    abandoned.push(record);
                }
            }
        }

        if !abandoned.is_empty() {
            self.pending.fetch_sub(abandoned.len(), Ordering::Relaxed);
        }

        abandoned
    }

    /// Copy of one pending command's record.
    pub fn get(&self, id: CRDCommandKey) -> Option<CRDCommandRecord> {
        self.shard(id).get(&id).cloned()
    }

    /// Number of pending commands (lock-free; momentarily approximate under
    /// concurrent updates).
    pub fn len(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Copy of every pending command, sorted by id. Locks one shard at a
    /// time, so it is not an atomic snapshot. Meant for UI and diagnostics.
    pub fn snapshot(&self) -> Vec<CRDCommandRecord> {
        let mut all = Vec::with_capacity(self.len());

        for shard in self.shards.iter() {
            all.extend(crd_lock(&shard.0).values().cloned());
        }

        all.sort_by_key(|record| record.id);
        all
    }
}