//! Typed per-job queues.
//!
//! - `CRDMasterCommandReportQueue<C, R>`: the owner-side endpoint of a job
//!   group. `send` assigns an id, registers the command in the command table
//!   and dispatches it round robin across the group's instances. Reports from
//!   every instance come back through the same queue.
//! - `CRDMasterDisplayQueue<D>`: unsolicited displays from every instance of
//!   a group. It can be moved to another thread (e.g. a UI) with
//!   `CRDJobGroup::take_display_queue`.
//!
//! Each queue has one consumer. Queues of different jobs are independent, so
//! several consumers can read different jobs in parallel.

use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SendError, Sender, TryRecvError};
use std::sync::Arc;
use std::time::Duration;

use crate::interfaces::crd_job::{CRDJobCommand, CRDJobReport};

use super::command_table::CRDMasterCommandTable;
use super::ids::{CRDCommandKey, CRDJobId, CRDJobKey};
use super::logs::{crd_field, crd_static, CRDLogLevel, CRDMasterLogs};

// ============================================================================
// 1. ENVELOPES
// ============================================================================

/// A report plus the job instance that produced it.
#[derive(Debug)]
pub struct CRDMasterReport<R> {
    pub instance: u32,
    pub report: CRDJobReport<u64, R>,
}

/// A display plus the job instance that published it.
#[derive(Debug)]
pub struct CRDMasterDisplay<D> {
    pub instance: u32,
    pub data: D,
}

/// A command could not be delivered. The command is handed back.
#[derive(Debug)]
pub enum CRDSendError<C> {
    /// No instance of the job group accepted the command (all ended, or the
    /// queue was closed).
    Disconnected(C),
}

impl<C> CRDSendError<C> {
    /// Recovers the undelivered command.
    pub fn into_command(self) -> C {
        match self {
            CRDSendError::Disconnected(command) => command,
        }
    }
}

impl<C> fmt::Display for CRDSendError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no live job instance accepted the command")
    }
}

impl<C: Debug> std::error::Error for CRDSendError<C> {}

// ============================================================================
// 2. COMMAND + REPORT QUEUE
// ============================================================================

/// Owner-side endpoint of a job group: commands out, reports in.
pub struct CRDMasterCommandReportQueue<C, R> {
    job: CRDJobId,
    name: &'static str,
    senders: Vec<Sender<CRDJobCommand<u64, C>>>,
    cursor: AtomicUsize,
    reports: Receiver<CRDMasterReport<R>>,
    table: Arc<CRDMasterCommandTable>,
    logs: CRDMasterLogs,
}

impl<C, R> CRDMasterCommandReportQueue<C, R> {
    pub(super) fn new(
        job: CRDJobId,
        name: &'static str,
        table: Arc<CRDMasterCommandTable>,
        logs: CRDMasterLogs,
        reports: Receiver<CRDMasterReport<R>>,
    ) -> Self {
        Self {
            job,
            name,
            senders: Vec::new(),
            cursor: AtomicUsize::new(0),
            reports,
            table,
            logs,
        }
    }

    /// Adds a job instance to the round-robin rotation. Returns its index.
    pub(super) fn add_instance(&mut self, sender: Sender<CRDJobCommand<u64, C>>) -> u32 {
        self.senders.push(sender);
        (self.senders.len() - 1) as u32
    }

    /// Stops accepting commands. Jobs finish what is already queued, then end.
    pub fn close_commands(&mut self) {
        self.senders.clear();
    }

    pub fn job(&self) -> CRDJobId {
        self.job
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Number of instances in the rotation.
    pub fn instance_count(&self) -> usize {
        self.senders.len()
    }

    /// Next report from any instance, without waiting.
    pub fn try_recv_report(&self) -> Result<CRDMasterReport<R>, TryRecvError> {
        self.reports.try_recv()
    }

    /// Next report from any instance, waiting up to `timeout`.
    /// `Disconnected` means every instance ended and the queue is drained.
    pub fn recv_report_timeout(
        &self,
        timeout: Duration,
    ) -> Result<CRDMasterReport<R>, RecvTimeoutError> {
        self.reports.recv_timeout(timeout)
    }

    /// Takes every report currently queued.
    pub fn drain_reports(&self) -> Vec<CRDMasterReport<R>> {
        std::iter::from_fn(|| self.reports.try_recv().ok()).collect()
    }
}

impl<C: Debug, R> CRDMasterCommandReportQueue<C, R> {
    /// Sends a command to the next instance (round robin) and returns its id.
    ///
    /// The command is registered in the command table before it is sent. An
    /// instance that has ended is skipped, and the command is retried on the
    /// next one. If none accepts it, the command comes back in the error.
    pub fn send(&self, mut data: C) -> Result<CRDCommandKey, CRDSendError<C>> {
        let count = self.senders.len();

        if count == 0 {
            return Err(CRDSendError::Disconnected(data));
        }

        let id = self.table.next_id();
        // The payload dump is a heap allocation plus a formatting pass, so it
        // is built only when some sink wants Trace.
        let command_text = self
            .logs
            .enabled(CRDLogLevel::Trace)
            .then(|| format!("{:?}", data));
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);

        for attempt in 0..count {
            let instance = start.wrapping_add(attempt) % count;
            let key = CRDJobKey {
                job: self.job,
                instance: instance as u32,
            };

            self.table.register(id, key, self.name);

            match self.senders[instance].send(CRDJobCommand { id, data }) {
                Ok(()) => {
                    if let Some(text) = command_text {
                        self.logs.trace_with(
                            "crd.queue",
                            "command dispatched",
                            &[
                                crd_field("command", id),
                                crd_static("job", self.name),
                                crd_field("instance", instance as u64),
                                crd_field("data", text),
                            ],
                        );
                    }
                    return Ok(id);
                }
                Err(SendError(returned)) => {
                    self.table.unregister(id);
                    data = returned.data;
                    self.logs.warn_with(
                        "crd.queue",
                        "job instance unreachable, trying the next one",
                        &[
                            crd_field("command", id),
                            crd_static("job", self.name),
                            crd_field("instance", instance as u64),
                        ],
                    );
                }
            }
        }

        Err(CRDSendError::Disconnected(data))
    }
}

// ============================================================================
// 3. DISPLAY QUEUE
// ============================================================================

/// Unsolicited displays from every instance of a job group.
pub struct CRDMasterDisplayQueue<D> {
    job: CRDJobId,
    name: &'static str,
    displays: Receiver<CRDMasterDisplay<D>>,
}

impl<D> CRDMasterDisplayQueue<D> {
    pub(super) fn new(
        job: CRDJobId,
        name: &'static str,
        displays: Receiver<CRDMasterDisplay<D>>,
    ) -> Self {
        Self { job, name, displays }
    }

    pub fn job(&self) -> CRDJobId {
        self.job
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Next display, without waiting.
    pub fn try_recv(&self) -> Result<CRDMasterDisplay<D>, TryRecvError> {
        self.displays.try_recv()
    }

    /// Next display, waiting up to `timeout`.
    /// `Disconnected` means every instance ended and the queue is drained.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<CRDMasterDisplay<D>, RecvTimeoutError> {
        self.displays.recv_timeout(timeout)
    }

    /// Takes every display currently queued.
    pub fn drain(&self) -> Vec<CRDMasterDisplay<D>> {
        std::iter::from_fn(|| self.displays.try_recv().ok()).collect()
    }
}