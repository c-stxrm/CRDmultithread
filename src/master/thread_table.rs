//! CRDMasterThreadTable: the threads that are alive and the jobs pinned to them.
//!
//! A thread registers itself when it starts and unregisters when it ends, so
//! the table only ever holds threads that are still working. Most of the time
//! a thread hosts one job, but the table allows several jobs per thread.
//!
//! Threads are few and change rarely, so one mutex is enough here. The
//! sharded design of the command table would add cost for nothing.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use super::ids::{crd_lock, CRDIdBuildHasher, CRDJobKey, CRDThreadId};

/// One live thread and the jobs pinned to it.
#[derive(Debug, Clone)]
pub struct CRDThreadRecord {
    pub thread_id: CRDThreadId,
    pub name: String,
    pub started_at: Instant,
    pub jobs: Vec<CRDJobKey>,
}

pub struct CRDMasterThreadTable {
    threads: Mutex<HashMap<CRDThreadId, CRDThreadRecord, CRDIdBuildHasher>>,
}

impl CRDMasterThreadTable {
    pub fn new() -> Self {
        Self {
            threads: Mutex::new(HashMap::with_hasher(CRDIdBuildHasher)),
        }
    }

    /// Registers the calling thread's id. Replaces a stale record with the
    /// same id (the OS may reuse ids after a thread ends).
    pub fn register_thread(&self, thread_id: CRDThreadId, name: &str) {
        crd_lock(&self.threads).insert(
            thread_id,
            CRDThreadRecord {
                thread_id,
                name: name.to_owned(),
                started_at: Instant::now(),
                jobs: Vec::new(),
            },
        );
    }

    /// Pins a job to a registered thread. Returns false if the thread is unknown.
    pub fn pin_job(&self, thread_id: CRDThreadId, job: CRDJobKey) -> bool {
        match crd_lock(&self.threads).get_mut(&thread_id) {
            Some(record) => {
                if !record.jobs.contains(&job) {
                    record.jobs.push(job);
                }
                true
            }
            None => false,
        }
    }

    /// Unpins a job. Returns false if the thread or the pin was not found.
    pub fn unpin_job(&self, thread_id: CRDThreadId, job: CRDJobKey) -> bool {
        match crd_lock(&self.threads).get_mut(&thread_id) {
            Some(record) => {
                let before = record.jobs.len();
                record.jobs.retain(|pinned| *pinned != job);
                record.jobs.len() != before
            }
            None => false,
        }
    }

    /// Removes a thread (and its pins). Returns its final record.
    pub fn unregister_thread(&self, thread_id: CRDThreadId) -> Option<CRDThreadRecord> {
        crd_lock(&self.threads).remove(&thread_id)
    }

    pub fn get(&self, thread_id: CRDThreadId) -> Option<CRDThreadRecord> {
        crd_lock(&self.threads).get(&thread_id).cloned()
    }

    pub fn len(&self) -> usize {
        crd_lock(&self.threads).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Copy of every live thread, sorted by thread id.
    pub fn snapshot(&self) -> Vec<CRDThreadRecord> {
        let mut all: Vec<CRDThreadRecord> = crd_lock(&self.threads).values().cloned().collect();
        all.sort_by_key(|record| record.thread_id);
        all
    }
}