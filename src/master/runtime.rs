//! CRDMaster: composition root. Spawns jobs and wires them to the tables,
//! queues and logs.
//!
//! # Hosting a job
//!
//! ```ignore
//! let logs = CRDMasterLogs::new();
//! logs.add_sink(CRDStdoutSink::new(CRDLogLevel::Info));
//!
//! let master = CRDMaster::new(logs);
//! let mut group = master.spawn("hello_world", vec![CRDHelloWorld::new()])?;
//!
//! let id = group.queue().send(CRDHelloWorldCommand::GetGreetingCount)?;
//! // ... group.queue().try_recv_report(), group.display() ...
//!
//! group.shutdown();
//! ```
//!
//! Pass several instances (`vec![a, b, c]`) to run one job type on several
//! threads. Commands are distributed round robin.
//!
//! # Requirements on a job
//!
//! - `CommandId = u64`: the master assigns ids.
//! - `Command`, `ReportData` and `DisplayData` implement `Debug` (logging).
//!
//! # Failure handling
//!
//! If a job ends or panics with commands still pending, each of them is
//! removed from the command table, logged as an error, and answered with a
//! synthetic `Error` report so the owner is never left waiting.

use std::fmt::{self, Debug};
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::interfaces::crd_job::{
    CRDJob, CRDJobCommand, CRDJobDisplay, CRDJobOutput, CRDJobReport, CRDReportStatus,
};

use super::command_table::{CRDMasterCommandTable, CRDReportOutcome};
use super::ids::{crd_current_thread_id, CRDJobId, CRDJobKey, CRDThreadId};
use super::logs::{crd_field, CRDLogLevel, CRDMasterLogs};
use super::queues::{
    CRDMasterCommandReportQueue, CRDMasterDisplay, CRDMasterDisplayQueue, CRDMasterReport,
};
use super::thread_table::CRDMasterThreadTable;

// ============================================================================
// 1. ERRORS
// ============================================================================

#[derive(Debug)]
pub enum CRDSpawnError {
    /// `spawn` was given no job instances.
    NoInstances,
    /// The OS refused to create a thread.
    Thread(io::Error),
}

impl fmt::Display for CRDSpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CRDSpawnError::NoInstances => f.write_str("no job instances were provided"),
            CRDSpawnError::Thread(error) => write!(f, "cannot create job thread: {}", error),
        }
    }
}

impl std::error::Error for CRDSpawnError {}

// ============================================================================
// 2. MASTER
// ============================================================================

/// Owns the shared bookkeeping and spawns job groups.
pub struct CRDMaster {
    logs: CRDMasterLogs,
    commands: Arc<CRDMasterCommandTable>,
    threads: Arc<CRDMasterThreadTable>,
    next_job_id: AtomicU32,
}

impl CRDMaster {
    pub fn new(logs: CRDMasterLogs) -> Self {
        Self {
            logs,
            commands: Arc::new(CRDMasterCommandTable::new()),
            threads: Arc::new(CRDMasterThreadTable::new()),
            next_job_id: AtomicU32::new(1),
        }
    }

    pub fn logs(&self) -> &CRDMasterLogs {
        &self.logs
    }

    /// Commands that were sent and have not finished.
    pub fn commands(&self) -> &CRDMasterCommandTable {
        &self.commands
    }

    /// Threads that are alive, with their pinned jobs.
    pub fn threads(&self) -> &CRDMasterThreadTable {
        &self.threads
    }

    /// Starts one job group: one OS thread per instance.
    ///
    /// `name` labels the group in logs and tables. Thread names are
    /// `<name>-<index>`.
    pub fn spawn<J>(
        &self,
        name: &'static str,
        instances: Vec<J>,
    ) -> Result<CRDJobGroup<J>, CRDSpawnError>
    where
        J: CRDJob<CommandId = u64>,
        J::ReportData: Debug,
        J::DisplayData: Debug,
    {
        if instances.is_empty() {
            return Err(CRDSpawnError::NoInstances);
        }

        let job_id = CRDJobId(self.next_job_id.fetch_add(1, Ordering::Relaxed));
        let instance_count = instances.len();

        let (report_tx, report_rx) = mpsc::channel::<CRDMasterReport<J::ReportData>>();
        let (display_tx, display_rx) = mpsc::channel::<CRDMasterDisplay<J::DisplayData>>();

        let mut queue = CRDMasterCommandReportQueue::<J::Command, J::ReportData>::new(
            job_id,
            name,
            Arc::clone(&self.commands),
            self.logs.clone(),
            report_rx,
        );
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(instance_count);

        for (index, job) in instances.into_iter().enumerate() {
            let (command_tx, command_rx) = mpsc::channel::<CRDJobCommand<u64, J::Command>>();

            let context = CRDJobThreadContext {
                key: CRDJobKey {
                    job: job_id,
                    instance: index as u32,
                },
                name,
                commands: Arc::clone(&self.commands),
                threads: Arc::clone(&self.threads),
                logs: self.logs.clone(),
                report_tx: report_tx.clone(),
                display_tx: display_tx.clone(),
            };

            let spawned = thread::Builder::new()
                .name(format!("{}-{}", name, index))
                .spawn(move || crd_job_thread_main::<J>(job, command_rx, context));

            match spawned {
                Ok(handle) => {
                    queue.add_instance(command_tx);
                    handles.push(handle);
                }
                Err(error) => {
                    // Closing the senders lets already-started instances end.
                    queue.close_commands();
                    drop(command_tx);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    self.logs.error_with(
                        "crd.master",
                        "cannot create job thread",
                        &[
                            crd_field("job", name),
                            crd_field("instance", index as u64),
                            crd_field("error", error.to_string()),
                        ],
                    );
                    return Err(CRDSpawnError::Thread(error));
                }
            }
        }

        // Only the job threads keep the report/display channels connected now.
        drop(report_tx);
        drop(display_tx);

        self.logs.info_with(
            "crd.master",
            "job group spawned",
            &[
                crd_field("job", name),
                crd_field("job_id", u64::from(job_id.0)),
                crd_field("instances", instance_count),
            ],
        );

        Ok(CRDJobGroup {
            name,
            queue,
            display: Some(CRDMasterDisplayQueue::new(job_id, name, display_rx)),
            threads: handles,
            logs: self.logs.clone(),
        })
    }
}

// ============================================================================
// 3. JOB GROUP (owner-side handle)
// ============================================================================

/// One job type running on one or more threads.
///
/// Dropping the group shuts it down (see `shutdown`).
pub struct CRDJobGroup<J: CRDJob> {
    name: &'static str,
    queue: CRDMasterCommandReportQueue<J::Command, J::ReportData>,
    display: Option<CRDMasterDisplayQueue<J::DisplayData>>,
    threads: Vec<JoinHandle<()>>,
    logs: CRDMasterLogs,
}

impl<J: CRDJob> CRDJobGroup<J> {
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Commands out, reports in.
    pub fn queue(&self) -> &CRDMasterCommandReportQueue<J::Command, J::ReportData> {
        &self.queue
    }

    /// The display queue, unless it was taken.
    pub fn display(&self) -> Option<&CRDMasterDisplayQueue<J::DisplayData>> {
        self.display.as_ref()
    }

    /// Moves the display queue out, e.g. to hand it to a UI thread.
    pub fn take_display_queue(&mut self) -> Option<CRDMasterDisplayQueue<J::DisplayData>> {
        self.display.take()
    }

    /// Stops accepting commands, lets the jobs finish what is queued, and
    /// waits for every thread to end. Safe to call more than once.
    pub fn shutdown(&mut self) {
        if self.threads.is_empty() {
            return;
        }

        self.logs.info_with(
            "crd.master",
            "job group shutting down",
            &[
                crd_field("job", self.name),
                crd_field("instances", self.threads.len()),
            ],
        );

        self.queue.close_commands();

        for handle in self.threads.drain(..) {
            if handle.join().is_err() {
                self.logs.error_with(
                    "crd.master",
                    "job thread panicked",
                    &[crd_field("job", self.name)],
                );
            }
        }
    }
}

impl<J: CRDJob> Drop for CRDJobGroup<J> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ============================================================================
// 4. JOB THREAD
// ============================================================================

/// Everything a job thread needs besides the job and its command input.
struct CRDJobThreadContext<R, D> {
    key: CRDJobKey,
    name: &'static str,
    commands: Arc<CRDMasterCommandTable>,
    threads: Arc<CRDMasterThreadTable>,
    logs: CRDMasterLogs,
    report_tx: Sender<CRDMasterReport<R>>,
    display_tx: Sender<CRDMasterDisplay<D>>,
}

/// Registers the thread and its pinned job on creation, and removes them on
/// drop, including when the job panics.
struct CRDThreadGuard {
    threads: Arc<CRDMasterThreadTable>,
    logs: CRDMasterLogs,
    thread_id: CRDThreadId,
    key: CRDJobKey,
    name: &'static str,
}

impl CRDThreadGuard {
    fn enter(
        threads: Arc<CRDMasterThreadTable>,
        logs: CRDMasterLogs,
        key: CRDJobKey,
        name: &'static str,
    ) -> Self {
        let thread_id = crd_current_thread_id();
        let thread_name = thread::current().name().unwrap_or("crd-job").to_owned();

        threads.register_thread(thread_id, &thread_name);
        threads.pin_job(thread_id, key);

        logs.info_with(
            "crd.thread",
            "job thread started",
            &[
                crd_field("job", name),
                crd_field("instance", key.instance),
                crd_field("thread", thread_name),
            ],
        );

        Self {
            threads,
            logs,
            thread_id,
            key,
            name,
        }
    }
}

impl Drop for CRDThreadGuard {
    fn drop(&mut self) {
        self.threads.unpin_job(self.thread_id, self.key);
        self.threads.unregister_thread(self.thread_id);

        let fields = [
            crd_field("job", self.name),
            crd_field("instance", self.key.instance),
        ];

        if thread::panicking() {
            self.logs
                .error_with("crd.thread", "job thread panicked", &fields);
        } else {
            self.logs
                .info_with("crd.thread", "job thread finished", &fields);
        }
    }
}

/// Body of every job thread.
fn crd_job_thread_main<J>(
    job: J,
    commands: Receiver<CRDJobCommand<u64, J::Command>>,
    context: CRDJobThreadContext<J::ReportData, J::DisplayData>,
) where
    J: CRDJob<CommandId = u64>,
    J::ReportData: Debug,
    J::DisplayData: Debug,
{
    // Declared first, dropped last: the thread stays in the table until the
    // output below has cleaned up its pending commands.
    let _guard = CRDThreadGuard::enter(
        context.threads,
        context.logs.clone(),
        context.key,
        context.name,
    );

    let mut output = CRDMasterJobOutput {
        key: context.key,
        name: context.name,
        commands: context.commands,
        logs: context.logs,
        report_tx: context.report_tx,
        display_tx: context.display_tx,
    };

    job.run(commands.into_iter(), &mut output);
}

// ============================================================================
// 5. JOB OUTPUT (what the job talks to)
// ============================================================================

fn crd_status_str(status: CRDReportStatus) -> &'static str {
    match status {
        CRDReportStatus::Working => "working",
        CRDReportStatus::Success => "success",
        CRDReportStatus::Error => "error",
    }
}

/// The `CRDJobOutput` handed to a job: it updates the command table, logs,
/// and forwards to the owner's queues.
struct CRDMasterJobOutput<R, D> {
    key: CRDJobKey,
    name: &'static str,
    commands: Arc<CRDMasterCommandTable>,
    logs: CRDMasterLogs,
    report_tx: Sender<CRDMasterReport<R>>,
    display_tx: Sender<CRDMasterDisplay<D>>,
}

impl<R: Debug, D: Debug> CRDJobOutput<u64, R, D> for CRDMasterJobOutput<R, D> {
    fn report(&mut self, report: CRDJobReport<u64, R>) {
        let outcome = self.commands.record_report(
            report.command_id,
            report.status,
            report.message.as_deref(),
        );

        let level = match (&outcome, report.status) {
            (CRDReportOutcome::Unknown, _) => CRDLogLevel::Warn,
            (_, CRDReportStatus::Working) => CRDLogLevel::Debug,
            (_, CRDReportStatus::Success) => CRDLogLevel::Info,
            (_, CRDReportStatus::Error) => CRDLogLevel::Warn,
        };

        if self.logs.enabled(level) {
            let mut fields = vec![
                crd_field("command", report.command_id),
                crd_field("status", crd_status_str(report.status)),
                crd_field("job", self.name),
                crd_field("instance", self.key.instance),
            ];

            if let Some(message) = &report.message {
                fields.push(crd_field("message", message.as_str()));
            }

            if self.logs.enabled(CRDLogLevel::Debug) {
                if let Some(data) = &report.data {
                    fields.push(crd_field("data", format!("{:?}", data)));
                }
            }

            if let CRDReportOutcome::Finished(record) = &outcome {
                fields.push(crd_field("reports", record.report_count));
                fields.push(crd_field(
                    "elapsed_us",
                    record.sent_at.elapsed().as_micros() as u64,
                ));
            }

            let text = match &outcome {
                CRDReportOutcome::Unknown => {
                    "report for a command that is not pending (protocol violation)"
                }
                CRDReportOutcome::Finished(_) => "command finished",
                CRDReportOutcome::Updated => "command progress",
            };

            self.logs.log(level, "crd.report", text, &fields);
        }

        // The owner may have dropped its queue; reports are then discarded.
        let _ = self.report_tx.send(CRDMasterReport {
            instance: self.key.instance,
            report,
        });
    }

    fn display(&mut self, display: CRDJobDisplay<D>) {
        if self.logs.enabled(CRDLogLevel::Debug) {
            self.logs.debug_with(
                "crd.display",
                "display published",
                &[
                    crd_field("job", self.name),
                    crd_field("instance", self.key.instance),
                    crd_field("data", format!("{:?}", display.data)),
                ],
            );
        }

        let _ = self.display_tx.send(CRDMasterDisplay {
            instance: self.key.instance,
            data: display.data,
        });
    }
}

impl<R, D> Drop for CRDMasterJobOutput<R, D> {
    /// Runs when the job ends (normally or by panic). Any command it never
    /// finished is removed from the table and answered with an `Error` report.
    fn drop(&mut self) {
        for record in self.commands.abandon_job(self.key) {
            self.logs.error_with(
                "crd.report",
                "command abandoned: the job ended without a terminal report",
                &[
                    crd_field("command", record.id),
                    crd_field("job", self.name),
                    crd_field("instance", self.key.instance),
                    crd_field("reports", record.report_count),
                ],
            );

            let _ = self.report_tx.send(CRDMasterReport {
                instance: self.key.instance,
                report: CRDJobReport {
                    command_id: record.id,
                    status: CRDReportStatus::Error,
                    message: Some("the job ended before finishing the command".to_string()),
                    data: None,
                },
            });
        }
    }
}