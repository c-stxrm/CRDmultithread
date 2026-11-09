//! # Hello World Job
//!
//! The canonical example of a CRD job written with the template
//! (`CRDJobHandler`). Every capability of the contract and the template is
//! demonstrated here once.
//!
//! # How to write a job
//!
//! 1. Define the **command**, **report** and **display** enums.
//! 2. `impl CRDCommand for <CommandEnum>` to declare its report data type.
//! 3. Define the job struct (its private state).
//! 4. `impl CRDJobHandler for <Job>`: three associated types and `on_command`.
//!    `on_start` and `on_stop` are optional.
//!
//! `CRDJob` is then provided automatically. A job never writes `run`.
//!
//! # Rules every job follows
//!
//! - **One terminal report per command.** A command produces zero or more
//!   `Working` reports, then exactly one `Success` or `Error`. Never report
//!   after a terminal report, and never leave a command without one.
//! - **Reports are for commands, displays are for the world.** Use a report to
//!   answer a command; use a display for anything the job publishes on its own.
//! - **Message is for humans, data is for programs.** They are independent
//!   and both optional.
//! - **Reusing the command id.** Several reports for one command need the id
//!   more than once, so `CommandId` should be `Copy` or `Clone` (`u64` is).
//!   The contract itself does not require it.
//!
//! # Output helpers (`CRDJobOutputExt`)
//!
//! | Helper                                  | Status    | Message | Data |
//! |-----------------------------------------|-----------|---------|------|
//! | `report_working_message(id, msg)`       | `Working` | yes     | no   |
//! | `report_working(id, msg, data)`         | `Working` | opt.    | opt. |
//! | `report_success_data(id, msg, data)`    | `Success` | yes     | yes  |
//! | `report_success(id, msg, data)`         | `Success` | opt.    | opt. |
//! | `report_error_message(id, msg)`         | `Error`   | yes     | no   |
//! | `report_error(id, msg, data)`           | `Error`   | opt.    | opt. |
//! | `publish_display(data)`                 | (display) | -       | yes  |
//!
//! # Alternatives to `CRDJobHandler`
//!
//! - **Closure style**, when you want to write your own `CRDJob::run` but
//!   still skip the loop:
//!
//!   ```ignore
//!   crd_process_commands(commands, output, |command, output| { /* ... */ });
//!   ```
//!
//! - **Fully manual**, for jobs needing total control (e.g. polling between
//!   commands): implement `CRDJob::run` directly. A type implements either
//!   `CRDJobHandler` or `CRDJob`, never both.
//!
//! This job implements neither threading nor transport; those belong to the
//! runtime.

// Report/display payload fields are only read through `Debug` by the demo
// sink in `main.rs`. A real owner would match on them.
#![allow(dead_code)]

use crate::interfaces::crd_job::{CRDCommand, CRDJobCommand};
use crate::templates::crd_job_template::{
    CRDHandlerCommand,
    CRDHandlerOutput,
    CRDJobHandler,
    CRDJobOutputExt,
};


// ============================================================================
// 1. COMMAND MANIFEST
// ============================================================================

/// Commands accepted by `CRDHelloWorld`.
#[derive(Debug)]
pub enum CRDHelloWorldCommand {
    /// Greet one person. Single-step: one terminal report.
    Greet { name: String },

    /// Greet several people. Multi-step: `Working` reports with progress,
    /// then one terminal report.
    GreetMany { names: Vec<String> },

    /// Ask for the current greeting count.
    GetGreetingCount,
}

/// Declares which report payload belongs to this command manifest.
///
/// This is what ties `CRDHelloWorldCommand` to `CRDHelloWorldReport` at the
/// type level.
impl CRDCommand for CRDHelloWorldCommand {
    type ReportData = CRDHelloWorldReport;
}


// ============================================================================
// 2. REPORT MANIFEST
// ============================================================================

/// Structured report data produced by `CRDHelloWorld`.
#[derive(Debug)]
pub enum CRDHelloWorldReport {
    /// `Greet` succeeded.
    GreetingDelivered { greeting: String },

    /// `GreetMany` progress (carried by `Working` reports).
    Progress { done: usize, total: usize },

    /// `GreetMany` succeeded for at least one name.
    GreetingsDelivered {
        greetings: Vec<String>,
        rejected: usize,
    },

    /// `GreetMany` failed: every name was rejected (carried by an `Error`
    /// report, showing that errors can hold structured data too).
    BatchRejected { rejected: usize },

    /// `GetGreetingCount` succeeded.
    GreetingCount { count: u64 },
}


// ============================================================================
// 3. DISPLAY MANIFEST
// ============================================================================

/// Unsolicited information published by `CRDHelloWorld`.
///
/// None of these answer a command; the job publishes them on its own.
#[derive(Debug)]
pub enum CRDHelloWorldDisplay {
    /// The job has started executing.
    Started,

    /// The greeting count has changed.
    GreetingCountChanged { count: u64 },

    /// The job has stopped executing.
    Stopped,
}


// ============================================================================
// 4. JOB
// ============================================================================

/// HelloWorld job. Maintains a greeting counter.
pub struct CRDHelloWorld {
    greeting_count: u64,
}

impl CRDHelloWorld {
    /// Creates a new `CRDHelloWorld` job.
    pub fn new() -> Self {
        Self { greeting_count: 0 }
    }
}


// ============================================================================
// 5. BEHAVIOUR
// ============================================================================

impl CRDJobHandler for CRDHelloWorld {
    type Command = CRDHelloWorldCommand;
    type CommandId = u64;
    type DisplayData = CRDHelloWorldDisplay;

    /// Lifecycle hook: runs once, before the first command.
    fn on_start(&mut self, output: CRDHandlerOutput<'_, Self>) {
        output.publish_display(CRDHelloWorldDisplay::Started);
    }

    /// Runs once per command, in arrival order.
    fn on_command(
        &mut self,
        command: CRDHandlerCommand<Self>,
        output: CRDHandlerOutput<'_, Self>,
    ) {
        let CRDJobCommand { id, data } = command;

        match data {
            // ----------------------------------------------------------------
            // Single-step command: validate, act, one terminal report.
            // ----------------------------------------------------------------
            CRDHelloWorldCommand::Greet { name } => {
                if name.trim().is_empty() {
                    // Terminal report: Error, message only.
                    output.report_error_message(id, "Cannot greet an empty name.");
                    return;
                }

                self.greeting_count += 1;

                // Terminal report: Success, message and data.
                output.report_success_data(
                    id,
                    "Greeting successfully generated.",
                    CRDHelloWorldReport::GreetingDelivered {
                        greeting: format!("Hello, {}!", name),
                    },
                );

                // Unsolicited display, independent of the report above.
                output.publish_display(CRDHelloWorldDisplay::GreetingCountChanged {
                    count: self.greeting_count,
                });
            }

            // ----------------------------------------------------------------
            // Multi-step command: Working* then exactly one terminal report.
            // ----------------------------------------------------------------
            CRDHelloWorldCommand::GreetMany { names } => {
                let total = names.len();

                if total == 0 {
                    output.report_error_message(id, "No names to greet.");
                    return;
                }

                // Working, message only.
                output.report_working_message(
                    id,
                    format!("Greeting {} name(s).", total),
                );

                let mut greetings = Vec::new();
                let mut rejected = 0usize;

                for (index, name) in names.into_iter().enumerate() {
                    if name.trim().is_empty() {
                        rejected += 1;
                    } else {
                        self.greeting_count += 1;
                        greetings.push(format!("Hello, {}!", name));
                    }

                    // Working, structured data only (raw helper, Option args).
                    output.report_working(
                        id,
                        None,
                        Some(CRDHelloWorldReport::Progress {
                            done: index + 1,
                            total,
                        }),
                    );
                }

                if greetings.is_empty() {
                    // Terminal report: Error that still carries data.
                    output.report_error(
                        id,
                        Some("Every name was empty.".to_string()),
                        Some(CRDHelloWorldReport::BatchRejected { rejected }),
                    );
                    return;
                }

                // Terminal report: Success (raw helper, Option args).
                output.report_success(
                    id,
                    Some(format!(
                        "Greeted {} name(s), rejected {}.",
                        greetings.len(),
                        rejected
                    )),
                    Some(CRDHelloWorldReport::GreetingsDelivered {
                        greetings,
                        rejected,
                    }),
                );

                output.publish_display(CRDHelloWorldDisplay::GreetingCountChanged {
                    count: self.greeting_count,
                });
            }

            // ----------------------------------------------------------------
            // Query command: read state, one terminal report, no display.
            // ----------------------------------------------------------------
            CRDHelloWorldCommand::GetGreetingCount => {
                output.report_success_data(
                    id,
                    "Greeting count retrieved.",
                    CRDHelloWorldReport::GreetingCount {
                        count: self.greeting_count,
                    },
                );
            }
        }
    }

    /// Lifecycle hook: runs once, after command input closes.
    fn on_stop(&mut self, output: CRDHandlerOutput<'_, Self>) {
        output.publish_display(CRDHelloWorldDisplay::Stopped);
    }
}