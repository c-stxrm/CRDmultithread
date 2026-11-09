//! Reusable implementation utilities for CRD jobs.
//!
//! This module builds on the communication contract in
//! `interfaces/crd_job.rs`. It does not define a transport, create threads,
//! create queues, or schedule jobs.
//!
//! # Writing a job (the short way)
//!
//! 1. Define the command, report and display enums.
//! 2. `impl CRDCommand for <CommandEnum>` to declare its report data type.
//! 3. `impl CRDJobHandler for <Job>` and write `on_command`.
//!
//! That is all. `CRDJob` is provided automatically for every
//! `CRDJobHandler`, including `run`, the start/stop hooks and the command loop.
//!
//! ```ignore
//! impl CRDJobHandler for MyJob {
//!     type Command = MyCommand;
//!     type CommandId = u64;
//!     type DisplayData = MyDisplay;
//!
//!     fn on_command(&mut self, command: CRDHandlerCommand<Self>, output: CRDHandlerOutput<'_, Self>) {
//!         match command.data {
//!             MyCommand::Ping => output.report_success_data(command.id, "pong", MyReport::Pong),
//!         }
//!     }
//! }
//! ```
//!
//! # Waiting behaviour
//!
//! A job can be driven with `Receiver::into_iter()`. The iterator is consumed
//! sequentially: it waits for the next command and ends when all senders are
//! dropped.

use crate::interfaces::crd_job::{
    CRDCommand,
    CRDJob,
    CRDJobCommand,
    CRDJobDisplay,
    CRDJobOutput,
    CRDJobReport,
    CRDReportStatus,
};


// ============================================================================
// 1. OUTPUT HELPERS
// ============================================================================

/// Convenience methods for publishing the standard CRD output envelopes.
///
/// This is an extension trait over the existing `CRDJobOutput` contract. It
/// does not replace or modify that contract. It is implemented for every
/// `CRDJobOutput`, including `dyn CRDJobOutput<I, R, D>`.
///
/// The trait must be in scope to call these methods.
pub trait CRDJobOutputExt<I, R, D>: CRDJobOutput<I, R, D> {
    /// Publishes a report indicating that the command is still being processed.
    fn report_working(
        &mut self,
        command_id: I,
        message: Option<String>,
        data: Option<R>,
    ) {
        self.report(CRDJobReport {
            command_id,
            status: CRDReportStatus::Working,
            message,
            data,
        });
    }

    /// Publishes a successful terminal report for a command.
    fn report_success(
        &mut self,
        command_id: I,
        message: Option<String>,
        data: Option<R>,
    ) {
        self.report(CRDJobReport {
            command_id,
            status: CRDReportStatus::Success,
            message,
            data,
        });
    }

    /// Publishes an error terminal report for a command.
    fn report_error(
        &mut self,
        command_id: I,
        message: Option<String>,
        data: Option<R>,
    ) {
        self.report(CRDJobReport {
            command_id,
            status: CRDReportStatus::Error,
            message,
            data,
        });
    }

    /// Shorthand: `Working` report with a message and no data.
    fn report_working_message(&mut self, command_id: I, message: impl Into<String>) {
        self.report_working(command_id, Some(message.into()), None);
    }

    /// Shorthand: `Success` report with a message and structured data.
    fn report_success_data(&mut self, command_id: I, message: impl Into<String>, data: R) {
        self.report_success(command_id, Some(message.into()), Some(data));
    }

    /// Shorthand: `Error` report with a message and no data.
    fn report_error_message(&mut self, command_id: I, message: impl Into<String>) {
        self.report_error(command_id, Some(message.into()), None);
    }

    /// Publishes an unsolicited display event.
    fn publish_display(&mut self, data: D) {
        self.display(CRDJobDisplay { data });
    }
}

// `T: ?Sized` makes the helpers available on `&mut dyn CRDJobOutput<..>`,
// which is what `CRDJobHandler` methods receive.
impl<I, R, D, T> CRDJobOutputExt<I, R, D> for T
where
    T: CRDJobOutput<I, R, D> + ?Sized,
{
}


// ============================================================================
// 2. COMMAND LOOP (closure style)
// ============================================================================

/// Consumes a job's incoming commands sequentially and delegates each command
/// to the given handler closure.
///
/// `C` must implement `CRDCommand`, and the output's report payload type must
/// match `C::ReportData`. This ties the command manifest to its declared
/// report manifest without changing the shared `CRDJob` contract.
///
/// Prefer `CRDJobHandler` for new jobs; this function is for jobs that want to
/// write their own `CRDJob::run` and only need the loop.
///
/// This function does not create or manage the iterator's transport.
pub fn crd_process_commands<I, C, D, O, Commands, Handler>(
    commands: Commands,
    output: &mut O,
    mut handler: Handler,
)
where
    C: CRDCommand,
    Commands: Iterator<Item = CRDJobCommand<I, C>>,
    O: CRDJobOutput<I, C::ReportData, D>,
    Handler: FnMut(CRDJobCommand<I, C>, &mut O),
{
    for command in commands {
        handler(command, output);
    }
}


// ============================================================================
// 3. JOB HANDLER (the easy way to write a job)
// ============================================================================

/// The output a handler method receives, derived from the handler's own
/// associated types so a job never has to spell out the generics.
pub type CRDHandlerOutput<'a, H> = &'a mut dyn CRDJobOutput<
    <H as CRDJobHandler>::CommandId,
    <<H as CRDJobHandler>::Command as CRDCommand>::ReportData,
    <H as CRDJobHandler>::DisplayData,
>;

/// The command envelope a handler receives.
pub type CRDHandlerCommand<H> = CRDJobCommand<
    <H as CRDJobHandler>::CommandId,
    <H as CRDJobHandler>::Command,
>;

/// A job that processes one command at a time.
///
/// Implementing this trait is all a typical job needs. `CRDJob` is
/// implemented automatically (see the blanket impl below), with this
/// lifecycle:
///
/// 1. `on_start` once, before the first command.
/// 2. `on_command` once per command, in arrival order.
/// 3. `on_stop` once, after the command input has closed.
///
/// A type must implement either `CRDJobHandler` or `CRDJob` directly, not both.
pub trait CRDJobHandler: Sized + Send + 'static {
    /// Command manifest (usually an enum). Its `CRDCommand::ReportData`
    /// is the report payload type of this job.
    type Command: CRDCommand;

    /// Type used to identify commands.
    type CommandId: Send + 'static;

    /// Display payload type (usually an enum).
    type DisplayData: Send + 'static;

    /// Called once before the first command. Default: does nothing.
    fn on_start(&mut self, output: CRDHandlerOutput<'_, Self>) {
        let _ = output;
    }

    /// Handles one command. Publish reports and displays through `output`.
    fn on_command(
        &mut self,
        command: CRDHandlerCommand<Self>,
        output: CRDHandlerOutput<'_, Self>,
    );

    /// Called once after command input closes. Default: does nothing.
    fn on_stop(&mut self, output: CRDHandlerOutput<'_, Self>) {
        let _ = output;
    }
}

/// Every `CRDJobHandler` is a `CRDJob`.
impl<H: CRDJobHandler> CRDJob for H {
    type Command = <H as CRDJobHandler>::Command;
    type CommandId = <H as CRDJobHandler>::CommandId;
    type ReportData = <<H as CRDJobHandler>::Command as CRDCommand>::ReportData;
    type DisplayData = <H as CRDJobHandler>::DisplayData;

    fn run<C, O>(
        mut self,
        commands: C,
        output: &mut O,
    )
    where
        C: Iterator<
            Item = CRDJobCommand<
                <H as CRDJobHandler>::CommandId,
                <H as CRDJobHandler>::Command,
            >,
        >,
        O: CRDJobOutput<
            <H as CRDJobHandler>::CommandId,
            <<H as CRDJobHandler>::Command as CRDCommand>::ReportData,
            <H as CRDJobHandler>::DisplayData,
        >,
    {
        self.on_start(output);

        for command in commands {
            self.on_command(command, output);
        }

        self.on_stop(output);
    }
}