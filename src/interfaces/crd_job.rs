use std::fmt::Debug;

// ============================================================================
// CRD — COMMON JOB COMMUNICATION PROTOCOL
// ============================================================================
//
// A CRDJob is an independent unit of execution.
//
// Every job communicates with its owner through three conceptual channels:
//
// 1. COMMAND : Owner -> Job
// 2. REPORT  : Job -> Owner (associated with a command)
// 3. DISPLAY : Job -> Owner (independent of commands)
//
// The protocol is generic. Each job defines its own communication manifest:
//
// - Which commands it accepts.
// - What structured data each command reports.
// - What structured data it can display.
//
// This file defines the shared protocol, not the transport mechanism.
// Thread creation, message queues, channels and scheduling belong elsewhere.


// ============================================================================
// 1. REPORT STATUS
// ============================================================================

/// Standardized execution state of a command.
///
/// These statuses have the same meaning across every CRDJob.
///
/// - `Working` : The command is being processed.
/// - `Success` : The command completed successfully.
/// - `Error`   : The command failed.
///
/// A command may produce multiple `Working` reports before reaching a
/// terminal state (`Success` or `Error`).
///
/// The status is standardized deliberately: the owner should not need to
/// understand job-specific terminology to determine execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CRDReportStatus {
    Working,
    Success,
    Error,
}


// ============================================================================
// 2. COMMAND
// ============================================================================

/// A standardized command envelope.
///
/// - `I` : Command identifier type.
/// - `C` : Job-specific command payload.
///
/// The identifier is assigned by the owner and is used to correlate every
/// subsequent report with the original command.
///
/// The payload is strongly typed. Its structure is defined by the job.
///
/// Example: `CRDJobCommand<u64, StartVM>` is a command with a `u64`
/// identifier and a `StartVM` payload.
#[derive(Debug)]
pub struct CRDJobCommand<I, C> {
    /// Identifier assigned to this command by its owner.
    ///
    /// Convention (when not a plain counter): timestamp + 4 alphanumeric chars.
    pub id: I,

    /// Job-specific command instructions and parameters.
    pub data: C,
}


// ============================================================================
// 3. REPORT
// ============================================================================

/// A standardized report envelope.
///
/// - `I` : Command identifier type.
/// - `D` : Job-specific report payload.
///
/// Every report references the command it concerns.
///
/// A report contains three independent pieces of information:
///
/// 1. Status  : Standardized execution state.
/// 2. Message : Optional human-readable information.
/// 3. Data    : Optional strongly typed structured information.
///
/// Message and Data are deliberately separate: Message is for humans, logs
/// and diagnostics; Data is for programmatic consumption. A report does not
/// necessarily contain both.
///
/// Example: `CRDJobReport<u64, VMStartupResult>`.
#[derive(Debug)]
pub struct CRDJobReport<I, D> {
    /// Identifier of the command this report belongs to.
    pub command_id: I,

    /// Standardized execution status.
    pub status: CRDReportStatus,

    /// Optional human-readable message.
    pub message: Option<String>,

    /// Optional job-specific structured report data.
    pub data: Option<D>,
}


// ============================================================================
// 4. DISPLAY
// ============================================================================

/// A standardized display envelope.
///
/// - `D` : Job-specific display payload.
///
/// DISPLAY is independent of COMMAND and REPORT. A job can publish display
/// information whenever it has something relevant to communicate, without
/// waiting for an owner instruction. Displays do not carry command identifiers.
///
/// Examples: CPU/memory metrics, network statistics, VM state changes,
/// hardware telemetry.
#[derive(Debug)]
pub struct CRDJobDisplay<D> {
    /// Job-specific information published to the owner.
    pub data: D,
}


// ============================================================================
// 5. COMMAND-SPECIFIC CONTRACT
// ============================================================================

/// Associates a command type with its corresponding report data type.
///
/// Every command manifest implements this trait and declares the structured
/// data that its reports are allowed to carry. This establishes the
/// command/report relationship at the type level.
///
/// This trait describes the association; it does not implement execution.
pub trait CRDCommand: Send + 'static {
    /// Structured data type associated with this command's reports.
    type ReportData: Send + 'static;
}


// ============================================================================
// 6. OUTGOING COMMUNICATION INTERFACE
// ============================================================================

/// The job's outgoing communication interface.
///
/// The runtime provides an implementation of this interface. Jobs use it to
/// communicate with their owner without knowing how messages are physically
/// transported.
///
/// REPORT and DISPLAY are separate operations because they have different
/// semantics.
///
/// This trait is object safe (`&mut dyn CRDJobOutput<I, R, D>` is valid).
///
/// - `I` : Command identifier type.
/// - `R` : Job-specific report data type.
/// - `D` : Job-specific display data type.
pub trait CRDJobOutput<I, R, D> {
    /// Publish a report concerning a specific command.
    ///
    /// Multiple reports may reference the same command identifier.
    fn report(&mut self, report: CRDJobReport<I, R>);

    /// Publish unsolicited information.
    ///
    /// This operation does not require a command identifier.
    fn display(&mut self, display: CRDJobDisplay<D>);
}


// ============================================================================
// 7. MAIN CRD JOB CONTRACT
// ============================================================================

/// Common contract implemented by every CRD job.
///
/// CRDJob defines the generic requirements of an independent job. Each
/// implementation supplies its own command manifest, command identifier type,
/// report payload type and display payload type.
///
/// The job implementation interprets commands, performs its work and decides
/// when to publish reports or displays.
///
/// The runtime is responsible for creating the job's execution thread,
/// providing incoming commands and the outgoing interface, and managing the
/// job's lifecycle. CRDJob does not prescribe a transport mechanism.
///
/// Most jobs should NOT implement this trait by hand: implement
/// `CRDJobHandler` (see `templates/crd_job_template.rs`), which provides
/// `CRDJob` automatically.
pub trait CRDJob: Send + 'static {
    /// Job-specific command manifest (usually an enum).
    type Command: Send + 'static;

    /// Type used to identify commands.
    ///
    /// Must be consistent between incoming commands and outgoing reports.
    type CommandId: Send + 'static;

    /// Job-specific structured report data (usually an enum).
    type ReportData: Send + 'static;

    /// Job-specific structured display data (usually an enum).
    type DisplayData: Send + 'static;

    /// Execute the job.
    ///
    /// The runtime supplies an iterator of incoming commands and an outgoing
    /// communication interface. The job controls its own command-processing
    /// logic. The iterator ends when command input is closed.
    fn run<C, O>(
        self,
        commands: C,
        output: &mut O,
    )
    where
        C: Iterator<Item = CRDJobCommand<Self::CommandId, Self::Command>>,
        O: CRDJobOutput<
            Self::CommandId,
            Self::ReportData,
            Self::DisplayData,
        >;
}