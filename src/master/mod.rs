//! # CRD master
//!
//! The owner-side machinery that hosts CRD jobs.
//!
//! ```text
//!                       CRDMaster  (composition root)
//!        ┌──────────────────┬──────────────────┬──────────────────┐
//!  CRDMasterCommandTable  CRDMasterThreadTable  CRDMasterLogs
//!  (in-flight commands)   (live threads + jobs) (multi-sink logging)
//!
//!  Per job group (one job type, 1..N instances, one thread each):
//!
//!   owner ──send──▶ CRDMasterCommandReportQueue ──round robin──▶ instance 0..N
//!   owner ◀─reports─────────────────────────────────────────────  instances
//!   owner ◀─displays── CRDMasterDisplayQueue ◀──────────────────  instances
//! ```
//!
//! Responsibilities (one reason to change each):
//!
//! - `ids`           : identifiers and the fast integer hasher.
//! - `logs`          : logging core and sinks. Knows nothing about jobs.
//! - `command_table` : in-flight command bookkeeping.
//! - `thread_table`  : live thread / pinned job bookkeeping.
//! - `queues`        : typed per-job queues (commands, reports, displays).
//! - `runtime`       : `CRDMaster`, which spawns jobs and wires everything.
//!
//! The master adds nothing to the job contract. A job is a plain `CRDJob`
//! (usually via `CRDJobHandler`) whose `CommandId` is `u64`. The master
//! assigns globally unique ids, so the command table needs no job type.

#![allow(dead_code, unused_imports)]

pub mod command_table;
pub mod ids;
pub mod logs;
pub mod queues;
pub mod runtime;
pub mod thread_table;

pub use command_table::{
    crd_is_terminal, CRDCommandRecord, CRDMasterCommandTable, CRDReportOutcome, CRDReportSummary,
};
pub use ids::{CRDCommandKey, CRDJobId, CRDJobKey, CRDThreadId};
pub use logs::{
    crd_field, CRDJsonlSink, CRDLogLevel, CRDLogRecord, CRDLogSink, CRDLogValue, CRDMasterLogs,
    CRDStdoutSink,
};
pub use queues::{
    CRDMasterCommandReportQueue, CRDMasterDisplay, CRDMasterDisplayQueue, CRDMasterReport,
    CRDSendError,
};
pub use runtime::{CRDJobGroup, CRDMaster, CRDSpawnError};
pub use thread_table::{CRDMasterThreadTable, CRDThreadRecord};