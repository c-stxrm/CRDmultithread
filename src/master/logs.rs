//! CRDMasterLogs: leveled, structured, multi-sink logging on a dedicated
//! logger thread.
//!
//! - `CRDMasterLogs` is a cheap-to-clone handle. Clone it into every thread.
//! - A `CRDLogSink` is a destination. Add as many as you like with
//!   `add_sink`; the logger never needs to change (open/closed).
//! - Built-in sinks: `CRDStdoutSink` (colored) and `CRDJsonlSink`
//!   (one JSON object per line, optional size-based rotation).
//!
//! # Threading model
//!
//! A call site only builds a small owned record and pushes it into a bounded
//! queue. One logger thread owns every sink and does all formatting and I/O,
//! so a slow sink (for example the Windows console) never blocks the threads
//! that log.
//!
//! - Queue full: `Trace` and `Debug` records are dropped and counted. `Info`
//!   and above block until there is room. When the backlog clears, the logger
//!   writes one `N log records dropped` warning, so the loss is visible.
//! - Flushing is batched: the logger flushes when its queue runs empty, at
//!   least every 200 ms under continuous load, immediately after a `Warn` or
//!   `Error` record, and on shutdown.
//! - `Error` records also wait (up to 2 s) until they have been flushed, so a
//!   crash right after cannot lose the line that explains it.
//! - `flush()` and `shutdown()` drain the queue. `shutdown()` also joins the
//!   logger thread. Dropping the last handle shuts down too.
//! - `install_panic_hook()` logs panics and drains the queue before the
//!   process unwinds or aborts.
//!
//! Filtering is two-stage. A lock-free check against the lowest level any
//! sink wants (`enabled`) rejects records without building them. Each sink
//! then applies its own minimum level on the logger thread.
//!
//! Logging never panics and never returns errors. A failing sink drops the
//! record.
//!
//! Payload dumps are expensive, so they belong at `Trace` and behind a guard:
//!
//! ```ignore
//! if logs.enabled(CRDLogLevel::Trace) {
//!     logs.trace_with("my.source", "state", &[crd_field("dump", format!("{:?}", state))]);
//! }
//! ```

use std::borrow::Cow;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, IsTerminal, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::ids::{crd_current_thread_id, crd_lock, CRDThreadId};

/// Records the logger thread may have queued at once, by default.
const CRD_DEFAULT_QUEUE_CAPACITY: usize = 8192;

/// Longest the logger lets written data sit unflushed under continuous load.
const CRD_FLUSH_INTERVAL: Duration = Duration::from_millis(200);

/// How long an `Error` record waits to be flushed to the sinks.
const CRD_ERROR_FLUSH_WAIT: Duration = Duration::from_secs(2);

/// Default wait of `flush()`.
const CRD_FLUSH_WAIT: Duration = Duration::from_secs(5);

// ============================================================================
// 1. LEVELS, VALUES, RECORDS
// ============================================================================

/// Severity of a log record. Ordered: `Trace < Debug < Info < Warn < Error`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CRDLogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl CRDLogLevel {
    /// Upper-case name, as written to sinks.
    pub fn as_str(self) -> &'static str {
        match self {
            CRDLogLevel::Trace => "TRACE",
            CRDLogLevel::Debug => "DEBUG",
            CRDLogLevel::Info => "INFO",
            CRDLogLevel::Warn => "WARN",
            CRDLogLevel::Error => "ERROR",
        }
    }

    /// Parses a level name, case-insensitive. Returns `None` if unknown.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "trace" => Some(CRDLogLevel::Trace),
            "debug" => Some(CRDLogLevel::Debug),
            "info" => Some(CRDLogLevel::Info),
            "warn" | "warning" => Some(CRDLogLevel::Warn),
            "error" => Some(CRDLogLevel::Error),
            _ => None,
        }
    }
}

/// A structured field value attached to a record.
///
/// Strings are `Cow<'static, str>`: names and statuses that are `&'static str`
/// (see `crd_static`) are carried without allocating.
#[derive(Debug, Clone, PartialEq)]
pub enum CRDLogValue {
    Str(Cow<'static, str>),
    U64(u64),
    I64(i64),
    Bool(bool),
}

impl From<&str> for CRDLogValue {
    fn from(value: &str) -> Self {
        CRDLogValue::Str(Cow::Owned(value.to_owned()))
    }
}
impl From<String> for CRDLogValue {
    fn from(value: String) -> Self {
        CRDLogValue::Str(Cow::Owned(value))
    }
}
impl From<Cow<'static, str>> for CRDLogValue {
    fn from(value: Cow<'static, str>) -> Self {
        CRDLogValue::Str(value)
    }
}
impl From<u64> for CRDLogValue {
    fn from(value: u64) -> Self {
        CRDLogValue::U64(value)
    }
}
impl From<u32> for CRDLogValue {
    fn from(value: u32) -> Self {
        CRDLogValue::U64(u64::from(value))
    }
}
impl From<usize> for CRDLogValue {
    fn from(value: usize) -> Self {
        CRDLogValue::U64(value as u64)
    }
}
impl From<i64> for CRDLogValue {
    fn from(value: i64) -> Self {
        CRDLogValue::I64(value)
    }
}
impl From<bool> for CRDLogValue {
    fn from(value: bool) -> Self {
        CRDLogValue::Bool(value)
    }
}

/// Builds one `(key, value)` field: `crd_field("command", 7u64)`.
///
/// A `&str` argument is copied. For a `&'static str` use `crd_static`.
pub fn crd_field(key: &'static str, value: impl Into<CRDLogValue>) -> (&'static str, CRDLogValue) {
    (key, value.into())
}

/// Builds a string field from a `&'static str` without allocating:
/// `crd_static("job", self.name)`.
pub fn crd_static(key: &'static str, value: &'static str) -> (&'static str, CRDLogValue) {
    (key, CRDLogValue::Str(Cow::Borrowed(value)))
}

/// A fixed-capacity, stack-allocated field list, for call sites where the
/// number of fields varies. Fields beyond `N` are ignored.
pub struct CRDFields<const N: usize> {
    items: [(&'static str, CRDLogValue); N],
    len: usize,
}

impl<const N: usize> CRDFields<N> {
    pub fn new() -> Self {
        Self {
            items: std::array::from_fn(|_| ("", CRDLogValue::Bool(false))),
            len: 0,
        }
    }

    pub fn push(&mut self, field: (&'static str, CRDLogValue)) {
        if self.len < N {
            self.items[self.len] = field;
            self.len += 1;
        }
    }

    pub fn as_slice(&self) -> &[(&'static str, CRDLogValue)] {
        &self.items[..self.len]
    }
}

impl<const N: usize> Default for CRDFields<N> {
    fn default() -> Self {
        Self::new()
    }
}

/// One log record, borrowed for the duration of a `CRDLogSink::write` call.
#[derive(Debug)]
pub struct CRDLogRecord<'a> {
    /// Milliseconds since the Unix epoch (UTC).
    pub ts_ms: u64,
    /// `ts_ms` already split into calendar fields. Computed once per record
    /// (and cached per second), so sinks never redo the date conversion.
    pub utc: CRDUtcTime,
    pub level: CRDLogLevel,
    /// Dotted component name, e.g. `crd.queue`.
    pub source: &'a str,
    /// Thread that emitted the record.
    pub thread_id: CRDThreadId,
    pub message: &'a str,
    pub fields: &'a [(&'static str, CRDLogValue)],
}

/// The record as it travels through the queue.
struct CRDOwnedRecord {
    ts_ms: u64,
    level: CRDLogLevel,
    source: &'static str,
    thread_id: CRDThreadId,
    message: String,
    fields: Box<[(&'static str, CRDLogValue)]>,
}

// ============================================================================
// 2. SINK CONTRACT
// ============================================================================

/// A log destination.
///
/// Every sink is owned by the logger thread, which calls it one record at a
/// time, so a sink needs no locking of its own. Sinks should buffer in
/// `write` and push data out in `flush`; the logger batches flushes.
pub trait CRDLogSink: Send {
    /// Records below this level are not delivered to this sink.
    fn min_level(&self) -> CRDLogLevel;

    /// Writes one record. Must not panic; drop the record on failure.
    fn write(&mut self, record: &CRDLogRecord<'_>);

    /// Flushes buffered output. Default: nothing to flush.
    fn flush(&mut self) {}
}

// ============================================================================
// 3. TIME HELPERS
// ============================================================================

fn crd_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// A UTC calendar time with millisecond precision.
#[derive(Debug, Clone, Copy)]
pub struct CRDUtcTime {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub millis: u32,
}

impl CRDUtcTime {
    fn from_unix_ms(ms: u64) -> Self {
        let secs = ms / 1000;
        let days = (secs / 86_400) as i64;
        let second_of_day = (secs % 86_400) as u32;
        let (year, month, day) = crd_civil_from_days(days);

        Self {
            year,
            month,
            day,
            hour: second_of_day / 3600,
            minute: (second_of_day % 3600) / 60,
            second: second_of_day % 60,
            millis: (ms % 1000) as u32,
        }
    }
}

/// Caches the calendar conversion per second. Only the logger thread uses it.
struct CRDTimeCache {
    second: u64,
    base: CRDUtcTime,
}

impl CRDTimeCache {
    fn new() -> Self {
        Self {
            second: u64::MAX,
            base: CRDUtcTime::from_unix_ms(0),
        }
    }

    fn get(&mut self, ms: u64) -> CRDUtcTime {
        let second = ms / 1000;

        if second != self.second {
            self.base = CRDUtcTime::from_unix_ms(second * 1000);
            self.second = second;
        }

        CRDUtcTime {
            millis: (ms % 1000) as u32,
            ..self.base
        }
    }
}

/// Days since 1970-01-01 to a proleptic Gregorian (year, month, day).
fn crd_civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;

    (if month <= 2 { year + 1 } else { year }, month, day)
}

// ============================================================================
// 4. STDOUT SINK (COLORED)
// ============================================================================

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_MAGENTA: &str = "\x1b[35m";

fn crd_level_color(level: CRDLogLevel) -> &'static str {
    match level {
        CRDLogLevel::Trace => "\x1b[90m",
        CRDLogLevel::Debug => "\x1b[36m",
        CRDLogLevel::Info => "\x1b[32m",
        CRDLogLevel::Warn => "\x1b[33m",
        CRDLogLevel::Error => "\x1b[1;31m",
    }
}

/// Turns on ANSI escape processing for the Windows console.
#[cfg(windows)]
fn crd_enable_ansi_stdout() -> bool {
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, SetConsoleMode, ENABLE_VIRTUAL_TERMINAL_PROCESSING,
        STD_OUTPUT_HANDLE,
    };

    // SAFETY: plain Win32 calls; the handle is only handed back to Win32.
    unsafe {
        let handle = GetStdHandle(STD_OUTPUT_HANDLE);
        let mut mode = 0;

        if GetConsoleMode(handle, &mut mode) == 0 {
            return false;
        }

        SetConsoleMode(handle, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING) != 0
    }
}

#[cfg(not(windows))]
fn crd_enable_ansi_stdout() -> bool {
    true
}

impl CRDLogValue {
    fn write_plain(&self, out: &mut String) {
        match self {
            CRDLogValue::Str(text) => {
                let text: &str = text;
                let needs_quotes = text.is_empty()
                    || text.chars().any(|c| c.is_whitespace() || c == '"' || c == '=');
                if needs_quotes {
                    let _ = write!(out, "{:?}", text);
                } else {
                    out.push_str(text);
                }
            }
            CRDLogValue::U64(value) => {
                let _ = write!(out, "{}", value);
            }
            CRDLogValue::I64(value) => {
                let _ = write!(out, "{}", value);
            }
            CRDLogValue::Bool(value) => {
                let _ = write!(out, "{}", value);
            }
        }
    }

    fn write_json(&self, out: &mut String) {
        match self {
            CRDLogValue::Str(text) => crd_json_escape(text, out),
            CRDLogValue::U64(value) => {
                let _ = write!(out, "{}", value);
            }
            CRDLogValue::I64(value) => {
                let _ = write!(out, "{}", value);
            }
            CRDLogValue::Bool(value) => {
                let _ = write!(out, "{}", value);
            }
        }
    }
}

/// Colored, human-readable stdout sink.
///
/// Color is enabled only when stdout is a terminal, `NO_COLOR` is unset and
/// (on Windows) ANSI processing could be switched on.
///
/// Output is buffered and pushed out on `flush`, which the logger calls in
/// batches (and at once after a warning or error).
pub struct CRDStdoutSink {
    min_level: CRDLogLevel,
    color: bool,
    out: BufWriter<io::Stdout>,
    /// Reused for every record.
    buf: String,
}

impl CRDStdoutSink {
    /// Creates a sink with automatic color detection.
    pub fn new(min_level: CRDLogLevel) -> Self {
        let color = io::stdout().is_terminal()
            && std::env::var_os("NO_COLOR").is_none()
            && crd_enable_ansi_stdout();

        Self::build(min_level, color)
    }

    /// Creates a sink with color forced on or off.
    pub fn with_color(min_level: CRDLogLevel, color: bool) -> Self {
        if color {
            crd_enable_ansi_stdout();
        }
        Self::build(min_level, color)
    }

    fn build(min_level: CRDLogLevel, color: bool) -> Self {
        Self {
            min_level,
            color,
            out: BufWriter::new(io::stdout()),
            buf: String::with_capacity(256),
        }
    }
}

impl CRDLogSink for CRDStdoutSink {
    fn min_level(&self) -> CRDLogLevel {
        self.min_level
    }

    fn write(&mut self, record: &CRDLogRecord<'_>) {
        let time = record.utc;

        let (reset, dim, source_color) = if self.color {
            (ANSI_RESET, ANSI_DIM, ANSI_MAGENTA)
        } else {
            ("", "", "")
        };
        let level_color = if self.color { crd_level_color(record.level) } else { "" };

        let line = &mut self.buf;
        line.clear();
        let _ = write!(
            line,
            "{dim}{:02}:{:02}:{:02}.{:03}{reset} {level_color}{:<5}{reset} {dim}tid={:<6}{reset} {source_color}{}{reset} {}",
            time.hour,
            time.minute,
            time.second,
            time.millis,
            record.level.as_str(),
            record.thread_id,
            record.source,
            record.message,
        );

        for (key, value) in record.fields {
            let _ = write!(line, " {dim}{}={reset}", key);
            value.write_plain(line);
        }

        line.push('\n');
        let _ = self.out.write_all(line.as_bytes());
    }

    fn flush(&mut self) {
        let _ = self.out.flush();
    }
}

// ============================================================================
// 5. JSONL SINK
// ============================================================================

/// Writes `value` as a JSON string literal (with quotes) into `out`.
fn crd_json_escape(value: &str, out: &mut String) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Size-based rotation settings of a `CRDJsonlSink`.
#[derive(Debug, Clone, Copy)]
struct CRDRotation {
    max_bytes: u64,
    max_files: u32,
}

/// JSON Lines sink: one self-contained JSON object per record.
///
/// ```text
/// {"ts":"2026-11-09T12:00:00.123Z","ts_ms":1788955200123,"level":"INFO","source":"crd.report","tid":4242,"msg":"command finished","fields":{"command":1}}
/// ```
///
/// The file is opened in append mode, and missing parent directories are
/// created. Records go through a `BufWriter` and reach the file when the
/// logger flushes (queue empty, every 200 ms at most, after a warning or
/// error, and on shutdown).
///
/// With `open_with_rotation`, the sink starts a new file once the current one
/// would exceed `max_bytes`: `app.jsonl` becomes `app.jsonl.1`, `.1` becomes
/// `.2`, and so on; the file past `max_files` is deleted. Rotation runs on the
/// logger thread.
pub struct CRDJsonlSink {
    min_level: CRDLogLevel,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    /// Reused for every record.
    buf: String,
    /// Bytes in the current file, including buffered ones.
    written: u64,
    rotation: Option<CRDRotation>,
}

impl CRDJsonlSink {
    /// Opens (or creates) the file at `path`. The file grows without limit.
    pub fn open(path: impl AsRef<Path>, min_level: CRDLogLevel) -> io::Result<Self> {
        Self::open_inner(path.as_ref(), min_level, None)
    }

    /// Like `open`, but rotates once the file would exceed `max_bytes`,
    /// keeping at most `max_files` older files (at least 1).
    pub fn open_with_rotation(
        path: impl AsRef<Path>,
        min_level: CRDLogLevel,
        max_bytes: u64,
        max_files: u32,
    ) -> io::Result<Self> {
        let rotation = CRDRotation {
            max_bytes: max_bytes.max(1),
            max_files: max_files.max(1),
        };
        Self::open_inner(path.as_ref(), min_level, Some(rotation))
    }

    fn open_inner(
        path: &Path,
        min_level: CRDLogLevel,
        rotation: Option<CRDRotation>,
    ) -> io::Result<Self> {
        let (writer, written) = Self::open_file(path)?;

        Ok(Self {
            min_level,
            path: path.to_path_buf(),
            writer: Some(writer),
            buf: String::with_capacity(512),
            written,
            rotation,
        })
    }

    fn open_file(path: &Path) -> io::Result<(BufWriter<File>, u64)> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let len = file.metadata().map(|meta| meta.len()).unwrap_or(0);

        Ok((BufWriter::with_capacity(64 * 1024, file), len))
    }

    fn numbered(&self, index: u32) -> PathBuf {
        let mut name: OsString = self.path.as_os_str().to_owned();
        name.push(format!(".{}", index));
        PathBuf::from(name)
    }

    /// Closes the current file and shifts the numbered backups up by one.
    fn rotate(&mut self, max_files: u32) {
        // Close the file first: Windows cannot rename an open file.
        if let Some(mut writer) = self.writer.take() {
            let _ = writer.flush();
        }

        let _ = fs::remove_file(self.numbered(max_files));
        for index in (1..max_files).rev() {
            let _ = fs::rename(self.numbered(index), self.numbered(index + 1));
        }
        let _ = fs::rename(&self.path, self.numbered(1));

        self.reopen();
    }

    fn reopen(&mut self) {
        match Self::open_file(&self.path) {
            Ok((writer, len)) => {
                self.writer = Some(writer);
                self.written = len;
            }
            Err(_) => {
                self.writer = None;
                self.written = 0;
            }
        }
    }
}

impl CRDLogSink for CRDJsonlSink {
    fn min_level(&self) -> CRDLogLevel {
        self.min_level
    }

    fn write(&mut self, record: &CRDLogRecord<'_>) {
        let time = record.utc;

        let line = &mut self.buf;
        line.clear();
        let _ = write!(
            line,
            "{{\"ts\":\"{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z\",\"ts_ms\":{},\"level\":\"{}\",\"source\":",
            time.year,
            time.month,
            time.day,
            time.hour,
            time.minute,
            time.second,
            time.millis,
            record.ts_ms,
            record.level.as_str(),
        );
        crd_json_escape(record.source, line);

        let _ = write!(line, ",\"tid\":{},\"msg\":", record.thread_id);
        crd_json_escape(record.message, line);

        if !record.fields.is_empty() {
            line.push_str(",\"fields\":{");
            for (index, (key, value)) in record.fields.iter().enumerate() {
                if index > 0 {
                    line.push(',');
                }
                crd_json_escape(key, line);
                line.push(':');
                value.write_json(line);
            }
            line.push('}');
        }

        line.push_str("}\n");
        let len = line.len() as u64;

        if let Some(rotation) = self.rotation {
            if self.written > 0 && self.written + len > rotation.max_bytes {
                self.rotate(rotation.max_files);
            }
        }

        if self.writer.is_none() {
            self.reopen();
        }

        if let Some(writer) = self.writer.as_mut() {
            if writer.write_all(self.buf.as_bytes()).is_ok() {
                self.written += len;
            }
        }
    }

    fn flush(&mut self) {
        if let Some(writer) = self.writer.as_mut() {
            let _ = writer.flush();
        }
    }
}

// ============================================================================
// 6. THE LOGGER
// ============================================================================

/// What travels through the queue.
enum CRDLogMsg {
    Record(CRDOwnedRecord),
    AddSink(Box<dyn CRDLogSink>),
    /// Flush every sink, then answer on the channel.
    Flush(SyncSender<()>),
    /// Process everything queued before this, flush, and exit.
    Shutdown,
}

/// State shared between the handles and the logger thread.
struct CRDLogShared {
    /// Records dropped because the queue was full, not yet reported.
    dropped: AtomicU64,
}

struct CRDLogsInner {
    /// Lowest level any sink accepts. `u8::MAX` while there are no sinks, and
    /// again after shutdown.
    min_level: AtomicU8,
    closed: AtomicBool,
    /// `None` only if the logger thread could not be started.
    tx: Option<SyncSender<CRDLogMsg>>,
    shared: Arc<CRDLogShared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl CRDLogsInner {
    fn shutdown(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.min_level.store(u8::MAX, Ordering::Relaxed);

        if let Some(tx) = &self.tx {
            let _ = tx.send(CRDLogMsg::Shutdown);
        }

        let handle = crd_lock(&self.worker).take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

impl Drop for CRDLogsInner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Shared logging handle. Clone it freely; all clones feed the same logger
/// thread and sinks.
#[derive(Clone)]
pub struct CRDMasterLogs {
    inner: Arc<CRDLogsInner>,
}

/// Generates `level()` and `level_with()` convenience methods.
macro_rules! crd_level_methods {
    ($(($plain:ident, $with:ident, $level:ident)),* $(,)?) => {
        $(
            pub fn $plain(&self, source: &'static str, message: &str) {
                self.log(CRDLogLevel::$level, source, message, &[]);
            }

            pub fn $with(
                &self,
                source: &'static str,
                message: &str,
                fields: &[(&'static str, CRDLogValue)],
            ) {
                self.log(CRDLogLevel::$level, source, message, fields);
            }
        )*
    };
}

impl CRDMasterLogs {
    /// Creates a logger with no sinks and the default queue capacity. Until a
    /// sink is added, nothing is logged.
    pub fn new() -> Self {
        Self::with_capacity(CRD_DEFAULT_QUEUE_CAPACITY)
    }

    /// Creates a logger whose queue holds at most `capacity` records.
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx) = sync_channel(capacity.max(1));
        let shared = Arc::new(CRDLogShared {
            dropped: AtomicU64::new(0),
        });
        let worker_shared = Arc::clone(&shared);

        let spawned = thread::Builder::new()
            .name("crd-logger".to_string())
            .spawn(move || crd_logger_main(rx, worker_shared));

        let (tx, worker) = match spawned {
            Ok(handle) => (Some(tx), Some(handle)),
            Err(error) => {
                eprintln!("crd: cannot start the logger thread, logging is off: {}", error);
                (None, None)
            }
        };

        Self {
            inner: Arc::new(CRDLogsInner {
                min_level: AtomicU8::new(u8::MAX),
                closed: AtomicBool::new(false),
                tx,
                shared,
                worker: Mutex::new(worker),
            }),
        }
    }

    /// Adds a sink. Safe to call at any time, from any clone. The sink moves
    /// to the logger thread and sees every record logged after this call.
    pub fn add_sink(&self, sink: impl CRDLogSink + 'static) {
        let Some(tx) = &self.inner.tx else {
            return;
        };

        if self.inner.closed.load(Ordering::Relaxed) {
            return;
        }

        let level = sink.min_level() as u8;

        if tx.send(CRDLogMsg::AddSink(Box::new(sink))).is_ok() {
            self.inner.min_level.fetch_min(level, Ordering::Relaxed);
        }
    }

    /// True if at least one sink would accept a record at `level`.
    /// Lock-free; use it to skip building expensive messages.
    #[inline]
    pub fn enabled(&self, level: CRDLogLevel) -> bool {
        level as u8 >= self.inner.min_level.load(Ordering::Relaxed)
    }

    /// Queues a record for the sinks whose minimum level admits it.
    ///
    /// The call copies `message` and `fields` into the queue and returns. If
    /// the queue is full, `Trace` and `Debug` records are dropped (and
    /// counted); `Info` and above wait for room. An `Error` record also waits
    /// until it has been flushed.
    pub fn log(
        &self,
        level: CRDLogLevel,
        source: &'static str,
        message: &str,
        fields: &[(&'static str, CRDLogValue)],
    ) {
        if !self.enabled(level) {
            return;
        }

        let Some(tx) = &self.inner.tx else {
            return;
        };

        let record = CRDOwnedRecord {
            ts_ms: crd_unix_ms(),
            level,
            source,
            thread_id: crd_current_thread_id(),
            message: message.to_owned(),
            fields: fields.to_vec().into_boxed_slice(),
        };

        if level <= CRDLogLevel::Debug {
            if let Err(TrySendError::Full(_)) = tx.try_send(CRDLogMsg::Record(record)) {
                self.inner.shared.dropped.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let _ = tx.send(CRDLogMsg::Record(record));
        }

        if level == CRDLogLevel::Error {
            self.flush_within(CRD_ERROR_FLUSH_WAIT);
        }
    }

    /// Writes everything queued so far to the sinks and flushes them. Returns
    /// when done (or after 5 s, if a sink is stuck).
    pub fn flush(&self) {
        self.flush_within(CRD_FLUSH_WAIT);
    }

    fn flush_within(&self, wait: Duration) {
        let Some(tx) = &self.inner.tx else {
            return;
        };

        let (ack_tx, ack_rx) = sync_channel(1);

        if tx.send(CRDLogMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv_timeout(wait);
        }
    }

    /// Drains the queue, flushes the sinks and joins the logger thread. Logs
    /// written afterwards are discarded. Safe to call more than once; also
    /// happens when the last handle is dropped.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    /// Logs panics (message and location) at `Error` and drains the queue
    /// before the previous panic hook runs, so the panic is on disk even if
    /// the process aborts right after.
    pub fn install_panic_hook(&self) {
        let weak = Arc::downgrade(&self.inner);
        let previous = std::panic::take_hook();

        std::panic::set_hook(Box::new(move |info| {
            if let Some(inner) = weak.upgrade() {
                // `Error` records are flushed before `error` returns.
                CRDMasterLogs { inner }.error("crd.panic", &info.to_string());
            }
            previous(info);
        }));
    }

    // trace/debug/info/warn/error, each with a `_with` variant taking fields.
    crd_level_methods! {
        (trace, trace_with, Trace),
        (debug, debug_with, Debug),
        (info, info_with, Info),
        (warn, warn_with, Warn),
        (error, error_with, Error),
    }
}

impl Default for CRDMasterLogs {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// 7. THE LOGGER THREAD
// ============================================================================

/// Delivers one record to every sink that wants it.
fn crd_deliver(
    sinks: &mut [Box<dyn CRDLogSink>],
    time_cache: &mut CRDTimeCache,
    record: &CRDOwnedRecord,
) {
    let view = CRDLogRecord {
        ts_ms: record.ts_ms,
        utc: time_cache.get(record.ts_ms),
        level: record.level,
        source: record.source,
        thread_id: record.thread_id,
        message: &record.message,
        fields: &record.fields,
    };

    for sink in sinks.iter_mut() {
        if record.level >= sink.min_level() {
            sink.write(&view);
        }
    }
}

fn crd_flush_all(sinks: &mut [Box<dyn CRDLogSink>]) {
    for sink in sinks.iter_mut() {
        sink.flush();
    }
}

/// Body of the logger thread. Returns on `Shutdown`, or when every sender is
/// gone and the queue is empty.
fn crd_logger_main(rx: Receiver<CRDLogMsg>, shared: Arc<CRDLogShared>) {
    let mut sinks: Vec<Box<dyn CRDLogSink>> = Vec::new();
    let mut time_cache = CRDTimeCache::new();
    let mut dirty = false;
    let mut last_flush = Instant::now();

    loop {
        let message = match rx.try_recv() {
            Ok(message) => message,
            Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => {
                // The backlog has cleared: report what was dropped, flush,
                // then sleep until the next message.
                let dropped = shared.dropped.swap(0, Ordering::Relaxed);
                if dropped > 0 {
                    let notice = CRDOwnedRecord {
                        ts_ms: crd_unix_ms(),
                        level: CRDLogLevel::Warn,
                        source: "crd.log",
                        thread_id: crd_current_thread_id(),
                        message: format!("{} log records dropped (queue full)", dropped),
                        fields: Box::new([("dropped", CRDLogValue::U64(dropped))]),
                    };
                    crd_deliver(&mut sinks, &mut time_cache, &notice);
                    dirty = true;
                }

                if dirty {
                    crd_flush_all(&mut sinks);
                    dirty = false;
                    last_flush = Instant::now();
                }

                match rx.recv() {
                    Ok(message) => message,
                    Err(_) => break,
                }
            }
        };

        match message {
            CRDLogMsg::Record(record) => {
                crd_deliver(&mut sinks, &mut time_cache, &record);
                dirty = true;

                // Warnings and errors go out at once; under continuous load
                // the queue never runs empty, so flush on a timer as well.
                if record.level >= CRDLogLevel::Warn || last_flush.elapsed() >= CRD_FLUSH_INTERVAL {
                    crd_flush_all(&mut sinks);
                    dirty = false;
                    last_flush = Instant::now();
                }
            }
            CRDLogMsg::AddSink(sink) => sinks.push(sink),
            CRDLogMsg::Flush(ack) => {
                crd_flush_all(&mut sinks);
                dirty = false;
                last_flush = Instant::now();
                let _ = ack.send(());
            }
            CRDLogMsg::Shutdown => break,
        }
    }

    crd_flush_all(&mut sinks);
}

// ============================================================================
// 8. TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Collects "level:message" lines; optionally sleeps per write.
    struct CollectSink {
        min: CRDLogLevel,
        lines: Arc<Mutex<Vec<String>>>,
        flushes: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl CRDLogSink for CollectSink {
        fn min_level(&self) -> CRDLogLevel {
            self.min
        }
        fn write(&mut self, record: &CRDLogRecord<'_>) {
            if !self.delay.is_zero() {
                thread::sleep(self.delay);
            }
            self.lines
                .lock()
                .unwrap()
                .push(format!("{}:{}", record.level.as_str(), record.message));
        }
        fn flush(&mut self) {
            self.flushes.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn collector(
        min: CRDLogLevel,
        delay: Duration,
    ) -> (CollectSink, Arc<Mutex<Vec<String>>>, Arc<AtomicUsize>) {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicUsize::new(0));
        let sink = CollectSink {
            min,
            lines: Arc::clone(&lines),
            flushes: Arc::clone(&flushes),
            delay,
        };
        (sink, lines, flushes)
    }

    #[test]
    fn records_arrive_in_order_and_shutdown_drains() {
        let logs = CRDMasterLogs::new();
        let (sink, lines, _) = collector(CRDLogLevel::Info, Duration::ZERO);
        logs.add_sink(sink);

        for i in 0..1000 {
            logs.info("t", &format!("m{}", i));
        }
        logs.debug("t", "filtered out");
        logs.shutdown();

        let lines = lines.lock().unwrap();
        assert_eq!(lines.len(), 1000);
        assert_eq!(lines[0], "INFO:m0");
        assert_eq!(lines[999], "INFO:m999");
    }

    #[test]
    fn disabled_levels_are_rejected_before_any_work() {
        let logs = CRDMasterLogs::new();
        assert!(!logs.enabled(CRDLogLevel::Error)); // no sinks yet
        let (sink, _, _) = collector(CRDLogLevel::Warn, Duration::ZERO);
        logs.add_sink(sink);
        assert!(!logs.enabled(CRDLogLevel::Info));
        assert!(logs.enabled(CRDLogLevel::Warn));
        logs.shutdown();
        assert!(!logs.enabled(CRDLogLevel::Error)); // closed
    }

    #[test]
    fn full_queue_drops_trace_and_reports_the_loss() {
        let logs = CRDMasterLogs::with_capacity(4);
        let (sink, lines, _) = collector(CRDLogLevel::Trace, Duration::from_millis(20));
        logs.add_sink(sink);

        // The slow sink cannot keep up, so most of these must be dropped
        // rather than blocking this thread.
        let started = Instant::now();
        for i in 0..200 {
            logs.trace("t", &format!("t{}", i));
        }
        assert!(started.elapsed() < Duration::from_millis(500), "call site blocked");

        logs.flush();
        logs.shutdown();

        let lines = lines.lock().unwrap();
        assert!(lines.len() < 200);
        assert!(
            lines.iter().any(|l| l.starts_with("WARN:") && l.contains("log records dropped")),
            "no drop notice in {:?}",
            lines
        );
    }

    #[test]
    fn info_is_never_dropped() {
        let logs = CRDMasterLogs::with_capacity(2);
        let (sink, lines, _) = collector(CRDLogLevel::Info, Duration::from_millis(2));
        logs.add_sink(sink);

        for i in 0..50 {
            logs.info("t", &format!("i{}", i));
        }
        logs.shutdown();

        assert_eq!(lines.lock().unwrap().len(), 50);
    }

    #[test]
    fn error_is_flushed_before_the_call_returns() {
        let logs = CRDMasterLogs::new();
        let (sink, lines, flushes) = collector(CRDLogLevel::Info, Duration::ZERO);
        logs.add_sink(sink);

        logs.error("t", "boom");

        assert_eq!(lines.lock().unwrap().as_slice(), ["ERROR:boom"]);
        assert!(flushes.load(Ordering::Relaxed) >= 1);
        logs.shutdown();
    }

    #[test]
    fn flushes_are_batched_not_per_record() {
        let logs = CRDMasterLogs::new();
        let (sink, _, flushes) = collector(CRDLogLevel::Info, Duration::ZERO);
        logs.add_sink(sink);

        for i in 0..5000 {
            logs.info("t", &format!("m{}", i));
        }
        logs.shutdown();

        let flushes = flushes.load(Ordering::Relaxed);
        assert!(flushes < 2500, "{} flushes for 5000 records", flushes);
    }

    #[test]
    fn many_threads_can_log_concurrently() {
        let logs = CRDMasterLogs::new();
        let (sink, lines, _) = collector(CRDLogLevel::Info, Duration::ZERO);
        logs.add_sink(sink);

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let logs = logs.clone();
                thread::spawn(move || {
                    for i in 0..500 {
                        logs.info_with("t", &format!("{}-{}", t, i), &[crd_field("i", i as u64)]);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        logs.shutdown();

        assert_eq!(lines.lock().unwrap().len(), 4000);
    }

    #[test]
    fn dropping_the_last_handle_drains_the_queue() {
        let (sink, lines, _) = collector(CRDLogLevel::Info, Duration::ZERO);
        {
            let logs = CRDMasterLogs::new();
            logs.add_sink(sink);
            for i in 0..100 {
                logs.info("t", &format!("m{}", i));
            }
        }
        assert_eq!(lines.lock().unwrap().len(), 100);
    }

    #[test]
    fn time_cache_matches_direct_conversion() {
        let mut cache = CRDTimeCache::new();
        for ms in [0u64, 999, 1000, 1_788_955_200_123, 1_788_955_200_999, 1_788_955_201_000] {
            let cached = cache.get(ms);
            let direct = CRDUtcTime::from_unix_ms(ms);
            assert_eq!(
                (cached.year, cached.month, cached.day, cached.hour, cached.minute, cached.second, cached.millis),
                (direct.year, direct.month, direct.day, direct.hour, direct.minute, direct.second, direct.millis),
            );
        }
    }

    #[test]
    fn jsonl_sink_rotates_by_size() {
        let dir = std::env::temp_dir().join(format!("crd_log_test_{}", crd_unix_ms()));
        let path = dir.join("app.jsonl");

        let logs = CRDMasterLogs::new();
        let sink = CRDJsonlSink::open_with_rotation(&path, CRDLogLevel::Info, 2_000, 2).unwrap();
        logs.add_sink(sink);

        for i in 0..200 {
            logs.info_with("t", "a line of some length", &[crd_field("i", i as u64)]);
        }
        logs.shutdown();

        assert!(path.exists());
        assert!(dir.join("app.jsonl.1").exists());
        assert!(dir.join("app.jsonl.2").exists());
        assert!(!dir.join("app.jsonl.3").exists(), "keeps at most max_files backups");

        for name in ["app.jsonl", "app.jsonl.1", "app.jsonl.2"] {
            let len = fs::metadata(dir.join(name)).unwrap().len();
            assert!(len <= 2_000, "{} is {} bytes", name, len);
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn jsonl_lines_are_valid_shape() {
        let dir = std::env::temp_dir().join(format!("crd_log_shape_{}", crd_unix_ms()));
        let path = dir.join("shape.jsonl");

        let logs = CRDMasterLogs::new();
        logs.add_sink(CRDJsonlSink::open(&path, CRDLogLevel::Info).unwrap());
        logs.info_with(
            "crd.test",
            "he said \"hi\"",
            &[crd_static("job", "hello"), crd_field("n", 7u64), crd_field("ok", true)],
        );
        logs.shutdown();

        let text = fs::read_to_string(&path).unwrap();
        let line = text.lines().next().unwrap();
        assert!(line.starts_with("{\"ts\":\""));
        assert!(line.contains("\"msg\":\"he said \\\"hi\\\"\""));
        assert!(line.ends_with("\"fields\":{\"job\":\"hello\",\"n\":7,\"ok\":true}}"));
        assert_eq!(text.lines().count(), 1);

        let _ = fs::remove_dir_all(&dir);
    }
}