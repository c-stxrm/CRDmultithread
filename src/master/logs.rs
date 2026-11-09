//! CRDMasterLogs: leveled, structured, multi-sink logging.
//!
//! - `CRDMasterLogs` is a cheap-to-clone handle. Clone it into every thread.
//! - A `CRDLogSink` is a destination. Add as many as you like with
//!   `add_sink`; the logger never needs to change (open/closed).
//! - Built-in sinks: `CRDStdoutSink` (colored) and `CRDJsonlSink`
//!   (one JSON object per line).
//!
//! Filtering is two-stage. A lock-free check against the lowest level any
//! sink wants (`enabled`) rejects records without building them. Each sink
//! then applies its own minimum level.
//!
//! Logging never panics and never returns errors. A failing sink drops the
//! record.
//!
//! Expensive messages should be guarded:
//!
//! ```ignore
//! if logs.enabled(CRDLogLevel::Debug) {
//!     logs.debug_with("my.source", "state", &[crd_field("dump", format!("{:?}", state))]);
//! }
//! ```

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, IsTerminal, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::ids::{crd_current_thread_id, crd_lock, CRDThreadId};

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
#[derive(Debug, Clone, PartialEq)]
pub enum CRDLogValue {
    Str(String),
    U64(u64),
    I64(i64),
    Bool(bool),
}

impl From<&str> for CRDLogValue {
    fn from(value: &str) -> Self {
        CRDLogValue::Str(value.to_owned())
    }
}
impl From<String> for CRDLogValue {
    fn from(value: String) -> Self {
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
pub fn crd_field(key: &'static str, value: impl Into<CRDLogValue>) -> (&'static str, CRDLogValue) {
    (key, value.into())
}

/// One log record, borrowed for the duration of a `CRDLogSink::write` call.
#[derive(Debug)]
pub struct CRDLogRecord<'a> {
    /// Milliseconds since the Unix epoch (UTC).
    pub ts_ms: u64,
    pub level: CRDLogLevel,
    /// Dotted component name, e.g. `crd.queue`.
    pub source: &'a str,
    /// Thread that emitted the record.
    pub thread_id: CRDThreadId,
    pub message: &'a str,
    pub fields: &'a [(&'static str, CRDLogValue)],
}

// ============================================================================
// 2. SINK CONTRACT
// ============================================================================

/// A log destination.
///
/// The logger serialises calls, so a sink never sees two `write` calls at
/// once and needs no locking of its own.
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

struct CRDUtcTime {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millis: u32,
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
pub struct CRDStdoutSink {
    min_level: CRDLogLevel,
    color: bool,
}

impl CRDStdoutSink {
    /// Creates a sink with automatic color detection.
    pub fn new(min_level: CRDLogLevel) -> Self {
        let color = io::stdout().is_terminal()
            && std::env::var_os("NO_COLOR").is_none()
            && crd_enable_ansi_stdout();

        Self { min_level, color }
    }

    /// Creates a sink with color forced on or off.
    pub fn with_color(min_level: CRDLogLevel, color: bool) -> Self {
        if color {
            crd_enable_ansi_stdout();
        }
        Self { min_level, color }
    }
}

impl CRDLogSink for CRDStdoutSink {
    fn min_level(&self) -> CRDLogLevel {
        self.min_level
    }

    fn write(&mut self, record: &CRDLogRecord<'_>) {
        let time = CRDUtcTime::from_unix_ms(record.ts_ms);

        let (reset, dim, source_color) = if self.color {
            (ANSI_RESET, ANSI_DIM, ANSI_MAGENTA)
        } else {
            ("", "", "")
        };
        let level_color = if self.color { crd_level_color(record.level) } else { "" };

        let mut line = String::with_capacity(192);
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
            value.write_plain(&mut line);
        }

        let stdout = io::stdout();
        let mut lock = stdout.lock();
        let _ = writeln!(lock, "{}", line);
    }

    fn flush(&mut self) {
        let _ = io::stdout().flush();
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

/// JSON Lines sink: one self-contained JSON object per record.
///
/// ```text
/// {"ts":"2026-11-09T12:00:00.123Z","ts_ms":1788955200123,"level":"INFO","source":"crd.report","tid":4242,"msg":"command finished","fields":{"command":1}}
/// ```
///
/// The file is opened in append mode, and missing parent directories are
/// created. Every record is flushed, so the file survives a crash.
pub struct CRDJsonlSink {
    min_level: CRDLogLevel,
    writer: BufWriter<File>,
}

impl CRDJsonlSink {
    /// Opens (or creates) the file at `path`.
    pub fn open(path: impl AsRef<Path>, min_level: CRDLogLevel) -> io::Result<Self> {
        let path = path.as_ref();

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            min_level,
            writer: BufWriter::new(file),
        })
    }
}

impl CRDLogSink for CRDJsonlSink {
    fn min_level(&self) -> CRDLogLevel {
        self.min_level
    }

    fn write(&mut self, record: &CRDLogRecord<'_>) {
        let time = CRDUtcTime::from_unix_ms(record.ts_ms);

        let mut line = String::with_capacity(256);
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
        crd_json_escape(record.source, &mut line);

        let _ = write!(line, ",\"tid\":{},\"msg\":", record.thread_id);
        crd_json_escape(record.message, &mut line);

        if !record.fields.is_empty() {
            line.push_str(",\"fields\":{");
            for (index, (key, value)) in record.fields.iter().enumerate() {
                if index > 0 {
                    line.push(',');
                }
                crd_json_escape(key, &mut line);
                line.push(':');
                value.write_json(&mut line);
            }
            line.push('}');
        }

        line.push('}');

        let _ = writeln!(self.writer, "{}", line);
        let _ = self.writer.flush();
    }

    fn flush(&mut self) {
        let _ = self.writer.flush();
    }
}

// ============================================================================
// 6. THE LOGGER
// ============================================================================

struct CRDLogsInner {
    /// Lowest level any sink accepts. `u8::MAX` while there are no sinks.
    min_level: AtomicU8,
    sinks: Mutex<Vec<Box<dyn CRDLogSink>>>,
}

/// Shared logging handle. Clone it freely; all clones feed the same sinks.
#[derive(Clone)]
pub struct CRDMasterLogs {
    inner: Arc<CRDLogsInner>,
}

/// Generates `level()` and `level_with()` convenience methods.
macro_rules! crd_level_methods {
    ($(($plain:ident, $with:ident, $level:ident)),* $(,)?) => {
        $(
            pub fn $plain(&self, source: &str, message: &str) {
                self.log(CRDLogLevel::$level, source, message, &[]);
            }

            pub fn $with(
                &self,
                source: &str,
                message: &str,
                fields: &[(&'static str, CRDLogValue)],
            ) {
                self.log(CRDLogLevel::$level, source, message, fields);
            }
        )*
    };
}

impl CRDMasterLogs {
    /// Creates a logger with no sinks. Until one is added, nothing is logged.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CRDLogsInner {
                min_level: AtomicU8::new(u8::MAX),
                sinks: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Adds a sink. Safe to call at any time, from any clone.
    pub fn add_sink(&self, sink: impl CRDLogSink + 'static) {
        let level = sink.min_level() as u8;
        crd_lock(&self.inner.sinks).push(Box::new(sink));
        self.inner.min_level.fetch_min(level, Ordering::Relaxed);
    }

    /// True if at least one sink would accept a record at `level`.
    /// Lock-free; use it to skip building expensive messages.
    #[inline]
    pub fn enabled(&self, level: CRDLogLevel) -> bool {
        level as u8 >= self.inner.min_level.load(Ordering::Relaxed)
    }

    /// Emits a record to every sink whose minimum level admits it.
    pub fn log(
        &self,
        level: CRDLogLevel,
        source: &str,
        message: &str,
        fields: &[(&'static str, CRDLogValue)],
    ) {
        if !self.enabled(level) {
            return;
        }

        let record = CRDLogRecord {
            ts_ms: crd_unix_ms(),
            level,
            source,
            thread_id: crd_current_thread_id(),
            message,
            fields,
        };

        let mut sinks = crd_lock(&self.inner.sinks);
        for sink in sinks.iter_mut() {
            if level >= sink.min_level() {
                sink.write(&record);
            }
        }
    }

    /// Flushes every sink.
    pub fn flush(&self) {
        for sink in crd_lock(&self.inner.sinks).iter_mut() {
            sink.flush();
        }
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