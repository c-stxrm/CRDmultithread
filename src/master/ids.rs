//! Identifiers and small low-level helpers shared by every master module.

use std::hash::{BuildHasher, Hasher};
use std::sync::{Mutex, MutexGuard};

/// Globally unique command id, assigned by the master (monotonic, starts at 1).
pub type CRDCommandKey = u64;

/// OS thread id (the Windows kernel TID on Windows).
pub type CRDThreadId = u64;

/// Identifies a job group: one job type, one or more instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CRDJobId(pub u32);

/// Identifies one running job instance inside a job group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CRDJobKey {
    pub job: CRDJobId,
    pub instance: u32,
}

impl std::fmt::Display for CRDJobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "job{}#{}", self.job.0, self.instance)
    }
}

/// Locks a mutex and ignores poisoning.
///
/// The master tables hold plain bookkeeping data, and one panicking job must
/// never take the whole master down with it.
pub fn crd_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Returns the id of the calling OS thread.
#[cfg(windows)]
pub fn crd_current_thread_id() -> CRDThreadId {
    // SAFETY: GetCurrentThreadId has no preconditions.
    unsafe { windows_sys::Win32::System::Threading::GetCurrentThreadId() as CRDThreadId }
}

/// Returns a stable per-thread id (non-Windows fallback). Hashed once per
/// thread and cached in a thread-local.
#[cfg(not(windows))]
pub fn crd_current_thread_id() -> CRDThreadId {
    use std::hash::Hash;

    fn compute() -> CRDThreadId {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        hasher.finish()
    }

    thread_local! {
        static CACHED: CRDThreadId = compute();
    }

    // `try_with` fails only while the thread-local is being destroyed.
    CACHED.try_with(|id| *id).unwrap_or_else(|_| compute())
}

// ============================================================================
// FAST INTEGER HASHER
// ============================================================================

const CRD_HASH_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

/// A one-multiplication hasher for integer keys.
///
/// The standard hasher (SipHash) defends against hostile keys. Our keys are
/// ids we generated ourselves, so that cost buys nothing. Multiplying by an
/// odd constant maps sequential ids to well-spread hash bits.
#[derive(Debug, Default, Clone, Copy)]
pub struct CRDIdHasher(u64);

impl Hasher for CRDIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Generic fallback; the tables only hash `u64`.
        for &byte in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(byte)).wrapping_mul(CRD_HASH_MULTIPLIER);
        }
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = (self.0 ^ value).wrapping_mul(CRD_HASH_MULTIPLIER);
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.write_u64(u64::from(value));
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }
}

/// `BuildHasher` for `CRDIdHasher`.
#[derive(Debug, Default, Clone, Copy)]
pub struct CRDIdBuildHasher;

impl BuildHasher for CRDIdBuildHasher {
    type Hasher = CRDIdHasher;

    #[inline]
    fn build_hasher(&self) -> CRDIdHasher {
        CRDIdHasher::default()
    }
}