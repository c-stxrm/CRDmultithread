// The contract is shared API: re-exports and variants are not all used by
// every binary built on it.
#![allow(dead_code, unused_imports)]

pub mod crd_job;
pub use crd_job::*;