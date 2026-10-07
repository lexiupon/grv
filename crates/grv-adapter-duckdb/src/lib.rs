//! DuckDB ownership, exact extraction seams and local transactional pull.
//! No core or storage implementation is imported; external S3 builds stay gated.

#[cfg(feature = "native")]
pub mod acquisition;
pub mod binding;
#[cfg(feature = "native")]
pub mod build;
#[cfg(feature = "native")]
pub mod build_runtime;
#[cfg(feature = "native")]
pub mod build_worker;
pub mod completion;
pub mod conversion;
#[cfg(feature = "native")]
pub mod driver;
#[cfg(feature = "native")]
pub mod extraction;
#[cfg(feature = "native")]
pub mod inspection;
#[cfg(feature = "native")]
pub mod ipc;
pub mod journal;
pub mod lock;
#[cfg(feature = "native")]
pub mod native;
#[cfg(feature = "native")]
mod native_extensions;
pub mod process;
pub mod pull;
#[cfg(feature = "native")]
pub mod pull_runtime;
#[cfg(feature = "native")]
pub mod pull_worker;
pub mod s3_config;
pub mod worker;

pub const DUCKDB_VERSION: &str = "1.5.6";
pub const DUCKDB_MEMORY_BYTES: usize = 512 * 1024 * 1024;
pub const DUCKDB_SPILL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub const SOURCE_BUDGET_BYTES: usize = 32 * 1024 * 1024;
pub const SCRATCH_BUDGET_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
