#![forbid(unsafe_code)]

//! Runtime support for sasql.
//!
//! This crate provides the types that `sasql::query!` generated code depends on:
//! error types, connection pool, and the executor trait.
//!
//! You should not depend on this crate directly — use [`sasql`] instead.

pub mod error;
pub mod executor;
pub mod pool;
pub mod types;

pub use error::{SasqlError, SasqlResult};
pub use executor::Executor;
pub use pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
