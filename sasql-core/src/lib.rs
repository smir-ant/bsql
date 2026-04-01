#![forbid(unsafe_code)]

//! Runtime support for sasql.
//!
//! This crate provides the types that `sasql::query!` generated code depends on:
//! error types, connection pool, and the executor trait.
//!
//! You should not depend on this crate directly — use [`sasql`] instead.

/// Re-export `postgres_types` so generated code (pg_enum) can reference it
/// via `::sasql_core::pg_types::*` without requiring users to add
/// `postgres-types` as a direct dependency.
pub use postgres_types as pg_types;

pub mod error;
pub mod executor;
pub mod pool;
pub mod types;

/// Re-exports from `tokio-postgres` and `postgres-types` used by generated code.
/// This avoids requiring users to add `tokio-postgres` to their dependencies.
pub mod pg {
    pub use postgres_types::ToSql;
    pub use tokio_postgres::Row;
}

pub use error::{SasqlError, SasqlResult};
pub use executor::Executor;
pub use pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
