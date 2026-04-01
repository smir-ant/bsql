#![forbid(unsafe_code)]

//! Runtime support for bsql.
//!
//! This crate provides the types that `bsql::query!` generated code depends on:
//! error types, connection pool, and the executor trait.
//!
//! You should not depend on this crate directly — use [`bsql`] instead.

/// Re-export `postgres_types` so generated code (pg_enum) can reference it
/// via `::bsql_core::pg_types::*` without requiring users to add
/// `postgres-types` as a direct dependency.
pub use postgres_types as pg_types;

pub mod error;
pub mod executor;
pub mod listener;
pub mod pool;
pub(crate) mod singleflight;
pub mod stream;
pub mod transaction;
pub mod types;

/// Re-exports from `tokio-postgres` and `postgres-types` used by generated code.
/// This avoids requiring users to add `tokio-postgres` to their dependencies.
pub mod pg {
    pub use postgres_types::ToSql;
    pub use tokio_postgres::Row;
}

/// Re-export `futures_core::Stream` so consumers can use `QueryStream`
/// without adding `futures-core` as a direct dependency.
pub use futures_core::Stream;

pub use error::{BsqlError, BsqlResult};
pub use executor::Executor;
pub use listener::{Listener, Notification};
pub use pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
pub use stream::QueryStream;
pub use transaction::Transaction;
