#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Shared core for bsql PostgreSQL drivers (async + sync).
//!
//! Contains: `Row` (Arc-shared arena), `ResultCollector` (materialiser over the
//! sans-IO engine surface), `ConnectConfig`, `DriverError`, `tls`/`ssl` modules.
//! Both `bsql-postgres-async` and `bsql-postgres-sync` depend on this.

pub mod test_scenarios;

pub mod config;
pub mod error;
pub mod footprint;
pub mod materialize;
pub mod sql_ident;
pub mod ssl;
// The in-memory fake PostgreSQL backend behind the engine's transport seam.
// Feature-gated OFF by default so the real transport path — and the shipped
// runtime closure — is untouched unless a consumer opts into the testkit.
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod tls;
// The bounded, typed result of a compile-checked `query!` — `Rows<Q>` plus the
// `RowsBuilder` prebuffer collector both drivers feed.
pub mod typed_rows;
pub mod types;

pub use config::{ConnectConfig, SslMode};
pub use error::{ColumnError, DbError, DriverError};
pub use materialize::{CollectedResult, DbErrorSink, ResultCollector};
pub use typed_rows::{Rows, RowsBuilder};
pub use types::{ArenaBuilder, ArenaSealError, Notification, QueryResult, Row};
