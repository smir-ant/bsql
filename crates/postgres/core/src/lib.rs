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
pub mod owned_row;
pub mod sql_ident;
pub mod ssl;
pub mod tls;
// The bounded, typed result of a compile-checked `query!` — `Rows<Q>` plus the
// `RowsBuilder` prebuffer collector both drivers feed.
pub mod typed_rows;
pub mod types;

pub use config::{ConnectConfig, SslMode};
pub use error::{DbError, DriverError};
pub use materialize::{CollectedResult, DbErrorSink, ResultCollector};
pub use owned_row::{OwnedRow, OwnedRowTooLarge};
pub use typed_rows::{Rows, RowsBuilder};
pub use types::{
    ArenaBuilder, ArenaSealError, FromText, Notification, PreparedStatement, QueryResult, Row,
};
