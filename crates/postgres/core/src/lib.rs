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
pub mod ssl;
pub mod tls;
pub mod types;

pub use config::{ConnectConfig, SslMode};
pub use error::{DbError, DriverError};
pub use materialize::{CollectedResult, ResultCollector};
pub use owned_row::{OwnedRow, OwnedRowTooLarge};
pub use types::{ArenaBuilder, FromText, Notification, PreparedStatement, QueryResult, Row, RowTooLarge};
