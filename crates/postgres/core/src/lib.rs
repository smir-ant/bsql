#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Shared core for bsql PostgreSQL drivers (async + sync).
//!
//! Contains: `Session` (pump state machine), `Handshake` (connect flow),
//! `Row` (Arc-shared arena), `ConnectConfig`, `DriverError`, `ssl` module.
//! Both `bsql-postgres-async` and `bsql-postgres-sync` depend on this.

pub mod config;
pub mod error;
pub mod session;
pub mod ssl;
pub mod types;

pub use config::{ConnectConfig, SslMode};
pub use error::{DbError, DriverError};
pub use session::{Handshake, HandshakeAction, PumpAction, Session};
pub use types::{ArenaBuilder, FromText, Notification, PreparedStatement, QueryResult, Row};
