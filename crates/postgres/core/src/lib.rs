#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Shared core for bsql PostgreSQL drivers (async + sync).
//!
//! Contains: `Session` (pump state machine), `Handshake` (connect flow),
//! `Row` (Arc-shared arena), `ConnectConfig`, `DriverError`, `ssl` module.
//! Both `bsql-postgres-async` and `bsql-postgres-sync` depend on this.

// Self-alias so the `fragment!` proc-macro's emitted absolute path
// (`::bsql_postgres_core::fragment::*`) resolves inside this crate itself
// (e.g. in the unit tests below the module). Mirrors `serde`'s
// `extern crate self as serde;` and proto's `extern crate self as
// bsql_postgres_proto;`.
extern crate self as bsql_postgres_core;

pub mod test_scenarios;

pub mod col;
pub mod config;
pub mod error;
pub mod fragment;
pub mod owned_row;
pub mod session;
pub mod ssl;
pub mod types;

pub use col::{AsIdent, Col, ColType, Text, UnknownColumn};
pub use fragment::{Assembled, BoundValue, ColPredicate, Dir, Fragment, IntoBound, Predicate};

/// `fragment!("SELECT ... WHERE x = {}", value)` — build a [`Fragment`]
/// value from a literal SQL skeleton with typed `{}` value holes.
///
/// Re-exported from the [`bsql-postgres-derive`](bsql_postgres_derive)
/// proc-macro pair-crate. See the [`fragment`] module for the full
/// representation, renumbering, and tier statement.
pub use bsql_postgres_derive::fragment;
pub use config::{ConnectConfig, SslMode};
pub use error::{DbError, DriverError};
pub use owned_row::{OwnedRow, OwnedRowTooLarge};
pub use session::{Handshake, HandshakeAction, PumpAction, Session};
pub use types::{ArenaBuilder, FromText, Notification, PreparedStatement, QueryResult, Row};
