#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Async PostgreSQL driver built on the `bsql-pg-proto` sans-IO state machine.
//!
//! This crate wraps the protocol state machine with real TCP I/O via
//! tokio. The sans-IO core guarantees cancellation safety by
//! construction — dropped futures cannot corrupt wire state.

mod config;
mod connection;
mod error;
mod pool;

pub use config::{ConnectConfig, SslMode};
pub use connection::{Connection, FromText, Notification, PreparedStatement, QueryResult, Row};
pub use error::{DbError, DriverError};
pub use pool::{Pool, PooledConnection};
