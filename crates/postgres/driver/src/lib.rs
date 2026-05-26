//! Async PostgreSQL driver built on the `bsql-pg-proto` sans-IO state machine.
//!
//! This crate wraps the protocol state machine with real TCP I/O via
//! tokio. The sans-IO core guarantees cancellation safety by
//! construction — dropped futures cannot corrupt wire state.

mod config;
mod connection;
mod error;

pub use config::{ConnectConfig, SslMode};
pub use connection::{Connection, FromText, QueryResult, Row};
pub use error::DriverError;
