#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Async PostgreSQL driver built on the `bsql-pg-proto` sans-IO state machine.

mod connection;
mod pool;

// Re-export shared types from core
pub use bsql_postgres_core::{
    ConnectConfig, DbError, DriverError, FromText, Notification,
    PreparedStatement, PumpAction, QueryResult, Row, Session, SslMode,
};

pub use connection::Connection;
pub use pool::{Pool, PooledConnection};
