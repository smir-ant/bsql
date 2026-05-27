#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod connection;
mod pool;

pub use bsql_postgres_core::{
    ConnectConfig, DbError, DriverError, FromText,
    Notification, PreparedStatement, PumpAction, QueryResult, Row, Session, SslMode,
};

pub use connection::{Connection, PgTransaction};
pub use pool::{Pool, PooledConnection};
