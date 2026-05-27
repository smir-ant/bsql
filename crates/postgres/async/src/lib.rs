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

// Tier-1 static assertions: Connection is Send (can cross .await points).
// Row is Send + Sync + 'static (Arc-shared arena).
const _: () = {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}
    fn _assert_static<T: 'static>() {}
    fn _assertions() {
        _assert_send::<Connection>();
        _assert_send::<Row>();
        _assert_sync::<Row>();
        _assert_static::<Row>();
        _assert_send::<Pool>();
        _assert_sync::<Pool>();
    }
};
