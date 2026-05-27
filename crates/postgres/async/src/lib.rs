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
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_static<T: 'static>() {}
    fn _assertions() {
        assert_send::<Connection>();
        assert_send::<Row>();
        assert_sync::<Row>();
        assert_static::<Row>();
        assert_send::<Pool>();
        assert_sync::<Pool>();
    }
};
