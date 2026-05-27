#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

mod connection;
mod pool;

pub use bsql_postgres_core::{
    ConnectConfig, DbError, DriverError, FromText,
    Notification, PreparedStatement, PumpAction, QueryResult, Row, Session, SslMode,
};

pub use connection::Connection;
pub use pool::{Pool, PooledConnection};

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
