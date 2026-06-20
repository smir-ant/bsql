#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Sync PostgreSQL driver built on the `bsql-pg-proto` sans-IO state machine.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports (`Row`, `DriverError`,
//! `ConnectConfig`, `PreparedStatement`, `Notification`, …) carry their
//! `size_of`/`align_of` pins in `bsql-postgres-core`, where they are defined.
//! Re-exporting does not change a type's footprint, so they are not re-pinned
//! here.
//!
//! Unlike the async driver, the sync driver has no futures — its operations are
//! blocking method calls whose working set lives on the caller's stack, not in a
//! lowered state machine. So there is no `future_pin!` surface here. The
//! connection type itself is slated for replacement by a unified engine, so it
//! is intentionally not footprint-pinned now (a pin on it would be a throwaway
//! corpse pin and a false drift signal); the regime applies to the engine's
//! stable types when they land.

mod connection;
mod pool;

pub use bsql_postgres_core::{
    ConnectConfig, DbError, DriverError, FromText,
    Notification, PreparedStatement, PumpAction, QueryResult, Row, Session, SslMode,
};

pub use connection::Connection;
pub use pool::{Pool, PooledConnection};

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
