#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Async PostgreSQL driver built on the `bsql-pg-proto` sans-IO state machine.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports (`Row`, `DriverError`,
//! `ConnectConfig`, `PreparedStatement`, `Notification`, …) carry their
//! `size_of`/`align_of` pins in `bsql-postgres-core`, where they are defined —
//! re-exporting does not change a type's footprint, so they are not re-pinned
//! here.
//!
//! The driver's own footprint surface is its **hot-path futures** — the
//! state-machine each `async fn` (`query`, `execute`, `query_prepared`, …)
//! lowers to. A future's type is unnameable and its size is not const-evaluable,
//! so the regime's [`bsql_postgres_core::future_pin!`] gates a future's
//! `size_of_val` from a `#[test]` that constructs it without polling. Applying
//! it to the hot-path futures requires a constructed connection (the future
//! captures `&mut Connection`, which owns a live socket), so those pins live
//! with whatever owns the futures.
//!
//! The `Connection::connect` future is the only hot-path future
//! constructible without an open socket. It is not pinned today: the
//! connection type that owns these futures is slated for replacement by a
//! unified engine, so a hard pin on it now would be a throwaway corpse pin
//! and a false drift signal. It will be footprint-pinned with `future_pin!`
//! when the unified engine lands — the regime (the macros) is exactly what
//! makes that a one-line addition per future.

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
