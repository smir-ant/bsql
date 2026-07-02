#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]

//! Sync PostgreSQL driver built on the `bsql-postgres-proto` sans-IO engine.
//!
//! [`Connection`] owns an `Engine` over a `Wire<SyncSocket>` and drives each
//! verb with the engine's single-poll executor over the blocking socket. The
//! linear `Live` token the engine threads is held as the connection's health
//! bit (`Some` = reusable, `None` = dead). The engine's tier-1 error model
//! returns the token inside `Ok(Outcome { live, status })` whenever the
//! connection is alive — including on a recoverable server error (reported as
//! `CommandStatus::ServerErrored`, the connection already drained to a clean
//! idle) — so a query-level error never kills the connection and there is no
//! separate token-reclaim step.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports (`Row`, `DriverError`,
//! `ConnectConfig`, `Notification`, …) carry their `size_of`/`align_of` pins in
//! `bsql-postgres-core`, where they are defined; re-exporting does not change a
//! type's footprint, so they are not re-pinned here. The engine surface types
//! the driver composes (`Engine`, `Live`, `Surface`, …) carry their pins in
//! `bsql-postgres-proto`. The sync driver has no futures of its own — its
//! operations are blocking method calls whose working set lives on the caller's
//! stack — so there is no `future_pin!` surface here; the `Connection` shell is
//! a thin handle (engine + token + control socket + cached params) and is not
//! separately pinned.

mod connection;
mod pool;
mod transport;

pub use bsql_postgres_core::{
    ColumnError, ConnectConfig, DbError, DriverError, Notification, QueryResult, Row, Rows, SslMode,
};

pub use connection::{Connection, PreparedStatement};
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
        // PooledConnection owns the connection + an Arc to the pool, so it is
        // Send + 'static (movable across threads), not a borrow-based guard.
        _assert_send::<PooledConnection>();
        _assert_static::<PooledConnection>();
    }
};
