#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// Mechanical-cast wall (tier-1) completing the workspace floor's
// `cast_sign_loss` + `integer_division` forbid: an `as` conversion, a truncating
// or sign-flipping `as` cast, and `unreachable!` are all rejected at compile
// time — a future `len as u32` on an untrusted-byte path is a build error, not a
// hand scan. `deny` (not `forbid`) preserves a greppable, reasoned
// `#[expect(..., reason = "...")]` escape for a provably-lossless widening (the
// workspace keystone `allow_attributes_without_reason` forces the reason).
#![deny(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::unreachable
)]

//! Async (tokio) PostgreSQL driver built on the `bsql-postgres-proto` sans-IO
//! engine.
//!
//! [`Connection`] owns an `Engine` over a `Wire<TokioSocket>` and drives each
//! verb by `.await`ing it over the tokio socket — the pump future suspends on a
//! real `Pending` until the socket is ready and is woken by tokio's reactor. The
//! linear `Live` token the engine threads is held as the connection's health bit
//! (`Some` = reusable, `None` = dead). The engine's tier-1 error model returns
//! the token inside `Ok(Outcome { live, status })` whenever the connection is
//! alive — including on a recoverable server error (reported as
//! `CommandStatus::ServerErrored`, the connection already drained to a clean
//! idle) — so a query-level error never kills the connection and there is no
//! separate token-reclaim step.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports (`Row`, `DriverError`,
//! `ConnectConfig`, `Notification`, …) carry their `size_of`/`align_of` pins in
//! `bsql-postgres-core`, where they are defined; re-exporting does not change a
//! type's footprint, so they are not re-pinned here. The engine surface types the
//! driver composes (`Engine`, `Live`, `Surface`, …) carry their pins in
//! `bsql-postgres-proto`.
//!
//! The driver's own footprint surface is its hot-path futures — the state machine
//! each `async fn` (`query`, `execute`, `query_prepared`, …) lowers to. A
//! future's type is unnameable and its size is not const-evaluable, and applying
//! `bsql_postgres_core::future_pin!` to one requires a constructed connection
//! (the future captures `&mut Connection`, which owns a live socket), so those
//! pins would live with whatever owns the futures. They are not pinned today: the
//! futures are thin (a `&mut Engine` borrow plus a local `ResultCollector`), and
//! the working set is the engine's already-pinned buffers, not driver-owned
//! state.

mod connection;
mod pool;
mod transport;

// Re-export shared types from core.
pub use bsql_postgres_core::{
    ColumnError, ConnectConfig, DbError, DriverError, Notification, ParamsWriter, QueryResult,
    Row, Rows, SafeIdent, SafeTable, SslMode, TypedNotification,
};

pub use connection::{Connection, CopyInWriter, PreparedStatement, Transaction};
pub use pool::{Pool, PooledConnection};

// Tier-1 static assertions: Connection is Send (its futures cross .await points
// and are spawned by the pool's concurrent tasks). Row is Send + Sync + 'static
// (Arc-shared arena). Pool is Send + Sync. PooledConnection is Send + 'static —
// it OWNS the connection (via `Option`) plus an `Arc` to the pool, so it can be
// moved across `.await` and into spawned tasks; a borrow-based guard would not
// be, which is why the pool keeps the owned handle.
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
        _assert_send::<PooledConnection>();
        _assert_static::<PooledConnection>();
    }
};

// Tier-1 static assertion: the flagship typed query future stays `Send` so it
// remains spawnable — the property the pool relies on. This holds in BOTH
// builds: as a plain `async fn` (default), and as the `n1-detect` reshape
// `fn -> impl Future + '_`, whose bare RPIT return type LEAKS the concrete
// future's auto-traits (a boxed `dyn Future` would not). The reshape thus adds
// no explicit `Send` bound and constrains no caller; `Send` is preserved exactly
// as the `async fn` had it (given `Send` params). Type-checked, never run.
const _: () = {
    fn _assert_query_future_send<Q: bsql_postgres_proto::TypedQuery>(
        conn: &mut Connection,
        params: Q::Params,
    ) where
        Q::Params: Send,
    {
        fn _is_send<T: Send>(_: &T) {}
        let fut = conn.query::<Q>(params);
        _is_send(&fut);
    }
};
