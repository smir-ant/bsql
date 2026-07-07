#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
// A future undocumented `pub` item is a build error, not silent doc rot.
#![deny(missing_docs)]
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
// Panic-class mechanical wall (tier-1): an unbounded `arr[i]` and an overflowing
// `+`/`-`/`*` on a cursor are now rejected by rustc, not review. `deny` (not
// `forbid`) keeps a reasoned `#[expect]` escape. Indexing in test code is
// exempted by clippy.toml `allow-indexing-slicing-in-tests`;
// `arithmetic_side_effects` has NO such key, so the `cfg_attr(test, allow)`
// below scopes it to production.
#![deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
#![cfg_attr(
    test,
    allow(
        clippy::arithmetic_side_effects,
        reason = "test assertions/fixtures use bare arithmetic; the production deny above is the tier-1 wall"
    )
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

mod cancel;
mod connection;
mod pool;
mod transport;

// Re-export shared types from core.
pub use bsql_postgres_core::{
    ColumnError, ConnectConfig, DbError, DriverError, Notification, ParamsWriter, QueryResult,
    Row, RowRef, Rows, SafeIdent, SafeTable, SslMode, TypedNotification,
};

pub use cancel::CancelToken;
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
        // The CancelToken is a DETACHED capability: Send + Sync + 'static so it
        // can be moved into another task and shared while the owning connection's
        // query future is in flight (the whole out-of-band design). Borrowing the
        // connection would alias its in-flight `&mut` — this pins that it does not.
        _assert_send::<CancelToken>();
        _assert_sync::<CancelToken>();
        _assert_static::<CancelToken>();
    }
};

// Footprint pin: the cancel key (8) + the redial snapshot (48) = 56 bytes. The
// key and redial are pinned individually in `bsql-postgres-core`; this pins that
// composing them adds no padding. A widened token trips it.
const _: () = assert!(core::mem::size_of::<CancelToken>() == 56);

// Tier-1 static assertions: EVERY typed verb future — on the bare connection AND
// on the transaction guard — plus the `transaction` combinator stay `Send`, so
// each remains spawnable (the property the pool relies on). Previously only
// `query` was pinned; the `Send`-ness of `query_one` / `query_opt` /
// `query_each` / `execute` (and every guard verb) was an incidental property
// leaked from `Core` — a tier-4 "happens to be Send". Pinning all of them
// converts a silent, downstream-only `tokio::spawn` break into a compile error
// in bsql's OWN build.
//
// Each holds in BOTH builds: the plain `async fn` (default), and the `n1-detect`
// reshape `fn -> impl Future + '_`, whose bare RPIT return type LEAKS the
// concrete future's auto-traits (a boxed `dyn Future` would not) — so the reshape
// adds no `Send` bound and constrains no caller. Type-checked, never run.
const _: () = {
    fn _is_send<T: Send>(_: &T) {}

    // The five leaf typed verbs on the bare `Connection`, each pinned in its OWN
    // helper. Because the parameter is now a lifetime GAT (`Q::Params<'p>`) and a
    // GAT is INVARIANT in a generic context, the verb's own `'a` cannot narrow
    // below the param `'p`; tying the `&'p mut Connection` borrow to that same
    // `'p` keeps the single borrow well-formed (a shared helper calling all five
    // would need five simultaneous `&mut` borrows). The `execute` verb takes a
    // plain `P: ParamsWriter` (no GAT), so it keeps the multi-borrow-free shape.
    fn _conn_query<'p, Q>(conn: &'p mut Connection, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&conn.query::<Q>(p));
    }
    fn _conn_query_one<'p, Q>(conn: &'p mut Connection, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&conn.query_one::<Q>(p));
    }
    fn _conn_query_opt<'p, Q>(conn: &'p mut Connection, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&conn.query_opt::<Q>(p));
    }
    fn _conn_query_each<'p, Q>(conn: &'p mut Connection, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&conn.query_each::<Q, _, ()>(p, |_row| core::ops::ControlFlow::Continue(())));
    }
    fn _conn_execute<P, R>(
        conn: &mut Connection,
        q: &'static bsql_postgres_proto::PreparedQuery<P, R>,
        p_exec: P,
    ) where
        P: ParamsWriter + Send + 'static,
        R: bsql_postgres_proto::RowDecode + 'static,
    {
        _is_send(&conn.execute::<P, R>(q, p_exec));
    }

    // The same typed verbs on the transaction guard.
    fn _tx_query<'p, Q>(tx: &'p mut Transaction<'_>, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&tx.query::<Q>(p));
    }
    fn _tx_query_one<'p, Q>(tx: &'p mut Transaction<'_>, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&tx.query_one::<Q>(p));
    }
    fn _tx_query_opt<'p, Q>(tx: &'p mut Transaction<'_>, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&tx.query_opt::<Q>(p));
    }
    fn _tx_query_each<'p, Q>(tx: &'p mut Transaction<'_>, p: Q::Params<'p>)
    where
        Q: bsql_postgres_proto::TypedQuery + 'p,
        Q::Params<'p>: 'p,
        for<'x> Q::Params<'x>: Send,
    {
        _is_send(&tx.query_each::<Q, _, ()>(p, |_row| core::ops::ControlFlow::Continue(())));
    }
    fn _tx_execute<P, R>(
        tx: &mut Transaction<'_>,
        q: &'static bsql_postgres_proto::PreparedQuery<P, R>,
        p_exec: P,
    ) where
        P: ParamsWriter + Send + 'static,
        R: bsql_postgres_proto::RowDecode + 'static,
    {
        _is_send(&tx.execute::<P, R>(q, p_exec));
    }

    // The `transaction` combinator itself, given a trivial `Send` body: proves
    // its deferred-BEGIN + guard + terminating COMMIT/ROLLBACK hold nothing
    // `!Send` across an await.
    fn _transaction_combinator(conn: &mut Connection) {
        _is_send(&conn.transaction(async |_tx: &mut Transaction<'_>| Ok::<(), DriverError>(())));
    }
};
