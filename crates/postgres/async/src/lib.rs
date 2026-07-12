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
//! future's type is unnameable and its size is not const-evaluable, so it carries
//! no `footprint_pin!`; the hot inbound dispatch it drives (`next_event`) is
//! instead gated at the machine level by the `engine_hotpath_codegen` gate
//! (`bsql-postgres-proto`), which pins that body panic-free and under a committed
//! instruction ceiling. The futures themselves are thin (a `&mut Engine` borrow
//! plus a local `ResultCollector`), and their working set is the engine's
//! already-pinned buffers, not driver-owned state.

// bsql's footprint pins (defined in `bsql-postgres-core` / `-proto`) assert exact
// `size_of` / `align_of` values computed for 64-bit pointers; on a non-64-bit
// target they fail as a wall of confusing `E0080` "FOOTPRINT DRIFT" panics. This
// one honest line replaces that wall. 64-bit is the only supported width
// (i686 / wasm32 / 32-bit ARM are unrequested and unsupported); 64-bit builds are
// unaffected.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("bsql requires a 64-bit target; the footprint pins assume 64-bit pointers");

mod cancel;
mod connection;
mod pool;
mod transport;

// Re-export shared types from core.
pub use bsql_postgres_core::{
    AppliedMigration, BorrowedRow, ChannelBindingMode, ColumnError, CommandTag, ConnectConfig, DbError, DiagEvent, DiagSink, Diagnostics, PoolStats, DriftKind, DriverError,
    MigrationError, MigrationReport, MigrationSource, MigrationSourceError, MigrationStatus,
    Notification, ParamsWriter, QueryResult, Row, RowRef, Rows, SafeIdent, SafeTable, SslMode,
    TypedNotification, LEDGER_TABLE,
};

// The diagnostics-only N+1 detector (feature `n1-detect`). Re-exported so
// `bsql::pg::N1Report` resolves at the path a consumer uses — the SAME
// `bsql_common::N1Report` the sync driver and the SQLite driver re-export, so a
// consumer can write ONE function over both backends' reports.
#[cfg(feature = "n1-detect")]
pub use bsql_postgres_core::{N1Report, N1Tracker};

pub use cancel::CancelToken;
pub use connection::{Connection, CopyInWriter, PreparedStatement, Transaction};
pub use pool::{Pool, PoolBuilder, PooledConnection};

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

// Tier-1 static assertions completing the wall over the DYNAMIC (runtime-SQL)
// surface — the same doctrine as the typed-verb wall above, applied to every
// remaining spawnable public future. Each of these forwards `Core`'s future
// directly (a `fn -> impl Future`, or on the guard a thin `armed` wrapper), so
// its `Send`-ness was an incidental property LEAKED from `Core` — a tier-4
// "happens to be Send". These convert a silent, downstream-only `tokio::spawn`
// break (a future refactor threading an `Rc`-shared plan into `DynStmtCache` or
// a `Cell` counter into the notification ledger) into a compile error in bsql's
// OWN build, and REPLACE the class of spawn-integration test that would
// otherwise be needed to prove each future spawnable. Both surfaces are pinned
// (the bare `Connection` AND the `Transaction` guard) because the guard wraps
// each verb in a distinct `armed` future — a genuinely separate type. Every
// bound is the MINIMAL one the future truly requires; over-constraining would
// weaken the guarantee. Type-checked, never run.
const _: () = {
    fn _is_send<T: Send>(_: &T) {}

    // ── Bare `Connection` ────────────────────────────────────────────────────

    // The no-generic verbs: each is a separate statement, so the `&mut conn`
    // reborrow the future holds is released at the `;` before the next call (no
    // GAT invariance to pin the borrow open, unlike the typed verbs above), so
    // one helper covers them all. Includes the lifecycle + session futures
    // (`begin`/`commit`/`rollback`/`reset_session`/`listen`/`unlisten`/`close`)
    // and BOTH notification futures — the complete no-generic async surface.
    fn _conn_dyn(conn: &mut Connection, sql: &str) {
        _is_send(&conn.ping());
        _is_send(&conn.simple_query(sql));
        _is_send(&conn.execute_sql(sql));
        _is_send(&conn.query_sql(sql));
        _is_send(&conn.query_one_sql(sql));
        _is_send(&conn.query_opt_sql(sql));
        _is_send(&conn.prepare(sql));
        _is_send(&conn.begin());
        _is_send(&conn.commit());
        _is_send(&conn.rollback());
        _is_send(&conn.reset_session());
        _is_send(&conn.listen(sql));
        _is_send(&conn.unlisten(sql));
        _is_send(&conn.close());
        _is_send(&conn.recv_notification(core::time::Duration::from_secs(1)));
        _is_send(&conn.recv_notification_as::<String>(core::time::Duration::from_secs(1)));
    }

    // The parameterised verbs. CRUCIAL bound: the params are held by REFERENCE
    // (`&'a P`) across the await, so the future is `Send` iff `&P: Send` iff
    // `P: Sync` — NOT `P: Send` (the future never owns a `P`). A `P: Send` bound
    // here would fail to compile against a `Sync`-only param and mis-state the
    // real contract. One representative `P` covers `query_prepared` /
    // `execute_prepared` / `query_params{,_one,_opt}` / `execute_params`.
    fn _conn_params<P: ParamsWriter + Sync>(
        conn: &mut Connection,
        sql: &str,
        stmt: &PreparedStatement,
        p: &P,
    ) {
        _is_send(&conn.query_prepared::<P>(stmt, p));
        _is_send(&conn.execute_prepared::<P>(stmt, p));
        _is_send(&conn.query_params::<P>(sql, p));
        _is_send(&conn.query_params_one::<P>(sql, p));
        _is_send(&conn.query_params_opt::<P>(sql, p));
        _is_send(&conn.execute_params::<P>(sql, p));
    }

    // `close_statement` CONSUMES the statement by value, so it takes its own
    // helper (the moved `stmt` cannot be reused for another call).
    fn _conn_close_statement(conn: &mut Connection, stmt: PreparedStatement) {
        _is_send(&conn.close_statement(stmt));
    }

    // The COPY-in / COPY-out futures, each with a representative `Send` argument
    // (an owned `Vec<String>` row source, and trivial nothing-captured
    // closures): proves the bulk-load / bulk-unload orchestration holds nothing
    // `!Send` across an await. The typed binary `copy_in_typed` is pinned
    // separately (its bound is a non-obvious GAT interaction).
    fn _conn_copy(conn: &mut Connection) {
        _is_send(&conn.copy_in("t", Vec::<String>::new()));
        _is_send(
            &conn.copy_in_with("t", async |_w: &mut CopyInWriter<'_>| Ok::<(), DriverError>(())),
        );
        _is_send(&conn.copy_out("t", |_c: &[u8]| core::ops::ControlFlow::<()>::Continue(())));
    }

    // The typed binary `copy_in_typed` future is the single most fragile
    // un-pinned spot: its `Send`-ness rides on a GENERIC-GAT interaction, so
    // the naive `I: IntoIterator` bound is not enough. The future holds `rows: I`
    // across the header round trip (`copy_in_begin`) BEFORE the iterator is
    // consumed, then holds `I::IntoIter` across each per-row write, and borrows
    // each `Q::Row<'q>` (`&row`) across that write — so the MINIMAL, honest
    // contract is all three: `I: Send`, `I::IntoIter: Send`, and the
    // `for<'x>`-quantified `Q::Row<'x>: Send + Sync` (the GAT is invariant, so
    // the quantified form is the one that holds). Each of the three is
    // load-bearing (dropping any one fails to compile), so this both PINS the
    // future spawnable and DOCUMENTS the exact bound a consumer spawning a typed
    // COPY must satisfy — otherwise discoverable only by trial compile.
    fn _conn_copy_in_typed<'q, Q, I>(conn: &mut Connection, rows: I)
    where
        Q: bsql_postgres_proto::TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>> + Send,
        I::IntoIter: Send,
        for<'x> Q::Row<'x>: Send + Sync,
    {
        _is_send(&conn.copy_in_typed::<Q, I>(rows));
    }

    // ── `Transaction` guard (each verb is a distinct `armed`-wrapped future) ──

    fn _tx_dyn(tx: &mut Transaction<'_>, sql: &str) {
        _is_send(&tx.ping());
        _is_send(&tx.simple_query(sql));
        _is_send(&tx.execute_sql(sql));
        _is_send(&tx.query_sql(sql));
        _is_send(&tx.query_one_sql(sql));
        _is_send(&tx.query_opt_sql(sql));
        _is_send(&tx.prepare(sql));
        _is_send(&tx.listen(sql));
        _is_send(&tx.unlisten(sql));
    }

    fn _tx_params<P: ParamsWriter + Sync>(
        tx: &mut Transaction<'_>,
        sql: &str,
        stmt: &PreparedStatement,
        p: &P,
    ) {
        _is_send(&tx.query_prepared::<P>(stmt, p));
        _is_send(&tx.execute_prepared::<P>(stmt, p));
        _is_send(&tx.query_params::<P>(sql, p));
        _is_send(&tx.query_params_one::<P>(sql, p));
        _is_send(&tx.query_params_opt::<P>(sql, p));
        _is_send(&tx.execute_params::<P>(sql, p));
    }

    fn _tx_close_statement(tx: &mut Transaction<'_>, stmt: PreparedStatement) {
        _is_send(&tx.close_statement(stmt));
    }

    fn _tx_copy(tx: &mut Transaction<'_>) {
        _is_send(&tx.copy_in("t", Vec::<String>::new()));
        _is_send(
            &tx.copy_in_with("t", async |_w: &mut CopyInWriter<'_>| Ok::<(), DriverError>(())),
        );
        _is_send(&tx.copy_out("t", |_c: &[u8]| core::ops::ControlFlow::<()>::Continue(())));
    }

    // The guard's typed COPY carries the SAME three-part bound as the connection
    // method (`armed` adds only `Send` captures — the `&mut Core` / `&mut bool`
    // it threads — so the contract is unchanged).
    fn _tx_copy_in_typed<'q, Q, I>(tx: &mut Transaction<'_>, rows: I)
    where
        Q: bsql_postgres_proto::TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>> + Send,
        I::IntoIter: Send,
        for<'x> Q::Row<'x>: Send + Sync,
    {
        _is_send(&tx.copy_in_typed::<Q, I>(rows));
    }
};
