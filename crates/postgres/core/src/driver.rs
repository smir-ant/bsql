//! The transport-generic driver engine shared by the async and sync drivers.
//!
//! [`Core<S>`] holds the sans-IO [`Engine`] over a [`Wire<S>`] plus the linear
//! liveness token the engine's verbs thread, and defines EVERY non-I/O verb
//! ONCE — written in `async` style (each leaf op is `.await`ed). The two drivers
//! wrap a `Core<S>` and drive its verbs differently:
//!
//! - the **async** driver `.await`s each verb future over its tokio socket
//!   (`S = TokioSocket`), where a leaf op genuinely suspends on `Pending`;
//! - the **sync** driver drives each verb future with a SINGLE
//!   [`poll_once`](bsql_postgres_proto::engine::poll_once) over its blocking
//!   socket (`S = SyncSocket`). Because
//!   every leaf op over a blocking transport resolves on its FIRST poll (never
//!   `Pending`), a whole composite verb future — synchronous prologue, the one
//!   awaited engine call, and synchronous epilogue — runs to completion in that
//!   one poll. `poll_once` is thus a TOTAL single-poll drive of the same verb
//!   body, so the sync driver reuses this async-shaped code verbatim.
//!
//! `Core<S>` is generic over the transport and MONOMORPHISES per driver
//! (`Core<TokioSocket>`, `Core<SyncSocket>`) — static dispatch, no `dyn`, no
//! `Box`, no vtable, no added indirection. The collapse is a SOURCE dedup, not a
//! runtime abstraction: each driver's emitted verb code is what it was before,
//! now produced from one definition instead of two hand-maintained twins. Making
//! driver parity a compiler guarantee is the point — a fix to a verb here cannot
//! silently fail to mirror to the other driver, because there is only one verb.
//!
//! # Token lifecycle and recovery (the health bit)
//!
//! The `live` token is the health bit: `Some` = the connection is at a clean
//! boundary and reusable, `None` = a verb failed fatally and the connection is
//! dead. The engine's tier-1 error model decides the bit: a verb returns its
//! linear [`Live`] token inside `Ok(Outcome { live, status })` whenever the
//! connection is ALIVE — including on a *recoverable* server error (a query-level
//! `ErrorResponse`), which the verb drains to a clean idle itself and reports as
//! [`CommandStatus::ServerErrored`]. So the internal `settle` step ALWAYS restores
//! the token from an `Ok` outcome (no separate token reclaim), then maps a
//! `ServerErrored` status to `Err(DriverError::Db)` while keeping the connection
//! pooled. Only a FATAL `Err(EngineError)` (transport/protocol/EOF) leaves the
//! token `None`.
//!
//! # What stays in the drivers
//!
//! Connect (dial + TLS + the timeout budget), the pool, `recv_notification`'s
//! deadline arming, `transaction` and `copy_in_with` (irreducibly different
//! closure kinds — async closure vs `FnOnce`), and the per-driver `CopyInWriter`
//! stay in each driver. This module exposes the small [copy seam](Core::copy_in_begin)
//! and [`recv_notification_inner`](Core::recv_notification_inner) those keep-per-driver
//! methods orchestrate.

use core::fmt::Write as _;
use core::ops::ControlFlow;
use std::io;
use std::sync::Arc;

use bsql_postgres_proto::engine::{
    Boundary, CommandStatus, ConnFail, Engine, EngineError, Live, NotifyStatus, Outcome,
    PreparedStatement as WireStatement, Surface, Transport,
};
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    DecodeError, PreparedQuery, RowDecode, StmtName, TxStatus, TypedQuery,
};

use crate::materialize::{self, ResultCollector};
use crate::sql_ident::{self, SafeIdent, SafeTable};
use crate::tls::{TlsError, Wire};
// `CaRootsError` names a rustls parse failure, so it exists only under `tls`.
#[cfg(feature = "tls")]
use crate::tls::CaRootsError;
use crate::{
    capture_notify, DbError, DbErrorSink, DriverError, Notification, NotificationLedger,
    QueryResult, Rows, RowsBuilder,
};

/// The arm-uniform transport error the drivers share: a plaintext socket error
/// rides [`TlsError::Socket`]; the TLS arm's error already is this type. Both the
/// tokio and the blocking socket have `Error = std::io::Error`, so a
/// [`Wire<S>`] over either has this concrete error — which is why [`Core<S>`] is
/// bounded `S: Transport<Error = io::Error>` and the `lift_*` helpers are
/// concrete (not generic over the socket).
pub type WireError = TlsError<io::Error>;

/// Why a streaming [`query_each`](Core::query_each) sink stopped the pump early —
/// the break payload it hands to the engine's breakable verb.
///
/// Two DISTINCT constructors keep a per-row typed-decode failure and a
/// caller-requested stop impossible to conflate: the pump boundary's `Stopped`
/// payload alone says which happened, so the driver never has to cross-reference
/// a side channel to know why the stream ended. Only ever constructed on the
/// cold break path (a stack value), never on the per-row hot path.
enum Stop<E> {
    /// A row's bytes did not match the query's compile-time record shape.
    Decode(DecodeError),
    /// The caller's `on_row` returned [`ControlFlow::Break`], carrying its payload.
    User(E),
}

/// A fixed-capacity ASCII sink so a generated prepared-statement name renders
/// with NO heap allocation (the old `format!` cost one `String` per prepare).
///
/// Capacity 16 = the 6-byte `_bsql_` prefix + a `u32`'s at-most-10 decimal
/// digits, so `write!(_, "_bsql_{id}")` for any `u32` fits exactly and never
/// overflows. A write past capacity is refused (a `fmt::Error`), never
/// truncated silently — but with the fixed prefix + a `u32` that is
/// structurally impossible.
struct StmtNameBuf {
    buf: [u8; 16],
    len: usize,
}

impl core::fmt::Write for StmtNameBuf {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let end = self.len.checked_add(s.len()).ok_or(core::fmt::Error)?;
        let dst = self.buf.get_mut(self.len..end).ok_or(core::fmt::Error)?;
        dst.copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

impl StmtNameBuf {
    /// A fresh empty sink.
    fn new() -> Self {
        Self {
            buf: [0u8; 16],
            len: 0,
        }
    }

    /// The bytes written so far as a `&str`. Every fragment came from a `&str`,
    /// so `[..len]` is valid UTF-8 by construction; the two failure edges are
    /// structurally unreachable, so they surface as a classified (fail-closed)
    /// error rather than a silent fallback.
    fn as_str(&self) -> Result<&str, DriverError> {
        let bytes = self
            .buf
            .get(..self.len)
            .ok_or(DriverError::Config("generated statement name invalid"))?;
        core::str::from_utf8(bytes).map_err(|_| DriverError::Config("generated statement name invalid"))
    }
}

/// A prepared statement handle, shared by both drivers.
///
/// Carries the engine's wire-level statement handle (statement name + recovered
/// result OIDs) plus the column names captured at prepare time — the extended
/// execute reply does not re-send them, so a prepared query's `QueryResult`
/// draws its names from here. Move-only: [`close_statement`](Core::close_statement)
/// consumes it by value, so a use after close is a compile error (E0382), not a
/// runtime use-after-close. Each driver re-exports this type.
///
/// [`close_statement`]: Core::close_statement
#[derive(Debug)]
pub struct PreparedStatement {
    inner: WireStatement,
    column_names: Arc<[String]>,
}

/// The transport-generic driver engine: the shared owner of the sans-IO
/// [`Engine`] + liveness token, defining every non-I/O verb once.
///
/// `S` is the concrete socket (a testkit fake, tokio, or blocking); the
/// engine is monomorphic over the plaintext-or-TLS [`Wire<S>`] multiplexer, so
/// each driver gets its own zero-cost monomorphisation. See the [module
/// docs](self) for the single-poll soundness that lets the sync driver reuse this
/// async-shaped code.
///
/// No `Debug`: it owns a live socket / TLS session (not `Debug`), the same reason
/// the driver `Connection`s carry none.
pub struct Core<S: Transport<Error = io::Error>> {
    /// The owned, poolable engine handle (branded `'static`).
    engine: Engine<'static, Wire<S>>,
    /// The liveness token, or `None` when the connection is dead. The health bit.
    live: Option<Live<'static>>,
    /// Whether the underlying wire is TLS-encrypted, snapshotted at connect from
    /// the built [`Wire`] arm (PostgreSQL negotiates TLS once, before startup,
    /// and never up/downgrades mid-session). Read via
    /// [`is_encrypted`](Self::is_encrypted).
    encrypted: bool,
    /// The server version reported at connect, if the startup `ParameterStatus`
    /// stream carried one (honest absence otherwise).
    server_version: Option<String>,
    /// The backend process id from `BackendKeyData`.
    backend_pid: i32,
    /// Monotonic counter for generating fresh prepared-statement names.
    stmt_counter: u32,
    /// The bounded, counted no-drop buffer of asynchronous notifications. Every
    /// verb's sink is wrapped with [`capture_notify`] so a `NOTIFY` arriving on
    /// any command's response stream is buffered here rather than dropped.
    notifications: NotificationLedger,
    /// The diagnostics-only N+1 query detector. Present ONLY under the
    /// `n1-detect` feature — a default build has no such field, so the flagship
    /// typed verbs stay byte-identical and the footprint is unchanged.
    #[cfg(feature = "n1-detect")]
    n1_tracker: crate::N1Tracker,
}

impl<S: Transport<Error = io::Error>> Core<S> {
    /// Assemble a `Core` from a freshly-handshaken engine and its liveness token.
    ///
    /// Called by each driver's per-driver `connect` after it has built the wire,
    /// opened the engine, driven the startup/auth handshake, and read the
    /// connect-time session facts (`encrypted`, `server_version`, `backend_pid`)
    /// off the engine. `#[doc(hidden)]`: the driver-facing construction seam, not
    /// a public API.
    #[doc(hidden)]
    #[must_use]
    pub fn new(
        engine: Engine<'static, Wire<S>>,
        live: Live<'static>,
        encrypted: bool,
        server_version: Option<String>,
        backend_pid: i32,
    ) -> Self {
        Self {
            engine,
            live: Some(live),
            encrypted,
            server_version,
            backend_pid,
            stmt_counter: 0,
            notifications: NotificationLedger::new(),
            #[cfg(feature = "n1-detect")]
            n1_tracker: crate::N1Tracker::new(),
        }
    }

    // ── Token + result plumbing (shared internals) ──────────────────────────

    /// Take the liveness token, or classify a dead connection.
    fn take_live(&mut self) -> Result<Live<'static>, DriverError> {
        self.live.take().ok_or(DriverError::NotReady)
    }

    /// Classify a command verb's [`Outcome`] and manage the token.
    ///
    /// An `Ok` outcome ALWAYS restores the token — the connection is alive
    /// whether the command completed or recovered from a server error (the verb
    /// already drained the recovering `ReadyForQuery`). A
    /// [`CommandStatus::ServerErrored`] then surfaces the parsed [`DbError`] the
    /// collector captured from the raw `ErrorResponse`, while the connection
    /// stays pooled. A fatal `Err` (transport/protocol/EOF) leaves the token gone
    /// (`self.live == None`) — no separate token-reclaim step exists.
    ///
    /// [`DbError`]: crate::DbError
    fn settle(
        &mut self,
        outcome: Result<Outcome<'static, CommandStatus>, EngineError<WireError>>,
        collector: &mut impl DbErrorSink,
    ) -> Result<(), DriverError> {
        match outcome {
            Ok(Outcome { live, status }) => {
                // The connection is alive on either status — restore the token.
                self.live = Some(live);
                match status {
                    CommandStatus::Completed => Ok(()),
                    CommandStatus::ServerErrored => match collector.take_db_error() {
                        Some(db) => Err(DriverError::Db(Box::new(db))),
                        None => Err(DriverError::UnclassifiedFailure),
                    },
                }
            }
            // Fatal: the verb consumed the token and the connection is dead.
            Err(other) => Err(lift_engine_error(other)),
        }
    }

    /// Generate a fresh, unique prepared-statement name.
    fn next_stmt_name(&mut self) -> Result<StmtName, DriverError> {
        let id = self.stmt_counter;
        self.stmt_counter = self.stmt_counter.wrapping_add(1);
        // Stack-render "_bsql_<id>" into a fixed 16-byte buffer — no heap
        // `String` / `format!` allocation per prepare.
        let mut name = StmtNameBuf::new();
        write!(name, "_bsql_{id}")
            .map_err(|_| DriverError::Config("generated statement name invalid"))?;
        StmtName::try_from_str(name.as_str()?)
            .map_err(|_| DriverError::Config("generated statement name invalid"))
    }

    /// Build a [`QueryResult`] from a finished collector, optionally overriding
    /// the column names (the prepared path supplies the names captured at prepare
    /// time, since the execute reply re-sends none).
    fn build_query_result(
        collector: ResultCollector,
        names_override: Option<Arc<[String]>>,
    ) -> Result<QueryResult, DriverError> {
        let collected = collector.finish()?;
        let column_names = match names_override {
            Some(names) => names,
            None => Arc::from(collected.column_names.into_boxed_slice()),
        };
        Ok(QueryResult {
            rows: collected.rows,
            command_tag: collected.command_tag,
            column_names,
        })
    }

    // ── Runtime-SQL verbs ───────────────────────────────────────────────────

    /// Round-trip a `Sync` to confirm the connection is live.
    pub async fn ping(&mut self) -> Result<(), DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .ping(
                live,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)
    }

    /// Issue a simple query, returning the command tag string.
    pub async fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .simple_query(
                live,
                sql,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        // Move the already-owned tag out — no clone (the collector is dropped).
        Ok(collector.into_command_tag())
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    pub async fn execute_sql(&mut self, sql: &str) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .execute(
                live,
                sql,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Ok(collector.affected())
    }

    /// Run a row-returning runtime-SQL query (text result columns).
    pub async fn query_sql(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .query(
                live,
                sql,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Self::build_query_result(collector, None)
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    pub async fn query_one_sql(&mut self, sql: &str) -> Result<crate::Row, DriverError> {
        self.query_sql(sql)
            .await?
            .rows
            .into_iter()
            .next()
            .ok_or(DriverError::NoRows)
    }

    /// Run a runtime-SQL query returning the first row if any.
    pub async fn query_opt(&mut self, sql: &str) -> Result<Option<crate::Row>, DriverError> {
        Ok(self.query_sql(sql).await?.rows.into_iter().next())
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let stmt_name = self.next_stmt_name()?;
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .prepare(
                live,
                &stmt_name,
                sql,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        let result_oids = collector.oids().to_vec();
        let column_names: Arc<[String]> =
            Arc::from(collector.column_names().to_vec().into_boxed_slice());
        Ok(PreparedStatement {
            inner: WireStatement::new(stmt_name, result_oids),
            column_names,
        })
    }

    /// Execute a prepared statement returning rows. Params are borrowed all the
    /// way to the engine, so a non-`Copy` owned param binds by reference.
    pub async fn query_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .query_prepared(
                live,
                &stmt.inner,
                params,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Self::build_query_result(collector, Some(stmt.column_names.clone()))
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count.
    pub async fn execute_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .execute_prepared(
                live,
                &stmt.inner,
                params,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Ok(collector.affected())
    }

    /// Run a one-shot runtime-SQL query with params in ONE round trip.
    ///
    /// Fuses `Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` + `Sync`
    /// into a single flush (see [`Engine::query_params_fused`]), so a one-shot
    /// parameterised query costs ONE round trip instead of the three the old
    /// prepare / bind+execute / close sequence took. The result schema (OIDs +
    /// names) is recovered from the inline `Describe`(portal) `RowDescription`, so
    /// the [`QueryResult`]'s column names come straight from the collector — no
    /// separate `prepare` round trip. The unnamed statement is implicitly
    /// discarded at the next `Parse`, so no `Close` is needed and the
    /// prepared-statement cache is untouched. For a query executed REPEATEDLY,
    /// prefer an explicit [`prepare`](Self::prepare) +
    /// [`query_prepared`](Self::query_prepared) to amortize the parse.
    ///
    /// [`Engine::query_params_fused`]: bsql_postgres_proto::engine::Engine::query_params_fused
    pub async fn query_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .query_params_fused(
                live,
                sql,
                params,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        // Names come from the collector (recovered from the inline
        // `Describe`(portal) `RowDescription`), not a prepared-statement override.
        Self::build_query_result(collector, None)
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub async fn query_params_one<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<crate::Row, DriverError> {
        self.query_params(sql, params)
            .await?
            .rows
            .into_iter()
            .next()
            .ok_or(DriverError::NoRows)
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub async fn query_params_opt<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<crate::Row>, DriverError> {
        Ok(self.query_params(sql, params).await?.rows.into_iter().next())
    }

    /// Run a one-shot runtime-SQL command with params in ONE round trip,
    /// returning the affected-row count.
    ///
    /// The side-effect twin of [`query_params`](Self::query_params): the same
    /// fused `Parse`+`Bind`+`Describe`+`Execute`+`Sync` single round trip. A
    /// no-RETURNING command answers the `Describe`(portal) with `NoData`; the
    /// affected count rides the `CommandComplete` tag exactly as before.
    pub async fn execute_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .query_params_fused(
                live,
                sql,
                params,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Ok(collector.affected())
    }

    /// Close a prepared statement, consuming it (use-after-close is a move error).
    pub async fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .close_statement(
                live,
                stmt.inner,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)
    }

    // ── Compile-checked typed verbs (the `query!` flagship) ─────────────────

    /// Execute a compile-checked `query!` query for its side effect, returning
    /// the affected-row count (binary-uniform params).
    ///
    /// Under `n1-detect` the `caller` the driver captured at the USER call site is
    /// recorded against the query for N+1 detection (diagnostics-only — the
    /// recording never alters the result).
    pub async fn execute<P, R>(
        &mut self,
        q: &'static PreparedQuery<P, R>,
        params: P,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<u64, DriverError>
    where
        P: ParamsWriter + 'static,
        R: RowDecode + 'static,
    {
        #[cfg(feature = "n1-detect")]
        self.n1_record(q.sql(), caller);
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .query_params(
                live,
                q,
                params,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Ok(collector.affected())
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query. Under `n1-detect` records the USER call site.
    pub async fn query<Q: TypedQuery>(
        &mut self,
        params: Q::Params,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Rows<Q>, DriverError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        self.query_collect::<Q>(params).await
    }

    /// The typed-collect body behind [`query`](Self::query): collects a typed
    /// result into a [`Rows<Q>`] prebuffer and classifies an oversize row loudly.
    /// Records nothing — the N+1 hook fires exactly once in the public verb that
    /// called this. ([`query_one`](Self::query_one) does NOT route through here:
    /// it decodes its single row directly off the wire, with no prebuffer.)
    async fn query_collect<Q: TypedQuery>(
        &mut self,
        params: Q::Params,
    ) -> Result<Rows<Q>, DriverError> {
        let live = self.take_live()?;
        let mut builder = RowsBuilder::new();
        let outcome = self
            .engine
            .query_params(
                live,
                &Q::PREPARED,
                params,
                capture_notify(&mut self.notifications, |s| {
                    builder.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut builder)?;
        // The connection is now idle + pooled. An oversize row cannot be decoded
        // contiguously by the bounded path; classify it loudly.
        if builder.saw_oversize() {
            return Err(DriverError::OversizeRow);
        }
        Ok(builder.finish::<Q>())
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record. Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`]. Under `n1-detect` records the USER call site
    /// exactly once.
    ///
    /// Decodes the single expected row DIRECTLY into its owned twin off the wire,
    /// with NO intermediate prebuffer: the [`query`](Self::query) collect path
    /// would allocate a [`Rows<Q>`]'s `wire` + `slots` vectors (plus a memcpy of
    /// the row bytes into `wire`) and then a per-result owned `Vec` — three heap
    /// allocations and a copy to return ONE record. Instead this streams via the
    /// engine's breakable verb, decodes the first `Surface::Row` straight into an
    /// `Option<Q::Owned>` (the owned twin does not borrow the transient ingest
    /// buffer, so it safely outlives the pump), and BREAKS on a second row.
    ///
    /// Error precedence is exactly the old collect-then-count path's: an oversize
    /// row dominates (it was checked before the count); then a second row is
    /// [`TooManyRows`](DriverError::TooManyRows) — dominating even a malformed
    /// first row, since the old `_ => TooManyRows` arm never decoded a >1-row
    /// result (so a first-row decode failure is PARKED, not raised, while a
    /// second row is still awaited); a lone malformed row is
    /// [`Decode`](DriverError::Decode); zero rows is
    /// [`NoRows`](DriverError::NoRows).
    pub async fn query_one<Q: TypedQuery>(
        &mut self,
        params: Q::Params,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Q::Owned, DriverError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        let live = self.take_live()?;
        // The single decoded row (owned, so it outlives the pump), plus the
        // read-after-settle flags the streaming sink parks.
        let mut row: Option<Q::Owned> = None;
        let mut seen_first = false;
        let mut decode_err: Option<DecodeError> = None;
        let mut db_error: Option<DbError> = None;
        let mut oversize = false;
        let outcome = self
            .engine
            .query_params_break(
                live,
                &Q::PREPARED,
                params,
                capture_notify(&mut self.notifications, |surface| match surface {
                    Surface::Row(body) => {
                        if seen_first {
                            // A SECOND row: the caller asked for exactly one, so
                            // stop the pump — a too-many-rows condition, reported
                            // after the reclaiming drain below.
                            return ControlFlow::Break(());
                        }
                        seen_first = true;
                        match Q::decode_owned(body) {
                            Ok(owned) => row = Some(owned),
                            // PARK a first-row decode failure — do NOT stop: a
                            // following row must still surface as too-many, exactly
                            // as the old collect-all path (which never decoded past
                            // a >1-row result) classified it.
                            Err(de) => decode_err = Some(de),
                        }
                        ControlFlow::Continue(())
                    }
                    // Capture the server error's cause; let the pump reach `Failed`
                    // so the connection can be drained to idle.
                    Surface::Fail(body) => {
                        db_error = Some(materialize::parse_error_response(body));
                        ControlFlow::Continue(())
                    }
                    // An oversize row streams as chunks the bounded typed decoder
                    // cannot reassemble; flag it for a classified `OversizeRow`
                    // after the stream ends — never reassemble, never truncate.
                    Surface::RowChunk(_) | Surface::RowChunkEnd => {
                        oversize = true;
                        ControlFlow::Continue(())
                    }
                    _ => ControlFlow::Continue(()),
                }),
            )
            .await;

        let (live, boundary) = match outcome {
            Ok(Outcome { live, status }) => (live, status),
            Err(other) => return Err(lift_engine_error(other)),
        };
        match boundary {
            Boundary::Idle => {
                // Streamed to a clean idle — token restored, no drain needed.
                self.live = Some(live);
                // Oversize dominates (the collect path checked it before the count).
                if oversize {
                    return Err(DriverError::OversizeRow);
                }
                match (row, decode_err) {
                    (Some(owned), _) => Ok(owned),
                    (None, Some(de)) => Err(DriverError::Decode(de)),
                    (None, None) => Err(DriverError::NoRows),
                }
            }
            Boundary::Failed => {
                // Server error: drain the recovering `ReadyForQuery`, then surface
                // the parsed cause. Connection stays alive + pooled.
                self.drain_to_idle(live).await?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(()) => {
                // Broke on the second row: drain to reclaim, then classify.
                // Oversize still dominates too-many (matching the collect path).
                self.drain_to_idle(live).await?;
                if oversize {
                    return Err(DriverError::OversizeRow);
                }
                Err(DriverError::TooManyRows)
            }
            // `query_params_break` maps Closed/Suspended to a fatal `Err`, so they
            // never ride an `Ok` outcome; `Boundary` is `#[non_exhaustive]`, so
            // this classified arm also covers any future boundary. The token is
            // dropped (not restored), leaving the connection dead + evictable.
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a single-row query",
            ))),
        }
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query). Under
    /// `n1-detect` records the USER call site.
    ///
    /// See each driver's `query_each` for the full contract (return values,
    /// early-abort cost, decode/oversize/server-error handling).
    pub async fn query_each<Q, F, E>(
        &mut self,
        params: Q::Params,
        mut on_row: F,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Option<E>, DriverError>
    where
        Q: TypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        let live = self.take_live()?;
        // Captured across the streaming sink; read after the verb settles.
        let mut db_error: Option<DbError> = None;
        let mut oversize = false;
        let outcome = self
            .engine
            .query_params_break(
                live,
                &Q::PREPARED,
                params,
                capture_notify(&mut self.notifications, |surface| match surface {
                    Surface::Row(body) => match Q::decode_borrowed(body) {
                        // The record borrows the transient ingest buffer; `on_row`
                        // consumes it in-scope (the `for<'q>` wall forbids escape).
                        Ok(rec) => match on_row(rec) {
                            ControlFlow::Continue(()) => ControlFlow::Continue(()),
                            ControlFlow::Break(e) => ControlFlow::Break(Stop::User(e)),
                        },
                        // A decode failure is LOUD: stop the pump, never Continue
                        // past it and never substitute a default.
                        Err(de) => ControlFlow::Break(Stop::Decode(de)),
                    },
                    // Capture the server error's cause, then let the pump reach its
                    // `Failed` boundary so the connection can be drained to idle.
                    Surface::Fail(body) => {
                        db_error = Some(materialize::parse_error_response(body));
                        ControlFlow::Continue(())
                    }
                    // An oversize row streams as chunks the bounded typed decoder
                    // cannot reassemble; flag it for a classified `OversizeRow`
                    // after the stream ends — never reassemble, never truncate.
                    Surface::RowChunk(_) | Surface::RowChunkEnd => {
                        oversize = true;
                        ControlFlow::Continue(())
                    }
                    // COPY / delivery / other async frames are not stream rows (a
                    // NOTIFY is captured into the ledger by the wrapper above this
                    // match, so it never reaches here to be dropped).
                    _ => ControlFlow::Continue(()),
                }),
            )
            .await;

        // The token rides `Ok` on any ALIVE boundary; a fatal is `Err`.
        let (live, boundary) = match outcome {
            Ok(Outcome { live, status }) => (live, status),
            Err(other) => return Err(lift_engine_error(other)),
        };
        match boundary {
            Boundary::Idle => {
                // Streamed to completion at a clean idle — no drain needed.
                self.live = Some(live);
                if oversize {
                    return Err(DriverError::OversizeRow);
                }
                Ok(None)
            }
            Boundary::Failed => {
                // Server error mid-stream: drain the recovering `ReadyForQuery`,
                // then surface the parsed cause. Connection stays alive + pooled.
                self.drain_to_idle(live).await?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(Stop::User(e)) => {
                // Caller broke early: drain to reclaim, then report the stop value.
                self.drain_to_idle(live).await?;
                if oversize {
                    return Err(DriverError::OversizeRow);
                }
                Ok(Some(e))
            }
            Boundary::Stopped(Stop::Decode(de)) => {
                // A per-row decode failure broke the stream: drain to reclaim, then
                // surface the loud classified decode error.
                self.drain_to_idle(live).await?;
                Err(DriverError::Decode(de))
            }
            // `query_params_break` maps Closed/Suspended to a fatal `Err`, so they
            // never ride an `Ok` outcome; `Boundary` is `#[non_exhaustive]`, so
            // this classified arm also covers any future boundary. The token is
            // dropped (not restored), so the connection is left dead + evictable.
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a streaming query",
            ))),
        }
    }

    /// Drain a connection left DIRTY by an early stop of a streaming query/unload
    /// to a clean idle boundary, restoring the token. Sends nothing (the request
    /// was already flushed). A fatal fault during the drain kills the connection
    /// (the token is consumed), never swallowed.
    async fn drain_to_idle(&mut self, live: Live<'static>) -> Result<(), DriverError> {
        // Thread the capture adapter through the reclaim: a NOTIFY riding the
        // drained remainder is buffered (or shed-counted at overflow), never
        // silently dropped.
        let outcome = self
            .engine
            .drain(
                live,
                capture_notify(&mut self.notifications, |_s: Surface<'_>| {
                    ControlFlow::Continue(())
                }),
            )
            .await;
        match outcome {
            // The drain reached a clean idle — its own status is irrelevant, so
            // only the token matters. Restore it.
            Ok(Outcome { live, .. }) => {
                self.live = Some(live);
                Ok(())
            }
            Err(other) => Err(lift_engine_error(other)),
        }
    }

    // ── Transaction / session boundary primitives ───────────────────────────

    /// Arm a DEFERRED `BEGIN`: it is not sent now, but fused into the flush of the
    /// first command the transaction body issues (so `BEGIN` + that statement ride
    /// ONE round trip instead of two). Used by each driver's `transaction`; the
    /// engine drains the fused `BEGIN`'s reply before the statement's, and if the
    /// body issues no command the following `COMMIT` / `ROLLBACK` flushes the still
    /// -pending `BEGIN`. A field-set only, no I/O and no token.
    #[inline]
    pub fn defer_begin(&mut self) {
        self.engine.defer_command_prelude("BEGIN");
    }

    /// `BEGIN` a transaction.
    pub async fn begin(&mut self) -> Result<(), DriverError> {
        self.simple_query("BEGIN").await?;
        Ok(())
    }

    /// `COMMIT` the current transaction (a logical-operation boundary: the N+1
    /// recency window is forgotten under `n1-detect`).
    pub async fn commit(&mut self) -> Result<(), DriverError> {
        self.simple_query("COMMIT").await?;
        self.n1_reset();
        Ok(())
    }

    /// `ROLLBACK` the current transaction (a logical-operation boundary).
    pub async fn rollback(&mut self) -> Result<(), DriverError> {
        self.simple_query("ROLLBACK").await?;
        self.n1_reset();
        Ok(())
    }

    /// Subscribe to a `LISTEN` channel. The name is validated into a
    /// [`SafeIdent`] — the injection-safe type the SQL is assembled from — so an
    /// injection-shaped name is a classified [`DriverError::Config`] and CANNOT
    /// reach the interpolated SQL. The `SafeIdent` (not a raw `&str`) is the
    /// splice currency, so the "cannot inject" guarantee is structural: the type
    /// is the proof.
    pub async fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        let sql = sql_ident::listen_sql(SafeIdent::validate(channel)?);
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .simple_query(
                live,
                &sql,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)
    }

    /// Unsubscribe from a `LISTEN` channel. The name is validated into a
    /// [`SafeIdent`] before interpolation (see [`listen`](Self::listen)).
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        let sql = sql_ident::unlisten_sql(SafeIdent::validate(channel)?);
        self.simple_query(&sql).await?;
        Ok(())
    }

    /// Reset all BLEEDABLE session state so this connection can be safely reused
    /// by a different logical user, WITHOUT dropping prepared statements.
    ///
    /// Runs `DISCARD ALL` MINUS `DEALLOCATE ALL` / `DISCARD PLANS` in one
    /// simple-query round trip (prefixed with `ROLLBACK` only when inside a
    /// transaction, decided from the cached `ReadyForQuery` tx status so the
    /// common idle path costs no extra round trip), then clears the notification
    /// ledger and the N+1 recency window. Prepared statements — content-addressed
    /// plans safe to share across logical users — are deliberately KEPT so the
    /// server-side plan reuse survives a pool checkout with NO cache
    /// invalidation. See each driver's `reset_session` for the full rationale.
    pub async fn reset_session(&mut self) -> Result<(), DriverError> {
        const RESET: &str = "SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; \
             UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        const RESET_WITH_ROLLBACK: &str =
            "ROLLBACK; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; \
             CLOSE ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        // Discard any fused prelude stranded by a transaction body that PANICKED
        // before issuing a statement (its COMMIT/ROLLBACK never ran to consume the
        // deferred BEGIN). Cleared BEFORE the reset's own simple query so the
        // stranded BEGIN cannot fuse into — and error out — the RESET (a `DISCARD`
        // inside a transaction block is a server error). Without this a stranded
        // BEGIN would make the reset fail; the pool would still self-heal (evict +
        // reconnect), but at the cost of a wasted connection and a failed round
        // trip. A no-op in the normal path (no prelude is pending at checkout).
        self.engine.clear_command_prelude();
        let sql = if matches!(self.engine.tx_status(), Ok(TxStatus::Idle)) {
            RESET
        } else {
            RESET_WITH_ROLLBACK
        };
        self.simple_query(sql).await?;
        // Clear the ledger AFTER the reset round trip: `UNLISTEN *` stops new
        // notifications, and this discards every notification captured before or
        // during the reset — so a pooled connection never delivers a prior user's
        // notifications to the next. Done last so a notification that rode the
        // reset's own response stream is cleared too.
        self.notifications.clear();
        // A pool session reset is the strongest logical-operation boundary.
        self.n1_reset();
        Ok(())
    }

    // ── COPY OUT (bulk unload) ──────────────────────────────────────────────

    /// `COPY <table> TO STDOUT`, streaming each row to `on_chunk` in CONSTANT
    /// memory. `table` is validated as an identifier BEFORE interpolation. See
    /// each driver's `copy_out` for the full return-value / early-abort contract.
    pub async fn copy_out<F, E>(
        &mut self,
        table: &str,
        mut on_chunk: F,
    ) -> Result<Option<E>, DriverError>
    where
        F: for<'q> FnMut(&'q [u8]) -> ControlFlow<E>,
    {
        let sql = sql_ident::copy_out_sql(SafeTable::validate(table)?);
        let live = self.take_live()?;
        let mut db_error: Option<DbError> = None;
        let outcome = self
            .engine
            .copy_out(
                live,
                &sql,
                capture_notify(&mut self.notifications, |surface| match surface {
                    // The chunk borrows the transient ingest buffer; `on_chunk`
                    // consumes it in-scope (the `for<'q>` wall forbids escape).
                    Surface::CopyData(body) => on_chunk(body),
                    Surface::Fail(body) => {
                        db_error = Some(materialize::parse_error_response(body));
                        ControlFlow::Continue(())
                    }
                    _ => ControlFlow::Continue(()),
                }),
            )
            .await;

        let (live, boundary) = match outcome {
            Ok(Outcome { live, status }) => (live, status),
            Err(other) => return Err(lift_engine_error(other)),
        };
        match boundary {
            Boundary::Idle => {
                self.live = Some(live);
                Ok(None)
            }
            Boundary::Failed => {
                self.drain_to_idle(live).await?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(e) => {
                self.drain_to_idle(live).await?;
                Ok(Some(e))
            }
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a streaming COPY OUT",
            ))),
        }
    }

    // ── COPY IN seam (the per-driver `copy_in_with` orchestrates these) ──────

    /// Begin `COPY <table> FROM STDIN`: validate `table` into a [`SafeTable`],
    /// assemble the injection-safe SQL, and issue the COPY. This is the SINGLE
    /// splice site for the COPY-in table name — both drivers' `copy_in_with`
    /// route through it, so the table identifier is validated in exactly one
    /// place and an injection-shaped name is a classified [`DriverError::Config`]
    /// that never reaches the wire. `#[doc(hidden)]`: the per-driver
    /// `copy_in_with` seam, not a public verb.
    #[doc(hidden)]
    pub async fn copy_in_begin_table(
        &mut self,
        table: &str,
    ) -> Result<Live<'static>, DriverError> {
        let sql = sql_ident::copy_in_sql(SafeTable::validate(table)?);
        self.copy_in_begin(&sql).await
    }

    /// Begin `COPY <sql> FROM STDIN` from an already-assembled statement: take
    /// the liveness token, issue the COPY command, and hand the token BACK to the
    /// caller to hold across the (token-less) streaming writes. On a transport
    /// fault the token is dropped — the connection is dead. Takes the full SQL
    /// (the table splice is the caller's responsibility via
    /// [`copy_in_begin_table`](Self::copy_in_begin_table), the single validated
    /// entry). `#[doc(hidden)]`.
    #[doc(hidden)]
    pub async fn copy_in_begin(&mut self, sql: &str) -> Result<Live<'static>, DriverError> {
        let live = self.take_live()?;
        match self.engine.copy_in_begin(sql).await {
            Ok(()) => Ok(live),
            Err(e) => Err(lift_engine_error(e)),
        }
    }

    /// Stream one `CopyData` frame for an open COPY-in and flush it. Token-less
    /// (the caller holds the token across writes). `#[doc(hidden)]`: driven by the
    /// per-driver `CopyInWriter`.
    #[doc(hidden)]
    pub async fn copy_in_write(&mut self, chunk: &[u8]) -> Result<(), DriverError> {
        self.engine
            .copy_in_write(chunk)
            .await
            .map_err(lift_engine_error)
    }

    /// Finish an open COPY-in cleanly (`CopyDone`), settle, and return the
    /// server's affected-row count. `#[doc(hidden)]`: the per-driver
    /// `copy_in_with` terminal-success step.
    #[doc(hidden)]
    pub async fn copy_in_finish(&mut self, live: Live<'static>) -> Result<u64, DriverError> {
        let mut collector = ResultCollector::new();
        let outcome = self
            .engine
            .copy_in_finish(
                live,
                capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)?;
        Ok(collector.affected())
    }

    /// Abort an open COPY-in (`CopyFail`) and reclaim the connection. The server
    /// ALWAYS answers `CopyFail` with an `ErrorResponse` + `ReadyForQuery`, so the
    /// abort's `ServerErrored` status is EXPECTED — the token is restored. A
    /// transport fault during the abort leaves the token gone (dead). The caller's
    /// own error dominates, so nothing is returned. `#[doc(hidden)]`: the
    /// per-driver `copy_in_with` terminal-abort step.
    #[doc(hidden)]
    pub async fn copy_in_abort(&mut self, live: Live<'static>) {
        if let Ok(Outcome { live, .. }) = self
            .engine
            .copy_in_abort(
                live,
                b"client aborted COPY",
                capture_notify(&mut self.notifications, |_s: Surface<'_>| ControlFlow::Continue(())),
            )
            .await
        {
            self.live = Some(live);
        }
    }

    // ── Notification seam (the per-driver `recv_notification` orchestrates) ──

    /// Drain the front of the notification ledger, if any already arrived.
    /// `#[doc(hidden)]`: the per-driver `recv_notification`'s no-round-trip fast
    /// path.
    #[doc(hidden)]
    #[must_use]
    pub fn drain_one_notification(&mut self) -> Option<Result<Notification, DriverError>> {
        self.notifications.drain_one()
    }

    /// Wait for the next asynchronous notification, capturing it into the ledger.
    /// Returns `true` iff a notification was buffered (`Received`), `false` on a
    /// quiet boundary / deadline.
    ///
    /// The token- and classification-managing CORE of `recv_notification`, shared
    /// by both drivers; the per-driver `recv_notification` wraps this with its own
    /// deadline arming (a shared read-deadline cell on the async socket, a socket
    /// read-timeout on the blocking one) around the call, then — on `true` —
    /// drains the buffered notification via [`take_expected_notification`].
    /// Deliberately does NOT drain here: the sync driver must restore its (fallible)
    /// socket read-timeout BEFORE draining, so a restore failure leaves the
    /// notification buffered (recoverable), never lost. `#[doc(hidden)]`.
    ///
    /// The deadline surfaces inside the engine (via [`Transport::is_would_block`])
    /// as the [`NotifyStatus::Quiet`] outcome — the token rides back in `Ok`, so
    /// the connection stays alive with no separate reclaim.
    ///
    /// [`take_expected_notification`]: Self::take_expected_notification
    #[doc(hidden)]
    pub async fn recv_notification_inner(&mut self) -> Result<bool, DriverError> {
        let live = self.take_live()?;
        let ledger = &mut self.notifications;
        let outcome = self
            .engine
            .recv_notification(live, |s| {
                if let Surface::Notify(body) = s {
                    // Capture into the ledger (the same buffer every verb feeds),
                    // then stop the pump — the notification is what we waited for.
                    ledger.capture(body);
                    return ControlFlow::Break(());
                }
                ControlFlow::Continue(())
            })
            .await;
        match outcome {
            Ok(Outcome { live, status }) => {
                // Alive on either status — the would-block deadline is the Quiet
                // outcome, handled inside the engine, so the token rides back.
                self.live = Some(live);
                Ok(matches!(status, NotifyStatus::Received))
            }
            Err(other) => Err(lift_engine_error(other)),
        }
    }

    /// Drain the notification the sink just buffered on a `Received` outcome.
    ///
    /// Called by the per-driver `recv_notification` AFTER it restores its deadline,
    /// so a restore failure never loses the notification. An empty ledger here is a
    /// classified inconsistency ([`DriverError::UnclassifiedFailure`]), never a
    /// silent `None`. `#[doc(hidden)]`.
    #[doc(hidden)]
    pub fn take_expected_notification(&mut self) -> Result<Option<Notification>, DriverError> {
        match self.notifications.drain_one() {
            Some(res) => res.map(Some),
            None => Err(DriverError::UnclassifiedFailure),
        }
    }

    // ── Lifecycle + accessors ───────────────────────────────────────────────

    /// Gracefully close the connection (`Terminate` + shutdown). Idempotent — a
    /// second call with no live token is `Ok(())`.
    pub async fn close(&mut self) -> Result<(), DriverError> {
        match self.live.take() {
            Some(live) => self.engine.terminate(live).await.map_err(lift_engine_error),
            None => Ok(()),
        }
    }

    /// Whether the connection is at a clean boundary and reusable.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.live.is_some()
    }

    /// Whether this connection's traffic is TLS-encrypted (snapshotted at connect).
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }

    /// The server version reported at connect, if recovered.
    #[must_use]
    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }

    /// The backend process id from `BackendKeyData`.
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// The count of asynchronous notifications currently buffered in the ledger.
    #[must_use]
    pub fn buffered_notifications(&self) -> usize {
        self.notifications.len()
    }

    /// The total number of asynchronous notifications ever captured (monotonic).
    #[must_use]
    pub fn notifications_received(&self) -> u64 {
        self.notifications.received()
    }

    /// The number of asynchronous notifications SHED because the bounded ledger
    /// was full (monotonic).
    #[must_use]
    pub fn notifications_shed(&self) -> u64 {
        self.notifications.shed()
    }

    // ── N+1 detector (diagnostics-only; compiled out when off) ──────────────

    /// Feed one typed-verb execution to the N+1 detector (diagnostics-only).
    #[cfg(feature = "n1-detect")]
    fn n1_record(&mut self, sql: &'static str, caller: &'static core::panic::Location<'static>) {
        self.n1_tracker.record(sql, caller);
    }

    /// Forget the N+1 recency window at a logical-operation boundary. A no-op with
    /// zero footprint when the feature is off (the whole call vanishes), so a
    /// per-driver `transaction` can call it unconditionally. `#[doc(hidden)]`.
    #[doc(hidden)]
    #[inline]
    pub fn n1_reset(&mut self) {
        #[cfg(feature = "n1-detect")]
        self.n1_tracker.reset();
    }

    /// The N+1 anti-patterns detected on this connection so far. Present ONLY under
    /// the `n1-detect` feature. Purely diagnostic — the driver builds this ledger
    /// as a side effect of the typed verbs and never acts on it.
    #[cfg(feature = "n1-detect")]
    #[must_use]
    pub fn n1_report(&self) -> &[crate::N1Report] {
        self.n1_tracker.report()
    }
}

// ── Error lifting (concrete; shared by the drivers' connect + verbs) ────────

/// Lift a FATAL [`EngineError`] over the wire transport to a classified
/// [`DriverError`]. A recoverable server error never reaches here — the verb
/// returns it as [`CommandStatus::ServerErrored`] inside `Ok`, which `Core`'s
/// internal settle step maps to `DriverError::Db`.
#[doc(hidden)]
#[must_use]
pub fn lift_engine_error(e: EngineError<WireError>) -> DriverError {
    match e {
        EngineError::Transport(t) => lift_tls_error(t),
        EngineError::Handshake(cf) => lift_conn_fail(cf),
        EngineError::WrongPhase(_) => DriverError::NotReady,
        EngineError::UnexpectedEof => {
            DriverError::Io(io::Error::other("server closed the connection"))
        }
        EngineError::ServerError => DriverError::UnclassifiedFailure,
        EngineError::ProtocolViolation => {
            DriverError::Io(io::Error::other("protocol violation; connection torn down"))
        }
        // `EngineError` is `#[non_exhaustive]`; the remaining framing/flush faults
        // surface as classified I/O carrying the engine's own detail.
        other => DriverError::Io(io::Error::other(format!("engine error: {other:?}"))),
    }
}

/// Lift a wire transport error to a [`DriverError`]. A would-block / timed-out
/// socket error is a deadline, mapped to [`DriverError::Timeout`]; every other
/// class keeps its detail.
#[doc(hidden)]
#[must_use]
pub fn lift_tls_error(e: WireError) -> DriverError {
    match e {
        TlsError::Socket(io) => match io.kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => DriverError::Timeout,
            _ => DriverError::Io(io),
        },
        // Preserve the TLS error verbatim as the source of a classified I/O error.
        // Only the `Socket` arm exists with `tls` off (every TLS-protocol variant
        // is `tls`-gated on `TlsError`), so this catch-all is reachable — and
        // needed — only under `tls`.
        #[cfg(feature = "tls")]
        other => DriverError::Io(io::Error::other(other)),
    }
}

/// Lift a custom-CA-roots build failure to a classified [`DriverError::Config`] —
/// fail-closed: a bad or empty CA PEM aborts the connect, never a silent fallback
/// to the default roots. `#[doc(hidden)]`: used by each driver's connect. Present
/// only under `tls`: `CaRootsError` names a rustls parse failure.
#[cfg(feature = "tls")]
#[doc(hidden)]
#[must_use]
pub fn lift_ca_roots_error(e: CaRootsError) -> DriverError {
    match e {
        CaRootsError::NoCertificates => DriverError::Config(
            "custom CA roots (with_ca_roots/sslrootcert) contained no certificate",
        ),
        CaRootsError::MalformedPem(_) => DriverError::Config("custom CA roots PEM is malformed"),
        CaRootsError::InvalidCertificate(_) => {
            DriverError::Config("a custom CA certificate is not a valid trust anchor")
        }
        CaRootsError::ProtocolVersions(_) => {
            DriverError::Config("TLS provider advertised no usable protocol versions")
        }
        // Matched exhaustively (same crate as `CaRootsError`): a new rejection class
        // is a loud compile error forcing an explicit fail-closed decision here,
        // stronger than a wildcard that would silently swallow it. The consumer's
        // `#[non_exhaustive]` still applies cross-crate.
    }
}

/// Lift a handshake failure to a [`DriverError`]. `#[doc(hidden)]`: used by each
/// driver's connect.
#[doc(hidden)]
#[must_use]
pub fn lift_conn_fail(cf: ConnFail) -> DriverError {
    match cf {
        ConnFail::UnsupportedAuthMethod => {
            DriverError::Config("server requested an unsupported authentication method")
        }
        ConnFail::ServerError => {
            DriverError::Io(io::Error::other("server returned an error during startup"))
        }
        // `ConnFail` is `#[non_exhaustive]`; the malformed-frame / SCRAM / overflow
        // causes surface as I/O carrying the classified detail.
        other => DriverError::Io(io::Error::other(format!("handshake failed: {other:?}"))),
    }
}

#[cfg(test)]
mod stmt_name_render_tests {
    //! The generated prepared-statement name is load-bearing: a wrong render
    //! would break every prepared query. These pin the exact `_bsql_<id>` shape
    //! the old `format!` produced, now stack-rendered with no heap allocation —
    //! across the `u32` extremes (0, 1, and `u32::MAX`, the 10-digit boundary
    //! the 16-byte capacity is sized for).
    use super::StmtNameBuf;
    use core::fmt::Write as _;

    fn render(id: u32) -> String {
        let mut buf = StmtNameBuf::new();
        write!(buf, "_bsql_{id}").expect("_bsql_<u32> always fits the 16-byte buffer");
        buf.as_str()
            .expect("the rendered bytes are valid ASCII")
            .to_string()
    }

    #[test]
    fn renders_the_bsql_prefixed_decimal_name() {
        assert_eq!(render(0), "_bsql_0");
        assert_eq!(render(1), "_bsql_1");
        assert_eq!(render(42), "_bsql_42");
        // u32::MAX is 10 digits — the widest name (6 + 10 = 16 = capacity).
        assert_eq!(render(u32::MAX), "_bsql_4294967295");
    }
}
