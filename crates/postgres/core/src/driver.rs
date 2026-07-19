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
use core::future::Future;
use core::ops::ControlFlow;
use core::pin::Pin;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Instant;

use bsql_postgres_proto::engine::{
    Boundary, CommandStatus, ConnFail, Engine, EngineError, Live, NotifyStatus, Outcome,
    PreparedStatement as WireStatement, Surface, Transport,
};
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    DecodeError, Sensitive, StmtName, TxStatus, TypedCopyIn, TypedQuery, PGCOPY_BINARY_HEADER,
    PGCOPY_BINARY_TRAILER,
};

use crate::cancel::CancelKey;
use crate::materialize::{self, ResultCollector};
use crate::pipeline::{Bound, Pipeline};
use crate::sql_ident::{self, SafeIdent, SafeTable};
use crate::types::ColSlot;
use crate::BorrowedRow;
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

/// Parse one whole `DataRow` `body` into a zero-copy [`BorrowedRow`] over the
/// REUSED `slots` table and hand it to `on_row`, translating the outcome into the
/// engine's [`Stop`] break payload.
///
/// A per-row decode failure is LOUD ([`Stop::Decode`] stops the pump), never a
/// Continue past it or a substituted default; a caller [`Break`](ControlFlow::Break)
/// rides [`Stop::User`]. Shared by the inline and the reassembled-oversize arms of
/// [`stream_dynamic_row`], so the two decode a row identically.
fn decode_and_hand<F, E>(
    body: &[u8],
    on_row: &mut F,
    slots: &mut Vec<ColSlot>,
) -> ControlFlow<Stop<E>>
where
    F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
{
    match BorrowedRow::parse(body, slots) {
        Ok(view) => match on_row(view) {
            ControlFlow::Continue(()) => ControlFlow::Continue(()),
            ControlFlow::Break(e) => ControlFlow::Break(Stop::User(e)),
        },
        Err(de) => ControlFlow::Break(Stop::Decode(de)),
    }
}

/// The shared per-surface sink body for BOTH dynamic streaming verbs
/// ([`Core::query_each_raw`] / [`Core::query_each_params`]): lend each
/// `Surface::Row` (or a reassembled oversize row) to `on_row` as a zero-copy
/// [`BorrowedRow`], capturing a mid-stream server error and swallowing async /
/// COPY frames.
///
/// Defined ONCE so the two verbs cannot drift in their per-row decode. The scratch
/// buffers (`slots`, `oversize`) are owned by the verb and reused per row, so this
/// accumulates NOTHING — the constant-memory invariant.
fn stream_dynamic_row<F, E>(
    surface: Surface<'_>,
    on_row: &mut F,
    slots: &mut Vec<ColSlot>,
    oversize: &mut Vec<u8>,
    db_error: &mut Option<DbError>,
) -> ControlFlow<Stop<E>>
where
    F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
{
    match surface {
        // A whole inline row: parse its cell offsets into the reused slot table
        // and lend a zero-copy view.
        Surface::Row(body) => decode_and_hand(body, on_row, slots),
        // Capture the server error's cause, then let the pump reach its `Failed`
        // boundary so the connection can be drained to idle.
        Surface::Fail(body) => {
            *db_error = Some(materialize::parse_error_response(body));
            ControlFlow::Continue(())
        }
        // An oversize row streams as `RowChunk` pieces: reassemble into the reused
        // buffer (bounded by the widest oversize row, not the whole result).
        Surface::RowChunk(bytes) => {
            oversize.extend_from_slice(bytes);
            ControlFlow::Continue(())
        }
        // The reassembled oversize row is complete: decode it exactly as an inline
        // row, then clear the buffer to reuse its allocation for the next one (the
        // view's borrow ends when `decode_and_hand` returns, before the clear).
        Surface::RowChunkEnd => {
            let flow = decode_and_hand(oversize, on_row, slots);
            oversize.clear();
            flow
        }
        // COPY / delivery / other async frames are not stream rows (a NOTIFY is
        // captured into the ledger by the `capture_notify` wrapper above this).
        _ => ControlFlow::Continue(()),
    }
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
/// It also retains `param_oids` — the parameter-type OIDs the SERVER inferred for
/// the statement's `$N` placeholders (from the prepare's `ParameterDescription`).
/// A prepared statement has a FIXED plan, so the server cannot coerce a
/// differently-typed binary `Bind` against it; [`query_prepared`](Core::query_prepared)
/// / [`execute_prepared`](Core::execute_prepared) therefore VERIFY the caller's
/// encoded parameter types against these BEFORE binding, rejecting a mismatch
/// with a classified [`DriverError::ParamTypeMismatch`] rather than letting the
/// server silently reinterpret the bytes.
///
/// [`close_statement`]: Core::close_statement
#[derive(Debug)]
pub struct PreparedStatement {
    inner: WireStatement,
    column_names: Arc<[String]>,
    /// Server-inferred parameter-type OIDs (`$1..$n` order), retained for the
    /// pre-`Bind` type verification. Empty when the server reported none.
    param_oids: Box<[u32]>,
    /// The process-unique identity ([`Core::conn_id`]) of the connection that
    /// minted this statement. A `PreparedStatement` names a server-side statement
    /// (`_bsql_<n>`) whose plan lives ONLY on that connection, so every prepared
    /// verb checks this against the connection's own id BEFORE any wire I/O — a
    /// mismatch is [`DriverError::WrongConnection`], never a silent bind against a
    /// like-named statement on a foreign connection. Stamped in
    /// [`prepare_with_oids`](Core::prepare_with_oids).
    origin: u64,
}

impl PreparedStatement {
    /// Verify the caller's encoded parameter types (`<P as ParamsWriter>::OIDS`)
    /// against the server-inferred types this statement was prepared with.
    ///
    /// A prepared statement's parameter types are FIXED at `Parse`; the server
    /// reads each `Bind` value AS the inferred type with no coercion, so a
    /// wrong-typed binary bind of the same wire width is silently reinterpreted.
    /// This closes that hole client-side: an arity or a per-parameter type
    /// disagreement is a classified error returned BEFORE the `Bind`, so no wire
    /// round trip is spent and the connection is untouched.
    ///
    /// STRICT EQUALITY (not the dynamic path's server-side coercion): a fixed plan
    /// cannot coerce, so anything but an exact OID match (or an `unspecified` `0`
    /// on either side — unverifiable, passed through) is a real mismatch.
    fn verify_params<P: ParamsWriter>(&self) -> Result<(), DriverError> {
        let declared = P::OIDS;
        if declared.len() != self.param_oids.len() {
            return Err(DriverError::ParamCountMismatch {
                expected: self.param_oids.len(),
                found: declared.len(),
            });
        }
        for (index, (&expected, &found)) in self.param_oids.iter().zip(declared.iter()).enumerate() {
            // `0` = `unspecified` on either side (an `EnumLabel` param the client
            // leaves to inference, or a param the server could not infer): not a
            // type, so not verifiable — pass through rather than falsely reject.
            if expected != 0 && found != 0 && expected != found {
                return Err(DriverError::ParamTypeMismatch { index, expected, found });
            }
        }
        Ok(())
    }
}

/// Capacity of the per-connection dynamic prepared-statement cache — the most
/// distinct hot dynamic SQL strings whose server-side plan is retained for
/// reuse. Bounded so a churn of one-shot SQL cannot grow the cache without
/// limit; a workload with MORE than this many hot dynamic queries leaves the
/// overflow on the (still 1-round-trip) fused path — no regression, just no
/// plan-reuse for the overflow. 32 covers the common handful of hot dynamic
/// queries with headroom.
const DYN_STMT_CACHE_CAP: usize = 32;

/// Process-global monotonic source of per-connection identity ids.
///
/// [`Core::new`] mints one id per connection with a single relaxed `fetch_add`,
/// stamping it onto every [`PreparedStatement`] the connection prepares. A `u64`
/// incremented ONCE per connection cannot realistically wrap (2^64 connections),
/// so every live `Core` in the process holds a DISTINCT id — the structural basis
/// for [`DriverError::WrongConnection`], rejecting a statement handle used on a
/// connection other than the one that minted it. Relaxed is sufficient: the RMW
/// is atomic (each caller reads a unique value from the single modification
/// order); no cross-thread ordering of other memory is implied or needed.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(0);

/// The `execute_batch` send-window batcher threshold, in bytes.
///
/// A homogeneous batch streams its `Bind`+`Execute` frames onto the send buffer and
/// flushes (with a `Flush`, then drains the window's responses) once the pending
/// bytes cross this, so the staged-bytes high-water is bounded regardless of N —
/// constant send memory. 64 KiB matches `copy_in`'s
/// `COPY_IN_FLUSH_THRESHOLD` (a typical socket send buffer): a batch whose commands
/// fit one window is exactly one round trip; a huge N pays ~`N / window` windows,
/// each a request→`Flush`→drain that cannot deadlock (unlike a single unbounded
/// flush against a server that answers per command).
const BATCH_WINDOW_THRESHOLD: usize = 64 * 1024;

/// The outcome of driving ONE window of a batch to its boundary
/// ([`Core::flush_window`]) — the shared, verb-agnostic classification the three
/// windowed drives (`pipeline` / `execute_batch` / `query_batch`) match on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowStep {
    /// The window drained cleanly to its inter-command boundary (the sink broke at
    /// the window's delivery target). Continue staging the next window.
    Drained,
    /// The window did NOT drain cleanly — a server `ErrorResponse` was parked, or a
    /// guarded window's result-OID mismatch BAILED (`Boundary::Failed`), or an
    /// unexpected non-`Idle` boundary (fail-closed). Stop staging; the caller
    /// breaks to the trailing `Sync` + final drain, and the settle classifies which.
    Halt,
}

/// Route ONE drained surface of a TYPED-result window (`pipeline` / `query_batch`)
/// to its command's [`RowsBuilder`], advancing the delivered-command cursor and
/// BREAKING once the window's delivery `target` is reached — the SHARED window
/// sink both the normal-window flush and the oversize-isolate prefix flush thread
/// through (so the collector + break logic exists ONCE, not duplicated per flush
/// call site). Rows (whole or reassembled-oversize chunks) feed the CURRENT
/// command's builder; a `Deliver` finalizes it, advances `current`, and breaks at
/// `target`; the FIRST `Fail` parks the failing command's zero-based index + cause
/// (the trailing `Sync` recovers the connection, the settle classifies it). Break
/// payload `()` — a breakable WINDOW drive (the final `Sync` drive keeps its own
/// non-breaking `Never` sink).
fn feed_typed_window(
    surface: Surface<'_>,
    current: &mut usize,
    target: usize,
    builders: &mut [RowsBuilder],
    failed_index: &mut Option<usize>,
    db_error: &mut Option<DbError>,
) -> ControlFlow<()> {
    match surface {
        Surface::Row(_) | Surface::RowChunk(_) | Surface::RowChunkEnd => {
            if let Some(b) = builders.get_mut(*current) {
                b.feed(surface);
            }
            ControlFlow::Continue(())
        }
        Surface::Deliver { .. } => {
            if let Some(b) = builders.get_mut(*current) {
                b.feed(surface);
            }
            *current = current.saturating_add(1);
            if *current >= target {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
        Surface::Fail(body) if failed_index.is_none() => {
            *failed_index = Some(*current);
            *db_error = Some(materialize::parse_error_response(body));
            ControlFlow::Continue(())
        }
        _ => ControlFlow::Continue(()),
    }
}

/// Route ONE drained surface of a COUNT-result window (`execute_batch`, which
/// reads affected COUNTS and discards its RETURNING rows) — the count peer of
/// [`feed_typed_window`]. A `Deliver` pushes the command tag's affected count,
/// advances `current`, and breaks at the window's `target`; the FIRST `Fail`
/// parks the failing index + cause. Break payload `()` (a breakable WINDOW drive).
fn feed_count_window(
    surface: Surface<'_>,
    current: &mut usize,
    target: usize,
    affected: &mut Vec<u64>,
    failed_index: &mut Option<usize>,
    db_error: &mut Option<DbError>,
) -> ControlFlow<()> {
    match surface {
        Surface::Deliver { tag, .. } => {
            // A tagless extended-protocol boundary has no row count (0); a
            // `CommandComplete` tag projects its own affected-row count.
            let n = match tag {
                Some(t) => t.rows_or_zero(),
                None => 0,
            };
            affected.push(n);
            *current = current.saturating_add(1);
            if *current >= target {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
        Surface::Fail(body) if failed_index.is_none() => {
            *failed_index = Some(*current);
            *db_error = Some(materialize::parse_error_response(body));
            ControlFlow::Continue(())
        }
        _ => ControlFlow::Continue(()),
    }
}

/// One entry in the [`DynStmtCache`]: the (SQL text, parameter-type OIDs) key
/// plus, once the query has been prepared, its server-side prepared statement.
#[derive(Debug)]
struct DynSlot {
    /// The runtime SQL text this slot caches (half the cache key).
    sql: Box<str>,
    /// The parameter-type OIDs (`<P as ParamsWriter>::OIDS`) the cached plan was
    /// PREPARED with — the OTHER half of the key. A REUSE requires BOTH the SQL
    /// text AND these OIDs to match, so a plan prepared for one parameter-type
    /// tuple is NEVER reused for a different-typed bind of the same SQL text. That
    /// would bind the new value's binary bytes to be decoded AS the prepared
    /// type — a SILENT reinterpretation for two types of the same wire width
    /// (int4/float4/date; int8/float8/timestamp), the exact hole the declared-OID
    /// `Parse` closes on the FIRST sighting and this key closes on REUSE. Stored
    /// as `&'static` (it points at the baked `const OIDS` array — no allocation).
    param_oids: &'static [u32],
    /// `None` = PENDING: the SQL has been seen ONCE (run through the fused,
    /// unnamed 1-round-trip path) and is queued to prepare on its NEXT sighting.
    /// `Some` = READY: a named server-side statement to `Bind`+`Execute` (plan
    /// reuse, no re-parse).
    prepared: Option<PreparedStatement>,
}

/// A bounded per-connection cache of DYNAMIC prepared statements, keyed on the
/// (SQL text, parameter-type OIDs) pair — driver-level plan reuse for the runtime
/// `query_params` / `execute_params` family, the dynamic peer of the engine's
/// compile-checked (typed) statement cache.
///
/// # Why the key includes the parameter-type OIDs, not just the SQL text
///
/// A cached plan is prepared with a specific parameter-type tuple (its `Parse`
/// declares `<P as ParamsWriter>::OIDS`), and a `Bind` sends each value's binary
/// bytes to be decoded AS that prepared type. Keying on SQL text ALONE would let
/// the same SQL string bound with a DIFFERENT Rust parameter type reuse the plan,
/// binding the new value's bytes to be reinterpreted as the prepared type — a
/// SILENT wrong value for two types of the same wire width (`int4`/`float4`/`date`;
/// `int8`/`float8`/`timestamp`). Including `P::OIDS` in the key makes such a call a
/// DISTINCT cache entry (its own plan, prepared for its own types), so a reuse
/// never crosses parameter types — the reuse-path peer of the first-sighting
/// declared-OID `Parse`. (An `EnumLabel`'s `unspecified` OID `0` is shared across
/// enum types, but the same SQL resolves the same enum from context, so a `0`-OID
/// reuse is type-consistent by construction.)
///
/// # Why prepare on the SECOND sighting, not the first
///
/// The fused unnamed path already runs a one-shot dynamic query in ONE round
/// trip (`Parse`+`Bind`+`Describe`+`Execute`+`Sync`), so preparing a NAMED
/// statement on the FIRST sighting — which needs a separate `prepare` round trip
/// before the first `Bind`+`Execute` — would REGRESS a genuinely one-shot query
/// from one round trip to two. So a first sighting stays on the fused path and
/// is only NOTED (`prepared = None`); the SECOND sighting prepares the named
/// statement (a one-time extra round trip) and every LATER sighting reuses the
/// server-side plan in one round trip (`Bind`+`Execute`+`Sync`, no re-parse) —
/// strictly better than fused for a repeated query (same round trip, no
/// server-side re-parse / re-plan). A query run exactly once therefore pays
/// nothing; a query run in a loop amortizes the single prepare to zero.
///
/// # Bounded, leak-free
///
/// At most [`DYN_STMT_CACHE_CAP`] slots. A first sighting evicts the OLDEST
/// PENDING slot if the cache is full — a pending slot holds NO server-side
/// statement, so eviction is free (never a leaked prepared statement). A READY
/// slot is NEVER evicted (only reclaimed by the reuse path's self-heal, which
/// `Close`s it), so the server-side statement count is bounded by the cache and
/// nothing leaks; on connection close PostgreSQL drops them all.
///
/// # Cleared on a session reset
///
/// The cache is CLEARED by [`reset_session`](Core::reset_session) — the SINGLE
/// reset used both by a direct consumer and by the pool at checkout — which
/// `Close`s each READY server-side statement (a protocol `Close` of an
/// already-dropped statement is a wire no-op, so it is robust even after a
/// mid-session `DISCARD` / `DEALLOCATE`) batched into ONE round trip.
///
/// Clearing this cache at the pool boundary is a CORRECTNESS requirement, not a
/// hygiene nicety — and the SAME rule applies to the engine's compile-checked
/// (TYPED) cache, which the reset ALSO drops (the ONE RULE: a statement cache never
/// crosses a checkout). A prepared plan resolves its relation NAMES once, at `Parse`
/// — a dynamic plan against an UNQUALIFIED name (the search path) or a SESSION
/// object, a typed plan against its migration table — so a plan a prior logical user
/// promoted (e.g. `SELECT … FROM orders …` bound to `public.orders`) must NOT survive
/// into the next user's checkout: keeping it warm would let a next user who creates a
/// shadowing `CREATE TEMP TABLE orders` (with `pg_temp` already active, so the
/// search-path OID list is unchanged) receive the PRIOR user's `public.orders` rows
/// on a cache HIT — a silent cross-user wrong result (a tenant-boundary leak).
/// `DISCARD PLANS` cannot fix this robustly (it invalidates a kept plan only ONCE,
/// and PostgreSQL re-validates it against the pre-shadow schema before the user's
/// shadow exists — verified live), so the airtight fix is to DROP the statements:
/// the next user's query re-`Parse`s fresh against their own schema, exactly as on a
/// fresh connection.
///
/// The typed result-schema guard + PostgreSQL's `0A000` do NOT rescue a kept typed
/// plan here: they catch a result-TYPE divergence, but a temp shadow with columns
/// matching the migration table has the SAME result type (no `0A000`), and a typed
/// cache HIT reuses the plan with a bare `Bind`+`Execute` that sends no `Describe`
/// (so the guard never runs). Only dropping the typed CLIENT cache — forcing the
/// next typed query to re-`Parse` fresh (and re-arm the guard) — is airtight, exactly
/// as for the dynamic cache. The typed cache's SERVER-side statements are FOLDED into
/// this batch's `Close`+`Sync` when the dynamic cache is non-empty (zero extra round
/// trip); when the dynamic cache is empty (the pure-typed flagship case) no batch is
/// forced, and the typed statements are reclaimed lazily by the next typed query's
/// MISS-path leading `Close` (a bounded, non-growing footprint). A DIRECT (non-pooled)
/// connection never resets on its own, so BOTH its caches persist for the connection's
/// life.
#[derive(Debug)]
struct DynStmtCache {
    /// Insertion-ordered slots (linear scan; `DYN_STMT_CACHE_CAP` is small, so a
    /// scan is far cheaper than the network round trip it saves). Insertion order
    /// makes "oldest pending" the first `prepared == None` slot.
    slots: Vec<DynSlot>,
}

impl DynStmtCache {
    /// An empty cache.
    const fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// The index of a READY slot for the (`sql`, `oids`) key, if the query has a
    /// cached plan prepared for THIS parameter-type tuple. A slot with the same
    /// SQL but different `param_oids` is NOT a hit (it would reinterpret the bind).
    fn ready_index(&self, sql: &str, oids: &[u32]) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.prepared.is_some() && &*s.sql == sql && s.param_oids == oids)
    }

    /// Whether the (`sql`, `oids`) key is a PENDING slot (seen once, awaiting its
    /// second sighting for THIS parameter-type tuple).
    fn is_pending(&self, sql: &str, oids: &[u32]) -> bool {
        self.slots
            .iter()
            .any(|s| s.prepared.is_none() && &*s.sql == sql && s.param_oids == oids)
    }

    /// Take the prepared statement out of READY slot `idx` (leaving it PENDING)
    /// so it can be executed while `&mut Core` is borrowed, then
    /// [`restore`](Self::restore)d (or the slot [`remove`](Self::remove)d on a
    /// self-heal). `None` only if `idx` is stale or already empty — an
    /// architecturally-dead arm the caller treats as a cache miss.
    fn take(&mut self, idx: usize) -> Option<PreparedStatement> {
        self.slots.get_mut(idx).and_then(|s| s.prepared.take())
    }

    /// Restore a prepared statement into slot `idx` after a successful (or a
    /// non-stale-error) reuse.
    fn restore(&mut self, idx: usize, stmt: PreparedStatement) {
        if let Some(s) = self.slots.get_mut(idx) {
            s.prepared = Some(stmt);
        }
    }

    /// Drop slot `idx` entirely (the reuse path evicts a stale cached plan here
    /// after `Close`ing its server-side statement).
    fn remove(&mut self, idx: usize) {
        if idx < self.slots.len() {
            self.slots.remove(idx);
        }
    }

    /// Promote the PENDING slot for `sql` to READY with its now-prepared
    /// statement. The pending slot exists by construction (the caller checked
    /// [`is_pending`](Self::is_pending) and nothing mutated the cache since), so
    /// this is an in-place `None -> Some` that never needs to evict; the fallback
    /// installs a fresh READY slot only for the architecturally-dead case where
    /// the pending slot vanished.
    fn promote(&mut self, sql: &str, oids: &'static [u32], stmt: PreparedStatement) {
        match self
            .slots
            .iter_mut()
            .find(|s| s.prepared.is_none() && &*s.sql == sql && s.param_oids == oids)
        {
            Some(s) => s.prepared = Some(stmt),
            None => {
                self.make_room();
                if self.slots.len() < DYN_STMT_CACHE_CAP {
                    self.slots.push(DynSlot {
                        sql: Box::from(sql),
                        param_oids: oids,
                        prepared: Some(stmt),
                    });
                }
            }
        }
    }

    /// Note a FIRST sighting of the (`sql`, `oids`) key as PENDING (its fused run
    /// just succeeded), evicting the oldest PENDING slot if the cache is full. A
    /// no-op if the key is already tracked (the same SQL with DIFFERENT param OIDs
    /// is a DISTINCT key, so it gets its own slot — never a cross-type reuse).
    fn note_pending(&mut self, sql: &str, oids: &'static [u32]) {
        if self.slots.iter().any(|s| &*s.sql == sql && s.param_oids == oids) {
            return;
        }
        self.make_room();
        if self.slots.len() < DYN_STMT_CACHE_CAP {
            self.slots.push(DynSlot { sql: Box::from(sql), param_oids: oids, prepared: None });
        }
    }

    /// Evict the oldest PENDING slot if the cache is at capacity, freeing a slot
    /// WITHOUT touching any server-side statement. If every slot is READY (all
    /// promoted), nothing is evicted — a new query then stays on the fused path
    /// rather than a READY statement being dropped (which would leak it).
    fn make_room(&mut self) {
        if self.slots.len() < DYN_STMT_CACHE_CAP {
            return;
        }
        if let Some(i) = self.slots.iter().position(|s| s.prepared.is_none()) {
            self.slots.remove(i);
        }
    }

    /// Empty the cache, returning every READY prepared statement so the caller
    /// can `Close` its server-side statement (a PENDING slot holds none). Used by
    /// [`reset_session`](Core::reset_session) to CLEAR the cache on a pool
    /// checkout — after this the cache is empty and no server-side statement is
    /// orphaned.
    fn drain(&mut self) -> Vec<PreparedStatement> {
        let mut out = Vec::new();
        for slot in self.slots.drain(..) {
            if let Some(stmt) = slot.prepared {
                out.push(stmt);
            }
        }
        out
    }
}

/// A scope guard that times a query verb and, on drop, emits a
/// [`DiagEvent::SlowQuery`](crate::diag::DiagEvent::SlowQuery) if the verb
/// COMPLETED and its elapsed time met the threshold.
///
/// Reporting on DROP covers every exit path of a multi-return verb (the cached /
/// promoted / fused / error branches of `query_params`) with ONE construction
/// site, and measures the WHOLE operation (including any plan-promotion round
/// trips). It owns a CLONED [`Diagnostics`](crate::diag::Diagnostics) rather than
/// borrowing `self`, so it does not alias the `&mut self` the verb body needs.
/// It is built ONLY when slow-query timing is armed (a threshold AND a sink), so
/// an off connection never clones or reads a clock.
///
/// # Success-gated + unwind-safe (never a rogue callback from a destructor)
///
/// The guard reports ONLY when [`commit`](Self::commit) marked the verb's
/// SUCCESSFUL (`Ok`) completion — a verb that errored or whose future was
/// cancelled mid-`.await` never committed, so it emits no "slow query" for a
/// query that did not complete. The drop ALSO short-circuits if the thread is
/// [`panicking`](std::thread::panicking): a verb unwinding on a panic must never
/// fire a consumer callback from a destructor (the double-panic → `SIGABRT`
/// hazard), and a panicked verb "failed", it was not "slow". The emit itself
/// routes through [`Diagnostics::emit`](crate::diag::Diagnostics::emit), whose
/// `catch_unwind` contains a panicking sink regardless.
struct SlowQueryGuard<'a> {
    /// A clone of the connection's diagnostics (sink + threshold). Cheap: an
    /// `Option<Arc>` bump plus an `Option<Duration>` copy.
    diag: crate::diag::Diagnostics,
    /// The SQL text to report (borrowed from the verb's `sql` argument, which
    /// outlives the guard). Never the parameter VALUES (no PII).
    sql: &'a str,
    /// When the verb started (read once, at guard construction).
    started: Instant,
    /// Set by [`commit`](Self::commit) once the verb completed SUCCESSFULLY; the
    /// drop emits only when this is `true`.
    committed: bool,
}

impl SlowQueryGuard<'_> {
    /// Mark the verb's successful completion, so the drop will report if slow.
    /// Called on the `Ok` path only; an errored/cancelled verb leaves this
    /// `false` and emits nothing.
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for SlowQueryGuard<'_> {
    fn drop(&mut self) {
        // Never fire a consumer callback from a destructor during an unwind (the
        // double-panic → SIGABRT hazard); a panicked verb failed, it was not slow.
        if std::thread::panicking() {
            return;
        }
        // Report only a verb that COMPLETED (Ok) — not an errored/cancelled one.
        if !self.committed {
            return;
        }
        // `threshold` is `Some` by construction (armed guard); route the emit
        // through `Diagnostics::emit`, whose `catch_unwind` contains a panicking
        // sink so it can never poison the driver.
        if let Some(threshold) = self.diag.slow_threshold() {
            let elapsed = self.started.elapsed();
            if elapsed >= threshold {
                self.diag
                    .emit(&crate::diag::DiagEvent::SlowQuery { sql: self.sql, elapsed });
            }
        }
    }
}

// ── Dropped-future recovery (the linear-token un-brick) ─────────────────────
//
// Every active-phase verb MOVES the linear `Live` out of `Core::live` into its
// future and returns it only on a clean boundary. If that future is DROPPED
// mid-command — the caller lost a `tokio::time::timeout` / `select!` race, the
// single most common async cancellation pattern — the token drops WITH the
// future (a ZST, no `Drop` runs), `Core::live` stays `None`, and with no
// re-mint the connection would be permanently unusable even though the socket is
// fine. AND the server keeps executing the abandoned query (a zombie holding
// locks). This recovers both transparently on the NEXT use.
//
// The mechanism: a per-connection `dirty` marker (an `Arc<AtomicU8>` so the
// verb-scoped `CancelScope` can set it from its `Drop` AFTER the future is gone,
// without borrowing `self` for the scope's life) carries WHAT recovery the next
// use must run.

/// `dirty` state: CLEAN — no dropped-future recovery is owed. A `Core::live` of
/// `None` with this state is a genuinely dead connection (a prior fatal error),
/// classified [`DriverError::NotReady`] as before.
const DIRTY_CLEAN: u8 = 0;
/// `dirty` state: a COMMAND verb's future was dropped mid-flight, so the server
/// owes a reply the engine never drained. The next use must cancel the abandoned
/// query (best-effort) then DRAIN the owed frames to a clean idle before
/// re-minting.
const DIRTY_DRAIN: u8 = 1;
/// `dirty` state: a WAIT (`recv_notification`) future was dropped. The wait owed
/// NO reply (it issued no command; the engine sits at a clean idle), so the next
/// use only RE-MINTS the token — draining here would block on frames that never
/// come.
const DIRTY_RECLAIM: u8 = 2;

/// A driver-provided capability to send an out-of-band `CancelRequest` on a
/// THROWAWAY socket — the ONE recovery step that needs driver-specific dial I/O
/// (a fresh socket to the same endpoint, honoring the original TLS decision),
/// which the transport-generic [`Core`] cannot perform itself.
///
/// Only the async driver installs one (via [`Core::set_recovery_cancel`]): the
/// blocking driver's verbs run to completion inside one `poll_once` and cannot be
/// dropped mid-command, so it never reaches a `DIRTY_DRAIN` recovery and needs no
/// hook (it leaves the field `None`). A testkit connection also leaves it `None`.
///
/// A `None` hook is not a bug: recovery still DRAINS the owed frames to idle
/// (bounded by the client-liveness window when a `statement_timeout` is set); the
/// cancel merely makes a long zombie stop FAST so the drain is quick. The
/// returned future is best-effort — it swallows its own dial/write errors (a
/// cancel that cannot be delivered is a documented no-op, never a recovery
/// failure) and is bounded by the connection's own connect-timeout.
pub trait RecoveryCancel: Send + Sync {
    /// Dial a throwaway socket and write the 16-byte `CancelRequest` `packet`,
    /// swallowing any error. Boxed because the dial is driver-specific `async`
    /// I/O behind a `dyn` boundary; only ever awaited on the cold recovery path.
    fn cancel<'a>(&'a self, packet: [u8; 16]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// A verb-scoped RAII guard whose `Drop` marks the connection recoverable IFF the
/// verb's future was DROPPED mid-command (never [`disarm`](Self::disarm)ed).
///
/// It owns a CLONE of the connection's `dirty` handle (not a borrow of `self`),
/// so it borrows the connection only for the instant it is created and then
/// coexists with the `&mut self` the verb body pumps through. Created right after
/// the token is taken and disarmed once the verb's body future has been polled to
/// completion (Ok OR Err — both are consistent terminal states the verb's own
/// settle already recorded); reached-while-armed means, and ONLY means, the outer
/// future was dropped before the body finished, which is the exact condition that
/// stranded the token.
struct CancelScope {
    dirty: Arc<AtomicU8>,
    /// The recovery mode to record on an armed drop (`DIRTY_DRAIN` for a command
    /// verb, `DIRTY_RECLAIM` for a wait).
    mode: u8,
    armed: bool,
}

impl CancelScope {
    /// Arm a scope over a clone of `dirty`, recording `mode` on an armed drop.
    #[inline]
    fn arm(dirty: Arc<AtomicU8>, mode: u8) -> Self {
        Self { dirty, mode, armed: true }
    }

    /// Disarm: the verb's body future completed, so its terminal state is already
    /// consistent and no dropped-future recovery is owed. Consumes the scope so a
    /// double-disarm is impossible.
    #[inline]
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for CancelScope {
    #[inline]
    fn drop(&mut self) {
        if self.armed {
            // Reached armed ⇒ the verb's body future was dropped mid-command. The
            // token is gone; record the recovery mode so the next use re-mints and
            // recovers. `Release` pairs with the `Acquire` load in `begin_command`.
            core::hint::cold_path();
            self.dirty.store(self.mode, Ordering::Release);
        }
    }
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
    /// The backend process id from `BackendKeyData` — the non-secret half of the
    /// cancel key.
    backend_pid: i32,
    /// The `BackendKeyData` secret — the SECRET half of the cancel key, captured
    /// at connect and kept in a `Sensitive` (redacted in `Debug`, zeroed on drop)
    /// so [`cancel_key`](Self::cancel_key) can mint an unforgeable [`CancelKey`]
    /// for an out-of-band `CancelRequest`. `Sensitive<i32>` is `#[repr(transparent)]`
    /// over the `i32`, so the field costs 4 bytes and no hot path reads it.
    secret_key: Sensitive<i32>,
    /// Monotonic counter for generating fresh prepared-statement names.
    stmt_counter: u32,
    /// This connection's PROCESS-UNIQUE identity, minted once at construction from
    /// [`NEXT_CONN_ID`]. Stamped onto every [`PreparedStatement`] this connection
    /// prepares ([`PreparedStatement::origin`]); every prepared verb rejects a
    /// handle whose origin differs (a cross-connection use) with
    /// [`DriverError::WrongConnection`] before any wire I/O — closing the
    /// silent-wrong-result hole a like-named `_bsql_<n>` on a foreign connection
    /// would otherwise open. Never read on a hot path.
    conn_id: u64,
    /// The bounded per-connection cache of DYNAMIC prepared statements, keyed on
    /// SQL text (see [`DynStmtCache`]) — plan reuse for the runtime
    /// `query_params` / `execute_params` family. Off the compile-checked typed
    /// path (which caches in the engine); this is the driver-level dynamic peer.
    dyn_cache: DynStmtCache,
    /// The bounded, counted no-drop buffer of asynchronous notifications. Every
    /// verb's sink is wrapped with [`capture_notify`] so a `NOTIFY` arriving on
    /// any command's response stream is buffered here rather than dropped.
    notifications: NotificationLedger,
    /// The structured-diagnostics configuration (the [`DiagSink`] callback + the
    /// slow-query threshold). `Default` (no sink) unless a
    /// [`set_diagnostics`](Self::set_diagnostics) call installs one, so an off
    /// connection pays only a never-taken branch at each cold boundary — the
    /// per-row hot path is untouched. Threaded from the pool to every minted
    /// connection, or set on a standalone connection.
    ///
    /// [`DiagSink`]: crate::diag::DiagSink
    diag: crate::diag::Diagnostics,
    /// The dropped-future recovery marker: `DIRTY_CLEAN` normally, or
    /// `DIRTY_DRAIN` / `DIRTY_RECLAIM` after a verb's future was dropped
    /// mid-command (see the recovery section above). An `Arc<AtomicU8>` so the
    /// verb-scoped [`CancelScope`] can set it from its `Drop` — which runs AFTER
    /// the dropped future (and the token) are gone — without borrowing `self` for
    /// the scope's lifetime. Read once (a relaxed-cost `Acquire` load) at the head
    /// of every verb via [`begin_command`](Self::begin_command); never on the hot
    /// per-row path.
    dirty: Arc<AtomicU8>,
    /// The driver-provided out-of-band cancel dial for dropped-future recovery,
    /// or `None` (the blocking driver + testkit, which never reach `DIRTY_DRAIN`).
    /// Set once after construction by the async driver's `connect_with`.
    recovery_cancel: Option<Arc<dyn RecoveryCancel>>,
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
    /// connect-time session facts (`encrypted`, `server_version`, `backend_pid`,
    /// `secret_key`) off the engine. `#[doc(hidden)]`: the driver-facing
    /// construction seam, not a public API.
    ///
    /// `secret_key` is the raw `BackendKeyData` secret (read once via
    /// [`Engine::with_secret_key`](bsql_postgres_proto::engine::Engine::with_secret_key));
    /// it is wrapped back into a `Sensitive` here immediately, so its only
    /// in-the-clear residence is the argument register on this call.
    #[doc(hidden)]
    #[must_use]
    pub fn new(
        engine: Engine<'static, Wire<S>>,
        live: Live<'static>,
        encrypted: bool,
        server_version: Option<String>,
        backend_pid: i32,
        secret_key: i32,
    ) -> Self {
        Self {
            engine,
            live: Some(live),
            encrypted,
            server_version,
            backend_pid,
            secret_key: Sensitive::new(secret_key),
            stmt_counter: 0,
            conn_id: NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed),
            dyn_cache: DynStmtCache::new(),
            notifications: NotificationLedger::new(),
            diag: crate::diag::Diagnostics::default(),
            dirty: Arc::new(AtomicU8::new(DIRTY_CLEAN)),
            recovery_cancel: None,
            #[cfg(feature = "n1-detect")]
            n1_tracker: crate::N1Tracker::new(),
        }
    }

    /// Install the driver-provided out-of-band cancel dial used by dropped-future
    /// recovery — called once by the async driver's `connect_with` after the
    /// handshake. The blocking driver and testkit never call this (they cannot be
    /// dropped mid-command, so they never reach a `DIRTY_DRAIN` recovery), leaving
    /// the hook `None`; recovery then still drains, just without the fast cancel.
    pub fn set_recovery_cancel(&mut self, hook: Arc<dyn RecoveryCancel>) {
        self.recovery_cancel = Some(hook);
    }

    /// Install the structured-diagnostics configuration on this connection.
    ///
    /// Called by a driver's `connect_with` after the handshake (so a connect-time
    /// event like an SSL downgrade routes through the SAME sink) and by a pool for
    /// every connection it mints, or directly by a consumer on a standalone
    /// connection. Replaces any prior configuration; passing
    /// [`Diagnostics::default`](crate::diag::Diagnostics::default) turns diagnostics
    /// off again.
    pub fn set_diagnostics(&mut self, diag: crate::diag::Diagnostics) {
        self.diag = diag;
    }

    /// The installed diagnostics configuration (its sink + slow-query threshold).
    #[must_use]
    pub fn diagnostics(&self) -> &crate::diag::Diagnostics {
        &self.diag
    }

    /// Arm a slow-query timer for `sql`, or `None` when slow-query diagnostics are
    /// off — the ZERO-COST-OFF gate.
    ///
    /// Returns `Some(guard)` ONLY when BOTH a slow-query threshold AND a sink are
    /// installed; otherwise `None`, so an off connection reads no clock
    /// (`Instant::now` is inside the `Some` arm) and clones no `Diagnostics`. The
    /// returned guard owns a clone (not a borrow of `self`), so it does not alias
    /// the `&mut self` the verb body then uses; it reports on drop, covering every
    /// exit path of the verb.
    fn armed_slow_guard<'a>(&self, sql: &'a str) -> Option<SlowQueryGuard<'a>> {
        // Zero-cost-off gate: `Instant::now` and the `Diagnostics` clone live
        // INSIDE this `if`, so an unarmed connection reads no clock and clones
        // nothing (proven offline by `Diagnostics::slow_query_armed` unit tests).
        if self.diag.slow_query_armed() {
            Some(SlowQueryGuard {
                diag: self.diag.clone(),
                sql,
                started: Instant::now(),
                committed: false,
            })
        } else {
            None
        }
    }

    /// Commit the slow-query guard iff `result` is `Ok`, so a slow query is
    /// reported ONLY for a verb that COMPLETED successfully (an errored/cancelled
    /// verb leaves the guard uncommitted and emits nothing). A no-op when the
    /// guard is `None` (slow-query timing off).
    fn commit_slow<T, E>(guard: &mut Option<SlowQueryGuard<'_>>, result: &Result<T, E>) {
        if result.is_ok()
            && let Some(g) = guard
        {
            g.commit();
        }
    }

    /// Mint the unforgeable [`CancelKey`] for this connection's backend — the
    /// `(backend_pid, secret_key)` authenticator a driver's `CancelToken` needs
    /// to build an out-of-band `CancelRequest`.
    ///
    /// Reads the pid and CLONES the secret out of the connection's `Sensitive`
    /// store into the returned key's own `Sensitive`, so the key is a detached,
    /// `Send + Sync + 'static` capability that does not borrow the live
    /// connection — mintable at any point (before starting a query) and movable
    /// to another task. Captured at connect, so it stays valid even after the
    /// owning connection goes dead.
    #[must_use]
    pub fn cancel_key(&self) -> CancelKey {
        CancelKey::new(
            self.backend_pid,
            self.secret_key.with_inner(|secret| Sensitive::new(*secret)),
        )
    }

    // ── Token + result plumbing (shared internals) ──────────────────────────

    /// The verb prologue: obtain the liveness token, transparently RECOVERING a
    /// connection whose PRIOR verb future was dropped mid-command.
    ///
    /// Three reachable states (see the recovery section on [`CancelScope`]):
    ///
    /// - `live == Some`: the normal fast path — take it and go. SYNCHRONOUS (no
    ///   `.await`), so a healthy verb pays only one `Option::take` + one relaxed
    ///   `Acquire` load, and no drop point exists before the scope is armed.
    /// - `live == None && dirty == DIRTY_DRAIN`: a command verb's future was
    ///   dropped, leaving the server owing a reply. Best-effort CANCEL the
    ///   abandoned query (so a long zombie stops fast), then re-mint + DRAIN the
    ///   owed frames to an RFQ (bounded by the client-liveness window when a
    ///   `statement_timeout` is set), then ROLL BACK any leftover transaction the
    ///   RFQ landed inside (a dropped `transaction` future) to a TRUE clean idle,
    ///   then take the fresh token.
    /// - `live == None && dirty == DIRTY_RECLAIM`: a `recv_notification` wait was
    ///   dropped. It owed nothing (the engine sits at a clean idle), so just
    ///   re-mint — a drain here would block on frames that never come.
    /// - `live == None && dirty == DIRTY_CLEAN`: a genuinely dead connection (a
    ///   prior FATAL error consumed the token), classified [`DriverError::NotReady`]
    ///   exactly as before this recovery existed.
    ///
    /// If recovery's drain does NOT reach a clean idle (a torn/black-holed wire),
    /// it FAILS: the connection is truly dead ([`DriverError::NotReady`] / a
    /// classified transport error, `is_disconnect()`), never a torn "recovered"
    /// connection handed to a pool.
    async fn begin_command(&mut self) -> Result<Live<'static>, DriverError> {
        // Fast path: a live token is present. No `.await` here, so a drop cannot
        // strand a half-taken state before the caller arms its scope.
        if let Some(live) = self.live.take() {
            return Ok(live);
        }
        match self.dirty.load(Ordering::Acquire) {
            DIRTY_DRAIN => {
                core::hint::cold_path();
                self.recover_drain().await?;
                // Recovery restored `self.live`; take it for the real verb.
                self.live.take().ok_or(DriverError::NotReady)
            }
            DIRTY_RECLAIM => {
                core::hint::cold_path();
                // The wait owed nothing — re-mint directly, no cancel, no drain.
                let live = self.engine.reclaim_live_after_drop();
                self.dirty.store(DIRTY_CLEAN, Ordering::Release);
                Ok(live)
            }
            // DIRTY_CLEAN (or any unexpected value): genuinely dead.
            _ => Err(DriverError::NotReady),
        }
    }

    /// The `DIRTY_DRAIN` recovery body: best-effort cancel the abandoned query,
    /// re-mint the token, DRAIN the owed reply frames to an RFQ, then ROLL BACK any
    /// leftover transaction so the recovered connection is at a GENUINE clean idle —
    /// restoring `self.live` and clearing `dirty` on success.
    ///
    /// On a FATAL drain OR a failed rollback (a torn wire, or a black-holed peer
    /// whose window elapsed) the connection is truly dead: `dirty` is cleared (it is
    /// no longer *recoverable* — it is dead) and the classified transport error is
    /// returned, leaving `self.live == None` so the next use is a clean
    /// [`DriverError::NotReady`].
    ///
    /// # At-least-once for a mid-FLUSH drop (honest)
    ///
    /// If the future was dropped while the command was only PARTIALLY written, the
    /// remainder is still queued in the engine's send buffer; the drain's entry
    /// flush completes the send, so the abandoned query RUNS to completion
    /// post-drop (the out-of-band cancel fired before the server had the full
    /// command). This is within best-effort / AT-LEAST-ONCE semantics — a
    /// NON-IDEMPOTENT write bound this way WILL apply. (A drop AFTER the command was
    /// fully sent is instead cancelled; either way the connection recovers clean.)
    ///
    /// # Drain boundedness depends on `statement_timeout` (honest)
    ///
    /// The drain's socket reads are bounded by the client-liveness window ONLY when
    /// a `statement_timeout` is configured (which derives that window); the cancel
    /// normally makes the drain quick regardless, but in the narrow case of a drop
    /// mid-INTERMEDIATE-window of a multi-window pipeline/batch (a `Flush`, no owed
    /// `Sync`), the drain awaits an RFQ only the never-sent trailing `Sync` would
    /// produce — so WITHOUT a `statement_timeout` that case can hang, and WITH one
    /// it elapses to a bounded classified `Timeout` → truly dead. A single-window
    /// batch and every ordinary command always owe an RFQ, so they recover.
    async fn recover_drain(&mut self) -> Result<(), DriverError> {
        // 1. Best-effort out-of-band cancel so a long-running zombie stops FAST and
        //    the drain below is quick. Absent hook (blocking driver / testkit) or a
        //    failed dial is fine — the drain still reaches idle once the server's
        //    own `statement_timeout` fires or the query finishes. Clone the `Arc`
        //    so the hook's future does not borrow `self` across the drain below.
        if let Some(hook) = self.recovery_cancel.clone() {
            let packet = self.cancel_key().request_bytes();
            hook.cancel(packet).await;
        }
        // 2. Re-mint (sound: `self.live` is provably `None` and `dirty` was
        //    DIRTY_DRAIN, so the prior token was dropped — no other token exists)
        //    and drain to an RFQ. `drain_to_idle` restores `self.live` on success.
        let live = self.engine.reclaim_live_after_drop();
        if let Err(e) = self.drain_to_idle(live).await {
            core::hint::cold_path();
            // The drain failed: the connection is dead, not recoverable.
            self.dirty.store(DIRTY_CLEAN, Ordering::Release);
            return Err(e);
        }
        // 3. The drain reached AN RFQ, but a dropped `transaction` future skipped the
        //    guard's async rollback (async `Drop` cannot `.await`), so that RFQ may
        //    sit INSIDE a leftover transaction (status `T`/`E`). Roll it back to a
        //    TRUE clean idle — the `transaction` guard's own non-Ok contract (a
        //    dropped/abandoned transaction MUST roll back, never commit) and exactly
        //    what the pooled `reset_session` path does — so the recovered connection
        //    is genuinely reusable, never left stuck in `25P02`.
        if let Err(e) = self.rollback_leftover_transaction().await {
            core::hint::cold_path();
            // The rollback itself failed → the connection is dead, not recoverable.
            self.dirty.store(DIRTY_CLEAN, Ordering::Release);
            return Err(e);
        }
        self.dirty.store(DIRTY_CLEAN, Ordering::Release);
        Ok(())
    }

    /// Roll back a leftover transaction the recovered RFQ landed inside (a dropped
    /// `transaction` future skipped the guard's async rollback). A NO-OP when the
    /// drain already reached a clean `Idle` (the common non-transaction case, so an
    /// ordinary dropped-query recovery pays NO extra round trip). Reuses the token
    /// [`drain_to_idle`](Self::drain_to_idle) just restored, and restores it again
    /// on success.
    async fn rollback_leftover_transaction(&mut self) -> Result<(), DriverError> {
        // The last RFQ's tx-status byte. `Idle` → nothing to roll back. A
        // `WrongPhase` (unreachable after a clean drain) is conservatively treated
        // as "nothing to roll back" — the drain proved the connection idle.
        if !matches!(
            self.engine.tx_status(),
            Ok(TxStatus::InTransaction | TxStatus::Failed)
        ) {
            return Ok(());
        }
        core::hint::cold_path();
        // `drain_to_idle` restored the token; take it for the rollback. Its absence
        // is unreachable (the drain succeeded above), classified defensively.
        let Some(live) = self.live.take() else {
            return Err(DriverError::NotReady);
        };
        let mut collector = ResultCollector::new();
        // `ROLLBACK` is the ONE command an aborted (`E`) transaction accepts and is
        // always valid in an open (`T`) one, so it reaches a clean `Idle`. `settle`
        // restores `self.live` on the alive outcome; a FATAL transport error during
        // the rollback leaves the token gone → the connection is dead.
        let outcome = self
            .engine
            .simple_query(
                live,
                "ROLLBACK",
                capture_notify(&mut self.notifications, self.diag.sink(), |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                }),
            )
            .await;
        self.settle(outcome, &mut collector)
    }

    /// Arm a [`CancelScope`] over a CLONE of this connection's `dirty` handle,
    /// recording `mode` on an armed drop — the second half (with
    /// [`begin_command`](Self::begin_command)) of every command verb's
    /// dropped-future guard.
    ///
    /// Every token-taking verb follows the SAME shape (the ONE forget-proof
    /// discipline): `begin_command().await?` to recover + take the token, then
    /// `arm_scope(mode)`, then run the body inside an `async move { … }` block, then
    /// `disarm`. The block is a plain async block (NOT an async closure): an async
    /// closure taking `&mut Self` would trip rustc's "`AsyncFnOnce` not general
    /// enough" wall when the driver requires the verb future to be `Send`, whereas an
    /// inline block moves `self` in and stays `Send`. The scope owns the `Arc` clone
    /// (not a borrow of `self`), so it coexists with the `&mut self` the block pumps
    /// through; `disarm` runs the instant the block's future is polled to completion
    /// (Ok OR Err), so only a DROP of the outer future mid-block leaves the scope
    /// armed — the exact condition that stranded the token, which its `Drop` then
    /// records (`mode`) for the next use to recover. `mode` is `DIRTY_DRAIN` for a
    /// command verb, `DIRTY_RECLAIM` for a wait (`recv_notification`). Momentary
    /// `&self` borrow.
    #[inline]
    fn arm_scope(&self, mode: u8) -> CancelScope {
        CancelScope::arm(Arc::clone(&self.dirty), mode)
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
                    // A recoverable failure the verb already drained to a clean
                    // idle. TWO client-visible classes ride this status: a too-wide
                    // result (checked FIRST — its own classification, never masked
                    // by the generic fallback) and a server `ErrorResponse`. The
                    // engine parks at most one, so the order only fixes precedence
                    // for the impossible both-set case.
                    CommandStatus::ServerErrored => match collector.take_overcap() {
                        Some((count, max)) => Err(DriverError::TooManyColumns { count, max }),
                        None => match collector.take_db_error() {
                            Some(db) => Err(DriverError::Db(Box::new(db))),
                            None => Err(DriverError::UnclassifiedFailure),
                        },
                    },
                }
            }
            // Fatal: the verb consumed the token and the connection is dead.
            Err(other) => Err(lift_engine_error(other)),
        }
    }

    /// Surface a typed result-schema OID mismatch the engine recorded during a
    /// compile-checked cache-MISS's `RowDescription` check, if any.
    ///
    /// The typed decode is positional / const-offset, so a runtime column whose
    /// type diverged from the migration schema (an out-of-band `ALTER COLUMN TYPE`,
    /// or a `TEMP TABLE` shadowing the migration table) would silently mis-decode.
    /// The engine's guard catches this at the fresh Parse's `RowDescription` (a
    /// cache MISS) and drains the result to a clean idle; every typed verb calls
    /// this AFTER its pump settles — the connection is already reusable — to turn
    /// the recorded mismatch into a classified
    /// [`DecodeError::ColumnOidMismatch`](bsql_postgres_proto::DecodeError::ColumnOidMismatch)
    /// (a `DriverError::Decode`, NOT a disconnect: fix the schema drift and retry on
    /// the same connection). A cache HIT cannot silently mis-decode — PostgreSQL
    /// refuses to change a reused plan's result type (`0A000`) — so on a HIT there
    /// is nothing recorded and this is a cheap `None` check.
    ///
    /// The engine records the checked `(index, found, expected)` triple directly
    /// (the `expected` OID is the value it SEATED from the carrier's `row_oids` and
    /// checked against), so the driver surfaces the classified error verbatim — the
    /// engine is the single source of the pair. This is what lets the heterogeneous
    /// [`pipeline`](Self::pipeline) surface the SAME triple from its batch-generic
    /// settle, which has no single carrier `Q` to recover an `expected` from.
    fn take_typed_schema_error(&mut self) -> Result<(), DriverError> {
        match self.engine.take_result_oid_mismatch() {
            Some((index, found, expected)) => {
                Err(DriverError::Decode(DecodeError::ColumnOidMismatch {
                    index,
                    expected,
                    found,
                }))
            }
            None => Ok(()),
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
        let (rows, command_tag, names) = collector.finish()?;
        let column_names = match names_override {
            Some(names) => names,
            None => Arc::from(names.into_boxed_slice()),
        };
        Ok(QueryResult::new(rows, command_tag, column_names))
    }

    // ── Runtime-SQL verbs ───────────────────────────────────────────────────

    /// Round-trip a `Sync` to confirm the connection is live.
    pub async fn ping(&mut self) -> Result<(), DriverError> {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .ping(
                    live,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)
        }
        .await;
        scope.disarm();
        out
    }

    /// Issue a simple query, returning the command tag string.
    pub async fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let mut slow = self.armed_slow_guard(sql);
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let result = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .simple_query(
                    live,
                    sql,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            // Move the already-owned tag out — no clone (collector is dropped).
            Ok(collector.into_command_tag())
        }
        .await;
        scope.disarm();
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    pub async fn execute_raw(&mut self, sql: &str) -> Result<u64, DriverError> {
        let mut slow = self.armed_slow_guard(sql);
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let result = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .execute(
                    live,
                    sql,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            Ok(collector.affected())
        }
        .await;
        scope.disarm();
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// Run a row-returning runtime-SQL query (text result columns).
    pub async fn query_raw(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        let mut slow = self.armed_slow_guard(sql);
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let result = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .query(
                    live,
                    sql,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            Self::build_query_result(collector, None)
        }
        .await;
        scope.disarm();
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    ///
    /// Mints EXACTLY one [`Row`](crate::Row) handle ([`QueryResult::get`]) — a
    /// single `Arc` refcount bump — never the whole result's worth of handles.
    pub async fn query_one_raw(&mut self, sql: &str) -> Result<crate::Row, DriverError> {
        self.query_raw(sql).await?.get(0).ok_or(DriverError::NoRows)
    }

    /// Run a runtime-SQL query returning the first row if any.
    ///
    /// The `_raw` suffix marks the runtime (unchecked-string) source, matching its
    /// [`query_raw`](Self::query_raw) / [`query_one_raw`](Self::query_one_raw)
    /// siblings; the bare [`query_opt`](Self::query_opt) is the compile-checked
    /// typed peer.
    pub async fn query_opt_raw(&mut self, sql: &str) -> Result<Option<crate::Row>, DriverError> {
        Ok(self.query_raw(sql).await?.get(0))
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
    ///
    /// The explicit-handle path declares NO parameter-type OIDs (the server infers
    /// each `$N` from the SQL context), because the parameter types are only known
    /// at a later `Bind`. The DYNAMIC plan-cache PROMOTE uses
    /// [`prepare_with_oids`](Self::prepare_with_oids) instead, which pins the
    /// caller's encoded parameter types into the `Parse`.
    ///
    /// The server-inferred parameter types are RETAINED on the returned
    /// [`PreparedStatement`] (from the prepare's `ParameterDescription`), so a
    /// later [`query_prepared`](Self::query_prepared) /
    /// [`execute_prepared`](Self::execute_prepared) VERIFIES the caller's encoded
    /// parameter types against them and rejects a mismatch loudly — a fixed plan
    /// cannot coerce a differently-typed binary bind, so this closes the silent
    /// reinterpretation a bare same-width bind would otherwise cause.
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        self.prepare_with_oids(sql, &[]).await
    }

    /// Prepare a statement declaring `param_oids` as the parameter-type OIDs in the
    /// `Parse` — the DYNAMIC plan-cache PROMOTE's named-statement prepare.
    ///
    /// Threads the caller's `<P as ParamsWriter>::OIDS` into the `Parse` so a
    /// repeated dynamic query's cached plan decodes each binary parameter AS the
    /// client's encoded type (a type disagreement is a LOUD classified server
    /// error, never a silent reinterpretation) — the named-statement peer of the
    /// fused first-sighting path's declared OIDs, and of the compile-checked
    /// `query!` path's baked template. `&[]` is the server-infers form the public
    /// [`prepare`](Self::prepare) uses.
    async fn prepare_with_oids(
        &mut self,
        sql: &str,
        param_oids: &[u32],
    ) -> Result<PreparedStatement, DriverError> {
        let stmt_name = self.next_stmt_name()?;
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            // `prepare` is the ONLY reader of the result-column OIDs, so it captures
            // them directly into this owned Vec in its pump closure — the shared
            // `ResultCollector` no longer stores an `oids` Vec (which charged every
            // dynamic row-returning verb one heap `Vec<u32>` per `Deliver` for a
            // value only this cold path reads). `Surface` is `Copy`, so the peeked
            // `s` still feeds the collector; `clear` + `extend` keeps the LAST
            // delivery (a prepare emits one result `Deliver`).
            let mut result_oids: Vec<u32> = Vec::new();
            let outcome = this
                .engine
                .prepare(
                    live,
                    &stmt_name,
                    sql,
                    param_oids,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        if let Surface::Deliver { oids, .. } = s {
                            result_oids.clear();
                            result_oids.extend_from_slice(oids);
                        }
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            let column_names: Arc<[String]> =
                Arc::from(collector.column_names().to_vec().into_boxed_slice());
            // The server-inferred parameter-type OIDs from the prepare's
            // `ParameterDescription`, retained for the pre-`Bind` type check in
            // `query_prepared` / `execute_prepared`. Read from the engine directly
            // (stable post-settle state, like `tx_status`): a successful prepare left
            // the engine active, so `current_param_oids` resolves; a `WrongPhase`
            // (unreachable on this success path) degrades to empty = best-effort skip,
            // never a panic.
            let param_oids: Box<[u32]> = match this.engine.current_param_oids() {
                Ok(oids) => Box::from(oids),
                // Unreachable on this success path (a settled prepare left the engine
                // active); empty = best-effort skip, never a panic.
                Err(_) => Box::from([]),
            };
            Ok(PreparedStatement {
                inner: WireStatement::new(stmt_name, result_oids),
                column_names,
                param_oids,
                // Stamp this connection's identity so a later prepared verb rejects a
                // cross-connection use of the handle (see [`check_stmt_origin`]).
                origin: this.conn_id,
            })
        }
        .await;
        scope.disarm();
        out
    }

    /// Reject a [`PreparedStatement`] minted by a DIFFERENT connection.
    ///
    /// A statement handle names a server-side statement (`_bsql_<n>`) whose plan
    /// lives ONLY on its originating connection; the per-connection name counter
    /// makes `_bsql_0` exist on every connection, each a different plan. So a
    /// cross-connection use would bind against a LIKE-NAMED but UNRELATED statement
    /// — a silent wrong result. Checked FIRST (before `verify_params`, before any
    /// wire I/O): a mismatch is [`DriverError::WrongConnection`], the connection is
    /// untouched, and the statement's own connection is still the place to run it.
    #[inline]
    fn check_stmt_origin(&self, stmt: &PreparedStatement) -> Result<(), DriverError> {
        if stmt.origin == self.conn_id {
            Ok(())
        } else {
            core::hint::cold_path();
            Err(DriverError::WrongConnection)
        }
    }

    /// Execute a prepared statement returning rows. Params are borrowed all the
    /// way to the engine, so a non-`Copy` owned param binds by reference.
    pub async fn query_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        // Reject a handle minted by a DIFFERENT connection FIRST — its server-side
        // name would bind a like-named but unrelated statement here (a silent wrong
        // result). No wire I/O, connection untouched.
        self.check_stmt_origin(stmt)?;
        // Verify the caller's encoded parameter types against the statement's
        // fixed (server-inferred) parameter types BEFORE binding — a mismatch is
        // rejected here with no wire round trip, closing the silent-reinterpret
        // hole a same-width wrong-typed bind would open against the fixed plan.
        stmt.verify_params::<P>()?;
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .query_prepared(
                    live,
                    &stmt.inner,
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            Self::build_query_result(collector, Some(stmt.column_names.clone()))
        }
        .await;
        scope.disarm();
        out
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count.
    pub async fn execute_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        // Reject a cross-connection handle FIRST (see
        // [`query_prepared`](Self::query_prepared)).
        self.check_stmt_origin(stmt)?;
        // Verify parameter types against the fixed plan before binding (see
        // [`query_prepared`](Self::query_prepared)).
        stmt.verify_params::<P>()?;
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .execute_prepared(
                    live,
                    &stmt.inner,
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            Ok(collector.affected())
        }
        .await;
        scope.disarm();
        out
    }

    /// Run a runtime-SQL query with params, transparently reusing a cached
    /// server-side plan for a REPEATED query.
    ///
    /// A one-shot query still costs ONE round trip: the FIRST sighting of a SQL
    /// runs the fused unnamed path (a single
    /// `Parse`(unnamed)/`Bind`/`Describe`(portal)/`Execute`/`Sync` flush, see
    /// [`query_params_uncached`](Self::query_params_uncached)), so a query run
    /// once pays nothing extra. A query run AGAIN is prepared to a named
    /// server-side statement (one one-time extra round trip on its second
    /// sighting) and every LATER call reuses that plan in ONE round trip
    /// (`Bind`/`Execute`/`Sync`, no server-side re-parse or re-plan) — strictly
    /// better than re-parsing the SQL on every call. The cache is INVISIBLE (the
    /// verb still takes SQL text), bounded, and self-healing: see [`DynStmtCache`].
    ///
    /// A schema change that invalidates a cached plan surfaces the classified
    /// server error ONCE (`0A000` / `26000`) while the cache reclaims the stale
    /// statement; the next sighting re-prepares against the current schema, so a
    /// stale result is never returned silently.
    ///
    /// # Parameters are borrowed (`&P`), the typed flagship's are by value
    ///
    /// The DYNAMIC parameterized verbs take `params: &P` where `P: ParamsWriter`,
    /// whereas the compile-checked flagship [`query`](Self::query) takes its
    /// concrete `Q::Params<'p>` tuple BY VALUE. This is a DELIBERATE reflection of
    /// two different roles, not an accidental inconsistency: `P: ParamsWriter` is
    /// an UNBOUNDED generic the verb only READS (it needs `&self` to write the
    /// Bind block), so a borrow is the idiomatic read-only-generic signature —
    /// exactly like `fn f<T: Display>(x: &T)`. The flagship's `Q::Params<'p>` is a
    /// CONCRETE macro-emitted associated type, constructed inline at the call site,
    /// for which by-value gives the cleanest flagship call. Aligning the two would
    /// only drop one `&` at the call site — a purely cosmetic change with no
    /// correctness, safety, or performance payoff — while de-idiomatizing the
    /// read-only generic; so the shapes are kept as-is by design.
    pub async fn query_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        // Time the WHOLE operation (fused/promoted/cached paths) via a thin
        // wrapper over the inner body, committing only on Ok — a multi-return verb
        // funnels through one point, so a slow repeated query (cache reuse) is
        // caught and an errored one is not reported.
        let mut slow = self.armed_slow_guard(sql);
        let result = self.query_params_inner(sql, params).await;
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// The body of [`query_params`](Self::query_params) — the dynamic plan-cache
    /// orchestration, wrapped by `query_params` for slow-query timing.
    async fn query_params_inner<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        if let Some(idx) = self.dyn_cache.ready_index(sql, P::OIDS) {
            // REUSE: Bind+Execute+Sync on the cached named statement (no re-parse).
            // The slot is keyed on (SQL, P::OIDS), so the cached plan was prepared
            // for EXACTLY this parameter-type tuple — no cross-type reinterpret.
            if let Some(stmt) = self.dyn_cache.take(idx) {
                match self.query_prepared(&stmt, params).await {
                    Ok(qr) => {
                        self.dyn_cache.restore(idx, stmt);
                        return Ok(qr);
                    }
                    Err(e) if e.is_stale_prepared_plan() => {
                        // The cached plan went stale (schema change / out-of-band
                        // DEALLOCATE). Reclaim the server-side statement, evict,
                        // then TRANSPARENTLY re-run THIS query on the fused path so
                        // the caller never sees the driver-internal staleness — a
                        // schema change costs one fused re-parse, never a spurious
                        // error. This is NOT fallback error-masking: it fires only
                        // on the two SQLSTATEs that can ONLY arise from a stale
                        // CACHED plan (a genuine 0A000 would have failed this
                        // query's first, uncached sighting and so never cached),
                        // and any OTHER error surfaces unchanged (below). Fall
                        // through to the fused re-run + re-warm.
                        //
                        // Surface the self-heal: a silent re-prepare on a stale
                        // plan is exactly the fallback path an operator must be
                        // able to see (only latency shows otherwise).
                        self.diag
                            .emit(&crate::diag::DiagEvent::PreparedCacheSelfHeal { sql });
                        self.dyn_cache.remove(idx);
                        self.close_statement(stmt).await?;
                    }
                    Err(e) => {
                        // A data error (a constraint violation, a bad cast) does
                        // NOT invalidate the plan — keep it cached, surface the error.
                        self.dyn_cache.restore(idx, stmt);
                        return Err(e);
                    }
                }
            }
            // Only reached on the stale-self-heal fall-through (the slot was
            // evicted above); re-run on the fused path and re-note PENDING.
        } else if self.dyn_cache.is_pending(sql, P::OIDS) {
            // PROMOTE (second sighting): prepare a named statement declaring the
            // caller's encoded parameter types (`P::OIDS`, so the cached plan
            // type-checks each binary parameter exactly as the fused first sighting
            // did), run it, and cache the (valid) statement regardless of the
            // execute OUTCOME — a data error does not invalidate a fresh plan.
            let stmt = self.prepare_with_oids(sql, P::OIDS).await?;
            let out = self.query_prepared(&stmt, params).await;
            self.dyn_cache.promote(sql, P::OIDS, stmt);
            return out;
        }
        // FIRST sighting — OR a stale-evicted re-warm (the transparent self-heal):
        // the fused one-round-trip path; note it PENDING on success so the next
        // sighting re-promotes.
        let qr = self.query_params_uncached(sql, params).await?;
        self.dyn_cache.note_pending(sql, P::OIDS);
        Ok(qr)
    }

    /// Run a one-shot runtime-SQL query with params in ONE round trip, WITHOUT
    /// the plan cache — the fused primitive behind [`query_params`](Self::query_params).
    ///
    /// Fuses `Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` + `Sync`
    /// into a single flush (see [`Engine::query_params_fused`]), so a one-shot
    /// parameterised query costs ONE round trip. The result schema (OIDs + names)
    /// is recovered from the inline `Describe`(portal) `RowDescription`, so the
    /// [`QueryResult`]'s column names come straight from the collector — no
    /// separate `prepare` round trip. The unnamed statement is implicitly
    /// discarded at the next `Parse`, so no `Close` is needed and the
    /// prepared-statement cache is untouched. This is the FIRST-sighting path;
    /// [`query_params`](Self::query_params) promotes a repeated query to a cached
    /// named statement.
    ///
    /// [`Engine::query_params_fused`]: bsql_postgres_proto::engine::Engine::query_params_fused
    async fn query_params_uncached<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .query_params_fused(
                    live,
                    sql,
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            // Names come from the collector (recovered from the inline
            // `Describe`(portal) `RowDescription`), not a prepared-statement override.
            Self::build_query_result(collector, None)
        }
        .await;
        scope.disarm();
        out
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub async fn query_params_one<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<crate::Row, DriverError> {
        self.query_params(sql, params).await?.get(0).ok_or(DriverError::NoRows)
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub async fn query_params_opt<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<crate::Row>, DriverError> {
        Ok(self.query_params(sql, params).await?.get(0))
    }

    /// Run a runtime-SQL command with params, returning the affected-row count —
    /// the side-effect twin of [`query_params`](Self::query_params), with the
    /// SAME transparent dynamic plan cache (first sighting fused, repeats reuse a
    /// cached named statement in one round trip). A no-RETURNING command answers
    /// the `Describe`(portal) with `NoData`; the affected count rides the
    /// `CommandComplete` tag. Self-heals a stale cached plan exactly as
    /// [`query_params`](Self::query_params) does.
    pub async fn execute_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        // Thin timing wrapper over the inner body (see `query_params`).
        let mut slow = self.armed_slow_guard(sql);
        let result = self.execute_params_inner(sql, params).await;
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// The body of [`execute_params`](Self::execute_params), wrapped by
    /// `execute_params` for slow-query timing.
    async fn execute_params_inner<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        if let Some(idx) = self.dyn_cache.ready_index(sql, P::OIDS) {
            // REUSE: Bind+Execute+Sync on the cached named statement (keyed on
            // (SQL, P::OIDS), so no cross-type reinterpret on reuse).
            if let Some(stmt) = self.dyn_cache.take(idx) {
                match self.execute_prepared(&stmt, params).await {
                    Ok(n) => {
                        self.dyn_cache.restore(idx, stmt);
                        return Ok(n);
                    }
                    // Stale cached plan → reclaim, evict, and TRANSPARENTLY re-run
                    // on the fused path (see `query_params` for the full rationale
                    // — this is a retry of a driver-internal optimization artifact,
                    // not error-masking; any non-stale error surfaces unchanged).
                    Err(e) if e.is_stale_prepared_plan() => {
                        // Surface the self-heal (see `query_params`).
                        self.diag
                            .emit(&crate::diag::DiagEvent::PreparedCacheSelfHeal { sql });
                        self.dyn_cache.remove(idx);
                        self.close_statement(stmt).await?;
                    }
                    Err(e) => {
                        self.dyn_cache.restore(idx, stmt);
                        return Err(e);
                    }
                }
            }
        } else if self.dyn_cache.is_pending(sql, P::OIDS) {
            // PROMOTE (second sighting): declare the caller's encoded parameter
            // types (`P::OIDS`) in the named-statement prepare, matching the fused
            // first sighting (see `query_params_inner`).
            let stmt = self.prepare_with_oids(sql, P::OIDS).await?;
            let out = self.execute_prepared(&stmt, params).await;
            self.dyn_cache.promote(sql, P::OIDS, stmt);
            return out;
        }
        // FIRST sighting — OR a stale-evicted re-warm: fused; note PENDING on success.
        let n = self.execute_params_uncached(sql, params).await?;
        self.dyn_cache.note_pending(sql, P::OIDS);
        Ok(n)
    }

    /// Run a one-shot runtime-SQL command with params in ONE round trip, WITHOUT
    /// the plan cache — the fused primitive behind [`execute_params`](Self::execute_params).
    /// Shares the fused wire with [`query_params_uncached`](Self::query_params_uncached);
    /// the affected count rides the `CommandComplete` tag.
    async fn execute_params_uncached<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .query_params_fused(
                    live,
                    sql,
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            Ok(collector.affected())
        }
        .await;
        scope.disarm();
        out
    }

    /// Close MANY cached statements by raw NAME BYTES in ONE round trip — the
    /// batched peer of [`close_statement`](Self::close_statement) that
    /// [`reset_session`](Self::reset_session) uses to drop BOTH prepared-statement
    /// caches without paying a round trip per statement. Takes the name bytes
    /// (dynamic-cache [`StmtName`]s AND the engine's typed `'static` names share the
    /// one `&[u8]` `Close` form), so the caller keeps the [`PreparedStatement`]s and
    /// drops them after; a `Close` of an already-dropped statement is a wire no-op.
    /// `#[doc(hidden)]`: the pool-reset seam, not a public verb.
    #[doc(hidden)]
    pub async fn close_cached_statements(&mut self, names: &[&[u8]]) -> Result<(), DriverError> {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .close_statements_bytes(
                    live,
                    names,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)
        }
        .await;
        scope.disarm();
        out
    }

    /// Close a prepared statement, consuming it (use-after-close is a move error).
    ///
    /// A handle minted by a DIFFERENT connection is [`DriverError::WrongConnection`]
    /// (checked BEFORE any wire I/O): closing it here would send a `Close` for a
    /// like-named statement on THIS connection, tearing down an unrelated live plan.
    /// The rejected handle is consumed (dropped); its own connection reclaims the
    /// server-side statement at that connection's `reset_session` / disconnect.
    pub async fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        self.check_stmt_origin(&stmt)?;
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .close_statement(
                    live,
                    stmt.inner,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)
        }
        .await;
        scope.disarm();
        out
    }

    // ── Compile-checked typed verbs (the `query!` flagship) ─────────────────

    /// Execute a compile-checked `query!` query for its side effect, returning
    /// the affected-row count (binary-uniform params).
    ///
    /// Everything is derived from the carrier type `Q` — SYMMETRIC with
    /// [`query`](Self::query) (`conn.execute::<Q>(params)`), not a hand-passed
    /// `&Q::PREPARED`. `Q` is a row-shaped `query!` carrier (a SELECT or a
    /// `… RETURNING` write); any RETURNING rows are read-and-ignored (only the
    /// affected count, from the `CommandComplete` tag, is returned) — the batch
    /// peer of this is [`execute_batch`](Self::execute_batch).
    ///
    /// Under `n1-detect` the `caller` the driver captured at the USER call site is
    /// recorded against the query for N+1 detection (diagnostics-only — the
    /// recording never alters the result).
    pub async fn execute<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<u64, DriverError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        let mut slow = self.armed_slow_guard(Q::PREPARED.sql());
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let result = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .query_params(
                    live,
                    &bsql_postgres_proto::prepared::prepared_at::<Q>(),
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)?;
            // Same RESULT-schema guard `query`/`query_collect` runs: a typed cache
            // MISS appended a `Describe`(portal), so fail loud if the fresh Parse's
            // RowDescription revealed a runtime column type diverging from the
            // migration schema — AND drain the parked mismatch so it cannot leak
            // into the next verb's guard (the old `execute<P, R>` armed the guard
            // via `query_params` but never drained it). A drift the caller's
            // RETURNING model no longer matches is a classified `DriverError::Decode`,
            // never a silent success.
            this.take_typed_schema_error()?;
            Ok(collector.affected())
        }
        .await;
        scope.disarm();
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query. Under `n1-detect` records the USER call site.
    pub async fn query<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Rows<Q>, DriverError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        // Slow-query timing for the compile-checked FLAGSHIP (parity with the
        // dynamic verbs): time the whole op, commit only on Ok, report the const
        // SQL (never the params — no PII).
        let mut slow = self.armed_slow_guard(Q::PREPARED.sql());
        let result = self.query_collect::<Q>(params).await;
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// The typed-collect body behind [`query`](Self::query): collects a typed
    /// result into a [`Rows<Q>`] prebuffer. An oversize row (wider than the
    /// engine's inline read buffer) is REASSEMBLED into the prebuffer by
    /// [`RowsBuilder::feed`] and decodes identically to an inline one — no cap.
    /// Records nothing — the N+1 hook fires exactly once in the public verb that
    /// called this. ([`query_one`](Self::query_one) does NOT route through here:
    /// it decodes its single row directly off the wire, with no prebuffer.)
    async fn query_collect<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
    ) -> Result<Rows<Q>, DriverError> {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut builder = RowsBuilder::new();
            let outcome = this
                .engine
                .query_params(
                    live,
                    &bsql_postgres_proto::prepared::prepared_at::<Q>(),
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        builder.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut builder)?;
            // The connection is now idle + pooled. Fail loud if the fresh Parse's
            // RowDescription revealed a runtime column type diverging from the
            // migration schema (the guard drained the result, so `builder` is empty).
            this.take_typed_schema_error()?;
            // An oversize row was reassembled into the prebuffer's `wire` by
            // `RowsBuilder::feed` and is just another contiguous span, so it decodes
            // exactly like an inline row — no cap.
            Ok(builder.finish::<Q>())
        }
        .await;
        scope.disarm();
        out
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record. Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`]. Under `n1-detect` records the USER call site
    /// exactly once.
    ///
    /// Shares the decode-direct streaming path with [`query_opt`](Self::query_opt)
    /// (see [`query_at_most_one`](Self::query_at_most_one)); the two differ ONLY in
    /// the zero-row outcome — `query_one` rejects it as
    /// [`NoRows`](DriverError::NoRows), `query_opt` returns `Ok(None)`.
    pub async fn query_one<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Q::Owned, DriverError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        let mut slow = self.armed_slow_guard(Q::PREPARED.sql());
        let result = self
            .query_at_most_one::<Q>(params)
            .await
            .and_then(|opt| opt.ok_or(DriverError::NoRows));
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// Run a compile-checked `query!` expecting AT MOST one row, returning the
    /// owned record if present or `None` if absent — the by-key maybe-absent
    /// shape, the flagship's most common cardinality. More than one row is
    /// [`DriverError::TooManyRows`]. Under `n1-detect` records the USER call site
    /// exactly once.
    ///
    /// The zero-or-one peer of [`query_one`](Self::query_one): it shares the exact
    /// decode-direct streaming path (see
    /// [`query_at_most_one`](Self::query_at_most_one)) and differs ONLY in that
    /// zero rows is `Ok(None)` rather than [`NoRows`](DriverError::NoRows). All
    /// other precedence is identical — a second row still dominates as
    /// [`TooManyRows`](DriverError::TooManyRows), and a lone malformed row is
    /// [`Decode`](DriverError::Decode).
    pub async fn query_opt<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Option<Q::Owned>, DriverError> {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        let mut slow = self.armed_slow_guard(Q::PREPARED.sql());
        let result = self.query_at_most_one::<Q>(params).await;
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// The shared zero-or-one decode-direct body behind
    /// [`query_one`](Self::query_one) and [`query_opt`](Self::query_opt): stream a
    /// compile-checked `query!` and return `Some(owned)` for exactly one row,
    /// `Ok(None)` for zero rows, or [`TooManyRows`](DriverError::TooManyRows) for
    /// two or more. Records NOTHING — the N+1 hook fires once in whichever public
    /// verb called this.
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
    /// An oversize FIRST row (wider than the engine's inline read buffer, so
    /// streamed as `RowChunk` pieces) is REASSEMBLED into a scratch `Vec` and
    /// decoded from there — no cap, no prebuffer for the common small single-row
    /// case (the scratch stays unallocated until the first chunk). A completed
    /// oversize row counts as a row exactly like an inline one, so a SECOND row
    /// (inline or oversize) is still the too-many condition.
    ///
    /// Error precedence: a second row is
    /// [`TooManyRows`](DriverError::TooManyRows) — dominating even a malformed
    /// first row, since the old `_ => TooManyRows` arm never decoded a >1-row
    /// result (so a first-row decode failure is PARKED, not raised, while a
    /// second row is still awaited); a lone malformed row is
    /// [`Decode`](DriverError::Decode); zero rows is `Ok(None)` (mapped to
    /// [`NoRows`](DriverError::NoRows) only by [`query_one`](Self::query_one)).
    async fn query_at_most_one<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
    ) -> Result<Option<Q::Owned>, DriverError> {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
        let this = self;
        // The single decoded row (owned, so it outlives the pump), plus the
        // read-after-settle flags the streaming sink parks.
        let mut row: Option<Q::Owned> = None;
        let mut seen_first = false;
        let mut decode_err: Option<DecodeError> = None;
        let mut db_error: Option<DbError> = None;
        // Reassembly scratch for an oversize FIRST row (chunk-streamed). Stays
        // UNALLOCATED for the common small single-row case (no chunk ever
        // arrives), so the zero-prebuffer fast path is unchanged.
        let mut oversize_scratch: Vec<u8> = Vec::new();
        let outcome = this
            .engine
            .query_params_break(
                live,
                &bsql_postgres_proto::prepared::prepared_at::<Q>(),
                params,
                capture_notify(&mut this.notifications, this.diag.sink(), |surface| match surface {
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
                    // An oversize row streams as `RowChunk` pieces. If a whole row
                    // was already seen, this chunk begins a SECOND row → stop for
                    // too-many (drained below), never accumulating it. Otherwise
                    // reassemble the FIRST row's chunks into the scratch `Vec`.
                    Surface::RowChunk(bytes) => {
                        if seen_first {
                            return ControlFlow::Break(());
                        }
                        oversize_scratch.extend_from_slice(bytes);
                        ControlFlow::Continue(())
                    }
                    // The reassembled first oversize row is complete: decode it
                    // from the contiguous scratch exactly as an inline first row,
                    // and count it (so a following row is still too-many). Only
                    // reachable with `seen_first` false — a second oversize row is
                    // stopped at its first `RowChunk` above, never reaching here.
                    Surface::RowChunkEnd => {
                        seen_first = true;
                        match Q::decode_owned(&oversize_scratch) {
                            Ok(owned) => row = Some(owned),
                            Err(de) => decode_err = Some(de),
                        }
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
                this.live = Some(live);
                // A typed result-schema mismatch (caught at the fresh Parse's
                // RowDescription) drained the rows before any reached the sink, so
                // it dominates the (empty) row/decode outcome — fail loud rather
                // than return `Ok(None)`.
                this.take_typed_schema_error()?;
                match (row, decode_err) {
                    (Some(owned), _) => Ok(Some(owned)),
                    (None, Some(de)) => Err(DriverError::Decode(de)),
                    // Zero rows: `Ok(None)`. `query_one` maps this to `NoRows`;
                    // `query_opt` returns it verbatim.
                    (None, None) => Ok(None),
                }
            }
            Boundary::Failed => {
                // Server error: drain the recovering `ReadyForQuery`, then surface
                // the parsed cause. Connection stays alive + pooled.
                this.drain_to_idle(live).await?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(()) => {
                // Broke on the second row (inline or the first chunk of a second
                // oversize row): drain to reclaim, then classify as too-many.
                this.drain_to_idle(live).await?;
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
        .await;
        scope.disarm();
        out
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query). Under
    /// `n1-detect` records the USER call site.
    ///
    /// See each driver's `query_each` for the full contract (return values,
    /// early-abort cost, decode/oversize/server-error handling).
    pub async fn query_each<'p, Q, F, E>(
        &mut self,
        params: Q::Params<'p>,
        mut on_row: F,
        #[cfg(feature = "n1-detect")] caller: &'static core::panic::Location<'static>,
    ) -> Result<Option<E>, DriverError>
    where
        Q: TypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), caller);
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let stream_result = async move {
        let this = self;
        // Captured across the streaming sink; read after the verb settles.
        let mut db_error: Option<DbError> = None;
        // Reassembly scratch for an oversize row (chunk-streamed), REUSED across
        // oversize rows (cleared after each), so streaming stays constant-memory
        // — bounded by the widest oversize row, not the whole result. Stays
        // unallocated while every row fits one buffer fill.
        let mut oversize_scratch: Vec<u8> = Vec::new();
        let outcome = this
            .engine
            .query_params_break(
                live,
                &bsql_postgres_proto::prepared::prepared_at::<Q>(),
                params,
                capture_notify(&mut this.notifications, this.diag.sink(), |surface| match surface {
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
                    // An oversize row streams as `RowChunk` pieces: reassemble them
                    // into the reused scratch buffer.
                    Surface::RowChunk(bytes) => {
                        oversize_scratch.extend_from_slice(bytes);
                        ControlFlow::Continue(())
                    }
                    // The reassembled oversize row is complete: decode the borrowed
                    // record from the contiguous scratch and hand it to `on_row`
                    // exactly as an inline row, then clear the scratch to reuse its
                    // allocation for the next oversize row (the borrow via `rec`
                    // ends when `on_row` returns, before the clear). A decode
                    // failure is LOUD; an `on_row` break rides `Stop::User`.
                    Surface::RowChunkEnd => {
                        let flow = match Q::decode_borrowed(&oversize_scratch) {
                            Ok(rec) => match on_row(rec) {
                                ControlFlow::Continue(()) => ControlFlow::Continue(()),
                                ControlFlow::Break(e) => ControlFlow::Break(Stop::User(e)),
                            },
                            Err(de) => ControlFlow::Break(Stop::Decode(de)),
                        };
                        oversize_scratch.clear();
                        flow
                    }
                    // COPY / delivery / other async frames are not stream rows (a
                    // NOTIFY is captured into the ledger by the wrapper above this
                    // match, so it never reaches here to be dropped).
                    _ => ControlFlow::Continue(()),
                }),
            )
            .await;
        let out = this.finish_stream(outcome, db_error).await?;
        // Fail loud if the fresh Parse's RowDescription revealed a runtime column
        // type diverging from the migration schema: the guard drained the rows
        // before any reached `on_row`, so `out` is `None` (no garbage row was
        // yielded) and the mismatch dominates. Checked HERE (not in the shared
        // `finish_stream`) because the carrier `Q` recovers the expected OID.
        this.take_typed_schema_error()?;
        Ok(out)
        }
        .await;
        scope.disarm();
        stream_result
    }

    /// The shared post-pump settle for EVERY streaming verb (typed
    /// [`query_each`](Self::query_each) and the dynamic
    /// [`query_each_raw`](Self::query_each_raw) / [`query_each_params`](Self::query_each_params)):
    /// classify the RAW [`Boundary`] the pump reached, draining a dirty
    /// connection back to a clean idle so it stays pooled, and map the outcome.
    ///
    /// Defined ONCE so the three streaming verbs cannot drift in how they reclaim
    /// the connection or classify the stop — they differ only in the engine verb
    /// that produced the boundary and the per-row decode, never in the settle.
    ///
    /// - [`Boundary::Idle`] → `Ok(None)` (streamed to completion; token restored).
    /// - [`Boundary::Failed`] → drain, then the parsed server error.
    /// - [`Boundary::Stopped(Stop::User(e))`] → drain, then `Ok(Some(e))` (the
    ///   caller's early-break payload).
    /// - [`Boundary::Stopped(Stop::Decode(de))`] → drain, then the loud classified
    ///   decode error.
    /// - a fatal boundary (the breakable engine verbs map Closed/Suspended to
    ///   `Err`, so they never ride an `Ok` outcome) → a classified I/O error; the
    ///   token is dropped, leaving the connection dead + evictable.
    async fn finish_stream<E>(
        &mut self,
        outcome: Result<Outcome<'static, Boundary<Stop<E>>>, EngineError<WireError>>,
        db_error: Option<DbError>,
    ) -> Result<Option<E>, DriverError> {
        // The token rides `Ok` on any ALIVE boundary; a fatal is `Err`.
        let (live, boundary) = match outcome {
            Ok(Outcome { live, status }) => (live, status),
            Err(other) => return Err(lift_engine_error(other)),
        };
        match boundary {
            Boundary::Idle => {
                // Streamed to completion at a clean idle — no drain needed. (A typed
                // `query_each` checks its result-schema guard in the verb itself,
                // where the carrier `Q` is in scope — `finish_stream` is shared with
                // the dynamic streaming verbs, which never arm the guard.)
                self.live = Some(live);
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
                Ok(Some(e))
            }
            Boundary::Stopped(Stop::Decode(de)) => {
                // A per-row decode failure broke the stream: drain to reclaim, then
                // surface the loud classified decode error.
                self.drain_to_idle(live).await?;
                Err(DriverError::Decode(de))
            }
            // The breakable engine verbs map Closed/Suspended to a fatal `Err`, so
            // they never ride an `Ok` outcome; `Boundary` is `#[non_exhaustive]`, so
            // this classified arm also covers any future boundary. The token is
            // dropped (not restored), so the connection is left dead + evictable.
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a streaming query",
            ))),
        }
    }

    /// Stream a runtime raw-SQL query's rows one at a time to `on_row` in CONSTANT
    /// memory — the dynamic (untyped) streaming peer of
    /// [`query_raw`](Self::query_raw), and the PostgreSQL peer of the SQLite
    /// driver's `query_each_raw`.
    ///
    /// See each driver's `query_each_raw` for the full contract; the mechanism is
    /// the typed [`query_each`](Self::query_each)'s breakable cursor over the
    /// dynamic simple-query engine verb, lending a zero-copy [`BorrowedRow`] per
    /// row (nothing accumulated).
    pub async fn query_each_raw<F, E>(
        &mut self,
        sql: &str,
        mut on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut db_error: Option<DbError> = None;
            // Reused per-row scratch: the slot table (offsets into the row body) and
            // the oversize-row reassembly buffer. Both retain their capacity across
            // rows (cleared, never reallocated), so streaming a colossal result
            // allocates NOTHING per row — the constant-memory invariant.
            let mut slots: Vec<ColSlot> = Vec::new();
            let mut oversize: Vec<u8> = Vec::new();
            let outcome = this
                .engine
                .query_break(
                    live,
                    sql,
                    capture_notify(&mut this.notifications, this.diag.sink(), |surface| {
                        stream_dynamic_row(surface, &mut on_row, &mut slots, &mut oversize, &mut db_error)
                    }),
                )
                .await;
            this.finish_stream(outcome, db_error).await
        }
        .await;
        scope.disarm();
        out
    }

    /// Stream a runtime parameterised query's rows one at a time to `on_row` in
    /// CONSTANT memory — the dynamic streaming peer of
    /// [`query_params`](Self::query_params), and the PostgreSQL peer of the SQLite
    /// driver's `query_each_params`.
    ///
    /// Rides the FUSED one-round-trip dynamic path (the same wire the one-shot
    /// [`query_params`](Self::query_params) first-sighting uses); a streaming bulk
    /// read is one-shot by nature, so it deliberately does NOT touch the dynamic
    /// prepared-statement cache. Params are borrowed all the way to the engine.
    pub async fn query_each_params<P, F, E>(
        &mut self,
        sql: &str,
        params: &P,
        mut on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        P: ParamsWriter,
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut db_error: Option<DbError> = None;
            let mut slots: Vec<ColSlot> = Vec::new();
            let mut oversize: Vec<u8> = Vec::new();
            let outcome = this
                .engine
                .query_params_fused_break(
                    live,
                    sql,
                    params,
                    capture_notify(&mut this.notifications, this.diag.sink(), |surface| {
                        stream_dynamic_row(surface, &mut on_row, &mut slots, &mut oversize, &mut db_error)
                    }),
                )
                .await;
            this.finish_stream(outcome, db_error).await
        }
        .await;
        scope.disarm();
        out
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
                capture_notify(&mut self.notifications, self.diag.sink(), |_s: Surface<'_>| {
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

    // ── Shared windowed-batch drive (pipeline / execute_batch / query_batch) ──

    /// Drive ONE window of a batch to its inter-command boundary — the SINGLE
    /// shared flush + drain + compact all three windowed verbs (`pipeline`,
    /// `execute_batch`, `query_batch`) route every window through, so the delicate
    /// window boundary handling (and the co-window-deadlock fix's isolate flushes)
    /// lives ONCE, not copy-pasted three times.
    ///
    /// Appends the window's trailing `Flush` (forcing the window's responses out
    /// WITHOUT ending the implicit transaction — only the batch's single trailing
    /// `Sync` does that, so all commands stay ONE atomic transaction), then drives
    /// the (optionally result-OID-GUARDED) breakable pump with the caller's `sink`.
    /// `sink` routes each surface to the verb's collector and BREAKS
    /// ([`ControlFlow::Break`]) once the window's delivery target is reached
    /// (leaving the engine at a clean `PipelineAwaitingNextOrRfq` boundary), or
    /// parks a server error and continues (the trailing `Sync` recovers it).
    ///
    /// `guarded = true` (heterogeneous `pipeline` / `query_batch`) uses the BAILING
    /// drive: a cache-MISS command whose result schema drifted parks a mismatch
    /// whose silent swallow-to-`ReadyForQuery` has no RFQ in an intermediate window
    /// (only a `Flush`), so the guard returns [`Boundary::Failed`] rather than
    /// blocking forever — the caller stages the trailing `Sync` to drain it.
    /// `guarded = false` (`execute_batch`, which discards its RETURNING rows and
    /// cannot park a mismatch) uses the inert non-bailing drive, byte-identical to
    /// the historical drive.
    ///
    /// On a clean drain ([`Boundary::Stopped`]) the send buffer is COMPACTED (the
    /// already-sent window bytes are dropped, capacity retained) so a long batch's
    /// send buffer stays bounded across windows, and returns [`WindowStep::Drained`];
    /// any other alive boundary (a parked server error, a guard BAIL, or a
    /// fail-closed unexpected boundary) returns [`WindowStep::Halt`] and the caller
    /// breaks to the trailing `Sync`. A FATAL transport/protocol fault consumes the
    /// token and is an `Err` (the connection is dead).
    async fn flush_window(
        &mut self,
        live: Live<'static>,
        guarded: bool,
        sink: impl FnMut(Surface<'_>) -> ControlFlow<()>,
    ) -> Result<(Live<'static>, WindowStep), DriverError> {
        self.engine.stage_flush();
        let notifying = capture_notify(&mut self.notifications, self.diag.sink(), sink);
        // The GUARDED drive bails on a parked result-OID mismatch (an intermediate
        // window has no `Sync` to reach); the unguarded one is inert (no mismatch is
        // ever parked). `<_, ()>`: the sink's break payload is `()`.
        let outcome = if guarded {
            self.engine
                .run_pipeline_break_guarded::<_, ()>(live, notifying)
                .await
        } else {
            self.engine
                .run_pipeline_break::<_, ()>(live, notifying)
                .await
        };
        let (live, status) = match outcome {
            Ok(Outcome { live, status }) => (live, status),
            Err(other) => {
                core::hint::cold_path();
                return Err(lift_engine_error(other));
            }
        };
        match status {
            // The window drained cleanly to its inter-command boundary. Compact the
            // (fully-sent) send buffer so it does not accumulate this window's bytes
            // for the batch's life, then signal "stage the next window".
            Boundary::Stopped(()) => {
                self.engine.compact_send_buf();
                Ok((live, WindowStep::Drained))
            }
            // A command in this window FAILED — a parked server `ErrorResponse`, OR a
            // guarded window's result-OID mismatch BAIL — OR an unexpected non-`Idle`
            // alive boundary (a `Flush`-terminated window cannot reach a clean `Idle`;
            // `Boundary` is `#[non_exhaustive]`, so this is fail-closed against a
            // future boundary, never a torn success). The caller breaks to the
            // trailing `Sync` + final drain, and the settle classifies which.
            _ => {
                core::hint::cold_path();
                Ok((live, WindowStep::Halt))
            }
        }
    }

    /// ISOLATE an OVERSIZE command from a co-window prefix — the shared fix for the
    /// SINGLE-OVERSIZE-COMMAND class of the windowed batch write-path deadlock. When a
    /// just-staged command's OWN `Bind` frame ALONE crosses the threshold (`k_size >=
    /// BATCH_WINDOW_THRESHOLD`) AND a non-empty prefix precedes it, that command must
    /// NOT share a flush with the prefix: an EARLY prefix command returning a LARGE
    /// result blocks the server's send buffer while the client blocks writing the
    /// oversize command's `Bind` — a bidirectional write-path deadlock (each end
    /// blocked writing, neither reading). This is the SEVERE, UNBOUNDED case (one
    /// command's Bind can be arbitrarily large, e.g. tens of MiB past any send
    /// buffer), and it is the case this isolate fully eliminates.
    ///
    /// The just-staged oversize command's `k_size` frame bytes are the pending TAIL
    /// (staging appends contiguously). This LIFTS them out (`split_last_staged`), so
    /// the PREFIX flushes + drains ALONE via [`flush_window`](Self::flush_window)
    /// (the client reads the prefix's large result before it can write-block on the
    /// oversize command); on a clean prefix drain it RE-STAGES the lifted bytes
    /// verbatim into the now-compacted, FRESH window (`restage_bytes`), where the
    /// oversize command is ISOLATED — a single command never self-deadlocks (the
    /// server reads its whole `Bind`, unblocking the client, before producing any
    /// result). Only WIRE BYTES move; the command's engine-side seat / guard-OID
    /// FIFO push established at its original staging is UNTOUCHED, so the receive FSM
    /// guards / decodes it correctly when the isolated window is later drained.
    ///
    /// Returns [`WindowStep::Drained`] with the lifted bytes re-staged (the caller
    /// then flushes the isolated command as its own window), or [`WindowStep::Halt`]
    /// if the PREFIX drain failed (a server error / guard mismatch in the prefix) —
    /// in which case the lifted bytes are DISCARDED (the command's frames are never
    /// sent; the batch is failing and the settle reports the prefix's cause).
    ///
    /// BOUNDED RESIDUAL (pre-existing, intentional): the isolate triggers ONLY on a
    /// single command whose OWN frame crosses the threshold. A window of MULTIPLE
    /// commands each UNDER the threshold but cumulatively up to ~`2 ×
    /// BATCH_WINDOW_THRESHOLD` (~126 KiB) is NOT isolated, so co-windowed with an
    /// early large-RESULT command it can still deadlock IFF the combined client-send +
    /// server-recv socket buffers are below ~126 KiB — narrow (needs sub-128 KiB tuned
    /// buffers, never default-autotuned Linux/loopback), BOUNDED (the window is capped
    /// at ~2×threshold, unlike the unbounded single-Bind case fixed here), and
    /// PRE-EXISTING (the pre-fix window sizing was identical — this is a strict
    /// improvement, not a regression). Fully closing it would require draining after
    /// EVERY command (1 RTT each), defeating pipelining, so it is a documented limit.
    async fn isolate_prefix(
        &mut self,
        live: Live<'static>,
        guarded: bool,
        k_size: usize,
        sink: impl FnMut(Surface<'_>) -> ControlFlow<()>,
    ) -> Result<(Live<'static>, WindowStep), DriverError> {
        core::hint::cold_path();
        // Lift the oversize command's frame bytes OUT of the buffer, leaving only the
        // prefix pending. Its engine seat / guard-OID push stay (only bytes move).
        let isolated = self.engine.split_last_staged(k_size);
        // Flush + drain the PREFIX alone (window target = the prefix commands, set by
        // the caller's `sink`). The client reads the prefix's response before it can
        // write-block on the oversize command — the single-oversize-command deadlock
        // cannot form (the bounded multi-command residual is documented above).
        let (live, step) = self.flush_window(live, guarded, sink).await?;
        if step == WindowStep::Drained {
            // Prefix drained cleanly into a compacted buffer — re-stage the oversize
            // command's frames verbatim into the FRESH window, where it is isolated.
            self.engine.restage_bytes(&isolated);
        }
        // On `Halt` the lifted bytes are dropped (never sent): the batch is failing,
        // and the caller breaks to the trailing `Sync` so the settle reports it.
        Ok((live, step))
    }

    // ── Heterogeneous atomic pipelining ─────────────────────────────────────

    /// Stage ONE pipelined command's frames onto the engine — the per-command seam
    /// the [`Pipeline`] tuple impls call (element `0` with `first = true`). Records
    /// the command's content-addressed statement name into `plan` for the batch
    /// cache settle. `#[doc(hidden)]`: a driver-facing staging seam, not a consumer
    /// API (a consumer builds a batch tuple of [`Bound`]s and calls
    /// [`pipeline`](Self::pipeline)).
    #[doc(hidden)]
    pub fn stage_pipeline_cmd<Q: TypedQuery>(
        &mut self,
        bound: &Bound<'_, Q>,
        first: bool,
        plan: &mut Vec<&'static str>,
    ) -> Result<(), DriverError> {
        // Re-type the const `PREPARED` to the caller's param lifetime (byte-identical
        // wire fields — the OIDs / templates are `'static`), exactly as the serial
        // typed verbs do via `prepared_at`.
        let prepared = bsql_postgres_proto::prepared::prepared_at::<Q>();
        plan.push(prepared.stmt_name());
        self.engine
            .stage_pipeline_command(&prepared, bound.params(), first)
            .map_err(lift_engine_error)
    }

    /// Run a HETEROGENEOUS ATOMIC pipeline — the N compile-checked `query!` commands
    /// of `batch` sent with ONE trailing `Sync`, forming a SINGLE implicit
    /// transaction, returning one [`Rows<Qi>`](crate::Rows) per command.
    ///
    /// # Airtight all-or-nothing (STRUCTURAL, not by discipline)
    ///
    /// The whole batch commits and returns every result, or it errors and returns
    /// ZERO — because on a mid-batch error the server ROLLS BACK the commands before
    /// the failure, errors the failing one, and SKIPS the rest (PG §55.2.3). The
    /// ONLY code path that builds the `Ok((Rows<Q0>, …))` tuple is reached AFTER the
    /// pump returns the batch-final clean `ReadyForQuery`
    /// ([`CommandStatus::Completed`]), which the server emits only if the whole
    /// implicit transaction COMMITTED. So a rolled-back / failing / skipped command
    /// can NEVER be materialised into an `Ok` — the provisional per-command row
    /// prebuffers are DISCARDED on any failure. A mid-batch failure is
    /// [`DriverError::BatchFailed`] naming the ZERO-BASED index of the failing
    /// command (read via [`DriverError::batch_failed_index`]).
    ///
    /// # Transaction state on error — CONSISTENT with a normal failed verb
    ///
    /// `pipeline` does NOT special-case the transaction: it leaves the connection
    /// EXACTLY as any other verb leaves it. A COMMON implicit-tx batch (no explicit
    /// `BEGIN`) has its single trailing `Sync` close the implicit transaction, so
    /// `tx_status` is `Idle` and the connection is immediately clean + reusable. A
    /// batch inside an EXPLICIT transaction (a `transaction` guard, or a manual
    /// `BEGIN` in the batch) leaves that transaction ABORTED (`'E'`), UNTOUCHED —
    /// the OWNER of the transaction rolls it back (the guard at closure exit, a
    /// pooled `reset_session` on checkin, or a direct caller's `rollback`). Auto-
    /// rolling-back here would make `pipeline` the ONLY verb that clears a caller's
    /// aborted transaction, so a caller who ignored the error and issued another
    /// verb inside a guard would silently run it in AUTOCOMMIT instead of getting a
    /// loud `25P02` — a silent transaction escape. Leaving it `'E'` keeps the next
    /// in-guard verb failing loudly, exactly as a normal failed verb does.
    ///
    /// # Errors
    ///
    /// - [`DriverError::BatchFailed`] — a specific command failed (a `Bind`/`Execute`
    ///   error); the whole batch rolled back (all-or-nothing). Names the failing
    ///   command's index (always `< arity`) + the server cause.
    /// - [`DriverError::Db`] — a COMMIT-TIME failure: every command succeeded at
    ///   Execute and the implicit COMMIT at the trailing `Sync` failed (a
    ///   `DEFERRABLE INITIALLY DEFERRED` constraint, a serialization failure). The
    ///   whole batch rolled back (zero results); the failure belongs to no single
    ///   command, so [`batch_failed_index`](DriverError::batch_failed_index) is
    ///   `None`.
    /// - a FATAL transport/protocol/EOF error — the connection is dead
    ///   ([`is_disconnect`](DriverError::is_disconnect) is `true`).
    pub async fn pipeline<'p, B: Pipeline<'p>>(
        &mut self,
        batch: B,
    ) -> Result<B::Output, DriverError> {
        let arity = B::ARITY;
        let mut plan: Vec<&'static str> = Vec::with_capacity(arity);
        let mut builders: Vec<RowsBuilder> = (0..arity).map(|_| RowsBuilder::new()).collect();
        let mut current: usize = 0; // delivered commands (global, 0-based)
        let mut db_error: Option<DbError> = None;
        let mut failed_index: Option<usize> = None;

        // Recover a prior dropped-future connection + take the token FIRST — BEFORE
        // staging command 0 onto the send buffer, so if this connection was left dirty
        // by an EARLIER dropped future, `begin_command`'s recovery drain (which flushes
        // the send buffer) never flushes command 0's staged bytes prematurely. The
        // whole staging + windowed drive + settle then runs under the dropped-future
        // guard: a batch future dropped mid-flight owes an owed-reply DRAIN on the next
        // use; an early `return` inside the block resolves it normally (the scope
        // disarms after), so only a DROP mid-block leaves the connection recoverable.
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let batch_result = async move {
        let mut live = live;

        // 1. STAGE command 0 (`first = true`: reset the buffer + seat pipeline mode).
        //    A build overflow (a > 2 GiB frame) leaves NOTHING flushed, so the
        //    connection is healthy — discard the partial staging and restore a clean
        //    idle. `ARITY >= 1`, so command 0 always exists.
        if let Err(e) = batch.stage_nth(self, 0, &mut plan) {
            core::hint::cold_path();
            self.engine.abort_pipeline_staging();
            // We already hold the token — restore it so the connection stays healthy
            // (nothing was flushed).
            self.live = Some(live);
            return Err(e);
        }
        let mut staged: usize = 1; // staged commands (command 0 above)
        let mut flushed_any = false;

        // 2. WINDOWED DRIVE — the deadlock-free peer of `execute_batch`'s, over the
        //    ONE shared [`flush_window`](Self::flush_window) / [`isolate_prefix`](Self::isolate_prefix)
        //    windowing helpers. A heterogeneous batch (an early LARGE result + later
        //    LARGE params) would DEADLOCK if the whole batch were staged and flushed
        //    before a single read (the client blocks writing the tail while the server
        //    blocks writing the early result). So the commands STREAM: stage until the
        //    send buffer crosses the batcher threshold, then `Flush` + DRAIN that
        //    window's responses (routing each command's rows to ITS builder) before
        //    staging the next. The COMMON batch (fits one window) stages every command,
        //    never flushes an intermediate `Flush`, and rides the single trailing
        //    `Sync` below — ~1 round trip, byte-identical to a single fused query.
        //    An OVERSIZE command that alone crosses the threshold on top of a non-empty
        //    prefix is ISOLATED (`isolate_prefix`): the prefix is flushed + drained
        //    ALONE first, so the oversize command never shares a flush with a
        //    large-result prefix — the SINGLE-OVERSIZE-COMMAND (unbounded) co-window
        //    deadlock cannot form. A bounded multi-command residual (a window of small
        //    commands up to ~2×threshold co-windowed with a large result, only on
        //    sub-128 KiB tuned socket buffers) is pre-existing + documented on
        //    [`isolate_prefix`](Self::isolate_prefix).
        'windows: loop {
            // Fill the current window: stage subsequent commands until the send
            // buffer crosses the batcher threshold or every command is staged.
            let mut window_full = false;
            while staged < arity {
                let before = self.engine.pending_send_len();
                if let Err(e) = batch.stage_nth(self, staged, &mut plan) {
                    core::hint::cold_path();
                    // A single command's `Bind` frame exceeded the wire length field.
                    if flushed_any {
                        // A window was already flushed: the implicit transaction is
                        // OPEN with committed-nothing partial commands. Sending a
                        // `Sync` would COMMIT the partial (breaking all-or-nothing),
                        // so the connection MUST die — returning WITHOUT restoring
                        // `self.live` leaves it NotReady (the token `live` drops), and
                        // its socket close rolls the open implicit transaction back.
                        // All-or-nothing preserved at the cost of the connection.
                        return Err(e);
                    }
                    // First window, nothing flushed: clean abort (a deferred `BEGIN`
                    // preserved), restore the token, stay healthy.
                    self.engine.abort_pipeline_staging();
                    self.live = Some(live);
                    return Err(e);
                }
                staged = staged.saturating_add(1);
                let after = self.engine.pending_send_len();
                if after >= BATCH_WINDOW_THRESHOLD {
                    let k_size = after.saturating_sub(before);
                    if before > 0 && k_size >= BATCH_WINDOW_THRESHOLD {
                        // OVERSIZE-command CO-WINDOW: this command's OWN frame alone
                        // crossed the threshold on top of a non-empty prefix. Flushing
                        // both together risks the write-path deadlock (an early prefix
                        // command's large result blocks the server while the client
                        // write-blocks on this command's Bind). ISOLATE it: flush +
                        // drain the PREFIX alone, then this command rides its own fresh
                        // window (below). The prefix target excludes this command.
                        // `flushed_any` is set by the guaranteed intermediate flush
                        // below (`window_full` forces it), not here: on `Halt` it is
                        // not read (we break to the trailing `Sync`).
                        let prefix_target = staged.saturating_sub(1);
                        let (l, step) = self
                            .isolate_prefix(live, true, k_size, |surface| {
                                feed_typed_window(
                                    surface,
                                    &mut current,
                                    prefix_target,
                                    &mut builders,
                                    &mut failed_index,
                                    &mut db_error,
                                )
                            })
                            .await?;
                        live = l;
                        if step == WindowStep::Halt {
                            break 'windows;
                        }
                        // The oversize command is now re-staged ALONE in a fresh
                        // window; flush it as its own window below (never co-windowed).
                    }
                    window_full = true;
                    break;
                }
            }
            if !window_full {
                // Every command staged — this is the FINAL window; it rides the
                // trailing `Sync` below, not a `Flush`.
                break 'windows;
            }

            // INTERMEDIATE window: `Flush` + drain (routing rows to builders) via the
            // shared `flush_window`. The sink breaks once every command staged so far
            // has delivered, leaving the engine at a clean inter-command boundary; the
            // GUARDED drive BAILS (`Halt`) on a MISS command's parked result-schema
            // mismatch (an intermediate window has no `Sync` to reach, so the silent
            // drain would otherwise block forever). On `Halt` (a parked server error,
            // a guard bail, or a fail-closed unexpected boundary) stop staging; the
            // trailing `Sync` + final drain recovers the connection and the settle
            // classifies which. HONEST NOTE: on a mismatch this stops staging later
            // windows, so the server commits only the windows flushed so far — but a
            // mismatch returns ZERO results + the classified drift regardless.
            flushed_any = true;
            let window_target = staged;
            let (l, step) = self
                .flush_window(live, true, |surface| {
                    feed_typed_window(
                        surface,
                        &mut current,
                        window_target,
                        &mut builders,
                        &mut failed_index,
                        &mut db_error,
                    )
                })
                .await?;
            live = l;
            if step == WindowStep::Halt {
                core::hint::cold_path();
                break 'windows;
            }
        }

        // 3. FINAL DRIVE: the ONE trailing `Sync` closes the batch. Sent whether the
        //    loop ended cleanly (drive the final window's remaining commands + the
        //    batch RFQ) or aborted mid-batch (the server is skipping-to-`Sync` after
        //    an error, or draining a parked mismatch — the `Sync` produces the
        //    recovering RFQ the parked drain reads). Routes rows to builders (none
        //    arrive after a mismatch/error — all swallowed by the drain).
        self.engine.stage_pipeline_seal();
        let outcome = self
            .engine
            .run_pipeline(
                live,
                capture_notify(&mut self.notifications, self.diag.sink(), |surface| {
                    match surface {
                        Surface::Row(_) | Surface::RowChunk(_) | Surface::RowChunkEnd => {
                            if let Some(b) = builders.get_mut(current) {
                                b.feed(surface);
                            }
                        }
                        Surface::Deliver { .. } => {
                            if let Some(b) = builders.get_mut(current) {
                                b.feed(surface);
                            }
                            current = current.saturating_add(1);
                        }
                        Surface::Fail(body) if failed_index.is_none() => {
                            failed_index = Some(current);
                            db_error = Some(materialize::parse_error_response(body));
                        }
                        _ => {}
                    }
                    ControlFlow::Continue(())
                }),
            )
            .await;
        let live = match outcome {
            Ok(Outcome { live, .. }) => live,
            // FATAL: the verb consumed the token and the connection is dead.
            Err(other) => {
                core::hint::cold_path();
                return Err(lift_engine_error(other));
            }
        };
        self.live = Some(live);

        // 4. SETTLE — driven by the PARKED failure + the guard mismatch, NOT the final
        //    boundary (which is `Idle` even after a mid-batch failure's recovery
        //    drain, exactly as `execute_batch`). Priority: a result-schema mismatch (a
        //    CLIENT-side rejection that may have committed server-side) first, then a
        //    server / commit-time failure, then the clean `Ok`.
        //
        // TYPED RESULT-SCHEMA guard: a cache-MISS command whose runtime result columns
        // diverged from its carrier `Qi`'s migration schema was caught at ITS
        // `RowDescription` and the batch drained to a clean idle (the same over-cap
        // drain the single-query guard reuses), so no garbage row ever reached a
        // builder. `current` is the failing command's zero-based index — frozen at the
        // mismatch, since the drain surfaces no further `Deliver`. A MISS whose guard
        // fired is NOT recorded in the cache (a repeat re-`Describe`s + re-guards).
        if let Some((column, found, expected)) = self.engine.take_result_oid_mismatch() {
            core::hint::cold_path();
            // `current < arity <= 16`, so the `u16` never saturates; the `Err` arm is
            // a total-conversion floor, never reached (no `as`, no `unwrap`).
            // `unwrap_or` is banned by the silent-fallback ledger, so this explicit
            // match is the sanctioned dead arm (the same shape `DbError::code` uses).
            #[expect(
                clippy::manual_unwrap_or,
                reason = "unwrap_or is a disallowed method; this explicit match is the \
                          sanctioned dead-arm narrow — `current < arity <= 16`, so the \
                          `Err` view is unreachable, never a masked failure"
            )]
            let command = match u16::try_from(current) {
                Ok(c) => c,
                Err(_) => u16::MAX,
            };
            return Err(DriverError::BatchColumnOidMismatch {
                command,
                source: DecodeError::ColumnOidMismatch {
                    index: column,
                    expected,
                    found,
                },
            });
        }
        match (failed_index, db_error) {
            // No failure: the whole implicit transaction committed. THE ONLY `Ok`
            // path. Record each MISS statement (deduped) IF the batch left the
            // connection at `Idle` tx status (mirrors the serial cache rule; inside an
            // explicit transaction it defers, since a rollback could drop it).
            (None, _) => {
                if matches!(self.engine.tx_status(), Ok(TxStatus::Idle)) {
                    for &name in &plan {
                        self.engine.record_pipeline_statement(name);
                    }
                }
                B::finish(builders)
            }
            // A mid-batch / commit-time server failure. The N commands are ONE
            // implicit transaction, so the server ROLLED BACK every command; the
            // provisional builders are DISCARDED (never an `Ok`). The connection is
            // left EXACTLY as a normal failed verb leaves it — `pipeline` does NOT
            // special-case the transaction state (auto-rolling-back here would make
            // `pipeline` the ONLY verb that clears a caller's aborted transaction, so
            // a caller who IGNORED the error inside a guard would run the next verb in
            // AUTOCOMMIT instead of getting a loud `25P02`). Self-heal: evict every
            // referenced statement so a next attempt re-Parses (a no-op for MISS
            // names, not cached).
            (Some(index), Some(db)) => {
                core::hint::cold_path();
                for &name in &plan {
                    self.engine.evict_pipeline_statement(name);
                }
                if index >= arity {
                    // COMMIT-TIME failure: every command completed at Execute
                    // (`index == arity`), then the trailing `Sync`'s implicit COMMIT
                    // failed — the error belongs to the whole batch, not any single
                    // command. Return a batch-level `Db` (so `batch_failed_index` is
                    // `None`), never an out-of-range `BatchFailed { index: arity }`.
                    Err(DriverError::Db(Box::new(db)))
                } else {
                    // A command-attributable failure: `index < arity` names the
                    // zero-based failing command.
                    Err(DriverError::BatchFailed {
                        index,
                        source: Box::new(db),
                    })
                }
            }
            // A parked failure with no parsed cause is unreachable (a failure is
            // reached ONLY via a surfaced `Fail`); fail-closed classified.
            (Some(_), None) => {
                core::hint::cold_path();
                for &name in &plan {
                    self.engine.evict_pipeline_statement(name);
                }
                Err(DriverError::UnclassifiedFailure)
            }
        }
        }
        .await;
        // The body future completed (Ok or Err) — disarm so its `Drop` records no
        // dropped-future recovery.
        scope.disarm();
        batch_result
    }

    /// Run a HOMOGENEOUS ATOMIC bulk write — ONE compile-checked `query!` write
    /// carrier `Q` (an `UPDATE` / `DELETE` / `INSERT`) against N runtime parameter
    /// sets, `Parse`d ONCE and re-bound for every set, in ~ONE round trip, returning
    /// each command's affected-row count. The batch peer of the typed
    /// [`execute`](Self::execute) and the homogeneous sibling of
    /// [`pipeline`](Self::pipeline) — it closes the gap those two leave (running N
    /// varying-parameter writes fits neither [`copy_in_typed`](Self::copy_in_typed),
    /// which is INSERT-only, nor `pipeline`, whose arity is a fixed compile-time
    /// tuple).
    ///
    /// # Airtight all-or-nothing (INHERITED from the pipeline core)
    ///
    /// The N commands ride ONE trailing `Sync`, forming a SINGLE implicit
    /// transaction (PG §55.2.3): the whole batch commits and returns every count, or
    /// it errors and returns ZERO (`Vec<u64>` is built ONLY after the batch reaches
    /// its clean trailing `ReadyForQuery` — the server emits that only if the whole
    /// implicit transaction COMMITTED). A mid-batch failure is
    /// [`DriverError::BatchFailed`] naming the zero-based failing command
    /// ([`batch_failed_index`](DriverError::batch_failed_index)); a COMMIT-TIME
    /// failure (a `DEFERRABLE INITIALLY DEFERRED` constraint, a serialization
    /// failure — every command succeeded at `Execute`, the implicit COMMIT at the
    /// trailing `Sync` failed) is [`DriverError::Db`] whose `batch_failed_index` is
    /// `None` (the failure belongs to no single command, never an out-of-range
    /// index). Like every other verb, `execute_batch` does NOT auto-rollback — a
    /// mid-batch failure inside an EXPLICIT transaction leaves it aborted (`'E'`) for
    /// its owner to roll back, so a subsequent in-guard verb is a loud `25P02`, never
    /// a silent autocommit. All three properties are inherited by driving the SAME
    /// staging / settle machinery as [`pipeline`](Self::pipeline).
    ///
    /// # Constant send memory, deadlock-free (the windowed batcher)
    ///
    /// A large N must NOT buffer all N `Bind` frames. The commands stream onto the
    /// send buffer and flush at the [`BATCH_WINDOW_THRESHOLD`] batcher threshold
    /// (like `copy_in`), so the staged-bytes high-water is bounded
    /// regardless of N (constant memory). UNLIKE COPY — where the server is silent
    /// while the client streams, so a write-ahead cannot deadlock — an
    /// extended-protocol command emits a per-command response, so streaming N `Bind`s
    /// without reading would fill the server's output buffer AND the client's send
    /// buffer and DEADLOCK. So each window ends with a `Flush` (not a `Sync` — a
    /// `Flush` forces the window's responses out WITHOUT ending the implicit
    /// transaction, so all N stay ONE atomic transaction under the single trailing
    /// `Sync`) and the window's responses are DRAINED before the next window is
    /// staged. A batch whose commands fit one window (the common case) is exactly ~1
    /// round trip with ONE `Sync` and no intermediate `Flush`; only a genuinely huge
    /// N pays ~`N / window` round trips — the honest floor for a deadlock-free,
    /// constant-memory bulk over a bidirectional protocol.
    ///
    /// # Boundary cases
    ///
    /// - `N == 0` → `Ok(vec![])` with NO wire I/O.
    /// - `N == 1` → one window, `Parse`+`Bind`+`Execute`+`Sync` — identical to a
    ///   single [`execute`](Self::execute) (no regression).
    ///
    /// # Errors
    ///
    /// [`DriverError::BatchFailed`] / [`DriverError::Db`] as above; a FATAL
    /// transport/protocol/EOF fault (the connection is dead,
    /// [`is_disconnect`](DriverError::is_disconnect) is `true`). A single
    /// parameter set whose `Bind` frame exceeds the wire length field
    /// ([`DriverError::Io`] via `FrameTooLong`) is a clean, connection-preserving
    /// error if it is the FIRST window (nothing flushed — a consumed deferred `BEGIN`
    /// is preserved) and a FATAL connection-kill (rolling back the open implicit
    /// transaction — all-or-nothing preserved) if a window was already flushed.
    pub async fn execute_batch<'p, Q>(
        &mut self,
        params: impl IntoIterator<Item = Q::Params<'p>>,
    ) -> Result<Vec<u64>, DriverError>
    where
        Q: TypedQuery,
    {
        let mut slow = self.armed_slow_guard(Q::PREPARED.sql());
        let result = self.execute_batch_inner::<Q, _>(params).await;
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// The windowed drive behind [`execute_batch`](Self::execute_batch).
    async fn execute_batch_inner<'p, Q, I>(
        &mut self,
        params: I,
    ) -> Result<Vec<u64>, DriverError>
    where
        Q: TypedQuery,
        I: IntoIterator<Item = Q::Params<'p>>,
    {
        let prepared = bsql_postgres_proto::prepared::prepared_at::<Q>();
        let stmt_name = prepared.stmt_name();
        let mut it = params.into_iter();

        // Stage command 0 (Parse-once). An EMPTY batch does NO wire I/O.
        let first = match it.next() {
            None => return Ok(Vec::new()),
            Some(p) => p,
        };
        // N>=1: recover a prior dropped-future connection + take the token FIRST —
        // BEFORE staging command 0 (so a recovery drain never flushes staged bytes),
        // exactly as `pipeline`. The whole staging + windowed drive + settle then runs
        // under the dropped-future guard (an owed-reply DRAIN on a dropped-mid-flight
        // future); an early `return` inside the block disarms normally.
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let batch_result = async move {
        let mut live = live;
        if let Err(e) = self
            .engine
            .stage_execute_batch_command(&prepared, &first, true)
        {
            core::hint::cold_path();
            // Nothing flushed yet — discard the partial staging (preserving a
            // consumed deferred `BEGIN` for the next verb) and stay healthy.
            self.engine.abort_pipeline_staging();
            // We already hold the token — restore it (nothing was flushed).
            self.live = Some(live);
            return Err(lift_engine_error(e));
        }

        // Shared collector across every window drive (used sequentially).
        let (lower, _) = it.size_hint();
        let mut affected: Vec<u64> = Vec::with_capacity(lower.saturating_add(1));
        let mut current: usize = 0; // delivered commands (global, 0-based)
        let mut db_error: Option<DbError> = None;
        let mut failed_index: Option<usize> = None;
        let mut total: usize = 1; // staged commands (command 0 staged above)
        let mut flushed_any = false;

        'windows: loop {
            // Fill the current window: stage subsequent commands until the send
            // buffer crosses the batcher threshold or the iterator is exhausted.
            let mut window_full = false;
            loop {
                let before = self.engine.pending_send_len();
                match it.next() {
                    None => break,
                    Some(p) => {
                        if let Err(e) =
                            self.engine.stage_execute_batch_command(&prepared, &p, false)
                        {
                            core::hint::cold_path();
                            // A single `Bind` frame exceeded the wire length field.
                            if flushed_any {
                                // A window was already flushed: the implicit
                                // transaction is OPEN with committed-nothing partial
                                // commands. Sending a `Sync` would COMMIT the partial
                                // (breaking all-or-nothing), so the connection MUST
                                // die — returning WITHOUT restoring `self.live` leaves
                                // it NotReady (the token `live` falls out of scope),
                                // and its socket close rolls the open implicit
                                // transaction back. All-or-nothing is preserved at the
                                // cost of the connection.
                                return Err(lift_engine_error(e));
                            }
                            // First window, nothing flushed: clean abort (deferred
                            // `BEGIN` preserved), restore the token, stay healthy.
                            self.engine.abort_pipeline_staging();
                            self.live = Some(live);
                            return Err(lift_engine_error(e));
                        }
                        total = total.saturating_add(1);
                        let after = self.engine.pending_send_len();
                        if after >= BATCH_WINDOW_THRESHOLD {
                            let k_size = after.saturating_sub(before);
                            if before > 0 && k_size >= BATCH_WINDOW_THRESHOLD {
                                // OVERSIZE-command CO-WINDOW: this parameter set's OWN
                                // Bind alone crossed the threshold on top of a non-empty
                                // prefix — a large-RETURNING early command + this large
                                // param would deadlock the write path if flushed
                                // together. ISOLATE it: flush + drain the PREFIX alone
                                // (via the shared `isolate_prefix`), then this command
                                // rides its own fresh window below. `execute_batch` is
                                // UNguarded (`false`): it discards RETURNING rows, so no
                                // result-OID mismatch is ever parked. Prefix target
                                // excludes this command. (`flushed_any` is set by the
                                // guaranteed intermediate flush below, not here.)
                                let prefix_target = total.saturating_sub(1);
                                let (l, step) = self
                                    .isolate_prefix(live, false, k_size, |surface| {
                                        feed_count_window(
                                            surface,
                                            &mut current,
                                            prefix_target,
                                            &mut affected,
                                            &mut failed_index,
                                            &mut db_error,
                                        )
                                    })
                                    .await?;
                                live = l;
                                if step == WindowStep::Halt {
                                    break 'windows;
                                }
                                // The oversize command is now re-staged ALONE; flush it
                                // as its own window below (never co-windowed).
                            }
                            window_full = true;
                            break;
                        }
                    }
                }
            }
            if !window_full {
                // The iterator is exhausted — this is the FINAL window; it is sent
                // with the trailing `Sync` below, not a `Flush`.
                break 'windows;
            }

            // INTERMEDIATE window: `Flush` + drain via the shared `flush_window`.
            // The sink breaks once every command of this window has delivered,
            // leaving the engine at a clean inter-command boundary; `execute_batch`
            // is UNguarded (`false`), so the drive is inert — byte-identical to the
            // historical drive. On `Halt` (a parked server error, or a fail-closed
            // unexpected boundary) stop staging; the trailing `Sync` + drain below
            // recovers the connection and the settle classifies which.
            flushed_any = true;
            let window_target = total;
            let (l, step) = self
                .flush_window(live, false, |surface| {
                    feed_count_window(
                        surface,
                        &mut current,
                        window_target,
                        &mut affected,
                        &mut failed_index,
                        &mut db_error,
                    )
                })
                .await?;
            live = l;
            if step == WindowStep::Halt {
                core::hint::cold_path();
                break 'windows;
            }
        }

        // FINAL DRIVE: the ONE trailing `Sync` closes the batch. It is sent whether
        // the loop ended cleanly (drive the final window's remaining commands + the
        // batch RFQ) or aborted mid-batch (the server is skipping-to-`Sync` after the
        // error, so the `Sync` produces the recovering RFQ the engine's parked drain
        // reads). `drive_to_outcome` handles a COMMIT-TIME failure (a `Fail` at the
        // Sync) by draining the owed RFQ — so a deferred-constraint failure surfaces
        // exactly as the pipeline's, `batch_failed_index` `None`.
        self.engine.stage_pipeline_seal();
        let outcome = self
            .engine
            .run_pipeline(
                live,
                capture_notify(&mut self.notifications, self.diag.sink(), |surface| {
                    match surface {
                        Surface::Deliver { tag, .. } => {
                            let n = match tag {
                                Some(t) => t.rows_or_zero(),
                                None => 0,
                            };
                            affected.push(n);
                            current = current.saturating_add(1);
                        }
                        Surface::Fail(body) if failed_index.is_none() => {
                            failed_index = Some(current);
                            db_error = Some(materialize::parse_error_response(body));
                        }
                        _ => {}
                    }
                    ControlFlow::Continue(())
                }),
            )
            .await;
        let (live, _status) = match outcome {
            Ok(Outcome { live, status }) => (live, status),
            Err(other) => {
                core::hint::cold_path();
                return Err(lift_engine_error(other));
            }
        };
        self.live = Some(live);

        // SETTLE — identical semantics to `pipeline`, driven by the PARKED failure
        // (not the final boundary, which is `Idle` even after a mid-batch failure's
        // recovery drain). One statement is shared by all N commands, so record /
        // evict it ONCE.
        match (failed_index, db_error) {
            // No failure: the whole implicit transaction committed. Record the
            // statement for future HITs IF the batch left the connection at `Idle`
            // (mirrors the serial cache rule; inside an explicit transaction it
            // defers, since a rollback could drop it).
            (None, _) => {
                if matches!(self.engine.tx_status(), Ok(TxStatus::Idle)) {
                    self.engine.record_pipeline_statement(stmt_name);
                }
                Ok(affected)
            }
            // A failure: the whole batch rolled back; the collected counts are
            // DISCARDED (never an `Ok`). Evict the statement so a next attempt
            // re-`Parse`s (self-healing against an out-of-band plan drop).
            (Some(index), Some(db)) => {
                core::hint::cold_path();
                self.engine.evict_pipeline_statement(stmt_name);
                if index >= total {
                    // COMMIT-TIME failure: every command Executed, the implicit
                    // COMMIT failed — belongs to no single command.
                    Err(DriverError::Db(Box::new(db)))
                } else {
                    Err(DriverError::BatchFailed {
                        index,
                        source: Box::new(db),
                    })
                }
            }
            // A `ServerErrored`-shaped state with no parsed cause is unreachable (a
            // failure is reached ONLY via a surfaced `Fail`); fail-closed classified.
            (Some(_), None) => {
                core::hint::cold_path();
                self.engine.evict_pipeline_statement(stmt_name);
                Err(DriverError::UnclassifiedFailure)
            }
        }
        }
        .await;
        scope.disarm();
        batch_result
    }

    /// Run a HOMOGENEOUS ATOMIC bulk QUERY — ONE compile-checked `query!` carrier
    /// `Q` against N runtime parameter sets, `Parse`d ONCE and re-bound for every
    /// set, in ~ONE round trip, returning one typed [`Rows<Q>`](crate::Rows) per
    /// command (KEEPING each command's rows). The typed-RETURNING peer of
    /// [`execute_batch`](Self::execute_batch) and the batch peer of
    /// [`query`](Self::query) — it closes the one gap the batch matrix leaves:
    /// running N varying-parameter commands and KEEPING each command's typed
    /// RETURNING rows (an `INSERT ... RETURNING id` bulk-insert wanting the N
    /// generated keys back, typed, in one atomic batch). `pipeline` cannot express a
    /// RUNTIME N (its arity is a fixed compile-time tuple), `execute_batch` DISCARDS
    /// its RETURNING rows (it returns only the affected counts), and `copy_in_typed`
    /// is INSERT-only with no RETURNING — so this verb is the only one that returns
    /// N grouped typed results for a runtime N.
    ///
    /// # Grouped `Vec<Rows<Q>>` — one result per command
    ///
    /// The N commands map to N [`Rows<Q>`](crate::Rows), in order — SYMMETRIC with
    /// [`execute_batch`](Self::execute_batch)'s `Vec<u64>` (one count per command).
    /// A FLATTENED single `Rows<Q>` was rejected: it would silently lose which rows
    /// came from which parameter set (unrecoverable for a multi-row-RETURNING
    /// command). Memory is O(total rows) by nature — this is the EAGER peer of
    /// [`query`](Self::query) (like `execute_batch` is eager); a constant-memory
    /// streaming batch is a different, non-goal verb.
    ///
    /// # Airtight all-or-nothing (INHERITED from the pipeline core)
    ///
    /// The N commands ride ONE trailing `Sync`, forming a SINGLE implicit
    /// transaction (PG §55.2.3): the whole batch commits and returns every
    /// `Rows<Q>`, or it errors and returns ZERO (the `Vec<Rows<Q>>` is built ONLY
    /// after the batch reaches its clean trailing `ReadyForQuery` — the server emits
    /// that only if the whole implicit transaction COMMITTED; the provisional
    /// per-command row prebuffers are DISCARDED on any failure). A mid-batch failure
    /// is [`DriverError::BatchFailed`] naming the zero-based failing command
    /// ([`batch_failed_index`](DriverError::batch_failed_index)); a COMMIT-TIME
    /// failure (a `DEFERRABLE INITIALLY DEFERRED` constraint — every command
    /// succeeded at `Execute`, the implicit COMMIT at the trailing `Sync` failed) is
    /// [`DriverError::Db`] whose `batch_failed_index` is `None`. Like every verb it
    /// does NOT auto-rollback — a mid-batch failure inside an EXPLICIT transaction
    /// leaves it aborted (`'E'`) for its owner, so a subsequent in-guard verb is a
    /// loud `25P02`, never a silent autocommit.
    ///
    /// # RESULT-schema OID guard — verified ONCE (the homogeneity optimization)
    ///
    /// Each command decodes into `Rows<Q>` POSITIONALLY, so — like the single typed
    /// verbs and the heterogeneous [`pipeline`](Self::pipeline) — a runtime result
    /// column whose type diverged from the migration schema `Q` was typed against
    /// (an out-of-band `ALTER COLUMN TYPE`, a `TEMP` shadow) must be caught, not
    /// silently mis-decoded. But UNLIKE the heterogeneous pipeline (which guards
    /// per-command, since each element is a DIFFERENT carrier), `query_batch` runs
    /// the SAME `Q` `Parse`d ONCE, so ALL N commands reuse ONE server-side plan with
    /// ONE result descriptor — verifying command 0's runtime column OIDs against
    /// `Q::row_oids` (via command 0's cache-MISS `Describe`(portal)) proves the
    /// shared schema for the WHOLE batch. Every subsequent command is a BARE
    /// `Bind`+`Execute` on that one statement (no `Describe`), so its rows conform to
    /// the SAME verified descriptor — airtight, and cheaper than N `Describe`s. A
    /// divergence is a classified [`DriverError::BatchColumnOidMismatch`] (the client
    /// drains to a clean idle and returns ZERO results). A HIT command 0 (the
    /// statement was already cached) sends no `Describe` and cannot silently
    /// mis-decode — PostgreSQL itself refuses a reused plan's result-type change
    /// (`0A000`, surfaced as a mid-batch `BatchFailed`), and all N reuse that plan.
    ///
    /// # Constant send memory, deadlock-free (the windowed batcher)
    ///
    /// Identical to [`execute_batch`](Self::execute_batch): the commands stream onto
    /// the send buffer and flush at the [`BATCH_WINDOW_THRESHOLD`] threshold, each
    /// window ending with a `Flush` (not a `Sync`, so all N stay ONE atomic
    /// transaction under the single trailing `Sync`) whose responses are DRAINED
    /// before the next window is staged — so a huge N never buffers all N `Bind`s and
    /// never deadlocks. The one difference from `execute_batch`'s drive is the
    /// GUARDED window drain ([`Engine::run_pipeline_break_guarded`](bsql_postgres_proto::engine::Engine::run_pipeline_break_guarded)):
    /// command 0's MISS `Describe` can park a result-OID mismatch in an INTERMEDIATE
    /// window (a `Flush`, no `Sync`), whose silent swallow-to-`ReadyForQuery` drain
    /// would block forever, so the guarded drive BAILS on a parked mismatch and the
    /// trailing `Sync` drains it (exactly the heterogeneous pipeline's windowed-guard
    /// handling).
    ///
    /// # Boundary cases
    ///
    /// - `N == 0` → `Ok(vec![])` with NO wire I/O.
    /// - `N == 1` → one window, `Parse`+`Bind`+`Describe`(MISS)+`Execute`+`Sync` —
    ///   identical to a single [`query`](Self::query) (no regression).
    ///
    /// # Errors
    ///
    /// [`DriverError::BatchFailed`] / [`DriverError::Db`] (commit-time) /
    /// [`DriverError::BatchColumnOidMismatch`] as above; a FATAL
    /// transport/protocol/EOF fault (the connection is dead,
    /// [`is_disconnect`](DriverError::is_disconnect) is `true`). A single parameter
    /// set whose `Bind` frame exceeds the wire length field ([`DriverError::Io`] via
    /// `FrameTooLong`) is a clean, connection-preserving error if it is the FIRST
    /// window (nothing flushed — a consumed deferred `BEGIN` is preserved) and a
    /// FATAL connection-kill (rolling back the open implicit transaction —
    /// all-or-nothing preserved) if a window was already flushed.
    pub async fn query_batch<'p, Q>(
        &mut self,
        params: impl IntoIterator<Item = Q::Params<'p>>,
    ) -> Result<Vec<Rows<Q>>, DriverError>
    where
        Q: TypedQuery,
    {
        let mut slow = self.armed_slow_guard(Q::PREPARED.sql());
        let result = self.query_batch_inner::<Q, _>(params).await;
        Self::commit_slow(&mut slow, &result);
        result
    }

    /// The guarded windowed drive behind [`query_batch`](Self::query_batch) — the
    /// typed-RETURNING peer of [`execute_batch_inner`](Self::execute_batch_inner).
    ///
    /// Structurally identical to `execute_batch_inner` (Parse-once command 0 + BARE
    /// subsequent commands + the windowed batcher + the pipeline settle) with exactly
    /// three deltas, all reusing EXISTING engine seams (so `next_event` stays
    /// byte-identical, no new dispatch state): (1) command 0 is staged via
    /// [`stage_pipeline_command`](Core::stage_pipeline_command) (`guard = true` — a
    /// MISS appends a `Describe`(portal) and arms the result-OID guard) instead of
    /// `stage_execute_batch_command` (`guard = false`); (2) the window drain routes
    /// each surface to its command's [`RowsBuilder`] (KEEP the typed rows) instead of
    /// pushing a `u64` count, and uses the GUARDED break drive (bail on a parked
    /// mismatch); (3) the settle first checks the parked result-OID mismatch (→
    /// [`DriverError::BatchColumnOidMismatch`]) before the failure/`Ok` arms, and the
    /// `Ok` arm builds the grouped `Vec<Rows<Q>>` from the per-command builders.
    async fn query_batch_inner<'p, Q, I>(
        &mut self,
        params: I,
    ) -> Result<Vec<Rows<Q>>, DriverError>
    where
        Q: TypedQuery,
        I: IntoIterator<Item = Q::Params<'p>>,
    {
        let prepared = bsql_postgres_proto::prepared::prepared_at::<Q>();
        let stmt_name = prepared.stmt_name();
        let mut it = params.into_iter();

        // Stage command 0 (Parse-once). An EMPTY batch does NO wire I/O.
        let first = match it.next() {
            None => return Ok(Vec::new()),
            Some(p) => p,
        };
        // N>=1: recover a prior dropped-future connection + take the token FIRST —
        // BEFORE staging command 0 (so a recovery drain never flushes staged bytes),
        // exactly as `pipeline` / `execute_batch`. The whole staging + windowed drive +
        // settle runs under the dropped-future guard (an owed-reply DRAIN); an early
        // `return` inside the block disarms normally.
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let batch_result = async move {
        let mut live = live;
        // Command 0 with the RESULT-OID GUARD (`stage_pipeline_command`, guard=true):
        // a MISS appends a `Describe`(portal) + arms the guard so the shared `Q`
        // schema is verified ONCE (every subsequent command reuses this one
        // server-side plan). This is the ONE staging delta from `execute_batch`,
        // which stages command 0 guard-FALSE (it discards its RETURNING rows).
        if let Err(e) = self.engine.stage_pipeline_command(&prepared, &first, true) {
            core::hint::cold_path();
            // Nothing flushed yet — discard the partial staging (preserving a
            // consumed deferred `BEGIN`) and stay healthy.
            self.engine.abort_pipeline_staging();
            // We already hold the token — restore it (nothing was flushed).
            self.live = Some(live);
            return Err(lift_engine_error(e));
        }

        // One row prebuffer per command, GROWN as each command is staged (a runtime
        // N, unlike `pipeline`'s const-arity pre-allocation): a builder is pushed
        // BEFORE its window is flushed, so `builders[current]` always exists when the
        // drain routes that command's rows. `RowsBuilder::new()` allocates nothing
        // until fed, so N empty builders cost only `N * size_of::<RowsBuilder>()`.
        let (lower, _) = it.size_hint();
        let mut builders: Vec<RowsBuilder> = Vec::with_capacity(lower.saturating_add(1));
        builders.push(RowsBuilder::new()); // command 0
        let mut current: usize = 0; // delivered commands (global, 0-based)
        let mut db_error: Option<DbError> = None;
        let mut failed_index: Option<usize> = None;
        let mut total: usize = 1; // staged commands (command 0 staged above)
        let mut flushed_any = false;

        'windows: loop {
            // Fill the current window: stage subsequent commands (BARE Bind+Execute,
            // Parse-once, NO Describe — the shared schema was guarded on command 0)
            // until the send buffer crosses the batcher threshold or the iterator is
            // exhausted.
            let mut window_full = false;
            loop {
                let before = self.engine.pending_send_len();
                match it.next() {
                    None => break,
                    Some(p) => {
                        if let Err(e) =
                            self.engine.stage_execute_batch_command(&prepared, &p, false)
                        {
                            core::hint::cold_path();
                            // A single `Bind` frame exceeded the wire length field.
                            if flushed_any {
                                // A window was already flushed: the implicit
                                // transaction is OPEN with committed-nothing partial
                                // commands. Sending a `Sync` would COMMIT the partial
                                // (breaking all-or-nothing), so the connection MUST
                                // die — returning WITHOUT restoring `self.live` leaves
                                // it NotReady (the token `live` falls out of scope),
                                // and its socket close rolls the open implicit
                                // transaction back. All-or-nothing preserved at the
                                // cost of the connection.
                                return Err(lift_engine_error(e));
                            }
                            // First window, nothing flushed: clean abort (deferred
                            // `BEGIN` preserved), restore the token, stay healthy.
                            self.engine.abort_pipeline_staging();
                            self.live = Some(live);
                            return Err(lift_engine_error(e));
                        }
                        builders.push(RowsBuilder::new());
                        total = total.saturating_add(1);
                        let after = self.engine.pending_send_len();
                        if after >= BATCH_WINDOW_THRESHOLD {
                            let k_size = after.saturating_sub(before);
                            if before > 0 && k_size >= BATCH_WINDOW_THRESHOLD {
                                // OVERSIZE-command CO-WINDOW: this parameter set's OWN
                                // Bind alone crossed the threshold on top of a non-empty
                                // prefix — a large-RESULT early command + this large
                                // param would deadlock the write path if flushed
                                // together. ISOLATE it: flush + drain the PREFIX alone
                                // (via the shared `isolate_prefix`, GUARDED — command 0's
                                // result-OID guard can bail here), then this command
                                // rides its own fresh window below. Prefix target
                                // excludes this command; its builder (pushed above) is
                                // fed when the isolated window drains. (`flushed_any` is
                                // set by the guaranteed intermediate flush below.)
                                let prefix_target = total.saturating_sub(1);
                                let (l, step) = self
                                    .isolate_prefix(live, true, k_size, |surface| {
                                        feed_typed_window(
                                            surface,
                                            &mut current,
                                            prefix_target,
                                            &mut builders,
                                            &mut failed_index,
                                            &mut db_error,
                                        )
                                    })
                                    .await?;
                                live = l;
                                if step == WindowStep::Halt {
                                    break 'windows;
                                }
                                // The oversize command is now re-staged ALONE; flush it
                                // as its own window below (never co-windowed).
                            }
                            window_full = true;
                            break;
                        }
                    }
                }
            }
            if !window_full {
                // The iterator is exhausted — this is the FINAL window; it is sent
                // with the trailing `Sync` below, not a `Flush`.
                break 'windows;
            }

            // INTERMEDIATE window: `Flush` + GUARDED drain (routing rows to builders)
            // via the shared `flush_window`. The sink breaks once every command staged
            // so far has delivered; the GUARDED drive BAILS (`Halt`) if command 0's
            // result-schema guard parked a mismatch in this window (an intermediate
            // window has no `Sync`, so the silent drain would otherwise block forever;
            // the trailing `Sync` below drains it). On `Halt` (a parked server error, a
            // guard bail, or a fail-closed unexpected boundary) stop staging; the
            // trailing `Sync` + final drain recovers the connection and the settle
            // classifies which.
            flushed_any = true;
            let window_target = total;
            let (l, step) = self
                .flush_window(live, true, |surface| {
                    feed_typed_window(
                        surface,
                        &mut current,
                        window_target,
                        &mut builders,
                        &mut failed_index,
                        &mut db_error,
                    )
                })
                .await?;
            live = l;
            if step == WindowStep::Halt {
                core::hint::cold_path();
                break 'windows;
            }
        }

        // FINAL DRIVE: the ONE trailing `Sync` closes the batch. Sent whether the loop
        // ended cleanly (drive the final window's remaining commands + the batch RFQ)
        // or aborted mid-batch (the server is skipping-to-`Sync` after an error, or
        // draining a parked mismatch — the `Sync` produces the recovering RFQ the
        // parked drain reads). Routes rows to builders (none arrive after a
        // mismatch/error — all swallowed by the drain).
        self.engine.stage_pipeline_seal();
        let outcome = self
            .engine
            .run_pipeline(
                live,
                capture_notify(&mut self.notifications, self.diag.sink(), |surface| {
                    match surface {
                        Surface::Row(_) | Surface::RowChunk(_) | Surface::RowChunkEnd => {
                            if let Some(b) = builders.get_mut(current) {
                                b.feed(surface);
                            }
                        }
                        Surface::Deliver { .. } => {
                            if let Some(b) = builders.get_mut(current) {
                                b.feed(surface);
                            }
                            current = current.saturating_add(1);
                        }
                        Surface::Fail(body) if failed_index.is_none() => {
                            failed_index = Some(current);
                            db_error = Some(materialize::parse_error_response(body));
                        }
                        _ => {}
                    }
                    ControlFlow::Continue(())
                }),
            )
            .await;
        let live = match outcome {
            Ok(Outcome { live, .. }) => live,
            // FATAL: the verb consumed the token and the connection is dead.
            Err(other) => {
                core::hint::cold_path();
                return Err(lift_engine_error(other));
            }
        };
        self.live = Some(live);

        // SETTLE — identical to `pipeline`'s: driven by the PARKED mismatch + failure,
        // NOT the final boundary (which is `Idle` even after a mid-batch failure's
        // recovery drain). Priority: a result-schema mismatch (a CLIENT-side rejection
        // that may have committed server-side) first, then a server / commit-time
        // failure, then the clean `Ok`. One statement is shared by all N commands, so
        // record / evict it ONCE.
        //
        // TYPED RESULT-SCHEMA guard: command 0's cache-MISS `Describe` caught a runtime
        // column type diverging from `Q`'s migration schema and the batch drained to a
        // clean idle, so no garbage row reached a builder. Only command 0 `Describe`s
        // (guard-once), so `current` is 0 here; the `current`-based index mirrors
        // `pipeline` for symmetry. A MISS whose guard fired is NOT recorded in the cache
        // (the mismatch check returns before the `(None, _)` record arm), so a repeat
        // re-`Describe`s + re-guards.
        if let Some((column, found, expected)) = self.engine.take_result_oid_mismatch() {
            core::hint::cold_path();
            // `current < total`, and a batch cannot exceed `u16::MAX` commands in
            // practice; the `Err` arm is a total-conversion floor, never reached (no
            // `as`, no `unwrap`). `unwrap_or` is banned by the silent-fallback ledger,
            // so this explicit match is the sanctioned dead arm (as in `pipeline`).
            #[expect(
                clippy::manual_unwrap_or,
                reason = "unwrap_or is a disallowed method; this explicit match is the \
                          sanctioned dead-arm narrow — guard-once means `current` is 0 \
                          here, so the `Err` view is unreachable, never a masked failure"
            )]
            let command = match u16::try_from(current) {
                Ok(c) => c,
                Err(_) => u16::MAX,
            };
            return Err(DriverError::BatchColumnOidMismatch {
                command,
                source: DecodeError::ColumnOidMismatch {
                    index: column,
                    expected,
                    found,
                },
            });
        }
        match (failed_index, db_error) {
            // No failure: the whole implicit transaction committed. THE ONLY `Ok`
            // path — build the grouped `Vec<Rows<Q>>` from the per-command builders
            // (in order). Record the statement for future HITs IF the batch left the
            // connection at `Idle` (mirrors the serial cache rule; inside an explicit
            // transaction it defers, since a rollback could drop it).
            (None, _) => {
                if matches!(self.engine.tx_status(), Ok(TxStatus::Idle)) {
                    self.engine.record_pipeline_statement(stmt_name);
                }
                Ok(builders.into_iter().map(RowsBuilder::finish::<Q>).collect())
            }
            // A mid-batch / commit-time server failure. The N commands are ONE implicit
            // transaction, so the server ROLLED BACK every command; the provisional
            // builders are DISCARDED (never an `Ok`). The connection is left EXACTLY as
            // a normal failed verb leaves it (no auto-rollback). Self-heal: evict the
            // statement so a next attempt re-`Parse`s.
            (Some(index), Some(db)) => {
                core::hint::cold_path();
                self.engine.evict_pipeline_statement(stmt_name);
                if index >= total {
                    // COMMIT-TIME failure: every command Executed, the implicit COMMIT
                    // at the trailing `Sync` failed — belongs to no single command.
                    Err(DriverError::Db(Box::new(db)))
                } else {
                    Err(DriverError::BatchFailed {
                        index,
                        source: Box::new(db),
                    })
                }
            }
            // A parked failure with no parsed cause is unreachable (a failure is reached
            // ONLY via a surfaced `Fail`); fail-closed classified.
            (Some(_), None) => {
                core::hint::cold_path();
                self.engine.evict_pipeline_statement(stmt_name);
                Err(DriverError::UnclassifiedFailure)
            }
        }
        }
        .await;
        scope.disarm();
        batch_result
    }

    // ── Transaction / session boundary primitives ───────────────────────────

    /// Arm a DEFERRED `BEGIN`: it is not sent now, but fused into the flush of the
    /// first command the transaction body issues (so `BEGIN` + that statement ride
    /// ONE round trip instead of two). Called by each driver's transaction GUARD
    /// from WITHIN its first verb — never out-of-band at `transaction()` entry — so
    /// the `BEGIN` is armed and consumed inside that one verb's flush and a
    /// transaction cancelled/panicked before any verb ran arms nothing. The engine
    /// drains the fused `BEGIN`'s reply before the statement's. A field-set only, no
    /// I/O and no token.
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

    /// The connection's current `ReadyForQuery` transaction-status indicator — the
    /// sibling `migrate` module's per-migration boundary backstop reads it (the
    /// engine field is private to this module). `None` only in the unreachable
    /// pre-active phase (a settled verb has always driven the engine active), so a
    /// caller after any completed query gets `Some(_)`.
    ///
    /// `#[doc(hidden)]`: an internal seam for the migration runner, not consumer
    /// surface.
    #[doc(hidden)]
    #[inline]
    pub(crate) fn tx_status(&self) -> Option<TxStatus> {
        self.engine.tx_status().ok()
    }

    /// Subscribe to a `LISTEN` channel. The name is validated into a
    /// [`SafeIdent`] — the injection-safe type the SQL is assembled from — so an
    /// injection-shaped name is a classified [`DriverError::Config`] and CANNOT
    /// reach the interpolated SQL. The `SafeIdent` (not a raw `&str`) is the
    /// splice currency, so the "cannot inject" guarantee is structural: the type
    /// is the proof.
    pub async fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        let sql = sql_ident::listen_sql(SafeIdent::validate(channel)?);
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
            let this = self;
            let mut collector = ResultCollector::new();
            let outcome = this
                .engine
                .simple_query(
                    live,
                    &sql,
                    capture_notify(&mut this.notifications, this.diag.sink(), |s| {
                        collector.feed(s);
                        ControlFlow::Continue(())
                    }),
                )
                .await;
            this.settle(outcome, &mut collector)
        }
        .await;
        scope.disarm();
        out
    }

    /// Unsubscribe from a `LISTEN` channel. The name is validated into a
    /// [`SafeIdent`] before interpolation (see [`listen`](Self::listen)).
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        let sql = sql_ident::unlisten_sql(SafeIdent::validate(channel)?);
        self.simple_query(&sql).await?;
        Ok(())
    }

    /// Reset all BLEEDABLE session state so this connection can be safely reused
    /// by a different logical user, DROPPING BOTH prepared-statement caches.
    ///
    /// Runs `DISCARD ALL` MINUS `DEALLOCATE ALL` in one simple-query round trip
    /// (prefixed with `ROLLBACK` only when inside a transaction, decided from the
    /// cached `ReadyForQuery` tx status so the common idle path costs no extra round
    /// trip), then DROPS both prepared-statement caches — the DYNAMIC (runtime-SQL)
    /// cache AND the compile-checked TYPED (`query!`) cache — and clears the
    /// notification ledger and the N+1 recency window.
    ///
    /// This is the SINGLE reset used both by a DIRECT consumer (an explicit clean
    /// slate) and by the POOL at checkout, so a pooled connection behaves EXACTLY
    /// like a fresh one for the next logical user.
    ///
    /// # The ONE RULE: a statement cache never crosses a checkout
    ///
    /// A prepared statement's relation NAMES are resolved once, at its `Parse`.
    /// A plan a prior logical user promoted — dynamic or typed — that survived into
    /// the next user's checkout would keep the prior user's name resolution, so it
    /// is DROPPED unconditionally. This is a CORRECTNESS requirement, not a hygiene
    /// nicety, and it holds identically for BOTH caches:
    ///
    /// - Keeping a plan warm lets a NEXT user who creates a shadowing `CREATE TEMP
    ///   TABLE orders` (with `pg_temp` already active, so the search-path OID list is
    ///   unchanged) receive the PRIOR user's `public.orders` rows on a cache HIT — a
    ///   silent cross-user WRONG RESULT (a tenant-boundary leak, verified live for
    ///   the dynamic cache). The typed cache had the SAME hole: a typed HIT reuses
    ///   the kept plan with a bare `Bind`+`Execute` and sends no `Describe`, so the
    ///   result-schema guard never runs; and because the shadow's columns match, the
    ///   result type is unchanged, so PostgreSQL's own `0A000` ("cached plan must not
    ///   change result type") never fires either. The guard + `0A000` cover a
    ///   result-TYPE divergence, NOT this same-type / different-data-source case.
    /// - `DISCARD PLANS` cannot fix this robustly: it INVALIDATES a kept plan ONCE at
    ///   checkout, but PostgreSQL RE-VALIDATES an invalidated plan at its next use,
    ///   resolving against whatever schema exists AT THAT MOMENT. Any reset or user
    ///   statement that touches the catalog before the user's shadow exists
    ///   re-validates the plan back to `public` (VERIFIED live: `DISCARD PLANS` placed
    ///   before `DISCARD TEMP` is re-validated by the trailing reset statements and
    ///   the wrong rows still come back), and a shadow created AFTER a re-validation
    ///   is not seen. Only DROPPING the statement (a fresh `Parse` on next use, which
    ///   resolves against the current user's schema, exactly as on a fresh
    ///   connection) is airtight.
    ///
    /// Dropping the CLIENT-side sets is the airtight fix and costs no wire (the next
    /// query of each cache is a MISS re-`Parse`d fresh; a typed MISS re-arms the
    /// result-schema guard too). For hygiene the SERVER-side statements are `Close`d:
    /// `RESET`/`DISCARD` run no `DEALLOCATE`, so they otherwise survive. The dynamic
    /// cache's statements are `Close`d in ONE batched round trip whenever the dynamic
    /// cache is non-empty, and the TYPED cache's statements are FOLDED into that same
    /// batch (zero extra round trip). In the pure-typed flagship case — an empty
    /// dynamic cache — no batch is forced (so the flagship's zero-RTT checkout is
    /// preserved): the typed client cache is cleared for correctness and its
    /// server-side statements are reclaimed lazily by the next typed query's
    /// MISS-path leading `Close` (a bounded, non-growing footprint — content-addressed
    /// names, at most one per distinct `query!`). See the [`DynStmtCache`] doc for the
    /// full argument. Witnessed by the `--ignored`
    /// `pooled_dynamic_plan_re_resolves_a_temp_shadow_across_users` (dynamic cache)
    /// and `pooled_typed_plan_re_resolves_a_temp_shadow_across_users` (typed cache)
    /// live tests (both drivers): a pooled plan a prior user promoted against a
    /// PERMANENT table returns the next user's `TEMP TABLE` data after checkout, never
    /// the permanent rows — because the cache is dropped and the query re-`Parse`s fresh.
    pub async fn reset_session(&mut self) -> Result<(), DriverError> {
        self.reset_session_impl().await
    }

    /// The shared reset body (defined once so the reset semantics cannot drift).
    async fn reset_session_impl(&mut self) -> Result<(), DriverError> {
        const RESET: &str = "SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; \
             UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        const RESET_WITH_ROLLBACK: &str =
            "ROLLBACK; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; \
             CLOSE ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        // No stranded-prelude clear is needed here: the transaction guards arm the
        // deferred BEGIN INSIDE the first verb (async: at that verb's poll; sync: at
        // its call), so it is armed and consumed within one verb's flush and can
        // never persist to a pool checkout. A cancelled or panicked transaction that
        // ran no verb armed nothing, so there is no prelude to discard.
        let sql = if matches!(self.engine.tx_status(), Ok(TxStatus::Idle)) {
            RESET
        } else {
            RESET_WITH_ROLLBACK
        };
        self.simple_query(sql).await?;
        // DROP BOTH prepared-statement caches — the ONE RULE: a statement cache lives
        // within a single connection LEASE, never across a pool checkout. Neither the
        // DYNAMIC (runtime-SQL) cache nor the TYPED (compile-checked `query!`) cache
        // may be reused by the next logical user, because a plan's relation names are
        // resolved at its `Parse`: a plan a prior user promoted (e.g. `… FROM orders`
        // bound to `public.orders`, driven generic) that survived would let the next
        // user's `CREATE TEMP TABLE orders` shadow read the PRIOR user's rows through
        // the kept plan on a cache HIT — a silent cross-user WRONG RESULT (a
        // tenant-boundary leak; the result type is unchanged, so PG's `0A000` never
        // fires, and a HIT sends no `Describe`, so the result-schema guard never runs).
        // Dropping the CLIENT-side sets is the airtight correctness fix (the next
        // query of each is a fresh `Parse` — a MISS — resolving against the CURRENT
        // user's schema and re-arming the typed result-schema guard), and it costs no
        // wire. `RESET`/`DISCARD` run no `DEALLOCATE`, so the SERVER-side statements
        // survive the reset SQL above; Close them explicitly for hygiene (a protocol
        // `Close` of an already-dropped statement is a wire no-op).
        {
            let stmts = self.dyn_cache.drain();
            if stmts.is_empty() {
                // No dynamic Close batch this checkout (the flagship's common case: a
                // pure-typed workload has an empty dynamic cache). Clear the TYPED
                // client cache — the airtight correctness fix, ZERO-wire; its
                // server-side statements are reclaimed lazily by the next typed query's
                // MISS-path leading `Close` (a bounded, non-growing footprint —
                // content-addressed names, at most one per distinct `query!`). No
                // round trip is FORCED, so the flagship's zero-RTT checkout is
                // preserved.
                self.engine.clear_statement_cache();
            } else {
                // The dynamic `Close`+`Sync` is already paid this checkout — FOLD the
                // TYPED cache's server-side statements into the SAME batch (zero extra
                // round trip), draining the typed CLIENT cache in lockstep. Both name
                // families share the raw-bytes `Close` form: dynamic `_bsql_<n>`
                // `StmtName`s and typed `bsql_q_<hex>` `'static` names have DISJOINT
                // prefixes, so a typed `Close` can never touch a dynamic/explicit
                // statement.
                let typed_names = self.engine.take_statement_cache();
                let mut names: Vec<&[u8]> =
                    Vec::with_capacity(stmts.len().saturating_add(typed_names.len()));
                names.extend(stmts.iter().map(|s| s.inner.stmt_name().as_bytes()));
                names.extend(typed_names.iter().map(|n| n.as_bytes()));
                self.close_cached_statements(&names).await?;
                // `stmts` / `typed_names` drop here: the name buffers free, and the
                // server-side statements were already closed by the batch above.
            }
        }
        // Clear the ledger AFTER the reset round trip: `UNLISTEN *` stops new
        // notifications, and this discards every notification captured before or
        // during the reset — so a pooled connection never delivers a prior user's
        // notifications to the next. Done last so a notification that rode the
        // reset's own response stream is cleared too.
        self.notifications.clear();
        // A pool session reset is the strongest logical-operation boundary.
        self.n1_reset();
        // Reclaim an OVERSIZED outbound send buffer before the connection is
        // handed back out. A prior large `Bind` parameter block (a multi-MB
        // `bytea` / `jsonb` / `text`) grew the buffer uncapped, and that capacity
        // is otherwise retained for the connection's whole life — a steady-state
        // memory bloat for a long-lived pooled connection. This is the cold,
        // once-per-checkout settle point (the reset round trip above already
        // DRAINED the buffer), so the reclaim never touches the hot per-query
        // path, and a normal small-query workload — whose buffer never crosses
        // the high-water mark — is left untouched (no shrink-then-regrow thrash).
        self.engine.reclaim_send_buffer();
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
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_DRAIN);
        let out = async move {
        let this = self;
        let mut db_error: Option<DbError> = None;
        let outcome = this
            .engine
            .copy_out(
                live,
                &sql,
                capture_notify(&mut this.notifications, this.diag.sink(), |surface| match surface {
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
                this.live = Some(live);
                Ok(None)
            }
            Boundary::Failed => {
                this.drain_to_idle(live).await?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(e) => {
                this.drain_to_idle(live).await?;
                Ok(Some(e))
            }
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a streaming COPY OUT",
            ))),
        }
        }
        .await;
        scope.disarm();
        out
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
    ///
    /// # Dropped-future recovery boundary (the ONE non-guarded verb)
    ///
    /// This is the single verb that HANDS the linear token OUT to the caller (a
    /// `CopyInWriter` holds it across the streaming writes), rather than restoring
    /// it to `self.live` on a clean boundary — so it cannot ride the
    /// [`guarded`](Self::guarded) combinator. It DOES recover a connection whose
    /// PRIOR verb was dropped ([`begin_command`](Self::begin_command) below), but it
    /// arms NO dropped-future scope: a COPY-in session (`copy_in_begin` …
    /// `copy_in_finish`) whose future is DROPPED mid-stream leaves the token gone
    /// with no dirty marker — a DIRECT connection is then dead (reconnect), a POOLED
    /// one is evicted + replaced on return (its socket close aborts the COPY
    /// server-side). This is unchanged from before this recovery existed and
    /// deliberately out of scope: a mid-COPY-in engine state needs a `CopyFail` +
    /// drain to recover, distinct from the owed-reply drain the command verbs use.
    #[doc(hidden)]
    pub async fn copy_in_begin(&mut self, sql: &str) -> Result<Live<'static>, DriverError> {
        // Recover a prior dropped-future connection before starting a COPY, but arm
        // NO scope — the token is handed out, so a dropped COPY session is the
        // documented residual above (never a guarded owed-reply drain).
        let live = self.begin_command().await?;
        // Thread the capture adapter into the engine's fused-prelude drain (a
        // deferred BEGIN when this COPY is a transaction's FIRST statement): a
        // NOTIFY riding the prelude's reply is buffered into the ledger — the same
        // no-drop guarantee `copy_in_finish` / `copy_in_abort` give — rather than
        // silently consumed. With no prelude pending the sink is never called.
        let outcome = self
            .engine
            .copy_in_begin(
                sql,
                capture_notify(&mut self.notifications, self.diag.sink(), |_s: Surface<'_>| ControlFlow::Continue(())),
            )
            .await;
        match outcome {
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
                capture_notify(&mut self.notifications, self.diag.sink(), |s| {
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
                capture_notify(&mut self.notifications, self.diag.sink(), |_s: Surface<'_>| ControlFlow::Continue(())),
            )
            .await
        {
            self.live = Some(live);
        }
    }

    /// Stream one PGCOPY BINARY row for an open COPY-in and batch its flush.
    /// Token-less (the caller holds the token across writes). `#[doc(hidden)]`:
    /// the per-driver typed COPY orchestration drives this, framed between the
    /// PGCOPY header and trailer.
    #[doc(hidden)]
    pub async fn copy_in_write_binary_row<P: ParamsWriter>(
        &mut self,
        row: &P,
    ) -> Result<(), DriverError> {
        self.engine
            .copy_in_write_binary_row(row)
            .await
            .map_err(lift_engine_error)
    }

    /// Bulk-load `rows` into the compile-checked target of a [`copy!`](TypedCopyIn)
    /// carrier `Q` via PGCOPY BINARY `COPY … FROM STDIN`, in CONSTANT memory,
    /// returning the server's affected-row count.
    ///
    /// The whole orchestration lives ONCE here in the transport-generic
    /// [`Core`](Self): issue the catalog-baked `Q::SQL` COPY command, stream the
    /// PGCOPY binary [header](PGCOPY_BINARY_HEADER), then each `rows` item as one
    /// typed binary row (through the SAME [`ParamsWriter`] encoders the `query!`
    /// param path uses), then the [trailer](PGCOPY_BINARY_TRAILER), then
    /// `CopyDone`. Both drivers forward here (async `.await`, sync single-poll),
    /// so their typed-COPY behaviour is a COMPILER guarantee, not hand-maintained
    /// twins. The rows are NOT pre-collected — a megarow load streams in bounded
    /// memory.
    ///
    /// # Errors and recovery
    ///
    /// A server rejection at `CopyDone` (a constraint / type violation on an
    /// ingested row) is a classified [`DriverError::Db`], and the connection
    /// RECOVERS to a clean idle (it stays pooled). A frame-encode overflow (a
    /// row body past the `u32` wire length) aborts the COPY recoverably. A
    /// transport fault is fatal.
    pub async fn copy_in_typed<'q, Q, I>(&mut self, rows: I) -> Result<u64, DriverError>
    where
        Q: TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>>,
    {
        // The COPY command is a compile-time constant baked from validated
        // catalog identifiers, so there is no runtime identifier to splice —
        // injection-safety is stronger here than the raw path's `SafeTable`
        // (there is no untrusted string at all). On a fault the token is dropped
        // by the begin (dead connection).
        let live = self.copy_in_begin(Q::SQL).await?;
        let streamed = self.copy_in_typed_stream::<Q, I>(rows).await;
        match streamed {
            // `copy_in_finish` restores the token on either status and maps a
            // server rejection to `DriverError::Db` with the connection pooled.
            Ok(()) => self.copy_in_finish(live).await,
            Err(e) => {
                // A mid-stream error (a frame-encode overflow) aborts the COPY:
                // `CopyFail` reclaims the connection recoverably; a transport
                // fault leaves it dead. The caller's `e` dominates.
                self.copy_in_abort(live).await;
                Err(e)
            }
        }
    }

    /// Stream the PGCOPY binary body — header, then each typed row, then trailer
    /// — for [`copy_in_typed`](Self::copy_in_typed). Factored out so the token is
    /// held by the caller across the whole stream and handed to the terminal
    /// `copy_in_finish` / `copy_in_abort` step. Each piece rides its own batched
    /// `CopyData`; frame boundaries are irrelevant to the PGCOPY stream.
    async fn copy_in_typed_stream<'q, Q, I>(&mut self, rows: I) -> Result<(), DriverError>
    where
        Q: TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>>,
    {
        self.copy_in_write(&PGCOPY_BINARY_HEADER).await?;
        for row in rows {
            self.copy_in_write_binary_row(&row).await?;
        }
        self.copy_in_write(&PGCOPY_BINARY_TRAILER).await?;
        Ok(())
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
        // A WAIT verb: it issues NO command, so on a dropped future the engine sits
        // at a clean idle owing nothing — `arm_scope(DIRTY_RECLAIM)` records it, so
        // the next use only RE-MINTS the token (a drain here would block on frames
        // that never come). This is the verb MOST likely to be dropped (it lives in
        // `select!` / timeout loops waiting for a NOTIFY), so its recovery matters.
        let live = self.begin_command().await?;
        let scope = self.arm_scope(DIRTY_RECLAIM);
        let out = async move {
            let this = self;
            let ledger = &mut this.notifications;
            let outcome = this
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
                    this.live = Some(live);
                    Ok(matches!(status, NotifyStatus::Received))
                }
                Err(other) => Err(lift_engine_error(other)),
            }
        }
        .await;
        scope.disarm();
        out
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
        EngineError::HandshakeServerError(body) => {
            // A server `ErrorResponse` during connect: decode the raw body with the
            // SAME authority the active path uses (`parse_error_response`), so a
            // connect-time failure carries its FULL SQLSTATE + message and a
            // consumer can `err.code()` / `is_too_many_connections()` /
            // `is_invalid_catalog_name()` on it EXACTLY as on an active-phase server
            // error — no longer collapsed to one opaque I/O string. One cold alloc
            // on the failure path only; the happy connect path is untouched.
            DriverError::from(materialize::parse_error_response(&body))
        }
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
        ConnFail::CleartextOverPlaintext => DriverError::Config(
            "server requested a cleartext password over an unencrypted connection — refused \
             (a cleartext password is sent only over TLS; use SslMode::Require, or a \
             SCRAM/MD5-authenticated role)",
        ),
        ConnFail::ServerError => {
            // Defensive: the connect flow routes a server `ErrorResponse` through
            // `EngineError::HandshakeServerError` (raw body → classified
            // `DriverError::Db` with the full SQLSTATE), so this `Copy` unit reaches
            // here ONLY via a non-connect re-drive of an already-failed engine. Kept
            // honest rather than removed.
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

#[cfg(test)]
mod dyn_cache_tests {
    //! Offline invariants of the dynamic prepared-statement cache — the pure
    //! data-structure logic (promotion, bounded eviction, take/restore/remove)
    //! driven directly, no wire. The end-to-end reuse + self-heal is witnessed
    //! by the `--ignored` live tests (they need a real server to Parse against).

    use super::{DynStmtCache, PreparedStatement, StmtName, WireStatement, DYN_STMT_CACHE_CAP};
    use std::sync::Arc;

    /// The parameter-type OID key most tests use (a single `int4` parameter). The
    /// cache is keyed on (SQL, OIDs), so a fixed key isolates the SQL-axis tests
    /// from the type-axis; `OIDS_B` exercises the type axis.
    const OIDS_A: &[u32] = &[23];
    /// A DIFFERENT parameter-type OID key (`float4`) — same wire width as `int4`,
    /// the exact silent-reinterpret pair the (SQL, OIDs) key defends against.
    const OIDS_B: &[u32] = &[700];

    /// A fabricated cached statement — the cache never inspects its contents (it
    /// only owns/moves it), so an empty OID/name payload is enough.
    fn fake_prepared(name: &str) -> PreparedStatement {
        let stmt_name = StmtName::try_from_str(name).expect("valid _bsql name");
        PreparedStatement {
            inner: WireStatement::new(stmt_name, Vec::new()),
            column_names: Arc::from(Vec::<String>::new().into_boxed_slice()),
            param_oids: Box::from([]),
            // These cache tests never route the handle through a connection's origin
            // check (they exercise `DynStmtCache` directly), so any id is fine.
            origin: 0,
        }
    }

    #[test]
    fn promotion_flips_pending_to_ready() {
        let mut cache = DynStmtCache::new();
        cache.note_pending("SELECT 1", OIDS_A);
        assert!(cache.is_pending("SELECT 1", OIDS_A));
        assert!(cache.ready_index("SELECT 1", OIDS_A).is_none());

        cache.promote("SELECT 1", OIDS_A, fake_prepared("_bsql_0"));
        assert!(!cache.is_pending("SELECT 1", OIDS_A));
        assert!(cache.ready_index("SELECT 1", OIDS_A).is_some());
        // Promotion is in place — no extra slot grew.
        assert_eq!(cache.slots.len(), 1);
    }

    #[test]
    fn take_restore_remove_round_trip() {
        let mut cache = DynStmtCache::new();
        cache.note_pending("Q", OIDS_A);
        cache.promote("Q", OIDS_A, fake_prepared("_bsql_1"));
        let idx = cache.ready_index("Q", OIDS_A).expect("ready");

        let stmt = cache.take(idx).expect("some");
        // Taken out → the slot reads as pending until restored.
        assert!(cache.is_pending("Q", OIDS_A));
        cache.restore(idx, stmt);
        assert!(cache.ready_index("Q", OIDS_A).is_some());

        cache.remove(idx);
        assert!(cache.ready_index("Q", OIDS_A).is_none());
        assert!(!cache.is_pending("Q", OIDS_A));
        assert_eq!(cache.slots.len(), 0);
    }

    #[test]
    fn bounded_at_capacity_evicting_oldest_pending() {
        let mut cache = DynStmtCache::new();
        for i in 0..DYN_STMT_CACHE_CAP {
            cache.note_pending(&format!("sql-{i}"), OIDS_A);
        }
        assert_eq!(cache.slots.len(), DYN_STMT_CACHE_CAP);
        // One more first-sighting evicts the OLDEST pending (sql-0), never grows.
        cache.note_pending("sql-new", OIDS_A);
        assert_eq!(cache.slots.len(), DYN_STMT_CACHE_CAP);
        assert!(!cache.is_pending("sql-0", OIDS_A), "oldest pending evicted");
        assert!(cache.is_pending("sql-new", OIDS_A));
        assert!(cache.is_pending("sql-1", OIDS_A), "younger pending retained");
    }

    #[test]
    fn ready_slots_are_never_evicted() {
        // Fill the cache with READY statements (promote every slot).
        let mut cache = DynStmtCache::new();
        for i in 0..DYN_STMT_CACHE_CAP {
            let sql = format!("ready-{i}");
            cache.note_pending(&sql, OIDS_A);
            cache.promote(&sql, OIDS_A, fake_prepared(&format!("_bsql_{i}")));
        }
        assert_eq!(cache.slots.len(), DYN_STMT_CACHE_CAP);
        // A NEW first sighting cannot evict a READY slot (which would leak its
        // server-side statement) — so it is simply NOT cached (stays fused).
        cache.note_pending("overflow", OIDS_A);
        assert_eq!(cache.slots.len(), DYN_STMT_CACHE_CAP);
        assert!(!cache.is_pending("overflow", OIDS_A), "overflow not tracked");
        // Every READY statement survived.
        for i in 0..DYN_STMT_CACHE_CAP {
            assert!(cache.ready_index(&format!("ready-{i}"), OIDS_A).is_some());
        }
    }

    #[test]
    fn note_pending_is_idempotent() {
        let mut cache = DynStmtCache::new();
        cache.note_pending("dup", OIDS_A);
        cache.note_pending("dup", OIDS_A);
        assert_eq!(cache.slots.len(), 1);
    }

    #[test]
    fn same_sql_distinct_param_oids_are_distinct_keys() {
        // The type-fidelity defense: the SAME SQL text bound with DIFFERENT
        // parameter types (`int4` vs `float4`, same 4-byte wire width) occupies
        // TWO independent slots — a REUSE never crosses them, so a plan prepared
        // for `int4` is never reused for a `float4` bind (which would reinterpret
        // the 4 bytes). Before the (SQL, OIDs) key, this was ONE slot.
        let mut cache = DynStmtCache::new();
        cache.note_pending("SELECT $1", OIDS_A);
        cache.promote("SELECT $1", OIDS_A, fake_prepared("_bsql_a"));
        // The float4-typed sighting of the SAME SQL is NOT a ready hit, NOT pending
        // under its own key — it is a fresh first sighting.
        assert!(cache.ready_index("SELECT $1", OIDS_B).is_none(), "no cross-type reuse");
        assert!(!cache.is_pending("SELECT $1", OIDS_B));
        cache.note_pending("SELECT $1", OIDS_B);
        cache.promote("SELECT $1", OIDS_B, fake_prepared("_bsql_b"));
        // Two independent slots — each keyed hit lands on its own plan.
        assert_eq!(cache.slots.len(), 2, "same SQL, two param-type keys → two slots");
        assert!(cache.ready_index("SELECT $1", OIDS_A).is_some());
        assert!(cache.ready_index("SELECT $1", OIDS_B).is_some());
        assert_ne!(
            cache.ready_index("SELECT $1", OIDS_A),
            cache.ready_index("SELECT $1", OIDS_B),
            "the two keys resolve to different slots",
        );
    }

    #[test]
    fn drain_returns_ready_statements_and_empties() {
        // Two READY + one PENDING: drain returns the two prepared statements (for
        // the caller to Close) and leaves the cache empty (the pool-checkout clear).
        let mut cache = DynStmtCache::new();
        cache.note_pending("r1", OIDS_A);
        cache.promote("r1", OIDS_A, fake_prepared("_bsql_1"));
        cache.note_pending("r2", OIDS_A);
        cache.promote("r2", OIDS_A, fake_prepared("_bsql_2"));
        cache.note_pending("p1", OIDS_A); // stays pending — no server-side statement

        let ready = cache.drain();
        assert_eq!(ready.len(), 2, "only the READY statements are returned to Close");
        assert_eq!(cache.slots.len(), 0, "the cache is empty after a drain");
        assert!(cache.ready_index("r1", OIDS_A).is_none());
        assert!(!cache.is_pending("p1", OIDS_A));
    }
}

#[cfg(test)]
mod connect_error_lift_tests {
    //! The connect-time server-error lift: a raw `ErrorResponse` body carried up
    //! by `EngineError::HandshakeServerError` (the connect flow's own error, from
    //! both drivers' `engine.connect().map_err(lift_engine_error)`) must classify
    //! to `DriverError::Db` with the FULL SQLSTATE — the SAME `parse_error_response`
    //! decode the ACTIVE path produces — so a consumer can `code()` / `is_*()` on a
    //! connect failure exactly as on a query failure. Offline + deterministic: the
    //! lift is `parse_error_response` + the `From<DbError>` box, both pure.
    use super::{lift_engine_error, DriverError, EngineError};

    /// Build a raw `ErrorResponse` BODY — the field list AFTER the tag+length
    /// header, i.e. the exact slice the engine's `frame_body` lends up: each field
    /// is `<type byte><value>\0`, terminated by a `\0` type byte (PG §55.7).
    fn error_body(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for (tag, text) in [(b'S', severity), (b'C', sqlstate), (b'M', message)] {
            body.push(tag);
            body.extend_from_slice(text.as_bytes());
            body.push(0);
        }
        body.push(0);
        body
    }

    /// The owner's named scenario: a connection-pool storm hitting the server's
    /// connection limit. `53300 too_many_connections` during connect must lift to a
    /// classified `DriverError::Db` a consumer can shed load on — `is_too_many_connections()`
    /// / `code() == "53300"` — never the former opaque
    /// `Io("server returned an error during startup")`.
    #[test]
    fn too_many_connections_connect_error_classifies_53300() {
        let body = error_body("FATAL", "53300", "sorry, too many clients already");
        let err = lift_engine_error(EngineError::HandshakeServerError(body.into_boxed_slice()));
        match err {
            DriverError::Db(db) => {
                assert_eq!(db.code(), "53300");
                assert!(
                    db.is_too_many_connections(),
                    "the 53300 predicate must hold on a CONNECT error, not only an active one",
                );
                assert!(db.message.contains("too many clients"));
            }
            other => panic!("a 53300 connect error must lift to DriverError::Db, got {other:?}"),
        }
    }

    /// The driver-level peer of the proto mid-SCRAM body-carry witness: the SAME
    /// lift path classifies the late-arm `28P01 invalid_password` body into a
    /// `DriverError::Db` carrying the full SQLSTATE + message.
    #[test]
    fn mid_scram_auth_connect_error_classifies_28p01() {
        let body = error_body("FATAL", "28P01", "password authentication failed");
        let err = lift_engine_error(EngineError::HandshakeServerError(body.into_boxed_slice()));
        match err {
            DriverError::Db(db) => {
                assert_eq!(db.code(), "28P01");
                assert!(db.message.contains("password authentication failed"));
            }
            other => panic!("a 28P01 connect error must lift to DriverError::Db, got {other:?}"),
        }
    }
}
