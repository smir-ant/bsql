//! Side-effect directives emitted by the protocol state machine.
//!
//! [`Action`]s are how the sans-I/O core communicates with whatever
//! sits outside it: "send these bytes", "deliver this reply", "fail
//! this reply", "close the socket". The async wrapper translates each
//! [`Action`] into the corresponding tokio call; a synchronous test
//! harness pattern-matches them directly. The protocol itself does
//! neither.
//!
//! # DEF-094 — staged dispatch + lifetime-bound SendBytes
//!
//! [`Action::SendBytes`] carries a `&'buf [u8]` reference into a
//! **caller-owned** [`crate::write_buf::WriteBuf`] that is passed to
//! every entry-point call. The host reads the slice, writes it to the
//! socket, and drops the [`Action`]; the backing bytes live in the
//! caller's `WriteBuf` until the caller reuses it on the next call
//! (each entry-point call clears the buffer at entry).
//!
//! The borrow-checker enforces the "consume before next call"
//! invariant at compile time: [`OutActions<'buf>`] borrows the
//! caller's `WriteBuf` for `'buf`; the next `&mut WriteBuf` call is
//! rejected while any `Action<'buf>` is alive. Zero-copy with tier-1
//! compile enforcement. **Inspection via `proto.state()` still works
//! alongside** — `OutActions` does NOT borrow `PgProtocol`, only the
//! separate `WriteBuf`, so shared `&self` reads on the protocol are
//! never blocked.
//!
//! Internally, dispatchers emit [`StagedAction`] values (range-based,
//! no refs) during the write phase; the entry-point materialises them
//! into ref-bound [`Action<'buf>`]s once the mutable write phase
//! completes. This two-phase split sidesteps the borrow-checker
//! conflict that had blocked an earlier DEF-094 attempt: holding
//! `Action<'buf>::SendBytes(&'buf [u8])` while re-entering the
//! dispatcher for the next frame in the same `feed_bytes` call.

use crate::error::ProtocolError;
use crate::protocol::MAX_ACTIONS_PER_CALL;
use core::num::{NonZeroU16, NonZeroU64};

/// PostgreSQL transaction-status indicator carried in every
/// `ReadyForQuery` frame (PG §55.7).
///
/// PG defines exactly three legal values on the wire:
/// `'I'` (idle), `'T'` (in-transaction), `'E'` (failed transaction
/// — needs `ROLLBACK`). Any other byte is a server-side wire
/// violation and classifies as
/// [`crate::ProtocolError::MalformedReadyForQuery`] at dispatch
/// time — users never receive an invalid `TxStatus`.
///
/// # Tier-1 compile guarantees for consumers
///
/// Exhaustive `match` on `TxStatus` catches every legal state at
/// build time. A refactor that adds a new PG tx-status (future
/// spec change) forces every consumer to handle it. Compare to
/// the pre-uplift `tx_status: u8` form where a byte-match had no
/// compiler help and forgetting the `'E'` arm was a tier-3 audit
/// seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TxStatus {
    /// `'I'` — idle, no transaction in progress.
    Idle = b'I',
    /// `'T'` — inside an explicit or implicit transaction block.
    InTransaction = b'T',
    /// `'E'` — transaction failed; commands are ignored until
    /// `ROLLBACK` or `ROLLBACK TO SAVEPOINT`.
    Failed = b'E',
}

impl TxStatus {
    /// Parse a PG wire byte into the typed status.
    ///
    /// Returns `Err(b)` carrying the offending byte when `b` is
    /// outside `{'I', 'T', 'E'}` — lets callers forward the actual
    /// rejected value to diagnostics if they choose. Mirrors the
    /// `FormatCode::try_from_wire_i16` shape. F-009 (pass-#8).
    #[inline]
    pub const fn try_from_byte(b: u8) -> Result<Self, u8> {
        match b {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::Failed),
            other => Err(other),
        }
    }

    /// The underlying PG wire byte. Used by builders + diagnostics.
    ///
    /// Explicit match (not `self as u8`) — the crate forbids
    /// `clippy::as_conversions`. With `#[repr(u8)]` and explicit
    /// discriminants above, each arm is a direct literal lookup;
    /// LLVM folds the match to a constant per monomorphic call.
    #[inline]
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

/// Typed non-empty range into a write buffer, replacing the raw
/// `(start, end): (usize, usize)` pair on [`StagedAction::SendBytesRange`].
/// DEF-100.
///
/// # Invariants
///
/// - `start` is the offset where the emission began.
/// - `len` is `NonZeroU16` — construction of a zero-length range
///   is a type-level impossibility, which in turn makes
///   `Action::SendBytes(&[])` a type-level impossibility along the
///   range path.
/// - At construction, `start.saturating_add(len) ≤ bounds` is
///   checked; the constructor returns `None` otherwise.
///
/// # Tier elevation
///
/// Before DEF-100, `SendBytesRange { start, end }` carried two raw
/// `usize`s with no proof of `start ≤ end` or `end ≤ write_buf.len()`.
/// `materialise` fell back silently to `&[]` on any violation — a
/// tier-3 audit-enforced seam. After DEF-100:
///
/// - `start ≤ end` is guaranteed by `len: NonZeroU16` built via
///   `end.checked_sub(start)?` — you cannot construct a range with
///   `start > end` (the `checked_sub` yields `None`).
/// - `end ≤ bounds` is checked explicitly in [`NonEmptyRange::new`].
/// - `materialise`'s `.apply(buf)` can only return `None` if a bug
///   in the caller passes a `buf` shorter than the emission-time
///   `bounds` — architecturally the same buffer is used, so this
///   branch is dead at call-site level.
///
/// # DEF-147 size narrowing
///
/// Storage narrowed from `usize + NonZeroUsize` (16 B on 64-bit) to
/// `u16 + NonZeroU16` (4 B). Valid because all range endpoints
/// originate in buffers bounded by `READ_BUF_CAP = 4096` or
/// `MAX_OWNED_SEND_LEN = 2176`, both ≤ `u16::MAX = 65535`
/// (const-asserted at `crate::buf::READ_BUF_CAP`). On a 1000-row
/// SELECT, 12 KB of stack traffic saved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NonEmptyRange {
    start: u16,
    len: NonZeroU16,
}

impl NonEmptyRange {
    /// Construct a non-empty range validated against a buffer length.
    /// Returns `None` if `start > end`, `end > bounds`, or the range
    /// is empty (`start == end`).
    ///
    /// DEF-147: signature stays `(usize, usize, usize)` for call-site
    /// compat. The `u16::try_from` narrowing fallbacks are
    /// architecturally dead for bounded buffer offsets
    /// (≤ READ_BUF_CAP ≤ u16::MAX) but the explicit try-from satisfies
    /// the forbid bundle's ban on `as` conversions.
    #[inline]
    pub(crate) fn new(start: usize, end: usize, bounds: usize) -> Option<Self> {
        if end > bounds {
            return None;
        }
        // end >= start is enforced by checked_sub returning Some.
        // len > 0 is enforced by NonZeroU16::new.
        let len_usize = end.checked_sub(start)?;
        // Narrow to u16: architecturally dead for bounded buffers
        // but satisfies forbid-bundle.
        let len_u16 = u16::try_from(len_usize).ok()?;
        let len = NonZeroU16::new(len_u16)?;
        let start_u16 = u16::try_from(start).ok()?;
        Some(Self { start: start_u16, len })
    }

    /// Construct from a write operation into `write_buf`: capture
    /// `start` before the writes; after the writes, `write_buf.len()`
    /// is the post-state end. Returns `None` if no bytes were
    /// written since `start`.
    ///
    /// This is the primary constructor at emission sites — it ties
    /// the range's validity to the `write_buf` state at emission.
    #[inline]
    pub(crate) fn from_write_span(start: usize, write_buf: &crate::write_buf::WriteBuf) -> Option<Self> {
        Self::new(start, write_buf.len(), write_buf.len())
    }

    /// Resolve the range against a buffer, returning the slice or
    /// `None` on bounds mismatch.
    ///
    /// The None branch is architecturally unreachable when `buf` is
    /// the same `write_buf` used at construction — the constructor
    /// already proved `start + len ≤ bounds` and we use the same
    /// buffer at `materialise` time.
    #[inline]
    pub(crate) fn apply<'a>(&self, buf: &'a [u8]) -> Option<&'a [u8]> {
        // F-007 (pass-#8): the None path is architecturally dead when
        // `buf` is the same buffer used at NonEmptyRange::new construction
        // (materialise's invariant — see protocol.rs::materialise). The
        // Option-returning shape is retained for forbid-bundle safety,
        // but debug-builds actively assert the invariant so a wiring
        // regression fails the test suite loudly instead of silently
        // producing `&[]`.
        // DEF-147: widen u16 → usize via infallible usize::from before
        // slice indexing.
        let start = usize::from(self.start);
        let end = start.checked_add(usize::from(self.len.get()))?;
        let slice = buf.get(start..end);
        debug_assert!(
            slice.is_some(),
            "NonEmptyRange::apply: buf shorter than emission-time bounds — check materialise wiring",
        );
        slice
    }
}

// DEF-147 drift pin: NonEmptyRange packs u16 + NonZeroU16 = 4 B.
// 1000-row SELECT: 12 KB of per-row stack traffic saved (vs the
// pre-DEF-147 16 B form).
const _: () = assert!(
    core::mem::size_of::<NonEmptyRange>() == 4,
    "NonEmptyRange size regression — DEF-147 narrowed storage to \
     u16 + NonZeroU16 = 4 B. Buffer offsets ≤ READ_BUF_CAP ≤ u16::MAX \
     are const-asserted at crate::buf::READ_BUF_CAP.",
);

/// Bounded list of actions emitted by a single protocol entry-point
/// call.
///
/// # POD, no Drop
///
/// [`OutActions`] is a pure-POD struct (`Copy` + no `Drop` impl) —
/// a fixed `[Action<'buf>; MAX_ACTIONS_PER_CALL]` + `u8` length,
/// not a `heapless::Vec` (which carries an unconditional `Drop`
/// impl even for `Copy` elements). The POD form lets Rust's NLL
/// release the `'buf` borrow at last-use rather than end-of-scope,
/// so tests do NOT need explicit `drop(out)` calls between
/// consecutive entry-point invocations.
///
/// # Lifetime
///
/// The `'buf` lifetime ties [`Action::SendBytes`] references back to
/// the caller-owned [`crate::write_buf::WriteBuf`] that was passed
/// to `feed_bytes` / `push_command`. While any emitted
/// `Action<'buf>::SendBytes` is still alive, the caller cannot
/// re-borrow `&mut WriteBuf` — the borrow checker refuses.
///
/// `MAX_ACTIONS_PER_CALL` is intentionally tiny in Phase 1a — see
/// its definition in `protocol.rs` for the per-method audit.
/// Overflow handling is compile-enforced via the `emit_actions!`
/// macro's `const _: () = assert!(MAX_ACTIONS_PER_CALL >= budget)`
/// checks at every push site.
///
/// # Stack-init cost and the no-unsafe tradeoff
///
/// `OutActions::new()` writes a `CloseSocket` sentinel into all 8
/// slots (3 KB under current `size_of::<Action>() ≈ 384 B`). Every
/// `feed_bytes` / `push_command` call pays this init — even the
/// typical 1-action Ping path walks the full array.
///
/// The eager-init path is the **cost of `#![forbid(unsafe_code)]`**.
/// Safe alternatives considered:
/// - `[MaybeUninit<Action>; 8]` + `assume_init` on populated slots
///   — requires `unsafe`, forbidden.
/// - `[Option<Action>; 8]` — `Option<Action>` is 392 B (discriminant
///   + pad), strictly WORSE than the sentinel-fill.
/// - Drop the sentinel and only-initialise populated slots via
///   `heapless::Vec<Action, 8>` — propagates `heapless::Vec<T>`'s
///   Drop into `OutActions`, breaking the POD shape required for
///   NLL last-use borrow-release (tests would need explicit
///   `drop(out)` between calls).
///
/// Net: the 3 KB init cost is the Pareto-optimal choice under the
/// forbid bundle. Shrinking `Action` itself shrinks this
/// proportionally — DEF-119's schema-arena refactor (1c-5)
/// externalises `RowDesc` out of `Reply::*Complete` payloads,
/// dropping `Action` from ~384 B to ~128 B and `OutActions` init
/// cost from 3 KB to ~1 KB.
#[derive(Debug, Clone, Copy)]
pub struct OutActions<'w, 'r> {
    /// Fixed slot storage; slots past `len` hold the default
    /// sentinel ([`Action::CloseSocket`]) from construction.
    items: [Action<'w, 'r>; MAX_ACTIONS_PER_CALL],
    /// Number of populated slots in `items[..len]`. `u8` suffices
    /// since `MAX_ACTIONS_PER_CALL` is tiny (currently 8 post-1c-1b
    /// bump from 4).
    len: u8,
}

impl Default for OutActions<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'w, 'r> OutActions<'w, 'r> {
    /// Construct an empty `OutActions`.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        // Fill with the Copy `CloseSocket` sentinel; the `len`
        // field tracks the actual occupancy.
        Self {
            items: [Action::CloseSocket; MAX_ACTIONS_PER_CALL],
            len: 0,
        }
    }

    /// Number of populated actions.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        // `u8 → usize` via `From` impl (infallible, widening). `as`
        // casts are banned by the crate forbid bundle; `usize::from`
        // is the only accepted form.
        usize::from(self.len)
    }

    /// Whether no actions have been pushed.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrow the populated prefix as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[Action<'w, 'r>] {
        self.items.get(..self.len()).unwrap_or(&[])
    }

    /// Return the first populated action (or `None` if empty).
    /// Convenience for test assertions.
    #[inline]
    pub fn first(&self) -> Option<&Action<'w, 'r>> {
        self.as_slice().first()
    }

    /// Push an action. Returns `Err(action)` (mirrors heapless's
    /// convention) if the container is full.
    #[inline]
    #[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; mirrors heapless::Vec::push API. Err is only hit under architecturally-bounded overflow (compile-time emit_actions! budget).")]
    pub fn push(&mut self, action: Action<'w, 'r>) -> Result<(), Action<'w, 'r>> {
        let idx = self.len();
        if idx >= MAX_ACTIONS_PER_CALL {
            return Err(action);
        }
        let Some(slot) = self.items.get_mut(idx) else {
            return Err(action);
        };
        *slot = action;
        self.len = self.len.saturating_add(1);
        Ok(())
    }
}

impl<'w, 'r> IntoIterator for OutActions<'w, 'r> {
    type Item = Action<'w, 'r>;
    type IntoIter = OutActionsIter<'w, 'r>;
    fn into_iter(self) -> Self::IntoIter {
        OutActionsIter { inner: self, pos: 0 }
    }
}

/// By-reference iteration — `for action in &out` yields `&Action`.
///
/// F-002 (pass-#8): callers who want to inspect actions without
/// consuming the container previously had to reach for
/// `out.as_slice().iter()`. This `IntoIterator for &OutActions` impl
/// makes `for a in &out { ... }` do the right thing natively. Both
/// by-value (`for a in out`) and by-reference (`for a in &out`) are
/// supported with the same ergonomic shape.
impl<'a, 'w, 'r> IntoIterator for &'a OutActions<'w, 'r> {
    type Item = &'a Action<'w, 'r>;
    type IntoIter = core::slice::Iter<'a, Action<'w, 'r>>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

/// Move-iterator for [`OutActions`]. Produces each populated
/// [`Action<'w, 'r>`] in insertion order, then ends.
#[derive(Debug)]
pub struct OutActionsIter<'w, 'r> {
    inner: OutActions<'w, 'r>,
    pos: u8,
}

impl<'w, 'r> Iterator for OutActionsIter<'w, 'r> {
    type Item = Action<'w, 'r>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.inner.len {
            return None;
        }
        let idx = usize::from(self.pos);
        let item = *self.inner.items.get(idx)?;
        self.pos = self.pos.saturating_add(1);
        Some(item)
    }
}

/// Internal staged list: dispatchers emit `StagedAction` during the
/// write-phase (`&mut write_buf`-holding) loop, the entry-point
/// materialises them into [`Action<'buf>`] in phase two (shared
/// borrow of `write_buf`). `pub(crate)` — not a public API.
///
/// # Why `heapless::Vec`, NOT the `OutActions` POD-array shape
///
/// A recurring audit suggestion is "for consistency with `OutActions`,
/// replace this alias with `[StagedAction; N] + u8 len`". **Rejected** —
/// `heapless::Vec<T, N>` is actually the BETTER choice here, not the
/// worse one:
///
/// - **Memory layout**: `heapless::Vec<T, N>` internally stores
///   `[MaybeUninit<T>; N]`. Stack footprint is identical to a POD
///   array (`N * size_of::<T>()`).
/// - **Init cost**: `heapless::Vec::new()` writes ZERO initialised
///   slots. A POD array `[StagedAction::CloseSocket; 8]` would write
///   ALL 8 slots eagerly, even when the typical call uses only 1-2.
///   That's hundreds of bytes of wasted stack init per entry-point
///   call (Ping=1, SimpleQuery=1, Parse=2, etc.).
/// - **Drop**: `heapless::Vec<Copy, N>` DOES have an `impl Drop`, but
///   its body is empty for Copy elements and LLVM elides it. The
///   "Drop propagation" concern is a compile-time trait bound
///   phantom, not a runtime cost.
/// - **Safety**: a hand-rolled POD with `[MaybeUninit<T>; N]` would
///   require `unsafe { assume_init_read }` on reads. Sans-I/O core
///   holds zero `unsafe` — that is a crate-level architectural rule.
///
/// So: the current alias is the Pareto-optimal choice — smaller init
/// than POD, smaller surface than `unsafe`, same memory footprint.
/// Future "consistency" refactors must address all three points
/// before proposing the change.
pub(crate) type StagedActions = heapless::Vec<StagedAction, MAX_ACTIONS_PER_CALL>;

/// A directive from the protocol to its host.
///
/// # Lifetime
///
/// `'buf` is the lifetime of the host's caller-owned [`crate::write_buf::WriteBuf`].
/// [`Action::SendBytes`] carries `&'buf [u8]` — either a reference
/// into that `WriteBuf` (for runtime-built frames) or a static
/// reference (for compile-time constants; `'static: 'buf`).
///
/// # Two lifetimes (1c-1a)
///
/// `'w` names bytes living in the caller's `WriteBuf` (outbound —
/// `SendBytes`). `'r` names bytes living in the protocol's
/// `ReadBuf` (inbound — `StreamRow`). Two distinct lifetimes
/// because the two buffers are distinct sources and the borrow
/// checker needs the information to enforce:
///
/// - Next `&mut WriteBuf` call blocked while `SendBytes(&'w …)`
///   alive (DEF-094 invariant).
/// - Next `&mut PgProtocol` call blocked while `StreamRow(&'r …)`
///   alive — the row slice is inside `self.read_buf`, so
///   `feed_bytes` takes `&'r mut self` and the output's `'r`
///   borrows back from `self`.
///
/// `#[non_exhaustive]` because more variants land with later
/// sub-phases. Internal `match` over `Action` is *not*
/// `non_exhaustive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use = "an Action carries a side-effect that must be executed"]
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable; DEF-119 shrunk DeliverReply's Reply<'r> payload from ~312 B to ~88 B — FailReply.cause (ProtocolError ~312 B) is now the dominant variant. FailReply is emitted only on protocol failure (cold path), never in the hot streaming path.")]
pub enum Action<'w, 'r> {
    /// Send these bytes verbatim to the server.
    ///
    /// The slice references the caller-owned [`crate::write_buf::WriteBuf`]
    /// (for runtime-built frames) or static storage (for compile-time
    /// constants; `'static: 'w`). The host reads the slice, writes
    /// it to the socket, and drops the [`Action`]; no data is copied
    /// out of the protocol. Zero-copy.
    ///
    /// The `'w` lifetime ensures the slice is valid for exactly
    /// as long as the owning `OutActions<'w, '_>` is alive — the
    /// next `&mut WriteBuf` call is blocked by the borrow checker
    /// until the caller drops `OutActions`.
    SendBytes(&'w [u8]),

    /// Deliver a successful reply to the wrapper.
    ///
    /// The wrapper looks up its `oneshot::Sender` by `id` and forwards
    /// `value`. The protocol does not keep any record after emitting
    /// this action.
    ///
    /// The `id` here is the raw `NonZeroU64` the command's `ReplyId`
    /// was built from — the protocol state machine called
    /// [`crate::ReplyId::consume`] on the handle to produce this value,
    /// which marks the reply as delivered (see the Drop-guard on
    /// `ReplyId`). The wrapper only needs the raw value to route;
    /// the consume-tracking handle is an internal protocol concept.
    DeliverReply {
        /// The correlator the user originally supplied with their
        /// command.
        id: NonZeroU64,
        /// The typed payload.
        ///
        /// DEF-119: `Reply<'r>` borrows schema data from the arena
        /// via the `'r` lifetime (same as `StreamRow::row_bytes`).
        value: Reply<'r>,
    },

    /// Deliver a failure to the wrapper.
    ///
    /// Same routing as `DeliverReply`; the wrapper translates `cause`
    /// into its public error type.
    FailReply {
        /// The correlator the user originally supplied with their
        /// command.
        id: NonZeroU64,
        /// Why the protocol failed the in-flight command.
        cause: ProtocolError,
    },

    /// Stream one row of a query result to the wrapper.
    ///
    /// The `row_bytes` slice points into the protocol's `ReadBuf`
    /// and is valid for the lifetime of the owning `OutActions<'_, 'r>`.
    /// The wrapper copies / decodes the bytes before the next
    /// `feed_bytes` call — architecturally enforced because that
    /// call requires `&'r mut self` which blocks while the row
    /// reference is alive. Zero-copy row delivery.
    ///
    /// The `id` matches the in-flight query's `ReplyId<QueryKind>`;
    /// the `ReplyId` itself is NOT consumed here — rows are
    /// "in-progress signals", the terminal `CommandComplete` consumes
    /// the `ReplyId` into `DeliverReply { value: QueryComplete {…} }`.
    ///
    /// # 1c-2a: `desc` schema reference
    ///
    /// `desc` points to [`crate::RowDesc`] inside
    /// [`crate::PgProtocol`], populated by the preceding
    /// `RowDescription` frame. Its lifetime `'r` ties it to the
    /// `&'r mut PgProtocol` borrow — the schema stays available
    /// exactly as long as the row bytes. Pairing is tier-2
    /// structural: a `StreamRow` action can't exist without a
    /// matching schema because `SimpleQueryStreamingRows` state is
    /// entered only via the 'T' dispatcher arm which populates the
    /// schema before staging any row.
    ///
    /// Round-4 finding #1 / 1c-1a; row_desc wiring in 1c-2a.
    ///
    /// F19 (2026-04-21): `desc` is carried BY VALUE, not by reference.
    /// Rationale: after F19 embedded `RowDesc` directly into the
    /// `SimpleQueryStreamingRows { row_desc }` state variant (tier-3
    /// audit-paired slot → tier-2 structural pairing), the schema's
    /// lifetime is now bounded by the StreamingRows variant — which
    /// may transition to `Idle` before `materialise` runs at the end
    /// of `feed_bytes`. By-value avoids the self-referential lifetime
    /// issue (Action would need to borrow from state, but state mutates
    /// mid-loop). Cost: `Action::StreamRow` grows from ~32 bytes to
    /// ~292 bytes, matching the existing `Action::DeliverReply`
    /// envelope (which already carries a full `RowDesc` via
    /// `QueryCompletePayload`). `OutActions` size bump is ~96 bytes
    /// on the `feed_bytes` stack frame — acceptable for the tier win.
    StreamRow {
        /// Correlator of the in-flight query.
        id: NonZeroU64,
        /// Raw row bytes as delivered by PG (PG's `DataRow` body —
        /// column-count prefix followed by per-column
        /// length/bytes pairs). Parsing into typed columns happens
        /// via [`crate::decode`] primitives (1c-2b).
        row_bytes: &'r [u8],
        /// Result-set schema — type OIDs + format codes per column.
        /// Stable across the entire row stream.
        ///
        /// DEF-119: borrows from `PgProtocol`'s schema arena via `'r`.
        /// Previously carried BY VALUE (260-byte copy per row);
        /// now an 8-byte reference. On a 1000-row SELECT this saves
        /// ~260 KB of per-row copy traffic. The arena slot is held
        /// live by `SimpleQueryStreamingRows { schema_ref }` state
        /// throughout the stream and freed at the terminal RFQ →
        /// Idle transition (after the Reply's `&RowDesc` borrow
        /// ended when `OutActions` dropped).
        desc: &'r crate::decode::RowDesc,
    },

    /// The socket is no longer safe to use; close it.
    ///
    /// Emitted alongside a failed reply when the connection is
    /// out-of-sync with the server (malformed framing, unexpected
    /// frame, etc.). The wrapper must close the underlying transport;
    /// the pool then discards this connection.
    CloseSocket,
}

/// Internal staging variant emitted by dispatchers during the
/// write-phase loop.
///
/// `StagedAction` carries ranges into the caller's `WriteBuf` (not
/// references) and owned values (for DeliverReply / FailReply). No
/// lifetime. Materialised by the entry-point into [`Action<'buf>`]
/// once the mutable write-phase completes.
///
/// Two variants map to [`Action::SendBytes`] at materialisation:
///
/// - [`Self::SendBytesRange`] — bytes were written into
///   `write_buf[start..end]`; the materialiser emits a slice ref
///   into that range.
/// - [`Self::SendBytesStatic`] — bytes are a compile-time `'static`
///   constant (e.g. the 5-byte `Sync` wire payload); the
///   materialiser emits the static ref directly (zero write, zero
///   copy — `Sync` bypasses `write_buf` entirely).
#[derive(Debug)]
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable; DEF-119 shrunk StagedAction::DeliverReply's StagedReply payload from ~312 B to ~80 B — FailReply.cause (ProtocolError ~312 B) is now the dominant variant. FailReply is emitted only on protocol failure (cold path), never in the hot streaming path. Mirrors the `Action<'w, 'r>` rationale.")]
pub(crate) enum StagedAction {
    /// Bytes live at the range `[start..start+len]` inside the
    /// emission-time `write_buf`. Typed as [`NonEmptyRange`] —
    /// non-zero length is a type invariant (DEF-100).
    SendBytesRange(NonEmptyRange),
    /// Bytes are a static compile-time constant. Materialiser passes
    /// through directly — no write, no copy.
    SendBytesStatic(&'static [u8]),
    /// Map to [`Action::DeliverReply`]. Opaque [`DeliverReplyEntry`]
    /// — the only construction path is [`deliver`] (below), which
    /// enforces kind-payload pairing at compile time via
    /// [`crate::reply_id::ReplyKind::Payload`]. DEF-112.
    DeliverReply(DeliverReplyEntry),
    /// Map to [`Action::FailReply`].
    FailReply {
        /// Raw correlator (post-consume of the `ReplyId`).
        id: NonZeroU64,
        /// Why the protocol failed.
        cause: ProtocolError,
    },
    /// Map to [`Action::StreamRow`] at materialise time. `row_range`
    /// is absolute coordinates into [`crate::buf::ReadBuf::populated`]
    /// — the full populated region of the inbound buffer, including
    /// bytes already advanced past the cursor. 1c-1b.
    ///
    /// Absolute (not unread-relative) coordinates survive the
    /// cursor advance that `feed_bytes` performs between dispatch
    /// and materialise: the bytes themselves remain in place until
    /// the next `append` triggers lazy compaction (which in turn
    /// cannot run while the `OutActions<'_, 'r>` borrow is alive).
    ///
    /// F19: carries `RowDesc` by value (copied from the
    /// `SimpleQueryStreamingRows { row_desc }` state variant at
    /// emission time). Avoids the lifetime issue of pointing at state
    /// that may have transitioned (StreamingRows → AwaitingRfq → Idle)
    /// by materialise time.
    StreamRowRange {
        /// Raw correlator (`reply.get()`; reply is NOT consumed here
        /// — rows are in-progress, the reply commits on terminal
        /// `CommandComplete`).
        id: NonZeroU64,
        /// Absolute range into the read buffer's populated region.
        row_range: NonEmptyRange,
        /// Schema arena handle (copy from StreamingRows state).
        ///
        /// DEF-119: 1-byte `SchemaRef` handle instead of the prior
        /// 260-byte `RowDesc` copy. Materialise resolves via
        /// `arena.get(schema_ref)` to a `&'r RowDesc` reference for
        /// the public `Action::StreamRow`.
        schema_ref: crate::schema_arena::SchemaRef,
    },
    /// Map to [`Action::CloseSocket`].
    CloseSocket,
}

/// Internal lifetime-free counterpart to the public [`Reply<'r>`].
///
/// # DEF-119 rationale
///
/// Dispatch runs BEFORE materialise. At dispatch time, the state
/// machine holds `SchemaRef` arena handles (no borrowing lifetime).
/// At materialise time, `PgProtocol.schema_arena` is borrowed for
/// `'r` and handles resolve to `&'r RowDesc` refs for public
/// `Reply<'r>`. `StagedReply` is the lifetime-free intermediate.
///
/// Variants mirror `Reply<'r>` 1:1. Schema-bearing variants carry
/// staged payload types (with `schema_ref` fields instead of
/// `&'r RowDesc`); schema-less variants share payloads with the
/// public side.
///
/// # Visibility
///
/// The type is nominally `pub` because `ReplyKind::StagedPayload`
/// (in the public sealed `ReplyKind` trait) references
/// `Staged*Payload` types which must wrap into `StagedReply` via
/// `Into<StagedReply>`. Rust requires `pub trait`'s associated
/// types (and the types they wrap into) to be `pub`. But this is
/// **`#[doc(hidden)]` + crate-internal-by-convention** — external
/// users should never construct or match on `StagedReply`; it's
/// exclusively produced/consumed inside the crate's dispatch /
/// materialise pipeline.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedReply {
    #[doc(hidden)] Pong(PongPayload),
    #[doc(hidden)] StartupComplete(StartupCompletePayload),
    #[doc(hidden)] QueryComplete(StagedQueryCompletePayload),
    #[doc(hidden)] ParseComplete(ParseCompletePayload),
    #[doc(hidden)] CloseComplete(CloseCompletePayload),
    #[doc(hidden)] DescribeStatementComplete(StagedDescribeStatementCompletePayload),
    #[doc(hidden)] DescribePortalComplete(StagedDescribePortalCompletePayload),
}

/// Lifetime-free staged counterpart to [`QueryCompletePayload<'r>`].
///
/// Holds `schema_ref: Option<SchemaRef>` instead of the public
/// `Option<&'r RowDesc>`. Materialise converts via
/// `arena.get(ref).map(|desc| ...)`.
///
/// `#[doc(hidden)] pub` — see [`StagedReply`] for visibility rationale.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedQueryCompletePayload {
    #[doc(hidden)] pub command_tag: crate::ident::BoundedStr<32>,
    #[doc(hidden)] pub tx_status: TxStatus,
    #[doc(hidden)] pub schema_ref: Option<crate::schema_arena::SchemaRef>,
}

/// Lifetime-free staged counterpart to
/// [`DescribeStatementCompletePayload<'r>`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedDescribeStatementCompletePayload {
    #[doc(hidden)] pub param_oids: ParamOids,
    #[doc(hidden)] pub rows: crate::state::DescribedRowsRef,
    #[doc(hidden)] pub tx_status: TxStatus,
}

/// Lifetime-free staged counterpart to
/// [`DescribePortalCompletePayload<'r>`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedDescribePortalCompletePayload {
    #[doc(hidden)] pub rows: crate::state::DescribedRowsRef,
    #[doc(hidden)] pub tx_status: TxStatus,
}

// From impls: schema-less payloads wrap directly; schema-bearing
// payloads use their staged form to produce a StagedReply.

impl From<PongPayload> for StagedReply {
    #[inline]
    fn from(p: PongPayload) -> Self {
        Self::Pong(p)
    }
}
impl From<StartupCompletePayload> for StagedReply {
    #[inline]
    fn from(p: StartupCompletePayload) -> Self {
        Self::StartupComplete(p)
    }
}
impl From<StagedQueryCompletePayload> for StagedReply {
    #[inline]
    fn from(p: StagedQueryCompletePayload) -> Self {
        Self::QueryComplete(p)
    }
}
impl From<ParseCompletePayload> for StagedReply {
    #[inline]
    fn from(p: ParseCompletePayload) -> Self {
        Self::ParseComplete(p)
    }
}
impl From<CloseCompletePayload> for StagedReply {
    #[inline]
    fn from(p: CloseCompletePayload) -> Self {
        Self::CloseComplete(p)
    }
}
impl From<StagedDescribeStatementCompletePayload> for StagedReply {
    #[inline]
    fn from(p: StagedDescribeStatementCompletePayload) -> Self {
        Self::DescribeStatementComplete(p)
    }
}
impl From<StagedDescribePortalCompletePayload> for StagedReply {
    #[inline]
    fn from(p: StagedDescribePortalCompletePayload) -> Self {
        Self::DescribePortalComplete(p)
    }
}

impl StagedReply {
    /// Resolve this staged reply into the public [`Reply<'r>`] by
    /// looking up any arena-borrowed schema via `arena.get(ref)`.
    ///
    /// # Stale-ref handling
    ///
    /// Architecturally every `SchemaRef` in a StagedReply points to
    /// a live slot at materialise time — the dispatch arms that
    /// construct these staged payloads allocate the slot immediately
    /// before the StagedReply is queued, and the slot stays live
    /// through materialise. A stale `schema_ref` (slot freed
    /// prematurely) is a crate-internal bug; `get` returns `None`
    /// which the conversion maps to `Option::None` / `NoData` —
    /// silently substituting absence. This is tier-3 "crate bug =
    /// degraded diagnostic" and is called out in the arena module's
    /// alloc/free discipline docstring.
    #[inline]
    pub(crate) fn into_public<'r>(
        self,
        arena: &'r crate::schema_arena::SchemaSlab,
    ) -> Reply<'r> {
        match self {
            Self::Pong(p) => Reply::Pong(p),
            Self::StartupComplete(p) => Reply::StartupComplete(p),
            Self::QueryComplete(staged) => Reply::QueryComplete(QueryCompletePayload {
                command_tag: staged.command_tag,
                tx_status: staged.tx_status,
                row_desc: staged.schema_ref.and_then(|r| arena.get(r)),
            }),
            Self::ParseComplete(p) => Reply::ParseComplete(p),
            Self::CloseComplete(p) => Reply::CloseComplete(p),
            Self::DescribeStatementComplete(staged) => {
                Reply::DescribeStatementComplete(DescribeStatementCompletePayload {
                    param_oids: staged.param_oids,
                    rows: described_rows_ref_into_public(staged.rows, arena),
                    tx_status: staged.tx_status,
                })
            }
            Self::DescribePortalComplete(staged) => {
                Reply::DescribePortalComplete(DescribePortalCompletePayload {
                    rows: described_rows_ref_into_public(staged.rows, arena),
                    tx_status: staged.tx_status,
                })
            }
        }
    }
}

/// Convert state-side [`crate::state::DescribedRowsRef`] to the
/// public [`DescribedRows<'r>`] by resolving any `SchemaRef` into a
/// borrow. Stale ref (crate bug) maps to `NoData` silently —
/// safer than producing a dangling reference.
///
/// F8 intent markers: uses [`DescribedRows::from_row_desc`] and
/// [`DescribedRows::no_data`] factories rather than direct variant
/// construction. Swapping the arm bodies still type-checks, but the
/// factory names make the swap obvious on code review and the
/// `arena.get(s)` resolution arm explicit.
#[inline]
fn described_rows_ref_into_public<'r>(
    r: crate::state::DescribedRowsRef,
    arena: &'r crate::schema_arena::SchemaSlab,
) -> DescribedRows<'r> {
    match r {
        crate::state::DescribedRowsRef::Rows(s) => match arena.get(s) {
            Some(desc) => DescribedRows::from_row_desc(desc),
            // Arena slot freed early — architecturally dead. Fall back
            // to NoData rather than dangle.
            None => DescribedRows::no_data(),
        },
        crate::state::DescribedRowsRef::NoData => DescribedRows::no_data(),
    }
}

// ═════════════════════════════════════════════════════════════════
// §2 / DEF-112 — typed DeliverReply gate
//
// The sole authority to construct a `StagedAction::DeliverReply` is
// the `deliver()` function below, whose generic signature
// `fn deliver<K: ReplyKind>(id: ReplyId<K>, payload: K::Payload) ->
// StagedAction` forces the reply id's kind and the payload type to
// match via the `ReplyKind::Payload` associated type.
//
// Passing a `ReplyId<PingKind>` with a `StartupCompletePayload` is
// a compile error (mismatched associated type). The historical
// runtime misroute class — dispatcher emits wrong `Reply` variant
// for the kind — becomes a tier-1 compile invariant.
//
// The nested `mod deliver_entry_priv` wraps the struct so its
// fields are module-private: even code inside `action.rs` (outside
// the inner module) cannot directly construct
// `DeliverReplyEntry { id, value }`. The only escape hatch is the
// internal `pub(super) fn new(...)` constructor, called once from
// `deliver()` itself.
// ═════════════════════════════════════════════════════════════════

mod deliver_entry_priv {
    use super::{NonZeroU64, StagedReply};

    /// Opaque payload for [`super::StagedAction::DeliverReply`].
    ///
    /// Fields are module-private (`deliver_entry_priv`-only
    /// visibility). The only constructor is `pub(super) fn new`,
    /// reachable exclusively from [`super::deliver`] — which in
    /// turn requires a typed [`crate::reply_id::ReplyId<K>`] and
    /// its matching `K::StagedPayload`. DEF-112 + DEF-119.
    ///
    /// DEF-119: carries [`StagedReply`] (lifetime-free) rather than
    /// the public [`super::Reply`] (which now has a `'r` lifetime
    /// tied to the arena). Materialise converts staged → public via
    /// `StagedReply::into_public(&arena)`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DeliverReplyEntry {
        id: NonZeroU64,
        value: StagedReply,
    }

    impl DeliverReplyEntry {
        /// Module-gated constructor — called only from
        /// [`super::deliver`]. Sealing this constructor at
        /// `pub(super)` is the load-bearing mechanism: a rogue
        /// dispatcher cannot produce a `DeliverReplyEntry` outside
        /// the typed path.
        #[inline]
        pub(super) const fn new(id: NonZeroU64, value: StagedReply) -> Self {
            Self { id, value }
        }

        /// Read access for the materialiser. `pub(crate)` because
        /// `protocol::materialise` lives outside this module.
        #[inline]
        pub(crate) const fn id(&self) -> NonZeroU64 {
            self.id
        }

        /// Read access for the materialiser. DEF-119: returns
        /// `StagedReply` (not the lifetime-bound public `Reply<'r>`)
        /// — materialise borrows the arena and converts.
        #[inline]
        pub(crate) const fn staged(&self) -> StagedReply {
            self.value
        }
    }
}

pub(crate) use deliver_entry_priv::DeliverReplyEntry;

/// Construct a [`StagedAction::DeliverReply`] from a typed
/// [`ReplyId<K>`](crate::reply_id::ReplyId) and its kind-matching
/// STAGED payload.
///
/// The `K: ReplyKind` bound + the `K::StagedPayload` argument type
/// jointly enforce at the call site that the payload matches the
/// reply id's kind. Passing a `ReplyId<PingKind>` with a
/// `StartupCompletePayload` — or any other mismatch — fails to
/// compile. DEF-112 tier-1 elevation of the "wrong payload per
/// reply kind" class; preserved across DEF-119 via
/// `ReplyKind::StagedPayload`.
#[inline]
#[must_use]
pub(crate) fn deliver<K: crate::reply_id::ReplyKind>(
    id: crate::reply_id::ReplyId<K>,
    payload: K::StagedPayload,
) -> StagedAction {
    StagedAction::DeliverReply(DeliverReplyEntry::new(id.consume(), payload.into()))
}

/// A typed protocol reply payload.
///
/// Each variant tuple-wraps its matching `*Payload` struct — the
/// payload IS the variant's inner. One source of truth: adding or
/// renaming a field on `PongPayload` immediately changes what
/// `Reply::Pong(..)` matches; no parallel field list to keep in
/// sync (DEF-112 drift seam closed).
///
/// `#[non_exhaustive]` because more variants (`BindComplete`,
/// `BackendKeyData`, …) land in later sub-phases.
///
/// # DEF-119 — lifetime `'r`
///
/// Schema-bearing payloads (`QueryComplete`, `DescribeStatementComplete`,
/// `DescribePortalComplete`) previously owned a 260-byte `RowDesc`
/// inline. DEF-119 externalises the schema into `PgProtocol`'s
/// [`crate::schema_arena::SchemaSlab`]; payloads now borrow
/// `&'r RowDesc` from the arena. The `'r` lifetime ties the borrow
/// to the `&'r mut PgProtocol` that produced the reply — same
/// lifetime as [`Action::StreamRow::row_bytes`], so both row bytes
/// and row schema have identical validity windows.
///
/// **User code ergonomics unchanged**: pattern-match on the variant
/// and access `payload.row_desc` / `payload.rows` as before; the
/// only difference is the field type is `Option<&RowDesc>` /
/// `DescribedRows<'r>` rather than owned.
///
/// **Lifetime-irrelevant variants** (Pong, StartupComplete,
/// ParseComplete, CloseComplete) carry no schema; the `'r` parameter
/// is phantom for them. Rust permits unused lifetime parameters on
/// enums — only the schema-bearing variants constrain `'r`.
///
/// # Size impact
///
/// Pre-DEF-119: `Reply` ~340 B (dominated by DescribeStatementComplete's
/// inline RowDesc + ParamOids).
/// Post-DEF-119: `Reply<'r>` ~96 B (DescribeStatementComplete holds
/// `&RowDesc` ref + ParamOids 68 + TxStatus = ~80 B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply<'r> {
    /// The server is alive and responsive. See [`PongPayload`].
    Pong(PongPayload),

    /// The startup handshake completed successfully. The connection
    /// is now in [`crate::ProtoState::Idle`] and ready for queries.
    /// See [`StartupCompletePayload`].
    StartupComplete(StartupCompletePayload),

    /// A Query / BindExecute command completed. Delivered on the
    /// terminal `CommandComplete + ReadyForQuery` pair at the end
    /// of the result stream. Rows (if any) were emitted individually
    /// via `Action::StreamRow` (sub-phase 1c-1). See
    /// [`QueryCompletePayload`].
    QueryComplete(QueryCompletePayload<'r>),

    /// A `Parse` command succeeded (server accepted the prepared
    /// statement). See [`ParseCompletePayload`]. 1c-3a.
    ParseComplete(ParseCompletePayload),

    /// A `Close` of a prepared statement or portal succeeded.
    /// See [`CloseCompletePayload`] (ZST — no body).
    CloseComplete(CloseCompletePayload),

    /// A statement-level `Describe` (`'D' 'S' name`) completed. See
    /// [`DescribeStatementCompletePayload`]. 1c-3c.
    DescribeStatementComplete(DescribeStatementCompletePayload<'r>),

    /// A portal-level `Describe` (`'D' 'P' name`) completed. See
    /// [`DescribePortalCompletePayload`]. 1c-3c.
    DescribePortalComplete(DescribePortalCompletePayload<'r>),
}

// ═════════════════════════════════════════════════════════════════
// Typed per-kind payload structs (DEF-112)
//
// Each `ReplyKind` in `reply_id.rs` has an associated `Payload`
// type. The `From<Payload> for Reply` impls tuple-wrap the payload
// into the matching `Reply::X(..)` variant — one-line bridge, no
// field list to drift.
// ═════════════════════════════════════════════════════════════════

/// Typed payload for [`crate::reply_id::PingKind`] replies.
///
/// The server confirmed it is alive and responsive.
///
/// Carries the [transaction-status indicator] from the matching
/// `ReadyForQuery` payload byte: `'I'` idle, `'T'` in-transaction,
/// `'E'` failed transaction.
///
/// [transaction-status indicator]: https://www.postgresql.org/docs/current/protocol-message-formats.html
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PongPayload {
    /// Transaction-status indicator byte (`'I'`, `'T'`, `'E'`) from
    /// the matching `ReadyForQuery` frame.
    pub tx_status: TxStatus,
}

impl<'r> From<PongPayload> for Reply<'r> {
    #[inline]
    fn from(p: PongPayload) -> Self {
        Self::Pong(p)
    }
}

/// Typed payload for [`crate::reply_id::StartupKind`] replies.
///
/// Delivered on the final `ReadyForQuery` that closes the startup
/// handshake. Carries the backend process ID / secret key (for
/// cancel requests) and the transaction-status byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupCompletePayload {
    /// Backend process ID from the `BackendKeyData` frame.
    pub pid: i32,
    /// Backend secret key (for cancel requests).
    pub secret_key: i32,
    /// Transaction status from the final `ReadyForQuery`.
    pub tx_status: TxStatus,
}

impl<'r> From<StartupCompletePayload> for Reply<'r> {
    #[inline]
    fn from(p: StartupCompletePayload) -> Self {
        Self::StartupComplete(p)
    }
}

/// Typed payload for [`crate::reply_id::QueryKind`] replies.
///
/// Delivered on `CommandComplete` at the end of a simple-query or
/// extended-query result stream. `command_tag` is the raw ASCII
/// tag PG returns (`"SELECT 5"`, `"INSERT 0 3"`, etc.) —
/// sub-phase 1c-6 parses this into a typed `CommandTag` struct
/// (round-4 finding #3).
///
/// DEF-119: `row_desc` borrows from `PgProtocol`'s schema arena via
/// the `'r` lifetime. `Some(&desc)` for SELECT (including 0-row),
/// `None` for DML / empty-query. The schema stays valid for the
/// lifetime of the owning `OutActions<'w, 'r>`; the next
/// `&mut PgProtocol` call is blocked by the borrow checker until
/// the caller drops the actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCompletePayload<'r> {
    /// Raw ASCII tag from `CommandComplete` body.
    pub command_tag: crate::ident::BoundedStr<32>,
    /// Transaction-status indicator from the trailing `ReadyForQuery`.
    pub tx_status: TxStatus,
    /// Result-set schema, if any. `Some` for SELECT (including
    /// 0-row SELECTs), `None` for DML / empty-query.
    ///
    /// DEF-119: borrowed from the arena. Previously owned
    /// (`Option<RowDesc>`, 260 B inline); now `Option<&'r RowDesc>`
    /// (8 B ref).
    pub row_desc: Option<&'r crate::decode::RowDesc>,
}

impl<'r> From<QueryCompletePayload<'r>> for Reply<'r> {
    #[inline]
    fn from(p: QueryCompletePayload<'r>) -> Self {
        Self::QueryComplete(p)
    }
}

/// Typed payload for [`crate::reply_id::ParseKind`] replies.
///
/// Carries the transaction-status byte from the trailing RFQ —
/// uniform with the other payloads. Was a ZST in 1c-2a; widened
/// in 1c-3a to preserve the tx_status value the dispatcher
/// already validates (architect-audit silent-discard fix).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseCompletePayload {
    /// Transaction-status indicator from the trailing `ReadyForQuery`.
    pub tx_status: TxStatus,
}

impl<'r> From<ParseCompletePayload> for Reply<'r> {
    #[inline]
    fn from(p: ParseCompletePayload) -> Self {
        Self::ParseComplete(p)
    }
}

/// Typed payload for [`crate::reply_id::CloseKind`] replies.
///
/// `CloseComplete` carries no body; ZST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CloseCompletePayload;

impl<'r> From<CloseCompletePayload> for Reply<'r> {
    #[inline]
    fn from(p: CloseCompletePayload) -> Self {
        Self::CloseComplete(p)
    }
}

// ═════════════════════════════════════════════════════════════════
// 1c-3c — Describe command payloads + helper types
//
// Two payload types (statement / portal) instead of one payload
// with `Option<ParamOids>`. Rationale: a user who called
// `DescribeStatement` always gets param OIDs back; a user who
// called `DescribePortal` never does. The split surfaces this as
// TWO distinct `Reply` variants, so the `oneshot::Receiver<Reply>`
// resolves with the payload shape that matches the command — no
// runtime `match Option` + no surface-level "why is this None?"
// ambiguity. DEF-112 kind-parameterisation carries the guarantee
// all the way into the `Action::DeliverReply` construction site.
// ═════════════════════════════════════════════════════════════════

/// Rows-or-not result of a [`crate::PgCommand::DescribeStatement`]
/// / [`crate::PgCommand::DescribePortal`] query.
///
/// PG sends EITHER a `RowDescription` (the statement/portal produces
/// a result-set) OR a `NoData` (`'n'`) response (no result columns —
/// e.g. plain `INSERT` without `RETURNING`). This sum type preserves
/// the distinction at the type level.
///
/// # Why named variants over `Option<RowDesc>`
///
/// `Option<RowDesc>` is functionally equivalent but semantically
/// weaker: `None` could mean "server explicitly sent NoData" OR
/// "we never set this field". The named variants make the server's
/// intent explicit in one glance, and a future refactor cannot
/// accidentally "forget to set" the field — constructing
/// [`DescribedRows`] forces picking one of the two documented PG
/// outcomes. Tier-1 clarity win.
///
/// # DEF-119 — borrowed `&'r RowDesc`
///
/// Prior to DEF-119 this enum embedded a 260-byte `RowDesc` inline
/// in the `Rows` variant, triggering `clippy::large_enum_variant`.
/// DEF-119 externalises the schema into `PgProtocol`'s schema
/// arena; the `Rows` variant now holds a `&'r RowDesc` reference
/// borrowed through the `'r` lifetime tied to the `&'r mut PgProtocol`
/// borrow.
///
/// Size: ~8 B (ref + discriminant) vs ~264 B pre-arena. The
/// `large_enum_variant` expect can be dropped alongside this
/// refactor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribedRows<'r> {
    /// Server sent a `RowDescription` (`'T'`) — the statement/portal
    /// produces result columns. The schema borrows from
    /// `PgProtocol`'s schema arena; `'r` matches the containing
    /// `Reply<'r>` / `Action<'_, 'r>` lifetime.
    Rows(&'r crate::decode::RowDesc),
    /// Server sent `NoData` (`'n'`) — the statement/portal has no
    /// result columns. DML without `RETURNING` is the common case.
    NoData,
}

impl<'r> DescribedRows<'r> {
    /// Construct from a parsed `RowDescription` (tag `'T'`, PG §55.7).
    ///
    /// F8 (pass-#7 audit): named constructors give the materialise
    /// arm an intent-telling alias to pair with `TAG_ROW_DESCRIPTION`.
    /// A swap at the arm body (`Rows(desc)` ↔ `NoData`) still
    /// compiles but tests flag the mismatch; this factory marks
    /// the construction site with human-readable intent that will
    /// fail a code review if inverted.
    #[inline]
    #[must_use]
    pub(crate) const fn from_row_desc(desc: &'r crate::decode::RowDesc) -> Self {
        Self::Rows(desc)
    }

    /// Construct the no-data sentinel. Pair to [`Self::from_row_desc`]
    /// — used in the dispatch arm for `TAG_NO_DATA` (`'n'`).
    #[inline]
    #[must_use]
    pub(crate) const fn no_data() -> Self {
        Self::NoData
    }
}

/// Bounded list of parameter OIDs returned by server `ParameterDescription`
/// (`'t'`) in response to a statement-level Describe.
///
/// POD shape: `n_params` (populated count) + `[u32; MAX_PARAMS_ARITY]`
/// array. `Copy + Default`. Mirrors the [`crate::decode::RowDesc`]
/// layout one-to-one.
///
/// # Bound
///
/// The capacity is [`crate::params::MAX_PARAMS_ARITY`] = 16, which
/// matches the crate's Bind-side cap. A server returning more OIDs
/// than that means the SQL declared more placeholders than we can
/// ever Bind against — classified as
/// [`crate::error::ProtocolError::TooManyParameters`] at parse time,
/// since the result is useless downstream.
///
/// # Niche
///
/// Not niche-friendly (`u32` OIDs, empty slots fill with 0 which IS
/// a valid OID sentinel). `Option<ParamOids>` keeps its full 4-byte
/// length-count overhead. Since the reply shape always includes
/// ParamOids (statement-describe only), we never need
/// `Option<ParamOids>` — the type is always present at the API
/// surface.
// Layout pinned `#[repr(C, align(4))]` (pass-#7 F4):
//
// - `align(4)` matches the natural alignment of `[u32; _]` — no
//   drift possible if future reorders the fields.
// - `repr(C)` nails field order: `n_params: u16` at offset 0,
//   2 bytes padding, `oids` at offset 4, no trailing pad (total
//   = 4 + 16*4 = 68).
//
// The padding bytes at offsets 2..4 are ALWAYS zero via the
// `EMPTY` / `from_parts` constructors (both initialise `oids` from
// a fully-populated `[u32; N]`, and the `n_params: u16` slot leaves
// its two padding bytes untouched — `Copy` struct init zeroes
// padding in practice, but to remain portable across future
// refactors, the `const _: () = assert!` below pins size and
// alignment so any drift fails the build.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(4))]
pub struct ParamOids {
    n_params: u16,
    oids: [u32; crate::params::MAX_PARAMS_ARITY],
}

// Drift pin: bumping `MAX_PARAMS_ARITY` or swapping the field
// order without updating this assertion fails the build. Size
// must be `2 (u16) + 2 (pad) + 4 * MAX_PARAMS_ARITY`.
const _: () = assert!(
    core::mem::size_of::<ParamOids>()
        == 4usize.saturating_add(4usize.saturating_mul(crate::params::MAX_PARAMS_ARITY)),
    "ParamOids layout drift — expected size = 4 (u16 + 2-byte pad) + 4 * MAX_PARAMS_ARITY",
);
const _: () = assert!(
    core::mem::align_of::<ParamOids>() == 4,
    "ParamOids alignment drift — expected 4 (u32-aligned oids array forces this)",
);
// SIMD-wide PartialEq pin: tail slots are constructor-filled with
// 0 so full-array `self.oids == other.oids` is byte-equivalent to
// a populated-prefix compare (Finding 5 — defensible full-array eq).
// Requiring total array size ≤ 64 bytes keeps it within a single
// AVX2 register. If `MAX_PARAMS_ARITY` grows past 16, revisit eq
// strategy (populated-prefix might become cheaper than the wide
// compare).
const _: () = assert!(
    4usize.saturating_mul(crate::params::MAX_PARAMS_ARITY) <= 64,
    "ParamOids eq is SIMD-wide (≤64 bytes). \
     Revisit populated-prefix eq if MAX_PARAMS_ARITY grows > 16.",
);

impl Default for ParamOids {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl ParamOids {
    /// Empty descriptor (0 parameters). Used as the default for
    /// statements that declare no parameters.
    pub const EMPTY: Self = Self {
        n_params: 0,
        oids: [0; crate::params::MAX_PARAMS_ARITY],
    };

    /// Construct from a populated count + a full-capacity OID array.
    /// `pub(crate)` — only the parser creates these; users read.
    #[inline]
    #[must_use]
    pub(crate) const fn from_parts(
        n_params: u16,
        oids: [u32; crate::params::MAX_PARAMS_ARITY],
    ) -> Self {
        Self { n_params, oids }
    }

    /// Number of populated parameters.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.n_params)
    }

    /// Whether the descriptor carries any parameters.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.n_params == 0
    }

    /// Borrow the populated OIDs as a slice — tail default-filled
    /// slots are not exposed.
    #[inline]
    #[must_use]
    pub fn oids(&self) -> &[u32] {
        self.oids.get(..self.len()).unwrap_or(&[])
    }

    /// Get one OID by index, or `None` if out of range.
    ///
    /// # Returns `Option<u32>` by value (not `Option<&u32>`)
    ///
    /// On 64-bit targets `u32` is 4 bytes vs `&u32` at 8 bytes. Returning
    /// the value directly is strictly smaller. The asymmetry with
    /// [`crate::decode::RowDesc::get`] (which returns `Option<&ColumnDesc>`)
    /// is **intentional and size-driven**: `ColumnDesc` is 8 bytes on
    /// x86_64, so `&ColumnDesc` (8 B) and `ColumnDesc` (8 B) are
    /// equivalent at the ABI — but `&ColumnDesc` preserves identity
    /// if the caller wants pointer-stability. For `ParamOids::get`
    /// none of those pointer-stability arguments apply: OIDs are
    /// opaque u32 catalog lookups, never-mutated, cheap to copy.
    ///
    /// Do NOT "normalise" this to `Option<&u32>` in a future refactor
    /// — the by-value form is deliberate.
    #[inline]
    #[must_use]
    pub fn get(&self, idx: usize) -> Option<u32> {
        if idx >= self.len() {
            return None;
        }
        self.oids.get(idx).copied()
    }
}

// `ParamOids` uses full-array Eq (tail slots are constructor-filled
// with 0 and never mutated; byte-equality of the arrays implies
// logical equality of populated-prefix semantics). Same pattern as
// `RowDesc` in decode.rs.
impl PartialEq for ParamOids {
    fn eq(&self, other: &Self) -> bool {
        self.n_params == other.n_params && self.oids == other.oids
    }
}
impl Eq for ParamOids {}

/// Typed payload for [`crate::reply_id::DescribeStatementKind`] replies.
///
/// Delivered on the trailing `ReadyForQuery` after a statement-level
/// Describe. Carries:
///
/// - `param_oids` — PG type OIDs for each placeholder the statement
///   expects, parsed from the `ParameterDescription` (`'t'`) frame.
/// - `rows` — `Rows(..)` if a `RowDescription` arrived, `NoData` if a
///   `NoData` (`'n'`) arrived.
/// - `tx_status` — from the final RFQ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribeStatementCompletePayload<'r> {
    /// Parameter OIDs, in positional order (`$1`, `$2`, …).
    pub param_oids: ParamOids,
    /// Rows-or-no-data sum from the subsequent response frame.
    /// DEF-119: `DescribedRows` now holds a `&'r RowDesc` borrow.
    pub rows: DescribedRows<'r>,
    /// Transaction status from the trailing `ReadyForQuery`.
    pub tx_status: TxStatus,
}

impl<'r> From<DescribeStatementCompletePayload<'r>> for Reply<'r> {
    #[inline]
    fn from(p: DescribeStatementCompletePayload<'r>) -> Self {
        Self::DescribeStatementComplete(p)
    }
}

/// Typed payload for [`crate::reply_id::DescribePortalKind`] replies.
///
/// Delivered on the trailing `ReadyForQuery` after a portal-level
/// Describe. Carries:
///
/// - `rows` — `Rows(..)` if a `RowDescription` arrived, `NoData` if a
///   `NoData` (`'n'`) arrived.
/// - `tx_status` — from the final RFQ.
///
/// No `param_oids` field: portals are bound-state handles; their
/// parameter values were fixed at Bind time, so PG does not replay
/// a `ParameterDescription` frame for a portal-Describe per PG §55.2.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribePortalCompletePayload<'r> {
    /// Rows-or-no-data sum from the response frame.
    /// DEF-119: `DescribedRows` now holds a `&'r RowDesc` borrow.
    pub rows: DescribedRows<'r>,
    /// Transaction status from the trailing `ReadyForQuery`.
    pub tx_status: TxStatus,
}

impl<'r> From<DescribePortalCompletePayload<'r>> for Reply<'r> {
    #[inline]
    fn from(p: DescribePortalCompletePayload<'r>) -> Self {
        Self::DescribePortalComplete(p)
    }
}
