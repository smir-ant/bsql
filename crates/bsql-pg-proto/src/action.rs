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
use core::num::{NonZeroU64, NonZeroUsize};

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
    /// Parse a PG wire byte into the typed status. `None` for any
    /// byte outside `{'I', 'T', 'E'}` — caller classifies as
    /// malformed wire.
    #[inline]
    #[must_use]
    pub const fn try_from_byte(b: u8) -> Option<Self> {
        match b {
            b'I' => Some(Self::Idle),
            b'T' => Some(Self::InTransaction),
            b'E' => Some(Self::Failed),
            _ => None,
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
/// - `len` is `NonZeroUsize` — construction of a zero-length range
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
/// - `start ≤ end` is guaranteed by `len: NonZeroUsize` built via
///   `end.checked_sub(start)?` — you cannot construct a range with
///   `start > end` (the `checked_sub` yields `None`).
/// - `end ≤ bounds` is checked explicitly in [`NonEmptyRange::new`].
/// - `materialise`'s `.apply(buf)` can only return `None` if a bug
///   in the caller passes a `buf` shorter than the emission-time
///   `bounds` — architecturally the same buffer is used, so this
///   branch is dead at call-site level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NonEmptyRange {
    start: usize,
    len: NonZeroUsize,
}

impl NonEmptyRange {
    /// Construct a non-empty range validated against a buffer length.
    /// Returns `None` if `start > end`, `end > bounds`, or the range
    /// is empty (`start == end`).
    #[inline]
    pub(crate) fn new(start: usize, end: usize, bounds: usize) -> Option<Self> {
        if end > bounds {
            return None;
        }
        let len = end.checked_sub(start)?;
        let len = NonZeroUsize::new(len)?;
        Some(Self { start, len })
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
        let end = self.start.checked_add(self.len.get())?;
        buf.get(self.start..end)
    }
}

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
/// Uses `heapless::Vec` (which does carry `Drop` — but that's fine
/// for an internal staging type that doesn't leak lifetimes).
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
        value: Reply,
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
    StreamRowRange {
        /// Raw correlator (`reply.get()`; reply is NOT consumed here
        /// — rows are in-progress, the reply commits on terminal
        /// `CommandComplete`).
        id: NonZeroU64,
        /// Absolute range into the read buffer's populated region.
        row_range: NonEmptyRange,
    },
    /// Map to [`Action::CloseSocket`].
    CloseSocket,
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
    use super::{NonZeroU64, Reply};

    /// Opaque payload for [`super::StagedAction::DeliverReply`].
    ///
    /// Fields are module-private (`deliver_entry_priv`-only
    /// visibility). The only constructor is `pub(super) fn new`,
    /// reachable exclusively from [`super::deliver`] — which in
    /// turn requires a typed [`crate::reply_id::ReplyId<K>`] and
    /// its matching `K::Payload`. DEF-112.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DeliverReplyEntry {
        id: NonZeroU64,
        value: Reply,
    }

    impl DeliverReplyEntry {
        /// Module-gated constructor — called only from
        /// [`super::deliver`]. Sealing this constructor at
        /// `pub(super)` is the load-bearing mechanism: a rogue
        /// dispatcher cannot produce a `DeliverReplyEntry` outside
        /// the typed path.
        #[inline]
        pub(super) fn new(id: NonZeroU64, value: Reply) -> Self {
            Self { id, value }
        }

        /// Read access for the materialiser. `pub(crate)` because
        /// `protocol::materialise` lives outside this module.
        #[inline]
        pub(crate) fn id(&self) -> NonZeroU64 {
            self.id
        }

        /// Read access for the materialiser.
        #[inline]
        pub(crate) fn value(&self) -> Reply {
            self.value
        }
    }
}

pub(crate) use deliver_entry_priv::DeliverReplyEntry;

/// Construct a [`StagedAction::DeliverReply`] from a typed
/// [`ReplyId<K>`](crate::reply_id::ReplyId) and its kind-matching
/// payload.
///
/// The `K: ReplyKind` bound + the `K::Payload` argument type jointly
/// enforce at the call site that the payload matches the reply id's
/// kind. Passing a `ReplyId<PingKind>` with a
/// `StartupCompletePayload` — or any other mismatch — fails to
/// compile. DEF-112 tier-1 elevation of the "wrong payload per
/// reply kind" class.
#[inline]
#[must_use]
pub(crate) fn deliver<K: crate::reply_id::ReplyKind>(
    id: crate::reply_id::ReplyId<K>,
    payload: K::Payload,
) -> StagedAction {
    StagedAction::DeliverReply(DeliverReplyEntry::new(id.consume(), payload.into()))
}

/// A typed protocol reply payload.
///
/// `#[non_exhaustive]` because more variants (`QueryResult`, `Row`,
/// `BackendKeyData`, …) land with their drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[expect(
    clippy::large_enum_variant,
    reason = "1c-2a: no_alloc crate; QueryComplete.row_desc carries an inline RowDesc (MAX_ROW_COLUMNS=32 columns × 8 bytes ≈ 260 bytes). Pong / ParseComplete / CloseComplete variants are small. Shrinking would require boxing row_desc, unavailable without alloc; the alternative (caller-managed schema arena) lands with DEF-119 pipelining in 1c-5."
)]
pub enum Reply {
    /// The server is alive and responsive.
    ///
    /// Carries the [transaction-status indicator] from the matching
    /// `ReadyForQuery` payload byte: `'I'` idle, `'T'` in-transaction,
    /// `'E'` failed transaction. In Phase 1a we surface it raw; the
    /// transaction state machine (1c) consumes it.
    ///
    /// [transaction-status indicator]: https://www.postgresql.org/docs/current/protocol-message-formats.html
    Pong {
        /// The single payload byte — `'I'`, `'T'`, or `'E'`.
        tx_status: TxStatus,
    },

    /// The startup handshake completed successfully.
    ///
    /// The connection is now in [`crate::ProtoState::Idle`] and ready
    /// for queries. Carries the backend process ID and secret key
    /// (for cancel requests) and the transaction status byte from the
    /// final `ReadyForQuery`.
    StartupComplete {
        /// Backend process ID from `BackendKeyData`.
        pid: i32,
        /// Backend secret key from `BackendKeyData` (cancel key).
        secret_key: i32,
        /// Transaction status from `ReadyForQuery` (`'I'`, `'T'`, `'E'`).
        tx_status: TxStatus,
    },

    // ───────────────── Phase 1c reply variants ─────────────────
    //
    // Payloads are stubs — sub-phase 1c-1 fills the CommandTag
    // parsing, 1c-3 fills the Parse/Close result shapes.

    /// A Query / BindExecute command completed. Delivered on the
    /// terminal `CommandComplete + ReadyForQuery` pair at the end
    /// of the result stream. Rows (if any) were emitted
    /// individually via `Action::StreamRow` (sub-phase 1c-1).
    QueryComplete {
        /// Raw ASCII command tag (`"SELECT 5"` etc.). 1c-1 ships a
        /// parser into a typed `CommandTag` struct via round-4
        /// finding #3; this raw form is the interim shape.
        command_tag: crate::ident::BoundedStr<32>,
        /// Transaction status from the trailing `ReadyForQuery`.
        tx_status: TxStatus,
        /// Result-set schema. `Some` for SELECT queries (including
        /// 0-row SELECTs — `RowDescription` arrived but no
        /// `DataRow`s followed); `None` for DML (no
        /// `RowDescription`) and empty-query responses. 1c-2a.
        row_desc: Option<crate::decode::RowDesc>,
    },

    /// A `Parse` command succeeded (server accepted the prepared
    /// statement). Carries no additional data.
    ParseComplete,

    /// A `Close` of a prepared statement or portal succeeded.
    /// Carries no additional data.
    CloseComplete,
}

// ═════════════════════════════════════════════════════════════════
// Typed per-kind payload structs (DEF-112)
//
// Each `ReplyKind` in `reply_id.rs` has an associated `Payload`
// type. The `From<Payload> for Reply` impls are the only bridges
// from typed payload → erased sum; the typed dispatcher path
// (`deliver` above) relies on them.
// ═════════════════════════════════════════════════════════════════

/// Typed payload for [`crate::reply_id::PingKind`] replies.
///
/// Mirrors the `Reply::Pong` variant's field layout. Separate type
/// so the dispatcher's kind-to-payload bond is type-enforced rather
/// than audit-enforced (DEF-112).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PongPayload {
    /// Transaction-status indicator byte (`'I'`, `'T'`, `'E'`) from
    /// the matching `ReadyForQuery` frame.
    pub tx_status: TxStatus,
}

impl From<PongPayload> for Reply {
    #[inline]
    fn from(p: PongPayload) -> Self {
        Self::Pong {
            tx_status: p.tx_status,
        }
    }
}

/// Typed payload for [`crate::reply_id::StartupKind`] replies.
///
/// Mirrors the `Reply::StartupComplete` variant's field layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupCompletePayload {
    /// Backend process ID from the `BackendKeyData` frame.
    pub pid: i32,
    /// Backend secret key (for cancel requests).
    pub secret_key: i32,
    /// Transaction status from the final `ReadyForQuery`.
    pub tx_status: TxStatus,
}

impl From<StartupCompletePayload> for Reply {
    #[inline]
    fn from(p: StartupCompletePayload) -> Self {
        Self::StartupComplete {
            pid: p.pid,
            secret_key: p.secret_key,
            tx_status: p.tx_status,
        }
    }
}

// ───────────────── Phase 1c payload stubs ─────────────────────
//
// Payloads for Query / Parse / Close kinds land in sub-phase
// 1c-1 (simple query) and 1c-3 (extended). These stubs pin the
// DEF-112 sealed-deliver pathway for each new kind; payload
// structure is minimal to start.

/// Typed payload for [`crate::reply_id::QueryKind`] replies.
///
/// Delivered on `CommandComplete` at the end of a simple-query or
/// extended-query result stream. `command_tag` is the raw ASCII
/// tag PG returns (`"SELECT 5"`, `"INSERT 0 3"`, etc.) —
/// sub-phase 1c-1 parses this into a typed `CommandTag` struct
/// (round-4 finding #3).
///
/// 1c-2a: `row_desc` carries the result-set schema — `Some` for
/// SELECT queries (even 0-row), `None` for DML / empty-query. The
/// schema is copied by value (RowDesc is `Copy` POD).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCompletePayload {
    /// Raw ASCII tag from `CommandComplete` body.
    pub command_tag: crate::ident::BoundedStr<32>,
    /// Transaction-status indicator from the trailing `ReadyForQuery`.
    pub tx_status: TxStatus,
    /// Result-set schema, if any. See [`Reply::QueryComplete::row_desc`].
    pub row_desc: Option<crate::decode::RowDesc>,
}

impl From<QueryCompletePayload> for Reply {
    #[inline]
    fn from(p: QueryCompletePayload) -> Self {
        Self::QueryComplete {
            command_tag: p.command_tag,
            tx_status: p.tx_status,
            row_desc: p.row_desc,
        }
    }
}

/// Typed payload for [`crate::reply_id::ParseKind`] replies.
///
/// `ParseComplete` carries no body; the struct is a ZST.
/// Preserves the DEF-112 sealed-deliver pathway for Parse's
/// `ReplyId<ParseKind>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParseCompletePayload;

impl From<ParseCompletePayload> for Reply {
    #[inline]
    fn from(_: ParseCompletePayload) -> Self {
        Self::ParseComplete
    }
}

/// Typed payload for [`crate::reply_id::CloseKind`] replies.
///
/// `CloseComplete` carries no body; ZST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CloseCompletePayload;

impl From<CloseCompletePayload> for Reply {
    #[inline]
    fn from(_: CloseCompletePayload) -> Self {
        Self::CloseComplete
    }
}
