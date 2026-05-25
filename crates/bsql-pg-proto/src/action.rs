//! Side-effect directives emitted by the protocol state machine.
//!
//! [`Action`]s are how the sans-I/O core communicates with whatever
//! sits outside it: "send these bytes", "deliver this reply", "fail
//! this reply", "close the socket". The async wrapper translates each
//! [`Action`] into the corresponding tokio call; a synchronous test
//! harness pattern-matches them directly. The protocol itself does
//! neither.
//!
//! # Staged dispatch + lifetime-bound SendBytes
//!
//! [`Action::SendBytes`] carries a `&'buf [u8]` reference into a
//! **caller-owned** [`crate::write_buf::WriteBuf`] that is passed to
//! every entry-point call. The host reads the slice, writes it to the
//! socket, and drops the [`Action`]; the backing bytes live in the
//! caller's `WriteBuf` until the caller reuses it on the next call
//! (each entry-point call clears the buffer at entry).
//!
//! The borrow-checker enforces the "consume before next call"
//! invariant at compile time: [`OutActions<'w>`] holds two
//! distinct borrows — `'w` ties [`Action::SendBytes`] back to the
//! caller's [`crate::write_buf::WriteBuf`] (`&mut WriteBuf` is
//! rejected while any `Action<'w, _>` is alive), and `'r` ties row-
//! arena slices inside `Reply<'r>` payloads back to `PgProtocol`'s
//! shared read state (the next `&mut PgProtocol` call is rejected
//! while any arena-borrowing Reply is alive). Both are zero-copy
//! with tier-1 compile enforcement. **Inspection via `proto.state()`
//! still works alongside** — `'r` is a *shared* (`&self`) reborrow,
//! so `&self`-method calls on the protocol are never blocked; only
//! `&mut self`-method reentry is gated.
//!
//! See the type-level doc on [`OutActions`] for the audit
//! investigation (Tier-3 #25, 2026-05-19) that confirmed the
//! 2-lifetime form is structurally load-bearing.
//!
//! Internally, dispatchers emit [`StagedAction`] values (range-based,
//! no refs) during the write phase; the entry-point materialises them
//! into ref-bound [`Action<'buf>`]s once the mutable write phase
//! completes. A naive shape that emitted ref-bound `Action<'buf>`
//! directly from the dispatcher hit a borrow-checker conflict: the
//! dispatcher would hold `Action<'buf>::SendBytes(&'buf [u8])` while
//! re-entering itself for the next frame in the same `feed_bytes`
//! call. Two-phase staging sidesteps that conflict.

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
/// spec change) forces every consumer to handle it. A naive
/// `tx_status: u8` form has no compiler help for the byte-match
/// — forgetting the `'E'` arm would be a tier-3
/// review-discipline seam.
///
/// # NOT `#[non_exhaustive]`
///
/// PG §55.7 defines `{'I', 'T', 'E'}` and this set is closed by
/// the wire protocol — a fourth status would require a major
/// protocol revision. Sealing via `non_exhaustive` would force
/// downstream catch-all arms for a case that **cannot exist on a
/// well-formed wire**; the dispatcher rejects non-{I,T,E} bytes
/// at framing-time as `MalformedReadyForQuery`. Closed-by-spec →
/// exhaustive `match` is the load-bearing tier-1 invariant.
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
    /// `FormatCode::try_from_wire_i16` shape.
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

// Round-trip compile pin for TxStatus.
// `try_from_byte(byte(v)) == Ok(v)` must hold for every variant —
// catches a body-swap drift (e.g. `Self::Idle => b'T'`) at build
// time rather than in an integration test. Tier-1 compile.
const _: () = {
    assert!(
        matches!(TxStatus::try_from_byte(TxStatus::Idle.byte()), Ok(TxStatus::Idle)),
        "TxStatus round-trip broken: Idle",
    );
    assert!(
        matches!(TxStatus::try_from_byte(TxStatus::InTransaction.byte()), Ok(TxStatus::InTransaction)),
        "TxStatus round-trip broken: InTransaction",
    );
    assert!(
        matches!(TxStatus::try_from_byte(TxStatus::Failed.byte()), Ok(TxStatus::Failed)),
        "TxStatus round-trip broken: Failed",
    );
};

/// Typed non-empty range into a write buffer, replacing the raw
/// `(start, end): (usize, usize)` pair on
/// [`StagedAction::SendBytesRange`].
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
/// # Tier
///
/// A naive `SendBytesRange { start, end }` shape carrying two raw
/// `usize`s with no proof of `start ≤ end` or `end ≤ write_buf.len()`
/// would leave `materialise` to fall back silently to `&[]` on any
/// violation — a tier-3 review-discipline seam. The current shape
/// is tighter:
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
/// # Size narrowing
///
/// Storage is `u16 + NonZeroU16` (4 B). A naive `usize + NonZeroUsize`
/// shape (16 B on 64-bit) would burn 12 B per range; on a 1000-row
/// SELECT, 12 KB of stack traffic. The narrow form is valid because
/// all range endpoints originate in buffers bounded by
/// `READ_BUF_CAP = 4096` or `MAX_OWNED_SEND_LEN = 2176`, both ≤
/// `u16::MAX = 65535` (const-asserted at `crate::buf::READ_BUF_CAP`).
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
    /// Signature is `(usize, usize, usize)` for call-site compat
    /// (callers carry `usize` offsets from indexing). The
    /// `u16::try_from` narrowing fallbacks are architecturally dead
    /// for bounded buffer offsets (≤ READ_BUF_CAP ≤ u16::MAX) but
    /// the explicit try-from satisfies the forbid bundle's ban on
    /// `as` conversions.
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

    /// Test-only constant constructor for a unit-length range
    /// (`start=0`, `len=1`). Returns `Self` directly (not `Option<Self>`)
    /// — the values are infallibly valid by construction, so the
    /// `unwrap_or(_)` route that the prior `DEAD_FALLBACK` constant
    /// induced is gone. Use for fixtures that need a concrete
    /// `NonEmptyRange` without the Option-discrimination ceremony of
    /// `new()`.
    ///
    /// `NonZeroU16::MIN` is the canonical "value 1" anchor (no `as`
    /// cast, no `unwrap()`, no panic-able construction).
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn test_unit() -> Self {
        Self {
            start: 0,
            len: NonZeroU16::MIN,
        }
    }

    /// Resolve the range against a buffer, returning the slice or
    /// `None` on bounds mismatch. A naive
    /// `debug_assert!(slice.is_some(), ...)` would form the "debug
    /// loud + release silent" pattern this crate bans; instead the
    /// None is propagated through `WriteRange::apply` and classified
    /// at materialise as a `CloseSocket` emission.
    #[inline]
    pub(crate) fn apply<'a>(&self, buf: &'a [u8]) -> Option<&'a [u8]> {
        // Widen u16 → usize via infallible usize::from before slice
        // indexing.
        let start = usize::from(self.start);
        let end = start.checked_add(usize::from(self.len.get()))?;
        buf.get(start..end)
    }
}

// Drift pin: NonEmptyRange packs u16 + NonZeroU16 = 4 B. A naive
// `usize + NonZeroUsize` form (16 B on 64-bit) would burn 12 KB of
// per-row stack traffic on a 1000-row SELECT.
const _: () = assert!(
    core::mem::size_of::<NonEmptyRange>() == 4,
    "NonEmptyRange size regression — must stay u16 + NonZeroU16 = 4 B. \
     Buffer offsets ≤ READ_BUF_CAP ≤ u16::MAX are const-asserted at \
     crate::buf::READ_BUF_CAP.",
);

// ═════════════════════════════════════════════════════════════════════
// Range newtype wrappers
// ═════════════════════════════════════════════════════════════════════
//
// `WriteRange` wraps [`NonEmptyRange`] as a typed wrapper over a
// caller-owned write buffer. A naive generative-brand shape (HRTB
// closure threading a `'brand` lifetime to prove buffer identity)
// was considered and rejected: the brand's deliverable was infallible
// `apply`, but `apply` still has to return `Option<&[u8]>` for the
// runtime mismatch arm (the brand cannot prove `start + len ≤
// buf.len()` post-clear). With the brand's tier-1 deliverable gone,
// the bare wrapper has the same tier-2-structural guarantee at lower
// API surface.

/// Range into an outbound [`crate::write_buf::WriteBuf`].
///
/// Tier-2 structural — construction validates
/// `start + len <= buf.len()`; apply `None` is classified as
/// `CloseSocket` emission at materialise (no silent `&[]`).
/// API narrowing on `WriteReserved` prevents mid-scope truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteRange {
    /// Underlying non-empty range — carries the validated
    /// `start`/`len` pair. Validation at construction proves
    /// `start + len <= buf.len()` at emission time.
    inner: NonEmptyRange,
}

impl WriteRange {
    /// Construct from a raw [`NonEmptyRange`] — crate-internal
    /// factory used by `from_write_span` and test fixtures.
    #[inline]
    #[must_use]
    pub(crate) const fn from_raw(inner: NonEmptyRange) -> Self {
        Self { inner }
    }

    /// Build a write range from the current span of a
    /// `WriteReserved`. `start` is captured before builder writes;
    /// `reserved.len()` after gives the post-state end.
    ///
    /// # Err classification
    ///
    /// Returns `Err(InternalCrateBug { locus: EmptyWriteRange })`
    /// if `reserved.len() <= start` (builder emitted zero bytes
    /// since `start`). Architecturally dead under intact builders
    /// (every PG wire frame ≥ 5 bytes); classified via the crate-
    /// bug locus rather than silently fabricating a fallback range.
    #[inline]
    pub(crate) fn from_write_span(
        start: usize,
        reserved: &crate::write_buf::BrandedWriteReserved<'_>,
    ) -> Result<Self, crate::error::ProtocolError> {
        match NonEmptyRange::new(start, reserved.len(), reserved.len()) {
            Some(raw) => Ok(Self::from_raw(raw)),
            None => Err(crate::error::ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::EmptyWriteRange,
            }),
        }
    }

    /// Apply the range to write-buffer bytes — returns `None` on
    /// bounds mismatch. Materialise classifies `None` via
    /// `CloseSocket` emission (not silent). A naive
    /// `debug_assert + unwrap_or(&[])` shape would form the banned
    /// "debug loud + release silent" pattern.
    ///
    /// `None` arm is architecturally dead under API-narrow
    /// `WriteReserved` (no truncating ops between construction +
    /// apply); materialise's `CloseSocket` emission is the
    /// tier-2 structural classifier.
    #[inline]
    #[must_use]
    pub(crate) fn apply<'a>(&self, bytes: &'a [u8]) -> Option<&'a [u8]> {
        self.inner.apply(bytes)
    }

    /// Access the underlying [`NonEmptyRange`] — for debug output,
    /// drift tests, and `StagedObs::from_staged` unbrand observation.
    /// Production code works through the branded type.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) const fn inner(&self) -> NonEmptyRange {
        self.inner
    }
}

// `StagedAction::StreamRowRange` carries `row_bytes: &'r [u8]`
// directly — slice is borrowed at dispatch time from
// `read_buf.populated()`, stored in staged, and passed through
// materialise unchanged. A naive `ReadRange<'brand>` shape (start +
// len + brand phantom) would require a runtime-checked apply and
// gain nothing: Rust's borrow checker already tracks slice lifetime,
// so `&'r [u8]` gives tier-1 apply "for free" (no Option, no
// unwrap_or, no debug_assert).

// Drift pin: WriteRange must stay the same size as the underlying
// `NonEmptyRange`.
const _: () = assert!(
    core::mem::size_of::<WriteRange>() == core::mem::size_of::<NonEmptyRange>(),
    "WriteRange size regression — must equal NonEmptyRange (4 B).",
);

#[cfg(test)]
mod range_newtype_tests {
    //! Range newtype shape + infallible-apply tests.
    //!
    //!   - Types are constructible via `from_raw`.
    //!   - `inner()` accessor round-trips.
    //!   - Size is 4 B (no phantom; bare wrapper).
    //!   - `apply()` round-trips bytes via a fresh `WriteBuf`.
    //!
    //! End-to-end builder → apply round-tripping through the
    //! branded reserved type lives in
    //! `crate::write_buf::branded_reserved_tests`.
    use super::*;
    use crate::write_buf::WriteBuf;

    /// Happy-path apply — build a range whose `(start=0, len=1)`
    /// fits the 1-byte populated buffer, apply it, observe the
    /// expected single-byte slice with no call-site `Option`
    /// unwrap.
    #[test]
    fn write_range_apply_returns_infallible_slice() {
        let mut buf = WriteBuf::new();
        let push_ok = buf.push_u8(0x42);
        assert!(push_ok.is_ok(), "push_u8 must succeed on fresh buffer");
        let bytes = buf.as_bytes();
        let raw = NonEmptyRange::test_unit();
        let range = WriteRange::from_raw(raw);
        let slice: &[u8] = range.apply(bytes).unwrap_or(&[]);
        let byte = slice.first().copied().unwrap_or(0);
        assert_eq!(byte, 0x42, "WriteRange apply round-trips the pushed byte");
    }

    /// Drift pin — WriteRange is 4 bytes (identical layout to
    /// `NonEmptyRange`; no phantom).
    #[test]
    fn write_range_sizes_match_raw() {
        assert_eq!(core::mem::size_of::<WriteRange>(), 4);
        assert_eq!(
            core::mem::size_of::<Option<WriteRange>>(),
            4,
            "Option<WriteRange> must niche-pack on NonZeroU16 inside NonEmptyRange.len",
        );
    }

    /// `inner()` accessor round-trip.
    #[test]
    fn write_range_inner_roundtrip() {
        let raw = NonEmptyRange::test_unit();
        let w = WriteRange::from_raw(raw);
        assert_eq!(w.inner(), raw);
    }

    /// Exercise the classified Err path of
    /// `WriteRange::from_write_span`.
    ///
    /// `NonEmptyRange::new(start, end, bounds)` returns `None` iff
    /// `end <= start` OR `end > bounds`. Post-builder,
    /// `end = reserved.len()` = `bounds`. So the only way to force
    /// `None` is `start >= reserved.len()` — simulating a builder
    /// that captured `start` post-push, skipped pushes, or
    /// overflowed the usize into the end field (all genuine
    /// builder-drift scenarios).
    ///
    /// The test forces `start > reserved.len()` by calling
    /// `from_write_span(10, ...)` on a fresh (empty) reserved. Err
    /// path fires with `CrateBugLocus::EmptyWriteRange` — a naive
    /// shape would silently return a unit-length `WriteRange`,
    /// emitting a tier-4 0-byte `Action::SendBytes` on apply.
    #[test]
    fn from_write_span_err_classified_as_empty_write_range() {
        let mut buf = crate::write_buf::WriteBuf::new();
        let is_empty_write_range = buf.with_branded(|mut wb| {
            let reserved = wb.reserve();
            // reserved.len() == 0 (fresh). start=10 > 0 forces
            // NonEmptyRange::new None → EmptyWriteRange.
            let result = WriteRange::from_write_span(10, &reserved);
            matches!(
                result,
                Err(crate::error::ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::EmptyWriteRange,
                })
            )
        });
        assert!(
            is_empty_write_range,
            "from_write_span must return Err(EmptyWriteRange) when \
             start > reserved.len() — a naive shape would silently fall \
             back to a unit-length range (tier-4 0-byte Action::SendBytes).",
        );
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
/// `MAX_ACTIONS_PER_CALL` is intentionally tiny — see its
/// definition in `protocol.rs` for the per-method budget. Overflow
/// handling is compile-enforced via the `emit_actions!` macro's
/// `const _: () = assert!(MAX_ACTIONS_PER_CALL >= budget)` checks
/// at every push site.
///
/// # `ManuallyDrop<heapless::Vec>` backing
///
/// `OutActions` is `ManuallyDrop<heapless::Vec<Action, N>>` — zero
/// init writes via `heapless::Vec::new()`; the wrapper suppresses
/// the Drop impl that would otherwise extend NLL borrows past
/// last-use. A naive `[Action; MAX_ACTIONS_PER_CALL]` + `u8 len`
/// shape with `Action::CloseSocket` sentinel-fill would pay
/// **5008 B zero-fill/call** (16 × 312 B + pad).
///
/// ## Why ManuallyDrop?
///
/// Plain `heapless::Vec<Action, N>` has `impl<T, const N: usize>
/// Drop for Vec<T, N>` — `needs_drop::<heapless::Vec<_, _>>()`
/// returns `true` even when `T: Copy`, extending borrow-check
/// lifetime of a value holding `'r` (the read-buf borrow) to its
/// scope's Drop point instead of last-use. Caller pattern
/// `let out = proto.feed_bytes(...); match out.as_slice() {...};
/// proto.state()` would then fail borrow-check (out's 'r-borrow
/// of proto still live at the `proto.state()` call).
///
/// `ManuallyDrop<T>` inhibits the inner Drop unconditionally.
/// Since `Action<'w>` is `Copy` (POD refs + small payloads),
/// the inner Drop body is trivial anyway — skipping it is sound.
///
/// ## Win
///
/// 5008 B zero-fill per `feed_bytes` / `push_command` / `iter_rows
/// .slow_path_once` → **0 B init**. Stack reservation size
/// unchanged (`[MaybeUninit<Action>; N]` still allocates the
/// slots), but the write bandwidth disappears.
///
/// # 2-lifetime form retained — documentation-bearing
///
/// A naive collapse to a single lifetime would assume "both
/// lifetimes originate from `&'_ mut PgProtocol` reborrows", but
/// the two lifetimes track **structurally distinct borrows** with
/// different origins:
///
/// **`'w` (write-buffer)** binds [`Action::SendBytes(&'w [u8])`] back
/// to the **caller-owned** [`crate::write_buf::WriteBuf`] passed as a
/// separate `&'w mut WriteBuf` parameter to every entry-point. It
/// is **not** a reborrow of `self`.
///
/// **`'r` (read-state)** binds the row-arena slices inside
/// the schema accessor `PgProtocol::current_row_desc()` + sibling
/// `Describe*Payload<'r>` fields back to `PgProtocol`'s internal
/// `row_desc_slot` / `error_arena` — a shared `&'r PgProtocol`
/// reborrow (NOT `&mut`; module-doc explains the read-only nature).
///
/// Unification to a single lifetime `'a = min('w, 'r)` would compile
/// in practice (covariance handles the coercion at every current
/// call site, all of which already see `'w` and `'r` originate from
/// the same scope). The retained 2-lifetime form delivers:
///
/// - **Type-level documentation**: the two parameters make the two
///   distinct borrow sources visible in every signature, instead of
///   collapsing them into one opaque lifetime that requires reading
///   doc-prose to disambiguate.
/// - **Push-path expressivity**: `OutActions<'w>` returned
///   from [`crate::PgProtocol::push_command_internal`] (16 sites)
///   states *at the type level* that the outbound batch carries no
///   arena-borrowed Reply variants — only the WriteBuf borrow
///   constrains the caller's hold-time. Unification erases this
///   distinction.
///
/// Net: the 2-lifetime form is documentation-bearing rather than
/// strictly load-bearing for borrow-check soundness. Explicit
/// signatures are preferred over signatures that depend on
/// prose-doc explanation.
#[derive(Debug)]
pub struct OutActions<'w> {
    /// ManuallyDrop-wrapped heapless vec. `ManuallyDrop` makes the
    /// wrapper Drop-free regardless of inner type, preserving
    /// pre-(184) NLL last-use borrow-release semantics.
    items: core::mem::ManuallyDrop<
        heapless::Vec<Action<'w>, MAX_ACTIONS_PER_CALL>,
    >,
}

impl Default for OutActions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'w> OutActions<'w> {
    /// Construct an empty `OutActions`.
    ///
    /// Backed by `ManuallyDrop::new(heapless::Vec::new())` — zero
    /// writes at construction. A naive `[Action::CloseSocket; N]`
    /// eager fill would write 5008 B every call on a fresh
    /// `OutActions`.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            items: core::mem::ManuallyDrop::new(heapless::Vec::new()),
        }
    }

    /// Number of populated actions.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether no actions have been pushed.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Borrow the populated prefix as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[Action<'w>] {
        self.items.as_slice()
    }

    /// Return the first populated action (or `None` if empty).
    /// Convenience for test assertions.
    #[inline]
    pub fn first(&self) -> Option<&Action<'w>> {
        self.items.first()
    }

    /// Push an action. Returns `Err(action)` (mirrors heapless's
    /// convention) if the container is full.
    #[inline]
    pub fn push(&mut self, action: Action<'w>) -> Result<(), Action<'w>> {
        self.items.push(action)
    }
}

impl<'w> IntoIterator for OutActions<'w> {
    type Item = Action<'w>;
    type IntoIter = <heapless::Vec<Action<'w>, MAX_ACTIONS_PER_CALL> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        // Unwrap ManuallyDrop to move inner vec; since `Action`
        // is Copy the inner vec's Drop is a no-op anyway. This is
        // sound per `ManuallyDrop::into_inner` safety contract
        // (forgotten drop would be unsound only for drop-active T).
        core::mem::ManuallyDrop::into_inner(self.items).into_iter()
    }
}

/// By-reference iteration — `for action in &out` yields `&Action`.
impl<'a, 'w> IntoIterator for &'a OutActions<'w> {
    type Item = &'a Action<'w>;
    type IntoIter = core::slice::Iter<'a, Action<'w>>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
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
///
/// A proposed `InlineArr<T, N, L>` safe replacement of
/// `heapless::Vec` was rejected: per-call types would ship ~700 B
/// memset on every entry-point call (+30–50% on
/// push_command/ping_amortised, violating the Q2 bench gate). The
/// Pareto-optimal analysis above is the load-bearing reason; any
/// future re-open requires new measurement evidence.
// Staged container uses `MAX_STAGED_PER_CALL` (dispatch-side cap);
// output uses `MAX_ACTIONS_PER_CALL` (fan-out).
//
// `'sql` lifetime parameter carries the borrow of any
// `StagedAction::SendBytesBorrowed(&'sql [u8])` variant — caller's
// SQL bytes referenced zero-copy via Parse / SimpleQuery push paths.
// Elision rules let most consumers write `&mut StagedActions<'_>`;
// only PushCommand impls that stage borrowed bytes (Parse,
// SimpleQuery) need to propagate the lifetime explicitly to ensure
// the borrow outlives the materialisation step.
pub(crate) type StagedActions<'sql> = heapless::Vec<StagedAction<'sql>, { crate::protocol::MAX_STAGED_PER_CALL }>;

/// A directive from the protocol to its host.
///
/// # Lifetime
///
/// `'buf` is the lifetime of the host's caller-owned [`crate::write_buf::WriteBuf`].
/// [`Action::SendBytes`] carries `&'buf [u8]` — either a reference
/// into that `WriteBuf` (for runtime-built frames) or a static
/// reference (for compile-time constants; `'static: 'buf`).
///
/// # Two lifetimes
///
/// `'w` names bytes living in the caller's `WriteBuf` (outbound —
/// `SendBytes`). `'r` names bytes living in the protocol's
/// `ReadBuf` (inbound — `StreamRow`). Two distinct lifetimes
/// because the two buffers are distinct sources and the borrow
/// checker needs the information to enforce:
///
/// - Next `&mut WriteBuf` call blocked while `SendBytes(&'w …)`
///   alive (caller-owned write-buffer invariant).
/// - Next `&mut PgProtocol` call blocked while `StreamRow(&'r …)`
///   alive — the row slice is inside `self.read_buf`, so
///   `feed_bytes` takes `&'r mut self` and the output's `'r`
///   borrows back from `self`.
///
/// `#[non_exhaustive]` reserves variant-addition headroom. Internal
/// `match` over `Action` is *not* `non_exhaustive`.
///
/// # Why two lifetimes (`'w` + `'r`)?
///
/// The two lifetimes are NOT cosmetic — they are load-bearing:
/// - `'w` borrows `write_buf` on the **push path**. Entry-points
///   `push_command` / `push_bind_execute` build outbound frames
///   into `WriteBuf`; `SendBytes(&'w [u8])` references the staged
///   bytes. The host writes them to the socket and drops the
///   Action, releasing `'w`.
/// - `'r` borrows `read_buf` + `terminal_row_desc` on the **feed
///   path**. `feed_bytes` parses inbound frames into `read_buf`;
///   row-streaming actions like
///   `StreamRow { desc: &'r RowDesc, row_bytes: &'r [u8] }` borrow
///   directly from the populated region (zero-copy). Terminal
///   `Reply::QueryComplete` payloads borrow the parked schema from
///   `PgProtocol::terminal_row_desc`. Host reads + drops the
///   Action, releasing `'r`.
///
/// **Why can't we unify `'w = 'r`?** On the push path, produced
/// `Action`s are all either `'static` (compile-time constant
/// frames like `Sync`) or `'w` (freshly-built frames). On the
/// feed path, `Action`s are `'r` (row bodies borrowed from
/// `read_buf`). Forcing `'w = 'r` would require every push-path
/// action to satisfy `'r` — but `'r` is `&'r mut self` of
/// `PgProtocol` from `feed_bytes`'s signature. Push-path actions
/// exit without a `&mut self` borrow, so `'r` is unbound there,
/// and the compiler would infer `'r = 'static` on push, breaking
/// the feed-path zero-copy guarantee (StreamRow needs actual-`'r`,
/// not static). Two distinct lifetimes prove zero-copy on both
/// paths; unification would force either staging copies or an
/// API split.
///
/// # Variant ordering
///
/// Variants are in **production-frequency order**:
///
/// - `SendBytes` — emitted on every push, every Sync residue,
///   every mid-handshake response → **dominant** across every
///   workload.
/// - `DeliverReply` — emitted once per successful command cycle.
/// - `FailReply` — emitted once per error.
/// - `CloseSocket` — emitted once per fatal teardown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use = "an Action carries a side-effect that must be executed"]
pub enum Action<'w> {
    /// Send these bytes verbatim to the server.
    ///
    /// The slice references the caller-owned [`crate::write_buf::WriteBuf`]
    /// (for runtime-built frames) or static storage (for compile-time
    /// constants; `'static: 'w`). The host reads the slice, writes
    /// it to the socket, and drops the [`Action`]; no data is copied
    /// out of the protocol. Zero-copy.
    ///
    /// The `'w` lifetime ensures the slice is valid for exactly
    /// as long as the owning `OutActions<'w>` is alive — the
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
        /// : `Reply` is lifetime-free post-payload
        /// externalisation; the `'r` parameter on the enclosing
        /// `Action<'w>` is preserved for `current_*` accessor
        /// borrows via `OutActions`. The `value` itself carries no
        /// borrowed data.
        value: Reply,
    },

    /// Deliver a failure to the wrapper.
    ///
    /// Same routing as `DeliverReply`; the wrapper resolves the
    /// failure cause via
    /// [`crate::PgProtocol::fail_cause`] AFTER consuming this action.
    ///
    /// .b: cause externalised into
    /// [`crate::fail_cause_slot::FailCauseSlotCell`] living on
    /// `<ActivePhase>::Extras` (and `<ConnectingPhase>::Inner` for
    /// handshake-phase fails). The inline `cause: ProtocolError`
    /// (24 B) was the dominator on `FailReply`'s 32-B variant body;
    /// externalising collapses `FailReply` body to 8 B (id only) and
    /// drops `Action`'s outer size 40 → 24 B (-40%).
    ///
    /// # Caller contract
    ///
    /// Query `pg.fail_cause()` IMMEDIATELY on observing this action.
    /// A subsequent `install_errored` (e.g. `ConnectionAlreadyClosed`
    /// raised when the caller pushes on an already-Errored protocol)
    /// overwrites the slot via latest-wins semantics. Deferring the
    /// query past a subsequent push loses the original cause.
    FailReply {
        /// The correlator the user originally supplied with their
        /// command. Query the cause via
        /// [`crate::PgProtocol::fail_cause`].
        id: NonZeroU64,
    },

    /// The socket is no longer safe to use; close it.
    ///
    /// Emitted alongside a failed reply when the connection is
    /// out-of-sync with the server (malformed framing, unexpected
    /// frame, etc.). The wrapper must close the underlying transport;
    /// the pool then discards this connection.
    CloseSocket,

    /// Asynchronous server-pushed notification (PG §55.7 LISTEN/NOTIFY).
    ///
    /// Emitted in the OutActions stream alongside other side-effects
    /// when a `NotificationResponse` ('A') frame arrives. The wrapper
    /// resolves the payload via
    /// [`crate::PgProtocol::get_notification`] passing `notif_ref` —
    /// returns `Result<&NotificationPayload, ArenaError>`. Refs are
    /// gen-tagged and valid only within the current OutActions
    /// iteration cycle; resolving after the next `feed_bytes` call
    /// returns `Err(ArenaError::Stale)`.
    ///
    /// `pid` is carried by value (4 B, `Copy`) so callers can route
    /// notifications without resolving the arena. Channel name +
    /// payload bytes live in the arena (variable-length payload up
    /// to PG's `NOTIFY_PAYLOAD_MAX_LENGTH` = 8000 B).
    Notify {
        /// PID of the backend process that issued the `NOTIFY`.
        pid: i32,
        /// Gen-tagged handle into the notifications arena. Resolve
        /// via [`crate::PgProtocol::get_notification`].
        notif_ref: crate::notifications_arena::NotificationRef,
    },

    /// Multi-statement batch intermediate command-complete signal
    /// (). PG SimpleQuery (Q frame) accepts `;`-separated
    /// batches like `"BEGIN; UPDATE; UPDATE; COMMIT;"` — the
    /// server emits one CommandComplete per statement followed by
    /// a single final RFQ. Pre-the second CommandComplete
    /// arriving in `SimpleQueryAwaitingRfq` triggered
    /// `UnexpectedFrame` teardown; post-each non-final
    /// CommandComplete / RowDescription / EmptyQueryResponse emits
    /// this variant carrying the PRIOR statement's tag, and the
    /// state cycles back into the next statement's response
    /// pattern (preserving the in-flight `ReplyId`).
    ///
    /// Caller observes one `IntermediateCommandComplete` per
    /// non-final statement + one final
    /// `DeliverReply { Reply::QueryComplete }` carrying the LAST
    /// statement's tag + transaction status.
    IntermediateCommandComplete {
        /// Gen-tagged handle into
        /// [`crate::command_tags_arena::CommandTagsArena`]. Resolve
        /// via [`crate::PgProtocol::get_command_tag`] to obtain
        /// `&CommandTag`. externalisation drops the
        /// inline 40-B tag payload to a 4-B arena handle; Action
        /// stays `Copy` and the variant now niche-packs into the
        /// outer enum's disc, collapsing Action 48 → 40 B and
        /// OutActions 440 → 368 B cascade.
        tag_ref: crate::command_tags_arena::CommandTagRef,
    },

    /// COPY OUT data chunk (Phase 3, PG §55.2.6). Emitted
    /// for each `CopyData` ('d') frame during a COPY OUT cycle.
    /// The wrapper resolves the payload via
    /// [`crate::PgProtocol::get_copy_chunk`] passing `chunk_ref` —
    /// returns `Result<&CopyChunkPayload, ArenaError>`. Refs are
    /// gen-tagged and valid only within the current OutActions
    /// iteration cycle (the per-feed_bytes arena clear invalidates
    /// outstanding refs).
    CopyDataChunk {
        /// Gen-tagged handle into the copy-chunks arena. Resolve
        /// via [`crate::PgProtocol::get_copy_chunk`].
        chunk_ref: crate::copy_chunks_arena::CopyChunkRef,
    },
}

/// One-event-per-call feed signal.
///
/// Per-call return type alternative to the batched
/// `OutActions<'w>` — used by
/// [`crate::PgProtocol::advance_one_frame`] to drive the protocol
/// in single-event steps. Forward-compat anchor for pipelining
/// work (where multiple in-flight replies may resolve in one call
/// cycle and the caller wants explicit control over event
/// consumption).
///
/// # Variants
///
/// - [`Self::Idle`] — state is `Idle` and read_buf is empty. No
///   work to do; caller can push a next command.
/// - [`Self::NeedMoreBytes`] — partial frame buffered (or empty
///   buffer in a non-Idle state). Caller must feed more bytes via
///   [`crate::PgProtocol::feed_inbound`] before the next call.
/// - [`Self::StreamingRows`] — state entered the row-streaming
///   territory (a `RowDescription` was parsed; the next inbound
///   frames are `DataRow`s). Caller should switch to
///   [`crate::PgProtocol::iter_rows`] for the per-row pull API.
///   On stream completion (`CommandComplete` + `ReadyForQuery`)
///   the protocol state returns to a non-streaming variant;
///   subsequent `advance_one_frame` calls resume the normal flow.
/// - [`Self::SendBytes`] — outbound bytes ready in the caller's
///   `WriteBuf`. The slice borrows from `wb` (lifetime `'wb`).
///   Caller drains the bytes to the socket BEFORE the next
///   `advance_one_frame` call, since the next call may overwrite
///   `wb`.
/// - [`Self::Deliver`] — terminal reply for an in-flight command.
///   Caller routes via `id` to the user's `oneshot::Sender` and
///   forwards `value`.
/// - [`Self::Fail`] — fatal failure for an in-flight command.
///   The variant **semantically implies socket close**: caller
///   MUST resolve the user's oneshot via `(id, cause)` AND close
///   the socket. Connection is in `Errored` state post-event.
/// - [`Self::Close`] — state→Errored without an in-flight reply
///   (e.g. adversarial frame in `Idle`, post-handshake fatal).
///   Caller MUST close the socket.
///
/// # Lifetime contract
///
/// Two lifetimes (mirror of [`OutActions<'w>`]):
///
/// - `'wb` for [`Self::SendBytes`] which borrows the caller's
///   [`crate::write_buf::WriteBuf`]. Collapsing to a single
///   lifetime would force `'wb = 'r` at use sites — breaks
///   composable patterns where push-side and feed-side lifetimes
///   diverge.
/// - `'r` for [`Self::Deliver`] which borrows from `PgProtocol`
///   internals (specifically `row_desc_slot` for
///   `Reply::QueryComplete` payloads). Tied to the `&'r mut self`
///   of `advance_one_frame`.
///
/// # Size pin
///
/// `size_of::<FeedEvent<'static>>() == 88` exact: max
/// variant is [`Self::Deliver`] = `NonZeroU64` (8 B) +
/// `Reply<'r>` (80 B) = 88 B; discriminant niche-optimised via
/// the payload's `NonZero` niche where possible. The exact `==`
/// pin lives in `lib.rs` per CREDO §III no-permissive-ranges
/// policy.
///
/// # `#[must_use]` discipline
///
/// Variants encode side-effect contracts (drain bytes, route
/// reply, close socket). The struct attribute pins the discipline
/// against `let _ = proto.advance_one_frame(...)` accidental
/// discards. The forbid bundle's
/// `clippy::let_underscore_must_use` lint makes ignoring without
/// explicit `match`/`?` a build failure.
///
/// # Variant ordering
///
/// Current declared order optimises for the polling pattern of an
/// async driver loop: `Idle` (caller polls when idle), then
/// `NeedMoreBytes` (caller awaits more network bytes), then
/// progressively-more-eventful variants. `advance_one_frame` is a
/// forward-compat surface — no production hot path exists today,
/// so reordering would be speculative without bench evidence.
/// When pipelining benches arrive, revisit: if `Deliver` (terminal
/// reply per response cycle) dominates, promote it to first.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
#[must_use = "FeedEvent variants carry side-effect contracts: \
              SendBytes/Deliver MUST be processed; Fail/Close MUST \
              trigger socket teardown"]
pub enum FeedEvent<'wb> {
    /// State is `Idle` and read_buf is empty — no work, caller can push.
    Idle,
    /// Partial frame buffered (or empty buffer non-Idle). Need more
    /// bytes from network.
    NeedMoreBytes,
    /// State entered row-streaming. Caller switches to
    /// [`crate::PgProtocol::iter_rows`] for per-row decoding.
    StreamingRows,
    /// Outbound bytes (e.g., SCRAM client-final). Drain to socket,
    /// then continue. The slice borrows `wb` for `'wb`.
    SendBytes(&'wb [u8]),
    /// Terminal reply for an in-flight command. Route via `id` to the
    /// user's `oneshot::Sender` and forward `value`.
    ///
    /// : `Reply` is lifetime-free post-payload
    /// externalisation. The `'r` parameter on the enclosing
    /// `FeedEvent<'wb>` is preserved structurally for backward
    /// compatibility with callers that bind `'r`; the `value` itself
    /// carries no borrowed data.
    Deliver(NonZeroU64, Reply),
    /// Fatal failure for an in-flight command. **Implies socket close**
    /// (M2). Caller resolves user's oneshot via `id` (cause queried
    /// separately via [`crate::PgProtocol::fail_cause`]) AND closes
    /// the socket. State is `Errored` post-event.
    ///
    /// .b: cause externalised into the per-phase
    /// `fail_cause_slot`. Caller contract: query
    /// `pg.fail_cause()` IMMEDIATELY on observing this event — the
    /// slot holds latest-wins semantics, so a subsequent
    /// `install_errored` (e.g. `ConnectionAlreadyClosed` raised when
    /// the caller pushes on an already-Errored protocol) overwrites.
    Fail(NonZeroU64),
    /// State→Errored without in-flight reply. Caller closes the socket.
    Close,
}

/// Classified push-side failure — the bytes-only push API
/// (`ReadyGuard::push_<cmd>`) returns `Result<(), PushFailure>`.
///
/// # Shape
///
/// Push paths write bytes directly into the caller's `WriteBuf`
/// (caller drains via `wb.as_bytes()`); failure signals come back
/// via `Result::Err(PushFailure)` — an ~80 B per-call return frame.
///
/// A naive shape returning the full `OutActions<'w>`
/// (`ManuallyDrop<heapless::Vec<Action, 9>>`, ~800 B per call)
/// would force callers to iterate the action list to drive the I/O
/// layer:
///
/// ```text
/// let actions = ready.push_command(cmd, &mut wb);
/// for a in actions.iter() {
///     match a {
///         Action::SendBytes(b) => socket.write(b),
///         Action::FailReply { id, cause } => deliver_err(id, cause),
///         Action::CloseSocket => socket.close(),
///         Action::DeliverReply { .. } => unreachable!()  // never on push paths
///     }
/// }
/// ```
///
/// The classified-result form is ~10× smaller and lets the success
/// path elide the iteration entirely.
///
/// # Caller contract on `Err(PushFailure)`
///
/// 1. **`wb`'s content is undefined.** Partial frame bytes may be
///    present from a builder that started writing before failing.
///    Caller MUST `wb.clear()` before the next push (note: the next
///    push's `push_*_internal` body does this automatically at
///    `protocol.rs:1005, 1102` — so the actual risk window is only
///    between this `Err` and any user-side `wb.as_bytes()` access).
///
/// 2. **State has already transitioned to `Errored`** (via
///    `install_errored` from inside the push internal). `as_ready()`
///    will return `None` on subsequent calls.
///
/// 3. **Caller MUST resolve the user's oneshot using `id` + `cause`.**
///    The `id` is the consumed correlator (post-`ReplyId::consume`);
///    the `cause` carries the typed failure classification. Drop
///    without resolving = silent leak of the user-visible reply.
///
/// 4. **Caller MUST close the socket.** Connection is in `Errored`;
///    the socket is no longer usable. Discard the connection from any
///    pool with the disposal flag set.
///
/// # `#[must_use]` discipline
///
/// The struct attribute below pins points 1-4 against accidental
/// `let _ = ready.push_command(...);` discards. The forbid bundle's
/// `clippy::let_underscore_must_use` and `let_underscore_drop`
/// lints make ignoring `Result` without explicit `match`/`?` a
/// build failure.
///
/// # Field privacy
///
/// `pub` fields: `id` is a non-secret correlator (ReplyId
/// discipline guarantees uniqueness, not secrecy); `cause` is
/// already a public `ProtocolError`. No encapsulation budget
/// gained by accessor methods.
///
/// # Size pin
///
/// `size_of::<PushFailure>() == 80` exact: `NonZeroU64` (8 B) +
/// `ProtocolError` (72 B). Const-assert pin lives in `lib.rs` per
/// CREDO §III no-permissive-ranges policy.
///
/// # Tier classification
///
/// Tier-1 by `Result::Err` arm exhaustive match (caller cannot ignore
/// without `#[expect]`-or-`?`-or-explicit-discard, all of which signal
/// intent). The `#[non_exhaustive]` marker reserves room for future
/// fields (e.g., a `wb_drained_safe: bool` if the contract narrows).
#[non_exhaustive]
#[must_use = "PushFailure carries the consumed ReplyId and the failure cause; \
              the caller MUST resolve the user's oneshot before discarding, \
              MUST close the socket (connection is Errored), and MUST clear \
              the WriteBuf if reusing it (its content is undefined post-Err)"]
#[derive(Debug, Clone)]
pub struct PushFailure {
    /// Consumed correlator (post-`ReplyId::consume`) — used by the
    /// caller's I/O layer to look up the user's oneshot sender and
    /// deliver the error.
    pub id: NonZeroU64,
    /// Typed failure classification. Examples:
    /// - `ProtocolError::ConnectionAlreadyClosed { prior_kind }` —
    ///   push attempted on already-closed connection.
    /// - `ProtocolError::InternalCrateBug { locus }` — builder
    ///   capacity overflow or empty write range (architecturally-dead
    ///   per const-asserts in `write_buf.rs`).
    ///
    /// (PushFailure-only Box hybrid, 2026-05-23):
    /// heap-boxed. Pre-shape was inline `ProtocolError` (72 B),
    /// making PushFailure 80 B. Boxing shrinks PushFailure 80 → 16 B
    /// (-80%) — every push-call failure return frame is 64 B
    /// smaller. PushFailure was already `Clone`-only (not `Copy`);
    /// Box is `Clone` so no API break beyond the field type.
    /// Consumers use `failure.cause.kind()` / `&*failure.cause` —
    /// both work through Box's auto-deref.
    ///
    /// **Hybrid rationale**: blanket-Boxing (Box on `Action` /
    /// `FeedEvent` / `StagedAction` / `DispatchOutcome`) bench-stable
    /// FAIL'd at +83–93% on `push_command/ping`. Root cause: Box
    /// makes the enum non-trivially-movable → heapless::Vec Drop
    /// chain propagated to success paths.
    ///
    /// PushFailure has **no `heapless::Vec` involvement** (return-
    /// by-value, not vec-stored). Box cascade isolated to the
    /// failure return frame. Safe footprint win.
    ///
    /// Arena pattern (DEFERRED): switch all FailReply cause fields
    /// to `ProtocolErrorRef<'r>` Copy borrows for the
    /// Action/OutActions cascade.
    ///
    /// One alloc per emitted PushFailure (cold path).
    pub cause: alloc::boxed::Box<ProtocolError>,
}

/// Failure classification for the COPY IN push methods
/// (`push_copy_data` / `push_copy_done` / `push_copy_fail`).
///
/// Unlike the broader [`PushFailure`] used by query-flow push commands,
/// COPY IN pushes have no in-flight `ReplyId` to drain (the reply_id
/// is owned by the SimpleQuery that initiated the COPY IN cycle and
/// is preserved in the `CopyInActive` state across all client push
/// frames). So the failure surface is a simple sum type without an
/// `id` correlator.
#[derive(Debug, Clone, Copy)]
pub enum CopyPushError {
    /// Current proto state is not `SimpleQueryCopyInActive`. The
    /// push was rejected without writing to the WriteBuf.
    ///
    /// Common causes: caller invoked `push_copy_data` before the
    /// server's `CopyInResponse` arrived, or after the server's
    /// `CommandComplete` already advanced state to `AwaitingRfq`.
    NotInCopyInState,
    /// The framed body would exceed PG's wire length limit
    /// (`i32::MAX = 2_147_483_647` bytes including the 4-byte
    /// self-inclusive length field). Caller must chunk the payload
    /// into multiple `push_copy_data` calls.
    FrameTooLarge,
    /// The `error` string passed to `push_copy_fail` contains an
    /// embedded NUL byte, which would corrupt the CSTR framing.
    /// Strip / replace NULs caller-side before retrying.
    EmbeddedNul,
    /// WriteBuf capacity exhausted — frame doesn't fit.
    WriteBufFull(crate::write_buf::WriteBufFull),
}

impl core::fmt::Display for CopyPushError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotInCopyInState => {
                f.write_str("push_copy_* called outside CopyInActive state")
            }
            Self::FrameTooLarge => {
                f.write_str("COPY IN payload exceeds PG i32 wire length limit")
            }
            Self::EmbeddedNul => {
                f.write_str("CopyFail error message contains embedded NUL byte")
            }
            Self::WriteBufFull(inner) => write!(f, "WriteBuf full: {inner}"),
        }
    }
}

impl core::error::Error for CopyPushError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::WriteBufFull(inner) => Some(inner),
            Self::NotInCopyInState | Self::FrameTooLarge | Self::EmbeddedNul => None,
        }
    }
}

// Additive Display + `core::error::Error` impls on `PushFailure`.
// Downstream `?`-propagation into `Box<dyn Error>` would otherwise
// require a manual `From<PushFailure>` bridge in every consumer.
// Display delegates to the underlying typed classification +
// correlator id; the Error impl exposes `cause` via `source()` so
// downstream chain-walking utilities (anyhow's `Display` chain,
// `Error::sources()` iterator) reach the typed `ProtocolError`.
impl core::fmt::Display for PushFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "push command (id {}) failed: {}", self.id, self.cause)
    }
}

impl core::error::Error for PushFailure {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&self.cause)
    }
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
//
// `'sql` lifetime parameter exists for the
// [`StagedAction::SendBytesBorrowed`] variant. Other variants do
// not reference `'sql` directly; the parameter is "phantom" for
// them and erased at monomorphisation. Lifetime is named `'sql`
// (not the generic `'a`) to make the borrow's origin explicit —
// caller's SQL from Parse / SimpleQuery push paths.
#[derive(Debug)]
pub(crate) enum StagedAction<'sql> {
    /// Bytes live at the range `[start..start+len]` in the
    /// caller's `write_buf`. Non-zero length (encoded by
    /// `NonZeroU16` inside `WriteRange`). Apply is classified
    /// (`None` → `CloseSocket` at materialise) — no silent
    /// fallback.
    SendBytesRange(WriteRange),
    /// Bytes are a static compile-time constant. Materialiser passes
    /// through directly — no write, no copy.
    SendBytesStatic(&'static [u8]),
    /// Bytes are borrowed from a caller-owned source — currently the
    /// SQL string of [`crate::push_command::Parse`] /
    /// [`crate::push_command::SimpleQuery`]. The materialiser passes
    /// the slice through unchanged (zero-copy, zero-stage).
    ///
    /// SQL is borrowed end-to-end and surfaces as this variant — no
    /// protocol cap on SQL size, no truncation, tier-1 by-construction
    /// at the protocol layer. A naive shape that copied SQL into a
    /// fixed-size `Sql = FixedStr<MAX_SQL_LEN, _>` (truncating at
    /// 2048 B with "…" marker) would silently corrupt > 2048-byte
    /// queries.
    ///
    /// Caller responsibility: SQL must outlive the materialised
    /// `Action<'w>` (lifetime checked by the unified `'w` in
    /// `materialise` / `materialise_push`). For SQL containing
    /// secrets (e.g. passwords in `UPDATE` clauses), caller holds
    /// the SQL in `Zeroizing<String>` — zeroize-on-drop happens at
    /// the caller, not in `WriteBuf::clear()` (which only scrubs
    /// inline bytes).
    SendBytesBorrowed(&'sql [u8]),
    /// Map to [`Action::DeliverReply`]. Opaque [`DeliverReplyEntry`]
    /// — the only construction path is [`deliver`] (below), which
    /// enforces kind-payload pairing at compile time via
    /// [`crate::reply_id::ReplyKind::Payload`].
    DeliverReply(DeliverReplyEntry),
    /// Map to [`Action::FailReply`].
    ///
    /// .b: staged retains `cause` inline; materialise
    /// PARKS the cause into the fail_cause_slot and emits the
    /// tag+id-only public `Action::FailReply`. Internal-only carrier
    /// — keeping cause inline here avoids threading `fail_cause_slot`
    /// through every `compute_push_*` signature (~16 sites).
    FailReply {
        /// Raw correlator (post-consume of the `ReplyId`).
        id: NonZeroU64,
        /// Why the protocol failed. Parked into
        /// `<ActivePhase>::Extras.fail_cause` /
        /// `ConnectingInner.fail_cause` at materialise time; the
        /// public [`Action::FailReply`] strips this field.
        cause: ProtocolError,
    },
    /// Map to [`Action::CloseSocket`].
    CloseSocket,
    /// Map to [`Action::Notify`]. Carries pid + arena ref by value
    /// (both `Copy`); materialise passes through unchanged.
    ///
    /// Staged by the dispatch pre-filter on `'A'` tag (see
    /// [`crate::protocol::_notification_response_admit_leaf`]).
    Notify {
        /// PID of the backend that issued the NOTIFY.
        pid: i32,
        /// Gen-tagged arena handle.
        notif_ref: crate::notifications_arena::NotificationRef,
    },
    /// Map to [`Action::IntermediateCommandComplete`] —     /// multi-statement SimpleQuery support. Emitted by the
    /// `SimpleQueryAwaitingRfq` dispatch arms when a SECOND+
    /// CommandComplete / RowDescription / EmptyQueryResponse arrives
    /// before RFQ (i.e., the original Q frame batched multiple
    /// statements). Carries the PRIOR statement's command_tag by
    /// value; the state retains the NEW tag for subsequent
    /// transitions.
    IntermediateCommandComplete {
        /// Gen-tagged arena handle (externalisation).
        /// Materialise passes through unchanged; the public
        /// [`Action::IntermediateCommandComplete`] carries the same
        /// `tag_ref` for the wrapper to resolve via
        /// [`crate::PgProtocol::get_command_tag`].
        tag_ref: crate::command_tags_arena::CommandTagRef,
    },

    /// Map to [`Action::CopyDataChunk`] — COPY OUT
    /// data surface. Staged by `(SimpleQueryCopyOutStreaming,
    /// TAG_COPY_DATA)` dispatch arm after the chunk bytes are
    /// allocated in the copy_chunks_arena.
    CopyDataChunk {
        /// Gen-tagged arena handle.
        chunk_ref: crate::copy_chunks_arena::CopyChunkRef,
    },
}

/// Internal lifetime-free counterpart to the public [`Reply<'r>`].
///
/// # Why a lifetime-free intermediate
///
/// Dispatch runs BEFORE materialise. At dispatch time, the state
/// machine carries `RowDesc` inline in its variants. The dispatch
/// Z arm parks the schema into `PgProtocol::terminal_row_desc`
/// right before transitioning to `Idle`; materialise borrows from
/// that slot to produce the lifetime-bound public `Reply<'r>`.
///
/// `StagedReply` is the lifetime-free intermediate carried inside
/// `StagedAction::DeliverReply(DeliverReplyEntry)`. A naive shape
/// would have schema-bearing variants carry a `schema_present:
/// bool` flag duplicating `PgProtocol::row_desc_slot.is_some()`,
/// silently corrupting if a future dispatch refactor set the flag
/// without populating the slot. Instead materialise reads the slot
/// directly via `into_public(row_desc_slot)` — single source of
/// truth.
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

/// Lifetime-free staged counterpart to [`QueryCompletePayload`].
///
/// # Single source of truth for schema presence
///
/// Materialise reads `row_desc_slot.map(...)` directly. A naive
/// shape would carry `schema_present: bool` set by the dispatch Z
/// arm — **tier-2 by-discipline**, silently corrupting if a future
/// dispatch refactor set the flag without populating the slot. The
/// current shape is **tier-1 by-construction**: the slot's own
/// `is_some()` is the single source of truth.
///
/// `#[doc(hidden)] pub` — see [`StagedReply`] for visibility rationale.
///
/// # Field visibility
///
/// Fields are `pub(crate)`: the type stays `pub` (forced by the
/// `ReplyKind::StagedPayload` trait bound), but only in-crate code
/// can construct via struct literal. A naive `pub` fields shape
/// would enable external enum-variant construction, bypassing the
/// `From` impls' crate-internal construction path —
/// the "hidden-but-reachable" bypass class.
///
/// # Variants discriminate the post-Sync terminal shape
///
/// PG's `BindExecute`-with-`FetchRows::Chunked(N)` flow can resolve
/// via TWO terminal frames per PG §55.2.7:
///
/// - **`CommandComplete` + `ReadyForQuery`** — portal exhausted
///   within the row cap (or `FetchRows::All`); materialise emits
///   [`Reply::QueryComplete`].
/// - **`PortalSuspended` + `ReadyForQuery`** — row cap hit before
///   portal exhaustion; the portal stays bound and can be resumed
///   via [`crate::push_command::ExecutePortal`]. Materialise emits
///   [`Reply::QuerySuspended`].
///
/// The dispatch arm that observes the terminal frame
/// (`CommandComplete` vs `PortalSuspended`) selects the staged case
/// at staging time; materialise reads the case to pick the public
/// [`Reply`] variant.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedQueryCompletePayload {
    /// Portal exhausted — terminal `CommandComplete + RFQ` observed.
    ///
    /// : `command_tag` field removed — slot pattern via
    /// [`crate::command_tag_slot::CommandTagSlotCell`]. :
    /// `tx_status` field removed — slot pattern via
    /// [`crate::tx_status_slot::TxStatusSlotCell`]. Both reads happen
    /// at materialise (slot `'r` borrow) and at
    /// [`crate::PgProtocol::terminal_tx_status`] respectively. The
    /// staged variant is now a unit tag.
    Completed,
    /// Row cap hit — terminal `PortalSuspended + RFQ` observed. No
    /// `command_tag` (server didn't send `CommandComplete`).     /// : `tx_status` field removed (mirror of `Completed`'s
    /// shape; reason colocated above).
    Suspended,
}

/// Lifetime-free staged counterpart to
/// [`DescribeStatementCompletePayload`].
///
/// # Single source of truth for schema presence
///
/// Materialise reads `row_desc_slot.map(...)` directly. A naive
/// shape would carry a `rows: DescribedRowsStagedSlim`
/// (`Rows | NoData`) discriminator, duplicating
/// `PgProtocol::row_desc_slot.is_some()` — the dispatch arm would
/// have to park the schema into the slot AND set `Rows` in the
/// same arm body (atomic-pair discipline), with a
/// `debug_assert!(false)` helper arm for the architecturally-
/// impossible `Rows`-without-slot mismatch (banned per CREDO §V
/// "defensive-for-impossible"). Tier-1 by-construction.
///
/// Fields are `pub(crate)`; see [`StagedQueryCompletePayload`] for
/// the bypass-closure rationale.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StagedDescribeStatementCompletePayload;

/// Lifetime-free staged counterpart to
/// [`DescribePortalCompletePayload`].
///
/// See [`StagedDescribeStatementCompletePayload`] for the
/// single-source-of-truth rationale on schema presence. :
/// formerly carried `tx_status` inline; now externalised into
/// `<ActivePhase>::Extras.tx_status` slot.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StagedDescribePortalCompletePayload;

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
    /// borrowing into the protocol's parked terminal RowDesc slot.
    ///
    /// State variants carry `RowDesc` inline, and the dispatch Z
    /// arm parks the schema into `PgProtocol::terminal_row_desc`
    /// before transitioning to `Idle`. This function takes
    /// `terminal_row_desc: Option<&'r RowDesc>` directly —
    /// `Some(&desc)` if the parked slot is populated, `None` if no
    /// schema was parked (DML / empty-query).
    ///
    /// # Tier-1 elevation: stale class architecturally impossible
    ///
    /// There is no handle that can become stale: either the slot
    /// holds a `RowDesc` (because the C → Z transition parked it)
    /// or it doesn't (because the path was DML / NoData). The
    /// slot's `is_some()` IS the schema-presence fact — no separate
    /// flag, no atomic-pair discipline, no drift surface. A naive
    /// shape carrying handles + arena would require a
    /// `StaleSchemaRef` classified-error path.
    /// : all payload fields externalised to slots on
    /// `PgProtocol::Extras`. Materialise no longer needs the slot
    /// args — payloads are unit ZSTs and `Reply` is lifetime-free.
    /// The actual data is queried via the `current_*` accessors on
    /// `OutActions<'_>` / `PgProtocol<ActivePhase>`.
    #[inline]
    pub(crate) fn into_public(self) -> Reply {
        match self {
            Self::Pong(p) => Reply::Pong(p),
            Self::StartupComplete(p) => Reply::StartupComplete(p),
            Self::QueryComplete(staged) => {
                // Staged variant discriminates between the two
                // terminal frames: `Completed` (CommandComplete + RFQ)
                // → `Reply::QueryComplete`, `Suspended` (PortalSuspended
                // + RFQ) → `Reply::QuerySuspended`. The discrimination
                // is set at staging time by the dispatch arm that
                // observed the terminal frame.
                //
                // : `command_tag` / `row_desc` fields
                // externalised; payloads are now unit-shape lifetime
                // markers.
                match staged {
                    StagedQueryCompletePayload::Completed => {
                        Reply::QueryComplete(QueryCompletePayload)
                    }
                    StagedQueryCompletePayload::Suspended => {
                        Reply::QuerySuspended(QuerySuspendedPayload)
                    }
                }
            }
            Self::ParseComplete(p) => Reply::ParseComplete(p),
            Self::CloseComplete(p) => Reply::CloseComplete(p),
            Self::DescribeStatementComplete(staged) => {
                describe_statement_complete_into_public(staged)
            }
            Self::DescribePortalComplete(staged) => {
                describe_portal_complete_into_public(staged)
            }
        }
    }
}

/// `#[inline(never)]` extraction of the Describe-arm materialise
/// body. The inline shape (Describe arms reading
/// `row_desc_slot.map(...)` directly alongside Pong / QueryComplete)
/// pushed `into_public`'s LTO-inlined body in `materialise` past
/// LLVM's register-allocator quality threshold: bench showed +13%
/// on `push_command/ping_amortised` because LLVM was spilling
/// values that previously stayed in registers (visible in asm as
/// 6-7 reloads from the same `ParamOids` u32 stack slots —
/// `[sp, #240]`, `[sp, #256]`, `[sp, #264]`, …).
///
/// Splitting the Describe arms into out-of-line helpers shrinks
/// `into_public`'s body so `materialise`'s inlined hot path
/// (Pong / QueryComplete) keeps a tight register-pressure profile.
/// The Describe paths pay a function call but they are NOT the hot
/// path — describe completion runs once per statement preparation,
/// not per row or per push.
/// : payload externalisation collapsed this helper to
/// a unit-construction. All data fields moved to slots on
/// `PgProtocol`; callers query via accessors. Kept as a 1-liner
/// for symmetry with `describe_portal_complete_into_public`.
#[inline]
fn describe_statement_complete_into_public(
    _staged: StagedDescribeStatementCompletePayload,
) -> Reply {
    Reply::DescribeStatementComplete(DescribeStatementCompletePayload)
}

/// See [`describe_statement_complete_into_public`] for rationale.
#[inline]
fn describe_portal_complete_into_public(
    _staged: StagedDescribePortalCompletePayload,
) -> Reply {
    Reply::DescribePortalComplete(DescribePortalCompletePayload)
}

/// Shared slot-projection helper to construct `DescribedRows<'r>`
/// from the parked schema slot. Used by the `OutActions` /
/// `PgProtocol` `current_described_rows()` accessors. :
/// the dispatch arms only park the slot on `T`-tag arrival; if the
/// slot is `None` at query time, semantically that means a `NoData`
/// frame was observed (the dispatch path skipped slot-park).
#[inline]
pub(crate) fn describe_rows_from_slot<'r>(
    row_desc_slot: Option<&'r crate::decode::RowDesc>,
) -> DescribedRows<'r> {
    match row_desc_slot {
        Some(desc) => DescribedRows::from_row_desc_borrow(
            crate::decode::RowDescBorrow::from_ref(desc),
        ),
        None => DescribedRows::no_data(),
    }
}

// ═════════════════════════════════════════════════════════════════
// Typed DeliverReply gate
//
// The sole authority to construct a `StagedAction::DeliverReply` is
// the `deliver()` function below, whose generic signature
// `fn deliver<K: ReplyKind>(id: ReplyId<K>, payload: K::Payload) ->
// StagedAction` forces the reply id's kind and the payload type to
// match via the `ReplyKind::Payload` associated type.
//
// Passing a `ReplyId<PingKind>` with a `StartupCompletePayload` is
// a compile error (mismatched associated type). The naive runtime-
// misroute class — dispatcher emits the wrong `Reply` variant for
// the kind — is a tier-1 compile invariant.
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
    /// its matching `K::StagedPayload`.
    ///
    /// Carries [`StagedReply`] (lifetime-free) rather than the
    /// public [`super::Reply`] (which has a `'r` lifetime tied to
    /// `PgProtocol::terminal_row_desc`). Materialise converts
    /// staged → public via `StagedReply::into_public(slot)` where
    /// `slot` is `Option<&'r RowDesc>` borrowed from the parked
    /// terminal slot.
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

        /// Read access for the materialiser. Returns `StagedReply`
        /// (not the lifetime-bound public `Reply<'r>`) — materialise
        /// borrows the parked terminal RowDesc slot and converts.
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
/// compile. Tier-1 elevation of the "wrong payload per reply kind"
/// class.
#[inline]
#[must_use]
pub(crate) fn deliver<K: crate::reply_id::ReplyKind>(
    id: crate::reply_id::ReplyId<K>,
    payload: K::StagedPayload,
) -> StagedAction<'static> {
    // `'static` because `DeliverReply` does not borrow any `'sql`
    // data. `StagedAction<'static>` is a subtype of
    // `StagedAction<'sql>` for any `'sql` (covariance via the
    // `&'sql [u8]` reference in `SendBytesBorrowed`), so callers in
    // any-`'sql` contexts can use this freely.
    StagedAction::DeliverReply(DeliverReplyEntry::new(id.consume(), payload.into()))
}

/// A typed protocol reply payload.
///
/// Each variant tuple-wraps its matching `*Payload` struct — the
/// payload IS the variant's inner. One source of truth: adding or
/// renaming a field on `PongPayload` immediately changes what
/// `Reply::Pong(..)` matches; no parallel field list to keep in
/// sync. A naive shape with per-variant duplicated field lists
/// would invite drift.
///
/// `#[non_exhaustive]` reserves variant-addition headroom
/// (`BindComplete`, `BackendKeyData`, …).
///
/// # Lifetime `'r`
///
/// Schema-bearing payloads (`QueryComplete`,
/// `DescribeStatementComplete`, `DescribePortalComplete`) carry
/// `&'r RowDesc` references into `PgProtocol::terminal_row_desc`
/// (parked by the dispatch Z arm before transitioning to `Idle`).
/// The `'r` lifetime ties the public payload's `&'r RowDesc` to
/// the `&'r mut PgProtocol` that produced the reply — same
/// lifetime as the row-streaming `ColEvent` pull API, so both row bytes
/// and row schema have identical validity windows. A naive
/// owned-inline-`RowDesc` shape would bloat the payload to ~340 B
/// per variant (RowDesc + ParamOids); the current `'r`-borrowed
/// shape is ~96 B.
///
/// **User code ergonomics**: pattern-match on the variant and
/// access `payload.row_desc` / `payload.rows` — the borrowed
/// fields are `Option<&RowDesc>` / `DescribedRows<'r>`.
///
/// **Lifetime-irrelevant variants** (Pong, StartupComplete,
/// ParseComplete, CloseComplete) carry no schema; the `'r`
/// parameter is phantom for them. Rust permits unused lifetime
/// parameters on enums — only the schema-bearing variants
/// constrain `'r`.
///
/// # Variant ordering
///
/// Variants are declared in **production-frequency order**. LLVM
/// lowers an exhaustive `match` on the discriminant to a cascade
/// (`cmp eax, 0; je ...; cmp eax, 1; je ...; ...`) at low variant
/// counts; the first-declared variant gets the cheapest predicted
/// path. Real-workload frequency analysis:
///
/// - `QueryComplete` — every successful SELECT / INSERT /
///   UPDATE / DELETE → **dominant** in OLTP and analytics.
/// - `ParseComplete` — once per prepared statement (Extended Query).
/// - `CloseComplete` — once per Close (Extended Query teardown).
/// - `Describe*Complete` — pre-Bind schema introspection.
/// - `Pong` — keep-alive Ping; rare in tight loops.
/// - `StartupComplete` — exactly once per connection.
///
/// : lifetime parameter DROPPED. All payload structs are
/// now unit ZSTs; data fields (row_desc / param_oids / command_tag /
/// tx_status) are externalised into per-phase slots on
/// `PgProtocol::Extras`. Callers query via the `current_*` /
/// `terminal_tx_status` / `fail_cause` accessors on
/// `PgProtocol<ActivePhase>` (or the corresponding `OutActions::current_*`
/// methods within the actions iteration borrow window).
///
/// Dropping the `'r` lifetime is a BREAKING API change but enables
/// the cascade: `Reply` is now lifetime-free, `Action<'w>`
/// keeps `'r` only for `current_*` accessor borrows on `OutActions`,
/// and the public action surface shrinks to:
///
/// - `Reply` (16 B max — `StartupCompletePayload` carrying pid +
///   secret_key + tx_status inline; ParseComplete / CloseComplete /
///   Pong are 0 B carriers; QueryComplete / QuerySuspended /
///   DescribeStatementComplete / DescribePortalComplete are 0 B
///   carriers — schema/cmd_tag/param_oids queried via accessors).
/// - `Action<'w>` (24 B max — DeliverReply { id: NonZeroU64,
///   value: Reply } = 8 + 16 = 24).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// A Query / BindExecute command completed. Delivered on the
    /// terminal `CommandComplete + ReadyForQuery` pair at the end
    /// of the result stream. Rows (if any) were emitted individually
    /// via the `ColEvent` row-streaming pull API. See [`QueryCompletePayload`].
    ///
    /// Ordered first — dominant variant in real workloads.
    QueryComplete(QueryCompletePayload),

    /// A `BindExecute`-with-`FetchRows::Chunked(N)` paused at the
    /// row cap before exhausting the portal (PG §55.2.7
    /// `PortalSuspended` + `ReadyForQuery`). The portal stays bound
    /// — caller can resume the stream by pushing
    /// [`crate::push_command::ExecutePortal`] referencing the same
    /// portal name (and, if needed, a new `FetchRows` cap).
    ///
    /// Unlike [`QueryComplete`](Self::QueryComplete), there is no
    /// `command_tag` (server didn't send `CommandComplete`). The
    /// `row_desc` is preserved because the portal's bound schema is
    /// still valid — the next `ExecutePortal` will produce rows of
    /// the same shape.
    ///
    /// [`QueryComplete`]: Self::QueryComplete
    QuerySuspended(QuerySuspendedPayload),

    /// A `Parse` command succeeded (server accepted the prepared
    /// statement). See [`ParseCompletePayload`].
    ParseComplete(ParseCompletePayload),

    /// A `Close` of a prepared statement or portal succeeded.
    /// See [`CloseCompletePayload`] (ZST — no body).
    CloseComplete(CloseCompletePayload),

    /// A statement-level `Describe` (`'D' 'S' name`) completed. See
    /// [`DescribeStatementCompletePayload`].
    DescribeStatementComplete(DescribeStatementCompletePayload),

    /// A portal-level `Describe` (`'D' 'P' name`) completed. See
    /// [`DescribePortalCompletePayload`].
    DescribePortalComplete(DescribePortalCompletePayload),

    /// The server is alive and responsive. See [`PongPayload`].
    Pong(PongPayload),

    /// The startup handshake completed successfully. The connection
    /// is now in [`crate::ProtoState::Idle`] and ready for queries.
    /// See [`StartupCompletePayload`].
    StartupComplete(StartupCompletePayload),
}

// ═════════════════════════════════════════════════════════════════
// Typed per-kind payload structs
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
/// Typed payload for [`crate::reply_id::PingKind`] replies (post-).
///
/// : `tx_status` field stripped. Callers query the
/// terminal transaction-status via
/// [`crate::PgProtocol::terminal_tx_status`] after consuming the
/// `Action::DeliverReply`. Externalisation cascades into Reply
/// (32 → 16-24 B) and Action (40 → 24-32 B) — the 7-byte
/// alignment-tail on 24-B-class variants collapses to zero once the
/// inline 1-B field is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PongPayload;

impl From<PongPayload> for Reply {
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
///
/// # Manual `Debug` redaction
///
/// `secret_key` is the backend's **CancelRequest authenticator** —
/// the client-side `CancelRequest` frame over TCP uses
/// `(pid, secret_key)` as the only auth. A leaked `secret_key` in
/// debug logs allows an attacker with network access to inject
/// cancel-requests impersonating the client — capability-token-
/// class leak, not password-class, but still worth redacting.
/// A naive `#[derive(Debug)]` would print
/// `StartupCompletePayload { pid: 12345, secret_key: 67890,
/// tx_status: Idle }`; manual `Debug` prints `<REDACTED>` for
/// `secret_key`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StartupCompletePayload {
    /// Backend process ID from the `BackendKeyData` frame.
    pub pid: i32,
    /// Backend secret key (for cancel requests).
    ///
    /// Logged as `<REDACTED>` via the manual `Debug` impl.
    pub secret_key: i32,
    /// Transaction status from the final `ReadyForQuery`.
    ///
    /// **exception**: tx_status is stripped from every
    /// other Reply variant (callers query
    /// [`crate::PgProtocol::terminal_tx_status`] on the post-
    /// `feed_bytes` Active-phase state), but kept inline HERE because
    /// the handshake-complete event fires from
    /// `PgProtocol<ConnectingPhase>` which lacks a persistent
    /// `ActiveExtras.tx_status` slot — only Active phase carries
    /// the slot. Callers inspect tx_status either via this field
    /// (during handshake) or via `terminal_tx_status()` (after
    /// `into_active()`). The inline 1-B field does NOT inflate
    /// `Reply<'r>` (max variant is QueryComplete at 16 B;
    /// StartupComplete at 12 B with tx_status stays under cap).
    pub tx_status: TxStatus,
}

impl core::fmt::Debug for StartupCompletePayload {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StartupCompletePayload")
            .field("pid", &self.pid)
            .field("secret_key", &"<REDACTED>")
            .field("tx_status", &self.tx_status)
            .finish()
    }
}

impl From<StartupCompletePayload> for Reply {
    #[inline]
    fn from(p: StartupCompletePayload) -> Self {
        Self::StartupComplete(p)
    }
}

/// Typed payload for [`crate::reply_id::QueryKind`] replies.
///
/// Delivered on `CommandComplete` at the end of a simple-query or
/// extended-query result stream. `command_tag` is the raw ASCII
/// tag PG returns (`"SELECT 5"`, `"INSERT 0 3"`, etc.) — typed
/// `CommandTag` parsing is a planned follow-up.
///
/// : payload is now a unit-shape ZST. The `command_tag`
/// field (formerly `&'r CommandTag`) and `row_desc` field (formerly
/// `Option<RowDescBorrow<'r>>`) have been externalised to
/// `<ActivePhase>::Extras.command_tag` /
/// `<ActivePhase>::Extras.row_desc` slots, queried via
/// [`crate::PgProtocol::current_command_tag`] /
/// [`crate::PgProtocol::current_row_desc`] (or
/// [`crate::action::OutActions::current_command_tag`] /
/// [`OutActions::current_row_desc`] within the actions iteration
/// borrow window).
///
/// The `'r` lifetime parameter has been DROPPED — the payload
/// carries no `'r`-borrowed data; accessors live on the protocol
/// surface and produce their own borrows.
///
/// # Why a payload struct at all (not a unit variant)
///
/// Preserving the struct wrapper keeps the `From<QueryCompletePayload> for Reply`
/// bridge usable for `<K: ReplyKind>::Payload` projection and
/// preserves the `Reply::QueryComplete(payload)` pattern shape
/// callers already match on (just the destructure becomes `(_)`
/// instead of `({ command_tag, row_desc })`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryCompletePayload;

impl From<QueryCompletePayload> for Reply {
    #[inline]
    fn from(p: QueryCompletePayload) -> Self {
        Self::QueryComplete(p)
    }
}

/// Typed payload for the `PortalSuspended` terminal of a
/// `BindExecute`-with-`FetchRows::Chunked(N)` flow (PG §55.2.7).
///
/// : `row_desc` field externalised. Query via
/// [`crate::PgProtocol::current_row_desc`] /
/// [`crate::action::OutActions::current_row_desc`]. The `'r`
/// lifetime parameter has been DROPPED — payload is a unit ZST.
/// See [`QueryCompletePayload`] for the externalisation rationale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QuerySuspendedPayload;

impl From<QuerySuspendedPayload> for Reply {
    #[inline]
    fn from(p: QuerySuspendedPayload) -> Self {
        Self::QuerySuspended(p)
    }
}

/// Typed payload for [`crate::reply_id::ParseKind`] replies
/// (post-).
///
/// : `tx_status` field stripped; callers query the
/// terminal transaction-status via
/// [`crate::PgProtocol::terminal_tx_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParseCompletePayload;

impl From<ParseCompletePayload> for Reply {
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

impl From<CloseCompletePayload> for Reply {
    #[inline]
    fn from(p: CloseCompletePayload) -> Self {
        Self::CloseComplete(p)
    }
}

// ═════════════════════════════════════════════════════════════════
// Describe command payloads + helper types
//
// Two payload types (statement / portal) instead of one payload
// with `Option<ParamOids>`. Rationale: a user who called
// `DescribeStatement` always gets param OIDs back; a user who
// called `DescribePortal` never does. The split surfaces this as
// TWO distinct `Reply` variants, so the `oneshot::Receiver<Reply>`
// resolves with the payload shape that matches the command — no
// runtime `match Option` + no surface-level "why is this None?"
// ambiguity. Kind-parameterisation carries the guarantee all the
// way into the `Action::DeliverReply` construction site.
// ═════════════════════════════════════════════════════════════════

/// Rows-or-not result of a `push_command::DescribeStatement`
/// / `push_command::DescribePortal` query.
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
/// # Borrowed `&'r RowDesc`
///
/// The `Rows` variant holds a `&'r RowDesc` reference borrowed
/// through the `'r` lifetime tied to the `&'r mut PgProtocol`
/// borrow. Size: ~8 B (ref + discriminant). A naive inline-
/// `RowDesc` shape would balloon to ~264 B and trigger
/// `clippy::large_enum_variant`.
///
/// # NOT `#[non_exhaustive]`
///
/// PG §55.7 defines exactly two outcomes for the post-Describe
/// schema-presence reply: `RowDescription` (`'T'`) → result
/// columns, `NoData` (`'n'`) → no columns. The wire vocabulary
/// is closed by spec — a third outcome would be a major-protocol
/// revision. Sealing via `non_exhaustive` would force downstream
/// catch-all arms for a case that **cannot exist on a well-formed
/// wire**; the dispatcher already classifies the byte BEFORE
/// constructing this enum. Closed-by-spec → exhaustive `match`
/// is the load-bearing tier-1 invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribedRows<'r> {
    /// Server sent a `RowDescription` (`'T'`) — the statement/portal
    /// produces result columns. The schema borrows from
    /// `PgProtocol::row_desc_slot` via the lifetime-bound
    /// [`crate::decode::RowDescBorrow`]; `'r` matches the containing
    /// `Reply<'r>` / `Action<'_>` lifetime.
    Rows(crate::decode::RowDescBorrow<'r>),
    /// Server sent `NoData` (`'n'`) — the statement/portal has no
    /// result columns. DML without `RETURNING` is the common case.
    NoData,
}

impl<'r> DescribedRows<'r> {
    /// Construct from a parsed `RowDescription` borrow (tag `'T'`,
    /// PG §55.7). Used by the dispatch arm for `TAG_ROW_DESCRIPTION`.
    #[inline]
    #[must_use]
    pub(crate) const fn from_row_desc_borrow(borrow: crate::decode::RowDescBorrow<'r>) -> Self {
        Self::Rows(borrow)
    }

    /// Construct the no-data sentinel. Pair to
    /// [`Self::from_row_desc_borrow`] — used in the dispatch arm for
    /// `TAG_NO_DATA` (`'n'`).
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
// Layout pinned `#[repr(C, align(4))]`:
//
// - `align(4)` matches the natural alignment of `[u32; _]` — no
//   drift possible if future code reorders the fields.
// - `repr(C)` nails field order: `n_params: BoundedU8<MAX>` at
//   offset 0 (1 B + 3 B padding), `oids` at offset 4, no trailing
//   pad (total = 4 + 16*4 = 68).
//
// (2026-05-23): migrated `n_params` from `u16` to
// `BoundedU8<MAX_PARAMS_ARITY>`. Tier-3 by-validation → tier-1
// by-construction: the type itself enforces `0 ≤ n_params ≤
// MAX_PARAMS_ARITY (= 16)`. A future refactor that constructs a
// `ParamOids` with `n_params > MAX_PARAMS_ARITY` is a compile error
// (BoundedU8's NonZeroU8-backed offset-by-one storage rejects
// out-of-range values at the `try_new` / `new_const` constructor).
// Size unchanged at 68 B (align(4) absorbs the 1-byte field into
// the same 4-byte slot the u16 occupied).
//
// The padding bytes at offsets 1..4 are ALWAYS zero via the
// `EMPTY` / `from_parts` constructors (both initialise `oids` from
// a fully-populated `[u32; N]`, and the `n_params: BoundedU8` slot
// leaves its 3 padding bytes untouched — `Copy` struct init zeroes
// padding in practice, but to remain portable across future
// refactors, the `const _: () = assert!` below pins size and
// alignment so any drift fails the build.
#[derive(Debug, Clone, Copy)]
#[repr(C, align(4))]
pub struct ParamOids {
    n_params: crate::bounded::BoundedU8<{ crate::params::MAX_PARAMS_ARITY }>,
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
// a populated-prefix compare. Requiring total array size ≤ 64
// bytes keeps it within a single AVX2 register. If
// `MAX_PARAMS_ARITY` grows past 16, revisit eq strategy
// (populated-prefix might become cheaper than the wide compare).
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
        n_params: crate::bounded::BoundedU8::ZERO,
        oids: [0; crate::params::MAX_PARAMS_ARITY],
    };

    /// Construct from a populated count + a full-capacity OID array.
    /// `pub(crate)` — only the parser creates these; users read.
    ///
    /// `n_params: BoundedU8<MAX_PARAMS_ARITY>` enforces the
    /// `0 ≤ n ≤ MAX_PARAMS_ARITY` invariant at the type level
    /// (). Callers parsing the wire frame validate the
    /// declared count against MAX_PARAMS_ARITY first, then construct
    /// a BoundedU8 via `try_new` — the out-of-range case classifies
    /// as `ProtocolError::TooManyParameters` BEFORE reaching this
    /// constructor.
    #[inline]
    #[must_use]
    pub(crate) const fn from_parts(
        n_params: crate::bounded::BoundedU8<{ crate::params::MAX_PARAMS_ARITY }>,
        oids: [u32; crate::params::MAX_PARAMS_ARITY],
    ) -> Self {
        Self { n_params, oids }
    }

    /// Number of populated parameters.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.n_params.get())
    }

    /// Whether the descriptor carries any parameters.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.n_params.get() == 0
    }

    /// Borrow the populated OIDs as a slice — tail default-filled
    /// slots are not exposed.
    ///
    /// Explicit `split_at_checked` match.
    /// `self.n_params ≤ MAX_PG_PARAMS ≤ self.oids.len()` by
    /// construction; the `None` arm is architecturally unreachable
    /// (empty-slice sentinel — same observable as "zero params",
    /// no corruption vector). A naive `.unwrap_or(&[])` would form
    /// the banned silent-fallback pattern.
    #[inline]
    #[must_use]
    pub fn oids(&self) -> &[u32] {
        let n = self.len();
        match self.oids.split_at_checked(n) {
            Some((head, _)) => head,
            None => &[],
        }
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
//
// A `heapless::Vec<u32, 16>` shape would save 60 B of zero-filled
// tail on common 0-3-param DescribeStatement replies but is
// rejected:
// 1. **Copy cascade break.** `ParamOids: Copy` flows through
//    `DescribeStatementCompletePayload` → `Reply` → `Action`. A
//    heapless::Vec-backed ParamOids loses Copy; cascading Copy
//    removal would ripple through the entire Action enum and
//    require the `ManuallyDrop` workaround at 3+ more sites. Net
//    code complexity outweighs the 60 B saved.
// 2. **Hot-path not exercised.** `ParamOids::eq` is never called
//    in hot paths; tests use `.oids()` slice view + `.len()`. The
//    SIMD-wide Eq doc claim is future-proofing, not active
//    optimisation.
// 3. **Size win marginal.** DescribeStatementComplete fires once
//    per Parse round-trip — not per-row. 60 B × N describes is
//    negligible vs `OutActions` per-feed_bytes already-addressed
//    overhead.
impl PartialEq for ParamOids {
    fn eq(&self, other: &Self) -> bool {
        self.n_params.get() == other.n_params.get() && self.oids == other.oids
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
///
/// : `param_oids` and `rows` fields externalised.
/// Query via [`crate::PgProtocol::current_param_oids`] /
/// [`crate::PgProtocol::current_described_rows`] (or the OutActions
/// equivalents within the actions iteration borrow window). The
/// `'r` lifetime parameter has been DROPPED — payload is a unit ZST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DescribeStatementCompletePayload;

impl From<DescribeStatementCompletePayload> for Reply {
    #[inline]
    fn from(p: DescribeStatementCompletePayload) -> Self {
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
///
/// : `rows` field externalised. Query via
/// [`crate::PgProtocol::current_described_rows`] /
/// [`crate::action::OutActions::current_described_rows`]. The
/// `'r` lifetime parameter has been DROPPED — payload is a unit ZST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DescribePortalCompletePayload;

impl From<DescribePortalCompletePayload> for Reply {
    #[inline]
    fn from(p: DescribePortalCompletePayload) -> Self {
        Self::DescribePortalComplete(p)
    }
}
