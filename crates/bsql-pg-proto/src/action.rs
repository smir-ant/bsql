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

    // DEF-154 (B) Phase B4: `from_write_span` unbranded helper
    // deleted. Production builders use
    // `WriteRange::from_branded_write_span` (branded equivalent
    // with buffer-identity proof); no remaining caller needed the
    // raw-buffer unbranded form.

    /// DEF-154 (A) — test-only fallback (post-P0-2 gated).
    ///
    /// A valid minimum `NonEmptyRange (start=0, len=1)`. Originally
    /// the `unwrap_or` fallback inside
    /// `WriteRange::from_branded_write_span`; deleted from
    /// production by DEF-154 (B) Phase B4-W P0-2 fix (architect
    /// audit) because it silently produced zero-length
    /// `Action::SendBytes` frames in release on builder drift —
    /// post-P0-2 the None branch classifies as
    /// `CrateBugLocus::EmptyWriteRange` and routes through
    /// `compute_push_*` → `FailReply + CloseSocket`.
    ///
    /// Retained under `#[cfg(test)]` for test fixtures that
    /// construct concrete `NonEmptyRange` values without going
    /// through `new`'s Option + explicit shield.
    #[cfg(test)]
    pub(crate) const DEAD_FALLBACK: Self = Self {
        start: 0,
        len: match NonZeroU16::new(1) {
            Some(n) => n,
            None => NonZeroU16::MIN,
        },
    };

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

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (B) Phase B3 — branded range newtypes
// ═════════════════════════════════════════════════════════════════════
//
// `WriteRange<'brand>` and `ReadRange<'brand>` wrap [`NonEmptyRange`]
// with a generative brand lifetime tied to the buffer the range was
// constructed against. Their `apply(BrandedBytes<'brand, '_>) -> &[u8]`
// methods are INFALLIBLE — the brand-identity proof combined with
// the `NonEmptyRange::new` construction-time bounds-check eliminates
// the "buffer shorter than emission-time bounds" failure mode that
// Phase B2's shielded `apply() -> Option<&[u8]>` retained at
// tier-2 runtime.
//
// # Tier-1 soundness argument
//
// Given a `WriteRange<'brand>` `r` and a `BrandedBytes<'brand, '_>`
// `b`:
//
// 1. Same brand `'brand` ⇒ same generative-lifetime scope ⇒ same
//    `with_branded` closure ⇒ same `BrandedWriteBuf` (invariant on
//    `'brand` prevents cross-closure leakage).
// 2. Inside the closure, [`crate::write_buf::BrandedWriteBuf`] exposes
//    only `reserve()` and `as_bytes_branded()`; neither `clear()`
//    nor any truncating op is reachable. The underlying `WriteBuf`
//    cannot shrink between `r`'s construction and `b`'s production.
// 3. `r`'s `start + len <= buf.len()` was validated at construction
//    (via `NonEmptyRange::new` or `from_write_span`); combined with
//    (2), `start + len <= buf.len() <= b.len()` holds at apply
//    time.
// 4. Therefore `b.as_slice().get(start..start + len)` is `Some` —
//    the brand-pattern closes the class structurally, and
//    `apply` returns `&[u8]` with no Option.
//
// Phase B3 scope — `#[cfg(test)]` scaffolding:
//   - Types and `apply` method defined and tested.
//   - Not yet threaded through [`StagedAction`] or [`materialise`]
//     (those land in B4).
//   - Existing [`NonEmptyRange::apply`] retained for the legacy
//     code path until B4 completes the migration.

/// Generatively-branded range into an outbound [`crate::write_buf::WriteBuf`].
///
/// Constructed by Phase B3+ branded builders (`build_*` on
/// [`crate::write_buf::BrandedWriteReserved<'brand, '_>`]); applied
/// at Phase B4 materialise time against
/// `BrandedWriteReserved::as_bytes_branded()`. The brand `'brand`
/// ties the range to the specific buffer + scope it was built for;
/// two distinct `with_branded` closures produce disjoint brands so
/// cross-buffer apply is a compile error.
///
/// Wraps [`NonEmptyRange`] by composition — storage layout is
/// `u16 + NonZeroU16 + ZST phantom = 4 B` (same as NonEmptyRange);
/// the brand is zero-cost at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteRange<'brand> {
    /// Underlying non-empty range — carries the validated
    /// `start`/`len` pair. Validation at construction proves
    /// `start + len <= buf.len()` at emission time; the brand
    /// preserves that bound through apply (see module block).
    inner: NonEmptyRange,
    /// Invariant phantom — see write_buf.rs Phase B1 block.
    _brand: core::marker::PhantomData<fn(&'brand ()) -> &'brand ()>,
}

impl<'brand> WriteRange<'brand> {
    /// Brand a raw [`NonEmptyRange`] — crate-internal factory.
    ///
    /// Called by Phase B3+ branded builders after producing a
    /// `NonEmptyRange` via `from_write_span` on the branded buffer's
    /// underlying slab. The `'brand` is inferred from the caller's
    /// `BrandedWriteReserved<'brand, '_>` scope, so misuse across
    /// scopes is a compile error (the HRTB-fresh `'brand` cannot
    /// unify with another scope's brand).
    #[inline]
    #[must_use]
    pub(crate) const fn from_raw(inner: NonEmptyRange) -> Self {
        Self {
            inner,
            _brand: core::marker::PhantomData,
        }
    }

    /// DEF-154 (B) — build a branded write range from the current
    /// span of a branded reserved. The `start` is captured before
    /// builder writes; after writes, `reserved.len()` gives the
    /// post-state end. The returned `WriteRange<'brand>` inherits
    /// `'brand` from the reserved — so it applies only to
    /// `BrandedBytes<'brand>` from the SAME branded scope.
    ///
    /// # Err / tier classification (P0-2 fix from architect audit)
    ///
    /// Returns `Err(InternalCrateBug { locus: EmptyWriteRange })`
    /// if `reserved.len() <= start` (i.e. builder emitted zero
    /// bytes since `start`). Architecturally dead under intact
    /// builders (every PG wire frame ≥ 5 bytes); emission
    /// indicates a builder bug or const-assert drift.
    ///
    /// Pre-fix, the None branch fell back silently to
    /// `NonEmptyRange::DEAD_FALLBACK = (0, 1)`, producing a
    /// 0-byte `Action::SendBytes` in materialise (handshake hang
    /// at the wire — tier-4 silent corruption). Tier-3 classified
    /// now: the `Err` propagates up through the builder return →
    /// `compute_push_*` → `FailReply + CloseSocket`.
    #[expect(
        clippy::result_large_err,
        reason = "Err carries ProtocolError (~300 B, large_enum_variant \
                  already accepted on ProtocolError). Architecturally \
                  cold path (builder bug / const-drift); by-value \
                  matches dispatch's DispatchOutcome::Errored surface."
    )]
    #[inline]
    pub(crate) fn from_branded_write_span(
        start: usize,
        reserved: &crate::write_buf::BrandedWriteReserved<'brand, '_>,
    ) -> Result<Self, crate::error::ProtocolError> {
        match NonEmptyRange::new(start, reserved.len(), reserved.len()) {
            Some(raw) => Ok(Self::from_raw(raw)),
            None => Err(crate::error::ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::EmptyWriteRange,
            }),
        }
    }

    /// Apply the range to same-branded bytes — **infallible at the
    /// type level**, tier-2 shielded at the body level.
    ///
    /// The same brand `'brand` on both operands, combined with the
    /// `BrandedWriteBuf`'s API narrow (no truncating ops reachable
    /// inside the branded scope) and the construction-time bounds
    /// check on `inner`, guarantees `bytes.as_slice().get(start..end)`
    /// is `Some` under intact invariants.
    ///
    /// The `debug_assert!` + `unwrap_or(&[])` pair below is the
    /// tier-2 body shield — debug builds fire loud on any
    /// invariant break (same pattern as DEF-170 / DEF-182); release
    /// preserves the `&[]` fallback (forbid-bundle bans `panic!`).
    ///
    /// Full tier-1 body closure requires `NonEmptyRange::apply`
    /// totality proof — deferred to a future refactor (would
    /// replace the Option-returning `apply` with a trusted
    /// typestate constructor).
    #[inline]
    #[must_use]
    pub(crate) fn apply<'a>(&self, bytes: crate::write_buf::BrandedBytes<'brand, 'a>) -> &'a [u8] {
        let slice = self.inner.apply(bytes.as_slice());
        debug_assert!(
            slice.is_some(),
            "WriteRange<'brand>::apply None — brand invariant broken. \
             Crate bug in the brand scaffolding (construction-bounds or \
             buffer non-shrink), not a usage error.",
        );
        slice.unwrap_or(&[])
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

/// Generatively-branded range into an inbound [`crate::buf::ReadBuf`].
///
/// Symmetric partner to [`WriteRange<'brand>`] on the read side.
/// DEF-154 (E): production-visible post read-side tier-1 lift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadRange<'brand> {
    /// Underlying non-empty range (see [`WriteRange::inner`]).
    inner: NonEmptyRange,
    /// Invariant phantom — see write_buf.rs Phase B1 block.
    _brand: core::marker::PhantomData<fn(&'brand ()) -> &'brand ()>,
}

impl<'brand> ReadRange<'brand> {
    /// Brand a raw [`NonEmptyRange`] — crate-internal factory.
    ///
    /// Production callers should prefer [`Self::new`] which
    /// threads the brand through a same-brand bounds-check
    /// witness (tier-1 soundness for apply). This raw form is
    /// kept for drift tests and corner cases where the witness
    /// shape doesn't fit.
    #[inline]
    #[must_use]
    pub(crate) const fn from_raw(inner: NonEmptyRange) -> Self {
        Self {
            inner,
            _brand: core::marker::PhantomData,
        }
    }

    /// DEF-154 (E) tier-1 constructor — validate bounds against a
    /// same-brand witness.
    ///
    /// `witness: BrandedBytes<'brand, '_>` is the read buffer's
    /// branded view (typically from `BrandedReadBuf::populated_branded()`);
    /// its brand `'brand` matches the range's own `'brand` by the
    /// HRTB closure's generativity. `start..end` is validated
    /// against `witness.as_slice().len()`; if within bounds, the
    /// returned `ReadRange<'brand>` carries the proof that `apply`
    /// will succeed against any same-brand bytes (non-shrinking
    /// invariant enforced by `BrandedReadBuf`'s API narrow — no
    /// truncating ops reachable outside the explicit
    /// `advance_scope_local` / `clear_scope_local` mutations, and
    /// those happen AFTER all range-construction sites per the
    /// feed_bytes dispatch-loop discipline).
    ///
    /// # Err classification (P0-2 pattern, mirror of write side)
    ///
    /// `CrateBugLocus::EmptyReadRange` on `NonEmptyRange::new` None —
    /// indicates dispatch computed `payload_start..payload_end`
    /// bounds that don't fit the current populated region. Routes
    /// through `DispatchOutcome::Errored` → `FailReply + CloseSocket`.
    #[expect(
        clippy::result_large_err,
        reason = "Err carries ProtocolError (~300 B, large_enum_variant \
                  already accepted on ProtocolError). Cold path (dispatch \
                  arm invariant break); by-value matches dispatch's \
                  DispatchOutcome::Errored surface."
    )]
    #[inline]
    pub(crate) fn new(
        start: usize,
        end: usize,
        witness: crate::write_buf::BrandedBytes<'brand, '_>,
    ) -> Result<Self, crate::error::ProtocolError> {
        match NonEmptyRange::new(start, end, witness.as_slice().len()) {
            Some(raw) => Ok(Self::from_raw(raw)),
            None => Err(crate::error::ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::EmptyReadRange,
            }),
        }
    }

    /// Apply the range to same-branded bytes — **infallible**.
    ///
    /// Tier-1 soundness chain (mirror of `WriteRange::apply`):
    /// 1. Same `'brand` ⇒ same `with_branded` closure ⇒ same
    ///    `&mut ReadBuf`.
    /// 2. `BrandedReadBuf` exposes only shared views
    ///    (`populated_branded`, `unread_branded`) + scope-local
    ///    mutations (`advance_scope_local`, `clear_scope_local`).
    ///    The mutations don't shrink populated content; advance
    ///    bumps the cursor but bytes remain in `populated()`.
    /// 3. `ReadRange::new(start, end, witness)` validated
    ///    `end <= witness.len()` at construction; (2) preserves
    ///    that bound through subsequent apply.
    /// 4. `NonEmptyRange::apply` body is `buf.get(start..end)` —
    ///    Some by (3). The `unwrap_or(&[])` fallback is forbid-
    ///    bundle compliance; architecturally dead under intact
    ///    brand discipline.
    ///
    /// No `debug_assert` shield — the class is closed structurally
    /// by the brand + bounds-validated constructor.
    #[inline]
    #[must_use]
    pub(crate) fn apply<'a>(&self, bytes: crate::write_buf::BrandedBytes<'brand, 'a>) -> &'a [u8] {
        self.inner.apply(bytes.as_slice()).unwrap_or(&[])
    }

    /// Access the underlying [`NonEmptyRange`] — for drift pins +
    /// tests. Production materialise path applies via `apply` +
    /// branded witness; direct unwrap is test-only.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) const fn inner(&self) -> NonEmptyRange {
        self.inner
    }
}

// Phase B3 drift pins: branded ranges must stay the same size as
// the underlying `NonEmptyRange` (phantom is ZST).
const _: () = assert!(
    core::mem::size_of::<WriteRange<'_>>() == core::mem::size_of::<NonEmptyRange>(),
    "WriteRange<'brand> size regression — brand phantom must be ZST (4 B total).",
);
const _: () = assert!(
    core::mem::size_of::<ReadRange<'_>>() == core::mem::size_of::<NonEmptyRange>(),
    "ReadRange<'brand> size regression — brand phantom must be ZST (4 B total).",
);

#[cfg(test)]
mod phase_b3_tests {
    //! DEF-154 (B) Phase B3 — branded range newtype tests.
    //!
    //! Phase B3 covers the shape + infallible `apply` of the
    //! branded ranges. Actual end-to-end builder → apply
    //! round-tripping (pushing bytes into a buffer via a branded
    //! reserved and applying the range against same-brand bytes)
    //! requires push methods on `BrandedWriteReserved`, which land
    //! in Phase B4 alongside builder migration. Phase B3 tests
    //! what CAN be tested without that:
    //!   - Types are constructible via `from_raw`.
    //!   - `inner()` accessor round-trips.
    //!   - Size is 4 B (phantom ZST).
    //!   - `apply()` on a same-brand `BrandedBytes` returns
    //!     `&[u8]` (not `Option<&[u8]>`) — tier-1 lift at the
    //!     type level, infallibility verified by the
    //!     construction-bounds + brand-identity argument in the
    //!     module block.
    //!
    //! Cross-brand rejection is a compile-time property and lands
    //! in a future trybuild harness.
    use super::*;
    use crate::write_buf::WriteBuf;

    /// B3-1: happy-path apply — build a range whose `(start=0,
    /// len=1)` fits the 1-byte populated buffer, apply it
    /// same-branded, observe the expected single-byte slice with
    /// no call-site `Option` unwrap.
    ///
    /// The byte is pushed BEFORE entering the branded scope
    /// (Phase B3 doesn't yet migrate push methods onto
    /// `BrandedWriteReserved` — that's Phase B4). Inside the
    /// branded scope we use `as_bytes_branded()` directly from
    /// `BrandedWriteBuf` (no `reserve()` call, since reserve's
    /// `is_empty` debug_assert is designed for fresh buffers at
    /// build-start, not for materialise-time read access).
    #[test]
    fn write_range_apply_returns_infallible_slice() {
        let mut buf = WriteBuf::new();
        let push_ok = buf.push_u8(0x42);
        assert!(push_ok.is_ok(), "push_u8 must succeed on fresh buffer");
        let byte = buf.with_branded(|wb| {
            // `as_bytes_branded()` is the materialise-time branded
            // view — no capacity gate, just a shared-brand slice.
            let bytes = wb.as_bytes_branded();
            // Build a raw NonEmptyRange covering [0, 1) against
            // the 1-byte buffer — validated by NonEmptyRange::new.
            let raw = NonEmptyRange::new(0, 1, 1).unwrap_or(NonEmptyRange::DEAD_FALLBACK);
            let range = WriteRange::from_raw(raw);
            // The `apply` return type is `&[u8]`, NOT
            // `Option<&[u8]>` — that's the tier-1 lift. No
            // unwrap_or at this call site.
            let slice: &[u8] = range.apply(bytes);
            slice.first().copied().unwrap_or(0)
        });
        assert_eq!(byte, 0x42, "branded WriteRange apply round-trips the pushed byte");
    }

    /// B3-2: drift pin — WriteRange / ReadRange are 4 bytes
    /// (phantom ZST + 4-byte NonEmptyRange).
    #[test]
    fn branded_range_sizes_match_raw() {
        assert_eq!(core::mem::size_of::<WriteRange<'_>>(), 4);
        assert_eq!(core::mem::size_of::<ReadRange<'_>>(), 4);
        assert_eq!(
            core::mem::size_of::<Option<WriteRange<'_>>>(),
            4,
            "Option<WriteRange> must niche-pack on NonZeroU16 inside NonEmptyRange.len",
        );
    }

    /// B3-3: `inner()` accessor round-trips the underlying
    /// NonEmptyRange — drift pin against accidental accessor
    /// removal (used by debug-output paths during migration).
    #[test]
    fn branded_range_inner_roundtrip() {
        let raw = NonEmptyRange::DEAD_FALLBACK;
        let w = WriteRange::<'static>::from_raw(raw);
        let r = ReadRange::<'static>::from_raw(raw);
        assert_eq!(w.inner(), raw);
        assert_eq!(r.inner(), raw);
    }

    /// B3-4: ReadRange apply via `ReadBuf::with_branded` scope —
    /// tier-1 lift on the read side. Uses the real Phase B2
    /// branded-read-buffer constructor to establish `'brand`
    /// naturally via HRTB, then constructs + applies a
    /// same-branded range.
    #[test]
    fn read_range_apply_returns_infallible_slice() {
        let mut rbuf = crate::buf::ReadBuf::new();
        let appended = rbuf.append(b"XY");
        assert!(appended.is_ok());
        let byte = rbuf.with_branded(|rb| {
            let bytes = rb.populated_branded();
            // The `'brand` here is HRTB-fresh; `ReadRange::from_raw`
            // inherits it from the expected type at the call site
            // (the `apply(bytes)` call below fixes `'brand` to
            // match `bytes`'s brand).
            let raw = NonEmptyRange::new(0, 2, 2).unwrap_or(NonEmptyRange::DEAD_FALLBACK);
            let range = ReadRange::from_raw(raw);
            let slice: &[u8] = range.apply(bytes);
            slice.first().copied().unwrap_or(0)
        });
        assert_eq!(byte, b'X');
    }

    /// DEF-154 (B) Phase B4-W P0-2 + P2 closure: exercise the
    /// classified Err path of `WriteRange::from_branded_write_span`.
    ///
    /// `NonEmptyRange::new(start, end, bounds)` returns `None` iff
    /// `end <= start` OR `end > bounds`. Post-builder,
    /// `end = reserved.len()` = `bounds`. So the only way to force
    /// None is `start >= reserved.len()` — simulating a builder
    /// that captured `start` post-push, skipped pushes, or
    /// overflowed the usize into the end field (all genuine
    /// builder-drift scenarios).
    ///
    /// The test forces `start > reserved.len()` by calling
    /// `from_branded_write_span(10, ...)` on a fresh (empty)
    /// reserved. Err path fires with
    /// `CrateBugLocus::EmptyWriteRange` — pre-P0-2 this silently
    /// returned `WriteRange(DEAD_FALLBACK)`, a tier-4 0-byte
    /// Action::SendBytes on apply.
    #[test]
    fn from_branded_write_span_err_classified_as_empty_write_range() {
        let mut buf = crate::write_buf::WriteBuf::new();
        // The `Result<WriteRange<'brand>, _>` itself cannot escape
        // the branded closure — `'brand` is HRTB-fresh and
        // invariant. Inside the closure, observe the Err variant
        // and return a brand-free discriminant (`true` iff
        // classified as EmptyWriteRange). This also doubles as a
        // *generativity smoke test*: if `WriteRange<'brand>` could
        // escape, the test would fail to compile.
        let is_empty_write_range = buf.with_branded(|mut wb| {
            let reserved = wb.reserve();
            // reserved.len() == 0 (fresh). start=10 > 0 forces
            // NonEmptyRange::new None → EmptyWriteRange.
            let result = WriteRange::from_branded_write_span(10, &reserved);
            matches!(
                result,
                Err(crate::error::ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::EmptyWriteRange,
                })
            )
        });
        assert!(
            is_empty_write_range,
            "from_branded_write_span must return Err(EmptyWriteRange) when \
             start > reserved.len() — pre-P0-2 this silently fell back to \
             DEAD_FALLBACK (tier-4 0-byte Action::SendBytes).",
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
        // DEF-178 (audit2 A023): debug_assert pins the
        // architecturally-dead unwrap_or(&[]) fallback. len() is
        // invariant-bounded to `MAX_ACTIONS_PER_CALL == items.len()`
        // by the push() capacity check. Sibling pattern to
        // `FixedStr::as_bytes` (pass-#8 F-061).
        debug_assert!(
            self.len() <= self.items.len(),
            "DEF-178: OutActions.len exceeds item capacity — invariant break",
        );
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
pub(crate) type StagedActions<'wb, 'rb> = heapless::Vec<StagedAction<'wb, 'rb>, MAX_ACTIONS_PER_CALL>;

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
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable. DeliverReply's Reply<'r> payload is small post-DEF-119 arena externalisation; the FailReply.cause (ProtocolError) is the dominant variant (see lib.rs::action::Action size budget for current exact bounds). FailReply is cold-path (emitted only on protocol failure), so the large variant's footprint is acceptable.")]
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
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable. Mirrors the `Action<'w, 'r>` rationale above — DEF-119 shrunk the DeliverReply payload; FailReply.cause (ProtocolError) dominates. FailReply is cold-path.")]
pub(crate) enum StagedAction<'wb, 'rb> {
    /// Bytes live at the range `[start..start+len]` inside the
    /// emission-time branded `write_buf` (brand `'wb`). Typed as
    /// [`WriteRange<'wb>`] — brand proves buffer-identity +
    /// non-zero length (DEF-100 + DEF-154 (B)).
    SendBytesRange(WriteRange<'wb>),
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
        /// DEF-154 (E): `ReadRange<'rb>` — brand proves buffer-
        /// identity with the read scope; `apply` at materialise
        /// time is tier-1 infallible.
        row_range: ReadRange<'rb>,
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
    /// DEF-154 (B+E) brand-lifetime anchor for BOTH `'wb` and `'rb`.
    ///
    /// `'wb` is carried by [`Self::SendBytesRange`] via
    /// [`WriteRange<'wb>`]; `'rb` by [`Self::StreamRowRange`] via
    /// [`ReadRange<'rb>`]. This variant anchors BOTH so that
    /// variants not carrying either brand still fix the lifetime
    /// parameters at the enum level. Tuple of two invariant
    /// phantom-fn pointers keeps both brands invariant under
    /// subtyping.
    ///
    /// # NEVER CONSTRUCT THIS VARIANT
    ///
    /// See `StagedAction`'s original `_Phantom` doc for the full
    /// contract. Match arms handle as neutral no-op.
    #[doc(hidden)]
    _Phantom(DualBrandInvariant<'wb, 'rb>),
}

/// DEF-154 (E): invariant-anchor for both `'wb` and `'rb`.
/// Factored out of `StagedAction::_Phantom` to silence
/// clippy::type_complexity on the enum definition. ZST; matches
/// the pair of `fn(&'X ()) -> &'X ()` phantom-fn-pointers pattern
/// from Phase B1 (see `write_buf.rs` "Invariance mechanism" block).
pub(crate) type DualBrandInvariant<'wb, 'rb> = core::marker::PhantomData<(
    fn(&'wb ()) -> &'wb (),
    fn(&'rb ()) -> &'rb (),
)>;

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
    /// prematurely) is a crate-internal bug.
    ///
    /// Tier classification (post-DEF-170, DEF-183 P1-C):
    /// - **Tier-2 structural runtime** in debug/test via
    ///   `debug_assert!(d.is_some(), ...)` — the shield fires loud
    ///   on any stale ref that slips past architectural invariants.
    /// - **Tier-4 silent fallback** in release — `get` returns
    ///   `None` which the conversion maps to `Option::None` /
    ///   `NoData` (forbid-bundle bans `panic!` in release user code,
    ///   so this is the tightest non-witness closure).
    /// - **Tier-1 compile-time** closure of the class is scheduled
    ///   in DEF-154 (D) — stale-ref compile elimination via
    ///   buffer-witness-with-brand + ArenaReader (C shipped).
    #[inline]
    pub(crate) fn into_public<'r>(
        self,
        arena: crate::schema_arena::ArenaReader<'r>,
    ) -> Reply<'r> {
        match self {
            Self::Pong(p) => Reply::Pong(p),
            Self::StartupComplete(p) => Reply::StartupComplete(p),
            Self::QueryComplete(staged) => Reply::QueryComplete(QueryCompletePayload {
                command_tag: staged.command_tag,
                tx_status: staged.tx_status,
                // DEF-170 (audit2 A010): stale SchemaRef → silent
                // `None` was the pre-DEF-170 behaviour, corrupting
                // SELECT→DML classification at the user boundary.
                // Debug build now fires loud on stale refs; release
                // preserves the `None` fallback (forbid-bundle bans
                // panic). DEF-154 witness-pattern will eliminate the
                // stale class structurally.
                row_desc: staged.schema_ref.and_then(|r| {
                    let d = arena.get(r);
                    debug_assert!(
                        d.is_some(),
                        "DEF-170: stale SchemaRef at QueryComplete materialise \
                         — crate bug; DEF-154 witness-pattern will eliminate \
                         this class structurally.",
                    );
                    d
                }),
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
/// borrow.
///
/// DEF-170 (audit2 A010): stale `Rows(ref)` → silent `NoData` was
/// the pre-DEF-170 behaviour, corrupting schema-bearing describe
/// results at the user boundary. Debug build now fires loud on
/// stale refs; release preserves the `NoData` fallback
/// (forbid-bundle bans panic). DEF-154 witness-pattern will
/// eliminate the stale class structurally.
///
/// F8 intent markers: uses [`DescribedRows::from_row_desc`] and
/// [`DescribedRows::no_data`] factories rather than direct variant
/// construction. Swapping the arm bodies still type-checks, but the
/// factory names make the swap obvious on code review and the
/// `arena.get(s)` resolution arm explicit.
#[inline]
fn described_rows_ref_into_public<'r>(
    r: crate::state::DescribedRowsRef,
    arena: crate::schema_arena::ArenaReader<'r>,
) -> DescribedRows<'r> {
    match r {
        crate::state::DescribedRowsRef::Rows(s) => match arena.get(s) {
            Some(desc) => DescribedRows::from_row_desc(desc),
            None => {
                debug_assert!(
                    false,
                    "DEF-170: stale SchemaRef at described_rows_ref_into_public \
                     — crate bug; DEF-154 witness-pattern will eliminate \
                     this class structurally.",
                );
                DescribedRows::no_data()
            }
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
    /// `StagedReply::into_public(reader)` where `reader` is the
    /// DEF-154 (C) [`crate::schema_arena::ArenaReader<'r>`] witness.
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
pub(crate) fn deliver<'wb, 'rb, K: crate::reply_id::ReplyKind>(
    id: crate::reply_id::ReplyId<K>,
    payload: K::StagedPayload,
) -> StagedAction<'wb, 'rb> {
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
