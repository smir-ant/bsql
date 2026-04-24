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

// DEF-154 (V) P2-3: round-trip compile pin for TxStatus.
// `try_from_byte(byte(v)) == Ok(v)` must hold for every variant —
// catches a body-swap drift (e.g. `Self::Idle => b'T'`) at build
// time rather than in an integration test. Tier-3 audit → tier-1
// compile.
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
    // `WriteRange::from_write_span` (branded equivalent
    // with buffer-identity proof); no remaining caller needed the
    // raw-buffer unbranded form.

    /// DEF-154 (A) — test-only fallback (post-P0-2 gated).
    ///
    /// A valid minimum `NonEmptyRange (start=0, len=1)`. Originally
    /// the `unwrap_or` fallback inside
    /// `WriteRange::from_write_span`; deleted from
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
    /// DEF-154 (N): `debug_assert!(slice.is_some(), ...)` REMOVED.
    /// The assert was the "debug loud + release silent" pattern
    /// user banned. Callers (`WriteRange::apply`) now propagate the
    /// None via their own Option return and materialise classifies
    /// the mismatch via `CloseSocket` emission (no silent `&[]`,
    /// no debug panic target).
    #[inline]
    pub(crate) fn apply<'a>(&self, buf: &'a [u8]) -> Option<&'a [u8]> {
        // DEF-147: widen u16 → usize via infallible usize::from before
        // slice indexing.
        let start = usize::from(self.start);
        let end = start.checked_add(usize::from(self.len.get()))?;
        buf.get(start..end)
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
// `WriteRange` and `ReadRange<'brand>` wrap [`NonEmptyRange`]
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
// Given a `WriteRange` `r` and a `BrandedBytes<'brand, '_>`
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

/// Range into an outbound [`crate::write_buf::WriteBuf`].
///
/// DEF-154 (W): `'brand` phantom deleted. Pre-(W) this was
/// `WriteRange` with a `PhantomData<fn(&'brand ()) ->
/// &'brand ()>` field, claiming tier-1 infallible apply via
/// buffer-identity proof. DEF-154 (N) reverted `apply` to return
/// `Option<&[u8]>` — the brand's only tier-1 deliverable
/// evaporated. Post-(W) bare `NonEmptyRange` wrapper; apply is
/// runtime-checked and classified (not silent) on mismatch.
///
/// Tier today: tier-2 structural (construction validates
/// `start + len <= buf.len()`; apply None is classified
/// `CloseSocket` emission at materialise). API-narrow on
/// `WriteReserved` prevents mid-scope truncation.
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

    /// DEF-154 (B+W) — build a write range from the current span
    /// of a `WriteReserved`. `start` is captured before builder
    /// writes; `reserved.len()` after gives the post-state end.
    ///
    /// # Err classification (P0-2 fix from architect audit)
    ///
    /// Returns `Err(InternalCrateBug { locus: EmptyWriteRange })`
    /// if `reserved.len() <= start` (builder emitted zero bytes
    /// since `start`). Architecturally dead under intact builders
    /// (every PG wire frame ≥ 5 bytes); classified via the crate-
    /// bug locus rather than silently fabricating a fallback
    /// range.
    ///
    /// DEF-154 (W) note: pre-(W) this was `from_branded_write_span`
    /// taking `&BrandedWriteReserved<'_>`; the brand
    /// phantom added zero tier-1 (apply returned Option anyway per
    /// DEF-154 N). Renamed + unbranded post-(W).
    // DEF-184 (A1+A13): ProtocolError shrunk 312 → ~72 B, below
    // the `result_large_err` 128 B threshold; no longer needs
    // #[expect].
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
    /// bounds mismatch. Materialise classifies None via
    /// `CloseSocket` emission (not silent).
    ///
    /// DEF-154 (N + W): apply signature is `Option<&[u8]>`. Pre-(N)
    /// it was `&[u8]` with `debug_assert + unwrap_or(&[])` —
    /// banned silent pattern. Pre-(W) it took
    /// `BrandedBytes<'brand, 'a>`; post-(W) takes plain `&'a [u8]`
    /// since the brand carried no additional guarantee (DEF-154 N
    /// had already reduced apply to runtime-checked Option).
    ///
    /// None arm is architecturally dead under API-narrow
    /// `WriteReserved` (no truncating ops between construction +
    /// apply); materialise's CloseSocket emission is the
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

// DEF-154 (H): `ReadRange<'brand>` type + `BrandedReadBuf` +
// `DualBrandInvariant` + the entire `'rb` brand scaffolding on
// the read side DELETED. `StagedAction::StreamRowRange` now
// carries `row_bytes: &'r [u8]` directly — slice is borrowed at
// dispatch time from `read_buf.populated()`, stored in staged,
// and passed through materialise unchanged (tier-1 identity apply).
//
// The `'rb` brand was introduced in (E) to prove "same buffer"
// for `ReadRange::apply` bounds-safety. But Rust's borrow checker
// already tracks slice lifetime — storing `&'r [u8]` directly is
// strictly simpler and gives tier-1 apply "for free" (no Option,
// no unwrap_or, no debug_assert). The only reason `(start, len)`
// indirection was needed was to decouple stage-time borrow from
// post-loop `advance_scope_local` mutation. DEF-154 (H) replaces
// that mutation with `PgProtocol.pending_advance: u16` —
// deferring the advance to the next feed_bytes call's entry —
// which allows the stage-time slice to keep its shared borrow
// through materialise without conflicting with a cursor move.

// Phase B3 drift pin: WriteRange must stay the same size as
// the underlying `NonEmptyRange` (phantom is ZST).
const _: () = assert!(
    core::mem::size_of::<WriteRange>() == core::mem::size_of::<NonEmptyRange>(),
    "WriteRange size regression — must equal NonEmptyRange (4 B) post DEF-154 (W) \
     phantom deletion.",
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
        // DEF-154 (W): no more `with_branded` HRTB closure; direct
        // access to the unbranded buffer.
        let bytes = buf.as_bytes();
        let raw = NonEmptyRange::new(0, 1, 1).unwrap_or(NonEmptyRange::DEAD_FALLBACK);
        let range = WriteRange::from_raw(raw);
        let slice: &[u8] = range.apply(bytes).unwrap_or(&[]);
        let byte = slice.first().copied().unwrap_or(0);
        assert_eq!(byte, 0x42, "WriteRange apply round-trips the pushed byte");
    }

    /// B3-2 + DEF-154 (W): drift pin — WriteRange is 4 bytes
    /// (post-(W) identical layout to `NonEmptyRange`; no phantom).
    #[test]
    fn branded_range_sizes_match_raw() {
        assert_eq!(core::mem::size_of::<WriteRange>(), 4);
        assert_eq!(
            core::mem::size_of::<Option<WriteRange>>(),
            4,
            "Option<WriteRange> must niche-pack on NonZeroU16 inside NonEmptyRange.len",
        );
    }

    /// B3-3 + DEF-154 (W): `inner()` accessor round-trip.
    #[test]
    fn branded_range_inner_roundtrip() {
        let raw = NonEmptyRange::DEAD_FALLBACK;
        let w = WriteRange::from_raw(raw);
        assert_eq!(w.inner(), raw);
    }

    /// DEF-154 (B) Phase B4-W P0-2 + P2 + (W) closure: exercise
    /// the classified Err path of `WriteRange::from_write_span`.
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
    /// `from_write_span(10, ...)` on a fresh (empty) reserved.
    /// Err path fires with `CrateBugLocus::EmptyWriteRange` —
    /// pre-P0-2 this silently returned `WriteRange(DEAD_FALLBACK)`,
    /// a tier-4 0-byte Action::SendBytes on apply.
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
/// # DEF-184 (A2/B1/B8): `ManuallyDrop<heapless::Vec>` backing
///
/// Pre-(184) used `[Action; MAX_ACTIONS_PER_CALL]` + `u8 len`
/// with `Action::CloseSocket` sentinel-fill — every
/// `OutActions::new()` paid **5008 B zero-fill/call** (16 × 312 B +
/// pad). Post-(184): `ManuallyDrop<heapless::Vec<Action, N>>` —
/// zero init writes via `heapless::Vec::new()`, wrapper suppresses
/// the Drop impl that would otherwise extend NLL borrows past
/// last-use.
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
/// Since `Action<'w, 'r>` is `Copy` (POD refs + small payloads),
/// the inner Drop body is trivial anyway — skipping it is sound.
///
/// ## Win
///
/// 5008 B zero-fill per `feed_bytes` / `push_command` / `iter_rows
/// .slow_path_once` → **0 B init**. Stack reservation size
/// unchanged (`[MaybeUninit<Action>; N]` still allocates the
/// slots), but the write bandwidth disappears.
#[derive(Debug)]
pub struct OutActions<'w, 'r> {
    /// ManuallyDrop-wrapped heapless vec. `ManuallyDrop` makes the
    /// wrapper Drop-free regardless of inner type, preserving
    /// pre-(184) NLL last-use borrow-release semantics.
    items: core::mem::ManuallyDrop<
        heapless::Vec<Action<'w, 'r>, MAX_ACTIONS_PER_CALL>,
    >,
}

impl Default for OutActions<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'w, 'r> OutActions<'w, 'r> {
    /// Construct an empty `OutActions`.
    ///
    /// DEF-184 (A2/B1/B8): replaced `[Action::CloseSocket; N]`
    /// eager fill (5008 B of writes) with `ManuallyDrop::new
    /// (heapless::Vec::new())` (zero writes).
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
    pub fn as_slice(&self) -> &[Action<'w, 'r>] {
        self.items.as_slice()
    }

    /// Return the first populated action (or `None` if empty).
    /// Convenience for test assertions.
    #[inline]
    pub fn first(&self) -> Option<&Action<'w, 'r>> {
        self.items.first()
    }

    /// Push an action. Returns `Err(action)` (mirrors heapless's
    /// convention) if the container is full.
    // DEF-184 (A1+A13): Action shrunk Reply-bounded ~88 B; Err
    // path no longer triggers `result_large_err`.
    #[inline]
    pub fn push(&mut self, action: Action<'w, 'r>) -> Result<(), Action<'w, 'r>> {
        self.items.push(action)
    }
}

impl<'w, 'r> IntoIterator for OutActions<'w, 'r> {
    type Item = Action<'w, 'r>;
    type IntoIter = <heapless::Vec<Action<'w, 'r>, MAX_ACTIONS_PER_CALL> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        // Unwrap ManuallyDrop to move inner vec; since `Action`
        // is Copy the inner vec's Drop is a no-op anyway. This is
        // sound per `ManuallyDrop::into_inner` safety contract
        // (forgotten drop would be unsound only for drop-active T).
        core::mem::ManuallyDrop::into_inner(self.items).into_iter()
    }
}

/// By-reference iteration — `for action in &out` yields `&Action`.
impl<'a, 'w, 'r> IntoIterator for &'a OutActions<'w, 'r> {
    type Item = &'a Action<'w, 'r>;
    type IntoIter = core::slice::Iter<'a, Action<'w, 'r>>;
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
// DEF-154 (L): staged container uses `MAX_STAGED_PER_CALL`
// (dispatch-side cap); output uses `MAX_ACTIONS_PER_CALL` (fan-out).
pub(crate) type StagedActions = heapless::Vec<StagedAction, { crate::protocol::MAX_STAGED_PER_CALL }>;

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
///
/// # DEF-163 B011: why two lifetimes (`'w` + `'r`)?
///
/// The two lifetimes are NOT cosmetic — they are load-bearing:
/// - `'w` borrows `write_buf` on the **push path**. Entry-points
///   `push_command` / `push_bind_execute` build outbound frames
///   into `WriteBuf`; `SendBytes(&'w [u8])` references the staged
///   bytes. The host writes them to the socket and drops the
///   Action, releasing `'w`.
/// - `'r` borrows `read_buf` + `schema_arena` on the **feed
///   path**. `feed_bytes` parses inbound frames into `read_buf`;
///   row-streaming actions like `StreamRow { desc: &'r RowDesc,
///   row_bytes: &'r [u8] }` borrow directly from the populated
///   region (zero-copy). Host reads + drops the Action, releasing
///   `'r`.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[must_use = "an Action carries a side-effect that must be executed"]
// DEF-184 (A1+A13): Action is no longer large-enum-variant-warn-worthy post-ErrorArena cascade; removed #[expect(large_enum_variant)].
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
// DEF-184 (A1+A13): StagedAction no longer triggers
// `large_enum_variant` post-ErrorArena cascade (FailReply.cause
// ProtocolError shrunk from 312 B to ~72 B).
#[derive(Debug)]
pub(crate) enum StagedAction {
    /// Bytes live at the range `[start..start+len]` in the
    /// caller's `write_buf`. Non-zero length (DEF-100).
    ///
    /// DEF-154 (W): `'wb` brand phantom deleted. Pre-(W) this was
    /// `WriteRange` — brand claimed tier-1 buffer-identity
    /// proof, but DEF-154 (N) reduced `apply` to
    /// `Option<&[u8]>` runtime-checked, the brand's only tier-1
    /// deliverable. Post-(W) plain `WriteRange` — apply is
    /// classified (None → `CloseSocket` at materialise).
    SendBytesRange(WriteRange),
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
    #[doc(hidden)] pub rows: crate::state::DescribedRowsStaged,
    #[doc(hidden)] pub tx_status: TxStatus,
}

/// Lifetime-free staged counterpart to
/// [`DescribePortalCompletePayload<'r>`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedDescribePortalCompletePayload {
    #[doc(hidden)] pub rows: crate::state::DescribedRowsStaged,
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
    /// DEF-154 (J) P0-D: returns `Err(StaleSchemaRef)` if any
    /// contained `SchemaRef` is stale in `arena`. Pre-(J) stale
    /// refs silently mapped to `None` (QueryComplete.row_desc) or
    /// `NoData` (DescribedRows) — invisible corruption at the user
    /// boundary (debug_assert shield in debug, silent in release).
    /// Post-(J) the caller (materialise) classifies and emits
    /// `FailReply { StaleSchemaRef } + CloseSocket` instead of the
    /// silently-degraded payload.
    ///
    /// Architecturally every `SchemaRef` here points to a live slot
    /// — dispatch arms alloc the slot and the slot stays live
    /// through materialise. A stale `schema_ref` is a crate bug;
    /// Err propagation replaces the prior debug_assert + silent
    /// fallback dyad.
    #[inline]
    pub(crate) fn into_public<'r>(
        self,
        arena: crate::schema_arena::ArenaReader<'r>,
    ) -> Result<Reply<'r>, StaleSchemaRef> {
        match self {
            Self::Pong(p) => Ok(Reply::Pong(p)),
            Self::StartupComplete(p) => Ok(Reply::StartupComplete(p)),
            Self::QueryComplete(staged) => {
                // `schema_ref: Option<SchemaRef>` — None is
                // legitimate (DML with no schema). Some(ref) must
                // resolve; stale → classified Err.
                let row_desc = match staged.schema_ref {
                    Some(r) => match arena.get(r) {
                        Some(d) => Some(d),
                        None => return Err(StaleSchemaRef),
                    },
                    None => None,
                };
                Ok(Reply::QueryComplete(QueryCompletePayload {
                    command_tag: staged.command_tag,
                    tx_status: staged.tx_status,
                    row_desc,
                }))
            }
            Self::ParseComplete(p) => Ok(Reply::ParseComplete(p)),
            Self::CloseComplete(p) => Ok(Reply::CloseComplete(p)),
            Self::DescribeStatementComplete(staged) => {
                let rows = described_rows_ref_into_public(staged.rows, arena)?;
                Ok(Reply::DescribeStatementComplete(DescribeStatementCompletePayload {
                    param_oids: staged.param_oids,
                    rows,
                    tx_status: staged.tx_status,
                }))
            }
            Self::DescribePortalComplete(staged) => {
                let rows = described_rows_ref_into_public(staged.rows, arena)?;
                Ok(Reply::DescribePortalComplete(DescribePortalCompletePayload {
                    rows,
                    tx_status: staged.tx_status,
                }))
            }
        }
    }
}

/// DEF-154 (J): classified sentinel for stale-SchemaRef at
/// materialise. Zero-sized; no payload needed — materialise
/// constructs a `ProtocolError::InternalCrateBug { StaleSchemaRef }`
/// when it sees this.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StaleSchemaRef;

/// Convert state-side [`crate::state::DescribedRowsStaged`] to the
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
    r: crate::state::DescribedRowsStaged,
    arena: crate::schema_arena::ArenaReader<'r>,
) -> Result<DescribedRows<'r>, StaleSchemaRef> {
    match r {
        crate::state::DescribedRowsStaged::Rows(s) => match arena.get(s) {
            Some(desc) => Ok(DescribedRows::from_row_desc(desc)),
            // DEF-154 (J): stale ref classification — Err propagates
            // through `into_public` → materialise emits classified
            // FailReply + CloseSocket. Pre-(J) the `NoData` fallback
            // was silent and indistinguishable from a legitimate
            // no-schema Describe result.
            None => Err(StaleSchemaRef),
        },
        crate::state::DescribedRowsStaged::NoData => Ok(DescribedRows::no_data()),
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
/// [`crate::schema_arena::SchemaArena`]; payloads now borrow
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
///
/// # DEF-185 P1-C (audit 2026-04-24): manual Debug redaction
///
/// `secret_key` is the backend's **CancelRequest authenticator** —
/// PG's cancel protocol (`pg_cancel_backend` server-side is gated by
/// pg_hba.conf, but the client-side `CancelRequest` frame over TCP
/// uses `(pid, secret_key)` as the only auth). A leaked `secret_key`
/// in debug logs allows an attacker with network access to inject
/// cancel-requests impersonating the client — capability-token-class
/// leak, not password-class, but still worth redacting.
///
/// Pre-fix: `#[derive(Debug)]` printed `StartupCompletePayload {
/// pid: 12345, secret_key: 67890, tx_status: Idle }` — operators
/// logging `OutActions` for diagnostics would expose secret_key.
/// Post-fix: manual `Debug` prints `<REDACTED>` for `secret_key`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StartupCompletePayload {
    /// Backend process ID from the `BackendKeyData` frame.
    pub pid: i32,
    /// Backend secret key (for cancel requests).
    ///
    /// Logged as `<REDACTED>` via manual Debug impl (DEF-185 P1-C).
    pub secret_key: i32,
    /// Transaction status from the final `ReadyForQuery`.
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
    ///
    /// DEF-154 (S) P1-1: explicit `split_at_checked` match.
    /// `self.n_params ≤ MAX_PG_PARAMS ≤ self.oids.len()` by
    /// construction; None architecturally unreachable. Empty-slice
    /// sentinel on the dead arm (same observable as "zero params",
    /// no corruption vector). Pre-(S) was `.unwrap_or(&[])` —
    /// silent fallback pattern.
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
// DEF-184 (B11 REJECTED): audit #2 proposed swapping to
// `heapless::Vec<u32, 16>` to save the 60 B zero-filled tail on
// common 0-3-param DescribeStatement replies. Rejected under
// closer analysis:
// 1. **Copy cascade break.** `ParamOids: Copy` flows through
//    `DescribeStatementCompletePayload` → `Reply` → `Action`. A
//    heapless::Vec-backed ParamOids loses Copy; cascading Copy
//    removal would ripple through the entire Action enum and
//    require the DEF-184 A2/B1/B8 ManuallyDrop workaround at 3+
//    more sites. Net code complexity > 60 B saved.
// 2. **Hot-path not exercised.** Grep confirms `ParamOids::eq`
//    is never called in hot paths; tests use `.oids()` slice view
//    + `.len()`. The SIMD-wide Eq doc claim is future-proofing,
//    not active optimisation.
// 3. **Size win marginal.** DescribeStatementComplete fires once
//    per Parse round-trip — not per-row. 60 B × N describes is
//    negligible vs OutActions 5 KB × per-feed_bytes (already
//    addressed by A2/B1/B8).
// 4. **Audit #2 self-flagged uncertainty** ("conflicts with A16?").
//    Audit #1 A16 CONFIRMED-DONE after analysis; B11 adds no new
//    argument, just an alternative that breaks Copy.
//
// CREDO §11 closure: (a) already closed by A16-class equivalent
// (POD with justified full-array Eq). NOT skipped per §5 — actively
// rejected with written analysis.
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
