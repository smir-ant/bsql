//! Sealed, bounded read buffer.
//!
//! `ReadBuf` wraps `heapless::Vec<u8, READ_BUF_CAP>` and exposes only
//! four operations: [`append`], [`unread`], [`advance`], [`clear`].
//! Methods that could panic on misuse (`insert`, `resize`, `drain`,
//! indexing) are physically absent from the API. This makes the
//! "buffer-OOB" class of bugs **STRUCTURALLY UNREACHABLE** (reforge.md
//! §3.2 / §16).
//!
//! The two error paths exposed:
//!
//! - [`append`] returns [`ReadBufFull`] when the bounded capacity is
//!   exceeded. The caller (the protocol's `feed_bytes`) classifies this
//!   as a fatal connection error and emits `CloseSocket`.
//! - [`advance`] returns [`AdvancePastEnd`] when asked to advance beyond
//!   the unread region. Inside the protocol the dispatcher only calls
//!   `advance(header.total_len)` after `parse_header` has confirmed the
//!   bytes are present, so this Err is dead in our code paths — but the
//!   public signature forces any future caller to handle it. Tier-1
//!   against silent corruption.
//!
//! [`append`]: ReadBuf::append
//! [`unread`]: ReadBuf::unread
//! [`advance`]: ReadBuf::advance
//! [`clear`]: ReadBuf::clear

use crate::frame::READ_BUF_CAP;
use core::fmt;
// `PhantomData` used only by the Phase B2 branded types below,
// all of which are `#[cfg(test)]`-gated until Phase B3/B4 wire
// them into production. Gate the import to match.
#[cfg(test)]
use core::marker::PhantomData;
use heapless::{CapacityError, Vec};

// DEF-120 drift guard: `ReadBuf::cursor` (below) is `u16`. The
// type is sound only while `READ_BUF_CAP` fits a u16. A future
// capacity bump that breaks this invariant must also widen the
// cursor type. Hard `65_535` literal (not `u16::MAX as usize`) —
// `as` casts are banned by the crate forbid-bundle.
const _: () = assert!(
    READ_BUF_CAP <= 65_535,
    "READ_BUF_CAP must fit ReadBuf::cursor (u16). Widen both together.",
);

/// Bounded byte buffer for inbound wire data.
///
/// Capacity is the const [`READ_BUF_CAP`] (4096 in Phase 1a; tunable
/// later). Beyond capacity, [`append`] returns [`ReadBufFull`] — the
/// protocol classifies this as a fatal connection error.
///
/// `Default` is the empty buffer.
///
/// [`append`]: ReadBuf::append
#[derive(Default)]
pub struct ReadBuf {
    /// Backing storage. Private — every public method preserves the
    /// invariants below.
    inner: Vec<u8, READ_BUF_CAP>,
    /// Read cursor. Bytes in `inner[..cursor]` are consumed and may be
    /// reclaimed on the next [`compact`] call.
    ///
    /// Invariant: `cursor <= inner.len() <= READ_BUF_CAP <= 65_535`
    /// (enforced by every mutator path + the const assert above).
    ///
    /// DEF-120: `u16` (not `usize`) — `READ_BUF_CAP = 4096` fits
    /// with headroom; narrower type saves 6 bytes per `ReadBuf`
    /// on 64-bit and propagates nothing into hot arithmetic (the
    /// few widenings to `usize` use `usize::from`, infallible).
    cursor: u16,
}

impl ReadBuf {
    /// Construct an empty buffer.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: Vec::new(),
            cursor: 0,
        }
    }

    /// Append `bytes` to the unread region.
    ///
    /// Returns [`ReadBufFull`] if the resulting length would exceed
    /// [`READ_BUF_CAP`] even after reclaiming the consumed prefix.
    ///
    /// **Lazy compaction (DEF-058).** We try to fit the incoming bytes
    /// into the tail first — `heapless::Vec::extend_from_slice` checks
    /// capacity before copying and returns `Err` without mutation, so
    /// we can safely retry after compacting. On the typical workload
    /// (8 KiB chunks on a 4 KiB buffer where previous frames have been
    /// consumed) this saves one `memmove` per `append` call whenever
    /// the tail already has room. Only when the tail is insufficient
    /// do we reclaim `[0..cursor)` and try again.
    #[inline]
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // Fast path: tail has room, no need to compact.
        if self.inner.extend_from_slice(bytes).is_ok() {
            return Ok(());
        }
        // Slow path: reclaim the consumed prefix and retry. If it
        // still does not fit, classify as a fatal buffer-full error.
        self.compact();
        self.inner
            .extend_from_slice(bytes)
            .map_err(|CapacityError { .. }| ReadBufFull {
                attempted: bytes.len(),
                available: READ_BUF_CAP.saturating_sub(self.inner.len()),
            })
    }

    /// Borrow the unread region.
    ///
    /// The returned slice is valid until the next `&mut self` method
    /// call on this buffer (`append`, `advance`, `clear`). The borrow
    /// checker enforces this — there is no way for the caller to
    /// invalidate the slice while still holding it.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        // SAFETY-style note (no `unsafe` involved): `self.cursor` is
        // maintained `<= self.inner.len()` by every mutator, so the
        // slice expression cannot index out of bounds. We use `get`
        // rather than `[..]` because the forbid-bundle bans
        // `indexing_slicing`. DEF-120: `u16 → usize` via infallible
        // widening `From` impl (no `as` cast).
        //
        // F-014 (pass-#8): debug-builds actively assert the invariant
        // `cursor <= inner.len()` so a mutator regression would fail
        // the test suite before the dead unwrap_or(&[]) fallback
        // masked it silently.
        debug_assert!(
            usize::from(self.cursor) <= self.inner.len(),
            "ReadBuf invariant: cursor ({}) must not exceed inner.len() ({})",
            self.cursor,
            self.inner.len(),
        );
        self.inner.get(usize::from(self.cursor)..).unwrap_or(&[])
    }

    /// Borrow the full populated region, including bytes already
    /// advanced past the cursor. Used by the `StreamRow` materialiser
    /// (1c-1b) which needs absolute-position slices into rows whose
    /// frames were advanced-past during the dispatch loop but whose
    /// bytes must remain valid until `OutActions` drops.
    ///
    /// # Lifetime invariant
    ///
    /// The returned slice is valid until the next `&mut self` method
    /// call on this buffer (`append`, `advance`, `clear`) — same as
    /// [`unread`]. Compaction happens lazily on the next `append`;
    /// by then no outstanding borrow can be alive (the borrow
    /// checker refuses the `&mut` call otherwise). Callers emit
    /// [`crate::action::Action::StreamRow`] with slices carved out
    /// of this region during [`crate::PgProtocol::feed_bytes`]; the
    /// `'r` lifetime on `OutActions<'w, 'r>` ties those slices back
    /// to the `&'r mut self` borrow on `PgProtocol`, which blocks
    /// the next `feed_bytes` call while they are alive.
    ///
    /// [`unread`]: ReadBuf::unread
    ///
    /// F-016 (pass-#8): visibility narrowed `pub` → `pub(crate)`.
    /// Only `materialise` / dispatch resolution need this view; an
    /// external caller reading `populated()` gets access to bytes
    /// already consumed past the cursor with no user benefit. Surface
    /// shrink closes a latent access hole.
    #[inline]
    #[must_use]
    pub(crate) fn populated(&self) -> &[u8] {
        self.inner.as_slice()
    }

    /// Absolute position of the read cursor, in bytes from the start
    /// of [`populated`]. Used by the dispatch loop to compute absolute
    /// row-range coordinates (1c-1b).
    ///
    /// [`populated`]: ReadBuf::populated
    ///
    /// F-017 (pass-#8): visibility narrowed `pub` → `pub(crate)`.
    /// Only the dispatch layer needs this. Same rationale as
    /// [`populated`].
    #[inline]
    #[must_use]
    pub(crate) fn cursor_position(&self) -> usize {
        usize::from(self.cursor)
    }

    /// Advance the read cursor by `n` bytes.
    ///
    /// Returns [`AdvancePastEnd`] if `n` exceeds the unread length.
    /// In the dispatcher this Err is dead (we only advance after
    /// `parse_header` confirmed the bytes are present); exposing it
    /// publicly is a tier-1 belt-and-braces.
    #[inline]
    pub fn advance(&mut self, n: usize) -> Result<(), AdvancePastEnd> {
        let available = self.unread_len();
        if n > available {
            return Err(AdvancePastEnd {
                requested: n,
                available,
            });
        }
        // DEF-120: cursor is `u16`. Widen to usize for the
        // add-check, then narrow via `u16::try_from`. Both steps
        // preserved for forbid-bundle safety (no `as`). Arithmetic
        // is architecturally bounded: cursor + n <= inner.len() <=
        // READ_BUF_CAP <= 65_535, so the `try_from` Err branch is
        // dead — kept as belt-and-braces.
        //
        // # LLVM codegen
        //
        // Under `opt-level >= 1` LLVM propagates the `n <= available`
        // bound through the checked_add and folds the `u16::try_from`
        // Err arm out of the emitted code entirely. Release builds
        // carry ZERO instructions for the Err path — it's purely a
        // type-level match-exhaustion concern. Verified by pass-#8
        // audit (F-015). Do NOT replace with an `unsafe` cast.
        let new_cursor_usize = usize::from(self.cursor).checked_add(n).ok_or(AdvancePastEnd {
            requested: n,
            available,
        })?;
        let new_cursor = u16::try_from(new_cursor_usize).map_err(|_| AdvancePastEnd {
            requested: n,
            available,
        })?;
        self.cursor = new_cursor;
        Ok(())
    }

    /// Reset the buffer to empty.
    ///
    /// Used on connection teardown / errored state transitions.
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.cursor = 0;
    }

    /// Number of bytes currently unread.
    #[inline]
    #[must_use]
    pub fn unread_len(&self) -> usize {
        // DEF-120: cursor is u16; widen via `usize::from` for the
        // subtraction (infallible, no `as`).
        self.inner.len().saturating_sub(usize::from(self.cursor))
    }

    /// Reclaim the consumed prefix `[0..cursor)`.
    ///
    /// Internal helper called from [`append`]. Cheap when `cursor == 0`
    /// (no-op); otherwise a `copy_within` of the unread tail.
    fn compact(&mut self) {
        if self.cursor == 0 {
            return;
        }
        // DEF-120: cursor is u16; widen once for all uses.
        let cursor = usize::from(self.cursor);
        let len = self.inner.len();
        // `cursor <= len` invariant; subtraction safe.
        let unread_len = len.saturating_sub(cursor);
        // `copy_within` accepts a Range; the source range is
        // `cursor..len` and dest is `0`. Both inside `len`.
        self.inner.copy_within(cursor..len, 0);
        // truncate to the new (compacted) length; `Vec::truncate`
        // never panics, only shortens.
        self.inner.truncate(unread_len);
        self.cursor = 0;
    }
}

impl fmt::Debug for ReadBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadBuf")
            .field("unread_len", &self.unread_len())
            .field("cap", &READ_BUF_CAP)
            .finish()
    }
}

/// Returned by [`ReadBuf::append`] when the bounded buffer cannot
/// accept the inbound bytes.
///
/// The protocol treats this as a fatal connection error: emits
/// `CloseSocket` and fails the in-flight reply (if any). Pool
/// discards the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBufFull {
    /// How many bytes the caller tried to append.
    pub attempted: usize,
    /// How much room was actually available in the buffer.
    pub available: usize,
}

impl fmt::Display for ReadBufFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read buffer full: tried to append {} bytes, only {} available (cap {})",
            self.attempted, self.available, READ_BUF_CAP,
        )
    }
}

/// Returned by [`ReadBuf::advance`] when asked to advance beyond the
/// unread region.
///
/// In bsql-pg-proto's own dispatcher this Err is unreachable (we only
/// advance after `parse_header` has confirmed the bytes), but the
/// public signature forces every future caller to handle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvancePastEnd {
    /// How far the caller asked to advance.
    pub requested: usize,
    /// How much was actually unread.
    pub available: usize,
}

impl fmt::Display for AdvancePastEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "advance past end: requested {} bytes, only {} unread",
            self.requested, self.available,
        )
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (B) Phase B2 — generatively-branded read-buffer scaffolding
// ═════════════════════════════════════════════════════════════════════
//
// Symmetric partner to Phase B1's [`crate::write_buf::BrandedWriteBuf`].
// The read buffer needs its own brand because:
//   - `materialise()` (Phase B4) consumes BOTH write-side ranges
//     (for `Action::SendBytes`) AND read-side ranges (for
//     `Action::StreamRow`). Each carries a different brand.
//   - Using ONE shared brand for both would let a write-side range
//     accidentally apply to the read buffer (same kind-confusion
//     seam that Candidate C alone — plain typed newtypes without
//     brand generativity — cannot close).
//
// The pattern mirrors write_buf.rs exactly:
//   - [`BrandedReadBuf<'brand, 'a>`]     — invariant-branded
//     shared borrow of [`ReadBuf`].
//   - [`ReadBuf::with_branded`]           — HRTB generative
//     constructor: `for<'brand> FnOnce(BrandedReadBuf<'brand, '_>) -> R`.
//   - [`BrandedReadBuf::populated_branded`] — branded view of
//     `populated()` (the full populated region including consumed
//     prefix — used by `StreamRowRange` materialise).
//   - [`BrandedReadBuf::unread_branded`]  — branded view of
//     `unread()` (the unconsumed suffix — used by payload-extract
//     in `feed_bytes`).
//
// # `#[cfg(test)]` gating rationale — same as Phase B1
//
// Branded types have no production callers in B2. Production code
// still reads `populated()` / `unread()` as unbranded slices. Phase
// B3/B4 removes the cfg when `ReadRange<'brand>::apply` requires
// `BrandedBytes<'brand, '_>` inputs.
//
// # Shared `BrandedBytes` type
//
// Both write and read paths return `BrandedBytes<'brand, 'a>` from
// their respective `_branded()` methods. The type itself lives in
// `write_buf.rs` (Phase B1 introduced it there); the shared
// re-export lets `ReadRange<'brand>::apply` (Phase B3) accept
// bytes from either source homogeneously — the brand on each
// concrete instance is disjoint from every other scope's brand,
// so cross-kind mixups remain compile errors.
//
// A Phase B3 follow-up will likely relocate `BrandedBytes` to a
// dedicated `src/brand.rs` module (co-located with `BrandedRange`,
// `WriteRange`, `ReadRange`). Keeping it in write_buf.rs for now
// avoids a churn-only rename in the B2 commit.

/// Generatively-branded shared borrow of a [`ReadBuf`].
///
/// Constructed via [`ReadBuf::with_branded`]. Inside the closure,
/// `'brand` is fresh and unique — it cannot be unified with any
/// brand outside the closure. [`Self::populated_branded`] and
/// [`Self::unread_branded`] yield
/// [`crate::write_buf::BrandedBytes<'brand, '_>`] slices tied to
/// this brand; ranges built against one brand cannot apply to
/// bytes of another.
///
/// Note: the read buffer is borrowed **immutably** — dispatch
/// processes inbound bytes via `self.read_buf.unread()` in
/// `feed_bytes`, and the brand here tracks that read-only scope.
/// Phase B4 entry-point closure handles the mutable-append and
/// mutable-advance paths OUTSIDE the brand (they happen before
/// and after the branded materialise phase respectively).
#[cfg(test)]
pub(crate) struct BrandedReadBuf<'brand, 'a> {
    /// Underlying shared borrow. Phase B4 materialise consumes
    /// the branded view via [`Self::populated_branded`] inside
    /// the `ReadBuf::with_branded` closure; the brand keeps
    /// `ReadRange<'brand>::apply` (Phase B3) infallible.
    buf: &'a ReadBuf,
    /// Invariant phantom — see `write_buf.rs` Phase B1 block
    /// comment for the variance reasoning.
    _brand: PhantomData<fn(&'brand ()) -> &'brand ()>,
}

#[cfg(test)]
impl<'brand> BrandedReadBuf<'brand, '_> {
    /// Branded view of the full populated region (including bytes
    /// consumed past the cursor). Matches [`ReadBuf::populated`]
    /// in semantics.
    ///
    /// Used by `StreamRowRange` materialise (Phase B4): the row's
    /// absolute-position range was built during dispatch against
    /// this same populated region; the brand-identity proof makes
    /// `ReadRange<'brand>::apply(bytes)` return `&[u8]` instead
    /// of `Option<&[u8]>`.
    #[inline]
    #[must_use]
    pub(crate) fn populated_branded(&self) -> crate::write_buf::BrandedBytes<'brand, '_> {
        // NB: the construction goes through the shared Phase B1
        // `BrandedBytes` type. A later refactor may extract both
        // sides' `BrandedBytes` and branded views into a dedicated
        // `crate::brand` module; for now the factory is on
        // `BrandedWriteBuf` and on `BrandedReadBuf` independently,
        // both producing compatible values (the brand is existential
        // per HRTB, so call-site identity is preserved).
        crate::write_buf::BrandedBytes::from_slice_branded(self.buf.populated())
    }

    /// Branded view of the unread (unconsumed) region. Matches
    /// [`ReadBuf::unread`] in semantics.
    ///
    /// Used by the payload-extract site in `feed_bytes` (Phase B4):
    /// after `parse_header` succeeds, the payload slice
    /// `unread()[HEADER_LEN..total_len]` is carved — branded, so
    /// any future typed range into this view carries the same
    /// brand and the extraction becomes infallible.
    #[inline]
    #[must_use]
    pub(crate) fn unread_branded(&self) -> crate::write_buf::BrandedBytes<'brand, '_> {
        crate::write_buf::BrandedBytes::from_slice_branded(self.buf.unread())
    }
}

#[cfg(test)]
impl fmt::Debug for BrandedReadBuf<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrandedReadBuf")
            .field("unread_len", &self.buf.unread_len())
            .finish()
    }
}

#[cfg(test)]
impl ReadBuf {
    /// Enter a generatively-branded scope.
    ///
    /// HRTB semantics as in [`crate::write_buf::WriteBuf::with_branded`]:
    /// `F: for<'brand> FnOnce(BrandedReadBuf<'brand, '_>) -> R`
    /// ensures every call produces a fresh, disjoint `'brand` that
    /// the invariant phantom prevents from unifying with any other
    /// scope.
    ///
    /// # Two-brand materialise (Phase B4 call site — illustrative)
    ///
    /// ```ignore
    /// self.write_buf.with_branded(|wb| {
    ///     self.read_buf.with_branded(|rb| {
    ///         // wb: BrandedWriteBuf<'wbrand, '_>
    ///         // rb: BrandedReadBuf<'rbrand, '_>
    ///         // 'wbrand and 'rbrand are DISJOINT — a write range
    ///         // cannot apply to read bytes and vice versa.
    ///         materialise(
    ///             staged,
    ///             wb.as_bytes_branded(),
    ///             rb.populated_branded(),
    ///             arena,
    ///         )
    ///     })
    /// })
    /// ```
    #[inline]
    pub(crate) fn with_branded<R, F>(&self, f: F) -> R
    where
        F: for<'brand> FnOnce(BrandedReadBuf<'brand, '_>) -> R,
    {
        f(BrandedReadBuf {
            buf: self,
            _brand: PhantomData,
        })
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (B) Phase B2 — tests
// ═════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod phase_b2_tests {
    //! Phase B2 behavioural + structural pins.
    //!
    //! Mirror of Phase B1's tests on the read side.
    use super::*;

    /// B2-1: `with_branded` round-trip — construct a ReadBuf, brand
    /// it, discharge via `populated_branded().as_slice()`, observe
    /// the slice unchanged. Pins the read-side generative constructor
    /// and the branded-slice unbranding boundary.
    #[test]
    fn read_with_branded_round_trip_empty() {
        let buf = ReadBuf::new();
        let (populated_len, slice_len) = buf.with_branded(|rb| {
            let branded = rb.populated_branded();
            (branded.len(), branded.as_slice().len())
        });
        assert_eq!(populated_len, 0, "fresh read buffer must branded-view as empty");
        assert_eq!(slice_len, 0, "unbranded slice len must match branded len");
    }

    /// B2-2: `unread_branded` mirrors the unbranded `unread()` on
    /// a buffer with some appended bytes. Pins the lazy-compact
    /// invariant through the branded view.
    #[test]
    fn read_branded_unread_matches_raw_unread() {
        let mut buf = ReadBuf::new();
        let result = buf.append(&[1, 2, 3, 4]);
        assert!(result.is_ok(), "append must succeed on fresh buffer");
        let (branded_len, raw_len) = {
            let raw_len = buf.unread().len();
            let branded_len = buf.with_branded(|rb| rb.unread_branded().len());
            (branded_len, raw_len)
        };
        assert_eq!(branded_len, raw_len, "branded unread_len must match raw unread_len");
        assert_eq!(branded_len, 4);
    }

    /// B2-3: drift pin on sizes — branded wrappers carry phantom
    /// only, so the wrapper size must match `&ReadBuf`.
    #[test]
    fn branded_read_wrapper_size_is_phantom_only() {
        assert_eq!(
            core::mem::size_of::<BrandedReadBuf<'_, '_>>(),
            core::mem::size_of::<&ReadBuf>(),
            "BrandedReadBuf must be &ReadBuf-sized (phantom is ZST).",
        );
    }

    /// B2-4: two nested `with_branded` scopes — one write, one
    /// read — produce DISJOINT brands. This pin protects the
    /// Phase B4 materialise shape: if the write-brand and
    /// read-brand were unifiable, a WriteRange could accidentally
    /// apply to read bytes. Runtime test cannot directly observe
    /// brand distinctness (it's a type-system fact); here we
    /// exercise the nested-scope shape to confirm the call
    /// pattern type-checks. A future trybuild harness adds a
    /// compile_fail case for the negative.
    #[test]
    fn nested_write_read_branded_scopes_type_check() {
        use crate::write_buf::WriteBuf;
        let mut wbuf = WriteBuf::new();
        let rbuf = ReadBuf::new();
        // If 'wbrand and 'rbrand accidentally unified, the closure
        // body's call to `populated_branded()` returning a
        // `BrandedBytes<'rbrand, '_>` could be used where a
        // `BrandedBytes<'wbrand, '_>` is expected — compile error.
        // We simply exercise the nesting pattern to confirm it
        // type-checks; runtime observation is via the paired
        // lengths (both zero on fresh bufs).
        let (wlen, rlen) = wbuf.with_branded(|wb| {
            rbuf.with_branded(|rb| (wb.as_bytes_branded().len(), rb.populated_branded().len()))
        });
        assert_eq!(wlen, 0);
        assert_eq!(rlen, 0);
    }
}
