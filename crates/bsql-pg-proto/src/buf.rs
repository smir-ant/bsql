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
    /// DEF-154 (G): u16 cursor accessor — storage type itself.
    /// Used by dispatch cursor math that wants to stay in u16 all
    /// the way through `AbsFrameStart::new(u16)` without ever
    /// widening to usize. Pre-(G) there was also a
    /// `cursor_position() -> usize` widening accessor, deleted
    /// because the only production callsite
    /// (`BrandedReadBuf::cursor_position_scope_local`) now returns
    /// u16, and no other callers remain.
    #[inline]
    #[must_use]
    pub(crate) const fn cursor_position_u16(&self) -> u16 {
        self.cursor
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

// DEF-154 (H): read-side `BrandedReadBuf<'brand, 'a>` + its
// `populated_branded` / `unread_branded` / `into_populated_branded`
// / `advance_scope_local` / `clear_scope_local` / `cursor_position_scope_local`
// methods + `ReadBuf::with_branded` HRTB entry — ALL DELETED.
//
// Read-side brand was introduced in (B) Phase B2 to prove
// buffer-identity for `ReadRange<'brand>::apply` bounds-safety.
// (H) deleted `ReadRange` entirely — `StagedAction::StreamRowRange`
// now carries `row_bytes: &'r [u8]` slices of populated() directly,
// with lifetime-enforced borrow safety (tier-1). The brand became
// dead scaffolding and is removed per user directive:
// "всё четко и однозначно должно быть; может что-то пригодится
// потом, а может что-то просто избыточно" — избыточно, deleted.

// DEF-154 (H): `BrandedReadBuf<'brand, 'a>` type + its impl +
// `ReadBuf::with_branded` method + `phase_b2_tests` module —
// ALL DELETED. See block comment above.
