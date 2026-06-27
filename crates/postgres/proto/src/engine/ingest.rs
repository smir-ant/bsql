//! Single-residence inbound ingest buffer for the session engine.
//!
//! # The single-residence property
//!
//! The existing read path is *copy-in*: the driver reads a TCP chunk into
//! some scratch buffer, then `ReadBuf::append(bytes)` copies those bytes a
//! second time into the protocol's own storage (`extend_from_slice` =
//! one memcpy per chunk). The inbound bytes have **two** residences (the
//! scratch buffer and the read buffer) and the copy bridges them.
//!
//! [`IngestBuf`] removes the copy. [`read_slot`](IngestBuf::read_slot)
//! lends the *destination* — a writable tail slice of the buffer's own
//! storage — and the socket reads straight into it.
//! [`commit`](IngestBuf::commit) then advances a watermark to publish the
//! bytes the socket actually wrote. The inbound bytes have **one**
//! residence (this buffer); there is no scratch buffer and no copy-in.
//!
//! # Why zero-once + watermark (and not the three alternatives)
//!
//! Lending a writable tail without `unsafe` is the load-bearing
//! constraint. Three shapes were weighed:
//!
//! 1. **Zero-once-at-construction + a `filled` watermark** — *chosen.* The
//!    backing storage is a plain `[u8; N]` array, fully initialised by the
//!    single `[0u8; N]` construction (one zero-fill for the array's whole
//!    lifetime). Every byte is therefore a valid `u8` forever, so lending
//!    `active[filled..]` as `&mut [u8]` is safe with no `unsafe`, no
//!    `MaybeUninit`, and — crucially — **no per-read zero-fill**. The
//!    `filled` watermark records how much of the always-initialised array
//!    is logically populated; `cursor` records how much has been consumed.
//!    Steady-state ingest performs zero allocations and zero memsets.
//!
//! Each half of that "zero allocations and zero memsets" guarantee names a
//! real gate (a claim that cannot point at its enforcement is drift): the
//! zero-allocation half is measured by the counting-allocator bench in the
//! `engine_ingest_alloc` integration test, and the zero-memset half — which
//! a counting allocator is structurally blind to, since a `fill`/`resize`
//! over already-owned storage allocates nothing — is enforced by the static
//! source-scan in the `engine_ingest_memset_guard` integration test, which
//! fails if any memset-family call reappears in `read_slot` / `commit` /
//! `next_event`.
//! 2. **Per-call `heapless::Vec::resize_default`** — `heapless::Vec`
//!    cannot lend its uninitialised spare capacity as `&mut [u8]` without
//!    `unsafe` (`spare_capacity_mut` yields `&mut [MaybeUninit<u8>]`, and
//!    committing it needs the `unsafe` `set_len`). The safe escape is to
//!    `resize_default` to grow by `want` zero-filled bytes and lend the
//!    now-initialised tail — but that is an O(`want`) **memset on every
//!    read**, a recurring per-read zero-fill. Rejected: it reintroduces
//!    exactly the recurring cost the watermark exists to remove.
//! 3. **Audited `unsafe` `set_len`** — lend `spare_capacity_mut()` and
//!    `set_len` after the socket writes. Zero memset, zero copy, but it
//!    requires `unsafe`. This crate is `#![forbid(unsafe_code)]` and is a
//!    shipped artifact; relocating the primitive to a `publish = false`
//!    helper does not help, because the shipped engine would still have to
//!    call it. Rejected: the zero-once array delivers the same
//!    no-memset/no-copy outcome while staying inside the forbid wall.
//!
//! # The no-escape wall (E0499)
//!
//! [`read_slot`](IngestBuf::read_slot) returns `&mut [u8]` borrowed from
//! `&mut self`, and [`next_event`](IngestBuf::next_event) returns an
//! [`Event`] borrowing `&mut self`. Holding either across the next
//! mutating call (`read_slot` / `commit` / `next_event`) is a borrow
//! conflict the compiler rejects with E0499 — a lent slot or a
//! borrow-through event cannot outlive the next mutation, for free, on
//! stable. Compaction can therefore relocate the live bytes (the two-tier
//! escape, or the consumed-prefix reclaim) without ever invalidating an
//! outstanding borrow: by the time a mutating call runs, no borrow from a
//! prior one is alive.

use crate::frame::{parse_header, HeaderParse, HEADER_LEN, READ_BUF_CAP};
use crate::narrow::usize_from_u32;
use alloc::boxed::Box;
use core::fmt;

use super::Event;

/// Inline-tier capacity. Frames whose live span fits in this many bytes
/// stay in stack-inline storage with full cache locality (the array is a
/// field of the buffer, no pointer chase). A wanted total that would
/// exceed it triggers the one-time escape to the heap tier.
///
/// Chosen to match the established inbound small-frame envelope: the
/// fixed-size control frames (`Sync`, `ReadyForQuery`, `ParseComplete`,
/// `BindComplete`, …) and typical narrow `DataRow`s all fit, so the common
/// OLTP path never escapes.
const INGEST_INLINE_CAP: usize = 128;

/// Single-residence two-tier inbound ingest buffer.
///
/// Exactly one of the two tiers is *active* at any moment:
///
/// - **Inline** (`heap == None`): the live bytes are `inline[cursor..filled]`.
/// - **Heap** (`heap == Some`): the live bytes are `heap[cursor..filled]`;
///   `inline` holds only a stale pre-escape copy (scrubbed on `Drop`).
///
/// Both arrays are fully initialised for their whole lifetime — `inline`
/// by its `[0u8; _]` construction, the heap array by its `[0u8; _]`
/// allocation at escape — which is what lets `read_slot` lend an
/// already-initialised `&mut [u8]` tail with no `unsafe` and no per-read
/// zero-fill.
///
/// # Invariants
///
/// - `cursor <= filled <= active_cap <= u16::MAX`, where `active_cap` is
///   [`INGEST_INLINE_CAP`] in the inline tier and [`READ_BUF_CAP`] in the
///   heap tier. The `u16` ceiling is asserted at construction.
/// - The active tier's `[..filled]` prefix is the populated region;
///   `[cursor..filled]` is the unread region; `[..cursor]` is consumed and
///   reclaimable by compaction.
pub struct IngestBuf {
    /// Inline tier storage. Always present; zeroed once at construction.
    inline: [u8; INGEST_INLINE_CAP],
    /// Heap tier storage. `None` until the first [`read_slot`] whose
    /// wanted total exceeds the inline tier; `Some` thereafter (the buffer
    /// stays in the heap tier for its remaining lifetime — downgrading
    /// would only add a copy-back with no benefit).
    ///
    /// [`read_slot`]: IngestBuf::read_slot
    heap: Option<Box<[u8; READ_BUF_CAP]>>,
    /// Read cursor: bytes in `active[..cursor]` are consumed.
    cursor: u16,
    /// Fill watermark: bytes in `active[..filled]` are populated. The
    /// spare region `active[filled..]` is initialised (the array is always
    /// fully initialised) but not yet logically written.
    filled: u16,
}

impl Default for IngestBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl IngestBuf {
    /// Construct an empty buffer in the inline tier.
    ///
    /// The single `[0u8; INGEST_INLINE_CAP]` here is the *only* inline-tier
    /// zero-fill for the buffer's whole life (the heap tier's single
    /// zero-fill happens once at escape). No method below ever zero-fills
    /// again — the watermark, not re-initialisation, tracks what is
    /// populated.
    ///
    /// `const fn`: caller-side construction allocates nothing (`heap` is
    /// `None`).
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                READ_BUF_CAP <= 65_535 && INGEST_INLINE_CAP <= 65_535,
                "IngestBuf: both tier capacities must be <= u16::MAX (cursor \
                 and filled are u16). Widen those fields before raising a cap \
                 past 65_535.",
            );
            assert!(
                INGEST_INLINE_CAP <= READ_BUF_CAP,
                "IngestBuf: the inline tier must not exceed the heap tier — \
                 the escape copies the live inline prefix into the heap array.",
            );
        }
        Self {
            inline: [0u8; INGEST_INLINE_CAP],
            heap: None,
            cursor: 0,
            filled: 0,
        }
    }

    /// Lend a writable tail slice the socket reads directly into.
    ///
    /// Single-residence ingest, lend side. The returned `&mut [u8]` is part
    /// of this buffer's own storage, so a `socket.read(slot)` writes the
    /// inbound bytes into their final residence with no copy-in. Pair every
    /// `read_slot` with a [`commit`](Self::commit) of the count the socket
    /// reported.
    ///
    /// Drives the two-tier escape **before** lending: if the wanted total
    /// (`filled + want`) would exceed the inline tier and the buffer is
    /// still inline, the live bytes are moved into a freshly-allocated,
    /// zeroed heap array first, so the lent slot — and therefore the
    /// socket's bytes — land in the post-escape storage deterministically
    /// from `want`.
    ///
    /// Returns [`IngestFull`] when no room remains even after reclaiming the
    /// consumed prefix and escaping to the heap tier (a wire frame larger
    /// than the bounded buffer). The caller classifies this as a fatal
    /// connection error.
    pub fn read_slot(&mut self, want: usize) -> Result<&mut [u8], IngestFull> {
        // Reclaim the consumed prefix so the tail offers maximal room in
        // the single residence. No outstanding borrow can be alive here:
        // the E0499 wall blocks calling this while a prior lent slot or
        // borrow-through event is held.
        self.compact();

        let filled = usize::from(self.filled);

        // Two-tier escape BEFORE lending: a wanted total beyond the inline
        // tier moves the live prefix into the heap array now, so the lend
        // below carves out of the final storage.
        if self.heap.is_none() && filled.saturating_add(want) > INGEST_INLINE_CAP {
            self.escape();
        }

        let cap = self.active_cap();
        let room = cap.saturating_sub(filled);
        let grow = want.min(room);
        if grow == 0 {
            core::hint::cold_path();
            return Err(IngestFull {
                attempted: want,
                available: room,
                cap,
            });
        }
        let end = filled.saturating_add(grow);
        // `filled..end` is within `active_cap` by the `grow = want.min(room)`
        // clamp; the `unwrap_or(&mut [])` arm is therefore dead and exists
        // only to satisfy the no-indexing forbid wall.
        Ok(self.active_mut().get_mut(filled..end).unwrap_or(&mut []))
    }

    /// Publish the `n` bytes the socket wrote into the slot from the most
    /// recent [`read_slot`](Self::read_slot), advancing the fill watermark.
    ///
    /// Single-residence ingest, commit side. `n` is the count the socket
    /// reported writing (`n <= slot.len()` by the read contract). The bytes
    /// `active[filled..filled + n]` join the unread region; no copy and no
    /// truncation — only the watermark moves.
    ///
    /// Returns [`IngestCommitOverflow`] if `n` would push the watermark past
    /// the active capacity (a caller that committed more than it was lent).
    pub fn commit(&mut self, n: usize) -> Result<(), IngestCommitOverflow> {
        let filled = usize::from(self.filled);
        let cap = self.active_cap();
        let available = cap.saturating_sub(filled);
        if n > available {
            core::hint::cold_path();
            return Err(IngestCommitOverflow {
                committed: n,
                available,
            });
        }
        // `filled + n <= cap <= u16::MAX`; the dead arms keep the
        // forbid-bundle happy without an `as` cast.
        let new_filled = filled.saturating_add(n);
        self.filled = u16::try_from(new_filled).unwrap_or(self.filled);
        Ok(())
    }

    /// Borrow the unread region in place.
    ///
    /// The returned slice is the live bytes `active[cursor..filled]`,
    /// borrowed straight out of the single residence. It is valid until the
    /// next `&mut self` call (`read_slot` / `commit` / `next_event`); the
    /// borrow checker forbids holding it across one.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        let cursor = usize::from(self.cursor);
        let filled = usize::from(self.filled);
        debug_assert!(
            cursor <= filled && filled <= self.active().len(),
            "IngestBuf invariant: cursor ({cursor}) <= filled ({filled}) <= \
             active cap ({})",
            self.active().len(),
        );
        // `cursor..filled` is within the active slice by the invariant; the
        // `unwrap_or(&[])` arm is dead.
        self.active().get(cursor..filled).unwrap_or(&[])
    }

    /// Number of unread bytes.
    #[inline]
    #[must_use]
    pub fn unread_len(&self) -> usize {
        usize::from(self.filled).saturating_sub(usize::from(self.cursor))
    }

    /// Borrow-through pull: lend one complete frame's payload in place.
    ///
    /// This is the ingest-layer borrow-through surface. It performs framing
    /// only — locate one complete frame (a 1-byte tag plus a big-endian,
    /// length-inclusive `u32` length field) in the unread region — and
    /// lending: advance the cursor past the frame and return its body as a
    /// borrow straight out of the buffer ([`Event::Row`], the variant whose
    /// contract is a single borrow of the read buffer). It deliberately does
    /// **not** classify the frame by tag onto the rest of the [`Event`]
    /// vocabulary; that dispatch composes in a later layer.
    ///
    /// Yields [`Event::NeedMore`] when fewer than a whole frame's bytes are
    /// buffered. The returned [`Event`] borrows `&mut self`, so holding it
    /// across the next mutating call is E0499 — the no-escape wall.
    ///
    /// # Framing-only: length VALIDATION is a recorded deferral, not a gap
    ///
    /// This surface checks frame *completeness* (is a whole frame buffered?),
    /// not the length field's *legality*. Two malformed length shapes are
    /// framed here without rejection, deliberately, because rejecting them
    /// belongs to the later tag-classification layer that gives each frame its
    /// meaning — and both shapes are already memory-safe and bounded:
    ///
    /// - A length field below the 4-byte self-count minimum produces an empty
    ///   [`Event::Row`] body and a short cursor advance (1..=4 bytes): the
    ///   body span collapses to an out-of-order range, which `get(..)` returns
    ///   as the empty slice — never an out-of-bounds read.
    /// - A length field above the bounded buffer's capacity produces perpetual
    ///   [`Event::NeedMore`]. That is bounded, not a hang: the caller's next
    ///   [`read_slot`](Self::read_slot) eventually returns [`IngestFull`] once
    ///   the buffer cannot grow, the fatal the engine treats as a connection
    ///   error.
    ///
    /// The min/max length-legality check composes in that later
    /// dispatch/classification layer; recording it here keeps the deferral
    /// explicit rather than silent.
    pub fn next_event(&mut self) -> Event<'_> {
        let cursor = usize::from(self.cursor);
        let filled = usize::from(self.filled);
        let unread = filled.saturating_sub(cursor);
        if unread < HEADER_LEN {
            return Event::NeedMore;
        }

        // Length field = the 4 bytes after the 1-byte tag, big-endian and
        // length-inclusive (it counts itself plus the body).
        let len_lo = cursor.saturating_add(1);
        let len_hi = cursor.saturating_add(HEADER_LEN);
        let arr: [u8; 4] = match self.active().get(len_lo..len_hi) {
            Some(bytes) => match bytes.try_into() {
                Ok(arr) => arr,
                // Dead: the slice is exactly 4 bytes (`HEADER_LEN - 1`).
                Err(_) => return Event::NeedMore,
            },
            // Dead: `unread >= HEADER_LEN` guarantees the slice exists.
            None => return Event::NeedMore,
        };
        let body_len = usize_from_u32(u32::from_be_bytes(arr));
        // Whole frame = 1 tag byte + the length-inclusive remainder.
        let total = body_len.saturating_add(1);
        if unread < total {
            return Event::NeedMore;
        }

        let body_start = cursor.saturating_add(HEADER_LEN);
        let body_end = cursor.saturating_add(total);
        // Advance the cursor past the whole frame. `total <= unread`, so
        // `cursor + total <= filled <= u16::MAX`; the dead arms avoid `as`.
        let new_cursor = match usize::from(self.cursor).checked_add(total) {
            Some(v) => v,
            None => return Event::NeedMore,
        };
        let new_cursor = match u16::try_from(new_cursor) {
            Ok(v) => v,
            Err(_) => return Event::NeedMore,
        };
        self.cursor = new_cursor;

        // Re-borrow the just-consumed body in place. The bytes are
        // physically resident until the next mutating call relocates them;
        // the returned borrow ties to `&'_ mut self`, so that call cannot
        // run while the event is held.
        let body = self.active().get(body_start..body_end).unwrap_or(&[]);
        Event::Row(body)
    }

    /// Locate and consume one complete frame, returning its 1-byte tag and
    /// the active-buffer offset range of its body (the bytes after the
    /// 5-byte header).
    ///
    /// This is the connecting-phase counterpart to
    /// [`next_event`](Self::next_event): it surfaces the wire tag byte
    /// (the connecting dispatch keys on it to classify each auth/startup
    /// frame) and a `(body_start, body_end)` range rather than a borrow, so
    /// the caller can dispatch (mutating other fields) and then re-borrow
    /// the body via [`frame_body`](Self::frame_body) without holding a read
    /// borrow across the mutation. The framing rules are identical to
    /// `next_event` — a 1-byte tag plus a big-endian, length-inclusive
    /// `u32` length field — with the same memory-safety/bounding properties
    /// (a sub-minimum length yields an out-of-order, hence empty, body
    /// range; an over-capacity length parks at perpetual `None`).
    ///
    /// The returned offsets are valid until the next compacting call
    /// ([`read_slot`](Self::read_slot)); pair with [`frame_body`] before any
    /// such call. Yields `None` when fewer than a whole frame is buffered.
    ///
    /// [`frame_body`]: Self::frame_body
    pub fn take_frame(&mut self) -> Option<(u8, usize, usize)> {
        let cursor = usize::from(self.cursor);
        let filled = usize::from(self.filled);
        let unread = filled.saturating_sub(cursor);
        if unread < HEADER_LEN {
            return None;
        }
        // The tag is the first byte of the frame at the read cursor.
        let tag = match self.active().get(cursor) {
            Some(&t) => t,
            // Dead: `unread >= HEADER_LEN >= 1` guarantees the byte exists.
            None => return None,
        };
        // Length field = the 4 bytes after the 1-byte tag, big-endian and
        // length-inclusive (it counts itself plus the body).
        let len_lo = cursor.saturating_add(1);
        let len_hi = cursor.saturating_add(HEADER_LEN);
        let arr: [u8; 4] = match self.active().get(len_lo..len_hi) {
            Some(bytes) => match bytes.try_into() {
                Ok(arr) => arr,
                // Dead: the slice is exactly 4 bytes (`HEADER_LEN - 1`).
                Err(_) => return None,
            },
            // Dead: `unread >= HEADER_LEN` guarantees the slice exists.
            None => return None,
        };
        let body_len = usize_from_u32(u32::from_be_bytes(arr));
        // Whole frame = 1 tag byte + the length-inclusive remainder.
        let total = body_len.saturating_add(1);
        if unread < total {
            return None;
        }
        let body_start = cursor.saturating_add(HEADER_LEN);
        let body_end = cursor.saturating_add(total);
        // Advance the cursor past the whole frame. `total <= unread`, so
        // `cursor + total <= filled <= u16::MAX`; the dead arms avoid `as`.
        let new_cursor = usize::from(self.cursor).checked_add(total)?;
        let new_cursor = u16::try_from(new_cursor).ok()?;
        self.cursor = new_cursor;
        Some((tag, body_start, body_end))
    }

    /// Borrow a frame body by the offset range returned from
    /// [`take_frame`](Self::take_frame).
    ///
    /// The bytes are physically resident until the next compacting call
    /// ([`read_slot`](Self::read_slot)) relocates them; the offsets are
    /// stable across the intervening dispatch (`commit` only advances a
    /// watermark, never compacts). An out-of-range pair returns the empty
    /// slice — never an out-of-bounds read.
    #[inline]
    #[must_use]
    pub fn frame_body(&self, start: usize, end: usize) -> &[u8] {
        self.active().get(start..end).unwrap_or(&[])
    }

    /// Inspect the leading header of the unread region without consuming.
    ///
    /// The framing counterpart to [`take_frame`](Self::take_frame) for the
    /// oversize path: a [`HeaderParse::FrameTooLarge`] verdict (a frame whose
    /// wire footprint exceeds [`READ_BUF_CAP`], so the whole frame can never
    /// reside in this bounded buffer) tells the caller to switch from
    /// whole-frame buffering to bounded-chunk streaming before any byte of the
    /// body is demanded. Pure inspection — no cursor movement.
    #[inline]
    #[must_use]
    pub fn peek_header(&self) -> HeaderParse {
        parse_header(self.unread())
    }

    /// The first unread byte — the frame tag — or `None` when the buffer is
    /// drained. Used by the oversize path to classify a [`HeaderParse::
    /// FrameTooLarge`] frame (whose verdict carries only the declared length)
    /// by its tag before streaming.
    #[inline]
    #[must_use]
    pub fn peek_tag(&self) -> Option<u8> {
        self.unread().first().copied()
    }

    /// Consume up to `max` unread bytes from the cursor, returning the
    /// active-buffer offset range of the bytes consumed for an in-place
    /// re-borrow via [`frame_body`](Self::frame_body).
    ///
    /// The bounded-chunk primitive the oversize streaming paths are built on:
    /// a frame larger than the buffer is drained `min(unread, max)` bytes at a
    /// time, each chunk re-borrowed in place (the bytes stay resident until the
    /// next compacting [`read_slot`](Self::read_slot)). Returns `None` when no
    /// bytes are unread. No allocation, no copy — only the cursor advances.
    #[inline]
    pub fn take_chunk(&mut self, max: usize) -> Option<(usize, usize)> {
        let cursor = usize::from(self.cursor);
        let take = self.unread_len().min(max);
        if take == 0 {
            return None;
        }
        let end = cursor.saturating_add(take);
        // `end <= filled <= cap <= u16::MAX`; the dead arm avoids an `as` cast.
        self.cursor = u16::try_from(end).unwrap_or(self.cursor);
        Some((cursor, end))
    }

    /// Active-tier capacity in bytes.
    #[inline]
    #[must_use]
    fn active_cap(&self) -> usize {
        match self.heap {
            None => INGEST_INLINE_CAP,
            Some(_) => READ_BUF_CAP,
        }
    }

    /// Shared view of the active tier's whole storage array.
    #[inline]
    #[must_use]
    fn active(&self) -> &[u8] {
        match &self.heap {
            None => self.inline.as_slice(),
            Some(heap) => heap.as_slice(),
        }
    }

    /// Mutable view of the active tier's whole storage array.
    #[inline]
    #[must_use]
    fn active_mut(&mut self) -> &mut [u8] {
        match &mut self.heap {
            None => self.inline.as_mut_slice(),
            Some(heap) => heap.as_mut_slice(),
        }
    }

    /// Reclaim the consumed prefix `[..cursor)` by moving the unread tail to
    /// the front of the active tier.
    ///
    /// No-op when `cursor == 0`. No zero-fill: the freeze mandates no
    /// recurring memset, so the abandoned bytes beyond the new watermark are
    /// left as-is (initialised, never returned — only `[cursor..filled]` is
    /// ever read) and are scrubbed wholesale on `Drop`.
    fn compact(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let cursor = usize::from(self.cursor);
        let filled = usize::from(self.filled);
        let unread = filled.saturating_sub(cursor);
        // `copy_within` moves `active[cursor..filled]` to `active[0..unread]`;
        // both ranges are within the active slice by the invariant.
        self.active_mut().copy_within(cursor..filled, 0);
        // `unread <= filled <= cap <= u16::MAX`; the dead arm avoids `as`.
        self.filled = u16::try_from(unread).unwrap_or(0);
        self.cursor = 0;
    }

    /// Move the live prefix into a freshly-allocated, zeroed heap array.
    ///
    /// Called once, from `read_slot`, before lending — so the socket's bytes
    /// land in the post-escape storage. No-op if already escaped. The heap
    /// array's `[0u8; _]` allocation is the heap tier's single zero-fill.
    fn escape(&mut self) {
        if self.heap.is_some() {
            return;
        }
        let filled = usize::from(self.filled);
        let mut heap: Box<[u8; READ_BUF_CAP]> = Box::new([0u8; READ_BUF_CAP]);
        // Copy the live prefix `inline[..filled]` into `heap[..filled]`.
        // Both slices are exactly `filled` bytes (or `None`, the dead arm),
        // so `copy_from_slice` cannot mismatch; `filled <= INGEST_INLINE_CAP
        // <= READ_BUF_CAP` by the construction asserts.
        if let (Some(src), Some(dst)) = (
            self.inline.as_slice().get(..filled),
            heap.as_mut_slice().get_mut(..filled),
        ) {
            dst.copy_from_slice(src);
        }
        self.heap = Some(heap);
        // `inline` now holds a stale copy of the moved bytes; it is no longer
        // the active tier and is scrubbed on `Drop`.
    }
}

/// Scrub both tiers once on teardown.
///
/// The buffer holds raw inbound wire bytes — SCRAM handshake material,
/// `ParameterStatus`/error payloads, and accumulated query result bytes.
/// `Drop` zeroizes the inline array (and the heap array, when escaped)
/// exactly once at teardown, so none of it survives in freed memory. This
/// is the only scrub on the buffer's lifetime; per-read scrubbing is
/// deliberately absent (it would be the recurring zero-fill the watermark
/// design exists to avoid).
impl Drop for IngestBuf {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.inline.as_mut_slice().zeroize();
        if let Some(heap) = &mut self.heap {
            heap.as_mut_slice().zeroize();
        }
    }
}

impl fmt::Debug for IngestBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tier = if self.heap.is_some() { "heap" } else { "inline" };
        f.debug_struct("IngestBuf")
            .field("tier", &tier)
            .field("unread_len", &self.unread_len())
            .field("inline_cap", &INGEST_INLINE_CAP)
            .field("heap_cap", &READ_BUF_CAP)
            .finish()
    }
}

/// Returned by [`IngestBuf::read_slot`] when no room remains for the wanted
/// bytes even after reclaiming the consumed prefix and escaping to the heap
/// tier — a wire frame larger than the bounded buffer. The engine treats
/// this as a fatal connection error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestFull {
    /// Bytes the caller asked to lend room for.
    pub attempted: usize,
    /// Bytes of room actually available in the active tier.
    pub available: usize,
    /// Active-tier capacity at the moment of the failure.
    pub cap: usize,
}

impl core::error::Error for IngestFull {}

impl fmt::Display for IngestFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ingest buffer full: wanted room for {} bytes, only {} available (cap {})",
            self.attempted, self.available, self.cap,
        )
    }
}

/// Returned by [`IngestBuf::commit`] when the committed count would push the
/// fill watermark past the active capacity — a caller that committed more
/// bytes than the slot it was lent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestCommitOverflow {
    /// Bytes the caller tried to commit.
    pub committed: usize,
    /// Bytes of room that were actually available to commit into.
    pub available: usize,
}

impl core::error::Error for IngestCommitOverflow {}

impl fmt::Display for IngestCommitOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ingest commit overflow: tried to commit {} bytes, only {} available",
            self.committed, self.available,
        )
    }
}

#[cfg(test)]
mod drop_witness_tests {
    //! Drop-fire witness for [`IngestBuf`] via the crate-internal
    //! [`crate::drop_witness::DropCounter`]. `DropCounter` /`DropProbe` are
    //! `pub(crate)`, so this witness lives in `src` (not an integration
    //! crate); behavioural coverage of the ingest API lives in the
    //! `engine_ingest_spec` integration test, which may use the
    //! panic-class test idioms the crate-root forbid wall bars here.

    use super::IngestBuf;
    use crate::drop_witness::{DropCounter, DropProbe};

    /// `IngestBuf::drop` fires its zeroize chain exactly once — the
    /// counterpart to its manifest registration in `drop_witness.rs`.
    #[test]
    fn ingest_buf_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let buf = IngestBuf::new();
        DropCounter::scoped(buf, probe.clone(), || {
            assert_eq!(probe.fired(), 0);
        });
        assert_eq!(probe.fired(), 1, "IngestBuf drop must fire exactly once");
    }
}
