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
    /// Invariant: `cursor <= inner.len()` (enforced by every mutator
    /// path; see the assertion comments in `advance`).
    cursor: usize,
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
    /// [`READ_BUF_CAP`]. Before writing, the buffer auto-compacts —
    /// reclaims the space `[0..cursor)` already consumed — so the cap
    /// applies to the *unread* region, not historical data.
    ///
    /// Compact-on-write is a deliberate choice over compact-on-read:
    /// the hot path is "read 8 KiB chunk → parse zero or more frames",
    /// where compaction once per chunk dominates compaction once per
    /// frame.
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        self.compact();
        // After compact, `inner.len() == self.unread_len()`. Capacity
        // headroom = READ_BUF_CAP - inner.len().
        // `extend_from_slice` returns Err(()) on overflow, no panic.
        // `extend_from_slice` returns `Err(CapacityError)` on overflow;
        // we carry no additional detail from the typed error (heapless
        // ships it as a marker). Reclassify as our bounded-full error.
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
        // `indexing_slicing`.
        self.inner.get(self.cursor..).unwrap_or(&[])
        // The `unwrap_or` is dead in practice (cursor <= len always),
        // but it lets us avoid `unwrap()` in production code while
        // still handling the case the type system cannot prove.
    }

    /// Advance the read cursor by `n` bytes.
    ///
    /// Returns [`AdvancePastEnd`] if `n` exceeds the unread length.
    /// In the dispatcher this Err is dead (we only advance after
    /// `parse_header` confirmed the bytes are present); exposing it
    /// publicly is a tier-1 belt-and-braces.
    pub fn advance(&mut self, n: usize) -> Result<(), AdvancePastEnd> {
        let available = self.unread_len();
        if n > available {
            return Err(AdvancePastEnd {
                requested: n,
                available,
            });
        }
        // checked_add cannot overflow: cursor + n <= cursor + available
        //   <= cursor + (inner.len() - cursor) == inner.len() <= READ_BUF_CAP.
        // We use checked_add to satisfy `arithmetic_side_effects`.
        let new_cursor = self.cursor.checked_add(n).ok_or(AdvancePastEnd {
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
        self.inner.len().saturating_sub(self.cursor)
    }

    /// Reclaim the consumed prefix `[0..cursor)`.
    ///
    /// Internal helper called from [`append`]. Cheap when `cursor == 0`
    /// (no-op); otherwise a `copy_within` of the unread tail.
    fn compact(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let len = self.inner.len();
        // `cursor <= len` invariant; subtraction safe.
        let unread_len = len.saturating_sub(self.cursor);
        // `copy_within` accepts a Range; the source range is
        // `cursor..len` and dest is `0`. Both inside `len`.
        self.inner.copy_within(self.cursor..len, 0);
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
