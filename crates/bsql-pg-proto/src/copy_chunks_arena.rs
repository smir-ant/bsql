//! Multi-slot arena for `CopyData` ('d') chunk bytes — PG §55.2.6
//! COPY OUT surface .
//!
//! # Why an arena
//!
//! PG `CopyData` frames carry bulk-transfer bytes (CSV row chunks
//! in text mode, PG binary tuples in binary mode). Chunk sizes
//! typically 4-64 KB. Inlining bytes into [`crate::Action`] would
//! balloon the variant past the 88 B size pin. The arena holds
//! chunk bytes (heap-allocated `Vec<u8>`) and surfaces a gen-tagged
//! [`CopyChunkRef`] (4 B, `Copy`) that [`crate::Action::CopyDataChunk`]
//! carries — Action stays `Copy`, arena holds the bytes.
//!
//! Mirror of [`crate::notifications_arena::NotificationsArena`]
//! () — both arenas use the same multi-slot + gen-tagged
//! ref pattern.
//!
//! # Per-call cap
//!
//! `MAX_CHUNKS_PER_CALL = 9` (tied to `MAX_ACTIONS_PER_CALL` — each
//! chunk occupies one `Action::CopyDataChunk` slot in OutActions).
//! At MAX 64 KB/chunk × 9 chunks/call = 576 KB max heap allocation
//! per `feed_bytes` cycle. Arena clears per cycle (gen bump) so
//! steady-state heap is bounded.

use crate::error_arena::ArenaError;

/// Maximum chunks per `feed_bytes` call.
///
/// Bounded by [`crate::MAX_ACTIONS_PER_CALL`] — each chunk
/// occupies one `Action::CopyDataChunk` slot in `OutActions`, and
/// the OutActions cap of 9 is the per-call structural ceiling.
pub(crate) const MAX_CHUNKS_PER_CALL: usize = 9;

/// Per-chunk payload — opaque bytes from a `CopyData` frame.
///
/// Heap-allocated `Vec<u8>` so the inline arena slot is small
/// (~24 B = Vec header); per-chunk heap traffic is exactly one
/// alloc on parse + one free on arena-clear. Acceptable on COPY
/// (cold relative to row-streaming hot path).
#[derive(Debug)]
pub struct CopyChunkPayload {
    /// Chunk bytes. Server-emitted COPY OUT data — opaque format
    /// (CSV in text mode, PG binary tuples in binary mode); the
    /// caller interprets per the [`crate::decode::CopyFormat`]
    /// captured from the `CopyOutResponse` header.
    pub bytes: alloc::vec::Vec<u8>,
}

/// Gen-tagged handle into `CopyChunksArena`.
///
/// `Copy` (4 B inline: slot 1 B + gen 2 B + 1 B padding). Carried
/// by [`crate::Action::CopyDataChunk`] so Action stays `Copy`.
/// Resolution via [`crate::PgProtocol::get_copy_chunk`] checks the
/// generation and returns the bytes by reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyChunkRef {
    /// Slot index into the arena's `slots` vec. `BoundedU8` enforces
    /// `0 ≤ slot < MAX_CHUNKS_PER_CALL` at the type level.
    slot: crate::bounded::BoundedU8<{ MAX_CHUNKS_PER_CALL.saturating_sub(1) }>,
    /// Arena generation at allocation time. A ref from cycle N
    /// resolves `Err(ArenaError::Stale)` in cycle N+1 via gen
    /// mismatch (cycle-boundary clear bumps `generation`).
    generation: u16,
}

/// Multi-slot copy-chunks arena.
///
/// One arena per `PgProtocol` instance, lazy-allocated on first
/// COPY OUT chunk arrival (see `ActiveInner::copy_chunks_arena:
/// Option<Box<CopyChunksArena>>`). Cleared at every `feed_bytes`
/// entry — refs are valid only within their allocation cycle.
#[derive(Debug)]
pub(crate) struct CopyChunksArena {
    slots: heapless::Vec<CopyChunkPayload, MAX_CHUNKS_PER_CALL>,
    generation: u16,
}

impl CopyChunksArena {
    /// Construct an empty arena (no slots, gen 0).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: heapless::Vec::new(),
            generation: 0,
        }
    }

    /// Allocate a slot for `payload`, returning a gen-tagged ref.
    ///
    /// Returns `None` if the per-cycle slot cap
    /// ([`MAX_CHUNKS_PER_CALL`]) is exhausted — caller classifies
    /// as cold-path drop (mirror of `OutActions` overflow
    /// behaviour).
    #[inline]
    pub(crate) fn alloc(&mut self, payload: CopyChunkPayload) -> Option<CopyChunkRef> {
        let slot_idx_usize = self.slots.len();
        let Ok(slot_idx_u8) = u8::try_from(slot_idx_usize) else {
            core::hint::cold_path();
            return None;
        };
        self.slots.push(payload).ok()?;
        let slot = crate::bounded::BoundedU8::try_new(slot_idx_u8)?;
        Some(CopyChunkRef {
            slot,
            generation: self.generation,
        })
    }

    /// Resolve a ref to its payload.
    ///
    /// - `Ok(&CopyChunkPayload)` — gen matches and slot is populated.
    /// - `Err(ArenaError::Stale)` — ref was issued in a prior cycle.
    /// - `Err(ArenaError::Empty)` — gen matches but slot index out
    ///   of bounds. Architecturally unreachable.
    #[inline]
    pub(crate) fn get(&self, r: CopyChunkRef) -> Result<&CopyChunkPayload, ArenaError> {
        if r.generation != self.generation {
            return Err(ArenaError::Stale);
        }
        let idx = usize::from(r.slot.get());
        match self.slots.get(idx) {
            Some(payload) => Ok(payload),
            None => Err(ArenaError::Empty),
        }
    }

    /// Clear all slots + bump generation.
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.slots.clear();
    }
}

impl Default for CopyChunksArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_payload(bytes: &[u8]) -> CopyChunkPayload {
        CopyChunkPayload {
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn alloc_then_get_round_trip() {
        let mut arena = CopyChunksArena::new();
        let opt = arena.alloc(make_payload(b"row1\trow1col2\n"));
        assert!(opt.is_some());
        let Some(r) = opt else { return };
        let res = arena.get(r);
        assert!(res.is_ok());
        let Ok(got) = res else { return };
        assert_eq!(got.bytes.as_slice(), b"row1\trow1col2\n");
    }

    #[test]
    fn get_after_clear_is_stale() {
        let mut arena = CopyChunksArena::new();
        let opt = arena.alloc(make_payload(b"hi"));
        let Some(r) = opt else { return };
        arena.clear();
        assert!(matches!(arena.get(r), Err(ArenaError::Stale)));
    }

    #[test]
    fn alloc_beyond_cap_returns_none() {
        let mut arena = CopyChunksArena::new();
        for _ in 0..MAX_CHUNKS_PER_CALL {
            assert!(arena.alloc(make_payload(b"x")).is_some());
        }
        assert!(arena.alloc(make_payload(b"y")).is_none());
    }
}
