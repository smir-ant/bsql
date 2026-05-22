//! Multi-slot arena for `NotificationResponse` (`'A'`) payloads — PG
//! §55.7 LISTEN/NOTIFY surface (DEF-220).
//!
//! # Why an arena
//!
//! PG `NotificationResponse` carries a variable-length `payload`
//! (up to `NOTIFY_PAYLOAD_MAX_LENGTH` = 8000 B per PG spec) plus a
//! `channel` name and `pid`. Inlining the payload into [`crate::Action`]
//! would balloon the variant past the 88 B size pin. The arena holds
//! payload bytes (heap-allocated `Vec<u8>`) and surfaces a gen-tagged
//! [`NotificationRef`] (4 B, `Copy`) that [`crate::Action::Notify`]
//! carries — Action stays `Copy`, arena holds the bytes.
//!
//! Mirror of [`crate::error_arena::ErrorArena`] (single-slot
//! server-error storage) but multi-slot — multiple `NotificationResponse`
//! frames may arrive in one `feed_bytes` call (server batches
//! NOTIFYs across a single network read), each needing a stable
//! slot for the OutActions iteration.
//!
//! # Lifecycle
//!
//! - **Lazy allocation**: `notifications_arena: Option<Box<NotificationsArena>>`
//!   on `ActiveInner` — pay one `Box` allocation on the first NOTIFY
//!   per LISTEN-using connection, zero cost for connections that
//!   never LISTEN.
//! - **Per-cycle clear**: cleared at `feed_bytes` entry (mirror of
//!   `ErrorArena`'s clear-at-entry pattern). Refs issued in cycle N
//!   resolve [`ArenaError::Stale`] in cycle N+1 via gen mismatch.
//!   The wrapper layer MUST consume `Action::Notify` payloads
//!   (via [`crate::PgProtocol::get_notification`]) within the same
//!   `OutActions` iteration cycle.
//! - **Slot cap**: `MAX_NOTIFICATIONS_PER_CALL = 9` (tied to
//!   `MAX_ACTIONS_PER_CALL` — at most one `Action::Notify` per
//!   notification per OutActions slot). Per-call overflow is
//!   structurally bounded by the existing OutActions cap, not an
//!   additional fallback.
//!
//! # Tier-1 by gen-tagged ref
//!
//! `NotificationRef { slot: BoundedU8<MAX-1>, generation: u16 }` is
//! `Copy` and gen-tagged: a ref from cycle N attempting `get()` in
//! cycle N+M resolves to [`ArenaError::Stale`]. No silent
//! wrong-payload read.

use crate::error_arena::ArenaError;
use crate::ident::Ident;

/// Maximum notifications per `feed_bytes` call.
///
/// Bounded by [`crate::MAX_ACTIONS_PER_CALL`] — each notification
/// occupies one `Action::Notify` slot in `OutActions`, and the
/// OutActions cap of 9 is the per-call structural ceiling.
pub(crate) const MAX_NOTIFICATIONS_PER_CALL: usize = 9;

/// Per-notification payload carried in the arena.
///
/// `payload` lives on the heap (`Vec<u8>`) so the inline arena slot
/// is small (~96 B for `pid + channel + Vec-header`); per-notification
/// heap traffic is exactly one alloc on parse + one free on
/// arena-clear. Acceptable on the cold notification-arrival path.
#[derive(Debug)]
pub struct NotificationPayload {
    /// PID of the backend process that issued the NOTIFY.
    pub pid: i32,
    /// Channel name — PG identifier, `Ident` enforces
    /// `≤ NAMEDATALEN-1 = 63` chars.
    pub channel: Ident,
    /// Notification payload bytes. Up to PG's
    /// `NOTIFY_PAYLOAD_MAX_LENGTH` = 8000 B. Heap-allocated —
    /// freed when the arena clears (next `feed_bytes` cycle entry).
    pub payload: alloc::vec::Vec<u8>,
}

/// Gen-tagged handle into [`NotificationsArena`].
///
/// `Copy` (4 B inline: slot 1 B + gen 2 B + 1 B padding). Carried
/// by [`crate::Action::Notify`] so Action stays `Copy`. Resolution
/// via [`crate::PgProtocol::get_notification`] checks the generation
/// and returns the payload by reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotificationRef {
    /// Slot index into the arena's `slots` vec. `BoundedU8` enforces
    /// `0 ≤ slot < MAX_NOTIFICATIONS_PER_CALL` at the type level.
    slot: crate::bounded::BoundedU8<{ MAX_NOTIFICATIONS_PER_CALL.saturating_sub(1) }>,
    /// Arena generation at allocation time. A ref from cycle N
    /// resolves `Err(ArenaError::Stale)` in cycle N+1 via gen
    /// mismatch (cycle-boundary clear bumps `generation`).
    generation: u16,
}

/// Multi-slot notifications arena.
///
/// One arena per `PgProtocol` instance, lazy-allocated on first
/// NOTIFY arrival (see `ActiveInner::notifications_arena:
/// Option<Box<NotificationsArena>>`). Cleared at every `feed_bytes`
/// entry — refs are valid only within their allocation cycle.
#[derive(Debug)]
pub(crate) struct NotificationsArena {
    /// Per-cycle ring of payload slots. Capacity bounded by
    /// `MAX_NOTIFICATIONS_PER_CALL`; `alloc` pushes to the next slot
    /// and returns `None` on overflow (cold path — exceeded the
    /// per-call structural cap).
    slots: heapless::Vec<NotificationPayload, MAX_NOTIFICATIONS_PER_CALL>,
    /// Monotonically-bumped generation counter. Incremented on every
    /// `clear()`. A `NotificationRef` from a prior cycle has
    /// `gen != self.generation` and resolves
    /// `Err(ArenaError::Stale)` via [`Self::get`].
    ///
    /// Width `u16`: with the per-cycle bump, wrap at 2¹⁶ = 65,536
    /// cycles requires a connection that survives ~10 days at one
    /// `feed_bytes` per 14 seconds. The wrap is sticky: a `gen=0`
    /// ref issued post-wrap matches a `gen=0` arena, surfacing as
    /// a wrong-payload read. For now this is acceptable (multi-day
    /// connections rarely accumulate stashed `NotificationRef`s);
    /// future widening to `u32` (~136 years to wrap) lands when
    /// a concrete consumer surfaces the long-connection case.
    generation: u16,
}

impl NotificationsArena {
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
    /// ([`MAX_NOTIFICATIONS_PER_CALL`]) is exhausted — caller
    /// classifies as cold-path drop (mirror of `OutActions` overflow
    /// behaviour; the wrapper observes fewer `Action::Notify`
    /// entries than the wire delivered, surfacing as silent loss
    /// only beyond the structural per-call cap).
    #[inline]
    pub(crate) fn alloc(&mut self, payload: NotificationPayload) -> Option<NotificationRef> {
        let slot_idx_usize = self.slots.len();
        let Ok(slot_idx_u8) = u8::try_from(slot_idx_usize) else {
            // slot count > 255 is architecturally unreachable (cap = 9).
            core::hint::cold_path();
            return None;
        };
        self.slots.push(payload).ok()?;
        let slot = crate::bounded::BoundedU8::try_new(slot_idx_u8)?;
        Some(NotificationRef {
            slot,
            generation: self.generation,
        })
    }

    /// Resolve a ref to its payload.
    ///
    /// - `Ok(&NotificationPayload)` — gen matches and slot is
    ///   populated.
    /// - `Err(ArenaError::Stale)` — ref was issued in a prior cycle
    ///   (gen mismatch); expected when a caller defers resolution
    ///   past the cycle boundary.
    /// - `Err(ArenaError::Empty)` — gen matches but slot index is
    ///   out of bounds. Architecturally unreachable: `alloc` pushes
    ///   to `slots` before issuing the ref, so the slot index always
    ///   corresponds to a populated entry within its issuing cycle.
    ///   Classified explicitly per [`ArenaError`] discipline.
    #[inline]
    pub(crate) fn get(&self, r: NotificationRef) -> Result<&NotificationPayload, ArenaError> {
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
    ///
    /// Drops every [`NotificationPayload`] in the arena — the inner
    /// `Vec<u8>` payloads free their heap allocations via their
    /// drop glue. Called at `feed_bytes` entry (mirror of
    /// `ErrorArena::clear` at-entry pattern); refs from the prior
    /// cycle resolve `Err(ArenaError::Stale)` after this fires.
    ///
    /// `wrapping_add` permitted by the forbid-bundle (no panic);
    /// the `u16` wrap is documented on the field — accept the
    /// long-connection edge case for now.
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.slots.clear();
    }

    /// Number of currently-allocated slots.
    ///
    /// `pub(crate)` — used by tests + dispatch-layer diagnostics
    /// (e.g., the post-feed_bytes invariant check
    /// «notifications_arena.len() ≤ outactions.notify_count()»).
    #[cfg(test)]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

impl Default for NotificationsArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ident(s: &str) -> Ident {
        Ident::try_from_str(s).unwrap_or_default()
    }

    fn make_payload(pid: i32, channel: &str, payload: &[u8]) -> NotificationPayload {
        NotificationPayload {
            pid,
            channel: make_ident(channel),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn empty_arena_is_zero_len() {
        let arena = NotificationsArena::new();
        assert_eq!(arena.len(), 0);
    }

    #[test]
    fn alloc_then_get_round_trip() {
        let mut arena = NotificationsArena::new();
        let opt = arena.alloc(make_payload(42, "ch1", b"hello"));
        assert!(opt.is_some(), "first alloc must fit (cap=9)");
        let Some(r) = opt else { return };
        let res = arena.get(r);
        assert!(res.is_ok(), "ref must resolve in same cycle");
        let Ok(got) = res else { return };
        assert_eq!(got.pid, 42);
        assert_eq!(got.channel.as_str(), "ch1");
        assert_eq!(got.payload.as_slice(), b"hello");
    }

    #[test]
    fn get_after_clear_is_stale() {
        let mut arena = NotificationsArena::new();
        let opt = arena.alloc(make_payload(7, "x", b"payload"));
        assert!(opt.is_some(), "alloc must fit");
        let Some(r) = opt else { return };
        arena.clear();
        assert!(matches!(arena.get(r), Err(ArenaError::Stale)));
    }

    #[test]
    fn multiple_allocs_get_distinct_slots() {
        let mut arena = NotificationsArena::new();
        let r1_opt = arena.alloc(make_payload(1, "a", b"one"));
        let r2_opt = arena.alloc(make_payload(2, "b", b"two"));
        assert!(r1_opt.is_some() && r2_opt.is_some(), "both allocs fit");
        let Some(r1) = r1_opt else { return };
        let Some(r2) = r2_opt else { return };
        assert_ne!(r1.slot.get(), r2.slot.get());
        let g1 = arena.get(r1);
        let g2 = arena.get(r2);
        assert!(g1.is_ok() && g2.is_ok(), "both refs resolve");
        let Ok(p1) = g1 else { return };
        let Ok(p2) = g2 else { return };
        assert_eq!(p1.pid, 1);
        assert_eq!(p2.pid, 2);
    }

    #[test]
    fn alloc_beyond_cap_returns_none() {
        let mut arena = NotificationsArena::new();
        for i in 0..MAX_NOTIFICATIONS_PER_CALL {
            let pid = i32::try_from(i).unwrap_or(0);
            assert!(arena.alloc(make_payload(pid, "ch", b"")).is_some());
        }
        // Cap exhausted.
        assert!(arena.alloc(make_payload(99, "ch", b"")).is_none());
    }

    #[test]
    fn clear_bumps_generation() {
        let mut arena = NotificationsArena::new();
        let gen_before = arena.generation;
        arena.clear();
        assert_ne!(arena.generation, gen_before);
    }

    #[test]
    fn get_with_wrong_gen_is_stale() {
        let mut arena_a = NotificationsArena::new();
        let mut arena_b = NotificationsArena::new();
        arena_b.clear(); // bump arena_b's gen to differ from arena_a
        let opt = arena_a.alloc(make_payload(1, "ch", b""));
        assert!(opt.is_some(), "alloc fits");
        let Some(r) = opt else { return };
        // Resolve r against arena_b (different gen) — should be Stale.
        assert!(matches!(arena_b.get(r), Err(ArenaError::Stale)));
    }
}
