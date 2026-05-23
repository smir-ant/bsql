//! Multi-slot arena for `IntermediateCommandComplete` command tags
//! (DEF-286 Φ-D — DEF-226 multi-statement footprint cascade).
//!
//! # Why an arena
//!
//! [`crate::Action::IntermediateCommandComplete`] is emitted by the
//! DEF-226 multi-statement dispatch arms — each non-final
//! `CommandComplete` / `RowDescription` / `EmptyQueryResponse` in a
//! batched SimpleQuery (`"BEGIN; UPDATE; UPDATE; COMMIT;"`) emits
//! one Action carrying the PRIOR statement's
//! [`crate::command_tag::CommandTag`].
//!
//! Pre-Φ-D the variant carried `tag: CommandTag` inline (40 B). With
//! 9 OutActions slots, the inline payload set Action's variant floor
//! at 48 B (`CommandTag 40 + explicit discriminant + alignment`) —
//! CommandTag has no NonZeroU\* niche, so the outer enum disc could
//! not absorb into a variant's payload. Externalising via this
//! arena drops the ICC variant to 4-8 B (`CommandTagRef` with
//! NonZeroU8 niche), letting Action's outer disc fold into the
//! NonZeroU8 niche of either ICC or DeliverReply's NonZeroU64
//! `id` — Action collapses to 40 B (−17 %), OutActions to 368 B
//! (−16 %).
//!
//! # Single-slot vs multi-slot — why the latter
//!
//! [`crate::command_tag_slot::CommandTagSlotCell`] is single-slot —
//! it parks the LATEST tag for the terminal
//! `Reply::QueryComplete` borrow at materialise. ICC cannot share
//! that semantic: each ICC emission references its OWN prior tag,
//! and a batch can fire several before materialise consumes them.
//! The arena holds up to [`MAX_INTERMEDIATE_TAGS_PER_CALL`] = 9
//! prior tags per OutActions cycle (tied to
//! [`crate::MAX_ACTIONS_PER_CALL`]; one ICC slot per Action slot).
//!
//! # Lifecycle
//!
//! - **Lazy allocation**: `command_tags_arena: Option<Box<CommandTagsArena>>`
//!   on `ActiveInner`/`ConnectingTransient` — pay one `Box`
//!   allocation on the first ICC per connection that uses batched
//!   SimpleQuery, zero cost for single-statement-only connections.
//! - **Per-cycle clear**: cleared at `feed_bytes` entry (mirror of
//!   [`crate::notifications_arena::NotificationsArena`] /
//!   [`crate::error_arena::ErrorArena`] clear-at-entry pattern).
//!   Refs issued in cycle N resolve [`ArenaError::Stale`] in
//!   cycle N+1 via gen mismatch — the wrapper layer MUST consume
//!   `Action::IntermediateCommandComplete` payloads within the same
//!   `OutActions` iteration.
//! - **Slot cap**: 9 (= [`crate::MAX_ACTIONS_PER_CALL`]; one ICC per
//!   Action slot). Per-call overflow is structurally bounded by the
//!   existing OutActions cap — no additional fallback.
//!
//! # Tier-1 by gen-tagged ref
//!
//! `CommandTagRef { slot: BoundedU8<8>, generation: u16 }` is
//! `Copy` and gen-tagged: a ref from cycle N attempting `get()` in
//! cycle N+M resolves to [`ArenaError::Stale`]. No silent
//! wrong-tag read.
//!
//! Mirror of [`crate::notifications_arena`] design verbatim — same
//! invariants, same staleness model.

use crate::error_arena::ArenaError;

/// Maximum intermediate command tags per `feed_bytes` call.
///
/// Bounded by [`crate::MAX_ACTIONS_PER_CALL`] — each
/// `IntermediateCommandComplete` occupies one Action slot, so the
/// per-call cap of 9 is the structural ceiling.
pub(crate) const MAX_INTERMEDIATE_TAGS_PER_CALL: usize = crate::MAX_ACTIONS_PER_CALL;

/// Gen-tagged handle into [`CommandTagsArena`].
///
/// `Copy` (4 B inline: slot 1 B + gen 2 B + 1 B padding). Carried
/// by [`crate::Action::IntermediateCommandComplete`] so Action stays
/// `Copy` and `IntermediateCommandComplete`'s payload shrinks from
/// 40 B inline to 4 B. The NonZeroU8 slot field provides the niche
/// the outer enum's disc absorbs.
///
/// Resolution via [`crate::PgProtocol::get_command_tag`] checks the
/// generation and returns the tag by reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandTagRef {
    /// Slot index into the arena's `slots` vec. `BoundedU8` enforces
    /// `0 ≤ slot < MAX_INTERMEDIATE_TAGS_PER_CALL` at the type level.
    slot: crate::bounded::BoundedU8<{ MAX_INTERMEDIATE_TAGS_PER_CALL.saturating_sub(1) }>,
    /// Arena generation at allocation time. A ref from cycle N
    /// resolves `Err(ArenaError::Stale)` in cycle N+1 via gen
    /// mismatch (cycle-boundary clear bumps `generation`).
    ///
    /// Width `u16` mirrors [`crate::notifications_arena::NotificationRef`]
    /// — same long-connection-wrap edge case (sticky wrap at 2¹⁶
    /// cycles; future widening lands when a consumer surfaces it).
    generation: u16,
}

/// Multi-slot intermediate-command-tag arena.
///
/// One arena per `PgProtocol` instance, lazy-allocated on first ICC
/// arrival (see `ActiveInner::command_tags_arena:
/// Option<Box<CommandTagsArena>>`). Cleared at every `feed_bytes`
/// entry — refs are valid only within their allocation cycle.
#[derive(Debug)]
pub(crate) struct CommandTagsArena {
    /// Per-cycle ring of tag slots. Capacity bounded by
    /// `MAX_INTERMEDIATE_TAGS_PER_CALL`; `alloc` pushes to the next
    /// slot and returns `None` on overflow (cold path — exceeded
    /// the per-call structural cap).
    slots: heapless::Vec<crate::command_tag::CommandTag, MAX_INTERMEDIATE_TAGS_PER_CALL>,
    /// Monotonically-bumped generation counter. Incremented on every
    /// `clear()`. A `CommandTagRef` from a prior cycle has
    /// `gen != self.generation` and resolves
    /// `Err(ArenaError::Stale)` via [`Self::get`].
    generation: u16,
}

impl CommandTagsArena {
    /// Construct an empty arena (no slots, gen 0).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: heapless::Vec::new(),
            generation: 0,
        }
    }

    /// Allocate a slot for `tag`, returning a gen-tagged ref.
    ///
    /// Returns `None` if the per-cycle slot cap
    /// ([`MAX_INTERMEDIATE_TAGS_PER_CALL`]) is exhausted —
    /// architecturally bounded by the OutActions cap (the dispatch
    /// loop cannot emit more ICCs than there are OutActions slots).
    /// Caller classifies as cold-path drop (mirror of
    /// `NotificationsArena::alloc` overflow behaviour).
    #[inline]
    pub(crate) fn alloc(&mut self, tag: crate::command_tag::CommandTag) -> Option<CommandTagRef> {
        let slot_idx_usize = self.slots.len();
        let Ok(slot_idx_u8) = u8::try_from(slot_idx_usize) else {
            // slot count > 255 is architecturally unreachable (cap = 9).
            core::hint::cold_path();
            return None;
        };
        self.slots.push(tag).ok()?;
        let slot = crate::bounded::BoundedU8::try_new(slot_idx_u8)?;
        Some(CommandTagRef {
            slot,
            generation: self.generation,
        })
    }

    /// Resolve a ref to its tag.
    ///
    /// - `Ok(&CommandTag)` — gen matches and slot is populated.
    /// - `Err(ArenaError::Stale)` — ref was issued in a prior cycle
    ///   (gen mismatch); expected when a caller defers resolution
    ///   past the cycle boundary.
    /// - `Err(ArenaError::Empty)` — gen matches but slot index is
    ///   out of bounds. Architecturally unreachable: `alloc` pushes
    ///   to `slots` before issuing the ref, so the slot index
    ///   always corresponds to a populated entry within its issuing
    ///   cycle. Classified explicitly per [`ArenaError`] discipline.
    #[inline]
    pub(crate) fn get(&self, r: CommandTagRef) -> Result<&crate::command_tag::CommandTag, ArenaError> {
        if r.generation != self.generation {
            return Err(ArenaError::Stale);
        }
        let idx = usize::from(r.slot.get());
        match self.slots.get(idx) {
            Some(tag) => Ok(tag),
            None => Err(ArenaError::Empty),
        }
    }

    /// Clear all slots + bump generation.
    ///
    /// Drops every [`crate::command_tag::CommandTag`] in the arena.
    /// Called at `feed_bytes` entry (mirror of the
    /// `NotificationsArena::clear` at-entry pattern); refs from the
    /// prior cycle resolve `Err(ArenaError::Stale)` after this fires.
    ///
    /// `wrapping_add` permitted by the forbid-bundle (no panic);
    /// the `u16` wrap is documented on the field.
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.slots.clear();
    }

    /// Number of currently-allocated slots. Used by tests +
    /// dispatch-layer diagnostics.
    #[cfg(test)]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

impl Default for CommandTagsArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// ─── Drift pins ────────────────────────────────────────────────────

// Size pin: CommandTagRef must stay ≤ 4 bytes — the niche-bearing
// shape that lets Action::IntermediateCommandComplete collapse from
// 40 B inline tag to a single arena handle. If this grows past 4 B,
// the Φ-D footprint cascade silently regresses.
const _: () = assert!(
    core::mem::size_of::<CommandTagRef>() == 4,
    "CommandTagRef exact size — 4 B (BoundedU8 slot + u16 generation + \
     1 B padding to align 2). If this grows: (a) a non-niche field \
     was added, or (b) generation widened to u32 (mirror of \
     NotificationRef width discussion — accept the long-connection \
     edge case for now). Either cascades into Action / OutActions \
     size pins.",
);

const _: () = assert!(
    core::mem::size_of::<Option<CommandTagRef>>() == 4,
    "Option<CommandTagRef> must niche-pack via the NonZeroU8 slot \
     marker — same size as CommandTagRef itself. If this regresses, \
     the niche optimisation was lost.",
);

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    //! Forbid-bundle compliance: tests use `assert!(matches!())` +
    //! `if let Ok(...)` idiom rather than `.unwrap()` / `.expect()`
    //! (which are crate-wide banned). Mirrors `notifications_arena`
    //! tests.
    use super::*;
    use crate::command_tag::CommandTag;

    #[test]
    fn alloc_then_get_returns_tag() {
        let mut arena = CommandTagsArena::new();
        let tag = CommandTag::Insert { rows: 5 };
        let r = arena.alloc(tag);
        assert!(r.is_some(), "alloc on fresh arena must succeed");
        if let Some(r) = r {
            let got = arena.get(r);
            assert!(got.is_ok(), "alloc'd ref must resolve, got {got:?}");
            if let Ok(got_tag) = got {
                assert_eq!(*got_tag, tag);
            }
        }
    }

    #[test]
    fn alloc_multiple_slots_returns_distinct_refs() {
        let mut arena = CommandTagsArena::new();
        let r1 = arena.alloc(CommandTag::Update { rows: 1 });
        let r2 = arena.alloc(CommandTag::Delete { rows: 2 });
        assert!(r1.is_some() && r2.is_some());
        if let (Some(r1), Some(r2)) = (r1, r2) {
            assert_ne!(r1, r2, "distinct allocations must produce distinct refs");
            if let (Ok(t1), Ok(t2)) = (arena.get(r1), arena.get(r2)) {
                assert_eq!(*t1, CommandTag::Update { rows: 1 });
                assert_eq!(*t2, CommandTag::Delete { rows: 2 });
            }
        }
    }

    #[test]
    fn get_after_clear_classifies_as_stale() {
        let mut arena = CommandTagsArena::new();
        let r = arena.alloc(CommandTag::Select { rows: 100 });
        assert!(r.is_some());
        arena.clear();
        if let Some(r) = r {
            assert!(
                matches!(arena.get(r), Err(ArenaError::Stale)),
                "post-clear ref must classify as Stale, got {:?}",
                arena.get(r),
            );
        }
    }

    #[test]
    fn alloc_at_cap_returns_none_on_overflow() {
        let mut arena = CommandTagsArena::new();
        // Cap = 9 (MAX_INTERMEDIATE_TAGS_PER_CALL). Fill the arena,
        // then verify the next alloc returns None (cold-path).
        const _: () = assert!(MAX_INTERMEDIATE_TAGS_PER_CALL == 9);
        let rows_seq: [u64; MAX_INTERMEDIATE_TAGS_PER_CALL] = [0, 1, 2, 3, 4, 5, 6, 7, 8];
        for &rows in &rows_seq {
            let r = arena.alloc(CommandTag::Insert { rows });
            assert!(r.is_some(), "alloc {rows} must succeed (cap = {MAX_INTERMEDIATE_TAGS_PER_CALL})");
        }
        // Cap reached — next alloc must return None (cold path).
        let overflow = arena.alloc(CommandTag::Move { rows: 0 });
        assert!(overflow.is_none(), "alloc past cap must return None, got {overflow:?}");
    }

    #[test]
    fn option_command_tag_ref_niche_packed() {
        assert_eq!(
            core::mem::size_of::<Option<CommandTagRef>>(),
            core::mem::size_of::<CommandTagRef>(),
        );
    }

    #[test]
    fn len_tracks_allocations_and_clear_resets() {
        let mut arena = CommandTagsArena::new();
        assert_eq!(arena.len(), 0, "fresh arena starts empty");
        let _ = arena.alloc(CommandTag::Insert { rows: 1 });
        let _ = arena.alloc(CommandTag::Update { rows: 2 });
        assert_eq!(arena.len(), 2, "two allocs → len == 2");
        arena.clear();
        assert_eq!(arena.len(), 0, "post-clear arena is empty");
    }
}
