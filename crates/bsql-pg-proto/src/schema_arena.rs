//! Schema arena — externalised `RowDesc` storage.
//!
//! # DEF-119 + DEF-148 rationale
//!
//! Pre-arena (pre-DEF-119), `RowDesc` (260 B POD) lived INLINE across
//! the state machine:
//!
//! - `ProtoState::*StreamingRows { row_desc }` — 260 B inside state.
//! - `ProtoState::*AwaitingRfq { row_desc }` — 260 B.
//! - `StagedAction::StreamRowRange { row_desc }` — 260 B copied per
//!   DataRow (1000-row query = 260 KB of copy traffic).
//! - `Reply::QueryComplete { row_desc: Option<RowDesc> }` — 260 B.
//! - `DescribedRows::Rows(RowDesc)` — 260 B.
//!
//! DEF-119 replaced inline storage with a single-owner slab on
//! `PgProtocol`; state / staged-actions / internal reply payloads
//! carry a small [`SchemaRef`] handle. Users read through the arena
//! at materialise time via a `&'r RowDesc` borrow tied to the
//! `PgProtocol` borrow lifetime — zero extra copies on the hot path.
//!
//! DEF-148 refines the handle shape to its final form:
//! `SchemaRef { slot: NonZeroU8, generation: u8 }` (2 bytes, plus
//! niche-packed `Option<SchemaRef>` at 2 bytes). The generation
//! counter makes stale refs a classifiable condition (a refactor
//! that calls `arena.get(r)` after `clear()` returns `None` with a
//! structural reason, not silent substitution). The `NonZeroU8`
//! slot eliminates the test-fixture `ZERO` sentinel class (a slot
//! index of 0 is physically impossible — the value 0 IS the
//! `Option<SchemaRef>::None` niche). The arena also gains a
//! `has_any: bool` fast-path: a `clear()` call on an already-empty
//! slab (the common case on the Ping-loop hot path) returns
//! immediately without walking the 528 B slots.
//!
//! # Size impact
//!
//! | Carrier | Before arena | DEF-119 | DEF-148 |
//! |---|---|---|---|
//! | `ProtoState` | ~1224 B | ~1224 B (SCRAM dominated) | unchanged |
//! | `Action::StreamRow` | ~280 B | ~32 B | unchanged |
//! | `Reply::QueryComplete` payload | ~300 B | ~16 B + `&'r RowDesc` | unchanged |
//! | Per-row DataRow emission | 260 B copy | 8 B ref | unchanged |
//! | `SchemaRef` | n/a | 1 B | 2 B |
//! | `Option<SchemaRef>` | n/a | 2 B | 2 B (niche) |
//!
//! Arena cost: 2 × (~264 B slot) + 2 × 1 B (generations) + 1 B
//! (has_any) + padding ≈ 536 B on `PgProtocol`. Paid once per
//! connection; amortised across all queries that connection services.
//!
//! # Slot count
//!
//! `MAX_ARENA_SLOTS = 2`. Single-inflight flow (current scope) uses
//! one slot; the second is headroom for mid-query transitions and
//! for 1c-5 pipelining's overlap window (Parse-then-Describe before
//! Sync, two concurrent schemas briefly coexist). Going past 2 buys
//! no functionality until pipelining lands and wastes 260 B per
//! extra slot.
//!
//! # Alloc / clear discipline
//!
//! - **Alloc** happens when the server sends schema:
//!   `parse_row_description` success in dispatch → `arena.alloc(desc)`
//!   → `SchemaRef` threaded into the new state variant. The handle
//!   captures the slot's CURRENT generation; subsequent `get()`
//!   validates the generation still matches.
//! - **Clear** happens at **next entry point** (start of the next
//!   `feed_bytes` / `push_command` / `push_bind_execute` call) when
//!   the prior state is `Idle` or `Errored`. This is the earliest
//!   safe moment to reclaim slots: the trailing `ReadyForQuery` from
//!   the previous cycle produced a `Reply<'r>` carrying `&'r RowDesc`
//!   out via `OutActions<'w, 'r>`. That borrow is bound to the
//!   `&mut PgProtocol` the user held — so as soon as `OutActions`
//!   drops (ending `'r`), the next call's `&mut self` can touch
//!   `schema_arena` again. `clear()` bumps per-slot generations so
//!   any stale `SchemaRef` built before the clear resolves to
//!   `None` via generation mismatch.
//! - **Fast-path**: `clear()` early-returns when `has_any` is false
//!   (A007) — the Ping-loop hot path never carries a schema, so no
//!   528 B memset per iteration.
//!
//! Why `clear()` rather than per-ref `free()`: the 2-slot arena
//! serves at most one inflight query at a time (single-inflight
//! invariant pre-1c-5). Every slot occupied post-cycle belongs to
//! the just-completed cycle; blanket clearing is both semantically
//! equivalent and cheaper (no ref tracking). `free()` is retained
//! as a test-only API; 1c-5 pipelining unconditionalises it.
//!
//! Tier-2 structural invariant: dispatch only allocs when
//! `parse_row_description` succeeds and the transition targets a
//! schema-bearing state variant; the Idle-entry clear guarantees
//! the arena is empty before each user interaction.
//!
//! # Stale-ref classification (DEF-148 + DEF-150)
//!
//! A `SchemaRef` whose generation no longer matches the slot's
//! current generation resolves to `None` from `arena.get()`. The
//! dispatch-layer discipline guarantees this cannot happen under
//! intact code: alloc captures the current generation; the
//! capturing slot is not freed or cleared while the caller still
//! holds the ref; the ref's lifetime (inside state and staged
//! payloads) ends before the next `clear()`. A `None` from `get()`
//! on a ref that SHOULD be live is therefore a crate bug; DEF-150
//! classifies it as `ProtocolError::InternalCrateBug { locus:
//! CrateBugLocus::StaleSchemaRef }` rather than silent substitution.

use core::num::NonZeroU8;

use crate::decode::RowDesc;

/// Maximum arena slot count. See [module-level](self) rationale for why 2.
///
/// Bound: must stay ≤ 254 because `SchemaRef::slot: NonZeroU8`
/// encodes `slot_idx + 1`; valid indices are `1..=MAX_ARENA_SLOTS`
/// which must fit `NonZeroU8`'s `1..=255` range.
pub(crate) const MAX_ARENA_SLOTS: usize = 2;

const _: () = assert!(
    MAX_ARENA_SLOTS <= 254,
    "MAX_ARENA_SLOTS must be ≤ 254 — SchemaRef::slot is NonZeroU8 encoding (slot_idx + 1).",
);

/// Handle into [`SchemaSlab`]. Two bytes: niche-packed slot index +
/// generation counter. `Copy`, `PartialEq`.
///
/// # Shape
///
/// ```text
/// struct SchemaRef {
///     slot: NonZeroU8,   // encodes slot_idx + 1; 1..=MAX_ARENA_SLOTS
///     generation: u8,    // captured at alloc; validated at get
/// }
/// ```
///
/// Size: 2 bytes. `Option<SchemaRef>` also 2 bytes via niche on
/// `slot` (the value 0 = `None`).
///
/// # No lifetime parameter
///
/// `SchemaRef` does NOT carry a lifetime. Rationale: state variants
/// own handles across transitions, and a lifetime-bound handle would
/// couple the state enum to the arena's borrow — impossible, because
/// state transitions happen via `core::mem::take` which moves state
/// out independently.
///
/// The arena borrow is instead bound at the **dereferencing** site:
/// [`SchemaSlab::get`] returns `Option<&'arena RowDesc>`. Stale
/// refs (generation mismatch after `clear`) return `None` — see
/// module-level "Stale-ref classification" for DEF-150's diagnostic
/// treatment.
///
/// # Invariants maintained by construction
///
/// A `SchemaRef` is **only** constructed via [`SchemaSlab::alloc`];
/// its `slot` is guaranteed in range `1..=MAX_ARENA_SLOTS` and its
/// `generation` matches the slab's counter for that slot at the
/// moment of allocation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRef {
    /// Slot index encoded as `slot_idx + 1`. Valid range:
    /// `1..=MAX_ARENA_SLOTS`. The `0` value is reserved for
    /// `Option<SchemaRef>::None` via the `NonZeroU8` niche.
    slot: NonZeroU8,
    /// Slab generation captured at [`SchemaSlab::alloc`] time.
    /// A mismatch at [`SchemaSlab::get`] time means the slot was
    /// cleared or freed since this ref was issued (stale ref).
    generation: u8,
}

impl SchemaRef {
    /// Slot index (`0..MAX_ARENA_SLOTS`). Crate-internal.
    ///
    /// Returns the 0-based index. `self.slot: NonZeroU8` encodes
    /// `slot_idx + 1`, so `slot_idx() = slot.get() - 1`. The
    /// subtraction is infallible (`slot.get() >= 1` by type).
    #[inline]
    pub(crate) const fn slot_idx(self) -> u8 {
        // `slot.get() >= 1` guaranteed by NonZeroU8; `- 1` stays in
        // u8 range. `saturating_sub` satisfies the forbid-bundle's
        // ban on `arithmetic_side_effects` — the saturating fallback
        // is architecturally dead.
        self.slot.get().saturating_sub(1)
    }

    /// Generation captured at alloc. Used by [`SchemaSlab::get`]
    /// for stale-ref detection (internal, via field access) and by
    /// tests for generation-invariant pinning. Production code
    /// doesn't need the accessor — `SchemaSlab::get` encapsulates
    /// the check. DEF-150 may promote to crate-wide usage for
    /// stale-ref diagnostic classification.
    #[cfg(test)]
    #[inline]
    pub(crate) const fn generation(self) -> u8 {
        self.generation
    }

    /// Test-only "dead" SchemaRef — valid shape (NonZeroU8 slot,
    /// u8 generation) but not issued by any live [`SchemaSlab`].
    /// Intended solely as the `unwrap_or` fallback in
    /// `assert!(alloc.is_some()) + unwrap_or(dead_for_test)` test
    /// fixtures; a `get()` on any real slab with this ref returns
    /// `None` via generation mismatch (most of the time) or via
    /// accidental match followed by slot-being-free — both safe.
    ///
    /// Replaces the pre-DEF-148 `SchemaRef::ZERO` sentinel which is
    /// physically impossible post-NonZeroU8. The forbid-bundle bans
    /// `panic!` and `.unwrap()`, so test fixtures need a concrete
    /// fallback to construct state enum variants that destructure
    /// `schema_ref: SchemaRef`.
    #[cfg(test)]
    #[inline]
    pub(crate) fn dead_for_test() -> Self {
        // NonZeroU8::MIN = 1 is the lowest valid slot index
        // (encoding slot_idx=0). `.unwrap_or(NonZeroU8::MIN)` is a
        // tautology on `NonZeroU8::new(1)` — the unwrap_or branch is
        // dead under `NonZeroU8::new(1).is_some() == true`. Keeps
        // the forbid-bundle happy without requiring a const.
        Self {
            slot: NonZeroU8::new(1).unwrap_or(NonZeroU8::MIN),
            generation: 0,
        }
    }
}

/// Fixed-slot schema arena on `PgProtocol`.
///
/// See [module-level](self) docstring for full design and
/// alloc/clear discipline.
#[derive(Debug)]
pub(crate) struct SchemaSlab {
    /// `None` = free slot, `Some(desc)` = occupied.
    ///
    /// `Option<RowDesc>` here is fine: `RowDesc` doesn't niche-pack,
    /// so each slot is ~4 (discriminant + pad) + 260 (`RowDesc`) =
    /// 264 B. Total slot footprint: 2 × 264 = 528 B.
    slots: [Option<RowDesc>; MAX_ARENA_SLOTS],
    /// Per-slot generation counters. Bumped on `free()` and on the
    /// subset of slots that are occupied at `clear()` time. A
    /// [`SchemaRef`] captures the current generation at alloc; any
    /// subsequent `get()` validates the counter still matches.
    ///
    /// `u8` wraps with a 256-cycle period. A stale ref surviving
    /// 256 full arena cycles on the same slot would collide; for
    /// the current flow (single-inflight, arena cleared between
    /// user-visible query boundaries) a stale ref's lifetime ends
    /// long before the next cycle even starts, so the collision
    /// window is architecturally dead. If 1c-5 pipelining reveals
    /// a real collision path, promote to `u16`.
    generations: [u8; MAX_ARENA_SLOTS],
    /// `true` iff any slot is occupied. DEF-148 fast-path: a
    /// `clear()` on an already-empty slab returns immediately
    /// without walking the slots, saving ~528 B of `None`-write
    /// memset per Ping-loop iteration (A007).
    has_any: bool,
}

impl SchemaSlab {
    /// Construct an empty slab (all slots free, all generations 0,
    /// has_any = false).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: [None; MAX_ARENA_SLOTS],
            generations: [0_u8; MAX_ARENA_SLOTS],
            has_any: false,
        }
    }

    /// Allocate a slot for `desc`, returning a handle capturing the
    /// current slot generation.
    ///
    /// Returns `None` if all slots are occupied — caller (dispatch)
    /// treats this as a structural invariant break (no flow should
    /// hold more than `MAX_ARENA_SLOTS` schemas simultaneously in
    /// pre-1c-5 single-inflight). DEF-150 classifies that path as
    /// `CrateBugLocus::SchemaArenaAllocFull`.
    #[inline]
    #[must_use]
    pub(crate) fn alloc(&mut self, desc: RowDesc) -> Option<SchemaRef> {
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(desc);
                self.has_any = true;
                // `idx < MAX_ARENA_SLOTS ≤ 254` (const-asserted
                // above) so `idx + 1` fits u8 unconditionally.
                // Explicit match + saturating fallback satisfies
                // the forbid bundle's ban on `as` conversions and
                // `arithmetic_side_effects`; LLVM elides the dead
                // Err / None under any optimisation level.
                let Ok(idx_u8) = u8::try_from(idx) else {
                    return None;
                };
                let slot_plus_one = idx_u8.saturating_add(1);
                let slot_nz = NonZeroU8::new(slot_plus_one)?;
                // Read the current generation for this slot. `.get()`
                // is bounds-safe (idx in range by for-loop + slots
                // length equals generations length by construction).
                let generation = self.generations.get(idx).copied().unwrap_or(0);
                return Some(SchemaRef {
                    slot: slot_nz,
                    generation,
                });
            }
        }
        None
    }

    /// Read the schema at `r`, or `None` if the slot is free /
    /// out-of-range / the generation no longer matches (stale ref).
    ///
    /// See module-level "Stale-ref classification" for DEF-150's
    /// treatment of `None` on a ref that SHOULD be live (crate bug).
    #[inline]
    #[must_use]
    pub(crate) fn get(&self, r: SchemaRef) -> Option<&RowDesc> {
        let idx = usize::from(r.slot_idx());
        let current_gen = self.generations.get(idx).copied()?;
        if current_gen != r.generation {
            return None;
        }
        self.slots.get(idx).and_then(Option::as_ref)
    }

    /// Release the slot at `r`. Bumps the slot's generation so any
    /// lingering SchemaRef built before this call resolves to `None`
    /// via generation-mismatch at `get()`. Safe to call on an
    /// already-free slot (idempotent — the generation still bumps,
    /// but that's a no-op unless someone holds a pre-bump ref).
    /// Safe to call on an out-of-range / stale ref (no-op).
    ///
    /// Currently test-only — the production discipline uses
    /// [`Self::clear`] at entry points (see module docs). Reserved
    /// for 1c-5 pipelining where per-ref release replaces blanket
    /// clearing.
    #[cfg(test)]
    #[inline]
    pub(crate) fn free(&mut self, r: SchemaRef) {
        let idx = usize::from(r.slot_idx());
        // Collapsed `if let ... if ...` per clippy::collapsible_if.
        // Rust 2024 reserves `gen` — use `gen_slot` for the per-slot
        // generation counter alias.
        if let Some(slot) = self.slots.get_mut(idx)
            && slot.is_some()
        {
            *slot = None;
            if let Some(gen_slot) = self.generations.get_mut(idx) {
                *gen_slot = gen_slot.wrapping_add(1);
            }
        }
        // Recompute has_any — cheap O(N=2) walk.
        self.has_any = self.slots.iter().any(Option::is_some);
    }

    /// Clear all slots. Called at entry of the next protocol
    /// interaction when the prior state is `Idle` or `Errored` —
    /// reclaims any schemas carried across the previous cycle.
    ///
    /// DEF-148 fast-path: early-return when `!has_any` — the
    /// Ping-loop hot path never carries a schema, so no 528 B
    /// memset per iteration.
    ///
    /// DEF-148 generation semantics: each occupied slot has its
    /// generation bumped, invalidating any pre-clear `SchemaRef`
    /// via generation-mismatch at `get()`.
    #[inline]
    pub(crate) fn clear(&mut self) {
        if !self.has_any {
            return;
        }
        // Walk occupied slots: clear each, bump its generation.
        // Free slots stay at their current generation (no ref
        // outstanding to invalidate).
        for (slot, gen_slot) in self.slots.iter_mut().zip(self.generations.iter_mut()) {
            if slot.is_some() {
                *slot = None;
                *gen_slot = gen_slot.wrapping_add(1);
            }
        }
        self.has_any = false;
    }

    /// Count occupied slots. Debug-only — production code doesn't
    /// need runtime occupancy checks because the dispatch-layer
    /// alloc-on-schema and entry-point-clear discipline is tier-2
    /// structural (see module docs). DEF-152 uses this as a
    /// `debug_assert_eq!` probe at each clear() call site to pin
    /// "clear() must leave arena empty" against future drift.
    #[cfg(debug_assertions)]
    #[inline]
    #[must_use]
    pub(crate) fn occupied_count(&self) -> u8 {
        let mut n: u8 = 0;
        for slot in &self.slots {
            if slot.is_some() {
                n = n.saturating_add(1);
            }
        }
        n
    }
}

impl Default for SchemaSlab {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// DEF-151 drift pin: total slab size. 2 slots × ~264 B each +
// generations (2 × u8) + has_any (bool) + padding.
// Measured post-DEF-148: ~520 B on aarch64-apple-darwin (padding is
// tight because Option<RowDesc>'s discriminant fits in the slot's
// trailing padding, so the arena slots end at a natural boundary
// where the generations + has_any fit into the same alignment).
// Range [512, 544] tolerates cross-platform alignment.
const _: () = assert!(
    core::mem::size_of::<SchemaSlab>() >= 512
        && core::mem::size_of::<SchemaSlab>() <= 544,
    "SchemaSlab size drift — post-DEF-148 actual ~520 B. Range [512, 544] \
     tolerates cross-platform alignment. If MAX_ARENA_SLOTS grows, update \
     PgProtocol size budget in lib.rs in lockstep.",
);

// Drift pin: SchemaRef is 2 bytes (NonZeroU8 slot + u8 generation).
const _: () = assert!(
    core::mem::size_of::<SchemaRef>() == 2,
    "SchemaRef must stay 2 bytes (NonZeroU8 slot + u8 generation).",
);

// Drift pin: `Option<SchemaRef>` size. NonZeroU8's niche (value 0)
// is the Option None discriminant — no extra discriminant byte, so
// Option<SchemaRef> is 2 bytes (same as SchemaRef itself).
const _: () = assert!(
    core::mem::size_of::<Option<SchemaRef>>() == 2,
    "Option<SchemaRef> must stay 2 bytes (NonZeroU8 niche on slot field).",
);

#[cfg(test)]
mod tests {
    //! Forbid-bundle compliance: `panic!`, `.unwrap()`, `.expect()`,
    //! `unreachable!()`, and `assert!(false)` are banned crate-wide
    //! (including unit tests). `must_alloc` below uses the idiomatic
    //! `assert!(is_some) + unwrap_or(fallback)` pattern — the `assert!`
    //! fires loudly when the precondition breaks; the `.unwrap_or(_)`
    //! fallback is defensive dead code keeping the test compiling with
    //! a concrete `SchemaRef`. Same pattern as `must_parse` in
    //! `decode::data_row_ref_tests`.
    use super::*;

    /// Alloc on `slab` and return the handle. Fails the test via
    /// `assert!` if alloc returns `None` — the fallback
    /// [`SchemaRef::dead_for_test`] is unreachable in correct tests.
    fn must_alloc(slab: &mut SchemaSlab, desc: RowDesc) -> SchemaRef {
        let r = slab.alloc(desc);
        assert!(r.is_some(), "alloc must succeed on slab with free slot, got {r:?}");
        r.unwrap_or_else(SchemaRef::dead_for_test)
    }

    /// Invariant (spec): fresh slab has zero occupied slots.
    #[test]
    fn fresh_slab_is_empty() {
        let slab = SchemaSlab::new();
        assert_eq!(slab.occupied_count(), 0);
        assert!(!slab.has_any);
    }

    /// Invariant (spec): alloc uses slots in order, returns slot
    /// indices 0, 1, then `None` when full. Captured generation is 0
    /// on a fresh slab.
    #[test]
    fn alloc_fills_in_order_then_returns_none() {
        let mut slab = SchemaSlab::new();
        let desc = RowDesc::EMPTY;

        let r0 = must_alloc(&mut slab, desc);
        assert_eq!(r0.slot_idx(), 0);
        assert_eq!(r0.generation(), 0);
        assert_eq!(slab.occupied_count(), 1);
        assert!(slab.has_any);

        let r1 = must_alloc(&mut slab, desc);
        assert_eq!(r1.slot_idx(), 1);
        assert_eq!(r1.generation(), 0);
        assert_eq!(slab.occupied_count(), 2);

        // Full — next alloc returns None.
        assert!(slab.alloc(desc).is_none());
        assert_eq!(slab.occupied_count(), 2);
    }

    /// Invariant (spec): `get` returns the stored value for a live
    /// ref; `free` invalidates the ref via generation bump.
    #[test]
    fn alloc_get_free_round_trip() {
        let mut slab = SchemaSlab::new();
        let desc = RowDesc::EMPTY;

        let r = must_alloc(&mut slab, desc);
        assert!(slab.get(r).is_some());

        slab.free(r);
        // Post-free: ref is stale (generation bumped). get() returns None.
        assert!(slab.get(r).is_none());
        assert_eq!(slab.occupied_count(), 0);
        assert!(!slab.has_any);

        // Slot 0 free → next alloc reuses it, but with a BUMPED
        // generation. Old r still resolves to None.
        let r2 = must_alloc(&mut slab, desc);
        assert_eq!(r2.slot_idx(), 0);
        assert_eq!(r2.generation(), 1, "generation bumps on free");
        assert!(slab.get(r2).is_some());
        assert!(slab.get(r).is_none(), "old ref stays stale");
    }

    /// Invariant (spec): double-free is idempotent (no panic, no
    /// additional generation bump on already-free slot).
    #[test]
    fn double_free_is_idempotent() {
        let mut slab = SchemaSlab::new();
        let r = must_alloc(&mut slab, RowDesc::EMPTY);
        slab.free(r);
        let gen_after_first_free = slab
            .generations
            .get(usize::from(r.slot_idx()))
            .copied()
            .unwrap_or(255);
        slab.free(r); // no-op on already-free slot
        let gen_after_second_free = slab
            .generations
            .get(usize::from(r.slot_idx()))
            .copied()
            .unwrap_or(255);
        assert_eq!(
            gen_after_first_free, gen_after_second_free,
            "second free on already-free slot must not bump generation",
        );
        assert_eq!(slab.occupied_count(), 0);
    }

    /// Invariant (spec): `clear` zeroes all slots AND bumps
    /// generations on occupied slots, making any pre-clear refs
    /// stale.
    #[test]
    fn clear_empties_all_slots_and_invalidates_refs() {
        let mut slab = SchemaSlab::new();
        let first = must_alloc(&mut slab, RowDesc::EMPTY);
        let second = must_alloc(&mut slab, RowDesc::EMPTY);
        assert_ne!(first.slot_idx(), second.slot_idx());
        assert!(slab.get(first).is_some());
        assert!(slab.get(second).is_some());
        assert_eq!(slab.occupied_count(), 2);
        assert!(slab.has_any);

        slab.clear();
        assert_eq!(slab.occupied_count(), 0);
        assert!(!slab.has_any);
        // Both pre-clear refs are now stale.
        assert!(slab.get(first).is_none());
        assert!(slab.get(second).is_none());
    }

    /// DEF-148 fast-path: clear() on already-empty slab doesn't
    /// bump any generation (no occupied slots to invalidate).
    #[test]
    fn clear_on_empty_slab_is_noop() {
        let mut slab = SchemaSlab::new();
        assert!(!slab.has_any);
        let gens_before: [u8; MAX_ARENA_SLOTS] = slab.generations;
        slab.clear();
        assert_eq!(slab.generations, gens_before, "clear on empty must not bump generations");
        assert!(!slab.has_any);
    }

    /// DEF-148 generational invalidation: a ref built BEFORE clear
    /// cannot accidentally alias a slot allocated AFTER clear, even
    /// when the fresh alloc lands in the same physical slot.
    #[test]
    fn stale_ref_across_clear_resolves_to_none() {
        let mut slab = SchemaSlab::new();
        let desc = RowDesc::EMPTY;
        let r_before = must_alloc(&mut slab, desc);
        slab.clear();
        // Fresh alloc takes the same physical slot (slot 0) but with
        // a bumped generation.
        let r_after = must_alloc(&mut slab, desc);
        assert_eq!(r_before.slot_idx(), r_after.slot_idx());
        assert_ne!(r_before.generation(), r_after.generation());
        // r_before is stale — resolves to None despite pointing at
        // the same physical slot as r_after.
        assert!(slab.get(r_before).is_none(), "stale ref must not alias a fresh alloc");
        // r_after is live.
        assert!(slab.get(r_after).is_some());
    }

    /// DEF-148 generational wraparound: after 256 clear cycles on
    /// the same slot, the generation wraps to 0 — a stale ref from
    /// generation 0 would then falsely validate. This collision
    /// window is architecturally dead in the current single-inflight
    /// flow (stale refs don't survive across even a single clear by
    /// design). Test documents the wraparound behaviour for future
    /// audit.
    #[test]
    fn generation_wraps_around_at_256_cycles() {
        let mut slab = SchemaSlab::new();
        let desc = RowDesc::EMPTY;
        let r_gen0 = must_alloc(&mut slab, desc);
        assert_eq!(r_gen0.generation(), 0);

        // 256 alloc/clear cycles to wrap the generation counter.
        // Each iteration asserts the freshly-allocated ref resolves
        // to live RowDesc — gives the `r` binding a meaningful use
        // without `let _ =` (banned per user feedback).
        for _ in 0..256 {
            let r = must_alloc(&mut slab, desc);
            assert!(slab.get(r).is_some(), "fresh alloc must resolve to live desc");
            slab.clear();
        }

        // After 256 cycles, generation for slot 0 is back to 0 (u8
        // wraparound). A fresh alloc captures generation 0 — same
        // as r_gen0's captured generation.
        let r_wrapped = must_alloc(&mut slab, desc);
        assert_eq!(r_wrapped.generation(), 0, "u8 generation wraps at 256");

        // Document the collision: BOTH r_gen0 and r_wrapped validate
        // against slot 0 with generation 0. Architecturally dead in
        // the current flow (r_gen0's lifetime ended long before the
        // 256-cycle wrap); if 1c-5 pipelining reveals a real
        // collision path, promote `generation` to u16.
        assert_eq!(r_gen0.slot_idx(), r_wrapped.slot_idx());
        assert_eq!(r_gen0.generation(), r_wrapped.generation());
        assert!(
            slab.get(r_gen0).is_some(),
            "expected collision: u8 generation wraps to the same value after 256 cycles",
        );
    }
}
