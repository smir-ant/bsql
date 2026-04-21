//! Schema arena — externalised `RowDesc` storage.
//!
//! # DEF-119 rationale
//!
//! Pre-arena, `RowDesc` (260 B POD) lived INLINE across the state
//! machine:
//!
//! - `ProtoState::*StreamingRows { row_desc }` — 260 B inside state.
//! - `ProtoState::*AwaitingRfq { row_desc }` — 260 B.
//! - `StagedAction::StreamRowRange { row_desc }` — 260 B copied per
//!   DataRow (1000-row query = 260 KB of copy traffic).
//! - `Reply::QueryComplete { row_desc: Option<RowDesc> }` — 260 B.
//! - `DescribedRows::Rows(RowDesc)` — 260 B.
//!
//! The arena replaces inline storage with a single-owner slab on
//! `PgProtocol`; state / staged-actions / internal reply payloads
//! carry a 1-byte [`SchemaRef`] index. Users read through the arena
//! at materialise time via a `&'r RowDesc` borrow tied to the
//! `PgProtocol` borrow lifetime — zero extra copies on the hot path.
//!
//! # Size impact
//!
//! | Carrier | Before arena | After arena |
//! |---|---|---|
//! | `ProtoState` | ~1224 B | ~300 B |
//! | `Action::StreamRow` | ~280 B | ~32 B |
//! | `Reply::QueryComplete` payload | ~300 B | ~44 B + `&'r RowDesc` |
//! | Per-row DataRow emission | 260 B copy | 8 B ref |
//!
//! Arena cost: 2 × (1 + 260) padded ≈ 528 B on `PgProtocol`. Paid
//! once per connection; amortised across all queries that connection
//! services.
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
//! # Alloc / free discipline
//!
//! - **Alloc** happens when the server sends schema:
//!   `parse_row_description` success in dispatch → `arena.alloc(desc)`
//!   → `SchemaRef` threaded into the new state variant.
//! - **Clear** happens at **next entry point** (start of the next
//!   `feed_bytes` / `push_command` / `push_bind_execute` call) when
//!   the prior state is `Idle` or `Errored`. This is the earliest
//!   safe moment to reclaim slots: the trailing `ReadyForQuery` from
//!   the previous cycle produced a `Reply<'r>` carrying `&'r RowDesc`
//!   out via `OutActions<'w, 'r>`. That borrow is bound to the
//!   `&mut PgProtocol` the user held — so as soon as `OutActions`
//!   drops (ending `'r`), the next call's `&mut self` can touch
//!   `schema_arena` again. Clearing here guarantees no stale
//!   allocations survive across user queries.
//!
//! Why `clear()` rather than per-ref `free()`: the 2-slot arena
//! serves at most one inflight query at a time (single-inflight
//! invariant pre-1c-5). Every slot occupied post-cycle belongs to
//! the just-completed cycle; blanket clearing is both semantically
//! equivalent and cheaper (no ref tracking). `free()` is retained
//! in the API for future 1c-5 pipelining where multiple concurrent
//! schemas need per-ref release.
//!
//! Tier-2 structural invariant: dispatch only allocs when
//! `parse_row_description` succeeds and the transition targets a
//! schema-bearing state variant; the Idle-entry clear guarantees
//! the arena is empty before each user interaction.

use crate::decode::RowDesc;

/// Maximum arena slot count. See [module-level](self) rationale for why 2.
pub(crate) const MAX_ARENA_SLOTS: usize = 2;

/// Handle into [`SchemaSlab`]. One-byte index; `Copy`, `#[repr(transparent)]`.
///
/// # Lifetime and safety
///
/// `SchemaRef` does NOT carry a lifetime parameter. Rationale: state
/// variants own handles across transitions, and a lifetime-bound
/// handle would couple the state enum to the arena's borrow —
/// impossible, because state transitions happen via `core::mem::take`
/// which moves state out independently.
///
/// The arena borrow is instead bound at the **dereferencing** site:
/// [`SchemaSlab::get`] returns `Option<&'arena RowDesc>`. Invalid
/// refs (e.g., stale after `free`) return `None` — the caller
/// handles the absence explicitly.
///
/// # Invariants maintained by construction
///
/// A `SchemaRef` is **only** constructed via [`SchemaSlab::alloc`]
/// which returns one pointing at an allocated (non-empty) slot.
/// Free-then-use is still possible structurally — mitigated by
/// `get` returning `None` rather than a zeroed `RowDesc`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct SchemaRef(u8);

impl SchemaRef {
    /// Raw slot index (`0..MAX_ARENA_SLOTS`). Crate-internal.
    #[inline]
    pub(crate) const fn index(self) -> u8 {
        self.0
    }

    /// Sentinel `SchemaRef(0)` for forbid-compliant test fixtures
    /// (`assert!(is_some) + .unwrap_or(ZERO)` pattern). Not a valid
    /// handle outside the pattern — dereferencing on an empty slab
    /// returns `None` via [`SchemaSlab::get`]'s out-of-range guard.
    #[cfg(test)]
    pub(crate) const ZERO: Self = Self(0);
}

/// Fixed-slot schema arena on `PgProtocol`.
///
/// See [module-level](self) docstring for full design and
/// alloc/free discipline.
#[derive(Debug)]
pub(crate) struct SchemaSlab {
    /// `None` = free slot, `Some(desc)` = occupied.
    ///
    /// `Option<RowDesc>` here is fine: `RowDesc` doesn't niche-pack,
    /// so each slot is 4 (discriminant + pad) + 260 (`RowDesc`) =
    /// 264 B. Total slab footprint: 2 × 264 = 528 B.
    slots: [Option<RowDesc>; MAX_ARENA_SLOTS],
}

impl SchemaSlab {
    /// Construct an empty slab (all slots free).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: [None; MAX_ARENA_SLOTS],
        }
    }

    /// Allocate a slot for `desc`, returning a handle to it.
    ///
    /// Returns `None` if all slots are occupied — caller treats this
    /// as a structural invariant break (dispatch shouldn't try to
    /// hold more than `MAX_ARENA_SLOTS` schemas simultaneously in
    /// the current no-pipelining flow). Future pipelining (1c-5)
    /// pairs alloc with a capacity check at push time.
    #[inline]
    #[must_use]
    pub(crate) fn alloc(&mut self, desc: RowDesc) -> Option<SchemaRef> {
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(desc);
                // `idx < MAX_ARENA_SLOTS <= 255` proved by the for-bound
                // (`slots.len() == MAX_ARENA_SLOTS`, hard-capped at 2).
                // The `u8::try_from` Err branch is architecturally dead;
                // explicit match satisfies the forbid bundle's no-`as`
                // discipline. LLVM elides the dead Err under opt.
                let Ok(idx_u8) = u8::try_from(idx) else {
                    return None;
                };
                return Some(SchemaRef(idx_u8));
            }
        }
        None
    }

    /// Read the schema at `r`, or `None` if the slot is free /
    /// out-of-range.
    #[inline]
    #[must_use]
    pub(crate) fn get(&self, r: SchemaRef) -> Option<&RowDesc> {
        let idx = usize::from(r.index());
        self.slots.get(idx).and_then(Option::as_ref)
    }

    /// Release the slot at `r`. Safe to call on an already-free
    /// slot (idempotent). Safe to call on an out-of-range index
    /// (no-op).
    ///
    /// Currently test-only — the production discipline uses
    /// [`Self::clear`] at entry points (see module docs). Reserved
    /// for 1c-5 pipelining where per-ref release replaces blanket
    /// clearing.
    #[cfg(test)]
    #[inline]
    pub(crate) fn free(&mut self, r: SchemaRef) {
        let idx = usize::from(r.index());
        if let Some(slot) = self.slots.get_mut(idx) {
            *slot = None;
        }
    }

    /// Clear all slots. Called at entry of the next protocol
    /// interaction when the prior state is `Idle` or `Errored` —
    /// reclaims any schemas carried across the previous cycle.
    #[inline]
    pub(crate) fn clear(&mut self) {
        for slot in &mut self.slots {
            *slot = None;
        }
    }

    /// Count occupied slots. Test-only invariant probe — production
    /// code doesn't need runtime occupancy checks because the
    /// dispatch-layer alloc-on-schema and entry-point-clear
    /// discipline is tier-2 structural (see module docs).
    #[cfg(test)]
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

// Drift pin: total slab size. Two slots × ~264 B each = ~528 B.
// `PgProtocol` size budget in `lib.rs` must track changes here.
const _: () = assert!(
    core::mem::size_of::<SchemaSlab>() <= 540,
    "SchemaSlab size regression — 2 slots × ~264 B each must stay ≤ 540 B. \
     If MAX_ARENA_SLOTS grows, update PgProtocol size budget in lib.rs.",
);

// Drift pin: SchemaRef is 1 byte.
const _: () = assert!(
    core::mem::size_of::<SchemaRef>() == 1,
    "SchemaRef must stay 1 byte (#[repr(transparent)] u8).",
);

// Drift pin: `Option<SchemaRef>` size.
// Plain `u8` uses all 256 values; no niche available, so
// `Option<SchemaRef>` is 2 bytes (1 B discriminant + 1 B value).
// Acceptable — still 263× smaller than `Option<RowDesc>`. Future
// polish (1c-6): swap to `NonZeroU8` with 0 reserved as "no slot"
// and 1..=MAX_ARENA_SLOTS as real indices — would niche-pack
// `Option<_>` to 1 B.
const _: () = assert!(
    core::mem::size_of::<Option<SchemaRef>>() <= 2,
    "Option<SchemaRef> must stay ≤ 2 B (bare-u8 discriminant — acceptable).",
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
    /// `assert!` if alloc returns `None` — the `unwrap_or(SchemaRef(0))`
    /// fallback is unreachable in correct tests.
    fn must_alloc(slab: &mut SchemaSlab, desc: RowDesc) -> SchemaRef {
        let r = slab.alloc(desc);
        assert!(r.is_some(), "alloc must succeed on slab with free slot, got {r:?}");
        r.unwrap_or(SchemaRef(0))
    }

    /// Invariant (spec): fresh slab has zero occupied slots; `get`
    /// returns `None` for any ref.
    #[test]
    fn fresh_slab_is_empty() {
        let slab = SchemaSlab::new();
        assert_eq!(slab.occupied_count(), 0);
        assert!(slab.get(SchemaRef(0)).is_none());
        assert!(slab.get(SchemaRef(1)).is_none());
    }

    /// Invariant (spec): alloc uses slots in order, returns indices
    /// 0, 1, then `None` when full.
    #[test]
    fn alloc_fills_in_order_then_returns_none() {
        let mut slab = SchemaSlab::new();
        let desc = RowDesc::EMPTY;

        let r0 = must_alloc(&mut slab, desc);
        assert_eq!(r0.index(), 0);
        assert_eq!(slab.occupied_count(), 1);

        let r1 = must_alloc(&mut slab, desc);
        assert_eq!(r1.index(), 1);
        assert_eq!(slab.occupied_count(), 2);

        // Full — next alloc returns None.
        assert!(slab.alloc(desc).is_none());
        assert_eq!(slab.occupied_count(), 2);
    }

    /// Invariant (spec): `get` returns the stored value; `free`
    /// makes the slot available again.
    #[test]
    fn alloc_get_free_round_trip() {
        let mut slab = SchemaSlab::new();
        let desc = RowDesc::EMPTY;

        let r = must_alloc(&mut slab, desc);
        assert!(slab.get(r).is_some());

        slab.free(r);
        assert!(slab.get(r).is_none());
        assert_eq!(slab.occupied_count(), 0);

        // Slot 0 free → next alloc reuses it.
        let r2 = must_alloc(&mut slab, desc);
        assert_eq!(r2.index(), 0);
    }

    /// Invariant (spec): double-free is idempotent (no panic, no
    /// state change beyond the first free).
    #[test]
    fn double_free_is_idempotent() {
        let mut slab = SchemaSlab::new();
        let r = must_alloc(&mut slab, RowDesc::EMPTY);
        slab.free(r);
        slab.free(r); // no-op
        assert_eq!(slab.occupied_count(), 0);
    }

    /// Invariant (spec): out-of-range ref resolves to `None`, free
    /// is a no-op.
    #[test]
    fn out_of_range_ref_is_safe() {
        let mut slab = SchemaSlab::new();
        let bad = SchemaRef(255);
        assert!(slab.get(bad).is_none());
        slab.free(bad); // no-op
        assert_eq!(slab.occupied_count(), 0);
    }

    /// Invariant (spec): `clear` zeroes all slots (both reachable
    /// via their pre-clear handles, both unreachable post-clear).
    #[test]
    fn clear_empties_all_slots() {
        let mut slab = SchemaSlab::new();
        let first = must_alloc(&mut slab, RowDesc::EMPTY);
        let second = must_alloc(&mut slab, RowDesc::EMPTY);
        assert_ne!(first.index(), second.index());
        assert!(slab.get(first).is_some());
        assert!(slab.get(second).is_some());
        assert_eq!(slab.occupied_count(), 2);
        slab.clear();
        assert_eq!(slab.occupied_count(), 0);
        assert!(slab.get(first).is_none());
        assert!(slab.get(second).is_none());
    }
}
