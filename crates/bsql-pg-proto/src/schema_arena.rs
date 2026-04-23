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
//! `Option<SchemaRef>::None` niche).
//!
//! DEF-171 (audit2 A002) follow-up: the initial DEF-148 design
//! carried a `has_any: bool` fast-path field on `SchemaSlab` to
//! short-circuit `clear()` on the Ping-loop hot case. DEF-171
//! deletes that field — it was a derived-state fallback with 6
//! mutation sites, no cross-check test, and a silent-corruption
//! failure mode (has_any=false while occupied). Post-DEF-171
//! `clear()` walks the 2-slot array directly; the Ping-loop hot
//! case (all slots free) hits the `is_some` check at each slot
//! and branches over — effectively a no-op, equivalent machine
//! code to the pre-DEF-171 bool-load.
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
//! Arena cost: 2 × (~264 B slot) + 2 × 1 B (generations) + padding
//! ≈ 530 B on `PgProtocol` (DEF-171 dropped 1 B has_any + alignment
//! pad vs DEF-148's ~536 B). Paid once per connection; amortised
//! across all queries that connection services.
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
//! - **Fast-path (Ping-loop hot case)**: `clear()` walks the 2
//!   slots; each `is_some()` check branches over a free slot
//!   without storing. Under LLVM the branch is one cycle per slot,
//!   effectively a no-op — equivalent to the pre-DEF-171
//!   `has_any: bool` fast-path but without the derived-field
//!   invariant surface.
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
    #[must_use]
    pub(crate) const fn dead_for_test() -> Self {
        // DEF-178 (audit2 A039) — `const fn` promotion.
        // `Option::unwrap_or` is NOT yet const-stable
        // (tracking: rust-lang/rust#143874, RU-01 in deferred.md
        // § Rust-unstable watchlist). Use explicit `match` instead —
        // `NonZeroU8::new` is const; `match` over Option is const;
        // `NonZeroU8::MIN` is const. Together they produce a
        // const-evaluable factory.
        //
        // Enables `const DEAD: SchemaRef = SchemaRef::dead_for_test();`
        // bindings in test code. `NonZeroU8::new(1)` always returns
        // Some so the `_` branch is architecturally dead.
        let slot = match NonZeroU8::new(1) {
            Some(nz) => nz,
            None => NonZeroU8::MIN,
        };
        Self { slot, generation: 0 }
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
    /// `u8` wraps at 256 cycles. Under single-inflight (pre-1c-5)
    /// the wrap is architecturally unreachable — a SchemaRef
    /// cannot be live when `clear()` runs:
    ///
    /// - SchemaRef lives only in mid-query ProtoState variants,
    ///   transient StagedAction / StagedReply during one
    ///   feed_bytes call, and resolved `&'r RowDesc` borrows in
    ///   materialised OutActions.
    /// - `clear()` is invoked only at entry-point boundaries with
    ///   `state ∈ {Idle, Errored}`, neither of which carries a
    ///   SchemaRef.
    /// - The borrow checker forces OutActions's `'r` to end before
    ///   the next `&mut self` call, so the next `clear()` sees no
    ///   live `&'r RowDesc` either.
    ///
    /// The generation counter is defence-in-depth for two classes:
    /// 1. Crate bugs that leak a SchemaRef beyond its architectural
    ///    lifetime (currently no known path exists; catching the
    ///    drift at arena.get() is tier-2 safety net).
    /// 2. 1c-5 pipelining — concurrent queries each holding refs
    ///    make the stale class real. At that point the u8 horizon
    ///    may be too tight; audit2 A008 flagged the lift as
    ///    deferred (revisit during H021 witness-guard session).
    ///
    /// DEF-180 deleted the `generation_wraps_around_at_256_cycles`
    /// test which exercised the wrap via manual stale-ref holding —
    /// architecturally-impossible scenario under current flow, so
    /// the test was testing dead behaviour. Core detection path
    /// still pinned by `stale_ref_across_clear_resolves_to_none`.
    generations: [u8; MAX_ARENA_SLOTS],
}

impl SchemaSlab {
    /// Construct an empty slab (all slots free, all generations 0).
    #[inline]
    #[must_use]
    pub(crate) const fn new() -> Self {
        Self {
            slots: [None; MAX_ARENA_SLOTS],
            generations: [0_u8; MAX_ARENA_SLOTS],
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
        // DEF-178 (audit2 A019): paired iter eliminates the redundant
        // `generations.get(idx).copied().unwrap_or(0)` lookup on the
        // proven-valid idx. slots and generations have equal length
        // by struct construction.
        for ((idx, slot), generation) in self
            .slots
            .iter_mut()
            .enumerate()
            .zip(self.generations.iter().copied())
        {
            if slot.is_none() {
                *slot = Some(desc);
                // `idx < MAX_ARENA_SLOTS ≤ 254` (const-asserted
                // above) so `idx + 1` fits u8 unconditionally.
                // Explicit try_from satisfies the forbid bundle's
                // ban on `as` conversions; LLVM elides the dead
                // Err branch.
                let idx_u8 = u8::try_from(idx).ok()?;
                let slot_nz = NonZeroU8::new(idx_u8.saturating_add(1))?;
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
        //
        // DEF-178 (audit2 A009) — LOAD-BEARING guard: the
        // `&& slot.is_some()` ties the generation bump to the
        // physical slot transition (occupied → free). Without it,
        // double-free would bump the generation twice, invalidating
        // refs that the user might still be holding from the last
        // alloc. Do NOT split this into two sequential `if let`s in
        // a future refactor — the coupling is what makes
        // double-free idempotent per
        // `double_free_is_idempotent` test.
        if let Some(slot) = self.slots.get_mut(idx)
            && slot.is_some()
        {
            *slot = None;
            if let Some(gen_slot) = self.generations.get_mut(idx) {
                *gen_slot = gen_slot.wrapping_add(1);
            }
        }
    }

    /// Clear all slots. Called at entry of the next protocol
    /// interaction when the prior state is `Idle` or `Errored` —
    /// reclaims any schemas carried across the previous cycle.
    ///
    /// DEF-171 (audit2 A002): the pre-DEF-171 `has_any: bool` fast-
    /// path was DELETED as a "fallback-with-shadow-correctness"
    /// pattern — 6 mutation sites had to preserve the invariant
    /// `has_any == slots.iter().any(is_some)` with no cross-check
    /// test, and the has_any=false-while-occupied failure mode
    /// would silently survive a stale schema across clears. The
    /// post-DEF-171 form walks the slots directly; on the 2-slot
    /// arena the walk is 2 load+compare ops — indistinguishable
    /// from the pre-DEF-171 byte-load under LLVM opt, and the
    /// Ping-loop hot case (slots all None) still hits the fast
    /// path (the for-loop skips every slot with the is_some check,
    /// NO memset). Net: same perf, one derived field eliminated,
    /// 6 mutation-site invariant closed structurally.
    ///
    /// DEF-148 generation semantics preserved: each occupied slot
    /// has its generation bumped, invalidating any pre-clear
    /// `SchemaRef` via generation-mismatch at `get()`.
    #[inline]
    pub(crate) fn clear(&mut self) {
        // Walk occupied slots: clear each, bump its generation.
        // Free slots stay at their current generation (no ref
        // outstanding to invalidate). On the Ping-loop hot case
        // (all slots free) this is two is_some() branches, no
        // stores — effectively a no-op, same as the deleted
        // has_any fast-path.
        for (slot, gen_slot) in self.slots.iter_mut().zip(self.generations.iter_mut()) {
            if slot.is_some() {
                *slot = None;
                *gen_slot = gen_slot.wrapping_add(1);
            }
        }
    }

    /// Count occupied slots. Used as a `debug_assert_eq!` probe at
    /// each `clear()` call site (DEF-152) to pin "clear() must leave
    /// arena empty" against future drift, and in tests for slot-
    /// occupancy invariants.
    ///
    /// # Cfg footprint
    ///
    /// The pre-DEF-154 (C) form gated this `#[cfg(debug_assertions)]`,
    /// which released builds type-check-failed because `debug_assert_eq!`
    /// expands to `if cfg!(debug_assertions) { assert_eq!(args) }` —
    /// the `args` still need to typecheck in release, and a cfg-gated-
    /// out method is not in scope. Post-DEF-154 (C)/DEF-181 the fn is
    /// always present; LLVM's dead-code elimination removes the
    /// single release-unused call site (the DEF-152 debug_assert_eq
    /// probe inside [`crate::protocol::PgProtocol::clear_arena_if_idle_or_errored`]
    /// optimises to nothing, so the counted result is unused → call
    /// is DCE'd). Net: same release-mode cost, compiles cleanly.
    ///
    /// # Adding callers
    ///
    /// A future caller OUTSIDE a `debug_assert*!` context would make
    /// the call non-DCE'd — still correct (the fn is side-effect-free,
    /// ~6 cycles on the 2-slot arena) but the DCE-cost argument
    /// above no longer applies. Either extend this docstring or
    /// ensure the new caller is also debug-only.
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

    /// Construct a read-only view for a materialise-phase caller.
    ///
    /// DEF-154 (C) witness-pattern: materialise only needs to
    /// resolve `SchemaRef → &'r RowDesc`. Exposing the full
    /// `&'r SchemaSlab` there would grant access to internals
    /// `get()` transitively reveals (nothing in the current API
    /// surface, but a future refactor could add a `slot_is_empty`
    /// or similar that drifts into the materialise path). The
    /// [`ArenaReader<'r>`] wrapper narrows the API to `get()`
    /// alone — tier-2 structural: materialise can no longer call
    /// `alloc` / `clear` / `free`, even accidentally, because the
    /// type simply does not expose them.
    #[inline]
    #[must_use]
    pub(crate) fn as_reader(&self) -> ArenaReader<'_> {
        ArenaReader { slab: self }
    }

    /// Construct a write-only view for a dispatch-phase caller.
    ///
    /// DEF-154 (C) witness-pattern counterpart to [`Self::as_reader`].
    /// Dispatch only needs to allocate schemas on RowDescription
    /// success; it never needs to `get`, `clear`, or `free`.
    /// [`ArenaWriter<'a>`] narrows the API to `alloc()` alone —
    /// tier-2 structural: dispatch can no longer accidentally read
    /// or reclaim slots, even through a future refactor, because
    /// the type simply does not expose those methods.
    ///
    /// # Rationale over `&mut SchemaSlab`
    ///
    /// The direct borrow grants the full surface (`alloc` + `get` +
    /// `clear` + `free`) to dispatch. The only operation dispatch
    /// performs is `alloc`. Narrowing via the writer witness turns
    /// "we discipline ourselves to only call alloc" from a tier-3
    /// code-review invariant into a tier-2 type-system one.
    #[inline]
    #[must_use]
    pub(crate) fn as_writer(&mut self) -> ArenaWriter<'_> {
        ArenaWriter { slab: self }
    }
}

/// Materialise-phase view of the [`SchemaSlab`] — read-only.
///
/// DEF-154 (C) witness-pattern. Wraps `&'r SchemaSlab` and exposes
/// only the `get` operation, narrowing the materialise call site's
/// access to the one method it actually uses.
///
/// # Copy
///
/// `Copy` is intentional: the underlying `&'r SchemaSlab` is `Copy`
/// (all shared references are), so [`ArenaReader<'r>`] is
/// zero-cost-copyable. Materialise passes the reader to
/// sub-resolvers (`StagedReply::into_public`,
/// `described_rows_ref_into_public`) by value without explicit
/// cloning.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ArenaReader<'r> {
    slab: &'r SchemaSlab,
}

impl<'r> ArenaReader<'r> {
    /// Resolve a [`SchemaRef`] to its `&'r RowDesc` borrow, or
    /// `None` for a stale / out-of-range / unoccupied handle.
    ///
    /// See [`SchemaSlab::get`] for the generation-match semantics.
    /// The returned borrow inherits `'r` from the wrapped slab
    /// reference — callers can propagate it into `OutActions<'w, 'r>`
    /// payloads as before (same lifetime, narrower API surface).
    #[inline]
    #[must_use]
    pub(crate) fn get(&self, r: SchemaRef) -> Option<&'r RowDesc> {
        // `self.slab: &'r SchemaSlab` — `&` is `Copy`, so field
        // access through `&self` projects out the full `'r`
        // lifetime. Calling `.get(r)` on the resulting `&'r`
        // reference returns `Option<&'r RowDesc>` (method signature
        // elided lifetime ties to the receiver's lifetime).
        self.slab.get(r)
    }
}

/// Dispatch-phase view of the [`SchemaSlab`] — alloc-only.
///
/// DEF-154 (C) witness-pattern. Wraps `&'a mut SchemaSlab` and
/// exposes only the `alloc` operation, preventing the dispatch
/// path from accidentally reading, clearing, or freeing slots.
#[derive(Debug)]
pub(crate) struct ArenaWriter<'a> {
    slab: &'a mut SchemaSlab,
}

impl ArenaWriter<'_> {
    /// Allocate a slot for `desc`, returning a handle.
    ///
    /// See [`SchemaSlab::alloc`] for the full semantics (slot order,
    /// generation capture, `None` on full arena).
    #[inline]
    #[must_use]
    pub(crate) fn alloc(&mut self, desc: RowDesc) -> Option<SchemaRef> {
        self.slab.alloc(desc)
    }
}

impl Default for SchemaSlab {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

// DEF-151 + DEF-171 drift pin: total slab size. 2 slots × ~264 B
// each + generations (2 × u8) + padding. Post-DEF-171 (has_any
// deleted): slab shrinks by 1 B + alignment pad. Actual ~520 B on
// aarch64-apple-darwin; range [512, 544] tolerates cross-platform
// alignment.
const _: () = assert!(
    core::mem::size_of::<SchemaSlab>() >= 512
        && core::mem::size_of::<SchemaSlab>() <= 544,
    "SchemaSlab size drift — post-DEF-171 actual ~520 B (has_any \
     deleted). Range [512, 544] tolerates cross-platform alignment. \
     If MAX_ARENA_SLOTS grows, update PgProtocol size budget in \
     lib.rs in lockstep.",
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

// DEF-154 (C) drift pins: the witness wrappers must stay
// pointer-sized. `ArenaReader<'r>` wraps `&'r SchemaSlab` (a thin
// reference — `SchemaSlab` is `Sized`, so the reference has no
// metadata on any supported target); `ArenaWriter<'a>` wraps
// `&'a mut SchemaSlab` (also thin for the same reason). On all
// supported targets both collapse to one usize. A future refactor
// that adds generation / brand fields — or (hypothetically)
// switches to `dyn Trait` storage that would force a fat reference
// — would trip these pins first, forcing a perf-impact review at
// the materialise / dispatch call sites before lifting.
const _: () = assert!(
    core::mem::size_of::<ArenaReader<'_>>() == core::mem::size_of::<usize>(),
    "ArenaReader must stay pointer-sized (thin &SchemaSlab wrapper; SchemaSlab is Sized).",
);
const _: () = assert!(
    core::mem::size_of::<ArenaWriter<'_>>() == core::mem::size_of::<usize>(),
    "ArenaWriter must stay pointer-sized (thin &mut SchemaSlab wrapper; SchemaSlab is Sized).",
);

// DEF-183 (P1-B from Senior audit): compile-time Copy pin for
// ArenaReader. The `reader_witness_is_copy` test below pins Copy
// behaviourally (assignment without move); this const check pins
// Copy at the *trait* level. A future refactor that adds a
// non-Copy field (e.g., a generative-brand `PhantomData<fn(&'a ())
// -> &'a ()>` — still Copy, fine — but a real non-Copy payload)
// would fail compilation here with a clear trait-bound error, not
// a confusing move-after-use error at the behavioural test site.
const _: fn() = || {
    const fn assert_copy<T: Copy>() {}
    assert_copy::<ArenaReader<'_>>();
};

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

        slab.clear();
        assert_eq!(slab.occupied_count(), 0);
        // Both pre-clear refs are now stale.
        assert!(slab.get(first).is_none());
        assert!(slab.get(second).is_none());
    }

    /// DEF-148 semantics: clear() on already-empty slab doesn't
    /// bump any generation (no occupied slots to invalidate).
    /// Post-DEF-171 (has_any removed): the walk still skips all
    /// empty slots without storing — behaviour preserved.
    #[test]
    fn clear_on_empty_slab_is_noop() {
        let mut slab = SchemaSlab::new();
        assert_eq!(slab.occupied_count(), 0);
        let gens_before: [u8; MAX_ARENA_SLOTS] = slab.generations;
        slab.clear();
        assert_eq!(slab.generations, gens_before, "clear on empty must not bump generations");
        assert_eq!(slab.occupied_count(), 0);
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

    /// DEF-154 (C): writer witness forwards alloc to the slab,
    /// preserving slot-order + generation capture semantics.
    /// Pins the forwarding contract against future drift (e.g., a
    /// refactor that accidentally filters or retries alloc at the
    /// wrapper layer).
    #[test]
    fn writer_witness_alloc_matches_direct_alloc() {
        let mut slab_a = SchemaSlab::new();
        let mut slab_b = SchemaSlab::new();
        let desc = RowDesc::EMPTY;

        // Direct-alloc reference trace.
        let direct = must_alloc(&mut slab_a, desc);

        // Writer-path alloc.
        let witness = {
            let mut writer = slab_b.as_writer();
            writer.alloc(desc).unwrap_or_else(SchemaRef::dead_for_test)
        };

        assert_eq!(direct.slot_idx(), witness.slot_idx(), "writer must follow same slot order");
        assert_eq!(direct.generation(), witness.generation(), "writer must capture same generation");
        assert_eq!(slab_a.occupied_count(), slab_b.occupied_count());
    }

    /// DEF-154 (C): reader witness forwards get with the full
    /// `'r` lifetime of the wrapped slab — propagating the borrow
    /// correctly for OutActions payloads.
    #[test]
    fn reader_witness_get_yields_live_desc() {
        let mut slab = SchemaSlab::new();
        let r = must_alloc(&mut slab, RowDesc::EMPTY);
        let reader = slab.as_reader();
        assert!(reader.get(r).is_some(), "live ref must resolve through reader");
    }

    /// DEF-154 (C): reader returns `None` for stale refs (generation
    /// mismatch after clear), matching the underlying SchemaSlab::get
    /// contract.
    #[test]
    fn reader_witness_stale_ref_returns_none() {
        let mut slab = SchemaSlab::new();
        let stale = must_alloc(&mut slab, RowDesc::EMPTY);
        slab.clear();
        // Fresh alloc re-uses slot 0 with bumped generation — asserted
        // so the test fails loudly if the arena's slot-reuse contract
        // regresses. The result itself is unused beyond the assert.
        let fresh = must_alloc(&mut slab, RowDesc::EMPTY);
        assert_eq!(fresh.slot_idx(), stale.slot_idx(), "fresh must reuse same physical slot");
        let reader = slab.as_reader();
        assert!(reader.get(stale).is_none(), "reader must surface stale-ref None");
        assert!(reader.get(fresh).is_some(), "fresh ref must resolve through reader");
    }

    /// DEF-154 (C): reader is Copy — callers can pass by value to
    /// sub-resolvers (StagedReply::into_public,
    /// described_rows_ref_into_public) without explicit cloning.
    /// Pins the Copy derive against accidental removal.
    #[test]
    fn reader_witness_is_copy() {
        let mut slab = SchemaSlab::new();
        let r = must_alloc(&mut slab, RowDesc::EMPTY);
        let reader_a = slab.as_reader();
        // Copy, not move — `reader_a` stays usable after the bind.
        let reader_b = reader_a;
        assert!(reader_a.get(r).is_some(), "reader_a stays usable after Copy");
        assert!(reader_b.get(r).is_some(), "reader_b is a valid copy");
    }

    // DEF-180: the pre-DEF-180 `generation_wraps_around_at_256_cycles`
    // test was DELETED. It exercised the u8 wrap behavior by manually
    // holding a SchemaRef across 256 clear() calls — a scenario
    // architecturally impossible in production code:
    //
    // - SchemaRef lives only in: (a) mid-query ProtoState variants,
    //   (b) StagedAction within one feed_bytes call, (c) StagedReply
    //   within materialise scope. In all three, its lifetime is
    //   upper-bounded by the OutActions 'r that the borrow checker
    //   forces to end before the next `&mut self` entry point.
    // - `clear()` runs only when state ∈ {Idle, Errored}, neither
    //   of which carries a SchemaRef.
    // - Therefore: no SchemaRef can be live when clear() runs, so
    //   the 256-cycle-wrap-while-holding-stale-ref collision cannot
    //   materialise under intact code.
    //
    // The generation counter is retained as defence-in-depth for
    // crate-bug scenarios (a refactor that somehow leaks a
    // SchemaRef beyond its architectural lifetime) and for 1c-5
    // pipelining prep (concurrent refs will make the stale-ref
    // class a real possibility, at which point the u8 horizon may
    // need widening — see A008 marker in audit2.txt).
    //
    // `stale_ref_across_clear_resolves_to_none` above still exercises
    // the generation detection path (manually holding a ref across
    // one clear() — still synthetic but it pins the core detection
    // mechanism rather than the u8-specific wrap edge).
}
