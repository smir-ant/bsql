//! Tier-1 within-crate `param_oids_slot` write provenance via
//! concrete-token + Cell newtype.
//!
//! # Architecture (mirror of [`crate::schema_slot::RowDescSlotCell`])
//!
//! [`ParamOids`] is parsed once on the inbound `'t'`
//! (`ParameterDescription`) frame after a `DescribeStatement` push, then
//! observed by [`materialise`](crate::protocol::PgProtocol::feed_bytes)
//! on the trailing `'Z'` (`ReadyForQuery`) frame that closes the
//! Describe cycle. The cell holds the parsed value across the two-frame
//! window (`'t' → ('T' | 'n') → 'Z'`). Per-Describe-cycle lifecycle:
//!
//! 1. `'t'` arrival → `park_at_param_desc_dispatch` mints the box and
//!    parks it. State transitions to
//!    [`crate::state::ProtoState::DescribeStatementAwaitingRowDescOrNoData`]
//!    — variant carries ONLY the correlator (bare `ReplyId`).
//! 2. `'T'`/`'n'` arrival → no slot interaction. State transitions to
//!    [`crate::state::ProtoState::DescribeStatementAwaitingRfq`].
//! 3. `'Z'` arrival → `materialise` reads slot via `as_ref()` and emits
//!    [`crate::action::DescribeStatementCompletePayload`] with
//!    `param_oids: &'r ParamOids` borrowed from the slot.
//! 4. Next Idle/Errored entry → `clear_at_residue` empties the slot.
//!
//! # Two structural mechanisms close the within-crate write surface
//!
//! 1. **`ParamOidsSlotCell` newtype** wraps the inner
//!    `Option<alloc::boxed::Box<ParamOids>>` with a PRIVATE `inner`
//!    field (private to `mod param_oids_slot`). The field is
//!    unreachable even from `mod protocol` — direct
//!    `*self.param_oids_slot.inner = ...` does not compile. The only
//!    paths to the inner value are read-only methods (`as_ref`,
//!    `is_some`, `is_none`) and write-methods that require a
//!    per-call-site **token** (private-field tuple struct).
//!
//! 2. **Per-leaf concrete-type tokens** are the seal. Each leaf
//!    submodule defines its own token type (`pub(crate) struct
//!    ParamDescDispatchToken(())` etc.) with a PRIVATE field. The
//!    token's struct-literal mint `Self(())` is callable ONLY inside
//!    the defining leaf submodule — the field's privacy is the seal,
//!    no trait, no sealed-supertrait. Each `ParamOidsSlotCell` write
//!    method takes a CONCRETE token type by value (consumed by the
//!    call):
//!    `park_at_param_desc_dispatch(&mut self, oids, _token: …)`.
//!
//! Identical to `RowDescSlotCell`'s concrete-token shape — closes the
//! within-crate write surface (tier-1 by-construction everywhere).
//!
//! # Tier-1 closure (within-crate, by-construction)
//!
//! Hostile attempts:
//!
//! - `slot.inner = ...` from anywhere outside `mod param_oids_slot`:
//!   FAILS — `inner` is private to this module.
//! - `slot.park_at_param_desc_dispatch(oids, ParamDescDispatchToken(()))`
//!   from anywhere outside `_param_description_dispatch_leaf`: FAILS —
//!   token's `()` field is private to the leaf submodule.
//! - `impl HostileTrait for Whatever`: NO TRAIT EXISTS to bypass. The
//!   sealed-trait + auth-tag pattern is gone; concrete-type tokens
//!   replace it.
//! - `slot.park_at_param_desc_dispatch(oids, X)` with `X !=
//!   ParamDescDispatchToken`: FAILS — Rust type system rejects
//!   parameter mismatch.
//!
//! The only paths to mutate the slot are:
//! - `_param_description_dispatch_leaf::park_param_oids_at_dispatch`
//!   (in `mod dispatch`) → mints `ParamDescDispatchToken(())` → calls
//!   `slot.park_at_param_desc_dispatch(oids, token)`.
//! - `_clear_residue_leaf::clear_param_oids_slot_residue` → mints
//!   `ClearResidueParamOidsToken(())` → calls
//!   `slot.clear_at_residue(token)`.
//!
//! # Heap-boxed `ParamOids` rationale
//!
//! Inline `Option<ParamOids>` is `Some(68 B) + 1 B disc + 3 B pad =
//! 72 B` per [`crate::protocol::PgProtocol<crate::ActivePhase>`]. Boxed
//! `Option<Box<ParamOids>>` is 8 B niche-packed slot + lazy 68 B heap
//! only when a Describe-statement is in flight (~< 1% of connection
//! lifetime on OLTP workloads). Saves ~64 B per active connection in
//! the no-Describe steady state.
//!
//! The DEF-282 lesson (per
//! [[feedback_box_state_outliers]] / [[bsql_perf_measurement]]):
//! Pareto frontier favours Box-reuse for state outliers even at 68 B —
//! the cancel_credentials bench showed −30.95% from the same boxing
//! pattern. Re-applied here at the slot layer.
//!
//! # Test-only setter
//!
//! [`ParamOidsSlotCell::_set_for_test`] is `#[cfg(test)]`-gated and
//! lets `mod protocol`'s residue-cleanup tests pre-populate the slot
//! with a synthetic `ParamOids` before exercising residue-cleanup
//! transitions. Production binaries do not see this method (the cfg
//! gate strips it). External crates cannot reach it regardless (cell
//! is `pub(crate)` only).
//!
//! # Bench cost
//!
//! The Cell is `#[repr(transparent)]` over
//! `Option<alloc::boxed::Box<ParamOids>>`. Read methods (`as_ref`,
//! `is_some`, etc.) compile to the same code as the bare `Option`
//! accessors. Write methods are a single field assignment plus a no-op
//! token consume; LLVM erases the token (zero-sized type). 0 ns / 0 B
//! perf cost vs the pre-refactor inline shape on this slot itself; the
//! +64 B savings on PgProtocol size are the net win.

use crate::action::ParamOids;

/// Tier-1 within-crate write provenance for the protocol's parked
/// `ParameterDescription` payload. Wraps
/// `Option<alloc::boxed::Box<ParamOids>>` with a PRIVATE inner field;
/// writes require per-leaf concrete-type tokens (see module-level
/// docs).
///
/// `#[repr(transparent)]` so the layout is identical to the bare
/// `Option<Box<ParamOids>>` — `mem::size_of::<ParamOidsSlotCell>() ==
/// mem::size_of::<Option<Box<ParamOids>>>() == 8` (niche-packed via
/// `Box`'s non-null pointer).
///
/// `pub` visibility (required so the type can appear as a field on
/// `<protocol::ActivePhase as protocol::SealedPhase>::Extras`). The
/// `inner` field stays private — external code cannot construct or
/// observe the cell's contents except via the token-gated
/// `pub(crate)` constructor + read-only `as_ref` projection.
#[allow(
    missing_copy_implementations,
    missing_debug_implementations,
    reason = "`Copy` is BANNED on the cell — the field-write protocol \
              (token-gated `park_at_*` / `clear_at_*`) would be \
              subvertable by mass-copying. `Debug` is suppressed \
              because `ParamOids` itself prints OID metadata callers \
              may not want exposed via `{:?}`; production code observes \
              via `as_ref()` and projects through the typed accessor."
)]
#[repr(transparent)]
pub struct ParamOidsSlotCell {
    inner: Option<alloc::boxed::Box<ParamOids>>,
}

impl ParamOidsSlotCell {
    /// Construct a fresh empty cell. Token-gated to
    /// [`crate::protocol::_proto_init_leaf::ProtoInitToken`] — that's
    /// the only mint site (private to that leaf submodule which also
    /// hosts the sole legitimate caller, `PgProtocol::new`). Closes
    /// wholesale-replacement (`*cell = ParamOidsSlotCell::empty(...)`)
    /// to the leaf by construction.
    ///
    /// The token is consumed (ZST, erased by LLVM); non-init code paths
    /// must use the token-gated `park_at_*` / `clear_at_*` methods
    /// which mutate in-place without producing a fresh cell.
    #[inline]
    #[must_use]
    pub(crate) const fn empty(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: None }
    }

    /// Borrow the inner ParamOids, if present. Read-only — no token
    /// needed. Used by materialise (action.rs) — projects through to
    /// `Reply::DescribeStatementComplete.param_oids: &'r ParamOids`.
    #[inline]
    #[must_use]
    pub(crate) fn as_ref(&self) -> Option<&ParamOids> {
        // `Option::as_deref` projects `Option<Box<T>>` → `Option<&T>`
        // without per-call allocation. Tier-1 by `Box::deref` const-
        // identity.
        self.inner.as_deref()
    }

    /// Returns `true` if the slot is populated. Read-only. Currently
    /// only used by `cfg(test)` residue-cleanup fixtures; production
    /// callers project `as_ref()` directly.
    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Test-only accessor — mirror of `RowDescSlotCell::is_some`. \
                  Currently unused (tests project `as_ref()`); kept for \
                  parallel-pattern symmetry with the row_desc slot so \
                  future residue-cleanup tests can use either projection."
    )]
    #[inline]
    #[must_use]
    pub(crate) fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    /// Returns `true` if the slot is empty. Read-only. Currently
    /// only used by `cfg(test)` residue-cleanup fixtures; production
    /// callers project `as_ref()` directly.
    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Test-only accessor — mirror of `RowDescSlotCell::is_none`. \
                  Currently unused (tests project `as_ref()`); kept for \
                  parallel-pattern symmetry."
    )]
    #[inline]
    #[must_use]
    pub(crate) fn is_none(&self) -> bool {
        self.inner.is_none()
    }

    /// Park `oids` from the inbound `'t'` (ParameterDescription) frame
    /// dispatch. The token's mint is gated to
    /// `_param_description_dispatch_leaf`.
    ///
    /// The arg is `Box<ParamOids>` (not `ParamOids` by value) so the
    /// caller decides where the allocation lives — dispatch boxes once
    /// at `'t'` parse time, hands the Box here, slot owns the heap.
    #[inline]
    pub(crate) fn park_at_param_desc_dispatch(
        &mut self,
        oids: alloc::boxed::Box<ParamOids>,
        _token: crate::dispatch::_param_description_dispatch_leaf::ParamDescDispatchToken,
    ) {
        self.inner = Some(oids);
    }

    /// Clear the slot at the residue-cleanup transition (Idle/Errored
    /// entry). The token's mint is gated to `_clear_residue_leaf`.
    ///
    /// Drops the boxed `ParamOids` if any. ParamOids contains only
    /// `u32` OIDs — no ZeroizeOnDrop targets, drop is a single
    /// allocator free.
    #[inline]
    pub(crate) fn clear_at_residue(
        &mut self,
        _token: crate::protocol::_clear_residue_leaf::ClearResidueParamOidsToken,
    ) {
        self.inner = None;
    }

    /// Test-only setter. `#[cfg(test)]`-gated — production binaries
    /// don't expose this. Used by `mod tests` in protocol.rs to
    /// pre-populate the slot with synthetic `ParamOids` before
    /// exercising residue-cleanup transitions.
    ///
    /// Takes `Option<ParamOids>` (not `Option<Box<_>>`) — the helper
    /// boxes internally so tests pass POD values. Matches the
    /// `_set_for_test` shape on `RowDescSlotCell`.
    #[cfg(test)]
    #[inline]
    pub(crate) fn _set_for_test(&mut self, value: Option<ParamOids>) {
        self.inner = value.map(alloc::boxed::Box::new);
    }
}
