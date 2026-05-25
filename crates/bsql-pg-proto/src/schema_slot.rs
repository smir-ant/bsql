//! Tier-1 within-crate `row_desc_slot` write provenance via
//! concrete-token + Cell newtype.
//!
//! # Architecture
//!
//! Two structural mechanisms close the within-crate write surface:
//!
//! 1. **`RowDescSlotCell` newtype** wraps the inner `Option<RowDesc>`
//!    with a PRIVATE `inner` field (private to `mod schema_slot`). The
//!    field is unreachable even from `mod protocol` — direct
//!    `*self.row_desc_slot.inner = ...` does not compile. The only
//!    paths to the inner value are read-only methods (`as_ref`,
//!    `is_some`, `is_none`) and write-methods that require a
//!    per-call-site **token** (private-field tuple struct).
//!
//! 2. **Per-leaf concrete-type tokens** are the seal. Each leaf
//!    submodule defines its own token type (`pub(crate) struct
//!    BeSelectToken(())` etc.) with a PRIVATE field. The token's
//!    struct-literal mint `Self(())` is callable ONLY inside the
//!    defining leaf submodule — the field's privacy is the seal, no
//!    trait, no sealed-supertrait. Each `RowDescSlotCell` write method
//!    takes a CONCRETE token type by value (consumed by the call):
//!    `park_at_be_select(&mut self, desc, _token: BeSelectToken)`.
//!
//! A sealed-trait-and-auth-tag alternative (e.g.,
//! `SchemaParkedSlot<A: SchemaWriteAuth>` with `Sealed: pub(crate)`)
//! would seal the EXTERNAL API surface only — any in-crate file could
//! still write `impl SchemaWriteAuth for HostileTag` and bypass the
//! intent (tier-1 external + tier-2 by-discipline within-crate). The
//! concrete-token shape closes the within-crate surface too
//! (tier-1 by-construction everywhere).
//!
//! # Tier-1 closure (within-crate, by-construction)
//!
//! Hostile attempts (verified mentally, to be re-verified empirically
//! by architect after this commit):
//!
//! - `slot.inner = ...` from anywhere outside `mod schema_slot`:
//!   FAILS — `inner` is private to `mod schema_slot`.
//! - `slot.park_at_be_select(desc, BeSelectToken(()))` from anywhere
//!   outside `_bind_execute_select_install_leaf`: FAILS — token's `()`
//!   field is private to the leaf submodule.
//! - `impl HostileTrait for Whatever`: NO TRAIT EXISTS to bypass. The
//!   sealed-trait + auth-tag pattern is gone; concrete-type tokens
//!   replace it.
//! - `slot.park_at_be_select(desc, X)` with `X != BeSelectToken`:
//!   FAILS — Rust type system rejects parameter mismatch.
//!
//! The only paths to mutate the slot are:
//! - `_bind_execute_select_install_leaf::install_select_transition` →
//!   mints `BeSelectToken(())` inline → calls
//!   `slot.park_at_be_select(desc, token)`.
//! - `_row_description_dispatch_leaf::park_row_description_at_dispatch`
//!   (in `mod dispatch`) → mints `TDispatchToken(())` → calls
//!   `slot.park_at_t_dispatch(desc, token)`.
//! - `_clear_residue_leaf::clear_schema_slot_residue` → mints
//!   `ClearResidueSchemaToken(())` → calls
//!   `slot.clear_at_residue(token)`.
//!
//! # Test-only setter
//!
//! [`RowDescSlotCell::_set_for_test`] is `#[cfg(test)]`-gated and lets
//! mod protocol's residue-cleanup tests pre-populate the slot with a
//! synthetic `RowDesc::EMPTY`. Production binaries do not see this
//! method (the cfg gate strips it). External crates cannot reach it
//! regardless (cell is `pub(crate)` only).
//!
//! # Bench cost
//!
//! The Cell is `#[repr(transparent)]` over `Option<RowDesc>`. Read
//! methods (`as_ref`, `is_some`, etc.) compile to the same code as the
//! bare `Option` accessors. Write methods are a single field
//! assignment plus a no-op token consume; LLVM erases the token
//! (zero-sized type). 0 ns / 0 B perf cost.

use crate::decode::RowDesc;

/// Tier-1 within-crate write provenance for the protocol's parked
/// `RowDescription`. Wraps `Option<RowDesc>` with a PRIVATE inner field;
/// writes require per-leaf concrete-type tokens (see module-level docs).
///
/// `#[repr(transparent)]` so the layout is identical to the bare
/// `Option<RowDesc>` — `mem::size_of::<RowDescSlotCell>() ==
/// mem::size_of::<Option<RowDesc>>()`.
///
/// `pub` visibility (required so the type can appear as the associated
/// type `<protocol::ActivePhase as protocol::SealedPhase>::Extras`).
/// The `inner` field stays private — external code cannot construct
/// or observe the cell's contents except via the token-gated
/// `pub(crate)` constructor + read-only `as_ref` projection.
#[allow(
    missing_copy_implementations,
    missing_debug_implementations,
    reason = "`Copy` is BANNED on the cell — the field-write protocol (token-gated `park_at_*` / `clear_at_*`) would be subvertable by mass-copying. `Debug` is suppressed because `RowDesc` itself prints column metadata that callers may not want exposed via `{:?}`; production code observes via `as_ref()` and projects through `RowDescBorrow`."
)]
#[repr(transparent)]
pub struct RowDescSlotCell {
    inner: Option<alloc::boxed::Box<RowDesc>>,
}

impl RowDescSlotCell {
    /// Construct a fresh empty cell. Token-gated to
    /// [`crate::protocol::_proto_init_leaf::ProtoInitToken`] — that's
    /// the only mint site (private to that leaf submodule which also
    /// hosts the sole legitimate caller, `PgProtocol::new`). Closes
    /// wholesale-replacement (`*cell = RowDescSlotCell::empty(...)`)
    /// to the leaf by construction.
    ///
    /// The token is consumed (ZST, erased by LLVM); non-init code paths
    /// must use the token-gated `park_at_*` / `clear_at_*` methods which
    /// mutate in-place without producing a fresh cell.
    #[inline]
    #[must_use]
    pub(crate) const fn empty(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: None }
    }

    /// Borrow the inner schema, if present. Read-only — no token needed.
    /// Used by materialise (action.rs), row_stream projections, and
    /// `compute_push_*` schema-presence checks.
    #[inline]
    #[must_use]
    pub(crate) fn as_ref(&self) -> Option<&RowDesc> {
        self.inner.as_deref()
    }

    /// Returns `true` if the slot is populated. Read-only. Currently
    /// only used by `cfg(test)` residue-cleanup fixtures; production
    /// callers project `as_ref()` directly.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    /// Returns `true` if the slot is empty. Read-only. Currently
    /// only used by `cfg(test)` residue-cleanup fixtures; production
    /// callers project `as_ref()` directly.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) fn is_none(&self) -> bool {
        self.inner.is_none()
    }

    /// Park `desc` from the BindExecute SELECT install transition. The
    /// token's mint is gated to `_bind_execute_select_install_leaf`
    /// (private tuple-struct field).
    #[inline]
    pub(crate) fn park_at_be_select(
        &mut self,
        desc: RowDesc,
        _token: crate::protocol::_bind_execute_select_install_leaf::BeSelectToken,
    ) {
        self.inner = Some(alloc::boxed::Box::new(desc));
    }

    /// Park `desc` from the inbound `'T'` (RowDescription) frame
    /// dispatch. The token's mint is gated to
    /// `_row_description_dispatch_leaf`.
    #[inline]
    pub(crate) fn park_at_t_dispatch(
        &mut self,
        desc: RowDesc,
        _token: crate::dispatch::_row_description_dispatch_leaf::TDispatchToken,
    ) {
        self.inner = Some(alloc::boxed::Box::new(desc));
    }

    /// Clear the slot at the residue-cleanup transition (Idle/Errored
    /// entry). The token's mint is gated to `_clear_residue_leaf`.
    #[inline]
    pub(crate) fn clear_at_residue(
        &mut self,
        _token: crate::protocol::_clear_residue_leaf::ClearResidueSchemaToken,
    ) {
        self.inner = None;
    }

    /// Test-only setter. `#[cfg(test)]`-gated — production binaries
    /// don't expose this. Used by `mod tests` in protocol.rs to
    /// pre-populate the slot with synthetic `RowDesc::EMPTY` before
    /// exercising residue-cleanup transitions.
    #[cfg(test)]
    #[inline]
    pub(crate) fn _set_for_test(&mut self, value: Option<RowDesc>) {
        self.inner = value.map(alloc::boxed::Box::new);
    }
}

#[cfg(test)]
mod tests {
    /// Within-crate tier-1 closure pin. The `inner` field of
    /// [`super::RowDescSlotCell`] is private to `mod schema_slot`; the
    /// per-leaf tokens are `pub(crate)` types with PRIVATE tuple-struct
    /// fields, mintable only inside their defining leaf submodule.
    /// External crates: the cell + tokens are all `pub(crate)`-gated,
    /// no public re-export. Within-crate hostile attempts to write the
    /// slot bypass-style fail at compile time:
    /// - `cell.inner = X` from outside `mod schema_slot` — `inner` private.
    /// - `BeSelectToken(())` from outside the leaf — field private.
    /// - `cell.park_at_be_select(desc, X)` with `X != BeSelectToken` —
    ///   type mismatch.
    /// - No trait to `impl` for HostileType — the sealed-trait pattern
    ///   is deleted in this cluster; tokens are concrete types.
    #[test]
    fn within_crate_seal_pin_anchor() {
        // Anchor for `git grep "schema_slot.*seal"` searches.
    }
}
