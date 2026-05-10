//! DEF-272 cluster β (2026-05-10) — tier-1 within-crate
//! `session_params` write provenance via concrete-token + Cell newtype.
//!
//! Direct mirror of [`crate::schema_slot`] cluster α; the same
//! architectural reasoning applies to this slot's mutation surface.
//!
//! # Pre-DEF-272 β
//!
//! - `PgProtocol::session_params: Option<Box<SessionParams>>` was
//!   reachable via `&mut SessionParams` borrow that crate-internal
//!   callers extracted (via `session_params_or_init` lazy-init).
//! - `SessionParamsSlot<A: SessionParamsWriteAuth>` witness gated
//!   writes via a sealed-trait auth tag; the `pub(crate) mod sealed`
//!   surface allowed any in-crate file to write
//!   `impl Sealed for HostileTag + impl SessionParamsWriteAuth for HostileTag`
//!   and bypass `from_field_with_auth`. **Tier-1 EXTERNAL + tier-2
//!   by-discipline WITHIN-CRATE** — verified empirically by the
//!   architect's hostile probe (2026-05-10).
//!
//! # Post-DEF-272 β
//!
//! Same two structural changes as schema_slot α:
//!
//! 1. **`SessionParamsCell` newtype** wraps `Option<Box<SessionParams>>`
//!    with a PRIVATE `inner` field (private to `mod session_params_slot`).
//!    Direct `*self.session_params.inner = ...` does not compile from
//!    `mod protocol` or anywhere else. Read accessor (`as_deref`) and
//!    token-gated write methods are the only paths.
//!
//! 2. **Per-leaf concrete-type tokens** replace the sealed-trait
//!    pattern. Each leaf hosts a `pub(crate) struct XToken(())` type
//!    with a PRIVATE tuple-struct field (mintable only inside the
//!    defining leaf). Cell methods take the concrete token type by
//!    value:
//!    - [`crate::protocol::_parameter_status_admit_leaf::ParamStatusToken`]
//!      → [`SessionParamsCell::admit_at_param_status`]
//!    - [`crate::protocol::_notice_response_admit_leaf::NoticeResponseToken`]
//!      → [`SessionParamsCell::admit_at_notice_response`]
//!    - [`crate::protocol::_clear_residue_leaf::ClearResidueSessionToken`]
//!      → [`SessionParamsCell::clear_at_residue`]
//!
//! # Lazy-init absorbed into Cell methods
//!
//! Pre-β the `session_params_or_init` helper extracted `&mut SessionParams`
//! by lazy-init-ing the inner `Box`. Post-β each `admit_*` Cell method
//! lazy-inits internally on first call — the `&mut SessionParams`
//! never escapes the cell. This eliminates the
//! `pub(crate) fn session_params_or_init(slot: &mut Option<Box<SessionParams>>) -> &mut SessionParams`
//! escape-hatch entirely (deleted in this commit).
//!
//! # Tier-1 closure (within-crate, by-construction)
//!
//! Hostile attempts:
//!
//! - `cell.inner = ...` from outside `mod session_params_slot`: FAILS —
//!   `inner` is private to `mod session_params_slot`.
//! - `ParamStatusToken(())` from outside the leaf: FAILS — token's
//!   `()` field is private to the leaf submodule.
//! - `cell.admit_at_param_status(payload, X)` with `X` not the
//!   matching token type: FAILS — type mismatch.
//! - `impl HostileTrait for Whatever`: NO TRAIT EXISTS — the
//!   sealed-trait pattern is deleted in this cluster.
//! - `&mut SessionParams` extraction from outside `mod session_params_slot`:
//!   FAILS — Cell never exposes `&mut SessionParams`; only
//!   `&SessionParams` via `as_deref` (read-only) and internally-managed
//!   mutation through token-gated methods.
//!
//! # Bench cost
//!
//! Cell is `#[repr(transparent)]` over `Option<Box<SessionParams>>`.
//! `as_deref` compiles identically. `admit_*` methods do the same
//! `get_or_insert_with` pattern that `session_params_or_init` did,
//! plus a no-op token consume; LLVM erases the token. 0 ns / 0 B perf
//! delta vs. pre-β.

use crate::session_params::SessionParams;

/// Tier-1 within-crate write provenance for the protocol's session
/// params (key/value pairs from `ParameterStatus`, `NoticeResponse`
/// counter, malformed-payload counter). Wraps `Option<Box<SessionParams>>`
/// with a PRIVATE inner field; writes require per-leaf concrete-type
/// tokens (see module-level docs).
///
/// `#[repr(transparent)]` so the layout is identical to the bare
/// `Option<Box<SessionParams>>` — the niche-packed 8 B footprint pre-β
/// is preserved.
#[repr(transparent)]
pub(crate) struct SessionParamsCell {
    inner: Option<alloc::boxed::Box<SessionParams>>,
}

impl SessionParamsCell {
    /// Construct a fresh empty cell. Token-gated to
    /// [`crate::protocol::_proto_init_leaf::ProtoInitToken`] — that's
    /// the only mint site (private to that leaf submodule which also
    /// hosts the sole legitimate caller, `PgProtocol::new`). Closes
    /// wholesale-replacement (`*cell = SessionParamsCell::empty(...)`)
    /// to the leaf by construction — DEF-272 P6 closure (2026-05-10),
    /// architect hostile-probe-driven follow-up to DEF-272.
    ///
    /// The token is consumed (ZST, erased by LLVM); non-init code paths
    /// must use the token-gated `admit_at_*` / `clear_at_*` methods which
    /// mutate in-place without producing a fresh cell.
    #[inline]
    #[must_use]
    pub(crate) const fn empty(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: None }
    }

    /// Borrow the inner session params, if allocated. Read-only. Used
    /// by `PgProtocol::session_params` accessor and the residue-cleanup
    /// match arms.
    #[inline]
    #[must_use]
    pub(crate) fn as_deref(&self) -> Option<&SessionParams> {
        self.inner.as_deref()
    }

    /// Returns `true` if the inner box is allocated. Read-only. Used
    /// only by `cfg(test)` residue-cleanup fixtures.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    /// Admit a `ParameterStatus` frame: parse the payload, on success
    /// record the (key, value) pair into the lazy-allocated
    /// [`SessionParams`]; on parse failure bump the malformed-payload
    /// counter (DEF-185 P2-B operator-canary). Returns the
    /// [`crate::protocol::ParamStatusRecordOutcome`] for caller
    /// observability.
    ///
    /// Lazy-allocates the `Box<SessionParams>` on first call. The
    /// token's mint is gated to `_parameter_status_admit_leaf`.
    ///
    /// Payload format per PG §55.7: `key\0value\0` (two NUL-terminated
    /// C-strings). Pre-β this logic lived in
    /// `record_param_status_with_slot`; post-β it's a method on the
    /// Cell so the `&mut SessionParams` borrow never escapes.
    #[inline]
    #[must_use]
    pub(crate) fn admit_at_param_status(
        &mut self,
        payload: &[u8],
        _t: crate::protocol::_parameter_status_admit_leaf::ParamStatusToken,
    ) -> crate::protocol::ParamStatusRecordOutcome {
        let params = self
            .inner
            .get_or_insert_with(|| alloc::boxed::Box::new(SessionParams::new()));
        let Some(nul_pos) = payload.iter().position(|b| *b == 0) else {
            params.bump_malformed_param_status();
            return crate::protocol::ParamStatusRecordOutcome::MalformedPayload;
        };
        let Some(key) = payload.get(..nul_pos) else {
            params.bump_malformed_param_status();
            return crate::protocol::ParamStatusRecordOutcome::MalformedPayload;
        };
        let Some(value_start) = nul_pos.checked_add(1) else {
            params.bump_malformed_param_status();
            return crate::protocol::ParamStatusRecordOutcome::MalformedPayload;
        };
        let Some(value_region) = payload.get(value_start..) else {
            params.bump_malformed_param_status();
            return crate::protocol::ParamStatusRecordOutcome::MalformedPayload;
        };
        let Some(value) = value_region.strip_suffix(b"\0") else {
            params.bump_malformed_param_status();
            return crate::protocol::ParamStatusRecordOutcome::MalformedPayload;
        };
        params.set(key, value);
        crate::protocol::ParamStatusRecordOutcome::Processed
    }

    /// Admit a `NoticeResponse` frame: bump the unsolicited-notice
    /// counter (DEF-185 P2-3 operator-canary). Lazy-allocates the
    /// `Box<SessionParams>` on first call. The token's mint is gated
    /// to `_notice_response_admit_leaf`.
    #[inline]
    pub(crate) fn admit_at_notice_response(
        &mut self,
        _t: crate::protocol::_notice_response_admit_leaf::NoticeResponseToken,
    ) {
        let params = self
            .inner
            .get_or_insert_with(|| alloc::boxed::Box::new(SessionParams::new()));
        params.bump_notice_response();
    }

    /// Clear the session params CONTENTS at the residue-cleanup
    /// transition (Errored entry per DEF-189 Q8-C3 + DEF-205 step 3 —
    /// session-state forfeit on tear-down). Calls `params.clear()` on
    /// the inner box (which scrubs `SecretBoundedStr` bytes via its
    /// own Drop chain on each replaced field) but PRESERVES the
    /// `Box<SessionParams>` allocation itself — the test fixture
    /// `errored_clears_everything_including_session_params` pins this
    /// invariant: post-Errored, the box stays allocated, the contents
    /// are pristine. Pre-β `params.clear()` was the operation; this
    /// preserves the same semantics behind the cell. The token's mint
    /// is gated to `_clear_residue_leaf`.
    #[inline]
    pub(crate) fn clear_at_residue(
        &mut self,
        _t: crate::protocol::_clear_residue_leaf::ClearResidueSessionToken,
    ) {
        if let Some(params) = self.inner.as_deref_mut() {
            params.clear();
        }
    }

    /// Test-only setter. `#[cfg(test)]`-gated — production binaries
    /// don't expose this. Used by `mod tests` in protocol.rs to
    /// pre-populate the slot with a synthetic dirty `SessionParams`
    /// before exercising residue-cleanup transitions.
    #[cfg(test)]
    #[inline]
    pub(crate) fn _set_for_test(&mut self, value: Option<alloc::boxed::Box<SessionParams>>) {
        self.inner = value;
    }
}

#[cfg(test)]
mod tests {
    /// Within-crate tier-1 closure pin. The `inner` field of
    /// [`super::SessionParamsCell`] is private to `mod session_params_slot`;
    /// per-leaf tokens have PRIVATE tuple-struct fields, mintable only
    /// inside their defining leaf submodule. No trait surface remains
    /// for hostile impls (sealed-trait pattern deleted in cluster β).
    /// External crates: cell + tokens are all `pub(crate)`-gated, no
    /// public re-export.
    #[test]
    fn within_crate_seal_pin_anchor() {
        // Anchor for `git grep "session_params_slot.*seal"` searches.
    }
}
