//! DEF-271 cluster B (2026-05-10) — tier-1 `SessionParams` write
//! provenance.
//!
//! Direct mirror of [`crate::schema_slot`] (DEF-270 R-rephrased) for
//! the `SessionParams` mutation surface.
//!
//! # Pre-DEF-271 B
//!
//! `PgProtocol::session_params: Option<Box<SessionParams>>` was
//! mutable through any `&mut SessionParams` borrow that crate-internal
//! callers extracted. Three mutation sites in `mod protocol`:
//! - the `ParameterStatus` pre-dispatch filter calling `record_param_status`
//!   (which internally calls `set` plus, on `MalformedPayload`, the
//!   caller separately bumps the malformed counter);
//! - the `NoticeResponse` pre-dispatch filter calling
//!   `bump_notice_response`;
//! - the residue-cleanup path inside `clear_session_residue_for_class`
//!   calling `params.clear()`.
//!
//! Each was a raw `&mut SessionParams` write — the audit invariant
//! "writes happen only at these three sites" was upheld by reviewer
//! attention plus naming. **Tier-2 by-discipline.**
//!
//! # Post-DEF-271 B
//!
//! - The sole write surface is [`SessionParamsSlot`], a `must_use`
//!   ZST-witness wrapping `&'a mut SessionParams`. Methods
//!   [`Self::record`] / [`Self::bump_malformed_param_status`] /
//!   [`Self::bump_notice_response`] / [`Self::clear`] consume self.
//! - Construction is gated on a [`SessionParamsWriteAuth`] sealed-trait
//!   witness. Per-host-module auth tags
//!   ([`crate::protocol::AtParameterStatusFrame`],
//!   [`crate::protocol::AtNoticeResponseFrame`],
//!   [`crate::protocol::AtClearSessionResidue`]) live in `mod protocol`
//!   with `pub(in crate::protocol) const fn new()` — only that module
//!   can mint a tag.
//! - `AtClearSessionResidue` is shared with the existing
//!   [`crate::schema_slot::SchemaWriteAuth`] tag (same residue-cleanup
//!   site clears both the schema slot and session params); one ZST,
//!   two sealed-trait impls.
//!
//! # Cluster C follow-up (deferred)
//!
//! `pub(in crate::protocol)` spans the entire `~5 K LoC` module.
//! Phase 3 cluster C narrows the scope to leaf submodules per
//! call site (architect's #2 finding); applies uniformly to all
//! schema_slot + session_params_slot tags.

use crate::session_params::SessionParams;

/// Sealed-supertrait module. `pub(crate)` so in-crate host modules
/// can `impl Sealed for <their-tag>`. External crates cannot reach
/// this module (no public re-export).
pub(crate) mod sealed {
    /// Sealed marker — implementors are crate-internal only by
    /// virtue of `mod sealed`'s `pub(crate)` visibility.
    pub trait Sealed {}
}

/// Sealed witness trait for a `SessionParams` mutation site.
/// Implementors are ZST tags emitted at specific transition sites
/// (see module-level docs). Mirror of
/// [`crate::schema_slot::SchemaWriteAuth`].
pub(crate) trait SessionParamsWriteAuth: sealed::Sealed {}

/// Tier-1 witness wrapping a mutable borrow of
/// `PgProtocol::session_params` (post-`session_params_or_init`
/// initialisation).
///
/// **Construction:** auth-typed via [`Self::from_field_with_auth`]
/// (gated on a [`SessionParamsWriteAuth`] tag). Tag minting lives
/// in `mod protocol` with `pub(in crate::protocol) const fn new()`.
///
/// **Methods:** [`Self::record`] / [`Self::bump_malformed_param_status`] /
/// [`Self::bump_notice_response`] / [`Self::clear`] — each consumes
/// `self` so a single witness performs exactly one write.
#[must_use = "session params slot witness must be consumed via record / bump_* / clear"]
pub(crate) struct SessionParamsSlot<'a> {
    slot: &'a mut SessionParams,
}

impl<'a> SessionParamsSlot<'a> {
    /// Auth-typed constructor. Crate-internal modules can construct
    /// a witness if and only if they hold a [`SessionParamsWriteAuth`]
    /// tag — and tag construction is gated by per-module
    /// `pub(in crate::protocol)` visibility on the tag's `new()`.
    #[inline]
    pub(crate) fn from_field_with_auth<A: SessionParamsWriteAuth>(
        slot: &'a mut SessionParams,
        _auth: A,
    ) -> Self {
        Self { slot }
    }

    /// Record a parsed `(key, value)` pair from a `ParameterStatus`
    /// payload. Consumes self.
    #[inline]
    pub(crate) fn record(self, key: &[u8], value: &[u8]) {
        self.slot.set(key, value);
    }

    /// Bump the malformed-`ParameterStatus`-payload counter
    /// (DEF-185 P2-B operator-canary). Consumes self.
    #[inline]
    pub(crate) fn bump_malformed_param_status(self) {
        self.slot.bump_malformed_param_status();
    }

    /// Bump the unsolicited-`NoticeResponse` counter
    /// (DEF-185 P2-3 operator-canary). Consumes self.
    #[inline]
    pub(crate) fn bump_notice_response(self) {
        self.slot.bump_notice_response();
    }

    /// Clear all session params (residue cleanup on Idle/Errored
    /// entry). Consumes self. Drop chain scrubs `SecretBoundedStr`
    /// bytes (DEF-189 Q8-C3 + DEF-205 step 3).
    #[inline]
    pub(crate) fn clear(self) {
        self.slot.clear();
    }
}

#[cfg(test)]
mod tests {
    /// Sealed-trait pin. The `mod sealed` module is `pub(crate)` and
    /// `Sealed` itself is `pub` within it; external crates have no
    /// path to either, so `impl SessionParamsWriteAuth for ExternalType`
    /// cannot be written outside this crate.
    #[test]
    fn seal_pin_anchor() {
        // Anchor for `git grep "session_params_slot.*seal"` searches.
    }
}
