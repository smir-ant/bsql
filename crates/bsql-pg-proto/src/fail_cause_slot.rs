//! Tier-1 within-crate `fail_cause_slot` write provenance via
//! concrete-token + Cell newtype. DEF-286 Φ-I.b.
//!
//! Mirror of [`crate::command_tag_slot::CommandTagSlotCell`] /
//! [`crate::param_oids_slot::ParamOidsSlotCell`] /
//! [`crate::schema_slot::RowDescSlotCell`] /
//! [`crate::tx_status_slot::TxStatusSlotCell`]. The cell holds the
//! parsed [`crate::error::ProtocolError`] across the FailReply
//! emission window — staged when the dispatch path calls
//! [`crate::dispatch::install_errored`] (state→Errored transition),
//! materialised through the FailReply path, queried by callers via
//! [`crate::PgProtocol::fail_cause`] after consuming
//! `Action::FailReply` from `OutActions`.
//!
//! # Why externalise the cause
//!
//! Pre-Φ-I.b shape:
//!
//! ```text
//! Action::FailReply { id: NonZeroU64, cause: ProtocolError }   // 32 B body
//! ```
//!
//! `ProtocolError` is 24 B (post-Φ-B'' SCRAM externalisation). Inline
//! cause tied `FailReply` body to 32 B = id(8) + cause(24). Outer
//! `Action` enum disc adds 8 B → Action = 40 B floor.
//!
//! Post-Φ-I.b:
//!
//! ```text
//! Action::FailReply { id: NonZeroU64 }                          // 8 B body
//! PgProtocol::fail_cause() -> Option<&ProtocolError>            // lookup slot
//! ```
//!
//! `FailReply` body collapses 32 → 8 B; with `DeliverReply` body
//! shrinking via Φ-F* (Reply 24 → 16 B → body 24 → 16 B → 8+16 = 24 B),
//! the Action floor drops 40 → 24 B (-40%). Cascade through
//! `OutActions` (9 × 40 → 9 × 24 + 8 = 224 B) = -39%.
//!
//! # Per-cycle lifecycle
//!
//! 1. `dispatch::install_errored` (or push-path equivalent like
//!    `compute_push_*` error classification arm) calls
//!    [`crate::dispatch::_install_errored_leaf::park_cause_at_install_errored`]
//!    which delegates to [`Self::park_at_install_errored`]. State
//!    transitions to `Errored`. Slot now holds `Some(Box<cause>)`.
//! 2. `Action::FailReply { id }` flows through the action surface; the
//!    cause is NOT inline. Callers query via
//!    [`crate::PgProtocol::fail_cause`].
//! 3. Next Idle entry → [`Self::clear_at_residue`] empties the slot.
//!    Errored entry does NOT clear — slot persists across Errored
//!    cycle so callers can query post-fact via `<ClosedPhase>` API
//!    OR re-query in the next feed_bytes call's iteration.
//!
//! # `ConnectionAlreadyClosed` semantic
//!
//! Pushing a command on an already-Errored protocol re-runs the
//! "park new cause" path with cause = `ConnectionAlreadyClosed`. The
//! prior cause is overwritten. Tier-3 ergonomic: callers query
//! `fail_cause` IMMEDIATELY on the first FailReply event; deferring
//! query past a subsequent push loses the original cause. Documented
//! contract — see push-path docs.
//!
//! # `repr(transparent)` over `Option<Box<ProtocolError>>`
//!
//! Niche-packed: `Box<T>`'s non-null pointer absorbs `Option`'s disc.
//! Total footprint = 8 B (one pointer-width) regardless of
//! `ProtocolError`'s size. Compare to inline `Option<ProtocolError>`
//! = 32 B (ProtocolError 24 + disc + padding). The slot's pointer
//! indirection costs ~1 ns per fail-path lookup vs zero on the
//! happy path. Lookup is cold; happy path is hot. Asymmetric cost
//! aligns with allocation patterns.

use crate::error::ProtocolError;

/// Tier-1 within-crate write provenance for the protocol's parked
/// `Action::FailReply.cause`. Wraps
/// `Option<alloc::boxed::Box<ProtocolError>>` with a PRIVATE inner
/// field; writes require per-leaf concrete-type tokens.
///
/// `#[repr(transparent)]` over the inner `Option<Box<ProtocolError>>`
/// — 8 B niche-packed via `Box`'s non-null pointer.
#[allow(
    missing_copy_implementations,
    missing_debug_implementations,
    reason = "`Copy` BANNED on the cell — would subvert token-gated \
              write protocol. `Debug` suppressed because \
              `ProtocolError`'s Display redacts SCRAM/error material; \
              the slot's debug projection would leak internal classifier \
              identifiers that callers should reach via `fail_cause` \
              (Display-safe) rather than `{:?}` (raw)."
)]
#[repr(transparent)]
pub struct FailCauseSlotCell {
    inner: Option<alloc::boxed::Box<ProtocolError>>,
}

impl FailCauseSlotCell {
    /// Construct an empty cell. Token-gated to
    /// [`crate::protocol::_proto_init_leaf::ProtoInitToken`].
    #[inline]
    #[must_use]
    pub(crate) const fn empty(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: None }
    }

    /// Borrow the inner `ProtocolError`, if present. Read-only — no
    /// token needed. Surfaced through
    /// [`crate::PgProtocol::fail_cause`].
    #[inline]
    #[must_use]
    pub(crate) fn as_ref(&self) -> Option<&ProtocolError> {
        self.inner.as_deref()
    }

    /// Test-only `is_some` accessor.
    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Test-only — mirror of \
                  `CommandTagSlotCell::is_some`."
    )]
    #[inline]
    #[must_use]
    pub(crate) fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    /// Park `cause` at the `install_errored` transition site. Token
    /// gated to `_install_errored_leaf::InstallErroredToken` (DEF-286
    /// Φ-I.b). The leaf submodule lives in `dispatch.rs` because the
    /// `install_errored` helper is dispatch-scoped — token type lives
    /// alongside its mint site.
    ///
    /// **Latest-wins semantics**: a subsequent `install_errored` (e.g.
    /// `ConnectionAlreadyClosed` raised when a user pushes a command
    /// on an already-Errored protocol) overwrites the prior `Box`,
    /// dropping it. Documented caller contract: query `fail_cause`
    /// IMMEDIATELY on the first FailReply event.
    #[inline]
    pub(crate) fn park_at_install_errored(
        &mut self,
        cause: alloc::boxed::Box<ProtocolError>,
        _token: crate::dispatch::_install_errored_leaf::InstallErroredToken,
    ) {
        self.inner = Some(cause);
    }

    /// Clear the slot at residue-cleanup transition. Token-gated to
    /// [`crate::protocol::_clear_residue_leaf::ClearResidueFailCauseToken`].
    ///
    /// **Cleared only at Idle** entry — Errored entry does NOT clear
    /// because the slot's whole purpose is to persist the cause for
    /// caller queries until the next legitimate query cycle. The
    /// `clear_session_residue_for_class_dispatch` helper's Errored
    /// arm intentionally skips this clear.
    #[inline]
    pub(crate) fn clear_at_residue(
        &mut self,
        _token: crate::protocol::_clear_residue_leaf::ClearResidueFailCauseToken,
    ) {
        self.inner = None;
    }
}

// ─── Drift pins ────────────────────────────────────────────────────

const _: () = assert!(
    core::mem::size_of::<FailCauseSlotCell>() == 8,
    "FailCauseSlotCell must stay 8 B (#[repr(transparent)] over \
     Option<Box<ProtocolError>>; niche-packed via Box's non-null \
     pointer). If this grows, either (a) Box semantics changed \
     (architecturally impossible under stable Rust), or (b) a non-niche \
     field crept in. The DEF-286 Φ-I.b footprint claim depends on \
     this 8-byte size: PgProtocol grows +8 B for the slot field; \
     Action's body collapses -24 B per FailReply variant.",
);
