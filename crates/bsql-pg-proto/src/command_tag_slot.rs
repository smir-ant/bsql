//! Tier-1 within-crate `command_tag_slot` write provenance via
//! concrete-token + Cell newtype. DEF-286 Φ3.
//!
//! Mirror of `crate::param_oids_slot::ParamOidsSlotCell` /
//! `crate::schema_slot::RowDescSlotCell`. The cell holds the
//! parsed [`CommandTag`] across the multi-frame
//! `'C' [→ 'C']* → 'Z'` window of a query/extended-query cycle.
//!
//! Per-cycle lifecycle:
//! 1. `'C'` arrival → [`park_at_command_complete_dispatch`] parses
//!    the wire-tag bytes via
//!    [`crate::command_tag::parse_command_tag_bytes`], boxes the
//!    result, and parks it.
//! 2. Multi-statement (DEF-226): another `'C'` arrival overwrites
//!    the slot. Latest-wins — the prior tag's
//!    `IntermediateCommandComplete` action was already emitted
//!    when the prior `'C'` fired (by-value copy from slot to
//!    `StagedAction::IntermediateCommandComplete` at C arm exit).
//! 3. `'Z'` arrival → `materialise` reads slot via `as_ref()` and
//!    emits [`crate::action::QueryCompletePayload`] with
//!    `command_tag: &'r CommandTag` borrowed from the slot.
//! 4. Next Idle/Errored entry → `clear_at_residue` empties the
//!    slot.

use crate::command_tag::CommandTag;

/// Tier-1 within-crate write provenance for the protocol's parked
/// `CommandComplete` payload. Wraps
/// `Option<alloc::boxed::Box<CommandTag>>` with a PRIVATE inner
/// field; writes require per-leaf concrete-type tokens.
///
/// `#[repr(transparent)]` over the inner `Option<Box<CommandTag>>`
/// — 8 B niche-packed via `Box`'s non-null pointer.
#[allow(
    missing_copy_implementations,
    missing_debug_implementations,
    reason = "`Copy` BANNED on the cell — would subvert token-gated \
              write protocol. `Debug` suppressed because `CommandTag` \
              prints query metadata callers may not want exposed."
)]
#[repr(transparent)]
pub struct CommandTagSlotCell {
    inner: Option<alloc::boxed::Box<CommandTag>>,
}

impl CommandTagSlotCell {
    /// Construct an empty cell. Token-gated to
    /// [`crate::protocol::_proto_init_leaf::ProtoInitToken`].
    #[inline]
    #[must_use]
    pub(crate) const fn empty(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: None }
    }

    /// Borrow the inner CommandTag, if present. Read-only — no
    /// token needed. Used by materialise — projects to
    /// `Reply::QueryComplete.command_tag: &'r CommandTag`.
    #[inline]
    #[must_use]
    pub(crate) fn as_ref(&self) -> Option<&CommandTag> {
        self.inner.as_deref()
    }

    /// Test-only `is_some` accessor.
    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Test-only — mirror of `ParamOidsSlotCell::is_some`."
    )]
    #[inline]
    #[must_use]
    pub(crate) fn is_some(&self) -> bool {
        self.inner.is_some()
    }

    /// Test-only `is_none` accessor.
    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Test-only — mirror parallel-pattern."
    )]
    #[inline]
    #[must_use]
    pub(crate) fn is_none(&self) -> bool {
        self.inner.is_none()
    }

    /// Park `tag` from the inbound `'C'` (CommandComplete) frame
    /// dispatch. Token gated to `_command_complete_dispatch_leaf`.
    /// Multi-statement `'C'` arrivals overwrite the prior box.
    #[inline]
    pub(crate) fn park_at_command_complete_dispatch(
        &mut self,
        tag: alloc::boxed::Box<CommandTag>,
        _token: crate::dispatch::_command_complete_dispatch_leaf::CommandCompleteDispatchToken,
    ) {
        self.inner = Some(tag);
    }

    /// Clear the slot at residue-cleanup transition.
    #[inline]
    pub(crate) fn clear_at_residue(
        &mut self,
        _token: crate::protocol::_clear_residue_leaf::ClearResidueCommandTagToken,
    ) {
        self.inner = None;
    }

    /// Test-only setter.
    #[cfg(test)]
    #[inline]
    pub(crate) fn _set_for_test(&mut self, value: Option<CommandTag>) {
        self.inner = value.map(alloc::boxed::Box::new);
    }
}
