//! Tier-1 within-crate `tx_status_slot` write provenance via
//! concrete-token + Cell newtype. DEF-286 Φ-E.
//!
//! Mirror of `crate::command_tag_slot::CommandTagSlotCell` /
//! `crate::param_oids_slot::ParamOidsSlotCell` /
//! `crate::schema_slot::RowDescSlotCell`. The cell holds the
//! parsed [`TxStatus`] across the `'Z'` (ReadyForQuery) arrival.
//! Externalising tx_status removes it from every `Reply<'r>` variant
//! payload — `Reply::QueryComplete`/`Suspended`/`ParseComplete`/
//! `CloseComplete`/`DescribeStatementComplete`/`DescribePortalComplete`/
//! `Pong`/`StartupComplete` strip their inline `pub tx_status: TxStatus`
//! field, collapsing the 7-byte alignment tail on every
//! 24-B-class variant to zero.
//!
//! Per-cycle lifecycle:
//! 1. `'Z'` arrival → [`park_at_rfq_dispatch`] parses
//!    the tx_status byte via [`crate::dispatch::parse_rfq_payload`]
//!    and parks it.
//! 2. `materialise` does NOT read this slot — the public Reply
//!    payloads no longer carry `tx_status`. Callers query the
//!    parked value via
//!    [`crate::PgProtocol::terminal_tx_status`].
//! 3. Next Idle/Errored entry → [`clear_at_residue`] resets the
//!    slot to `TxStatus::Idle` (the conn-start default; mirrors
//!    the post-handshake initial state).
//!
//! # Why not `Option<TxStatus>`
//!
//! TxStatus is `#[repr(u8)]` with three known discriminants
//! (`Idle = 'I'`, `InTransaction = 'T'`, `Failed = 'E'`); the
//! 253 unused byte-patterns make `Option<TxStatus>` niche-pack to
//! 1 B. Either shape is 1 byte. The plain `TxStatus` shape with
//! `Idle` as the post-clear default matches PG's actual wire-level
//! "no transaction in progress" semantic — a freshly-handshaked
//! Active connection IS idle. Callers reading `terminal_tx_status`
//! pre-first-RFQ get a semantically-correct `Idle` rather than a
//! `None` that has no obvious operator interpretation.

use crate::action::TxStatus;

/// Tier-1 within-crate write provenance for the protocol's parked
/// terminal `ReadyForQuery` transaction-status byte. Wraps a
/// `TxStatus` with a PRIVATE inner field; writes require per-leaf
/// concrete-type tokens.
///
/// `#[repr(transparent)]` over the inner `TxStatus` — 1 B inline,
/// no overhead.
#[allow(
    missing_copy_implementations,
    missing_debug_implementations,
    reason = "`Copy` BANNED on the cell — would subvert token-gated \
              write protocol. `Debug` suppressed for consistency with \
              other slot cells (mirror of `CommandTagSlotCell`)."
)]
#[repr(transparent)]
pub struct TxStatusSlotCell {
    inner: TxStatus,
}

impl TxStatusSlotCell {
    /// Construct a cell at the conn-start default
    /// (`TxStatus::Idle`). Token-gated to
    /// [`crate::protocol::_proto_init_leaf::ProtoInitToken`].
    #[inline]
    #[must_use]
    pub(crate) const fn fresh(
        _token: crate::protocol::_proto_init_leaf::ProtoInitToken,
    ) -> Self {
        Self { inner: TxStatus::Idle }
    }

    /// Read the parked transaction-status. Read-only — no token
    /// needed. Used by [`crate::PgProtocol::terminal_tx_status`]
    /// to surface the parked byte to callers after they consume
    /// an `Action::DeliverReply` from `OutActions`.
    #[inline]
    #[must_use]
    pub(crate) fn get(&self) -> TxStatus {
        self.inner
    }

    /// Park `tx_status` from the inbound `'Z'` (ReadyForQuery) frame
    /// dispatch. Token gated to `_rfq_dispatch_leaf`. Latest-wins:
    /// the next `'Z'` overwrites; a batched-RFQ scenario does not
    /// arise in practice (PG emits exactly one trailing `'Z'` per
    /// query cycle).
    #[inline]
    pub(crate) fn park_at_rfq_dispatch(
        &mut self,
        tx_status: TxStatus,
        _token: crate::dispatch::_rfq_dispatch_leaf::RfqDispatchToken,
    ) {
        self.inner = tx_status;
    }

    /// Reset the slot to the conn-start default
    /// (`TxStatus::Idle`) at residue-cleanup transition. Token-gated
    /// to [`crate::protocol::_clear_residue_leaf::ClearResidueTxStatusToken`].
    #[inline]
    pub(crate) fn clear_at_residue(
        &mut self,
        _token: crate::protocol::_clear_residue_leaf::ClearResidueTxStatusToken,
    ) {
        self.inner = TxStatus::Idle;
    }
}

// ─── Drift pins ────────────────────────────────────────────────────

const _: () = assert!(
    core::mem::size_of::<TxStatusSlotCell>() == 1,
    "TxStatusSlotCell must stay 1 byte (#[repr(transparent)] over \
     TxStatus, which is #[repr(u8)]). If this grows, the slot's \
     footprint on PgProtocol regresses.",
);
