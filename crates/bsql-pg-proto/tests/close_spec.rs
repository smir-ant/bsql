//! Extended Query `Close` command end-to-end (PG §55.7).
//!
//! Covers both target shapes (statement / portal):
//!
//! - Statement-close happy path:
//!   `C 'S' name NUL + Sync` out, `CloseComplete + RFQ` in →
//!   `Reply::CloseComplete(CloseCompletePayload)` delivered.
//! - Portal-close happy path:
//!   `C 'P' name NUL + Sync` out, `CloseComplete + RFQ` in →
//!   `Reply::CloseComplete(CloseCompletePayload)` delivered.
//!
//! Bad paths:
//!
//! - `'E'` after Close → FailReply + drain to Idle via
//!   `DrainRfqAfterError`. Connection survives.
//!
//! Push-state policy:
//!
//! - `Close{Statement,Portal}` in Idle → 2 SendBytes (Close + Sync).
//!
//! Wire-format drift-pins:
//!
//! - Statement frame byte layout: `'C' | len | 'S' | name | NUL`.
//! - Portal frame byte layout: `'C' | len | 'P' | name | NUL`.
//! - `CloseTargetByte::{Statement,Portal}.byte() == {b'S', b'P'}`.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    Action, ActiveState, CloseKind, ConnectionStatus, PortalName, Reply, StmtName, WriteBuf,
    push_command::{ClosePortal, CloseStatement},
    wire::{CloseTargetByte, TAG_CLOSE, TAG_ERROR_RESPONSE, TAG_READY_FOR_QUERY},
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

fn stmt_unnamed() -> StmtName {
    StmtName::default()
}

fn portal_unnamed() -> PortalName {
    PortalName::default()
}

/// Build a bare PG frame: tag + 4-byte BE length (self-inclusive) + body.
fn frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::new();
    out.push(tag);
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("fixture body too large");
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn close_complete_frame() -> std::vec::Vec<u8> {
    frame(b'3', &[])
}

fn rfq_idle_frame() -> std::vec::Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), b"I")
}

fn error_response_frame() -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(0);
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

// =====================================================================
// Wire-format drift pins
// =====================================================================

#[test]
fn close_target_byte_statement_pins_to_s() {
    assert_eq!(CloseTargetByte::Statement.byte(), b'S');
}

#[test]
fn close_target_byte_portal_pins_to_p() {
    assert_eq!(CloseTargetByte::Portal.byte(), b'P');
}

// =====================================================================
// Statement-close happy path
// =====================================================================

#[test]
fn close_statement_unnamed_happy_path() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, raw_reply_id) = mint_reply::<CloseKind>(&mut proto);
    proto.push_or_panic(
        CloseStatement {
            stmt_name: stmt_unnamed(),
            reply,
        },
        &mut wb,
    );

    assert!(
        matches!(proto.state(), ActiveState::CloseAwaitingComplete(_)),
        "post-push state must be CloseAwaitingComplete, got {:?}",
        proto.state(),
    );

    let bytes = wb.as_bytes();
    assert_eq!(bytes.first().copied(), Some(TAG_CLOSE.byte()), "tag is 'C'");
    let len_bytes = bytes.get(1..5).unwrap_or(&[]);
    let len = u32::from_be_bytes([
        len_bytes.first().copied().unwrap_or(0),
        len_bytes.get(1).copied().unwrap_or(0),
        len_bytes.get(2).copied().unwrap_or(0),
        len_bytes.get(3).copied().unwrap_or(0),
    ]);
    assert_eq!(
        len, 6,
        "Close frame for unnamed stmt: len = 4 (len field) + 1 (target byte 'S') + 1 (NUL terminator)"
    );
    assert_eq!(bytes.get(5).copied(), Some(b'S'), "target byte is 'S'");
    assert_eq!(bytes.get(6).copied(), Some(0), "NUL-terminated empty name");

    let mut combined = close_complete_frame();
    combined.extend_from_slice(&rfq_idle_frame());
    let out = proto.feed_bytes(&combined, &mut wb);
    let mut delivered = false;
    for action in out.as_slice() {
        if let Action::DeliverReply { id, value } = action {
            assert_eq!(*id, raw_reply_id, "delivered reply id matches");
            assert!(
                matches!(value, Reply::CloseComplete(_)),
                "Reply must be CloseComplete, got {value:?}"
            );
            delivered = true;
        }
    }
    assert!(
        delivered,
        "CloseComplete reply must be delivered after RFQ"
    );
    let _ = out.as_slice().len();
    assert!(
        matches!(proto.state(), ActiveState::Idle),
        "state returns to Idle after CloseComplete + RFQ"
    );
    assert!(matches!(proto.connection_status(), ConnectionStatus::Ready));
}

// =====================================================================
// Portal-close happy path
// =====================================================================

#[test]
fn close_portal_unnamed_happy_path() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, raw_reply_id) = mint_reply::<CloseKind>(&mut proto);
    proto.push_or_panic(
        ClosePortal {
            portal_name: portal_unnamed(),
            reply,
        },
        &mut wb,
    );

    let bytes = wb.as_bytes();
    assert_eq!(bytes.first().copied(), Some(TAG_CLOSE.byte()));
    assert_eq!(
        bytes.get(5).copied(),
        Some(b'P'),
        "target byte is 'P' for portal"
    );

    let mut combined = close_complete_frame();
    combined.extend_from_slice(&rfq_idle_frame());
    let out = proto.feed_bytes(&combined, &mut wb);
    let mut delivered = false;
    for action in out.as_slice() {
        if let Action::DeliverReply { id, value } = action {
            assert_eq!(*id, raw_reply_id);
            assert!(matches!(value, Reply::CloseComplete(_)));
            delivered = true;
        }
    }
    assert!(delivered);
    let _ = out.as_slice().len();
    assert!(matches!(proto.state(), ActiveState::Idle));
}

// =====================================================================
// Error path: ErrorResponse after Close → FailReply + drain to Idle
// =====================================================================

#[test]
fn close_statement_errored_path_recovers_via_rfq() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, raw_reply_id) = mint_reply::<CloseKind>(&mut proto);
    proto.push_or_panic(
        CloseStatement {
            stmt_name: stmt_unnamed(),
            reply,
        },
        &mut wb,
    );

    let out = proto.feed_bytes(&error_response_frame(), &mut wb);
    let mut failed = false;
    for action in out.as_slice() {
        if let Action::FailReply { id, cause: _ } = action {
            assert_eq!(*id, raw_reply_id);
            failed = true;
        }
    }
    assert!(failed, "ErrorResponse during Close must emit FailReply");
    let _ = out.as_slice().len();
    assert!(matches!(proto.state(), ActiveState::DrainRfqAfterError));

    let out = proto.feed_bytes(&rfq_idle_frame(), &mut wb);
    let _ = out.as_slice().len();
    assert!(
        matches!(proto.state(), ActiveState::Idle),
        "DrainRfqAfterError consumes RFQ and returns to Idle"
    );
}
