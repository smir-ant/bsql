//! DEF-224 NoticeResponse integration tests.
//!
//! Validates end-to-end NoticeResponse ('N') delivery:
//! 1. Feed a NoticeResponse frame via `feed_bytes`
//! 2. OutActions contains `Action::Notice { notice_ref }`
//! 3. `proto.get_notice(notice_ref)` resolves to the payload
//! 4. Per-cycle arena clear invalidates the ref → `ArenaError::Stale`

#![forbid(unsafe_code)]

use bsql_postgres_proto::{Action, ArenaError, WriteBuf};

mod common;
use common::fresh_active_via_trust_handshake;

fn build_notice_frame(severity: &[u8], code: &[u8], message: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity);
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(code);
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message);
    body.push(0);
    body.push(0); // terminator

    let Ok(body_len_with_self) = u32::try_from(body.len().saturating_add(4)) else {
        return std::vec::Vec::new();
    };
    let mut frame = std::vec::Vec::new();
    frame.push(b'N'); // NoticeResponse tag
    frame.extend_from_slice(&body_len_with_self.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Single notice arrives and is delivered via Action::Notice.
#[test]
fn single_notice_delivered() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let notice_frame = build_notice_frame(b"WARNING", b"01000", b"test notice");
    let actions = proto.feed_bytes(&notice_frame, &mut wb);

    let mut found_notice = false;
    for action in actions.as_slice() {
        if let Action::Notice { notice_ref } = action {
            found_notice = true;
            let payload = proto.get_notice(*notice_ref);
            assert!(payload.is_ok());
            if let Ok(p) = payload {
                assert_eq!(p.message.as_str(), "test notice");
                assert_eq!(p.severity.as_str(), "WARNING");
            }
        }
    }
    assert!(found_notice, "expected Action::Notice in OutActions");
}

/// Notice ref becomes stale after next feed_bytes cycle.
#[test]
fn notice_ref_stale_after_cycle() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let notice_frame = build_notice_frame(b"NOTICE", b"00000", b"hello");
    let actions = proto.feed_bytes(&notice_frame, &mut wb);

    let mut saved_ref = None;
    for action in actions.as_slice() {
        if let Action::Notice { notice_ref } = action {
            saved_ref = Some(*notice_ref);
        }
    }
    assert!(saved_ref.is_some());

    // Trigger next cycle — feed empty bytes with Idle state
    // fires the per-cycle clear.
    let _actions2 = proto.feed_bytes(&[], &mut wb);

    // Now resolve the stale ref.
    if let Some(r) = saved_ref {
        assert!(matches!(proto.get_notice(r), Err(ArenaError::Stale)));
    }
}

/// Multiple notices in a single feed_bytes call.
#[test]
fn multiple_notices_per_feed() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let mut wire = std::vec::Vec::new();
    wire.extend_from_slice(&build_notice_frame(b"NOTICE", b"00000", b"first"));
    wire.extend_from_slice(&build_notice_frame(b"WARNING", b"01000", b"second"));
    wire.extend_from_slice(&build_notice_frame(b"INFO", b"00000", b"third"));

    let actions = proto.feed_bytes(&wire, &mut wb);

    let mut notice_count = 0u32;
    for action in actions.as_slice() {
        if matches!(action, Action::Notice { .. }) {
            notice_count = notice_count.saturating_add(1);
        }
    }
    assert_eq!(notice_count, 3, "expected 3 notices");
}
