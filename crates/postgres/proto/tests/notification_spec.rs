//! DEF-220 LISTEN/NOTIFY integration tests.
//!
//! Validates the end-to-end NotificationResponse ('A') wire-protocol
//! surface:
//! 1. Feed a NotificationResponse frame via `feed_bytes`
//! 2. OutActions stream contains `Action::Notify { pid, notif_ref }`
//! 3. `proto.get_notification(notif_ref)` resolves to the payload
//!    with `channel.as_str()` and `payload` matching the wire bytes
//! 4. The per-cycle arena clear (Idle/Errored transition) invalidates
//!    the ref — subsequent resolution returns `ArenaError::Stale`

#![forbid(unsafe_code)]

use bsql_postgres_proto::{Action, ArenaError, WriteBuf, wire::TAG_NOTIFICATION_RESPONSE};

mod common;
use common::fresh_active_via_trust_handshake;

/// Build a NotificationResponse frame body: pid (BE i32) + CSTR
/// channel + CSTR payload.
fn build_notification_frame(pid: i32, channel: &[u8], payload: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(channel);
    body.push(0);
    body.extend_from_slice(payload);
    body.push(0);

    let Ok(body_len_with_self) = u32::try_from(body.len().saturating_add(4)) else {
        return std::vec::Vec::new();
    };

    let mut frame = std::vec::Vec::new();
    frame.push(TAG_NOTIFICATION_RESPONSE.byte());
    frame.extend_from_slice(&body_len_with_self.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

#[test]
fn notification_frame_surfaces_via_action_notify() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let frame = build_notification_frame(12345, b"my_channel", b"hello world");

    // feed_bytes processes the NotificationResponse frame; OutActions
    // should contain Action::Notify { pid, notif_ref }. Extract the
    // ref by-value (Copy) so we can drop the actions borrow before
    // calling get_notification.
    let (pid, notif_ref) = {
        let actions = proto.feed_bytes(&frame, &mut wb);
        let slice = actions.as_slice();
        assert_eq!(slice.len(), 1, "expected single Action::Notify");
        let Action::Notify { pid, notif_ref } = slice[0] else {
            panic!("expected Action::Notify variant; got {:?}", slice[0])
        };
        (pid, notif_ref)
    };
    assert_eq!(pid, 12345);

    // Resolve via get_notification — must succeed within the same
    // OutActions cycle.
    let resolved = proto.get_notification(notif_ref);
    let Ok(payload) = resolved else {
        panic!("get_notification must resolve in same cycle; got {resolved:?}")
    };
    assert_eq!(payload.pid, 12345);
    assert_eq!(payload.channel.as_str(), "my_channel");
    assert_eq!(payload.payload.as_slice(), b"hello world");
}

#[test]
fn multiple_notifications_in_one_feed() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Three NotificationResponse frames in one feed_bytes call.
    let mut combined = std::vec::Vec::new();
    combined.extend(build_notification_frame(1, b"ch1", b"p1"));
    combined.extend(build_notification_frame(2, b"ch2", b"p2"));
    combined.extend(build_notification_frame(3, b"ch3", b"p3"));

    // Extract refs by-value (Copy) so we can resolve after the
    // OutActions borrow drops.
    let refs: std::vec::Vec<(i32, _)> = {
        let actions = proto.feed_bytes(&combined, &mut wb);
        let slice = actions.as_slice();
        assert_eq!(slice.len(), 3, "expected 3 Action::Notify entries");
        let mut out = std::vec::Vec::new();
        for action in slice.iter() {
            if let Action::Notify { pid, notif_ref } = action {
                out.push((*pid, *notif_ref));
            }
        }
        out
    };
    assert_eq!(refs.len(), 3);

    for (idx, expected_pid) in [(0_usize, 1_i32), (1, 2), (2, 3)] {
        let (pid, notif_ref) = refs[idx];
        assert_eq!(pid, expected_pid);
        let Ok(payload) = proto.get_notification(notif_ref) else {
            panic!("ref {idx} must resolve")
        };
        assert_eq!(payload.pid, expected_pid);
    }
}

#[test]
fn refs_become_stale_after_next_feed_bytes() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Cycle 1: receive a notification.
    let frame1 = build_notification_frame(42, b"chan_a", b"payload_a");
    let stashed_ref = {
        let actions = proto.feed_bytes(&frame1, &mut wb);
        let slice = actions.as_slice();
        let Action::Notify { notif_ref, .. } = slice[0] else {
            panic!("cycle 1 must produce Notify")
        };
        notif_ref
    };

    // Cycle 2: any feed_bytes — even an empty one — triggers the
    // Idle-boundary arena clear (via clear_session_residue_for_class
    // at dispatch entry). The stashed ref should now resolve Stale.
    let _ = proto.feed_bytes(&[], &mut wb);

    assert!(matches!(
        proto.get_notification(stashed_ref),
        Err(ArenaError::Stale)
    ));
}

#[test]
fn empty_payload_notification_resolves() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Channel set, payload empty (PG NOTIFY without payload string).
    let frame = build_notification_frame(99, b"signal", b"");
    let (pid, notif_ref) = {
        let actions = proto.feed_bytes(&frame, &mut wb);
        let slice = actions.as_slice();
        assert_eq!(slice.len(), 1);
        let Action::Notify { pid, notif_ref } = slice[0] else {
            panic!("must produce Notify")
        };
        (pid, notif_ref)
    };
    assert_eq!(pid, 99);

    let Ok(payload) = proto.get_notification(notif_ref) else {
        panic!("ref must resolve")
    };
    assert_eq!(payload.channel.as_str(), "signal");
    assert!(payload.payload.is_empty());
}
