//! Integration tests: LISTEN/NOTIFY via Listener.
//!
//! Requires a running PostgreSQL.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{BsqlError, Listener};
use std::sync::atomic::{AtomicU64, Ordering};

const DB_URL: &str = "postgres://bsql:bsql@localhost/bsql_test";

/// Generate a unique channel name to prevent cross-test interference.
/// PG delivers NOTIFY to ALL sessions that LISTEN on the same channel,
/// so parallel tests must use distinct names.
fn unique_channel(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!("{}_{}", prefix, COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[test]
fn listen_and_receive_notification() {
    let ch = unique_channel("test_channel");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    // Send a notification from the same listener connection
    listener.notify(&ch, "hello world").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), ch);
    assert_eq!(notif.payload(), "hello world");
}

#[test]
fn notification_payload_preserved() {
    let ch = unique_channel("payload_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    let payload = r#"{"event":"created","id":42}"#;
    listener.notify(&ch, payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn multiple_channels() {
    let ch_a = unique_channel("chan_a");
    let ch_b = unique_channel("chan_b");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch_a).unwrap();
    listener.listen(&ch_b).unwrap();

    // notify() now uses a separate short-lived connection internally,
    // avoiding the self-notification race condition entirely.
    listener.notify(&ch_a, "from_a").unwrap();
    listener.notify(&ch_b, "from_b").unwrap();

    let n1 = listener.recv().unwrap();
    let n2 = listener.recv().unwrap();

    // Both notifications received (order not guaranteed by PG)
    let mut channels: Vec<&str> = vec![n1.channel(), n2.channel()];
    channels.sort();
    let mut expected_channels = vec![ch_a.as_str(), ch_b.as_str()];
    expected_channels.sort();
    assert_eq!(channels, expected_channels);

    let mut payloads: Vec<&str> = vec![n1.payload(), n2.payload()];
    payloads.sort();
    assert_eq!(payloads, vec!["from_a", "from_b"]);
}

#[test]
fn unlisten_stops_receiving() {
    let ch = unique_channel("unlisten_test");
    let ch_control = unique_channel("unlisten_control");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();
    listener.unlisten(&ch).unwrap();

    // Send a notification -- should NOT be received since we unlistened
    listener.notify(&ch, "should_not_arrive").unwrap();

    // Listen on a different channel and send there to prove recv works
    listener.listen(&ch_control).unwrap();
    listener.notify(&ch_control, "control").unwrap();

    let notif = listener.recv().unwrap();
    // We should receive the control notification, not the unlistened one
    assert_eq!(notif.channel(), ch_control);
    assert_eq!(notif.payload(), "control");
}

#[test]
fn unlisten_all() {
    let ch_a = unique_channel("all_a");
    let ch_b = unique_channel("all_b");
    let ch_control = unique_channel("all_control");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch_a).unwrap();
    listener.listen(&ch_b).unwrap();
    listener.unlisten_all().unwrap();

    // Neither channel should receive
    listener.notify(&ch_a, "no").unwrap();
    listener.notify(&ch_b, "no").unwrap();

    // Listen on a control channel
    listener.listen(&ch_control).unwrap();
    listener.notify(&ch_control, "yes").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), ch_control);
}

#[test]
fn empty_channel_name_rejected() {
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.listen("");

    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Connect(e) => {
            assert!(
                e.message.contains("must not be empty"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Connect error, got: {other:?}"),
    }
}

#[test]
fn empty_payload_notification() {
    let ch = unique_channel("empty_payload");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    listener.notify(&ch, "").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), ch);
    assert_eq!(notif.payload(), "");
}

#[test]
fn channel_name_with_special_chars() {
    let ch = unique_channel("my-channel.v2");
    let mut listener = Listener::connect(DB_URL).unwrap();
    // Channel with dashes and dots -- valid PG identifier when quoted
    listener.listen(&ch).unwrap();

    listener.notify(&ch, "special").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), ch);
    assert_eq!(notif.payload(), "special");
}

#[test]
fn payload_with_single_quotes() {
    let ch = unique_channel("quote_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    listener.notify(&ch, "it's a test").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), "it's a test");
}

#[test]
fn connect_bad_url_fails() {
    let result = Listener::connect("postgres://nobody:wrong@localhost:1/nope");
    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Connect(e) => {
            assert!(
                e.message.contains("listener connect failed"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Connect error, got: {other:?}"),
    }
}

#[test]
fn notification_is_clone() {
    let ch = unique_channel("clone_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    listener.notify(&ch, "data").unwrap();

    let notif = listener.recv().unwrap();
    let cloned = notif.clone();
    assert_eq!(cloned.channel(), notif.channel());
    assert_eq!(cloned.payload(), notif.payload());
}

#[test]
fn receive_notify_from_separate_connection() {
    let ch = unique_channel("cross_conn_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    // Send from a separate connection -- different PG backend than the listener
    let sender = Listener::connect(DB_URL).unwrap();
    sender.notify(&ch, "from_sender").unwrap();

    // recv() blocks until a notification arrives (sync API)
    let n = listener.recv().unwrap();

    assert_eq!(n.channel(), ch);
    assert_eq!(n.payload(), "from_sender");
}

#[test]
fn null_byte_in_channel_rejected() {
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.listen("chan\0nel");
    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Connect(e) => {
            assert!(
                e.message.contains("null bytes"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Connect error, got: {other:?}"),
    }
}

#[test]
fn null_byte_in_payload_rejected() {
    let ch = unique_channel("null_payload_test");
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.notify(&ch, "pay\0load");
    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Connect(e) => {
            assert!(
                e.message.contains("null bytes"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Connect error, got: {other:?}"),
    }
}

#[test]
fn channel_name_sql_injection_attempt() {
    // Attempt SQL injection via channel name -- should be safely quoted
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.listen(r#"test"; DROP TABLE users; --"#);

    // This should succeed (the channel name is just a weird identifier)
    // OR it should fail with a PG error, but NOT actually drop the table
    if result.is_ok() {
        // Verify users table still exists
        let pool = bsql::Pool::connect(DB_URL).unwrap();
        let users = bsql::query!("SELECT id FROM users LIMIT 1").fetch_optional(&pool);
        assert!(users.is_ok(), "users table should still exist");
    }
    // If it errored, that's also fine -- the point is no injection
}

#[test]
fn listener_drop_cleans_up() {
    {
        let ch = unique_channel("drop_test");
        let listener = Listener::connect(DB_URL).unwrap();
        listener.listen(&ch).unwrap();
        // listener dropped here -- should not panic or leak
    }
    // If we got here, drop succeeded
}

#[test]
fn listener_debug_format() {
    let listener = Listener::connect(DB_URL).unwrap();
    let debug = format!("{:?}", listener);
    assert!(debug.contains("Listener"), "debug: {debug}");
    assert!(debug.contains("active"), "debug: {debug}");
}

#[test]
fn unlisten_empty_name_rejected() {
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.unlisten("");
    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Connect(e) => {
            assert!(
                e.message.contains("must not be empty"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Connect error, got: {other:?}"),
    }
}

#[test]
fn notify_empty_channel_rejected() {
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.notify("", "payload");
    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Connect(e) => {
            assert!(
                e.message.contains("must not be empty"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Connect error, got: {other:?}"),
    }
}

#[test]
fn channel_name_with_double_quotes() {
    let ch = unique_channel(r#"my"chan"#);
    let mut listener = Listener::connect(DB_URL).unwrap();
    // Channel name with embedded double quotes -- tests quote_ident escaping
    listener.listen(&ch).unwrap();
    listener.notify(&ch, "quoted").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), ch);
    assert_eq!(notif.payload(), "quoted");
}

#[test]
fn payload_with_multiple_quotes() {
    let ch = unique_channel("multi_quote_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    let payload = "it''s a ''test''";
    listener.notify(&ch, payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn payload_with_backslash() {
    let ch = unique_channel("backslash_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    let payload = r"C:\Users\test\file.txt";
    listener.notify(&ch, payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn payload_with_lone_quote() {
    let ch = unique_channel("lone_quote_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    let payload = "it's";
    listener.notify(&ch, payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn large_payload() {
    let ch = unique_channel("large_payload_test");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    // PG NOTIFY payloads can be up to ~8000 bytes
    let payload = "x".repeat(4000);
    listener.notify(&ch, &payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload().len(), 4000);
}

// ---------------------------------------------------------------------------
// edge case: listen same channel twice (idempotent)
// ---------------------------------------------------------------------------

#[test]
fn listen_same_channel_twice() {
    let ch = unique_channel("dup_listen_ch");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();
    // Second listen on the same channel should not error (PG LISTEN is idempotent).
    listener.listen(&ch).unwrap();

    // Sending one notification should produce exactly one received message.
    listener.notify(&ch, "once").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), ch);
    assert_eq!(notif.payload(), "once");

    // Verify there is no duplicate notification waiting.
    let maybe = listener.try_recv().unwrap();
    assert!(
        maybe.is_none(),
        "should not receive a duplicate notification"
    );
}

// ---------------------------------------------------------------------------
// edge case: unlisten a channel that was never listened
// ---------------------------------------------------------------------------

#[test]
fn unlisten_never_listened_channel() {
    let ch = unique_channel("never_listened_ch");
    let listener = Listener::connect(DB_URL).unwrap();
    // PG UNLISTEN on a channel we never LISTENed should not error.
    let result = listener.unlisten(&ch);
    assert!(
        result.is_ok(),
        "unlisten on never-listened channel should succeed"
    );
}

// ---------------------------------------------------------------------------
// edge case: try_recv when no notifications pending
// ---------------------------------------------------------------------------

#[test]
fn try_recv_empty() {
    let ch = unique_channel("try_recv_empty_ch");
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen(&ch).unwrap();

    // No notifications have been sent — try_recv should return None.
    let result = listener.try_recv().unwrap();
    assert!(
        result.is_none(),
        "try_recv with no pending notifications should return None"
    );
}

// ---------------------------------------------------------------------------
// subscribed_channels
// ---------------------------------------------------------------------------

#[test]
fn subscribed_channels_returns_list() {
    let ch_a = unique_channel("sub_ch_a");
    let ch_b = unique_channel("sub_ch_b");
    let listener = Listener::connect(DB_URL).unwrap();

    // Before any listen, subscribed_channels should be empty
    let channels = listener.subscribed_channels();
    assert!(channels.is_empty());

    // Listen to two channels
    listener.listen(&ch_a).unwrap();
    listener.listen(&ch_b).unwrap();

    let mut channels = listener.subscribed_channels();
    channels.sort();
    let mut expected = vec![ch_a.as_str(), ch_b.as_str()];
    expected.sort();
    assert_eq!(channels, expected);
}

#[test]
fn subscribed_channels_updates_on_unlisten() {
    let ch_a = unique_channel("sub_ul_a");
    let ch_b = unique_channel("sub_ul_b");
    let ch_c = unique_channel("sub_ul_c");
    let listener = Listener::connect(DB_URL).unwrap();

    listener.listen(&ch_a).unwrap();
    listener.listen(&ch_b).unwrap();
    listener.listen(&ch_c).unwrap();

    let mut channels = listener.subscribed_channels();
    channels.sort();
    let mut expected_abc = vec![ch_a.as_str(), ch_b.as_str(), ch_c.as_str()];
    expected_abc.sort();
    assert_eq!(channels, expected_abc);

    listener.unlisten(&ch_b).unwrap();

    let mut channels = listener.subscribed_channels();
    channels.sort();
    let mut expected_ac = vec![ch_a.as_str(), ch_c.as_str()];
    expected_ac.sort();
    assert_eq!(channels, expected_ac);
}

#[test]
fn subscribed_channels_empty_after_unlisten_all() {
    let ch_a = unique_channel("sub_ua_a");
    let ch_b = unique_channel("sub_ua_b");
    let listener = Listener::connect(DB_URL).unwrap();

    listener.listen(&ch_a).unwrap();
    listener.listen(&ch_b).unwrap();
    assert_eq!(listener.subscribed_channels().len(), 2);

    listener.unlisten_all().unwrap();
    assert!(listener.subscribed_channels().is_empty());
}

#[test]
fn subscribed_channels_idempotent_listen() {
    let ch = unique_channel("sub_idem");
    let listener = Listener::connect(DB_URL).unwrap();

    listener.listen(&ch).unwrap();
    listener.listen(&ch).unwrap(); // duplicate

    let channels = listener.subscribed_channels();
    // Should have exactly 1 entry, not 2
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0], ch);
}
