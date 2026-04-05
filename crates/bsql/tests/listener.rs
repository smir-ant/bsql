//! Integration tests: LISTEN/NOTIFY via Listener.
//!
//! Requires a running PostgreSQL.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{BsqlError, Listener};

const DB_URL: &str = "postgres://bsql:bsql@localhost/bsql_test";

#[test]
fn listen_and_receive_notification() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("test_channel").unwrap();

    // Send a notification from the same listener connection
    listener.notify("test_channel", "hello world").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), "test_channel");
    assert_eq!(notif.payload(), "hello world");
}

#[test]
fn notification_payload_preserved() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("payload_test").unwrap();

    let payload = r#"{"event":"created","id":42}"#;
    listener.notify("payload_test", payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn multiple_channels() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("chan_a").unwrap();
    listener.listen("chan_b").unwrap();

    // notify() now uses a separate short-lived connection internally,
    // avoiding the self-notification race condition entirely.
    listener.notify("chan_a", "from_a").unwrap();
    listener.notify("chan_b", "from_b").unwrap();

    let n1 = listener.recv().unwrap();
    let n2 = listener.recv().unwrap();

    // Both notifications received (order not guaranteed by PG)
    let mut channels: Vec<&str> = vec![n1.channel(), n2.channel()];
    channels.sort();
    assert_eq!(channels, vec!["chan_a", "chan_b"]);

    let mut payloads: Vec<&str> = vec![n1.payload(), n2.payload()];
    payloads.sort();
    assert_eq!(payloads, vec!["from_a", "from_b"]);
}

#[test]
fn unlisten_stops_receiving() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("unlisten_test").unwrap();
    listener.unlisten("unlisten_test").unwrap();

    // Send a notification -- should NOT be received since we unlistened
    listener.notify("unlisten_test", "should_not_arrive").unwrap();

    // Listen on a different channel and send there to prove recv works
    listener.listen("unlisten_control").unwrap();
    listener.notify("unlisten_control", "control").unwrap();

    let notif = listener.recv().unwrap();
    // We should receive the control notification, not the unlistened one
    assert_eq!(notif.channel(), "unlisten_control");
    assert_eq!(notif.payload(), "control");
}

#[test]
fn unlisten_all() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("all_a").unwrap();
    listener.listen("all_b").unwrap();
    listener.unlisten_all().unwrap();

    // Neither channel should receive
    listener.notify("all_a", "no").unwrap();
    listener.notify("all_b", "no").unwrap();

    // Listen on a control channel
    listener.listen("all_control").unwrap();
    listener.notify("all_control", "yes").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), "all_control");
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
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("empty_payload").unwrap();

    listener.notify("empty_payload", "").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), "empty_payload");
    assert_eq!(notif.payload(), "");
}

#[test]
fn channel_name_with_special_chars() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    // Channel with dashes and dots -- valid PG identifier when quoted
    listener.listen("my-channel.v2").unwrap();

    listener.notify("my-channel.v2", "special").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), "my-channel.v2");
    assert_eq!(notif.payload(), "special");
}

#[test]
fn payload_with_single_quotes() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("quote_test").unwrap();

    listener.notify("quote_test", "it's a test").unwrap();

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
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("clone_test").unwrap();

    listener.notify("clone_test", "data").unwrap();

    let notif = listener.recv().unwrap();
    let cloned = notif.clone();
    assert_eq!(cloned.channel(), notif.channel());
    assert_eq!(cloned.payload(), notif.payload());
}

#[test]
fn receive_notify_from_separate_connection() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("cross_conn_test").unwrap();

    // Send from a separate connection -- different PG backend than the listener
    let sender = Listener::connect(DB_URL).unwrap();
    sender.notify("cross_conn_test", "from_sender").unwrap();

    // recv() blocks until a notification arrives (sync API)
    let n = listener.recv().unwrap();

    assert_eq!(n.channel(), "cross_conn_test");
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
    let listener = Listener::connect(DB_URL).unwrap();
    let result = listener.notify("test", "pay\0load");
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
        let users = bsql::query!("SELECT id FROM users LIMIT 1")
            .fetch_optional(&pool);
        assert!(users.is_ok(), "users table should still exist");
    }
    // If it errored, that's also fine -- the point is no injection
}

#[test]
fn listener_drop_cleans_up() {
    {
        let listener = Listener::connect(DB_URL).unwrap();
        listener.listen("drop_test").unwrap();
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
    let mut listener = Listener::connect(DB_URL).unwrap();
    // Channel name with embedded double quotes -- tests quote_ident escaping
    listener.listen(r#"my"chan"#).unwrap();
    listener.notify(r#"my"chan"#, "quoted").unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.channel(), r#"my"chan"#);
    assert_eq!(notif.payload(), "quoted");
}

#[test]
fn payload_with_multiple_quotes() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("multi_quote_test").unwrap();

    let payload = "it''s a ''test''";
    listener.notify("multi_quote_test", payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn payload_with_backslash() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("backslash_test").unwrap();

    let payload = r"C:\Users\test\file.txt";
    listener.notify("backslash_test", payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn payload_with_lone_quote() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("lone_quote_test").unwrap();

    let payload = "it's";
    listener.notify("lone_quote_test", payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload(), payload);
}

#[test]
fn large_payload() {
    let mut listener = Listener::connect(DB_URL).unwrap();
    listener.listen("large_payload_test").unwrap();

    // PG NOTIFY payloads can be up to ~8000 bytes
    let payload = "x".repeat(4000);
    listener.notify("large_payload_test", &payload).unwrap();

    let notif = listener.recv().unwrap();
    assert_eq!(notif.payload().len(), 4000);
}
