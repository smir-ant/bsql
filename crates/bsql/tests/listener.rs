//! Integration tests: LISTEN/NOTIFY via Listener.
//!
//! Requires a running PostgreSQL.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{BsqlError, Listener};

const DB_URL: &str = "postgres://bsql:bsql@localhost/bsql_test";

#[tokio::test]
async fn listen_and_receive_notification() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("test_channel").await.unwrap();

    // Send a notification from the same listener connection
    listener
        .notify("test_channel", "hello world")
        .await
        .unwrap();

    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.channel(), "test_channel");
    assert_eq!(notif.payload(), "hello world");
}

#[tokio::test]
async fn notification_payload_preserved() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("payload_test").await.unwrap();

    let payload = r#"{"event":"created","id":42}"#;
    listener.notify("payload_test", payload).await.unwrap();

    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.payload(), payload);
}

#[tokio::test]
async fn multiple_channels() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("chan_a").await.unwrap();
    listener.listen("chan_b").await.unwrap();

    listener.notify("chan_a", "from_a").await.unwrap();
    listener.notify("chan_b", "from_b").await.unwrap();

    let n1 = listener.recv().await.unwrap();
    let n2 = listener.recv().await.unwrap();

    // Notifications arrive in order sent
    assert_eq!(n1.channel(), "chan_a");
    assert_eq!(n1.payload(), "from_a");
    assert_eq!(n2.channel(), "chan_b");
    assert_eq!(n2.payload(), "from_b");
}

#[tokio::test]
async fn unlisten_stops_receiving() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("unlisten_test").await.unwrap();
    listener.unlisten("unlisten_test").await.unwrap();

    // Send a notification — should NOT be received since we unlistened
    listener
        .notify("unlisten_test", "should_not_arrive")
        .await
        .unwrap();

    // Listen on a different channel and send there to prove recv works
    listener.listen("unlisten_control").await.unwrap();
    listener
        .notify("unlisten_control", "control")
        .await
        .unwrap();

    let notif = listener.recv().await.unwrap();
    // We should receive the control notification, not the unlistened one
    assert_eq!(notif.channel(), "unlisten_control");
    assert_eq!(notif.payload(), "control");
}

#[tokio::test]
async fn unlisten_all() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("all_a").await.unwrap();
    listener.listen("all_b").await.unwrap();
    listener.unlisten_all().await.unwrap();

    // Neither channel should receive
    listener.notify("all_a", "no").await.unwrap();
    listener.notify("all_b", "no").await.unwrap();

    // Listen on a control channel
    listener.listen("all_control").await.unwrap();
    listener.notify("all_control", "yes").await.unwrap();

    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.channel(), "all_control");
}

#[tokio::test]
async fn empty_channel_name_rejected() {
    let listener = Listener::connect(DB_URL).await.unwrap();
    let result = listener.listen("").await;

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

#[tokio::test]
async fn empty_payload_notification() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("empty_payload").await.unwrap();

    listener.notify("empty_payload", "").await.unwrap();

    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.channel(), "empty_payload");
    assert_eq!(notif.payload(), "");
}

#[tokio::test]
async fn channel_name_with_special_chars() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    // Channel with dashes and dots — valid PG identifier when quoted
    listener.listen("my-channel.v2").await.unwrap();

    listener.notify("my-channel.v2", "special").await.unwrap();

    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.channel(), "my-channel.v2");
    assert_eq!(notif.payload(), "special");
}

#[tokio::test]
async fn payload_with_single_quotes() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("quote_test").await.unwrap();

    listener.notify("quote_test", "it's a test").await.unwrap();

    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.payload(), "it's a test");
}

#[tokio::test]
async fn connect_bad_url_fails() {
    let result = Listener::connect("postgres://nobody:wrong@localhost:1/nope").await;
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

#[tokio::test]
async fn notification_is_clone() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("clone_test").await.unwrap();

    listener.notify("clone_test", "data").await.unwrap();

    let notif = listener.recv().await.unwrap();
    let cloned = notif.clone();
    assert_eq!(cloned.channel(), notif.channel());
    assert_eq!(cloned.payload(), notif.payload());
}

#[tokio::test]
async fn receive_notify_from_separate_connection() {
    let mut listener = Listener::connect(DB_URL).await.unwrap();
    listener.listen("cross_conn_test").await.unwrap();

    // Send from a separate connection — different PG backend than the listener
    let sender = Listener::connect(DB_URL).await.unwrap();
    sender
        .notify("cross_conn_test", "from_sender")
        .await
        .unwrap();

    let n = tokio::time::timeout(std::time::Duration::from_secs(2), listener.recv())
        .await
        .expect("timed out waiting for cross-connection notification")
        .unwrap();

    assert_eq!(n.channel(), "cross_conn_test");
    assert_eq!(n.payload(), "from_sender");
}

#[tokio::test]
async fn null_byte_in_channel_rejected() {
    let listener = Listener::connect(DB_URL).await.unwrap();
    let result = listener.listen("chan\0nel").await;
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

#[tokio::test]
async fn null_byte_in_payload_rejected() {
    let listener = Listener::connect(DB_URL).await.unwrap();
    let result = listener.notify("test", "pay\0load").await;
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

#[tokio::test]
async fn channel_name_sql_injection_attempt() {
    // Attempt SQL injection via channel name — should be safely quoted
    let listener = Listener::connect(DB_URL).await.unwrap();
    let result = listener.listen(r#"test"; DROP TABLE users; --"#).await;

    // This should succeed (the channel name is just a weird identifier)
    // OR it should fail with a PG error, but NOT actually drop the table
    if result.is_ok() {
        // Verify users table still exists
        let pool = bsql::Pool::connect(DB_URL).await.unwrap();
        let users = bsql::query!("SELECT id FROM users LIMIT 1")
            .fetch_optional(&pool)
            .await;
        assert!(users.is_ok(), "users table should still exist");
    }
    // If it errored, that's also fine — the point is no injection
}

#[tokio::test]
async fn listener_drop_cleans_up() {
    {
        let listener = Listener::connect(DB_URL).await.unwrap();
        listener.listen("drop_test").await.unwrap();
        // listener dropped here -- should not panic or leak
    }
    // If we got here, drop succeeded
}

#[tokio::test]
async fn listener_debug_format() {
    let listener = Listener::connect(DB_URL).await.unwrap();
    let debug = format!("{:?}", listener);
    assert!(debug.contains("Listener"), "debug: {debug}");
    assert!(debug.contains("active"), "debug: {debug}");
}
