//! The transport-injection seam, proven at the driver level with NO socket.
//!
//! A genuine [`Connection`] — the same concrete type `connect` returns — is
//! built over an in-memory [`FakeTransport`] and driven through the real
//! startup handshake and a real `query_sql` round trip. The bytes the fake
//! serves are parsed by the real sans-IO engine, so a passing decode proves
//! both that the injection works and that the fake's bytes are wire-correct.
//!
//! Feature-gated on `testkit`: run with
//! `cargo test -p bsql-postgres-async --features testkit`.
#![cfg(feature = "testkit")]
// The `handshake`/`*_reply`/`script` fixture helpers below are not `#[test]`
// fns, so the floor's `allow-expect-in-tests` carve-out (keyed on `#[test]`
// context) cannot reach their `.expect()`s. They build synthetic wire bytes and
// panic loudly on a malformed fixture (the intended test-authoring signal); no
// production data path exists here.
#![allow(
    clippy::expect_used,
    reason = "test harness — fixture-builder helpers panic loudly on malformed synthetic wire bytes; not a `#[test]` fn so the in-tests carve-out cannot reach them, and there is no production data-fallback path"
)]

use bsql_postgres_async::Connection;
use bsql_postgres_core::testkit::wire::{self, OID_INT8, TX_IDLE};
use bsql_postgres_core::testkit::{FakeScript, FakeTransport, QueryReply};

/// A trust-auth handshake reply: `AuthenticationOk` + `ParameterStatus` +
/// `BackendKeyData` + `ReadyForQuery(idle)`.
fn handshake() -> Vec<u8> {
    wire::concat(&[
        wire::auth_ok().expect("auth_ok"),
        wire::parameter_status("server_version", "17.0 (bsql-testkit)").expect("param status"),
        wire::backend_key_data(4321, 0).expect("backend key data"),
        wire::ready_for_query(TX_IDLE).expect("ready for query"),
    ])
}

/// A two-row `int8` result for `SELECT id FROM users`, in both protocols: text
/// for the simple path, binary for the extended path.
fn users_reply() -> QueryReply {
    let simple = wire::concat(&[
        wire::row_description(&[("id".to_owned(), OID_INT8)]).expect("row description"),
        wire::data_row(&[Some(b"1".to_vec())]).expect("data row 1"),
        wire::data_row(&[Some(b"2".to_vec())]).expect("data row 2"),
        wire::command_complete("SELECT 2").expect("command complete"),
        wire::ready_for_query(TX_IDLE).expect("ready for query"),
    ]);
    let extended = wire::concat(&[
        wire::data_row(&[Some(wire::binary_int8(1))]).expect("data row 1"),
        wire::data_row(&[Some(wire::binary_int8(2))]).expect("data row 2"),
        wire::command_complete("SELECT 2").expect("command complete"),
    ]);
    QueryReply { simple, extended }
}

fn error_reply(sqlstate: &str, message: &str) -> QueryReply {
    let simple = wire::concat(&[
        wire::error_response("ERROR", sqlstate, message).expect("error response"),
        wire::ready_for_query(TX_IDLE).expect("ready for query"),
    ]);
    let extended = wire::error_response("ERROR", sqlstate, message).expect("error response");
    QueryReply { simple, extended }
}

fn script() -> FakeScript {
    FakeScript {
        handshake: handshake(),
        queries: vec![("SELECT id FROM users".to_owned(), users_reply())],
        unmatched_simple: wire::concat(&[
            wire::error_response("ERROR", "XX000", "no scripted reply").expect("error response"),
            wire::ready_for_query(TX_IDLE).expect("ready for query"),
        ]),
        unmatched_extended: wire::error_response("ERROR", "XX000", "no scripted reply")
            .expect("error response"),
        parse_complete: wire::parse_complete().expect("parse complete"),
        bind_complete: wire::bind_complete().expect("bind complete"),
        close_complete: wire::close_complete().expect("close complete"),
        unsupported_error: wire::error_response("ERROR", "0A000", "extended protocol unsupported")
            .expect("error response"),
        ready_for_query: wire::ready_for_query(TX_IDLE).expect("ready for query"),
    }
}

#[tokio::test]
async fn connect_and_query_over_the_fake_with_no_socket() {
    let mut conn = Connection::connect_fake(FakeTransport::new(script()))
        .await
        .expect("connect over the in-memory fake");

    // The server_version captured from the handshake ParameterStatus.
    assert_eq!(conn.server_version(), Some("17.0 (bsql-testkit)"));

    // The in-memory fake is plaintext by construction, so the encryption
    // accessor — wired to the real `Wire` arm — reports `false`. (A live TLS
    // connect reports `true`; that half needs a TLS-enabled server, exercised in
    // the ignored live suite.)
    assert!(!conn.is_encrypted(), "the plaintext fake wire must report unencrypted");

    let result = conn
        .query_sql("SELECT id FROM users")
        .await
        .expect("query the fake");

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get_i64(0), Ok(Some(1)));
    assert_eq!(result.rows[1].get_i64(0), Ok(Some(2)));
    assert_eq!(result.command_tag, "SELECT 2");
}

#[tokio::test]
async fn scripted_error_surfaces_as_a_db_error() {
    let mut script = script();
    script
        .queries
        .push(("SELECT boom".to_owned(), error_reply("22012", "division by zero")));
    let mut conn = Connection::connect_fake(FakeTransport::new(script))
        .await
        .expect("connect over the in-memory fake");

    let err = conn
        .query_sql("SELECT boom")
        .await
        .expect_err("scripted error must surface");
    let text = format!("{err}");
    assert!(text.contains("division by zero"), "got: {text}");
}

#[tokio::test]
async fn unmatched_query_is_a_loud_error_not_empty_rows() {
    let mut conn = Connection::connect_fake(FakeTransport::new(script()))
        .await
        .expect("connect over the in-memory fake");

    let err = conn
        .query_sql("SELECT something_unscripted")
        .await
        .expect_err("an unscripted query must be a loud error, never empty rows");
    let text = format!("{err}");
    assert!(text.contains("no scripted reply"), "got: {text}");
}

#[tokio::test]
async fn a_query_after_a_failed_extended_op_returns_its_rows_on_the_same_connection() {
    // `prepare` is the extended query protocol — a BATCH of Parse + Describe +
    // Sync. The fake does not support extended, so it must emit exactly ONE
    // ErrorResponse + ONE ReadyForQuery for the whole batch (PostgreSQL's
    // error-then-skip-to-Sync recovery), leaving the connection clean. If the
    // fake instead emitted one E+Z per message, the surplus would strand in the
    // outbox and this REUSED connection's next scripted query would wrongly read
    // the stale error instead of its rows.
    let mut conn = Connection::connect_fake(FakeTransport::new(script()))
        .await
        .expect("connect over the in-memory fake");

    // The extended op fails loudly.
    let err = conn
        .prepare("SELECT 1")
        .await
        .expect_err("extended protocol must be a loud error on the fake");
    assert!(
        format!("{err}").contains("extended protocol unsupported"),
        "got: {err}"
    );

    // The SAME connection is clean: the scripted simple query returns ITS rows,
    // not the stale error from the failed extended batch.
    let result = conn
        .query_sql("SELECT id FROM users")
        .await
        .expect("the reused connection must return the scripted rows");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get_i64(0), Ok(Some(1)));
    assert_eq!(result.rows[1].get_i64(0), Ok(Some(2)));
}
