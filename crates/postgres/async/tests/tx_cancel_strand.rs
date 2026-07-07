//! Witness: a transaction future dropped BEFORE its first verb leaves NO armed
//! `BEGIN` on the reused (bare, non-pooled) connection — the async driver.
//!
//! The hazard: `transaction()` used to arm the deferred `BEGIN` out-of-band at
//! entry, so a body whose first suspending await is non-bsql (a sleep / lock /
//! external fetch) that is then dropped (tokio `timeout`, a lost `select!`
//! branch, a task abort) left the `BEGIN` armed-but-unsent while the liveness
//! token was still `Some` (healthy). The next verb on the reused connection then
//! SILENTLY fused a transaction the caller never asked for. Option 2 arms the
//! `BEGIN` INSIDE the first verb, within the `take_live` window that verb opens,
//! so a drop-before-any-verb arms nothing.
//!
//! The observable, over the in-memory fake with NO socket: `BEGIN` is
//! deliberately NOT scripted, so a stranded fused `BEGIN` hits the unmatched
//! `ErrorResponse` and the fused-prelude drain KILLS the connection — the next
//! verb fails. No strand ⇒ the bare verb runs clean.
//!
//! Run with `cargo test -p bsql-postgres-async --features testkit`.
#![cfg(feature = "testkit")]
#![allow(
    clippy::expect_used,
    reason = "test harness — fixture builders panic loudly on malformed synthetic wire bytes; not a `#[test]` fn so the in-tests carve-out cannot reach them, and there is no production data-fallback path"
)]

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use bsql_postgres_async::Connection;
use bsql_postgres_core::testkit::wire::{self, OID_INT8, TX_IDLE};
use bsql_postgres_core::testkit::{FakeScript, FakeTransport, QueryReply};

fn handshake() -> Vec<u8> {
    wire::concat(&[
        wire::auth_ok().expect("auth_ok"),
        wire::parameter_status("server_version", "17.0 (bsql-testkit)").expect("param status"),
        wire::backend_key_data(4321, 0).expect("backend key data"),
        wire::ready_for_query(TX_IDLE).expect("ready for query"),
    ])
}

/// A one-row `int8` result for `SELECT 1` (simple + extended).
fn select_one_reply() -> QueryReply {
    let simple = wire::concat(&[
        wire::row_description(&[("one".to_owned(), OID_INT8)]).expect("row description"),
        wire::data_row(&[Some(b"1".to_vec())]).expect("data row"),
        wire::command_complete("SELECT 1").expect("command complete"),
        wire::ready_for_query(TX_IDLE).expect("ready for query"),
    ]);
    let extended = wire::concat(&[
        wire::data_row(&[Some(wire::binary_int8(1))]).expect("data row"),
        wire::command_complete("SELECT 1").expect("command complete"),
    ]);
    QueryReply { simple, extended }
}

/// Scripts `SELECT 1` but deliberately NOT `BEGIN`: a stranded fused `BEGIN`
/// hits the unmatched `ErrorResponse` → the fused-prelude drain kills the
/// connection, so the verb fails. No strand ⇒ clean success.
fn script() -> FakeScript {
    FakeScript {
        handshake: handshake(),
        queries: vec![("SELECT 1".to_owned(), select_one_reply())],
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
async fn a_transaction_dropped_before_its_first_verb_leaves_no_armed_begin() {
    let mut conn = Connection::connect_fake(FakeTransport::new(script()))
        .await
        .expect("connect over the in-memory fake");

    // Build a transaction whose FIRST await is a non-bsql pending future, poll it
    // once (so the old out-of-band arming would have staged the BEGIN), then let
    // the future drop mid-body — exactly a tokio `timeout` / lost `select!`
    // branch / task abort. The token is never taken, so the connection stays
    // healthy and is reused BARE.
    {
        let mut fut = pin!(conn.transaction(async |tx| {
            core::future::pending::<()>().await; // never resolves; no verb runs
            tx.query_sql("SELECT 1").await.map(|_| ())
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
            "the body suspends at the non-bsql await before any verb runs",
        );
    } // the transaction future is dropped here, releasing the borrow on `conn`

    // The reused bare connection must run a BARE verb, not a fused BEGIN. Under
    // the strand bug the unscripted fused BEGIN would kill the connection here.
    let result = conn
        .query_sql("SELECT 1")
        .await
        .expect("the reused connection must run a BARE verb, not a fused BEGIN");
    assert_eq!(result.len(), 1, "the bare SELECT returns its one row");
    assert_eq!(result.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    assert_eq!(result.command_tag, "SELECT 1");
}
