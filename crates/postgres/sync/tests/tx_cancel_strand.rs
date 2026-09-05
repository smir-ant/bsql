//! Witness: a transaction body that PANICS before its first verb leaves NO
//! armed `BEGIN` on the reused (bare, non-pooled) connection — the sync driver.
//!
//! The sync analogue of the async cancel: `transaction()` used to arm the
//! deferred `BEGIN` out-of-band at entry, so a body that panics before issuing
//! any verb (the panic caught upstream, the bare connection reused) left the
//! `BEGIN` armed-but-unsent while the liveness token was still `Some` — the next
//! verb then silently fused a transaction the caller never asked for. Option 2
//! arms the `BEGIN` inside the first verb, which never ran, so nothing is armed.
//!
//! The observable, over the in-memory fake with NO socket: `BEGIN` is
//! deliberately NOT scripted, so a stranded fused `BEGIN` hits the unmatched
//! `ErrorResponse` and the fused-prelude drain KILLS the connection — the next
//! verb fails. No strand ⇒ the bare verb runs clean.
//!
//! Run with `cargo test -p bsql-postgres-sync --features testkit`.
#![cfg(feature = "testkit")]
#![allow(
    clippy::expect_used,
    reason = "test harness — fixture builders panic loudly on malformed synthetic wire bytes; not a `#[test]` fn so the in-tests carve-out cannot reach them, and there is no production data-fallback path"
)]

use std::panic::{catch_unwind, AssertUnwindSafe};

use bsql_postgres_core::testkit::wire::{self, OID_INT8, TX_IDLE};
use bsql_postgres_core::testkit::{FakeScript, FakeTransport, QueryReply};
use bsql_postgres_sync::{Connection, DriverError};

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
    // The Describe(portal) reply for the typed cache-MISS path (int8 column).
    let row_description =
        wire::row_description(&[("one".to_owned(), OID_INT8)]).expect("row description");
    QueryReply { simple, extended, row_description }
}

/// Scripts `SELECT 1` but deliberately NOT `BEGIN` (see the async twin).
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

#[test]
fn a_transaction_panicking_before_its_first_verb_leaves_no_armed_begin() {
    let mut conn = Connection::connect_fake(FakeTransport::new(script()))
        .expect("connect over the in-memory fake");

    // Silence the default panic hook: the deliberate body panic below is
    // EXPECTED and caught; without this it would spam the test log. Restored
    // immediately after so an unrelated panic still reports.
    let prior_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        conn.transaction(|_tx| -> Result<(), DriverError> {
            panic!("boom before any verb");
        })
    }));
    std::panic::set_hook(prior_hook);
    assert!(outcome.is_err(), "the body panic must propagate out of transaction()");

    // The reused bare connection must run a BARE verb, not a fused BEGIN. Under
    // the strand bug the unscripted fused BEGIN would kill the connection here.
    let result = conn
        .query_raw("SELECT 1")
        .expect("the reused connection must run a BARE verb, not a fused BEGIN");
    assert_eq!(result.len(), 1, "the bare SELECT returns its one row");
    assert_eq!(result.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    assert_eq!(result.command_tag().to_string(), "SELECT 1");
}

fn simple_command_reply(cmd: &str, tx_status: u8) -> QueryReply {
    let simple = wire::concat(&[
        wire::command_complete(cmd).expect("command complete"),
        wire::ready_for_query(tx_status).expect("ready for query"),
    ]);
    QueryReply {
        simple,
        extended: Vec::new(),
        row_description: Vec::new(),
    }
}

fn select_val_reply(val: i64, tag: &str, tx_status: u8) -> QueryReply {
    let simple = wire::concat(&[
        wire::row_description(&[("val".to_owned(), OID_INT8)]).expect("row description"),
        wire::data_row(&[Some(val.to_string().into_bytes())]).expect("data row"),
        wire::command_complete(tag).expect("command complete"),
        wire::ready_for_query(tx_status).expect("ready for query"),
    ]);
    let extended = wire::concat(&[
        wire::data_row(&[Some(wire::binary_int8(val))]).expect("data row"),
        wire::command_complete(tag).expect("command complete"),
    ]);
    let row_description =
        wire::row_description(&[("val".to_owned(), OID_INT8)]).expect("row description");
    QueryReply { simple, extended, row_description }
}

fn script_with_rollback() -> FakeScript {
    FakeScript {
        handshake: handshake(),
        queries: vec![
            ("BEGIN".to_owned(), simple_command_reply("BEGIN", b'T')),
            ("SELECT 10".to_owned(), select_val_reply(10, "SELECT 10", b'T')),
            ("ROLLBACK".to_owned(), simple_command_reply("ROLLBACK", TX_IDLE)),
            ("SELECT 20".to_owned(), select_val_reply(20, "SELECT 20", TX_IDLE)),
        ],
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

#[test]
fn a_transaction_panicking_after_its_first_verb_rolls_back_synchronously_on_drop() {
    let mut conn = Connection::connect_fake(FakeTransport::new(script_with_rollback()))
        .expect("connect over the in-memory fake");

    let prior_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        conn.transaction(|tx| -> Result<(), DriverError> {
            let res = tx.query_raw("SELECT 10")?;
            assert_eq!(res.len(), 1);
            panic!("boom after first verb inside tx");
        })
    }));
    std::panic::set_hook(prior_hook);
    assert!(outcome.is_err(), "the body panic must propagate out of transaction()");

    // Verify connection state after panic unwinding:
    assert!(!conn.tx_needs_rollback(), "sync Drop cleared tx_needs_rollback");
    assert_eq!(conn.tx_status(), Some(bsql_postgres_sync::TxStatus::Idle));
    assert!(conn.is_healthy());

    // Subsequent command on reused connection succeeds cleanly:
    let result = conn
        .query_raw("SELECT 20")
        .expect("connection is cleanly reusable after panic rollback");
    assert_eq!(result.len(), 1);
    assert_eq!(result.get(0).expect("row 0").get_i64(0), Ok(Some(20)));
}

