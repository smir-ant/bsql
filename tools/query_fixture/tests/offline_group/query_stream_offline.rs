//! OFFLINE proof of the streaming tier-1 no-swallow decode-error path, over a
//! real sync `Connection` driven against a SCRIPTED loopback server (no live PG).
//!
//! A correctly-validated `query!` cannot honestly produce a live decode error (the
//! build-checked record type matches what PG sends), so the no-swallow guarantee
//! is proven here with a scripted server that deliberately delivers a MALFORMED
//! `DataRow` mid-stream. The driver's `query_each`:
//!   1. delivers the well-formed rows before it to `on_row`,
//!   2. stops the stream on the malformed row with a LOUD
//!      `Err(DriverError::Decode(..))` — never a Continue-past, never a default,
//!   3. DRAINS the connection's remaining frames to a clean idle and restores the
//!      linear token, so the connection stays HEALTHY and REUSABLE — a follow-up
//!      verb on the SAME connection succeeds.
//!
//! The scripted server writes its whole reply script upfront (a few hundred bytes,
//! well under the socket buffer) then drains the client's writes to EOF, so there
//! is no lockstep to race: the sync client reads exactly the bytes it needs for
//! each phase from the OS receive buffer.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "offline harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::ops::ControlFlow;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

// Two `int8 NOT NULL` columns -> the all-fixed-width record `MalStream { id,
// user_id }` (both `i64`). `orders.id` / `orders.user_id` exist in the fixture's
// migrations, so this validates against the build catalog.
bsql::query!(MalStream, "SELECT id, user_id FROM orders");

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A `ParameterStatus` ('S') report: `key\0value\0`.
fn parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = key.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

/// AuthenticationOk + a `server_version` `ParameterStatus` + BackendKeyData +
/// ReadyForQuery — the trust handshake as a real server sends it. `connect`
/// captures `server_version` from the report, so no `SHOW` round-trip follows.
fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&parameter_status("server_version", "16.0"));
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = tag.as_bytes().to_vec();
    body.push(0);
    frame(b'C', &body)
}

fn rfq() -> Vec<u8> {
    frame(b'Z', b"I")
}

/// A `RowDescription` for the `(int8 id, int8 user_id)` row shape — the reply to
/// the `Describe(portal)` a cache MISS appends, which the typed result-schema
/// guard verifies (OIDs [20, 20]) then discards.
fn row_desc() -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for name in ["id", "user_id"] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0_i16.to_be_bytes()); // column attr
        body.extend_from_slice(&20_i32.to_be_bytes()); // type OID (int8)
        body.extend_from_slice(&8_i16.to_be_bytes()); // typlen
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // typmod
        body.extend_from_slice(&0_i16.to_be_bytes()); // format
    }
    frame(b'T', &body)
}

/// A well-formed `[int8=id][int8=user_id]` `DataRow`.
fn int8_row(id: i64, user_id: i64) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for v in [id, user_id] {
        body.extend_from_slice(&8_i32.to_be_bytes());
        body.extend_from_slice(&v.to_be_bytes());
    }
    frame(b'D', &body)
}

/// A DataRow whose SECOND column declares an 8-byte length but supplies only 2
/// bytes: the wire FRAME is well-formed (correct length prefix), but the internal
/// column framing is truncated, so the typed decode classifies a `DecodeError`
/// instead of reading past the row body.
fn malformed_int8_row() -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    body.extend_from_slice(&8_i32.to_be_bytes()); // col0 len 8
    body.extend_from_slice(&100_i64.to_be_bytes()); // col0 = 100 (well-formed)
    body.extend_from_slice(&8_i32.to_be_bytes()); // col1 declares len 8
    body.extend_from_slice(&[0, 7]); // col1 supplies only 2 bytes
    frame(b'D', &body)
}

/// The full scripted server reply: handshake (with `server_version` captured
/// from its `ParameterStatus`, so no connect-time `SHOW` follows), the streaming
/// query's MISS reply (one GOOD row then a MALFORMED row, then CommandComplete +
/// ReadyForQuery for the drain to reach), and finally the follow-up ping's
/// ReadyForQuery.
fn server_script() -> Vec<u8> {
    let mut out = handshake();
    // query_each MISS reply.
    out.extend_from_slice(&frame(b'3', &[])); // CloseComplete
    out.extend_from_slice(&frame(b'1', &[])); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    out.extend_from_slice(&row_desc()); // RowDescription (Describe portal)
    out.extend_from_slice(&int8_row(100, 7)); // GOOD row -> delivered to on_row
    out.extend_from_slice(&malformed_int8_row()); // MALFORMED -> Err(Decode)
    out.extend_from_slice(&command_complete("SELECT 2")); // drain reads this ...
    out.extend_from_slice(&rfq()); // ... and this, reaching Idle
    // Follow-up ping reply (a bare Sync -> ReadyForQuery).
    out.extend_from_slice(&rfq());
    out
}

#[test]
fn malformed_row_mid_stream_is_loud_decode_err_and_connection_survives() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let port = listener.local_addr().expect("addr").port();

    let handle = thread::spawn(move || {
        let (mut sock, _) = listener.accept().expect("accept");
        // Write the whole script upfront; the client reads each phase's bytes from
        // the OS buffer as it needs them.
        sock.write_all(&server_script()).expect("write script");
        sock.flush().ok();
        // Drain the client's writes to EOF so its sends never block and the socket
        // stays open until the client closes.
        let mut buf = [0u8; 1024];
        while matches!(sock.read(&mut buf), Ok(n) if n > 0) {}
    });

    let cfg = ConnectConfig::new("127.0.0.1", "streamer")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
        .port(port)
        .connect_timeout(5);
    let mut c = Connection::connect(&cfg).expect("connect to scripted server");

    // Stream: the good row reaches `on_row`, the malformed one stops with a LOUD
    // classified decode error.
    let mut seen = 0usize;
    let result = c.query_each::<MalStreamQuery, _, _>((), |_row| {
        seen += 1;
        ControlFlow::<()>::Continue(())
    });
    assert!(
        matches!(result, Err(DriverError::Decode(_))),
        "a malformed row must be a loud Err(Decode), never swallowed — got {result:?}"
    );
    assert_eq!(
        seen, 1,
        "the well-formed row was delivered before the malformed one broke the stream"
    );

    // The token was restored by the drain: the connection is healthy + reusable.
    assert!(
        c.is_healthy(),
        "the connection stays healthy after a decode error (drained + token restored)"
    );
    c.ping().expect("a follow-up verb succeeds on the reused connection");

    // Close the client so the server thread sees EOF and exits.
    c.close().expect("close");
    drop(c);
    handle.join().expect("server thread joins");
}
