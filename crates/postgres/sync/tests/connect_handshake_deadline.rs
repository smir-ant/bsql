#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "offline loopback witness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

//! Regression witness for the sync handshake AGGREGATE wall-clock deadline
//! (audit-9 item 1).
//!
//! A real `std::net` [`Connection`] connects to an in-test loopback server that
//! completes the client's startup packet and then DRIPS endless `NoticeResponse`
//! frames — never sending `AuthenticationOk`/`ReadyForQuery`. Such a frame is
//! valid in ANY handshaking state, so the connecting pump keeps consuming them;
//! because each arrives well inside the per-read `SO_RCVTIMEO` window, the OLD
//! sync driver never tripped that per-read timeout and the connecting thread
//! pinned FOREVER.
//!
//! - BEFORE: `connect` blocks indefinitely against this server (the blind zone —
//!   a few such connects exhaust a blocking pool).
//! - AFTER: `connect` bounds the WHOLE startup/auth handshake by its
//!   `connect_timeout` budget and returns a classified `DriverError::Timeout`
//!   (which `is_disconnect()`), in bounded wall-clock time — the sync analogue of
//!   the async driver's whole-connect `tokio::time::timeout`.
//!
//! No live PG is needed; the whole exchange is scripted and deterministic.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

/// Build a tagged, length-prefixed backend frame: `tag | len | body`.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A minimal, well-formed `NoticeResponse` ('N') frame: a `Severity` field, a
/// `Code` field, a `Message` field, then the field-list terminator.
fn notice_response() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"NOTICE\0");
    body.push(b'C');
    body.extend_from_slice(b"00000\0");
    body.push(b'M');
    body.extend_from_slice(b"drip\0");
    body.push(0); // field-list terminator
    frame(b'N', &body)
}

/// Drain the client's startup message: `[i32 len (incl. itself)][len-4 body]`.
fn drain_startup(stream: &mut TcpStream) {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("read startup length");
    let total = usize::try_from(u32::from_be_bytes(len_buf)).expect("len fits usize");
    let mut body = vec![0u8; total.saturating_sub(4)];
    stream.read_exact(&mut body).expect("read startup body");
}

#[test]
fn a_dripping_handshake_is_bounded_by_connect_timeout_not_a_hang() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("loopback local addr");

    // The hostile loopback server: complete the startup packet, then DRIP
    // `NoticeResponse` frames until the client disconnects (its write fails).
    let server = thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("accept client");
        drain_startup(&mut stream);
        // A batch per write keeps the socket busy (the busy-flood the short-circuit
        // bounds) without one syscall per frame.
        let mut batch = Vec::new();
        for _ in 0..256 {
            batch.extend_from_slice(&notice_response());
        }
        // Loop until the client drops (post-timeout) and the write fails.
        while stream.write_all(&batch).is_ok() {}
    });

    let config = ConnectConfig::new("127.0.0.1", "test")
        .port(addr.port())
        .ssl_mode(SslMode::Disable)
        .connect_timeout(1);

    // Run the connect on a worker thread with a HARD watchdog: without the fix the
    // connect never returns, so a plain call would hang the whole test binary. The
    // watchdog turns that hang into a loud, bounded failure.
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let start = Instant::now();
        let result = Connection::connect(&config);
        let elapsed = start.elapsed();
        // Send before the receiver's recv_timeout fires; a send error means the
        // receiver already gave up (the watchdog failed the test), so ignore it.
        match tx.send((result.is_err(), result.err(), elapsed)) {
            Ok(()) | Err(_) => {}
        }
    });

    // The whole handshake budget is 1 s; a healthy bound completes far inside this.
    // A miss (the hang regression) trips the watchdog.
    let outcome = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("connect did not return within the watchdog window — the handshake HUNG (regression)");
    worker.join().expect("join connect worker");
    let (is_err, err, elapsed) = outcome;

    assert!(is_err, "a dripping handshake must FAIL, not connect");
    let err = err.expect("an errored connect carries a classified DriverError");
    assert!(
        matches!(err, DriverError::Timeout),
        "a dripping handshake past the budget is a classified Timeout, got {err:?}"
    );
    assert!(
        err.is_disconnect(),
        "a connect Timeout is a disconnect (a resilient consumer reconnects)"
    );
    // It waited ~the whole budget (not an instant unrelated failure) and was
    // bounded (not a hang): roughly `connect_timeout`, generously bracketed.
    assert!(
        elapsed >= Duration::from_millis(500),
        "connect returned too fast to be the aggregate-budget bound: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "connect was not bounded near its 1 s budget: {elapsed:?}"
    );

    server.join().expect("join loopback drip server");
}
