#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "offline loopback witness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

//! Deterministic −1 RTT witness: connecting captures `server_version` from the
//! handshake and issues NO `SHOW server_version` round-trip.
//!
//! A real `std::net` [`Connection`] connects to an in-test loopback server that
//! serves the trust handshake — including a `server_version` `ParameterStatus`
//! report — and then records every byte the client sends afterwards. No live PG
//! is needed; the whole exchange is scripted and deterministic.
//!
//! # Before → after
//!
//! - BEFORE: `connect` recovered the server version with a post-handshake
//!   `Query` ('Q') carrying `SHOW server_version`, then read a
//!   `RowDescription`/`DataRow`/`ReadyForQuery` reply — a full network
//!   round-trip on every connection. Against this loopback server (which never
//!   sends that reply) the old `connect` would additionally block forever.
//! - AFTER: `connect` reads the version captured from the handshake's
//!   `ParameterStatus`. The only bytes the client sends after the startup packet
//!   are the graceful `Terminate` ('X') from [`Connection::close`] — no `Query`,
//!   no `SHOW`, no `server_version` reference. The round-trip is gone by
//!   construction.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

/// The version string the loopback server reports — a realistic packaged form,
/// longer than 24 bytes, to prove the capture is full-fidelity (no truncation).
const SERVER_VERSION: &str = "17.4 (Debian 17.4-1.pgdg120+1)";

/// Build a tagged, length-prefixed backend frame: `tag | len | body`.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A `ParameterStatus` ('S') frame: `key\0value\0`.
fn parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = key.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

/// Drain the client's startup message: `[i32 len (incl. itself)][len-4 body]`.
fn drain_startup(stream: &mut TcpStream) {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("read startup length");
    let total = usize::try_from(u32::from_be_bytes(len_buf)).expect("len fits usize");
    let mut body = vec![0u8; total.saturating_sub(4)];
    stream.read_exact(&mut body).expect("read startup body");
}

/// `true` if `haystack` contains `needle` as a contiguous subslice.
fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    needle.len() <= haystack.len() && haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn connect_captures_server_version_without_a_show_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let addr = listener.local_addr().expect("loopback local addr");

    // The loopback PG server: serve the trust handshake with a `server_version`
    // report, then record every post-handshake client byte until EOF.
    let server = thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("accept client");
        drain_startup(&mut stream);

        let mut reply = frame(b'R', &0_i32.to_be_bytes()); // AuthenticationOk
        reply.extend_from_slice(&parameter_status("server_version", SERVER_VERSION));
        reply.extend_from_slice(&parameter_status("client_encoding", "UTF8"));
        let mut key = 4321_i32.to_be_bytes().to_vec();
        key.extend_from_slice(&8765_i32.to_be_bytes());
        reply.extend_from_slice(&frame(b'K', &key)); // BackendKeyData
        reply.extend_from_slice(&frame(b'Z', b"I")); // ReadyForQuery(Idle)
        stream.write_all(&reply).expect("write handshake");
        stream.flush().expect("flush handshake");

        // Everything the client sends after the handshake. On the new code this
        // is only the graceful `Terminate` from `close()`.
        let mut post_handshake = Vec::new();
        stream
            .read_to_end(&mut post_handshake)
            .expect("read post-handshake client bytes");
        post_handshake
    });

    let config = ConnectConfig::new("127.0.0.1", "test")
        .port(addr.port())
        .ssl_mode(SslMode::Disable);
    let mut conn = Connection::connect(&config).expect("connect over loopback");

    // Capture works: the value is exactly what a `SHOW server_version` returns.
    assert_eq!(conn.server_version(), Some(SERVER_VERSION));

    // The only post-handshake client frame: a graceful Terminate.
    conn.close().expect("close");

    let post_handshake = server.join().expect("join loopback server");

    // The −1 RTT witness: no `SHOW server_version` was ever issued.
    assert_ne!(
        post_handshake.first(),
        Some(&b'Q'),
        "client opened with a Query frame post-handshake (a resurrected SHOW): {post_handshake:?}"
    );
    assert!(
        !contains_subslice(&post_handshake, b"SHOW"),
        "client sent a SHOW post-handshake: {post_handshake:?}"
    );
    assert!(
        !contains_subslice(&post_handshake, b"server_version"),
        "client referenced server_version post-handshake (a resurrected SHOW): {post_handshake:?}"
    );
}
