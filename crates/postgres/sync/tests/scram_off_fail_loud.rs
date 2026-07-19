// This witness proves the NO-PASSWORD-MECHANISM fail-loud contract, so it exists
// ONLY when BOTH the `scram` and `md5-auth` features are off — with either on the
// driver can satisfy a password challenge (SCRAM, or MD5 / cleartext-over-TLS), so
// a password is NOT fail-loud. Under the default (both on) build it compiles to an
// empty binary (no tests). Run it with:
//   cargo test -p bsql-postgres-sync --no-default-features --features tls,webpki-roots \
//     --test scram_off_fail_loud
#![cfg(not(any(feature = "scram", feature = "md5-auth")))]
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "offline loopback witness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

//! Fail-loud witness for a build with NO password mechanism (`scram` AND
//! `md5-auth` both off).
//!
//! With neither SCRAM nor MD5 compiled in, the client has no mechanism to satisfy
//! a supplied password (cleartext-over-TLS alone is not a mechanism bsql advertises
//! for this ultra-minimal build). A connect that carries a password must therefore
//! fail LOUD — a classified [`DriverError::Config`] naming the missing features —
//! and must NEVER silently attempt a Trust handshake the server would reject, nor
//! panic. This drives the real `std::net` [`Connection::connect`] path against a
//! loopback listener (so the TCP connect succeeds and the driver reaches its
//! credential decision) and asserts a password-bearing config is rejected, while a
//! Trust config (no password) is NOT rejected at the credential step.

use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

/// Bind a loopback listener and hold each accepted connection briefly, so the
/// client's TCP `connect` succeeds and the driver reaches its credential decision
/// (which, with NO password mechanism and a password present, is the fail-loud path).
fn spawn_accept_and_hold() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let port = listener.local_addr().expect("loopback local addr").port();
    thread::spawn(move || {
        for _ in 0..8 {
            if let Ok((stream, _peer)) = listener.accept() {
                thread::sleep(Duration::from_millis(50));
                drop(stream);
            }
        }
    });
    port
}

#[test]
fn password_without_a_mechanism_is_a_loud_config_error() {
    let port = spawn_accept_and_hold();
    // `SslMode::Disable` so the connect skips the SSLRequest probe against the
    // dummy server and reaches the credential decision — the no-mechanism fail-loud.
    let config = ConnectConfig::new("127.0.0.1", "postgres")
        .port(port)
        .ssl_mode(SslMode::Disable)
        .password("hunter2");

    match Connection::connect(&config) {
        Err(DriverError::Config(msg)) => {
            assert!(
                msg.contains("scram"),
                "the fail-loud message must name the missing `scram`/`md5-auth` feature, got: {msg}"
            );
        }
        Err(other) => panic!(
            "a password with no compiled mechanism must be a DriverError::Config, got: {other:?}"
        ),
        Ok(_) => panic!(
            "a password with no compiled mechanism MUST NOT open a connection — there is no \
             client mechanism to satisfy it, and a silent Trust attempt is forbidden"
        ),
    }
}

#[test]
fn trust_without_a_mechanism_is_not_rejected_at_the_credential_step() {
    // No password → Trust credentials, which need no SCRAM. The connect proceeds
    // past credential selection and only fails later on the loopback server's
    // non-Postgres bytes (never a `Config` error blaming a missing feature).
    let port = spawn_accept_and_hold();
    let config = ConnectConfig::new("127.0.0.1", "postgres")
        .port(port)
        .ssl_mode(SslMode::Disable);

    // The loopback server speaks no protocol, so the handshake fails — but NOT
    // with the missing-feature config error: Trust needs no SCRAM. Any other
    // outcome (a transport/handshake error against the dummy server, or —
    // implausibly — success) is fine: the point is only that Trust is not blocked
    // at the credential step.
    if let Err(DriverError::Config(msg)) = Connection::connect(&config) {
        assert!(
            !msg.contains("scram"),
            "a Trust connection must not be rejected for a missing `scram` feature, got: {msg}"
        );
    }
}
