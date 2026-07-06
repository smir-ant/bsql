// This witness proves the `tls`-OFF fail-loud contract, so it exists ONLY when
// the `tls` feature is off. Under the default (tls-on) build it compiles to an
// empty binary (no tests). Run it with:
//   cargo test -p bsql-postgres-sync --no-default-features --test tls_off_fail_loud
#![cfg(not(feature = "tls"))]
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "offline loopback witness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

//! Fail-loud witness for a build WITHOUT the `tls` feature.
//!
//! With TLS compiled out the client cannot negotiate an encrypted transport, so
//! a connect that DEMANDS one must fail LOUD at connect — a classified
//! [`DriverError::Config`] — and must NEVER silently open a plaintext connection
//! the consumer believes is encrypted. This drives the real `std::net`
//! [`Connection::connect`] path against a loopback listener (so the TCP connect
//! succeeds and the driver reaches its post-connect TLS decision) and asserts:
//!
//! 1. `SslMode::Require` → `DriverError::Config` (never a plaintext connect).
//! 2. A custom CA (`with_ca_roots`) → `DriverError::Config`.
//!
//! Both messages name the missing `tls` feature so the failure is actionable.

use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

/// Bind a loopback listener and hold each accepted connection briefly, so the
/// client's TCP `connect` succeeds and the driver reaches its post-connect TLS
/// decision (which, with `tls` off, is the fail-loud path — no bytes exchanged).
fn spawn_accept_and_hold() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
    let port = listener.local_addr().expect("loopback local addr").port();
    thread::spawn(move || {
        // Accept a few connections and hold each so the client is never refused.
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
fn ssl_mode_require_without_tls_is_a_loud_config_error() {
    let port = spawn_accept_and_hold();
    let config = ConnectConfig::new("127.0.0.1", "postgres")
        .port(port)
        .ssl_mode(SslMode::Require);

    match Connection::connect(&config) {
        Err(DriverError::Config(msg)) => {
            assert!(
                msg.contains("tls"),
                "the fail-loud message must name the missing `tls` feature, got: {msg}"
            );
        }
        Err(other) => panic!(
            "SslMode::Require without `tls` must be a DriverError::Config, got: {other:?}"
        ),
        Ok(_) => panic!(
            "SslMode::Require without `tls` MUST NOT open a (plaintext) connection — \
             a silent downgrade is exactly the event this build forbids"
        ),
    }
}

#[test]
fn custom_ca_roots_without_tls_is_a_loud_config_error() {
    let port = spawn_accept_and_hold();
    // Unset SslMode over a loopback host resolves to `Prefer` (the threat-scoped
    // LOCAL default), plus a custom CA: with TLS the CA would be used; without
    // TLS supplying one is contradictory, so it fails loud rather than being
    // silently ignored on a plaintext connection.
    let config = ConnectConfig::new("127.0.0.1", "postgres")
        .port(port)
        .with_ca_roots(b"-----BEGIN CERTIFICATE-----\nnot-a-real-cert\n-----END CERTIFICATE-----\n");

    match Connection::connect(&config) {
        Err(DriverError::Config(msg)) => {
            assert!(
                msg.contains("tls"),
                "the fail-loud message must name the missing `tls` feature, got: {msg}"
            );
        }
        Err(other) => panic!(
            "a custom CA without `tls` must be a DriverError::Config, got: {other:?}"
        ),
        Ok(_) => panic!(
            "a custom CA without `tls` MUST NOT open a (plaintext) connection — \
             the consumer asked for a verified TLS peer"
        ),
    }
}
