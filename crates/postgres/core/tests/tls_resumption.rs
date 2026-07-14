// The whole gate targets the rustls-backed `TlsTransport`, which exists only
// under the `tls` feature; with TLS off this test binary is empty (no tests).
#![cfg(feature = "tls")]
//! TLS SESSION-RESUMPTION gate for [`bsql_postgres_core::tls::TlsTransport`] —
//! the hermetic (no-network, no-PostgreSQL) proof of the two properties that let
//! bsql share one `Arc<ClientConfig>` across reconnects for a faster handshake
//! WITHOUT weakening SCRAM channel binding:
//!
//! 1. **Sharing the config resumes.** Two `TlsTransport::connect`s over the SAME
//!    `Arc<ClientConfig>` (a shared resumption store) against a ticket-issuing
//!    server: the FIRST is a full handshake, the SECOND RESUMES (an abbreviated
//!    handshake). The witness reads the SERVER's own `handshake_kind()` (rustls's
//!    public API), so no bsql accessor is added to observe it.
//! 2. **Resumption preserves the peer certificate (the channel-binding crux).**
//!    On the RESUMED connection, `TlsTransport::peer_end_entity_cert()` returns
//!    the SAME certificate as the full handshake did — so the
//!    `tls-server-end-point` SCRAM binding hash is identical, and a
//!    SCRAM-SHA-256-PLUS proof over a resumed session still binds to the right
//!    certificate (RFC 5929 §4). rustls restores the original full-handshake
//!    cert on resumption; this test pins that bsql surfaces it.
//!
//! And the NEGATIVE control: two connects with SEPARATE (unshared) configs — the
//! per-connection-fresh config that DEFEATED resumption before this change — do
//! NOT resume, proving that config SHARING is exactly what enables it.
//!
//! Real rustls key exchange + AEAD run on both sides against the in-memory
//! loopback server; only the client's cert-chain *check* is stubbed (irrelevant
//! to resumption mechanics). Every future resolves in a single poll.

// Helper fns and impl methods in the shared harness are not in `#[test]`
// context, so the in-tests carve-out does not reach them; these scoped allows
// (keystone-required reason) cover the harness's loud-failure expects/panics.
#![allow(
    clippy::expect_used,
    reason = "test harness — expect() is the loud failure signal; the in-tests carve-out reaches #[test] fns but not free helper fns / impl methods"
)]
#![allow(
    clippy::panic,
    reason = "test harness — the loopback panics on an impossible state as the loud failure signal; the in-tests carve-out does not reach free helper fns"
)]

mod tls_common;

use std::sync::Arc;

use tls_common::{
    block_on, resumable_server_config, test_client_config, test_server_name, MockInner, CERT_DER,
};

use bsql_postgres_core::tls::TlsTransport;
use bsql_postgres_proto::engine::Transport;

/// A shared client config gives its inner resumption store to every connection
/// built from it, so the SECOND connection to a ticket-issuing server RESUMES —
/// and the resumed session still exposes the ORIGINAL peer certificate, so
/// channel binding stays anchored to the right cert.
#[test]
fn shared_config_resumes_and_preserves_the_peer_certificate() {
    // ONE client config (⇒ ONE resumption store) shared by both connects, and
    // ONE ticket-issuing server config (⇒ ONE ticketer key) shared by both
    // server instances — the two ingredients resumption needs.
    let client_cfg = test_client_config();
    let server_cfg = resumable_server_config();

    // --- Connection #1: a FULL handshake. -----------------------------------
    let (inner1, state1) = MockInner::new_with_server_config(Arc::clone(&server_cfg));
    let mut transport1 = block_on(TlsTransport::connect(
        inner1,
        Arc::clone(&client_cfg),
        test_server_name(),
    ))
    .expect("first TLS handshake completes");

    // Settle the server so it consumes the client Finished and EMITS its
    // NewSessionTicket, then send one app record. Reading it on the client both
    // proves data flows AND drives rustls to STORE the ticket in the shared
    // client config (a post-handshake message processed during the read).
    {
        let mut g = state1.lock().expect("lock");
        g.pump_server();
        g.server_send_app(b"ping");
    }
    let mut buf = [0u8; 8];
    let n = block_on(transport1.read(&mut buf)).expect("read app data over TLS #1");
    assert_eq!(&buf[..n], b"ping", "the first session delivers app data");

    assert!(
        state1.lock().expect("lock").server_did_full(),
        "the first handshake (empty store) must be FULL",
    );
    let cert1 = transport1
        .peer_end_entity_cert()
        .expect("a full handshake presents a peer certificate")
        .to_vec();
    assert_eq!(cert1, CERT_DER, "the presented leaf is the server's cert");

    // --- Connection #2: sharing the config ⇒ RESUMES. ------------------------
    let (inner2, state2) = MockInner::new_with_server_config(Arc::clone(&server_cfg));
    let transport2 = block_on(TlsTransport::connect(
        inner2,
        Arc::clone(&client_cfg), // the SAME store, now holding the ticket
        test_server_name(),
    ))
    .expect("second TLS handshake completes");

    // THE RESUMPTION WITNESS: the server negotiated an abbreviated handshake.
    assert!(
        state2.lock().expect("lock").server_did_resume(),
        "the second handshake sharing the config MUST resume (abbreviated), \
         not run a full handshake",
    );

    // THE CHANNEL-BINDING CRUX: on the RESUMED session the server sends NO
    // Certificate message, yet rustls restores the ORIGINAL full-handshake cert,
    // so bsql's `peer_end_entity_cert` — the input to the `tls-server-end-point`
    // binding — is the SAME certificate. A SCRAM-SHA-256-PLUS proof over a
    // resumed session therefore binds to the correct cert (RFC 5929 §4).
    let cert2 = transport2
        .peer_end_entity_cert()
        .expect("a RESUMED session must still expose the original peer certificate")
        .to_vec();
    assert_eq!(
        cert2, cert1,
        "the resumed session must expose the ORIGINAL peer certificate, so the \
         channel-binding hash is unchanged — never None and never a different cert",
    );

    // Make the binding-hash equality concrete: the exact `tls-server-end-point`
    // value the SCRAM `-PLUS` proof carries is byte-identical on both sessions.
    #[cfg(feature = "scram")]
    {
        use bsql_postgres_proto::scram::channel_binding::tls_server_end_point;
        assert_eq!(
            tls_server_end_point(cert1.as_slice()).as_slice(),
            tls_server_end_point(cert2.as_slice()).as_slice(),
            "the tls-server-end-point channel-binding hash is identical on the resumed session",
        );
    }
}

/// The NEGATIVE control: two connects with SEPARATE client configs (each its own
/// empty resumption store) do NOT resume — the second is a FULL handshake. This
/// is exactly the per-connection-fresh-config behaviour that DEFEATED resumption
/// before this change, proving that SHARING the config is what enables it.
#[test]
fn separate_configs_do_not_resume() {
    let server_cfg = resumable_server_config();

    // Connect #1 with its OWN client config; warm it to receive a ticket.
    let (inner1, state1) = MockInner::new_with_server_config(Arc::clone(&server_cfg));
    let mut transport1 = block_on(TlsTransport::connect(
        inner1,
        test_client_config(), // a FRESH store, discarded with this connection
        test_server_name(),
    ))
    .expect("first TLS handshake completes");
    {
        let mut g = state1.lock().expect("lock");
        g.pump_server();
        g.server_send_app(b"ping");
    }
    let mut buf = [0u8; 8];
    let _ = block_on(transport1.read(&mut buf)).expect("read app data over TLS #1");
    assert!(state1.lock().expect("lock").server_did_full());

    // Connect #2 with a DIFFERENT, fresh client config (empty store) → no ticket
    // to offer → the server runs a FULL handshake, NOT a resumption.
    let (inner2, state2) = MockInner::new_with_server_config(Arc::clone(&server_cfg));
    let _transport2 = block_on(TlsTransport::connect(
        inner2,
        test_client_config(), // a DIFFERENT store — the pre-change behaviour
        test_server_name(),
    ))
    .expect("second TLS handshake completes");
    assert!(
        !state2.lock().expect("lock").server_did_resume(),
        "an UNSHARED client config must NOT resume — its store is empty",
    );
    assert!(
        state2.lock().expect("lock").server_did_full(),
        "an unshared config's second connect is a FULL handshake",
    );
}
