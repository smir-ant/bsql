//! Bench-side helper for driving a fresh `<DisconnectedPhase>`
//! through a synthetic Trust handshake to `<ActivePhase>`. Mirror
//! of `tests/common/mod.rs`'s `fresh_active_via_trust_handshake` —
//! body duplicated by design (~140 LoC) so the bench crate has zero
//! coupling to integration-test code beyond the public API. The
//! dispatch is explicit: no `src/test_support/` cfg(test) bridge.
//!
//! # Why duplicated, not shared
//!
//! Cargo's bench harness compiles `benches/*.rs` as separate crates;
//! integration tests in `tests/` are also separate crates. Both link
//! against the published `bsql-pg-proto` artifact. Sharing the helper
//! via a `pub` re-export would add a public-API surface ("helper for
//! constructing Active via fake-handshake"); the dispatch wants that
//! kept INSIDE the test/bench harnesses, not on the public API.
//! Duplication is the chosen tradeoff: ~140 LoC of pure-public-API
//! code that lives in two places.

#![allow(
    dead_code,
    reason = "shared bench helper module — not every bench file uses every helper"
)]

use bsql_postgres_proto::{
    ActivePhase, Credentials, DisconnectedPhase, Ident, IntoActiveError, PgProtocol, StartupKind,
    WriteBuf,
};

/// Build an AuthenticationOk frame: tag 'R', length 8, sub-code 0.
fn auth_ok_frame() -> [u8; 9] {
    [b'R', 0, 0, 0, 8, 0, 0, 0, 0]
}

/// Build a ParameterStatus frame: tag 'S', key\0value\0.
fn param_status_frame(key: &str, value: &str) -> std::vec::Vec<u8> {
    let body_len = key.len().saturating_add(1).saturating_add(value.len()).saturating_add(1);
    let declared = u32::try_from(body_len).unwrap_or(0).saturating_add(4);
    let mut frame: std::vec::Vec<u8> = std::vec::Vec::new();
    frame.push(b'S');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame.push(0);
    frame.extend_from_slice(value.as_bytes());
    frame.push(0);
    frame
}

/// Build a BackendKeyData frame: tag 'K', 8-byte payload (pid + secret_key).
fn backend_key_data_frame(pid: i32, secret_key: i32) -> [u8; 13] {
    let pid_bytes = pid.to_be_bytes();
    let key_bytes = secret_key.to_be_bytes();
    [
        b'K', 0, 0, 0, 12,
        pid_bytes[0], pid_bytes[1], pid_bytes[2], pid_bytes[3],
        key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
    ]
}

/// Build a ReadyForQuery frame.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [b'Z', 0, 0, 0, 5, tx_status]
}

/// Drive a fresh `PgProtocol` through a synthetic Trust-auth
/// handshake to `<ActivePhase>`. Uses ONLY the public API (mirror
/// of `tests/common/mod.rs::fresh_active_via_trust_handshake`).
///
/// **Panics** on any unexpected handshake failure — the caller is in a
/// happy-path bench fixture.
#[track_caller]
pub fn fresh_active_via_trust_handshake() -> PgProtocol<ActivePhase> {
    let mut proto = PgProtocol::<DisconnectedPhase>::new();
    let mut wb = WriteBuf::new();
    let user = match Ident::try_from_str("benchuser") {
        Ok(u) => u,
        Err(e) => panic!("bench fixture: 'benchuser' is a valid Ident, got {e}"),
    };
    let reply = proto.next_reply_id::<StartupKind>();
    let mut proto_connecting = {
        let (_actions, p) = match proto.push_startup(
            user,
            None,
            None,
            Credentials::Trust,
            reply,
            &mut wb,
        ) {
            Ok((a, p)) => (a, p),
            Err(f) => panic!(
                "bench fixture: push_startup must succeed for Trust auth, got {:?}",
                f.cause,
            ),
        };
        // `_actions` borrows into `wb` — drop it (and the inner block
        // scope) before re-using `wb` for subsequent feed_inbound /
        // advance_one_frame calls.
        let _ = _actions;
        p
    };

    // Drive AuthOk → ParameterStatus×N → BackendKeyData → RFQ.
    if let Err(e) = proto_connecting.feed_inbound(&auth_ok_frame()) {
        panic!("bench fixture: feed_inbound(AuthOk) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&param_status_frame("server_version", "17.2")) {
        panic!("bench fixture: feed_inbound(PS server_version) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&param_status_frame("client_encoding", "UTF8")) {
        panic!("bench fixture: feed_inbound(PS client_encoding) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&backend_key_data_frame(12345, 67890)) {
        panic!("bench fixture: feed_inbound(BackendKeyData) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    if let Err(e) = proto_connecting.feed_inbound(&rfq_frame(b'I')) {
        panic!("bench fixture: feed_inbound(RFQ) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    match proto_connecting.into_active() {
        Ok(p) => p,
        Err(IntoActiveError::Closed(_)) => panic!(
            "bench fixture: trust handshake landed in Closed unexpectedly",
        ),
        Err(IntoActiveError::StillConnecting(_)) => panic!(
            "bench fixture: trust handshake landed in StillConnecting unexpectedly",
        ),
    }
}
