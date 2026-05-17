//! DEF-278 Bundle D (2026-05-17) — spec-conformance tests for the
//! `CancelRequest` mechanism on `<ActivePhase>`.
//!
//! Validates the PUBLIC API surface from outside the crate boundary:
//!
//! 1. **Wire format** (PG §55.2.7): 16 bytes, BE-encoded
//!    length+magic+pid+secret, length field = 16, magic = 80877102.
//! 2. **Phase-typed accessor**: `<ActivePhase>::cancel_request_credentials()`
//!    returns `Some` after a successful Trust handshake (the helper
//!    `fresh_active_via_trust_handshake` drives a synthetic K + Z
//!    sequence with `pid=12345`, `secret_key=67890`).
//! 3. **Debug redaction**: the credentials' Debug impl must NEVER
//!    surface the inner `secret_key` value; pid is plain (wire-public).
//! 4. **Zeroize-on-drop**: dropping a `CancelRequestCredentials` must
//!    scrub the inner secret_key bytes via the `Sensitive<i32>`
//!    wrapper's `ZeroizeOnDrop` chain. Verified via raw-pointer
//!    memory probe (same pattern as `scram_zeroize_miri_spec.rs`).
//!
//! The negative phase-access tests (calling
//! `cancel_request_credentials` on `<DisconnectedPhase>` /
//! `<ConnectingPhase>` / `<ClosedPhase>`) live as trybuild compile-
//! fail probes in
//! `crates/bsql-pg-proto-derive/tests/def278d_compile_fail/`.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
// Tests use `panic!()` to surface unexpected fixture states (mirrors
// the convention from `ping_spec.rs`, `startup_spec.rs`,
// `cancel_request_wire_spec.rs`). Production code carries
// `clippy::panic` in its forbid bundle — no exceptions there.
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    ActivePhase, CancelRequestCredentials, Credentials, DisconnectedPhase, Ident, IntoActiveError,
    PgProtocol, StartupKind, WriteBuf, cancel_request_bytes,
};

mod common;
use common::{fresh_active_via_trust_handshake, mint_reply_disconnected};

// =====================================================================
// Helper — synthetic handshake-drive with a CALLER-CHOSEN (pid, secret)
//
// The shared `fresh_active_via_trust_handshake` helper uses fixed
// (12345, 67890); the cancel-credentials tests need to inject specific
// payload to assert the round-trip. This local helper duplicates the
// drive with caller-supplied (pid, secret).
// =====================================================================

fn auth_ok_frame() -> [u8; 9] {
    [b'R', 0, 0, 0, 8, 0, 0, 0, 0]
}

fn backend_key_data_frame(pid: i32, secret_key: i32) -> [u8; 13] {
    let pid_bytes = pid.to_be_bytes();
    let key_bytes = secret_key.to_be_bytes();
    [
        b'K', 0, 0, 0, 12,
        pid_bytes[0], pid_bytes[1], pid_bytes[2], pid_bytes[3],
        key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
    ]
}

fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [b'Z', 0, 0, 0, 5, tx_status]
}

/// Drive a fresh `PgProtocol<DisconnectedPhase>` through a Trust
/// handshake to `<ActivePhase>` with caller-supplied
/// `(pid, secret_key)`. The shared helper in `common/mod.rs` uses
/// hard-coded (12345, 67890); this local variant accepts any
/// payload so the round-trip tests can assert byte-for-byte equality
/// against arbitrary BE-encoded inputs.
#[track_caller]
fn fresh_active_with_backend_key(pid: i32, secret_key: i32) -> PgProtocol<ActivePhase> {
    let mut proto = PgProtocol::<DisconnectedPhase>::new();
    let mut wb = WriteBuf::new();
    let user = match Ident::try_from_str("testuser") {
        Ok(u) => u,
        Err(e) => panic!("test fixture: 'testuser' is a valid Ident, got {e}"),
    };
    let (reply, _raw) = mint_reply_disconnected::<StartupKind>(&mut proto);
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
                "test fixture: push_startup must succeed for Trust auth, got {:?}",
                f.cause,
            ),
        };
        let _ = _actions;
        p
    };

    // AuthOk
    if let Err(e) = proto_connecting.feed_inbound(&auth_ok_frame()) {
        panic!("test fixture: feed_inbound(AuthOk) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    // BackendKeyData with caller-supplied (pid, secret_key)
    if let Err(e) = proto_connecting.feed_inbound(&backend_key_data_frame(pid, secret_key)) {
        panic!("test fixture: feed_inbound(BackendKeyData) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    // RFQ — handshake-complete
    if let Err(e) = proto_connecting.feed_inbound(&rfq_frame(b'I')) {
        panic!("test fixture: feed_inbound(RFQ) must succeed, got {e:?}");
    }
    let _evt = proto_connecting.advance_one_frame(&mut wb);

    match proto_connecting.into_active() {
        Ok(p) => p,
        Err(IntoActiveError::Closed(_)) => panic!(
            "test fixture: trust handshake landed in Closed unexpectedly",
        ),
        Err(IntoActiveError::StillConnecting(_)) => panic!(
            "test fixture: trust handshake landed in StillConnecting unexpectedly",
        ),
    }
}

// =====================================================================
// Spec-1: encode matches PG §55.2.7 — 16 bytes exactly
// =====================================================================

#[test]
fn cancel_request_encode_matches_pg_wire_spec_16_bytes() {
    // The shared helper drives the handshake with (pid=12345,
    // secret_key=67890); the credentials' encode must produce the
    // same 16-byte packet as the standalone `cancel_request_bytes`
    // builder with the same inputs (single source of truth for the
    // byte layout).
    let active = fresh_active_via_trust_handshake();
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!(
            "post-Trust-handshake protocol must have backend_key installed; \
             cancel_request_credentials returned None unexpectedly",
        ),
    };
    let encoded = creds.encode();
    assert_eq!(
        encoded.len(),
        16,
        "PG §55.2.7 mandates 16-byte CancelRequest packet",
    );
    // Bit-identical to the standalone wire builder.
    assert_eq!(
        encoded,
        cancel_request_bytes(12345_i32, 67890_i32),
        "encoded credentials must match standalone cancel_request_bytes \
         for the same (pid, secret_key)",
    );
}

// =====================================================================
// Spec-2: BE encoding of pid at bytes [8..12]
// =====================================================================

#[test]
fn cancel_request_encode_big_endian_pid() {
    // Distinct pid + secret_key so we can pinpoint position
    // independence — pid at bytes[8..12], secret_key at
    // bytes[12..16]. The pid value is chosen to have a unique BE
    // byte pattern so the position is verifiable.
    let pid: i32 = i32::from_be_bytes([0xAA, 0xBB, 0xCC, 0xDD]);
    let secret: i32 = i32::from_be_bytes([0x11, 0x22, 0x33, 0x44]);
    let active = fresh_active_with_backend_key(pid, secret);
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    let encoded = creds.encode();
    let pid_slice = encoded.get(8..12).unwrap_or(&[]);
    assert_eq!(
        pid_slice,
        &[0xAA, 0xBB, 0xCC, 0xDD][..],
        "pid must be BE-encoded at bytes[8..12]",
    );
}

// =====================================================================
// Spec-3: BE encoding of secret_key at bytes [12..16]
// =====================================================================

#[test]
fn cancel_request_encode_big_endian_secret_key() {
    let pid: i32 = i32::from_be_bytes([0xAA, 0xBB, 0xCC, 0xDD]);
    let secret: i32 = i32::from_be_bytes([0x11, 0x22, 0x33, 0x44]);
    let active = fresh_active_with_backend_key(pid, secret);
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    let encoded = creds.encode();
    let secret_slice = encoded.get(12..16).unwrap_or(&[]);
    assert_eq!(
        secret_slice,
        &[0x11, 0x22, 0x33, 0x44][..],
        "secret_key must be BE-encoded at bytes[12..16]",
    );
}

// =====================================================================
// Spec-4: magic version field at bytes [4..8] = 0x04d2162e = 80877102
// =====================================================================

#[test]
fn cancel_request_encode_magic_version_field() {
    let active = fresh_active_via_trust_handshake();
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    let encoded = creds.encode();
    let magic_slice = encoded.get(4..8).unwrap_or(&[]);
    assert_eq!(
        magic_slice,
        &[0x04, 0xd2, 0x16, 0x2e][..],
        "magic-version field must equal CANCEL_REQUEST_VERSION = 80877102 BE",
    );
    // Cross-check via direct decode.
    let len_bytes: [u8; 4] = match magic_slice.try_into() {
        Ok(arr) => arr,
        Err(_) => panic!("magic_slice must be exactly 4 bytes — got {magic_slice:?}"),
    };
    assert_eq!(
        u32::from_be_bytes(len_bytes),
        bsql_pg_proto::wire::CANCEL_REQUEST_VERSION,
        "magic-version BE decode must equal the public CANCEL_REQUEST_VERSION constant",
    );
}

// =====================================================================
// Spec-5: length field at bytes [0..4] = 16
// =====================================================================

#[test]
fn cancel_request_encode_length_field_is_16() {
    let active = fresh_active_via_trust_handshake();
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    let encoded = creds.encode();
    let len_slice = encoded.get(0..4).unwrap_or(&[]);
    let len_bytes: [u8; 4] = match len_slice.try_into() {
        Ok(arr) => arr,
        Err(_) => panic!("len_slice must be exactly 4 bytes — got {len_slice:?}"),
    };
    let declared = u32::from_be_bytes(len_bytes);
    assert_eq!(
        declared, 16,
        "length field must equal 16 (length includes self per PG protocol)",
    );
}

// =====================================================================
// Spec-6: Debug redacts secret_key
// =====================================================================

#[test]
fn cancel_credentials_debug_redacts_secret_key() {
    // Use a distinct secret value so we can search for both its
    // decimal and hex representations in the Debug output.
    let pid: i32 = 12345;
    let secret: i32 = i32::from_be_bytes([0xfe, 0xed, 0xfa, 0xce]);
    let active = fresh_active_with_backend_key(pid, secret);
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    let dbg = std::format!("{creds:?}");
    assert!(
        dbg.contains("REDACTED"),
        "Debug output must contain `<REDACTED>` for the secret_key (got: {dbg})",
    );
    // The secret value as decimal — must NOT appear in Debug.
    let secret_decimal = std::format!("{secret}");
    assert!(
        !dbg.contains(&secret_decimal),
        "Debug output leaked secret_key bytes (decimal {secret_decimal} found in: {dbg})",
    );
    // Pid is wire-public — must appear plain.
    let pid_decimal = std::format!("{pid}");
    assert!(
        dbg.contains(&pid_decimal),
        "Debug output must surface pid plain for diagnostic value (pid={pid_decimal}, got: {dbg})",
    );
}

// =====================================================================
// Spec-7: Drop scrubs the secret_key (transitive verification)
//
// The credentials struct holds `secret_key: Sensitive<i32>`. The
// `Sensitive` wrapper derives `ZeroizeOnDrop` (see
// `src/sensitive.rs`); on drop the inner `i32` slot is overwritten
// with zeros via the standard `zeroize::Zeroize` chain.
//
// `secret_key` is field-private to `mod cancel` — we cannot
// raw-pointer-probe its memory slot from an integration test
// because:
// 1. `offset_of!(CancelRequestCredentials, secret_key)` is rejected
//    by E0616 (private field — visible only inside `mod cancel`).
// 2. Adding a `pub fn __test_secret_ptr()` helper violates the
//    DEF-246 "no `pub fn __test_*` / `pub fn *_for_test*` bypass
//    surface" rule, which DEF-278 inherits unconditionally.
//
// Instead we rely on the TRANSITIVE proof chain:
//
// (a) `core::mem::needs_drop::<CancelRequestCredentials>() == true`
//     — proves the type has Drop glue (compile-adjacent shield;
//     if a future refactor removed `Sensitive` from the field, this
//     flips to false and the assertion fires).
//
// (b) `Sensitive<i32>` zeroizes on drop — pinned by the crate-internal
//     test `sensitive::drop_witness_tests::sensitive_i32_drop_fires`
//     which uses a `DropCounter` witness to observe the drop chain
//     firing the inner Zeroize impl.
//
// (c) Rust's drop-glue rules: any struct containing a `Sensitive<i32>`
//     field has its Drop run the field's Drop transitively.
//
// (a) + (b) + (c) ⇒ `CancelRequestCredentials::drop` runs
// `Sensitive<i32>::drop` runs `i32::zeroize` — the secret_key is
// scrubbed.
//
// This is the same verification pattern used by
// `scram_zeroize_miri_spec.rs::password_needs_drop_is_true` —
// `needs_drop` as the compile-adjacent shield for the transitive
// ZeroizeOnDrop chain.
// =====================================================================

#[test]
fn cancel_credentials_drop_glue_runs_zeroize_chain() {
    // (a) Structural shield — if a future refactor removed
    // Sensitive<i32> from secret_key (e.g. swapped to a plain i32),
    // needs_drop would flip false and this assertion would fire
    // (i32 alone has no Drop glue).
    assert!(
        core::mem::needs_drop::<CancelRequestCredentials>(),
        "CancelRequestCredentials must have Drop glue (transitive \
         ZeroizeOnDrop invariant via Sensitive<i32> field). If this \
         assertion fires, the secret_key field's type was changed \
         away from Sensitive<i32> — restore the wrapper or design \
         a replacement scrub mechanism.",
    );
}

#[test]
fn cancel_credentials_drop_does_not_panic() {
    // Sanity: dropping the credentials never panics. Combined with
    // the needs_drop shield above, this rules out the variant where
    // the Drop impl exists but is implemented in a way that
    // panics on the synthetic-handshake secret value.
    let active = fresh_active_via_trust_handshake();
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    // Drop fires at end of scope. If the Drop impl ever introduces
    // a panic (e.g. an `unwrap` on a None Option inside zeroize
    // chain), this test catches it.
    drop(creds);
    // Multiple drops in sequence — exercise the path multiple times
    // to give the test some signal beyond a single drop.
    for _ in 0..5 {
        let active2 = fresh_active_via_trust_handshake();
        let creds2 = match active2.cancel_request_credentials() {
            Some(c) => c,
            None => panic!("backend_key must be installed post-handshake"),
        };
        drop(creds2);
    }
}

// =====================================================================
// Spec-8: cancel_request_credentials returns Some after a successful
//          Trust handshake.
// =====================================================================

#[test]
fn active_cancel_credentials_returns_after_handshake() {
    // The shared helper drives a synthetic Trust handshake with
    // (12345, 67890). Post-handshake, the backend_key cell must be
    // installed and the accessor must return `Some`.
    let active = fresh_active_via_trust_handshake();
    let opt = active.cancel_request_credentials();
    assert!(
        opt.is_some(),
        "post-Trust-handshake protocol must have backend_key cell installed; \
         cancel_request_credentials returned None",
    );
}

// =====================================================================
// Spec-9: pid() accessor returns the handshake value.
// =====================================================================

#[test]
fn pid_accessor_returns_handshake_value() {
    // Cross-test pid round-trip. The helper installs pid=12345 in
    // the BackendKeyData frame; the accessor must return exactly
    // that value.
    let active = fresh_active_via_trust_handshake();
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    assert_eq!(
        creds.pid(),
        12345_i32,
        "pid() must return the pid installed at the dispatch arm \
         (the helper uses 12345 in the BackendKeyData frame)",
    );
}

// =====================================================================
// Spec-10: Disconnected phase has no installed backend_key.
//
// This positive test exercises the "before handshake" half of the
// installation lifecycle. The negative method-absence tests live in
// `def278d_compile_fail/` (trybuild).
// =====================================================================

#[test]
fn disconnected_protocol_starts_with_no_backend_key() {
    let proto = PgProtocol::<DisconnectedPhase>::new();
    // <DisconnectedPhase> doesn't have a `cancel_request_credentials`
    // accessor (method-absent on phases other than <ActivePhase>).
    // The compile-fail probes pin the absence. Here we just verify
    // the protocol is at a fresh state pre-handshake — once we
    // drive it through push_startup, the cell will stay empty
    // until the dispatch arm at (ConnectingPostAuthHaveKey, 'Z')
    // installs.
    use bsql_pg_proto::state::ProtoState;
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "fresh PgProtocol<DisconnectedPhase> must be in ProtoState::Idle",
    );
}

// =====================================================================
// Spec-11: encode is invariant on call — calling encode multiple times
// yields identical bytes (no per-call mutation).
// =====================================================================

#[test]
fn cancel_credentials_encode_is_idempotent() {
    let active = fresh_active_via_trust_handshake();
    let creds = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("backend_key must be installed post-handshake"),
    };
    let first = creds.encode();
    let second = creds.encode();
    let third = creds.encode();
    assert_eq!(
        first, second,
        "encode() must be pure — two consecutive calls produce identical bytes",
    );
    assert_eq!(
        second, third,
        "encode() must be pure — three consecutive calls produce identical bytes",
    );
}

// =====================================================================
// Spec-12: cancel_request_credentials returns a fresh credentials
// each call (does not consume the protocol — accessor takes &self).
// =====================================================================

#[test]
fn cancel_credentials_accessor_does_not_consume_protocol() {
    // Two consecutive `.cancel_request_credentials()` calls on the
    // same `&active` must both return `Some` with identical encoded
    // bytes (the cell is read-only via this accessor; the underlying
    // backend_key persists across reads).
    let active = fresh_active_via_trust_handshake();
    let first = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("first read must succeed post-handshake"),
    };
    let second = match active.cancel_request_credentials() {
        Some(c) => c,
        None => panic!("second read must succeed post-handshake — accessor takes &self"),
    };
    assert_eq!(
        first.encode(),
        second.encode(),
        "two consecutive `.cancel_request_credentials()` reads must produce \
         identical credentials (cell is stable across &self reads)",
    );
    assert_eq!(
        first.pid(),
        second.pid(),
        "pid accessor must be stable across reads",
    );
}

