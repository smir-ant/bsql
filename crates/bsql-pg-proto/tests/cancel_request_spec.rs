//! DEF-278 Bundle D' (2026-05-18) — spec-conformance tests for the
//! closure-scoped `CancelRequest` mechanism on `<ActivePhase>`.
//!
//! Validates the PUBLIC API surface from outside the crate boundary:
//!
//! 1. **Wire format** (PG §55.2.7): 16 bytes, BE-encoded
//!    length+magic+pid+secret, length field = 16, magic = 80877102.
//!    Verified by copying the bytes out of the closure scope into a
//!    caller-owned `[u8; 16]` and asserting against the standalone
//!    `cancel_request_bytes` builder.
//! 2. **Phase-typed accessor**: `<ActivePhase>::with_cancel_request`
//!    invokes its closure when the cell is installed (post-Trust
//!    handshake) and returns `Some(R)`; returns `None` when the cell
//!    is absent (architecturally-distant case — non-standard PG fork
//!    skipping the `K` frame).
//! 3. **Closure-scope retention impossibility**: the bytes borrow
//!    cannot escape the closure (HRTB on `FnOnce(&[u8; 16], i32) -> R`
//!    quantifies the lifetime). Negative test is a trybuild compile-
//!    fail probe (`p_d278d_6_lifetime_escape`).
//! 4. **Zeroize-on-drop**: the in-flight `Zeroizing<[u8; 16]>` guard
//!    that owns the bytes carries `ZeroizeOnDrop` glue — verified via
//!    `core::mem::needs_drop` compile-adjacent shield. On normal
//!    return + on unwind panic, the guard's Drop fires
//!    `Zeroize::zeroize` on the 16-byte array.
//!
//! The negative phase-access tests (calling `with_cancel_request` on
//! `<DisconnectedPhase>` / `<ConnectingPhase>` / `<ClosedPhase>`) live
//! as trybuild compile-fail probes in
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
    ActivePhase, Credentials, DisconnectedPhase, Ident, IntoActiveError, PgProtocol, StartupKind,
    WriteBuf, cancel_request_bytes,
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
// Spec-1: with_cancel_request lends 16 bytes matching PG §55.2.7
// =====================================================================

#[test]
fn with_cancel_request_lends_16_bytes_matching_pg_wire_spec() {
    // The shared helper drives the handshake with (pid=12345,
    // secret_key=67890); the closure-lent bytes must equal the
    // standalone `cancel_request_bytes` builder output for the same
    // inputs (single source of truth for the byte layout).
    let active = fresh_active_via_trust_handshake();
    let owned: [u8; 16] = active.with_cancel_request(|bytes, _pid| {
        assert_eq!(
            bytes.len(),
            16,
            "PG §55.2.7 mandates 16-byte CancelRequest packet",
        );
        // Copy bytes contents into caller-owned [u8; 16]. The borrow
        // does not escape — this is a memcpy, not a reference leak.
        *bytes
    });
    // Bit-identical to the standalone wire builder.
    assert_eq!(
        owned,
        cancel_request_bytes(12345_i32, 67890_i32),
        "lent bytes must match standalone cancel_request_bytes for the \
         same (pid, secret_key)",
    );
}

// =====================================================================
// Spec-2: BE encoding of pid at bytes [8..12]
// =====================================================================

#[test]
fn with_cancel_request_lends_big_endian_pid_at_bytes_8_12() {
    // Distinct pid + secret_key so we can pinpoint position
    // independence — pid at bytes[8..12], secret_key at
    // bytes[12..16]. The pid value is chosen to have a unique BE
    // byte pattern so the position is verifiable.
    let pid: i32 = i32::from_be_bytes([0xAA, 0xBB, 0xCC, 0xDD]);
    let secret: i32 = i32::from_be_bytes([0x11, 0x22, 0x33, 0x44]);
    let active = fresh_active_with_backend_key(pid, secret);
    let pid_slice_owned: [u8; 4] = active.with_cancel_request(|bytes, _pid| {
        let slice = bytes.get(8..12).unwrap_or(&[]);
        let mut buf = [0u8; 4];
        let arr: [u8; 4] = match slice.try_into() {
            Ok(a) => a,
            Err(_) => panic!("bytes[8..12] must be exactly 4 bytes — got {slice:?}"),
        };
        buf.copy_from_slice(&arr);
        buf
    });
    assert_eq!(
        pid_slice_owned,
        [0xAA, 0xBB, 0xCC, 0xDD],
        "pid must be BE-encoded at bytes[8..12]",
    );
}

// =====================================================================
// Spec-3: BE encoding of secret_key at bytes [12..16]
// =====================================================================

#[test]
fn with_cancel_request_lends_big_endian_secret_key_at_bytes_12_16() {
    let pid: i32 = i32::from_be_bytes([0xAA, 0xBB, 0xCC, 0xDD]);
    let secret: i32 = i32::from_be_bytes([0x11, 0x22, 0x33, 0x44]);
    let active = fresh_active_with_backend_key(pid, secret);
    let secret_slice_owned: [u8; 4] = active.with_cancel_request(|bytes, _pid| {
        let slice = bytes.get(12..16).unwrap_or(&[]);
        let arr: [u8; 4] = match slice.try_into() {
            Ok(a) => a,
            Err(_) => panic!("bytes[12..16] must be exactly 4 bytes — got {slice:?}"),
        };
        arr
    });
    assert_eq!(
        secret_slice_owned,
        [0x11, 0x22, 0x33, 0x44],
        "secret_key must be BE-encoded at bytes[12..16]",
    );
}

// =====================================================================
// Spec-4: magic version field at bytes [4..8] = 0x04d2162e = 80877102
// =====================================================================

#[test]
fn with_cancel_request_lends_magic_version_field_at_bytes_4_8() {
    let active = fresh_active_via_trust_handshake();
    let magic_owned: u32 = active.with_cancel_request(|bytes, _pid| {
        let slice = bytes.get(4..8).unwrap_or(&[]);
        let arr: [u8; 4] = match slice.try_into() {
            Ok(a) => a,
            Err(_) => panic!("bytes[4..8] must be exactly 4 bytes — got {slice:?}"),
        };
        // Also assert literal byte pattern from inside the closure.
        assert_eq!(
            arr,
            [0x04, 0xd2, 0x16, 0x2e],
            "magic-version field must equal CANCEL_REQUEST_VERSION = 80877102 BE",
        );
        u32::from_be_bytes(arr)
    });
    assert_eq!(
        magic_owned,
        bsql_pg_proto::wire::CANCEL_REQUEST_VERSION,
        "magic-version BE decode must equal the public CANCEL_REQUEST_VERSION constant",
    );
}

// =====================================================================
// Spec-5: length field at bytes [0..4] = 16
// =====================================================================

#[test]
fn with_cancel_request_lends_length_field_16_at_bytes_0_4() {
    let active = fresh_active_via_trust_handshake();
    let declared: u32 = active.with_cancel_request(|bytes, _pid| {
        let slice = bytes.get(0..4).unwrap_or(&[]);
        let arr: [u8; 4] = match slice.try_into() {
            Ok(a) => a,
            Err(_) => panic!("bytes[0..4] must be exactly 4 bytes — got {slice:?}"),
        };
        u32::from_be_bytes(arr)
    });
    assert_eq!(
        declared, 16,
        "length field must equal 16 (length includes self per PG protocol)",
    );
}

// =====================================================================
// Spec-6: Zeroizing<[u8; 16]> guard structural Drop-glue shield
//
// Bundle D' replaced the public `CancelRequestCredentials` struct
// (whose Drop was driven by `Sensitive<i32>`'s ZeroizeOnDrop) with
// a stack-local `Zeroizing<[u8; 16]>` guard inside
// `with_cancel_request`. We cannot directly probe the guard from
// outside the crate (it lives on `with_cancel_request`'s stack frame
// and is never named in any public type), but we CAN pin the
// transitive proof:
//
// (a) `core::mem::needs_drop::<zeroize::Zeroizing<[u8; 16]>>() ==
//     true` — `Zeroizing` derives `ZeroizeOnDrop`. If a future
//     refactor of the `zeroize` crate removed that, this assertion
//     fires.
//
// (b) Rust drop-glue rules: any function with a local
//     `Zeroizing<[u8; 16]>` runs its Drop on every scope-exit path
//     (Ok return, Err return, panic unwind under `panic = "unwind"`).
//
// (c) `Zeroizing::Drop` runs `zeroize::Zeroize` on its inner T —
//     pinned upstream by the `zeroize` crate's own test suite.
//
// (a) + (b) + (c) ⇒ the bytes lent into the closure are scrubbed on
// every return path. Same shape as
// `scram_zeroize_miri_spec.rs::password_needs_drop_is_true`.
// =====================================================================

#[test]
fn zeroizing_guard_has_drop_glue() {
    // Structural shield. If `Zeroizing<T>` ever loses its
    // `ZeroizeOnDrop` derive (upstream `zeroize` refactor), the
    // assertion fires and we get to investigate before the silent
    // tier-downgrade lands in production.
    assert!(
        core::mem::needs_drop::<zeroize::Zeroizing<[u8; 16]>>(),
        "Zeroizing<[u8; 16]> must have Drop glue (ZeroizeOnDrop \
         invariant). If this fires, the upstream `zeroize` crate's \
         Zeroizing<T> lost its drop impl — investigate and either \
         pin the working version or design a replacement scrub \
         mechanism for `with_cancel_request`'s wire-frame guard.",
    );
    // Also pin the [u8; 16] standalone (no Drop expected).
    assert!(
        !core::mem::needs_drop::<[u8; 16]>(),
        "[u8; 16] alone must NOT have Drop glue — used as the inner \
         type inside Zeroizing<[u8; 16]>. If this changes, the budget \
         calculations in `with_cancel_request` need a re-audit.",
    );
}

// =====================================================================
// Spec-7: with_cancel_request returns Some after a successful Trust
//          handshake.
// =====================================================================

#[test]
fn with_cancel_request_invokes_closure_with_post_handshake_creds() {
    // The shared helper drives a synthetic Trust handshake with
    // (12345, 67890). Post-handshake, `<ActivePhase>` carries
    // `BackendKey` inline (storage-absence proof on `into_active`),
    // so `with_cancel_request` is infallible — the call returning
    // here is the proof the closure was invoked.
    let active = fresh_active_via_trust_handshake();
    let counter = core::cell::Cell::new(0_u32);
    active.with_cancel_request(|_bytes, _pid| {
        counter.set(counter.get() + 1);
    });
    assert_eq!(
        counter.get(),
        1,
        "post-Trust-handshake `with_cancel_request` must invoke the \
         closure exactly once (infallible accessor on ActivePhase)",
    );
}

// =====================================================================
// Spec-8: closure receives the pid value installed at handshake.
// =====================================================================

#[test]
fn with_cancel_request_passes_handshake_pid_to_closure() {
    // Cross-test pid round-trip. The helper installs pid=12345 in
    // the BackendKeyData frame; the closure's `pid` arg must be
    // exactly that value.
    let active = fresh_active_via_trust_handshake();
    let pid_seen: i32 = active.with_cancel_request(|_bytes, pid| pid);
    assert_eq!(
        pid_seen, 12345_i32,
        "with_cancel_request closure's `pid` arg must be the pid installed \
         at the dispatch arm (the helper uses 12345 in the BackendKeyData frame)",
    );
}

// =====================================================================
// Spec-9: Disconnected phase has no installed backend_key.
//
// This positive test exercises the "before handshake" half of the
// installation lifecycle. The negative method-absence tests live in
// `def278d_compile_fail/` (trybuild).
// =====================================================================

#[test]
fn disconnected_protocol_starts_with_no_backend_key() {
    let proto = PgProtocol::<DisconnectedPhase>::new();
    // <DisconnectedPhase> doesn't have a `with_cancel_request`
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
// Spec-10: with_cancel_request is idempotent — calling the closure
// multiple times via repeated invocations yields identical bytes.
// =====================================================================

#[test]
fn with_cancel_request_is_idempotent_across_calls() {
    let active = fresh_active_via_trust_handshake();
    let first: [u8; 16] = active.with_cancel_request(|bytes, _pid| *bytes);
    let second: [u8; 16] = active.with_cancel_request(|bytes, _pid| *bytes);
    let third: [u8; 16] = active.with_cancel_request(|bytes, _pid| *bytes);
    assert_eq!(
        first, second,
        "with_cancel_request must be pure — two consecutive calls produce identical bytes",
    );
    assert_eq!(
        second, third,
        "with_cancel_request must be pure — three consecutive calls produce identical bytes",
    );
}

// =====================================================================
// Spec-11: with_cancel_request takes &self (does not consume).
// =====================================================================

#[test]
fn with_cancel_request_takes_shared_ref_does_not_consume_protocol() {
    // Two consecutive `with_cancel_request` calls on the same
    // `&active` must both return `Some` with identical owned bytes
    // (the cell is read-only via this accessor; the underlying
    // backend_key persists across reads).
    let active = fresh_active_via_trust_handshake();
    let first_pid: i32 = active.with_cancel_request(|_b, pid| pid);
    let second_pid: i32 = active.with_cancel_request(|_b, pid| pid);
    assert_eq!(
        first_pid, second_pid,
        "pid must be stable across consecutive reads (cell is &self-stable)",
    );
}

// =====================================================================
// Spec-12: closure panic propagates; bytes guard's Drop fires during
// unwind. The crate runs under `panic = "unwind"` for `cargo test`
// (the workspace `release` profile uses `panic = "abort"` but tests
// always link with unwind). `catch_unwind` lets us observe the
// post-unwind state without crashing the test process.
//
// Tier-1 by closure-scope: even if the closure panics, the
// `Zeroizing<[u8; 16]>` guard in `with_cancel_request`'s frame is
// dropped during the stack unwind. We cannot directly probe the
// scrubbed bytes from outside (the guard's storage is on a
// post-pop stack frame by the time we observe post-catch_unwind),
// but the transitive proof chain in `zeroizing_guard_has_drop_glue`
// + Rust drop-glue rules + `Zeroizing::Drop` semantics establishes
// the scrub.
//
// What this test verifies: the panic actually propagates through
// `with_cancel_request` (no swallowing), and a subsequent
// `with_cancel_request` call on the same protocol still works
// (no internal-state corruption from the unwind). The combination
// of (a) panic propagates + (b) state is sane + (c) drop-glue
// shield = closure-panic unwind is safe and scrubs the bytes.
// =====================================================================

#[test]
fn with_cancel_request_closure_panic_propagates_and_leaves_protocol_intact() {
    let active = fresh_active_via_trust_handshake();

    // Panic inside the closure. catch_unwind is needed because the
    // panic would otherwise abort the test process under default
    // workspace test-link settings (panic = "unwind" enables this).
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        active.with_cancel_request(|_bytes, _pid| {
            panic!("intentional test panic — Zeroizing guard must scrub on unwind");
        });
    }));
    assert!(
        result.is_err(),
        "closure panic must propagate out of with_cancel_request — \
         got Ok unexpectedly",
    );

    // Post-unwind: the protocol is still &self-borrowed elsewhere,
    // so its internal state is intact. A subsequent call must
    // succeed (the cell was not corrupted by the unwind because
    // the cell read happens BEFORE the closure runs; the unwind
    // only touches `with_cancel_request`'s stack frame).
    let bytes_again: [u8; 16] = active.with_cancel_request(|bytes, _pid| *bytes);
    // Same handshake (pid=12345, secret=67890) — must produce the
    // canonical bytes.
    assert_eq!(
        bytes_again,
        cancel_request_bytes(12345_i32, 67890_i32),
        "post-panic-recovery: bytes must still match the canonical \
         wire frame for the same (pid, secret) pair",
    );
}

// =====================================================================
// Spec-13: copying bytes contents into caller-owned storage is
// allowed (the documented gap). The original guard's bytes are
// scrubbed on closure return regardless.
//
// This is a positive control for the documented behaviour, not a
// security test. It pins the intentional design: callers MAY copy
// the byte contents (drivers need this for async writes that span
// `.await` points), but the borrow itself cannot escape (pinned by
// trybuild probe `p_d278d_6`).
// =====================================================================

#[test]
fn with_cancel_request_caller_may_copy_bytes_contents() {
    let active = fresh_active_via_trust_handshake();
    // Copy by-value into a caller-owned [u8; 16]. This is a memcpy,
    // not a reference leak — the caller's copy lives in their own
    // memory and is their responsibility to scrub if the threat
    // model requires it. The original Zeroizing guard's bytes
    // (inside `with_cancel_request`'s frame) are scrubbed on
    // closure return regardless.
    let copied: [u8; 16] = active.with_cancel_request(|bytes, _pid| {
        let mut local = [0u8; 16];
        local.copy_from_slice(bytes);
        local
    });
    assert_eq!(
        copied,
        cancel_request_bytes(12345_i32, 67890_i32),
        "caller-owned copy must equal the wire-frame bytes",
    );
    // Caller-side scrub responsibility: dropping `copied: [u8; 16]`
    // does NOT zero the array (plain [u8; 16] has no Drop glue).
    // If the driver's threat model requires scrubbing the copy,
    // they should wrap in `zeroize::Zeroizing<[u8; 16]>`.
    assert!(
        !core::mem::needs_drop::<[u8; 16]>(),
        "caller-owned [u8; 16] copy intentionally has no Drop — \
         documented in `with_cancel_request` doc-comment",
    );
}
