//! `cancel_request_bytes` public API spec.
//!
//! Validates the CancelRequest wire-builder and the magic-version
//! family pin from OUTSIDE the crate boundary. Internal `const _:
//! () = assert!(...)` drift-pins live in `wire.rs` next to the
//! function; this file covers the **visibility surface** invariants
//! that those internal pins cannot — that the function is
//! reachable through both `bsql_postgres_proto::cancel_request_bytes`
//! (top-level re-export) and `bsql_postgres_proto::wire::
//! cancel_request_bytes` (module path), and that the bytes match
//! PG §55.4 exactly when observed from a consuming crate.
//!
//! # Why this file matters
//!
//! Phase 1e wrapper drivers (`bsql-driver-postgres`) consume
//! `cancel_request_bytes(pid, secret_key)` on a parallel TCP
//! connection — they materialise the 16-byte packet and write it
//! to a fresh socket, then close. Internal drift-pins prove the
//! function output is correct WITHIN the crate; this file proves
//! the same function output is correct from a downstream
//! consumer's POV after re-export. The magic-version family pin
//! is also exercised here so a SemVer break that mishapes the
//! family formula is caught from the consumer side.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::arithmetic_side_effects
)]

// Compile-time pin: const-callable from the consumer side. If the
// re-export demotes to non-const or to `pub(crate)`, this fails to
// build before any test runs.
const _ASSERT_RETURN_LEN: () = assert!(
    bsql_postgres_proto::cancel_request_bytes(0, 0).len() == 16,
    "CancelRequest packet is 16 bytes: length(4) + version(4) + pid(4) + secret(4)",
);
const _ASSERT_LENGTH_FIELD: () = {
    let bytes = bsql_postgres_proto::cancel_request_bytes(0, 0);
    assert!(
        bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 16,
        "Length field is BE u32 = 16 (length includes self)",
    );
};
const _ASSERT_VERSION_BYTES: () = {
    let bytes = bsql_postgres_proto::cancel_request_bytes(0, 0);
    assert!(
        bytes[4] == 0x04 && bytes[5] == 0xd2 && bytes[6] == 0x16 && bytes[7] == 0x2e,
        "Version bytes must encode 80877102 = 0x04d2162e per PG §55.4",
    );
};

#[test]
fn cancel_request_bytes_match_pg_spec_zero_payload() {
    assert_eq!(
        bsql_postgres_proto::cancel_request_bytes(0, 0),
        [
            0, 0, 0, 16, // length BE = 16
            0x04, 0xd2, 0x16, 0x2e, // version BE = 80877102
            0, 0, 0, 0, // pid = 0
            0, 0, 0, 0, // secret_key = 0
        ],
        "PG §55.4 CancelRequest packet: length(16) BE + magic version 80877102 BE \
         + zero pid + zero secret_key",
    );
}

#[test]
fn cancel_request_bytes_match_pg_spec_nonzero_payload() {
    // Realistic non-zero payload — PG hands out pid + secret_key
    // as i32, server-assigned at startup. Test pins BE-encoding
    // of both fields at their dynamic positions [8..16].
    let pid: i32 = 0x1234_5678; // 305419896
    let key: i32 = 0x09ab_cdef; // 162254319
    assert_eq!(
        bsql_postgres_proto::cancel_request_bytes(pid, key),
        [
            0, 0, 0, 16,
            0x04, 0xd2, 0x16, 0x2e,
            0x12, 0x34, 0x56, 0x78,
            0x09, 0xab, 0xcd, 0xef,
        ],
    );
}

#[test]
fn cancel_request_top_level_and_module_paths_agree() {
    // Top-level re-export and module path resolve to the same
    // function. A hypothetical accidental duplicate (separate
    // const fn copy under a different path) would diverge here.
    let pid: i32 = 0x7777_7777;
    let key: i32 = 0x3333_3333;
    assert_eq!(
        bsql_postgres_proto::cancel_request_bytes(pid, key),
        bsql_postgres_proto::wire::cancel_request_bytes(pid, key),
    );
}

#[test]
fn cancel_request_version_const_matches_byte_literal() {
    let v = bsql_postgres_proto::wire::CANCEL_REQUEST_VERSION;
    assert_eq!(
        v, 80_877_102u32,
        "CANCEL_REQUEST_VERSION constant value per PG §55.4",
    );
    // Materialise via builder; pull version bytes out of slice
    // [4..8]; verify they match the const's BE encoding.
    let bytes = bsql_postgres_proto::cancel_request_bytes(0, 0);
    let v_bytes = v.to_be_bytes();
    let wire_v = bytes.get(4..8).unwrap_or_default();
    assert_eq!(
        wire_v,
        v_bytes.as_slice(),
        "wire-byte version field must equal CANCEL_REQUEST_VERSION.to_be_bytes()",
    );
}

#[test]
fn cancel_request_length_field_includes_self() {
    // PG protocol convention: every length-prefixed message has
    // the length field include itself. CancelRequest body is
    // 12 bytes (version + pid + secret_key); length = 12 + 4 = 16.
    // Use `i32::from_be_bytes` instead of `as i32` cast since
    // the test file forbids `as`-conversions.
    let pid: i32 = i32::from_be_bytes([0xde, 0xad, 0xbe, 0xef]);
    let bytes = bsql_postgres_proto::cancel_request_bytes(pid, 42);
    let len_bytes: [u8; 4] = bytes
        .get(..4)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .unwrap_or_default();
    let declared = u32::from_be_bytes(len_bytes);
    assert_eq!(
        declared, 16,
        "length field (BE u32) must equal total packet size (16)",
    );
    assert_eq!(
        usize::try_from(declared).ok(),
        Some(bytes.len()),
        "declared length matches actual packet length",
    );
}

#[test]
fn cancel_request_distinct_from_ssl_and_terminate() {
    // CancelRequest is a 16-byte StartupMessage-shaped packet
    // with magic version 80877102. SSLRequest is 8-byte with
    // magic 80877103; Terminate is 5-byte 'X' frame. The version
    // codes share a family (1234 high half) but discriminate via
    // low half — pin the distinctness here so a copy-paste error
    // is caught from outside the crate.
    let cancel = bsql_postgres_proto::cancel_request_bytes(0, 0);
    assert_ne!(
        &cancel[..],
        &bsql_postgres_proto::SSL_REQUEST_WIRE_BYTES[..],
        "CancelRequest must not collide with SSL_REQUEST_WIRE_BYTES",
    );
    assert_ne!(
        &cancel[..],
        &bsql_postgres_proto::TERMINATE_WIRE_BYTES[..],
        "CancelRequest must not collide with TERMINATE_WIRE_BYTES",
    );
    // Magic-version distinctness vs SSL.
    assert_ne!(
        bsql_postgres_proto::wire::CANCEL_REQUEST_VERSION,
        bsql_postgres_proto::wire::SSL_REQUEST_VERSION,
        "Cancel and SSL magic version codes must be distinct",
    );
    // Magic-version distinctness vs real protocol version.
    assert_ne!(
        bsql_postgres_proto::wire::CANCEL_REQUEST_VERSION,
        bsql_postgres_proto::wire::PROTOCOL_VERSION_3_0,
        "Cancel magic version must differ from real protocol version (3.0)",
    );
}

#[test]
fn cancel_request_pg_canonical_magic_decomposition() {
    // PG's "magic-version" packets all share the shape
    // (1234 << 16) | code. CancelRequest = 1234<<16 | 5678 = 80877102.
    // Pin the specific decomposition so a typo in the magic
    // constant is caught from the consumer side.
    let v = bsql_postgres_proto::wire::CANCEL_REQUEST_VERSION;
    let high = v >> 16;
    let low = v & 0xffff;
    assert_eq!(high, 1234, "high 16 bits must be PG's magic 1234 marker");
    assert_eq!(low, 5678, "low 16 bits must be 5678 (CancelRequest specific)");
}

#[test]
fn cancel_request_magic_version_family_pin() {
    // The MAGIC_VERSION_HIGH_HALF const is the family marker
    // shared by SSL_REQUEST_VERSION + CANCEL_REQUEST_VERSION
    // (and a future GSSENC_REQUEST_VERSION = 1234<<16 | 5680).
    // Verify the formula holds for both currently-defined family
    // members from the consumer side.
    let high = bsql_postgres_proto::wire::MAGIC_VERSION_HIGH_HALF;
    assert_eq!(high, 1234, "magic-version family high half is 1234");

    // SSL low half = 5679.
    assert_eq!(
        bsql_postgres_proto::wire::SSL_REQUEST_VERSION,
        (high << 16) | 5679,
        "SSL_REQUEST_VERSION must satisfy family formula with low=5679",
    );
    // Cancel low half = 5678.
    assert_eq!(
        bsql_postgres_proto::wire::CANCEL_REQUEST_VERSION,
        (high << 16) | 5678,
        "CANCEL_REQUEST_VERSION must satisfy family formula with low=5678",
    );
}

#[test]
fn cancel_request_negative_pid_be_signed_encoding() {
    // pid is i32; PG accepts the full range. Negative values
    // encode via two's complement BE — pin the contract.
    let bytes_neg_one = bsql_postgres_proto::cancel_request_bytes(-1, 0);
    assert_eq!(
        bytes_neg_one.get(8..12).unwrap_or_default(),
        &[0xff, 0xff, 0xff, 0xff][..],
        "pid = -1 encodes as 0xFFFFFFFF BE",
    );
    let bytes_min = bsql_postgres_proto::cancel_request_bytes(i32::MIN, i32::MIN);
    assert_eq!(
        bytes_min.get(8..12).unwrap_or_default(),
        &[0x80, 0x00, 0x00, 0x00][..],
        "pid = i32::MIN encodes as 0x80000000 BE",
    );
    assert_eq!(
        bytes_min.get(12..16).unwrap_or_default(),
        &[0x80, 0x00, 0x00, 0x00][..],
        "secret_key = i32::MIN encodes as 0x80000000 BE",
    );
}

#[test]
fn cancel_request_max_pid_secret_be_encoding() {
    // i32::MAX edge — 0x7FFFFFFF.
    let bytes = bsql_postgres_proto::cancel_request_bytes(i32::MAX, i32::MAX);
    assert_eq!(
        bytes.get(8..12).unwrap_or_default(),
        &[0x7f, 0xff, 0xff, 0xff][..],
        "pid = i32::MAX encodes as 0x7FFFFFFF BE",
    );
    assert_eq!(
        bytes.get(12..16).unwrap_or_default(),
        &[0x7f, 0xff, 0xff, 0xff][..],
        "secret_key = i32::MAX encodes as 0x7FFFFFFF BE",
    );
}

#[test]
fn cancel_request_pid_independent_of_secret_key() {
    // Position-independence: changing only pid must not affect
    // the secret_key bytes (and vice-versa). Catches a hypothetical
    // bug where the two fields are accidentally OR'd / concatenated
    // wrong.
    // `i32::from_be_bytes` instead of `as`-cast (forbidden by
    // file-level lint config).
    let payload: i32 = i32::from_be_bytes([0xaa, 0xbb, 0xcc, 0xdd]);
    let with_pid = bsql_postgres_proto::cancel_request_bytes(payload, 0);
    let with_key = bsql_postgres_proto::cancel_request_bytes(0, payload);
    // pid bytes appear at [8..12] in `with_pid` but [12..16] in `with_key`.
    assert_eq!(
        with_pid.get(8..12).unwrap_or_default(),
        &[0xaa, 0xbb, 0xcc, 0xdd][..],
    );
    assert_eq!(
        with_pid.get(12..16).unwrap_or_default(),
        &[0, 0, 0, 0][..],
        "secret_key should be zero when only pid was set",
    );
    assert_eq!(
        with_key.get(8..12).unwrap_or_default(),
        &[0, 0, 0, 0][..],
        "pid should be zero when only secret_key was set",
    );
    assert_eq!(
        with_key.get(12..16).unwrap_or_default(),
        &[0xaa, 0xbb, 0xcc, 0xdd][..],
    );
}

/// `cancel_request_bytes` is `const fn`. Pin via compile-time
/// `const _` evaluation outside the crate boundary so that
/// demotion to non-const breaks the build.
const _PIN_CONST_FN: () = {
    let _ = bsql_postgres_proto::cancel_request_bytes(0, 0);
    let _ = bsql_postgres_proto::cancel_request_bytes(i32::MAX, i32::MIN);
    let _ = bsql_postgres_proto::cancel_request_bytes(-1, 1);
};
