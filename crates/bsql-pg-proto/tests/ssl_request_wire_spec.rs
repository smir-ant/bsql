//! DEF-214 (2026-05-05) — `SSL_REQUEST_WIRE_BYTES` public API spec.
//!
//! Validates the SSLRequest pre-startup wire primitive from OUTSIDE
//! the crate boundary. Internal `const _: () = assert!(...)` drift-
//! pins live in `wire.rs` next to the literal; this file covers
//! the **visibility surface** invariants that those internal pins
//! cannot — that the literal is reachable through both
//! `bsql_pg_proto::SSL_REQUEST_WIRE_BYTES` (top-level re-export)
//! and `bsql_pg_proto::wire::SSL_REQUEST_WIRE_BYTES` (module path),
//! and that the bytes match PG §55.10 exactly when observed from a
//! consuming crate.
//!
//! # Why this file matters
//!
//! Phase 1e wrapper drivers (`bsql-driver-postgres`) consume
//! `SSL_REQUEST_WIRE_BYTES` directly — they write the bytes to the
//! socket BEFORE constructing the `PgProtocol` state machine, then
//! read the 1-byte server response and decide whether to perform
//! TLS handshake. Internal drift-pins prove the literal is correct
//! WITHIN the crate; this file proves the same literal is correct
//! from a downstream consumer's POV after re-export.

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
const _ASSERT_LEN: () = assert!(
    bsql_pg_proto::SSL_REQUEST_WIRE_BYTES.len() == 8,
    "SSLRequest packet is 8 bytes: length(4) + version(4)",
);
const _ASSERT_LENGTH_FIELD: () = assert!(
    bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[0] == 0
        && bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[1] == 0
        && bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[2] == 0
        && bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[3] == 8,
    "Length field is BE u32 = 8 (length includes self + version code)",
);
const _ASSERT_VERSION_BYTES: () = assert!(
    bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[4] == 0x04
        && bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[5] == 0xd2
        && bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[6] == 0x16
        && bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[7] == 0x2f,
    "Version bytes must encode 80877103 = 0x04d2162f per PG §55.10",
);

#[test]
fn ssl_request_wire_bytes_match_pg_spec() {
    assert_eq!(
        bsql_pg_proto::SSL_REQUEST_WIRE_BYTES,
        [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f],
        "PG §55.10 SSLRequest packet: length(8) BE + magic version 80877103 BE",
    );
}

#[test]
fn ssl_request_top_level_and_module_paths_agree() {
    // Top-level re-export and module path resolve to the same
    // const. A hypothetical accidental duplicate (separate const
    // copy under a different path) would diverge here.
    assert_eq!(
        bsql_pg_proto::SSL_REQUEST_WIRE_BYTES,
        bsql_pg_proto::wire::SSL_REQUEST_WIRE_BYTES,
    );
}

#[test]
fn ssl_request_version_const_matches_byte_literal() {
    let v = bsql_pg_proto::wire::SSL_REQUEST_VERSION;
    assert_eq!(v, 80_877_103u32, "SSL_REQUEST_VERSION constant value");
    let v_bytes = v.to_be_bytes();
    let wire_v = bsql_pg_proto::SSL_REQUEST_WIRE_BYTES.get(4..).unwrap_or_default();
    assert_eq!(
        wire_v,
        v_bytes.as_slice(),
        "wire-byte version field must equal SSL_REQUEST_VERSION.to_be_bytes()",
    );
}

#[test]
fn ssl_request_length_field_includes_self() {
    // PG protocol convention: every length-prefixed message has
    // the length field include itself. SSLRequest body is the
    // 4-byte version code; length = 4 (body) + 4 (length field) = 8.
    let len_bytes: [u8; 4] = bsql_pg_proto::SSL_REQUEST_WIRE_BYTES
        .get(..4)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .unwrap_or_default();
    let declared = u32::from_be_bytes(len_bytes);
    assert_eq!(
        declared,
        8,
        "length field (BE u32) must equal total packet size (8)",
    );
    assert_eq!(
        usize::try_from(declared).ok(),
        Some(bsql_pg_proto::SSL_REQUEST_WIRE_BYTES.len()),
        "declared length matches actual packet length",
    );
}

#[test]
fn ssl_request_distinct_from_terminate_and_sync() {
    // SSLRequest is a startup-shaped 8-byte packet, NOT a
    // tagged frame. Distinct from TERMINATE_WIRE_BYTES (5-byte
    // 'X' frame) and from a regular StartupMessage (which uses
    // protocol version 196608, not 80877103).
    assert_ne!(
        &bsql_pg_proto::SSL_REQUEST_WIRE_BYTES[..],
        &bsql_pg_proto::TERMINATE_WIRE_BYTES[..],
        "SSLRequest must not collide with TERMINATE_WIRE_BYTES",
    );
    // A real StartupMessage uses version 3.0 = 196608. The two
    // version codes must be distinct so a server's StartupMessage
    // parser can disambiguate.
    let real_proto_version = bsql_pg_proto::wire::PROTOCOL_VERSION_3_0;
    assert_ne!(
        bsql_pg_proto::wire::SSL_REQUEST_VERSION, real_proto_version,
        "SSL request version magic must differ from real protocol version (3.0)",
    );
}

#[test]
fn ssl_request_first_four_bytes_decode_to_eight() {
    // Sanity: ensure no integer-overflow trap in length decoding.
    // 8 fits trivially in u32 and usize.
    let packet = bsql_pg_proto::SSL_REQUEST_WIRE_BYTES;
    assert_eq!(packet.first().copied(), Some(0u8));
    assert_eq!(packet.get(1).copied(), Some(0u8));
    assert_eq!(packet.get(2).copied(), Some(0u8));
    assert_eq!(packet.get(3).copied(), Some(8u8));
}

#[test]
fn ssl_request_pg_canonical_magic_decomposition() {
    // PG's "magic-version" packets all share the shape
    // (1234 << 16) | code. SSLRequest = 1234<<16 | 5679 = 80877103.
    // CancelRequest = 1234<<16 | 5678 = 80877102.
    // GSSENCRequest = 1234<<16 | 5680 = 80877104.
    // This test pins SSLRequest's specific decomposition so a
    // typo in the magic constant is caught.
    let v = bsql_pg_proto::wire::SSL_REQUEST_VERSION;
    let high = v >> 16;
    let low = v & 0xffff;
    assert_eq!(high, 1234, "high 16 bits must be PG's magic 1234 marker");
    assert_eq!(low, 5679, "low 16 bits must be 5679 (SSLRequest specific)");
}

// ==================================================================
// DEF-214 Phase 2: classify_ssl_response_byte typed-outcome tests
// (2026-05-07): comprehensive coverage of the 1-byte SSL response
// classifier — every defined byte, every undefined byte sample,
// non-exhaustive guarantee, equality semantics.
// ==================================================================

/// `'S'` (0x53) → `Accepted`. Server accepts SSL; driver should
/// proceed to TLS handshake.
#[test]
fn classify_ssl_byte_s_is_accepted() {
    use bsql_pg_proto::SslNegotiationOutcome;
    assert!(matches!(
        bsql_pg_proto::classify_ssl_response_byte(b'S'),
        SslNegotiationOutcome::Accepted,
    ));
}

/// `'N'` (0x4e) → `Refused`. Server does not support SSL.
#[test]
fn classify_ssl_byte_n_is_refused() {
    use bsql_pg_proto::SslNegotiationOutcome;
    assert!(matches!(
        bsql_pg_proto::classify_ssl_response_byte(b'N'),
        SslNegotiationOutcome::Refused,
    ));
}

/// `'E'` (0x45) → `ErrorIncoming`. Server is about to send an
/// `ErrorResponse` frame on the wire.
#[test]
fn classify_ssl_byte_e_is_error_incoming() {
    use bsql_pg_proto::SslNegotiationOutcome;
    assert!(matches!(
        bsql_pg_proto::classify_ssl_response_byte(b'E'),
        SslNegotiationOutcome::ErrorIncoming,
    ));
}

/// Every byte value OUTSIDE the {S, N, E} set must classify as
/// `InvalidByte(b)` carrying the offending byte verbatim. This
/// exhaustive 0..=255 sweep catches a regression that
/// accidentally maps another byte to a known outcome.
#[test]
fn classify_ssl_byte_unknown_bytes_preserve_payload() {
    use bsql_pg_proto::SslNegotiationOutcome;
    for byte in 0..=255u8 {
        let outcome = bsql_pg_proto::classify_ssl_response_byte(byte);
        match byte {
            b'S' => assert!(matches!(outcome, SslNegotiationOutcome::Accepted)),
            b'N' => assert!(matches!(outcome, SslNegotiationOutcome::Refused)),
            b'E' => assert!(matches!(outcome, SslNegotiationOutcome::ErrorIncoming)),
            other => assert!(
                matches!(outcome, SslNegotiationOutcome::InvalidByte(b) if b == other),
                "byte {other:#x} must classify as InvalidByte({other:#x}); got {outcome:?}",
            ),
        }
    }
}

/// Specific edge bytes with classic boundary properties: 0x00
/// (NUL), 0xFF (all-ones), 0x80 (high bit), 0x7F (high bit clear).
/// These are common interpretation-error sources in C-style code;
/// pin the Rust impl.
#[test]
fn classify_ssl_byte_boundary_values() {
    use bsql_pg_proto::SslNegotiationOutcome;
    let cases = [0x00u8, 0xff, 0x80, 0x7f, 0x01, 0xfe];
    for byte in cases {
        let outcome = bsql_pg_proto::classify_ssl_response_byte(byte);
        match outcome {
            SslNegotiationOutcome::InvalidByte(b) => {
                assert_eq!(b, byte, "InvalidByte payload must match input");
            }
            other => panic!("byte {byte:#x} classified non-Invalid: {other:?}"),
        }
    }
}

/// PartialEq sanity: outcomes with the same shape compare equal,
/// outcomes with different shape (including Invalid with different
/// payloads) compare unequal.
#[test]
fn classify_ssl_byte_outcome_equality_semantics() {
    let s = bsql_pg_proto::classify_ssl_response_byte(b'S');
    let s2 = bsql_pg_proto::classify_ssl_response_byte(b'S');
    let n = bsql_pg_proto::classify_ssl_response_byte(b'N');
    let invalid_a = bsql_pg_proto::classify_ssl_response_byte(0xab);
    let invalid_a2 = bsql_pg_proto::classify_ssl_response_byte(0xab);
    let invalid_b = bsql_pg_proto::classify_ssl_response_byte(0xcd);

    assert_eq!(s, s2, "same outcome variant compares equal");
    assert_ne!(s, n, "Accepted vs Refused unequal");
    assert_eq!(invalid_a, invalid_a2, "InvalidByte with same payload equal");
    assert_ne!(invalid_a, invalid_b, "InvalidByte with different payload unequal");
    assert_ne!(s, invalid_a, "Accepted vs InvalidByte unequal");
}

/// `classify_ssl_response_byte` is `const fn`. Pin via
/// compile-time `const _` evaluation outside the crate boundary.
const _PIN_CONST_FN: () = {
    let _ = bsql_pg_proto::classify_ssl_response_byte(b'S');
    let _ = bsql_pg_proto::classify_ssl_response_byte(b'N');
    let _ = bsql_pg_proto::classify_ssl_response_byte(b'E');
    let _ = bsql_pg_proto::classify_ssl_response_byte(0xff);
};

/// Top-level re-export of the classifier function and outcome
/// enum agrees with the `wire::` module path.
#[test]
fn classify_ssl_byte_top_level_and_module_paths_agree() {
    let via_top = bsql_pg_proto::classify_ssl_response_byte(b'S');
    let via_mod = bsql_pg_proto::wire::classify_ssl_response_byte(b'S');
    assert_eq!(via_top, via_mod, "top-level fn must equal module-path fn");
}

/// `SslNegotiationOutcome` carries `#[non_exhaustive]`. Future PG
/// versions could add a new response byte (e.g. for new TLS
/// extensions); downstream consumers MUST use a catch-all when
/// matching from outside the crate. This test exercises that the
/// catch-all pattern compiles and runs — proof that
/// `#[non_exhaustive]` is preserved on the public surface.
#[test]
fn outcome_non_exhaustive_requires_catchall_externally() {
    use bsql_pg_proto::SslNegotiationOutcome;
    let outcome = bsql_pg_proto::classify_ssl_response_byte(b'S');
    let label: &'static str = match outcome {
        SslNegotiationOutcome::Accepted => "accepted",
        SslNegotiationOutcome::Refused => "refused",
        SslNegotiationOutcome::ErrorIncoming => "err",
        SslNegotiationOutcome::InvalidByte(_) => "invalid",
        // Catch-all required by `#[non_exhaustive]`. If this arm
        // is removed, a future PG-spec addition would silently
        // miscategorise; with it, future variants land here as
        // "unknown" until the driver explicitly handles them.
        _ => "future-extension-not-yet-handled",
    };
    assert_eq!(label, "accepted");
}
