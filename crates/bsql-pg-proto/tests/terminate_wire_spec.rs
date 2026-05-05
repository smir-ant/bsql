//! DEF-223 (2026-05-05) — `TERMINATE_WIRE_BYTES` public API spec.
//!
//! Validates the `Terminate` ('X') frontend wire primitive from
//! OUTSIDE the crate boundary. Internal `const _: () = assert!(...)`
//! drift-pins live in `wire.rs` next to the literal; this file
//! covers the **visibility surface** invariants that those internal
//! pins cannot — that the literal is reachable through both
//! `bsql_pg_proto::TERMINATE_WIRE_BYTES` (top-level re-export) and
//! `bsql_pg_proto::wire::TERMINATE_WIRE_BYTES` (module path), and
//! that the bytes match PG §55.7 exactly when observed from a
//! consuming crate.
//!
//! # Why this file matters
//!
//! Phase 1e wrapper drivers (`bsql-driver-postgres`) consume
//! `TERMINATE_WIRE_BYTES` directly — they write the bytes to the
//! socket immediately before TCP shutdown. Internal drift-pins
//! prove the literal is correct WITHIN the crate; this file proves
//! the same literal is correct from a downstream consumer's POV
//! after re-export. Without this file, a hypothetical regression
//! that demoted the re-export to `pub(crate)` would only fire on
//! driver compile, not on `cargo test -p bsql-pg-proto`.

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
    bsql_pg_proto::TERMINATE_WIRE_BYTES.len() == 5,
    "Terminate frame is 5 bytes: tag (1) + length-field (4)",
);
const _ASSERT_TAG: () = assert!(
    bsql_pg_proto::TERMINATE_WIRE_BYTES[0] == b'X',
    "Terminate tag is 'X' per PG §55.7",
);
const _ASSERT_LENGTH_FIELD: () = assert!(
    bsql_pg_proto::TERMINATE_WIRE_BYTES[1] == 0
        && bsql_pg_proto::TERMINATE_WIRE_BYTES[2] == 0
        && bsql_pg_proto::TERMINATE_WIRE_BYTES[3] == 0
        && bsql_pg_proto::TERMINATE_WIRE_BYTES[4] == 4,
    "Length field is BE u32 = 4 (length includes self, no payload)",
);

#[test]
fn terminate_wire_bytes_match_pg_spec() {
    assert_eq!(
        bsql_pg_proto::TERMINATE_WIRE_BYTES,
        [b'X', 0, 0, 0, 4],
        "PG §55.7 Terminate frame: tag 'X' + BE u32 length=4",
    );
}

#[test]
fn terminate_wire_bytes_top_level_and_module_paths_agree() {
    // Top-level re-export and module path resolve to the same
    // const. A hypothetical accidental duplicate (separate const
    // copy under a different path) would diverge here.
    assert_eq!(
        bsql_pg_proto::TERMINATE_WIRE_BYTES,
        bsql_pg_proto::wire::TERMINATE_WIRE_BYTES,
    );
}

#[test]
fn terminate_wire_bytes_distinct_from_sync() {
    // Sync wire bytes are `pub(crate)` so we cannot reference
    // them directly here; reconstructed literal mirror suffices
    // for the distinctness check at this scope.
    let sync_bytes: [u8; 5] = [b'S', 0, 0, 0, 4];
    assert_ne!(
        bsql_pg_proto::TERMINATE_WIRE_BYTES, sync_bytes,
        "Terminate ('X') and Sync ('S') must not share bytes",
    );
}
