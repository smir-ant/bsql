//! `FLUSH_WIRE_BYTES` public API spec.
//!
//! Validates the `Flush` ('H') frontend wire primitive from OUTSIDE
//! the crate boundary. Internal `const _: () = assert!(...)` drift-pins
//! live in `wire.rs` next to the literal; this file covers the
//! **visibility surface** invariants that those internal pins cannot
//! — that the literal is reachable through both
//! `bsql_pg_proto::FLUSH_WIRE_BYTES` (top-level re-export) and
//! `bsql_pg_proto::wire::FLUSH_WIRE_BYTES` (module path), and that
//! the bytes match PG §55.7 exactly when observed from a consuming
//! crate.
//!
//! # Why this file matters
//!
//! Pipelining drivers will consume `FLUSH_WIRE_BYTES`
//! directly — they write the bytes to the socket mid-batch to
//! extract intermediate responses without committing the implicit
//! transaction. Internal drift-pins prove the literal is correct
//! WITHIN the crate; this file proves the same literal is correct
//! from a downstream consumer's POV after re-export. Without this
//! file, a hypothetical regression that demoted the re-export to
//! `pub(crate)` would only fire on driver compile, not on
//! `cargo test -p bsql-pg-proto`.
//!
//! Mirror of `tests/terminate_wire_spec.rs` — same shape, same
//! tier-1 closure pattern.

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
    bsql_pg_proto::FLUSH_WIRE_BYTES.len() == 5,
    "Flush frame is 5 bytes: tag (1) + length-field (4)",
);
const _ASSERT_TAG: () = assert!(
    bsql_pg_proto::FLUSH_WIRE_BYTES[0] == b'H',
    "Flush tag is 'H' per PG §55.7",
);
const _ASSERT_LENGTH_FIELD: () = assert!(
    bsql_pg_proto::FLUSH_WIRE_BYTES[1] == 0
        && bsql_pg_proto::FLUSH_WIRE_BYTES[2] == 0
        && bsql_pg_proto::FLUSH_WIRE_BYTES[3] == 0
        && bsql_pg_proto::FLUSH_WIRE_BYTES[4] == 4,
    "Length field is BE u32 = 4 (length includes self, no payload)",
);

#[test]
fn flush_wire_bytes_match_pg_spec() {
    assert_eq!(
        bsql_pg_proto::FLUSH_WIRE_BYTES,
        [b'H', 0, 0, 0, 4],
        "PG §55.7 Flush frame: tag 'H' + BE u32 length=4",
    );
}

#[test]
fn flush_wire_bytes_top_level_and_module_paths_agree() {
    // Top-level re-export and module path resolve to the same
    // const. A hypothetical accidental duplicate (separate const
    // copy under a different path) would diverge here.
    assert_eq!(
        bsql_pg_proto::FLUSH_WIRE_BYTES,
        bsql_pg_proto::wire::FLUSH_WIRE_BYTES,
    );
}

#[test]
fn flush_wire_bytes_distinct_from_sibling_parameterless_frames() {
    // The three parameterless 5-byte frames (Sync 'S', Terminate 'X',
    // Flush 'H') share identical length-field bytes. The ONLY
    // distinguishing byte is the tag. Pin pairwise distinctness from
    // the consumer side — internal drift-pin already covers this at
    // build time, but mirroring it here surfaces the invariant in
    // the public-API contract.
    //
    // SYNC_WIRE_BYTES is `pub(crate)`, so we mirror its literal
    // shape; TERMINATE_WIRE_BYTES is the public sibling already
    // re-exported.
    let sync_bytes: [u8; 5] = [b'S', 0, 0, 0, 4];
    assert_ne!(
        bsql_pg_proto::FLUSH_WIRE_BYTES, sync_bytes,
        "Flush ('H') and Sync ('S') must not share bytes",
    );
    assert_ne!(
        bsql_pg_proto::FLUSH_WIRE_BYTES,
        bsql_pg_proto::TERMINATE_WIRE_BYTES,
        "Flush ('H') and Terminate ('X') must not share bytes",
    );
}
