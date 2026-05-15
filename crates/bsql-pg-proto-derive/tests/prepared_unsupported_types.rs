//! DEF-244 — drift-detection for the `prepared!` typemap's v1.0
//! supported set vs the DEF-228 pending types.
//!
//! For each PG type that the runtime crate does NOT yet support
//! (`bytea` / `varchar` / `float4` / `float8` / `numeric` /
//! `timestamp` / `uuid` / `jsonb` / ...), this harness pins the
//! macro-expand-time rejection's exact diagnostic. The pinning
//! mechanism: a `.rs` source file invoking `prepared!("...$1::TYPE")`
//! must fail to compile, with the `.stderr` golden matching the
//! current diagnostic word-for-word.
//!
//! # Drift detection contract
//!
//! When DEF-228 lands `DecodeFormat<TextFmt>` (and `EncodeBinary`)
//! impls for a type, the typemap in `src/typemap.rs` grows a new
//! arm AND the matching `.rs` + `.stderr` files under
//! `tests/prepared_unsupported_types/` must be **deleted** in the
//! same commit. If the contributor forgets to delete the file, the
//! `prepared!` macro will now ACCEPT the type, the `.rs` file will
//! compile, and the trybuild assertion that the file fails to
//! compile fires — forcing the contributor to either delete the
//! file or revert the typemap change.
//!
//! # Why per-type files (not one consolidated probe)
//!
//! - **Selective re-run**: `cargo test --test prepared_unsupported_types
//!   bytea` runs just the bytea probe.
//! - **Per-type documentation**: each `.rs` file's header comment
//!   names the Rust type, the DEF-228 dependency, and the deletion
//!   instruction.
//! - **Compounding evidence**: a v2 contributor scanning the
//!   directory sees the exact set of types that are pending and
//!   their tier-1-rejection enforcement.
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test prepared_unsupported_types
//! ```

#![forbid(unsafe_code)]

/// `bytea` (PG BYTEA / binary blob). Tracks DEF-228 for `&[u8]` or
/// canonical `Bytea` newtype decoder.
#[test]
fn bytea_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/bytea.rs");
}

/// `varchar` (PG VARCHAR / variable-length text). Tracks DEF-228 —
/// can likely re-use the `text` decoder when added.
#[test]
fn varchar_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/varchar.rs");
}

/// `float4` (PG REAL / single-precision IEEE 754).
#[test]
fn float4_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/float4.rs");
}

/// `float8` (PG DOUBLE PRECISION / double-precision IEEE 754).
#[test]
fn float8_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/float8.rs");
}

/// `numeric` (PG arbitrary-precision DECIMAL).
#[test]
fn numeric_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/numeric.rs");
}

/// `timestamp` (PG TIMESTAMP / timezone-less).
#[test]
fn timestamp_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/timestamp.rs");
}

/// `uuid` (PG UUID / 128-bit RFC 4122 identifier).
#[test]
fn uuid_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/uuid.rs");
}

/// `jsonb` (PG JSONB / binary JSON with version header).
#[test]
fn jsonb_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_unsupported_types/jsonb.rs");
}
