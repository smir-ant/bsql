//! DEF-244 — `trybuild` golden harness for the `prepared!`
//! proc-macro's hostile-bypass probes (memo §7 P1-P12).
//!
//! Each probe is a self-contained `.rs` file under
//! `tests/prepared_compile_fail/`. Trybuild attempts to compile each
//! file and compares the stderr output against the matching
//! `.stderr` golden. Mismatch = test failure with a coloured diff.
//!
//! # Why per-probe `#[test]` (not one `pass`/`compile_fail` glob)
//!
//! One `#[test]` per probe gives:
//! - **Named failure**: cargo test output names which probe broke.
//! - **Selective re-run**: `cargo test --test prepared_compile_fail
//!   p7_arg_type_mismatch` runs only P7.
//! - **Per-probe documentation**: each `#[test]` carries a doc
//!   comment naming the probe + memo section.
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test prepared_compile_fail
//! ```
//!
//! Overwrites every `.stderr` golden with the current compiler's
//! actual diagnostic. After regenerating, **review every diff** —
//! a diagnostic shift can mean a real regression (the closure
//! mechanism changed) or an innocuous rustc message tweak. Audit
//! trail: the goldens are committed; PRs that regenerate them must
//! justify the change.
//!
//! # Tier framing
//!
//! Eight of twelve probes close tier-1 inside the language: P1, P2,
//! P3, P6, P7, P8, P9, P11. The remaining four (P4, P5, P10, P12)
//! pin the language-level half of the OS-boundary closure:
//! `#![forbid(unsafe_code)]` at the probe-file scope mechanically
//! rejects `unsafe` blocks. The OS-level half (`.rodata` is read-only,
//! `mprotect` segments at process boundary) is the production-runtime
//! enforcement; trybuild cannot exercise it but the architectural
//! statement is documented in each probe's header comment.
//!
//! See `/tmp/def244-design-memo.md` §7 for the full hostile-bypass
//! enumeration and §12 for the OS-boundary framing parallel to
//! DEF-248 Sub-A's `panic = "abort"`.

#![forbid(unsafe_code)]

/// **P1** — direct struct construction with hostile SQL.
/// Expected: `error[E0451]: field 'sql' of struct 'PreparedQuery' is
/// private` (plus same for other fields). Memo §7 Probe P1.
#[test]
fn p1_direct_struct_construction() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p1_direct_struct_construction.rs");
}

/// **P2** — call `PreparedQuery::new(...)`. Expected:
/// `error[E0599]: no function or associated item named 'new' found`.
/// Memo §7 Probe P2.
#[test]
fn p2_no_public_new() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p2_no_public_new.rs");
}

/// **P3** — read `Q.sql` from outside crate. Expected:
/// `error[E0616]: field 'sql' of struct 'PreparedQuery' is private`.
/// Memo §7 Probe P3.
#[test]
fn p3_field_read_from_outside() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p3_field_read_from_outside.rs");
}

/// **P4** — mutate via raw pointer (`unsafe` raw-ptr mutate).
/// Expected: `error: usage of an 'unsafe' block` (from
/// `#[forbid(unsafe_code)]`). OS-boundary parallel to DEF-248 Sub-A.
/// Memo §7 Probe P4 + §12.
#[test]
fn p4_unsafe_raw_ptr_mutate() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p4_unsafe_raw_ptr_mutate.rs");
}

/// **P5** — fabricate `PreparedQuery` via `unsafe` pointer cast.
/// Expected: `error: usage of an 'unsafe' block`. Same OS-boundary
/// class as P4. Memo §7 Probe P5 + §12.
#[test]
fn p5_unsafe_fabricate() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p5_unsafe_fabricate.rs");
}

/// **P6** — pass a runtime string into `prepared!`. Expected:
/// `error: prepared!: SQL must be a single string literal`
/// (macro-emitted `compile_error!`). Memo §7 Probe P6.
#[test]
fn p6_runtime_string_into_macro() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p6_runtime_string_into_macro.rs");
}

/// **P7** — bind wrong-type arguments to a prepared query. Expected:
/// `error[E0308]: mismatched types`. Tuple types nominally distinct.
/// Memo §7 Probe P7.
#[test]
fn p7_arg_type_mismatch() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p7_arg_type_mismatch.rs");
}

/// **P8** — hostile `impl ParamsWriter for EvilParams`. Expected:
/// `error[E0277]: ... ParamsWriterSealed`. Sealed super-trait
/// barrier — external crates cannot satisfy the bound. Memo §7
/// Probe P8.
#[test]
fn p8_hostile_paramswriter_impl() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p8_hostile_paramswriter_impl.rs");
}

/// **P9** — `Box::leak(Box::new(PreparedQuery { ... }))` + field
/// mutate. Expected: `error[E0451]: field 'sql' of struct
/// 'PreparedQuery' is private` (struct literal step blocked before
/// Box::leak is reached). Memo §7 Probe P9.
#[test]
fn p9_box_leak_field_mutate() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p9_box_leak_field_mutate.rs");
}

/// **P10** — `core::mem::transmute` byte-array into `PreparedQuery`.
/// Expected: `error: usage of an 'unsafe' block`. Same OS-boundary
/// class as P4. Memo §7 Probe P10 + §12.
#[test]
fn p10_mem_transmute() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p10_mem_transmute.rs");
}

/// **P11** — read `Q.stmt_name` directly (collision-exploitation
/// requires harvesting the macro-baked statement name). Expected:
/// `error[E0616]: field 'stmt_name' of struct 'PreparedQuery' is
/// private`. The accessor `q.stmt_name()` exists for legitimate
/// diagnostic use; raw field access is barred. Memo §7 Probe P11.
#[test]
fn p11_stmt_name_hash_collision() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p11_stmt_name_hash_collision.rs");
}

/// **P12** — mutate baked `parse_template` bytes. Expected:
/// `error[E0616]: field 'parse_template' of struct 'PreparedQuery'
/// is private` (the visibility check fires; the unsafe block would
/// also be rejected by the file-scope forbid). Memo §7 Probe P12
/// + §12.
#[test]
fn p12_mutate_wire_template() {
    trybuild::TestCases::new()
        .compile_fail("tests/prepared_compile_fail/p12_mutate_wire_template.rs");
}
