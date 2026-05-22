//! `trybuild` golden harness for the `#[derive(Pristine)]` rejection
//! path on unsupported field types (Audit #100).
//!
//! `synthesise_check` in `crates/bsql-pg-proto-derive/src/lib.rs:326`
//! emits `syn::Error::new_spanned(...)` when a struct field's type is
//! not in the supported set (`Option<T>`, `bool`, integer types,
//! `PhantomData<T>`). The error MUST surface as a compile-time
//! diagnostic — runtime drift on this rejection path would let an
//! unsupported field (e.g., `String`, `Vec<u8>`) sneak past with a
//! silently-wrong `is_pristine` impl.
//!
//! This harness pins the compile-time diagnostic. Each probe attempts
//! to compile a self-contained `.rs` file and compares stderr against
//! the matching `.stderr` golden.
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test pristine_unsupported_compile_fail
//! ```

#![forbid(unsafe_code)]

/// **P-PRIS-1** — `#[derive(Pristine)]` on a struct with a `String`
/// field rejects at compile time. The expected error surfaces from
/// `synthesise_check`'s `Err(syn::Error::new_spanned(...))` arm.
#[test]
fn p_pris_1_string_field_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/pristine_unsupported_compile_fail/p_pris_1_string_field.rs");
}

/// **P-PRIS-2** — `#[derive(Pristine)]` on a struct with a `Vec<u8>`
/// field rejects at compile time. Coverage for the non-Option, non-
/// integer, non-bool, non-PhantomData container types.
#[test]
fn p_pris_2_vec_field_rejected() {
    trybuild::TestCases::new()
        .compile_fail("tests/pristine_unsupported_compile_fail/p_pris_2_vec_field.rs");
}
