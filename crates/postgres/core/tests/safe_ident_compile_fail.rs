//! Compile-fail goldens for the `SafeIdent` / `SafeTable` structural injection
//! guard — the compile-time half of "forgot to validate an identifier before
//! splicing it into SQL is impossible".
//!
//! - `splice_rejects_raw_str.rs` → E0308 — a splice site takes a `SafeIdent`,
//!   not a `&str`; a raw (unvalidated) identifier will not coerce, so the splice
//!   cannot be written without first validating.
//! - `safe_ident_no_fabrication.rs` / `safe_table_no_fabrication.rs` — the
//!   newtype's field is private, so it cannot be constructed bypassing its
//!   `validate` constructor (the SOLE, validating door).
//!
//! Together: the only value a splice site accepts is a `SafeIdent` / `SafeTable`,
//! and the only way to obtain one is the injection-safe validator — so the
//! guard is enforced by the type system, not by remembering to call it.
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-core \
//!     --test safe_ident_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// The three structural walls, batched into one trybuild invocation.
#[test]
fn safe_identifier_guard_is_structural() {
    let t = trybuild::TestCases::new();
    // A raw &str where a splice site requires the injection-safe newtype: E0308.
    t.compile_fail("tests/safe_ident_ui/splice_rejects_raw_str.rs");
    // The newtypes cannot be fabricated bypassing their `validate` constructor.
    t.compile_fail("tests/safe_ident_ui/safe_ident_no_fabrication.rs");
    t.compile_fail("tests/safe_ident_ui/safe_table_no_fabrication.rs");
}
