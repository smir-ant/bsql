//! trybuild gate for the SQLite build-time validation backend.
//!
//! A conforming, SQLite-validated query compiles; an unknown table /
//! column, invalid SQL, a lattice/SQLite type-or-nullability disagreement,
//! and an unacknowledged full-scan-on-toggle each fail to compile with a
//! pinned diagnostic.
//!
//! trybuild compiles each `compile_fail/*.rs` as its own crate via a
//! spawned `cargo`. Those crates do NOT have this fixture's `build.rs`, so
//! they would not, on their own, see the `BSQL_SCHEMA_CATALOG` (lattice)
//! and `BSQL_SQLITE_TEMPLATE` (conformance) rustc-env channels. We forward
//! both: `env!(..)` resolves HERE at this test crate's own compile time
//! (the rustc-env our `build.rs` set applies to every rustc invocation of
//! this crate, including its test targets), and we re-export them into the
//! child compile's environment so the macro reaches the real catalog and
//! the real template database — proving each rejection is schema/SQLite-
//! driven, not a missing-artifact artifact.

#[test]
fn sqlite_conformance_gate() {
    // SAFETY: `set_var` is `unsafe` in edition 2024 because concurrent env
    // access is a data race. This single-test file runs serially (one test,
    // no threads spawned) and sets the vars once before any trybuild child
    // is spawned, so there is no concurrent reader. The values are this
    // crate's own catalog + template paths, baked in as rustc-env by our
    // `build.rs` and captured at compile time via `env!`.
    unsafe {
        std::env::set_var("BSQL_SCHEMA_CATALOG", env!("BSQL_SCHEMA_CATALOG"));
        std::env::set_var("BSQL_SQLITE_TEMPLATE", env!("BSQL_SQLITE_TEMPLATE"));
    }

    let t = trybuild::TestCases::new();

    // A conforming, SQLite-validated query (and an acknowledged toggle)
    // type-checks at macro expansion.
    t.pass("tests/compile_pass/sqlite_ok.rs");

    // Caught by the shared inference lattice (which the SQLite path conforms
    // to): an unknown table, an unknown column, and invalid SQL.
    t.compile_fail("tests/compile_fail/unknown_table.rs");
    t.compile_fail("tests/compile_fail/unknown_column.rs");
    t.compile_fail("tests/compile_fail/invalid_sql.rs");

    // Caught by the SQLite conformance cross-check specifically:
    //   * a column whose lattice type (`u32`, PostgreSQL `oid`) has no
    //     SQLite equivalent — a type disagreement, and
    //   * a dynamic OPTIONAL(...) toggle whose enabled form forces a full
    //     table scan and is NOT acknowledged.
    t.compile_fail("tests/compile_fail/type_disagreement.rs");
    t.compile_fail("tests/compile_fail/scan_on_toggle.rs");

    // RECORDED DECISION — there is deliberately NO nullability-disagreement
    // compile-fail fixture here (only the type-disagreement one above). A
    // TYPE fork arises organically: PostgreSQL `oid` types as `u32` in the
    // lattice while SQLite has no equivalent, so a real migration projecting
    // it is a genuine disagreement. A NULLABILITY disagreement cannot arise
    // organically: the catalog the lattice types against and the SQLite
    // template are both replayed from the SAME migration set, so for a
    // genuine base-column reference (the only column the nullability check
    // applies to) the lattice's nullability and SQLite's base nullability
    // always AGREE — the dangerous direction the check guards is unreachable
    // through a real `query!`. The loud-rejection behaviour is covered by the
    // `bsql_build` unit test `nullability_disagreement_is_loud`, which
    // constructs the disagreeing pair directly. See the module docs on
    // `crates/build/src/sqlite.rs` for the full reasoning.
}
