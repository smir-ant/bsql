//! Fixture build script — the exact shape a real consumer uses.
//!
//! It hands the `migrations/` directory to `bsql-build`, which:
//!   * emits `cargo:rerun-if-changed` for the directory (membership: ADD
//!     / DELETE of a migration recompiles) and each file (content: EDIT
//!     recompiles),
//!   * replays the DDL into a column catalog,
//!   * writes the catalog to OUT_DIR and sets the `BSQL_SCHEMA_CATALOG`
//!     rustc-env channel the `schema_check!` macro reads at expansion.
//!
//! Any error is propagated (fail the build) — never swallowed.

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit_catalog("migrations")
}
