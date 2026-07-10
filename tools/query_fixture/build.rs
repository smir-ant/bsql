//! Fixture build script — the exact shape a real consumer uses.
//!
//! It hands the `migrations/` directory to `bsql-build`, which:
//!   * emits `cargo:rerun-if-changed` for the directory (membership: ADD
//!     / DELETE of a migration recompiles) and each file (content: EDIT
//!     recompiles),
//!   * replays the DDL into a column catalog,
//!   * writes the catalog to OUT_DIR and sets the `BSQL_SCHEMA_CATALOG`
//!     rustc-env channel the `query!` macro reads at expansion.
//!
//! Any error is propagated (fail the build) — never swallowed.

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit_catalog("migrations")?;
    // Bake the (separate, self-contained) runner migration set for the embedded
    // migration-runner witness. This re-runs the SAME destructive-ack gate on
    // `runner_migrations/` (its `0003_drop_scratch.sql` is acked — drop the
    // marker and this build fails), and sets the `BSQL_EMBEDDED_MIGRATIONS`
    // channel the `bsql::embed_migrations!()` macro reads.
    bsql_build::emit_migrations("runner_migrations")
}
