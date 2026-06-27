//! Fixture build script — the exact shape a real SQLite-targeting consumer
//! uses.
//!
//! It hands the `migrations/` directory to `bsql-build` TWICE:
//!   * `emit_catalog` replays the DDL into the PostgreSQL column catalog
//!     and sets `BSQL_SCHEMA_CATALOG` (the inference-lattice channel), and
//!   * `emit_sqlite_template` replays the SAME DDL into a fresh SQLite
//!     database in OUT_DIR with the real engine and sets
//!     `BSQL_SQLITE_TEMPLATE` (the conformance-cross-check channel).
//!
//! Both emit `cargo:rerun-if-changed` for the directory (membership) and
//! each file (content). Any error — including a migration form SQLite
//! cannot replay — is propagated (fail the build), never swallowed.

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit_catalog("migrations")?;
    bsql_build::emit_sqlite_template("migrations")
}
