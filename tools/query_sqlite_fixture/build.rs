//! Fixture build script — the exact shape a real SQLite-targeting consumer
//! uses.
//!
//! A single `bsql_build::emit` call hands the `migrations/` directory to
//! `bsql-build`, which — because this crate enables `bsql-build`'s `sqlite`
//! feature in `[build-dependencies]` — does BOTH in one call:
//!   * replays the DDL into the PostgreSQL column catalog and sets
//!     `BSQL_SCHEMA_CATALOG` (the inference-lattice channel), and
//!   * replays the SAME DDL into a fresh SQLite database in OUT_DIR with the
//!     real engine, declares the SQLite target (`BSQL_SQLITE_TARGET`), and
//!     sets `BSQL_SQLITE_TEMPLATE` (the conformance-cross-check channel).
//!
//! Emitting the template is inseparable from enabling the SQLite target, so
//! there is no second build-script step to forget: the conformance oracle
//! cannot silently disengage. It emits `cargo:rerun-if-changed` for the
//! directory (membership) and each file (content). Any error — including a
//! migration form SQLite cannot replay — is propagated (fail the build).

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")
}
