//! Build script for the example suite — the exact shape a real consumer uses.
//!
//! It does TWO things, both feeding channels the `bsql` macros read at compile
//! time:
//!
//! 1. Replays `migrations/` into the schema CATALOG the `query!` / `copy!` /
//!    `user_types!` macros type against, AND registers the external-type BRIDGES
//!    (`timestamptz` -> `chrono::DateTime<Utc>`, `uuid` -> `uuid::Uuid`) used by
//!    the `external_bridges` example. The converter free functions live in
//!    `src/lib.rs` and are named here by their `bsql_examples::…` path (they
//!    resolve from the bin crates, which depend on this package's library).
//!
//!    The terminal is `.emit_catalog()` — PostgreSQL-only, NO SQLite template —
//!    because the shared migration set contains PostgreSQL-only DDL (`CREATE
//!    TYPE` enum/composite/domain, `timestamptz`, `uuid`) that a SQLite template
//!    replay cannot model. This keeps the build-time SQLite conformance oracle
//!    disengaged even if the surrounding workspace build activates `bsql-build`'s
//!    `sqlite` feature via feature unification. (The SQLite RUNTIME typed
//!    flagship still works for the portable tables — it is orthogonal to the
//!    build-time oracle.)
//!
//! 2. Bakes the SAME migration set for `bsql::embed_migrations!()` via
//!    `emit_migrations`, so the `migrations` example can apply an EMBEDDED set
//!    (no filesystem at run time). This re-runs the destructive-acknowledgement
//!    + transaction-control gates on the set at build time.
//!
//! Any error is propagated (fail the build) — never swallowed.

fn main() -> Result<(), bsql_build::BuildError> {
    // (1) Catalog + external-type bridges. PostgreSQL-only terminal.
    bsql_build::Catalog::from_migrations("migrations")?
        .bridge(
            "timestamptz",
            "chrono::DateTime<chrono::Utc>",
            "bsql_examples::to_chrono",
        )
        .bridge("uuid", "uuid::Uuid", "bsql_examples::to_uuid")
        .emit_catalog()?;

    // (2) Bake the same set for `embed_migrations!()` (the runtime runner).
    bsql_build::emit_migrations("migrations")
}
