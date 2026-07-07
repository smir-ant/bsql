//! Probe build script — the exact shape a dual-backend consumer uses. One
//! `emit` call replays `migrations/` into the PostgreSQL catalog AND the
//! SQLite template DB (the build-dep's `sqlite` feature does both), so each
//! `query!` carrier is typed against the lattice and conformance-checked
//! against real SQLite — the precondition for a carrier to gain BOTH
//! `TypedQuery` and `SqliteTypedQuery`.

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")
}
