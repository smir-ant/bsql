//! Bench build script — replays `migrations/` into the `query!` schema catalog
//! so the PARITY runner's compile-checked typed carriers validate against the
//! original `bench_users` / `bench_orders` shape (the SAME wiring a real
//! consumer uses; see the umbrella crate docs). PostgreSQL-only (`emit_catalog`)
//! — the parity SQLite runner uses the dynamic verbs, so no SQLite template is
//! needed here. Any error fails the build (never swallowed).

fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit_catalog("migrations")
}
