//! # migrations — apply a versioned schema to a live database
//!
//! The runtime migration RUNNER: the bridge between build-time schema validation
//! and a live database. It applies your migration set EXACTLY ONCE, in
//! deterministic order, recording each in a `_bsql_migrations` ledger, and
//! detects DRIFT (an edited/reordered/deleted applied migration is a classified
//! error, never silently re-run). It serializes concurrent deployers on an
//! advisory lock. This example shows both the EMBEDDED source (baked into the
//! binary — no filesystem at run time) and the DIRECTORY source, plus
//! `migration_status` and `dry_run_migrations`.
//!
//! Features/verbs: `embed_migrations!()`, `MigrationSource::{embedded, directory}`,
//! `conn.{migration_status, dry_run_migrations, run_migrations}`.
//!
//! Backend: PostgreSQL — needs a live server.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin migrations
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::pg::{ConnectConfig, Connection, MigrationSource};

// The migration set baked into this binary at build time (`build.rs` calls
// `bsql_build::emit_migrations`). It carries the SAME files as `migrations/`.
const EMBEDDED: &[(&str, &str)] = bsql_examples::EMBEDDED_MIGRATIONS;

// The directory source: an ABSOLUTE path baked at COMPILE time so it resolves no
// matter the working directory. The ops-friendly alternative to embedding.
const MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;

    // BEFORE: what is applied vs pending? (A read-only snapshot — no lock, no
    // apply.) On a fresh database everything is pending; after a prior run
    // everything is applied.
    let status = conn.migration_status(MigrationSource::embedded(EMBEDDED)).await?;
    println!(
        "status: {} applied, {} pending",
        status.applied.len(),
        status.pending.len()
    );
    for name in &status.pending {
        println!("  pending: {name}");
    }

    // DRY RUN: which migrations WOULD apply (running the same drift checks as the
    // real run), without applying anything.
    let would_apply = conn.dry_run_migrations(MigrationSource::embedded(EMBEDDED)).await?;
    println!("dry run would apply {} migration(s)", would_apply.len());

    // APPLY the embedded set. The report names what THIS run applied (empty when
    // already up to date) and how many were already applied.
    let report = conn.run_migrations(MigrationSource::embedded(EMBEDDED)).await?;
    if report.applied.is_empty() {
        println!("already up to date ({} previously applied)", report.already_applied);
    } else {
        println!("applied {} migration(s):", report.applied.len());
        for name in &report.applied {
            println!("  + {name}");
        }
    }

    // IDEMPOTENT: applying the DIRECTORY source (the SAME files) is now a no-op —
    // the ledger already records every migration, and the checksums match, so
    // embedded and directory are interchangeable views of one set.
    let again = conn.run_migrations(MigrationSource::directory(MIGRATIONS_DIR)).await?;
    println!(
        "re-run from directory applied {} more (idempotent)",
        again.applied.len()
    );

    conn.close().await?;
    Ok(())
}
