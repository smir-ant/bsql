#![forbid(unsafe_code)]
// The `tempdir` fixture helper below is not a `#[test]` fn, so the floor's
// `allow-expect-in-tests` carve-out (keyed on `#[test]` context) does not reach
// its `expect`s; they panic loudly on a broken test fixture (the intended
// signal), never a production data fallback.
#![allow(
    clippy::expect_used,
    clippy::unwrap_in_result,
    reason = "test fixture helper — its `expect`s panic loudly on setup failure; not a `#[test]` fn so the in-tests carve-out cannot reach it, and there is no production fallback"
)]
//! In-process witnesses for the SQLite migration RUNNER (no PostgreSQL needed).
//!
//! Proves, over the embedded backend: fresh-DB apply in order, idempotent
//! re-run (exactly-once), checksum-drift classified, a failing migration rolls
//! back + stops + names itself, status / dry-run, a directory source, the
//! `-- bsql:no-transaction` path, and cross-process concurrency (two runners
//! over one file — one applies, the other no-ops) — the SQLite half of the
//! cross-backend runner witness.

use std::sync::{Arc, Barrier};

use bsql_sqlite::{Connection, DriftKind, MigrationError, MigrationSource, MigrationSourceError};

/// Three well-formed migrations, in order.
const M1: (&str, &str) = ("0001_users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
const M2: (&str, &str) = ("0002_email.sql", "ALTER TABLE users ADD COLUMN email TEXT");
const M3: (&str, &str) = ("0003_posts.sql", "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER)");

fn three() -> [(&'static str, &'static str); 3] {
    [M1, M2, M3]
}

/// WITNESS: a fresh database applies all three migrations, in order, and the
/// ledger records them.
#[test]
fn fresh_database_applies_all_in_order() {
    let conn = Connection::open_in_memory().expect("open");
    let set = three();

    let report = conn.run_migrations(MigrationSource::embedded(&set)).expect("run");
    assert_eq!(report.applied, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);
    assert_eq!(report.already_applied, 0);
    assert!(report.applied_any());

    // The ledger records all three, in apply order.
    let status = conn.migration_status(MigrationSource::embedded(&set)).expect("status");
    let names: Vec<&str> = status.applied.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);
    assert!(status.pending.is_empty());

    // The migrations really ran: the tables exist.
    conn.execute_raw("INSERT INTO users (id, name, email) VALUES (1, 'a', 'a@x')").expect("users exists");
    conn.execute_raw("INSERT INTO posts (id, user_id) VALUES (1, 1)").expect("posts exists");
}

/// WITNESS: re-running is a no-op — each migration runs EXACTLY once.
#[test]
fn rerun_is_idempotent() {
    let conn = Connection::open_in_memory().expect("open");
    let set = three();
    conn.run_migrations(MigrationSource::embedded(&set)).expect("first run");

    let report = conn.run_migrations(MigrationSource::embedded(&set)).expect("second run");
    assert!(report.applied.is_empty(), "nothing re-applied");
    assert_eq!(report.already_applied, 3);
    assert!(!report.applied_any());
}

/// WITNESS: adding a fourth migration applies ONLY the new one.
#[test]
fn adding_a_migration_applies_only_the_new_one() {
    let conn = Connection::open_in_memory().expect("open");
    conn.run_migrations(MigrationSource::embedded(&three())).expect("first three");

    let four = [M1, M2, M3, ("0004_idx.sql", "CREATE INDEX i ON posts (user_id)")];
    let report = conn.run_migrations(MigrationSource::embedded(&four)).expect("run four");
    assert_eq!(report.applied, vec!["0004_idx.sql"]);
    assert_eq!(report.already_applied, 3);
}

/// WITNESS: editing an already-applied migration is a classified
/// checksum-drift error — never silently re-run or ignored.
#[test]
fn editing_an_applied_migration_is_checksum_drift() {
    let conn = Connection::open_in_memory().expect("open");
    conn.run_migrations(MigrationSource::embedded(&three())).expect("run");

    // 0002 was applied; now its content changes.
    let edited = [M1, ("0002_email.sql", "ALTER TABLE users ADD COLUMN email VARCHAR(50)"), M3];
    let err = conn
        .run_migrations(MigrationSource::embedded(&edited))
        .expect_err("edited applied migration must be drift");
    match err {
        MigrationError::Drift { migration, kind } => {
            assert_eq!(migration, "0002_email.sql");
            assert!(matches!(kind, DriftKind::ChecksumMismatch { .. }));
        }
        other => panic!("expected Drift, got {other:?}"),
    }

    // A dry run surfaces the SAME drift, still without touching anything.
    assert!(matches!(
        conn.dry_run_migrations(MigrationSource::embedded(&edited)),
        Err(MigrationError::Drift { .. })
    ));
}

/// WITNESS: deleting an applied migration from the source is classified drift.
#[test]
fn deleting_an_applied_migration_is_drift() {
    let conn = Connection::open_in_memory().expect("open");
    conn.run_migrations(MigrationSource::embedded(&three())).expect("run");

    // Dropping the FIRST migration while a LATER one (0002/0003) survives is a
    // MIDDLE gap: an unambiguous deletion (`source_is_strict_prefix: false`).
    let without_first = [M2, M3];
    let err = conn
        .run_migrations(MigrationSource::embedded(&without_first))
        .expect_err("deleted applied migration must be drift");
    assert!(matches!(
        err,
        MigrationError::Drift {
            migration,
            kind: DriftKind::MissingFromSource { source_is_strict_prefix: false }
        } if migration == "0001_users.sql"
    ));

    // Dropping the LAST migration (source is a strict prefix of the applied set)
    // is a TAIL extra: EITHER a tail deletion OR an older instance against a newer
    // DB (`source_is_strict_prefix: true`) — the driver surfaces both cases.
    let without_last = [M1, M2];
    let err = conn
        .run_migrations(MigrationSource::embedded(&without_last))
        .expect_err("a strict-prefix source is drift");
    assert!(matches!(
        err,
        MigrationError::Drift {
            migration,
            kind: DriftKind::MissingFromSource { source_is_strict_prefix: true }
        } if migration == "0003_posts.sql"
    ));
}

/// WITNESS: a migration that FAILS mid-way rolls back its transaction, the
/// runner STOPS (later migrations do not run), and the error NAMES the failed
/// migration.
#[test]
fn a_failing_migration_rolls_back_and_stops() {
    let conn = Connection::open_in_memory().expect("open");
    // 0002 is broken SQL; 0003 must NOT run.
    let broken = [
        M1,
        ("0002_broken.sql", "CREATE TABLE this is not valid sql"),
        ("0003_after.sql", "CREATE TABLE after_marker (id INTEGER)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&broken))
        .expect_err("broken migration must fail");
    match err {
        MigrationError::MigrationFailed { migration, .. } => assert_eq!(migration, "0002_broken.sql"),
        other => panic!("expected MigrationFailed naming 0002, got {other:?}"),
    }

    // 0001 committed (it is a separate transaction), 0003 never ran, and the
    // failed 0002 left nothing behind (its transaction rolled back). The ledger
    // shows exactly 0001.
    let status = conn.migration_status(MigrationSource::embedded(&broken)).expect("status");
    let applied: Vec<&str> = status.applied.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(applied, vec!["0001_users.sql"]);
    // 0003's table must NOT exist — the runner stopped at 0002.
    let after = conn.query_raw("SELECT 1 FROM sqlite_master WHERE type='table' AND name='after_marker'").expect("q");
    assert!(after.is_empty(), "0003 must not have run after 0002 failed");
    // The connection is still usable (0002 rolled back cleanly).
    conn.execute_raw("INSERT INTO users (id, name) VALUES (7, 'g')").expect("connection reusable");
}

/// WITNESS (runtime backstop): a migration containing a top-level `COMMIT`
/// commits the runner's `BEGIN IMMEDIATE` transaction mid-way — a broken
/// per-migration boundary. `sqlite3_get_autocommit` catches it as the classified
/// `TransactionBoundaryBroken` (naming the migration), never a silent piecemeal
/// apply, and never the confusing "cannot commit - no transaction is active" the
/// runner's own trailing COMMIT would otherwise raise. The runner STOPS: a later
/// migration does not run. The ledger reflects the HONEST state — the boundary
/// break already committed its statements + its own ledger row (fail-loud AFTER
/// the fact, the documented trade-off for what the build-time gate cannot see in
/// a directory source or a `DO`/procedure body).
#[test]
fn a_top_level_commit_in_a_migration_is_a_boundary_broken_error() {
    let conn = Connection::open_in_memory().expect("open");
    let set = [
        M1,
        ("0002_commit.sql", "CREATE TABLE mid (id INTEGER);\nCOMMIT;"),
        ("0003_after.sql", "CREATE TABLE after_marker (id INTEGER)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&set))
        .expect_err("a top-level COMMIT must be a boundary-broken error");
    match err {
        MigrationError::TransactionBoundaryBroken { migration } => {
            assert_eq!(migration, "0002_commit.sql");
        }
        other => panic!("expected TransactionBoundaryBroken naming 0002, got {other:?}"),
    }

    // 0003 never ran — the runner STOPPED at the boundary break.
    let after = conn
        .query_raw("SELECT 1 FROM sqlite_master WHERE type='table' AND name='after_marker'")
        .expect("q");
    assert!(after.is_empty(), "0003 must not run after the boundary break");

    // The HONEST ledger state: 0001 (its own transaction) AND 0002 (whose stray
    // COMMIT committed its DDL + ledger row before the check caught it) are both
    // recorded — fail-loud AFTER the boundary already broke, not refuse-before.
    let status = conn.migration_status(MigrationSource::embedded(&set)).expect("status");
    let applied: Vec<&str> = status.applied.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(applied, vec!["0001_users.sql", "0002_commit.sql"]);

    // The connection is still usable — autocommit means no stray transaction to
    // clean up, so no rollback was needed.
    conn.execute_raw("INSERT INTO users (id, name) VALUES (9, 'z')").expect("connection reusable");
}

/// WITNESS (runtime backstop, no-transaction path): a `-- bsql:no-transaction`
/// migration runs as autocommit statements, so one that opens a `BEGIN` and
/// leaves it open breaks the boundary (its ledger insert lands inside the stray,
/// uncommitted transaction). `sqlite3_get_autocommit` catches it as
/// `TransactionBoundaryBroken`; the stray transaction is rolled back so the
/// connection stays reusable.
#[test]
fn a_no_transaction_migration_that_opens_a_transaction_is_boundary_broken() {
    let conn = Connection::open_in_memory().expect("open");
    let set = [(
        "0001_open.sql",
        "-- bsql:no-transaction\nCREATE TABLE opened (id INTEGER);\nBEGIN;",
    )];
    let err = conn
        .run_migrations(MigrationSource::embedded(&set))
        .expect_err("a no-transaction BEGIN must be boundary-broken");
    match err {
        MigrationError::TransactionBoundaryBroken { migration } => {
            assert_eq!(migration, "0001_open.sql");
        }
        other => panic!("expected TransactionBoundaryBroken naming 0001, got {other:?}"),
    }

    // The stray transaction was rolled back (the ledger insert with it), so the
    // connection is reusable — a fresh write succeeds, not "database is locked".
    conn.execute_raw("CREATE TABLE probe (id INTEGER)")
        .expect("connection reusable after the stray transaction is rolled back");
}

/// WITNESS: `migration_status` on a fresh database shows everything pending;
/// `dry_run` reports what WOULD run without applying it.
#[test]
fn status_and_dry_run_on_a_fresh_database() {
    let conn = Connection::open_in_memory().expect("open");
    let set = three();

    let status = conn.migration_status(MigrationSource::embedded(&set)).expect("status");
    assert!(status.applied.is_empty());
    assert_eq!(status.pending, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);

    let would = conn.dry_run_migrations(MigrationSource::embedded(&set)).expect("dry run");
    assert_eq!(would, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);

    // Dry run applied NOTHING — the ledger table still does not exist.
    let still = conn.migration_status(MigrationSource::embedded(&set)).expect("status again");
    assert!(still.applied.is_empty(), "dry run applied nothing");
}

/// WITNESS: a DIRECTORY source walks + applies the same way an embedded slice
/// does (the ops-friendly path), in the same order.
#[test]
fn directory_source_applies_in_order() {
    let dir = tempdir("mig-dir");
    std::fs::write(dir.join("0002_b.sql"), "CREATE TABLE b (x INTEGER)").expect("w");
    std::fs::write(dir.join("0001_a.sql"), "CREATE TABLE a (x INTEGER)").expect("w");
    std::fs::write(dir.join("ignore.txt"), "not a migration").expect("w");

    let conn = Connection::open_in_memory().expect("open");
    let report = conn.run_migrations(MigrationSource::directory(&dir)).expect("run dir");
    // Applied in lexicographic name order regardless of write order.
    assert_eq!(report.applied, vec!["0001_a.sql", "0002_b.sql"]);
    conn.execute_raw("INSERT INTO a (x) VALUES (1)").expect("a exists");
    conn.execute_raw("INSERT INTO b (x) VALUES (1)").expect("b exists");
}

/// WITNESS (ordering authority): a nested prefix collision (`a.sql` + `a/b.sql`)
/// applies in the SAME order the build validates — the relative-name STRING
/// order, where `.` (0x2E) < `/` (0x2F), so `a.sql` precedes `a/b.sql`. (A raw
/// `PathBuf` sort would flip these; the build now shares the runner's string
/// key, so build-validated order == apply order.)
#[test]
fn nested_directory_applies_in_the_build_shared_order() {
    let dir = tempdir("mig-nested");
    std::fs::create_dir_all(dir.join("a")).expect("subdir");
    std::fs::write(dir.join("a.sql"), "CREATE TABLE a_top (x INTEGER)").expect("w");
    std::fs::write(dir.join("a").join("b.sql"), "CREATE TABLE a_b (x INTEGER)").expect("w");

    let conn = Connection::open_in_memory().expect("open");
    let report = conn.run_migrations(MigrationSource::directory(&dir)).expect("run");
    assert_eq!(report.applied, vec!["a.sql", "a/b.sql"]);
}

/// WITNESS: a `-- bsql:no-transaction` migration applies (outside a
/// transaction) and is recorded exactly once.
#[test]
fn non_transactional_migration_applies_and_records() {
    let conn = Connection::open_in_memory().expect("open");
    let set = [
        ("0001_t.sql", "CREATE TABLE t (v INTEGER)"),
        ("0002_vac.sql", "-- bsql:no-transaction\nVACUUM"),
    ];
    let report = conn.run_migrations(MigrationSource::embedded(&set)).expect("run");
    assert_eq!(report.applied, vec!["0001_t.sql", "0002_vac.sql"]);
    // Re-run: no-op (recorded once).
    let again = conn.run_migrations(MigrationSource::embedded(&set)).expect("rerun");
    assert!(again.applied.is_empty());
}

/// WITNESS (concurrency): two runners over the SAME database FILE apply the set
/// exactly once between them — no double-apply. `BEGIN IMMEDIATE` + the in-txn
/// ledger re-check serialize them. Also exercises the fresh-file concurrent-OPEN
/// path: both `Connection::open` calls race on the one-time `journal_mode=WAL`
/// switch, and SQLite bypasses the busy handler for that shared-lock-upgrade
/// contention, so `open` retries the switch (see `connection.rs::open` /
/// `enable_wal_with_retry`) — otherwise the losing open fails with
/// `database is locked` before either runner even begins.
#[test]
fn two_concurrent_runners_apply_exactly_once() {
    let dir = tempdir("mig-concurrent");
    let db_path = dir.join("app.db");
    // A migration set with several steps so the two runners genuinely race.
    let set: [(&'static str, &'static str); 4] = [
        ("0001.sql", "CREATE TABLE a (id INTEGER PRIMARY KEY)"),
        ("0002.sql", "CREATE TABLE b (id INTEGER PRIMARY KEY)"),
        ("0003.sql", "CREATE TABLE c (id INTEGER PRIMARY KEY)"),
        ("0004.sql", "CREATE TABLE d (id INTEGER PRIMARY KEY)"),
    ];

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2u8 {
        let path = db_path.clone();
        let gate = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let conn = Connection::open(&path).expect("open file");
            gate.wait(); // both start together
            conn.run_migrations(MigrationSource::embedded(&set))
        }));
    }
    let mut total_applied = 0usize;
    for h in handles {
        let report = h.join().expect("thread joined").expect("runner ok");
        total_applied += report.applied.len();
    }
    // Between the two runners, each of the four migrations ran EXACTLY once.
    assert_eq!(total_applied, 4, "no double-apply, no lost migration");

    // The final database has all four tables and a ledger of exactly four rows.
    let conn = Connection::open(&db_path).expect("reopen");
    let status = conn.migration_status(MigrationSource::embedded(&set)).expect("status");
    assert_eq!(status.applied.len(), 4);
    assert!(status.pending.is_empty());
    for t in ["a", "b", "c", "d"] {
        let sql = format!("SELECT 1 FROM sqlite_master WHERE type='table' AND name='{t}'");
        assert!(!conn.query_raw(&sql).expect("q").is_empty(), "table {t} exists");
    }
}

/// WITNESS (parity, no silent skip): a hand-built slice with two same-named
/// migrations is a LOUD classified error before any apply — matching the
/// PostgreSQL runner (which fails on the ledger PK), never silently applying only
/// the first.
#[test]
fn duplicate_named_migration_is_loud_not_a_silent_skip() {
    let conn = Connection::open_in_memory().expect("open");
    let dup = [
        ("0001.sql", "CREATE TABLE a (x INTEGER)"),
        ("0001.sql", "CREATE TABLE b (x INTEGER)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&dup))
        .expect_err("a duplicate name must be loud");
    assert!(matches!(
        err,
        MigrationError::Source(MigrationSourceError::DuplicateName { name }) if name == "0001.sql"
    ));
    // Nothing was applied (the check is pre-flight).
    let a = conn.query_raw("SELECT 1 FROM sqlite_master WHERE type='table' AND name='a'").expect("q");
    assert!(a.is_empty(), "no migration applied on a duplicate-name error");
}

// ── minimal temp-dir helper (no external crate) ───────────────────────────────

/// Create a unique temp directory under the OS temp dir, removed on drop.
///
/// The name's COLLISION-PROOF component is a process-global monotonic counter:
/// `pid` + a nanosecond clock are not enough, because two parallel test threads
/// (same pid) can call `tempdir` within one clock tick on a coarse-resolution
/// platform (macOS), collide on the same directory, and cross-contaminate — the
/// source of the flaky `a_failing_migration_rolls_back_and_stops` failure under
/// `cargo test`'s default thread parallelism. The `AtomicU64` guarantees a
/// distinct name per call regardless of clock resolution or same-tick races; the
/// pid + nanos stay for human-readable disambiguation across runs.
fn tempdir(tag: &str) -> TempPath {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);

    let mut base = std::env::temp_dir();
    let unique = format!(
        "bsql_{tag}_{}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    );
    base.push(unique);
    std::fs::create_dir_all(&base).expect("mkdir temp");
    TempPath { path: base }
}

/// An owned temp path removed (recursively) on drop.
struct TempPath {
    path: std::path::PathBuf,
}

impl std::ops::Deref for TempPath {
    type Target = std::path::Path;
    fn deref(&self) -> &std::path::Path {
        &self.path
    }
}

impl AsRef<std::path::Path> for TempPath {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        // Best-effort cleanup; a leftover temp dir is harmless.
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) | Err(_) => {}
        }
    }
}
