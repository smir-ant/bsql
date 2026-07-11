//! Build-time robustness: a consumer's build (this crate runs at THEIR build
//! time on THEIR migration SQL) must never be CRASHED or HUNG by a pathological
//! migration file. A build-time DoS — an uncatchable abort or an infinite hang
//! with no diagnostic — is a real DX defect: the consumer gets no `BuildError`,
//! no file name, just a cryptic dead build.
//!
//! Two hazards are pinned here, each exercised through the REAL public entry
//! point (`catalog_from_dir`, the pure replay core `emit_catalog` wraps), over a
//! scratch directory — the same walk + parse + replay path a real `build.rs`
//! drives:
//!
//!  * A pathologically deep SQL nesting (thousands of parens / prefix operators
//!    / sub-queries) used to overflow the parser's native stack and ABORT the
//!    build (SIGABRT, exit 134) — uncatchable by `catch_unwind`. It must now
//!    fail CLEANLY as a classified `BuildError::Parse` naming the file and the
//!    recursion limit. (If the fix regressed, the deep-nesting cases below would
//!    abort THIS test binary — a red gate, not a silent pass.)
//!  * A FIFO / socket / device named `*.sql` used to HANG the build forever at
//!    `read_to_string`. It must now be a classified `BuildError::NonRegularFile`
//!    naming the path — and a legitimate symlink to a real `.sql` must still be
//!    followed and applied.

// The `TempDir` helper below is NOT a `#[test]` fn, so the workspace
// `allow-{panic,expect}-in-tests` carve-out does not reach it. A panic /
// `expect` in this scratch-directory harness IS the loud failure signal we want
// (a broken temp dir or an unexpected error must fail the test), the opposite of
// a silent production fallback, so the panic-class floor lints are allowed here.
#![allow(
    clippy::panic,
    clippy::expect_used,
    reason = "test-only failure signal in non-#[test] integration-test helpers, which the in-tests carve-out does not cover"
)]

use std::path::PathBuf;

use bsql_build::{BuildError, catalog_from_dir};

/// A unique scratch directory under the system temp dir, removed on drop.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        let pid = std::process::id();
        let nanos = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => d.as_nanos(),
            Err(e) => panic!("system clock before unix epoch: {e}"),
        };
        path.push(format!("bsql_robustness_{tag}_{pid}_{nanos}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }

    fn write(&self, rel: &str, contents: &str) {
        std::fs::write(self.path.join(rel), contents).expect("write migration");
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// A `CREATE TABLE` whose `CHECK` constraint nests `depth` parentheses around a
/// trivial predicate. Each level is one `parse_subexpr` recursion — the parser's
/// counter-guarded expression production.
fn nested_parens_migration(depth: usize) -> String {
    let open = "(".repeat(depth);
    let close = ")".repeat(depth);
    format!("CREATE TABLE t (id int, CHECK ({open}id > 0{close}));")
}

// ── deeply-nested SQL is a classified parse error, not a build-aborting SIGABRT ──

#[test]
fn deeply_nested_parens_is_classified_not_abort() {
    let tmp = TempDir::new("deep_parens");
    // 10 000 deep: the exact repro that ABORTED the build (exit 134) before the
    // fix. If this regressed, THIS test binary aborts (a red gate).
    tmp.write("0001.sql", &nested_parens_migration(10_000));

    match catalog_from_dir(&tmp.path) {
        Err(BuildError::Parse { path, message }) => {
            assert_eq!(path, tmp.path.join("0001.sql"), "names the offending file");
            assert!(
                message.to_lowercase().contains("recursion"),
                "classified as a recursion-limit rejection, got: {message}"
            );
        }
        other => panic!("expected a classified BuildError::Parse (recursion), got: {other:?}"),
    }
}

#[test]
fn deeply_nested_prefix_not_is_classified_not_abort() {
    // A deep prefix-`NOT` chain has ZERO parentheses, so a hand-rolled
    // paren-depth counter would MISS it and still abort — this pins that the
    // library's own recursion counter (which guards `parse_subexpr`, the prefix
    // production) catches it too.
    let tmp = TempDir::new("deep_not");
    let nots = "NOT ".repeat(10_000);
    tmp.write("0001.sql", &format!("CREATE TABLE t (id int, CHECK ({nots}id > 0));"));

    // The recursion counter bounds the prefix chain (a limit-sweep confirms the
    // stop depth scales with the limit, i.e. it is the counter, not a coincidence
    // of precedence). sqlparser surfaces a mid-prefix-chain counter fire as a
    // POSITIONAL "Expected )" rather than the "recursion limit exceeded" string
    // the paren/sub-query paths report — an internal message detail. The invariant
    // that matters is the same: a CLASSIFIED `BuildError::Parse` naming the file,
    // never the uncatchable stack-overflow SIGABRT (which, without the fix, aborts
    // THIS binary at depth ~512 even on the main thread — the probe confirms it).
    match catalog_from_dir(&tmp.path) {
        Err(BuildError::Parse { path, .. }) => {
            assert_eq!(path, tmp.path.join("0001.sql"), "names the offending file");
        }
        other => panic!("expected a classified BuildError::Parse, got: {other:?}"),
    }
}

#[test]
fn deeply_nested_subquery_is_classified_not_abort() {
    // Deeply nested derived tables exercise `parse_query` / `parse_table_factor`
    // recursion (a different production than the expression parens above), so
    // this pins that the query/table-factor path is bounded too.
    let tmp = TempDir::new("deep_subquery");
    let mut inner = String::from("t");
    for _ in 0..5_000 {
        inner = format!("(SELECT * FROM {inner}) s");
    }
    tmp.write("0001.sql", &format!("CREATE VIEW v AS SELECT * FROM {inner};"));

    match catalog_from_dir(&tmp.path) {
        Err(BuildError::Parse { message, .. }) => assert!(
            message.to_lowercase().contains("recursion"),
            "classified as recursion, got: {message}"
        ),
        other => panic!("expected BuildError::Parse (recursion), got: {other:?}"),
    }
}

#[test]
fn modestly_deep_migration_still_parses() {
    // 100 levels deep — far above `sqlparser`'s DEFAULT recursion limit of 50,
    // so this passing proves the raised bound is in effect (a default-50 build
    // would reject it), while still being absurdly deeper than any real
    // migration, so it never false-rejects authored schema.
    let tmp = TempDir::new("modest");
    tmp.write("0001.sql", &nested_parens_migration(100));

    let catalog = catalog_from_dir(&tmp.path)
        .expect("a 100-deep CHECK is well within the bound and replays cleanly");
    assert!(catalog.tables.contains_key("t"), "the table is catalogued");
}

// ── a non-regular `*.sql` file is a classified error, not an infinite build hang ──

#[cfg(unix)]
#[test]
fn fifo_named_sql_is_classified_not_hang() {
    use std::sync::mpsc;
    use std::time::Duration;

    let tmp = TempDir::new("fifo");
    let fifo = tmp.path.join("0001.sql");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("spawn mkfifo");
    assert!(status.success(), "mkfifo created the FIFO");

    // Run the walk on a worker thread and bound the wait: before the fix, the
    // walk admitted the FIFO and blocked FOREVER at `read_to_string`; a timeout
    // here turns any such regression into a clean test FAILURE instead of
    // hanging the suite. With the fix, `fs::metadata` (a non-blocking `stat`)
    // classifies the FIFO and the call returns instantly.
    let dir = tmp.path.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(catalog_from_dir(&dir));
    });

    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Err(BuildError::NonRegularFile { path })) => {
            assert_eq!(path, fifo, "names the FIFO");
        }
        Ok(other) => panic!("expected BuildError::NonRegularFile, got: {other:?}"),
        Err(_) => panic!("catalog_from_dir hung on a FIFO named *.sql (a non-regular file must not block the walk)"),
    }
}

#[cfg(unix)]
#[test]
fn symlink_to_real_sql_is_followed() {
    // The real migration lives OUTSIDE the migrations directory so it is not
    // itself walked; only the SYMLINK to it sits inside, named `0001.sql`. The
    // regular-file admission uses `fs::metadata`, which FOLLOWS symlinks, so the
    // symlink must resolve to its regular target and replay normally.
    let outer = TempDir::new("symlink_outer");
    let target = outer.path.join("real_source.sql");
    std::fs::write(&target, "CREATE TABLE s (id int);").expect("write real target");

    let migrations = TempDir::new("symlink_dir");
    std::os::unix::fs::symlink(&target, migrations.path.join("0001.sql")).expect("symlink");

    let catalog =
        catalog_from_dir(&migrations.path).expect("a symlink to a real .sql is followed + replayed");
    assert!(
        catalog.tables.contains_key("s"),
        "the symlinked migration's table is catalogued"
    );
}

#[cfg(unix)]
#[test]
fn dangling_symlink_sql_is_classified_read_error() {
    // A `*.sql` symlink whose target does not exist: `fs::metadata` FOLLOWS the
    // link and the `stat` fails (ENOENT), which the admission maps to the
    // classified `BuildError::ReadFile` naming the entry — never a panic, and
    // never a silent skip that would drop a migration the author believes exists.
    let tmp = TempDir::new("dangling");
    let link = tmp.path.join("0001.sql");
    std::os::unix::fs::symlink(tmp.path.join("does_not_exist.sql"), &link).expect("symlink");

    match catalog_from_dir(&tmp.path) {
        Err(BuildError::ReadFile { path, .. }) => {
            assert_eq!(path, link, "names the dangling symlink");
        }
        other => panic!("expected a classified BuildError::ReadFile, got: {other:?}"),
    }
}
