//! Freshness: the catalog tracks the migration FILES, so adding, editing,
//! deleting, and renaming migrations — at the top level OR inside a
//! subdirectory — is reflected in the next replay. This is the property
//! that closes the stale-schema blind spot: a catalog can never disagree
//! with the migrations under version control.
//!
//! These tests drive the pure replay core (`catalog_from_dir`) over a
//! scratch directory, mutating the files between rebuilds and asserting
//! the catalog changes accordingly. The cargo-side recompile trigger
//! (`cargo:rerun-if-changed` for the directory and every nested one) is
//! what makes a real consumer re-run this replay on a file change; that
//! per-directory membership emission is covered by the crate's unit tests.

// Integration-test helper fns (the `TempDir` methods and the `has_*`
// helpers below) are NOT `#[test]` fns, so Cargo compiles them WITHOUT
// `cfg(test)` and the workspace `allow-{panic,expect}-in-tests` carve-out
// does not reach them. A panic / `expect` in this scratch-directory test
// harness IS the loud failure signal we want (a broken temp dir or an
// unexpected replay error must fail the test), the opposite of a silent
// production fallback, so the panic-class floor lints are allowed here.
#![allow(
    clippy::panic,
    clippy::expect_used,
    reason = "test-only failure signal in non-#[test] integration-test helpers, which the in-tests carve-out does not cover"
)]

use std::path::{Path, PathBuf};

use bsql_build::catalog_from_dir;

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
        path.push(format!("bsql_freshness_{tag}_{pid}_{nanos}"));
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }

    fn write(&self, rel: &str, contents: &str) {
        let full = self.path.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir");
        }
        std::fs::write(full, contents).expect("write migration");
    }

    fn remove(&self, rel: &str) {
        std::fs::remove_file(self.path.join(rel)).expect("remove migration");
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn has_column(dir: &Path, table: &str, column: &str) -> bool {
    let catalog = catalog_from_dir(dir).expect("replay catalog");
    catalog
        .tables
        .get(table)
        .is_some_and(|cols| cols.contains_key(column))
}

fn has_table(dir: &Path, table: &str) -> bool {
    let catalog = catalog_from_dir(dir).expect("replay catalog");
    catalog.tables.contains_key(table)
}

#[test]
fn adding_a_migration_adds_its_columns() {
    let tmp = TempDir::new("add");
    tmp.write("0001_init.sql", "CREATE TABLE users (id bigint primary key);");
    assert!(has_table(&tmp.path, "users"));
    assert!(!has_column(&tmp.path, "users", "email"), "no email yet");

    // ADD a migration: the new column appears on the next replay.
    tmp.write("0002_email.sql", "ALTER TABLE users ADD COLUMN email text;");
    assert!(has_column(&tmp.path, "users", "email"), "email picked up");
}

#[test]
fn editing_a_migration_changes_the_catalog() {
    let tmp = TempDir::new("edit");
    tmp.write("0001_init.sql", "CREATE TABLE t (a int);");
    assert!(has_column(&tmp.path, "t", "a"));
    assert!(!has_column(&tmp.path, "t", "b"));

    // EDIT the file's contents: the replay reflects the new column.
    tmp.write("0001_init.sql", "CREATE TABLE t (a int, b text);");
    assert!(has_column(&tmp.path, "t", "b"), "edit reflected");
}

#[test]
fn deleting_a_migration_removes_its_effect() {
    let tmp = TempDir::new("delete");
    tmp.write("0001_init.sql", "CREATE TABLE t (a int);");
    tmp.write("0002_add.sql", "ALTER TABLE t ADD COLUMN b text;");
    assert!(has_column(&tmp.path, "t", "b"));

    // DELETE the second migration: its column is gone on the next replay.
    tmp.remove("0002_add.sql");
    assert!(!has_column(&tmp.path, "t", "b"), "delete reflected");
    assert!(has_column(&tmp.path, "t", "a"), "first migration intact");
}

#[test]
fn subdirectory_migration_is_picked_up_and_removable() {
    let tmp = TempDir::new("subdir");
    tmp.write("0001_init.sql", "CREATE TABLE t (a int);");
    assert!(!has_table(&tmp.path, "audit"), "no audit table yet");

    // A migration nested in a subdirectory is picked up (recursion).
    tmp.write("regional/0002_audit.sql", "CREATE TABLE audit (id bigint);");
    assert!(has_table(&tmp.path, "audit"), "subdir migration picked up");

    // Removing it inside the subdirectory is reflected too (membership at
    // the nested level).
    tmp.remove("regional/0002_audit.sql");
    assert!(!has_table(&tmp.path, "audit"), "subdir removal reflected");
}

#[test]
fn renaming_a_table_rekeys_to_the_new_name() {
    let tmp = TempDir::new("rename");
    tmp.write(
        "0001_init.sql",
        "CREATE TABLE legacy (id bigint primary key, v text);",
    );
    assert!(has_table(&tmp.path, "legacy"));

    // RENAME TO: the old name stops resolving, the new name starts.
    tmp.write("0002_rename.sql", "ALTER TABLE legacy RENAME TO current;");
    assert!(!has_table(&tmp.path, "legacy"), "old name gone");
    assert!(has_column(&tmp.path, "current", "v"), "new name resolves");
}
