#![forbid(unsafe_code)]
//! Regression tests for the [`SqliteError`] class predicates.
//!
//! SQLite reports errors as EXTENDED codes (`primary | (sub << 8)`), and the
//! driver stores the extended code. A class predicate must therefore match on
//! the PRIMARY code (the low byte) so every subtype counts — otherwise
//! `is_busy` is false in exactly the driver's own default WAL mode (which
//! yields `SQLITE_BUSY_SNAPSHOT = 517`, not bare `5`) and
//! `is_constraint_violation` misses every specific constraint (UNIQUE = 2067,
//! …). These tests provoke the real extended codes and assert the predicates
//! recognise them.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use bsql_sqlite::Connection;

/// Process-unique temp DB path that removes its file and the WAL/SHM sidecars on
/// drop (so a WAL-mode file DB leaves nothing behind, even on a panic).
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("bsql_err_predicates_{tag}_{}_{n}.db", std::process::id()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut p = self.path.clone().into_os_string();
            p.push(suffix);
            drop(std::fs::remove_file(PathBuf::from(p)));
        }
    }
}

#[test]
fn unique_violation_is_a_constraint_violation() {
    // A duplicate PRIMARY KEY yields an EXTENDED constraint code
    // (SQLITE_CONSTRAINT_PRIMARYKEY = 1555 / _UNIQUE = 2067), never bare 19.
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_sql("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT UNIQUE)").expect("create");
    conn.execute_sql("INSERT INTO t VALUES (1, 'alice')").expect("first insert");

    let err = conn.execute_sql("INSERT INTO t VALUES (1, 'bob')").expect_err("duplicate PK must fail");
    let code = err.code().expect("a SQLite error carries a code");
    assert_ne!(
        code, 19,
        "a real UNIQUE/PK violation is an EXTENDED constraint code, not bare 19 — \
         that extended code is exactly what the primary-code masking must recognise",
    );
    assert_eq!(code & 0xFF, 19, "extended constraint code masks to primary SQLITE_CONSTRAINT (19)");
    assert!(
        err.is_constraint_violation(),
        "is_constraint_violation must recognise the extended constraint code {code}",
    );
    assert!(!err.is_busy(), "a constraint violation is not a busy error");

    // A distinct constraint subtype (the second, UNIQUE, column) also classifies.
    let dup_name = conn
        .execute_sql("INSERT INTO t VALUES (2, 'alice')")
        .expect_err("duplicate name must violate the UNIQUE constraint");
    assert!(
        dup_name.is_constraint_violation(),
        "a UNIQUE-column violation is also a constraint violation",
    );
}

#[test]
fn wal_write_conflict_is_busy() {
    // The driver's default file-mode journaling is WAL. A read snapshot that a
    // concurrent commit invalidates yields SQLITE_BUSY_SNAPSHOT = 517 (an
    // EXTENDED busy code), not bare 5 — the exact case the old `== 5` missed.
    let db = TempDb::new("busy");
    let reader = Connection::open(&db.path).expect("open reader (WAL)");
    let writer = Connection::open(&db.path).expect("open writer (WAL)");

    reader.execute_sql("CREATE TABLE t(x INTEGER)").expect("create");
    reader.execute_sql("INSERT INTO t VALUES (0)").expect("seed");

    // Establish a read snapshot on `reader` (a deferred BEGIN takes the snapshot
    // at the first read).
    reader.execute_sql("BEGIN").expect("begin read txn");
    let seen = reader.query_sql("SELECT x FROM t").expect("read snapshot");
    assert_eq!(seen.len(), 1);

    // A concurrent autocommit write advances the WAL past `reader`'s snapshot.
    writer.execute_sql("INSERT INTO t VALUES (1)").expect("concurrent commit");

    // `reader` now tries to upgrade its stale snapshot to a write → BUSY_SNAPSHOT.
    let err = reader.execute_sql("INSERT INTO t VALUES (2)").expect_err("stale-snapshot write must fail");
    let code = err.code().expect("a SQLite busy error carries a code");
    assert_ne!(
        code, 5,
        "WAL yields an EXTENDED busy code (e.g. BUSY_SNAPSHOT 517), not bare 5 — \
         recognising that extended code is the whole fix",
    );
    assert_eq!(code & 0xFF, 5, "extended busy code masks to primary SQLITE_BUSY (5)");
    assert!(err.is_busy(), "is_busy must recognise the extended busy code {code}");
    assert!(!err.is_constraint_violation(), "a busy error is not a constraint violation");

    // Roll the reader back so its WAL read lock is released before the sidecar
    // files are removed on drop.
    reader.execute_sql("ROLLBACK").expect("rollback");
}
