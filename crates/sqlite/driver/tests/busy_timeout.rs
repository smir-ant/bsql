#![forbid(unsafe_code)]
//! Witness for the busy-timeout policy.
//!
//! A locked database no longer surfaces a raw `SQLITE_BUSY` the instant it hits
//! a held lock: `open` sets a default busy timeout so a briefly-contended write
//! WAITS (bounded) for the lock. When the wait expires the operation returns a
//! CLASSIFIED busy error (`is_busy()`) — never a hang, never an unclassified
//! raw code. `set_busy_timeout(Duration::ZERO)` restores the honest immediate
//! fail-loud (no hidden blocking) for a caller who wants it.
//!
//! The contention is real: connection A takes the write lock with
//! `BEGIN IMMEDIATE`, and connection B's write must wait for it. The timing
//! bounds (a 150 ms wait vs an immediate ZERO-timeout fail) are separated by a
//! wide margin, so the two policies are distinguishable without flakiness.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bsql_sqlite::Connection;

/// Process-unique temp DB path that removes its file and WAL/SHM sidecars on
/// drop (so a WAL-mode file DB leaves nothing behind, even on a panic).
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new(tag: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!("bsql_busy_timeout_{tag}_{}_{n}.db", std::process::id()));
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
fn busy_timeout_waits_then_classifies_and_zero_fails_immediately() {
    let db = TempDb::new("wait");
    let a = Connection::open(&db.path).expect("open A (WAL)");
    a.execute("CREATE TABLE t(x INTEGER)").expect("create");

    // A takes the write lock and holds it for the rest of the test.
    a.execute("BEGIN IMMEDIATE").expect("A acquires the write lock");

    let b = Connection::open(&db.path).expect("open B (WAL)");

    // (1) A bounded wait: B waits ~150 ms for A's lock, then returns a CLASSIFIED
    // busy error — proof it waited (elapsed well above zero) but did NOT hang.
    b.set_busy_timeout(Duration::from_millis(150)).expect("set 150ms");
    let start = Instant::now();
    let waited = b.execute("INSERT INTO t VALUES (1)").expect_err("contended write must fail");
    let waited_elapsed = start.elapsed();
    assert!(waited.is_busy(), "a contended write is a classified busy error, got {waited:?}");
    assert!(
        waited_elapsed >= Duration::from_millis(80),
        "the write must WAIT for the lock (~150 ms), waited only {waited_elapsed:?}",
    );

    // (2) ZERO timeout: B fails IMMEDIATELY — the honest fail-loud, no blocking.
    b.set_busy_timeout(Duration::ZERO).expect("set zero");
    let start = Instant::now();
    let immediate = b.execute("INSERT INTO t VALUES (2)").expect_err("contended write must fail");
    let immediate_elapsed = start.elapsed();
    assert!(immediate.is_busy(), "still a classified busy error, got {immediate:?}");
    assert!(
        immediate_elapsed < Duration::from_millis(80),
        "a ZERO timeout must fail immediately, took {immediate_elapsed:?}",
    );

    // Release A's lock before the sidecar files are removed on drop.
    a.execute("ROLLBACK").expect("release lock");
}
