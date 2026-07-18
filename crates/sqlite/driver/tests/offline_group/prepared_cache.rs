#![forbid(unsafe_code)]
//! Witnesses for the per-connection prepared-statement cache.
//!
//! The eager / execute / typed-single-row verbs reuse compiled statements
//! (rusqlite's `prepare_cached`, `SQLITE_PREPARE_PERSISTENT`) so a query re-run
//! in a loop compiles ONCE. These tests pin the properties that make that reuse
//! CORRECT rather than merely fast: bindings do not leak across reuses, a schema
//! change is never served a stale compiled statement, and the capacity knob
//! bounds the retained set without changing results.

use bsql_sqlite::{Connection, ValueRef};

/// Reuse correctness: the SAME parameterized SQL run many times with DIFFERENT
/// params returns each call's OWN correct row — proving the cache resets the
/// statement and clears prior bindings between reuses (a leaked binding would
/// return a stale row).
#[test]
fn cached_params_reuse_stays_correct() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        .expect("create");
    for i in 1..=50_i64 {
        let v = format!("row-{i}");
        conn.execute_params(
            "INSERT INTO t (id, v) VALUES (?1, ?2)",
            &[ValueRef::Integer(i), ValueRef::Text(v.as_bytes())],
        )
        .expect("insert");
    }
    // Same SQL, 200 reuses across the whole key range — each must fetch its own
    // row (a cache that failed to clear the prior bind would mismatch).
    for round in 0..4 {
        for i in 1..=50_i64 {
            let row = conn
                .query_params_one("SELECT id, v FROM t WHERE id = ?1", &[ValueRef::Integer(i)])
                .expect("fetch");
            assert_eq!(row.get::<i64>(0).expect("id"), i, "round {round}");
            assert_eq!(
                row.get::<&str>(1).expect("v"),
                format!("row-{i}"),
                "round {round}"
            );
        }
    }
}

/// A cached `INSERT` (execute path) reused across a loop applies each call —
/// the change count and the final table state prove no reuse dropped a write.
#[test]
fn cached_execute_reuse_applies_every_call() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (v INTEGER NOT NULL)").expect("create");
    let sql = "INSERT INTO t (v) VALUES (?1)";
    for i in 0..100_i64 {
        let changed = conn.execute_params(sql, &[ValueRef::Integer(i)]).expect("insert");
        assert_eq!(changed, 1, "each cached execute inserts exactly one row");
    }
    let count = conn
        .query_one_raw("SELECT COUNT(*) FROM t")
        .expect("count")
        .get::<i64>(0)
        .expect("i64");
    assert_eq!(count, 100, "all 100 cached-statement inserts landed");
}

/// Schema-change non-staleness: a `SELECT *` cached against a 1-column table,
/// then re-run after an `ALTER TABLE ... ADD COLUMN`, must reflect the NEW
/// schema (2 columns) — never serve the stale compiled statement's column
/// shape. SQLite's `prepare_v3` auto-reprepares on the schema cookie change; the
/// eager verb must observe the reprepared width, not the width it cached.
#[test]
fn cached_select_star_reflects_schema_change() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (a INTEGER NOT NULL)").expect("create");
    conn.execute_raw("INSERT INTO t (a) VALUES (1)").expect("seed");

    // Cache "SELECT *" while the table has ONE column.
    let before = conn.query_raw("SELECT * FROM t").expect("select 1-col");
    assert_eq!(before.column_count(), 1, "one column before ALTER");

    // Widen the table. The cached compiled statement is now schema-stale.
    conn.execute_raw("ALTER TABLE t ADD COLUMN b TEXT DEFAULT 'x'").expect("alter");

    // Re-run the SAME "SELECT *": must see TWO columns, not the cached one.
    let after = conn.query_raw("SELECT * FROM t").expect("select 2-col");
    assert_eq!(
        after.column_count(),
        2,
        "SELECT * must reflect the widened schema, never the stale cached width"
    );
    let row = after.get(0).expect("row");
    assert_eq!(row.get::<i64>(0).expect("a"), 1);
    assert_eq!(row.get::<&str>(1).expect("b"), "x");
}

/// A cached query whose referenced column is DROPPED out from under it must
/// surface a CLASSIFIED error on re-run, never a stale result or a panic.
#[test]
fn cached_query_over_dropped_column_is_classified_error() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (a INTEGER NOT NULL, b INTEGER NOT NULL)")
        .expect("create");
    conn.execute_raw("INSERT INTO t (a, b) VALUES (1, 2)").expect("seed");

    // Cache a query naming column `b`.
    assert_eq!(
        conn.query_one_raw("SELECT b FROM t")
            .expect("select b")
            .get::<i64>(0)
            .expect("b"),
        2
    );

    // Drop `b`. The cached statement can no longer resolve it.
    conn.execute_raw("ALTER TABLE t DROP COLUMN b").expect("drop b");

    // Re-run the cached "SELECT b": a classified error (unknown column), not a
    // stale 2 and not a panic.
    let err = conn.query_one_raw("SELECT b FROM t").expect_err("b is gone");
    // Any classified SqliteError is acceptable; the point is it does not return
    // a stale value or panic. Assert it is not silently Ok by construction (the
    // `expect_err` above), and that its Display is non-empty.
    assert!(!format!("{err}").is_empty(), "the error classifies with a message");
}

/// The capacity knob bounds the retained set and does not change results: after
/// setting a tiny capacity, cycling through MORE distinct SQL than the capacity
/// still returns correct answers (evicted statements are simply recompiled).
#[test]
fn cache_capacity_knob_bounds_without_changing_results() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)").expect("create");
    for i in 1..=10_i64 {
        conn.execute_params("INSERT INTO t (id) VALUES (?1)", &[ValueRef::Integer(i)])
            .expect("insert");
    }
    conn.set_prepared_statement_cache_capacity(2);

    // Ten DISTINCT SQL strings (LIMIT literal varies) with capacity 2: the cache
    // thrashes (evict + recompile), but every result is still correct.
    for lim in 1..=10_i64 {
        let sql = format!("SELECT id FROM t ORDER BY id LIMIT {lim}");
        let got = conn.query_raw(&sql).expect("query").len();
        let want = usize::try_from(lim).expect("positive limit");
        assert_eq!(got, want, "capacity-bounded cache still returns all rows");
    }

    // Capacity 0 disables caching entirely; results remain correct.
    conn.set_prepared_statement_cache_capacity(0);
    let n = conn
        .query_one_raw("SELECT COUNT(*) FROM t")
        .expect("count")
        .get::<i64>(0)
        .expect("i64");
    assert_eq!(n, 10);
}
