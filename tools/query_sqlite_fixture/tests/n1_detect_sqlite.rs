//! WITNESS for the SQLite N+1 query detector (feature `n1-detect`) — the twin of
//! the PostgreSQL `n1_detect_live.rs`.
//!
//! In-process over an in-memory SQLite (no network), so these are plain
//! `#[test]`s — but the whole file compiles to nothing unless the fixture's
//! `n1-detect` feature is on (it pulls `bsql`'s `n1-detect`, which lights up the
//! SQLite driver's detector), so a default `--workspace` build never forces it.
//!
//! Run with:
//! ```text
//! cargo test -p bsql-query-sqlite-fixture --features n1-detect --test n1_detect_sqlite
//! ```
#![cfg(feature = "n1-detect")]
#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "witness test — unwrap/expect surface failures loudly; not production fallbacks"
)]

use bsql::sqlite::{Connection, SqliteTypedQuery, ValueRef};

// The looped query. Its baked SQLite `const SQL` is the report's `sql`.
bsql::query!(MeasById, "SELECT id FROM measurements WHERE id = $1");
// A DIFFERENT query, for the "run once, not flagged" case.
bsql::query!(AllIds, "SELECT id FROM measurements");

// The detector's default threshold is 25; loop clearly past it.
const LOOP: i32 = 30;

const SCHEMA: &str = "CREATE TABLE measurements ( \
     id BIGINT PRIMARY KEY, label TEXT NOT NULL, weight DOUBLE PRECISION NOT NULL, \
     payload BYTEA, count BIGINT, note TEXT );";

fn seed() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch(SCHEMA).expect("schema");
    conn.execute_params(
        "INSERT INTO measurements VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            ValueRef::Integer(1),
            ValueRef::Text(b"alpha"),
            ValueRef::Real(1.5),
            ValueRef::Null,
            ValueRef::Null,
            ValueRef::Null,
        ],
    )
    .expect("insert");
    conn
}

/// The looped query's baked SQLite SQL (the `$1`→`?1`-rewritten form the macro
/// emits), used to assert the report names the right query.
fn meas_sql() -> &'static str {
    <MeasByIdQuery as SqliteTypedQuery>::SQL
}

/// 1. The classic N+1: the SAME typed query, N times, from ONE source line, is
///    flagged with the right sql + file + line + count — AND every query still
///    returns the correct rows (diagnostics-only).
#[test]
fn n_plus_one_is_flagged_with_source_and_count() {
    let conn = seed();
    let mut call_line = 0u32;
    for _ in 0..LOOP {
        call_line = line!() + 1;
        let rows = conn.query::<MeasByIdQuery>((1i64,)).unwrap();
        // DIAGNOSTICS-ONLY: the detector altered nothing — the row is returned.
        let owned = rows.into_owned().unwrap();
        assert_eq!(owned.len(), 1, "the seeded row is returned every iteration");
        assert_eq!(owned[0].id, 1);
    }

    let report = conn.n1_report();
    assert_eq!(report.len(), 1, "exactly one N+1 site flagged, got {report:?}");
    let r = &report[0];
    assert_eq!(r.sql, meas_sql(), "the flagged query's SQL");
    assert!(
        r.file.ends_with("n1_detect_sqlite.rs"),
        "flagged in this source file, got {:?}",
        r.file
    );
    assert_eq!(r.line, call_line, "flagged at the loop's call line");
    assert_eq!(r.count, u32::try_from(LOOP).unwrap(), "count reflects every execution");
}

/// 2. NO false positive: a query run ONCE, and a second query run once, are
///    never flagged.
#[test]
fn a_single_query_is_not_flagged() {
    let conn = seed();
    let a = conn.query::<MeasByIdQuery>((1i64,)).unwrap();
    assert_eq!(a.into_owned().unwrap().len(), 1);
    let b = conn.query::<AllIdsQuery>(()).unwrap();
    assert_eq!(b.into_owned().unwrap().len(), 1);

    assert!(
        conn.n1_report().is_empty(),
        "one-shot queries must not flag, got {:?}",
        conn.n1_report()
    );
}

/// 3. NO false positive across DISTINCT call sites: the SAME query from two
///    different source lines, each below the threshold, is not flagged — even
///    though the combined count exceeds it. Proves the (sql, call-site)
///    composite key, not a bare per-query counter.
#[test]
fn distinct_call_sites_are_not_conflated() {
    let conn = seed();
    // 20 + 20 = 40 (> 25 threshold) but split across two lines: 20 each.
    for _ in 0..20 {
        let a = conn.query::<MeasByIdQuery>((1i64,)).unwrap(); // site A
        assert_eq!(a.into_owned().unwrap().len(), 1);
    }
    for _ in 0..20 {
        let b = conn.query::<MeasByIdQuery>((1i64,)).unwrap(); // site B (distinct line)
        assert_eq!(b.into_owned().unwrap().len(), 1);
    }
    assert!(
        conn.n1_report().is_empty(),
        "two distinct sites each below threshold must not flag, got {:?}",
        conn.n1_report()
    );
}

/// 4. `query_one` is tracked too, and attributed to the USER's call site (not the
///    inner shared body).
#[test]
fn query_one_loop_is_flagged_once() {
    let conn = seed();
    let mut call_line = 0u32;
    for _ in 0..LOOP {
        call_line = line!() + 1;
        let one = conn.query_one::<MeasByIdQuery>((1i64,)).unwrap();
        assert_eq!(one.id, 1);
    }
    let report = conn.n1_report();
    assert_eq!(report.len(), 1, "query_one N+1 flagged exactly once, got {report:?}");
    assert_eq!(report[0].sql, meas_sql());
    assert_eq!(report[0].line, call_line, "attributed to the user's call site");
    assert_eq!(report[0].count, u32::try_from(LOOP).unwrap());
}

/// 5. The window is reset by EXPLICIT `commit()` / `rollback()` on the manual
///    begin/commit API, exactly like PostgreSQL — so the SAME query at the SAME
///    call site repeated ACROSS two manual transactions (20 each, 40 > 25 total)
///    is NOT flagged, because each `commit()` forgives the window. Without the
///    reset on the explicit verb this would spuriously cross the threshold — a
///    parity divergence PG does not have.
#[test]
fn manual_commit_resets_the_window_like_pg() {
    let conn = seed();
    // ONE call site (the `conn.query` line below) hit 20 times per transaction,
    // 40 total across the two — but the `commit()` between resets the window, so
    // neither transaction's 20 reaches the threshold of 25.
    for _ in 0..2 {
        conn.begin().unwrap();
        for _ in 0..20 {
            let rows = conn.query::<MeasByIdQuery>((1i64,)).unwrap();
            assert_eq!(rows.into_owned().unwrap().len(), 1);
        }
        conn.commit().unwrap();
    }
    assert!(
        conn.n1_report().is_empty(),
        "commit() must reset the window like PG (20+20 split by a commit must not \
         flag), got {:?}",
        conn.n1_report()
    );
}

/// `rollback()` is a logical-operation boundary too: the window is forgiven. The
/// SAME call site (the single `conn.query` line below) is hit 20 times in a
/// rolled-back transaction and 20 in a committed one — 40 > 25 total, but the
/// `rollback()` between resets the window, so it is not flagged.
#[test]
fn manual_rollback_resets_the_window_like_pg() {
    let conn = seed();
    for tx_idx in 0..2 {
        conn.begin().unwrap();
        for _ in 0..20 {
            let rows = conn.query::<MeasByIdQuery>((1i64,)).unwrap(); // ONE site
            assert_eq!(rows.into_owned().unwrap().len(), 1);
        }
        // First transaction rolls back, second commits — both are boundaries that
        // must reset the window.
        if tx_idx == 0 {
            conn.rollback().unwrap();
        } else {
            conn.commit().unwrap();
        }
    }
    assert!(
        conn.n1_report().is_empty(),
        "rollback() must reset the window like PG, got {:?}",
        conn.n1_report()
    );
}

/// 6. The detector works THROUGH the transaction guard: an N+1 loop on the
///    borrowing `Transaction` guard is flagged and attributed to the CONSUMER's
///    call line (via `#[track_caller]` propagation through the guard forwarder).
///    The report survives the tx-boundary window reset (which clears the recency
///    window but keeps the accumulated reports).
#[test]
fn n_plus_one_through_the_transaction_guard_is_flagged_at_the_consumer_line() {
    let conn = seed();
    let mut call_line = 0u32;
    conn.transaction(|tx| {
        for _ in 0..LOOP {
            call_line = line!() + 1;
            let rows = tx.query::<MeasByIdQuery>((1i64,))?;
            let owned = rows.into_owned()?;
            assert_eq!(owned.len(), 1);
            assert_eq!(owned[0].id, 1);
        }
        Ok(())
    })
    .unwrap();

    let report = conn.n1_report();
    assert_eq!(report.len(), 1, "the guard-path N+1 site is flagged once, got {report:?}");
    let r = &report[0];
    assert_eq!(r.sql, meas_sql());
    assert!(
        r.file.ends_with("n1_detect_sqlite.rs"),
        "flagged in this source file, got {:?}",
        r.file
    );
    assert_eq!(
        r.line, call_line,
        "attributed to the guard-body call line, not a driver-internal line"
    );
    assert_eq!(r.count, u32::try_from(LOOP).unwrap());
}
