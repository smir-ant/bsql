//! LIVE witness for the N+1 query detector (feature `n1-detect`).
//!
//! Each test is a `#[bsql::test]`: it runs in its own isolated PostgreSQL schema
//! (needs a real server at `BSQL_TEST_DSN`), so the connection — and thus the
//! per-connection N+1 report — starts clean and empty. All `#[ignore]`.
//!
//! Run with, e.g.:
//! ```text
//! BSQL_TEST_DSN=postgres://smir-ant@localhost/postgres \
//!   cargo test -p bsql-query-fixture --features n1-detect -- --ignored
//! ```
//!
//! The whole file compiles to nothing unless the fixture's `n1-detect` feature
//! is on (it pulls `bsql`'s `n1-detect` + `test-harness`), so a default
//! `--workspace` build never forces the detector on.
#![cfg(feature = "n1-detect")]
#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "live test — unwrap/expect surface failures loudly; not production fallbacks"
)]

use bsql::pg::Connection;

// The query the N+1 loops run. Its `&'static str` is the report's `sql`.
const ECHO_SQL: &str = "SELECT $1::int4 AS n";
bsql::query!(Echo, "SELECT $1::int4 AS n");
// A DIFFERENT query, for the "run once, not flagged" and multi-query cases.
bsql::query!(Ping, "SELECT 1::int4 AS n");

// The detector's default threshold is 25; loop clearly past it.
const LOOP: i32 = 30;

// ─────────────────────────────────────────────────────────────────────
// 1. The classic N+1: the SAME query, N times, from ONE source line, is
//    flagged with the right sql + file + line + count — AND every one of
//    those queries still returns the correct result (diagnostics-only).
// ─────────────────────────────────────────────────────────────────────
#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn n_plus_one_is_flagged_with_source_and_count(conn: &mut Connection) {
    let mut call_line = 0u32;
    for i in 0..LOOP {
        call_line = line!() + 1;
        let rows = conn.query::<EchoQuery>((i,)).await.unwrap();
        // DIAGNOSTICS-ONLY: the detector altered nothing — each query returns
        // its own echoed value.
        let owned = rows.into_owned().unwrap();
        assert_eq!(owned.len(), 1, "iteration {i}: one row");
        assert_eq!(owned[0].n, i, "iteration {i}: echoed value unchanged");
    }

    let report = conn.n1_report();
    assert_eq!(report.len(), 1, "exactly one N+1 site flagged, got {report:?}");
    let r = &report[0];
    assert_eq!(r.sql, ECHO_SQL, "the flagged query's SQL");
    assert!(
        r.file.ends_with("n1_detect_live.rs"),
        "flagged in this source file, got {:?}",
        r.file
    );
    assert_eq!(r.line, call_line, "flagged at the loop's call line");
    assert_eq!(r.count, u32::try_from(LOOP).unwrap(), "count reflects every execution");
}

// ─────────────────────────────────────────────────────────────────────
// 2. NO false positive: a query run ONCE, and a second query run once, are
//    never flagged.
// ─────────────────────────────────────────────────────────────────────
#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn a_single_query_is_not_flagged(conn: &mut Connection) {
    let a = conn.query::<EchoQuery>((7,)).await.unwrap();
    assert_eq!(a.into_owned().unwrap()[0].n, 7);
    let b = conn.query::<PingQuery>(()).await.unwrap();
    assert_eq!(b.into_owned().unwrap()[0].n, 1);

    assert!(
        conn.n1_report().is_empty(),
        "one-shot queries must not flag, got {:?}",
        conn.n1_report()
    );
}

// ─────────────────────────────────────────────────────────────────────
// 3. NO false positive across DISTINCT call sites: the SAME query run from
//    two different source lines, each below the threshold, is not flagged —
//    even though the combined count exceeds it. Proves the (sql, call-site)
//    composite key, not a bare per-query counter.
// ─────────────────────────────────────────────────────────────────────
#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn distinct_call_sites_are_not_conflated(conn: &mut Connection) {
    // 20 + 20 = 40 (> 25 threshold) but split across two lines: 20 each.
    for i in 0..20 {
        let a = conn.query::<EchoQuery>((i,)).await.unwrap(); // site A
        assert_eq!(a.into_owned().unwrap()[0].n, i);
    }
    for i in 0..20 {
        let b = conn.query::<EchoQuery>((i,)).await.unwrap(); // site B (distinct line)
        assert_eq!(b.into_owned().unwrap()[0].n, i);
    }
    assert!(
        conn.n1_report().is_empty(),
        "two distinct sites each below threshold must not flag, got {:?}",
        conn.n1_report()
    );
}

// ─────────────────────────────────────────────────────────────────────
// 4. query_one is tracked too, and attributed to the USER's call site (not
//    double-counted through the inner collect body).
// ─────────────────────────────────────────────────────────────────────
#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn query_one_loop_is_flagged_once(conn: &mut Connection) {
    let mut call_line = 0u32;
    for i in 0..LOOP {
        call_line = line!() + 1;
        let one = conn.query_one::<EchoQuery>((i,)).await.unwrap();
        assert_eq!(one.n, i, "iteration {i}: echoed value unchanged");
    }
    let report = conn.n1_report();
    assert_eq!(report.len(), 1, "query_one N+1 flagged exactly once, got {report:?}");
    assert_eq!(report[0].sql, ECHO_SQL);
    assert_eq!(report[0].line, call_line, "attributed to the user's call site");
    assert_eq!(report[0].count, u32::try_from(LOOP).unwrap());
}
