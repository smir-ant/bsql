//! WITNESS: the N+1 detector reports the CONSUMER's call site through the
//! generic `SyncBackend` layer, not the `backend.rs` forwarder.
//!
//! A generic data layer calls `conn.fetch_*::<Q>()` (the `SyncQueries` verbs),
//! which forward through the `RunsOn` blanket to the concrete driver verb. Before
//! the `#[track_caller]` propagation, the detector's `Location::caller()` stopped
//! at the umbrella's `backend.rs` forwarder — useless for a consumer. With
//! `#[track_caller]` on the `fetch_*` defaults, the `RunsOn` forwarders, and the
//! blanket impls (all gated on `n1-detect`), the location now names the
//! consumer's `fetch_*` call line.
//!
//! In-process over an in-memory SQLite (no network) — a plain `#[test]`, but the
//! whole file compiles to nothing unless the fixture's `n1-detect` feature is on.
//!
//! Run with:
//! ```text
//! cargo test -p bsql-syncbackend-fixture --features n1-detect --test n1_generic
//! ```
#![cfg(feature = "n1-detect")]
#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "witness test — unwrap/expect surface failures loudly; not production fallbacks"
)]

use bsql::SyncQueries;
use bsql::sqlite::Connection;
use bsql_syncbackend_fixture::UserByIdQuery;

// The detector's default threshold is 25; loop clearly past it.
const LOOP: i32 = 30;

#[test]
fn n1_through_generic_layer_reports_consumer_line_not_backend() {
    let mut conn = Connection::open_in_memory().expect("open");
    conn.execute_sql("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, name TEXT)")
        .expect("create");
    conn.execute_sql("INSERT INTO users VALUES (1, 'a@b', 'Alice')")
        .expect("insert");

    // The N+1: the SAME typed query, N times, from ONE source line — but reached
    // through the GENERIC `SyncQueries::fetch_opt`, not the concrete driver verb.
    let mut call_line = 0u32;
    for _ in 0..LOOP {
        call_line = line!() + 1;
        let got = conn.fetch_opt::<UserByIdQuery>((1i64,)).unwrap();
        // Diagnostics-only: the row is still returned unchanged.
        assert!(got.is_some(), "the seeded row is returned every iteration");
    }

    let report = conn.n1_report();
    assert_eq!(report.len(), 1, "exactly one N+1 site flagged, got {report:?}");
    let r = &report[0];
    // THE FIX: the location is the CONSUMER's `fetch_opt` call, in THIS test file
    // — never the umbrella's generic forwarder.
    assert!(
        r.file.ends_with("n1_generic.rs"),
        "flagged at the consumer's file, got {:?}",
        r.file,
    );
    assert!(
        !r.file.contains("backend.rs"),
        "must NOT stop at the generic forwarder, got {:?}",
        r.file,
    );
    assert_eq!(r.line, call_line, "flagged at the consumer's fetch call line");
    assert_eq!(r.count, u32::try_from(LOOP).unwrap(), "count reflects every call");
}
