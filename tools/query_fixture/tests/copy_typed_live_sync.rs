//! LIVE typed binary COPY-in over the SYNC (blocking) driver — the B4 witness.
//!
//! `copy!` + `copy_in_typed` bulk-load typed rows through the PGCOPY *binary*
//! path and read them back byte-exact. Proves the three things a text COPY
//! cannot: (1) a text field carrying embedded TAB / NEWLINE / QUOTE / BACKSLASH
//! — the bytes that CORRUPT a text COPY — round-trips verbatim; (2) a SQL NULL
//! (`Option::None`) round-trips; (3) a large batch spanning MULTIPLE 64 KiB
//! flushes lands every row (constant-memory streaming). The async twin is
//! `copy_typed_live_async.rs`.
//!
//! Run with: `cargo test -p bsql-query-fixture --test copy_typed_live_sync -- --ignored`

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    reason = "live test harness — loud failures + index arithmetic over trusted test data; not production code"
)]

use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

// `copy_bulk` (migration 0014): id BIGINT NOT NULL, label TEXT NOT NULL,
// note TEXT (nullable), amount INTEGER (nullable).
bsql::copy!(BulkRow, "copy_bulk", (id, label, note, amount));
bsql::query!(BulkBack, "SELECT id, label, note, amount FROM copy_bulk ORDER BY id");

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// A tab, a newline, a double-quote, and a backslash — every byte that carries
/// special meaning in a TEXT COPY, so this string would corrupt a text-format
/// row. In binary COPY it is just length-prefixed bytes.
const HOSTILE: &str = "a\tb\nc\"d\\e";

#[test]
#[ignore = "requires local PG"]
fn typed_binary_copy_round_trips_hostile_null_and_large_batch() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // A session-scoped table matching the catalog shape — auto-dropped at close,
    // never touches a persistent schema.
    c.execute_raw(
        "CREATE TEMP TABLE copy_bulk (\
         id BIGINT NOT NULL, label TEXT NOT NULL, note TEXT, amount INTEGER)",
    )
    .expect("create temp table");

    // A large batch of owned labels the rows borrow (`&str`), enough to span
    // multiple 64 KiB flushes (3000 × ~40 framed bytes ≈ 120 KiB).
    const BULK: usize = 3000;
    let labels: Vec<String> = (0..BULK).map(|i| format!("bulk-row-{i:010}")).collect();

    let mut rows: Vec<(i64, &str, Option<&str>, Option<i32>)> = Vec::with_capacity(BULK + 2);
    // Row 1: the HOSTILE string in BOTH a NOT NULL and a nullable text field.
    rows.push((1, HOSTILE, Some(HOSTILE), Some(100)));
    // Row 2: a SQL NULL note and NULL amount (`Option::None`).
    rows.push((2, "plain-label", None, None));
    // Rows 3..: the large batch (ids 3..BULK+2).
    for (idx, label) in labels.iter().enumerate() {
        rows.push((idx as i64 + 3, label.as_str(), Some("note"), Some(idx as i32)));
    }

    let affected = c
        .copy_in_typed::<BulkRow, _>(rows)
        .expect("typed binary COPY");
    assert_eq!(
        affected as usize,
        BULK + 2,
        "COPY reports every streamed row loaded",
    );

    // Read the rows back through the typed flagship and collect owned copies so
    // the assertions do not interleave prebuffer borrows.
    let back = c.query::<BulkBack>(()).expect("read back");
    let collected: Vec<(i64, String, Option<String>, Option<i32>)> = back
        .iter()
        .map(|r| {
            let r = r.expect("row decodes");
            (r.id, r.label.to_string(), r.note.map(str::to_string), r.amount)
        })
        .collect();

    assert_eq!(collected.len(), BULK + 2, "all rows present after COPY");

    // Row 1 — the HOSTILE string must be byte-exact (a text COPY would have
    // split the row at the embedded tab/newline).
    let row1 = collected.first().expect("row 1");
    assert_eq!(row1.0, 1);
    assert_eq!(row1.1, HOSTILE, "hostile label survived byte-exact");
    assert_eq!(row1.2.as_deref(), Some(HOSTILE), "hostile note survived byte-exact");
    assert_eq!(row1.3, Some(100));

    // Row 2 — the SQL NULLs round-trip as `None`.
    let row2 = collected.get(1).expect("row 2");
    assert_eq!(row2.0, 2);
    assert_eq!(row2.1, "plain-label");
    assert_eq!(row2.2, None, "NULL note round-trips as None");
    assert_eq!(row2.3, None, "NULL amount round-trips as None");

    // The last row of the large batch — proves multi-flush integrity (the tail
    // of the stream landed, not just the first flush).
    let last = collected.last().expect("last row");
    assert_eq!(last.0, BULK as i64 + 2);
    assert_eq!(last.1, format!("bulk-row-{:010}", BULK - 1));
    assert_eq!(last.2.as_deref(), Some("note"));
    assert_eq!(last.3, Some(BULK as i32 - 1));

    c.close().expect("close");
}

/// A row the server rejects at ingest (a duplicate PRIMARY KEY) is a classified
/// error and the connection RECOVERS — a follow-on query on the SAME connection
/// succeeds.
#[test]
#[ignore = "requires local PG"]
fn a_rejected_row_recovers_the_connection() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // A PRIMARY KEY on `id` makes two rows with the same id a unique violation
    // the server detects while ingesting the streamed COPY.
    c.execute_raw(
        "CREATE TEMP TABLE copy_bulk (\
         id BIGINT PRIMARY KEY, label TEXT NOT NULL, note TEXT, amount INTEGER)",
    )
    .expect("create temp table");

    let dup: Vec<(i64, &str, Option<&str>, Option<i32>)> =
        vec![(1, "a", None, None), (1, "b", None, None)];
    let err = c
        .copy_in_typed::<BulkRow, _>(dup)
        .expect_err("duplicate primary key must be rejected");
    // Classified (a DB error with SQLSTATE), never a panic.
    assert!(
        matches!(err, bsql_postgres_sync::DriverError::Db(_)),
        "a rejected COPY is a classified DriverError::Db, got {err:?}",
    );

    // The connection RECOVERED: a plain query succeeds on the same connection.
    let alive = c
        .query_one_raw("SELECT 1::int4")
        .expect("connection recovered after the rejected COPY");
    assert_eq!(alive.get::<i32>(0).expect("column present"), Some(1));

    c.close().expect("close");
}
