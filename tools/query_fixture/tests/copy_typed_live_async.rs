//! LIVE typed binary COPY-in over the ASYNC (tokio) driver — the B4 witness.
//!
//! The async twin of `copy_typed_live_sync.rs`: same `copy!` carrier, same
//! hostile-string / SQL-NULL / large-multi-flush-batch round-trip, only
//! `.await`ed. Because both drivers forward to the ONE `Core::copy_in_typed`,
//! this proves the async path is byte-identical to the sync one at the verb
//! level.
//!
//! Run with: `cargo test -p bsql-query-fixture --test copy_typed_live_async -- --ignored`

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    reason = "live test harness — loud failures + index arithmetic over trusted test data; not production code"
)]

use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

bsql::copy!(BulkRow, "copy_bulk", (id, label, note, amount));
bsql::query!(BulkBack, "SELECT id, label, note, amount FROM copy_bulk ORDER BY id");

fn async_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// A tab, a newline, a double-quote, and a backslash — every byte that would
/// corrupt a TEXT COPY row. Binary COPY carries it verbatim.
const HOSTILE: &str = "a\tb\nc\"d\\e";

#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_binary_copy_round_trips_hostile_null_and_large_batch() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    c.execute_sql(
        "CREATE TEMP TABLE copy_bulk (\
         id BIGINT NOT NULL, label TEXT NOT NULL, note TEXT, amount INTEGER)",
    )
    .await
    .expect("create temp table");

    const BULK: usize = 3000;
    let labels: Vec<String> = (0..BULK).map(|i| format!("bulk-row-{i:010}")).collect();

    let mut rows: Vec<(i64, &str, Option<&str>, Option<i32>)> = Vec::with_capacity(BULK + 2);
    rows.push((1, HOSTILE, Some(HOSTILE), Some(100)));
    rows.push((2, "plain-label", None, None));
    for (idx, label) in labels.iter().enumerate() {
        rows.push((idx as i64 + 3, label.as_str(), Some("note"), Some(idx as i32)));
    }

    let affected = c
        .copy_in_typed::<BulkRow, _>(rows)
        .await
        .expect("typed binary COPY");
    assert_eq!(affected as usize, BULK + 2, "COPY reports every streamed row loaded");

    let back = c.query::<BulkBack>(()).await.expect("read back");
    let collected: Vec<(i64, String, Option<String>, Option<i32>)> = back
        .iter()
        .map(|r| {
            let r = r.expect("row decodes");
            (r.id, r.label.to_string(), r.note.map(str::to_string), r.amount)
        })
        .collect();

    assert_eq!(collected.len(), BULK + 2, "all rows present after COPY");

    let row1 = collected.first().expect("row 1");
    assert_eq!(row1.0, 1);
    assert_eq!(row1.1, HOSTILE, "hostile label survived byte-exact");
    assert_eq!(row1.2.as_deref(), Some(HOSTILE), "hostile note survived byte-exact");
    assert_eq!(row1.3, Some(100));

    let row2 = collected.get(1).expect("row 2");
    assert_eq!(row2.0, 2);
    assert_eq!(row2.1, "plain-label");
    assert_eq!(row2.2, None, "NULL note round-trips as None");
    assert_eq!(row2.3, None, "NULL amount round-trips as None");

    let last = collected.last().expect("last row");
    assert_eq!(last.0, BULK as i64 + 2);
    assert_eq!(last.1, format!("bulk-row-{:010}", BULK - 1));
    assert_eq!(last.2.as_deref(), Some("note"));
    assert_eq!(last.3, Some(BULK as i32 - 1));

    c.close().await.expect("close");
}

/// A duplicate PRIMARY KEY is rejected at ingest as a classified error, and the
/// connection RECOVERS.
#[tokio::test]
#[ignore = "requires local PG"]
async fn a_rejected_row_recovers_the_connection() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    c.execute_sql(
        "CREATE TEMP TABLE copy_bulk (\
         id BIGINT PRIMARY KEY, label TEXT NOT NULL, note TEXT, amount INTEGER)",
    )
    .await
    .expect("create temp table");

    let dup: Vec<(i64, &str, Option<&str>, Option<i32>)> =
        vec![(1, "a", None, None), (1, "b", None, None)];
    let err = c
        .copy_in_typed::<BulkRow, _>(dup)
        .await
        .expect_err("duplicate primary key must be rejected");
    assert!(
        matches!(err, bsql_postgres_async::DriverError::Db(_)),
        "a rejected COPY is a classified DriverError::Db, got {err:?}",
    );

    let alive = c
        .query_one_sql("SELECT 1::int4")
        .await
        .expect("connection recovered after the rejected COPY");
    assert_eq!(alive.get::<i32>(0).expect("column present"), Some(1));

    c.close().await.expect("close");
}
