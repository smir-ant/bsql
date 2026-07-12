//! GATE (audit-8, --ignored live): `query_each_sql` streams in CONSTANT memory —
//! its resident set is O(1) in the ROW COUNT, not O(N). The offline
//! `engine_query_break_alloc` gate already proves the ALLOCATION COUNT is
//! row-count-independent; this is its LIVE peer: it streams a real 5-million-row
//! result and asserts the process resident set does NOT grow with the row count,
//! against a GENEROUS machine-independent margin (an O(N) regression — the driver
//! accidentally accumulating the result instead of streaming it — would grow the
//! RSS by tens of megabytes at 5M rows and turn this red, while true constant-
//! memory streaming adds ~nothing).
//!
//! The RSS is read coarsely via `ps -o rss=` (no `unsafe`, no libc, no new dep),
//! and the assertion is a DELTA between a small (10 000-row) and a large
//! (5 000 000-row) stream on the SAME process/connection, so the process baseline
//! cancels out. Every stream also verifies the exact count + the exact Gauss sum
//! in order, so a dropped/duplicated/torn row at scale is caught too. Needs a
//! local PG, so `#[ignore]`.
//!
//! Note on transport: RSS-constancy in the row count is a property of the shared
//! streaming materializer (the reused per-row slot table + bounded wire buffer),
//! which is transport-agnostic — a TLS connection adds a FIXED per-connection
//! staging buffer, a constant, not a per-row cost. TLS streaming CORRECTNESS
//! under adversarial fragmentation is proven separately by `tls_fragmentation`
//! (which streams over a byte-fragmented TLS channel). So this measures the
//! row-count scaling over plaintext, where the signal is cleanest.

use core::ops::ControlFlow;
use std::process::Command;

use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

/// A small warm-up scale and a large scale 500x bigger.
const SMALL: i64 = 10_000;
const LARGE: i64 = 5_000_000;

/// The generous margin (in KiB) the large stream's RSS may exceed the small
/// stream's by and still count as O(1). True constant-memory streaming adds far
/// under 1 MiB; an O(N) regression at 5M rows would add tens of MiB (the eager
/// materialisation of a 5M-row int8 result is ~80 MiB), so 48 MiB is comfortably
/// above the noise floor and comfortably below the regression signal.
const RSS_MARGIN_KIB: u64 = 48 * 1024;

/// Coarse current resident set size of THIS process, in KiB, via `ps -o rss=`.
/// `None` if `ps` is unavailable or unpariseable (→ the test skips the assertion
/// rather than fail spuriously). RSS is monotonic under a retaining allocator, so
/// a read right after a constant-memory stream reflects its (flat) peak.
fn rss_kib() -> Option<u64> {
    let pid = std::process::id().to_string();
    let out = Command::new("ps").args(["-o", "rss=", "-p", &pid]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?;
    text.trim().parse::<u64>().ok()
}

/// Stream `SELECT generate_series(1, n)::int8` and return `(count, sum)` —
/// accumulating NOTHING per row (the whole point). The count + Gauss sum let the
/// caller verify no row was dropped, duplicated, or torn at scale.
async fn stream_series(conn: &mut Connection, n: i64) -> Result<(u64, i128), DriverError> {
    let sql = format!("SELECT generate_series(1, {n})::int8 AS v");
    let mut count: u64 = 0;
    let mut sum: i128 = 0;
    let mut expected: i64 = 1;
    let mut order_ok = true;
    let broke = conn
        .query_each_sql::<_, ()>(&sql, |row| {
            match row.get_i64(0) {
                Ok(Some(v)) => {
                    if v != expected {
                        order_ok = false;
                    }
                    expected += 1;
                    count += 1;
                    sum += i128::from(v);
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(()),
            }
        })
        .await?;
    assert!(broke.is_none(), "a NULL/decode-failed cell must not appear in generate_series");
    assert!(order_ok, "streamed rows must arrive in order");
    Ok((count, sum))
}

/// The exact Gauss sum of `1..=n` as `i128`. `n * (n + 1)` is always even, so the
/// `>> 1` halving is exact — and avoids the forbidden integer-division operator.
fn gauss(n: i64) -> i128 {
    let n = i128::from(n);
    (n * (n + 1)) >> 1
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn streaming_rss_is_constant_in_row_count() {
    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable);
    let mut conn = Connection::connect(&cfg).await.expect("connect");

    // Warm-up stream (10k) — allocates the fixed streaming buffers.
    let (c_small, s_small) = stream_series(&mut conn, SMALL).await.expect("small stream");
    assert_eq!(c_small, u64::try_from(SMALL).expect("SMALL fits u64"), "small count");
    assert_eq!(s_small, gauss(SMALL), "small Gauss sum");
    let rss_small = rss_kib();

    // The 500x-bigger stream — must reuse those buffers, not grow with N.
    let (c_large, s_large) = stream_series(&mut conn, LARGE).await.expect("large stream");
    assert_eq!(c_large, u64::try_from(LARGE).expect("LARGE fits u64"), "large count");
    assert_eq!(s_large, gauss(LARGE), "large Gauss sum (no drop/dup/tear at 5M rows)");
    let rss_large = rss_kib();

    // O(1)-in-row-count: 500x more rows adds less than the generous margin. Skip
    // the numeric assertion (not the stream correctness) if `ps` is unavailable.
    match (rss_small, rss_large) {
        (Some(small), Some(large)) => {
            let grew = large.saturating_sub(small);
            assert!(
                grew <= RSS_MARGIN_KIB,
                "streaming RSS must be O(1) in row count: 10k-row RSS {small} KiB, \
                 5M-row RSS {large} KiB, grew {grew} KiB > {RSS_MARGIN_KIB} KiB margin \
                 (a stream accumulating the result would grow ~80 MiB at 5M rows)",
            );
            eprintln!("streaming RSS: 10k={small} KiB, 5M={large} KiB, delta={grew} KiB (O(1) holds)");
        }
        _ => eprintln!("SKIP RSS assertion: `ps -o rss=` unavailable (stream correctness still checked)"),
    }

    // Reusable after the 5M-row stream.
    let row = conn.query_one_sql("SELECT 1::int4").await.expect("reusable after a 5M-row stream");
    assert_eq!(row.get_i32(0), Ok(Some(1)));
}
