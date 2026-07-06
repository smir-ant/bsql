//! CONSTANT-ALLOCATION proof for the eager `query()` arena.
//!
//! The eager path previously materialized `Vec<Row>` — one outer `Vec`, one
//! `Vec<SqliteValue>` PER ROW, and one owned `String`/`Vec<u8>` PER text/blob
//! cell: `1 + R + T` allocations, LINEAR in the row count. The arena model
//! collapses that to ONE shared store (a `data`/`slots` pair plus the shared
//! name/arena `Arc`s), so a whole result costs a CONSTANT number of allocations
//! regardless of row count — 0 per row. The only per-scale growth left is the
//! logarithmic `Vec`-doubling of the two arena buffers as they fill.
//!
//! With the workspace counting allocator installed, this test drives `query()`
//! at two row counts two orders of magnitude apart and asserts the charged
//! allocations are (a) a small absolute constant — impossible under the old
//! `>= R` linear model — and (b) within a tiny additive window of each other
//! (only the extra buffer-doublings differ). The rows are integer-only (a
//! recursive CTE), so no text/blob bytes enter `data` and the per-row cost is
//! provably zero, not merely small.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global: all measurements live in a single
//! `#[test]` run sequentially, so no concurrent test thread can allocate inside
//! a measured window.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "alloc-proof harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use bsql_devgates::CountingAllocator;
use bsql_sqlite::Connection;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

/// Eagerly materialize `n` integer rows via `query()` and return
/// `(rows_materialized, allocations charged to the query window)`. The SQL build
/// happens OUTSIDE the measured window; only the query + arena seal is bracketed.
fn materialize_rows(conn: &Connection, n: i64) -> (usize, usize) {
    let sql = format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < {n}) \
         SELECT x, x*2 FROM c"
    );

    let before = ALLOC.snapshot();
    let result = conn.query(&sql).expect("query");
    let after = ALLOC.snapshot();

    // Touch every row so the arena is fully addressed (mint each handle, read
    // both integer cells) — proving the handles are lazy, not pre-built.
    let mut sum = 0i64;
    for row in result.iter() {
        sum = sum.wrapping_add(row.get::<i64>(0).expect("x"));
        sum = sum.wrapping_add(row.get::<i64>(1).expect("2x"));
    }
    let _ = sum;

    (result.len(), after.delta(before).allocs)
}

#[test]
fn eager_query_allocations_are_constant_independent_of_row_count() {
    let conn = Connection::open_in_memory().expect("open");

    // Two row counts two orders of magnitude apart. The OLD eager model charged
    // `>= R` allocations; the arena charges a small constant plus a few
    // buffer-doublings.
    let (small_rows, small_allocs) = materialize_rows(&conn, 200);
    let (large_rows, large_allocs) = materialize_rows(&conn, 20_000);

    assert_eq!(small_rows, 200, "every small-N row materialized");
    assert_eq!(large_rows, 20_000, "every large-N row materialized");

    // Non-vacuousness floor: the arena genuinely allocates (its `slots` Vec, the
    // arena `Arc`, the names `Arc`), so a materialization that charged ZERO would
    // mean the counting allocator is broken — and every upper-bound assertion
    // below (`0 <= 48`, `0 - 0 <= 24`) would pass vacuously. Refuse that.
    assert!(
        small_allocs >= 1 && large_allocs >= 1,
        "the counting allocator must observe the arena's allocations \
         (small {small_allocs}, large {large_allocs}); zero means it is not counting"
    );

    // (a) Absolute constant: the old `Vec<Row>` model would charge >= 20000 for
    // the large case (one per-row Vec each). A small ceiling proves 0-per-row.
    assert!(
        large_allocs <= 48,
        "eager materialization of 20000 rows must be a small constant, got {large_allocs} — \
         a linear (>= row-count) charge means the arena is not 0-per-row"
    );

    // (b) The two scales differ only by the extra `Vec`-doublings between 200 and
    // 20000 slots — a tiny additive window, NOT the ~19800 a per-row alloc adds.
    let extra = large_allocs.saturating_sub(small_allocs);
    assert!(
        extra <= 24,
        "the 100x row-count jump added {extra} allocations (200 -> {small_allocs}, \
         20000 -> {large_allocs}); a per-row allocation would add ~19800"
    );
}
