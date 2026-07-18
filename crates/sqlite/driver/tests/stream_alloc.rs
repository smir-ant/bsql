//! CONSTANT-MEMORY proof for the streaming read path.
//!
//! `query_each` decodes each row borrowed (zero-copy) and hands it to the
//! callback, accumulating nothing. With the workspace counting allocator
//! installed, the Rust-side allocations charged to a `query_each` drive are
//! BOUNDED INDEPENDENT of the row count: the statement + row-cursor setup
//! allocates a small constant, and each row allocates ZERO. A per-row
//! allocation would make `delta(N_large) > delta(N_small)`; asserting the two
//! deltas are EQUAL (and small) proves the streaming guarantee rather than
//! merely claiming it.
//!
//! The rows are generated entirely inside SQLite via a recursive CTE — no
//! inserts, and SQLite's own memory goes through its C allocator, invisible to
//! the Rust counting allocator, so the measurement isolates the driver's
//! Rust-side per-row cost. The SQL string is built OUTSIDE the measured window.
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

use core::ops::ControlFlow;

use bsql_devgates::CountingAllocator;
use bsql_sqlite::Connection;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

/// Stream `n` rows through `query_each` (each row read borrowed, discarded,
/// counted) and return `(rows_seen, allocations charged to the drive window)`.
/// The SQL build happens OUTSIDE the measured window; only the drive is
/// bracketed.
fn stream_rows(conn: &Connection, n: i64) -> (usize, usize) {
    let sql = format!(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < {n}) \
         SELECT x, x*2 FROM c"
    );
    let mut rows = 0usize;
    let mut sum = 0i64;

    let before = ALLOC.snapshot();
    let outcome = conn
        .query_each_raw(&sql, |row| {
            // The exact per-row work: borrowed primitive reads (zero-copy, no
            // alloc), nothing accumulated.
            sum = sum.wrapping_add(row.get::<i64>(0).expect("x"));
            sum = sum.wrapping_add(row.get::<i64>(1).expect("2x"));
            rows += 1;
            ControlFlow::<()>::Continue(())
        })
        .expect("stream");
    let after = ALLOC.snapshot();

    assert_eq!(outcome, None, "the full stream reaches exhaustion");
    let _ = sum;
    (rows, after.delta(before).allocs)
}

#[test]
fn streaming_is_constant_memory_independent_of_row_count() {
    let conn = Connection::open_in_memory().expect("open");

    // Two row counts two orders of magnitude apart. Both incur the identical
    // constant statement/cursor setup and ZERO per-row allocation, so their
    // measured deltas are EQUAL.
    let (small_rows, small_allocs) = stream_rows(&conn, 200);
    let (large_rows, large_allocs) = stream_rows(&conn, 20_000);

    assert_eq!(small_rows, 200, "every small-N row streamed to the callback");
    assert_eq!(large_rows, 20_000, "every large-N row streamed to the callback");

    assert_eq!(
        small_allocs, large_allocs,
        "streaming allocations must be independent of row count \
         (200 rows charged {small_allocs}, 20000 rows charged {large_allocs}) — \
         a difference means the path accumulates per row"
    );
    assert!(
        large_allocs <= 16,
        "the constant setup cost must be small, got {large_allocs} allocations for 20000 rows"
    );
}
