//! Constant-memory streaming — the bsql star of the deep matrix.
//!
//! Usage: `stream_bsql <rows>`  (the sweep uses 1_000_000 and 5_000_000)
//!
//! Streams a synthetic `rows`-row result (see [`bsql_bench::stream_sql`]) through
//! bsql's dynamic `query_each_sql`, which lends each row as a zero-copy
//! `BorrowedRow` and accumulates NOTHING — O(1) resident memory regardless of the
//! row count. Every column is decoded (the real work). It reports two things a
//! materialising client structurally cannot match:
//!
//!   * **peak RSS** (`getrusage`), which stays FLAT as `rows` grows — the whole
//!     point of the curve; and
//!   * **allocations during the stream**, measured by a process-wide counting
//!     global allocator snapshotted around the stream loop. The delta is a small
//!     CONSTANT independent of `rows` (the reused per-row slot table + the fixed
//!     engine read buffer), so `alloc_per_row → 0` — bsql's "0 alloc/row" claim,
//!     proven by construction rather than asserted.
//!
//! Run one `rows` value per process (a fresh process = a clean peak-RSS reading),
//! exactly like the other `rss_*` harnesses.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::ops::ControlFlow;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use bsql_bench as h;

// ─── Counting global allocator ──────────────────────────────────────────────
// Counts every allocation EVENT (alloc / alloc_zeroed / a growing realloc) and
// the bytes requested. Deallocation is not counted — we want the number of
// allocations a code region performs, snapshotted before/after the stream. It
// delegates to `System`, so it is a pure meter with no behaviour change.
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

// SAFETY: every method forwards verbatim to `System` (a valid `GlobalAlloc`)
// with the exact same `ptr`/`layout`/`new_size` arguments; the only added work
// is two relaxed atomic increments, which cannot affect allocation soundness.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: forwarding the caller's valid `layout` to the system allocator.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: forwarding the caller's valid `layout` to the system allocator.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarding the caller's valid `ptr`/`layout` pair.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A grow is a fresh allocation event (it may move the block); count it.
        if new_size > layout.size() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
        }
        // SAFETY: forwarding the caller's valid `ptr`/`layout`/`new_size`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Rows streamed in the pre-measurement warm-up (sizes the engine read buffer
/// and any lazy first-call state so the measured delta is steady-state).
const WARM_ROWS: u64 = 10_000;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let rows: u64 = match args.get(1).map(|s| s.parse::<u64>()) {
        Some(Ok(n)) if n >= 1 => n,
        _ => {
            eprintln!("usage: stream_bsql <rows>=positive int");
            return ExitCode::from(2);
        }
    };

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail("runtime", &e.to_string()),
    };

    match rt.block_on(run(rows)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail("stream", &e),
    }
}

fn fail(scenario: &str, msg: &str) -> ExitCode {
    println!("ERR {scenario} {msg}");
    eprintln!("ERR {scenario} {msg}");
    ExitCode::FAILURE
}

async fn run(rows: u64) -> Result<(), String> {
    println!("VERSION bsql-postgres-async 1.0.0-alpha.0");
    let mut conn = bsql::pg::Connection::connect(&h::bsql_config_env())
        .await
        .map_err(|e| format!("connect: {e:?}"))?;

    // Warm up (sizes buffers so the measured delta is steady-state).
    let warm_sql = h::stream_sql(WARM_ROWS);
    consume_stream(&mut conn, &warm_sql).await?;

    // Build the measured SQL BEFORE the snapshot so its one-shot String alloc is
    // not attributed to the stream.
    let sql = h::stream_sql(rows);

    let a0 = ALLOC_COUNT.load(Ordering::Relaxed);
    let b0 = ALLOC_BYTES.load(Ordering::Relaxed);
    let (count, sink) = consume_stream(&mut conn, &sql).await?;
    let a1 = ALLOC_COUNT.load(Ordering::Relaxed);
    let b1 = ALLOC_BYTES.load(Ordering::Relaxed);
    black_box(sink);

    if count != rows {
        return Err(format!("streamed {count} rows, expected {rows}"));
    }

    let stream_allocs = a1.saturating_sub(a0);
    let stream_bytes = b1.saturating_sub(b0);
    let alloc_per_row = stream_allocs as f64 / rows as f64;
    let rss = h::peak_rss_bytes();

    // Machine-parseable line the sweep script greps + a human RSS echo.
    println!(
        "STREAM bsql rows={rows} rss_bytes={rss} rows_read={count} \
         stream_allocs={stream_allocs} stream_alloc_bytes={stream_bytes} alloc_per_row={alloc_per_row:.6}"
    );
    println!("PEAK_RSS {}", h::mib(rss));
    Ok(())
}

/// Stream `sql`, decoding every column of every row; returns `(rows, sink)`.
/// A per-row decode failure breaks the stream and surfaces loudly.
async fn consume_stream(conn: &mut bsql::pg::Connection, sql: &str) -> Result<(u64, u64), String> {
    let mut count: u64 = 0;
    let mut sink: u64 = 0;
    let outcome = conn
        .query_each_sql(sql, |row| {
            match (row.get_i32(0), row.get_str(1), row.get_i32(2)) {
                (Ok(id), Ok(name), Ok(val)) => {
                    if let Some(v) = id {
                        sink = sink.wrapping_add(v as u64);
                    }
                    if let Some(s) = name {
                        sink = sink.wrapping_add(s.len() as u64);
                    }
                    if let Some(v) = val {
                        sink = sink.wrapping_add(v as u64);
                    }
                    count += 1;
                    ControlFlow::Continue(())
                }
                (a, b, c) => ControlFlow::Break(format!("decode: {a:?} {b:?} {c:?}")),
            }
        })
        .await
        .map_err(|e| format!("stream: {e:?}"))?;
    match outcome {
        None => Ok((count, sink)),
        Some(e) => Err(e),
    }
}
