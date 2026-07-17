//! Concurrency-throughput benchmark — the regime the single-op latency table
//! does NOT reward, and where an async driver's per-op reactor cost pays for
//! itself.
//!
//! Usage: `concurrency_pg <client> <workers>`
//!   client  ∈ { bsql, tokio_postgres, sqlx }
//!   workers ∈ a positive integer (the sweep uses 8 / 32 / 128)
//!
//! Model (identical for every client — the FAIR comparison): a multi-thread
//! tokio runtime (`worker_threads` = the machine's parallelism, printed) runs
//! `workers` concurrent tasks, EACH holding one dedicated connection for the
//! whole run (the pgbench `-c` model). Every task loops the SAME by-PK read
//! (`bench_items`, one row, three columns, every column decoded) as fast as it
//! can. Every connection is established FIRST; then all tasks run one shared
//! absolute window — a warm-up phase (unmeasured) followed by a measured phase —
//! so their measured windows overlap. The process reports aggregate QPS and the
//! p50/p99/p999 of the merged per-op latencies.
//!
//! Why hold one connection per worker rather than checkout-per-op: it isolates
//! the DRIVER + RUNTIME's concurrent throughput from pool-checkout policy (which
//! differs per library — bsql resets on checkout for exactly-once liveness,
//! others do not), so the number reflects concurrency, not pool bookkeeping.
//! bsql still uses ITS pool to hand out the connections (mandated), it is simply
//! exercised once per worker.
//!
//! Connection coordinates come from the `PG*` env (see `bsql_bench`), so the
//! sweep script can point every client at the dedicated ephemeral server it
//! stands up (`max_connections` raised so 128 held connections fit).
//!
//! Windows are env-tunable: `CONC_WARMUP_MS` (default 1500), `CONC_MEASURE_MS`
//! (default 5000).

use std::hint::black_box;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use bsql_bench as h;

/// Per-task measurement result: ops completed in the window and their latencies.
struct TaskResult {
    ops: u64,
    lat_ns: Vec<u32>,
}

/// The two-phase window, resolved to ABSOLUTE instants shared by every worker so
/// their measured windows overlap (no start barrier — a barrier would deadlock if
/// one worker failed to connect; connecting all workers up front avoids that).
#[derive(Clone, Copy)]
struct Window {
    measure_start: Instant,
    measure_end: Instant,
}

impl Window {
    /// Anchor the warm-up + measured phases at `now`.
    fn anchored(now: Instant, warmup: Duration, measure: Duration) -> Self {
        Window {
            measure_start: now + warmup,
            measure_end: now + warmup + measure,
        }
    }
}

fn duration_from_env(key: &str, default_ms: u64) -> Duration {
    let ms = match std::env::var(key) {
        Ok(v) => match v.parse::<u64>() {
            Ok(n) => n,
            Err(_) => default_ms,
        },
        Err(_) => default_ms,
    };
    Duration::from_millis(ms)
}

/// Worker threads for the runtime = the machine's parallelism (printed so a run
/// is reproducible). Same for every client, so the field stays even.
fn worker_threads() -> usize {
    match std::thread::available_parallelism() {
        Ok(n) => n.get(),
        Err(_) => 8,
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let client = match args.get(1) {
        Some(s) => s.as_str(),
        None => "",
    };
    let workers: usize = match args.get(2).map(|s| s.parse::<usize>()) {
        Some(Ok(n)) if n >= 1 => n,
        _ => {
            eprintln!("usage: concurrency_pg <bsql|tokio_postgres|sqlx> <workers>=positive int");
            return ExitCode::from(2);
        }
    };
    let threads = worker_threads();
    let warmup = duration_from_env("CONC_WARMUP_MS", 1_500);
    let measure = duration_from_env("CONC_MEASURE_MS", 5_000);

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail("runtime", &e.to_string()),
    };

    let outcome = rt.block_on(async move {
        match client {
            "bsql" => run_bsql(workers, warmup, measure).await,
            "tokio_postgres" => run_tokio(workers, warmup, measure).await,
            "sqlx" => run_sqlx(workers, warmup, measure).await,
            other => Err(format!("unknown client `{other}`")),
        }
    });

    match outcome {
        Ok(results) => {
            report(client, workers, threads, measure, &results);
            ExitCode::SUCCESS
        }
        Err(e) => fail(client, &e),
    }
}

fn fail(scenario: &str, msg: &str) -> ExitCode {
    println!("ERR {scenario} {msg}");
    eprintln!("ERR {scenario} {msg}");
    ExitCode::FAILURE
}

/// Drive one worker's timed loop, calling `op` (an async closure that runs ONE
/// by-PK read and decodes every column) as fast as it can within the shared
/// window.
///
/// `op` is an `AsyncFnMut` so its returned future may borrow the worker's owned
/// connection each call — a plain `FnMut() -> impl Future` cannot express that
/// per-call borrow.
async fn drive<F>(window: Window, mut op: F) -> Result<TaskResult, String>
where
    F: AsyncFnMut() -> Result<(), String>,
{
    let mut lat_ns: Vec<u32> = Vec::with_capacity(1 << 20);
    let mut ops: u64 = 0;
    loop {
        let now = Instant::now();
        if now >= window.measure_end {
            break;
        }
        let in_window = now >= window.measure_start;
        let op_start = Instant::now();
        op().await?;
        if in_window {
            let dt = op_start.elapsed().as_nanos();
            // Clamp at u32::MAX ns (~4.29 s) — a by-PK read never approaches it;
            // the saturating clamp keeps the histogram total-function.
            let ns = if dt > u128::from(u32::MAX) {
                u32::MAX
            } else {
                dt as u32
            };
            lat_ns.push(ns);
            ops += 1;
        }
    }
    Ok(TaskResult { ops, lat_ns })
}

// ═══════════════════════════════════════════════════════════════
//  bsql — compile-checked query! flagship, ASYNC driver, its Pool
// ═══════════════════════════════════════════════════════════════
bsql::query!(ByPk, "SELECT id, name, val FROM bench_items WHERE id = $1");

async fn run_bsql(workers: usize, warmup: Duration, measure: Duration) -> Result<Vec<TaskResult>, String> {
    use bsql::pg::Pool;
    println!("VERSION bsql-postgres-async 1.0.0-alpha.0");

    // A pool sized to the worker count: every worker checks out exactly one
    // connection (a fresh connect, no reset) and holds it for the run. Connect
    // ALL up front (a failure aborts cleanly here, never a mid-run hang), then
    // anchor the shared window and spawn the timed loops.
    let pool = Pool::builder(h::bsql_config_env(), workers).build();
    let mut conns = Vec::with_capacity(workers);
    for _ in 0..workers {
        conns.push(pool.get().await.map_err(|e| format!("get: {e:?}"))?);
    }

    let window = Window::anchored(Instant::now(), warmup, measure);
    let mut handles = Vec::with_capacity(workers);
    for mut conn in conns {
        handles.push(tokio::spawn(async move {
            let mut id: i32 = 0;
            drive(window, async move || {
                id = (id % 10_000) + 1;
                let c = conn.conn_mut().map_err(|e| format!("conn_mut: {e:?}"))?;
                let rows = c.query::<ByPk>((id,)).await.map_err(|e| format!("by_pk: {e:?}"))?;
                let mut sink: u64 = 0;
                for r in rows.iter() {
                    let r = r.map_err(|e| format!("decode: {e:?}"))?;
                    sink = sink.wrapping_add(r.id as u64 + r.name.len() as u64 + r.val as u64);
                }
                black_box(sink);
                Ok(())
            })
            .await
        }));
    }
    join_all(handles).await
}

// ═══════════════════════════════════════════════════════════════
//  tokio-postgres — one dedicated connection per worker (binary prepared)
// ═══════════════════════════════════════════════════════════════
async fn run_tokio(workers: usize, warmup: Duration, measure: Duration) -> Result<Vec<TaskResult>, String> {
    use tokio_postgres::{Client, NoTls, Statement};
    println!("VERSION tokio-postgres 0.7.18");

    let conninfo = h::pg_conn_string_env();
    // Connect all up front.
    let mut conns: Vec<(Client, Statement)> = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let stmt = client
            .prepare(h::SQL_CONC_BY_PK)
            .await
            .map_err(|e| format!("prepare: {e}"))?;
        conns.push((client, stmt));
    }

    let window = Window::anchored(Instant::now(), warmup, measure);
    let mut handles = Vec::with_capacity(workers);
    for (client, stmt) in conns {
        handles.push(tokio::spawn(async move {
            let mut id: i32 = 0;
            drive(window, async move || {
                id = (id % 10_000) + 1;
                let rows = client
                    .query(&stmt, &[&id])
                    .await
                    .map_err(|e| format!("by_pk: {e}"))?;
                let mut sink: u64 = 0;
                for row in &rows {
                    let rid: i32 = row.get(0);
                    let name: &str = row.get(1);
                    let val: i32 = row.get(2);
                    sink = sink.wrapping_add(rid as u64 + name.len() as u64 + val as u64);
                }
                black_box(sink);
                Ok(())
            })
            .await
        }));
    }
    join_all(handles).await
}

// ═══════════════════════════════════════════════════════════════
//  sqlx — its PgPool (one held connection per worker)
// ═══════════════════════════════════════════════════════════════
async fn run_sqlx(workers: usize, warmup: Duration, measure: Duration) -> Result<Vec<TaskResult>, String> {
    use sqlx::Row;
    use sqlx::pool::PoolConnection;
    use sqlx::postgres::{PgPoolOptions, Postgres};
    println!("VERSION sqlx 0.8.6");

    let max = match u32::try_from(workers) {
        Ok(n) => n,
        Err(_) => return Err("workers exceeds u32".to_owned()),
    };
    let pool = PgPoolOptions::new()
        .max_connections(max)
        .min_connections(max)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&h::pg_url_env())
        .await
        .map_err(|e| format!("pool: {e}"))?;
    // Acquire all up front (pre-warmed by min_connections).
    let mut conns: Vec<PoolConnection<Postgres>> = Vec::with_capacity(workers);
    for _ in 0..workers {
        conns.push(pool.acquire().await.map_err(|e| format!("acquire: {e}"))?);
    }

    let window = Window::anchored(Instant::now(), warmup, measure);
    let mut handles = Vec::with_capacity(workers);
    for mut conn in conns {
        handles.push(tokio::spawn(async move {
            let mut id: i32 = 0;
            drive(window, async move || {
                id = (id % 10_000) + 1;
                let row = sqlx::query(h::SQL_CONC_BY_PK)
                    .bind(id)
                    .fetch_one(&mut *conn)
                    .await
                    .map_err(|e| format!("by_pk: {e}"))?;
                let rid: i32 = row.try_get(0).map_err(|e| format!("decode id: {e}"))?;
                let name: &str = row.try_get(1).map_err(|e| format!("decode name: {e}"))?;
                let val: i32 = row.try_get(2).map_err(|e| format!("decode val: {e}"))?;
                let sink = rid as u64 + name.len() as u64 + val as u64;
                black_box(sink);
                Ok(())
            })
            .await
        }));
    }
    join_all(handles).await
}

/// Join every worker handle, surfacing the FIRST error (a task panic or an inner
/// query failure) as a loud `Err` — never a silently-dropped worker.
async fn join_all(
    handles: Vec<tokio::task::JoinHandle<Result<TaskResult, String>>>,
) -> Result<Vec<TaskResult>, String> {
    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(Ok(tr)) => out.push(tr),
            Ok(Err(e)) => return Err(e),
            Err(join_e) => return Err(format!("worker panicked: {join_e}")),
        }
    }
    Ok(out)
}

/// Aggregate + print. QPS = total measured ops ÷ nominal measured window (every
/// worker measures for the same `measure` duration). p50/p99/p999 come from the
/// merged per-op latency histogram.
fn report(client: &str, workers: usize, threads: usize, measure: Duration, results: &[TaskResult]) {
    let total_ops: u64 = results.iter().map(|r| r.ops).sum();
    let measure_secs = measure.as_secs_f64();
    let qps = if measure_secs > 0.0 {
        total_ops as f64 / measure_secs
    } else {
        0.0
    };

    let mut all: Vec<u32> = Vec::with_capacity(total_ops as usize);
    for r in results {
        all.extend_from_slice(&r.lat_ns);
    }
    all.sort_unstable();
    let p50 = percentile_us(&all, 0.50);
    let p99 = percentile_us(&all, 0.99);
    let p999 = percentile_us(&all, 0.999);

    // Machine-parseable line the sweep script greps.
    println!(
        "CONC {client} workers={workers} threads={threads} qps={qps:.0} \
         p50_us={p50:.2} p99_us={p99:.2} p999_us={p999:.2} ops={total_ops} secs={measure_secs:.1}"
    );
}

/// The `q`-quantile of a SORTED nanosecond slice, in microseconds. Empty → 0.
fn percentile_us(sorted_ns: &[u32], q: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let last = sorted_ns.len() - 1;
    let idx = (q * last as f64).round() as usize;
    let idx = if idx > last { last } else { idx };
    f64::from(sorted_ns[idx]) / 1000.0
}
