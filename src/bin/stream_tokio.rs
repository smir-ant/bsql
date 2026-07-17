//! Constant-memory streaming — the O(rows) MATERIALISING contrast.
//!
//! Usage: `stream_tokio <rows>`  (the sweep uses 1_000_000 and 5_000_000)
//!
//! Runs the SAME synthetic `rows`-row query as `stream_bsql`, but through
//! tokio-postgres's ordinary `Client::query`, which BUFFERS the entire result
//! into a `Vec<Row>` before returning — so its peak RSS grows with the row count
//! (O(rows)), the structural opposite of bsql's `query_each_sql` O(1) stream.
//! This is the Rust peer of the libpq `PQexec` O(rows) contrast in the C client;
//! together they make the RSS curve concrete.
//!
//! Run one `rows` value per process (a fresh process = a clean peak-RSS reading).

use std::hint::black_box;
use std::process::ExitCode;

use bsql_bench as h;
use tokio_postgres::NoTls;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let rows: u64 = match args.get(1).map(|s| s.parse::<u64>()) {
        Some(Ok(n)) if n >= 1 => n,
        _ => {
            eprintln!("usage: stream_tokio <rows>=positive int");
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
    println!("VERSION tokio-postgres 0.7.18");
    let (client, connection) = tokio_postgres::connect(&h::pg_conn_string_env(), NoTls)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let sql = h::stream_sql(rows);
    // MATERIALISE: the whole result buffers into a Vec<Row> here — O(rows) RAM.
    let all = client
        .query(sql.as_str(), &[])
        .await
        .map_err(|e| format!("query: {e}"))?;

    // Touch every row so the Vec cannot be optimised away and decode happens.
    let mut sink: u64 = 0;
    for row in &all {
        let id: i32 = row.get(0);
        let name: &str = row.get(1);
        let val: i32 = row.get(2);
        sink = sink.wrapping_add(id as u64 + name.len() as u64 + val as u64);
    }
    let count = all.len() as u64;
    black_box(sink);
    // Keep the buffer resident until AFTER the RSS reading (drop below).
    let rss = h::peak_rss_bytes();
    drop(all);

    if count != rows {
        return Err(format!("materialised {count} rows, expected {rows}"));
    }

    println!("STREAM tokio rows={rows} rss_bytes={rss} rows_read={count}");
    println!("PEAK_RSS {}", h::mib(rss));
    Ok(())
}
