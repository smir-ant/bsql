//! Peak-RSS harness for the bsql BLOCKING driver.
//!
//! One direct connection, `RSS_SELECT_ITERS` single-row-by-PK reads (every
//! column read), then `RSS_INSERT_ITERS` single-row inserts. Prints the
//! process-lifetime peak resident-set size the workload reached. Run in a fresh
//! process so the figure is this client's footprint alone.
//!
//! Assumes `bench/setup/pg_setup.sql` has already been applied.

use std::hint::black_box;
use std::process::ExitCode;

use bsql_bench as h;

fn main() -> ExitCode {
    match run() {
        Ok(()) => {
            h::report_rss();
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rss_bsql_sync: {e:?}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), bsql::pg_sync::DriverError> {
    let mut conn = bsql::pg_sync::Connection::connect(&h::bsql_config())?;

    // Prepare once, execute many — the cache-HIT path every real workload uses.
    let select = conn.prepare(h::SQL_SELECT_BY_PK)?;
    let mut acc: i64 = 0;
    for i in 0..h::RSS_SELECT_ITERS {
        let id = (i % h::SEED_ROWS) + 1;
        let qr = conn.query_prepared(&select, &(id,))?;
        for row in qr.iter() {
            // Read every column, exactly like the latency benches.
            if let Some(v) = row.get_i32(0)? {
                acc = acc.wrapping_add(i64::from(v));
            }
            if let Some(s) = row.get_str(1)? {
                acc = acc.wrapping_add(s.len() as i64);
            }
            if let Some(v) = row.get_i32(2)? {
                acc = acc.wrapping_add(i64::from(v));
            }
        }
    }
    conn.close_statement(select)?;

    let insert = conn.prepare(h::SQL_INSERT_ONE)?;
    let id_base = h::insert_id_base();
    for i in 0..h::RSS_INSERT_ITERS {
        conn.execute_prepared(&insert, &(id_base + i, "rss-bench", 7_i32))?;
    }
    conn.close_statement(insert)?;

    black_box(acc);
    Ok(())
}
