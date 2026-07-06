//! Peak-RSS harness for the bsql ASYNC (tokio) driver.
//!
//! Same workload as `rss_bsql_sync`, but over the async driver on a
//! current-thread tokio runtime — so the figure includes the runtime's resident
//! cost, which the blocking driver does not pay. Run in a fresh process.

use std::hint::black_box;
use std::process::ExitCode;

use bsql_bench as h;

fn main() -> ExitCode {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rss_bsql_async: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run()) {
        Ok(()) => {
            h::report_rss();
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rss_bsql_async: {e:?}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), bsql::pg::DriverError> {
    let mut conn = bsql::pg::Connection::connect(&h::bsql_config()).await?;

    let select = conn.prepare(h::SQL_SELECT_BY_PK).await?;
    let mut acc: i64 = 0;
    for i in 0..h::RSS_SELECT_ITERS {
        let id = (i % h::SEED_ROWS) + 1;
        let qr = conn.query_prepared(&select, &(id,)).await?;
        for row in qr.iter() {
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
    conn.close_statement(select).await?;

    let insert = conn.prepare(h::SQL_INSERT_ONE).await?;
    let id_base = h::insert_id_base();
    for i in 0..h::RSS_INSERT_ITERS {
        conn.execute_prepared(&insert, &(id_base + i, "rss-bench", 7_i32))
            .await?;
    }
    conn.close_statement(insert).await?;

    black_box(acc);
    Ok(())
}
