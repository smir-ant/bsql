//! Peak-RSS harness for `sqlx` (competitor).
//!
//! Same workload + transport as the bsql harnesses: a single `PgConnection`
//! (not a pool), 10k single-row reads + 1k inserts, current-thread runtime.
//! sqlx auto-prepares and caches statements per connection, so the read loop is
//! the cache-HIT path after the first iteration. Run fresh.

use std::hint::black_box;
use std::process::ExitCode;

use bsql_bench as h;
use sqlx::{Connection, Row};

fn main() -> ExitCode {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rss_sqlx: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run()) {
        Ok(()) => {
            h::report_rss();
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rss_sqlx: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = sqlx::postgres::PgConnection::connect(&h::pg_url()).await?;

    let mut acc: i64 = 0;
    for i in 0..h::RSS_SELECT_ITERS {
        let id = (i % h::SEED_ROWS) + 1;
        let rows = sqlx::query(h::SQL_SELECT_BY_PK)
            .bind(id)
            .fetch_all(&mut conn)
            .await?;
        for row in &rows {
            let v0: i32 = row.try_get(0)?;
            let s: &str = row.try_get(1)?;
            let v2: i32 = row.try_get(2)?;
            acc = acc
                .wrapping_add(i64::from(v0))
                .wrapping_add(s.len() as i64)
                .wrapping_add(i64::from(v2));
        }
    }

    let id_base = h::insert_id_base();
    for i in 0..h::RSS_INSERT_ITERS {
        sqlx::query(h::SQL_INSERT_ONE)
            .bind(id_base + i)
            .bind("rss-bench")
            .bind(7_i32)
            .execute(&mut conn)
            .await?;
    }

    black_box(acc);
    Ok(())
}
