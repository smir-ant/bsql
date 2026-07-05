//! Peak-RSS harness for `tokio-postgres` (competitor).
//!
//! Same workload + transport as the bsql harnesses: one connection, prepare
//! once, 10k single-row reads + 1k inserts, current-thread runtime. Run fresh.

use std::hint::black_box;
use std::process::ExitCode;

use bsql_bench as h;
use tokio_postgres::NoTls;

fn main() -> ExitCode {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("rss_tokio_postgres: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run()) {
        Ok(()) => {
            h::report_rss();
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rss_tokio_postgres: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(&h::pg_conn_string(), NoTls).await?;
    // tokio-postgres drives all I/O on this connection task.
    let conn_task = tokio::spawn(async move { connection.await });

    let select = client.prepare(h::SQL_SELECT_BY_PK).await?;
    let mut acc: i64 = 0;
    for i in 0..h::RSS_SELECT_ITERS {
        let id = (i % h::SEED_ROWS) + 1;
        let rows = client.query(&select, &[&id]).await?;
        for row in &rows {
            let v0: i32 = row.get(0);
            let s: &str = row.get(1);
            let v2: i32 = row.get(2);
            acc = acc
                .wrapping_add(i64::from(v0))
                .wrapping_add(s.len() as i64)
                .wrapping_add(i64::from(v2));
        }
    }

    let insert = client.prepare(h::SQL_INSERT_ONE).await?;
    let id_base = h::insert_id_base();
    for i in 0..h::RSS_INSERT_ITERS {
        let id = id_base + i;
        client.execute(&insert, &[&id, &"rss-bench", &7_i32]).await?;
    }

    black_box(acc);
    drop(client);
    let _ = conn_task.await;
    Ok(())
}
