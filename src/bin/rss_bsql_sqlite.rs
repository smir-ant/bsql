//! Peak-RSS harness for the bsql EMBEDDED SQLite driver — the SQLite twin of
//! `rss_bsql_sync` / `rss_bsql_async`. One connection over the shared bench DB,
//! `RSS_SELECT_ITERS` single-row-by-PK reads (every column read, through a reused
//! prepared handle — the cache-hit path a real workload lives on), then
//! `RSS_INSERT_ITERS` single-row inserts. Prints the process-lifetime peak RSS.
//!
//! Requires `BENCH_SQLITE_PATH` to point at a DB seeded by `setup/sqlite_setup.sql`.

use std::hint::black_box;
use std::ops::ControlFlow;
use std::process::ExitCode;

use bsql::sqlite::{BorrowedRow, Connection, SqliteError, ValueRef};
use bsql_bench as h;

fn touch(row: &BorrowedRow<'_>) -> ControlFlow<SqliteError> {
    for col in 0..row.column_count() {
        match row.value_ref(col) {
            Ok(v) => {
                black_box(v);
            }
            Err(e) => return ControlFlow::Break(e),
        }
    }
    ControlFlow::Continue(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => {
            h::report_rss();
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("rss_bsql_sqlite: {e:?}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), SqliteError> {
    let path = std::env::var("BENCH_SQLITE_PATH")
        .map_err(|_| SqliteError::Open("BENCH_SQLITE_PATH must be set".to_owned()))?;
    let conn = Connection::open(&path)?;

    // Reads — a reused prepared handle (no per-call recompile), every column touched.
    let mut stmt = conn.prepare_sql("SELECT id, name, email FROM bench_users WHERE id = ?1")?;
    for i in 0..h::RSS_SELECT_ITERS {
        let id = i64::from((i % h::SEED_ROWS) + 1);
        let p = [ValueRef::Integer(id)];
        if let Some(e) = stmt.query_each(&p, |r| touch(&r))? {
            return Err(e);
        }
    }

    // Inserts — RETURNING id, matching parity_sqlite's insert cell.
    let isql = "INSERT INTO bench_users (name, email, active, score) \
                VALUES (?1, ?2, 1, 0.0) RETURNING id";
    let ip = [
        ValueRef::Text(b"rss-bench"),
        ValueRef::Text(b"rss@example.com"),
    ];
    for _ in 0..h::RSS_INSERT_ITERS {
        if let Some(e) = conn.query_each_params(isql, &ip, |r| touch(&r))? {
            return Err(e);
        }
    }

    Ok(())
}
