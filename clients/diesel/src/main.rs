use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Text};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use std::time::Instant;

// ---- diesel schema (tables already seeded in the DB) ----
diesel::table! {
    bench_items (id) {
        id -> Integer,
        name -> Text,
        val -> Integer,
    }
}

diesel::table! {
    bench_cat (val) {
        val -> Integer,
        label -> Text,
    }
}

diesel::table! {
    bench_ins (id) {
        id -> BigInt,
        name -> Text,
        val -> Integer,
    }
}

// join_agg result row (decoded via sql_query)
#[derive(QueryableByName)]
struct AggRow {
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = BigInt)]
    cnt: i64,
    #[diesel(sql_type = BigInt)]
    total: i64,
}

const DB_URL: &str = "postgres://smir-ant@127.0.0.1:5432/postgres?sslmode=disable";

fn peak_rss_bytes() -> u64 {
    // getrusage(RUSAGE_SELF).ru_maxrss — BYTES on macOS.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if rc != 0 {
        return 0;
    }
    usage.ru_maxrss as u64
}

// ---- scenario ops: one iteration each, reading EVERY column into a real var ----

async fn op_by_pk(conn: &mut AsyncPgConnection, pk: i32) {
    let rows: Vec<(i32, String, i32)> = bench_items::table
        .filter(bench_items::id.eq(pk))
        .select((bench_items::id, bench_items::name, bench_items::val))
        .load(conn)
        .await
        .expect("by_pk");
    let mut acc: i64 = 0;
    for (id, name, val) in &rows {
        acc += *id as i64 + name.len() as i64 + *val as i64;
    }
    std::hint::black_box(acc);
}

async fn op_rows(conn: &mut AsyncPgConnection, limit: i32) {
    let rows: Vec<(i32, String, i32)> = bench_items::table
        .filter(bench_items::id.le(limit))
        .order(bench_items::id.asc())
        .select((bench_items::id, bench_items::name, bench_items::val))
        .load(conn)
        .await
        .expect("rows");
    let mut acc: i64 = 0;
    for (id, name, val) in &rows {
        acc += *id as i64 + name.len() as i64 + *val as i64;
    }
    std::hint::black_box(acc);
}

async fn op_insert(conn: &mut AsyncPgConnection, id: i64) {
    let n = diesel::insert_into(bench_ins::table)
        .values((
            bench_ins::id.eq(id),
            bench_ins::name.eq("x"),
            bench_ins::val.eq(1),
        ))
        .execute(conn)
        .await
        .expect("insert");
    std::hint::black_box(n);
}

async fn op_join_agg(conn: &mut AsyncPgConnection, limit: i32) {
    let rows: Vec<AggRow> = diesel::sql_query(
        "SELECT c.label AS label, count(*)::int8 AS cnt, sum(i.val)::int8 AS total \
         FROM bench_items i JOIN bench_cat c ON i.val = c.val \
         WHERE i.id <= $1 GROUP BY c.label ORDER BY c.label",
    )
    .bind::<Integer, _>(limit)
    .load(conn)
    .await
    .expect("join_agg");
    let mut acc: i64 = 0;
    for r in &rows {
        acc += r.label.len() as i64 + r.cnt + r.total;
    }
    std::hint::black_box(acc);
}

async fn correctness_check(conn: &mut AsyncPgConnection) {
    let rows: Vec<(i32, String, i32)> = bench_items::table
        .filter(bench_items::id.eq(1))
        .select((bench_items::id, bench_items::name, bench_items::val))
        .load(conn)
        .await
        .expect("correctness by_pk");
    assert_eq!(rows.len(), 1, "by_pk id=1 must return one row");
    let (id, ref name, val) = rows[0];
    assert_eq!(id, 1, "by_pk id=1 id");
    assert_eq!(name, "name_1", "by_pk id=1 name");
    assert_eq!(val, 2, "by_pk id=1 val");

    // also spot-check a 10-row read decodes correctly
    let ten: Vec<(i32, String, i32)> = bench_items::table
        .filter(bench_items::id.le(10))
        .order(bench_items::id.asc())
        .select((bench_items::id, bench_items::name, bench_items::val))
        .load(conn)
        .await
        .expect("correctness rows_10");
    assert_eq!(ten.len(), 10, "rows_10 count");
    assert_eq!(ten[9].0, 10);
    assert_eq!(ten[9].1, "name_10");
    assert_eq!(ten[9].2, 20);
}

fn median(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    v[v.len() / 2]
}

// warm `warm` iters, then 7 reps of `n` timed iters; report median ns/op.
// $body is an async block using $i as the iteration counter and `conn`.
macro_rules! bench {
    ($name:expr, $conn:expr, $warm:expr, $n:expr, $i:ident, $body:expr) => {{
        for $i in 0..$warm {
            $body;
        }
        let mut reps: Vec<u64> = Vec::with_capacity(7);
        for _rep in 0..7 {
            let start = Instant::now();
            for $i in 0..$n {
                $body;
            }
            let elapsed = start.elapsed().as_nanos() as u64;
            reps.push(elapsed / ($n as u64));
        }
        println!("LAT {} {}", $name, median(reps));
    }};
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();

    // VERSION line first.
    println!("VERSION diesel-async {}", env!("DIESEL_ASYNC_VER"));

    use diesel_async::AsyncConnection;
    let mut conn = AsyncPgConnection::establish(DB_URL)
        .await
        .expect("connect to PG");

    // sanity: a silently-wrong decode must not pass.
    correctness_check(&mut conn).await;

    match mode.as_str() {
        "latency" => {
            bench!("by_pk", conn, 2000usize, 20000usize, i, {
                let pk = ((i % 10000) + 1) as i32;
                op_by_pk(&mut conn, pk).await;
            });
            bench!("rows_10", conn, 2000usize, 10000usize, i, {
                let _ = i;
                op_rows(&mut conn, 10).await;
            });
            bench!("rows_100", conn, 2000usize, 5000usize, i, {
                let _ = i;
                op_rows(&mut conn, 100).await;
            });
            bench!("rows_1000", conn, 2000usize, 2000usize, i, {
                let _ = i;
                op_rows(&mut conn, 1000).await;
            });

            // insert: TRUNCATE first, then count up from 1 across warm+timed.
            diesel::sql_query("TRUNCATE bench_ins")
                .execute(&mut conn)
                .await
                .expect("truncate bench_ins");
            let mut next_id: i64 = 1;
            bench!("insert", conn, 2000usize, 10000usize, i, {
                let _ = i;
                op_insert(&mut conn, next_id).await;
                next_id += 1;
            });

            bench!("join_agg", conn, 2000usize, 500usize, i, {
                let _ = i;
                op_join_agg(&mut conn, 10000).await;
            });
        }
        "rss" => {
            diesel::sql_query("TRUNCATE bench_ins")
                .execute(&mut conn)
                .await
                .expect("truncate bench_ins");
            for i in 0..10000i32 {
                op_by_pk(&mut conn, (i % 10000) + 1).await;
            }
            for id in 1..=1000i64 {
                op_insert(&mut conn, id).await;
            }
            println!("RSS {}", peak_rss_bytes());
        }
        other => {
            eprintln!("ERR mode unknown mode '{}' (want latency|rss)", other);
            std::process::exit(1);
        }
    }
}
