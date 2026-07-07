//! LIVE witness for the RAISED param-arity cap (16 → 32): a 20-COLUMN `copy!`
//! and a 20-PARAM `query!` both compile AND round-trip over the sync driver —
//! proving `ParamsWriter` now covers arity 0..=32 and neither the typed binary
//! COPY nor a wide parameterised query is capped at 16.
//!
//! Run with: `cargo test -p bsql-query-fixture --test copy_wide_cap_live -- --ignored`

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::type_complexity,
    reason = "live test harness — expect/unwrap surface failures loudly; the 20-tuple is the deliberately wide row under test (the raised >16 arity cap), not production API"
)]

use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

// A 20-column carrier — ABOVE the former 16-cap. `copy_wide` (migration 0015)
// is 20 `INTEGER NOT NULL` columns, so `Row<'q>` is `(i32, …, i32)` × 20.
bsql::copy!(
    WideRow,
    "copy_wide",
    (c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
);

// A 20-PARAM query — ABOVE the former 16-cap. Sums 20 `$N` int4 params.
bsql::query!(
    Sum20,
    "SELECT ($1::int4 + $2::int4 + $3::int4 + $4::int4 + $5::int4 + $6::int4 + $7::int4 \
     + $8::int4 + $9::int4 + $10::int4 + $11::int4 + $12::int4 + $13::int4 + $14::int4 \
     + $15::int4 + $16::int4 + $17::int4 + $18::int4 + $19::int4 + $20::int4)::int4 AS total"
);

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

#[test]
#[ignore = "requires local PG"]
fn twenty_column_copy_and_twenty_param_query_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql(
        "CREATE TEMP TABLE copy_wide (\
         c01 INTEGER NOT NULL, c02 INTEGER NOT NULL, c03 INTEGER NOT NULL, c04 INTEGER NOT NULL, \
         c05 INTEGER NOT NULL, c06 INTEGER NOT NULL, c07 INTEGER NOT NULL, c08 INTEGER NOT NULL, \
         c09 INTEGER NOT NULL, c10 INTEGER NOT NULL, c11 INTEGER NOT NULL, c12 INTEGER NOT NULL, \
         c13 INTEGER NOT NULL, c14 INTEGER NOT NULL, c15 INTEGER NOT NULL, c16 INTEGER NOT NULL, \
         c17 INTEGER NOT NULL, c18 INTEGER NOT NULL, c19 INTEGER NOT NULL, c20 INTEGER NOT NULL)",
    )
    .expect("create temp table");

    // Two 20-column rows: the fields are 1..20 and 100..119 respectively.
    let rows: Vec<(
        i32, i32, i32, i32, i32, i32, i32, i32, i32, i32,
        i32, i32, i32, i32, i32, i32, i32, i32, i32, i32,
    )> = vec![
        (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
        (100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119),
    ];
    let affected = c
        .copy_in_typed::<WideRow, _>(rows)
        .expect("20-column typed binary COPY");
    assert_eq!(affected, 2, "both wide rows loaded");

    // Read back via dynamic SQL (a 20-column typed query! would hit the 16
    // result-column cap; the PARAM side is the raised axis under test). Check
    // the first and last column of each row + the row count.
    let sums = c
        .query_one_sql("SELECT sum(c01)::int8 AS s1, sum(c20)::int8 AS s20, count(*)::int8 AS n FROM copy_wide")
        .expect("aggregate read back");
    assert_eq!(sums.get::<i64>(0).expect("s1"), Some(101), "sum(c01) = 1 + 100");
    assert_eq!(sums.get::<i64>(1).expect("s20"), Some(139), "sum(c20) = 20 + 119");
    assert_eq!(sums.get::<i64>(2).expect("n"), Some(2), "two rows");

    // The 20-PARAM query round-trips: sum of params 1..20 = 210.
    let total = c
        .query_one::<Sum20Query>((1i32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20))
        .expect("20-param query_one");
    assert_eq!(total.total, Some(210), "sum of $1..$20 (1..20) = 210");

    c.close().expect("close");
}
