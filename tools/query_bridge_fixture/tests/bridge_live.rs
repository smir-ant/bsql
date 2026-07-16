//! LIVE external-type bridge round-trip over BOTH drivers — `#[ignore]` (needs
//! local PG).
//!
//! A `timestamptz` literal round-trips through real PostgreSQL and decodes
//! directly into the dep-free `MyTs` stand-in; a `uuid` literal decodes directly
//! into the real `uuid::Uuid`. This proves the whole typed pipeline (macro ->
//! TypedQuery -> query_params -> prebuffer -> typed decode) applies the bridge
//! converter to real wire bytes — bsql depending on and forcing nothing.
//!
//! Each query is a LITERAL cast needing no table, so it validates against the
//! migration catalog with no live schema setup.
//!
//! Run with:
//!   cargo test -p bsql-query-bridge-fixture --test bridge_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql_query_bridge_fixture::bridge::{MyDate, MyDecimal, MyTs};

// A `timestamptz` at the PostgreSQL epoch (2000-01-01 00:00:00 UTC) -> 0 raw
// micros -> the bridged `MyTs(0)`.
bsql::query!(
    LiveTs,
    "SELECT '2000-01-01 00:00:00+00'::timestamptz AS created"
);
// A `uuid` literal -> the bridged `uuid::Uuid` of the same 16 bytes.
bsql::query!(
    LiveUuid,
    "SELECT '01234567-89ab-cdef-fedc-ba9876543210'::uuid AS id"
);
// A `numeric` literal -> the bridged `MyDecimal` holding the exact decimal
// text, proving the arbitrary-precision pivot bridges through real wire bytes.
bsql::query!(LiveNumeric, "SELECT '1234.5600'::numeric AS amount");
// A `date` literal (a leap day) -> the bridged `MyDate`, reshaped from the
// native `bsql::Date` via the civil conversion through real wire bytes.
bsql::query!(LiveDate, "SELECT '2000-02-29'::date AS day");

const EXPECTED_UUID: [u8; 16] = [
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
];

#[test]
#[ignore = "requires local PG"]
fn sync_bridged_columns_round_trip() {
    use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable);
    let mut c = Connection::connect(&config).expect("connect");

    // `timestamptz` -> the dep-free stand-in target.
    let ts = c.query_one::<LiveTs>(()).expect("query_one LiveTs");
    let created: MyTs = ts.created;
    assert_eq!(created, MyTs(0), "PG-epoch timestamptz -> MyTs(0)");

    // `uuid` -> the real external `uuid::Uuid` target.
    let id_row = c.query_one::<LiveUuid>(()).expect("query_one LiveUuid");
    let id: uuid::Uuid = id_row.id;
    assert_eq!(id, uuid::Uuid::from_bytes(EXPECTED_UUID));

    // `numeric` -> the dep-free `MyDecimal` target, exact decimal text.
    let amt = c.query_one::<LiveNumeric>(()).expect("query_one LiveNumeric");
    let amount: MyDecimal = amt.amount;
    assert_eq!(amount, MyDecimal("1234.5600".to_string()), "numeric -> exact MyDecimal");

    // `date` -> the dep-free `MyDate` target via the civil conversion.
    let dt = c.query_one::<LiveDate>(()).expect("query_one LiveDate");
    let day: MyDate = dt.day;
    assert_eq!(
        day,
        MyDate { year: 2000, month: 2, day: 29 },
        "date -> MyDate via the civil conversion",
    );

    c.close().expect("close");
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn async_bridged_columns_round_trip() {
    use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

    let config = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable);
    let mut c = Connection::connect(&config).await.expect("connect");

    let ts = c
        .query_one::<LiveTs>(())
        .await
        .expect("query_one LiveTs");
    let created: MyTs = ts.created;
    assert_eq!(created, MyTs(0), "PG-epoch timestamptz -> MyTs(0)");

    let id_row = c
        .query_one::<LiveUuid>(())
        .await
        .expect("query_one LiveUuid");
    let id: uuid::Uuid = id_row.id;
    assert_eq!(id, uuid::Uuid::from_bytes(EXPECTED_UUID));

    let amt = c
        .query_one::<LiveNumeric>(())
        .await
        .expect("query_one LiveNumeric");
    let amount: MyDecimal = amt.amount;
    assert_eq!(amount, MyDecimal("1234.5600".to_string()), "numeric -> exact MyDecimal");

    let dt = c
        .query_one::<LiveDate>(())
        .await
        .expect("query_one LiveDate");
    let day: MyDate = dt.day;
    assert_eq!(
        day,
        MyDate { year: 2000, month: 2, day: 29 },
        "date -> MyDate via the civil conversion",
    );

    c.close().await.expect("close");
}
