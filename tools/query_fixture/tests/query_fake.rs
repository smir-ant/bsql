//! The flagship against the fake: a compile-checked `query!` typed query runs
//! over the in-memory fake PostgreSQL with NO network, and its binary
//! `DataRow` bytes decode into the typed record.
//!
//! This is the moat proof for the FLAGSHIP. `query_sql` (the simple protocol)
//! already ran against the fake; here the extended query protocol
//! (Parse/Bind/Execute/Sync, binary result cells) does too. The same
//! `fake.on(sql).returns(rows)` script that answers `query_sql` also answers
//! `query!` — the fake matches the `Parse` message's SQL text — so one script
//! serves both. A passing decode proves the fake's binary bytes are exactly
//! what the real engine + macro-emitted `Cell<BinaryFmt>` decoder expect.
//!
//! `query!` needs the build catalog this fixture crate provides (its `build.rs`
//! replays `migrations/`), which is why the demo lives here rather than in the
//! testkit crate.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql::{Date, Interval, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid};
use bsql_postgres_async::DriverError;
use bsql_testkit::{rows, FakePostgres};

// `users` (from migrations/): id BIGINT (i64), name TEXT NOT NULL (String).
bsql::query!(UsersByName, "SELECT id, name FROM users");

// `array_rows` (from migrations/): id INTEGER, ints INT4[] NOT NULL, labels
// TEXT[] NOT NULL, ids UUID[] NOT NULL, tags TEXT[] (nullable whole array). The
// record fields type to `Vec<Option<T>>` (a 1-D array; each element may be
// NULL) and `Option<Vec<Option<String>>>` (the nullable `tags` adds the outer
// Option) — proven end-to-end below by decoding the fake's binary array bytes.
bsql::query!(
    ArrayCols,
    "SELECT id, ints, labels, ids, tags FROM array_rows"
);
// A query the fake will NOT script — to prove an unscripted `query!` is loud.
bsql::query!(UnscriptedById, "SELECT id FROM users WHERE id = 999");

// The FULL scalar type surface the compile-checked `query!` path decodes, as
// literal casts (no table) so each record field types to the exact bsql-native
// / primitive type. A single row scripted over the fake exercises every new
// `FakeValue` variant's BINARY wire and asserts it decodes back to the exact
// scripted value — the moat now covers uuid / numeric / temporal / json /
// float / bytea, not just int / text / bool.
bsql::query!(
    AllTypes,
    "SELECT \
     '00000000-0000-0000-0000-000000000000'::uuid AS u, \
     '3.14'::numeric AS n, \
     '2000-01-01 00:00:01+00'::timestamptz AS tstz, \
     '2000-01-01 00:00:02'::timestamp AS ts, \
     '2000-02-29'::date AS d, \
     '12:34:56.789012'::time AS tm, \
     '1 year 2 mons 3 days 04:05:06'::interval AS iv, \
     '{\"k\":1}'::json AS j, \
     '[1,2,3]'::jsonb AS jb, \
     1.5::float8 AS f8, \
     2.5::float4 AS f4, \
     '\\xDEADBEEF'::bytea AS by"
);

/// A recognizable uuid (not the SQL literal's value — the fake matches on the
/// SQL TEXT and serves the scripted bytes, so the value is ours to choose).
const WITNESS_UUID: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];

/// The one full-surface row the witness scripts. Each cell is a distinct
/// bsql-native / primitive value; every `From<..> for FakeValue` is exercised.
fn all_types_row() -> bsql_testkit::ScriptedRows {
    rows![[
        Uuid::from_bytes(WITNESS_UUID),
        "3.14".parse::<Numeric>().expect("numeric parses"),
        Timestamptz::from_micros(1_000_000),
        Timestamp::from_micros(2_000_000),
        Date::from_days(59), // 2000-02-29 (leap day)
        Time::from_micros(45_296_789_012), // 12:34:56.789012
        Interval::new(14, 3, 14_706_000_000), // 1 year 2 mons 3 days 04:05:06
        Json::new(String::from(r#"{"k":1}"#)),
        Jsonb::new(String::from("[1,2,3]")),
        1.5_f64,
        2.5_f32,
        [0xDE_u8, 0xAD, 0xBE, 0xEF].as_slice(),
    ]]
}

/// The flagship proof: a real `query!` decodes the fake's BINARY rows into the
/// typed record, asserting the exact field values — no socket, no PostgreSQL.
#[tokio::test]
async fn query_macro_decodes_the_fakes_binary_rows() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .returns(rows![[1_i64, "alice"], [2_i64, "bob"]]);

    let mut conn = fake.connect().await.expect("connect over the fake");

    let result = conn
        .query::<UsersByNameQuery>(())
        .await
        .expect("run query! over the fake");
    assert_eq!(result.len(), 2);

    // Borrowed decode: each record's fields come straight from the fake's
    // binary cell bytes (i64 = 8 big-endian, text = UTF-8).
    let decoded: Vec<(i64, String)> = result
        .iter()
        .map(|row| {
            let row = row.expect("row decodes");
            (row.id, row.name.to_owned())
        })
        .collect();
    assert_eq!(
        decoded,
        vec![(1_i64, "alice".to_owned()), (2_i64, "bob".to_owned())]
    );

    // Owned twin: same values, outliving the result buffer.
    let owned = result.into_owned().expect("into_owned");
    assert_eq!(owned.len(), 2);
    assert_eq!(owned[0].id, 1);
    assert_eq!(owned[0].name, "alice");
    assert_eq!(owned[1].id, 2);
    assert_eq!(owned[1].name, "bob");
}

/// `query_one` over the fake returns the single owned record.
#[tokio::test]
async fn query_macro_query_one_over_the_fake() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .returns(rows![[7_i64, "solo"]]);

    let mut conn = fake.connect().await.expect("connect over the fake");
    let one = conn
        .query_one::<UsersByNameQuery>(())
        .await
        .expect("query_one over the fake");
    assert_eq!(one.id, 7);
    assert_eq!(one.name, "solo");
}

/// An unscripted `query!` is a LOUD classified error, never a silent empty
/// result — and the connection stays healthy, so a scripted `query!` on the
/// SAME connection then returns its rows (the reuse invariant).
#[tokio::test]
async fn unscripted_query_macro_is_a_loud_error_then_reuse_works() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .returns(rows![[1_i64, "alice"]]);

    let mut conn = fake.connect().await.expect("connect over the fake");

    // The unscripted extended query is a loud server error, not empty rows.
    let err = conn
        .query::<UnscriptedByIdQuery>(())
        .await
        .expect_err("an unscripted query! must be a loud error, never empty");
    assert!(matches!(err, DriverError::Db(_)), "got: {err:?}");
    assert!(
        format!("{err}").contains("no scripted reply"),
        "got: {err}"
    );
    assert!(conn.is_healthy(), "the connection recovers after the error");

    // The SAME connection returns the scripted rows.
    let one = conn
        .query_one::<UsersByNameQuery>(())
        .await
        .expect("the reused connection runs the scripted query!");
    assert_eq!(one.id, 1);
    assert_eq!(one.name, "alice");
}

/// Repeating one `query!` on a single connection keeps working: the second run
/// is a cache-hit re-execute (bare Bind + Execute, no Parse), which the fake
/// resolves from the statement recorded by the first run.
#[tokio::test]
async fn repeated_query_macro_on_one_connection_hits_the_cache() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .returns(rows![[1_i64, "alice"]]);

    let mut conn = fake.connect().await.expect("connect over the fake");

    let first = conn
        .query_one::<UsersByNameQuery>(())
        .await
        .expect("first run (cache miss)");
    assert_eq!(first.name, "alice");

    let second = conn
        .query_one::<UsersByNameQuery>(())
        .await
        .expect("second run (cache hit)");
    assert_eq!(second.name, "alice");
}

// ── the SYNC twin: the same fake, the same script, the blocking driver ──

/// The flagship proof over the SYNC driver: `connect_sync` returns a real
/// blocking connection backed by the fake, and a real `query!` decodes the
/// fake's binary rows into the typed record — no socket, no `.await`.
#[test]
fn query_macro_decodes_the_fakes_binary_rows_sync() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .returns(rows![[1_i64, "alice"], [2_i64, "bob"]]);

    let mut conn = fake.connect_sync().expect("connect over the fake (sync)");

    let result = conn
        .query::<UsersByNameQuery>(())
        .expect("run query! over the fake (sync)");
    assert_eq!(result.len(), 2);

    let decoded: Vec<(i64, String)> = result
        .iter()
        .map(|row| {
            let row = row.expect("row decodes");
            (row.id, row.name.to_owned())
        })
        .collect();
    assert_eq!(
        decoded,
        vec![(1_i64, "alice".to_owned()), (2_i64, "bob".to_owned())]
    );

    let owned = result.into_owned().expect("into_owned");
    assert_eq!(owned[0].id, 1);
    assert_eq!(owned[0].name, "alice");
    assert_eq!(owned[1].id, 2);
    assert_eq!(owned[1].name, "bob");
}

/// The sync twin of the loud-unscripted + reuse invariant.
#[test]
fn unscripted_query_macro_is_a_loud_error_then_reuse_works_sync() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .returns(rows![[42_i64, "solo"]]);

    let mut conn = fake.connect_sync().expect("connect over the fake (sync)");

    let err = conn
        .query::<UnscriptedByIdQuery>(())
        .expect_err("an unscripted query! must be a loud error, never empty");
    assert!(matches!(err, DriverError::Db(_)), "got: {err:?}");
    assert!(
        format!("{err}").contains("no scripted reply"),
        "got: {err}"
    );
    assert!(conn.is_healthy(), "the connection recovers after the error");

    let one = conn
        .query_one::<UsersByNameQuery>(())
        .expect("the reused connection runs the scripted query!");
    assert_eq!(one.id, 42);
    assert_eq!(one.name, "solo");
}

// ── the FULL scalar type surface over the fake, both drivers ────────────────

/// The exact SQL the driver puts in the `Parse` message. Referencing
/// `PREPARED.sql()` (rather than re-typing the literal) guarantees the fake's
/// match key equals the driver's query byte-for-byte.
fn all_types_sql() -> &'static str {
    <AllTypesQuery as bsql::TypedQuery>::PREPARED.sql()
}

/// Assert every field of the OWNED full-surface record equals the scripted
/// value — shared by the async + sync witnesses.
fn assert_all_types_owned(r: &AllTypesOwned) {
    assert_eq!(r.u, Uuid::from_bytes(WITNESS_UUID));
    assert_eq!(r.n.to_string(), "3.14");
    assert_eq!(r.tstz, Timestamptz::from_micros(1_000_000));
    assert_eq!(r.ts, Timestamp::from_micros(2_000_000));
    assert_eq!(r.d.to_string(), "2000-02-29");
    assert_eq!(r.tm.to_string(), "12:34:56.789012");
    assert_eq!(r.iv.to_string(), "1 year 2 mons 3 days 04:05:06");
    assert_eq!(r.j.as_str(), r#"{"k":1}"#);
    assert_eq!(r.jb.as_str(), "[1,2,3]");
    assert_eq!(r.f8, 1.5);
    assert_eq!(r.f4, 2.5);
    assert_eq!(r.by, vec![0xDE_u8, 0xAD, 0xBE, 0xEF]);
}

/// The moat now covers the FULL scalar surface: a real `query!` over the fake
/// decodes every new type (uuid / numeric / timestamptz / timestamp / date /
/// time / interval / json / jsonb / float8 / float4 / bytea) from the fake's
/// BINARY bytes into the typed record — the exact scripted value, no network.
#[tokio::test]
async fn query_macro_decodes_the_full_type_surface_over_the_fake() {
    let mut fake = FakePostgres::new();
    fake.on(all_types_sql()).returns(all_types_row());

    let mut conn = fake.connect().await.expect("connect over the fake");
    let result = conn
        .query::<AllTypesQuery>(())
        .await
        .expect("run the full-surface query! over the fake");
    assert_eq!(result.len(), 1);

    // Borrowed, zero-copy decode: every field comes straight from the fake's
    // binary cell bytes; `by` (bytea) aliases the prebuffer as `&[u8]`.
    let row = result.iter().next().expect("one row").expect("row decodes");
    assert_eq!(row.u, Uuid::from_bytes(WITNESS_UUID));
    assert_eq!(row.n.to_string(), "3.14");
    assert_eq!(row.tstz, Timestamptz::from_micros(1_000_000));
    assert_eq!(row.ts, Timestamp::from_micros(2_000_000));
    assert_eq!(row.d.to_string(), "2000-02-29");
    assert_eq!(row.tm.to_string(), "12:34:56.789012");
    assert_eq!(row.iv.to_string(), "1 year 2 mons 3 days 04:05:06");
    assert_eq!(row.j.as_str(), r#"{"k":1}"#);
    assert_eq!(row.jb.as_str(), "[1,2,3]");
    assert_eq!(row.f8, 1.5);
    assert_eq!(row.f4, 2.5);
    assert_eq!(row.by, [0xDE_u8, 0xAD, 0xBE, 0xEF].as_slice());

    // Owned twin: same values, outliving the prebuffer (`by` copies to Vec<u8>).
    let owned = result.into_owned().expect("into_owned");
    assert_all_types_owned(&owned[0]);
}

/// The sync twin: the same fake, the same script, the blocking driver.
#[test]
fn query_macro_decodes_the_full_type_surface_over_the_fake_sync() {
    let mut fake = FakePostgres::new();
    fake.on(all_types_sql()).returns(all_types_row());

    let mut conn = fake.connect_sync().expect("connect over the fake (sync)");
    let result = conn
        .query::<AllTypesQuery>(())
        .expect("run the full-surface query! over the fake (sync)");
    assert_eq!(result.len(), 1);

    let owned = result.into_owned().expect("into_owned");
    assert_all_types_owned(&owned[0]);
}

// ── 1-D array columns over the fake, both drivers ──────────────────────────

/// Two witness UUIDs for the `ids` (`uuid[]`) column — chosen values (the fake
/// serves the scripted bytes, so they need not match any SQL literal).
const ARRAY_UUID_A: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];
const ARRAY_UUID_B: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];

/// The exact SQL the driver puts in the `Parse` message for the array query —
/// referenced (not re-typed) so the fake's match key equals it byte-for-byte.
fn array_cols_sql() -> &'static str {
    <ArrayColsQuery as bsql::TypedQuery>::PREPARED.sql()
}

/// Two scripted `array_rows` rows covering every array shape the decoder reads:
/// row 1 has POPULATED arrays with an interior NULL element and a NULL WHOLE
/// array (`tags`); row 2 has EMPTY arrays and a present nullable array.
fn array_cols_script() -> bsql_testkit::ScriptedRows {
    rows![
        [
            1_i32,
            vec![Some(10_i32), None, Some(30)],            // ints: {10, NULL, 30}
            vec![Some("a"), None, Some("c")],              // labels: {"a", NULL, "c"}
            vec![Uuid::from_bytes(ARRAY_UUID_A), Uuid::from_bytes(ARRAY_UUID_B)], // ids
            Option::<Vec<Option<&str>>>::None,             // tags: NULL whole array
        ],
        [
            2_i32,
            Vec::<i32>::new(),                             // ints: {} (empty)
            Vec::<&str>::new(),                            // labels: {} (empty)
            Vec::<Uuid>::new(),                            // ids: {} (empty)
            Some(vec![Some("hot"), None]),                 // tags: {"hot", NULL}
        ],
    ]
}

/// Assert the two OWNED array records equal the scripted values — shared by the
/// async + sync witnesses. Every array field decoded from the fake's binary
/// array bytes: a populated array with a NULL element (`None` in the `Vec`), an
/// EMPTY array (an empty `Vec`), a NULL whole array (`None`), and a present
/// nullable array.
fn assert_array_cols_owned(rows: &[ArrayColsOwned]) {
    // Row 1 — populated arrays, interior NULL element, NULL whole `tags`.
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].ints, vec![Some(10), None, Some(30)]);
    assert_eq!(
        rows[0].labels,
        vec![Some(String::from("a")), None, Some(String::from("c"))]
    );
    assert_eq!(
        rows[0].ids,
        vec![Some(Uuid::from_bytes(ARRAY_UUID_A)), Some(Uuid::from_bytes(ARRAY_UUID_B))]
    );
    assert_eq!(rows[0].tags, None);

    // Row 2 — empty arrays decode to empty `Vec`s; present nullable `tags`.
    assert_eq!(rows[1].id, 2);
    assert!(rows[1].ints.is_empty());
    assert!(rows[1].labels.is_empty());
    assert!(rows[1].ids.is_empty());
    assert_eq!(rows[1].tags, Some(vec![Some(String::from("hot")), None]));
}

/// The moat now covers 1-D ARRAY columns: a real `query!` over the fake decodes
/// `int4[]` / `text[]` / `uuid[]` (and a nullable `text[]`) from the fake's
/// BINARY array bytes into `Vec<Option<T>>` / `Option<Vec<Option<T>>>` — with a
/// NULL element and an empty array — no network.
#[tokio::test]
async fn query_macro_decodes_array_columns_over_the_fake() {
    let mut fake = FakePostgres::new();
    fake.on(array_cols_sql()).returns(array_cols_script());

    let mut conn = fake.connect().await.expect("connect over the fake");
    let result = conn
        .query::<ArrayColsQuery>(())
        .await
        .expect("run the array query! over the fake");
    assert_eq!(result.len(), 2);

    // Borrowed decode: arrays are self-owning, so each field is already owned.
    let row0 = result.iter().next().expect("row 0").expect("row 0 decodes");
    assert_eq!(row0.ints, vec![Some(10), None, Some(30)]);
    assert_eq!(
        row0.labels,
        vec![Some(String::from("a")), None, Some(String::from("c"))]
    );
    assert_eq!(row0.tags, None);

    // Owned twin: same values across both rows (populated + empty).
    let owned = result.into_owned().expect("into_owned");
    assert_array_cols_owned(&owned);
}

/// The sync twin: the same fake, the same script, the blocking driver.
#[test]
fn query_macro_decodes_array_columns_over_the_fake_sync() {
    let mut fake = FakePostgres::new();
    fake.on(array_cols_sql()).returns(array_cols_script());

    let mut conn = fake.connect_sync().expect("connect over the fake (sync)");
    let result = conn
        .query::<ArrayColsQuery>(())
        .expect("run the array query! over the fake (sync)");
    assert_eq!(result.len(), 2);

    let owned = result.into_owned().expect("into_owned");
    assert_array_cols_owned(&owned);
}
