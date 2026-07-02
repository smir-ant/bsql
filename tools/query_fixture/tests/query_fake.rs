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

use bsql_postgres_async::DriverError;
use bsql_testkit::{rows, FakePostgres};

// `users` (from migrations/): id BIGINT (i64), name TEXT NOT NULL (String).
bsql::query!(UsersByName, "SELECT id, name FROM users");
// A query the fake will NOT script — to prove an unscripted `query!` is loud.
bsql::query!(UnscriptedById, "SELECT id FROM users WHERE id = 999");

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
