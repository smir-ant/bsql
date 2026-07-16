//! LIVE `query!`-against-a-VIEW round-trip over the ASYNC (tokio) driver.
//!
//! The killer feature: `bsql-build` inferred each view's SELECT body against the
//! migration catalog (`0022_views.sql`) and registered it like any relation, so
//! a `query!` SELECTing from a view types AT COMPILE TIME and decodes AT RUN TIME
//! through the SAME path a base table uses — with NO new consumer API.
//!
//! Parallel-safe by construction: each test shadows the migration views with
//! session-local `CREATE TEMP TABLE` + `CREATE TEMP VIEW` (a TEMP object is
//! visible only to the test's own connection, and `pg_temp` is searched first,
//! so the carrier's `FROM vaccount_summary` resolves to the TEMP view) — exactly
//! the pattern `query_oid_guard_live.rs` uses over `0020_oidguard.sql`. Run
//! WITHOUT `--test-threads=1`.
//!
//! Run with: `cargo test -p bsql-query-fixture --test view_live_async -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql_postgres_async::{ConnectConfig, Connection, SslMode};

// A simple projection view: `id` + `balance`, both NOT NULL.
bsql::query!(VSummary, "SELECT id, balance FROM vaccount_summary ORDER BY id");
// A LEFT JOIN view: `nickname` is NOT NULL in `vprofile` but NULLABLE through the
// view, so the record field is `Option<..>` — the nullability-fidelity proof.
bsql::query!(
    VProfile,
    "SELECT id, balance, nickname FROM vaccount_profile ORDER BY id"
);
// A view OVER a view.
bsql::query!(VIds, "SELECT id FROM vaccount_ids ORDER BY id");

fn async_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Create the session-local TEMP shadows of the `0022_views.sql` relations and
/// seed two accounts — account 1 HAS a profile, account 2 does NOT (so its
/// LEFT-JOINed `nickname` is NULL).
async fn setup(c: &mut Connection) {
    for ddl in [
        "CREATE TEMP TABLE vaccount (id bigint primary key, balance integer not null, label text)",
        "CREATE TEMP TABLE vprofile (account_id bigint primary key, nickname text not null)",
        "CREATE TEMP VIEW vaccount_summary AS SELECT id, balance FROM vaccount",
        "CREATE TEMP VIEW vaccount_profile AS \
         SELECT a.id AS id, a.balance AS balance, p.nickname AS nickname \
         FROM vaccount a LEFT JOIN vprofile p ON p.account_id = a.id",
        "CREATE TEMP VIEW vaccount_ids AS SELECT id FROM vaccount_summary",
        "INSERT INTO vaccount (id, balance, label) VALUES (1, 100, 'a'), (2, 200, NULL)",
        "INSERT INTO vprofile (account_id, nickname) VALUES (1, 'ace')",
    ] {
        c.simple_query(ddl).await.expect("setup DDL");
    }
}

/// A `query!` over a simple projection view decodes its columns end-to-end.
#[tokio::test]
#[ignore = "requires local PG"]
async fn select_from_a_view_round_trips() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    setup(&mut c).await;

    let rows = c.query::<VSummaryQuery>(()).await.expect("view summary");
    let got: Vec<(i64, i32)> = rows
        .iter()
        .map(|r| {
            let rec = r.expect("row decodes");
            (rec.id, rec.balance)
        })
        .collect();
    assert_eq!(got, vec![(1, 100), (2, 200)]);

    c.close().await.expect("close");
}

/// NULLABILITY FIDELITY: a LEFT JOIN view column is decoded as `Option` — the
/// matched row carries `Some`, the unmatched row carries `None`. An under-nullify
/// would have this fail with `UnexpectedNull` on the second row.
#[tokio::test]
#[ignore = "requires local PG"]
async fn left_join_view_column_is_nullable() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    setup(&mut c).await;

    let rows = c.query::<VProfileQuery>(()).await.expect("view profile");
    let got: Vec<(i64, Option<String>)> = rows
        .iter()
        .map(|r| {
            let rec = r.expect("row decodes");
            (rec.id, rec.nickname.map(str::to_string))
        })
        .collect();
    assert_eq!(
        got,
        vec![(1, Some("ace".to_string())), (2, None)],
        "account 1 has a profile (Some), account 2 does not (None)"
    );

    c.close().await.expect("close");
}

/// A view OVER a view resolves and decodes.
#[tokio::test]
#[ignore = "requires local PG"]
async fn view_over_view_round_trips() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    setup(&mut c).await;

    let rows = c.query::<VIdsQuery>(()).await.expect("view over view");
    let got: Vec<i64> = rows
        .iter()
        .map(|r| r.expect("row decodes").id)
        .collect();
    assert_eq!(got, vec![1, 2]);

    c.close().await.expect("close");
}
