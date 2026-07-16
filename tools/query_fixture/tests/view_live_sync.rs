//! LIVE `query!`-against-a-VIEW round-trip over the SYNC (blocking) driver — the
//! sync twin of `view_live_async.rs`. Same TEMP-shadow setup, same carriers,
//! same assertions, minus `.await`. See that file for the design notes.
//!
//! Run with: `cargo test -p bsql-query-fixture --test view_live_sync -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql_postgres_sync::{ConnectConfig, Connection, SslMode};

bsql::query!(VSummary, "SELECT id, balance FROM vaccount_summary ORDER BY id");
bsql::query!(
    VProfile,
    "SELECT id, balance, nickname FROM vaccount_profile ORDER BY id"
);
bsql::query!(VIds, "SELECT id FROM vaccount_ids ORDER BY id");

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

fn setup(c: &mut Connection) {
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
        c.simple_query(ddl).expect("setup DDL");
    }
}

#[test]
#[ignore = "requires local PG"]
fn select_from_a_view_round_trips() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    setup(&mut c);

    let rows = c.query::<VSummaryQuery>(()).expect("view summary");
    let got: Vec<(i64, i32)> = rows
        .iter()
        .map(|r| {
            let rec = r.expect("row decodes");
            (rec.id, rec.balance)
        })
        .collect();
    assert_eq!(got, vec![(1, 100), (2, 200)]);

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn left_join_view_column_is_nullable() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    setup(&mut c);

    let rows = c.query::<VProfileQuery>(()).expect("view profile");
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

    c.close().expect("close");
}

#[test]
#[ignore = "requires local PG"]
fn view_over_view_round_trips() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    setup(&mut c);

    let rows = c.query::<VIdsQuery>(()).expect("view over view");
    let got: Vec<i64> = rows.iter().map(|r| r.expect("row decodes").id).collect();
    assert_eq!(got, vec![1, 2]);

    c.close().expect("close");
}
