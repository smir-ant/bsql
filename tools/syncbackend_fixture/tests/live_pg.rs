//! LIVE proof: the SAME generic-over-backend data layer that runs in-process on
//! SQLite (the crate's offline tests) ALSO executes unchanged against a real
//! PostgreSQL through the blocking driver. Together the two arms are the
//! write-once cross-backend guarantee — one source, two backends, no `dyn`.
//!
//! The generic functions (`load_users`, the parameterized `user_by_id`, the
//! transaction combinator `wipe_in_tx`) are defined ONCE in the library and
//! called here with a concrete `pg_sync::Connection`.
//!
//! The assertions read the decoded column VALUES (the `query!`-generated record
//! fields are public), so this arm proves the generic functions decode the RIGHT
//! data against a live PostgreSQL — identical values to the in-process SQLite
//! tests, the write-once guarantee made concrete.
//!
//! The schema uses SESSION-TEMPORARY tables (`CREATE TEMP TABLE users/orders`),
//! which shadow the unqualified `users` / `orders` names for this connection and
//! auto-drop at disconnect — so the test needs no migration on the live server
//! and leaves no residue. Their column types match the migration catalog the
//! `query!` carriers were validated against.
//!
//! Run with:
//!   `cargo test -p bsql-syncbackend-fixture --test live_pg -- --ignored`
//! (needs PostgreSQL on localhost:5432, user `smir-ant`, database `postgres`,
//! trust auth — the same environment the other live suites use.)

#![forbid(unsafe_code)]
// Live integration harness: `.expect(..)` here is the loud test-failure signal
// (it panics, surfacing the failure), not a silent production fallback.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql::pg_sync::{ConnectConfig, Connection, SslMode};
use bsql_syncbackend_fixture::{
    find_user_by_email, load_users, load_users_and_orders, user_by_id, user_by_id_required,
    wipe_in_tx,
};

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Create the session-temporary schema (matching the migration catalog's column
/// types) and seed it, driving the SAME generic raw-SQL verb the library uses.
fn seed(conn: &mut Connection) {
    conn.execute_sql(
        "CREATE TEMP TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, name TEXT)",
    )
    .expect("create temp users");
    conn.execute_sql("CREATE TEMP TABLE orders (id BIGINT PRIMARY KEY, ref_no TEXT NOT NULL)")
        .expect("create temp orders");
    conn.execute_sql("INSERT INTO users VALUES (1, 'a@b', 'Alice'), (2, 'c@d', NULL)")
        .expect("insert users");
    conn.execute_sql("INSERT INTO orders VALUES (10, 'R-1'), (20, 'R-2')")
        .expect("insert orders");
}

/// The PARAM-FREE flagship `load_users<B>`, monomorphised for the LIVE PG
/// backend, actually runs and decodes both rows of the shared owned record twin.
#[test]
#[ignore = "requires local PG"]
fn param_free_flagship_executes_on_live_pg() {
    let mut conn = Connection::connect(&sync_config()).expect("connect");
    seed(&mut conn);

    // Decode the RIGHT values — identical to the in-process SQLite tests.
    let users = load_users(&mut conn).expect("load_users runs on live pg");
    assert_eq!(users.len(), 2);
    assert_eq!(users[0].id, 1);
    assert_eq!(users[0].email, "a@b");
    assert_eq!(users[0].name.as_deref(), Some("Alice"));
    assert_eq!(users[1].id, 2);
    assert_eq!(users[1].name, None);

    // The two-query helper also runs (proving the N-bounds shape live).
    let (u, o) = load_users_and_orders(&mut conn).expect("two-query helper on live pg");
    assert_eq!(u.len(), 2);
    assert_eq!(o.len(), 2);
    assert_eq!(o[0].ref_no, "R-1");
}

/// The PARAMETERIZED flagship `user_by_id<B>` — the SQLite `$N` param-bridge
/// payoff — runs unchanged on live PG, binding the SAME typed `(i64,)` tuple.
/// The param genuinely reaches the `WHERE` clause: a present id yields `Some`, an
/// absent one `None`.
#[test]
#[ignore = "requires local PG"]
fn parameterized_flagship_executes_on_live_pg() {
    let mut conn = Connection::connect(&sync_config()).expect("connect");
    seed(&mut conn);

    let alice = user_by_id(&mut conn, 1)
        .expect("user_by_id runs on live pg")
        .expect("user 1 exists");
    assert_eq!(alice.id, 1);
    assert_eq!(alice.email, "a@b");
    assert_eq!(alice.name.as_deref(), Some("Alice"));

    let bob = user_by_id(&mut conn, 2)
        .expect("user_by_id runs")
        .expect("user 2 exists");
    assert_eq!(bob.name, None);

    // The BORROWED-PARAM flagship: a RUNTIME `String` (not a `'static` literal)
    // binds on the compile-checked typed path — the `'static`-params wall closed.
    let email = String::from("a@b");
    let by_email = find_user_by_email(&mut conn, &email)
        .expect("find_user_by_email runs on live pg")
        .expect("alice exists");
    assert_eq!(by_email.id, 1);
    assert_eq!(by_email.email, "a@b");
    let missing = String::from("nobody@nowhere");
    assert!(
        find_user_by_email(&mut conn, &missing)
            .expect("runs")
            .is_none()
    );
    // A miss is a clean `None` (at-most-one contract) — proof the bound `(i64,)`
    // param filtered server-side.
    assert!(
        user_by_id(&mut conn, 999)
            .expect("user_by_id runs")
            .is_none()
    );

    // The cross-backend no-rows classification: `fetch_one` on an absent id is a
    // no-rows error `is_no_rows()` recognises identically to SQLite.
    assert!(
        user_by_id_required(&mut conn, 1)
            .expect("required load runs on live pg")
            .is_some()
    );
    assert!(
        user_by_id_required(&mut conn, 999)
            .expect("no-rows folds to None via is_no_rows")
            .is_none()
    );
}

/// The generic transaction combinator runs atomically on live PG.
#[test]
#[ignore = "requires local PG"]
fn transaction_combinator_executes_on_live_pg() {
    let mut conn = Connection::connect(&sync_config()).expect("connect");
    seed(&mut conn);
    conn.execute_sql("INSERT INTO users VALUES (3, '', 'ghost')")
        .expect("insert ghost");

    // Atomic raw-SQL group: deletes both orders and the one empty-email user.
    let affected = wipe_in_tx(&mut conn).expect("wipe_in_tx runs on live pg");
    assert_eq!(affected, 1);

    // The two real users survive; read back at connection level.
    let survivors = load_users(&mut conn).expect("load_users after tx");
    assert_eq!(survivors.len(), 2);
}
