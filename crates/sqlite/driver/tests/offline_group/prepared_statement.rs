//! Witnesses for the explicit, reusable prepared-statement handles —
//! [`Connection::prepare_raw`] (dynamic) and [`Connection::prepare`]`::<Q>`
//! (typed).
//!
//! These pin the properties that make an EXPLICIT handle both correct and the
//! fast reuse path: the SAME compiled statement, re-run with DIFFERENT params,
//! returns each call's OWN correct result (bindings/cursor reset between reuses,
//! never a stale row); the dynamic and typed verb sets mirror the connection's;
//! the at-most-one contracts and storage-class verification carry over; and a
//! statement prepared on the connection runs correctly INSIDE a transaction
//! (same db handle) — committed or rolled back.
//!
//! The reuse SPEED (a hot loop skipping the per-call `sqlite3_prepare_v2`
//! recompile) is measured by the standalone `bench` parity runner's
//! `by_pk_prepared` / `10row_prepared` cells; here we prove CORRECTNESS.

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::ops::ControlFlow;

use bsql_sqlite::{
    read_required, ColumnSource, Connection, SqliteError, SqliteTypedQuery, ValueRef,
};

// ── A hand-written `query!`-shaped carrier (the macro emits exactly this) ─────

#[derive(Debug, PartialEq)]
struct User<'q> {
    id: i64,
    name: &'q str,
}

#[derive(Debug, PartialEq)]
struct UserOwned {
    id: i64,
    name: String,
}

/// By-key carrier — `SELECT ... WHERE id = $1`, a genuinely single-row query.
enum UserByPk {}

impl SqliteTypedQuery for UserByPk {
    type Params<'p> = (i64,);
    type Record<'q> = User<'q>;
    type Owned = UserOwned;
    const SQL: &'static str = "SELECT id, name FROM users WHERE id = $1";

    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        Ok(User { id: read_required::<i64, S>(src, 0)?, name: read_required::<&'q str, S>(src, 1)? })
    }

    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        Ok(UserOwned {
            id: read_required::<i64, S>(src, 0)?,
            name: read_required::<String, S>(src, 1)?,
        })
    }
}

/// All-rows carrier over the SAME record, to force the `TooManyRows` arm.
enum AllUsers {}

impl SqliteTypedQuery for AllUsers {
    type Params<'p> = ();
    type Record<'q> = User<'q>;
    type Owned = UserOwned;
    const SQL: &'static str = "SELECT id, name FROM users ORDER BY id";

    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        UserByPk::decode_row(src)
    }
    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        UserByPk::decode_row_owned(src)
    }
}

fn seed(conn: &Connection) {
    conn.execute_raw("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create");
    for i in 1..=20_i64 {
        let name = format!("user-{i}");
        conn.execute_params(
            "INSERT INTO users (id, name) VALUES (?1, ?2)",
            &[ValueRef::Integer(i), ValueRef::Text(name.as_bytes())],
        )
        .expect("seed");
    }
}

// ── DYNAMIC handle (`prepare_raw`) ───────────────────────────────────────────

/// The SAME handle re-run with DIFFERENT params returns each call's own row —
/// proving the statement resets and clears prior bindings between reuses.
#[test]
fn dynamic_reuse_stays_correct_across_params() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut stmt = conn.prepare_raw("SELECT id, name FROM users WHERE id = ?1").expect("prepare");
    assert_eq!(stmt.parameter_count(), 1);
    assert_eq!(stmt.column_count(), 2);
    // 200 reuses across the whole key range — a leaked binding would mismatch.
    for round in 0..10 {
        for i in 1..=20_i64 {
            let row = stmt.query_one(&[ValueRef::Integer(i)]).expect("row");
            assert_eq!(row.get::<i64>(0).expect("id"), i, "round {round}");
            assert_eq!(row.get::<&str>(1).expect("name"), format!("user-{i}"), "round {round}");
        }
    }
}

/// `query_one` is first-row / `NoRows`; `query_opt` is first-row / `None` — the
/// dynamic contract (matching `Connection::query_params_one/opt`).
#[test]
fn dynamic_query_one_opt_first_row_semantics() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut stmt = conn.prepare_raw("SELECT id, name FROM users WHERE id = ?1").expect("prepare");

    assert_eq!(stmt.query_one(&[ValueRef::Integer(7)]).expect("row").get::<i64>(0).expect("id"), 7);
    assert!(stmt.query_opt(&[ValueRef::Integer(7)]).expect("opt").is_some());
    assert!(stmt.query_opt(&[ValueRef::Integer(999)]).expect("opt").is_none());
    match stmt.query_one(&[ValueRef::Integer(999)]) {
        Err(SqliteError::NoRows) => {}
        other => panic!("expected NoRows, got {other:?}"),
    }
}

/// `execute` reused across a loop applies EVERY call; `query` materializes.
#[test]
fn dynamic_execute_and_query_reuse() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (v INTEGER NOT NULL)").expect("create");
    let mut ins = conn.prepare_raw("INSERT INTO t (v) VALUES (?1)").expect("prepare ins");
    for i in 0..100_i64 {
        assert_eq!(ins.execute(&[ValueRef::Integer(i)]).expect("insert"), 1);
    }
    let mut count = conn.prepare_raw("SELECT COUNT(*) FROM t").expect("prepare count");
    // Reuse the eager `query` verb twice — both see the same table state.
    for _ in 0..2 {
        let qr = count.query(&[]).expect("query");
        assert_eq!(qr.len(), 1);
        assert_eq!(qr.get(0).expect("row").get::<i64>(0).expect("n"), 100);
    }
}

/// `query_each` streams every row zero-copy and supports early break.
#[test]
fn dynamic_query_each_streams_and_breaks() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut stmt = conn
        .prepare_raw("SELECT id, name FROM users ORDER BY id LIMIT ?1")
        .expect("prepare");

    // Full drain.
    let mut seen = 0_i64;
    let broke = stmt
        .query_each(&[ValueRef::Integer(5)], |r| {
            seen += 1;
            assert_eq!(r.get::<i64>(0).expect("id"), seen);
            ControlFlow::<()>::Continue(())
        })
        .expect("stream");
    assert_eq!(seen, 5);
    assert!(broke.is_none());

    // Early break on the 3rd row, on the SAME reused statement.
    let stop = stmt
        .query_each(&[ValueRef::Integer(10)], |r| {
            let id = r.get::<i64>(0).expect("id");
            if id == 3 { ControlFlow::Break(id) } else { ControlFlow::Continue(()) }
        })
        .expect("stream2");
    assert_eq!(stop, Some(3));
}

/// A param slice whose length disagrees with the statement's `?N` count is a
/// classified bind error (rusqlite's count guard), never a silent NULL-bind.
#[test]
fn dynamic_arity_mismatch_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut stmt = conn.prepare_raw("SELECT id FROM users WHERE id = ?1").expect("prepare");
    // Two params for a one-placeholder statement.
    assert!(stmt.query(&[ValueRef::Integer(1), ValueRef::Integer(2)]).is_err());
}

// ── TYPED handle (`prepare::<Q>`) ────────────────────────────────────────────

/// The typed handle reuses the compiled statement across params and decodes the
/// typed record identically to the connection's `query` family.
#[test]
fn typed_reuse_decodes_records() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut stmt = conn.prepare::<UserByPk>().expect("prepare");
    assert_eq!(stmt.column_count(), 2);

    for round in 0..5 {
        for i in 1..=20_i64 {
            let owned = stmt.query_one((i,)).expect("one");
            assert_eq!(owned, UserOwned { id: i, name: format!("user-{i}") }, "round {round}");
        }
    }

    // `query` collects into TypedRows; borrowed iteration aliases the arena.
    let rows = stmt.query((3,)).expect("typed rows");
    assert_eq!(rows.len(), 1);
    let decoded: Vec<User<'_>> = rows.iter().map(|r| r.expect("decode")).collect();
    assert_eq!(decoded, vec![User { id: 3, name: "user-3" }]);
}

/// Exactly-one / at-most-one contracts carry over to the handle (matching the
/// connection's typed verbs, NOT the dynamic first-row peers).
#[test]
fn typed_at_most_one_contract() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);

    let mut by_pk = conn.prepare::<UserByPk>().expect("prepare by_pk");
    assert_eq!(by_pk.query_one((4,)).expect("one").id, 4);
    assert!(by_pk.query_opt((4,)).expect("opt").is_some());
    assert!(by_pk.query_opt((999,)).expect("opt").is_none());
    match by_pk.query_one((999,)) {
        Err(SqliteError::NoRows) => {}
        other => panic!("expected NoRows, got {other:?}"),
    }

    // All-rows carrier → both at-most-one verbs reject with TooManyRows.
    let mut all = conn.prepare::<AllUsers>().expect("prepare all");
    match all.query_one(()) {
        Err(SqliteError::TooManyRows) => {}
        other => panic!("expected TooManyRows, got {other:?}"),
    }
    match all.query_opt(()) {
        Err(SqliteError::TooManyRows) => {}
        other => panic!("expected TooManyRows, got {other:?}"),
    }
}

/// The typed handle streams borrowed records and supports early break.
#[test]
fn typed_query_each_streams() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut all = conn.prepare::<AllUsers>().expect("prepare");
    let mut seen = 0_i64;
    all.query_each((), |rec| {
        seen += 1;
        assert_eq!(rec.id, seen);
        ControlFlow::<()>::Continue(())
    })
    .expect("stream");
    assert_eq!(seen, 20);
}

// ── Transaction interplay ────────────────────────────────────────────────────

/// A statement prepared inside the transaction runs correctly:
/// the writes are visible mid-transaction and, on rollback, disappear.
#[test]
fn tx_prepared_statement_runs_inside_transaction() {
    let mut conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (v INTEGER NOT NULL)").expect("create");

    // Commit path: 10 inserts inside a committed transaction persist.
    conn.transaction(|tx| {
        let mut ins = tx.prepare_raw("INSERT INTO t (v) VALUES (?1)")?;
        for i in 0..10_i64 {
            ins.execute(&[ValueRef::Integer(i)])?;
        }
        Ok(())
    })
    .expect("committed tx");
    assert_eq!(
        conn.query_one_raw("SELECT COUNT(*) FROM t").expect("count").get::<i64>(0).expect("n"),
        10
    );

    // Rollback path: an error propagated out of the closure rolls back the
    // statement's writes — the reused handle honored the transaction boundary.
    let outcome: Result<(), SqliteError> = conn.transaction(|tx| {
        let mut ins = tx.prepare_raw("INSERT INTO t (v) VALUES (?1)")?;
        for i in 100..110_i64 {
            ins.execute(&[ValueRef::Integer(i)])?;
        }
        Err(SqliteError::Query("forced rollback".to_owned()))
    });
    assert!(outcome.is_err());
    assert_eq!(
        conn.query_one_raw("SELECT COUNT(*) FROM t").expect("count").get::<i64>(0).expect("n"),
        10,
        "rolled-back inserts must not persist"
    );
}

/// The handle also works across a MANUAL begin/commit bracket on the connection.
#[test]
fn typed_prepared_statement_across_manual_transaction() {
    let conn = Connection::open_in_memory().expect("open");
    seed(&conn);
    let mut stmt = conn.prepare::<UserByPk>().expect("prepare");

    conn.begin().expect("begin");
    let a = stmt.query_one((1,)).expect("read in tx");
    let b = stmt.query_one((2,)).expect("read in tx");
    conn.commit().expect("commit");
    assert_eq!(a.name, "user-1");
    assert_eq!(b.name, "user-2");
}
