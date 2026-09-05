//! Offline proof of the SQLite typed-flagship RUNTIME, independent of the
//! `query!` macro.
//!
//! The macro emits a `SqliteTypedQuery` impl per query; here we hand-write the
//! exact shape it emits (borrowed + owned record twins, `decode_row` /
//! `decode_row_owned` routing every field through `read_required` /
//! `read_optional`) and drive the driver's typed verbs against a real in-memory
//! SQLite. This exercises the runtime seam — `ColumnSource`, `TypedRows`, the
//! `query` / `query_one` / `query_opt` / `query_each` verbs, and the transaction
//! guard's typed peers — with no proc-macro in the loop, so a runtime regression
//! is caught even by a workspace member that never invokes `query!`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::ops::ControlFlow;

use bsql_sqlite::{
    read_optional, read_required, ColumnSource, Connection, SqliteError, SqliteTypedQuery, ValueRef,
};

// ─── A row with every SQLite storage class + a nullable column ───────────────

#[derive(Debug, PartialEq)]
struct Cell<'q> {
    i: i64,
    r: f64,
    t: &'q str,
    b: &'q [u8],
    n: Option<i64>,
}

#[derive(Debug, PartialEq)]
struct CellOwned {
    i: i64,
    r: f64,
    t: String,
    b: Vec<u8>,
    n: Option<i64>,
}

enum CellQuery {}

impl SqliteTypedQuery for CellQuery {
    type Params<'p> = ();
    type Record<'q> = Cell<'q>;
    type Owned = CellOwned;
    const SQL: &'static str = "SELECT i, r, t, b, n FROM cells ORDER BY i";

    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        Ok(Cell {
            i: read_required::<i64, S>(src, 0)?,
            r: read_required::<f64, S>(src, 1)?,
            t: read_required::<&'q str, S>(src, 2)?,
            b: read_required::<&'q [u8], S>(src, 3)?,
            n: read_optional::<i64, S>(src, 4)?,
        })
    }

    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        Ok(CellOwned {
            i: read_required::<i64, S>(src, 0)?,
            r: read_required::<f64, S>(src, 1)?,
            t: read_required::<String, S>(src, 2)?,
            b: read_required::<Vec<u8>, S>(src, 3)?,
            n: read_optional::<i64, S>(src, 4)?,
        })
    }
}

fn seed_cells(conn: &Connection) {
    conn.execute_raw("CREATE TABLE cells (i INTEGER, r REAL, t TEXT, b BLOB, n INTEGER)")
        .expect("create");
    conn.execute_params(
        "INSERT INTO cells VALUES ($1, $2, $3, $4, $5)",
        &[
            ValueRef::Integer(1),
            ValueRef::Real(1.5),
            ValueRef::Text(b"one"),
            ValueRef::Blob(&[0xDE, 0xAD]),
            ValueRef::Integer(10),
        ],
    )
    .expect("insert 1");
    conn.execute_params(
        "INSERT INTO cells VALUES ($1, $2, $3, $4, $5)",
        &[
            ValueRef::Integer(2),
            ValueRef::Real(2.5),
            ValueRef::Text(b"two"),
            ValueRef::Blob(&[0xBE, 0xEF]),
            ValueRef::Null, // nullable column carries a real NULL
        ],
    )
    .expect("insert 2");
}

#[test]
fn query_iter_decodes_all_storage_classes_borrowed() {
    let conn = Connection::open_in_memory().expect("open");
    seed_cells(&conn);

    let rows = conn.query::<CellQuery>(()).expect("typed query");
    assert_eq!(rows.len(), 2);
    assert!(!rows.is_empty());

    let decoded: Vec<Cell<'_>> = rows.iter().map(|r| r.expect("decode")).collect();
    assert_eq!(
        decoded,
        vec![
            Cell { i: 1, r: 1.5, t: "one", b: &[0xDE, 0xAD], n: Some(10) },
            Cell { i: 2, r: 2.5, t: "two", b: &[0xBE, 0xEF], n: None },
        ]
    );
}

#[test]
fn query_into_owned_decodes_owned_twin() {
    let conn = Connection::open_in_memory().expect("open");
    seed_cells(&conn);

    let owned = conn.query::<CellQuery>(()).expect("typed query").into_owned().expect("owned");
    assert_eq!(
        owned,
        vec![
            CellOwned { i: 1, r: 1.5, t: "one".to_owned(), b: vec![0xDE, 0xAD], n: Some(10) },
            CellOwned { i: 2, r: 2.5, t: "two".to_owned(), b: vec![0xBE, 0xEF], n: None },
        ]
    );
}

// A by-key carrier over the SAME record as `CellQuery`, so `query_one` /
// `query_opt` can be exercised on a genuinely single-row query. Decode delegates
// to `CellQuery`'s methods (one decode, no drift).
enum OneCellQuery {}

impl SqliteTypedQuery for OneCellQuery {
    type Params<'p> = (i64,);
    type Record<'q> = Cell<'q>;
    type Owned = CellOwned;
    const SQL: &'static str = "SELECT i, r, t, b, n FROM cells WHERE i = $1";

    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        CellQuery::decode_row(src)
    }

    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        CellQuery::decode_row_owned(src)
    }
}

#[test]
fn query_one_and_query_opt_enforce_at_most_one() {
    let conn = Connection::open_in_memory().expect("open");
    seed_cells(&conn); // two rows (i = 1, 2)

    // A single-row query: `query_one` returns it, `query_opt` is `Some`.
    let one = conn.query_one::<OneCellQuery>((1i64,)).expect("one");
    assert_eq!(one.i, 1);
    assert!(conn.query_opt::<OneCellQuery>((1i64,)).expect("opt").is_some());

    // Zero rows: `query_opt` is `Ok(None)`, `query_one` is the classified no-rows
    // error (the peer of PostgreSQL's `DriverError::NoRows`).
    assert!(conn.query_opt::<OneCellQuery>((999i64,)).expect("opt").is_none());
    match conn.query_one::<OneCellQuery>((999i64,)) {
        Err(SqliteError::NoRows) => {}
        other => panic!("expected NoRows, got {other:?}"),
    }

    // TWO rows (`CellQuery` selects all): BOTH typed verbs reject with the
    // classified TooManyRows — the exactly-one / at-most-one contract, matching
    // the PostgreSQL typed verbs (not first-row like the dynamic *_sql peers).
    match conn.query_one::<CellQuery>(()) {
        Err(SqliteError::TooManyRows) => {}
        other => panic!("expected TooManyRows, got {other:?}"),
    }
    match conn.query_opt::<CellQuery>(()) {
        Err(SqliteError::TooManyRows) => {}
        other => panic!("expected TooManyRows, got {other:?}"),
    }
}

#[test]
fn query_each_streams_borrowed_records() {
    let conn = Connection::open_in_memory().expect("open");
    seed_cells(&conn);

    let mut seen: Vec<i64> = Vec::new();
    let out = conn
        .query_each::<CellQuery, _, ()>((), |rec| {
            seen.push(rec.i);
            ControlFlow::Continue(())
        })
        .expect("stream");
    assert_eq!(out, None); // every row streamed
    assert_eq!(seen, vec![1, 2]);

    // Early break carries the payload.
    let stop = conn
        .query_each::<CellQuery, _, &str>((), |rec| {
            if rec.i == 1 {
                ControlFlow::Break("stopped at one")
            } else {
                ControlFlow::Continue(())
            }
        })
        .expect("stream break");
    assert_eq!(stop, Some("stopped at one"));
}

// ─── Storage-class mismatch is a classified error, never a silent coercion ───

#[derive(Debug, PartialEq)]
struct IntRow {
    v: i64,
}

enum IntRowQuery {}

impl SqliteTypedQuery for IntRowQuery {
    type Params<'p> = ();
    type Record<'q> = IntRow;
    type Owned = IntRow;
    const SQL: &'static str = "SELECT v FROM t";

    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
    }

    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
    }
}

#[test]
fn storage_class_mismatch_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (v)").expect("create"); // no affinity — stores what it gets
    conn.execute_params("INSERT INTO t VALUES ($1)", &[ValueRef::Text(b"not an int")])
        .expect("insert text");

    // The record declares `v: i64`, but a TEXT value arrives — a classified
    // TypeMismatch (INTEGER expected, TEXT found), never a coercion.
    match conn.query::<IntRowQuery>(()).expect("query").iter().next().expect("one row") {
        Err(SqliteError::TypeMismatch { column, expected, found }) => {
            assert_eq!(column, 0);
            assert_eq!(expected.to_string(), "INTEGER");
            assert_eq!(found.to_string(), "TEXT");
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
    // The same mismatch on the direct-decode path (`query_one`).
    match conn.query_one::<IntRowQuery>(()) {
        Err(SqliteError::TypeMismatch { .. }) => {}
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
}

#[test]
fn null_in_non_null_field_is_classified() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE t (v)").expect("create");
    conn.execute_raw("INSERT INTO t VALUES (NULL)").expect("insert null");

    // The record declares `v: i64` (non-Option), but the value is NULL — a
    // classified UnexpectedNull, distinct from a type mismatch.
    match conn.query::<IntRowQuery>(()).expect("query").iter().next().expect("one row") {
        Err(SqliteError::UnexpectedNull { column }) => assert_eq!(column, 0),
        other => panic!("expected UnexpectedNull, got {other:?}"),
    }
}

// ─── Parameter binding + the transaction guard's typed verbs ─────────────────

#[test]
fn typed_query_binds_params_and_runs_in_a_transaction() {
    let mut conn = Connection::open_in_memory().expect("open");
    seed_cells(&conn);

    // A `$1` parameter binds by position; a positive filter selects one row.
    enum ByIQuery {}
    impl SqliteTypedQuery for ByIQuery {
        type Params<'p> = (i64,);
        type Record<'q> = IntRow;
        type Owned = IntRow;
        const SQL: &'static str = "SELECT i FROM cells WHERE i = $1";
        fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
            Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
        }
        fn decode_row_owned<'a, S: ColumnSource<'a>>(
            src: &S,
        ) -> Result<Self::Owned, SqliteError> {
            Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
        }
    }

    let hit = conn.query_one::<ByIQuery>((2i64,)).expect("param query");
    assert_eq!(hit.v, 2);

    // The same typed verbs are exposed on the transaction guard.
    let inside: Vec<i64> = conn
        .transaction(|tx| {
            let rows = tx.query::<CellQuery>(()).expect("typed in tx");
            Ok(rows.into_owned().expect("owned in tx").into_iter().map(|c| c.i).collect())
        })
        .expect("transaction");
    assert_eq!(inside, vec![1, 2]);
}

// A MULTI-parameter INSERT ... RETURNING. The typed param tuple `(i64, &str)`
// binds each value in its true storage class onto the write (arity > 1), and
// RETURNING decodes the result straight back into the typed record — the
// write-path peer of the by-key read, with the SAME typed `Q::Params` the
// PostgreSQL path takes (no `&[ValueRef]`).
enum InsertRetQuery {}
impl SqliteTypedQuery for InsertRetQuery {
    type Params<'p> = (i64, &'static str);
    type Record<'q> = IntRow;
    type Owned = IntRow;
    const SQL: &'static str =
        "INSERT INTO cells (i, r, t, b, n) VALUES (?1, 9.0, ?2, x'AB', NULL) RETURNING i";
    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
    }
    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
    }
}

#[test]
fn typed_multi_param_insert_returning_binds_and_decodes() {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_raw("CREATE TABLE cells (i INTEGER, r REAL, t TEXT, b BLOB, n INTEGER)")
        .expect("create");

    // Two typed params bind positionally; RETURNING decodes the new key.
    let ret = conn.query_one::<InsertRetQuery>((42i64, "gamma")).expect("insert returning");
    assert_eq!(ret.v, 42);

    // The `&str` param really persisted in its TEXT storage class (read it back).
    let back = conn.query_one_raw("SELECT t FROM cells WHERE i = 42").expect("read back");
    assert_eq!(back.get::<&str>(0).expect("text"), "gamma");
}

// A NULLABLE parameter (`Option<i64>`): `None` binds SQL `NULL` (disabling the
// `?1 IS NULL OR …` filter — every row), `Some(v)` binds the value (filters).
// Proves the NULL-parameter path end-to-end through the typed verb — the bind
// twin of a nullable RECORD field.
enum NullFilterQuery {}
impl SqliteTypedQuery for NullFilterQuery {
    type Params<'p> = (Option<i64>,);
    type Record<'q> = IntRow;
    type Owned = IntRow;
    const SQL: &'static str = "SELECT i FROM cells WHERE (?1 IS NULL OR i = ?1) ORDER BY i";
    fn decode_row<'q, S: ColumnSource<'q>>(src: &S) -> Result<Self::Record<'q>, SqliteError> {
        Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
    }
    fn decode_row_owned<'a, S: ColumnSource<'a>>(src: &S) -> Result<Self::Owned, SqliteError> {
        Ok(IntRow { v: read_required::<i64, S>(src, 0)? })
    }
}

#[test]
fn typed_nullable_param_binds_null_and_value() {
    let conn = Connection::open_in_memory().expect("open");
    seed_cells(&conn); // i = 1, 2

    // `None` → the parameter is SQL NULL → the filter is disabled → both rows.
    let all: Vec<i64> = conn
        .query::<NullFilterQuery>((None,))
        .expect("none param")
        .into_owned()
        .expect("owned")
        .into_iter()
        .map(|r| r.v)
        .collect();
    assert_eq!(all, vec![1, 2]);

    // `Some(2)` → the filter is active → exactly one row.
    let one = conn.query_one::<NullFilterQuery>((Some(2i64),)).expect("some param");
    assert_eq!(one.v, 2);
}
