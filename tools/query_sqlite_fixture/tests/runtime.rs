//! End-to-end proof of the SQLite typed-flagship RUNTIME through the REAL
//! `query!` macro.
//!
//! Each `query!` below is typed at build time against the migration-replayed
//! catalog AND cross-checked against the SQLite template (the conformance
//! oracle), then — because this fixture enables the `sqlite` runtime driver —
//! the macro emits a `SqliteTypedQuery` impl on the carrier. These tests open a
//! fresh in-memory SQLite, apply the same schema, and execute the typed verbs,
//! decoding into the macro's typed records. That this compiles AND round-trips
//! is the end-to-end proof that a consumer can EXECUTE a compile-checked
//! `query!` against `bsql::sqlite::Connection` with the same guarantees the
//! PostgreSQL path has (typed record fields, nullability honored) plus SQLite's
//! runtime storage-class verification.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration test — unwrap/expect/panic are the loud failure signal; the in-tests \
              carve-out does not reach the non-#[test] setup helper, so this reasoned allow does"
)]

use core::ops::ControlFlow;

use bsql::sqlite::{Connection, ValueRef};

// Every storage class, borrowed + owned twins emitted by the macro. `label` is a
// borrowed `&'q str`, `payload` a borrowed `&'q [u8]`; the nullable columns
// (`payload` / `count` / `note`) are `Option<_>`.
bsql::query!(
    Measurement,
    "SELECT id, label, weight, payload, count, note FROM measurements ORDER BY id"
);

// A `$1` positional parameter, bound by the runtime `&[ValueRef]` slice.
bsql::query!(
    MeasurementById,
    "SELECT id, label, weight, payload, count, note FROM measurements WHERE id = $1"
);

// An all-scalar row (no borrowing column): the borrowed record is lifetime-free.
bsql::query!(WeightById, "SELECT weight FROM measurements WHERE id = $1");

const SCHEMA: &str = "CREATE TABLE measurements ( \
     id BIGINT PRIMARY KEY, label TEXT NOT NULL, weight DOUBLE PRECISION NOT NULL, \
     payload BYTEA, count BIGINT, note TEXT );";

fn seed() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch(SCHEMA).expect("schema");
    // Row 1: every column present.
    conn.execute_params(
        "INSERT INTO measurements VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            ValueRef::Integer(1),
            ValueRef::Text(b"alpha"),
            ValueRef::Real(1.5),
            ValueRef::Blob(&[0xAA, 0xBB]),
            ValueRef::Integer(100),
            ValueRef::Text(b"first"),
        ],
    )
    .expect("insert 1");
    // Row 2: the three nullable columns are a real NULL.
    conn.execute_params(
        "INSERT INTO measurements VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            ValueRef::Integer(2),
            ValueRef::Text(b"beta"),
            ValueRef::Real(2.5),
            ValueRef::Null,
            ValueRef::Null,
            ValueRef::Null,
        ],
    )
    .expect("insert 2");
    conn
}

#[test]
fn typed_query_round_trips_every_storage_class_borrowed() {
    let conn = seed();
    let rows = conn.query::<MeasurementQuery>(&[]).expect("typed query");
    assert_eq!(rows.len(), 2);

    let decoded: Vec<_> = rows.iter().map(|r| r.expect("decode")).collect();

    // Row 1: INTEGER, TEXT (borrowed &str), REAL, BLOB (borrowed &[u8]), all present.
    assert_eq!(decoded[0].id, 1);
    assert_eq!(decoded[0].label, "alpha");
    assert!((decoded[0].weight - 1.5).abs() < f64::EPSILON);
    assert_eq!(decoded[0].payload, Some(&[0xAA, 0xBB][..]));
    assert_eq!(decoded[0].count, Some(100));
    assert_eq!(decoded[0].note, Some("first"));

    // Row 2: NULL decodes as `None` for the nullable columns.
    assert_eq!(decoded[1].id, 2);
    assert_eq!(decoded[1].label, "beta");
    assert_eq!(decoded[1].payload, None);
    assert_eq!(decoded[1].count, None);
    assert_eq!(decoded[1].note, None);
}

#[test]
fn typed_query_into_owned_copies_text_and_blob() {
    let conn = seed();
    let owned = conn.query::<MeasurementQuery>(&[]).expect("query").into_owned().expect("owned");
    assert_eq!(owned[0].label, "alpha".to_owned());
    assert_eq!(owned[0].payload, Some(vec![0xAA, 0xBB]));
    assert_eq!(owned[0].note, Some("first".to_owned()));
    assert_eq!(owned[1].note, None);
}

#[test]
fn typed_query_one_and_opt_with_param() {
    let conn = seed();
    // `query_one` with a bound `$1` returns the owned record for that key.
    let one = conn.query_one::<MeasurementByIdQuery>(&[ValueRef::Integer(2)]).expect("one");
    assert_eq!(one.id, 2);
    assert_eq!(one.count, None);

    // `query_opt` is `None` for an absent key, `Some` for a present one.
    assert!(conn
        .query_opt::<MeasurementByIdQuery>(&[ValueRef::Integer(99)])
        .expect("opt")
        .is_none());
    assert!(conn
        .query_opt::<MeasurementByIdQuery>(&[ValueRef::Integer(1)])
        .expect("opt")
        .is_some());

    // An all-scalar (lifetime-free) typed record over a param query.
    let w = conn.query_one::<WeightByIdQuery>(&[ValueRef::Integer(1)]).expect("weight");
    assert!((w.weight - 1.5).abs() < f64::EPSILON);
}

#[test]
fn typed_query_one_and_opt_enforce_at_most_one() {
    let conn = seed(); // two rows
    // `Measurement` selects both rows, so the TYPED at-most-one verbs reject it
    // with the classified TooManyRows — the SAME contract the PostgreSQL typed
    // `query_one` / `query_opt` enforce, so a query ported PostgreSQL→SQLite keeps
    // its multi-row semantics (the dynamic `*_sql` verbs stay first-row).
    match conn.query_one::<MeasurementQuery>(&[]) {
        Err(bsql::sqlite::SqliteError::TooManyRows) => {}
        other => panic!("expected TooManyRows from query_one, got {other:?}"),
    }
    match conn.query_opt::<MeasurementQuery>(&[]) {
        Err(bsql::sqlite::SqliteError::TooManyRows) => {}
        other => panic!("expected TooManyRows from query_opt, got {other:?}"),
    }
}

#[test]
fn typed_query_each_streams() {
    let conn = seed();
    let mut ids = Vec::new();
    let out = conn
        .query_each::<MeasurementQuery, _, ()>(&[], |rec| {
            ids.push(rec.id);
            ControlFlow::Continue(())
        })
        .expect("stream");
    assert_eq!(out, None);
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn typed_verbs_on_the_transaction_guard() {
    let conn = seed();
    let ids: Vec<i64> = conn
        .transaction(|tx| {
            let rows = tx.query::<MeasurementQuery>(&[]).expect("typed in tx");
            Ok(rows.iter().map(|r| r.expect("decode").id).collect())
        })
        .expect("transaction");
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn storage_class_mismatch_is_a_classified_error() {
    let conn = seed();
    // A hostile row: bind a TEXT into the REAL-affinity `weight` column. SQLite's
    // affinity keeps a non-numeric string as TEXT, so the stored `weight` is a
    // TEXT value — the record declares `weight: f64`, so decoding it is a
    // classified TypeMismatch (REAL expected, TEXT found), never a coercion.
    conn.execute_params(
        "INSERT INTO measurements VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            ValueRef::Integer(3),
            ValueRef::Text(b"gamma"),
            ValueRef::Text(b"not a number"),
            ValueRef::Null,
            ValueRef::Null,
            ValueRef::Null,
        ],
    )
    .expect("insert hostile");

    let rows = conn.query::<MeasurementQuery>(&[]).expect("query");
    // Rows 0 and 1 decode fine; row 2's `weight` is the classified mismatch.
    let third = rows.iter().nth(2).expect("third row item");
    match third {
        Err(bsql::sqlite::SqliteError::TypeMismatch { column, expected, found }) => {
            assert_eq!(column, 2); // weight is the 3rd projected column (0-based)
            assert_eq!(expected.to_string(), "REAL");
            assert_eq!(found.to_string(), "TEXT");
        }
        other => panic!("expected TypeMismatch, got {other:?}"),
    }
}
