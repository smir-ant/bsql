//! Heterogeneous atomic pipeline over the SQLite driver — the SEQUENTIAL twin of
//! the PostgreSQL `pipeline`. In-process (no server), so these run in the default
//! (non-`--ignored`) suite.
//!
//! # Scope note (honest): typed carriers are READ-ONLY here
//!
//! SQLite's build-time conformance oracle (feature `macros-sqlite`, on in this
//! fixture) validates each `query!` against a template database through a
//! DENY-ALL-BUT-READONLY authorizer, so a typed WRITE carrier (`INSERT`/`UPDATE`/
//! `DELETE ... RETURNING`) does not compile here — the SQLite typed flagship is
//! read-only. So the SQLite `pipeline` is a read-only ATOMIC batch: a consistent
//! multi-read snapshot inside one transaction. Its all-or-nothing rollback is
//! inherited VERBATIM from [`Connection::transaction`] (the pipeline is literally
//! `self.transaction(|tx| batch.run(tx))`), whose write-rollback atomicity is
//! proven by the driver's transaction tests + the PostgreSQL live all-or-nothing
//! proof (`pipeline_live_{async,sync}.rs`). These tests witness the typed-tuple
//! decode + the one-transaction snapshot.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "in-process test harness — expect/unwrap/panic surface failures loudly"
)]

use bsql::sqlite::{BindExt, Connection};

bsql::query!(PlWeightById, "SELECT weight FROM measurements WHERE id = $1");
bsql::query!(PlLabelById, "SELECT label FROM measurements WHERE id = $1");
bsql::query!(PlCountAll, "SELECT count(*) AS c FROM measurements");

const SCHEMA: &str = "CREATE TABLE measurements ( \
     id BIGINT PRIMARY KEY, label TEXT NOT NULL, weight DOUBLE PRECISION NOT NULL, \
     payload BYTEA, count BIGINT, note TEXT );";

fn seed() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch_sql(SCHEMA).expect("schema");
    conn.execute_sql("INSERT INTO measurements (id, label, weight) VALUES (1, 'one', 1.5)")
        .expect("seed row 1");
    conn.execute_sql("INSERT INTO measurements (id, label, weight) VALUES (2, 'two', 2.5)")
        .expect("seed row 2");
    conn
}

/// (a) A heterogeneous typed batch runs in ONE transaction, each command decoded
/// against ITS carrier's compile-time record shape into its own `TypedRows<Qi>`.
#[test]
fn heterogeneous_typed_batch_decodes_per_element() {
    let conn = seed();
    let (w, label, count) = conn
        .pipeline((
            PlWeightByIdQuery::bind((1,)),
            PlLabelByIdQuery::bind((2,)),
            PlCountAllQuery::bind(()),
        ))
        .expect("pipeline runs");

    assert!((w.iter().next().expect("row").expect("decode").weight - 1.5).abs() < f64::EPSILON);
    assert_eq!(label.iter().next().expect("row").expect("decode").label, "two");
    assert_eq!(count.iter().next().expect("row").expect("decode").c, 2);
    // The connection is reusable after a committed batch.
    assert_eq!(conn.query::<PlCountAllQuery>(()).expect("reuse").len(), 1);
}

/// A one-command pipeline is the degenerate (still-atomic) case, same typed tuple.
#[test]
fn single_command_pipeline() {
    let conn = seed();
    let (w,) = conn
        .pipeline((PlWeightByIdQuery::bind((1,)),))
        .expect("single-command pipeline");
    assert_eq!(w.len(), 1);
}
