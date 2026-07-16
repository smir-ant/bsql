//! Homogeneous typed-RETURNING `query_batch::<Q>` over the SQLite driver — the
//! SEQUENTIAL twin of the PostgreSQL `query_batch` (SAME method name on BOTH
//! backends). N runs of ONE `query!` carrier against N parameter sets, inside ONE
//! transaction, returning a GROUPED `Vec<TypedRows<Q>>` — one typed result per
//! command, KEEPING each command's rows (the typed-RETURNING peer of the
//! `execute_batch::<Q>` twin, which returns `Vec<u64>` counts). In-process (no
//! server), default suite.
//!
//! # Scope note (honest): typed carriers are READ-ONLY here
//!
//! SQLite's build-time conformance oracle (feature `macros-sqlite`, on in this
//! fixture) validates each `query!` through a DENY-ALL-BUT-READONLY authorizer, so a
//! typed WRITE carrier (`INSERT/UPDATE/DELETE ... RETURNING`) does NOT compile here —
//! the SQLite typed flagship is read-only. So `query_batch::<Q>` is a read-only ATOMIC
//! batch: N reads inside ONE transaction, each decoded into its own `TypedRows<Q>`.
//! Its all-or-nothing rollback is inherited VERBATIM from [`Connection::transaction`]
//! (the method is literally `self.transaction(|tx| …)`), whose write-rollback
//! atomicity is proven by the driver's transaction tests + the PostgreSQL live
//! all-or-nothing proof (`query_batch_live_{async,sync}.rs`). These tests witness the
//! grouped `Vec<TypedRows<Q>>` shape, per-command grouping, N == 0, and reusability.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "in-process test harness — expect/unwrap/panic surface failures loudly"
)]

use bsql::sqlite::Connection;

bsql::query!(QbWeightById, "SELECT weight FROM measurements WHERE id = $1");
// A SELECT whose row COUNT varies with `$1` — the grouping witness.
bsql::query!(QbUpToId, "SELECT id FROM measurements WHERE id <= $1 ORDER BY id");

const SCHEMA: &str = "CREATE TABLE measurements ( \
     id BIGINT PRIMARY KEY, label TEXT NOT NULL, weight DOUBLE PRECISION NOT NULL, \
     payload BYTEA, count BIGINT, note TEXT );";

fn seed() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch_sql(SCHEMA).expect("schema");
    conn.execute_sql("INSERT INTO measurements (id, label, weight) VALUES (1, 'one', 1.5)")
        .expect("seed 1");
    conn.execute_sql("INSERT INTO measurements (id, label, weight) VALUES (2, 'two', 2.5)")
        .expect("seed 2");
    conn.execute_sql("INSERT INTO measurements (id, label, weight) VALUES (3, 'three', 3.5)")
        .expect("seed 3");
    conn
}

/// N read carriers run as ONE atomic batch, returning one grouped `TypedRows<Q>` per
/// command with the correct DECODED values (not just counts); connection reusable.
#[test]
fn n_reads_return_grouped_decoded_typed_rows() {
    let conn = seed();
    let grouped = conn
        .query_batch::<QbWeightByIdQuery, _>(vec![(1_i64,), (2,), (3,)])
        .expect("batch runs");
    assert_eq!(grouped.len(), 3, "one TypedRows<Q> per command, in order");
    let weights: Vec<f64> = grouped
        .iter()
        .map(|rows| rows.iter().next().expect("row").expect("decode").weight)
        .collect();
    assert!((weights[0] - 1.5).abs() < f64::EPSILON);
    assert!((weights[1] - 2.5).abs() < f64::EPSILON);
    assert!((weights[2] - 3.5).abs() < f64::EPSILON);
    // Reusable after a committed batch.
    assert_eq!(conn.query::<QbWeightByIdQuery>((1,)).expect("reuse").len(), 1);
}

/// GROUPING: a multi-row-per-command batch keeps each command's rows in its OWN
/// `TypedRows<Q>` (the reason the return type is `Vec<TypedRows<Q>>`).
#[test]
fn grouping_is_preserved_per_command() {
    let conn = seed();
    let grouped = conn
        .query_batch::<QbUpToIdQuery, _>(vec![(1_i64,), (2,), (3,)])
        .expect("batch runs");
    assert_eq!(grouped.len(), 3);
    assert_eq!(grouped[0].len(), 1, "id <= 1 → 1 row (grouping intact)");
    assert_eq!(grouped[1].len(), 2, "id <= 2 → 2 rows (grouping intact)");
    assert_eq!(grouped[2].len(), 3, "id <= 3 → 3 rows (grouping intact)");
    let ids1: Vec<i64> = grouped[1].iter().map(|r| r.expect("decode").id).collect();
    assert_eq!(ids1, vec![1, 2], "command #1's own rows");
}

/// N == 0 → an empty `Vec` (an empty transaction).
#[test]
fn zero_is_empty() {
    let conn = seed();
    let grouped = conn
        .query_batch::<QbWeightByIdQuery, _>(Vec::<(i64,)>::new())
        .expect("N=0");
    assert!(grouped.is_empty());
}

/// Inside an explicit transaction guard the batch runs in the SAME transaction (no
/// nested BEGIN), and its grouped results flow out.
#[test]
fn inside_a_transaction_guard() {
    let conn = seed();
    let grouped = conn
        .transaction(|tx| tx.query_batch::<QbWeightByIdQuery, _>(vec![(1_i64,), (2,)]))
        .expect("guard commits");
    assert_eq!(grouped.len(), 2);
}
