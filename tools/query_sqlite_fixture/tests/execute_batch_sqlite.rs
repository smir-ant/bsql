//! Homogeneous typed `execute_batch::<Q>` over the SQLite driver — the SEQUENTIAL twin
//! of the PostgreSQL `execute_batch` (SAME method name on BOTH backends; the SQLite-only
//! DYNAMIC raw-SQL script executor is the disambiguated `execute_batch_raw`). In-process
//! (no server), default suite.
//!
//! # Scope note (honest): typed carriers are READ-ONLY here
//!
//! SQLite's build-time conformance oracle (feature `macros-sqlite`, on in this
//! fixture) validates each `query!` through a DENY-ALL-BUT-READONLY authorizer, so a
//! typed WRITE carrier (`INSERT`/`UPDATE`/`DELETE ... RETURNING`) does NOT compile
//! here — the SQLite typed flagship is read-only. So typed `execute_batch::<Q>` is a
//! read-only ATOMIC batch: N reads inside ONE transaction, returning `Vec<u64>`
//! affected counts (0 for a SELECT). Its all-or-nothing rollback is inherited VERBATIM
//! from [`Connection::transaction`] (the method is literally
//! `self.transaction(|tx| …)`), whose write-rollback atomicity is proven by the
//! driver's transaction tests + the PostgreSQL live all-or-nothing proof
//! (`execute_batch_live_{async,sync}.rs`). These tests witness the `Vec<u64>` shape,
//! N == 0, and reusability. A typed WRITE batch is a PostgreSQL-only capability.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "in-process test harness — expect/unwrap/panic surface failures loudly"
)]

use bsql::sqlite::Connection;

bsql::query!(EbWeightById, "SELECT weight FROM measurements WHERE id = $1");

const SCHEMA: &str = "CREATE TABLE measurements ( \
     id BIGINT PRIMARY KEY, label TEXT NOT NULL, weight DOUBLE PRECISION NOT NULL, \
     payload BYTEA, count BIGINT, note TEXT );";

fn seed() -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch_raw(SCHEMA).expect("schema"); // the dynamic multi-statement executor
    conn.execute_raw("INSERT INTO measurements (id, label, weight) VALUES (1, 'one', 1.5)")
        .expect("seed 1");
    conn.execute_raw("INSERT INTO measurements (id, label, weight) VALUES (2, 'two', 2.5)")
        .expect("seed 2");
    conn
}

/// N read carriers run as ONE atomic batch, returning one `u64` per command (0 for a
/// SELECT — read-only), and the connection is reusable afterwards.
#[test]
fn n_reads_return_a_count_vec_and_commit() {
    let conn = seed();
    let counts = conn
        .execute_batch::<EbWeightById>(vec![(1_i64,), (2,), (1,)])
        .expect("batch runs");
    assert_eq!(counts, vec![0, 0, 0], "a read affects no rows (read-only twin)");
    // Reusable after a committed batch.
    assert_eq!(conn.query::<EbWeightById>((1,)).expect("reuse").len(), 1);
}

/// N == 0 → an empty `Vec<u64>` (an empty transaction).
#[test]
fn zero_is_empty() {
    let conn = seed();
    let counts = conn
        .execute_batch::<EbWeightById>(Vec::<(i64,)>::new())
        .expect("N=0");
    assert_eq!(counts, Vec::<u64>::new());
}

/// Inside an explicit transaction guard the batch runs in the SAME transaction (no
/// nested BEGIN), and its counts flow out.
#[test]
fn inside_a_transaction_guard() {
    let conn = seed();
    let counts = conn
        .transaction(|tx| tx.execute_batch::<EbWeightById>(vec![(1_i64,), (2,)]))
        .expect("guard commits");
    assert_eq!(counts, vec![0, 0]);
}
