//! No-escape wall — `trybuild` golden harness for the single-residence
//! ingest borrows.
//!
//! [`read_slot`] lends a `&mut [u8]` borrowed from `&mut self`, and
//! [`next_event`] returns an [`Event`] borrowing `&mut self`. Holding either
//! across the next mutating call (`read_slot` / `commit` / `next_event`) is a
//! borrow conflict the compiler rejects with E0499 — a lent slot or a
//! borrow-through event cannot outlive the next mutation. That is the
//! no-escape wall, and it is free: compaction (the inline->heap escape, the
//! consumed-prefix reclaim) can relocate the live bytes because no borrow
//! from a prior call is ever alive when the next one runs.
//!
//! These goldens pin the two failure modes plus the sanctioned positive
//! path:
//!
//! - `fail_slot_held.rs`   → E0499 — a lent slot held across the next
//!   `read_slot`.
//! - `fail_event_held.rs`  → E0499 — a borrow-through `Event` held across
//!   the next `read_slot`.
//! - `pass_sequential.rs`  → the sequential read_slot/commit/next_event
//!   reuse the wall permits compiles.
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto \
//!     --test engine_ingest_compile_fail
//! ```
//! Then review every `.stderr` diff.
//!
//! [`read_slot`]: bsql_postgres_proto::engine::IngestBuf::read_slot
//! [`next_event`]: bsql_postgres_proto::engine::IngestBuf::next_event
//! [`Event`]: bsql_postgres_proto::engine::Event

#![forbid(unsafe_code)]

/// A lent slot cannot outlive the next mutating call — E0499.
#[test]
fn lent_slot_across_next_read_slot_is_e0499() {
    trybuild::TestCases::new().compile_fail("tests/engine_ingest/fail_slot_held.rs");
}

/// A borrow-through event cannot outlive the next mutating call — E0499.
#[test]
fn borrow_through_event_across_read_slot_is_e0499() {
    trybuild::TestCases::new().compile_fail("tests/engine_ingest/fail_event_held.rs");
}

/// The sanctioned path — sequential read_slot/commit/next_event reuse —
/// compiles.
#[test]
fn sequential_reuse_compiles() {
    trybuild::TestCases::new().pass("tests/engine_ingest/pass_sequential.rs");
}
