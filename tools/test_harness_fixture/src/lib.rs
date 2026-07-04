#![forbid(unsafe_code)]

//! Fixture consumer for the `#[bsql::test]` schema-per-test isolation attribute.
//!
//! This crate has no library surface of its own; it exists so its `tests/`
//! exercise, through `bsql` alone, that:
//!
//! * two `#[bsql::test]` tests run in parallel against one server without
//!   interfering (each in its own schema) — proven for the async attribute and
//!   again for its sync twin;
//! * an async and a sync `#[bsql::test]` coexist in one file, each over its own
//!   driver, both isolated and both cleaned up;
//! * a schema is dropped on a passing test and on a panicking test, over both
//!   drivers;
//! * an unset `BSQL_TEST_DSN` is a loud, named error rather than a silent skip
//!   (the resolver is shared, so this covers both paths).
//!
//! The live tests are `#[ignore]` (they need a real PostgreSQL at
//! `BSQL_TEST_DSN`); the unset-DSN test is deterministic and runs everywhere.
