#![forbid(unsafe_code)]

//! Fixture consumer for the `#[bsql::test]` schema-per-test isolation attribute.
//!
//! This crate has no library surface of its own; it exists so its `tests/`
//! exercise, through `bsql` alone, that:
//!
//! * two `#[bsql::test]` tests run in parallel against one server without
//!   interfering (each in its own schema);
//! * a schema is dropped on a passing test and on a panicking test;
//! * an unset `BSQL_TEST_DSN` is a loud, named error rather than a silent skip.
//!
//! The live tests are `#[ignore]` (they need a real PostgreSQL at
//! `BSQL_TEST_DSN`); the unset-DSN test is deterministic and runs everywhere.
