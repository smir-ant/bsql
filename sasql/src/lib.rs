#![forbid(unsafe_code)]

//! # sasql — Safe SQL for Rust
//!
//! **If it compiles, the SQL is correct.**
//!
//! sasql is a proc-macro library that validates every SQL query against a real
//! PostgreSQL instance at compile time. There is no `query()` function. There is
//! no escape hatch. There is `query!` — validated, typed, checked. If the binary
//! is produced, every SQL query in it is correct.
