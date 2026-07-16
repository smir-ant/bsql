//! Umbrella test binary — groups the in-process (bundled-SQLite, no external
//! DB) OFFLINE driver tests into ONE linked binary instead of one per file,
//! cutting the dominant per-binary link cost of `cargo test -p bsql-sqlite`.
//!
//! Each grouped file is included VERBATIM as its own module via `#[path]`,
//! so every `#[test]` still compiles into this binary and still runs — ZERO
//! assertion change, only the binary count shrinks. The files live in
//! `tests/offline_group/` (a subdirectory, so cargo does NOT auto-build each
//! as its own integration-test binary).
//!
//! DELIBERATELY still their OWN binary (NOT grouped): the documented gates
//! `cancel` and `migrate`, the `*_alloc` constant-memory gates
//! (`arena_alloc`, `stream_alloc`), and the `compile_fail` trybuild binary.
#![forbid(unsafe_code)]

#[path = "offline_group/basic.rs"]
mod basic;
#[path = "offline_group/busy_timeout.rs"]
mod busy_timeout;
#[path = "offline_group/error_predicates.rs"]
mod error_predicates;
#[path = "offline_group/nan_bind.rs"]
mod nan_bind;
#[path = "offline_group/param_count.rs"]
mod param_count;
#[path = "offline_group/prepared_cache.rs"]
mod prepared_cache;
#[path = "offline_group/prepared_statement.rs"]
mod prepared_statement;
#[path = "offline_group/typed.rs"]
mod typed;
