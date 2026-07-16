//! Umbrella test binary — groups the query_fixture OFFLINE `query!` tests
//! (no PG, no trybuild, no alloc gate) into ONE linked binary instead of
//! one per file, cutting the dominant per-binary link cost of
//! `cargo test -p bsql-query-fixture`.
//!
//! Each grouped file is included VERBATIM as its own module via `#[path]`,
//! so every `#[test]` still compiles into this binary and still runs — ZERO
//! assertion change, only the binary count shrinks. The files live in
//! `tests/offline_group/` (a subdirectory, so cargo does NOT auto-build
//! each as its own integration-test binary). Each `query!`/`user_types!`
//! invocation stays in its own module, so carriers/records never collide.
//!
//! DELIBERATELY still their OWN binary (NOT grouped): every `*_live`
//! (`--ignored`, needs PG) suite, the `compile_fail` / `query_same_width_decode`
//! / `query_live_sync` trybuild binaries, the `*_alloc` constant-memory gates,
//! `copy_typed_offline` (named in CLAUDE.md), and the benches.
#![forbid(unsafe_code)]

#[path = "offline_group/query_any_bind.rs"]
mod query_any_bind;
#[path = "offline_group/query_arrays.rs"]
mod query_arrays;
#[path = "offline_group/query_composite_offline.rs"]
mod query_composite_offline;
#[path = "offline_group/query_decode.rs"]
mod query_decode;
#[path = "offline_group/query_dynamics.rs"]
mod query_dynamics;
#[path = "offline_group/query_enum_offline.rs"]
mod query_enum_offline;
#[path = "offline_group/query_fake.rs"]
mod query_fake;
#[path = "offline_group/query_numeric.rs"]
mod query_numeric;
#[path = "offline_group/query_stream_notify_drain.rs"]
mod query_stream_notify_drain;
#[path = "offline_group/query_stream_offline.rs"]
mod query_stream_offline;
#[path = "offline_group/query_temporal.rs"]
mod query_temporal;
#[path = "offline_group/query_widen_types.rs"]
mod query_widen_types;
#[path = "offline_group/query_wire.rs"]
mod query_wire;
#[path = "offline_group/typed_rows_malformed.rs"]
mod typed_rows_malformed;
