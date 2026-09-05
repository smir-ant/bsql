//! Umbrella test binary — groups the proto in-process OFFLINE spec tests
//! (each entirely sans-IO) into ONE linked binary instead of one binary
//! per file, cutting the dominant per-binary link cost of
//! `cargo test -p bsql-postgres-proto`.
//!
//! Each grouped file is included VERBATIM as its own module via `#[path]`,
//! so every `#[test]` in it still compiles into this binary and still runs
//! — ZERO assertion change, only the binary count shrinks. The files live
//! in `tests/offline_specs/` (a subdirectory, so cargo does NOT auto-build
//! each as its own integration-test binary).
//!
//! DELIBERATELY still their OWN binary (NOT grouped): the documented gates
//! (the `engine_*_spec` named in CLAUDE.md, every `engine_*_alloc`, every
//! `*_wire_spec`, `engine_hotpath_codegen`, the pipeline gates), the
//! `*_compile_fail` trybuild binaries, the feature-gated fail-loud tests,
//! the fuzz/miri binaries, the two files using `crate::`-rooted paths, the
//! two white-box tests that `include_str!("../src/...")` (a relative path
//! that would break under the subdirectory), and the one test that uses
//! `unsafe` (incompatible with this umbrella's `#![forbid(unsafe_code)]`).
#![forbid(unsafe_code)]

#[path = "offline_specs/engine_active_spec.rs"]
mod engine_active_spec;
#[path = "offline_specs/engine_close_many.rs"]
mod engine_close_many;
#[path = "offline_specs/engine_copy_aggregated_spec.rs"]
mod engine_copy_aggregated_spec;
#[path = "offline_specs/engine_copy_batch.rs"]
mod engine_copy_batch;
#[path = "offline_specs/engine_flush_cancel.rs"]
mod engine_flush_cancel;
#[path = "offline_specs/engine_flush_spec.rs"]
mod engine_flush_spec;
#[path = "offline_specs/engine_hostile_spec.rs"]
mod engine_hostile_spec;
#[path = "offline_specs/engine_ingest_spec.rs"]
mod engine_ingest_spec;
#[path = "offline_specs/engine_open_owned_spec.rs"]
mod engine_open_owned_spec;
#[path = "offline_specs/engine_prelude_fusion_spec.rs"]
mod engine_prelude_fusion_spec;
#[path = "offline_specs/engine_pump_pending_sentinel.rs"]
mod engine_pump_pending_sentinel;
#[path = "offline_specs/engine_pump_spec.rs"]
mod engine_pump_spec;
#[path = "offline_specs/engine_read_ramp_spec.rs"]
mod engine_read_ramp_spec;
#[path = "offline_specs/engine_spec.rs"]
mod engine_spec;
#[path = "offline_specs/engine_terminate_spec.rs"]
mod engine_terminate_spec;
#[path = "offline_specs/frame_parse.rs"]
mod frame_parse;
#[path = "offline_specs/password_message_size_spec.rs"]
mod password_message_size_spec;
