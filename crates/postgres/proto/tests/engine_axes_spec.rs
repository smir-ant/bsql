//! The engine's §7 twelve-axis edge-case pass — the durable,
//! self-verifying coverage artifact.
//!
//! # What this file is
//!
//! The project canon mandates that every non-trivial component be walked across
//! twelve edge-case axes, and that each axis (and each material sub-point) be
//! resolved EITHER by a concrete green gate/test OR by an explicit, justified
//! "not applicable" — silence on an applicable axis is itself a §7 violation.
//!
//! [`SECTION7`] below is the AUTHORITATIVE per-sub-point table for the rewritten
//! engine (`crates/postgres/proto/src/engine/`). It is the single source of
//! truth: there is no second copy to drift out of sync. Each row names its axis,
//! a sub-point, and the EVIDENCE that resolves it — a proving test, a
//! compile-time gate / source invariant, or a justified [`Ev::Na`].
//!
//! # Why this form (and not the alternatives)
//!
//! The headline risk a §7 artifact must defeat is a citation going stale
//! SILENTLY — a table claiming coverage by a test that was renamed or deleted.
//! Three forms were weighed:
//!
//! 1. **A free-prose / markdown table cross-referencing test names** — rejected:
//!    nothing fails when a cited test disappears, so the table can quietly lie.
//! 2. **A `#[doc]` table in engine source + a separate test asserting the
//!    citations** — viable, but it splits the table (engine source) from its
//!    verifier (the test) across the crate boundary, so the two can drift in
//!    format, and it forces the artifact's data into the engine's PUBLIC API or
//!    into prose a test must re-parse.
//! 3. **A structured table CO-LOCATED with the test that verifies it (this
//!    file)** — chosen: the table is machine-readable Rust data, and the tests
//!    in this same file read the real evidence files from disk and FAIL if any
//!    cited test/gate is gone (`every_cited_test_exists_on_disk`,
//!    `every_cited_src_marker_exists`). A citation that disappears turns the
//!    build red; it can never silently lie. The engine module carries a short
//!    `//!` narrative pointing here, and `engine_doc_lists_all_twelve_axis_names`
//!    pins that narrative to this table's axis names.
//!
//! Structural completeness (all twelve axes present, every row well-formed) is
//! lifted to a tier-1 compile-time `const _` assertion below — it fails at
//! `cargo check`, not merely at `cargo test`.
//!
//! # Census discipline (the load-bearing counts)
//!
//! The project pins every approximate count to a REPRODUCIBLE COMMAND committed
//! as a gate (naive recounts have drifted before). The `census_*` tests at the
//! bottom re-derive each count from the engine source on disk and assert it
//! against the committed constant, so a drift fails the build rather than rotting
//! in a comment. Each carries its exact reproducing shell command.
//!
//! # Genuine gaps found
//!
//! None: every axis is resolved by a cited green gate/test or a justified N/A.
//! The N/A sub-points are the sans-IO core's structural non-applicabilities
//! (OS signals, task-locals, FFI-owned pointers, multi-threaded sharing) — each
//! carries its reason inline, never silence.

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test/gate harness — the disk-reading and census helpers use expect()/panic as the loud failure signal when a cited evidence file is missing or the engine source cannot be scanned (which is itself the gate firing); clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this gate is factored into"
)]

use std::path::PathBuf;

// ===========================================================================
// The structured §7 table
// ===========================================================================

/// The evidence resolving one §7 sub-point.
#[derive(Clone, Copy, Debug)]
enum Ev {
    /// One proving `#[test] fn`: `(fn_name, repo-relative file)`. The test
    /// `every_cited_test_exists_on_disk` asserts `fn <name>(` is present in the
    /// file, so deleting or renaming the test turns the build red.
    Test(&'static str, &'static str),
    /// Several proving `#[test] fn`s, each `(fn_name, repo-relative file)`.
    Tests(&'static [(&'static str, &'static str)]),
    /// A compile-time gate or source invariant proven by the presence of a
    /// marker substring: `(marker, repo-relative file)`. Used where the evidence
    /// is a `const _` gate, a `#![forbid(...)]` line, or a structural invariant
    /// rather than a named `#[test]`. Asserted by `every_cited_src_marker_exists`.
    Src(&'static str, &'static str),
    /// Several compile-time gates / source invariants, each `(marker, file)`.
    Srcs(&'static [(&'static str, &'static str)]),
    /// A justified not-applicable. The reason must be non-empty (a §7 axis left
    /// silent is a violation); the `const` well-formedness gate enforces that.
    Na(&'static str),
}

/// One `(axis, sub-point, evidence)` row of the §7 pass.
#[derive(Clone, Copy, Debug)]
struct Row {
    /// Axis number, 1..=12.
    axis: u8,
    /// Axis name (canon).
    axis_name: &'static str,
    /// The sub-point this row resolves.
    subpoint: &'static str,
    /// The evidence that resolves it.
    ev: Ev,
}

const fn r(axis: u8, axis_name: &'static str, subpoint: &'static str, ev: Ev) -> Row {
    Row {
        axis,
        axis_name,
        subpoint,
        ev,
    }
}

// Repo-relative evidence-file paths (kept as consts to make the table readable).
// Files under `tests/offline_specs/` are grouped into the single
// `engine_offline_specs` umbrella binary (link-cost consolidation); their axis
// coverage is unchanged — only the on-disk path moved one directory deeper.
const HOSTILE: &str = "crates/postgres/proto/tests/offline_specs/engine_hostile_spec.rs";
const PUMP: &str = "crates/postgres/proto/tests/offline_specs/engine_pump_spec.rs";
const ACTIVE: &str = "crates/postgres/proto/tests/offline_specs/engine_active_spec.rs";
const VERBS: &str = "crates/postgres/proto/tests/engine_verbs_spec.rs";
const CONNECT: &str = "crates/postgres/proto/tests/engine_connect_spec.rs";
const TERMINATE: &str = "crates/postgres/proto/tests/offline_specs/engine_terminate_spec.rs";
const CONNECTING: &str = "crates/postgres/proto/tests/engine_connecting_spec.rs";
const FLUSH_CANCEL: &str = "crates/postgres/proto/tests/offline_specs/engine_flush_cancel.rs";
const FLUSH_ALLOC: &str = "crates/postgres/proto/tests/engine_flush_alloc.rs";
const INGEST_ALLOC: &str = "crates/postgres/proto/tests/engine_ingest_alloc.rs";
const MEMSET_GUARD: &str = "crates/postgres/proto/tests/engine_ingest_memset_guard.rs";
const PENDING_SENTINEL: &str = "crates/postgres/proto/tests/offline_specs/engine_pump_pending_sentinel.rs";
const LINEARITY_CF: &str = "crates/postgres/proto/tests/engine_linearity_compile_fail.rs";
const INGEST_CF: &str = "crates/postgres/proto/tests/engine_ingest_compile_fail.rs";
const ACTIVE_CF: &str = "crates/postgres/proto/tests/engine_active_compile_fail.rs";
const VERBS_CF: &str = "crates/postgres/proto/tests/engine_verbs_compile_fail.rs";
const FOOTPRINT_CF: &str = "crates/postgres/proto/tests/footprint_drift_compile_fail.rs";
const FRAME: &str = "crates/postgres/proto/tests/offline_specs/frame_parse.rs";
const ZEROIZE: &str = "crates/postgres/proto/tests/zeroize_coverage_spec.rs";
const MIRI: &str = "crates/postgres/proto/tests/scram_zeroize_miri_spec.rs";
// In-crate `#[cfg(test)]` modules (proven by the same `cargo test`).
const SRC_MOD: &str = "crates/postgres/proto/src/engine/mod.rs";
const SRC_FLUSH: &str = "crates/postgres/proto/src/engine/flush.rs";
const SRC_INGEST: &str = "crates/postgres/proto/src/engine/ingest.rs";
const SRC_PUMP: &str = "crates/postgres/proto/src/engine/pump.rs";
const SRC_ERROR: &str = "crates/postgres/proto/src/engine/error.rs";
const SRC_LIB: &str = "crates/postgres/proto/src/lib.rs";
// Replay corpus (the observable-I/O oracle over the frozen goldens).
const SEED: &str = "tools/corpus/tests/seed.rs";
const SURFACES: &str = "tools/corpus/tests/surfaces.rs";
const FALSIFIER: &str = "tools/corpus/tests/falsifier_a2.rs";
const ADVERSARIAL: &str = "tools/corpus/tests/adversarial.rs";

const A1: &str = "Cardinality";
const A2: &str = "Presence";
const A3: &str = "Concurrency";
const A4: &str = "Temporal";
const A5: &str = "Trust level";
const A6: &str = "Size";
const A7: &str = "State lifecycle";
const A8: &str = "Resource pressure";
const A9: &str = "Platform";
const A10: &str = "Failure composition";
const A11: &str = "Memory-leak / ownership";
const A12: &str = "Fallback / recovery";

/// The authoritative §7 table for the rewritten engine. Single source of truth;
/// the tests in this file verify every citation resolves and that all twelve
/// axes are covered.
const SECTION7: &[Row] = &[
    // ---- Axis 1 — Cardinality -------------------------------------------
    r(1, A1, "empty / zero rows", Ev::Tests(&[
        ("query_one_rejects_zero_rows", VERBS),
        ("query_opt_accepts_zero_and_one_but_rejects_two", VERBS),
        ("prepare_nodata_surfaces_empty_row_schema", VERBS),
    ])),
    r(1, A1, "single row", Ev::Tests(&[
        ("query_one_accepts_exactly_one_row", VERBS),
        ("select_rows_single_pass_borrow_through", ACTIVE),
    ])),
    r(1, A1, "few / many rows", Ev::Tests(&[
        ("query_prepared_streams_rows", VERBS),
        ("idle_select_drives_rows_deliver_and_flushes_request_once", PUMP),
        ("multi_statement_delineated", ACTIVE),
    ])),
    r(1, A1, "at-capacity / overflow (row-count guard, capacity+1)", Ev::Tests(&[
        ("query_one_rejects_two_rows", VERBS),
        ("query_opt_accepts_zero_and_one_but_rejects_two", VERBS),
    ])),
    r(1, A1, "cross-fixture cardinality (engine vs frozen goldens)",
        Ev::Test("seed_corpus_matches_golden_on_new_engine", SEED)),
    // ---- Axis 2 — Presence ----------------------------------------------
    r(2, A2, "all fields / none / partial", Ev::Tests(&[
        ("prepare_surfaces_recovered_schema", VERBS),
        ("prepare_nodata_surfaces_empty_row_schema", VERBS),
        ("partial_assembly_one_byte_per_read", ACTIVE),
    ])),
    r(2, A2, "DUPLICATE (frame sent twice — what wins?)", Ev::Tests(&[
        ("adversarial_fixtures_match_golden_on_new_engine", ADVERSARIAL),
        ("second_row_description_tears_down", ACTIVE),
    ])),
    r(2, A2, "unexpected frame / tag", Ev::Tests(&[
        ("out_of_phase_or_unknown_tag_tears_down", HOSTILE),
        ("unexpected_frame_during_connect_is_classified_fail", CONNECTING),
    ])),
    // ---- Axis 3 — Concurrency -------------------------------------------
    r(3, A3, "single-threaded sans-IO default", Ev::Na(
        "the sans-IO core owns no threads, locks, or shared state and every \
         method takes &mut self, so a data race is unrepresentable; \
         multi-threaded use is the driver layer's concern, exercised below via \
         the Send bound.")),
    r(3, A3, "Send (verb/pump futures cross task boundaries)", Ev::Src(
        "PUMP-FUTURE-SEND", SRC_MOD)),
    r(3, A3, "async cancellation-safety + Drop-in-flight", Ev::Tests(&[
        ("flush_unrolls_at_every_drop_point_with_byte_identical_resume", FLUSH_CANCEL),
        ("flush_survives_repeated_cancellation_without_replay", FLUSH_CANCEL),
        ("negative_control_future_local_cursor_double_sends", FLUSH_CANCEL),
    ])),
    r(3, A3, "single-poll / no spin (blocking transport)", Ev::Tests(&[
        ("pump_over_blocking_transport_resolves_in_one_poll", PENDING_SENTINEL),
        ("pending_future_classifies_spurious_pending", PENDING_SENTINEL),
    ])),
    r(3, A3, "reentrancy / at-most-one command in flight (linear token)", Ev::Tests(&[
        ("foreign_brand_is_rejected", LINEARITY_CF),
        ("brand_cannot_escape_scope", LINEARITY_CF),
    ])),
    r(3, A3, "signal", Ev::Na(
        "the sans-IO core installs no signal handlers and performs no syscalls; \
         OS signal handling belongs to the driver/runtime layer, not the \
         protocol engine.")),
    // ---- Axis 4 — Temporal ----------------------------------------------
    r(4, A4, "full sequence in one call",
        Ev::Test("idle_select_drives_rows_deliver_and_flushes_request_once", PUMP)),
    r(4, A4, "split byte-by-byte / mid-frame / mid-header", Ev::Tests(&[
        ("truncated_at_every_offset_yields_needmore_never_misclassifies", HOSTILE),
        ("partial_assembly_one_byte_per_read", ACTIVE),
        ("seed_corpus_is_schedule_invariant", SEED),
    ])),
    r(4, A4, "mid-transition drop|cancel (Connecting→Active never across await)", Ev::Tests(&[
        ("flush_unrolls_at_every_drop_point_with_byte_identical_resume", FLUSH_CANCEL),
    ])),
    r(4, A4, "reorder / interleave", Ev::Tests(&[
        ("declared_length_delimits_frame_trailing_bytes_are_next_frame", HOSTILE),
        ("async_frames_interleaved_mid_row_stream_do_not_disturb_command", HOSTILE),
    ])),
    r(4, A4, "stale gen-ref / re-entry (E0499 no-escape wall)", Ev::Tests(&[
        ("lent_slot_across_next_read_slot_is_e0499", INGEST_CF),
        ("borrow_through_event_across_read_slot_is_e0499", INGEST_CF),
    ])),
    // ---- Axis 5 — Trust level -------------------------------------------
    r(5, A5, "internal-trusted (verb misuse)",
        Ev::Test("verb_before_connect_is_wrong_phase", VERBS)),
    r(5, A5, "untrusted pre-auth (handshake replies)", Ev::Tests(&[
        ("hostile_handshake_replies_classify_connfail", HOSTILE),
        ("scram_final_signature_mismatch_fails", CONNECTING),
        ("scram_offered_without_supported_mechanism_fails", CONNECTING),
        ("trust_rejects_sasl_challenge", CONNECTING),
    ])),
    r(5, A5, "malformed (wire-valid shape, semantically wrong)", Ev::Tests(&[
        ("length_below_minimum_tears_down", HOSTILE),
        ("declared_longer_than_body_waits_never_misclassifies", HOSTILE),
    ])),
    r(5, A5, "adversarial (crafted to bypass the parser)", Ev::Tests(&[
        ("embedded_rfq_frame_in_cell_does_not_false_terminate", HOSTILE),
        ("out_of_phase_or_unknown_tag_tears_down", HOSTILE),
        ("active_protocol_violation_pumps_to_boundary_closed", HOSTILE),
    ])),
    r(5, A5, "adversarial coverage strength (corpus falsifier kill-rate)",
        Ev::Test("a2_falsifier_catch_rate", FALSIFIER)),
    // ---- Axis 6 — Size --------------------------------------------------
    r(6, A6, "zero bytes (Ok(0) read / drained buffer)", Ev::Tests(&[
        ("unexpected_eof_is_classified", PUMP),
        ("need_more_when_buffer_drained", CONNECTING),
    ])),
    r(6, A6, "max (cap-1, cap, cap+1)", Ev::Tests(&[
        ("minimal_legal_header_parses_ok", FRAME),
        ("length_above_max_is_frame_too_large", FRAME),
        ("oversize_control_frame_tears_down", ACTIVE),
    ])),
    r(6, A6, "declared length != actual", Ev::Tests(&[
        ("declared_longer_than_body_waits_never_misclassifies", HOSTILE),
        ("declared_length_delimits_frame_trailing_bytes_are_next_frame", HOSTILE),
        ("length_below_minimum_tears_down", HOSTILE),
    ])),
    r(6, A6, "oversize (cap+1) streamed bounded, never OOM", Ev::Tests(&[
        ("oversize_row_streams_sub_a_bounded", ACTIVE),
        ("oversize_notice_streams_sub_b_truncated", ACTIVE),
        ("oversize_non_streaming_tag_tears_down", HOSTILE),
    ])),
    r(6, A6, "integer width overflow (u16 cursor/filled ceiling)",
        Ev::Src("<= u16::MAX", SRC_INGEST)),
    // ---- Axis 7 — State lifecycle ---------------------------------------
    r(7, A7, "pre-init (verb / accessor before handshake)", Ev::Tests(&[
        ("verb_before_connect_is_wrong_phase", VERBS),
        ("connect_when_already_active_is_wrong_phase", CONNECT),
    ])),
    r(7, A7, "transition (connecting → active)", Ev::Tests(&[
        ("trust_connect_reaches_active", CONNECT),
        ("scram_connect_reaches_active", CONNECT),
    ])),
    r(7, A7, "terminal (drained / completed boundaries)", Ev::Tests(&[
        ("suspended_terminal_on_portal_suspended", PUMP),
        ("closed_terminal_on_out_of_phase_frame", PUMP),
        ("into_active_before_ready_returns_still_connecting", CONNECTING),
    ])),
    r(7, A7, "errored recovery", Ev::Tests(&[
        ("recoverable_server_error", ACTIVE),
        ("failed_terminal_surfaces_error_then_returns_failed", PUMP),
        ("server_error_is_classified", VERBS),
    ])),
    r(7, A7, "graceful close (terminate → Closed; post-close accessor classified)", Ev::Tests(&[
        ("terminate_sends_frame_shuts_down_and_closes", TERMINATE),
        ("accessor_after_terminate_is_wrong_phase", TERMINATE),
    ])),
    r(7, A7, "post-consume (move-after-consume is a compile error)",
        Ev::Test("use_after_close_is_e0382", VERBS_CF)),
    r(7, A7, "phase-typed events (an active event is not an auth event)",
        Ev::Test("active_event_is_not_auth_event_is_e0308", ACTIVE_CF)),
    // ---- Axis 8 — Resource pressure -------------------------------------
    r(8, A8, "bounded slack / steady-state zero-alloc", Ev::Tests(&[
        ("steady_state_flush_is_zero_alloc_first_fill_allocates_once", FLUSH_ALLOC),
        ("ingest_steady_state_is_zero_alloc_escape_is_one_time", INGEST_ALLOC),
    ])),
    r(8, A8, "over-capacity (classified refuse, not silent drop)", Ev::Tests(&[
        ("oversize_control_frame_tears_down", ACTIVE),
        ("oversize_non_streaming_tag_tears_down", HOSTILE),
    ])),
    r(8, A8, "arena/slot exhaustion (frame larger than bounded buffer)", Ev::Tests(&[
        ("oversize_row_streams_sub_a_bounded", ACTIVE),
        ("oversize_notice_streams_sub_b_truncated", ACTIVE),
    ])),
    r(8, A8, "stack pressure (footprint pinned, drift = E0080)", Ev::Tests(&[
        ("size_drift_is_e0080", FOOTPRINT_CF),
        ("align_drift_is_e0080", FOOTPRINT_CF),
    ])),
    // ---- Axis 9 — Platform ----------------------------------------------
    r(9, A9, "endianness (wire big-endian, exact bytes pinned)", Ev::Tests(&[
        ("seed_corpus_matches_golden_on_new_engine", SEED),
        ("verb_surface_has_teeth", SURFACES),
    ])),
    r(9, A9, "alignment (size + align pinned together)",
        Ev::Test("align_drift_is_e0080", FOOTPRINT_CF)),
    r(9, A9, "target_pointer_width (usize >= 32; u16 frame caps)", Ev::Srcs(&[
        ("usize::BITS >= 32", SRC_LIB),
        ("<= u16::MAX", SRC_INGEST),
    ])),
    r(9, A9, "panic=abort vs unwind", Ev::Na(
        "the engine emits no panics of its own — the crate-root forbid bundle \
         bans unwrap/expect/panic/unreachable/indexing/arithmetic-overflow — so \
         no unwind path ORIGINATES in engine code. A panic arriving from outside \
         (e.g. a caller's sink closure) unwinds through engine frames: under \
         unwind, Drop-based zeroize runs as the stack unwinds (structurally the \
         cancellation / drop-in-flight case already tested under axes 3-4); under \
         panic=abort the process terminates immediately, so Drop does NOT run — \
         which is benign, because the secret's memory dies with the process, \
         leaving no live-process window with an un-scrubbed secret. Witnessed by \
         the #![forbid(unsafe_code)] + clippy::panic bundle in lib.rs and the \
         Drop zeroize tests under axis 11.")),
    // ---- Axis 10 — Failure composition ----------------------------------
    r(10, A10, "fatal-vs-recoverable classification (who tags)",
        Ev::Src("pub enum EngineError", SRC_ERROR)),
    r(10, A10, "recoverable vs fatal observed end-to-end", Ev::Tests(&[
        ("recoverable_server_error", ACTIVE),
        ("closed_terminal_on_out_of_phase_frame", PUMP),
    ])),
    r(10, A10, "cascading / no retry on a broken transport", Ev::Tests(&[
        ("unexpected_eof_is_classified", PUMP),
        ("negative_control_future_local_cursor_double_sends", FLUSH_CANCEL),
    ])),
    r(10, A10, "partial (some surfaced, then failed)",
        Ev::Test("failed_terminal_surfaces_error_then_returns_failed", PUMP)),
    // ---- Axis 11 — Memory-leak / ownership ------------------------------
    r(11, A11, "sensitive data zeroized on drop / at handshake completion", Ev::Tests(&[
        ("send_buf_drop_fires_zeroize", SRC_FLUSH),
        ("ingest_buf_drop_fires_zeroize_chain", SRC_INGEST),
        ("connect_scrubs_secret_outbound_wire_at_handshake_completion", SRC_MOD),
        ("scrub_drained_empties_queued_region_and_retains_capacity", SRC_FLUSH),
    ])),
    r(11, A11, "zeroize manifest gate (the Drop scrub cannot be silently removed)", Ev::Tests(&[
        ("manifest_covers_every_zeroize_on_drop_secret_type", ZEROIZE),
        ("password_drop_zeros_backing_buffer", MIRI),
    ])),
    r(11, A11, "bounded buffer cleared / no per-read memset",
        Ev::Test("hot_path_bodies_are_memset_free", MEMSET_GUARD)),
    r(11, A11, "no un-freed growth in a long-running connection", Ev::Tests(&[
        ("steady_state_flush_is_zero_alloc_first_fill_allocates_once", FLUSH_ALLOC),
        ("ingest_steady_state_is_zero_alloc_escape_is_one_time", INGEST_ALLOC),
    ])),
    r(11, A11, "task-local cleared / FFI-owned pointer freed", Ev::Na(
        "the sans-IO engine uses no task-locals and no FFI: it is \
         #![forbid(unsafe_code)] with no raw pointers, so there is no \
         foreign-owned resource to free; all state is owned Rust values \
         scrubbed on Drop (proven by the zeroize rows above).")),
    // ---- Axis 12 — Fallback / recovery ----------------------------------
    r(12, A12, "no tier-4 silent default (forbid bundle + sealed construction)", Ev::Tests(&[
        ("foreign_brand_is_rejected", LINEARITY_CF),
        ("brand_cannot_escape_scope", LINEARITY_CF),
    ])),
    r(12, A12, "every non-happy branch classified (cold-path discipline)",
        Ev::Src("core::hint::cold_path()", SRC_PUMP)),
    r(12, A12, "unknown frame is a compile error, never a silent wildcard drop",
        Ev::Test("nonexhaustive_event_match_is_e0004", ACTIVE_CF)),
    r(12, A12, "no silent fallback on the read path",
        Ev::Test("unexpected_eof_is_classified", PUMP)),
    r(12, A12, "finite retry — never spins, never replays", Ev::Tests(&[
        ("negative_control_future_local_cursor_double_sends", FLUSH_CANCEL),
        ("flush_survives_repeated_cancellation_without_replay", FLUSH_CANCEL),
    ])),
];

// ===========================================================================
// Committed census constants (re-derived from disk by the census_* tests)
// ===========================================================================

/// Number of §7 axes (canon). Self-checking: `const _` below + the test.
const SECTION7_AXES: usize = 12;
/// `crate::wire_pin!(` footprint-pin invocations across `engine/`.
/// Reproduce: `grep -rho 'crate::wire_pin!(' crates/postgres/proto/src/engine/*.rs | wc -l`
/// Text-level count (this scan is cfg-blind): only `ConnFail` still carries a
/// `#[cfg(feature = "scram")]` / `#[cfg(not(...))]` pin PAIR (its footprint shrinks
/// when the SCRAM leaf class is compiled out), contributing two invocations.
/// `HandshakeProgress` and `HandshakeOutcome` are now SINGLE pins: their widest
/// variant is the `ServerError(Box<[u8]>)` raw-body carrier (24/8), which dominates
/// `ConnFail` regardless of the SCRAM feature, so their footprint is
/// feature-INDEPENDENT (down from a pair each — the 30→28 re-baseline).
const WIRE_PINS: usize = 28;
/// Variants of `EngineError<E>` (the classified error taxonomy).
/// Reproduce: count the upper-case-leading lines inside the `pub enum
/// EngineError<E> { .. }` block in `engine/error.rs`.
/// 15 adds `HandshakeServerError(Box<[u8]>)` — a connect-time server `ErrorResponse`
/// carried up as raw bytes for the driver to classify into `DriverError::Db`,
/// distinct from the client-side classified `Handshake(ConnFail)`.
const ENGINE_ERROR_VARIANTS: usize = 15;
/// Active-phase verbs (each takes the linear `Live` token; all return it save the
/// session-ending `terminate`, which consumes it into the closed phase).
/// Reproduce: `grep -c "live: Live<'b>" crates/postgres/proto/src/engine/verbs.rs`
/// (21 includes `query_params_fused`, the one-round-trip runtime-param verb, and
/// `close_statements`, the BATCHED close the pool-reset dynamic-cache clear uses
/// to Close N statements in one round trip. `LISTEN` has NO dedicated verb: the
/// driver validates the channel into a `SafeIdent` and issues `LISTEN <channel>`
/// through `simple_query`, so the injection-safe type is the sole splice currency.)
/// 23 adds the two BREAKABLE dynamic streaming verbs — `query_break` (the
/// simple-query peer of `query`) and `query_params_fused_break` (the fused peer
/// of `query_params_fused`) — behind the driver's `query_each_raw` /
/// `query_each_params` constant-memory streaming.
/// 24 adds `run_pipeline`, the drive verb for a HETEROGENEOUS pipelined batch (N
/// compile-checked commands under ONE trailing Sync — one implicit transaction).
/// Its two staging helpers (`stage_pipeline_command` / `stage_pipeline_seal`) take
/// NO `Live` token (pure send-buffer builds), so they do not count.
/// 25 adds `run_pipeline_break`, the BREAKABLE window-drive verb for a homogeneous
/// `execute_batch` (drives one Flush-terminated window, breaking at its delivery
/// count). Its staging helpers (`stage_execute_batch_command` / `stage_flush` /
/// `pending_send_len`) take NO `Live` token, so they do not count.
/// 27 adds the two verbs the windowed HETEROGENEOUS `pipeline` needs:
/// `run_pipeline_break_guarded` (the GUARDED intermediate-window drive — a
/// pipeline decodes each command, so it also BAILS if a MISS command's
/// result-schema guard parks a mismatch mid-window, which a `Flush`-terminated
/// window has no `Sync` to drain) and the shared `run_pipeline_break_impl`
/// (`run_pipeline_break` + `run_pipeline_break_guarded` differ only in the
/// `const BAIL_ON_GUARD_MISMATCH` they thread into the pump; both delegate here).
/// 28 adds `close_statements_bytes`, the byte-named core of `close_statements`
/// (which now delegates to it): a `Close` frame names a statement by raw bytes, so
/// the pool reset's COMBINED cache-drop folds the dynamic cache's `StmtName`s AND
/// the typed cache's `'static` names into ONE batch through this one verb.
const ACTIVE_VERBS: usize = 28;
/// `core::hint::cold_path()` classified-branch markers across `engine/`.
/// Reproduce: `grep -rho 'core::hint::cold_path()' crates/postgres/proto/src/engine/*.rs | wc -l`
/// (52 includes the COPY-in `write_all` `WriteZero`/`SendOverrun` branches,
/// `query_params_fused`'s oversize-SQL `FrameTooLong` branch, the streaming
/// `Bind`'s `frame_too_long` overflow landing, and the six fused-prelude markers:
/// the `stage_prelude` prepend, the `pump_active_to_boundary` prelude-drain branch,
/// the `drain_fused_prelude` EOF + fatal-frame arms, `surface_during_prelude`'s
/// inapplicable-`Break` guard, and `copy_in_begin`'s prelude-drain branch.
/// 53 adds the `pump_active_to_boundary` `Event::Overcap` sink-`Break` marker —
/// the too-wide-result recovery's early-stop landing, the twin of the `Fail` arm's.
/// 56 adds the three `classify_break_boundary` markers (the shared post-pump
/// classifier for the breakable dynamic streaming verbs: the `Failed` recover,
/// the `Closed` protocol-violation, and the `Suspended` fatal arm — the `FrameTooLong`
/// cold markers of `stage_simple_query` / `stage_fused_params` were MOVED out of
/// `run_simple` / `query_params_fused`, so those are net-zero.)
/// 58 adds the two connect-time server-error markers: the pump's
/// `HandshakeProgress::ServerError` arm (raw body → `HandshakeOutcome::ServerError`)
/// and the connect verb's `HandshakeOutcome::ServerError` arm
/// (→ `EngineError::HandshakeServerError`) — both cold connect-failure landings.
/// 56 removes the two oversize-SQL `FrameTooLong` cold markers of
/// `stage_fused_params` and the `prepare` verb: both now stream the whole `Parse`
/// (SQL + parameter-type OID list) onto the send buffer via `build_parse` over a
/// `SendFrame` with a back-patched length, so the explicit `u32::try_from(sql_len)`
/// pre-check is subsumed by the streamed builder's own overflow landing
/// (`frame_too_long`, already counted once at its definition).
///
/// 56 → 57: the typed result-schema guard's mismatch branch in
/// `apply_fused_row_stream` marks its (rare — a live/build schema divergence)
/// drain-and-record path `#[cold]` via `core::hint::cold_path()`.
/// 57 → 58: the `pump_active_to_boundary_impl` GUARDED-window bail marker — when
/// a pipeline intermediate window (a `Flush`, no `Sync`) parks a result-OID
/// mismatch, the drain cannot reach an RFQ, so the pump returns `Failed` on the
/// rare parked-mismatch branch (const-folded away for every non-guarded caller).
/// 58 → 60: `take_frame_fast` in `ingest.rs` marks the malformed declared < 4 and
/// `declared > MAX_FRAME_LEN_FIELD` branches as cold via `core::hint::cold_path()`.
const COLD_CLASSIFIED_BRANCHES: usize = 60;
/// `#[non_exhaustive]` attribute lines across `engine/`.
/// Reproduce: `grep -rcE '^#\[non_exhaustive\]' crates/postgres/proto/src/engine/*.rs` (summed)
const NON_EXHAUSTIVE_ATTRS: usize = 4;

// ===========================================================================
// Tier-1 structural self-checks (fail at `cargo check`)
// ===========================================================================

const fn covers_all_axes(rows: &[Row]) -> bool {
    let mut n = 1u8;
    while n <= 12 {
        let mut found = false;
        let mut i = 0;
        while i < rows.len() {
            if rows[i].axis == n {
                found = true;
            }
            i += 1;
        }
        if !found {
            return false;
        }
        n += 1;
    }
    true
}

const fn every_row_well_formed(rows: &[Row]) -> bool {
    let mut i = 0;
    while i < rows.len() {
        let row = rows[i];
        if row.axis < 1 || row.axis > 12 {
            return false;
        }
        if row.axis_name.is_empty() || row.subpoint.is_empty() {
            return false;
        }
        // Every evidence variant must carry non-empty content; an Na with an
        // empty reason is the "silence on an applicable axis" the canon forbids.
        let ok = match row.ev {
            Ev::Test(name, file) => !name.is_empty() && !file.is_empty(),
            Ev::Src(marker, file) => !marker.is_empty() && !file.is_empty(),
            Ev::Na(reason) => !reason.is_empty(),
            Ev::Tests(list) => !list.is_empty(),
            Ev::Srcs(list) => !list.is_empty(),
        };
        if !ok {
            return false;
        }
        i += 1;
    }
    true
}

const _: () = assert!(
    SECTION7_AXES == 12,
    "the canon defines exactly twelve §7 axes",
);
const _: () = assert!(
    covers_all_axes(SECTION7),
    "every §7 axis 1..=12 must have at least one row — silence on an axis is a violation",
);
const _: () = assert!(
    every_row_well_formed(SECTION7),
    "a §7 row is malformed: axis out of 1..=12, an empty field, or an Na with no reason",
);

// ===========================================================================
// Disk helpers
// ===========================================================================

/// Walk up from this crate's manifest dir to the worktree root (the dir that
/// holds `crates/postgres/proto`).
fn repo_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join("crates").join("postgres").join("proto").is_dir() {
            return dir;
        }
        assert!(
            dir.pop(),
            "could not locate the worktree root (a dir containing crates/postgres/proto) \
             walking up from {}",
            env!("CARGO_MANIFEST_DIR"),
        );
    }
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    match std::fs::read_to_string(&path) {
        Ok(body) => body,
        Err(e) => panic!("§7 artifact cites a file that cannot be read: {} ({e})", path.display()),
    }
}

/// True iff `src` defines a `#[test] fn <name>(` (with or without leading
/// whitespace before `fn`). Matches the form every cited test is written in.
fn defines_fn(src: &str, name: &str) -> bool {
    let needle = format!("fn {name}(");
    src.contains(&needle)
}

/// All cited `(test_fn, file)` pairs, flattening `Tests`. Only the `#[test]`
/// kinds (`Test`/`Tests`); `Src` markers are checked separately.
fn cited_tests() -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    for row in SECTION7 {
        match row.ev {
            Ev::Test(name, file) => out.push((name, file)),
            Ev::Tests(list) => out.extend_from_slice(list),
            Ev::Src(..) | Ev::Srcs(_) | Ev::Na(_) => {}
        }
    }
    out
}

fn cited_src_markers() -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    for row in SECTION7 {
        match row.ev {
            Ev::Src(marker, file) => out.push((marker, file)),
            Ev::Srcs(list) => out.extend_from_slice(list),
            Ev::Test(..) | Ev::Tests(_) | Ev::Na(_) => {}
        }
    }
    out
}

// ===========================================================================
// Teeth: the citations resolve, the structure holds
// ===========================================================================

/// Tier-1 is the const gate above; this mirrors it at test level so a runner
/// sees an explicit green for "all twelve axes covered".
#[test]
fn section7_covers_all_twelve_axes() {
    let mut seen = [false; 12];
    for row in SECTION7 {
        assert!(
            (1..=12).contains(&row.axis),
            "row for sub-point {:?} has out-of-range axis {}",
            row.subpoint,
            row.axis,
        );
        seen[usize::from(row.axis) - 1] = true;
    }
    for (idx, present) in seen.iter().enumerate() {
        assert!(present, "§7 axis {} has no row in SECTION7", idx + 1);
    }
    assert_eq!(seen.len(), SECTION7_AXES, "SECTION7_AXES must equal the canon's 12 axes");
}

/// Every `Na` sub-point carries a non-empty justification — silence on an
/// applicable axis is itself a §7 violation.
#[test]
fn every_subpoint_has_evidence_or_justified_na() {
    for row in SECTION7 {
        match row.ev {
            Ev::Na(reason) => assert!(
                !reason.trim().is_empty(),
                "axis {} sub-point {:?} is N/A with no reason — that is a §7 violation",
                row.axis,
                row.subpoint,
            ),
            Ev::Test(name, _) => assert!(!name.is_empty()),
            Ev::Src(marker, _) => assert!(!marker.is_empty()),
            Ev::Tests(list) => assert!(!list.is_empty(),
                "axis {} sub-point {:?} cites an empty test list", row.axis, row.subpoint),
            Ev::Srcs(list) => assert!(!list.is_empty(),
                "axis {} sub-point {:?} cites an empty source-marker list", row.axis, row.subpoint),
        }
    }
}

/// THE rot guard: every cited `#[test]` exists on disk. Renaming or deleting a
/// cited test turns this red, so the table can never claim phantom coverage.
#[test]
fn every_cited_test_exists_on_disk() {
    // Cache file bodies so each is read once.
    let mut bodies: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
    for (name, file) in cited_tests() {
        let body = bodies.entry(file).or_insert_with(|| read(file));
        assert!(
            defines_fn(body, name),
            "§7 table cites `{name}` in {file}, but no `fn {name}(` exists there — \
             the test was renamed or deleted; fix the citation or restore the test.",
        );
    }
}

/// Every cited source marker (a compile-time gate / structural invariant)
/// exists on disk. Same teeth as the test citations, for `Src` evidence.
#[test]
fn every_cited_src_marker_exists() {
    let mut bodies: std::collections::HashMap<&str, String> = std::collections::HashMap::new();
    for (marker, file) in cited_src_markers() {
        let body = bodies.entry(file).or_insert_with(|| read(file));
        assert!(
            body.contains(marker),
            "§7 table cites source marker {marker:?} in {file}, but it is absent — \
             the gate/invariant was renamed or removed; fix the citation or restore it.",
        );
    }
}

/// The engine module's `//!` narrative must list all twelve axis names, so a
/// reader of the engine source finds the same axes this table verifies (and an
/// edit dropping an axis from the narrative turns red).
#[test]
fn engine_doc_lists_all_twelve_axis_names() {
    // Normalize: drop `//!` doc prefixes and collapse all whitespace to single
    // spaces, so an axis name wrapped across two doc lines still matches.
    let raw = read(SRC_MOD);
    let flat = raw
        .lines()
        .map(|l| l.trim_start().trim_start_matches("//!"))
        .collect::<Vec<_>>()
        .join(" ");
    let flat = flat.split_whitespace().collect::<Vec<_>>().join(" ");
    for name in [A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12] {
        let norm = name.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            flat.contains(&norm),
            "engine/mod.rs §7 narrative does not mention axis {name:?}",
        );
    }
}

/// Negative control: the disk-existence checker discriminates — a deliberately
/// absent fn name is NOT found, and a real one IS. Proves the teeth bite (the
/// existence test is not trivially always-green).
#[test]
fn existence_check_has_teeth() {
    let body = read(HOSTILE);
    assert!(
        !defines_fn(&body, "this_test_does_not_exist_anywhere_zzz"),
        "the fn-existence finder is too loose — it matched a name that is absent",
    );
    assert!(
        defines_fn(&body, "out_of_phase_or_unknown_tag_tears_down"),
        "the fn-existence finder is broken — it missed a real cited test",
    );
}

// ===========================================================================
// Census: every load-bearing count re-derived from disk
// ===========================================================================

/// Read every `engine/*.rs` source file's body (the census scan surface).
fn engine_sources() -> Vec<String> {
    let dir = repo_root().join("crates/postgres/proto/src/engine");
    let mut out = Vec::new();
    let entries = std::fs::read_dir(&dir).expect("read engine source dir");
    for entry in entries {
        let path = entry.expect("engine dir entry").path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(std::fs::read_to_string(&path).expect("read engine source file"));
        }
    }
    out
}

fn count_in_engine(needle: &str) -> usize {
    engine_sources().iter().map(|body| body.matches(needle).count()).sum()
}

/// `crate::wire_pin!(` count across `engine/` == WIRE_PINS.
/// Reproduce: `grep -rho 'crate::wire_pin!(' crates/postgres/proto/src/engine/*.rs | wc -l`
#[test]
fn census_pin_count() {
    assert_eq!(
        count_in_engine("crate::wire_pin!("),
        WIRE_PINS,
        "footprint-pin count drifted; re-derive and update WIRE_PINS (the pin census)",
    );
}

/// `EngineError<E>` variant count == ENGINE_ERROR_VARIANTS. Scans the enum body
/// and counts upper-case-leading lines (the variant decls).
#[test]
fn census_engine_error_variants() {
    let src = read(SRC_ERROR);
    let mut in_enum = false;
    let mut count = 0usize;
    for line in src.lines() {
        if line.starts_with("pub enum EngineError<E> {") {
            in_enum = true;
            continue;
        }
        if in_enum {
            if line.starts_with('}') {
                break;
            }
            if line.trim_start().chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                count += 1;
            }
        }
    }
    assert_eq!(
        count, ENGINE_ERROR_VARIANTS,
        "EngineError variant count drifted; re-derive and update ENGINE_ERROR_VARIANTS",
    );
}

/// Active-verb count == ACTIVE_VERBS.
/// Reproduce: `grep -c "live: Live<'b>" crates/postgres/proto/src/engine/verbs.rs`
#[test]
fn census_active_verbs() {
    let src = read("crates/postgres/proto/src/engine/verbs.rs");
    assert_eq!(
        src.matches("live: Live<'b>").count(),
        ACTIVE_VERBS,
        "active-verb count drifted; re-derive and update ACTIVE_VERBS",
    );
}

/// Cold-classified-branch count == COLD_CLASSIFIED_BRANCHES.
/// Reproduce: `grep -rho 'core::hint::cold_path()' crates/postgres/proto/src/engine/*.rs | wc -l`
#[test]
fn census_cold_classified_branches() {
    assert_eq!(
        count_in_engine("core::hint::cold_path()"),
        COLD_CLASSIFIED_BRANCHES,
        "cold-classified-branch count drifted; re-derive and update COLD_CLASSIFIED_BRANCHES",
    );
}

/// `#[non_exhaustive]` attribute count == NON_EXHAUSTIVE_ATTRS (line-leading,
/// so doc-comment mentions of the attribute are not counted).
/// Reproduce: `grep -rcE '^#\[non_exhaustive\]' crates/postgres/proto/src/engine/*.rs` (summed)
#[test]
fn census_non_exhaustive_attrs() {
    let count: usize = engine_sources()
        .iter()
        .map(|body| body.lines().filter(|l| l.trim_start() == "#[non_exhaustive]").count())
        .sum();
    assert_eq!(
        count, NON_EXHAUSTIVE_ATTRS,
        "#[non_exhaustive] surface count drifted; re-derive and update NON_EXHAUSTIVE_ATTRS",
    );
}
