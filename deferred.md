# Deferred Items Registry

Active registry of **OPEN work items**, unstable-Rust blockers, and
measurement-rejected decisions. For full per-DEF history see
`git log` — commit messages capture the detail.

**Cleaned 2026-04-24** — prior 4800-line session journal archived
to git history. Keep this file a live work queue, NOT a diary.

## Conventions

- Each active item has a stable `DEF-NNN` ID, phase-trigger (when
  to revisit), and verification hook (what closes it).
- Grep this file before touching anything it references — original
  context may be load-bearing.
- On close: delete the entry OR collapse to one line in `## Closed
  Index`. Don't keep verbose "why it was open" text after ship.

---

## §A. Active OPEN — live work queue

### Phase-gated (blocked on other phases)

| DEF | Item | Blocked on |
|-----|------|-----------|
| DEF-005 | `AwaitingQueryReply { reply, hash, columns }` | Phase 1c Query/Execute flow |
| DEF-006 | `StreamingRows { stream, hash, columns }` | Phase 1d QueryStream |
| DEF-007 | `InTransaction { level, depth }` | Phase 1c Begin/Commit/Rollback |
| DEF-009 | `Closed` state variant | Phase 1e async wrapper shutdown |
| DEF-155 | Generational counter prep for 1c-5 pipelining | Phase 1c-5 |
| DEF-156 | `materialise_push` vs `materialise_feed` type split | Phase 1c-5 pipelining |
| DEF-157 | `ProtoState` sum-of-subsums restructure | Phase 1c research |
| DEF-159 | SCRAM arena (D001) | A10 shipped but SCRAM arena scope is 1c-5+ |
| DEF-160 | `PgCommand::Parse` carries `&'a str` SQL | Phase 1c-3a+ lifetime API |
| DEF-161 | Error-body arena (closed — see DEF-184 A1+A13 shipped) | — |
| DEF-162 | cargo-mutants kill-rate target | Phase 1d |

### Infra (ship any time)

| DEF | Item | Size | Priority |
|-----|------|------|----------|
| DEF-143 | criterion bench harness | **PARTIAL SHIPPED** — 4 groups live, `push_bind_execute` bench still pending | Low (works) |
| DEF-167 | Split `action.rs` / `dispatch.rs` into submodules | Large | Cosmetic; deferred after phase 1c fully lands |

**DEF-134 closed 2026-04-24** (`1fde5d1`) — pragmatic stable-Rust
property-test harness instead of nightly `cargo-fuzz`: 4 tests,
120K random-input iterations total, asserts parse_header /
feed_bytes / push+feed / progressive-feed all classify without
panic / infinite loops / torn state. See §D closed index.

### Docs (DEF-163 remainder)

Partial shipment 2026-04-24 (commit 252ed6b) — 7 of ~20 sub-items
done. Remaining:

| Sub-ID | Item | Notes |
|--------|------|-------|
| G001 | Cross-reference "1c-5 pipelining" at docstring sites | Grep for TODO markers |
| G008 | Sweep all "Tier-1 compile" claims for enforcement citations | Mechanical |
| G016 | ident.rs module-level FixedStrKind trait hierarchy diagram | ASCII-art |
| G017 | scram/mod.rs RFC 5802 exchange-flow diagram | ASCII-art |
| G018 | compute_push vs dispatch naming convention top-of-file | Single file |
| G021 | Template `#[expect(...)]` reason strings for grep-ability | Mechanical |
| F001 | `#[doc(hidden)] pub` comment sweep (uniform citation) | Several sites |

### Stragglers (low-impact, defer-until-driver)

| DEF | Item | Why deferred |
|-----|------|--------------|
| B5 | Public API narrowing | Wait for `bsql-driver-postgres` to identify truly-external items |
| A8 | `usize → u16/u8` narrowing remainder | Diminishing returns; remaining sites are architecturally-usize or cold |
| DEF-164 | `ReplyId.delivered` debug-assertions-gated | Only if DEF-143 bench shows measurable impact |
| DEF-165 | `ParamOids::n_params` u16→u8 | Trivial; bundle with future small-wins session |
| DEF-166 | `PodBytes<N>` visibility `pub → pub(crate)` | Via state-field privatize; pairs with B5 |

### Layout / perf wave (2026-04-27 audit)

**Origin:** user-driven audit 2026-04-27 — confronted the DEF-190/191 RowStream
wins with the observation that they're pre-decoder gains and don't address the
real measurable hot-path (per-column decode), wire-I/O dominance on RTT, or
the 720 B of cold-path inline footprint in `PgProtocol`. **All ten items below
ship under §1 priority pyramid (safety → tier → perf), each measurement-gated
per CREDO §4.12. NONE deferred without structural reason.**

| DEF | Item | Tier delta | Expected win | Status |
|-----|------|-----------|--------------|--------|
| DEF-195 | `BoundedU8<MAX>` newtype + apply to `RowDesc::n_columns` (u16 → BoundedU8<32>). NonZeroU8-backed offset-by-one encoding gives stable-Rust niche on `Option<BoundedU8<MAX>>` (1 B). New module `crates/bsql-pg-proto/src/bounded.rs` (~180 LoC + tests). Tier-1 size pins on `BoundedU8<32>`, `Option<BoundedU8<32>>`, `Option<RowDesc>`. | Tier-2 (rejected at construct via `try_new`) → tier-1 niche `Option<BoundedU8<MAX>>` (NonZeroU8 niche absorbs Option discriminant). | `Option<RowDesc>` 140 → 136 B (4 B saved per Option). `PgProtocol` 4352 B unchanged (alignment of other fields absorbs the field-level saving — niche-saving captured at Option<RowDesc> level where it counts). Bench: no regression (numbers within noise vs def202-simdutf8). | SHIPPED 2026-04-28 |
| DEF-198 | Witness-guard typestate (DEF-119 round-4): `proto.as_ready()` → `Option<ReadyGuard<'_>>`; `push_command` / `push_bind_execute` are methods of guard. `connection_status() -> ConnectionStatus { Ready, Busy, Handshaking, Errored(StateErrorKind) }` for caller-side recovery decisions when guard returns None. | Tier-1 (compile-rejected on public API surface) | Foundation for pipelining | SHIPPED 2026-04-28 |
| DEF-200 | Per-state-bucket dispatch LUTs — A7 adjacent shape (split global tag dispatch into `[fn; 14]` per bucket) | Same tier | Branch-predictor learns smaller patterns | PROPOSED, measurement-gated |
| DEF-201 | `PgCommand` per-kind monomorphisation: `trait PgCommandT { const TAG; type Payload }` + generic `push_command<C: PgCommandT>` | Tier-1 (typed dispatch) | Caller pays only HIS command size; current 2176 B per-command → real size | PROPOSED, **design discussion required** before impl |
| DEF-202 | `simdutf8` for `<&str as FromPgText>::from_pg_text`. Routes UTF-8 validation through `simdutf8::basic::from_utf8` (lane-wise NEON shuffles + masks on aarch64; scalar fast-path elsewhere). Behaviour byte-identical to `core::str::from_utf8`. Hybrid length-threshold dispatch was tested and rejected (branch overhead ~1.5 ns/col exceeded short-ASCII savings). Workspace dep `simdutf8 = { version = "0.1", default-features = false, features = ["aarch64_neon"] }`. Bench `def202-simdutf8` vs `pre-simdutf8` baselines on aarch64-apple-darwin: short ASCII (17 B × 5) +9.9% (43.6 vs 40.3 ns — acceptable cost on the cheapest path), **long ASCII (~200 B × 5) −49.9% (~2× faster: 26.6 vs 53.0 ns)**, **multi-byte UTF-8 (~78 B Cyrillic × 5) −74.0% (~3.9× faster: 78.5 vs 309.4 ns)**. | Same tier (runtime UTF-8 classification → tier-3 by `DecodeError::NonUtf8`; behaviour parity with `core::str::from_utf8` documented + property-tested upstream). | 2-4× speedup on the dominant decoder bottleneck for analytics / internationalised workloads. Binary-format codec sub-item (`i32::from_be_bytes` for binary wire) deferred to a follow-up DEF — requires server-side per-column binary opt-in via `format_codes` in Bind. | SHIPPED 2026-04-28 |
| DEF-203 | Niche audit sweep — apply [`BoundedU8`]/[`BoundedU16`] to `len`-style fields uniformly. **API SHIPPED 2026-04-28**: unified `bounded.rs` with `BoundedU8<const MAX: usize>` (MAX ≤ 254), `BoundedU16<const MAX: usize>` (MAX ≤ 65_534), and the [`BoundedLen<N>`] sealed trait bridging both for generic length-storage parameters in container types. Tier-1 const-asserts pin size + alignment + niche for both; tier-1 compile-time construction via `new_const::<VAL>()` and `bounded_u8!`/`bounded_u16!` macros (compile_fail doctests prove out-of-range build error); tier-2 runtime via `try_new` returning Option. **Sites migrated**: `RowDesc::n_columns` (BoundedU8<32>); `OtherEncoding::len` (BoundedU8<32>). **Sites pending migration of the same pattern**: `FixedStr<N, Tag>::len` (currently `u16`; needs `LenT: BoundedLen<N>` generic on FixedStr with `BoundedU16<N>` default for backward-compat — type aliases `Ident`/`DatabaseName`/`StmtName`/`PortalName` would pick `BoundedU8<63>` for the niche win; const-fn cascade audit needed since `Self::new` is `pub const fn`). `PodBytes<N>::len` (similar). Both pending due to const-fn cascade through `LenT::ZERO` trait const access (requires either dropping `const fn` on FixedStr::new or adding inherent const-bound bridge). ~150-300 LoC follow-ups. | Tier-2 by-construct (try_new) + tier-1 compile-time (new_const) + tier-1 niche on `Option<T>` per site. | API SHIPPED + 2 sites SHIPPED: ~4 B saved per migrated Option (cumulative across crate ~10 B). FixedStr migration would add ~3 B saved per Ident/DatabaseName/StmtName/PortalName (4 types × 4 instances per session = 16 instances; ~48 B saved per session). | API + 2 SITES SHIPPED, FixedStr/PodBytes migrations PROPOSED |
| DEF-209 | **Row-level batch UTF-8 validation API** — surfaced by DEF-202 audit ("+10% можно сократить?" probe). Per-column `<&str as FromPgText>::from_pg_text` calls `simdutf8::basic::from_utf8` once per text column, paying the ~3 ns simdutf8 setup cost N times for an N-text-column row. The simdutf8 setup cost (NEON state-vector init, dispatch boundary) is amortisable across columns via `simdutf8::basic::imp::ChunkedUtf8Validator` (streaming validation that pause-resumes across non-contiguous inputs) — but `ChunkedUtf8Validator` is an `unsafe` trait, blocked by `#![forbid(unsafe_code)]`. **Design**: introduce `DataRowRef::columns_text(self) -> Result<TextColumnsIter<'a>, DecodeError>` — validates ALL non-NULL column-data slices in a single `ChunkedUtf8Validator` session (encapsulated in a small `unsafe`-using inner module, audited and gated behind a tier-1 contract). Returns an iterator yielding pre-validated `&'a str` slices for each column without per-column re-validation. Callers with text-heavy rows (analytics queries, log columns) get amortised setup cost — projected savings of ~12 ns on a 5-text-column row (4 of 5 simdutf8 setups eliminated). **Tier impact**: introduces a CONTROLLED `unsafe` boundary inside a sealed `pub(crate) mod row_text_validate` (the only unsafe site outside the `#![forbid(unsafe_code)]` blanket — would require relaxing forbid to deny + per-site allow). Callers see a 100% safe API. **Tier-1 closure** via: (a) the `ChunkedUtf8Validator` API contract is upheld in one audited site; (b) caller-facing types prove valid UTF-8 by construction; (c) memory-probe + property tests cover boundary cases (column boundaries fall inside a multi-byte UTF-8 sequence — must be classified as decode error). ~250-400 LoC + audit doc + property tests. Distinct from DEF-202 (already shipped pure simdutf8 path) — DEF-209 is layered on top to amortise the SHIPPED simdutf8's per-column overhead. | Same tier (still tier-3 by-classification on validation failure; tier-1 closure on the unsafe boundary via single-site audit). | Speculative ~12 ns per row on text-heavy rows (4× simdutf8 setup eliminated for 5-text-col row). Concrete bench gate: must demonstrate measurable win on representative workloads. | PROPOSED, requires `unsafe` boundary policy decision (relax `forbid` to `deny + per-site allow`) — design discussion before impl per architect.txt process |
| DEF-208 | **`compute_push_*` Idle-only refactor** — closes DEF-198 surface 6 ("это точно tier-1 полностью?" probe). Pre-DEF-208, internal `compute_push_<cmd>` retained 5-arm dispatch on `state.push_class()` with non-Idle arms emitting `FailReply` defensively — dead code from the public ReadyGuard path. Refactor: extracted 7 `compute_push_<cmd>_idle_only` siblings (single Idle-arm bodies with `debug_assert!(matches!(state, ProtoState::Idle))`), added top-level `compute_push_idle_only` dispatcher, routed `push_command_internal` and `push_bind_execute_internal` through Idle-only path. Original dispatching `compute_push_<cmd>` and `compute_push` retained but gated behind `#[cfg(test)]` (only the internal `compute_push_tests` module uses them — production binary does not contain those bytes). The standalone `compute_push_bind_execute<P>` was DELETED entirely (zero callers post-refactor). | Tier-3 (internal defensive arms) → tier-1 by-construction: production push routes only through Idle-only siblings; non-Idle defensive code is `#[cfg(test)]` test-target-only. Surface 5 (runtime "is state Idle right now" via `Option<ReadyGuard>`) remains tier-3 — irreducible per Postgres server-driven state nature. | Tier closure of DEF-198 surface 6 + recovered the +3.2% perf cost from `push_command/ping` bench. Bench `def208-idle-only` vs `def207-letfix` (pre-DEF-198 baseline): push_command/ping -0.4% (within noise — fully recovered), ping_round_trip +0.4%, push_command/ping_amortised +1% (residual from `as_ready` borrow check), column_decode/iter_columns_raw -2.2% (improved). | SHIPPED 2026-04-28 |
| DEF-207 | **Branchless wrapping accumulator for `parse_pg_int_signed`** — corrected scope after audit (initial framing was wrong: claimed UTF-8-validate skip on i32 path, but DEF-184 already shipped that — `<i32 as FromPgText>` uses `parse_pg_int_signed!` macro directly, never routes through `<&str>::from_pg_text`). The remaining opportunity is on the **digit-loop body itself**: current macro uses `checked_mul(10) + checked_sub/add(d)` per digit (one branch each). Refactor to `wrapping_mul(10) + wrapping_add(d)` over an i64 accumulator, with a single end-of-loop overflow check via `i32::try_from(signed_acc)`. Requires a length pre-check (`digits.len() <= 10` for i32; max canonical digits) so the i64 accumulator cannot overflow during loop. Speculative win ~30% on the i32 hot loop (per-digit branches eliminated); requires bench-evidence per CREDO §4.12 before ship. **Tier-1 closure**: exhaustive const-asserts on i32::MIN, i32::MAX, off-by-one, every non-digit byte rejected, empty/lone-sign edge inputs. ~120 LoC parser + ~80 LoC const-assert grid. Generalises to i16/i64/u32/u64 via the same length-pre-check pattern (digit caps differ). | Same tier (runtime parse → tier-3 by classified `IntParse` variant; tier-1 via const-assert grid for boundary correctness). | Speculative ~30% on i32 decode (~7.6 → ~5 ns/col). Real win pending bench measurement. Smaller than initially claimed — the dominant wasted-cycle opportunity is on `<&str>::from_pg_text` UTF-8 validate, not i32. | PROPOSED, low priority — measurement-gated, smaller scope than original DEF-207 framing |
| DEF-210 | **Tier-1 audit findings (architect-driven 4-pass audit, 2026-04-28 + 2026-05-04)** — full closure detail in §D Phase 1c entry below; this row stays in §A only as a pointer. **ALL CLOSED in §D**: SR-01 Path C/D, SR-02, SR-03, SR-04, SR-05, SR-06, SR-07, ML-01, ML-03, BS-11, CF-02 (initial batch, commit `d0b794e`); REC-06 SCRAM Box consolidation + NB-04 residue-policy per-class pin tests (follow-up batch, commit `f69ecd7`); PERF-02 single-Box SCRAM final closure (architect 4th-pass finding, commit `69a86a7`). **Acknowledged-as-policy**: BS-01 `panic = "abort"` zeroize gap (REOPENED in DEF-211 SAFE-06); BS-02 transitive `unsafe` in `simdutf8`/`heapless`/`sha2` deps per CREDO §11 (REOPENED in DEF-211 SAFE-01). | Tier-2 by-discipline → tier-1 by-construction at 15 sites; see §D for per-site detail. | Closure of "стеклянная архитектура" risks. | §D ALL CLOSED 2026-05-04. |
| DEF-211 | **5th-pass radical architecture audit (architect-agent, 2026-05-04)** — principal asked for the deepest, most thorough audit including alternative architectures. Verdict: **INCREMENTAL-improvements (~250 LoC), NOT at architectural ceiling.** **Headline finding**: **FAKE-01** — wildcard `_ => {}` in `clear_session_residue_if_idle_or_errored` is **the one production-code FAKE TIER-1** in the audit, directly contradicts CREDO §0 ("the invariant hangs on luck rather than construction"). Latent hazards (don't violate today's tier claims, but reduce future-proofing): **FAKE-08** `IdleStateProof::new()` accidental-Default-derive surface; **FAKE-16** `#[derive(Default)]` on `ProtoState` enables `mem::take→Idle` latent hazard; **FAKE-17** `panic = "abort"` zeroize gap (now actionable via SAFE-06 SecretRegistry pattern, not policy-only); **FAKE-19** `bench-hooks` cargo feature exposes test-only API surface to release builds. Substantive improvements: **SAFE-01/ALT-10** — replace `heapless::Vec` with hand-rolled `BoundedVec<T, N>` (eliminates the largest transitive `unsafe` surface in the dep graph; ~80 LoC); **SAFE-06** SecretRegistry trait + panic_hook integration for zeroize-on-panic (~30 LoC); **INNO-01** `#[derive(Pristine)]` proc-macro for `SessionParams::is_pristine` (~120 LoC, lifts BS-11 broad-scope tier-3 to tier-1 across all fields). Trivial wins: **FAKE-11** math-identity assert `MAX_FRAME_LEN_FIELD == READ_BUF_CAP - 1` (~10 LoC); **SAFE-05** add `clippy::let_underscore_drop`/`let_underscore_must_use` to forbid bundle (2 LoC); **SAFE-07** add `#[non_exhaustive]` to `ErrorKind` + `ConnectionStatus` (4 LoC, pre-empts SemVer footgun); **FAKE-14** add `cargo test --doc` to CI (docs/CI). Per principal directive "никаких 'это сложно, отложим' или 'это не bottleneck — пропустим'" + "каждый байт ценен / каждая наносекунда на счету" — every actionable item executed, no defer. | Tier-2/3 → tier-1 by-construction at 10 sites; eliminates one of the two largest transitive `unsafe` surfaces. | Closes the one production FAKE TIER-1; future-proofs latent hazards; opens panic=abort gap closure. | PROPOSED — Round 1 (trivial tier, ~31 LoC) shipping next; Rounds 2-4 sequential. |
| DEF-212 | **Hybrid architecture re-design — per-command bytes-only push + retained amortised feed (design phase verified 2026-05-04, architect-agent adversarial audit)** — surfaced after DEF-211 ship: principal challenged "is `OutActions = ManuallyDrop<heapless::Vec<Action,9>> = 800 B` per-call return frame the cleanest possible shape?" and asked for boldest viable redesign. **Three alternatives initially evaluated:** **(Alt X)** typed per-command tuples Push only, feed unchanged. **(Alt Y)** bytes-only push + one-frame-at-a-time feed + arena-extended FailReply. **(Alt Z)** defer, Box FailReply. **Architect-agent verification 2026-05-04 REJECTED Alt Y as posed**, found 6 concrete flaws + 5 new risks not in initial brief: **(F1)** SCRAM client-final emits SendBytesRange from FEED path (`dispatch.rs:1262-1273`) — Alt Y's FeedEvent had no SendBytes variant, must add; FeedEvent grows 40 → ~88 B; **(F2)** ErrorArena extension to all ProtocolError variants breaks single-slot single-inflight discipline — multi-slot would cost ~2.6 KB and defeat `PgProtocol == 4352 B` pin; honest path keeps ProtocolError value-type → "Action enum eliminated" was incorrect, payload reborn in FeedEvent at same 88 B size; **(F3)** WriteBuf brand discipline (DEF-154) — single-inflight invariant breaks if push-side-effect doesn't enforce drain-before-next-push; mitigation = explicit `#[must_use]` contract on PushFailure (tier-3 by-discipline, same as today); **(F4)** ZeroizeOnDrop for SCRAM bytes in WriteBuf — need `wb.clear()` at entry of every push call to preserve P0-C zeroize-on-clear; **(F5 — KILLER)** per-frame caller loop overhead — Alt Y's `loop { match advance_one_frame {...} }` processes 1 frame per call vs today's `feed_bytes` batched N frames per call; per-call overhead ~50 ns × N frames means **+93% regression on ping_round_trip** (54 ns baseline + 50 ns added per frame on a 1-frame Z consume); **(F6)** loses `feed_bytes_bounded<const BOUNDED: bool>` (B6) specialisation — verified load-bearing per §B (false in remove-experiment regressed 4/4 benches +9-18%, p<0.05). **Verdict: Alt Y' (modified Alt Y)** = Alt X's per-command typed pushes structurally + Alt Y's bytes-only push delta layered + **KEEP `feed_bytes` + `OutActions` 800 B for amortised batched feed** + optional secondary `advance_one_frame -> FeedEvent<'r>` ~88 B per call (gated on principal's pipelining-timing decision — see open question below). **Alt Y' final shape:** **(1)** KEEP `OutActions<'w, 'r>` 800 B for `feed_bytes` — amortised batching is the perf win, breaking it kills ping bench. **(2)** NEW per-command typed pushes `ReadyGuard::push_<cmd>(...) -> Result<(), PushFailure>` ~88 B (PushFailure carries consumed ReplyId + full ProtocolError so caller can resolve oneshot on Err). **(3)** OPTIONAL `advance_one_frame(&mut wb) -> FeedEvent<'r>` ~88 B per call as SECONDARY API for pipelining 1c-5 forward-compat. **(4)** `FeedEvent<'r>` variants: `Idle, NeedMoreBytes, StreamingRows, SendBytes(&'r [u8]), Deliver(id, Reply<'r>), Fail(id, ProtocolError), Close` — note `SendBytes` (Flaw 1 SCRAM client-final), note `ProtocolError` value-type (Flaw 2 single-slot arena preserved). **(5)** `wb.clear()` at every push entry preserves zeroize discipline (Flaw 4). **(6)** `#[must_use = "PushFailure carries consumed ReplyId; you MUST resolve user's oneshot before discarding"]` on PushFailure (R-5). **(7)** Brand discipline preserved (`with_branded` closure inside each push, no public exposure). **(8)** Action enum kept (used by OutActions from feed_bytes; FeedEvent::Fail variant payload mirrors Action::FailReply at same 88 B). **Honest tier table (after audit):** push surface tier preserved (DEF-198 ReadyGuard + IdleStateProof intact), feed surface tier preserved (`feed_bytes` retained as primary, `advance_one_frame` secondary), single-inflight invariant preserved, brand integrity preserved, zeroize-on-clear preserved, push return frame 800 → ~88 B, feed return frame UNCHANGED 800 B (amortised), Action enum size UNCHANGED 88 B (mirror in FeedEvent). **Bench projections (architect-verified):** push_command/ping_amortised 54 → ~37 ns (-31%); feed_bytes/ping_amortised UNCHANGED; iter_rows/per_row UNCHANGED; iter_rows/select_1k_rows marginal +2%. **Effort estimate (architect-revised):** ~600-900 LoC, ~6-9 days. **`generic_const_exprs` (RU-03) re-verified unstable on Rust 1.95.0** during design phase — still nightly-only (rust-lang/rust#76560, soundness bugs delaying stabilisation since 1.50 era); ruled out as a path. **DEF-213 (Action size reduction) — STATUS: NOT NEEDED.** Architect Flaw 2 finding: keeping ProtocolError value-type means Action::FailReply stays at 88 B; the alternative path of `Box<FailReplyPayload>` (88→32 B + 1 cold alloc) trades cold allocs for stack savings — reject per CREDO §1 (allocations are tier surface; stack frame is amortised at 800 B / 9 actions). **PRINCIPAL DECISIONS (2026-05-04):** ship `advance_one_frame` + `FeedEvent` NOW (Variant A — forward-compat for 1c-5; ~+150 LoC). Calibration: Q1=A (PushFailure pub fields), Q2=regression-only gating (max +3-5% on existing benches; no improvement floor — any positive perf delta is a win), Q3=A (FeedEvent enum with `Idle` ZST variant, exhaustive match preferred over `Option<FeedEvent>`). **IMPL PLAN ARCHITECT-VETTED 2026-05-04 — APPROVE WITH 5 CRITICAL/STRONG MODIFICATIONS + 1 ADVISORY:** **(M1 critical)** add `MAX_OWNED_SEND_LEN >= max_parse_message_size() + 5` const-assert in `write_buf.rs` — sibling to existing Bind+Execute+Sync and Describe+Sync pins; closes tier-4 "happens to fit" gap to tier-1 since Alt Y' appends Sync inline post-impl; **(M2 critical)** `FeedEvent::Fail(id, cause)` semantically implies socket close (documented on variant via `#[must_use]` text) — reserves `FeedEvent::Close` for the no-id case (state→Errored without in-flight reply); resolves the 2-event-per-frame gap (FailReply+CloseSocket pair from install_errored); **(M3 critical)** `FeedEvent<'wb, 'r>` not `FeedEvent<'r>` — `SendBytes(&'wb [u8])` borrows caller's wb; `Deliver(_, Reply<'r>)` borrows proto's row_desc_slot — collapsing to single lifetime forces `'wb = 'r` at use sites and breaks composable patterns (mirrors existing `OutActions<'w, 'r>` two-lifetime contract); **(M4 strong)** exact `==` size pins in `lib.rs` per CREDO §III no-permissive-ranges — `size_of::<PushFailure>() == 80` (NonZeroU64 + ProtocolError 72 B), `size_of::<FeedEvent<'static, 'static>>() == 88` (max variant Fail + tag with niche optimisation); **(M5 strong)** `materialise_push` mirrors `push_within_fanout_budget` discipline — `debug_assert!(false, "...")` dead arm for architecturally-impossible `SendBytesRange.apply == None` case; loud in dev/test, no-op in release; **(M6 advisory)** narrow the wb-residue contract docs — the actual risk window is ONLY between Err return and next push call (push_*_internal does `wb.clear()` at entry, `protocol.rs:1005, 1102`). All 5 modifications upgrade tier-3/4 surfaces to tier-1/2; preserve zero-cost performance (debug_asserts release-no-op; const-asserts compile-time). Plan touch (architect-counted): 12 test files migrate, ~35-50 test fns, 13 bench fns + 1 common helper trait, ~±50-200 LoC delta in tests; total impl ~+450/-100 LoC across 3 atomic commits. **Bench gates (regression-only per Q2):** iter_rows/per_row max +3% from baseline; feed_bytes/ping_amortised max +3%; parse_header max +3%; push_command/ping_amortised max +3% (i.e., not WORSE than today). Improvement floor: NONE — projected -31% on push_command, but any improvement is acceptable (revert only on regression). | Tier preserved on every surface (push, feed, brand, single-inflight, zeroize); 5 surfaces UPGRADED tier-3/4 → tier-1/2 (M1-M5). Push frame -88% (800→88 B). Zero new allocations. Zero new unsafe. | Smallest viable stack frames without breaking amortised batching; opens path to pipelining (DEF-005/006/155) via secondary `advance_one_frame` API. | **PHASE 1 SHIPPED 2026-05-04** — Commit 1 closes Phase 1a/1b/1c/1d atomically. **Foundation (Phase 1a)**: M1 const-assert (`write_buf.rs`), `pub struct PushFailure` with full caller-contract docstring (`action.rs:909`), `materialise_push` helper with M5 dead arms (`protocol.rs:3428+`), `push_command_internal`/`push_bind_execute_internal` return `Result<(), PushFailure>`, `ReadyGuard::push_command`/`push_bind_execute` updated, `BrandedWriteReserved::as_bytes` promoted to non-test `pub(crate)`, `lib.rs` re-exports `PushFailure`, `tests/common/mod.rs` `PushOrPanic` migrated. **Test migration (Phase 1b)**: 12 integration test files migrated (parse_spec, bind_execute_spec, describe_spec, audit_coverage_spec, def198_guard_closure_spec, scram_fuzz_spec, fuzz_stress_spec, row_stream_spec, ping_spec, simple_query_spec, startup_spec, tier_seams_spec) + `protocol.rs` internal `compute_push_tests` module + `tests/common/mod.rs` extended with shared `split_frame_plus_sync` and `split_bind_execute_sync` helpers (DRY across bind_execute and describe). 240+ tests green. **Bench migration (Phase 1c)**: `BenchPushOrPanic` trait switched return from `OutActions<'w,'p>` to `Result<(), PushFailure>` (mirrors production typed-push surface); 8 callsites updated to `let _ = black_box(push_out)` for explicit Result discard. **Validation (Phase 1d)**: `cargo test -p bsql-pg-proto` all green; `cargo clippy --workspace --all-targets --features bench-hooks -- -D warnings` clean (fixed pre-existing `doc_lazy_continuation` lint hits in `protocol.rs::materialise_push` docstring + `parse_spec.rs::parse_setup` docstring — `+`/`—` chars were misclassified as markdown bullet markers). **Bench results — bench gate Q2 PASSED with massive head-room** (regression-only gate, max +3% on existing benches; no improvement floor): `parse_header/rfq_header` 2.52 ns (matches def184-complete baseline — pure function, unaffected); `ping_round_trip/push_then_feed` 113 ns (vs pre-(212) 172 ns post-DEF-189 — −34%); `push_command/ping` 53.5 ns (vs pre-(212) ~99 ns post-DEF-189 — −46%); **`push_command/ping_amortised` 10.28 ns vs pre-(212) 54.17 ns post-PERF-02 — −81%, far exceeded projected −31%**. Iter rows benches preserved within noise (1.41-1.47 µs per 100-row pull = ~14-15 ns/row). The −81% headline reflects DEF-212's structural payoff: 800 B `OutActions` per-call return frame eliminated on push paths (replaced by ~80 B `Result<(), PushFailure>`); per-call materialise overhead removed; `wb`-as-output discipline avoids the borrow-checker friction that pre-(212) tests routed around with scope blocks. **PHASE 2 SHIPPED 2026-05-04** — Commit 2 closes secondary API for 1c-5 forward-compat. **`pub enum FeedEvent<'wb, 'r>`** (action.rs) with 7 variants (Idle, NeedMoreBytes, StreamingRows, SendBytes, Deliver, Fail, Close); `#[non_exhaustive]` for SemVer-safe additions; `#[must_use]` for tier-1 closure on caller side-effect contracts. M2: `Fail(id, cause)` semantically implies socket close (no separate Close event for in-flight Err). M3: two lifetimes preserved (`'wb` for SendBytes, `'r` for Deliver). **`pub fn feed_inbound(&mut self, &[u8]) -> Result<(), ReadBufFull>`** appends inbound bytes to read_buf without dispatching (Errored state silent no-op). **`pub fn advance_one_frame<'w,'r>(&'r mut self, &'w mut WriteBuf) -> FeedEvent<'w,'r>`** processes at-most-one user-observable event; reuses `feed_bytes_bounded(b"", wb, 1)` as driver (single source of truth for dispatch logic preserved — Phase 2 is additive, not a refactor). 10 new tests in `tests/advance_one_frame_spec.rs` cover all 7 FeedEvent variants + equivalence pin (advance_one_frame loop ≡ feed_bytes on canonical Ping/RFQ round trip). **Bench Q2 gate PASSED** (re-verified after first-run noise): `parse_header/rfq_header` 2.59 ns (+2.8% — within gate); `ping_round_trip/push_then_feed` 112.9 ns (-0.4%); `push_command/ping_amortised` 10.19 ns (-1%); zero regressions on iter_rows. **Phase 3 PENDING** (Commit 3 — M4 exact `==` size pins on `PushFailure == 80` + `FeedEvent<'static,'static> == 88` + ship doc). |
| DEF-206 | **Box\<PodBytes\<N\>\> heap-scrub gap** — surfaced by DEF-205 step 4 audit. SCRAM state variants `ConnectingScramAwaitingServerFirst` carry `client_first_bare: Box<PodBytes<128>>` and `client_nonce_b64: Box<PodBytes<48>>`. `PodBytes<N>` is `Copy` POD without `Drop`, so when the `Box` deallocates, heap memory is freed but bytes are NOT scrubbed. Severity LOW — content is SCRAM `client-first-message` (username + client nonce), all sent unencrypted on the wire (not actual secrets). Fix: `SecretPodBytes<N>` type parallel to `PodBytes<N>` with `ZeroizeOnDrop` derive; SCRAM variants migrated. Tier-1 by Drop chain via Box's drop firing the SecretPodBytes Drop. Defer until handshake-state scope work (Phase 1c-5+ pipelining). | Tier-3 by-audit → tier-1 via SecretPodBytes wrapper. | Security closure (low severity) — heap bytes scrubbed before free. ~60-80 LoC: SecretPodBytes type + 2 state field migrations + memory-probe test. | PROPOSED, low priority (LOW severity) |

**Priority order (§1) — REVISED 2026-04-28 after DEF-202 ship + Ext A/C register:**
1. **Decoder perf wins (bench-gated by DEF-197):**
   - **DEF-200** — per-state dispatch LUT (A7 adjacent shape). Branch-predictor-friendly bucket split. Measurement-gated.
   - **DEF-207** (rescoped) — branchless wrapping accumulator on i32 hot loop (~30% speculative; measurement-gated; lower priority).
2. **Architectural pre-discussion:** DEF-201 before any code (massive refactor; ≥3 alternatives per architect.txt process).
3. **Zero-cost micro-wins:** ~~DEF-195~~ SHIPPED. ~~DEF-203 wave 1~~ SHIPPED (OtherEncoding). DEF-203 waves 2/3 (PodBytes::len, FixedStr::len) PROPOSED — small scope follow-ups.
4. **Low-severity heap scrub:** DEF-206 (Box<PodBytes> SCRAM client-message scrub — wire-public bytes, low priority).
5. **DEF-202 follow-ups (decoder amortisation, design-discussion-gated):**
   - **DEF-209** — row-level batch UTF-8 validation API. Amortises simdutf8 setup cost across multiple text columns of the same row via `ChunkedUtf8Validator`. **Requires `#![forbid(unsafe_code)]` → `deny + per-site allow` policy decision** before impl. ~12 ns saved on text-heavy 5-col rows. Policy discussion gate per architect.txt.
   - **RU-07** (§C) — `aarch64_neon_prefetch` MSRV watch. Single-line flag flip when `stdarch_aarch64_prefetch` stabilises in stable Rust. Marginal win on long inputs.

**DEF-197 baseline insights logged here for future reference** (so each insight has a tracked owner item, not just bench output):

| # | Observation | Optimisation owner |
|---|-------------|--------------------|
| 1 | Per-row decode ~44 ns on typical 5-col SELECT row. Production scale: 1M rows × 5 cols = 44 ms decode-only — comparable to frame dispatch (14 ms / 1M frames). | Context for DEF-207 + DEF-202 (decoder perf), DEF-200 (dispatch perf). Not standalone DEF. |
| 2 | `<i32 as FromPgText>::from_pg_text` ~7.6 ns/col on 8-digit values. **CORRECTED 2026-04-28**: this is already the optimised `parse_pg_int_signed!` macro from DEF-184 (no UTF-8 validate in the i32 path). Per-digit cost ~0.95 ns is super-tight. Remaining opportunity is branchless wrapping accumulator (~30% speculative). | DEF-207 (rescoped, low priority, measurement-gated). |
| 3 | `<&str>::from_pg_text` ~7.2 ns/col — pure UTF-8 validation via `core::str::from_utf8`. **The actual hot point.** | **DEF-202** (simdutf8 SIMD validate, 5-10× win). |
| 4 | NULL fast-path 0.6 ns/null. `col_len == -1` shortcut in `ColumnsIter` (DEF-184 A5/B10) confirmed working. | Confirmation, not opportunity. |
| 5 | Pure header parse 1.10 ns. Sub-2-ns. | Confirmation, not bottleneck. |

(DEF-194 + DEF-196 + DEF-197 + DEF-204 + DEF-205 all closed —
bit-pack + cold field externalization + staleness via Drop chain
closure + decoder bench infra. Per CREDO §1: safety > perf;
security closures shipped first.)

**Cross-platform CI matrix** (project-wide concern):

All current size pins (`ProtocolError == 72`, `Action == 88`, `OutActions == 800`, `DispatchOutcome == 88`, `RowDesc == 136`, `Option<RowDesc> == 140`, `PgProtocol == 5080`, etc.) are **exact `==`** and reference target = **aarch64-apple-darwin**. The crate doesn't currently ship CI for x86_64-linux / riscv64 / wasm32 / windows / freebsd. When CI matrix extends, two outcomes are possible per pin:

1. **All targets converge** — the field types are alignment-stable across the targets in scope, so a single `==` pin works everywhere (most likely for POD-only structs like RowDesc, ProtocolError).
2. **Targets diverge** — per-target `#[cfg(target_pointer_width = "64")] const _: ...` blocks, set in the same commit that adds the target to CI.

**Forbidden**: permissive range pins as a "cushion for variance we haven't measured". CREDO §3 + §4.12: drift surface > variance cushion; silent regression beats explicit per-target pin. If a single value works everywhere, single `==` is correct. If targets diverge, list them all explicitly.

**Crazy / bold ideas pool** (not committed work — registered to prevent loss):

- **Compile-time precomputed message bytes** for ALL parameterless commands (extend `SYNC_WIRE_BYTES` pattern to `TERMINATE_WIRE_BYTES`, `FLUSH_WIRE_BYTES`, default `DESCRIBE_PORTAL`). Tier-1 static dispatch.
- **`ReadBuf` ringbuffer rewrite** (DEF-058): lazy-compaction `advance()` becomes wraparound. 3-10× win on large frames; complicates `&[u8]` → `(&[u8], &[u8])` slice surface. Spike via `[T]::as_chunks` API (stable 1.95+).
- **`#[inline(always)]` audit on hot path** — verify all `feed_bytes` / `push_command` callees inline via `cargo asm`. May expose under-tuned hints.
- **`core::hint::cold_path()` audit** — DEF-185 sprinkled cold hints; sweep for missed call sites in `if let Err(_) = …` patterns on hot path.
- **`feed_bytes_into(&mut self, &[u8], &mut OutActions, &mut WriteBuf)`** — caller provides OutActions buffer, no allocation inside. DEF-190/191 partially via `RowStream`; extend to all hot paths. Saves 800 B stack zero-init per call.
- **Stmt cache (DEF-035 prepared statement LRU)** — Phase 1c. Massive cache-hit win on repeated queries (compile-time stmt name → server-side cache key).
- **Sub-frame prefetch hint** — `core::intrinsics::prefetch_read_data` is unstable + requires unsafe. Closed for stable + forbid(unsafe). Track if stable form lands.
- **`MaybeUninit<T>` for cold fields** — skip `Default::default()` cost. Closed: requires `unsafe { assume_init_ref }`.
- **Fanout-2 amortisation** — per-call OutActions cap is 9 (8 staged + 1 fanout). Reach for `MAX_FANOUT2 = 0` cap if no path needs > 1 action per arm. Audit each arm; potentially tighten cap → smaller OutActions footprint.
- **Vectored write via `IoSlice<'a>`** — Phase 1e wrapper concern. Track for handoff.
- **`PgProtocol<const ENABLE_DIAGNOSTICS: bool>`** — embedded targets compile diagnostic counters off (saturating drops, adversarial-flood guards). Production servers compile diagnostics on. Note: original framing paired this with DEF-199 phase 2 const-generic refactor; that phase was rejected (see DEF-199 closed entry §D Phase 1c). If this idea is revived, it would need to land **without** PgProtocol-wide const-generic propagation — e.g. as a separate `cfg(feature)` gate or `pub type PgProtocolEmbedded = PgProtocolBase<DiagOff>` typestate parameter rather than a `const N`.
- **Pipelining via scope-bracket Drop** — `proto.pipeline(|p| { p.push_parse; p.push_bind; p.push_execute; })` auto-inserts Sync at scope end via Drop. Linear-style discipline through `#[must_use]` + RAII (paired with DEF-198 witness-guard).

---

## §B. Measurement-rejected (DO NOT retry without evidence)

Rules: these items have been **implemented, measured, rejected**.
Commit history carries the failed attempt + revert. Reopening
requires a new DEF entry with measurement evidence refuting the
prior result.

**Meta-observation:** each row below is a confirmed piece of
knowledge about the crate. "Tried X, measured Y, why: Z." The
discipline of bench-first + revert-on-regression turns audit
proposals into factual data instead of speculation.

### Verified load-bearing (architect's concern falsified)

- **B6 `const BOUNDED` specialisation** — VERIFIED load-bearing
  2026-04-24. Architect suspected 2-monomorph compile bloat
  without runtime benefit. `cargo asm` showed LLVM inlines both
  call sites (single shared closure body in emitted asm),
  suggesting no compile-bloat harm. But **empirical removal
  experiment** (replaced `const BOUNDED: bool` gate with runtime
  check against `u16::MAX` sentinel) regressed ALL benches:
  parse_header +18.3%, ping_round_trip +9.2%, iter_rows +7.7%,
  push_command +11.3%, all p<0.05. Reverted clean.
  **Mechanism:** specialisation delivers gate-elimination via
  per-inline-site const-prop — the apparent "single body" in asm
  is post-inlining; at each call site (feed_bytes vs
  feed_bytes_bounded), LLVM emits specialized code with or
  without the gate. KEEP as-is; architect's "no benefit" concern
  falsified empirically.

| DEF / Audit ID | Item | Disposition | Commit |
|----------------|------|-------------|--------|
| **A7** | Tag byte LUT via `InboundTagClass` enum + `classify` fn | **MEASURED REGRESSION** — all 4 bench groups regressed (+2.6% to +8.2%, p<0.05). LLVM's sparse-byte switch beats dense-enum form; classify step adds indirection not foldable. Hypothesis "dense discriminant jump table wins" falsified on modern LLVM. | `1a762ca` (2026-04-24) |
| **A4/B16** | Cache-line layout reorder via `#[repr(C)]` on `PgProtocol` + `ReadBuf` | **MEASURED REGRESSION (partial).** parse_header +6.3% (p<0.05), push_command/ping +3.8% (p<0.05), iter_rows +1.6% marginal (p=0.02), ping_round_trip no change (p=0.86). Net loss. Rust default layout + Rust's choice of register allocation apparently beats explicit `#[repr(C)]` declaration-order pinning. `repr(C)` subtly affects codegen even for standalone functions like `parse_header` via `PgProtocol` size changes propagating through inlining decisions. Same lesson as A7: modern LLVM+Rust default > manual layout hints. | revert 2026-04-24 (uncommitted — not even worth a revert commit) |
| **W3** | `parse_header` range-pattern match (`4..=MAX`/`0..=3`/`_`) instead of sequential `if declared < 4`/`> MAX` guards | **MEASURED MASSIVE REGRESSION.** parse_header **+70%** (!!), iter_rows +19.5%, ping_round_trip +6.7%, push_command +3.9%, all p<0.05. Hypothesis "LLVM emits one ucmp + one conditional jump for range match vs two separate compare-branches" falsified — LLVM's compare-chain lowering with `#[cold_path]` hints on the Err arms is dramatically better than range pattern with fall-through. The range match appears to disable LLVM's value-range-propagation + dead-arm-merge that it does on the sequential-if form. | revert 2026-04-24 (uncommitted) |
| **B11** | `DescribedRowsStaged` unification | Audit-analysis rejected: Copy-cascade break outweighs 60 B × N saving. | DEF-184 audit |
| **C5** | Bitpacked `StateErrorKind` | Factually already done via DEF-142 — StateErrorKind pinned at 1 B exact; further bit-packing has no consumer. | Closed 2026-04-24 |
| **B19** | `ParamOids::EMPTY` all-zeros Eq check | False positive — current doc-safe, fresh-empty matches populated-empty correctly. | Audit #2 |
| **B14** | HList `ParamsWriter` | Stable-Rust form requires `FORMATS`/`OIDS` tier-1 → tier-3 OID regression. Blocked on `generic_const_exprs` stabilisation (see §C). Reopen: measure binary delta via `cargo asm` first. | DEF-185 |

---

## §C. Rust-Unstable Blockers

Features we're working around because they're not yet stable.
Revisit at each MSRV bump. Single grep-point replaces per-site
"when X stabilises…" comments scattered across source.

### RU-01 — `From<u16> for usize` / `TryFrom` in const fn
- **Tracking:** rust-lang/rust#143874 (const-traits)
- **Worked-around:** non-const `pub fn len()` in `decode::RowDesc`;
  `MAX_FRAME_LEN_FIELD` hard-coded literal in `frame.rs`.
- **Action on stabilisation:** keyword flip `fn` → `const fn`.

### RU-02 — `<[T]>::split_once` with predicate
- **Tracking:** rust-lang/rust#112811
- **Worked-around:** `record_param_status` uses manual
  `iter().position` + `split_at`.
- **Action on stabilisation:** 5-line tightening to `.split_once`.

### RU-03 — `generic_const_exprs`
- **Tracking:** rust-lang/rust#76560
- **Status as of 2026-05-04 (Rust 1.95.0):** still nightly-only.
  Re-verified during DEF-212 design phase. Has known soundness bugs
  delaying stabilisation (~3+ years since 1.50 era). Unsuitable for
  v1.0 production crate.
- **Blocks:**
  - DEF-141 infallible `build_*_message` via type-level capacity
    witness.
  - B14 HList `ParamsWriter` with `FORMATS: &'static [_; N]`
    computed from tuple arity.
  - `OutActions::push_infallible<const IDX>` with compile-asserted
    bound check.
  - DEF-212 Alt X infallible per-command tuple variant — would
    eliminate `Result<TypedTuple, PushFailure>` Err arm entirely;
    blocked, falls back to stable `Result` shape (or DEF-212 Alt Y
    bytes-only API which sidesteps the typed-tuple need entirely).
- **Worked-around:** tier-2 classified dead-arm `InternalCrateBug`
  loci (ParamsWriterOverflow / EmptyWriteRange / BuilderCapacityOverflow).
- **Action on stabilisation:** refactor ~50 LoC of dead-Err branches
  into witness-proven infallible paths.

### RU-04 — `std::simd` portable SIMD
- **Tracking:** rust-lang/rust#86656
- **Blocks:** DEF-108 `u8x32` XOR in SCRAM client-proof; A11/C4
  SIMD column batch decode.
- **Worked-around:** zip-iterator form auto-vectorises on
  x86-64-v2+/aarch64 via LLVM.
- **Action on stabilisation:** swap zip-iter → `u8x32::from_slice`
  in scram/crypto.rs; unblock A11/C4 column batch (pair with DEF-143
  bench before ship).

### RU-05 — `const unwrap_unchecked` or safe `never_type`
- **Tracking:** architectural — `unreachable_unchecked` remains
  `unsafe` by design. Watch `!` (never_type) stabilisation
  (rust-lang/rust#35121).
- **Blocks:** none critical. Current tier-3 explicit-error patterns
  are correct under `#[forbid(unsafe_code)]`.
- **Action on stabilisation:** none — current form is optimal under
  the forbid bundle.

### RU-06 — `i32::cast_unsigned` / `integer_sign_cast`
- **Tracking:** rust-lang/rust#125882
- **Blocks:** A5/B10 branchless sign-path (3-branch instead of 5).
- **Worked-around:** `usize::try_from(i32)` — LLVM fuses non-negative
  fast path; 3 branches per column already.
- **Note:** Even on stabilisation, the crate's
  `#![forbid(clippy::as_conversions)]` would block `as u32` literal
  idiom. Keep `try_from` as the permanent form.

### RU-07 — `stdarch_aarch64_prefetch` for simdutf8 NEON prefetch
- **Tracking:** rust-lang/rust (search `stdarch_aarch64_prefetch`
  feature gate). Used by `simdutf8`'s `aarch64_neon_prefetch` feature
  to emit `prfm` prefetch hints inside the validate hot loop.
- **Blocks:** DEF-202 ext A — incremental win on long-input UTF-8
  validation (~few % on 200+ B inputs; the prefetch reduces L1d
  miss latency on the NEON chunk fetcher).
- **Worked-around:** simdutf8 with `aarch64_neon` feature only
  (no prefetch). Already SHIPPED in DEF-202 — wins of 2× on long
  ASCII / 3.9× on multi-byte are intact without prefetch.
- **Action on stabilisation:** add `aarch64_neon_prefetch` to the
  `simdutf8` feature list in workspace `Cargo.toml`. One-line change.
- **MSRV check protocol:** at each MSRV bump, run
  `cargo build --features simdutf8/aarch64_neon_prefetch` on stable
  toolchain; once it compiles without `error[E0554]`, the feature
  is stable — flip the flag.

---

## §D. Closed Index (one-line per major DEF)

Full detail in git log; this is just a navigation aid.

### Phase 1a/1b
- DEF-001..DEF-004: Phase 1b state variants (Startup, SCRAM, PostAuth) — SHIPPED `6382cdc`
- DEF-008: Errored variant — SHIPPED `6382cdc`
- DEF-012..DEF-014: WriteBuf / SendBuf::Owned / MAX_OWNED_SEND_LEN — SHIPPED `690e30e`
- DEF-039, DEF-044, DEF-054, DEF-060..DEF-064: Phase 1b audit round 1
- DEF-094..DEF-102: staged actions, credential typestate, DEF-102 base64ct swap
- DEF-108: SCRAM SIMD (BLOCKED on RU-04)
- DEF-115, DEF-119: schema arena externalisation
- DEF-134: fuzz/stress harness — SHIPPED `1fde5d1` (property tests, stable Rust instead of cargo-fuzz)
- DEF-138..DEF-142: pass-#8 audit cleanups + DEF-142 StateErrorKind newtype
- DEF-143: criterion bench harness (PARTIAL — see §A)

### Phase 1c
- DEF-144..DEF-154: Phase α/β audit batch (parse_header, StatePushClass, FrameCoords narrow, SchemaRef shape, transition_to_errored, InternalCrateBug locus, size pins, SessionParams counter)
- DEF-154 (A-Y): buffer-witness pattern + branded write/read scopes + build-time infallibility + RowStream pull API + Action::StreamRow deletion. Full cascade across multiple sessions.
- DEF-163..DEF-187: Phase α2, γ ship, deferred sub-phases. DEF-163 PARTIAL (see §A).
- **DEF-194**: `RowDesc::format_codes` bit-pack `[FormatCode; 32]` → `FormatCodeSet(u32)`. RowDesc 164→136 B exact; Option<RowDesc> 168→140 B exact; PgProtocol 5108→5080 B exact (−28 B). 330+ tier-1 const-asserts (round-trip 32×5×2 + OOR field preservation 3×4 + raw_bits 7 patterns + boundary + independence + size pins). `Default` derive removed (tier-1 by elimination). 7 redundant runtime tests removed; 2 tier-3 retained with structural reason; 1 wide-row integration test added. Production push (amortised) = 64.5 ns. SHIPPED 2026-04-27.
- **DEF-205**: Broader staleness pattern closure via `SecretBoundedStr<N>` (non-Copy, ZeroizeOnDrop). Tier-1 by Drop chain (compiler-enforced) closes 2 sites: `ErrorArena::clear()` and `SessionParams::clear()` — old payload bytes scrubbed by Drop before discriminant flip / struct overwrite. 4 commits (e8934b0 foundation, 210890c ErrorPayload, 62df843 SessionParams, ebaff0d step-4 audit). Memory-probe tests per site (sister to DEF-185 P3-1 / DEF-204 prior art). Step 4 audit confirmed `mem::replace(state, ...)` SCRAM variant secret-bearing fields all heap-allocated with ZeroizeOnDrop chain (Box<ScramSession>, Sensitive<i32>, SecretDigest); padding bytes in state's storage region after `mem::replace` is compiler-dependent (documented gap, same class as `panic = "abort"` Cargo.toml limitation). Box<PodBytes<N>> heap-scrub gap registered as DEF-206 (LOW severity — username/nonce bytes, public-on-wire). SHIPPED 2026-04-27.
- **DEF-196**: Cold-field externalization. Three independent lazy slots in `PgProtocol`: `session_params: Option<Box<SessionParams>>`, `error_arena: Option<Box<ErrorArena>>`, `malformed_frame_count: u32` (inline). PgProtocol **5080 B → 4352 B exact (−728 B inline)**. Each Box lazy-allocated only on actual write at its specific callsite (ParameterStatus filter, NoticeResponse filter, ErrorResponse arm); malformed paths zero-alloc (counter inline). Per-connection heap economics: Trust auth + no errors = 0 allocations; Startup + no errors = 1 alloc (436 B Box<SessionParams>); rare errors = 2 allocs (~732 B); malformed = 0. For typical Pool-served 10K connections: ~3 MB heap saved vs single-Box bundle approach. Bench `def196-v2`: push_command/ping_amortised **−14% per-query** (production hot path 64.5 → 55.7 ns), push_command/ping **−15%** full-cycle, parse_header **−1.5%**, ping_round_trip ±0% (no regression), iter_rows preserved. Cross-platform CI matrix policy: per-target cfg-gated pins when CI extends, no permissive ranges. Final commit 987cc29. SHIPPED 2026-04-28.
- **DEF-210 (architect-driven tier-1 audit, 2026-04-28)**: deep-audit on user request to verify "стеклянная архитектура" — every claimed tier-1 surface in `bsql-pg-proto` re-verified for "build-fails" rigor (not surface-label or "verified by tests"). **Three architect-agent passes** (initial audit → first re-audit after closure batch v1 → final verification after closure batch v2) found 10 actionable items + 1 cleaner formulation (CF-02) + 2 acknowledged-policy items. **One false-positive caught by spot-check** — agent claimed `IdleStateProof::new()` was `pub(crate)`; verified `guard.rs:205` is `const fn new()` (no visibility keyword = module-private), already at maximum tier-1. **CLOSURES SHIPPED 2026-04-28**: (a) **SR-01 Path C** — `schema_present: bool` flag deleted from `SimpleQueryAwaitingRfq` variant + `StagedQueryCompletePayload` staged struct; `PgProtocol::row_desc_slot.is_some()` is single source of truth (slot equals itself; no second variable to drift). (b) **SR-01 Path D** (re-audit closure) — sibling pattern in Describe path: `state::DescribedRowsStaged` (Rows/NoData discriminator on `Describe*AwaitingRfq` variants) + `action::DescribedRowsStagedSlim` (mirror in StagedDescribe*CompletePayload) + `dispatch::stage_described_rows` bridge + `action::described_rows_slim_into_public` helper (with its `debug_assert!(false)` defensive arm — CREDO §V banned defensive-for-impossible) ALL DELETED. Materialise reads `row_desc_slot.map(...)` directly; ~80 LoC eliminated. (c) **SR-02** — `clear_session_residue_if_idle_or_errored` matches `state.push_class()` exhaustively (5-variant `StatePushClass`); wildcard `_ => {}` removed. NB-04 attempted `residue_policy(StatePushClass) -> ResiduePolicy` extraction for testability — caused +21% regression on `push_command/ping_amortised` (LLVM didn't inline the extracted function despite `#[inline]`). Reverted; per-class arm-body verification documented as tier-3-by-discipline with a future integration-test path. (d) **SR-03** — two parallel identical `allows_unsolicited_param_status` / `..._notice_response` matches collapsed to thin wrappers over `state.unsolicited_admit() -> UnsolicitedAdmit`; one exhaustive match, two bool projections. Cross-classifier drift now structurally impossible. (e) **SR-04 + REC-02** — `ProtoState` size pin tightened from `>= 16 && <= 96` to exact `== 80` (aarch64-apple-darwin reference). Re-audit caught + fixed factual errors in the size-pin docstring (originally claimed SCRAM dominates with `ReplyId<K>` 16 B; reality is `DescribeStatementAwaitingRfq` dominant with `ReplyId<K>` 8 B + `ParamOids` 68 B + 1 B disc + 3 B align-pad = 80). (f) **SR-05 + REC-10** — restored named consts `MAX_FANOUT_PER_STAGED = 2` + `MAX_FANOUT2_ENTRIES_PER_CALL = 1` (previously DEF-184 had collapsed to magic `+1`). Re-audit downgraded their visibility from `pub const` to `pub(crate) const` (implementation-detail topology, not public-API). Formula `MAX_STAGED + FANOUT2 × (FANOUT − 1) = 9` self-documents; 1c-5 pipelining bumps a NAMED magnitude. (g) **SR-06 + REC-09** — `MAX_ERROR_FIELDS = 32` drift-pinned via `KNOWN_TYPED_ERROR_FIELD_TAGS = b"SVCMDH"` + `assert!(MAX >= len * 2)`. Honest tier framing in docstring: tier-3 by-discipline at the broad scope (slice-vs-arms manual lockstep), tier-1 narrow at the cap-relative-to-documented-count scope. (h) **SR-07** — SCRAM `scram`-field docstring drift fixed (claimed "1 alloc per SCRAM connection", reality is 3 Box allocs peak live during ServerFirst await; REC-06 forward-ref tracks consolidation). (i) **ML-01 / REC-03** — dead `_ASSERT_MAX_FITS_NICHE` const removed from `bounded.rs:98+225`; tier-1 enforcement of `MAX <= 254/65_534` lives in inline `const { assert!(...) }` in `ZERO`/`new_const`/`try_new` (every reachable monomorph site verified). (j) **CF-02** (re-audit elevation) — `BoundedLen<MAX>` impl associated const lifted to `const { assert!(MAX <= 254/65_534, ...); MAX }`; closes the last MAX-bound hole on the trait surface (anyone naming `<BoundedU8<300> as BoundedLen<300>>::MAX` const-eval-fails at the impl site, not silently passes). (k) **ML-03** — `frame.rs:81-83` triple drift pin collapsed to value-pair (third assert was tautological); single-equation `READ_BUF_CAP == MAX_FRAME_LEN_FIELD as usize + 1` form blocked by stable-Rust `usize::try_from(u32)` not being const-callable (RU-01) AND `as` cast forbidden by clippy bundle. Value-pair pin preserves drift detection. (l) **BS-11 + REC-08** — `SessionParams::is_pristine()` const predicate added; `static EMPTY: SessionParams = SessionParams::new()` site carries tier-1 `const _: () = assert!(EMPTY.is_pristine())`. Re-audit refined docstring with honest tier framing: **tier-1 narrow** (existing-field default changes caught at build), **tier-3 broad** (new field added without updating predicate stays uncaught — Rust does not expose reflective field iteration in const). `#[derive(Pristine)]` procmacro candidate registered as future-CF-04. **PROPOSED, NOT CLOSED**: REC-06 SCRAM Box consolidation (3 Box → 1 `Box<ScramHandshakeState>`, ~40 LoC, alloc-count win + makes SR-07-fixed docstring literally accurate). **Acknowledged-as-policy** (no closure without separate policy decision): BS-01 `panic = "abort"` makes ZeroizeOnDrop tier-2 on panic flow / tier-4 on downstream-unwound panic (acknowledged in `Cargo.toml:115-156`); BS-02 transitive unsafe via deps (`simdutf8`/`heapless`/`sha2`/`hmac`/`pbkdf2`/`getrandom`/`subtle`) per CREDO §11 audit-trust. **Validation**: 106 lib + 200+ integration + 10 compile_fail doctests (8 pre-CF-02 + 2 new for `<BoundedU{8,16}<MAX> as BoundedLen<MAX>>::MAX` proving CF-02 build-fire) + 5 doc-tests pass; clippy clean; **`push_command/ping_amortised` 54.24 ns vs tip 54.68 ns — zero regression (within noise).**

**Re-audit perf root-cause investigation (audit 2026-04-28, "путь Z"):** initial post-Path-D bench showed `push_command/ping_amortised` regressed +13% vs tip baseline (54.5 → 62 ns). Surgical bisects (inline(always) on into_public, inline(always) on push_class, CF-02 revert, BS-11 hoist, wildcard vs or-pattern in clear_session_residue) accumulated only ~5 ns of recovery — leaving a residual ~+7 ns regression that wasn't isolatable to any single closure. Root cause identified via **assembly-level diff** (`cargo rustc --emit=asm` + ldr-histogram by sp offset): current asm had **+38 single-load `ldr` instructions** vs tip, concentrated at 4-byte-stride sp offsets (`[sp, #240]` … `[sp, #276]`, accessed 6-7 times each — `ParamOids` u32 stack spills). Path D's rewrite of `into_public`'s Describe arms (replacing the prior `DescribedRowsStagedSlim` discriminator + helper with inline `match row_desc_slot`) pushed the LTO-inlined `into_public` body in `materialise` past LLVM's register-allocator quality threshold; LLVM started spilling `ParamOids` fields that previously stayed in registers. **Fix**: extracted `Self::DescribeStatementComplete` and `Self::DescribePortalComplete` arms into `#[inline(never)]` helpers (`describe_statement_complete_into_public` / `describe_portal_complete_into_public`). The Pong / QueryComplete hot-path arms keep their register-friendly inline shape; the Describe paths pay one function call but they are NOT the per-push hot path (describe completion runs once per statement preparation). Result: regression fully eliminated, current bench 54.24 ns vs tip 54.68 ns (−0.8% trend, within noise). **Architectural lesson**: `#[inline(never)]` on RARE-arm bodies preserves register pressure on the COMMON arms in fully-inlined match expressions — keep the hot-arm body under LLVM's register-allocator budget instead of letting cold-arm complexity push the whole function over. CREDO §1 priority pyramid (safety > tier > perf) supports the trade — Path D removed CREDO §V banned `debug_assert!(false)` defensive-for-impossible, eliminated tier-2-by-discipline silent corruption surface. SHIPPED 2026-04-28 (initial batch in commit `d0b794e`).

**REC-06 → PERF-02 (final closure of "one Box per handshake") + NB-04 residue-policy per-class pin tests (follow-up batches, audits 2026-04-28 + 2026-05-04):**

(m) **REC-06** (commit `f69ecd7`) introduced `state::ScramHandshakeState` struct holding the three SCRAM handshake fields previously each heap-boxed in `ConnectingScramAwaitingServerFirst` (`scram` + `client_first_bare` + `client_nonce_b64`). The variant carried a single `Box<ScramHandshakeState>`. Per-handshake allocator ops at the SASL-continue arm: 0 allocs + 3 Box-frees → 0 allocs + 1 Box-free + 1 stack `ScramSession::drop` (cumulatively −2 allocator ops). Half-measure: the StartupScram → ServerFirst transition still incurred 1 alloc (`Box::new(ScramHandshakeState{...})`) + 1 free (`*scram` deref-move).

(n) **PERF-02** (4th-pass architect-agent finding, 2026-05-04, this commit) closes the gap. `client_first_bare` + `client_nonce_b64` moved INSIDE `ScramSession` itself (with `#[zeroize(skip)]` — wire-public bytes per DEF-205 step 4 / DEF-206 audit). Both `ConnectingStartupScram` and `ConnectingScramAwaitingServerFirst` carry the **same** `Box<ScramSession>`; the StartupScram → ServerFirst transition is a state-discriminant flip with the Box pointer copy-moved across variants (zero allocator ops). `build_sasl_initial_response` rewritten to take `&mut ScramSession` and populate the two fields in-place; `dispatch_auth_sasl_continue` reduced from 7 args to 5 (drops the two PodBytes args; reads `scram.client_first_bare` / `scram.client_nonce_b64` through the `&ScramSession` borrow). `ScramHandshakeState` struct deleted entirely (closes BS-01-fresh — pub-API leak gone; struct never existed). **Per-handshake total post-PERF-02: 1 alloc (StartupScram construction) + 1 free (ServerFinal drop), zero transitions in between.** The principal's documented "one heap alloc per SCRAM connection" invariant is now LITERALLY accurate. Drop chain unchanged: `Box::drop` → `ScramSession::drop` → password.zeroize() (PodBytes fields skip-zeroed per wire-public classification). Sizes: ScramSession ~520 B → ~694 B (grew by ~174 B for the inline PodBytes fields + alignment); ProtoState size pin holds at 80 B (DescribeStatementAwaitingRfq still dominant). Bench `push_command/ping_amortised` post-PERF-02: 54.17 ns vs prior commit `f69ecd7` 54.20 ns — within noise (push path is not affected by the SCRAM cold-path refactor; first warmup measurement showed +3% but stabilised at ±0% over 3 runs).

(o) **NB-04** (commit `f69ecd7`) adds the `protocol::residue_policy_per_class_tests` lib unit-test module, pinning the 5-class residue policy of `clear_session_residue_if_idle_or_errored` against arm-body swaps. Five tests cover: `Idle` (clears `row_desc_slot`, preserves `session_params` content), `Errored(_)` (clears all three; verified via `SessionParams::is_pristine`), and `Connecting` / `PingAwaiting` / `BusyQuery` (preserve every observable residue field). The wildcard `_ => {}` form remains in production (chosen for register-pressure preservation per the путь-Z investigation); the test pin lifts the broad-scope tier-2-by-discipline to tier-2-by-test-pin. Total tests: 297 → 302. SHIPPED 2026-04-28.

**Architect-agent passes (4 total)**: initial audit → first re-audit (closure batch v1) → second re-audit (closure batch v2) → fresh-eyes audit (post-REC-06/NB-04). Final agent verdict: **SHIP-as-is**, no P0/P1, all dribble closed except where bench evidence explicitly justifies (e.g., `_ => {}` wildcard chosen over exhaustive or-pattern for register-pressure reasons). PERF-02 is the agent's top actionable finding from pass 4 — closes the half-measure of REC-06 and restores documented invariant to literal accuracy.
- **DEF-199**: `READ_BUF_CAP` const-generic — **PHASE 1 SHIPPED, PHASE 2 ARCHITECTURALLY REJECTED**. Phase 1 introduced `ReadBufN<const N: usize>` const-generic in `crates/bsql-pg-proto/src/buf.rs` with `pub type ReadBuf = ReadBufN<READ_BUF_CAP>` backward-compat alias. Tier-1 const-block `assert!(N <= 65_535, "ReadBufN<N>: N must be ≤ u16::MAX...")` inside `ReadBufN::new()` (initial commit 85f1e45 omitted this block; audit hot-fix 669cdd0 restored — the omission would have been a **silent tier regression**, type would compile with N > u16::MAX and silently overflow `cursor: u16` at runtime; the const-block makes such N a build error per CREDO §0 Tier-1). `ReadBufFull` gained `pub cap: usize` field (so callers see the actual cap that overflowed, not a hardcoded constant). The pair `READ_BUF_CAP` / `MAX_FRAME_LEN_FIELD` remain global consts — drift pin in `frame.rs:81-83` (`const _: () = assert!(MAX_FRAME_LEN_FIELD.saturating_add(1) == 4096)`) catches lockstep violation at build. **Phase 2 (PgProtocol-wide `const N: usize` propagation) was REJECTED on architectural audit** — reasoning preserved here for future archeology: (1) **Tier delta = 0** — `READ_BUF_CAP`/`MAX_FRAME_LEN_FIELD` are already compile-time const, both pinned via `const _: () = assert!(...)`. Const-generic on `PgProtocol` would relabel the source of those constants but not create a NEW tier-1 invariant. The proposed "tier-1 (caller chooses)" is misleading — caller-flexibility is API-surface, not safety-tier. (2) **Blast radius 188 references / 32 files** for zero current consumer demand — every `impl PgProtocol`, every `&mut PgProtocol` in test/bench/internal helpers would need `<const N: usize>`. CLAUDE.md prohibits *"designing for hypothetical future requirements"* and *"premature abstraction over three similar lines"*. (3) **`parse_header` would need awkward `usize → u32` const-conversion** (`MAX_FRAME_LEN_FIELD = N - 1` projected to u32) — `u32::try_from` not yet const-callable on stable (RU per `#143874`); the current global `const MAX_FRAME_LEN_FIELD: u32 = 4095` is clean and tier-1 by drift-pin. (4) **Multi-N benchmark sweep (the original goal of phase 2)** is achievable simpler via temporary `READ_BUF_CAP` const-swap → cargo bench → revert, no permanent API churn. Future revival requires a concrete consumer (embedded target, real driver-side knob) — not just bench flexibility. Cross-reference: DEF-198 ext IdleStateProof ZST witness folded into DEF-198 (commit 60f2b49) — separate concern. Phase 1 commits `85f1e45` (initial), `669cdd0` (audit hot-fix). SHIPPED+REJECTED 2026-04-28.
- **DEF-203 ext (FixedStr migration)**: Applied the unified `BoundedLen<N>` pattern to `FixedStr<const N: usize, Tag>` by adding a third generic `LenT: BoundedLen<N>` parameter (default `BoundedU16<N>` for backward compat across the variable-N type aliases). Type aliases for the 4 small-N validated types now pick `BoundedU8<N>`: `Ident` / `DatabaseName` / `StmtName` / `PortalName` (all `N=63`); `ApplicationName` (`N=128`). `Sql` (`N=2048`) and `BoundedStr<N>` (variable `N`) keep the `BoundedU16<N>` default. Concrete `const fn new()` impls for both `BoundedU8<N>`-LenT and `BoundedU16<N>`-LenT preserve the previous const-fn API surface; generic constructors switched to `Self::default()` (non-const, but `static EMPTY` consumers (SessionParams, ErrorArena) don't transitively call FixedStr::new). All `len: u16` reads/writes migrated to `BoundedLen::try_new_usize` / `LenT::default` / `LenT::get_usize`. Removed dead `narrow_len_u16` helper. Tier impact: `len` field now **tier-2 by-construct** (BoundedLen rejects out-of-range at construct, type carries the bound at every use site). `Option<Ident>`/`Option<DatabaseName>`/`Option<StmtName>`/`Option<PortalName>`: 70 → **65 B** (5 B saved each via `NonZeroU8` niche on first field with `BoundedU8<63>` + smaller struct). `Option<ApplicationName>`: 132 → **130 B** (2 B saved). 8 new exact size const-asserts pin the layout. All 19 test crates pass; clippy clean; no bench regression vs `def202-simdutf8` baseline. SHIPPED 2026-04-28.
- **DEF-203 (API + 2 sites)**: Unified niche-audit module `bounded.rs` plus first migrations. Refactored `BoundedU8<const MAX: u8>` → `BoundedU8<const MAX: usize>` (cleaner integration with usize-based const-generic structs); added `BoundedU16<const MAX: usize>` (NonZeroU16-backed, parallel design); added sealed `BoundedLen<N>` trait providing uniform `try_new_usize`/`get_usize`/`MAX` interface for generic length-storage parameters in container types. Tier-1 const-asserts pin size + niche for both (`BoundedU8<32>` 1 B, `Option<BoundedU8<32>>` 1 B; `BoundedU16<2048>` 2 B, `Option<BoundedU16<2048>>` 2 B). Compile-time construction macros `bounded_u8!(MAX, VAL)` and `bounded_u16!(MAX, VAL)` plus 4 compile_fail doctests (2 per type) prove out-of-range build error. Sites migrated: `RowDesc::n_columns` (drop helper `MAX_ROW_COLUMNS_U8` const, use `BoundedU8<MAX_ROW_COLUMNS>` directly); `OtherEncoding::len` (same). Net layout: `Option<RowDesc>` 140 → 136 B (4 B saved); `OtherEncoding` 34 → 33 B + `Option<OtherEncoding>` 36 → 33 B (3 B saved per Option). Construction calls migrated to `BoundedLen::try_new_usize` for uniform usize-input handling. Lib tests: 106 pass (was 100 — gained 6 from BoundedU16/BoundedLen unit tests). All 19 test crates pass; clippy clean. Pending sites (`FixedStr::len`, `PodBytes::len`) tracked under DEF-203 entry; these need `LenT: BoundedLen<N>` generic on the host structs with const-fn cascade audit (Self::new currently `pub const fn`). SHIPPED 2026-04-28.
- **DEF-195**: `BoundedU8<MAX>` newtype + applied to `RowDesc::n_columns`. New module `bounded.rs` (~280 LoC + tests) introduces a generic 0..=MAX-bounded u8 with NonZeroU8 offset-by-one encoding for niche optimisation on stable Rust (no unsafe, no nightly). **Two-tier construction surface (DEF-195 ext, "tier-1 возможно?" probe)**: (a) `pub const fn new_const<const VAL: u8>() -> Self` — **tier-1 compile-time construction**, `VAL > MAX` is a build failure via `const { assert!(VAL <= MAX) }`; (b) `pub const fn try_new(value: u8) -> Option<Self>` — tier-3 runtime construction, returns Option for wire-derived inputs. Plus `bounded_u8!(MAX, VAL)` macro for ergonomic tier-1 construction. Runtime tier-3 path is irreducible without `unsafe` (forbidden) — wire bytes from server cannot be compile-time validated. `BoundedU8<MAX>` and `Option<BoundedU8<MAX>>` are both 1 B (niche absorbs the discriminant via NonZeroU8::MIN). Applied to `RowDesc::n_columns: BoundedU8<32>` — niche absorbs `Option<RowDesc>` discriminant. **Option<RowDesc> 140 → 136 B (4 B saved per Option).** `PgProtocol` size 4352 B unchanged (alignment of cold-slot fields absorbs the field-level saving — but the Option<RowDesc> saving is real where Option<RowDesc> is used in other contexts: tests, parse paths, future row caches). Public API preserves backward-compat: `RowDesc::n_columns()` still returns `u16`, `len()` returns `usize` (now non-const due to `From<u8>` non-const-trait — tracked under RU-01). Tier-1 closure: 11 unit tests on BoundedU8 (range, niche, default, ordering, debug, boundary, const construction, macro expansion), const-asserts on size + alignment for `BoundedU8<32>` and `Option<BoundedU8<32>>`, **2 compile_fail doctests proving out-of-range `new_const::<33>()` and `bounded_u8!(32, 33)` are build errors**. Bench `def195-bounded` vs `def202-simdutf8` baseline: all benches within noise (no regression). SHIPPED 2026-04-28.
- **DEF-202**: SIMD-accelerated UTF-8 validation via `simdutf8` for `<&str as FromPgText>::from_pg_text`. Workspace dep added (`simdutf8 = "0.1"`, `default-features = false`, `features = ["aarch64_neon"]`). The single-line impl swap routes through `simdutf8::basic::from_utf8` instead of `core::str::from_utf8`. Bench fixtures expanded: previous single `iter_5cols_decode_text` (17 B short ASCII) replaced with three shape-distinguishing benches — `iter_5cols_decode_text_short_ascii` (17 B), `iter_5cols_decode_text_long_ascii` (~200 B), `iter_5cols_decode_text_cyrillic` (~78 B multi-byte). Bench evidence (aarch64-apple-darwin): short ASCII +9.9% (43.6 vs 40.3 ns — small cost on already-cheapest path), **long ASCII −49.9%** (~2× faster, 26.6 vs 53.0 ns), **multi-byte UTF-8 −74.0%** (~3.9× faster, 78.5 vs 309.4 ns). Hybrid length-threshold dispatch was tested and rejected: dispatch branch ~1.5 ns/col exceeded the short-ASCII savings, so pure simdutf8 is strictly better. Behaviour byte-identical to `core::str::from_utf8` (parity documented + property-tested upstream by simdutf8 maintainers). Tier preserved (tier-3 runtime UTF-8 classification → `DecodeError::NonUtf8`). Binary-format codec sub-item deferred — requires server-side opt-in via `format_codes`. Commit follows. SHIPPED 2026-04-28.
- **DEF-208**: Idle-only `compute_push` refactor — closes DEF-198 surface 6 (internal tier-3 defensive arms). Extracted 7 `compute_push_<cmd>_idle_only` siblings (single Idle-arm bodies + `debug_assert!`); added top-level `compute_push_idle_only` dispatcher; routed `push_command_internal` and `push_bind_execute_internal` through Idle-only path. Original dispatching `compute_push_<cmd>` and `compute_push` retained but gated behind `#[cfg(test)]` (only the in-file `compute_push_tests` module uses them — production binary does not contain those bytes). The standalone `compute_push_bind_execute<P>` was DELETED entirely (zero callers post-refactor). Tier impact: surface 6 (internal defensive 5-arm dispatch) elevated to tier-1 by-construction (`#[cfg(test)]` makes the dispatch test-target-only; production code has only Idle-only single-arm bodies). Surface 5 (`Option<ReadyGuard>` runtime "is state Idle?") remains tier-3 — irreducible per Postgres server-driven state. Bench `def208-idle-only` vs `def207-letfix` (pre-DEF-198): push_command/ping -0.4% (within noise — fully recovered the +3.2% DEF-198 cost), ping_round_trip +0.4%, push_command/ping_amortised +1% (small residual), column_decode/iter_columns_raw -2.2% (improved). All tests pass, clippy clean. SHIPPED 2026-04-28.
- **DEF-198**: Witness-guard typestate. New module `crates/bsql-pg-proto/src/guard.rs` introduces `ReadyGuard<'a>` (a `&'a mut PgProtocol` zero-sized newtype) plus `ConnectionStatus { Ready, Busy, Handshaking, Errored(StateErrorKind) }` enum. `PgProtocol::push_command` and `push_bind_execute` moved from public to `pub(crate)` (now `push_command_internal` / `push_bind_execute_internal`). Public surface is `proto.as_ready() -> Option<ReadyGuard<'_>>` (returns `Some` only for `state == Idle`) and `proto.connection_status() -> ConnectionStatus`. Tier-1 closure via two-step transitive exhaustive match: `ProtoState → push_class() (5 variants) → as_ready/connection_status` (exhaustive over `StatePushClass`). Closure pinned by 4 `compile_fail` doctests in `guard.rs` (proves `proto.push_command(...)` from outside crate is compile-rejected, two simultaneous guards are borrow-checker-rejected, consumed-guard reuse is move-checker-rejected) and 9 behavioural tests in `tests/def198_guard_closure_spec.rs` (one per `StatePushClass`). 60 test/bench callsites migrated via `tests/common/mod.rs` `PushOrPanic` extension trait + bench-internal `BenchPushOrPanic` trait. 12 prior FailReply-on-non-Idle integration tests rewritten to test public-API `as_ready().is_none()` + `connection_status()` classification (the underlying `compute_push_*` defensive arms remain, tested by `compute_push_tests` private module). Bench `def198-final`: push_command/ping +3.2% (~3 ns added — irreducible tier-1 closure cost, the as_ready dispatch on `state.push_class()`); push_command/ping_amortised +2.5% (~1.5 ns); ping_round_trip −3.2% (within noise but trends improved); column_decode/iter_rows benches noise-level (±2%). Send asserted for `ReadyGuard<'static>` and `ConnectionStatus`. SHIPPED 2026-04-28.
- **DEF-197**: Column-decode bench infrastructure. Added 5 `column_decode/*` benches measuring `DataRowRef::parse`, `ColumnsIter` per-column walk, `FromPgText` typed decode (i32, &str), and NULL fast-path. Baseline `def197-decoder`: header parse 1.10 ns, raw iter 1.23 ns/col, i32 decode 8.84 ns/col (~7.6 ns is `str::parse`), text decode 8.42 ns/col (~7.2 ns is UTF-8 validate), NULL fast-path 0.6 ns/null. **Closes the largest measurement blind spot** — future decoder optimisations (DEF-200/202/203) now ship with evidence per CREDO §4.12. Per-row decode 44 ns × 1M rows = 44 ms on large SELECTs — comparable to frame dispatch cost. No production code change. Commit 0a14efa. SHIPPED 2026-04-28.
- **DEF-204**: `ReadBuf::compact()` staleness leak closure. Pre-fix `copy_within + truncate` left bytes physically at `[unread_len..pre_compact_len)` retaining pre-compact content (consumed prefix's content + source side of copy_within); secret-correlated bytes from prior frames persisted in the array. Post-fix: in-place zeroize of abandoned tail BEFORE truncate. Tier-3 by-audit → tier-2 structural. ~5 LoC change in `buf.rs::compact`. 2 memory-probe tests added (`tests/buf_compact_staleness_spec.rs`, `#[ignore]`-gated, Miri-validated): `def204_compact_zeroizes_abandoned_tail` + `def204_compact_no_op_when_cursor_zero_does_not_zero`. SHIPPED 2026-04-27.

### DEF-184 (post-Y comprehensive audit)
Shipped batches (commit-anchored):
- Batch 1-6: B2-B28 cleanup + perf — `19d4426`, `579dddd`, `cee0591`, `ace874d`, `e3581a7`, `ac3c3d9`, `fefce6e`, `68a8d09`, `dfc3ee7`
- **A1+A13 crown — ErrorArena cascade** — `51ed3d8`
- **Audit-cascade on A1+A13 (13 fixes across 2 rounds)** — `7a0e54d`
- **B21/C6 dispatch by-ref** (DispatchOutcome 800 → 88 B) — `7a0e54d`
- **A10/B22 SCRAM hot/cold split** (ProtoState 712 → 80 B) — `7a0e54d`
- **Audit-2 feedback response** (ident.as_str, row_stream flush, arena tier-claim, assert_copy rename) — `da32203`
- **C5 + DEF-163 partial (G002/G004/G011/G012/A006/A012/B011)** — `252ed6b`
- **DEF-143 bench harness** — `04df157`, `8df975f` (per-row throughput), `1ff4076` (feature-gate hook)
- **A7 — MEASURED REJECTED** — `1a762ca`

### DEF-184 A10/B22 REVERTED (2026-04-24)
- **Reverted:** SCRAM hot/cold split — restored tier-1 variant-carries-field
  per CREDO §1 (safety > tier-1 > perf). Pre-revert split demoted the
  correlation invariant to tier-2 classified `ScramStateDrift` +
  introduced zeroize-hygiene gap on non-dispatch fail paths. Post-revert
  `ProtoState` grew 80 → ~712 B but variant-carries-field is compile-
  enforced + `ZeroizeOnDrop` fires automatically on every state
  transition via variant drop glue. `src/scram_state.rs` deleted (577
  lines), `CrateBugLocus::ScramStateDrift` removed, `PgProtocol::scram_state`
  field removed. Architect audit confirmed tier-1 genuine + hygiene
  correct on every exit path.
- **Cost:** ~632 B × N dispatches memcpy (2-3 frames/SCRAM handshake);
  below audit-sensitivity threshold.
- **Architect findings:** 2 cosmetic (tight size pins + docstring
  clarification), both implemented.

### DEF-189 v1 — strip RowDesc from state, single slot (2026-04-26)

Architect-driven breakthrough refactor (autonomous worktree agent).
6 state variants stripped of `row_desc: RowDesc` field; single
`row_desc_slot: Option<RowDesc>` on PgProtocol with single-inflight
invariant. New tier-1 invariant via `RowDescBorrow<'r>` borrow type.

**Wins vs def184-complete baseline:**
- parse_header: -1.7%
- ping_round_trip: **-5.7%** (172 ns vs 182.5 baseline)
- push_command: **-7%** (98.6 ns vs 107.6 baseline)
- iter_rows_per_row: +100% residual (structural)

**Tier uplift:**
- ProtoState 336 → ~64 B (RowDesc 264 B stripped)
- IterRowsClass classifier — single state-match per next_event
- Reply::QueryComplete::row_desc typed as Option<RowDescBorrow<'r>>
- session_params auto-clear on Errored

**iter_rows_per_row residual** structurally accepted (not from
ProtoState size — proven by ping/push improvements with same
architecture). Future paths: hot/cold state split, peek/consume API
redesign, or accept.

### DEF-186 perf-recovery + 6 P1 closures (2026-04-24)

Pre-DEF-186 architect re-audit identified 6 P1 + 3 P2 findings on the
post-A10/B22-revert + DEF-185 codebase. All P1 closed:

- **P1-1**: `install_errored_malformed_data_row` now takes `total_len: usize`
  param (was hardcoded 0); aligns FailReply payload with state-kind input.
- **P1-2**: 5 pin tests added (`simple_query/parse/describe_statement/
  describe_portal/preserve_arms_simple_query`) covering all `compute_push_*`
  Idle-arm transitions + non-Idle preserve invariant. Closes the
  "&mut state refactor lost build-time guarantee that Idle arm writes
  *state" tier-3 seam (now tier-3 covered by tests vs tier-1 of pre-refactor).
- **P1-3**: `try_builder!` macro `debug_assert!(matches!(*state, Idle))`
  pin — catches future misplacement that would silently leak embedded
  inflight ReplyId.
- **P1-4**: `read_buf.clear()` → `fail_inflight_no_readbuf` ordering
  invariant documented inline in `IngressClassification::AppendFailed`
  arm (zero-on-clear scrubs SCRAM bytes BEFORE state transition consumes
  variant).
- **P1-5**: `malformed_frame_count` + `n_malformed_param_status_dropped` +
  `n_notice_response_dropped` widened from `u16` → `u32`. Pre-fix
  saturation at 65535 collapsed adversarial-flood diagnostics on long-
  lived connections; u32 saturation at 4B is architecturally distant.
  Cost: +6 B aggregate.
- **P1-6**: `take_inflight_reply_raw_id` `Errored(_) => None` arm
  documented as 1c-5 pipelining trigger (today single-inflight makes
  None correct, but pipelining will widen the return type).

P2 findings:
- **P2-1**: Bind/Execute partial-frame scrub on Execute build failure —
  deferred (window is short; WriteBuf::clear at next entry-point scrubs).
- **P2-2/P2-3**: cosmetic, no action needed.

Architect re-audit conclusion: **no P0 regressions**, safe 1-lookup
alternative for fast_path_data_row is **structurally impossible** under
forbid(unsafe_code) + tier-2 arena gen-ref + tier-1 cursor borrow —
the 2× arena lookup is the minimum-cost safe shape. Path to recover
iter_rows_per_row beyond accepting +110% is API restructure
(peek_row/consume_row split — public-API churn deferred to 1c-5).

### DEF-186 (perf-recovery partial — 2026-04-24)

Bench-replay против `def184-complete` baseline после A10/B22 revert
выявил регрессии:
- ping_round_trip +30.7%, push_command +28.5%, iter_rows_per_row +108%
- (parse_header no change — pure function, не aфектится)

Корень — ProtoState 80→712 B + новые zero-on-clear/Drop impls + двойной
arena lookup в P0-E (zombie-prevention safety fix).

**Применённые исправления:**
- **compute_push_* → `&mut ProtoState`** (signature refactor 7 функций):
  push_command +28.5% → **-5.1% улучшение** (экономит 1424 B memcpy на
  каждом push: `mem::take` убран + write-back через прямой
  `*state = ...` только при реальном переходе).
- ping_round_trip частично восстановлен: +30.7% → +4.1%

**Структурно недостижимо без архитектурных изменений:**
- iter_rows_per_row остаётся +108% (8.48 → 17.55 ns/row): fast_path_data_row
  делает 2 arena lookups (P0-E zombie-prevention) + cache-locality эффекты
  от увеличенного PgProtocol. Альтернатива через unsafe pointer или
  alloc/Box — оба нарушают `forbid(unsafe_code)` и `no_std + no alloc`.
  CREDO §1: safety > perf принят.
- ping_round_trip residual +4.1% — dispatch.rs `mem::replace(state, Idle)`
  per dispatch (712 B). Refactor требует pattern-match alternative для
  SCRAM variants's field moves; multi-session работа.

Записать как principled accepted regression: revert восстановил tier-1
SCRAM safety (CREDO §1), приняли cost. Recovery `compute_push_*` refactor
unlocked 5% improvement vs pre-revert push_command baseline.

### DEF-185 (security hardening — tripled architect audit, 2026-04-24)
Post-A10/B22-revert comprehensive audit via 3 parallel architect agents
(runtime safety, crypto/secrets, protocol/DoS). **All 33 actionable
findings closed** — 6 P0 + 14 P1 + 13 P2/P3.

**P0 (safety-critical, ship-blockers):**
- **P0-A** `panic = "abort"` vs `ZeroizeOnDrop` trade-off documented in
  `Cargo.toml` with 3-option design space (unwind / mlock / pre-abort
  hook). Current stance: keep abort + honest docs + Zeroizing scope
  guards for defense-in-depth.
- **P0-B** `WriteBuf::clear()` zeroizes backing bytes + `Drop` impl;
  SASL ClientProof / SQL history no longer lingers.
- **P0-C** Same for `ReadBuf::clear()` + `Drop`; server signatures /
  query history scrubbed.
- **P0-D** `compute_client_proof` — `stored_key` + `client_signature`
  wrapped in `Zeroizing<[u8; 32]>`; `hmac_sha256` / `hmac_auth_message`
  return `Zeroizing` typed.
- **P0-E** `install_errored_stale_schema_ref` helper — StaleSchemaRef
  now transitions state to Errored + emits CloseSocket (pre-fix: zombie
  connection with no teardown signal).
- **P0-F** 5 zero-body frames (`EmptyQueryResponse` / `ParseComplete` /
  `BindComplete` / `NoData` / `CloseComplete`) strict slice-pattern `[]`
  validation; new `ProtocolError::UnexpectedFrameBody` variant closes
  tier-4 spec drift.

**P1 (tier uplifts + docs):**
- **P1-A** `proof_b64_buf` / `client_final_msg` — Zeroizing + explicit
  post-use `zeroize()` call (heapless::Vec doesn't impl Zeroize).
- **P1-B** `parse_server_final` — `<[u8; 32]>::try_from` replaces
  dead-arm silent `SecretDigest::new([0; 32])` fallback.
- **P1-C** `StartupCompletePayload::Debug` manual redact for
  `secret_key` (CancelRequest capability token leak class).
- **P1-D** SCRAM `parse_server_first` / `parse_server_final` accept
  RFC 5802 extensions (PgBouncer/proxy interop).
- **P1-E** `allows_unsolicited_notice_response` exhaustive classifier —
  pre-auth states reject notices (avoids attacker-controlled text in
  operator logs).
- **P1-F** `TAG_CLOSE_COMPLETE` narrowed to `pub(crate)` until
  1c-6 Close-frame support.
- **P1-G** `DrainRfqAfterError` uses `parse_rfq_payload` for uniform
  tx_status validation.
- **P1-H** `const_assert!(READ_BUF_CAP <= u16::MAX)` drift pin in
  `protocol.rs` couples to `frames_consumed: u16`.
- **P1-I** `ScopedTestNonce` RAII guard — panic-safe cleanup.
- **P1-3** Fast-path DataRow rejects `body_len < 2` (can't carry
  column-count header) via MalformedDataRow.
- **P1-6** `IngressClassification` enum consolidates scattered
  control flow in `feed_bytes_impl` into exhaustive match.

**P2 (hygiene + diagnostics):**
- **P2-A** `MAX_SCRAM_ITERATIONS: 10M → 100K` — DoS surface closed
  (pre-fix allowed ~2s PBKDF2/connection attempt).
- **P2-B** `n_malformed_param_status_dropped` counter in SessionParams.
- **P2-C** `BackendKeyData.secret_key` trade-off documented (inline i32
  vs `Sensitive<i32>` — ergonomic cost vs zeroize discipline).
- **P2-D** `FixedStr::was_lossy()` accessor + `was_lossy_flag` bit
  (distinguish legitimate `?` chars from lossy UTF-8 coercion).
- **P2-E** `MAX_PASSWORD_LEN` docs synced (1024 → 512) + symbolic
  boundary test (`MAX_PASSWORD_LEN + 1`).
- **P2-F** SCRAM `n=""` PG-convention documented at call site.
- **P2-G** `ErrorArena::overwrite_count()` canary + public
  `PgProtocol::error_arena_overwrite_count()` accessor.
- **P2-H** base64 strict RFC 4648 stance documented (no whitespace
  relax per CREDO §1 safety > interop).
- **P2-3** `n_notice_response_dropped` counter in SessionParams.
- **P2-9** `PgProtocol::malformed_frame_count()` accessor — operator
  canary for repeated adversarial framing.
- **P2-7** `parse_command_tag` rejects embedded NUL as malformed.
- **P2-4/6/8/10** explicit boundary coverage tests for ErrorResponse
  max-fields / RowDescription max-cap / ParameterDescription n=0 /
  DataRow short-body.

**P3 (coverage gaps):**
- **P3-1** Memory-probe tests (pointer read post-drop) verify
  `ZeroizeOnDrop` actually scrubs backing buffer; `#[ignore]` by
  default, Miri-compatible.
- **P3-4** `ScopedForceRngFailure` test-only RAII guard exercises
  `ScramError::RandomnessUnavailable` path.
- **P3-5** 3 structured fuzz tests × 5K iterations each (deterministic
  xorshift) on SCRAM parsers — no panic / silent desync in 15K random
  inputs.
- **P3-B/C/F/G/H** + misc via 12 tests in new `audit_coverage_spec.rs`.

**Test growth:** 235 → 254 passing + 2 memory-probe `#[ignore]` that
pass via `--ignored`. Total 256 test outcomes green.

### DEF-186 (DEF-184 session bonus findings — folded into main commits)
### DEF-187 (DEF-184 batch 7-9 stragglers — individually dispositioned)

---

## §E. Session log — last 3 sessions (compact)

### 2026-04-27 — Layout/perf wave registered + DEF-194 shipped + DEF-204 found

User-driven audit confronted the DEF-190/191 wins as pre-decoder gains
that don't address the real measurable hot-path. Outcome: **11 new DEF
items registered** (DEF-194..DEF-204 — see §A "Layout / perf wave"
subsection) + crazy-ideas pool kept inline so future sessions don't
lose strategic options.

**DEF-194 shipped end-to-end (multi-pass tier-1 audit + glass-arch closure):**

Pass 1 — bit-pack: `[FormatCode; 32]` (32 B) → `FormatCodeSet(u32)`
(4 B). RowDesc 164 → 136 B exact pin. Initial range pin
`PgProtocol [4960, 5400]` shipped on first attempt.

Pass 2 — tier elevation challenge from user ("посмотри может всё же
можно что-то гарантировать"): `set` / `get` / `mask_for_const`
promoted to `const fn` (Rust 1.83+ const_mut_refs; RU-01 worked
around via repeated `wrapping_mul(2)` instead of non-const
`u32::try_from(usize)` shift count). 320 round-trip + boundary +
independence const-asserts pin every (idx ∈ 0..32, code ∈
{Text, Binary}) combination at compile time.

Pass 3 — glass-arch challenge from user ("стеклянная архитектура,
теория работает, на деле паника"): exact size pins added for
`Option<RowDesc> == 140` (was unpinned), tightened `PgProtocol`
range → exact. Wide-row 32-column alternating-format integration
test added (closes max-input edge: bit ordering, all 32 bits
independent, bit-31 boundary).

Pass 4 — second tier-1 challenge ("есть четкое ощущение что всё же
возможно"): `Default` derive **removed** entirely (zero production
consumers — tier-1 by elimination of `default()` surface). 2 more
tier-3 tests removed by promotion: `OutOfRange.idx/.max` field
preservation pin (3 cases × 4 properties) + `raw_bits` round-trip
pin (7 patterns). Total 330+ const-asserted properties.

Pass 5 — cross-platform challenge from user ("безусловно
кроссплатформенным, что risc-v, что apple m, что x86_64, что
windows"): `PgProtocol` range pin replaced with exact `== 5080`
consistent with crate prior art (`ProtocolError == 72`, `Action ==
88`, etc. — all exact). Cross-platform CI matrix policy
documented as project-wide concern: per-target cfg-gated pins
when CI extends, **not** permissive ranges.

Net DEF-194: **−28 B per PgProtocol exact** (5108 → 5080); **330+
tier-1 const-asserts** validated at compile; **7 redundant runtime
tests removed** + **2 tier-3 retained with structural reason**
(`fmt::Write` not const; parser uses heapless::Vec) + **1 wide-row
integration test added**. 92 lib tests + 14 integration suites
green; clippy clean.

**DEF-204 registered → re-framed (initial misdiagnosis corrected):**

Initial framing (WRONG): "DEF-185 P0-B/P0-C always-on full-capacity
8 KB zeroize is overengineering / CREDO §4.1 violation". Reframing
came from re-reading `clear()` and `Drop` impls in `write_buf.rs`
and `buf.rs` — `as_mut_slice()` returns `&mut [u8]` of length
`self.inner.len()` (populated bytes only), not full capacity.
Actual current cost: O(populated_len) — ~1 ns per clear for Ping
push (5 B Sync), ~50 B per SCRAM message. **Already zero-cost-ish.**

The "+13% bench drift" attributed to DEF-185 was mis-attribution
on my part: cumulative diffuse drift across DEF-186/187/188/189/
190/191/194 (compute_push refactor, Box<ScramSession>, RowDesc
strip, RowStream additions, bit-pack), not DEF-185 alone. Each
small individually but accumulating. Production amortised path =
64.5 ns is the production-relevant cost.

**Real issue under DEF-204** (re-framed): `ReadBuf::compact()`
(`buf.rs:278`) leaves stale tail bytes physically present after
`copy_within` + `truncate`. Possible leak vector: a 2 KB SCRAM
frame compacted to 100 B leaves ~1.9 KB of secret-correlated
bytes physically in the array. Future `clear()` zeroizes only
`[0..current_len)`; stale tail at `[current_len..pre_compact_len)`
persists until future pushes overwrite. Tier-3 by-audit → tier-2
structural via in-place zeroize of abandoned tail before truncate
(~5 LoC fix in `compact()`).

**Lesson:** future framing of work items requires reading the
target code first, not relying on commit-message summaries. The
initial DEF-204 framing in this session ascribed perf cost to the
wrong site. Skepticism on one's own framing must precede skepticism
on others'. CREDO §3 + §6 reaffirmed.

**Bench methodology lessons:**

`b.iter(|| { let mut proto = PgProtocol::new(); ... })` measures
**connection-lifecycle cost per iter**, not **per-query cost**.
Production reuses one `PgProtocol` per connection across thousands
of queries; Drop fires once at connection close. Added
`push_command/ping_amortised` bench with `reset_for_bench()` hook
(under `#[cfg(feature = "bench-hooks")]`) reusing `PgProtocol`
across iters — gives accurate per-query number. Pattern: every
hot-path bench should land in BOTH forms (full-cycle for
connection-lifecycle cost, amortised for steady-state hot-path).

**Cross-platform CI matrix:**

Documented as project-wide concern. All current size pins are
exact `==` referencing aarch64-apple-darwin. When CI extends to
x86_64-linux / riscv64 / wasm32 / windows / freebsd, either single
`==` works everywhere (most likely for POD-only structs) or
per-target `cfg`-gated pins land in the same commit. Permissive
ranges forbidden (drift surface > variance cushion).

### 2026-04-24 — DEF-186 perf-recovery
- Bench-replay против `def184-complete` baseline выявил регрессии
  (push_command +28.5%, ping_round_trip +30.7%, iter_rows +108%) после
  ProtoState 80→712 B revert.
- compute_push_* → `&mut ProtoState` refactor (7 функций) — экономит
  1424 B memcpy per push. push_command +28.5% → **-5.1% улучшение**
  относительно pre-revert baseline (102.3 ns vs 107.6 ns).
- ping_round_trip частично восстановлен +30.7% → +4.1%.
- iter_rows +108% structurally accepted (P0-E zombie-prevention требует
  2× arena lookup; refactor через unsafe / alloc нарушает forbid /
  no_std). CREDO §1: safety > perf.

### 2026-04-24 — DEF-184 A10/B22 revert + DEF-185 security audit
- A10/B22 SCRAM externalisation REVERTED — tier-1 variant-carries-field
  restored per CREDO §1 (safety > tier-1 > perf). `src/scram_state.rs`
  deleted (577 lines); `CrateBugLocus::ScramStateDrift` + field removed.
  Architect audit confirmed tier-1 genuine + zeroize hygiene correct.
- DEF-185 three-angle architect audit: 33 actionable findings closed
  (6 P0 + 14 P1 + 13 P2/P3). Key items: ReadBuf/WriteBuf zero-on-clear,
  Zeroizing<[u8;32]> cascade in crypto.rs, UnexpectedFrameBody variant
  for 5 zero-body frames, StaleSchemaRef teardown fix, RFC 5802
  extensions, SCRAM iteration cap 10M→100K.
- Coverage expansion: `audit_coverage_spec.rs` (12 tests),
  `scram_fuzz_spec.rs` (3 tests × 5K iters = 15K fuzz inputs),
  `scram_zeroize_miri_spec.rs` (memory-probe tests via unsafe
  pointer read under `tests/` lint-allowance).
- Test count 235 → 254 passing + 2 `#[ignore]` memory probes.
- `cargo clippy --workspace --all-targets -- -D warnings` clean.

### 2026-04-24 — DEF-143 bench harness + A7 measurement
- DEF-163 partial (`252ed6b`): 7 sub-items — size baselines split,
  SchemaSlab→SchemaArena rename, DescribedRowsRef→DescribedRowsStaged
  rename, PgProtocol size budget doc, ReplyId not-secret note,
  dispatch Errored dead-code note, Action 'w/'r lifetime rationale.
- C5 closed as factually-done (`252ed6b`).
- DEF-143 harness (`04df157`): criterion bench, 4 groups
  (parse_header, ping_round_trip, datarow_stream, push_command).
- Per-row throughput bench + feature-gate `bench-hooks` (`8df975f`,
  `1ff4076`): `bench_append_read_buf` `#[cfg(feature=...)]` hook,
  real per-row measurement 8.48 ns/row (118 M rows/s).
- A7 rejected (`1a762ca`): tag LUT measured regression on all 4
  benches (+2.6-8.2%, p<0.05). Permanent closure with post-mortem.
- **Baseline:** `def184-complete` (aarch64-apple-darwin, stable):
  parse_header 2.52 ns, push_command/ping 107.6 ns,
  ping_round_trip 182.5 ns, iter_rows_per_row 8.48 ns/row.

### 2026-04-23 — DEF-184 crown cascade
- ErrorArena A1+A13 (`51ed3d8`): ProtocolError 312 → 72 B,
  Action 312 → 88 B, OutActions 2808 → 800 B.
- Audit cascade (`7a0e54d`): 13 architect-reviewed fixes
  (Result-returning arena API, loud Display advisory,
  ParsedServerError dedicated struct, ErrorRef gen u32,
  Clear unconditional bump, etc.).
- B21/C6 dispatch by-ref (`7a0e54d`): DispatchOutcome 800 → 88 B;
  install_errored / install_internal_bug atomic helpers.
- A10/B22 SCRAM split (`7a0e54d`): ProtoState 712 → 80 B via
  externalising ScramHandshakeState to PgProtocol; tier-2 drift
  classifier via CrateBugLocus::ScramStateDrift.
- Feedback response (`da32203`): 4-item audit-2 with one
  scaled-back (row_stream silent-drop).

### 2026-04-22 — DEF-154 Y + ErrorArena prep
- DEF-154 (Y) Action::StreamRow full deletion; pull-based RowStream
  API. (Commits 162e39c through 7eea711.)

---

## §F. Principles carried forward (reference)

- **CREDO §1 tier ladder:** safety > tier-1 compile > tier-2
  structural > tier-3 classified > tier-4 silent (banned).
- **CREDO §3 skepticism:** "первая идея — редко лучшая"; every
  non-trivial design has a second-choice audit step.
- **CREDO §5 no-discard:** every audit finding is dispositioned.
  "Reject" is dispositioned (this file's §B). "Forget about it" is
  banned.
- **CREDO §9 zero-leak:** full memory discipline; ZeroizeOnDrop on
  secret material; per-commit leak spot-check.
- **Bench-gated perf (2026-04-24):** every performance refactor
  requires a baseline measurement + post-change measurement +
  statistical significance (criterion p<0.05) before ship. Failed
  case studies live in §B. See `reforge.md §X` for the full
  principle.

---

## How to add / close entries

**Add an OPEN item:**
1. Pick the next DEF-NNN from git log (last used: 193 as of 2026-04-24).
2. Add row to the appropriate §A table.
3. Reference the ID in commit messages.

**Close an item:**
1. Ship it, commit with DEF-NNN reference.
2. Delete the §A row OR collapse into §D closed-index one-liner.
3. If measurement-rejected: move to §B with commit hash + post-mortem.

**Never** keep verbose "why it was open" text after ship — git log
is authoritative. This file is a work queue, not a diary.
