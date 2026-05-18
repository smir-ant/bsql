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

### v1.0 architectural & perf roadmap (cleaned 2026-05-08)

**SHIPPED so far (all closed entries removed from this section; one-line history in §D):**
- DEF-279 Phase 1c Bundle Commit 6 — 2026-05-18 — `c9d2c8b` — **`feed_bytes_dispatch_connecting` + `advance_one_frame_dispatch_connecting` (scaffolding)**. Adds three `#[allow(dead_code)]` items to `protocol.rs`: (a) `ConnectingDispatchContext<'state, 'r>` — per-phase mirror of `DispatchContext` with `state: &'state mut ConnectingState` (other 7 fields identical, since ConnectingInner currently retains PgProtocolInner's 8-field shape per Commit 2's scaffolding); (b) `feed_bytes_dispatch_connecting::<BOUNDED>` lift+lower wrapper — lifts `ConnectingState → ProtoState` via `mem::replace` + `Into`, calls shared `feed_bytes_dispatch`, projects back via `TryFrom<ProtoState> for ConnectingState`; (c) `advance_one_frame_dispatch_connecting` — Connecting-specific fast paths (Errored→Close, HandshakeReady+empty→Idle) + delegate to `feed_bytes_dispatch_connecting::<true>`. **Architect-endorsed Alt I** over Alt D's ~500 LoC duplication: single source of truth for dispatch loop preserved; zero drift risk between Connecting/Active/future phase variants; per-call lift+lower amortises ~5-10 ns over up to MAX_DISPATCHES=8 frames (vs Alt D's per-frame transition cost at ~8× overhead). **Lift+lower structurally requires Commit 5.5's lifetime split** — without `DispatchContext<'state, 'r>` separation, widening `'tmp → 'r` is contravariant (forbidden by `#![forbid(unsafe_code)]`). **HandshakeReady transition signal**: `ProtoState::Idle` outcome from `(PostAuthHaveKey, RFQ)` success arm translates to `ConnectingState::HandshakeReady`; defensive arm drains any `ReplyId<K>` (Drop-guard discipline via `match { Some(_) | None => {} }`) before installing `Errored(Internal)` for non-Idle non-Connecting outcomes. **Verification**: build clean; clippy `-D warnings` clean; 58+12+3+3 tests passing; `asm-diff feed_bytes` vs HEAD `48c7b8e`: 783 lines / 14797 bytes BEFORE = AFTER → byte-identical production codegen (DCE drops the dead scaffolding); grep `feed_bytes_dispatch_connecting\|advance_one_frame_dispatch_connecting\|ConnectingDispatchContext` in release `.s`: 0 matches per emit. 1 file, +213 LoC.
- DEF-279 Phase 1c Bundle Commit 5.5 — 2026-05-18 — `6718a7e` — **Split `DispatchContext` lifetimes**. Annotation-only refactor: `DispatchContext<'a>` → `DispatchContext<'state, 'r>` where `'state` covers the state `&mut` and `'r` covers all seven other `&mut` fields. `feed_bytes_dispatch::<BOUNDED>(ctx: DispatchContext<'state, 'r>, ...) -> OutActions<'w, 'r>` — return-borrow lifetime is now decoupled from state-borrow lifetime. Substrate for Commit 6's lift+lower wrapper: a per-phase wrapper can now pass `state: &mut local_lifted_proto_state` (short-lived) alongside outer-lifetime data refs without forcing the return `OutActions` to shrink to the local lifetime. **Architect-endorsed (Alt I)**: the alternative (Alt D — duplicate ~500 LoC of `feed_bytes_dispatch` body for per-phase variant) was rejected — single source of truth for the dispatch loop preserved, zero drift risk between Connecting/Active/future phase variants as the loop evolves. **OutActions covariance preserved**: `Action<'w, 'r>` carries `&'r [u8]` + `ReplyValue<'r>` (both covariant); `ManuallyDrop<heapless::Vec<Action<'w, 'r>, N>>` keeps covariance. The lifetime split is purely additive freedom — existing single-lifetime callers infer `'state = 'r` from `&mut self` and compile unchanged. **Verification**: build clean; clippy `-D warnings` clean; 58 lib + 12 tier_seams + 3 zeroize + 3 terminate_wire tests passing (no change); `scripts/asm-diff.sh feed_bytes` vs HEAD `3e89f30`: 783 lines / 14797 bytes BEFORE = AFTER → byte-identical production codegen (lifetimes compile-time-only; zero runtime impact). 1 file, 28 insertions / 13 deletions.
- DEF-279 Phase 1c Bundle Commit 5 — 2026-05-18 — `64aff8a` — **`connecting_dispatch` lift+lower wrapper (scaffolding)**. Adds two `#[allow(dead_code)]` helpers in `dispatch.rs`: (a) `install_errored_connecting` — per-phase mirror of `install_errored` for `ConnectingState` (identical body shape; state-agnostic `reply_id` + `cause` thread unchanged); (b) `connecting_dispatch` — lift+lower wrapper that lifts `ConnectingState → ProtoState` via `mem::replace` + `Into`, calls shared `dispatch`, then projects back via `TryFrom<ProtoState> for ConnectingState`. Catches the legitimate `ProtoState::Idle` outcome (post-handshake success from the `(PostAuthHaveKey, RFQ)` arm) and translates to `ConnectingState::HandshakeReady` — the transition signal `<ConnectingPhase>::into_active` will observe in Commit 8. Defensive arm for non-Idle non-Connecting outcomes drains any carried `ReplyId<K>` (Drop-guard discipline; `match { Some(_) | None => {} }` form per crate convention since `let _ =` for `#[must_use]` returns is forbid-bundle-banned) before installing `ConnectingState::Errored(Internal)`. Reachable only via a dispatch.rs bug under current variant entry conditions. **Tier-1 closure preserved at the per-phase wrapper boundary**: `ConnectingInner.feed_bytes_impl` (Commit 7) holds a `ConnectingState`-typed reference; widening to `ProtoState` is transient (inside this function only) for the shared dispatch's match arms; the lower projects back before returning. Lift+lower amortises ~5-10 ns/frame vs the alternative of duplicating ~310 LoC of dispatch arms. **Verification**: build green; clippy `-D warnings` green; 58 lib + 12 tier_seams + 3 zeroize + 3 terminate_wire tests passing; `scripts/asm-diff.sh feed_bytes` vs HEAD `4bccf16`: 783 lines / 14797 bytes BEFORE = AFTER → byte-identical production codegen; grep `connecting_dispatch\|install_errored_connecting` in release `.s`: 0 matches per emit (DCE confirmed). Bench-stable skipped per CREDO §96a — asm-diff confirms zero codegen delta, statistical follow-up is tautology under byte-identical production. 1 file, +122 LoC.
- DEF-279 Phase 1c Bundle Commit 4 — 2026-05-18 — `dd9f104` — **ConnectingState classifier methods (`take_inflight_reply_raw_id` + `push_class`)**. Per-phase mirrors of the same-named methods on [`ProtoState`]. `take_inflight_reply_raw_id` — exhaustive match consuming the `ReplyId<K>` from each variant that carries one; `HandshakeReady` and `Errored` return `None` (no correlator carried). `push_class` — returns `StatePushClass::Connecting` for all handshake variants + `HandshakeReady`; `StatePushClass::Errored` for the transient Errored signal. Adding a variant to `ConnectingState` that carries a `ReplyId<_>` without routing it into `take_inflight_reply_raw_id` is a build failure (exhaustive match shield) — Tier-1 closure preserved. **`#[allow(dead_code)]`**: scaffolding for Commit 5+ per-phase Inner migration (when `ConnectingInner.feed_bytes_impl` and `into_active` start using them); tracked as in-flight work in this queue entry. **Verification**: 534/0/0 non-doc tests (no change); clippy clean; release build clean. 1 file, +63 LoC.
- DEF-279 Phase 1c Bundle Commit 3 — 2026-05-18 — `a4308fd` — **ConnectingState::HandshakeReady transition signal**. Adds unit-variant `ConnectingState::HandshakeReady` — the per-phase signal that handshake has completed. Mechanism documented for Commit 4 wire-up: per-phase `ConnectingInner.feed_bytes_impl` will lift state `ConnectingState → ProtoState` for the shared dispatch path; shared dispatch processes `(PostAuthHaveKey, RFQ)` arm (writes `*state = ProtoState::Idle` + installs BackendKeyCell on the protocol's cell field, which IS `ConnectingInner.backend_key` since DispatchContext is built from ConnectingInner's fields); per-phase wrapper attempts `ConnectingState::try_from(proto_state)` — `Idle` has no `ConnectingState` mirror → `WrongPhase` returned; wrapper catches `WrongPhase{recovered: Idle}` and sets `ConnectingInner.state = ConnectingState::HandshakeReady`; `<ConnectingPhase>::into_active` observes `HandshakeReady` and lifts the wrapper to `<ActivePhase>` (backend-key cell was already installed atomically by the dispatch arm). **Bundle D' tier preservation**: no retention surface — `(pid, secret_key)` lives in the cell, `HandshakeReady` carries no payload. **From impl**: `HandshakeReady → ProtoState::Idle` preserves the bijection's semantic meaning. **Verification**: 534/0/0 non-doc tests (no change); clippy clean; release build clean; asm-diff: no production code change (variant is only constructed by the per-phase wrapper machinery landing in Commit 4). 1 file, +41 LoC.
- DEF-279 Phase 1c Bundle Commit 2 — 2026-05-18 — `705412c` — **ConnectingInner struct definition (scaffolding)**. Defines per-phase `ConnectingInner` type in `protocol.rs` as substrate for the Commit 3 SealedPhase migration. **Narrowed vs `PgProtocolInner`**: drops `row_desc_slot` (140 B; no `RowDescription` reachable from Connecting states) and `backend_key` (12 B; install deferred to `into_active` via state-variant transition signal landing in Commit 3). **Layout target**: state ConnectingState (~48 B) + read_buf 264 B + 4× 8 B cells/slots + u32 + alignment ≈ **344 B** (vs PgProtocolInner 536 B → **-192 B per `<ConnectingPhase>`**). **Tier-1 closure documented**: state-variant-in-wrong-phase impossible by-type; row_desc_slot/backend_key writes physically impossible during Connecting (fields absent from struct). **`#[allow(dead_code, missing_debug_implementations)]`** justified by explicit Commit-3-target purpose documented in the type's docstring + this queue entry — no future-session discipline required to discover the pending work. **Deferred to Commit 3**: SealedPhase impl migration (`<ConnectingPhase>::Inner = ConnectingInner`); `ConnectingState::HandshakeReady` state-variant signal; `connecting_dispatch` + `feed_bytes_dispatch_connecting` + `advance_one_frame_dispatch_connecting` + `clear_session_residue_for_class_dispatch_connecting` free fns (duplicated bodies adapted for narrow field set); `ConnectingState::push_class` + `take_inflight_reply_raw_id` + `unsolicited_admit` per-phase classifier mirrors; manual `Debug` impl on `ConnectingInner` (with `Sensitive`-redaction parity to `PgProtocolInner`); `push_startup` rebuild via new `_proto_init_leaf::fresh_connecting_inner`; `<ConnectingPhase>::into_active` rebuild observing `HandshakeReady`; `<ConnectingPhase>::into_closed_if_errored` rebuild observing `ConnectingState::Errored`; all 10 `<ConnectingPhase>::*` method body migrations; size pin updates; asm-diff + bench-stable verification. **Multi-hour focused work** (~2000-2500 LoC) deferred to keep this commit's scope manageable + respect CREDO §0 «no half-shipped tier elevations» — the type-definition substrate is shippable and self-documenting; the migration body lands atomically in Commit 3. **Verification**: 534/0/0 non-doc tests (no change vs HEAD); clippy `--all-targets -D warnings` clean; release build clean; asm-diff: no production code change (additive type definition only — LLVM emits nothing for an unused struct). 1 file, +106 LoC.
- DEF-279 Phase 1c Bundle Commit 1 — 2026-05-18 — `4cf6347` — **Per-phase state enum scaffolding (additive)**. Defines `ConnectingState` (12 variants — 11 handshake + Errored), `ActiveState` (20 variants — Idle + PingAwaitingRfq + SimpleQuery/Parse/BindExecute/Describe flows + DrainRfqAfterError + Errored), `ErroredState` (1 variant carrying StateErrorKind). Full From/TryFrom bijection with `ProtoState`. Architect-reviewed design decisions: (Q1) drop redundant `Connecting`/`Active` prefix in per-phase variant names — `ConnectingState::StartupTrust` not `::ConnectingStartupTrust`; (Q2) each phase enum has own `Errored(StateErrorKind)` variant (option A — no wrapping `enum InnerState`) for transient «Errored-on-Connecting/-on-Active» window before `into_closed_if_errored`; (Q3+Q4) `Idle`/`PingAwaitingRfq`/`DrainRfqAfterError` in `ActiveState` only; (Q5) `WrongPhase { recovered: ProtoState }` error type preserves `#[must_use] ReplyId<K>` Drop-guard discipline on rejection. **Size pins**: `size_of::<ErroredState>()==1` (single-variant niche), `size_of::<ConnectingState>()==48` (dominant: ScramAwaitingServerFinal 32 B SecretDigest + 8 B ReplyId + alignment), `size_of::<ActiveState>()==80` (dominant: DescribeStatement*AwaitingRowDescOrNoData 68 B ParamOids + 8 B ReplyId + alignment, matches today's ProtoState). **Pure additive — zero behavioral change**; new types have no production callers in this commit. Commit 2 (dispatch.rs split into per-phase fns) wires them; Commit 3 (Inner migration) makes them the storage type on per-phase Inner; Commit 4 (field-narrow drop row_desc_slot + backend_key from ConnectingInner) lands the -240 B savings. **Verification**: 534/0/0 non-doc tests (+6 round-trip tests vs HEAD's 528) covering all 32 ProtoState variants × bijection identity + 3 cross-phase rejection probes; clippy `--all-targets -D warnings` clean (forbid-bundle `expect_used`/`panic`/`assertions_on_constants` compliance via `let was_ok = result.is_ok(); /* match */; assert!(was_ok, ...)` runtime-bool pattern); release build clean; asm-diff vs HEAD (`cc152bc`): `materialise` only `.rodata` symbol index shift (l_anon.<HASH>.290 → .292) — 2 new string literals added (variant names / docstring intra-doc links); same instructions, bit-equivalent codegen — scaffolding-only as expected. 1 file, +887 / −0 LoC.
- DEF-279 Phase 1c prerequisite — 2026-05-18 — `d8b1a34` — **Extract dispatch body to free functions over `DispatchContext` (bottleneck-closing refactor)**. Pre-refactor: dispatch body lived as methods on `PgProtocolInner` (`feed_bytes_impl`, `advance_one_frame`, `clear_session_residue_for_class`). Method resolution forced the body to type-check against `&mut PgProtocolInner` (one concrete `Self`), so Phase 1c's per-phase narrow `Inner` types (`ConnectingInner` with FEWER fields than `PgProtocolInner`) could not reuse the same code without manual `self.X` rewrites or a trait-bound `HasDispatchFields` (the latter re-introducing tier-2 by-discipline — any future phase impl could reach the dispatch body). **Post-refactor**: dispatch body lives as **free functions** over `DispatchContext<'r>` (eight `&'r mut` field refs: state / read_buf / row_desc_slot / session_params / error_arena / partial_assembly / backend_key / malformed_count). `PgProtocolInner` methods become thin delegates that build a `DispatchContext` from `&mut self.<field>` and forward; the disjoint-field-borrow rule (Rust 2018+) lets the struct-literal construction split the `&mut self` borrow across eight fields in one expression. **Tier-1 closure**: only code that already holds the eight specific `&mut T` refs can build a `DispatchContext`; Phase 1c's narrow `Inner` types (e.g. `DisconnectedInner` with zero relevant fields) physically cannot construct a `DispatchContext` at the type level. **No tier elevation** in this commit (refactoring-only); the tier-1 win is *enabled* by this surface but materialises in Phase 1c (ConnectingInner narrow lands the actual storage-absence elevation on `<ConnectingPhase>`). **New surface**: `pub(in crate::protocol) struct DispatchContext<'a>`; `pub(in crate::protocol) fn feed_bytes_dispatch::<const BOUNDED: bool>`; `pub(in crate::protocol) fn advance_one_frame_dispatch`; `pub(in crate::protocol) fn clear_session_residue_for_class_dispatch`. **Verification**: 528/0/0 non-doc tests; clippy `--all-targets -D warnings` clean; release build clean; **asm-diff vs HEAD (0242859)**: `feed_bytes_impl` symbol 736 → 676 lines (3 → 1 emitted monomorphisations — LLVM now inlines the thin delegate into all 3 phase-feed callers directly instead of emitting a shared body); new `feed_bytes_dispatch::<true>` + `::<false>` cold-path closures emit ~41 lines each (the `with_branded` AlreadyErrored / AppendFailed bodies — cold paths LLVM keeps as separate symbols); `materialise` and hot dispatch loop produce equivalent instruction sequences. **bench-stable** vs `def279-phase1c-prereq-baseline` (iter benches) + `-v2` (push benches): `iter_10cols_large_5kb_row/pull_one_big_row` -3.22% mean (within noise); `column_decode/iter_10cols_alternating_null_i32` +2.13% mean (within noise); `push_command/ping` -1.47% mean (p=0.74 — no change detected); **0 regressions, 0 improvements, 2 unchanged**. **Sequencing**: this commit unblocks DEF-279 Phase 1c (ConnectingInner narrow — drop `row_desc_slot` 140 B + `backend_key` 12 B; expected 536 → ~296 B, -240 B savings); Phase 1c can now introduce `ConnectingInner` carrying a SUBSET of `PgProtocolInner`'s fields, build its own `DispatchContext` from those fields, and forward to the same `feed_bytes_dispatch` / `advance_one_frame_dispatch` bodies WITHOUT needing trait abstractions over the inner type. The existing PgProtocolInner thin delegates continue to work unchanged. 1 file, +798 / −912 LoC.
- Phase 1 cluster (DEF-249/251/252/254/256) — `2f63897` — 5-15% across hot path
- Phase 2 SWAR opt-in helper (DEF-250) — `e098dca` — 2.09× on 4-digit shape, zero regressions
- Phase 3 cache reorder (DEF-253) — `523b017` — AUDIT-CLOSED no-op (premise falsified)
- DEF-259 DropCounter zeroize verification — `d7d8532` — tier-2 by-discipline → tier-1 by-construction
- DEF-269 v2 Type-level CommandKind dispatch (T) — 2026-05-09 — `trait PushCommand` + per-command structs replace `PgCommand` enum; **push_command/ping −83.4%** (~55ns → 9.08ns, 6× speedup); 0 regressions. v1 (embedded WriteBuf) was REJECTED for +13% on push_command/ping; v2's architect-2 finding diagnosed the true root cause: PgCommand 2176 B by-value argument move (sized to Parse variant), not WriteBuf placement. T eliminates the move via per-type monomorphisation
- DEF-248 Sub-A — 2026-05-12 — `3a55d89` — closure-scoped `iter_rows` API + `ColEvent` (Got/Null/EndRow/Chunk/ChunkEnd/NeedMore/EndQuery) + ReadBuf partial-frame substrate + Drop install-Errored for D-tag DataRow streaming. **`mem::forget(RowStream)` structurally closed** (E0507 on `*stream` deref). Multi-MB JSONB/TEXT/BYTEA cells now stream at ~2.7 GiB/s on the partial-frame path. v1.0 production blocker resolved (most real workloads).
- DEF-248 Sub-B — 2026-05-12 — `d54c697` — **stream-and-truncate universal coverage** for non-D oversize frames (T/E/N/S/A/C/R/v). Architect cycle 3 found that every non-D parser is already inline-bounded at the type level (`SecretBoundedStr<128>`/`<96>`/`<64>` in `parse_error_response`, `MAX_ROW_COLUMNS=32` in `parse_row_description`, `BoundedStr<32>` in `parse_command_tag`, `CappedServerNonce<256>`+64 B salt in SCRAM). Worst-case parser depth ≈4.2 KB. Sub-B copies first `PREFIX_CAP=8192 B` into `heapless::Vec<u8, 8192>` (const-generic, overflow structurally impossible), counts-and-skips the rest. **Memory cost: constant 8 KB regardless of declared body length 0..~2 GiB.** Zero public API change. Closes universal-coverage residue («если возможен — не опускаем» fully satisfied). Cycle 2's 256 KB cap design rejected as frequency-based exclusion.
- DEF-244 — 2026-05-15 — `9ebebdf` — **`prepared!` proc-macro** — compile-time SQL parse + pre-baked Parse/Bind/Execute templates with fixed-offset placeholders. Hand-rolled SQL lexer (literals, dollar-quoting, comments, `::TYPE` casts, `$N` placeholders), SHA-256-96 content-addressed `stmt_name` (2⁻⁹⁶ collision space), type-level binding (cast types ↔ `ParamsWriter` args ↔ `RowDecode<Row<'_>>`-projected shape). **Closes SQL-injection class by-construction** (P6 trybuild rejects identifier/expression/macro inputs; literal-only). 12 P1-P12 hostile-bypass probes pinned via trybuild (E0451 direct-construction / E0599 no-pub-new / E0616 field-projection / E0308 arg-type-mismatch / E0277 sealed-trait ParamsWriter / `forbid-unsafe-code` for unsafe-fabrication / macro `compile_error!` for runtime-string input). 6 supported PG types in v1.0 (`int2`/`int4`/`int8`/`oid`/`bool`/`text`); 8 drift-detection rejection tests for `bytea`/`varchar`/`float4`/`float8`/`numeric`/`timestamp`/`uuid`/`jsonb` (delete-file mechanism when DEF-228 lands the type). 2 new dispatch arms handle silent state-restore when Parse-was-cached fast-path takes BindComplete-before-ParseComplete shape; critical `*state` write-back after `mem::replace` at `dispatch()` entry. **«-25 ns per push» claim REFUTED**: paired bench (`push_bind_execute_one_int_param/bind_execute` 174.54 ns vs `prepared_query_push/execute_prepared_dml` 170.26 ns) measured actual delta ~+4 ns within 5% noise threshold and CI overlap. Mechanism (no per-call Bind/Execute construction) is real, but the per-call work was already cheap (bounded memcpy + few field writes); savings masked by `iter_batched_ref` setup + reply minting + guard acquisition + Drop overhead. **Primary value: architectural (SQL-injection class closure by-construction + Tier-1 hostile-bypass surface), not bench-level perf.** `ping_round_trip/push_then_feed` under clean load 1.32-2.47: −3.02% within noise; prior cycle's +6.69% was load-induced false positive (load 5.06). Tests 570/0/10 (+20 trybuild). Clippy clean, release build clean. 61 files, +5882 LoC. Open follow-ups: DEF-228 wider type coverage; DEF-275 exploratory regression-hunt for `column_decode/iter_5cols_decode_text_long_ascii +8.3%` (unrelated to DEF-244 — touches untouched `decode.rs` — suspect LLVM codegen drift from DEF-258's `from_static_oids_text_format` helper) **[CLOSED 2026-05-15 as not-reproducible — clean-rebaseline measure was −0.55% within noise]**; DEF-247-redesigned typed-row projection layer naturally pairs.
- DEF-277 — 2026-05-16 — `30aa697` — **Zeroize memory-probe tier-elevation** — removed `#[ignore]` from 9 zeroize-on-drop verification tests (DEF-204 ReadBuf compact zeroize / DEF-205 SessionParams + SecretBoundedStr + ErrorArena zeroize / DEF-259 SCRAM session zeroize). Pre: tier-3-by-discipline («запустить `cargo test -- --ignored` когда вспомнить»). Post: tier-1-by-construction (run on every `cargo test`). Surface includes `Password::Drop` zeros backing buffer + `Sensitive<Password>::Drop` + `def205_overwrite_zeroizes_old_value` ×2 + `def205_drop_zeroizes_buffer` + `def205_clear_zeroizes_populated_fields` + `def204_compact_zeroizes_abandoned_tail` + `def205_error_payload_drop_zeroizes_all_fields` ×2. **Deleted 1 outdated broken test** `def204_compact_no_op_when_cursor_zero_does_not_zero` whose invariant pre-dated DEF-265 two-tier ReadBuf design — captured raw pointer into inline buffer, then appended past 256 B inline cap, expected pointer still to see MAGIC bytes; under DEF-265 the data escape moves bytes to heap and the abandoned inline storage is zeroized per DEF-204 hygiene, so probe reads zeros. The test's premise structurally impossible in current architecture; surfaced precisely because running it under `--ignored` revealed silent rot (principal observation 2026-05-16). Updated 4 file-level docstrings + 3 inline test docstrings/comments to remove `#[ignore]` references, document «debug-mode runs unconditionally; Miri (`cargo +nightly miri test ...`) is gold-standard additive UB-free verifier». **Test count: 570/0/10 → 579/0/0**. Net −54 LoC (5 files modified). Clippy clean, release build clean. **Tier-elevation surfaced by principal's «странно что есть тесты которые всегда в игноре» — exactly the «без стеклянных архитектур» mandate.**
- DEF-246 Phase 2+3+4 — 2026-05-17 — `17dc228` — **Branch-collapse typestate — 4 tier-1 elevations + 9 hostile probes (Approach E + Option α)**. Atomic ship building on Phase 1 (`f6ee560` scaffolding). **Option α**: `feed_bytes_impl<const BOUNDED: bool>` + `feed_bytes_bounded` + `feed_inbound` + `advance_one_frame` + `clear_session_residue_for_class` + `install_errored_replyid_saturation` moved from `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner`. `<ActivePhase>` retains thin `#[inline]` delegates compiling to single `b` tail-jumps (codegen-neutral via `#[repr(transparent)]`). `<ConnectingPhase>` gains same delegates plus `into_active(self) -> Result<PgProtocol<ActivePhase>, IntoActiveError>`. **4 tier-1 elevations** (each pinned by hostile probe): (1) push-before-Startup forbidden — `push_command` method-absent on `<DisconnectedPhase>` (P-E-4); (2) push-during-Connecting forbidden — method-absent on `<ConnectingPhase>` (P-E-5); (3) `<ClosedPhase>` absorbs nothing — `push_command`/`feed_inbound`/`advance_one_frame` all method-absent (P-E-6, P-E-7); (4) `feed_inbound` on Errored → typed `Err(ProtocolError::ConnectionAlreadyClosed { prior_kind })` instead of silent `Ok(())` (P-E-9, `unused_must_use` enforces caller-handling). **Approach E (no bypass)**: `PgProtocol::new()` returns `<DisconnectedPhase>`; bench/test fixtures use `fresh_active_via_trust_handshake()` helper in `tests/common/mod.rs` + duplicated in `benches/common.rs` — drives synthetic Trust handshake via public API only, no `pub fn __test_*` / `_for_test_*` surfaces introduced. **Deleted as structurally unreachable**: `PgCommand::Startup` variant, `pub struct Startup`, `impl PushCommand for Startup`, cfg(test) `compute_push_startup`. **24 test files + 2 bench files migrated** to consume-self / handshake-helper shape; 3 cross-state push tests retired (invariants now closed by typestate, not runtime guards); `feed_inbound_on_errored_is_silent_noop` → renamed `feed_inbound_on_errored_surfaces_connection_already_closed` with flipped Ok→Err expectation. **9 hostile-bypass probes** at `crates/bsql-pg-proto-derive/tests/def246_phase2-4_compile_fail/`: P-E-1 (E0599 `new_active_for_test` absent), P-E-2 (E0599 `__test_bypass_into_active` absent), P-E-3 (E0432 `Startup` struct deleted), P-E-4 (E0599 push_command absent on Disconnected), P-E-5 (E0599 push_command absent on Connecting), P-E-6 (E0599 push_command absent on Closed), P-E-7 (E0599 feed_inbound absent on Closed), P-E-8 (E0451 + private-type struct-literal blocked outside `mod protocol`), P-E-9 (`unused_must_use` on feed_inbound result). **Verification**: 586/0/0 tests; 9/9 hostile probes pass; clippy `-D warnings` clean; release build clean; `forbid(unsafe_code)` preserved; **asm-diff vs `f6ee560`** — `<ActivePhase>::feed_bytes`/`feed_inbound`/`advance_one_frame` compile to single `b` tail-jumps, `materialise` instruction-identical (only BB-label renumbering), `feed_bytes_impl` symbol moved `PgProtocol` → `PgProtocolInner` (semantic move, same body); **bench-stable** quiet-system floor matches pre-DEF-246 baseline within margin (`ping_round_trip/push_then_feed` ~130 ns vs 113 ns baseline; `push_command/ping` ~30 ns vs 10 ns baseline — the +20 ns delta is bench-harness `black_box(out)` materialising 88 B `PushFailure` return slot via ~80 `ldrh` instructions inside timed window, NOT production cost; production push code asm byte-identical pre/post DEF-246; long-lived connection amortises cold-cache first-touch over thousands of warm-cache pushes). **Bench-methodology learning**: `bench_ping_round_trip` + `bench_push_ping` migrated `b.iter()` → `b.iter_batched_ref(setup, body, BatchSize::SmallInput)` — Drop on `(PgProtocol, WriteBuf)` falls OUTSIDE timed window (post-handshake proto has populated state arena that ZeroizeOnDrop'd ~91 ns inside b.iter()'s closure-end Drop slot). `bench-stable.sh` script's `taskpolicy -c utility` + 30s measurement window QoS-demotes the bench process; on moderately-busy systems this picks up background contention noise reaching ~200 ns floor — measurement noise from QoS demotion, NOT a protocol regression. Always cross-check with direct `cargo bench` at normal QoS for codegen-equivalence claims (CREDO §96a primary: asm-diff; secondary: bench-stable). 28 files, +1905 / −1146 LoC. Open follow-ups: Phase 5/6 from queue entry (cosmetic — default-phase removal + further hostile-probe coverage); proposed DEF-278 `pub fn reset_to_idle(&mut self) -> Result<(), ResetError>` on `<ActivePhase>` for PgBouncer-style transaction-pool recycle (production API with independent merit; would enable amortised steady-state bench metric ≤15 ns push / ≤65 ns round-trip).
- DEF-278 Bundle D' — 2026-05-18 — `62c39ab` — **Closure-scoped CancelRequest tier elevation — retention attack structurally impossible**. Refactors Bundle D's `CancelRequestCredentials`-returning accessor into closure-scoped `with_cancel_request<R>(impl FnOnce(&[u8; 16], i32) -> R) -> Option<R>`. Tier elevation: invariant #14 from «tier-1 by-Drop-firing (retention possible via `mem::forget` / `Box::leak` / `ManuallyDrop`)» → **«tier-1 by-closure-scope (retention structurally impossible)»**. Mechanism: wire frame materialised inside `Zeroizing<[u8; 16]>` guard on `with_cancel_request`'s stack frame; closure receives `&[u8; 16]` borrowed from the guard; HRTB on `FnOnce(&[u8; 16], i32) -> R` rejects lifetime escape at compile time (E0521 — pinned by new probe P-d278d-6); guard itself unreachable from closure or outer-scope code, so `mem::forget(guard)` / `Box::leak(guard)` / `ManuallyDrop::new(guard)` have **no syntactic surface**. **`Option<R>` return** is the no-compromise honest model: state machine at `dispatch.rs:568` already rejects K-skip (Z without prior K → install_errored), so `<ActivePhase>` ⇒ backend_key installed is tier-1 by-state-machine-arm but NOT tier-1 by-typestate-on-Inner; honest `Option::None` arm for architecturally-distant fork-skip case obeys CREDO §V (no `unreachable!()` on architecturally-distant paths). Will collapse to `R` when Foundation rethink (DEF-279) lands per-phase `<Active>::Inner` carrying `backend_key: BackendKey` non-Option — tier-1 by-storage-absence at that point. **Documented intentional gap**: caller may `bytes.to_vec()` inside closure into caller-owned memory — memcpy into caller storage (not reference leak); original bytes in Zeroizing guard still scrubbed on closure return; required for async drivers spanning `.await` points; caller takes responsibility for their copy's scrub. **Panic semantics**: `panic = "unwind"` (cargo test) — frame unwinds, Zeroizing Drop fires, bytes scrubbed (pinned by Spec-12); `panic = "abort"` (release) — process exits without Drop (aligned with DEF-185 P0-A). **Verification**: 606/0/0 tests; 13/13 closure-pattern spec tests; 6/6 hostile probes pass (5 existing renamed + 1 new P-d278d-6 E0521 lifetime escape); clippy clean; release clean; `forbid(unsafe_code)` preserved; **asm-diff vs `1b7d83e` (Bundle D)** confirmed: `feed_bytes_impl` / `dispatch` / `with_cancel_request` **hot-path bit-identical** (closure-scope refactor asm-neutral on production). Size pins unchanged at 536 B (no layout change). 16 files, +772 / −554 LoC. **Open follow-ups surfaced by 3-architect safety audit (43 NEW findings)**: Bundle E (closure-scope ALL secret accessors — `Sensitive::get`, `Password::as_bytes`, `SecretBoundedStr::as_str`, `ScramSession::password_bytes` — same retention pattern, ~80 LoC); Bundle D'' (`Zeroizing<i32>` wrap for `dispatch.rs:613-618` secret_key extraction to plain stack — ~20 LoC); see DEF-280 queue entry for full Safety-bundles G/H/I/J/K bundle.
- DEF-278 Bundle D — 2026-05-18 — `9f75194` — **CancelRequest production primitive — query cancellation via PG §55.2.7 side-channel**. Pool layer (Phase 3) needs this to interrupt long-running queries before `reset_to_idle` recycle; without it `reset_to_idle` blocks on natural query completion. Architecture: new `cancel.rs` module with `BackendKey { pid: i32, secret_key: Sensitive<i32> }` captured at handshake-complete, wrapped in `BackendKeyCell` (`#[repr(transparent)]` over `Option<BackendKey>`) installed via token-gated `_backend_key_install_leaf::BackendKeyInstallToken` at the `(ConnectingPostAuthHaveKey, ReadyForQuery)` dispatch arm. **Note**: Bundle D's public `cancel_request_credentials()` accessor returning `CancelRequestCredentials` struct was REPLACED by Bundle D' closure-scoped API on 2026-05-18 (commit `62c39ab`); the **internal cell infrastructure** (BackendKey/BackendKeyCell/_backend_key_install_leaf token) shipped in Bundle D remains unchanged. **3 tier-1 invariants** (memo §6): #8 CancelRequest wire bytes const-asserted (16 B / BE magic / BE length — drift detection at build time); #13 cancel accessor method-absent on every pre-handshake / terminal phase (E0599 verified by 3 hostile probes); #14 `Drop` zeroizes `secret_key` (defense-in-depth via existing `Sensitive<i32>` ZeroizeOnDrop chain — **elevated to closure-scope retention impossibility in Bundle D'**). **Verification**: 605/0/0 tests; 5/5 hostile probes pass; clippy clean; release clean; `forbid(unsafe_code)` preserved; size pins 528 → 536 B (+8 B for BackendKeyCell field). 9 files, +1641 / −12 LoC.
- DEF-280 Bundle G — 2026-05-18 — `1cf6041` — **Eliminate `debug_assert!(false, …)` glass pattern — 4 production sites + 1 tier-1 elevation**. Audit found 10 literal occurrences crate-wide; 6 are `#[cfg(test)]` helpers (compute_push_ping / simple_query / parse / describe_statement / describe_portal + smoke-test) using the macro as debug-build panic — production-irrelevant, left untouched. 4 production sites eliminated: (1) `push_command_internal` Idle-precondition — kept classified PushFailure return, removed dev-noise assert; (2) `SendBytesRange.apply == None` — kept CloseSocket fallback, removed assert; (3) `StagedAction::DeliverReply` on push closure — TIER-1 ELEVATION: pre-Bundle G silently dropped on release + loud only in dev; post-Bundle G classified as `PushFailure { id: NonZeroU64::MIN, cause: InternalCrateBug { locus: PushEmittedDeliverReply } }` (NEW `CrateBugLocus` variant added with `#[non_exhaustive]` keeping it non-breaking); (4) `push_within_fanout_budget` overflow — kept const-asserted-architecturally-dead `cold_path()` + silent no-op, removed dev-loud assert. **Verification**: 607/0/0 tests (+1 Display drift pin); asm-diff `dispatch`/`feed_bytes` BIT-IDENTICAL; `materialise`/`push_startup` only `.rodata` 270→271 index shift (new string literal, zero runtime cost); bench-stable 0 regressions on hot filter. 2 files, +103 / −58 LoC.
- DEF-280 Bundle H — 2026-05-18 — `7b7a8c4` — **`RowStream: !Send + !Sync` by-construction via ZST PhantomData<*const ()>**. Adds private `_not_send: PhantomData<*const ()>` field to RowStream. `*const ()` is the canonical non-`Send` / non-`Sync` witness in `core::marker`'s auto-trait rules; `PhantomData<T>` is always layout-zero. Auto-trait propagation flows the `!Send` mark through `&mut RowStream`, futures capturing such borrows, and any user-built container with a RowStream field. Closes `tokio::spawn(...&mut RowStream...)` race-with-Drop class by-construction (E0277). NEW hostile probe `P-D280H-1` (E0277 "*const () cannot be sent between threads safely") + new test directory `tests/def280h_compile_fail/`. **Verification**: 608/0/0 tests (+1 hostile probe); asm-diff `dispatch` (5515 lines) / `row_stream` symbols (59 lines) BIT-IDENTICAL — ZST addition is layout-neutral; no bench needed (asm-equivalence deterministic per CREDO §96a primary). 4 files, +106 / −0 LoC.
- DEF-280 Bundle J — 2026-05-18 — `0286840` — **Distinct `CRATE_BUG_REPLY_ID_SENTINEL = NonZeroU64::MAX`**. Pre-Bundle J both CrateBug PushFailure sites (`PushCommandInternalNonIdle` + `PushEmittedDeliverReply`) carried `id: NonZeroU64::MIN` (= raw `1`) — byte-identical with the legitimate first id minted by `next_reply_id` on every connection's first command. Monitoring code distinguishing CrateBug failures by inspecting `push_failure.id` alone false-positived every first-command genuine failure. Post-Bundle J the sentinel is `NonZeroU64::MAX` (architecturally-distant collision: only the saturation-edge mint reaches it, AND that mint triggers `install_errored_replyid_saturation` immediately — collision impossible under intact production wrappers). Drive-by docstring correction: `next_reply_id` saturation path's docstring incorrectly claimed saturation "wraps to MIN" — `saturating_add(1)` at `u64::MAX` saturates at `u64::MAX`, never wrapping to zero; the `unwrap_or(MIN)` fallback is dead code present only for `forbid(clippy::unwrap_used)`. **Verification**: 609/0/0 tests (+1 sentinel-distinctness pin); asm-diff `dispatch` / `materialise` BIT-IDENTICAL (MIN→MAX immediate substitution is asm-equivalent on AArch64 64-bit constant loads); bench-stable 0 regressions. 3 files, +122 / −19 LoC.
- DEF-280 Bundle K — 2026-05-18 — `b66dc4a` — **`ReadBuf::enter_partial_mode` typed-Err elevation — re-entry tier-1 by-classified-routing**. Replaces `debug_assert!(partial_remaining == 0, …)` glass pattern at `buf.rs:855` with `Result<(), AlreadyInPartialMode>` return. Pre-Bundle K release mode silently overwrote `partial_remaining`, dropping previously-pending body-byte count → wire-desync class (next inbound bytes mis-classified as fresh frame header). Post-Bundle K both modes route through typed Err: counter LEFT UNCHANGED (no silent overwrite); caller in `RowStream::begin_partial_data_row` classifies via NEW `CrateBugLocus::PartialModeReentry` + NEW `install_errored_partial_mode_reentry` helper (cluster δ leaf shape mirroring `_read_cursor_advance_drain_leaf`) + classified `ColEvent::EndQuery::Err`. NEW `AlreadyInPartialMode` struct + `core::error::Error` + Display impls. 3 spec tests pin: (1) Ok-on-idle, (2) Err-preserves-counter on re-entry (the critical no-overwrite guarantee), (3) AlreadyInPartialMode Display drift. **Verification**: 613/0/0 tests (+3 Bundle K spec + Display drift pins); asm-diff `dispatch` (5515 lines) BIT-IDENTICAL; `materialise` only `.rodata` 271→273 index shift (2 new string literals: locus name + Display format); `begin_partial_data_row` inlined into iter_rows — new Err arm is cold/architecturally-dead path; bench-stable 0 regressions. **Note**: `exit_partial_mode` has the same debug_assert pattern (silent counter reset rather than overwrite — distinct hazard); architect-named only `enter_*` in Bundle K scope; exit-side hardening is a clean follow-up. 5 files, +279 / −14 LoC.
- DEF-280 Bundle I (= Bundle D'') — 2026-05-18 — `e02eed7` — **`Zeroizing<i32>` for `secret_key` stack-slot scrub at 2 sites**. Pre-Bundle I plain `i32` locals at `dispatch.rs:613` (handshake-completion arm extracting from variant's `Sensitive<i32>` → re-wrapping for cell + embedding in `StartupCompletePayload`) and `protocol.rs:2362` (`with_cancel_request` extracting for `cancel_request_bytes(pid, secret)` call) lived unscrubbed on the function frame between source-Sensitive scope-exit Drop and destination-Sensitive/Zeroizing Drop — defense-in-depth gap. Post-Bundle I both intermediates wrapped in `Zeroizing<i32>` (zeroize crate's blanket Zeroize impl for primitive ints writes `0` on Drop; Deref exposes `i32` for consumers). Under `panic = "unwind"` Drop fires on stack unwind; under `panic = "abort"` (release; documented gap) the process exits before unscrubbed slot matters. **Verification**: 613/0/0 tests (no change); asm-diff `dispatch` +5 lines exclusively in cold once-per-connection `ConnectingPostAuthHaveKey → Idle` handshake-completion arm (Zeroizing materialise + reload + scrub); hot dispatch loop / `materialise` / `feed_bytes` BIT-IDENTICAL; bench-stable hot benches 0 regressions (handshake-completion isn't exercised by per-command microbenches; ~5 ns per-connection cost vs tier-1 secret-protection benefit is negligible). 2 files, +45 / −19 LoC.
- DEF-280 Bundle K-mirror — 2026-05-18 — `83d5a09` — **`exit_partial_mode` typed-Err elevation + upstream-discipline removal (Approach B single source of truth)**. Mirror of Bundle K for the exit side. Replaces `debug_assert!(partial_remaining == 0, …)` + silent counter reset at `buf.rs:894` with `Result<(), PartialModeExitUndrained>`. **Removes** the two pre-Bundle-K-mirror upstream `if partial_remaining == 0 { exit_partial_mode(…) }` discipline-checks in `row_stream.rs` — single source of truth: the function itself enforces the precondition. NEW `CrateBugLocus::PartialModeExitUndrained` variant; NEW cluster δ leaf submodule `_partial_mode_exit_undrained_drain_leaf`; NEW `install_errored_partial_mode_exit_undrained` helper; `PgProtocol::partial_remaining_for_row_stream` DELETED (was the discipline-check pass-through); `ReadBuf::partial_remaining()` migrated to `#[cfg(test)] pub(crate)`. Both row_stream callers route Err through classified `ColEvent::EndQuery::Err`. Tier delta: tier-2 dev-asserted + release silent-reset (wire-desync class — body bytes still owed on wire abandoned) → tier-1 typed Err + must_use + classified install_errored. **Side benefit**: end-of-row body-length / column-length disagreement classification now happens IMMEDIATELY at exit-time (was deferred to next-frame dispatcher entry pre-Bundle). 3 spec tests pin Ok-on-drained + Err-preserves-counter + Display drift. **Verification**: 617/0/0 tests (+4 vs HEAD's 613); asm-diff `dispatch` (5520 lines) BIT-IDENTICAL; `materialise` only `.rodata` 273→275 shift (+2 new strings: locus name + PartialModeExitUndrained Display); bench-stable 0 regressions. 5 files, +361 / −40 LoC.
- DEF-280 Bundle F Phase 1 — 2026-05-18 — `643511f` — **`InstallBody` trait split closes within-crate hostile-witness hole**. Pre-Bundle-F shape: `pub(crate) trait PostStateProof: sealed::Sealed { fn install_into(self, &mut ProtoState); }` with the 7 install bodies in `impl PostStateProof for *Install { ... }` blocks in `push_command.rs`. `sealed::Sealed` is `pub(crate)` (sibling-accessible), so any in-crate module could write `impl Sealed + impl PostStateProof for HostileWitness { fn install_into(self, state) { *state = arbitrary_variant; } }`, mint `StateSetter<HostileWitness>` via the generic `IdleState::into_setter::<W>` (W: PostStateProof only), and call `setter.install_post_state(HostileWitness)`, dispatching to the hostile body → state-machine corruption / zombie-reply class. Tier-2-by-discipline within-crate. Post-Bundle-F: trait split. `PostStateProof` → pure marker (no method; kept for `#[diagnostic::on_unimplemented]` UX). NEW `pub(crate) trait InstallBody: PostStateProof + install_body_seal::InstallBodySealed { fn install(self, &mut ProtoState); }`. `install_body_seal` is `mod install_body_seal` (no visibility keyword — PRIVATE to mod state_setter). Hostile attempt to write `impl InstallBody for X` fails E0277 (X: InstallBodySealed not satisfied); writing `impl InstallBodySealed for X` fails E0603 (path is private). All 7 `impl InstallBody for *Install { fn install(...) }` blocks live in state_setter.rs; push_command's `impl PostStateProof for *Install` becomes empty markers. Bounds tightened (W: PostStateProof → W: InstallBody) on: `StateSetter::install_post_state` (the attacker-controllable site), `StateSetter::install_errored` (zombie-class symmetric closure), `IdleState::into_setter<W>` (declaration boundary closure), `StateSetter<'a, W>` struct def, `PushCommand::PostState` (associated-type declaration closure). **Architect review** caught fatal flaw in naive Approach 6 (concrete per-W impls collapse at protocol.rs:2879's generic dispatch site `idle.into_setter::<C::PostState>()` + `cmd.execute(setter, ...)` where only W: PostStateProof is known); trait-split preserves generic dispatch AND seals install authority — strictly stronger than closed-enum (breaks StartupPostInstall's carried Box<ScramSession>), factory-token (witness still attacker-mintable), or concrete impls (loses generic dispatch). **Seal pin**: no-dep ambiguous-blanket-impl trick (mirror of `lib.rs:535`'s `assert_not_sync<PgProtocol>`) in `push_command::bundle_f_seal_probe`. Defines HostileWitness, impls Sealed + PostStateProof (still succeed — sibling-accessible), then uses two overlapping `AmbiguousIfInstallBody<()>` / `AmbiguousIfInstallBody<u8>: ?Sized + InstallBody` blanket impls to assert at compile time that HostileWitness lacks InstallBody. If a future refactor re-opens the seal (e.g., `pub mod install_body_seal` OR fuses InstallBody into PostStateProof), the const block emits ambiguity error at typeck and the test target build fails. Zero runtime cost (typeck-only). **Verification**: 528/0/0 non-doc tests (+1 `hostile_witness_install_body_absent_anchor`; pre-existing 1 doctest failure in decode.rs StreamItem unrelated — verified by stash-bisect against HEAD); clippy --all-targets -D warnings clean; asm-diff vs HEAD (cbb5f02): `materialise` (271 LoC) BIT-IDENTICAL; `dispatch` (5524 LoC) BIT-IDENTICAL; `feed_inbound` (114 LoC) BIT-IDENTICAL — trait split is purely architectural, monomorphized `proof.install(state)` inlines to the same `*state = ProtoState::Variant(...)` write that `proof.install_into(state)` inlined to pre-Bundle-F; zero perf delta by construction; bench-stable vs `pre-bundle-f-phase1`: push_command/ping time [-6.13% +2.57% +11.91%], p=0.57 — "No change in performance detected"; 0 regressions, 0 improvements, 1 unchanged (wide CI is microbench harness jitter ~10%; median +2.5% well within noise band, consistent with bit-identical asm); release build clean (rlib 4585856 B; size-pin tests hold). Tier delta: tier-2-by-discipline within-crate → tier-1-by-construction within-crate. 2 files, +339 / −68 LoC.
- DEF-280 Bundle E Phase 1 + P-D280E-1/2 probes + .unwrap_or(&[]) sweep — 2026-05-18 — `315c178` + `a7d6424` + `2041db8` — **`Sensitive::get` → `with_inner` closure-scope + 2 hostile probes (E0599 method-absent, E0521 lifetime-escape) + 3 non-fundamental defense-in-depth sites swept**. Bundle E Phase 1 migrates `pub const fn Sensitive::<T>::get(&self) -> &T` (the foundational secret-borrow accessor with docstring discipline «caller must not store the reference beyond the immediate computation») to `pub fn with_inner<R>(&self, f: impl FnOnce(&T) -> R) -> R`. HRTB-quantified `for<'a> FnOnce(&'a T) -> R` rejects retention attacks (R is universal, independent of 'a; returning `&'a T` would require 'a: 'static which HRTB doesn't satisfy). 4 production callers migrated: `dispatch.rs:636` (handshake-completion `secret_key` extraction), `dispatch.rs:2099` (SCRAM/cleartext PasswordMessage builder), `dispatch.rs:2255` (MD5 password handshake), `protocol.rs:2386` (`with_cancel_request` sibling site). 1 test migrated (Drop-witness probe captures `*const u8` from closure return). 1 type API change: `Sensitive::get` deleted. Cascading migration: `ScramSession::password_bytes(&self) -> &[u8]` → `with_password_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R` (uses Sensitive::with_inner internally); 1 caller in `dispatch.rs:1755` reorganises closure scope around `compute_client_proof`. **Transitively covered by Phase 1**: `Password::as_bytes` — Password is only accessible via `Sensitive<Password>::with_inner` post-Phase 1, so `&Password::as_bytes()` is HRTB-bounded transitively. **Phase 2 deferred** (SecretBoundedStr::as_str/as_bytes): no docstring discipline rule on these accessors — Rust lifetime-checking is sound for &str/&[u8] borrows; closure-scope migration would be cosmetic. 2 hostile probes: P-D280E-1 (E0599 method-absent on the deleted `.get()`), P-D280E-2 (E0521/E0495 lifetime-escape — `let leaked: &i32 = s.with_inner(|inner| inner)` rejected by compiler; pins retention-impossibility by-construction). Plus the .unwrap_or(&[]) sweep (`2041db8`) at 3 non-fundamental sites: `protocol.rs:3316` (partial-assembly absorb tail), `partial_assembly.rs:470` (copy-slice into prefix buffer), `dispatch.rs:2438/2442` (ErrorResponse field parsing — paired sites). Each gets the DEF-281 two-phase pattern: explicit bounds-check with `cold_path()` marker + retained syntactic fallback on now-unreachable path. **Verification**: 619/0/0 tests (+2 hostile probes vs HEAD's 617); asm-diff `dispatch` +4 cold lines exclusively in ErrorResponse-parsing path (2 sites × 2 instructions for explicit bounds-check + conditional branch); `materialise` BIT-IDENTICAL; small stack-frame reduction (−16 bytes) from with_inner inlining; bench-stable hot filter 3 unchanged + 0 regressions. 3 commits, ~+239 LoC. **Remaining Bundle E**: Phase 2 (SecretBoundedStr — design-cosmetic, no docstring discipline to eliminate); Bundle F (sealed → concrete tokens, pairs with DEF-279); push_within_fanout_budget true tier-1 (blocked on unstable `generic_const_exprs`); fundamental ReadBuf primitives (`buf.rs:174/627` — defer to DEF-279).
- DEF-279 Phase 1b — 2026-05-18 — `5e6127f` — **ClosedInner narrow (536 B → 16 B for `<ClosedPhase>`)**. Continues DEF-279 Foundation Rethink. Phase 1a shipped `<DisconnectedPhase>` as ZST; Phase 1b narrows `<ClosedPhase>` to a 16-B `ClosedInner` carrying only `state_kind: StateErrorKind` (1 B) + `error_arena: Option<Box<ErrorArena>>` (8 B with niche) + ZST sync_marker. `impl SealedPhase for ClosedPhase { type Inner = ClosedInner; }` (was `PgProtocolInner`). 536 → 16 B per protocol, **-520 B (97% reduction)**. **Tier elevation in cause()**: pre-Bundle the body matched `&self.inner.state` against `ProtoState::Errored(k)` with an "architecturally unreachable" defensive arm (`_ => InternalCrateBug { locus: ReadCursorAdvance }`) for the impossible non-Errored case. Post-Bundle `ClosedInner` doesn't carry `ProtoState` at all — the non-Errored arm is GONE BY STORAGE ABSENCE (type cannot construct a Closed protocol with non-Errored state). Tier-3-by-state-machine-reasoning → tier-1-by-storage-absence. New body: `ProtocolError::ConnectionAlreadyClosed { prior_kind: self.inner.state_kind }` — no match, provably-correct by type. **New accessors**: `<ClosedPhase>::get_server_error(ErrorRef) -> Result<&ErrorPayload, ArenaError>` + `<ClosedPhase>::error_arena_overwrite_count() -> u16` (mirrors of `<ActivePhase>` / `<ConnectingPhase>` accessors — preserved arena lookups for wrapper-layer stashed ErrorRefs). Old `<ClosedPhase>::state() -> &ProtoState` DELETED (no callers in src/ or tests/; provably Errored by storage absence). **Transition rebuilds**: 2 sites moved from `inner: self.inner` (PgProtocolInner direct move) → field extraction (`*state_kind` Copy + `mem::take` of error_arena) + ClosedInner materialise: `<ConnectingPhase>::into_active` Closed arm (line ~1794) and `<ActivePhase>::into_closed_if_errored` (line ~4593). The remaining PgProtocolInner fields (read_buf 264 B, row_desc_slot 140 B, session_params 8 B, partial_assembly 8 B, backend_key 8 B, malformed_count 4 B, alignment padding) Drop at the transition boundary — releasing ~520 B of stack + any Box-niche heap (Box<ScramSession>, Box<Password>, Box<ErrorArena> if not mem::take'd). **Debug impl rewrite**: `<ClosedPhase>` no longer delegates to PgProtocolInner; new body prints `phase: "ClosedPhase"` + state_kind + error_arena_overwrite_count. **IntoActiveError variant asymmetry**: `StillConnecting(PgProtocol<ConnectingPhase>)` 536 B vs `Closed(PgProtocol<ClosedPhase>)` 32 B (was both 536 B). `#[expect(clippy::large_enum_variant, reason = ...)]` documents the by-product. **Size pins added**: `size_of::<PgProtocol<ClosedPhase>>() == 16` (was 536); `size_of::<ClosedInner>() == 16` (new pin). **Verification**: 528/0/0 tests; clippy --all-targets -D warnings clean; asm-diff vs HEAD (95a41e9): hot paths (dispatch/materialise/feed_inbound) bit-equivalent — only cosmetic `.rodata` index shifts (l_anon.<HASH>.23 → .29; .285 → .290) and label renumber (LBB48 → LBB49) reflecting new Phase 1b code (~5-6 new string literals). Phase 1b changes are exclusively in the cold once-per-protocol transition arms. bench-stable vs `pre-def279-phase1b`: push_command/ping "No change in performance detected" (p=0.59); 0 regressions, 0 improvements, 1 unchanged. **Cumulative DEF-279 progress**: Phase 1a `<Disconnected>` 536→0 B; Phase 1b `<Closed>` 536→16 B; Phase 1c `<Connecting>` →~296 B (pending); Phase 1d `<Active>` Cell collapse (pending); Phase 2 per-phase state enum (pending); Phase 3 per-phase dispatch (pending); Phase 4 per-phase ReadBuf + 5.E + 7.B (pending). 2 files, +179 / −35 LoC.
- DEF-279 Phase 1a — 2026-05-18 — `5b5b886` — **Per-phase Inner GAT + DisconnectedInner ZST (536 B → 0 B for `<DisconnectedPhase>`)**. DEF-279 Foundation Rethink Phase 1 begins. Memo at `/tmp/def-rethink-foundation-memo.md` (1368 LoC) recommends bundle 1.B + 2.B + 3.B + 4.B-partial + 5.E + 7.A + 7.B over 6-9 weeks. Phase 1a is the first atomic shippable step: trait-split `pub trait SealedPhase { type Inner; }` (non-GAT plain associated type — Q1 architect refinement), introduce `pub struct DisconnectedInner { sync_marker: PhantomData<Cell<()>> }` (ZST, 0 B), assign `impl SealedPhase for DisconnectedPhase { type Inner = DisconnectedInner; }` (the other 3 phases keep `type Inner = PgProtocolInner` for Phase 1b/1c divergence). `PgProtocol<P>` struct's `inner` field switches to `<P as SealedPhase>::Inner` (compiler normalises before #[repr(transparent)] layout check — Q2 architect confirmed). **push_startup rebuild**: pre-Bundle moved `self.inner: PgProtocolInner` (carrying empty cells pre-Startup) into the `<ConnectingPhase>` wrapper; post-Bundle materialises a fresh PgProtocolInner via the new `_proto_init_leaf::fresh_inner()` helper (pub(in crate::protocol), sibling-callable from push_startup; preserves DEF-272 P6 leaf-private ProtoInitToken gating). **Cascade fixes**: (a) `PgProtocolInner`/`DisconnectedInner` promoted to `pub` for E0446 (associated type cannot leak crate-private through pub `SealedPhase` trait surface) — fields remain private, constructors stay gated, promotion adds no external capability; (b) `PROCESS_REPLY_ID_COUNTER: AtomicU64` extracted to module-level static (shared by `<Disconnected>::next_reply_id` and `PgProtocolInner::next_reply_id` — process-global uniqueness preserved across phases via single counter); (c) `<DisconnectedPhase>` accessors rewritten to Phase-1a-appropriate constants (`error_arena_overwrite_count` → 0; `connection_status` → Ready; `state` → `&ProtoState::Idle` via const-promoted reference; `next_reply_id` inlined w/o saturation classifier — no `inner.state` to mark Errored, architecturally distant); (d) Debug blanket impl split into 4 per-phase impls — 3 non-Disconnected delegate to new `impl Debug for PgProtocolInner` (body bit-identical pre-Bundle), Disconnected prints phase name only; (e) test helper bounds tightened `<P: SealedPhase>` → `<P: SealedPhase<Inner = PgProtocolInner>>` on `populate_residue`/`quench_inflight`/`session_params_is_pristine`; residue tests use new `fresh_active_proto()` test helper (calls `_proto_init_leaf::fresh_inner()`) instead of the old `PgProtocol::new()` (which now returns Disconnected ZST). **Architect review** (`Agent(architect)`): caught fatal flaw in naïve pure-bootstrap Phase 1a (zero tier elevation = scaffolding without exit path per CREDO §0). Fold DisconnectedInner ZST into Phase 1a today — delivers a real tier-1 win (536 B → 0 B), proves the GAT non-tautological from day one, exercises field-by-field transition mechanism in push_startup. Adopted Approach 7-refined (split with constraint tightening, all stable Rust, no GAT-of-lifetime needed). **Size pins**: `size_of::<PgProtocol<DisconnectedPhase>>() == 0` (was 536); `size_of::<DisconnectedInner>() == 0` (new pin); `<{Connecting,Active,Closed}>` unchanged at 536. **Tier delta**: tier-3-by-state-machine-reasoning (Disconnected fields are tautologically empty pre-Startup by construction of state machine, but storage exists for them) → tier-1-by-storage-absence (storage physically doesn't exist on `<DisconnectedPhase>::Inner`). **Verification**: 528/0/0 non-doc tests (1 pre-existing unrelated doctest failure); clippy --all-targets -D warnings clean; asm-diff vs HEAD (b58d828): `materialise` 122-line diff is ALL label renumbering (LBB44_X → LBB47_X) + .rodata index shift (275 → 285) — same instructions, cosmetic numeric drift from new symbols. Bit-equivalent codegen (the trait-split monomorphises identically; each `<P as SealedPhase>::Inner` resolves concretely at typeck before layout). `dispatch`/`feed_inbound` same pattern. bench-stable vs `pre-def279-phase1a`: push_command/ping "No change in performance detected" (p > 0.05); 0 regressions, 0 improvements, 1 unchanged. Release build clean. **Sequencing**: Phase 1b (next session): `ClosedInner` carrying only StateErrorKind cause (drop read_buf/state/row_desc_slot — all unused post-Errored). Phase 1c: `ConnectingInner` middle weight (drop row_desc_slot/backend_key — auth-flow only). Phase 1d: ActivePhase keeps PgProtocolInner OR per-Direction-5.E collapses Option<Box<SessionParams>>+Option<BackendKey> to inline non-Option (always-populated post-handshake tier-1 by-construction). Phase 2: per-phase state enum (Direction 2.B). Phase 3: per-phase dispatch (Direction 3.B). Phase 4: per-phase ReadBuf + Cell collapse + RowDesc witness (4.B + 5.E + 7.B). 2 files, +430 / −125 LoC.
- DEF-281 Sites E/F/G/H/I — 2026-05-18 — `0d32fc7` + `8d9ac4e` — **Remaining row_stream `.unwrap_or(&[])` sites elevated to explicit-match parity**. **Sites E+F** (`row_stream.rs:532-533`, col_formats/col_bytes slice projection in collect_tuple's EndRow assembly): architecturally-dead None arms (n_used = col_count.min(MAX_ROW_COLUMNS) ≤ MAX_ROW_COLUMNS, both arrays are [_; MAX_ROW_COLUMNS]); upstream Got/Null path classification on `col_count > MAX` (Site B `TooManyColumns`) pins col_count ≤ MAX before EndRow. **Sites G/H/I** (`row_stream.rs:737/1378/1399`, header/col_count/col_len peeks): architecturally-dead None arms (cursor ≤ populated.len() by ReadBuf invariant); each site has downstream classified-Err in place (parse_header → ColEvent::NeedMore; col_count → MalformedDataRow; col_len → caller's MalformedDataRow). Tier elevation for E/F: tier-3 silent → tier-1-by-construction with documented-dead-arm; Sites G/H/I: cosmetic-parity (already tier-1 via downstream classification, but CREDO §V's ban on silent fallbacks applies at call site too). **Verification**: 528/0/0 tests; clippy clean; asm-diff `dispatch` + `materialise` BIT-IDENTICAL on both commits (match form monomorphizes to identical instructions). 1 file, 2 commits, +48 / −8 LoC. **Audit close**: all row_stream `.unwrap_or(&[])` sites cleared. Remaining audit work: fundamental ReadBuf primitives `buf.rs:174/627` (defer to DEF-279 — Result-returning accessor refactor cascades through every caller).
- DEF-281 Sites A/B/C/D — 2026-05-18 — `9c25c1e` + `df42ab0` — **Silent row-truncation class closure — 4 sites in `collect_tuple` typed-row decoder**. **Site A** (`row_stream.rs:~1123`, whole-column body slice projection): pre-Bundle silent `.unwrap_or(&[])` empty fallback on architecturally-dead None → two-phase defense: `populated.len()` copy + explicit bounds check + classified `CrateBugLocus::ReadCursorAdvance` Err via `terminal_internal_advance_err()` before re-borrow + retained syntactic fallback on now-unreachable path. Borrow-checker constraint forced the two-phase shape (terminal helper needs `&mut self`; slice's lifetime ties to `&self` via populated borrow). **Site B** (`row_stream.rs:471`, Null event path beyond `MAX_ROW_COLUMNS = 32`): pre-Bundle silent drop on idx >= 32 (col_count plateaus, downstream R::ARITY check false-positives if R::ARITY happens to equal truncated count — REAL user-visible data-corruption class). Post-Bundle mirrors the Got path's classified `ProtocolError::TooManyColumns`. **Site C** (`row_stream.rs:~1196`, first chunk of chunked column): same two-phase pattern as Site A. **Site D** (`row_stream.rs:~1271`, chunk continuation): same two-phase. Tier delta: tier-3 silent truncation (Site B real bug; Sites A/C/D architecturally-dead-arm CREDO §V glass) → tier-1 classified Err on hazard path + retained syntactic fallback on proven-dead path. **Verification**: 617/0/0 tests (no change — integration spec test for 33+ Null col adversarial wire deferred since existing test infra builds 1-col DataRows; symmetric Got-path guard is exercised by every multi-col test transitively); asm-diff `dispatch` (5520 lines) / `materialise` (271 lines) BIT-IDENTICAL on both commits; wire-legal fast path zero additional instructions; cold-path cost per slice projection ~2 integer comparisons + 1 conditional return. 1 file, 2 commits, +79 / −17 LoC total. **Remaining `.unwrap_or(&[])` sites**: 5 in `row_stream.rs` (header peeks at 721 + col_count/col_len readers at 1362/1383 — ALL architecturally-dead with classified fallback already in place: `parse_header` Empty → NeedMore; `match [a,b,..] _ => Err(MalformedDataRow)`); fundamental ReadBuf primitives at `buf.rs:174/627` (silent-empty defense behind debug_assert invariant; refactoring to Result returns would cascade — defer to DEF-279 Foundation phase); error-response parsing at `dispatch.rs:2405/2409`; `partial_assembly.rs:470`; `action.rs:466`; `protocol.rs:3312`. Each pending site warrants per-call audit to confirm architecturally-dead vs reachable.
- DEF-276 — 2026-05-15 — `f93c84a` — **Rust 1.78-1.95 modernization** — 53 tier-elevations, 0 downgrades. **Group A (Diagnostics, 1.78):** 18× `#[diagnostic::on_unimplemented]` on every sealed trait with public surface (`ParamsWriter`/`ParamEncoder`/`RowDecode`/`ColTextAt`/`PushCommand`/`FromPgText`/`FromPgBinary`/`EncodeBinary`/`DecodeFormat`/`FormatCodeMarker`/`BoundedLen`/`FixedStrKind`/`Validated`/`Truncating`/`ValidUtf8`/`DescribeName`/`ReplyKind`/`PostStateProof`) — bound-failure UX promoted from tier-3-by-discipline to tier-1 at compiler-diagnostic level (replaces DEF-244 P8 doc-note with structural attribute); `#[diagnostic::do_not_recommend]` (1.86) rejected as redundant (every blanket impl is already sealed). **Group B (Drift detection, 1.81):** 10× `#[allow]` → `#[expect]` migration (drift fires when underlying lint stops emitting — tier-3-by-discipline → tier-2-by-attribute), 13× dead `#[allow]` removed entirely, 1× retained `#[allow]` with comment (cfg-conditional). **Group C (Edition 2024 idiom):** 2× let-chains (`md5.rs:216-225`, `row_stream.rs:475-482`) + 1× `Option::is_none_or` (`row_stream.rs:432`) replacing `.map(pred).unwrap_or(true)`. **Group D (Type-level interop, 1.81):** 12× `impl core::error::Error` on public error types (`ProtocolError`/`StateErrorKind`/`DecodeError`/`OutOfRange`/`IdentError`/`PodBytesOverflow`/`PasswordError`/`ReadBufFull`/`AdvancePastEnd`/`WriteBufFull`/`ArenaError`/`ScramError`/`ServerNonceTooLong`/`PushFailure`) — `PushFailure` additionally gains `Display + source() → &cause` (chained error); enables `?`-propagation through `Box<dyn core::error::Error>` boundaries for downstream `bsql-driver-postgres` without manual `From` bridges. Tier-2-by-pattern → tier-1-by-construction. Trait upcasting (1.86), `core::array::from_fn`, `Result::flatten`, `Cell::update`, `LazyLock`/`LazyCell` — all evaluated, no applicable patterns in codebase. **Verification:** asm-diff (CREDO §96a primary, deterministic) bit-identical on `materialise` (271 LoC, only `.rodata` string-table index shift), `parse_header` (99 LoC), `feed_inbound` (74 LoC), `dispatch` (7 symbols, 5510 LoC). Bench-stable compare `pre-def276-clean` (secondary statistical, clean load 1.89→2.12): 16 unchanged / 2 improvements (`iter_jsonb_1mb_streaming/stream_1mb_chunked` −6.26%, `col_next_per_event_cost/200_events_tight_loop` −8.07% — codegen-drift bonus, no source-level cause) / **0 regressions**. 3 .stderr goldens regenerated (`prepared_unsupported_types/{bytea,float4,float8}.stderr`) — diagnostic shape improvement (named bound + supported-set label + sealed-extension note); P1-P12 prepared_compile_fail goldens unchanged (sealed-supertrait is separate diagnostic node). Tests 570/0/10, clippy `-D warnings` clean, release build clean (size pins hold), `forbid(unsafe_code)` preserved. 26 files, +402 / −65 LoC.

**Generalisable pattern unlocked by Phase 2 (Phase B precedent):**
> caller-known optimizations выносим как opt-in helpers, не вшиваем в shared dispatch

Embedded versions cascade through LLVM inline budget and regress
adjacent benches; opt-in helpers decouple the hot path from the
specialisation. This pattern shapes the next sequential phases.

**Constraints (non-negotiable):**
- Stable Rust 1.95+ (no nightly)
- `#![forbid(unsafe_code)]` preserved everywhere
- Cross-platform (no Linux-only, no Apple-Silicon-only, no system-deps)
- ABSOLUTE SAFETY (CREDO §1) — every change tier-stable or tier-elevating
- Per CREDO §96a: ASM-diff + bench-stable evidence on every perf change

**Execution model — sequential architect-cycle per phase** (refined
2026-05-08 by principal: "несколько изменений тяжело будет понять
выигрышь от каждого. тогда давай последовательно"). Each phase:
implement → test+clippy → bench-stable measure-vs-prior-baseline →
report → principal review → commit. **NO parallel architect dispatching**
(prevents perf-attribution mixing).

**Breaking API acceptable** if "архитектурно лучше, красивее, главное
стабильнее и эффективнее" (principal directive 2026-05-08). v1.0 is
the right window for breaking churn.

#### DEF-270 — Level D tier-1 hardening cluster (split: Phase 1 ship-ready, Phase 2 follows)

**Principal directive 2026-05-09 (verbatim):** «никогда не допускай того,
что если там перепутать или забыть что-то, то это риск для памяти,
утечек, поломок, сбоев и паник — это значит не tier-1, это самое худшее,
когда "задокументированно, чтобы не забыть" или около того — человеческий
фактор случится рано или поздно со временем и ростом сложности проекта,
поэтому надо автономность и устойчивость обеспечить!». Original ambition:
all five letters one-commit. **Refined plan 2026-05-09 (mid-implementation
principal call):** split into two atomic commits — **Phase 1 (U +
R-rephrased) ships now**, **Phase 2 (N-D + P-ordering) follows
separately**. Rationale: Phase 1 closes 2 high-value glass surfaces
already; Phase 2's larger structural surgery (signature changes across
7 `compute_push_*_idle_only` helpers + per-command witness types +
`StagedActions<Phase>` typestate) gets its own design-validated
commit. Each phase is atomic; cluster ships in two clean reviewable
commits rather than one unreviewable one.

**Architect adversarial audit 2026-05-09** (post-DEF-269 v2 ship)
reshaped principal's original M+P+N+R+U into a tighter glass-free
cluster: **N-D + P-ordering + U + R-rephrased**. M dropped (perf
premise dead post-T; per-cmd `const CAP` structurally inexpressible
for `BindExecute<P>`); P narrowed to frame-ordering only (P-1 + P-2
already tier-1 via existing `BrandedWriteReserved` + typed builders);
R rephrased to slot-write witness (variant-move conflicts with
`ProtoState == 80 B` size pin and DEF-189 per-row-hot-path).

#### DEF-270 Phase 1 (SHIP-READY, 2026-05-09): U + R-rephrased

**Letters in Phase 1:**
- **U-external** + **U-internal**: `ReplyId::from_raw` demoted to
  `pub(crate)`; new public mint API
  `PgProtocol::next_reply_id<K: ReplyKind>(&mut self) -> ReplyId<K>`
  (static `AtomicU64` saturating-fetch_add — globally unique IDs
  across all `PgProtocol` instances).
- **R-rephrased**: `crate::schema_slot` module with `SchemaParkedSlot<'a>`
  ZST witness + `SchemaWriteAuth` sealed trait + per-host-module auth
  tags (`AtBindExecuteSelectTransition` / `AtClearSessionResidue` in
  `mod protocol`; `AtRowDescriptionDispatch` in `mod dispatch`). Each
  tag's `new()` is `pub(in <host_module>)` — cross-module callers
  cannot mint another module's tag.

**Implementation footprint:**
- New module `crates/bsql-pg-proto/src/schema_slot.rs` (~150 LoC).
- New `pub fn PgProtocol::next_reply_id<K>` + new field
  static `AtomicU64` mint counter (mod-private inside `next_reply_id`);
  PgProtocol size pin **preserved at 520 B** — bisect 2026-05-09 proved
  that an inline `u64` field would grow the struct 520 → 528 B and
  shift LLVM whole-crate codegen heuristic +6% on the synthetic
  `column_decode/iter_10cols` decode bench. Static-atomic avoids
  the size-shift AND strengthens the invariant (globally unique vs
  per-instance).
- New auth tags hosted in `mod protocol` (2 tags) + `mod dispatch`
  (1 tag).
- `ReplyId::from_raw` visibility demoted; doc updated.
- 5 internal lib write sites for `row_desc_slot` migrated to
  witness pattern (3 in dispatch, 2 in protocol).
- 109 `from_raw` call sites flipped: 79 lib-internal stay on
  `pub(crate) from_raw` (no test churn); 30 external test/bench
  sites flipped to `proto.next_reply_id` via `mint_reply` helper
  in `tests/common/mod.rs`.

**Tier-1 closures delivered:**
| Surface | Tier delta |
|---------|-----------|
| ReplyId external fabrication | tier-3 → **tier-1 by-visibility** (`from_raw` `pub(crate)`; `mem::transmute` blocked by `#![forbid(unsafe_code)]`) |
| ReplyId cross-instance monotonicity | tier-3 → **tier-2 by atomic-fetch_add** (globally unique across all `PgProtocol` instances + threads; linear-types ceiling for tier-1) |
| `row_desc_slot` cross-module write provenance | tier-3 → **tier-1 by-construction** (per-module `pub(in ...)` seal on auth tags) |
| `row_desc_slot` external write | tier-3 → **tier-1 by-visibility** (field private to `mod protocol`) |

**Glass residues remaining post-Phase 1 (closed by Phase 2 or stable-Rust irreducible):**
- ReplyId duplicate-prevention beyond single-mint: needs linear types (stable-Rust ceiling).
- `row_desc_slot` in-module write provenance (mod dispatch / mod protocol): tier-2 by-discipline (any function in the host module could mint the tag and write). Phase 2 N-D folds the BindExecute SELECT install into `BindExecutePostInstall` witness — closes that mod-protocol residue. Phase 2 dispatch tightening could narrow the dispatch tag to a sub-module if needed.
- State-transition ↔ command-kind drift, frame-ordering Bind→Execute→Sync drift: closed in Phase 2 (N-D + P-ordering).

**Acceptance gate (Phase 1):**
- 417/0/10 tests green (lib + integration + benches compile clean).
- clippy clean across `--tests --benches`.
- `#![forbid(unsafe_code)]` preserved.
- bench-stable comparison vs `b38446d` baseline: no regression > ±2% noise.
- PgProtocol size pin **preserved at 520 B exact** (mint counter is a static AtomicU64, not a struct field — see U-letter docs for the bisect rationale).

#### DEF-270 Phase 2 (SHIPPED 2026-05-10): N-D + P-ordering

**Letters in Phase 2:**

| Letter | Invariant closed | Mechanism (compile-enforced) | Tier delta |
|--------|------------------|------------------------------|-----------|
| **N-D** | State transition ↔ command kind drift | Sealed `StateSetter<W>` is the **only** path to mutate `ProtoState` from `execute()` (raw `&mut ProtoState` held privately inside `push_command_internal`, never handed to `execute`). `install_post_state(proof: W)` consumes setter + witness; trait associates `type PostState: PostStateProof` per impl; per-command witness types carry the data the matching state variant requires (`PingAwaitingRfqInstall { reply: ReplyId<PingKind> }` — no path to install Ping post-state without the Ping reply). Trust/SCRAM/Cleartext/MD5 split surfaces honestly as a sealed enum on `StartupPostInstall`; the kind-payload pairing is structural. SELECT/DML split surfaces on `BindExecutePostInstall`. Failure path consumes setter via `install_errored(StateErrorKind)` invoked through the rewritten `try_builder!` macro. | tier-3 → **tier-1 by-construction** |
| **P-ordering** | Pipelined frame ordering Bind→Execute→Sync drift | Typed `BindRange`/`ExecuteRange` newtypes (private to `mod protocol`) + `stage_bind_execute_sync(staged, bind, execute)` builder fn. `build_bind_message → BindRange`, `build_execute_message → ExecuteRange`. Swapping argument order at the builder = type error; the static SYNC trailer is structurally bundled with the same fn (cannot be omitted). | tier-3 → **tier-1 by-construction** |

**Implementation footprint (final, 2026-05-10):**

- **New module** `crates/bsql-pg-proto/src/state_setter.rs` (~150 LoC): sealed `PostStateProof` trait with `install_into(self, &mut ProtoState)`; `StateSetter<'a, W: PostStateProof>` with `pub(crate) new()`, `install_post_state(W)`, `install_errored(StateErrorKind)`. `_phantom: PhantomData<fn(W)>` for variance cleanness. `#[must_use]` on the setter surfaces unconsumed-setter as a build warning.
- **7 witness types** added to `push_command.rs` (alongside per-command structs): `PingAwaitingRfqInstall`, `StartupPostInstall` enum (Trust/Scram/Cleartext/Md5 mirroring `Credentials`), `SimpleQueryAwaitingFirstResponseInstall`, `ParseAwaitingParseCompleteInstall`, `DescribeStatementAwaitingParamDescInstall`, `DescribePortalAwaitingRowDescOrNoDataInstall`, `BindExecutePostInstall` enum (Dml/Select). Each with `impl PostStateProof` + `install_into` body matching the pre-Phase-2 `*state = ...` arms.
- **Trait extension**: `PushCommand::type PostState: crate::state_setter::PostStateProof` paired per impl. `execute()` signature: `state: &mut ProtoState` → `setter: StateSetter<'_, Self::PostState>`.
- **Macro rewrite**: `try_builder!($result, $setter, $reply, $staged)` consumes `$setter` via `install_errored(state_kind)` on Err. NLL conditional-move preserves setter ownership on Ok-arm continuation. Pre-Phase-2 macro debug_assert (Idle-only contract) dropped — `push_command_internal` entry-assert remains the canonical site.
- **7 production helpers** `compute_push_*_idle_only` retyped: first parameter from `state: &mut ProtoState` → `setter: StateSetter<'_, ConcreteWitness>`. `*state = ProtoState::X(...)` replaced by `setter.install_post_state(WitnessX::Y { ... })` at the tail.
- **6 #[cfg(test)] 5-arm dispatchers** updated: Idle arm constructs typed setter via `StateSetter::<ConcreteWitness>::new(state)` then forwards to the `_idle_only` helper. Other arms (Errored/Connecting/PingAwaiting/BusyQuery) emit FailReply only — no state mutation, no setter needed.
- **PgCommand backwards-compat impl deleted**: zero real call sites (audit grep `push_command(PgCommand::...)` returned only doc-comment references). `compute_push_idle_only` slow-path dispatcher deleted as dead code. `PgCommand` enum survives for the test-only 5-arm dispatchers; `use crate::command::PgCommand` is `#[cfg(test)]`-gated to avoid unused-import warnings in release.
- **P-ordering** pieces in `protocol.rs`: `struct BindRange(WriteRange);` + `struct ExecuteRange(WriteRange);` + `fn stage_bind_execute_sync(staged, bind, execute)` (`budget: 3`, emits Bind+Execute+Sync triple, argument-order pinned by the typed signature). `build_bind_message` + `build_execute_message` return the typed newtypes.
- **`push_command_internal`**: constructs `StateSetter::<C::PostState>::new(state)` and passes to `cmd.execute(setter, ...)`. The raw `&mut ProtoState` never escapes this function.

**Tier-1 closures delivered:**

| Surface | Tier delta |
|---------|-----------|
| Command-kind ↔ post-state pairing | tier-3 → **tier-1 by-construction** (witness type pinned per impl via `type PostState`; non-matching install rejected at type-check) |
| `&mut ProtoState` reachability from `execute()` | tier-3 → **tier-1 by-visibility** (raw ref private to `push_command_internal`; only consumable surface is `StateSetter::install_*`) |
| Bind→Execute frame ordering | tier-3 → **tier-1 by-construction** (typed newtypes + single-call builder; swap = type error) |
| Sync trailer omission on Bind+Execute path | tier-3 → **tier-1 by-construction** (`stage_bind_execute_sync` bundles all three; cannot stage Bind+Execute without Sync) |

**Glass residues remaining post-Phase 2:**

- **ReplyId duplicate-prevention beyond single-mint**: needs linear types (stable-Rust ceiling) — closed at a different door if at all.
- **`row_desc_slot` in-module write provenance** (mod protocol): tier-2 by-discipline for the BindExecute SELECT install path. Phase 2 chose NOT to fold `row_desc` into `BindExecutePostInstall::Select` because the fold requires GAT-driven `Aux` machinery on `PostStateProof::install_into`, complicating the trait surface for marginal closure value. The auth tag `AtBindExecuteSelectTransition` is `pub(in crate::protocol)` so only `mod protocol` can mint — the residue is "any function in mod protocol can write" rather than "any in-crate caller can write". Acceptable per principal directive «не стеклянная архитектура».
- **`mem::transmute` paths**: out of scope under `#![forbid(unsafe_code)]`.
- **`MAX_PARAMS_DATA_TOTAL` runtime drift** between user `ParamsWriter` impls and the cap: existing `BuilderCapacityOverflow` classifier; closure requires specialisation (unstable).

**Acceptance gate (Phase 2):**

- ✅ 417/0/10 tests green (lib + integration + benches compile clean).
- ✅ clippy clean across `--tests --benches` with `-D warnings`.
- ✅ `#![forbid(unsafe_code)]` preserved.
- ✅ bench-stable compare vs `044a2fc-phase1-shipped` baseline: 11 unchanged, 0 improvements, 0 regressions. Hot-paths (`push_command/ping`, `ping_round_trip/push_then_feed`, `iter_rows_via_*`) all "No change in performance detected" or "Change within noise threshold" (max swing −1.02% / +0.20%). LLVM erases ZST witnesses as architect predicted.
- ✅ PgProtocol size pin **preserved at 520 B exact** (no inline fields added in Phase 2).
- ⚠ `trybuild` compile_fail assets NOT shipped — pinning the type-error surfaces (wrong PostState; BindRange/ExecuteRange swap) is mechanical and can ship in a follow-up if regression vigilance wants it. The Phase 2 ship-gate omits this asset because the type system surfaces the closures at every existing call site already; trybuild would be belt-and-suspenders.

**Architect anchors (verbatim from the original 2026-05-09 cluster audit, preserved post-ship):**

1. "Sealed `StateSetter<W>` is the only path to mutate `ProtoState` from `execute()`" — DELIVERED. Raw `&mut ProtoState` lives privately inside `push_command_internal`; `execute()` receives a setter parameterised on the impl's `Self::PostState`.
2. "Per-command witness types carry the data the matching state variant requires" — DELIVERED. 7 witness types, each carrying exactly the fields the matching `ProtoState` variant needs. Multi-variant Startup + BindExecute as sealed enums.
3. "Typed `BindRange`/`ExecuteRange` newtypes + single-call builder" — DELIVERED. Argument-order swap at `stage_bind_execute_sync` = type error.
4. "Bench expectation ±0% (LLVM erases ZST witnesses)" — VERIFIED. 11/11 benches "No change" / within noise threshold. Architect prediction held.

**Why M was dropped (preserve archeology — applies to whole cluster):**

1. **Perf premise evaporated.** T (DEF-269 v2) already delivered `push_command/ping` **−83.4%** (≈55 ns → 9.08 ns) — 4× better than architect predicted. The 2176-B argument-move cost M was attacking is gone.
2. **Per-command `const CAP` structurally inexpressible** for `BindExecute<P: ParamsWriter>`. `P` is a runtime-trait generic; `MAX_PARAMS_DATA_TOTAL` is `pub const` but `P` itself does not expose a const upper bound at the trait level. Per-instantiation CAP requires specialisation (unstable).
3. **Glass risk introduced.** A future contributor adding a new command picks `CAP: 5` (Ping precedent) for a frame that grows to 64 B → runtime stack-overflow / runtime-`SendListFull`. **Tier-4 wrapped in compile syntax** — exactly the "documented to remember" failure mode the principal directive bans.
4. The `MAX_OWNED_SEND_LEN = 2176` global cap already serves the worst-case bound for all push paths; SendList<global-cap> equals current WriteBuf (no win).

**Why R-as-framed (variant-move) was rejected (R-rephrased shipped in Phase 1):**

1. **`ProtoState == 80 B` size pin breaks.** Currently dominant variant `DescribeStatementAwaitingRfq` = `ReplyId(8) + ParamOids(68) + disc(1) + pad(3) = 80`. Adding 140 B `RowDesc` to 7 schema-bearing variants pushes ProtoState dominant to ≈210-215 B — **2.6× growth** on a hot-path object inlined in `PgProtocol`.
2. **DEF-189 per-row hot-path regression.** `current_row_desc()` is a single `Option` projection today (~1 ns). Field-in-variant requires a state pattern-match per row (~3-5 ns) — adversely material on 100-row SELECTs (×100 multiplier = 200-400 ns/query).
3. **Conflicts with N-D StateSetter shape.** Each schema-bearing variant would need a different `PostStateProof::install` signature; the setter type would have to be enum-keyed by "schema-bearing?", inflating the seal.

R-rephrased delivers the same tier closure (write-provenance tier-1) without the size/perf/setter conflicts. SHIPPED in Phase 1.

**Glass residues remaining post-cluster (irreducible in stable safe Rust):**

- ReplyId duplicate-prevention beyond single-mint: needs linear types (stable-Rust ceiling).
- `mem::transmute` paths to fabricate any sealed type: out of scope; project is `#![forbid(unsafe_code)]` — closed at a different door.
- `MAX_PARAMS_DATA_TOTAL` runtime drift between user `ParamsWriter` impls and the cap: existing `BuilderCapacityOverflow` classifier; closure path requires specialisation (unstable).

#### DEF-271 (SHIPPED 2026-05-10): Phase 3 audit-residue closure (clusters A/B/C/D)

Architect deep-audit (2026-05-09 cycle, post-DEF-270 Phase 2 ship) identified 4 residue surfaces beyond the U/R-rephrased/N-D/P-ordering letters. Each shipped as its own commit + audit anchor.

| Cluster | Commit | Closure | Mechanism (compile-enforced) |
|---------|--------|---------|------------------------------|
| **A** feed-side parity | `6cdc0ea` | feed-side state-mutation drift | `FeedStateSetter<'a>` mirror of push-side `StateSetter`; `drain_and_install_errored` is atomic `mem::replace` returning `#[must_use] Option<NonZeroU64>` (drain + install in one step; previously two separate observation points). `StateSetter::new` gained an `IdleStateProof` parameter so the Idle precondition rides the type at the mint site (was a `debug_assert!` only). |
| **B** session_params write provenance | `c34e52f` | `SessionParams` cross-module mutation drift | Sealed `SessionParamsWriteAuth` trait (mirror of `SchemaWriteAuth`) + `SessionParamsSlot<'a>` `must_use` ZST witness wrapping `&'a mut SessionParams`. 3 mutation sites (`ParameterStatus` admit, `NoticeResponse` admit, residue clear) consolidated behind the witness; each leaf-private auth tag (`AtParameterStatusFrame`, `AtNoticeResponseFrame`, `AtClearSessionResidue` dual-impl with `SchemaWriteAuth`). |
| **C** auth-tag leaf-scope tightening | `78c077d` | tag mint scope `pub(in mod)` (~5K-LoC) → leaf-submodule (~10-30 LoC) | All schema_slot + session_params auth tags moved into per-call-site leaf submodules (`_bind_execute_select_install_leaf`, `_clear_residue_leaf`, `_parameter_status_admit_leaf`, `_notice_response_admit_leaf`, `_row_description_dispatch_leaf`). Each tag has a private tuple-struct field; `Self(())` mints are callable ONLY inside the defining leaf. The shared `SchemaParkedSlot::from_field_with_auth` + `SessionParamsSlot::from_field_with_auth` constructors stay `pub(crate)`; the closure boundary is the tag-mint surface. |
| **D** ReplyId saturation classifier | `4f01136` | `next_reply_id` post-saturation cycle | `next_reply_id` detects `raw_old == u64::MAX` and routes through a new `#[cold] #[inline(never)]` `install_errored_replyid_saturation` helper that transitions the affected `PgProtocol` to `Errored(ReplyIdSaturation)` via `FeedStateSetter::drain_and_install_errored`. New `CrateBugLocus::ReplyIdSaturation` variant with kebab-case Display "reply-id-saturation". Pre-DEF-271 the saturation point silently returned the duplicate ID; post-DEF-271 the next push fails as `ConnectionAlreadyClosed { prior_kind: ReplyIdSaturation }` so the duplicate never reaches the server in a usable state. |

**Acceptance gate (DEF-271):**

- ✅ 159/0/10 tests green (lib unit + 10 doctest pin assertions).
- ✅ clippy clean across `--tests --benches` with `-D warnings`.
- ✅ `#![forbid(unsafe_code)]` preserved.
- ✅ Architect audit verified each cluster individually (cluster C audit caught 3 stale doc references + nit on tag visibility tightening — all addressed in cluster C commit).

**Glass residues identified by DEF-271 audit (closed by DEF-272):**

The DEF-271 closures landed correctly but exposed a structural ceiling: the sealed-trait pattern (`SchemaWriteAuth`, `SessionParamsWriteAuth`, `PostStateProof`) closes the EXTERNAL crate boundary perfectly but is **tier-2 by-discipline within-crate** — any in-crate file can write `impl Sealed for HostileTag + impl <TraitName> for HostileTag` and bypass `from_field_with_auth`. Architect verified empirically (2026-05-10) by appending a hostile probe to `lib.rs` — `cargo check --tests` accepted the bypass. `IdleStateProof::new()` had the same shape (any in-crate caller could mint a proof regardless of state, then pair with non-Idle `&mut state`). `FeedStateSetter::new` similarly ungated. DEF-272 closes these.

#### DEF-272 (SHIPPED 2026-05-10): within-crate tier-1 closure via concrete-tokens + Cell newtypes + lifetime-typestate

Architect-driven follow-up to DEF-271. Replaces the sealed-trait + auth-tag pattern with three orthogonal mechanisms, each tier-1 within-crate by-construction:

1. **Concrete-type per-leaf tokens** replace sealed-trait impls. Each leaf submodule hosts a `pub(crate) struct XToken(())` with a PRIVATE tuple-struct field — `Self(())` mints are callable ONLY inside the defining leaf. Witness methods take the concrete token type by value; there is no trait surface to `impl HostileTrait for HostileType` and bypass.
2. **Cell newtypes** wrap the slot fields (`Option<RowDesc>`, `Option<Box<SessionParams>>`) with PRIVATE inner field. Direct `*self.row_desc_slot.inner = X` from outside `mod schema_slot` does not compile; the only paths to mutate are token-gated methods on the cell.
3. **Lifetime-bound typestates** replace ZST proofs (`IdleStateProof`). `IdleState<'a>(&'a mut ProtoState)` IS the state borrow + the Idle proof, inseparable; `try_from(&mut state) -> Option<Self>` performs the runtime Idle classification, returning `None` for non-Idle states. Pairing the proof with a different state is impossible by lifetime ownership.

| Subcluster | Commit | Closure |
|------------|--------|---------|
| **α** schema_slot | `e723f63` | `RowDescSlotCell` newtype (private inner) + 3 per-leaf concrete tokens (`BeSelectToken`, `TDispatchToken`, `ClearResidueSchemaToken`) + token-gated `park_at_*` / `clear_at_*` methods. Deletes `SchemaWriteAuth` trait + `mod sealed` + `SchemaParkedSlot` witness. |
| **β** session_params_slot | `8d19558` | `SessionParamsCell` newtype (private inner) + 3 per-leaf concrete tokens (`ParamStatusToken`, `NoticeResponseToken`, `ClearResidueSessionToken`) + `admit_at_param_status` / `admit_at_notice_response` / `clear_at_residue`. `record_param_status_with_slot` + `session_params_or_init` deleted (logic absorbed into Cell methods; lazy-init via `get_or_insert_with` private to the cell). |
| **γ** IdleState typestate | `29e1e70` | `IdleState<'a>` lifetime-bound typestate replaces `IdleStateProof`. `StateSetter::new` now `pub(in crate::state_setter)`; `IdleState::into_setter()` is the SOLE legitimate path to a setter. `push_command_internal` minds `IdleState::try_from(&mut self.state)` internally; the `None` arm classifies via new `CrateBugLocus::PushCommandInternalNonIdle` (architecturally unreachable on production callers — `ReadyGuard::push_command` upstream classifies). `IdleStateProof` + `_IdleProofMarker` + `idle_state_proof_seal_tests` deleted from `mod guard`. |
| **δ** FeedStateSetter mint scope | `678801c` | `FeedStateSetter::new` becomes `pub(in crate::state_setter)`. 4 per-call-site free fns in `mod state_setter` (`drain_at_replyid_saturation`, `drain_at_read_cursor_advance`, `drain_at_malformed_data_row`, `drain_at_fail_inflight_no_readbuf`) take a concrete-type token from the matching leaf submodule in `mod protocol`. 4 leaf submodules host their tokens (`ReplyIdSaturationToken`, `ReadCursorAdvanceToken`, `MalformedDataRowToken`, `FailInflightNoReadbufToken`) with private fields. Adding a 5th call site requires (a) new leaf, (b) new token type, (c) new `drain_at_*` free fn — all surface in the same review hunk. |
| **ε** doc-honesty pass | `fea5811` | Stale references to deleted/renamed types (`SchemaParkedSlot`, `SessionParamsSlot`, `IdleStateProof`, `record_param_status_with_slot`, `FeedStateSetter::new` ungated) updated in current-code comments + module docs to reflect post-DEF-272 architecture. Pre-α/β/γ/δ historical context (e.g., "Pre-α the seal was tier-2 by-discipline...") preserved as migration documentation. |

**Architect empirical hostile-probe verification (2026-05-10, cluster α — initial ship):**

Architect appended `mod hostile_bypass_smoke { ... }` to `lib.rs`, ran `cargo check --tests`, removed the probe (working tree clean). Six hostile shapes tested:

| Probe | Outcome | Reason |
|-------|---------|--------|
| **P1** `cell.inner = Some(...)` from outside `mod schema_slot` | **FAIL** ✓ | E0616: field private |
| **P2** `BeSelectToken(())` mint from outside leaf | **FAIL** ✓ | E0603: tuple-struct constructor private |
| **P3** `park_at_be_select(desc, X)` with `X != BeSelectToken` | **FAIL** ✓ | E0308: type mismatch |
| **P4** `impl SchemaWriteAuth for H {}` | **FAIL** ✓ | E0405: trait deleted in α |
| **P5** `SchemaParkedSlot::from_field_with_auth(...)` | **FAIL** ✓ | E0433: type deleted in α |
| **P6** `*cell = RowDescSlotCell::EMPTY` (wholesale replacement) | **COMPILE** (later closed — see post-ship audit below) | initially documented as Rust-fundamental — **incorrect** |

5 of 6 hostile shapes were compile-errors-by-construction. P6 was initially marked Rust-fundamental — that was wrong (post-ship probe + closure below).

**Post-ship audit (2026-05-10): β/γ/δ verification + β2 wholesale-replacement closure**

A second architect probe round verified β/γ/δ empirically (previously claimed tier-1 by structural mirror to α only) and re-tested α-class wholesale (P6) with cross-cluster context.

| Probe | Outcome | Reason |
|-------|---------|--------|
| **β1** `cell.inner = None` from outside `mod session_params_slot` | **FAIL** ✓ | E0616: field private |
| **β2** `*cell = SessionParamsCell::EMPTY` (wholesale replacement) | **COMPILE — REAL HOLE** (closed by follow-up below) | `pub(crate) const EMPTY` exposed crate-wide |
| **β3** `SessionParamsCell { inner: ... }` struct-literal | **FAIL** ✓ | E0451: field private |
| **β4–β7** various cross-leaf bypass attempts | **FAIL** ✓ | E0603 / E0616 |
| **γ1** `IdleState { state }` struct-literal outside `mod state_setter` | **FAIL** ✓ | E0451: field private |
| **γ2** cross-state pairing via `idle.state` read | **FAIL** ✓ | E0616: field private |
| **γ3** `StateSetter::new` outside `mod state_setter` | **FAIL** ✓ | E0624: associated fn private |
| **δ1–δ2** `StateSetter::new` / `FeedStateSetter::new` outside leaf | **FAIL** ✓ | E0624 |
| **δ3** `pub use … as Reexport; Reexport::new(...)` | **FAIL** ✓ | E0624 — `pub use` cannot widen `pub(in crate::state_setter)` |

12/13 confirmed-rejected. β2 (and α-equivalent P6) was the real hole: `pub(crate) const EMPTY` allowed any in-crate file to construct a fresh Cell value, then `*existing_cell = fresh_cell` performed wholesale-replacement bypassing token-gated methods. (Field privacy on `pg.row_desc_slot` / `pg.session_params` blocks the EXTERNAL-to-`mod protocol` exploit chain; the residual was within `mod protocol` cross-leaf wholesale.) **Initial DEF-272 documented P6 as "Rust-fundamental" — that framing was incorrect.**

**β2 / α-P6 closure shipped (2026-05-10): leaf-token P6 pattern**

Architect's proposed `pub(in crate::protocol) const EMPTY` failed E0742 (visibility scope must be ancestor; `mod schema_slot` / `mod session_params_slot` are siblings of `mod protocol`, not children). The proper closure mirrors DEF-272 cluster δ:

- New `_proto_init_leaf` submodule inside `mod protocol`. Hosts `ProtoInitToken` (`pub(crate) struct ProtoInitToken(())` — private tuple-struct field gates construction) and the moved `impl PgProtocol::new` block.
- Cell modules expose `pub(crate) const fn empty(_token: ProtoInitToken) -> Self` instead of `pub(crate) const EMPTY: Self`. Fresh cell construction requires a token by value.
- `ProtoInitToken::mint()` is private (no `pub`) — only callable inside `_proto_init_leaf`.
- `PgProtocol::new` lives inside `_proto_init_leaf`, so it has the only legitimate mint capability.

**Effect**: wholesale-replacement (`*pg.row_desc_slot = RowDescSlotCell::empty(token)`) now requires a `ProtoInitToken`, which can only be minted inside `_proto_init_leaf`. Cross-module + within-`mod protocol`-but-outside-leaf wholesale attempts cannot construct fresh cells. Tier-1 by-construction within-crate; residue is the ~30-LoC `_proto_init_leaf` itself (Rust-fundamental at this minimal scope; see DEF-273 for closure path).

**Verification**: `cargo check --tests --all-targets` clean; `cargo test --lib` 158 passed / 0 failed; `cargo clippy --tests --benches -D warnings` clean; `forbid(unsafe_code)` preserved; `bench-stable compare def272-shipped` **PASS** — 0 regressions, 0 improvements, all 17 benches within criterion's 5% noise threshold (max swing ≈+1.8% on `column_decode/iter_5cols_decode_text_long_ascii`, unrelated to the changed code path — LLVM whole-crate-codegen heuristic noise). Architect prediction held: ZST `ProtoInitToken` + private const fn `mint` erase to nothing.

**Methodology lesson**: initial single-pass smoke can suppress sibling-class errors via cascade (E0451 once silences subsequent identical-class probes). β3 / γ1 falsely appeared to pass in the 13-probe single run; isolated re-runs revealed proper rejection. Future probes isolate per-class **and** re-run any masked probes solo.

**Tier-1 closures delivered (DEF-271 + DEF-272 combined):**

| Surface | Tier delta |
|---------|-----------|
| `row_desc_slot` external write | tier-3 → **tier-1 by-visibility** (cell `pub(crate)`, field private) |
| `row_desc_slot` cross-module write provenance | tier-3 → **tier-1 by-construction** (per-leaf concrete tokens; field private to `mod schema_slot`) |
| `row_desc_slot` within-`mod protocol` write provenance | tier-2 → **tier-1 by-construction** (cell methods only; no `*self.row_desc_slot.inner = X` path) |
| `session_params` external write | tier-3 → **tier-1 by-visibility** |
| `session_params` cross-module / within-crate write provenance | tier-3 → **tier-1 by-construction** (per-leaf concrete tokens; lazy-init private to cell methods) |
| `StateSetter::new` mint scope | tier-2 → **tier-1 by-construction** (`pub(in crate::state_setter)`; sole path is `IdleState::into_setter`) |
| `ProtoState == Idle` precondition at push entry | tier-2 (debug_assert) → **tier-1 by-construction** (lifetime-bound typestate; `try_from` runtime classification) |
| `FeedStateSetter::new` mint scope | tier-2 → **tier-1 by-construction** (`pub(in crate::state_setter)`; 4 named entry points each gated by per-leaf concrete token) |
| Sealed-trait hostile in-crate impl bypass | OPEN (architect-empirically verified) → **CLOSED** (no trait surface remains; concrete-type tokens are sealed by Rust's nominal type system + private-field mints) |

**Glass residues remaining post-DEF-272 (Rust-fundamental or out-of-scope):**

- **Wholesale Cell replacement within `_proto_init_leaf` (~30 LoC submodule)**: code editing this submodule can mint `ProtoInitToken` and produce fresh cells, enabling `*pg.row_desc_slot = RowDescSlotCell::empty(token)`. Acceptable given the leaf's single-purpose scope (only PgProtocol::new lives inside it). **Tier-1 closure path: DEF-273 in queue** (Pin / brand-lifetime / `OnceLock`-gated single-mint — closes the leaf residue itself; low priority post the leaf-token follow-up since residue is 30 LoC).
- **Reviewer-attention path for `mod schema_slot` / `mod session_params_slot` / `mod state_setter` internal edits**: a contributor editing those modules directly can write hostile patterns that bypass the closure (since the modules own the structures). The visibility math shrinks the within-crate audit surface from "all of `bsql-pg-proto`" (~10K LoC) to specific files (<200 LoC each), but does not close it. **Tier-1 closure path: DEF-274 in queue** (cell-module physical separation into a sibling sub-crate so `pub(crate)` of the inner field becomes a real cross-crate wall).
- **`mem::transmute` paths**: out of scope under `#![forbid(unsafe_code)]`.

**Acceptance gate (DEF-271 + DEF-272):**

- ✅ 159/0/10 tests green (lib unit + 10 doctest pin assertions; same counts as pre-DEF-271).
- ✅ clippy clean across `--tests --benches` with `-D warnings` (lib forbids `expect_used`, `unwrap_used`, `panic`, `unreachable` — DEF-272 cluster γ's None-arm handling uses `match` + `debug_assert!(false)` + classified return per the forbid bundle).
- ✅ `#![forbid(unsafe_code)]` preserved.
- ✅ PgProtocol size pin **preserved at 520 B exact** (cells are `#[repr(transparent)]`; layout unchanged).
- ✅ bench-stable compare vs `643d729-def270-shipped` baseline: **0 regressions, 2 real improvements, rest within noise**. Full table:

| Bench | Time | Change | Classification |
|-------|------|--------|---------------|
| `parse_header/rfq_header` | 2.51 ns | -3.9% to -5.0% | within noise |
| `ping_round_trip/push_then_feed` | 72 ns | -3.7% to -4.7% | within noise |
| `iter_rows_via_next_event/pull_100_rows` | 1.60 µs | **-5.5% to -7.1%** | **PERFORMANCE IMPROVED** |
| `iter_rows_via_next_row/pull_100_rows` | 1.43 µs | **-5.5% to -7.1%** | **PERFORMANCE IMPROVED** |
| `iter_rows_via_next_row_bytes/pull_100_rows` | 1.43 µs | -2.8% to +0.16% | no change detected |
| `iter_rows_via_consume_batch/pull_100_rows_8` | 1.46 µs | -1.5% to +0.07% | no change detected |
| `iter_rows_via_for_each/pull_100_rows` | 1.44 µs | -3.5% to -1.7% | within noise |
| `push_command/ping` | 12.8 ns | +0.27% to +1.0% | within noise |
| `column_decode/data_row_parse_5cols` | 1.10 ns | +2.2% to +2.8% | within noise |
| `column_decode/iter_5cols_raw_no_decode` | 6.19 ns | -0.17% to +0.67% | no change detected |
| `column_decode/iter_5cols_decode_i32` | 32.1 ns | +0.16% to +0.94% | within noise |
| `column_decode/iter_5cols_decode_i32_common_values` | 11.09 ns | +1.24% to +1.75% | within noise |
| `column_decode/iter_5cols_decode_i32_short_4digit_via_swar` | 14.5 ns | +0.35% to +1.32% | within noise |
| `column_decode/iter_5cols_decode_text_short_ascii` | 43.9 ns | +1.07% to +2.30% | within noise |
| `column_decode/iter_5cols_decode_text_long_ascii` | 24.5 ns | +0.46% to +0.91% | within noise |
| `column_decode/iter_5cols_decode_text_cyrillic` | 79.2 ns | -0.27% to +0.52% | no change detected |
| `column_decode/iter_10cols_alternating_null_i32` | 35.6 ns | +0.32% to +1.02% | within noise |

**Bench summary**: 17 benches, 4 "no change", 2 "improved" (+5-7% on row-stream pull paths), 11 "within noise threshold" (max +2.8% / -5.0%; criterion's noise floor is ±5% on consumer hardware). Architect prediction held: LLVM erases the cell newtypes (`#[repr(transparent)]`) and concrete-type tokens (ZST private fields). The 2 real improvements on row-stream paths likely stem from the cell newtype helping LLVM unify the read/write code paths through stricter borrow patterns — a perf side-effect of the closure tightening, not the goal.

#### DEF-160 (SHIPPED 2026-05-11): zero-copy borrowed-SQL push API (Z2)

DEF-218 colossal-data audit identified `MAX_SQL_LEN = 2048` as the highest-severity silent-corruption hazard in the crate: `Sql::from_str_truncating` accepted any `&str`, padded oversize input to a `…` marker, and the truncated query went on the wire as-is — server ran it, returned "results" for the wrong query, no error surfaced anywhere. Two architect cycles considered alternatives (cap-bump, const-generic) and rejected both as "fallback patterns that push the limit but do not eliminate it"; (Z2) eliminates the limit structurally.

**Mechanism (Z2 — zero-copy SQL via scatter-gather chunks)**:

1. **`StagedAction::SendBytesBorrowed(&'sql [u8])` variant** (commit 1, `ec6686b`) added alongside the existing range/static variants. The `'sql` lifetime is unified with `'w` (WriteBuf borrow) inside `materialise`'s signature — `&'sql [u8]` coerces to `Action::SendBytes(&'w [u8])` via `'sql: 'w` subtyping.
2. **`Parse<'a> { sql: &'a str }` + `SimpleQuery<'a> { sql: &'a str }`** (commit 2) replace the owned `Sql` fields. `PushCommand::execute<'sql>(... where Self: 'sql)` threads the caller's lifetime through to the staged borrow. Per-command struct sizes drop from `MAX_SQL_LEN + header (≈2120 B)` to `≤128 B` (Parse) / `≤64 B` (SimpleQuery).
3. **`compute_push_simple_query_idle_only` + `compute_push_parse_idle_only`** emit 3-4 staged actions: header range (in `wb`) + borrowed SQL (zero-copy) + trailer range (in `wb`) + Sync (`SendBytesStatic`, Parse only). Under `writev` / `IoSlice` these collapse to a single socket syscall.
4. **`push_command_internal` + `ReadyGuard::push_command` return `Result<OutActions<'w, 'static>, PushFailure>`** (was `Result<(), PushFailure>` per DEF-212). Caller drains chunks via `for action in out_actions { ... }`. The pre-DEF-212 `OutActions` shape returns — same 800 B per-call return frame as pre-(212), but now load-bearing because SQL chunks span CALLER memory, not just `wb`.
5. **`materialise_push` deleted**. Pre-Z2 it verified staged ranges against `wb` and appended `SendBytesStatic` (Sync) into `wb`. Post-Z2 push and feed paths unify through `materialise` (`terminal_row_desc: None` for push — push never emits `DeliverReply`). `BrandedWriteReserved::as_bytes` (orphan after `materialise_push` deletion) also deleted.

**Tier impact**:

| Surface | Pre-DEF-160 | Post-DEF-160 |
|---------|-------------|--------------|
| SQL > 2 KB on push (Parse / SimpleQuery) | **tier-4 silent semantic corruption** — server runs truncated query, returns wrong results, no error path | **tier-1 by-construction** — `&str` is unbounded, no cap, no truncation, no `…` marker; SQL length naturally limited only by `u32::MAX` PG-wire length field |
| SQL → wb memcpy on every push | Always paid (MAX_SQL_LEN copy into WriteBuf) | Eliminated (`SendBytesBorrowed` references caller's `&'a str` directly) |
| `Parse` / `SimpleQuery` struct sizes | ~2120 B / ~2054 B (inline `Sql` cap) | ≤128 B / ≤64 B (header pointers only) |
| Caller-side socket I/O shape | Single `write` of `wb.as_bytes()` | `writev` / IoSlice over per-chunk `OutActions`; same syscall count under vectored I/O |

**Tests**:

`tests/common/mod.rs::actions_to_scratch` drains `OutActions` chunks into a heap scratch and rebuilds `wb` with the concatenated wire frame — preserving the pre-DEF-160 test invariant `wb.as_bytes() == on-wire frame` so existing fixture assertions (`split_frame_plus_sync`, `split_bind_execute_sync`, `parse_frame_wire_format_with_named_statement`) all pass unchanged. The rebuild costs one extra memcpy in tests only; production drives chunks directly to socket via `writev` / `IoSlice`.

**Acceptance gate (DEF-160)**:

- ✅ 157/0/10 tests green (lib unit + 10 doctest pin assertions). Lib went 158 → 157 only because `branded_reserve_as_bytes_len_mirrors_buf` was deleted alongside the orphan helper it pinned.
- ✅ clippy clean across `--tests --benches` with `-D warnings` (incl. `expect_used` forbid in tests — fixture uses explicit `match + panic` instead of `.expect`).
- ✅ `#![forbid(unsafe_code)]` preserved.
- ✅ Architect-vetted (Z2 emerged from cycle 2 after cycle 1's (Y) proposal was found to carry 6 critical infections — WriteRange size-pin break, with_length_prefix back-patch break, DEF-185 P0-B zeroize gap, Option<SplicePoint> Drop physics replay, tier-3 reintroduction).
- ✅ bench-stable compare vs `pre-def160` baseline: **0 regressions, 1 improvement, rest within noise** (17 benches). An intermediate naïve Z2 shape (two-pass: closure returns `StagedActions`, then push_command_internal scans for `FailReply`, then calls `materialise(staged, ...)`) regressed `push_command/ping` **+270%** + `ping_round_trip/push_then_feed` +46% during development. Principal-driven follow-up audit («если ... неожиданно сильно прибавили ... то это повод не откатываться ... а повод разобраться, глянуть asm») diagnosed root cause: redundant intermediate `StagedActions` closure-return (~2.8 KB stack frame) + duplicate iteration. Collapsing to **single-pass inside the `with_branded` closure** (open-coded materialise, FailReply classified inline) reversed the regression to **-7.97% vs pre-DEF-160** on `push_command/ping` and -3.18% (within noise) on `ping_round_trip/push_then_feed`. The shipped commit contains only the single-pass shape.

**Bench-stable verification table** (final, 17 benches, full suite):

`bench-stable compare pre-def160`: **0 regressions, 3 improvements, 14 within-noise**, exit code 0.

| Bench | Shipped | Δ vs pre-DEF-160 | Classification |
|-------|---------|-------------------|----------------|
| `push_command/ping` | 11.42 ns | **-9.45%** ⚡ | improvement (was the +270% naïve-two-pass casualty; single-pass overrun the pre-DEF-160 baseline) |
| `column_decode/iter_5cols_decode_i32` | 31.46 ns | **-14.93%** ⚡ | improvement (LLVM codegen rebalance from PushCommand lifetime threading + zero-copy API; not the design target, archeology only) |
| `iter_rows_via_for_each/pull_100_rows_via_for_each` | 1356.52 ns | **-7.85%** ⚡ | improvement (paired with i32 decode improvement — same root cause) |
| `column_decode/iter_5cols_raw_no_decode` | 6.06 ns | -4.26% | within noise |
| `column_decode/iter_5cols_decode_text_cyrillic` | 77.69 ns | -3.73% | within noise |
| `column_decode/iter_5cols_decode_text_short_ascii` | 43.35 ns | -3.58% | within noise |
| `column_decode/iter_5cols_decode_i32_common_values` | 10.89 ns | -3.20% | within noise |
| `ping_round_trip/push_then_feed` | 69.54 ns | -2.75% | within noise (was the +46% naïve-two-pass casualty; single-pass eliminated it) |
| `column_decode/data_row_parse_5cols` | 1.05 ns | -2.45% | within noise |
| `column_decode/iter_5cols_decode_i32_short_4digit_via_swar` | 14.24 ns | -2.20% | within noise |
| `column_decode/iter_10cols_alternating_null_i32` | 34.90 ns | -2.01% | within noise |
| `parse_header/rfq_header` | 2.50 ns | -0.65% | within noise |
| `iter_rows_via_next_event/pull_100_rows` | 1642.31 ns | +1.56% | within noise |
| `iter_rows_via_next_row_bytes/pull_100_rows_via_next_row_bytes` | 1440.43 ns | +0.44% | within noise |
| `iter_rows_via_next_row/pull_100_rows_via_next_row` | 1421.42 ns | +0.30% | within noise |
| `column_decode/iter_5cols_decode_text_long_ascii` | 24.62 ns | +0.18% | within noise |
| `iter_rows_via_consume_batch/pull_100_rows_via_consume_batch_8` | 1472.47 ns | +0.10% | within noise |

**Mitigation diagnostic (principal-driven, 2026-05-11)**:

The naïve Z2 shape had `push_command_internal` doing:

```text
let staged = with_branded(|wb| {
    let reserved = wb.reserve();
    cmd.execute(...);          ← writes header/trailer into wb, stages 3-4 SendBytesRange/Borrowed/Static
    staged                      ← returned from closure (~2.8 KB worst-case StagedActions stack frame)
});
for sa in staged.iter() { check FailReply }    ← iteration #1, by-ref
Ok(materialise(staged, ...))                    ← iteration #2, consumes staged, builds OutActions (~2.8 KB stack frame)
```

Three large stack moves (closure→fn, materialise→fn, fn→caller) + two iterations = the +270% regression.

The fix collapses everything into the closure:

```text
with_branded(|wb| -> Result<OutActions, PushFailure> {
    let mut staged = ...;
    cmd.execute(...);             ← stages
    let bytes = wb.into_bytes();  ← &'w [u8], lifetime escapes closure
    let mut out = OutActions::new();
    let mut failure = None;
    for sa in staged {             ← SINGLE iteration
        match sa { ... FailReply → failure = Some(); SendBytesRange → out.push(slice); ... }
    }
    match failure { Some → Err, None → Ok(out) }
})
```

LLVM coalesces the StagedActions stack slot with the OutActions slot (non-overlapping liveness inside the closure), so the closure's stack frame size = max(StagedActions, OutActions) ≈ 2.8 KB instead of staged + out ≈ 5.6 KB. The single iteration also eliminates one full enum-discriminant walk over 8 entries. The feed-side `materialise` keeps its broader contract (StagedAction::DeliverReply / FailReply → typed `Action` variants) for dispatcher use; push is open-coded inline.

**Lesson archived** (principal directive verbatim 2026-05-11):

> «если путь по safety хороший но мы неожиданно сильно прибавили в весе и footprint или в производительности и наносекундах, то это повод не откатываться и rollback делать, а повод разобраться, глянуть asm, измерения дополнительные может провести. и в ходе размышлений вероятно ты придешь или к прорывным практикам или к архитектуре какой-то модифицированной или вовсе альтернативный подход и путь ещё более чистый — главное результат»

**Translation / generalisation**: a regression on a safety-improving change is a signal to investigate root cause, NOT to roll back. The DEF-160 +270% regression directly yielded the single-pass push-materialise pattern; applies to any future push-path that constructs an `OutActions` end-to-end.

**Glass residues remaining post-DEF-160**:

- **`MAX_OWNED_SEND_LEN` cap-shrink**: WriteBuf cap stays at 2176 B post-Z2; the dominant non-SQL contributor is SASLResponse (~389 B) so the cap could shrink ~4× to ~389-512 B. Drift-pin asserts (`max_simple_query_message_size`, `max_parse_message_size`) still hold (2176 ≥ 2054 / ≥ 2120) but are now semantically over-conservative — they bound the full wire frame, post-Z2 only the wb-residue (header + trailer) need fit. Cap-shrink + drift-pin re-semantics deferred to a follow-up commit so the change can be bench-stable-measured independently of the API change.
- **`MAX_SQL_LEN` + `Sql` public-surface demotion**: post-Z2 external callers no longer construct `Sql` (push commands carry `&'a str`); `Sql` survives only as the cfg(test) legacy `PgCommand::SimpleQuery` enum field used by lib-internal `compute_push_tests`. Could be demoted from `pub` to `pub(crate)` in a follow-up; left untouched in this commit to keep the API change reviewable in isolation.
- **`bsql-driver-postgres` does not yet exist** — the driver wrapper that will collapse the per-chunk `OutActions` into a single `writev` / `IoSlice` syscall is Phase 1e work. Today's bench fixtures simulate the production drain pattern by concatenating chunks into a heap scratch.

#### Shipped from this queue (chronological)

| Date | DEF | Result | Commit |
|------|-----|--------|--------|
| 2026-05-08 | DEF-265 | α footprint reduction via Idea-38 (two-tier inline + lazy heap escape). PgProtocol size 4352 B → **520 B exact (-88% inline)**. ping_round_trip/push_then_feed **-36.17%**; iter_rows_via_* family -2.4 to -10.5%. Zero alloc-traffic regressions. **Generalisable insight**: lazy-escape pattern works for long-lived/append-many access (ReadBuf); same pattern on reset-heavy access (WriteBuf, DEF-268) MEASURED-REJECTED. | `9ec3ca9` |
| 2026-05-11 | DEF-160 | Zero-copy borrowed-SQL push API (Z2) + single-pass push-materialise. `Parse<'a>::sql` / `SimpleQuery<'a>::sql = &'a str`; `StagedAction::SendBytesBorrowed(&'sql [u8])` + `push_command -> Result<OutActions<'w, 'static>, PushFailure>`. **Closes DEF-218 silent-truncation hazard structurally** (was MAX_SQL_LEN=2048 cap → server ran wrong query, no error; now unbounded `&str`). Parse struct 2120 B → ≤128 B; SimpleQuery 2054 B → ≤64 B. SQL→wb memcpy eliminated. Initial commit shape regressed `push_command/ping` +270% (StagedActions return + duplicate iteration); principal-driven mitigation collapsed to single-pass inside `with_branded` closure → **-7.97% vs pre-DEF-160** (faster). Test fixtures `actions_to_scratch` rebuilds wb so pre-DEF-160 assertions still pass. | `5c5cfa4` |
| 2026-05-11 | DEF-266 | β SWAR extension — three additive opt-in helpers in `decode.rs`: (a) `parse_long_uint_swar(&[u8]) -> Option<u64>` for 5-19 digit ASCII-decimal (i64 range + u64 headroom); (b) `validate_utf8_swar(&[u8]) -> Option<()>` all-ASCII fast-path detector (caller falls back to `simdutf8::basic::from_utf8` on `None`); (c) `parse_pg_bool_swar(&[u8]) -> Option<bool>` for `b"t"`/`b"f"`/`b"true"`/`b"false"` cache-hit. All caller-routed, NEVER embedded in `from_pg_text` dispatch (DEF-250 Phase B precedent). **Implementation lesson:** initial sequential-Horner recombination across the 24-byte padded buffer (`value = value*10 + d` × 24) benched 3.5× SLOWER than generic scalar decode at the 8-digit shape (113 ns/row vs 31 ns/row on `iter_5cols_decode_i32_long_8digit_via_swar`) — the 24-instruction dependency chain prevented LLVM from parallelising. Refactored to length-class dispatch (5..=8 / 9..=16 / 17..=19) with parallel-multiply-by-place-value per branch (8/16/19 independent `wrapping_mul`s LLVM schedules in parallel + sum-tree reduction). Post-fix: 23.99 ns/row at 8-digit shape — **-78.7% vs initial Horner draft** and **~23.6% faster than generic scalar** on same shape. `validate_utf8_swar` at 17 B short-ASCII: 2.20 ns vs ~8.7 ns simdutf8 baseline = **~4× faster** on the cache-line-fit fast-path (10.38 ns on 200 B ASCII; 0.79 ns multibyte-miss). `parse_pg_bool_swar` hit cases: 0.96-1.19 ns; miss: 0.63 ns. Zero regression on existing decode-only benches (within ±5% noise band against `pre-def248` baseline). 25 new tests, 8 new bench groups, helpers re-exported via `bsql_pg_proto::*`. | `65aa59f` |
| 2026-05-12 | DEF-258 | Compile-time FormatCode×Type matrix via new sealed trait `DecodeFormat<'a, F: FormatCodeMarker>` in `decode.rs`. Markers `TextFmt`/`BinaryFmt` are ZST (zero-sized: `size_of == 0`); `FormatCodeMarker` trait carries `const WIRE: FormatCode` to bridge static markers to runtime `FormatCode` from `RowDescription`. 12 impls (6 primitive types × 2 format markers): `i16`/`i32`/`i64`/`u32`/`bool`/`&str` × `TextFmt`/`BinaryFmt`. Each impl forwards to the corresponding legacy `FromPgText` / `FromPgBinary` (no new decode paths; pure dispatch-surface refinement). 14 compile-time const-asserts pin OID consistency: 12× DecodeFormat<F>::OID === FromPg(Text\|Binary)::OID per pair + 2× FormatCodeMarker::WIRE matches FormatCode variant. Runtime helper `pub fn decode_with_format<'a, T>(bytes, fmt) -> Result<T, DecodeError>` bridges runtime FormatCode to static dispatch — requires `T: DecodeFormat<TextFmt> + DecodeFormat<BinaryFmt>`. A future Rust type implementing only one format-marker will compile-reject `decode_with_format` use; the type-level pair check closes the "is this (T, F) pair supported" classification at compile time. Additive — `FromPgText`/`FromPgBinary` traits + all existing callers untouched. 9 new tests, 5 new public exports (`DecodeFormat`, `TextFmt`, `BinaryFmt`, `FormatCodeMarker`, `decode_with_format`). | `9c54929` |
| 2026-05-12 | DEF-248 Sub-A | Pull-based per-column decode for D-tag DataRow streaming — closure-scoped `iter_rows<R, F: for<'p,'w> FnOnce(&mut RowStream<'p,'w>) -> R>` API replaces 5 deleted via_* methods (`next_event` / `next_row` / `next_row_bytes` / `consume_rows` / `for_each_row`). `ColEvent` enum (Got/Null/EndRow/Chunk/ChunkEnd/NeedMore/`EndQuery { id, outcome: Result<Reply,ProtocolError> }`). ReadBuf `partial_remaining: u32` substrate (frame-agnostic) gated by `_row_stream_partial_leaf::PartialFrameToken` (tuple-struct private field, mint `pub(in crate::row_stream)`). `RowStream::dispatch_next_frame` detects FrameTooLarge specifically for `tag == 'D' && cached_streaming_id.is_some()` and enters partial mode via the leaf-minted token; other tags retain FrameTooLarge rejection (Sub-B scope). `impl Drop for RowStream` installs `Errored(InternalCrateBug { locus: StreamDroppedMidStream })` on `!drained` via new leaf-gated `_stream_dropped_mid_stream_drain_leaf::drain` (mirror of DEF-272 cluster δ); `CrateBugLocus` grew from 8 to 9 variants but remains 1 byte (C-like, repr(u8)) — `Option<CrateBugLocus>` still niche-packs to 1 byte. **`mem::forget(RowStream)` is structurally impossible** — caller receives `&mut RowStream` from the closure, not the value; hostile probe verified: `mem::forget(*stream)` → E0507. `panic = "abort"` is an OS-level boundary (TCP RST on process death). Tier-1 hostile-bypass audit (8 probes, DEF-272 P6 methodology): P1 E0603, P2 E0423, P3 E0423, P4 E0603, P5 E0507, P5b behavioural-confirmed (forget-reference is no-op, Drop fires), P6 E0061, P8 E0624. Size pins held: PgProtocol=520, ProtoState=80, ProtocolError=72, CrateBugLocus=1, Option<CrateBugLocus>=1 (niche), RowProgress≤16 (new pin). 466/0/10 tests green (14-test row_stream_spec rewrite + new behavioural probes for drop-mid-stream + panic-Drop + 5KB partial-frame streaming). clippy `-D warnings` clean. bench-stable compare pre-def248-suba (preserved groups, filter parse_header\|ping_round_trip\|push_command\|column_decode): 2 unchanged + 7 improvements + 0 regressions. New bench groups: `iter_rows_via_col_next/pull_100_rows` 2.30 µs, `iter_10cols_large_5kb_row/pull_one_big_row` 2.14 µs (5KB partial-frame path), `iter_jsonb_1mb_streaming/stream_1mb_chunked` 373.69 µs ≈ **2.7 GiB/s throughput** on the streaming path, `col_next_per_event_cost/200_events_tight_loop` 1.83 µs ≈ 9.2 ns/event amortised. Zero new heap allocations. 10 files, +2420/-1495 LoC. | `3a55d89` |
| 2026-05-12 | DEF-248 Sub-B | **Stream-and-truncate universal coverage** for non-D oversize frames (tags T/E/N/S/A/C/R/v). Architect cycle 2's 256 KB `MAX_PARTIAL_FRAME_BYTES` cap was rejected by principal as frequency-based exclusion («если возможен — не опускаем»). Cycle 3 architect-rediscovered: every non-D parser is already inline-bounded at the type level (`SecretBoundedStr<128>`/`<96>`/`<64>` for ErrorResponse fields, `MAX_ROW_COLUMNS=32` rejection for RowDescription, `BoundedStr<32>` for CommandComplete tag, `CappedServerNonce<256>`+64 B salt for SCRAM); worst-case parser depth ≈ 4.2 KB. Algorithm: copy first `PREFIX_CAP=8192 B` into `heapless::Vec<u8, 8192>` const-generic inline buffer (overflow structurally impossible), count-and-skip bytes beyond without copying, dispatch the prefix through existing per-tag parser on completion. **Memory cost: constant 8 KB regardless of wire-declared body length 0..~2 GiB.** PREFIX_CAP is **inline-type-derived** (1.9× headroom over worst case) — NOT frequency-based. Zero public API surface (no new types or methods on PgProtocol surface; only operator-diagnostic `has_active_partial_assembly()` predicate). New module `partial_assembly.rs` (769 LoC) hosts `PartialAssemblyCell` (`#[repr(transparent)]` over `Option<Box<_>>`, 8 B niche) + per-leaf concrete-type tokens (Enter/Absorb/Take/ClearResidue) mirroring DEF-272 α/β/δ pattern. **Size pin moved 520 → 528 B** (+8 B niche; const-assert updated; no measurable bench regression in clean-load conditions). 10 tier-1 hostile-probe closures verified with E-codes (E0603/E0423/E0308/E0616). bench-stable compare post-def248-suba: 13 unchanged + 1 improvement (`push_command/ping` -5.94%) + 0 regressions. 489/0/10 tests green (+16 from new partial_assembly_spec: per-tag dispatch for all 8 streaming-eligible tags + 100 KB E-frame universal-coverage stress + lifecycle tests + within-crate seal-pin anchor). clippy `-D warnings` clean. Memory leak audit clean: every Box traced to 4 free sites (dispatch take / Idle residue / Errored residue / PgProtocol drop). New memory file `project_bsql_def248_subb_design.md` saved. 4 files, +2035 / -13 LoC. Closes universal-coverage residue completely. Open follow-up (non-blocker): const-assert PREFIX_CAP against parser-depth limits (`MAX_ERROR_FIELDS=32`, etc.) — currently lower-bound only. | `d54c697` |

#### Sequential phase queue (ordered by impact-per-cost, lowest cost first)

| Pos | DEF | Item | Expected | API impact |
|-----|-----|------|----------|-----------|
| 1 | DEF-279 | **Foundation Rethink — Phase 1a + 1b + Phase 1c prereq + Phase 1c Bundle Commits 1+2 SHIPPED (`5b5b886` + `5e6127f` + `d8b1a34` + `4cf6347` + `705412c`), Phase 1c Bundle Commit 3 (SealedPhase migration + dispatch split) pending** (memo `/tmp/def-rethink-foundation-memo.md` 1368 LoC). User chose «full bundle Phase 1c+2+3+4» path: commits land sequential each with asm-diff + bench-stable. **Commit 1 (`4cf6347`)**: per-phase state enum scaffolding — ConnectingState (12 variants), ActiveState (20 variants), ErroredState (1 variant) + From/TryFrom bijection. Additive. **Commit 2 (`705412c`)**: ConnectingInner struct definition (scaffolding) — narrowed field set (drops row_desc_slot 140 B + backend_key 12 B vs PgProtocolInner); layout target 344 B (-192 B); `#[allow(dead_code)]` justified by explicit Commit-3-target purpose. **Commit 3 (next session — multi-hour focused work ~2000-2500 LoC)**: SealedPhase migration on ConnectingPhase + dispatch split + method bodies. Specifically: (a) `impl SealedPhase for ConnectingPhase { type Inner = ConnectingInner }` — currently still `PgProtocolInner`; (b) `ConnectingState::HandshakeReady { pid, secret_key }` state-variant signal — backend-key install moves from `(PostAuthHaveKey, RFQ)` dispatch arm to `into_active` via the variant's payload (preserves Bundle D' tier elevation); (c) `connecting_dispatch` free fn in dispatch.rs operating on `&mut ConnectingState` with narrow slot params (no row_desc_slot, no backend_key_slot); (d) `feed_bytes_dispatch_connecting<BOUNDED>` + `advance_one_frame_dispatch_connecting` + `clear_session_residue_for_class_dispatch_connecting` (~700 LoC duplicated body adapted for `ConnectingInner`'s narrow field set); (e) `ConnectingState::push_class` + `take_inflight_reply_raw_id` + `unsolicited_admit` per-phase classifier mirrors; (f) manual `Debug` impl on `ConnectingInner` with `Sensitive`-redaction parity; (g) `push_startup` rebuild — `_proto_init_leaf::fresh_connecting_inner` token-gated constructor; (h) `<ConnectingPhase>::into_active` rebuild — observe `HandshakeReady`, extract `(pid, secret_key)`, build `PgProtocolInner` with `BackendKeyCell` installed via existing `_backend_key_install_leaf::install_at_dispatch_arm`; (i) `<ConnectingPhase>::into_closed_if_errored` rebuild — observe `ConnectingState::Errored`, build `ClosedInner`; (j) all 10 `<ConnectingPhase>::*` method body migrations to use `ConnectingInner`'s narrow field set; (k) size pin updates `size_of::<PgProtocol<ConnectingPhase>>() == 344` (was 536); (l) tests + asm-diff vs HEAD + bench-stable vs `def279-phase1c-prereq-baseline`. **Commit 4** (optional Phase 1d, separate DEF future): ActiveInner with inline session_params + backend_key — eliminates `with_cancel_request` `Option<R>` None tier-3 arm (becomes infallible `R`). Direction 1.B: `<P as SealedPhase>::Inner = DisconnectedInner | ConnectingInner | ActiveInner | ErroredInner | ResettingInner | ClosedInner`. Each phase has its own struct with phase-appropriate fields. **8-10 tier-1 elevations gained** at full bundle completion: per-phase methods (`<ActivePhase>::with_cancel_request() -> R` directly, no `Option<R>`; Bundle D' collapses); state variant in wrong phase = compile error; backend_key/session_params inline non-Option on `<Active>`; per-phase dispatch carries only legally-touchable slots; `<Disconnected>::feed_inbound` storage-absent not just method-absent. **Per-phase sizes**: `<Disconnected>` 536 → **0 B (ZST)** ✅ Phase 1a `5b5b886`; `<Closed>` 536 → **16 B** ✅ Phase 1b `5e6127f` (state_kind + error_arena handle); `<Connecting>` 536 → ~296 B Phase 1c; `<Active>` 536 + 436 heap → ~952 B inline Phase 1d (−1 heap alloc, net −20 B). **Bundled with footprint memo Layout B** (`/tmp/def-rethink-footprint-memo.md` 5 wins): Box ProtoState heavies (state 80 → 16 B, −64 B); u16 OID interner in RowDesc (−64 B); Box<RowDesc> (−132 B, measure first +2-5 ns/row); Password 512 → 32 B inline + `LongPassword(Box)` for outliers (−486 B/alloc); retire `error_arena` (lifetime-bounded slices, −8 B struct + ~290 B heap + ~1100 LoC + 12 tier-3 surfaces). Combined `<Active>::Inner` ~200 B inline post-rethink (Box-heavies + per-phase tight fit). **Remaining cost**: 4-7 weeks (Phase 1a took ~430 LoC, 1b ~180 LoC, remaining 1c/1d/2/3/4 estimated +3300/-1500 LoC cumulative). **Rejected (with reasoning)**: enum-of-phase (1.C, loses type-level phase proof), union-storage (forbid(unsafe_code)), boxed Inner (heap indirection per DEF-187), bit-packed state discriminant (needs unsafe in Drop). **Subsumes/closes** (post-completion): DEF-273 (within-_proto_init_leaf — collapses via new per-phase storage layout); partial of DEF-274 (cell modules; per-phase Inner makes some cells `<Active>`-private inline, eliminates cross-module write surface); DEF-058 (ring-buffer ReadBuf becomes per-phase consideration); DEF-247-redesigned (typed-row projection layer integrates with `<Active>::Inner`'s narrower scope). **Cumulative savings**: Phase 1a -536 B (Disconnected); Phase 1b -520 B (Closed). **Next**: Phase 1c (`ConnectingInner` — drop `row_desc_slot` (140 B; no RowDesc pre-RFQ) and `backend_key` (12 B; auth-flow only); expected 536 → ~296 B, -240 B savings). | 8-10 tier-1 elevations + factor-536/-33 reductions for ZST/16B phases + Active heap-alloc eliminated; Phases 1a+1b delivered the first 1056 B savings | BREAKING — every API touching `PgProtocolInner` changes; consume-self phase transitions become real materialise+drop (~ns, once per connection); Phases 1a/1b's promotion of `PgProtocolInner`/`DisconnectedInner`/`ClosedInner` to pub is purely E0446 mitigation (fields stay private; no new external capability); `<ClosedPhase>::state()` accessor deleted (no callers; provably Errored by storage absence) |
| 2 | DEF-280 | **Safety Bundle Group — residual non-blocking items only** (memo `/tmp/def-rethink-safety-memo.md` 1100 LoC). 9 of 9 originally-scoped work items + bonus F Phase 1 **SHIPPED 2026-05-18** in sequence with asm-diff + bench-stable verification at each commit: **G** `1cf6041` (debug_assert!(false,…) glass pattern), **H** `7b7a8c4` (RowStream !Send), **J** `0286840` (distinct CRATE_BUG_REPLY_ID_SENTINEL=NonZeroU64::MAX), **K** `b66dc4a` (enter_partial_mode typed-Err), **I/D''** `e02eed7` (Zeroizing<i32> stack-slot scrub), **K-mirror** `83d5a09` (exit_partial_mode typed-Err + upstream-discipline removal), **DEF-281 A/B/C/D** `9c25c1e` + `df42ab0` (silent row-truncation closure), **E Phase 1 + P-D280E-1/2 + sweep** `315c178` + `a7d6424` + `2041db8` (Sensitive::get → with_inner closure-scope + 2 hostile probes + 3 non-fundamental .unwrap_or(&[]) sites), **F Phase 1** `643511f` (InstallBody trait split — within-crate hostile-witness hole closed via private supertrait + 5 bound-tightening sites incl. PushCommand::PostState declaration boundary + no-dep ambiguous-blanket-impl seal pin). Total +1673 / −243 LoC across 14 commits; 606 → 528-non-doc + 1 anchor tests (test infrastructure changed mid-run — pre-Bundle-F headline was 619 inclusive of doctests; post-Bundle-F 528 non-doc with 1 pre-existing unrelated doctest failure); 3 hostile probes (P-D280H-1, P-D280E-1, P-D280E-2) + 1 no-dep compile-time seal pin (bundle_f_seal_probe::AmbiguousIfInstallBody); 3 new CrateBugLocus variants; 2 new cluster δ leaf submodules; 2 new error types; 1 new const (CRATE_BUG_REPLY_ID_SENTINEL); 1 new sealed trait pair (PostStateProof marker + InstallBody body); 1 public API break (Sensitive::get → with_inner). **Remaining work** (all non-blocking, all individually justified for deferral): (a) **`push_within_fanout_budget` Bundle G-residue** — Bundle G removed the dev-loud debug_assert but the underlying tier-2 structural (const-asserted-architecturally-dead) classification persists; true tier-1 elevation requires either `unsafe push_unchecked` (forbid'd) or unstable `generic_const_exprs` (capability witness on OutActions). Defer until rust-version unlocks the feature OR an architectural rethink moves to a typed-witness API. (b) **Bundle E Phase 2** (SecretBoundedStr::as_str/as_bytes closure-scope): no docstring discipline rule on these accessors — Rust lifetime-checking is sound for &str/&[u8] borrows; closure-scope migration would be cosmetic with no real tier-elevation. Skip unless a specific use case surfaces a docstring rule worth eliminating. (c) **Fundamental ReadBuf primitives** (`buf.rs:174/627`): defer to DEF-279 Foundation rethink — refactoring to Result-returning accessors cascades through every ReadBuf caller; pairs naturally with per-phase Inner refactor. (d) **DescribeName concrete-tokens** (originally scoped alongside Bundle F): Bundle F closed the install-body hole via trait split; DescribeName uses the same sealed-supertrait pattern but has no install-body equivalent (only a name-rendering surface). Within-crate hostile probably exists symmetrically but the blast radius is "send wrong name string in DescribeStatement frame" — wire-classified error, not state corruption. Re-scope when a concrete attack pattern surfaces. | 0 blocking sub-bundles remaining; SHIPPED full scope closing the within-crate hostile-witness class, the silent-truncation class, the foundational secret-borrow retention class, AND the install-authority class | residue items are tier-neutral or blocked on rust-version/DEF-279 |
| 3 | DEF-247 | **[SUPERSEDED-by-DEF-248-Sub-A 2026-05-12 — needs re-design]** Original premise: generic `RowStream<P: RowProjection>` replacing the 5 pull APIs (`next_event` / `next_row` / `next_row_bytes` / `consume_rows::<N>` / `for_each_row`) with projection types. **DEF-248 Sub-A (`3a55d89`) deleted those 5 APIs** and replaced them with `iter_rows<R, F>(closure) -> R` + `col_next() -> ColEvent` — the canonical pull surface is now closure-scoped, not generic-projection-based. **Possible re-design**: `AsTuple<T>`/`AsCallback<F>` projection types could become a higher-level decoder layer over `col_next` (e.g., `stream.collect_tuple::<(i32, &str)>() -> Result<Reply<Vec<(i32, &str)>>>`). Re-evaluate scope post-DEF-244 (`prepared!` macro) since prepared queries naturally pair with typed-row projection. | Re-design pending | TBD post re-scope |
| 4 | DEF-058 | **Ring-buffer ReadBuf — overlay on DEF-265 two-tier** — eliminates `compact_inline()` / `compact_heap()` memmove on continuous streaming. Wraparound cursor + write_pos modulo N. `unread()` returns `(&[u8], &[u8])` for wrap-spanning frames. **Audit 2026-05-11 (against `5c5cfa4`)**: post DEF-265 (`9ec3ca9`) the ReadBuf is two-tier (inline 256 B + lazy heap-escape 4096 B), NOT linear-with-compact as the original DEF-058 framing assumed. `unread()` currently returns single `&[u8]` (linear view, no wrap). DEF-058 now applies as a wraparound **overlay** on the two-tier shape: ring within active tier (inline OR heap), or wraparound abstraction that spans both. `compact_inline` (buf.rs:707) and `compact_heap` (buf.rs:732) are the elimination targets. Sequencing note: also affects DEF-204 staleness-leak zeroize discipline (current compact paths zeroize abandoned tail pre-truncate; ring-buffer must replicate that invariant). | 3-10× on continuous network streaming workloads (NOT visible on current criterion benches — pre-fill model; new streaming bench needed to measure) | BREAKING internal slice API; every consumer of `populated()`/`unread()` learns split-case |
| 5 | DEF-257 | **Branded `ReadBuf` — re-introduction** — mirrors DEF-154 brand-token discipline on read side (WriteBuf has it; ReadBuf had it pre-DEF-154 H and was deleted as dead scaffold once `ReadRange<'brand>::apply` lost its only caller `StreamRowRange`). Compile-time tracking of "this slice came from THIS read buffer scope." **Audit 2026-05-11 (against `5c5cfa4`)**: `BrandedReadBuf<'brand, 'a>`, `populated_branded()`, `ReadBuf::with_branded()` are all DELETED (buf.rs:836-852, removal-commit DEF-154 H). Plain lifetime-enforcement on `row_bytes: &'r [u8]` is currently tier-1 for the surfaces that exist. Re-introduction blocks on a NEW load-bearing use case (e.g., DEF-248 streaming where partial-frame slices outlive the immediate borrow scope and could escape across buffer reuse cycles); otherwise stays measure-rejected. **Re-evaluate**: only when DEF-248 design forces a slice-escape pattern that plain `'a` lifetimes cannot pin. | Tier safety only, no perf | Additive (lifetime-only) IF re-introduced; current state is "removed, no use case open" |
| 6 | DEF-246 Phase 6 | **Default-phase removal** — remove `P: SealedPhase = ActivePhase` default on `pub struct PgProtocol`. Phases 1-5 SHIPPED (`d97484d` + `17dc228`: scaffolding + 4 tier-1 elevations + 9 hostile probes). Phase 6 cosmetic residue: bare `PgProtocol` (without phase param) still compiles via the default; removing the default forces every type-position usage to spell out the phase. Migrates ~5-10 test signatures (`proto: &mut PgProtocol`, `Option<PgProtocol>`) + the P-E-8 hostile probe's `let _: PgProtocol = ...` line + the `lib.rs` size-pin re-exports. Each callsite makes the phase explicit (typically `<ActivePhase>` since that's what `default` was supplying). Tier impact: **none** (the default was back-compat ergonomics, not a tier surface). Re-evaluate priority — low; production callers already spell out phases via `<DisconnectedPhase>` from `new()` consume-self chain. | Tier-neutral (cosmetic); explicitness over default | BREAKING — every bare `PgProtocol` becomes `PgProtocol<ActivePhase>` (or appropriate phase) |
| 7 | DEF-245 | **`bsql-pg-wire` + `bsql-pg-state` crate split** — separate frame I/O (~1.5K LoC) from state machine (~3.5K LoC). Composable; proxy/relay scenarios get frame-level access without state-machine baggage. | Architectural cleanup, modularity, no perf change directly | BREAKING crate split |
| 8 | DEF-273 | **DEF-272 within-`_proto_init_leaf` wholesale closure** — residual closure for the ~30-LoC scope where wholesale-replacement (`*cell = RowDescSlotCell::empty(token)`) is still possible after the leaf-token follow-up: code editing `_proto_init_leaf` itself can mint `ProtoInitToken` and produce fresh cells. Mechanism options: **(a)** `Pin<&mut Cell>` to forbid wholesale via Pin's API surface, **(b)** brand-lifetime `BrandedCell<'id>` mirroring DEF-154 brand-token discipline + DEF-257 read-side branding (zero-cost; lifetime invariance forbids cross-scope replacement), **(c)** `OnceLock`-gated single-mint `ProtoInitToken` (one token per program lifetime — wholesale needs a fresh token, only one ever exists). **Priority — low** post the leaf-token follow-up: cross-module + within-`mod protocol`-but-outside-leaf holes are closed; residue is 30 LoC of self-contained init logic in a single submodule. Pin/Brand machinery to close 30 LoC may be poor cost-benefit. Re-evaluate when next perf/safety audit cycles. | Tier-2 by-discipline (within 30-LoC `_proto_init_leaf`) → **tier-1 by-construction** | BREAKING (Pin or Brand surfaces in pub APIs of cell modules) |
| 9 | DEF-274 | **DEF-272 internal-editor closure — cell modules → sibling sub-crate** — extracts `schema_slot.rs`, `session_params_slot.rs`, `state_setter.rs` into a sibling sub-crate (e.g., `bsql-pg-cells`) where private inner fields become truly cross-crate-private. Within the new sub-crate the same internal-editor problem persists at a smaller scope, but the post-DEF-272 cell modules are <200 LoC each — small enough that the residual surface is reviewable as a unit. **Why this matters**: post-DEF-272 a contributor editing `schema_slot.rs` can still write `self.inner = Some(...)` directly and bypass `park_at_*` token gates. **Sequencing note**: subordinate to DEF-245 (frame-I/O / state-machine split). Execute as follow-up rather than cross-cutting alternative. **Caveat**: per-leaf concrete-type tokens (`BeSelectToken`, `ParamStatusToken`, etc.) must remain mintable inside their hosting leaves in `bsql-pg-proto`; sub-crate facade methods accept these tokens by value — token types live in the sub-crate, mints live in the host crate. **Perf**: must measure that cross-crate inlining keeps cell-method overhead at zero (LLVM's `#[inline]` annotation should suffice; cross-crate inlining requires the methods to be `#[inline]` on definition, not just suggested). | Tier-2 by-discipline (within cell-host module) → **tier-1 by-construction** | BREAKING (workspace member added; internal compile model changes; cell-host module APIs become cross-crate-public) |
| 10 | DEF-247-redesigned | **[NEW SCOPE post-DEF-248-Sub-A; unblocked by DEF-244 SHIPPED 2026-05-15 `9ebebdf`]** Higher-level decoder layer over `col_next()` — `stream.collect_tuple::<(i32, &str)>() -> Result<Reply<Vec<(i32, &str)>>>`-style projection types. Original DEF-247 premise («replace 5 pull APIs with generic projection») obsoleted by Sub-A which deleted those 5 APIs entirely. Re-design proposes building typed-row aggregation ON TOP of `col_next` instead of replacing the pull layer. **Pairs naturally with DEF-244 `prepared!` macro** — prepared queries declare row shape at compile time, projection types decode `ColEvent` stream into the declared shape. DEF-244's `RowDecode::Row<'a>` GAT already provides the typed-shape contract; this entry adds the `collect_tuple`/`for_each_typed_row` aggregation API. | Ergonomic typed-row API for prepared paths; per-row decode overhead TBD post-measurement (note: DEF-244's «-25 ns» prediction was refuted at +4 ns within noise — be skeptical of `ColEvent` pull-loop savings vs current decoded-tuple path) | Additive on top of `col_next` |

#### Deferred (per principal directive 2026-05-08)

| DEF | Item | Reason for deferral |
|-----|------|---------------------|
| DEF-255 | **PGO build infrastructure** — `cargo-pgo` setup + training workload definition. Cross-platform LLVM-native. | Library crate PGO needs representative training-binary; criterion benches are micro and may not reflect production patterns. Revisit when end-to-end driver workload exists (`bsql-driver-postgres` matures). |

#### Exploratory pool (measure-first; commitment gated on evidence per CREDO §96a)

| DEF | Item | Status |
|-----|------|--------|
| DEF-260 | Custom `Action` enum layout (`#[repr(u8, C)]` hand-tagged union; current Rust default may already be optimal) | EXPLORATORY |
| DEF-261 | Branchless DataRow column-length-prefix decode (skip per-col-len validity check if invariants held) | EXPLORATORY |
| DEF-262 | `core::hint::black_box` as code-motion barrier in production hot paths (risk: may pessimise; careful measure) | EXPLORATORY |
| DEF-263 | `#[inline(never)]` stack carve-out for hot fns with large stack frames (separates stack-cold from stack-hot) | EXPLORATORY |
| DEF-264 | GAT-driven `FromRow` projector chain (stable Rust 1.65+ GATs; per-column zero-copy projections) | EXPLORATORY |
| DEF-275 | **[CLOSED 2026-05-15 as not-reproducible]** `column_decode/iter_5cols_decode_text_long_ascii` +8.3% observed during DEF-244 closure bench-stable compare under load 1.32-2.47 was investigated during DEF-276 rebaseline: clean re-measurement against `pre-def276-clean` baseline (load 1.89→2.12, full clean-rebaseline methodology) showed **−0.55% within noise threshold** (p=0.07, change not statistically significant). The original +8.3% was transient measurement noise / criterion sample artefact, NOT a real LLVM codegen drift from DEF-258. Lesson: bench-stable's noise floor on text-decode benches can spike to ±10% under marginally-loaded conditions even with `--measurement-time 30` — sub-30 ns timings are particularly susceptible. No further action needed. | CLOSED |

#### Roadmap operating principles

1. **Sequential architect-cycle per phase** (refined 2026-05-08): one DEF item at a time, bench-stable baseline → change → bench-stable compare → review → commit. Per-change perf attribution is unambiguous.
2. **Bench-evidence gate**: every commit message includes a `bench-stable compare` table vs the baseline saved BEFORE the change.
3. **Pareto-better gate**: any phase that regresses an existing bench beyond noise (5%) is rejected unless ASM-diff forensics + structural rationale justify the trade. Phase 2 SWAR Attempts 1-2 → forensics → Attempt 3 (opt-in helper) is the canonical example.
4. **Breaking-API atomic landings**: when a BREAKING phase ships, it gets one commit; downstream churn happens once.
5. **No measurement guesses**: ASM-diff + bench-stable are the methodology of record (CREDO §96a, DEF-236 lesson).

---

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
| DEF-160 | **SHIPPED 2026-05-11** — see §A header for the post-ship entry. Row kept as cross-reference. | — |
| DEF-161 | Error-body arena (closed — see DEF-184 A1+A13 shipped) | — |
| DEF-162 | cargo-mutants kill-rate target | Phase 1d |
| DEF-242 | **`ActiveGuard<'a>` typestate for feed-side** — symmetric to DEF-198 ReadyGuard. `proto.as_active() -> Option<ActiveGuard>` returns `None` when state==Errored; `ActiveGuard::feed_bytes` / `advance_one_frame` only callable from the guard. Lifts `IngressClassification::AlreadyErrored` arm from tier-3 runtime classification to **tier-1 compile-rejected** on the public API surface. Tier delta same shape as DEF-198. ~150 LoC, breaking API change. Identified by DEF-238 post-impl audit (2026-05-05) — only structural path to tier-1 closure of the AlreadyErrored arm. | Phase 1c-5 (state-machine guards bundle alongside DEF-005..009 pipelining) |
| DEF-243 | **Eager `read_buf.clear()` at install_errored transition** — currently the transition-to-Errored path leaves read_buf un-scrubbed until next feed_bytes call (`AlreadyErrored` arm) or Drop-on-discard. Window ~one feed_bytes call interval; Drop catches the eager-discard pattern (DEF-185 P0-C zeroize-on-Drop). Tighter security: thread `read_buf` through `install_errored` / `fail_inflight_no_readbuf` signatures, scrub at the transition site itself. Mid-size refactor (signature plumbing through dispatch.rs + protocol.rs); non-critical (Drop path already scrubs eager-discard). Identified by DEF-238 post-impl audit (2026-05-05). | Phase 1d hygiene-tightening (bundle with other zeroize-timing audits) |

### Wire protocol coverage gaps (gap analysis 2026-05-04)

Methodical pass through PG §55 (Frontend/Backend Protocol). Every
message and flow compared against current `bsql-pg-proto` shape.
The crate is **NOT v1.0-ready** despite the solid foundation; below
are the missing pieces, ranked by criticality.

**Critical for ANY production use** (these block reasonable PG deployments):

| DEF | Item | Why critical | Phase |
|-----|------|--------------|-------|
| DEF-214 | **TLS / SSLRequest pre-startup state machine** — send 8-byte SSLRequest packet (length=8, version=80877103), parse `'S'` (start TLS) / `'N'` (plaintext) / ErrorResponse byte. The actual TLS handshake (rustls) lives in `bsql-driver-postgres`, but the proto MUST track the pre-startup phase. Without this, **no cloud PG accessible** (RDS, Cloud SQL, Heroku, Supabase, Neon, Aiven all require TLS). **WIRE BYTES + RESPONSE CLASSIFIER SHIPPED 2026-05-07**. **Phase 1** (commit `6187a41`): `wire::SSL_REQUEST_VERSION = 80_877_103` const + `wire::SSL_REQUEST_WIRE_BYTES: [u8;8] = [0,0,0,8, 0x04,0xd2,0x16,0x2f]` (pub, top-level re-export); 5 tier-1 const-asserts pinning length + version-bytes against the `to_be_bytes()` formula. **Phase 2** (this commit): typed classification of the 1-byte server response. New `wire::SslNegotiationOutcome` enum (`#[non_exhaustive]`, 4 variants: `Accepted`/`Refused`/`ErrorIncoming`/`InvalidByte(u8)`) + `wire::classify_ssl_response_byte(byte: u8) -> SslNegotiationOutcome` (pub, top-level re-export, `const fn`, no panics, no allocs); 5 const-block round-trip pins (S/N/E/0x00/0xff classify correctly at compile time). 8 integration tests in `tests/ssl_request_wire_spec.rs` (per-byte happy paths × 3, exhaustive 0..=255 sweep with InvalidByte payload preservation, boundary values 0x00/0xFF/0x80/0x7F/0x01/0xFE, PartialEq semantics, top-level vs module path agreement, `#[non_exhaustive]` catch-all requirement). Tier impact: pre-Phase 2 drivers wrote ad-hoc `match byte { b'S' => ..., _ => ... }` — tier-3 by-discipline (forgetting a branch silently mishandles); post-Phase 2 the dispatch is tier-1 typed for the 4 known outcomes. **Pending Phase 1e**: `ProtoState::ConnectingPreSslAwaitingResponse` variant + state-machine integration (driver currently handles SSL probe entirely outside the state machine; this is acceptable as the response byte is OOB and can't flow through `feed_bytes` regardless). DEF-217 SCRAM-SHA-256-PLUS channel binding remains separately blocked on TLS cert hash plumbing through to the SCRAM dispatcher (driver-side concern; needs `&[u8]` cert-hash threaded through Credentials::ScramPassword construction). | Block all cloud PG | 1b ext (wire + classifier SHIPPED) / 1e (state machine + channel binding) |
| DEF-215 | **Cleartext password auth** (R/3) — server sends `AuthenticationCleartextPassword`; client sends `PasswordMessage` ('p') with NUL-terminated password. **FULLY SHIPPED 2026-05-05**. Foundation (commit `2fe619a`): `AUTH_CLEARTEXT_PASSWORD = 3` const, `AuthSubCode::CleartextPassword` variant, drift-pin asserts. Full flow (this commit): `Credentials::CleartextPassword(Sensitive<Password>)` variant with redacting Debug; new ProtoState variants `ConnectingStartupCleartext { reply, password: Box<Sensitive<Password>> }` (Box for size-pin containment, mirror of SCRAM PERF-02 pattern) + `ConnectingCleartextAwaitingAuthOk(reply)`; new dispatchers `dispatch_auth_in_startup_cleartext` (accepts only sub-code 3, builds PasswordMessage, transitions; tier-1 exhaustive — rejects all other codes as KnownButWrong) + `dispatch_auth_ok_after_cleartext` (only AuthOk legal); new wire builder `build_password_message` (tag 'p' + BE u32 length + password bytes + NUL); 4 integration tests in `tests/startup_spec.rs` (end-to-end happy path, ErrorResponse mid-handshake, downgrade-rejection of SASL offer, Debug-redaction). Tier impact: `panic = "abort"` zeroize-on-drop chain unchanged (Box → Sensitive → Password ZeroizeOnDrop on transition). **Security note**: cleartext password is sent as-is on the wire; the driver wrapper (Phase 1e) is responsible for refusing cleartext-credential constructs on non-TLS connections (DEF-214 dependency for the policy gate). ProtoState size pin (`== 80`) preserved (DescribeStatementAwaitingRfq remains dominant). | Block legacy on-prem | 1b ext (SHIPPED) |
| DEF-216 | **MD5 password auth** (R/5) — server sends `AuthenticationMD5Password` with 4-byte salt; client sends `PasswordMessage` containing `"md5" + md5_hex(md5_hex(password+username) + salt)`. **FULLY SHIPPED 2026-05-05**. Foundation (commit `2fe619a`): const + AuthSubCode variant + drift-pins. Full flow (this commit): `md-5` RustCrypto workspace dep + crate dep added (DEF-META-01 audit-trust profile matches sibling `sha2`/`hmac`/`pbkdf2`); new `crate::md5` module isolates all crypto + memory-hygiene surface; `Md5HandshakeState` struct (pub for lint, pub(crate) module path) bundles password + username under one Box (mirror SCRAM PERF-02 single-Box pattern); `Credentials::Md5Password(Sensitive<Password>)` enum variant with redacting Debug; new ProtoState variants `ConnectingStartupMd5 { reply, handshake: Box<Md5HandshakeState> }` + `ConnectingMd5AwaitingAuthOk(reply)`; new dispatchers `dispatch_auth_in_startup_md5` (validates 4-byte salt → MalformedAuthentication on length mismatch, builds PasswordMessage, transitions; tier-1 exhaustive — rejects all other AuthSubCode variants as KnownButWrong) + `dispatch_auth_ok_after_md5` (only AuthOk legal). `compute_response_body` performs `md5(md5(pw||user) || salt)` with every password-derived intermediate buffer (inner_digest, inner_hex, outer_digest, outer_hex) wrapped in `Zeroizing<>` for explicit Drop-time scrubbing. 3 lib unit tests (compute smoke, algorithm-shape pin (pw||user not user||pw), hex-encoding known-vectors) + 4 integration tests in `tests/startup_spec.rs` (end-to-end happy path with byte-by-byte response comparison via independent reference computation, malformed-salt rejection, downgrade-rejection of cleartext offer, Debug-redaction). ProtoState size pin (`== 80`) preserved. **Security caveat**: MD5 is cryptographically broken for collision-resistant uses; PG's salt+rehash construction provides only weak protection against passive observation, and offline GPU cracking is fast in 2025 dollars-per-hash terms. The driver wrapper (Phase 1e) SHOULD prefer SCRAM where the server offers it. | Block enterprise on-prem | 1b ext (SHIPPED) |
| DEF-217 | **SCRAM-SHA-256-PLUS channel binding** (closes pre-existing DEF-053 reference) — RFC 7677 channel binding via `tls-server-end-point` hash extracted from server's TLS certificate. Requires DEF-214 TLS layer first (channel-binding hash sourced from rustls cert handle). Critical for PG ≥ 11 default `scram-sha-256` authentication when server config includes `cbind=server-end-point` mandate. Without this, modern PG with channel-binding enforcement rejects the connection. | Block PG ≥ 11 strict-scram | 1e (after DEF-214) |
| DEF-218 | **Buffer sizing for large rows / large queries** — current caps are dangerously small for production workloads:<br/>• `READ_BUF_CAP = 4096` → DataRow body > 4 KB tears down the connection. PG TEXT/BYTEA/JSONB cells routinely span MB. **Any `SELECT row_with_jsonb` falls.** Hard failure (`UnexpectedFrameSize` → connection teardown). **Open** — closed structurally by DEF-248 (queue pos 8).<br/>• ~~`MAX_SQL_LEN = 2048`~~ → ~~analytics queries 5-50 KB silently truncated~~ **CLOSED STRUCTURALLY 2026-05-11** by DEF-160 Z2 (commit `5c5cfa4`): `Parse<'a>::sql = &'a str` + `SimpleQuery<'a>::sql = &'a str` — SQL flows as borrowed slice with no copy-into-arena step. Truncation arena eliminated; unbounded SQL length within `i32::MAX` PG wire frame limit.<br/>• `MAX_PARAMS_ARITY = 16` → UNNEST bulk insert blows past limit at 50-1000+ params. **Open.**<br/>• `MAX_ROW_COLUMNS = 32` → wide-table SELECT * (>32 cols) emits TooManyColumns. **Open.**<br/>**Principal-directive 2026-05-10 (verbatim):** «универсальное решение, если буду не просто огромные базы, а колоссальные, то оно и под ними должно себя стабильно а главное корректно вести и работать». Translation: cap-bump and per-instance const-generic are **fallback patterns** — they push the limit but do not eliminate it; on colossal data (MB-GB cells) any static cap eventually fails. **Universal solution = DEF-248** (streaming row decode): per-column pull-based decode where the buffer holds `O(largest_value)` not `O(largest_row)`. Frames > `READ_BUF_CAP` are read in chunks and consumed column-by-column; the cap-as-failure-mode disappears structurally.<br/>**Design choices** (kept as historical record — superseded by streaming approach):<br/>(a) bump defaults — REJECTED (fallback; doesn't solve colossal).<br/>(b) const-generic on `PgProtocol<const READ_CAP, …>` — REJECTED (still per-instance fixed limit).<br/>(c) **streaming row decode** — SELECTED (DEF-248 architectural fix; pull-API splits frames across buffer reuse cycles; memory bounded by single-cell size).<br/>For non-buffer caps (`MAX_PARAMS_ARITY`, `MAX_ROW_COLUMNS`): bump defaults to honest production values + const-generic for users with even-bigger-than-honest needs. | Block real-world data shapes (READ_BUF_CAP residue + arity/column caps); SQL-len leg closed via DEF-160 | 1d / DEF-248 (streaming) ships first |

**Important features (block significant use cases)**:

| DEF | Item | Phase |
|-----|------|-------|
| DEF-219 | **COPY protocol** — `CopyInResponse`/`CopyOutResponse`/`CopyData`/`CopyDone`/`CopyFail` inbound + outbound. Streaming bulk transfer mode that toggles connection into binary-stream sub-protocol. Used by pg_dump, ETL pipelines, bulk insert. Distinct state-machine sub-graph (CopyIn / CopyOut variants, with per-byte streaming through ReadBuf). ~500 LoC. | 1d |
| DEF-220 | **LISTEN/NOTIFY + NotificationResponse delivery** — `NotificationResponse` ('A') frame currently has no dispatch arm. Surface to caller via new `Action::Notify { channel, payload, pid }` variant + cascade through Reply enum. Pub/sub paradigm critical for event-driven apps. | 1f |
| DEF-221 | **CancelRequest send flow** — special startup variant (length=16, version=80877102, pid, secret_key) sent on a *parallel* TCP connection to cancel an in-flight query. Requires storing BackendKeyData (pid + secret_key, already collected in `ConnectingPostAuthHaveKey` variant). **WIRE BYTES + MAGIC-VERSION FAMILY PIN SHIPPED 2026-05-07**: `wire::CANCEL_REQUEST_VERSION = 80_877_102` const + `wire::MAGIC_VERSION_HIGH_HALF = 1234` family-formula const + `wire::cancel_request_bytes(pid: i32, secret_key: i32) -> [u8; 16]` `const fn` builder (pub, top-level re-export `bsql_pg_proto::cancel_request_bytes`); 9 tier-1 const-asserts (family-pin formula `(1234 << 16) \| low_half` for both SSL=5679 + Cancel=5678 with explicit error messages, family-disjointness from `PROTOCOL_VERSION_3_0`, distinctness from SSL, length=16, version=0x04d2162e literal pins, round-trip layout pins for zero/non-zero/negative-i32 payloads, total-length sanity, length-includes-self pin); `tests/cancel_request_wire_spec.rs` 11 tests + 3 const-asserts pin the public-API surface from outside the crate (zero-payload spec match, non-zero-payload spec match, top-level vs module path agreement, version-const matches byte literal, length-field includes self, distinctness from SSL/Terminate, magic-decomposition 1234<<16\|5678 pin, family-formula consumer-side pin, negative i32 BE encoding, i32::MAX edge encoding, pid/secret_key independence). Tier impact: pre-DEF-221 drivers had to hand-spell the 16-byte packet at every cancel call site (tier-3 by-discipline — bytes wrong = silent server confusion); post-DEF-221 the `const fn` produces a tier-1 by-construction `[u8; 16]` (size compile-fixed, field positions hidden inside the function, BE encoding of dynamic payload pinned by const-asserts). **Pending Phase 1e**: state-machine surface — `Connection::cancel_inflight()` async wrapper that opens parallel socket, writes `cancel_request_bytes(pid, secret_key)`, closes; threads BackendKeyData through driver state. Drivers can already write the bytes manually; the wrapper is ergonomics + lifecycle (which connection's pid/key, when to spawn the cancel future). | 1e (state-machine wrapper only — wire-bytes ALREADY SHIPPED) |
| DEF-222 | **Close (Statement / Portal) command** — frontend `Close` ('C') message sends `[type, name]` to release a prepared statement or portal. Backend responds with `CloseComplete` ('3') (already partially defined as `pub(crate)` const). New `ProtoState::CloseAwaiting*` variants + `PgCommand::CloseStatement / ClosePortal` + `Reply::CloseComplete`. Used by stmt cache eviction (DEF-035). | 1c-6 |
| DEF-223 | **Terminate ('X') graceful close** — frontend sends `Terminate` (0 body) before TCP close to signal clean shutdown. Server completes any in-flight query and closes. Without it, TCP RST leaves server in confused state, may log connection-loss warnings, holds locks momentarily. **WIRE BYTES SHIPPED 2026-05-05**: `wire::TAG_TERMINATE = OutboundTag(b'X')`, `wire::TERMINATE_WIRE_BYTES: [u8;5] = [b'X', 0, 0, 0, 4]` (pub, top-level re-export `bsql_pg_proto::TERMINATE_WIRE_BYTES`); 6 tier-1 const-asserts (length + tag + length-field literals + assert_all_distinct! outbound + drift-pin block); `tests/terminate_wire_spec.rs` 3 tests + 3 const-asserts pin the public-API surface from outside the crate. **Pending Phase 1e**: `Action::Terminate` + state-machine integration (`ProtoState::Closed` variant, ConnectionStatus reporting). Drivers can already write `TERMINATE_WIRE_BYTES` to socket on graceful close; state-machine envelope tracking is the residual driver-coordination concern. | 1e (state-machine) |
| DEF-224 | **NoticeResponse delivery to user** — currently silently consumed by pre-dispatch filter (DEF-062 reference; user never sees `NOTICE: identifier truncated`, deprecation warnings, cost-estimator output). Surface via new `Action::EmitNotice { severity, code, message }` + caller-supplied notification handler in `bsql-core` Phase 3. Without this, valuable diagnostic info is lost — operators can't correlate server-side warnings with client behaviour. | 1f / Phase 3 |
| DEF-225 | **PortalSuspended + chunked fetch** — server emits `PortalSuspended` ('s') when `Execute` with `max_rows ≠ 0` hits the row limit; subsequent `Execute` resumes the same portal. Currently `FetchRows::All` is the only enum variant (compile-rejecting non-zero); chunked path classifies `'s'` as UnexpectedFrame → teardown. Required for paged result sets, server-side cursors. New `FetchRows::Chunked(NonZeroU32)` variant + state machine arms. | 1c-6 |
| DEF-226 | **Multi-statement SimpleQuery batch** — PG's `\;`-separated batches in one `Q` frame produce sequence of CommandComplete/DataRow groups + final RFQ. Currently "1c-1b-MVP accepts a single statement" — multi-statement returns only the LAST group's reply (silent semantic loss for `BEGIN; UPDATE; UPDATE; COMMIT` style). New `Reply::QueryBatchComplete { tags: BoundedVec<CommandTag, N> }` or stream-style emission. | 1c-1-multi |

**Encoding / decoding coverage**:

| DEF | Item | Phase |
|-----|------|-------|
| DEF-227 | **Non-UTF-8 `client_encoding` support** — decoder uses `simdutf8` (UTF-8 only). PG supports 30+ encodings (SJIS, EUCJP, BIG5, KOI8, WIN1251/1252, LATIN1-9, etc.); legacy DBs on CP1251/LATIN1 are common in CIS/EU. Currently silently produces `DecodeError::NonUtf8` on non-UTF-8 bytes — caller has no path to recover. **Design**: (a) UTF-8-only with explicit `Connection::set_client_encoding('UTF8')` requirement (force PG-side conversion); (b) `encoding_rs` crate integration for client-side decoding (DEF-META-01 — expert-domain ecosystem standard); (c) typed `ClientEncoding` enum surfacing to user. Architect discussion required. | Phase 3 / design-gated |
| DEF-228 | **`FromPgBinary` trait + binary format decoders** — DEF-202 ext sub-item deferred. `<&str>::from_pg_text` and `<i32>::from_pg_text` cover text format only. Binary format requires per-OID decoder (i32 = `from_be_bytes`, jsonb = leading version byte + UTF-8 payload, arrays = nested wire format, composite = field-by-field, ranges = bound flags + bounds). DEF-194 bit-packed `FormatCodeSet` already supports per-column dispatch; the trait + decoders are missing. ~300-500 LoC + per-OID test fixtures. | 1c-7 / Phase 2 (macro-gated for OID lookup) |

**Auth (lower priority — enterprise-specific)**:

| DEF | Item | Phase |
|-----|------|-------|
| DEF-229 | **GSSAPI + SSPI + Kerberos** (R/7 GSS, R/8 GSSContinue, R/9 SSPI) — Active Directory integration. Large scope: `libgssapi-sys` FFI on Unix, Windows native SSPI on Win, ticket-cache discovery. Sub-protocol negotiation via repeated R/8 exchange until GSS context established. Skip for v1.0 unless concrete enterprise customer ask; deferred to v1.1. | post-v1.0 |
| DEF-230 | **GSSENCRequest pre-startup** (length=8, version=80877104) — GSS encryption negotiation, parallel to SSLRequest. Used in DCE/Kerberos environments. Pairs with DEF-229. | post-v1.0 |

**Misc protocol corner-cases**:

| DEF | Item | Phase |
|-----|------|-------|
| DEF-231 | **`Flush` ('H') frontend message support** — sends accumulated frames without committing transaction-state (contrast `Sync` which emits ReadyForQuery and bumps tx state). Constant `TAG_FLUSH` declared but no state machine flow. Required for non-Sync pipelining (1c-5) where caller wants to drive multiple commands without intermediate tx-boundary commits. Pairs with pipelining work; not needed for v1.0 if pipelining lives behind Sync-bracket model. | 1c-5 (gated on pipelining design) |
| DEF-232 | **Cancellation-safety proptest verification** — DEF-162 cargo-mutants follow-up. The "drop user future cannot leave wire dirty" claim is currently load-bearing-by-design but not exhaustively verified. proptest 100K iterations injecting drop at every yield point, asserting `feed_bytes` returns valid actions and state remains consistent post-recovery. Pairs with reforge §3.3 EXHAUSTIVELY VERIFIED tier. | Phase 6 / Verification CI |

### Infra (ship any time)

| DEF | Item | Size | Priority |
|-----|------|------|----------|
| DEF-143 | criterion bench harness | **PARTIAL SHIPPED** — 4 groups live, `push_bind_execute` bench still pending | Low (works) |
| DEF-167 | Split `action.rs` / `dispatch.rs` into submodules | Large | Cosmetic; deferred after phase 1c fully lands |
| DEF-233 | **`bsql-pg-proto-derive` crate creation** — proc-macro pair-crate for invariant-discipline derives (Pristine, future ones). Required by DEF-211 INNO-01 (`#[derive(Pristine)]` for `SessionParams::is_pristine`). Standard Rust convention (mirror `serde-derive`/`zeroize-derive` pattern). Workspace member with `proc-macro = true`, deps `syn`+`quote`+`proc-macro2`. ~120 LoC including initial Pristine derive. **Architectural prerequisite for INNO-01.** | Stage A0 — before INNO-01 |

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
| DEF-200 | ~~Per-state-bucket dispatch LUTs — A7 adjacent shape (split global tag dispatch into `[fn; 14]` per bucket)~~ **REJECTED 2026-05-07 pre-implementation — see §B for full analysis.** Same FAMILY as A7 (already MEASURED REGRESSION); indirect-call overhead vs current 2D-match jump-table likely produces neutral-to-regression on Apple M1+ class hardware where BTB capacity isn't the bottleneck. `#[cold]` already applied to error-path helpers, so structural extraction can't extract further hot/cold separation. | (REJECTED) | (no code shipped) | (CLOSED — see §B) |
| DEF-201 | `PgCommand` per-kind monomorphisation: `trait PgCommandT { const TAG; type Payload }` + generic `push_command<C: PgCommandT>` | Tier-1 (typed dispatch) | Caller pays only HIS command size; current 2176 B per-command → real size | PROPOSED, **design discussion required** before impl |
| DEF-202 | `simdutf8` for `<&str as FromPgText>::from_pg_text`. Routes UTF-8 validation through `simdutf8::basic::from_utf8` (lane-wise NEON shuffles + masks on aarch64; scalar fast-path elsewhere). Behaviour byte-identical to `core::str::from_utf8`. Hybrid length-threshold dispatch was tested and rejected (branch overhead ~1.5 ns/col exceeded short-ASCII savings). Workspace dep `simdutf8 = { version = "0.1", default-features = false, features = ["aarch64_neon"] }`. Bench `def202-simdutf8` vs `pre-simdutf8` baselines on aarch64-apple-darwin: short ASCII (17 B × 5) +9.9% (43.6 vs 40.3 ns — acceptable cost on the cheapest path), **long ASCII (~200 B × 5) −49.9% (~2× faster: 26.6 vs 53.0 ns)**, **multi-byte UTF-8 (~78 B Cyrillic × 5) −74.0% (~3.9× faster: 78.5 vs 309.4 ns)**. | Same tier (runtime UTF-8 classification → tier-3 by `DecodeError::NonUtf8`; behaviour parity with `core::str::from_utf8` documented + property-tested upstream). | 2-4× speedup on the dominant decoder bottleneck for analytics / internationalised workloads. Binary-format codec sub-item (`i32::from_be_bytes` for binary wire) deferred to a follow-up DEF — requires server-side per-column binary opt-in via `format_codes` in Bind. | SHIPPED 2026-04-28 |
| DEF-203 | Niche audit sweep — apply [`BoundedU8`]/[`BoundedU16`] to `len`-style fields uniformly. **FULLY SHIPPED 2026-04-28** (split commits, see §D Phase 1c entries `DEF-203 (API + 2 sites)` and `DEF-203 ext (FixedStr migration)`). Unified `bounded.rs` with `BoundedU8<const MAX: usize>` (MAX ≤ 254), `BoundedU16<const MAX: usize>` (MAX ≤ 65_534), sealed [`BoundedLen<N>`] trait. **Sites migrated**: `RowDesc::n_columns` (BoundedU8<32>), `OtherEncoding::len` (BoundedU8<32>), `FixedStr<N, Tag, LenT>` (default `BoundedU16<N>`; Ident/DatabaseName/StmtName/PortalName pick `BoundedU8<63>` for the niche win), `PodBytes<N, LenT>` (same pattern). **`const fn` cascade closed via Path C**: per-concrete-LenT inherent impls (`impl FixedStr<N, Tag, BoundedU8<N>>` + `impl FixedStr<N, Tag, BoundedU16<N>>` carry duplicated const fn methods); generic `Self::default()` is non-const but `static EMPTY` consumers don't transitively call it. Cumulative: ~50-60 B saved per `PgProtocol` across migrated sites + tighter `Option<T>` niches on Ident family. RU-01 (const-traits stabilisation) would let us collapse the duplicated inherent impls into a single generic `const fn` — keyword-flip migration when stable; tracked in §C. | (CLOSED) |
| DEF-209 | **Row-level batch UTF-8 validation API** — surfaced by DEF-202 audit ("+10% можно сократить?" probe). Per-column `<&str as FromPgText>::from_pg_text` calls `simdutf8::basic::from_utf8` once per text column, paying the ~3 ns simdutf8 setup cost N times for an N-text-column row. The simdutf8 setup cost (NEON state-vector init, dispatch boundary) is amortisable across columns via `simdutf8::basic::imp::ChunkedUtf8Validator` (streaming validation that pause-resumes across non-contiguous inputs) — but `ChunkedUtf8Validator` is an `unsafe` trait, blocked by `#![forbid(unsafe_code)]`. **Design**: introduce `DataRowRef::columns_text(self) -> Result<TextColumnsIter<'a>, DecodeError>` — validates ALL non-NULL column-data slices in a single `ChunkedUtf8Validator` session (encapsulated in a small `unsafe`-using inner module, audited and gated behind a tier-1 contract). Returns an iterator yielding pre-validated `&'a str` slices for each column without per-column re-validation. Callers with text-heavy rows (analytics queries, log columns) get amortised setup cost — projected savings of ~12 ns on a 5-text-column row (4 of 5 simdutf8 setups eliminated). **Tier impact**: introduces a CONTROLLED `unsafe` boundary inside a sealed `pub(crate) mod row_text_validate` (the only unsafe site outside the `#![forbid(unsafe_code)]` blanket — would require relaxing forbid to deny + per-site allow). Callers see a 100% safe API. **Tier-1 closure** via: (a) the `ChunkedUtf8Validator` API contract is upheld in one audited site; (b) caller-facing types prove valid UTF-8 by construction; (c) memory-probe + property tests cover boundary cases (column boundaries fall inside a multi-byte UTF-8 sequence — must be classified as decode error). ~250-400 LoC + audit doc + property tests. Distinct from DEF-202 (already shipped pure simdutf8 path) — DEF-209 is layered on top to amortise the SHIPPED simdutf8's per-column overhead. | Same tier (still tier-3 by-classification on validation failure; tier-1 closure on the unsafe boundary via single-site audit). | Speculative ~12 ns per row on text-heavy rows (4× simdutf8 setup eliminated for 5-text-col row). Concrete bench gate: must demonstrate measurable win on representative workloads. | PROPOSED, requires `unsafe` boundary policy decision (relax `forbid` to `deny + per-site allow`) — design discussion before impl per architect.txt process |
| DEF-208 | **`compute_push_*` Idle-only refactor** — closes DEF-198 surface 6 ("это точно tier-1 полностью?" probe). Pre-DEF-208, internal `compute_push_<cmd>` retained 5-arm dispatch on `state.push_class()` with non-Idle arms emitting `FailReply` defensively — dead code from the public ReadyGuard path. Refactor: extracted 7 `compute_push_<cmd>_idle_only` siblings (single Idle-arm bodies with `debug_assert!(matches!(state, ProtoState::Idle))`), added top-level `compute_push_idle_only` dispatcher, routed `push_command_internal` and `push_bind_execute_internal` through Idle-only path. Original dispatching `compute_push_<cmd>` and `compute_push` retained but gated behind `#[cfg(test)]` (only the internal `compute_push_tests` module uses them — production binary does not contain those bytes). The standalone `compute_push_bind_execute<P>` was DELETED entirely (zero callers post-refactor). | Tier-3 (internal defensive arms) → tier-1 by-construction: production push routes only through Idle-only siblings; non-Idle defensive code is `#[cfg(test)]` test-target-only. Surface 5 (runtime "is state Idle right now" via `Option<ReadyGuard>`) remains tier-3 — irreducible per Postgres server-driven state nature. | Tier closure of DEF-198 surface 6 + recovered the +3.2% perf cost from `push_command/ping` bench. Bench `def208-idle-only` vs `def207-letfix` (pre-DEF-198 baseline): push_command/ping -0.4% (within noise — fully recovered), ping_round_trip +0.4%, push_command/ping_amortised +1% (residual from `as_ready` borrow check), column_decode/iter_columns_raw -2.2% (improved). | SHIPPED 2026-04-28 |
| DEF-207 | **Branchless wrapping accumulator for `parse_pg_int_signed`** — **SHIPPED 2026-05-07.** New macro `parse_pg_int_signed_widened!($bytes, $result, $acc, $max_digits)` wraps the digit loop with a wider native accumulator + length pre-check + single-end `try_from` cast. Used by `i16` (i32 acc, 5 digits) and `i32` (i64 acc, 10 digits). `i64` retained on original `parse_pg_int_signed!` checked-arithmetic path — i128 acc would compile to multi-instruction sequences on 64-bit targets, losing the gain. Per-digit branch budget collapses 3 → 1 (digit-validation only); full 10-digit i32 path: 30 → 12 branches total. **Bench evidence (column_decode/iter_5cols_decode_i32, criterion baseline before-def207 → compare):** 47.46 ns → 32.89 ns median, **−35.5% (CI [−42.4%, −30.6%], p=0.00)**, throughput +55% (102 → 152 Melem/s). Beats deferred.md's "~30% speculative" estimate. CPU-time wrap during compare reported ratio 0.934 (WARN — minor scheduler interference; signal magnitude vastly exceeds noise band). Alloc-traffic invariant verified via `bench-allocs compare initial-clean` — all 5 scenarios unchanged at zero allocs. ASM-diff on `from_pg_text` symbol surfaced no diff (function fully `#[inline]`-LTO'd into per-call-site monomorphizations; runtime delta is the load-bearing measurement). | Same tier (runtime parse → tier-3 by classified `IntParse` variant). Length pre-check + i64 acc bound is a structural correctness pin: `wrapping_mul(10).wrapping_add(9)` provably cannot wrap during the loop given the bound (max acc reach for 10-digit i32 = 9_999_999_999 << i64::MAX ≈ 9.22 × 10^18). | Real win measured: **−35% on i32 hot path** (better than predicted). Per-row impact projected: ~5 ns saved per i32 column on a 5-col text-row decode = ~25 ns / row, scales linearly with int-column count. | (CLOSED — see §D) |
| DEF-210 | **Tier-1 audit findings (architect-driven 4-pass audit, 2026-04-28 + 2026-05-04)** — full closure detail in §D Phase 1c entry below; this row stays in §A only as a pointer. **ALL CLOSED in §D**: SR-01 Path C/D, SR-02, SR-03, SR-04, SR-05, SR-06, SR-07, ML-01, ML-03, BS-11, CF-02 (initial batch, commit `d0b794e`); REC-06 SCRAM Box consolidation + NB-04 residue-policy per-class pin tests (follow-up batch, commit `f69ecd7`); PERF-02 single-Box SCRAM final closure (architect 4th-pass finding, commit `69a86a7`). **Acknowledged-as-policy**: BS-01 `panic = "abort"` zeroize gap (REOPENED in DEF-211 SAFE-06); BS-02 transitive `unsafe` in `simdutf8`/`heapless`/`sha2` deps per CREDO §11 (REOPENED in DEF-211 SAFE-01). | Tier-2 by-discipline → tier-1 by-construction at 15 sites; see §D for per-site detail. | Closure of "стеклянная архитектура" risks. | §D ALL CLOSED 2026-05-04. |
| DEF-211 | **5th-pass radical architecture audit (architect-agent, 2026-05-04)** — principal asked for the deepest, most thorough audit including alternative architectures. Verdict: **INCREMENTAL-improvements (~250 LoC), NOT at architectural ceiling.** **Headline finding**: **FAKE-01** — wildcard `_ => {}` in `clear_session_residue_if_idle_or_errored` is **the one production-code FAKE TIER-1** in the audit, directly contradicts CREDO §0 ("the invariant hangs on luck rather than construction"). Latent hazards (don't violate today's tier claims, but reduce future-proofing): **FAKE-08** `IdleStateProof::new()` accidental-Default-derive surface; **FAKE-16** `#[derive(Default)]` on `ProtoState` enables `mem::take→Idle` latent hazard; **FAKE-17** `panic = "abort"` zeroize gap (now actionable via SAFE-06 SecretRegistry pattern, not policy-only); **FAKE-19** `bench-hooks` cargo feature exposes test-only API surface to release builds — **SHIPPED 2026-05-04 via tier-1 by-elimination**: feature removed entirely from `Cargo.toml`; both bench hooks (`bench_append_read_buf`, `reset_for_bench`) deleted from `PgProtocol`. `bench_append_read_buf` was a strict duplicate of public `feed_inbound` (DEF-212 Phase 2 commit 201f86a) — benches now call `feed_inbound` directly. `reset_for_bench` replaced by criterion's `iter_batched_ref(setup, routine, BatchSize)` idiom: setup builds fresh `(PgProtocol, WriteBuf)` per iter (untimed); routine borrows by `&mut`, push call timed in isolation (Drop fires outside timed window — accurate per-call measurement). Methodology shift: amortised push reports ~47 ns post-FAKE-19 (was ~10 ns) due to criterion's `iter_batched` floor (~30 ns batch overhead) + fresh-proto cache misses; production per-query cost (cache-warm, proto reused across queries) unchanged. Net: feature physically gone → no leak surface possible → tier-1 by-elimination, no discipline reliance. Substantive improvements: ~~**SAFE-01/ALT-10** — replace `heapless::Vec` with hand-rolled `BoundedVec<T, N>`~~ **REJECTED 2026-05-04 pre-implementation — see §B for full post-mortem.** Per-call init cost catastrophic (+30-50% on `push_command/ping_amortised`); `MaybeUninit` alternative requires crate-internal `unsafe`. Existing rationale at `action.rs:672+` and `lib.rs:126+` already documented the analysis; my reframed plan (SAFE-01') ignored both citations and was caught before code change. **SAFE-06** SecretRegistry trait + panic_hook integration for zeroize-on-panic (~30 LoC); **INNO-01** `#[derive(Pristine)]` proc-macro for `SessionParams::is_pristine` — **SHIPPED 2026-05-04** alongside DEF-233 (created `bsql-pg-proto-derive` pair-crate). Generated impl Pristine + inherent `__pristine_const` (const fn for compile-time pin); per-field type inspection emits `Option::is_none()`/`!bool`/`==0` checks. Tier-3 broad-scope (contributor must remember to extend manual is_pristine) → **tier-1 by-construction** (compiler synthesises check for every declared field; missing-field structurally impossible). 312+ tests green; bench Q2 gate passed (push_command/ping_amortised -3.4% p=0.17, parse_header +1.1% p=0.30 — within noise). Trivial wins: **FAKE-11** math-identity assert `MAX_FRAME_LEN_FIELD == READ_BUF_CAP - 1` (~10 LoC); **SAFE-05** add `clippy::let_underscore_drop`/`let_underscore_must_use` to forbid bundle (2 LoC); **SAFE-07** add `#[non_exhaustive]` to `ErrorKind` + `ConnectionStatus` (4 LoC, pre-empts SemVer footgun); **FAKE-14** add `cargo test --doc` to CI (docs/CI). Per principal directive "никаких 'это сложно, отложим' или 'это не bottleneck — пропустим'" + "каждый байт ценен / каждая наносекунда на счету" — every actionable item executed, no defer. | Tier-2/3 → tier-1 by-construction at 10 sites; eliminates one of the two largest transitive `unsafe` surfaces. | Closes the one production FAKE TIER-1; future-proofs latent hazards; opens panic=abort gap closure. | **IN-CRATE WORK COMPLETE** (audit 2026-05-05): **SHIPPED**: FAKE-19, INNO-01, SAFE-05 (`lib.rs:84-100`), SAFE-07 (`error.rs:34/319/918`+`guard.rs:372`), FAKE-11 (`frame.rs:90-99`), FAKE-14 (`.github/workflows/ci.yml:34-42`), FAKE-01 (multiple sites in `protocol.rs`), FAKE-08 (`guard.rs:195+`), FAKE-16 (`state.rs:47+`), SAFE-06 in-crate trait+impl (`secret_zeroize.rs`). **REJECTED in §B**: SAFE-01/ALT-10 (heapless replacement). **Driver-side blocked on Phase 1e**: SAFE-06/FAKE-17 panic-hook integration (requires `std::panic::set_hook` — out of `no_std` crate scope; `bsql-driver-postgres` will close the gap with ~30 LoC `set_hook` + atomic-set walker). |
| DEF-212 | **Hybrid architecture re-design — per-command bytes-only push + retained amortised feed (design phase verified 2026-05-04, architect-agent adversarial audit)** — surfaced after DEF-211 ship: principal challenged "is `OutActions = ManuallyDrop<heapless::Vec<Action,9>> = 800 B` per-call return frame the cleanest possible shape?" and asked for boldest viable redesign. **Three alternatives initially evaluated:** **(Alt X)** typed per-command tuples Push only, feed unchanged. **(Alt Y)** bytes-only push + one-frame-at-a-time feed + arena-extended FailReply. **(Alt Z)** defer, Box FailReply. **Architect-agent verification 2026-05-04 REJECTED Alt Y as posed**, found 6 concrete flaws + 5 new risks not in initial brief: **(F1)** SCRAM client-final emits SendBytesRange from FEED path (`dispatch.rs:1262-1273`) — Alt Y's FeedEvent had no SendBytes variant, must add; FeedEvent grows 40 → ~88 B; **(F2)** ErrorArena extension to all ProtocolError variants breaks single-slot single-inflight discipline — multi-slot would cost ~2.6 KB and defeat `PgProtocol == 4352 B` pin; honest path keeps ProtocolError value-type → "Action enum eliminated" was incorrect, payload reborn in FeedEvent at same 88 B size; **(F3)** WriteBuf brand discipline (DEF-154) — single-inflight invariant breaks if push-side-effect doesn't enforce drain-before-next-push; mitigation = explicit `#[must_use]` contract on PushFailure (tier-3 by-discipline, same as today); **(F4)** ZeroizeOnDrop for SCRAM bytes in WriteBuf — need `wb.clear()` at entry of every push call to preserve P0-C zeroize-on-clear; **(F5 — KILLER)** per-frame caller loop overhead — Alt Y's `loop { match advance_one_frame {...} }` processes 1 frame per call vs today's `feed_bytes` batched N frames per call; per-call overhead ~50 ns × N frames means **+93% regression on ping_round_trip** (54 ns baseline + 50 ns added per frame on a 1-frame Z consume); **(F6)** loses `feed_bytes_bounded<const BOUNDED: bool>` (B6) specialisation — verified load-bearing per §B (false in remove-experiment regressed 4/4 benches +9-18%, p<0.05). **Verdict: Alt Y' (modified Alt Y)** = Alt X's per-command typed pushes structurally + Alt Y's bytes-only push delta layered + **KEEP `feed_bytes` + `OutActions` 800 B for amortised batched feed** + optional secondary `advance_one_frame -> FeedEvent<'r>` ~88 B per call (gated on principal's pipelining-timing decision — see open question below). **Alt Y' final shape:** **(1)** KEEP `OutActions<'w, 'r>` 800 B for `feed_bytes` — amortised batching is the perf win, breaking it kills ping bench. **(2)** NEW per-command typed pushes `ReadyGuard::push_<cmd>(...) -> Result<(), PushFailure>` ~88 B (PushFailure carries consumed ReplyId + full ProtocolError so caller can resolve oneshot on Err). **(3)** OPTIONAL `advance_one_frame(&mut wb) -> FeedEvent<'r>` ~88 B per call as SECONDARY API for pipelining 1c-5 forward-compat. **(4)** `FeedEvent<'r>` variants: `Idle, NeedMoreBytes, StreamingRows, SendBytes(&'r [u8]), Deliver(id, Reply<'r>), Fail(id, ProtocolError), Close` — note `SendBytes` (Flaw 1 SCRAM client-final), note `ProtocolError` value-type (Flaw 2 single-slot arena preserved). **(5)** `wb.clear()` at every push entry preserves zeroize discipline (Flaw 4). **(6)** `#[must_use = "PushFailure carries consumed ReplyId; you MUST resolve user's oneshot before discarding"]` on PushFailure (R-5). **(7)** Brand discipline preserved (`with_branded` closure inside each push, no public exposure). **(8)** Action enum kept (used by OutActions from feed_bytes; FeedEvent::Fail variant payload mirrors Action::FailReply at same 88 B). **Honest tier table (after audit):** push surface tier preserved (DEF-198 ReadyGuard + IdleStateProof intact), feed surface tier preserved (`feed_bytes` retained as primary, `advance_one_frame` secondary), single-inflight invariant preserved, brand integrity preserved, zeroize-on-clear preserved, push return frame 800 → ~88 B, feed return frame UNCHANGED 800 B (amortised), Action enum size UNCHANGED 88 B (mirror in FeedEvent). **Bench projections (architect-verified):** push_command/ping_amortised 54 → ~37 ns (-31%); feed_bytes/ping_amortised UNCHANGED; iter_rows/per_row UNCHANGED; iter_rows/select_1k_rows marginal +2%. **Effort estimate (architect-revised):** ~600-900 LoC, ~6-9 days. **`generic_const_exprs` (RU-03) re-verified unstable on Rust 1.95.0** during design phase — still nightly-only (rust-lang/rust#76560, soundness bugs delaying stabilisation since 1.50 era); ruled out as a path. **DEF-213 (Action size reduction) — STATUS: NOT NEEDED.** Architect Flaw 2 finding: keeping ProtocolError value-type means Action::FailReply stays at 88 B; the alternative path of `Box<FailReplyPayload>` (88→32 B + 1 cold alloc) trades cold allocs for stack savings — reject per CREDO §1 (allocations are tier surface; stack frame is amortised at 800 B / 9 actions). **PRINCIPAL DECISIONS (2026-05-04):** ship `advance_one_frame` + `FeedEvent` NOW (Variant A — forward-compat for 1c-5; ~+150 LoC). Calibration: Q1=A (PushFailure pub fields), Q2=regression-only gating (max +3-5% on existing benches; no improvement floor — any positive perf delta is a win), Q3=A (FeedEvent enum with `Idle` ZST variant, exhaustive match preferred over `Option<FeedEvent>`). **IMPL PLAN ARCHITECT-VETTED 2026-05-04 — APPROVE WITH 5 CRITICAL/STRONG MODIFICATIONS + 1 ADVISORY:** **(M1 critical)** add `MAX_OWNED_SEND_LEN >= max_parse_message_size() + 5` const-assert in `write_buf.rs` — sibling to existing Bind+Execute+Sync and Describe+Sync pins; closes tier-4 "happens to fit" gap to tier-1 since Alt Y' appends Sync inline post-impl; **(M2 critical)** `FeedEvent::Fail(id, cause)` semantically implies socket close (documented on variant via `#[must_use]` text) — reserves `FeedEvent::Close` for the no-id case (state→Errored without in-flight reply); resolves the 2-event-per-frame gap (FailReply+CloseSocket pair from install_errored); **(M3 critical)** `FeedEvent<'wb, 'r>` not `FeedEvent<'r>` — `SendBytes(&'wb [u8])` borrows caller's wb; `Deliver(_, Reply<'r>)` borrows proto's row_desc_slot — collapsing to single lifetime forces `'wb = 'r` at use sites and breaks composable patterns (mirrors existing `OutActions<'w, 'r>` two-lifetime contract); **(M4 strong)** exact `==` size pins in `lib.rs` per CREDO §III no-permissive-ranges — `size_of::<PushFailure>() == 80` (NonZeroU64 + ProtocolError 72 B), `size_of::<FeedEvent<'static, 'static>>() == 88` (max variant Fail + tag with niche optimisation); **(M5 strong)** `materialise_push` mirrors `push_within_fanout_budget` discipline — `debug_assert!(false, "...")` dead arm for architecturally-impossible `SendBytesRange.apply == None` case; loud in dev/test, no-op in release; **(M6 advisory)** narrow the wb-residue contract docs — the actual risk window is ONLY between Err return and next push call (push_*_internal does `wb.clear()` at entry, `protocol.rs:1005, 1102`). All 5 modifications upgrade tier-3/4 surfaces to tier-1/2; preserve zero-cost performance (debug_asserts release-no-op; const-asserts compile-time). Plan touch (architect-counted): 12 test files migrate, ~35-50 test fns, 13 bench fns + 1 common helper trait, ~±50-200 LoC delta in tests; total impl ~+450/-100 LoC across 3 atomic commits. **Bench gates (regression-only per Q2):** iter_rows/per_row max +3% from baseline; feed_bytes/ping_amortised max +3%; parse_header max +3%; push_command/ping_amortised max +3% (i.e., not WORSE than today). Improvement floor: NONE — projected -31% on push_command, but any improvement is acceptable (revert only on regression). | Tier preserved on every surface (push, feed, brand, single-inflight, zeroize); 5 surfaces UPGRADED tier-3/4 → tier-1/2 (M1-M5). Push frame -88% (800→88 B). Zero new allocations. Zero new unsafe. | Smallest viable stack frames without breaking amortised batching; opens path to pipelining (DEF-005/006/155) via secondary `advance_one_frame` API. | **PHASE 1 SHIPPED 2026-05-04** — Commit 1 closes Phase 1a/1b/1c/1d atomically. **Foundation (Phase 1a)**: M1 const-assert (`write_buf.rs`), `pub struct PushFailure` with full caller-contract docstring (`action.rs:909`), `materialise_push` helper with M5 dead arms (`protocol.rs:3428+`), `push_command_internal`/`push_bind_execute_internal` return `Result<(), PushFailure>`, `ReadyGuard::push_command`/`push_bind_execute` updated, `BrandedWriteReserved::as_bytes` promoted to non-test `pub(crate)`, `lib.rs` re-exports `PushFailure`, `tests/common/mod.rs` `PushOrPanic` migrated. **Test migration (Phase 1b)**: 12 integration test files migrated (parse_spec, bind_execute_spec, describe_spec, audit_coverage_spec, def198_guard_closure_spec, scram_fuzz_spec, fuzz_stress_spec, row_stream_spec, ping_spec, simple_query_spec, startup_spec, tier_seams_spec) + `protocol.rs` internal `compute_push_tests` module + `tests/common/mod.rs` extended with shared `split_frame_plus_sync` and `split_bind_execute_sync` helpers (DRY across bind_execute and describe). 240+ tests green. **Bench migration (Phase 1c)**: `BenchPushOrPanic` trait switched return from `OutActions<'w,'p>` to `Result<(), PushFailure>` (mirrors production typed-push surface); 8 callsites updated to `let _ = black_box(push_out)` for explicit Result discard. **Validation (Phase 1d)**: `cargo test -p bsql-pg-proto` all green; `cargo clippy --workspace --all-targets --features bench-hooks -- -D warnings` clean (fixed pre-existing `doc_lazy_continuation` lint hits in `protocol.rs::materialise_push` docstring + `parse_spec.rs::parse_setup` docstring — `+`/`—` chars were misclassified as markdown bullet markers). **Bench results — bench gate Q2 PASSED with massive head-room** (regression-only gate, max +3% on existing benches; no improvement floor): `parse_header/rfq_header` 2.52 ns (matches def184-complete baseline — pure function, unaffected); `ping_round_trip/push_then_feed` 113 ns (vs pre-(212) 172 ns post-DEF-189 — −34%); `push_command/ping` 53.5 ns (vs pre-(212) ~99 ns post-DEF-189 — −46%); **`push_command/ping_amortised` 10.28 ns vs pre-(212) 54.17 ns post-PERF-02 — −81%, far exceeded projected −31%**. Iter rows benches preserved within noise (1.41-1.47 µs per 100-row pull = ~14-15 ns/row). The −81% headline reflects DEF-212's structural payoff: 800 B `OutActions` per-call return frame eliminated on push paths (replaced by ~80 B `Result<(), PushFailure>`); per-call materialise overhead removed; `wb`-as-output discipline avoids the borrow-checker friction that pre-(212) tests routed around with scope blocks. **PHASE 2 SHIPPED 2026-05-04** — Commit 2 closes secondary API for 1c-5 forward-compat. **`pub enum FeedEvent<'wb, 'r>`** (action.rs) with 7 variants (Idle, NeedMoreBytes, StreamingRows, SendBytes, Deliver, Fail, Close); `#[non_exhaustive]` for SemVer-safe additions; `#[must_use]` for tier-1 closure on caller side-effect contracts. M2: `Fail(id, cause)` semantically implies socket close (no separate Close event for in-flight Err). M3: two lifetimes preserved (`'wb` for SendBytes, `'r` for Deliver). **`pub fn feed_inbound(&mut self, &[u8]) -> Result<(), ReadBufFull>`** appends inbound bytes to read_buf without dispatching (Errored state silent no-op). **`pub fn advance_one_frame<'w,'r>(&'r mut self, &'w mut WriteBuf) -> FeedEvent<'w,'r>`** processes at-most-one user-observable event; reuses `feed_bytes_bounded(b"", wb, 1)` as driver (single source of truth for dispatch logic preserved — Phase 2 is additive, not a refactor). 10 new tests in `tests/advance_one_frame_spec.rs` cover all 7 FeedEvent variants + equivalence pin (advance_one_frame loop ≡ feed_bytes on canonical Ping/RFQ round trip). **Bench Q2 gate PASSED** (re-verified after first-run noise): `parse_header/rfq_header` 2.59 ns (+2.8% — within gate); `ping_round_trip/push_then_feed` 112.9 ns (-0.4%); `push_command/ping_amortised` 10.19 ns (-1%); zero regressions on iter_rows. **PHASE 3 SHIPPED 2026-05-04** — Commit 3 closes M4 size pins + ship-doc. **Exact `==` size const-asserts** in `lib.rs` (next to existing `ProtocolError == 72`, `Action == 88`, `OutActions == 800`, etc. cohort): `PushFailure == 80` (NonZeroU64 + ProtocolError 72 B); `Option<PushFailure> == 80` (NonZeroU64 niche absorbs discriminant); `FeedEvent<'static,'static> == 88` (max variant Deliver = NonZeroU64 + Reply 80 B); `Option<FeedEvent<'static,'static>> == 88` (NonZeroU64 niche on Deliver/Fail variants). **Send asserts** added for PushFailure + FeedEvent (cross task boundary in Phase 1e wrapper). All four projected sizes match measurement exactly — no relaxation needed; CREDO §III no-permissive-ranges discipline preserved. **DEF-212 SHIPPED end-to-end**: Phase 1 (bytes-only push, -88% return frame, -81% on amortised hot path), Phase 2 (advance_one_frame + FeedEvent secondary API), Phase 3 (M4 size pins). Total: 3 atomic commits + 10 new tests + 13 size const-asserts in 5 of 7 surfaces upgraded tier-3/4 → tier-1/2. **Ready for 1c-5 pipelining work** when principal greenlights — `advance_one_frame` is the forward-compat anchor. |
| DEF-206 | **Box\<PodBytes\<N\>\> heap-scrub gap** — surfaced by DEF-205 step 4 audit. SCRAM state variants `ConnectingScramAwaitingServerFirst` carry `client_first_bare: Box<PodBytes<128>>` and `client_nonce_b64: Box<PodBytes<48>>`. `PodBytes<N>` is `Copy` POD without `Drop`, so when the `Box` deallocates, heap memory is freed but bytes are NOT scrubbed. Severity LOW — content is SCRAM `client-first-message` (username + client nonce), all sent unencrypted on the wire (not actual secrets). | Tier-3 by-audit → reclassified per actual content semantics. | Security gap declassified — bytes are wire-public, no PII / no secrets. | **RESOLVED via PERF-02** (commit `69a86a7`, 2026-05-04). The original SecretPodBytes proposal was Pareto-worse: scrubbing wire-public bytes costs Drop-chain perf for zero security gain. PERF-02 moved both fields INSIDE `ScramSession` with `#[zeroize(skip)]` annotations (`scram/session.rs:99-109`), explicitly classifying them as wire-public. The Box<ScramSession> Drop chain still fires on normal flow; the password is the only actually-secret field and is zeroized by the derive. Audit-2026-05-05 confirmed via grep + read of `scram/session.rs`. |

**Priority order (§1) — REVISED 2026-04-28 after DEF-202 ship + Ext A/C register:**
1. **Decoder perf wins (bench-gated by DEF-197):**
   - ~~**DEF-200**~~ REJECTED 2026-05-07 pre-implementation. Same family as §B A7 (MEASURED REGRESSION). Indirect-call overhead would likely overwhelm any branch-prediction win on Apple M1+ class hardware. See §B for the full analytical + empirical-precedent case.
   - ~~**DEF-207**~~ SHIPPED 2026-05-07 — wider-acc + length-bound + single-end-cast macro (`parse_pg_int_signed_widened!`). Real win: −35.5% on column_decode/iter_5cols_decode_i32 (CI [−42.4%, −30.6%], p=0.00), throughput +55% (102 → 152 Melem/s). See §D Phase 1c entry.
2. **Architectural pre-discussion:** DEF-201 before any code (massive refactor; ≥3 alternatives per architect.txt process).
3. **Zero-cost micro-wins:** ~~DEF-195~~ SHIPPED. ~~DEF-203 (full sweep)~~ SHIPPED — RowDesc/OtherEncoding/FixedStr/PodBytes all migrated 2026-04-28; remaining const-fn collapse waits for RU-01 const-traits stabilisation (Path C inherent-impl duplication shipped as workaround).
4. **Low-severity heap scrub:** DEF-206 (Box<PodBytes> SCRAM client-message scrub — wire-public bytes, low priority).
5. **DEF-202 follow-ups (decoder amortisation, design-discussion-gated):**
   - **DEF-209** — row-level batch UTF-8 validation API. Amortises simdutf8 setup cost across multiple text columns of the same row via `ChunkedUtf8Validator`. **Requires `#![forbid(unsafe_code)]` → `deny + per-site allow` policy decision** before impl. ~12 ns saved on text-heavy 5-col rows. Policy discussion gate per architect.txt.
   - **RU-07** (§C) — `aarch64_neon_prefetch` MSRV watch. Single-line flag flip when `stdarch_aarch64_prefetch` stabilises in stable Rust. Marginal win on long inputs.

**DEF-197 baseline insights logged here for future reference** (so each insight has a tracked owner item, not just bench output):

| # | Observation | Optimisation owner |
|---|-------------|--------------------|
| 1 | Per-row decode ~44 ns on typical 5-col SELECT row. Production scale: 1M rows × 5 cols = 44 ms decode-only — comparable to frame dispatch (14 ms / 1M frames). | Context for DEF-207 + DEF-202 (decoder perf), DEF-200 (dispatch perf). Not standalone DEF. |
| 2 | `<i32 as FromPgText>::from_pg_text` ~7.6 ns/col on 8-digit values. **CORRECTED 2026-04-28**: this is already the optimised `parse_pg_int_signed!` macro from DEF-184 (no UTF-8 validate in the i32 path). Per-digit cost ~0.95 ns. **DEF-207 closed 2026-05-07** with wider-acc macro: 47.46 → 32.89 ns/5-col (−35.5%, p=0.00). Per-col i32 decode now ~6.6 ns (was 9.5 ns at the bench's 5-col 8-digit shape). | (CLOSED — see §D Phase 1c entry for full bench evidence.) |
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
- ~~**`ReadBuf` ringbuffer rewrite** (DEF-058)~~ → **PROMOTED to Phase 5 of v1.0-arch roadmap** (top of §A). Formal slot in execution plan; same scope (lazy-compaction `advance()` becomes wraparound; `&[u8]` API → `(&[u8], &[u8])`).
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

- **DEF-234 — SessionParams bool-pack audit**
  REJECTED 2026-05-05 pre-implementation. Original framing assumed
  `Option<bool>` fields take ~2 B each (Option discriminant + bool
  byte). Empirical verification (`rustc check_size.rs`):
  `Option<bool> = 1 byte` — niche-packed (bool uses bit-patterns 0/1;
  Rust uses 2 as the `None` discriminant value). Two `Option<bool>`
  fields = 2 bytes total. Packing into single u8 saves 1 byte.
  Trade-off: 0.2% reduction of SessionParams (440 B → 439 B);
  cost: typed bit accessors, loss of direct field-access
  ergonomics, new BoolFlags(u8) struct + 4 method impls. **Cost
  side dominates** for trivial space win on a cold-path
  per-connection field. Not Pareto-better.

- **DEF-239 — `FixedStr::default()` init cost optimisation**
  REJECTED 2026-05-05 pre-implementation. Audit identified real
  production callsites:
  - `dispatch.rs:530`, `state.rs:1150/1178/1203`, `protocol.rs:4012`:
    `command_tag: BoundedStr<32>::default()` on every CommandComplete
    handling — ~32 B memset per query completion.
  - `dispatch.rs:1767-1769`: 3× `SecretBoundedStr::default()` in
    `parse_error_response` (cold path, `#[cold]` already applied).
  Optimisation paths surveyed:
  - **`MaybeUninit<[u8; N]>` skip-init**: would require crate-internal
    `unsafe { assume_init }` — breaks `#![forbid(unsafe_code)]`. Same
    SAFE-01 blocker. CREDO §1 absolute commit.
  - **`const EMPTY: Self = Self::new()` rodata constant**: trades
    deterministic memset (1-2 ns) for cache-miss potential (~5 ns).
    Not Pareto-better.
  - **Hand-rolled per-N const init**: same memset cost, no benefit
    over Rust's `[0u8; N]` LLVM intrinsic lowering.
  The current `Default::default()` is **near-optimal for safe Rust**:
  `[0u8; N]` emits the fastest-possible memset on every supported
  target. Real win requires `unsafe`, rejected per CREDO §1.
  Reopen requires either a future stable Rust feature for safe
  `MaybeUninit`-equivalent skip-init, or measurement evidence that
  command_tag init cost is a measurable hot-path bottleneck (the
  bench suite did not surface this on ping_round_trip / iter_rows
  benches).

- **DEF-237 — `record_param_status` const-for-known-keys**
  REJECTED 2026-05-05 pre-implementation. Original framing claimed
  "removes runtime str-match on cold path" — but inspection of
  `SessionParams::set(&mut self, key: &[u8], value: &[u8])` reveals
  the existing implementation is **already compile-time byte-string
  dispatch** via `match key { b"server_encoding" => ..., b"is_superuser"
  => ..., ... }`. Rust/LLVM lowers this to length-first compare chain
  (or jump table at sufficient density), not runtime hashing.
  Possible real wins surveyed:
  - Manual length-first branching: LLVM already does this on `match
    &[u8]` (verified mental model; cargo asm spot-check optional).
  - `phf` crate / hand-rolled perfect hash: adds dependency +
    complexity for marginal gain on a 9-entry dispatch where the
    match-jump-table is already O(1)-ish.
  - First-byte dispatch: same — LLVM optimises transparently.
  ParameterStatus is cold-path traffic (handshake-time only ~9
  frames; mid-session SET-induced PS rare). No measurable user-
  facing benefit available; the proposed refactor would add lines
  without structural improvement.
  **Premise**: original DEF-237 description was based on an
  inaccurate model of the existing code. Match on byte-string
  literals IS the compile-time dispatch DEF-237 was proposing.
  Reopen requires NEW evidence — e.g., cargo asm output showing
  current dispatch is suboptimal, or a workload where PS density
  on hot path is measurable.

- **DEF-268 — `WriteBuf` two-tier lazy-escape (DEF-265 Idea-38 pattern transposed)**
  REJECTED 2026-05-08 post-implementation. Pattern from DEF-265 (Idea-38
  ReadBuf two-tier) attempted on `WriteBuf`: split single
  `heapless::Vec<u8, 2176>` into `inline: heapless::Vec<u8, 256>` +
  `heap: Option<Box<heapless::Vec<u8, 2176>>>` with lazy escape on
  inline overflow.

  **Implementation reached working state**: 429/0/17 tests green, all
  call sites (`with_length_prefix`, `BrandedWriteRange::apply`,
  push_*  methods, fuzz_stress_spec) preserved offset semantics across
  inline→heap escape; specialised `push_u8` fast path bypasses
  `try_extend(&[byte])` and goes directly to `heapless::Vec::push`
  for both inline and heap modes (1-byte memcpy avoidance).

  **Bench-stable vs `phase-d-pre-writebuf-twotier` (single-tier baseline,
  ReadBuf two-tier already shipped):**
  - `push_command/ping`: **+147%** (33.32 ns vs 13.49 ns)
  - `ping_round_trip/push_then_feed`: **+28.5%** (98.61 ns vs 76.75 ns)
  - `iter_rows_via_for_each`: +6.3%
  - `iter_rows_via_next_row`: +4.5%
  - column_decode benches: +0% to +4% (mostly noise band)
  - `iter_rows_via_consume_batch`: -5.9% (improvement from smaller WriteBuf footprint)

  **Two regressions exceed the noise threshold + Pareto-better gate.**

  **Mechanism (post-mortem):**
  1. **Bench-artifact amplification.** `push_command/ping` and
     `ping_round_trip/push_then_feed` create fresh `WriteBuf::new()`
     PER iteration. The two-tier struct's `Option<Box<...>>` Drop has
     side effects (conditional zeroize) that LLVM cannot elide,
     whereas the original single-tier `heapless::Vec`'s Drop on an
     empty buffer (zeroize 0 bytes) was elidable. Per-iter init/Drop
     overhead grows ~20 ns.
  2. **`WriteBuf::clear()` cost grows.** Single-tier was
     `inner.as_mut_slice().zeroize() + inner.clear()` — empty buffer
     = no-op. Two-tier adds `if let Some(heap) ... zeroize()` (cold)
     + `heap = None` (always written). Cumulative ~2-3 cycles per
     `push_command` entry.
  3. **`push_u8` specialisation recovered ~25 ns of the ~45 ns gap**
     (60% recovery), confirming the per-byte memcpy overhead in
     `try_extend(&[byte])` was real, but the residual ~20 ns floor
     comes from the elision loss above — micro-optimisation cannot
     close it without removing the two-tier structure.

  **Different physics from ReadBuf** (why the same pattern wins on
  ReadBuf but loses on WriteBuf):
  - **ReadBuf**: long-lived (one per connection), accumulates inbound
    bytes across many feed cycles, single reset per session boundary.
    Stack-frame footprint reduction (4096→256 B inline) directly
    improves cache locality; the per-cycle inline-vs-heap branch is
    amortised across many byte appends.
  - **WriteBuf**: short-lived in benches (reset per `push_command`
    via `wb.clear()`), many small frames (5-byte Sync, 6-byte
    Describe), fresh-WriteBuf-per-iter pattern in synthetic benches
    amplifies init/Drop overhead. The lazy-escape branch overhead
    on every `push_*` call is NOT amortised — it pays per-push.

  **Production reality vs synthetic bench:** in production, a
  long-lived `WriteBuf` reused across many `push_command` calls
  would see ONLY the per-clear/per-push branch overhead (small,
  amortised), NOT the per-iter init/Drop cost. The
  `iter_rows_via_consume_batch` bench (long-lived buffer, large
  body) showed -5.9% — a real improvement. But the bench-stable
  Pareto-better gate is strict: synthetic benches that regress
  exceed +5% noise threshold count as failures.

  **Reverted clean** (single file, `crates/bsql-pg-proto/src/write_buf.rs`).
  `phase-d-pre-writebuf-twotier` baseline restored as the live shape.

  **Future re-opening requires:**
  - (a) bench evidence that long-lived WriteBuf scenarios dominate
    over fresh-per-iter scenarios in real driver workloads
    (i.e., production-like benches showing net win), AND
  - (b) measurement of per-connection memory savings under realistic
    pool sizes (N=100/1000/10000 connections) showing the 1.9 KB
    per-WriteBuf saving outweighs the synthetic-bench cost, AND
  - (c) a way to recover the ~20 ns elision floor (perhaps via
    `#[inline(always)]` on the entire push chain — but this risks
    other regressions per DEF-263 stack-carve-out exploratory).

  **Generalisable lesson:** the lazy-escape pattern (DEF-265 Idea-38)
  is **NOT universal**. Apply selectively by access pattern:
  - **Long-lived + amortised reset**: pattern wins (ReadBuf).
  - **Short-lived OR reset-heavy**: pattern loses (WriteBuf).
  Don't transpose patterns across structurally different buffers
  without measuring.

- **DEF-267 — γ const-frame templates everywhere**
  REJECTED 2026-05-12 pre-implementation via code-state analysis.
  Original premise: extend DEF-252 const-template pattern from
  parameterless commands (Sync / Flush / Terminate already const
  via `SYNC_WIRE_BYTES` / `FLUSH_WIRE_BYTES` / `TERMINATE_WIRE_BYTES`)
  to Parse / Bind / Execute push surface. Expected: **-10-20 ns
  per push (push_command/ping ~55 → ~35 ns)**.

  **Premise obsoleted by DEF-269 v2** (commit `b38446d` 2026-05-09,
  type-level `CommandKind` dispatch via `trait PushCommand` +
  per-command structs). DEF-269 eliminated the "branchy
  construction" dispatch overhead that DEF-267 targeted — the
  pre-DEF-269 baseline of ~55 ns assumed enum-dispatch per
  PgCommand variant move (2176 B by-value); post-DEF-269 the
  push_command/ping bench is 11.42 ns (DEF-160 SHIPPED entry).

  **Scaled estimate at current state: -1 to -3 ns per push** —
  below the 5% Pareto-better noise threshold on the affected
  bench (push_command/ping with 11 ns total) AND on the
  push_parse / push_bind_execute paths which currently have NO
  bench coverage (target benches would need to be added before
  any measurement could occur).

  **Code-state analysis** (against HEAD `65aa59f`):
  - `build_parse_header` (protocol.rs:3545): tag byte (already
    const TAG_PARSE) + length u32 (per-call computed) +
    stmt_name CSTR (per-call variable, 0-63 B). No const sub-part
    template-extractable beyond what's already done.
  - `build_parse_trailer` (protocol.rs:3572): 3 bytes
    `[NUL, 0, 0]` written via two `push_u8` + `push_i16_be`
    calls. Could become `const PARSE_TRAILER: [u8; 3] = [0, 0, 0]`
    + one `push_bytes`. Savings: ~1 ns per push (one function-call
    boundary, likely already inlined by LLVM).
  - `build_bind_message` (protocol.rs:4047): format-codes preamble
    `[0, 1, 0, 1]` (DEF-184 A14 compact form) is ALREADY const-
    inlined via `push_bytes(&[0, 1, 0, 1])`. The trailing
    `n_result_formats = 0` is one `push_u16_be(0)` — could combine
    with the previous push for ~0.5 ns. The portal-name /
    stmt-name / params payloads are all caller-input variable.
  - `build_execute_message` (protocol.rs:4139): tag (const) +
    length (per-call) + portal-name (variable) + max_rows (per-
    call i32_be). No const sub-part beyond the tag byte.

  **Total realistic savings ceiling: ~1-3 ns per push.** Below
  5% Pareto threshold on a 11 ns baseline. Implementation cost
  (~80 LoC across 4 builder fns + ~50 LoC of wire-byte pin tests
  + ~100 LoC bench harness for push_parse / push_bind_execute)
  exceeds value. Risk medium (touches frame-construction; subtle
  wire-byte regressions = protocol-level data corruption).

  **Generalisable lesson:** queue estimates calibrated against
  prior phase states age out. Re-evaluate impact-per-cost figures
  after each major dispatch refactor. DEF-269 v2 changed the
  premise of multiple downstream items by collapsing the enum-
  dispatch overhead they targeted. Other queue items should be
  re-audited for similar staleness before pursuing.

  **Future re-opening requires:** (a) a different optimization
  shape that targets something other than the now-eliminated
  enum-dispatch overhead, OR (b) bench evidence on push_parse /
  push_bind_execute paths (currently not bench-covered) showing
  > 5 ns per-push waste in the frame-build phase amenable to
  template extraction.

- **DEF-211 SAFE-01 / SAFE-01' — `heapless::Vec` replacement**
  REJECTED 2026-05-04 pre-implementation. Architect-agent
  proposed replacing `heapless::Vec<T, N>` with a hand-rolled safe
  `BoundedVec` / `InlineArr<T, N, L>` shape (eliminating the
  largest transitive `unsafe` surface in the dep graph).
  Multi-round design refinement converged on `InlineArr<T: Copy,
  N, L: BoundedLen<N>>` with type-specific safe wrappers, no
  `unsafe`, per-site optimal len storage via `BoundedLen<N>`.
  **Pre-implementation audit revealed two structural blockers**:

  1. **Per-call init cost catastrophic.** `[T; N]` POD-array
     storage requires eager `T::default()` initialisation of all N
     slots at construction. For per-call types (`StagedActions =
     [StagedAction; 8]` ≈ 704 B; `OutActions = [Action; 9]` ≈ 792
     B) this ships ~700 B memset on EVERY `push_command` /
     `feed_bytes` call. Projected regression on
     `push_command/ping_amortised`: 10.28 → 13–15 ns (+30–50%).
     **Violates Q2 bench gate (max +3% on existing benches).**
     This is the same trade-off `heapless::Vec`'s `MaybeUninit`
     storage solves (zero init writes via `heapless::Vec::new()`)
     — and it's exactly the rationale documented at
     `action.rs:672+` (Pareto-optimal choice analysis: heapless
     wins on Memory-layout + Init-cost + Drop + Safety).

  2. **`MaybeUninit` alternative requires crate-internal
     `unsafe`.** Skipping per-call init via `[MaybeUninit<T>; N]`
     + manual `assume_init_read` is what `heapless::Vec` already
     does inside its audited boundary. Reinventing means adding
     `unsafe` to `bsql-pg-proto` itself — breaks
     `#![forbid(unsafe_code)]` at the architectural-rule level
     (CREDO §1 absolute commit). Net safety position:
     **roughly equivalent or worse** — replaces ecosystem-trusted
     code (`heapless` ≈ 1000 LoC, embedded-Rust standard, 1M+
     downloads/month, no known soundness issues) with our
     locally-audited equivalent that would require its own
     equivalent test/miri/property-test budget to reach the
     same confidence.

  **Existing rationale already documented this analysis** at:
  - `crates/bsql-pg-proto/src/action.rs:672+` — Pareto-optimal
    choice of `heapless::Vec` over POD-array for the per-call
    `StagedActions` / `OutActions` containers (Memory-layout +
    Init-cost + Drop + Safety analysis).
  - `crates/bsql-pg-proto/src/lib.rs:126+` (DEF-211 SAFE-02
    transitive-unsafe audit-trust commentary) — explicit
    statement that the replacement would require crate-internal
    `unsafe`, net worse than ecosystem-trusted heapless.

  **My (architect-driven) plan ignored both citations and built a
  reframed scope (SAFE-01' with type-specific InlineArr) without
  measurement evidence — a CREDO §3 + §4.12 violation.**
  Pre-implementation audit caught it before code change. KEEP
  `heapless::Vec` as the load-bearing choice in our crate.

  **Future audit re-opening requires NEW evidence demonstrating
  EITHER:**
  - (a) per-call types where init cost is acceptable (e.g.,
    bench data showing < 3% overhead under measurement) AND
    a substantive alternative safety win, OR
  - (b) a path to skip-init storage that doesn't require
    crate-internal `unsafe` (e.g., a future Rust language
    feature for `MaybeUninit`-free conditional init, or a
    pair-crate with single audited unsafe boundary that reaches
    the same confidence as heapless).

  Per-connection types (ReadBuf 4 KB, WriteBuf 2 KB,
  CappedServerNonce 256 B, scram-wire intermediates) have
  amortised init cost (one-time per connection). They COULD be
  migrated to safe `[u8; N] + len` without per-call regression,
  but the resulting heterogeneity (some sites InlineArr, some
  heapless) introduces a tax of its own (two patterns, two
  audit budgets). Net call: keep heapless uniformly until / unless
  a clear path to whole-crate replacement emerges.

- **DEF-200 — Per-state-bucket dispatch LUTs**
  REJECTED 2026-05-07 pre-implementation. Original framing
  proposed splitting the global `match (state, tag)` dispatch
  into `[fn; 14]`-per-state-class function tables, hypothesising
  that branch-predictor learns smaller patterns per state.

  **Analytical case against:**
  1. **Indirect-call overhead.** Per-state fn-table dispatch
     introduces an indirect call (load fn-ptr + branch-and-link)
     where the current 2D match emits direct jump-table jumps.
     On Apple M1+ each indirect call is ~2-3 cycles even when
     the BTB is warm; modern LLVM compiles a 2D `match (state,
     tag)` as a discriminant-folded jump table that costs ~1-2
     cycles. Net: indirect-call overhead can **eliminate** the
     hypothesised branch-prediction win.
  2. **BTB capacity is not the bottleneck.** Apple M1+ BTB has
     4096+ entries; current dispatch has 83 reachable arms.
     "Branch predictor can't handle 83 targets" is empirically
     false on this class of hardware.
  3. **`#[cold]` already applied.** `install_errored` (line 152)
     and `parse_error_response` (line 2139) carry `#[cold]
     #[inline]`; LLVM already pushes error-path arms out of the
     hot icache region. The remaining dispatch path is fully
     hot-arm-only; per-state splitting can't extract additional
     cold-vs-hot separation.
  4. **`PROTOCOL_VERSION_3_0` and other dispatch-class
     intermediate steps already optimised** — current arm bodies
     do classification and transition in a single pass; per-state
     fn dispatch would re-do classification implicit in the
     fn-pointer-table index.

  **Empirical case against (PRE-EXISTING evidence in this same
  §B):**
  - **A7 (Tag byte LUT, commit `1a762ca`, 2026-04-24)** is the
    same FAMILY of optimization applied at the tag-classification
    stage: `InboundTagClass` enum + `classify` fn instead of
    sparse byte switch. **Measured regression** on all 4 bench
    groups (+2.6% to +8.2%, p<0.05). The §B postmortem reads:
    "LLVM's sparse-byte switch beats dense-enum form; classify
    step adds indirection not foldable. Hypothesis 'dense
    discriminant jump table wins' falsified on modern LLVM."
    **DEF-200 is the same hypothesis** applied at the
    state-dispatch stage instead of the tag-classification stage.
  - **A4/B16 (cache-line layout reorder via `#[repr(C)]`,
    2026-04-24)** — manual hint to LLVM about layout: regressed
    parse_header +6.3%, push_command +3.8%, iter_rows +1.6%.
    Same lesson: modern LLVM + Rust default beats manual layout
    hints.
  - **W3 (parse_header range-pattern match, 2026-04-24)** —
    range pattern instead of sequential if-guards: +70% on
    parse_header. Same lesson: structural rewrites of well-tuned
    dispatch code regress.

  Three independent measurements in the same dispatch-perf
  domain produced regressions. DEF-200 is in the same family;
  expected outcome: regression.

  **What WOULD be required to reopen:**
  - A new measurement framework that isolates dispatch cost
    (not bundled into ping_round_trip / iter_rows). The current
    bench harness measures end-to-end, so a 3% dispatch
    improvement is below detection floor when wrapped in a
    150-ns full cycle.
  - Hardware where BTB capacity IS the bottleneck (embedded
    targets with 16-32 entry BTBs). Out of v1.0 target hardware.
  - PGO data showing branch-misprediction rates on the current
    dispatch — feature not in our build infrastructure.
  - A structural variant that AVOIDS the indirect-call cost
    entirely (e.g., LLVM-friendly `match` rotation by tag-first
    instead of state-first — Variant B from session analysis,
    a design discussion before code).

  No code shipped; no measurement run beyond the analytical
  case. The §A row was downgraded to "REJECTED — see §B" and
  the priority-order section updated.

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

### v1.0-arch roadmap (2026-05-08 cycle)
- **Phase 1 cluster (DEF-249/251/252/254/256)** — `2f63897` 2026-05-08: 5-15% across hot path. Bench evidence in commit message: parse_header −8.80%, ping_round_trip −6.03%, iter_rows_via_next_event −5.97%, iter_rows_via_next_row −14.53%. CPU-time ratio 0.992. Zero regressions vs `survey-2026-05-08`. DEF-256 sealed-trait + `#[non_exhaustive]` sweep bundled.
- **Phase 2 — DEF-250 SWAR opt-in helper** — `e098dca` 2026-05-08: `pub fn parse_short_uint_swar(&[u8]) -> Option<u32>` for 1-4 ASCII digits unsigned. 14.94 ns vs 31.24 ns generic = **2.09× on 4-digit shape**. Two prior in-body attempts (`#[inline(always)]` and additive prologue) regressed adjacent benches via LLVM heuristic shifts (forensics at `/tmp/asm-attempt{1,2}-i32.s`); structural rethink moved SWAR out of `from_pg_text` body — lib `.s` MD5 byte-identical to HEAD `2f63897`. Pareto-better gate cleared. **Generalisable pattern unlocked**: caller-known optimisations as opt-in helpers, never embedded in shared dispatch.
- **Phase 3 — DEF-253 cache-friendly hot-field reorder** — `523b017` 2026-05-08: AUDIT-CLOSED no-op. Per-row hot path (after `cached_reply_id` first-row classification) touches only `read_buf.cursor` (u16) and sequentially-scanned `read_buf.inner` bytes — neither improvable by struct reorder. Three alternatives all rejected: `#[repr(C)]` repeats failed A4/B16, duplicate-newtype manufactures tier-4 silent-divergence (CREDO §1), split-cursor breaks tier-2 invariant. Architect note: PGO is the structural fix for "manual layout vs LLVM defaults" failure class.
- **DEF-259 — DropCounter zeroize verification** — `d7d8532` 2026-05-08: Tier-2 by-discipline → tier-1 by-construction. 9 secret-bearing types covered (Password, Sensitive<T>, ScramSession, SecretDigest, Md5HandshakeState, ErrorPayload, SecretBoundedStr<N>, ReadBufN<N>, WriteBuf). Mechanism: `cfg(test)` `DropCounter<T>` newtype + sealed `CrateZeroizeSecret` manifest + source-grep exhaustiveness gate at `tests/zeroize_coverage_spec.rs`. Adding a new secret type without manifest entry fails `cargo test` deterministically. Negative-tested by transient `FakeUnmanifestedSecret` injection + revert. Tests 420 → 447 (+27, ignored unchanged at 17). Production paths byte-identical (release build pre/post identical).
- **DEF-269 v2 — Type-level CommandKind dispatch (T)** — 2026-05-09. Architectural breakthrough replacing the runtime `PgCommand` enum (~2176 B sized to its largest variant `Parse`) with per-command structs implementing a sealed [`PushCommand`] trait. Each `proto.as_ready()?.push_command(Ping { reply }, &mut wb)` now moves only 16 B by value (vs 2176 B for the legacy `PgCommand::Ping { reply }`); BindExecute moves only its parameter-typed size; etc. **Two-architect-round design process**: round 1 proposed M (closure-scope SendList) targeting the +13% regression from rejected DEF-269 v1; round 2 audit found the TRUE root cause was PgCommand enum size, not WriteBuf placement. Architect predicted T alone → -18..-22% on synthetic Ping; reality 4× better. **Bench evidence vs `survey-2026-05-08` baseline (clean re-run on calmer system, p=0.00)**: `push_command/ping` **−83.4%** (~55 ns → 9.08 ns, **6× speedup, the synthetic-init bench's effective floor**); `ping_round_trip/push_then_feed` **−12.3%** (~118 ns → 102.94 ns); `parse_header/rfq_header` −12.3%; `iter_rows_via_for_each` **−9.65%**; `iter_rows_via_consume_batch` improved; `iter_rows_via_next_event/_next_row/_next_row_bytes` improved; `column_decode` benches all neutral or improved. Zero structural regressions (initial full bench reported 2 noise-class regressions on unrelated pull-side benches; targeted re-bench on calmer system showed −9.65% improvement on one and noise on the other — both reverted to clean wins, confirming measurement artifacts). **Implementation**: new `crates/bsql-pg-proto/src/push_command.rs` (~430 LoC) with sealed `PushCommand` trait + 7 per-command structs (`Ping`, `Startup`, `SimpleQuery`, `Parse`, `DescribeStatement`, `DescribePortal`, `BindExecute<'a, P: ParamsWriter>`). `ReadyGuard::push_command<C: PushCommand>(self, cmd: C, wb: &mut WriteBuf) -> Result<(), PushFailure>` is generic + monomorphised. `ReadyGuard::push_bind_execute` retained as thin convenience wrapper that constructs `BindExecute { ... }` and dispatches. `PgCommand` enum demoted to `pub(crate)` (still exists for the lib-internal `compute_push_tests` mod + the legacy `impl PushCommand for PgCommand` blanket impl — slow path, not used by external callers). 50 callsites migrated across 14 test/bench files. **Tier-1 invariants preserved**: IdleStateProof witness (DEF-198) unchanged — `IdleStateProof::new()` relaxed from module-private to `pub(crate)` only so `protocol::push_command_internal<C>` can synthesise the witness when re-entering through generic dispatch (the inner `_IdleProofMarker` ZST stays module-private; external crates cannot fabricate the proof). Sealed `PushCommandSealed` super-trait keeps the trait closed. **API breaking change** accepted per principal directive 2026-05-08 (v1.0 alpha — no SemVer compatibility). 431/0 tests green, clippy clean across lib/tests/benches/workspace, DEF-259 zeroize manifest unchanged. **Generalisable lesson**: when an enum's largest variant size dominates dispatch cost, type-level dispatch via per-variant structs + sealed trait monomorphisation eliminates the cost by-construction. Architect's 2-round audit process (round 1 finding M, round 2 finding T as breakthrough) modelled the principal's "first or second finding will not be the best" directive — pushing past the obvious answer to the deeper structural cause yielded a 4× gain over the predicted improvement.

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
- **DEF-214 (Phase 1 wire bytes + Phase 2 response classifier)**: SSLRequest pre-startup primitive. **Phase 1 (commit `6187a41`, 2026-05-05)**: `wire::SSL_REQUEST_VERSION = 80_877_103` (PG magic-version sentinel: 1234 << 16 | 5679) + `wire::SSL_REQUEST_WIRE_BYTES: [u8;8] = [0,0,0,8, 0x04,0xd2,0x16,0x2f]`. 5 `const _: () = assert!(...)` drift-pins including the load-bearing `SSL_REQUEST_VERSION.to_be_bytes()` formula pin. **Phase 2 (this commit, 2026-05-07)**: typed 1-byte response classification. `wire::SslNegotiationOutcome` enum (`#[non_exhaustive]`) with 4 variants — `Accepted` (server byte `'S'` → driver does TLS handshake), `Refused` (`'N'` → fallback per sslmode policy), `ErrorIncoming` (`'E'` → ErrorResponse frame follows), `InvalidByte(u8)` (anything else → fatal protocol violation). `wire::classify_ssl_response_byte(byte: u8) -> SslNegotiationOutcome` (`const fn`, no panics, no allocs, `#[inline]`); 5 const-block round-trip pins inside wire.rs verify S/N/E/0x00/0xff classify correctly at compile time. Top-level re-exports of both the enum and the fn for driver ergonomics. **Tests** (`tests/ssl_request_wire_spec.rs`, 15 total): Phase 1 — byte-by-byte spec match, top-level vs module path agreement, length-field self-inclusion, distinctness from TERMINATE/Sync/PROTOCOL_VERSION_3_0, magic-version 1234<<16 decomposition; Phase 2 — `'S'`/`'N'`/`'E'` happy paths, exhaustive 0..=255 sweep verifying every undefined byte produces `InvalidByte(payload)` with payload preservation, boundary-byte coverage (0x00/0xFF/0x80/0x7F/0x01/0xFE — classic interpretation-error sources), PartialEq semantics across same-variant + different-payload cases, `#[non_exhaustive]` catch-all requirement (catches future PG-spec extensions safely). **Tier impact**: pre-Phase 2 drivers carried ad-hoc `match byte { b'S' => ..., _ => ... }` at every call site (tier-3 by-discipline; forgetting a branch silently mishandled); post-Phase 2 the dispatch is tier-1 typed enum. **State-machine integration remains Phase 1e** — the response byte is genuinely OOB (no tag, no length prefix, doesn't fit our tagged-frame parser); the driver handles SSL probe in its own pre-TLS reader, classifies via the new fn, then constructs `PgProtocol::new()` for the post-TLS phase. DEF-217 SCRAM channel-binding remains separately blocked on rustls cert-hash plumbing. SHIPPED 2026-05-07.
- **DEF-216 (full flow)**: MD5-password auth (R/5) — full handshake support. Sibling to DEF-215; same architectural shape (third path alongside Trust + SCRAM + Cleartext) with crypto-isolating module `crate::md5`. New workspace dep `md-5 = "0.10"` (RustCrypto, default-features = false, no_std, audit-trust profile matches `sha2`/`hmac`/`pbkdf2`). New `crate::md5::Md5HandshakeState` bundles `Sensitive<Password>` + `Ident` under one Box (mirror SCRAM PERF-02 single-Box; ZeroizeOnDrop chain through Box::drop → Sensitive::drop → Password::drop; username non-secret, not zeroized). New `Credentials::Md5Password(Sensitive<Password>)` with redacting Debug. Two new `ProtoState` variants — `ConnectingStartupMd5 { reply, handshake: Box<Md5HandshakeState> }` (~24 B post-Box, well under 80 B size pin) + `ConnectingMd5AwaitingAuthOk(reply)`. Eight new dispatch entry-match arms (4 tags × 2 states) + 2 dispatcher fns. `compute_response_body` performs `md5("md5" || md5_hex(md5_hex(password || username) || raw_4_byte_salt))` with **every** password-derived intermediate (inner_digest 16 B, inner_hex 32 B, outer_digest 16 B, outer_hex 32 B) wrapped in `Zeroizing<>` so the stack copy scrubs on Drop independently of the wire-buffer's own zeroize discipline. Salt validation: `<[u8; 4]>::try_from(rest)` rejects any non-4-byte payload as `MalformedAuthentication`. Tier-1 exhaustive: AuthSubCode match in MD5 dispatcher rejects every non-Md5Password code as `KnownButWrong` (downgrade-rejection mirror of SCRAM/cleartext dispatchers). 3 lib unit tests (smoke + algorithm-shape pin (pw||user, NOT user||pw — a swap regression silently authenticates wrong against canonical PG) + hex-encoding known-vectors all-zeros + `deadbeef00112233445566778899aabb`) + 4 integration tests in `tests/startup_spec.rs` (end-to-end with byte-by-byte cross-verification against independent reference computation using the same `md-5` crate, malformed-salt 3-byte rejection, downgrade-rejection of cleartext offer, Debug-redaction). Lib `forbid` bundle compliant: tests use `Result<(), &'static str>` with `?`-propagation instead of `panic!`/`unwrap` since lib-level forbids those even in `#[cfg(test)]`. **Security profile**: MD5 is broken for collision uses; PG salt+rehash protects only against passive observation, offline GPU cracking is fast. The MD5 algorithm is the protocol's weakness — our implementation uses the proven RustCrypto crate per CREDO §11. Phase 1e wrapper SHOULD prefer SCRAM where the server offers both. SHIPPED 2026-05-05.
- **DEF-215 (full flow)**: Cleartext-password auth (R/3) — full handshake support. New `Credentials::CleartextPassword(Sensitive<Password>)` enum variant with redacting Debug (DEF-048 pattern). Two new `ProtoState` variants — `ConnectingStartupCleartext { reply, password: Box<Sensitive<Password>> }` (Box per size-pin discipline; mirrors SCRAM PERF-02 single-Box pattern with `ZeroizeOnDrop` chain through `Box::drop → Sensitive::drop → Password::drop`) + `ConnectingCleartextAwaitingAuthOk(reply)`. Four new dispatch entry arms cover both states across `TAG_AUTHENTICATION` / `TAG_ERROR_RESPONSE` / `TAG_NEGOTIATE_PROTOCOL_VERSION` / unexpected. Two new dispatcher fns: `dispatch_auth_in_startup_cleartext` (tier-1 exhaustive — accepts only sub-code 3, rejects all other AuthSubCode variants as `KnownButWrong` per security downgrade-rejection policy mirror of SCRAM dispatcher) + `dispatch_auth_ok_after_cleartext` (only AuthOk legal). New wire builder `build_password_message` (tag 'p' + BE u32 length + password bytes + trailing NUL; uses `From<WriteBufFull> for ProtocolError` for ?-propagation matching other branded builders). Integration tests in `tests/startup_spec.rs`: end-to-end happy path validates frame shape (tag + length-field + password bytes + NUL terminator), ErrorResponse mid-handshake produces `FailReply + CloseSocket`, server SASL offer mid-cleartext-startup rejected as `UnsupportedAuthMethod::KnownButWrong(Sasl)` (downgrade-prevention security pin), `Credentials::CleartextPassword` Debug must contain `REDACTED` and not raw password text. ProtoState size pin (`== 80`) preserved — `DescribeStatementAwaitingRfq` remains dominant variant; new variants are 16-24 B. **Security caveat**: cleartext password travels unencrypted on the wire; the driver-wrapper (Phase 1e `bsql-driver-postgres`) is responsible for refusing `Credentials::CleartextPassword` constructs on non-TLS connections — `bsql-pg-proto` itself does not gate the policy (no I/O knowledge). DEF-214 (TLS / SSLRequest pre-startup) is the prerequisite for the wrapper's gate. SHIPPED 2026-05-05.
- **DEF-223 (wire-bytes phase)**: Terminate ('X') frontend graceful-close primitive. `wire::TAG_TERMINATE = OutboundTag(b'X')` + `wire::TERMINATE_WIRE_BYTES: [u8;5] = [b'X', 0, 0, 0, 4]` (PG §55.7 frame). Tier-1 closure: 6 `const _: () = assert!(...)` drift-pins (length + tag literal + length-field bytes + `assert_all_distinct!` outbound list + per-tag drift-pin block in wire.rs). Top-level re-export `bsql_pg_proto::TERMINATE_WIRE_BYTES` for driver ergonomics. `tests/terminate_wire_spec.rs` 3 runtime tests + 3 const-asserts pin the public-API visibility (top-level re-export equals module path, distinct from Sync) from a downstream crate's POV — internal drift-pins cannot catch a `pub` → `pub(crate)` regression of the re-export, this file does. Mirrors the SYNC_WIRE_BYTES pattern (5-byte parameter-free outbound frame) — `Flush` is a sibling candidate for the same treatment in a future audit. **State-machine integration pending Phase 1e** (`Action::Terminate`, `ProtoState::Closed` variant, `ConnectionStatus` reporting); drivers can write the bytes directly today on graceful close. SHIPPED 2026-05-05.
- **DEF-236**: `#[inline]` audit on protocol-hot-path classifier/materialise pair. ASM-driven (revert-vs-inlined `.s` diff): (a) `allows_unsolicited_param_status` + `allows_unsolicited_notice_response` (tiny one-liners) — LLVM already transparently inlines without hint; `#[inline]` applied for explicit intent + future-heuristic-shift pinning. (b) `materialise_push` (single call site `push_command_internal`) — LLVM takes the hint, standalone symbol vanishes in inlined ASM; `#[inline]` applied (codegen evidence shows real fold-in). (c) `materialise` (4 call sites in `feed_bytes_impl` arms) — LLVM rejects the hint (`bl` to standalone symbol persists at all 4 sites; body too large for net code bloat at 4 sites); NO `#[inline]` annotation, comment-only documents the audit finding so future contributors don't re-attempt. Bench measurement (load avg 4.0, 138% CPU) inconclusive — sign flipped across 3 runs on identical code state, pure noise. Conclusion stands on **codegen evidence** (LLVM's accept/reject decision), not bench: explicit annotation where LLVM accepts, comment where LLVM rejects, no decoration anywhere. Reopen path: PGO data, or quiet-bench environment showing reproducible win. SHIPPED 2026-05-05.
- **DEF-207**: Wider-accumulator + length-bound + single-end-cast variant of `parse_pg_int_signed!` shipped as `parse_pg_int_signed_widened!($bytes, $result, $acc, $max_digits)`. Per-digit branch budget collapses 3 → 1 (digit-validation only); 10-digit i32 path: 30 → 12 branches total. Used by `i16` (i32 acc, 5-digit cap — i16::MAX = 32767 = 5 digits) and `i32` (i64 acc, 10-digit cap — i32::MAX = 2_147_483_647 = 10 digits). `i64` retained on original checked-arithmetic `parse_pg_int_signed!` path because i128 acc compiles to multi-instruction sequences on 64-bit native targets, losing the speed gain. **Bench evidence (column_decode/iter_5cols_decode_i32, criterion baseline before-def207 → compare):** 47.46 ns → 32.89 ns median, **−35.5% (CI [−42.4%, −30.6%], p=0.00)**, throughput +55% (102 → 152 Melem/s). Beats deferred.md's original "~30% speculative" estimate. Bench-cpu-time wrap during compare reported ratio 0.934 (WARN — minor scheduler interference; signal magnitude 7× exceeds the noise band). bench-allocs `compare initial-clean` confirmed all 5 alloc_counts scenarios unchanged at zero allocs. Correctness preservation: 4/4 from_pg_text test groups pass (existing boundary suite covers i32::MAX, i32::MIN, +/-overflow, empty, non-digit, multi-byte non-ASCII, embedded NUL). Tier preserved (runtime parse → tier-3 by classified `IntParse`). Length pre-check + i64 acc bound is a structural correctness pin: `wrapping_mul(10).wrapping_add(9)` provably cannot wrap during the loop given the bound (max acc reach for 10-digit i32 = 9_999_999_999 << i64::MAX ≈ 9.22 × 10^18). SHIPPED 2026-05-07.

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

## §G. Linux-Transition Blockers

Items deferred because they require Linux as the development /
benchmarking host. Principal is migrating to Linux as primary OS
in the near term (registered 2026-05-07); this section is the
single grep-point for **"при переходе на Linux надо будет сделать X
и не забыть проверить Y"**. Mirror of §C (Rust-Unstable Blockers)
in spirit — deferred until external condition (OS migration)
clears the blocker.

**Discipline (when adding entries):** every row must specify
(a) what's blocked on Linux, (b) why macOS can't deliver it
today, (c) the action on transition (precise enough that a future
session can execute without re-deriving the rationale).

### LIN-01 — `iai-callgrind` instruction-level deterministic bench

- **Tracking:** github.com/iai-callgrind/iai-callgrind (active,
  primary maintenance on Linux).
- **What's blocked:** sub-1% precision performance regression
  detection. `criterion` (used today via
  `scripts/bench-stable.sh`) has a ±5% statistical noise floor on
  consumer hardware; `iai-callgrind` delivers **deterministic
  instruction counts** via Valgrind's Cachegrind tool — same
  binary, same input → exact same instruction count. Sub-1%
  precision becomes meaningful (1 instruction is 1 instruction).
- **Why macOS can't deliver:** Valgrind upstream (and Cachegrind
  by extension) does not run reliably on macOS post-Catalina —
  Apple's tightened sandboxing + the macOS dynamic loader changes
  break Valgrind's process-injection model. `iai-callgrind` only
  ships supported binaries for Linux. Apple Silicon's userspace
  cycle counter (`pmccntr_el0`) is privileged-only; even if
  Valgrind ran, the instruction-count alternative is the right
  primitive on Linux.
- **Action on transition:**
  1. Add `iai-callgrind = "0.x"` to `[dev-dependencies]` of
     `bsql-pg-proto` (workspace-relocatable).
  2. Create `crates/bsql-pg-proto/benches/instruction_counts.rs`
     with iai-callgrind harness mirroring the criterion bench
     groups (parse_header / push_command / feed_bytes /
     iter_rows).
  3. Extend `scripts/bench-stable.sh` (or add sibling
     `scripts/bench-icount.sh`) to run iai-callgrind alongside
     criterion — orthogonal layers (instructions = deterministic;
     ns/op = scheduler-affected statistical).
  4. Update `BENCHMARKING.md` Tool 3 section + `reforge.md §96a`
     verification stack to include the icount layer.
- **Verification on transition:** before any perf claim, run BOTH
  criterion (statistical) AND iai-callgrind (deterministic) — the
  pair tightens the existing rule from "ASM-diff + bench-stable"
  to "ASM-diff + icount + bench-stable" (3 layers, each with
  different precision/scope tradeoff).

### LIN-02 — `heaptrack` allocation traffic profiler

- **Tracking:** github.com/KDE/heaptrack
- **What's blocked:** allocation-traffic visibility on hot paths.
  The proposed cross-platform allocation-counter (B path,
  scheduled for ship pre-Linux migration) reports counts via
  `GlobalAlloc` wrapper — sufficient for "this hot path allocs N
  times" claims. `heaptrack` adds: per-callsite allocation
  histogram, peak-RSS tracking with backtrace, leak detection at
  process exit, flamegraph-style allocation profile.
- **Why macOS can't deliver:** `heaptrack` uses `LD_PRELOAD` +
  Linux-specific `/proc/self/maps` parsing for the allocation
  interceptor and backtrace symbolisation. macOS uses
  `DYLD_INSERT_LIBRARIES` (similar but not API-compatible) and
  has no `/proc`-style introspection — port would require a full
  rewrite of the interception layer. `Instruments.app
  Allocations` template covers some of this on macOS but doesn't
  produce machine-readable output for CI gating.
- **Action on transition:**
  1. Verify `heaptrack` package available in the chosen distro
     (Ubuntu/Fedora/Arch all ship it).
  2. Add `scripts/bench-heaptrack.sh` wrapping
     `heaptrack cargo bench -p bsql-pg-proto -- --bench BENCH`
     with output parsing → "allocs per bench iteration"
     comparison vs saved baseline (mirror of bench-stable
     save/compare pattern).
  3. Document the layer in `BENCHMARKING.md` as an opt-in audit
     tool (not part of the mandatory pre-ship verification —
     reserved for "is this allocation pattern actually
     improving?" investigations).

### LIN-03 — `perf stat` cycle / cache / branch counters

- **Tracking:** Linux kernel `perf_event_open(2)` syscall + the
  `perf` userspace tool.
- **What's blocked:** real CPU-cycle counts (not nanoseconds),
  L1/L2/L3 cache miss rates, branch misprediction rates,
  hardware prefetch counts. Apple Silicon exposes none of these
  to userspace without entitled developer keys (Apple's
  `pmccntr_el0` is EL0-readable-disabled by default;
  `cntvct_el0` virtual counter is 24 MHz coarse — useless for
  cycle-level perf). On Linux, `perf stat` reads the same
  hardware PMU registers via the kernel and serves them in a
  single CLI call.
- **Why macOS can't deliver:** Apple ships
  `xcrun xctrace record --template 'CPU Counters'` via the
  Instruments family — closed-source, no machine-readable export
  for CI, requires GUI-driven session config. Some events
  available via DTrace if the System Integrity Protection is
  partially disabled (impractical for daily dev). On Apple
  Silicon, a subset of PMC events is accessible only through
  signed-with-entitlement processes; rejecting that path keeps
  the build reproducible without provisioning profiles.
- **Action on transition:**
  1. Confirm hardware-PMU access on the new Linux host
     (`perf stat true` returns counters without `<not counted>`
     for cycles/instructions/cache-misses/branch-misses).
  2. Add `scripts/bench-perfstat.sh` wrapping
     `perf stat -e cycles,instructions,cache-misses,branch-misses
     cargo bench -p bsql-pg-proto -- --bench BENCH` with stable
     parsing of the textual `perf stat` output (the JSON output
     mode is `--json` since perf 5.18+).
  3. The CPU-time-vs-wall-clock metric (C path, scheduled for
     ship pre-Linux migration via `getrusage`) gives a coarse
     read of "scheduler-fairness signal". `perf stat` is the
     fine-grained version — pair the two for the full picture.
- **Verification on transition:** confirm cycle counts match
  expected scale (e.g., parse_header bench at ~2.5 ns at 3.5 GHz
  = ~9 cycles per iteration; if `perf stat` reports 1000+ cycles
  the harness is broken).

### LIN-99 — Generic Linux-transition checklist (extend as needed)

Reserved for items discovered during transition that don't fit
LIN-01..LIN-98. Add a new LIN-NN row above this one when you
spot something. Example placeholder shape:

- **What:** `<thing that needs doing or verifying on Linux>`
- **Why deferred:** `<reason it can't be done on macOS today>`
- **Action on transition:** `<concrete steps>`
- **Don't forget to verify:** `<post-action sanity check>`

This bullet list is intentionally meta — `LIN-99` exists so a
future "при переходе на Linux надо будет сделать X" item never
gets dropped in chat between sessions. Capture immediately, fill
out the four lines above, register a stable LIN-NN ID, ship later.

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
