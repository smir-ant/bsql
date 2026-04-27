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
- **Blocks:**
  - DEF-141 infallible `build_*_message` via type-level capacity
    witness.
  - B14 HList `ParamsWriter` with `FORMATS: &'static [_; N]`
    computed from tuple arity.
  - `OutActions::push_infallible<const IDX>` with compile-asserted
    bound check.
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
