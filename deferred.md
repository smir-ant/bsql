# Deferred items registry

Living document tracking every **deliberate deferral, tier downgrade, and
interim tool** decision taken during v1.0 construction. Every item here
has a phase-trigger (when we revisit it), a verification hook (what will
tell us we can close the deferral), and a current-tier vs target-tier
claim.

If you are making a decision that touches any item on this list,
**check it first** — the original context may be load-bearing. If you
close a deferral, remove the entry or move it to `## Closed`.

Grep conventions: every row has a stable `DEF-NNN` ID; reference these
in commits, code comments, and CHANGELOG entries.

## 1. Manufactured state variants — deferred until driver code lands

Per CREDO §0 ban-on-manufactured-variants (reforge.md §4.6): a
`ProtoState` variant lands **only** in the commit that ships both its
entry path and its exit path. Phase 1a ships `Idle` and
`AwaitingPingReply(ReplyId)` only.

| ID | Variant | Arrives in | Lands with |
|---|---|---|---|
| DEF-001 | `ConnectingStartup { reply, credentials }` | 1b | **CLOSED** — landed with entry+exit in Phase 1b (6382cdc) |
| DEF-002 | `ConnectingScramAwaitServerFirst/Final/AuthOk` | 1b | **CLOSED** — three SCRAM variants landed with full SCRAM flow (6382cdc) |
| DEF-003 | `ConnectingPostAuthWaitKey(ReplyId)` | 1b | **CLOSED** — landed with post-auth chain (6382cdc) |
| DEF-004 | `ConnectingPostAuthHaveKey { reply, pid, secret_key }` | 1b | **CLOSED** — landed with BackendKeyData + RFQ (6382cdc) |
| DEF-005 | `AwaitingQueryReply { reply, hash, columns }` | 1c | Query / Execute flow |
| DEF-006 | `StreamingRows { stream, hash, columns }` | 1d | QueryStream |
| DEF-007 | `InTransaction { level, depth }` | 1c | Begin/Commit/Rollback |
| DEF-008 | `Errored(ProtocolError)` | every flow | **CLOSED** — entry from all Phase 1a+1b states verified (6382cdc) |
| DEF-009 | `Closed` | 1e | Terminate flow / async wrapper shutdown |

**Verification hook:** before closing any ID, confirm the commit that
adds the variant **also** adds (a) an entry path from another state,
(b) an exit path to another state or reply delivery, (c) tests that
traverse both paths.

## 2. Capacity budgets — re-audit on every new push site

`MAX_ACTIONS_PER_CALL` is the const-asserted ceiling on actions emitted
per `push_command` / `feed_bytes` call. Over-budget push **does not
panic** — it returns `Err(Action)`; Phase 1a's callers take the Err
branch but the branch is currently dead by audit, not by compile-time
proof.

| ID | Constant | Phase 1a value | Worst case today | Gate today | Tier today |
|---|---|---|---|---|---|
| DEF-010 | `MAX_ACTIONS_PER_CALL` | 4 | 2 (FailReply + CloseSocket on malformed frame) | Single global `const _: () = assert!(MAX >= 2)` | 2 (global assert, not per-site) |
| DEF-011 | `READ_BUF_CAP` | 4096 (4 KiB) | frame up to 4095 bytes | Drift-detection `const _: () = assert!(cap == MAX_FRAME_LEN_FIELD + 1)` | 1 (compile error on drift) |
| DEF-012 | `MAX_OWNED_SEND_LEN` | 1b | 512 bytes | **CLOSED** — sized for worst-case StartupMessage (690e30e) | 1 (const_assert >= 297) |
| DEF-013 | `SendBuf::Owned` variant | 1b | StartupMessage + SASL frames | **CLOSED** — landed (6382cdc) | 1 (exhaustive match) |
| DEF-014 | `WriteBuf` newtype | 1b | runtime-built outbound frames | **CLOSED** — landed (690e30e) | 2 (sealed API surface) |

**Verification hook for DEF-010:** before merging any PR that adds a
new `push_action` call site, confirm (a) the per-method-budget
docstring is updated, (b) a local `const _: () = assert!(MAX_ACTIONS_PER_CALL
>= NEW_LOCAL_BUDGET);` sits at the site, (c) if the new sum-of-sites
would exceed current MAX, the MAX is bumped in the same PR.

**Upgrade target for DEF-010** (tier 2 → tier 1): derive
MAX_ACTIONS_PER_CALL via a `const fn` that sums per-site budgets, or
a proc-macro that computes it from `#[budget(N)]` attributes. See
DEF-020.

## 3. Tier downgrades — honest tier-2 claims that reforge.md labeled tier-1

Each row documents an invariant that **reforge.md spec claimed at tier
1** but which, on stable Rust today, is actually **tier 2** (no path in
our code today, but compiler does not guard). Listed with the upgrade
path to real tier-1 when it becomes feasible.

| ID | Invariant | Current tier | Why not tier-1 today | Path to tier-1 |
|---|---|---|---|---|
| DEF-015 | State correlator (`ReplyId`) is consumed when transitioning from `AwaitingPingReply` | 2 — audit | Rust forces you to *name* the field in `match`, but not to *use* it (`drop(id)` / `_ => ...` both compile). No `#[must_use]` on enum-variant fields. No linear types. | Linear types (Rust RFC, no date); or `#[must_use]` on variant fields (no RFC yet); or drop `ReplyId: Copy` + wrap in a consume-only newtype. |
| DEF-016 | Sealed traits (`pub mod private { pub trait Sealed {} }`) | 2 — audit | Simple pattern is defeatable: external crate can `impl OurCrate::private::Sealed for TheirType` (orphan rules permit). | **Token pattern**: `pub struct PrivToken { _priv: () }` inside private mod; `Sealed` requires method returning `PrivToken`; external crates cannot construct the token. Tier-1. Apply at every seal point. |
| DEF-017 | DoS via length amplification is STRUCTURALLY UNREACHABLE | 2 — arrangement | True *today* because `parse_header` rejects `declared > MAX_FRAME_LEN_FIELD` before any allocation. A future refactor moving the check after the buffer-grow would silently regress. | Introduce `CappedLen<const N: u32>(u32)` phantom-wrapper, constructible only via `try_from_capped`. Make `ReadBuf::grow_to` accept only `CappedLen`. Ordering becomes type-level. |

**Verification hook:** when Rust stabilises a feature that changes any
of these assessments (linear types, field `#[must_use]`, sealing RFC),
revisit the rows and upgrade. Write the upgrade commit against the
matching ID.

## 4. Interim tooling — replace when permanent lands

| ID | Interim form | Permanent form | Replace in |
|---|---|---|---|
| DEF-018 | Hand-rolled SplitMix64 PRNG + `for _ in 0..100_000 { parse(bytes) }` loop in `tests/frame_parse.rs` | `proptest` with strategy definitions and shrinking; `cargo-fuzz` for corpus-guided continuous fuzzing | **Phase 6** (verification infrastructure §111) |

**Nota bene** on DEF-018: this is **not a reimplementation of
proptest**. It is a 30-line PRNG and a loop. The *property* under
test ("parser never panics on random bytes") is the same; the harness
around it changes in Phase 6. Migration diff: drop the PRNG, replace
the loop with `proptest! { #[test] fn parse_header_never_panics(bytes in any::<Vec<u8>>()) { let _ = parse_header(&bytes); } }`.

## 5. Runtime gaps — must fix before Phase 1e ships

These are places where a future runtime violation would surface as
**silent loss** or **caller hang**, because the compiler does not guard
them. They are not user-facing yet (no async wrapper exists), but each
becomes critical the moment it does.

| ID | Gap | Symptom if triggered | Fix path |
|---|---|---|---|
| DEF-019 | `push_action` Err at runtime (MAX_ACTIONS_PER_CALL exceeded): caller short-circuits out of `handle_push_ping` / error path without emitting Sync or delivering a reply. | User's oneshot never resolves → `await` hangs forever. | Option A: elevate Err to protocol-level `Errored(BudgetOverflow)` + `Action::CloseSocket`. Option B (preferred, DEF-020): make overflow impossible at compile-time. |
| DEF-020 | No per-site const-assert on `push_action` budget: a future contributor can add a 5th push without bumping MAX_ACTIONS_PER_CALL, and only a runtime branch catches it. | Same as DEF-019 — hung caller. | `#[budget(N)]` attribute macro + workspace check that MAX ≥ sum of all `#[budget]` values. Or compile-time summation in `const fn`. |

**Verification hook:** Phase 1e's `run_io` integration test must
explicitly assert: for every terminal state machine path (Ok replies,
protocol errors, transport errors), a reply has been delivered OR
the caller has been notified of disconnection. Zero silent drops.

## 6. Dep additions — staged, not bundled

| ID | Dep | Added in | Justification |
|---|---|---|---|
| DEF-021 | `heapless = 0.9.2` (`default-features = false`) | Phase 1a | Only bounded `Vec`, zero alloc, zero-unsafe-from-our-perspective. |
| DEF-022 | `rapidhash` | Phase 2+ | Stmt cache keys. Not needed before macros ship. |
| DEF-023 | `sha2 = 0.10` (`default-features = false`) | 1b | SCRAM-SHA-256. |
| DEF-024 | `zeroize` + `zeroize_derive` | 1b | Password buffer scrubbing. |
| DEF-025 | `subtle = 2` (`default-features = false`) | 1b | Constant-time SCRAM signature comparison. |
| DEF-026 | `proptest`, `cargo-fuzz`, `loom`, `cargo-mutants`, `cargo-vet`, `cargo-deny` | Phase 6 | Verification infrastructure bundle. |

## 7. Audit-driven architectural commits (2026-04-15 review round 1)

The first end-to-end audit of `reforge.md` by the rust-senior-architect
agent, reviewed independently and reconciled with user input, produced
the following binding decisions. Each item here is a **commit for Phase
1b+ to honour**; items that were proposed but rejected are listed with
explicit rationale so a future reader does not re-litigate them.

### Accepted

| ID | Commit | Where it lands | Tier after |
|---|---|---|---|
| DEF-027 | **Sealed-token pattern** for every sealed trait: `pub mod private { pub struct Token { _priv: () } pub trait Sealed { fn __seal(&self) -> Token; } }`. External crates cannot construct the token (private field), so cannot impl `Sealed`, so cannot impl the sealed trait. Applies to `Backend`, `Encode`, every pub trait that must stay closed. Supersedes the simple `pub mod private { pub trait Sealed {} }` pattern in reforge §7.5 (which is tier-2 only because external crates can impl Sealed via orphan rules). | `bsql-backend` (Phase 1e), `bsql-core`, every crate with sealed traits | 1 — compile error on external impl |
| DEF-028 | **`ReplyId` consume discipline**: strip `Copy`/`Clone`; `#[must_use = "..."]`; `consume(self) -> NonZeroU64` method; `Drop` impl that `assert!(self.delivered)` on drop. **Honest tier labels:** (a) non-duplication = tier-1 compile (no Copy/Clone — `clone()` does not compile); (b) unused-variable detection = tier-1 compile (`deny(unused_variables)` — unmentioned binding is build error); (c) `drop(id)` silent loss = **tier-2 structural** (no path in our code calls `drop(id)` + Drop-guard catches at runtime if someone adds one — but the buggy code COMPILES, so this is NOT tier-1; per CREDO §3.4 runtime checks are tier-2, not tier-1). | `bsql-pg-proto` — reply_id.rs | 1 compile (a,b) / 2 structural (c) |
| DEF-029 | **Transaction without auto-Drop commit/rollback**: `Transaction<'pool, B>` is `ManuallyDrop`-backed; the compiler rejects implicit drop because there is no `Drop` impl. User must call `.commit().await` or `.rollback().await` — both consume `self`. **Ergonomic helper:** ship `tx.auto(\|tx\| async { ... }).await` — on `Ok` commits, on `Err` rollbacks, propagates the error. Closes reforge §7.6's reliance on `#[must_use]` alone (which is a warning, not an error). | `bsql-core` (Phase 1c onward) | 1 — compile error on implicit drop |
| DEF-030 | **Send/Sync const-asserts** pinned on every cross-task type at crate root: `const _: fn() = \|\| { fn a<T: Send>() {} a::<PgCommand>(); a::<Action>(); a::<OutActions>(); a::<Client<NoopBackend>>(); };`. Non-`Send` regression becomes a compile error. Complements the existing assert for `PgProtocol: Send` (already in Phase 1a). Per R-15. | `bsql-pg-proto` (amend `lib.rs`), `bsql-backend`, `bsql-driver-postgres` | 1 |
| DEF-031 | **`JoinHandle` owned inside `Client<B>`, not exposed**: the background task handle lives in the `Client` struct; drop-order becomes irrelevant. `Client::drop` aborts task + closes `cmd_tx`. Explicit `Client::shutdown(self, timeout: Duration) -> Result<_, _>` for graceful drain. No external `JoinHandle` handed to user. Closes the tier-4 "currently works if dropped in the right order" hole in reforge §12. **Independent justification** (not by analogy to other libraries): one owner → one Drop path → no ambiguity; `shutdown` is explicit for users who want graceful cleanup. | `bsql-backend` (Phase 1e) | 1 |
| DEF-032 | **`fsync(dir)` after `rename`** for bitcode cache durable-rename. Current reforge §31 `write → fsync(file) → rename` is not durable on ext4/xfs without a trailing dir fsync — power loss between rename and dir fsync can vanish the new file. One extra syscall, off hot path. | `bsql-macros` (Phase 2 offline cache) | 1 |
| DEF-033 | **Pre-encoded `Bind` prefix at macro time**: the macro emits `const BIND_PREFIX: [u8; N]` and `const PARAM_OFFSETS: [usize; P]`. At runtime only the variable-length parameter payloads are written (via `writev` with the const prefix as the first slice). Cuts per-query encode CPU from ~80 ns to ~25 ns on 3-param queries (scylla-rust-driver pattern). | `bsql-macros` (Phase 2) | — (perf) |
| DEF-034 | **Slotted pending-replies** — replace the proposed `heapless::FnvIndexMap<ReplyId, Sender, MAX>` (reforge §7.4) with `Box<[Option<Sender>]>` sized at connect, `ReplyId` carrying the slot index. Array index, zero hash, zero collision class. ~11 ns saved per lookup. See DEF-035 for the sizing contract. | `bsql-backend` (Phase 1e) | — (perf + simplification) |
| DEF-035 | **`max_inflight` runtime-configurable with semaphore backpressure + telemetry**. Decision: **variant B** (runtime `Box<[Option<Sender>]>`, one heap alloc per Client at connect — **not** in hot path). Flow: user sets `Pool::builder().max_inflight(N)`; Client allocates the pending-replies slice at connect; `tokio::sync::Semaphore` with N permits gates `send_command`; overflow awaits (no panic, no error by default). Telemetry: `pool.metrics().inflight_high_water_mark`; one-shot warn-log at 80% utilisation with actionable message. No compile-time ceiling (user picks any value); trade-off is one alloc per Client at connect, which is boundary-phase work and amortised over the Client's lifetime. Resolves the discussed failure-mode question (never panic; user discovers need to raise cap via latency spike + metric + warn-log). | `bsql-backend` (Phase 1e), `bsql-core` (builder + metrics) | 1 runtime (semaphore-bounded) + observability |
| DEF-036 | **Cross-platform throughput commits only:** `writev`/`AsyncWrite::poll_write_vectored` for pipelined writes; `TCP_NODELAY`; `SO_RCVBUF`/`SO_SNDBUF` tuning. No OS-specific code in the default build path. | Phase 1e + 1f | — (perf, portable) |

### Rejected — do not revisit without new evidence

| ID | Rejection | Rationale |
|---|---|---|
| DEF-037 | `io_uring` / `tokio-uring` / `monoio`, `sendmmsg`/`recvmmsg`, `TCP_CORK`, `SO_ZEROCOPY`. | Linux-only (breaks cross-platform) and each requires `unsafe` either directly or via abstraction crates that are themselves pre-1.0. Violates architect.txt Part V (unsafe restricted to FFI modules only) and the cross-platform guarantee. Re-visit only if (a) a stable, safe, cross-platform API emerges, or (b) a user with a measured latency floor that cross-platform mechanisms cannot hit requests an opt-in feature — in that case, scope it as a separate Linux-only crate with fully isolated `unsafe`, not a feature flag on the main driver. |
| DEF-038 | `bsql::connect!("postgres://…")` macro + `const fn parse_pg_url`. | ~300 LoC const URL parser to catch malformed URLs at `cargo build` instead of `pool.connect()` — a first-run error. Runtime parser (`PgConfig::from_url`) is needed anyway for the common `.env → dotenv → std::env::var` workflow. `env!("DATABASE_URL")` already catches **missing** env vars at compile time for free. The macro only adds "catch malformed **syntax**" at compile, which is a first-request-at-startup failure mode in practice — an early error with a clear message is a fine user experience. Macro's cost/value ratio is unfavourable; ship runtime parser + typed `PgConfig::builder()` only. Revisit if user feedback surfaces real pain from runtime URL validation failures. |

## 8. Meta-policies (override all heuristics)

Bright-line rules that apply across the workspace forever, not
bounded to any phase. When a heuristic from elsewhere in `reforge.md`,
this file, or `architect.txt` appears to conflict with a meta-policy,
**the meta-policy wins**.

| ID | Rule | Context / lesson |
|---|---|---|
| DEF-META-01 | **Never hand-roll expert-domain code, never ship facades over it.** This covers cryptography (SHA, HMAC, PBKDF2, AES, ChaCha20, RSA, ECDSA, Ed25519), TLS (use `rustls`), encoding formats (base64, hex, base32, base58 — use `base64` / `base64ct` / `hex`), random number generation (use `getrandom`), regex (use `regex`), compression (`zstd`, `flate2`, etc.), large-format parsers (JSON/YAML/TOML/protobuf — use maintained crates per perf policy), OS primitives, and CPU-architecture specifics. Always depend on maintained, audited crates (RustCrypto organisation, `rustls`, etc.). Thin facade wrappers over trusted crates are also banned — they add maintenance surface for zero value. | Lesson from Phase 1b round 1: the agent, reading architect.txt Part II policy 8 ("write it yourself if < 200 LoC"), hand-rolled HMAC-SHA-256 (~130 LoC), PBKDF2 (~114 LoC), and RFC-4648 base64 (~498 LoC), and also wrote a 94-LoC facade over `sha2::Sha256`. All four passed their RFC test vectors — **but unit tests cannot catch timing side-channels, constant-time-compare regressions, or subtle edge cases that expert adversaries exploit**. We are not crypto auditors. The "<200 LoC" heuristic was misapplied to a domain where the asymmetry is "afternoon of reading" vs "weeks of expertise plus third-party audit". DEF-META-01 is the non-negotiable carve-out; architect.txt Part II policy 9 and Part XI bans codify it in the agent's operating instructions. Any future proposal to hand-roll in these domains is rejected at review without discussion. |

## 9. Phase-1a architectural hardening (2026-04-17)

Following the "no tests where architecture can speak" review, Phase 1a
was tightened before Phase 1b expansion lands. Every upgrade below
moves an invariant from tier-3 (test-verified), tier-4 (happens-not-to-
fail), or implicit audit-enforced tier-2 up to a tier-1 or honest
tier-2 structural form — the foundation that Phase 1b's much larger
state surface will inherit.

### Closed

| ID | Status | What closed it |
|---|---|---|
| DEF-018 | Closed — overspec | Hand-rolled SplitMix64 fuzz test for `parse_header_never_panics_on_random_bytes` deleted (49849b9). Invariant is tier-1 by forbid-bundle + slice patterns + checked arithmetic — every panic-able expression is a compile error. The companion `parse_ok_always_yields_total_len_within_cap` fuzz test is also gone: `total_len ≤ READ_BUF_CAP` is structurally pinned by the `READ_BUF_CAP == MAX_FRAME_LEN_FIELD + 1` const-assert plus saturating arithmetic. No verification harness replaces these — none is needed. |
| DEF-015 (partial, Phase 1a scope) | Closed for every Phase 1a path | `ReplyId`/`PgCommand` stripped of `Clone` (396d9e0) + `deny(unused_variables)` at crate root (e9ec81c) + architect.txt ban on `let _ = expr;` and `_varname` suppression. Combined, these make it a compile error to extract a `ReplyId` from a state variant and silently drop it: the arm must use the id, the id cannot be duplicated, and discarding bindings is forbidden. Phase 1b inherits the discipline automatically for every new `ConnectingStartup { reply, … }`-style variant. |
| DEF-019 (Phase 1a scope) | Closed | The `push_action` budget-overflow runtime branch in Phase 1a's error paths is now observably classified — overflow leads to `ProtoState::Errored(cause)` (52fd13e) and all subsequent bytes/commands land on the dedicated dispatcher arm. No silent hang. |

### Registered upgrades (post-hardening, before Phase 1b)

| ID | Change | Tier before | Tier after |
|---|---|---|---|
| DEF-028 (partial) | `ReplyId`: non-`Copy` + non-`Clone`, `pub` ctor (tier-2 — the cross-crate seal of "only the wrapper mints" is not expressible until a sealed `Backend` trait lands in 1e). Drop-guard still pending; the combination of non-duplication + compile-enforced consumption is already enough for Phase 1a's flows. | — | 1 (non-duplication); 1 (consume discipline via unused_variables); 2 (minting provenance audit-enforced) |
| DEF-030 (Phase 1a scope) | Send const-asserts at crate root for `Action`, `OutActions`, `Reply`, `SendBuf`, `PgCommand`, `ProtocolError`, `PgProtocol`, `ReplyId`, `ProtoState` (52c8704). Future non-`Send` regressions fail compile. | — | 1 |
| DEF-META-02 | `DispatchOutcome::Advanced` no longer echoes the `by` the caller just handed in. Slice patterns in the dispatcher body replace `unread.get(5)` / `saturating_sub(5)` / "unreachable in practice; classify" patterns (f178160). Payload shape is verified by the compiler, not by review. | 4 (happens not to fail) / 2 (audit) | 1 (slice pattern exhaustive) / 2 (buffer-advance local invariant) |
| DEF-META-03 | `ProtoState::Errored(ProtocolError)` terminal variant replaces the `state = Idle` after-close tier-4 pattern (52fd13e). Post-classify frames flow through a dedicated dispatcher arm, post-classify commands get a typed `FailReply` with the stored cause. | 4 | 2 (structural terminal-sink, no re-open path) |

### Phase 1b+ binding commitments (open, to-be-implemented in their phase)

These inherit the Phase 1a discipline and lock the tier ambitions for
new mechanisms. Each is **mandatory at tier-1** unless the cell says
otherwise — "tier-2" here means "genuinely unreachable at tier-1 on
stable Rust without adding synthetic types or nightly, and the tier-2
mechanism fully closes the observable behaviour".

| ID | Commitment | Phase | Target tier |
|---|---|---|---|
| DEF-039 | `SecretDigest` newtype — no `PartialEq`, only `ct_eq`. | 1b | **CLOSED** — landed (690e30e). tier-1: `==` on SecretDigest is a compile error. |
| DEF-040 | `CappedServerNonce` — bounded server nonce buffer. | 1b | **CLOSED** — landed (690e30e). tier-1 by constructor rejection. |
| DEF-041 | `Ident` / `ApplicationName` / `DatabaseName` — NUL-free newtypes. | 1b | **CLOSED** — landed (690e30e). tier-1: NUL bytes rejected at construction. |
| DEF-042 | `SessionParams` — fixed struct, no map, no overflow class. | 1b | **CLOSED** — landed (690e30e). tier-1 by absence of growable container. |
| DEF-043 | `NoticeResponse` (tag `N`) pre-dispatch filter: any state, any frame, a single filter at the top of `feed_bytes` extracts the notice and emits `Action::EmitNotice(…)` without touching state. Dispatcher arms do not need to handle `N` separately. | 1c (first Query flow; Phase 1b's Startup flow does not need it) | 1 (single site, structural) |
| DEF-044 | `NegotiateProtocolVersion` during startup → `UnsupportedProtocolOption`. | 1b | **CLOSED** — landed (6382cdc). tier-1 by exhaustive match. |
| DEF-045 | `emit_actions!` macro — compile-time per-site action budget. Any future contributor who adds a push without declaring its budget gets a build error; exceeding the declared budget is also a build error. Global `MAX_ACTIONS_PER_CALL` becomes an upper bound checked against the sum of declared budgets. | 1a+ (applicable the moment Phase 1b adds more push sites; deferred until then to avoid refactoring-for-nothing) | 1 |
| DEF-046 | `ReplyId` counter wraparound: wrapper crate's counter uses `checked_add(1)` + classified `IdPoolExhausted` error. 2⁶⁴ IDs on 1-ns cadence is 584 years; the guard is there for the "24/7 pool over a long-running service" edge only. | 1e | 1 |
| DEF-047 | Wrapper-level typestate for connection lifecycle: `IdleConnection<B>` / `ActiveConnection<B, R>` / `DeadConnection<B>`. Typed handles enforce "no command on dead connection" and "no second command while first in flight" at compile time — the equivalent runtime rejects in `bsql-pg-proto` become the safety-net of last resort, not the primary guard. | 1e | 1 (at the wrapper API; protocol-crate-level stays tier-2 via `Errored` arm) |
| DEF-048 | `Sensitive<T>` + `!Debug` audit: every type that contains a `Sensitive<T>` field gets either a manual `Debug` that redacts the field, or no `Debug` at all. No Rust stable negative trait bound; enforced by naming convention (`Credentials`, `Secret*`) + reviewer discipline — honest tier-2, not tier-1. | 1b onward | 2 |
| DEF-049 | `ReadBuf` capacity sizing study: confirm that `READ_BUF_CAP = 4 KiB` is above the largest frame Phase 1-4 will emit. If not (e.g. COPY rows), bump — always at compile time, never at runtime. | 1c–1d | 1 |
| DEF-050 | SASLPrep for unicode passwords: `stringprep` crate integration, opt-in. ASCII passwords are the common case; non-ASCII without SASLPrep can fail interop with PG servers that normalize. Tier-3 best-effort without the dep; tier-2 structural with it. | 1b (optional) or later | 2-3 (design decision) |
| DEF-051 | Empty-password rejection: `Password::try_from_str` → `Err(PasswordError::Empty)`. | 1b | **CLOSED** — landed (690e30e). tier-1 via Result return. |
| DEF-053 | Channel binding (SCRAM-SHA-256-PLUS): Phase 1b uses `n,,` GS2 header and `biws` cbind data (no channel binding). Requires TLS channel binding data from the transport layer, which does not exist until the async wrapper lands in Phase 1e. GS2 header always `n,,`, cbind always `biws`. | 1e (with TLS) | 2 (structural — requires transport-layer data not yet available) |
| DEF-052 | **CLOSED** (DEF-101) — `ReplyId::drop` now guards on `std::thread::panicking()` under `#[cfg(test)]` and returns early during unwinding. The `cfg(test)` gate is zero-cost in production (`panic = "abort"` never unwinds) and uses the pre-existing `#[cfg(test)] extern crate std;` — no new feature flag, no new dep. Regression test `unrelated_panic_while_reply_id_alive_surfaces_original_message` exercises the fix: it panics for an unrelated reason with a live ReplyId, `#[should_panic(expected = "unrelated panic")]` asserts the original message reaches the harness without double-panic masking. |

## 11. Phase-1b hardening (2026-04-18)

Second paranoid audit pass before Phase 1c begins. Found one latent
Phase-1c bug, one Part-V discipline violation, one manufactured-variant
leak, and two classes of regression brittleness ("glass architecture"
where changing a literal constant silently breaks invariants). All
resolved in this commit; future Phase-1c commitments registered as
open items below.

### Closed in this pass

| ID | Status | What closed it |
|---|---|---|
| DEF-054 | Closed | `is_post_auth_state` renamed to `allows_unsolicited_param_status` with exhaustive `match` over every `ProtoState` variant. `Idle` and `AwaitingPingReply` now accept unsolicited `ParameterStatus` (per PG spec: server emits PS after `SET`, `ALTER SYSTEM`, session-parameter updates). Pre-filter in `feed_bytes` records and skips dispatch; dispatcher PS arms removed. Adding a new state variant without explicitly deciding its PS policy now fails the build here. **Latent Phase-1c bug fixed preemptively**: before this, any runtime `SET` command's PS would have triggered `UnexpectedFrame` → `CloseSocket`. Three regression tests added (`unsolicited_param_status_in_idle_is_recorded_and_skipped`, `unsolicited_param_status_in_awaiting_ping_reply_is_recorded`, `param_status_during_pre_auth_is_unexpected`). |
| DEF-055 | Closed | `emit_actions!` split into two forms: `on_overflow: break` (loop callers, bails out of the enclosing loop) and no-bail (non-loop callers where the `const _ = assert!(MAX >= budget)` proves the push cannot fail on a fresh `OutActions`). Previous Part-V violation (`#[allow(clippy::needless_return)]` without `reason`) removed entirely. |
| DEF-056 | Closed | `ProtocolError::MalformedParameterStatus` variant removed. It was manufactured (never emitted anywhere; classification path has never existed). Violation of §4.6 manufactured-variant ban. Phase 1c can re-introduce with a real emit path if `record_param_status`'s silent-drop policy changes. |
| DEF-057 | Closed | `max_startup_message_size()` (`write_buf.rs`) and `sasl_initial_response_frame_size()` / `sasl_response_frame_size()` / `expected_client_first_bare_size()` / `expected_client_first_msg_size()` / `expected_client_final_msg_size()` (`scram/wire.rs`) as `const fn` over the underlying inputs (`MAX_IDENT_LEN`, `MAX_APP_NAME_LEN`, `MAX_CLIENT_NONCE_B64_LEN`, `MAX_SERVER_NONCE_LEN`). All `const _ = assert!(...)` drift-guards link `MAX_OWNED_SEND_LEN` / SCRAM buffer caps to the computed worst case. Bumping any contributing input without growing the cap now fails the build. Tier 2 → 1 on regression resilience. |
| DEF-058 | Closed | `ReadBuf::append` lazy-compacts — attempts `extend_from_slice` first (fast path when tail has room, no memmove) and only reclaims the consumed prefix when the tail is insufficient. `heapless::Vec::extend_from_slice` is all-or-nothing on overflow so the retry is safe. `ReadBuf::append`, `ReadBuf::advance` gained `#[inline]` for per-I/O-cycle hot path. Saves one memmove per read-heavy append call on typical workload. |

### Phase-1c binding commitments (open)

All items below are **mandatory at tier-1** unless the cell says
otherwise. Each must land in Phase 1c's sub-phases before that phase
closes.

| ID | Commitment | Phase | Target tier |
|---|---|---|---|
| DEF-062 | `NoticeResponse` (tag `'N'`) pre-dispatch filter — extracts the notice from any state, emits `Action::EmitNotice(...)`, skips the dispatcher. Matches the DEF-054 pattern for ParameterStatus. Supersedes DEF-043 with concrete shape. | 1c | 1 (single-site, structural) |
| DEF-063 | Handshake-flow typestate extraction. The linear chain `Idle → ConnectingStartup → ConnectingScram* → ConnectingPostAuth* → Idle` becomes a dedicated `handshake` module with typestate transitions: each step is `fn step(PrevState, ...) -> Result<(NextState, Action), ProtocolError>`. Reactive states (AwaitingQueryReply, StreamingRows, InTransaction) remain in the enum — typestate doesn't apply to them because server events can drive multiple outcomes. Tier 2 → 1 on "invalid handshake-step call from wrong state". | 1c | 1 on handshake path |
| DEF-064 | `parse_error_response` bounded-iterations loop — replace unbounded `loop { pos += 1; ... }` with `for _ in 0..MAX_ERROR_FIELDS (=16) { ... }`. Closes a potential DoS vector on malformed `ErrorResponse` with adversarially-crafted fields. | 1c | 2 (structural bound) |
| DEF-065 | SCRAM message assembly writes directly into `WriteBuf` — remove `build_client_first_bare` / `build_client_first_message` intermediate `heapless::Vec` buffers (currently 128 + 136 = 264 bytes stack + 2 memcpy per SCRAM init). Save state `client_first_bare` only (the one input needed later for HMAC). | 1c | — (perf + simplification) |

### Phase-1e+ binding commitments (open)

| ID | Commitment | Phase | Target tier |
|---|---|---|---|
| DEF-066 | `ReplyId` layout optimisation: pack `delivered: bool` into the LSB of `value: NonZeroU64` (or equivalent niche). Saves 8 bytes per in-flight reply. Adopt iff bench on the Phase-1e wrapper shows meaningful impact (pool with `heapless::FnvIndexMap<ReplyId, Sender, N>` or slotted `Box<[Option<Sender>]>` — DEF-034). | 1e | — (perf, measured) |

### Measurement milestones (verification, not code)

| ID | Milestone | Phase |
|---|---|---|
| DEF-067 | `cargo rustc -p bsql-pg-proto --release -- --emit asm` on `parse_header` and `dispatch`. Verify: (a) `parse_header` compiles to ≤ 12 instructions on happy path, branchless BE-load; (b) `dispatch`'s big `match (ProtoState, u8)` generates a jump table or tree of comparisons (not a linear chain). Results committed to `docs/asm/phase-1c.md`. Guards against silent perf regression when state surface expands. | end of 1c |
| DEF-068 | `base64-simd` vs `base64` benchmark on large payloads (bytea decode, JSONB text). Small-payload SCRAM paths (nonce 24 chars, proof 44 chars) are too small for SIMD overhead to amortise; the benchmark is for the forthcoming binary codec in 1c. Adopt iff ≥ 2× gain on target arch (x86_64 AVX2, aarch64 NEON). Fallback preserved on unsupported targets. | 1c codec |
| DEF-069 | Allocation-profile harness + baseline (`benchmarks/alloc_profile.md`) per `reforge.md §80.10`. Counting global allocator in benches crate, comparative workload vs sqlx / tokio-postgres / diesel / libpq. | 6 |

### Research track (Phase 5+ / v1.x+)

| ID | Idea | Phase | Rationale |
|---|---|---|---|
| DEF-070 | Proc-macro `state_machine!` DSL — declarative state + transition syntax, auto-generates `ProtoState` enum + dispatcher + GraphViz diagram for docs. Revisit when the machine exceeds ~15 states (query + stream + copy + listen + cancel + terminate + transaction + savepoints + ...). DX / documentation win; no tier change (same enum + exhaustive match under the hood). | 5+ | Declarative form becomes more readable than hand-written match when state count grows. Not urgent until Phase 3. |
| DEF-071 | Session-types POC — compile-time wire sequencing via type-level encoding. Would give tier-1 on "send/recv in correct order" instead of today's tier-2-via-exhaustive-match. Rust session-type crates are pre-production; error messages are notoriously opaque. Revisit if a production-ready library emerges, or as a standalone research crate without public API exposure. Related to §80.20 proof-carrying tokens. | v1.x+ | Current design covers ~95% of the session-type guarantees via exhaustive match + state-as-data. Adopting session types would trade DX for a marginal tier upgrade. |

### Architectural commitment: sync-driver path

The sans-I/O core (`bsql-pg-proto`) has no dependence on tokio. This
was always the plan (§7.1), but the **sibling sync-driver crates**
that take full advantage of it need explicit registration.

| ID | Commitment | Phase |
|---|---|---|
| DEF-072 | `bsql-driver-postgres-sync` and `bsql-driver-sqlite-sync` sibling crates — pure-sync implementations of `Backend` using `std::net::TcpStream` (blocking) + `std::sync::mpsc` / `crossbeam-channel` instead of tokio primitives. `bsql-pg-proto` is shared byte-for-byte; the driver layer is the only difference. Users feature-select async or sync (or both, for heterogeneous pools). Benefit: apps without tokio (CLI tools, embedded, sync libraries) never transitively pull tokio. | 3 (async lands 1e; sync sibling in 3 with pool) |

## 12. Phase-1b second paranoid pass (2026-04-18 — same day)

User requested a third-order paranoid audit of the Phase-1b hardening
commit (`08ee095`): "are we certain nothing's glass architecture,
really tier-N as claimed, no missing tests". An independent audit
pass by the architect sub-agent found:

- **T1: real manufactured variant** (`ProtocolError::ServerError`
  declared, never emitted) — direct §4.6 violation, missed in round 1.
- **T2: tier-honesty drift** (`MAX_ACTIONS_PER_CALL` aggregate claim
  was loose).
- **S1–S4: seams without tests** (literal-swap-in-arm-body paths
  where the compiler does not pin `{input → output}` mapping).
- **U1: tier upgrade available** (`PgProtocol: !Sync` was tier-2
  structural via `PhantomData<Cell<()>>`; compile-time assert via
  "ambiguous-impl" trait trick raises it to tier-1).
- **U2: Errored-loss hazard** (`core::mem::take` pattern is tier-2
  by audit; a `take_unless_errored() -> Option<ProtoState>` helper
  raises to tier-1 via NonErroredState sum-type).

### Closed in this pass

| ID | Status | What closed it |
|---|---|---|
| DEF-073 | Closed | `ProtocolError::ServerError` variant stricken — was manufactured (declared at `error.rs:52`, zero emit sites — `parse_error_response` only emits `ServerErrorResponse`). §3.5/§4.6 violation caught by independent architect pass; missed in round 1. Doc reference in `wire.rs` updated to point at `ServerErrorResponse`. |
| DEF-074 | Closed | `PgProtocol: !Sync` raised from tier-2 structural to **tier-1 compile** via the ambiguous-impl trait trick in `lib.rs`. A private trait `AmbiguousIfSync<A>` has two overlapping blanket impls: `impl<T: ?Sized> AmbiguousIfSync<()> for T` and `impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T`. For `T: Sync` both match and method resolution is ambiguous (build fail); for `T: !Sync` only the first matches (build succeeds). Removing the `_not_sync: PhantomData<Cell<()>>` field now fails the build — the structural defence became a compile-time proof. Zero dep, zero runtime cost. |
| DEF-075 | Closed | Tier-1 shield seams pinned by tests (second-pass audit categories S1/S2/S3/S4/U3 + direct bounded-buffer / newtype / BackendKeyData coverage): <br>• `tests/bounded_buffers_spec.rs` (15 tests): `ReadBuf` append/advance/clear/overflow/lazy-compact; `WriteBuf` push methods + length-prefix + overflow; `CappedServerNonce` bound classification.<br>• `tests/tier_seams_spec.rs` (9 tests): `SendBuf::as_bytes` Static/Owned fidelity (S2); `SessionParams::set` key→field mapping table (S3) + unknown-key drop + non-UTF8 skip + overwrite semantics; `Errored` cause preservation in state AND reply (U3) with distinguishable `FrameTooLarge{declared:0xDEAD}`; `DatabaseName` validation + `ApplicationName` allow-empty; `BackendKeyData` malformed-size classification.<br>• In-module tests in `protocol.rs` (1 test): `allows_unsolicited_param_status` policy table over all 9 `ProtoState` variants (S1).<br>• In-module tests in `dispatch.rs` (7 tests): `parse_error_response` field-type → field mapping (S4), severity S/V precedence, unknown-field skip, empty/unterminated payload graceful handling (B1 partial), duplicate-code last-wins.<br>Test count: 41 → 73 tests. Each test maps to a reforge.md §4.11.1 seam class (literal swap, arm return swap, arm-body access, classification boundary, one-line impl drift). |
| DEF-076 | Closed | `MAX_ACTIONS_PER_CALL` docstring in `protocol.rs` rewrote the tier claim honestly: **per-site budget** is tier-1 compile (via `emit_actions!` const_asserts, DEF-045); **aggregate across loop iterations** is tier-2 structural (bounded container + `on_overflow: break` bail, not compile-proven in frame count). The previous `"tier-1"` summary label was loose. §3.4 ban on "tier-1 runtime" labels honored. |

### Carried open (architect second-pass findings not closed yet)

| ID | Commitment | Phase | Rationale / note |
|---|---|---|---|
| DEF-077 | **`NonErroredState` refactor** — extract non-Errored variants into a separate enum; `take_state_or_read_errored_cause()` helper returns either `NonErroredState` (moved out) or `&ProtocolError` (borrowed from Errored). Forgetting to restore Errored becomes compile-impossible (you literally never get the cause out to lose). Currently tier-2 by audit; refactor raises to tier-1. **Deferred to Phase 1c** because it touches every state-handling function and naturally bundles with DEF-059 (compute/apply split). Not urgent because all current `mem::take` sites correctly restore `Errored` (pinned by U3 test `errored_cause_is_preserved_in_state_and_reply`). | 1c | Architect second pass U2. |
| DEF-078 | `parse_server_first` accepts RFC 5802 extension fields (`m=required` prefix) — currently `splitn(3, ',')` may silently ignore extra comma-separated fields. Bumping split cap + classifying unexpected `m=` prefix per RFC. | 1c (with query flow) | Architect second pass B3. Real-world PG servers don't send extensions, but strict spec-conformance for future interop. |
| DEF-079 | `record_param_status` edge-shape coverage — tests for empty key (leading NUL), missing trailing NUL, missing value-NUL. All handled by current code (`strip_suffix(&[0])` falls back to region, `checked_add` bounds the walk), but no spec-conformance test pins the behaviour. Low-risk, worth documenting. | 1c | Architect second pass B2. |
| DEF-080 | `buf::compact` proptest — random sequences of `append → partial advance → append` exercise the `copy_within` overlap cases. `heapless::Vec::copy_within` is safe per std semantics, but DEF-058 changed the code; a property test formalizes the invariant. | 6 (verification infra) | Architect second pass B4. Falls into the proptest bundle (DEF-026). |

### Rust 1.95 features — future applications

Audit of every 1.95-stabilised feature against our current code produced one
immediate application (`#[cold]` / `core::hint::cold_path()` added in
Phase 1b hardening, see DEF-081 below). The rest are registered here for
phases where they naturally fit.

| ID | Feature | Target phase | Use-case |
|---|---|---|---|
| DEF-082 | `bool: TryFrom<{integer}>` | 1c codec | PG binary `bool` type (OID 16) encodes as exactly `0` or `1`. Using `bool::try_from(byte)` instead of `byte != 0` rejects any other byte value as a decode error — tier-1 against server-protocol violation. Today unused because no binary codec yet. |
| DEF-083 | `AtomicBool::update` / `AtomicPtr::update` / `Atomic{Isize,Usize}::update` | 1e wrapper | `Client<B>::is_connected: AtomicBool` — lock-free state flip on transport close. Pending-replies slot updates if DEF-034 slotted map uses atomics. Cleaner than `compare_exchange` loops. |
| DEF-084 | `if let` guards on match arms | Not applicable today | Audited every parse-then-dispatch site; slice patterns + match-over-Result already cover our use cases. An Err branch always needs the concrete error value, which `if let` guards hide. Keep in mind if Phase 1c / 2 produces new "pattern + optional validation" shapes. |
| DEF-085 | `core::range::RangeInclusive` + `Iter` | Not applicable today | The new `core::range` mod provides inclusive-range iteration in no_std. No current use but potentially useful for test fixtures or future bounded counters. Register for visibility. |
| DEF-086 | `fmt::from_fn` (now const) | Not applicable after DEF-060 | Could replace Display impls with const-builders. Made irrelevant by DEF-060's move to `&'static str` discrete error enums (no runtime formatting needed on cold path). |

### Closed in this sub-pass (cold-path application)

| ID | Status | What closed it |
|---|---|---|
| DEF-081 | Closed | `core::hint::cold_path()` (stable 1.95) applied inside `parse_header` at MalformedLength / FrameTooLarge / NonZero-None arms — keeps the happy path contiguous in I-cache. `#[cold]` attribute applied to `parse_error_response`, `parse_backend_key_data`, and `PgProtocol::fail_inflight_and_close` — whole-function cold markers so LLVM places their bodies out of hot-path instruction-cache neighborhoods. No safety impact (hints only); perf gain measured later via DEF-067 `cargo asm` milestone. reforge.md §5.6 corrected: cold_path stabilised 1.95, not 1.83. |
| DEF-089 | Closed | **`SendBuf` enum collapsed to single-shape tuple struct**. The two-arm `enum SendBuf { Static(&'static [u8]), Owned(heapless::Vec<u8, N>) }` had a tier-3 shield seam in `as_bytes` — arm-body swap compiled silently and would cross-wire every outbound message. Collapsed to `pub struct SendBuf(heapless::Vec<u8, N>)` with `from_slice(&[u8]) -> Result<_, SendBufFull>` and `pub(crate) from_owned(heapless::Vec)` constructors. Static-payload Sync (5 bytes) now memcpy'd into buffer (was zero-copy `&'static [u8]`) — negligible since Ping is rare. **Tier-3 test → tier-1 structural**: `send_buf_as_bytes_static_and_owned_round_trip` deleted (surface gone). Test count: 74 → 73. Full zero-copy via lifetime-bound `Action<'buf>::SendBytes(&'buf [u8])` was expected to bundle with DEF-059 compute/apply split in Phase 1c batch 1 — on attempt the refactor was rejected (see DEF-094); SendBuf (owned memcpy) is the terminal shape. |
| DEF-093 | Closed | **Newtype zero-cost formality + Drop-semantics compile gates + Errored pre-check pattern**. Three orthogonal cleanups: <br>• **`#[repr(transparent)]`** on every single-field newtype: `SendBuf`, `Ident`, `DatabaseName`, `ApplicationName`, `SecretDigest`, `CappedServerNonce`, `Sensitive<T>`. Formal ABI guarantee that layout is identical to inner — zero-cost abstraction attested at type level. `Password` excluded (two fields, not transparent).<br>• **`const _: () = assert!(core::mem::needs_drop::<T>())` compile gates** in `lib.rs`: positive asserts on types that MUST zeroize/guard (`Password`, `SecretDigest`, `ReplyId`); negative asserts on Copy-like value types that MUST stay drop-free (`Reply`, `HeaderParse`, `IdentError`, `PasswordError`). Tier-1 compile enforcement of Drop-semantics invariants. Caught real bug during integration: `Password` had manual `impl Zeroize` but no `Drop` — self-zeroize relied on external `Sensitive` wrapper. Fixed by `#[derive(Zeroize, ZeroizeOnDrop)]` on `Password` making it self-zeroizing regardless of wrapper context (defensive).<br>• **Errored pre-check pattern** in `handle_push_ping` / `handle_push_startup`: `if let ProtoState::Errored(cause) = &self.state { ... return; }` before `core::mem::take(&mut self.state)`. Eliminates the transient `Idle` window that `take` introduces for Errored states (state was going Errored → Idle → Errored-same-cause on every push during Errored). Cleaner and semantically honest — Errored never momentarily "lost" during a push. |
| DEF-088 | Closed | **`ReplyId` equality seam eliminated at tier-1 structural**. Previous `impl PartialEq / Eq / Hash for ReplyId` had a one-line body `self.value == other.value` — a semantic drift (`... && self.delivered == other.delivered`) would compile silently. Removed all three impls entirely. Rationale: (a) no production code compares `ReplyId == ReplyId` (the wrapper's pending-replies map is keyed on `NonZeroU64` per `Action::DeliverReply { id: NonZeroU64, .. }`, confirmed via grep); (b) the single test `partial_eq_ignores_delivered_flag` was the only consumer, and it tested an API no real code uses. Callers needing to compare wire values explicitly extract via `.get() -> NonZeroU64` and compare those — makes the comparison site greppable and auditable. Tier-3 test → tier-1 structural (impl does not exist → no body to drift). Test count: 75 → 74 (one removed, zero added — the test closed a seam that no longer exists). |
| DEF-087 | Closed | **Size-of compile-time bounds + E2E blind-spot closures**. Three additions: <br>• **Size-of `const _` asserts in `lib.rs`** for `ProtocolError` ≤ 1024, `Action` ≤ 1024, `SendBuf` ≤ 768, `Reply` ≤ 64, `ReplyId` ≤ 24, `ProtoState` ≤ 2048, `PgCommand` ≤ 2048, `PgProtocol` ≤ 8192, `OutActions` ≤ 4096. A new variant silently carrying a heapless::String<4096> or any other inline-bound inflation fails the build here — a form of tier-1 drift guard that complements DEF-057's pair-consistency asserts. Baseline (x86_64 Linux, 2026-04-20) documented inline: ProtocolError 856, Action 864, SendBuf 528, Reply 12, ReplyId 16, ProtoState 1248, PgCommand 1352, PgProtocol 6656, OutActions 3464. Bounds set ~1.2-1.7× above baseline — tight enough to catch doubling regressions, loose enough to allow ordinary variant additions without rewriting the guard on each commit.<br>• **SCRAM ScramAwait* PS rejection E2E** — `unsolicited_ps_during_scram_await_server_first_is_unexpected` in `startup_spec.rs`. Drives protocol to `ConnectingScramAwaitServerFirst`, feeds unsolicited `ParameterStatus`, verifies FailReply+CloseSocket with `UnexpectedFrame{tag:b'S'}`. Policy unit `policy_per_variant` already pins the boolean for all 9 variants; this E2E verifies filter + dispatch composition delivers the expected action sequence. One test covers all three ConnectingScram* states — they share the same policy function and dispatcher catch-all.<br>• **ReadBufFull propagation E2E** — `read_buf_overflow_through_feed_bytes_propagates_as_classified_error` in `ping_spec.rs`. Feeds a chunk `READ_BUF_CAP + 1` bytes into AwaitingPingReply state, verifies full chain: `ReadBuf::append` classifies with exact `attempted`/`available`, `feed_bytes` routes to `fail_inflight_and_close`, emits `FailReply(ping_id, ReadBufferFull{attempted: 4097, available: 4096})` + `CloseSocket`, state becomes `Errored(ReadBufferFull{..})` with dimensions byte-preserved. Complements `bounded_buffers_spec::append_overflow_is_classified_and_fail_atomic` at the API level. Test count: 73 → 75. |

## 13. Phase-1c batch 1 (2026-04-19)

First Phase-1c refactor wave. Goal for this commit: ship the
compute/apply split (DEF-059) — a structural prerequisite for
subsequent 1c work (DEF-060/061/062/063) because every one of those
touches the push-path decision table.

### Closed in this pass (compute/apply split)

| ID | Status | What closed it |
|---|---|---|
| DEF-059 | Closed | **Pure compute/apply split for the push path**. Extracted three free functions in `protocol.rs`: `compute_push(cmd: PgCommand, state: ProtoState) -> (ProtoState, OutActions)` and its per-command helpers `compute_push_ping` / `compute_push_startup`. Each helper takes `ProtoState` by value, returns the new state, accumulates actions into a passed-in `&mut OutActions`. `PgProtocol::push_command` collapsed to a four-line delegate: `core::mem::take(&mut self.state) → compute_push(cmd, prev) → self.state = new_state → return actions`. **The DEF-093 "Errored pre-check" pattern dissolved**: the transient-`Idle`-window workaround is no longer needed because `ProtoState::Errored(cause)` is a first-class match arm in each `compute_push_*` helper — the arm body clones the cause for `FailReply` and returns `Errored(cause)` unchanged (state preserved end-to-end). The pre-check's empty `ProtoState::Errored(_) => {}` arm (unreachable-by-construction) also dissolved: no more unreachable arms in the push path. **Seam coverage**: new `compute_push_tests` module inline in `protocol.rs` (companion to `allows_unsolicited_param_status_tests`) pins the per-variant policy table for Ping and Startup. 7 new unit tests enumerate every `(cmd, state)` pair and assert both the returned state shape (with preserved reply IDs) and the emitted `Action`'s cause/payload. DEF-059's original framing promised "pure half is testable without constructing `PgProtocol`" — the new tests are the proof (none construct `PgProtocol`). Test count: 73 → 80. **Compile/runtime**: build clean, clippy clean, full workspace suite passes (80/80). |
| DEF-094 | **Investigated 2026-04-19, deferred** | **Staged-dispatch architecture proved feasible in the crate; lib implementation reached a clean build.** The paranoid 2026-04-19 audit (architect sub-agent) identified staged dispatch as the route through the original DEF-094 rejection: **Phase 1** (mut) — dispatchers write into a `WriteBuf`, emit `StagedAction` values carrying *ranges*. **Phase 2** (shared) — materialise ranges into `Action<'buf>::SendBytes(&'buf [u8])` once the mutable borrow releases. Works; the borrow-checker conflict that blocked the original attempt is sidestepped by decoupling the mut-write phase from the shared-ref phase. **However, two findings blocked landing the refactor in this wave:** (1) **Size savings are deferred.** `Action`'s dominant variant is `FailReply { cause: ProtocolError }` (856 bytes from five `heapless::String<N>`s inside `ProtocolError::ServerErrorResponse`), not `SendBytes`. Shrinking `SendBytes` from 528-byte `SendBuf` to 16-byte `&[u8]` does not shrink `Action` or `OutActions` until DEF-060 (typed SCRAM error enums replacing `heapless::String<128>`) and DEF-061 (`Errored(ErrorKind)` replacing stored `ProtocolError`) land first. The immediate perf gain is only the per-connection ~1 KB of avoided memcpy on startup + SASL frames. (2) **Test disruption cost.** ~74 call-site updates across `tests/*.rs` to thread a `&mut WriteBuf` through every `proto.feed_bytes` / `proto.push_command` call (caller-owned write buffer is the cleaner design — lets `proto.state()` be inspected alongside `OutActions<'buf>`). Ship order: land DEF-060 + DEF-061 first (those alone shrink `ProtocolError` meaningfully), then land DEF-094 with the full ~10× `OutActions` size reduction realised (3464 B → ~320 B). |

## 14. Phase-1c batch 2 (2026-04-19, same day)

Second 1c wave: paranoid-audit cleanup (F1 user-feedback-alignment,
H1/H2/H3 dead-API removal), tier-3 refactors (A2 Credentials
typestate, DEF-077 NonErroredState), and structural perf (DEF-094
Action<'buf> staged-dispatch). Split into multiple commits per
audit-finding cluster to preserve bisect-ability.

### Commit 2a — cleanup wave (F1/F2/H1/H2/H3)

| ID | Status | What closed it |
|---|---|---|
| AUDIT-F1 | Closed | **`let _ = write_fmt(...)` silent-truncation class eliminated.** 7 sites in `dispatch.rs` built `ProtocolError::ScramError { detail: heapless::String<128> }` via `let mut detail = heapless::String::new(); let _ = core::fmt::Write::write_fmt(&mut detail, format_args!("{e}"));` — the `let _` discarded the `Result<(), fmt::Error>` returned by `write_fmt`. `heapless::String::write_fmt` returns `Err(fmt::Error)` on capacity exhaustion, *after* possibly writing partial content: classic tier-3 silent-truncation pattern. **Also a user-feedback violation** per `feedback_no_underscore_vars.md` ("never `_var`, `let _`"). Replaced with single helper `scram_err_from<E: Display>(e: E) -> ProtocolError` marked `#[cold]`. The helper explicitly branches on `is_err()`: on cap exhaustion it `clear()`s the partial content and substitutes a static sentinel `"scram error (detail truncated)"` (30 bytes ≤ 128 cap). Fallback's `try_from` Err branch surfaced via `if let Ok` — on cap tightening below 30 bytes, detail stays empty rather than panic. Call-site savings: 7 × 4-line blocks → 7 × one-line `.map_err(scram_err_from)` / `cause: scram_err_from(e)`. Superseded by DEF-060 (typed SCRAM enum) when that ships. |
| AUDIT-F2 | Closed | **`let _ = ...` banned pattern in tests eliminated.** 12 sites across `tests/startup_spec.rs` and `tests/tier_seams_spec.rs` used `let _ = proto.feed_bytes(...)` / `let _sasl_response_bytes: Vec<u8> = ...` / `let (_proof, expected_server_sig) = ...`. All replaced with explicit forms: setup-side-effect frames use `drop(proto.feed_bytes(...))` (explicit discard of `#[must_use]` result, no underscore binding); shape-check matches use `match ... { [pat] => {} other => panic!(...) }` form (no `let _name` binding); tuple destructuring where only one field is needed uses `compute_client_proof(...).1` (field access, not underscore binding). Comment in `ping_spec.rs` referencing the banned pattern as "what not to do" kept — documentation of the ban itself. |
| AUDIT-H1 | Closed | **`SendBuf::len` / `SendBuf::is_empty` dead public API removed.** Verified via grep across workspace: neither method called from src/ or tests/. At v1.0.0-alpha.0 the architect.txt Part V "no v<1.0 back-compat shims" applies — dead public surface is not "backwards compatibility", it is just dead code waiting to ossify into a contract. Deleted cleanly. |
| AUDIT-H2 | Closed | **`Sensitive::get_mut` dead public API removed.** Same rationale as H1. `Password` (the only `Sensitive<T>` user currently) is constructed once, borrowed via `&[u8]` through `get().as_bytes()`, then dropped. No mutation path needed. |
| AUDIT-H3 | Closed | **`sasl_initial_response_frame_size` / `sasl_response_frame_size` visibility tightened `pub → pub(crate)`.** Both are const fn used exclusively inside the same `scram::wire` module as drift-guard inputs to `const _: () = assert!(MAX_OWNED_SEND_LEN >= …)`. External callers have no use case — they cannot override `MAX_OWNED_SEND_LEN` or influence the drift guard. Tighter visibility narrows the public surface of the crate at v<1.0 before ossification. |

### Commit 2b — A2 Credentials typestate (ScramSession)

| ID | Status | What closed it |
|---|---|---|
| AUDIT-A2 | Closed | **`Credentials` double-match seam eliminated via `ScramSession` typestate (tier-3 → tier-1 compile).** Before the fix, two independent sites in `dispatch.rs` — the `AUTH_SASL` arm of `dispatch_auth_in_startup` and the head of `build_sasl_initial_response` — each matched `Credentials` directly, classifying `Trust` as an error. A body swap between the two sites' arms (e.g. flipping `Trust → success, ScramPassword → error`) compiled cleanly: the two match sites had no structural linkage — the compiler could not see that they were discriminating the same value across call boundaries. Fix: new `pub(crate)` module `scram::session` with struct `ScramSession { password: Sensitive<Password> }`. The only constructor is `ScramSession::try_from_credentials(Credentials) -> Result<Self, ()>` — the unique decision site for Trust-vs-ScramPassword. Every downstream site takes `&ScramSession` (shared borrow) or owns one: `build_sasl_initial_response(_: &ScramSession)` (parameter shape load-bearing; anonymous binding since password isn't needed until SASL-continue), `dispatch_auth_sasl_continue(scram: ScramSession, ...)` (owns), `ProtoState::ConnectingScramAwaitServerFirst { scram: ScramSession, ... }` (field-owns). **The `Credentials::Trust` variant cannot reach any SCRAM-path site by type** — a body drift in any one site becomes a compile error (the variant does not exist in `ScramSession`'s shape), not silent semantic breakage. Bonus: `Err(())` instead of `Err(Credentials)` keeps the result-type 32 bytes vs 1040 bytes; no `#[expect(clippy::result_large_err)]` needed. Memory-layout savings on `ProtoState::ConnectingScramAwaitServerFirst`: ~8 bytes per instance (Credentials's enum discriminant + padding eliminated). Test construction sites updated to nest `if let Ok(pw) && let Ok(scram) = ...` (let-chain, stable Rust 1.65+). |

## 15. Phase-1c batch 3 + 4 (2026-04-19, mega-session)

Paranoid audit round 2 surfaced 27 findings; the most impactful
landed in this session. Sequence driven by the architect's
recommended order (DEF-061 unblocks DEF-060 and DEF-094; A4 and
polish items flow around them).

### Commits in this session

| Commit | ID | Notes |
|---|---|---|
| `c616f1b` | AUDIT-C1/D1/E1 | `_not_sync` → `sync_marker`; +11 Send asserts; +7 needs_drop asserts |
| `3047ef2` | AUDIT-A4 | `DispatchOutcome::Advanced { action: Option<Action> }` → 2 distinct variants (AdvancedSilent / AdvancedWithAction). Tier-3 arm-drift seam elevated to tier-1 compile. |
| `adf14da` | AUDIT-H1/H2/H3/H4 | reforge.md §16/§17/§19/§20 drifts corrected to match actual code |
| `6d2bb9f` | **DEF-061** | `ProtoState::Errored(ErrorKind)` — 1-byte state vs 856-byte ProtocolError |
| `e827bf2` | **DEF-060 pt1** | `ProtocolError::Scram(ScramError)` typed variant, silent-truncation class eliminated (5 sites) |
| `19e2aab` | **DEF-060 pt2** | `ServerErrorResponse` typed fields: `Severity` enum (1 byte), `SqlStateCode` newtype ([u8;5]), `BoundedStr<N>` with explicit `"…"` truncation marker. Size 848 → 280 bytes. |
| `1ff1c2d` | **DEF-094** | `Action<'buf>::SendBytes(&'buf [u8])` staged dispatch. Caller-owned `WriteBuf`. Zero-copy send path. |

### Closed DEFs

| ID | Status | What closed it |
|---|---|---|
| DEF-060 | **Closed** | Two parts. (1) `ScramError { detail: String<128> }` → `Scram(scram::wire::ScramError)` (typed 11-variant enum). The `unwrap_or_default()` silent-truncation class is structurally absent — no intermediate string to truncate. `scram_err_from<E: Display>` helper gone. (2) `ServerErrorResponse` fields: severity → `Severity` enum (1 byte, 9 variants + Unknown); code → `SqlStateCode([u8; 5])` (5 bytes, byte-packed); message/detail/hint → `BoundedStr<N>` (96/64/64 bytes) with explicit `"…"` truncation marker on overflow (previously `unwrap_or_default` → empty string). Size: 848 → ~280 bytes. Tier-4 silent-truncation eliminated across 5 parse-time sites + 4 static-string construction sites. |
| DEF-061 | **Closed** | `ProtoState::Errored(ErrorKind)` — `ErrorKind` is a 6-variant `#[repr(u8)]` enum (Framing / Transport / ServerError / Auth / Internal / AlreadyClosed). Fatal's full `ProtocolError` goes out **once** in the first `FailReply` action; subsequent pushes against Errored emit `ProtocolError::ConnectionAlreadyClosed { prior_kind: ErrorKind }` (17-byte compact echo). `ProtoState::Errored` went from 856 bytes to 1 byte. Cold-path clone cost on every push against Errored: eliminated. `ProtocolError::kind(&self) -> ErrorKind` exhaustive match — adding a new ProtocolError variant without classifying it is a build error (tier-1 compile). |
| DEF-094 | **Closed** | Two-phase staged dispatch. **Phase 1 (write):** dispatchers take `&mut WriteBuf` (caller-owned) and emit `StagedAction` values carrying ranges (`SendBytesRange { start, end }` or `SendBytesStatic(&'static [u8])`) — no refs. **Phase 2 (ref):** once the write-phase's mutable borrow releases, the entry-point iterates `StagedActions` and materialises `Action<'buf>::SendBytes(&'buf [u8])` into `OutActions<'buf>`. The `'buf` lifetime is tied to the caller's WriteBuf (not PgProtocol) — **`proto.state()` / other shared-borrow inspection works alongside a live `OutActions<'buf>`**; only the next `&mut wb` call is blocked until OutActions drops. Tier-1 compile enforcement of consume-before-next-call. `SendBuf` / `SendBufFull` removed from public API. `Action::SendBytes` shrank from 528 bytes (inline buffer) to 16 bytes (fat pointer). Ping's static `SYNC_WIRE_BYTES` takes the zero-write path via `SendBytesStatic`. ~74 test sites threaded a `&mut wb` parameter and `let mut wb = WriteBuf::new()` at the top of each test. Post-`OutActions` inspection of `proto.state()` works cleanly (no blocker). |

### Audit-finding cleanup

| ID | Status | What closed it |
|---|---|---|
| AUDIT-C1 | Closed | `_not_sync` field → `sync_marker`. Load-bearing !Sync gate; leading underscore was misleading ("unused in purpose" convention — but the field IS structurally used). Also: `let (code, _rest) = ...` in `dispatch_auth_ok_after_scram` → `Ok((code, _))` anonymous-pattern discard (not a `_`-prefixed identifier). |
| AUDIT-A4 | Closed | `DispatchOutcome::Advanced { action: Option<Action> }` split into `AdvancedSilent { new_state }` and `AdvancedWithAction { new_state, action }`. Drift that flipped a meaningful `Some(act)` to `None` silently compiled before; now a compile error (AdvancedWithAction requires the field). 9 dispatch sites reclassified; 3 match arms in `feed_bytes` exhaustively handle the three variants. |
| AUDIT-D1/E1 | Closed | Send + needs_drop const-assertion coverage expanded from 17+7 types to 28+13 types. Future refactor introducing non-Send or non-Drop semantics into any of the protected types fails at crate root. |
| AUDIT-H1/H2/H3/H4 | Closed | reforge.md drifts fixed: §16 PgProtocol shape (Phase 1b actual vs full target); §17 ProtoState (single ConnectingScram → three typestate variants per DEF-002); §19 SendBuf (two-arm enum → single-shape newtype per DEF-089; plus DEF-094 target shape); §20 parse_header (spec text contained a forbidden `as` cast — replaced with `usize::try_from` form). |

### Remaining open (re-prioritised)

**Blocking for Phase 1c close:**
- **DEF-062** — NoticeResponse pre-dispatch filter. Small, structural, follows DEF-054 pattern.
- **DEF-064** — `parse_error_response` bounded loop. Tiny DoS-shield fix.

**Registered, not blocking:**
- **DEF-063** — handshake-flow typestate module (tier-2 → tier-1 on invalid-step call).
- **DEF-065** — SCRAM message assembly writes into WriteBuf directly (perf + simplification). Superseded by DEF-107.
- **DEF-077** — `NonErroredState` typestate. **Recommend skip after DEF-061**: Errored is now 1 byte, so "preserve Errored across mem::take" costs ~zero. DEF-077 would move it from tier-2 structural to tier-1 compile, but the payoff is marginal now. Re-evaluate if the preservation pattern becomes a hotspot.

## 16. Phase-1c batch 5 — post-DEF-094 architect audit (2026-04-20)

Full `bsql-pg-proto` audit by the rust-senior-architect agent
catalogued **24 findings** ranked by `(win × tier-elevation) / cost`.
See `reforge.md` §17.1 for the master table. Execution is tier-elevation
→ security → perf, per user directive.

### Closed in this session

| Commit | ID | One-liner |
|---|---|---|
| `8e4690a` | **DEF-095** | `Password.len: u16` + SCRAM const-generic drift guard + `record_param_status` let-else compression |
| `ac57e62` | **DEF-096** | `FixedStr<N, Tag>` generic — 4 string types → 1 POD form with phantom-tag nominal typing |
| `052febe` | **DEF-097** | `ConnectingStartup` → `Trust | Scram` typestate split. "Server asked wrong auth method" becomes a type-level impossibility. |
| `fd1f5cd` | **DEF-098** | `size_of` drift-guard tightening post DEF-095/096/097 (ProtoState budget 2048 → 1280, etc.) |
| `ecee97c` | **DEF-099** | `PodBytes<N>` for SCRAM state buffers + pattern-rationale doc (`.get(..n).unwrap_or(&[])` is forbid-bundle idiom, not kludge) |
| `8ff256f` | **DEF-100** | `NonEmptyRange { start, len: NonZeroUsize }` replaces raw `(start, end)` on `StagedAction::SendBytesRange` — non-empty is a type invariant, zero-length SendBytes can't compile. Tier-3 audit → tier-2 structural. |
| `b0dbd46` | **DEF-101** | Path audit + DEF-052 close via `cfg(test)` thread-panicking guard (re-scoped from original "remove Drop" — honest tier analysis below). |
| `43c1877` | **DEF-111** | §10: const-assert per-direction `wire::TAG_*` distinctness + `AUTH_*` sub-code distinctness. Catches copy-paste duplication at build time. |
| `ce6a8bf` | **DEF-112** | §2: typed `ReplyId<K: ReplyKind>` + sealed `action::deliver<K>(id, payload)` constructor. Tier-1 compile on "dispatcher emits wrong Reply variant for command-kind". |
| *next*   | **DEF-114** | §4 (selective): `is_superuser` / `integer_datetimes` → `Option<bool>`; `server_encoding` / `client_encoding` → `Option<Encoding>` with `Other(OtherEncoding)` information-preserving fallback. |

### DEF-101 re-scoping (honest tier analysis)

The architect's original proposal (audit finding #7) was to *remove*
the `ReplyId::Drop` impl after a full-path audit proves no production
path reaches an undelivered drop. After landing the work I realised
removing Drop would be a **tier regression**, not elevation:

- **Stable Rust has no linear types.** "Cannot drop unconsumed" is
  structurally impossible as a tier-1 compile invariant. Even with
  `#[must_use]` + `deny(unused_variables)` + no-Copy, patterns like
  `let r = id(); r.get(); // scope-drops` silently compile.
- **The Drop-guard catches exactly that residual class at runtime.**
  Removing it would replace tier-2 runtime with tier-3 audit
  (= strictly weaker guarantee).
- Production uses `panic = "abort"`, so the guard aborts cleanly —
  no UB, no hang. It *is* the ceiling for stable-Rust safety on this
  invariant.

**What DEF-101 actually delivered:**

1. **Full-path audit (tier-3 → tier-2 evidence).** Every
   `core::mem::take(&mut self.state)` site in `protocol.rs`
   (`push_command`, `feed_bytes` dispatcher loop, `fail_inflight_and_close`)
   exhaustively matches the returned state. Every ReplyId-carrying
   variant either consumes via `.consume()` or re-places the id in
   the next state variant. Every `DispatchOutcome::Errored { reply_id:
   Some(id), ... }` site consumes `id.consume()` exactly once at the
   feed_bytes layer. Every `compute_push_*` function owns the
   incoming `reply: ReplyId` and threads it to consume or
   re-placement. No path reaches scope-drop in production.
2. **DEF-052 close.** The `Drop::drop` body now has a
   `#[cfg(test)] if std::thread::panicking() { return; }` branch
   that prevents the double-panic diagnostic-masking class. Zero
   cost in production (`panic = "abort"` never unwinds).
3. **Regression test** `unrelated_panic_while_reply_id_alive_surfaces_original_message`
   exercises the fix: a test panics for an unrelated reason while a
   non-delivered ReplyId is alive; `#[should_panic(expected =
   "unrelated panic")]` asserts the original message survives.

The Drop-guard stays. The user-facing guarantee is strictly stronger
than before (audit written down + regression test added + DEF-052
closed), but the tier label remains "tier-2 runtime" — because that
*is* the stable-Rust ceiling and we're not going to pretend otherwise.

### Open — security

- **DEF-102** — `base64` → `base64ct` swap. `base64ct` is RustCrypto's constant-time, `no_std`, branchless encoder. ClientProof encoding over a secret-derived byte array becomes side-channel-free in formal sense (vs current "probably constant-time because the lookup table is cache-line-sized"). Small API shift — replaces workspace `base64 = "0.22"`.

### Open — perf/architecture wave

- **DEF-103** — `core::hint::cold_path()` at every `DispatchOutcome::Errored` construction site. ~20 sites in `dispatch.rs`. Hot-path I-cache improvement via LLVM block-layout.
- **DEF-104** — `parse_error_response` field dispatch via `[Option<FieldKind>; 256]` static table. The nine-arm match on field_type byte becomes a table lookup + six-variant kind match. Legibility + jump-table emission on cold path.
- **DEF-105** — `OutActions<'_>` shrink via retuning `ServerErrorResponse.{message,detail,hint}` `BoundedStr` bounds. Current: 128/96/64. Candidate: 96/64/48 (−64B) or 64/48/32 (−144B per `ProtocolError`). Propagates to `Action<'_>` and `OutActions<'_>`. Weigh against real-world PG error-message length profile.
- **DEF-106** — `SessionParams` POD layout. Current: 9 × `Option<heapless::String<128>>` ≈ 1233 bytes (90%+ zeroed padding). Candidate: flat `[u8; TOTAL]` + `slot_bitmap: u16` + `slot_ends: [u16; 9]` ≈ 600 bytes. POD, Drop-free, better cache behaviour.
- **DEF-107** (supersedes DEF-065) — SCRAM wire builders write-into caller-owned buffers. `generate_client_nonce()` returning `heapless::Vec<u8, N>` → `generate_client_nonce_into(&mut PodBytes<N>) -> Result<(), ScramError>`. Same for `build_client_first_bare`, `build_client_first_message`, `build_client_final_*`. Removes the heapless::Vec builder-return pattern from the SCRAM hot path; state bufs already POD (DEF-099), wire bufs follow.
- **DEF-108** — `std::simd::u8x32` for ClientKey ⊕ ClientSignature XOR in `compute_client_proof`. Current: 32-iteration zip loop; candidate: single `vpxor`. Cold path (once per connection), but architectural correctness — constant-time XOR via portable SIMD is the canonical SCRAM pattern.

### Open — validation gates

Close only after `cargo asm` confirms the win is real:

- **DEF-109** — `Severity::from_bytes` first-byte dispatch. Current: 9-arm `match bytes` on byte-slice literals. If LLVM folds to byte-tree / memcmp chain (likely): skip. If branch chain (unlikely on cold path): ship first-byte table.
- **DEF-110** — `ProtocolError::kind` repr optimisation. Current: exhaustive match, called on every fatal classification. If jump-table already emitted: skip. Otherwise: `#[repr(u8)]` discriminant reordering to pack by ErrorKind.

### Re-evaluated and skipped after exploratory work

- **DEF-113 / §5 — Internal StagedAction Success/Teardown split.**
  Architect's original proposal: split `StagedAction` into
  `SuccessStaged` (SendBytes\*/DeliverReply) and `TeardownStaged`
  (FailReply + CloseSocket), so a dispatcher function signed
  `fn … -> SuccessStaged` cannot emit CloseSocket. Explored and
  dropped: `compute_push_*` naturally produces BOTH success
  emissions AND "soft-reject" FailReply emissions (e.g. pushing
  Ping onto an Errored connection yields `FailReply { cause:
  ConnectionAlreadyClosed }` without CloseSocket — the socket is
  already closed). A clean Success/Teardown partition requires
  a third "SoftReject" bucket, inflating the enum family and the
  dispatcher signatures. The CloseSocket-via-sealed-constructor
  variant gives a weaker gate (tier-3: "only sanctioned helpers
  call the constructor, anyone crate-internal can call the
  helpers"). Net: the tier-1 claim requires restructuring
  `compute_push` that doesn't pay for itself at Phase 1c scope.
  Re-evaluate when the driver work in Phase 1c lands new command
  variants and the action surface reshapes anyway.

### Legitimately rejected — do not revisit without new evidence

- **#8 — ASCII fast-path bit on `FixedStr::as_str`.** stdlib `core::str::from_utf8` already SIMD-dispatches on ASCII; the "cache the ascii bit" optimisation would skip nothing unless we also skipped from_utf8 entirely, which requires `unsafe { from_utf8_unchecked }` → banned by `#![forbid(unsafe_code)]`. No win.
- **#9 — `WriteBuf::with_length_prefix` closure inline.** Already inlined by LLVM at every known call site; `#[inline]` hint redundant.
- **#14 — `HeaderParse` slice-pattern match.** LLVM folds slice-pattern exhaustive matches to a length check + field extract; `cargo asm` confirms equivalent codegen to manual length-check.
- **#16 — `parse_u32` replaced by stdlib.** Current hand-rolled form takes `&[u8]` (already ASCII digits from SCRAM parse); stdlib `str::parse::<u32>` would require `core::str::from_utf8` first — an O(N) UTF-8 revalidation on already-ASCII bytes. Net negative.
- **#19 — Session-params perfect-hash dispatch.** Cold path (9 ParameterStatus frames per connection); current linear match against 9 byte-string literals is already compiler-optimal for this size. Re-evaluate if session_params set is called mid-flight for DB SET commands.
- **#24 — `emit_actions!` `unlikely` hint.** The overflow branch is const-asserted as architecturally dead (`const _: () = assert!(MAX_ACTIONS_PER_CALL >= N)` per emit site). LLVM cold-hoists the dead branch during DCE. Adding `unlikely` over a provably-dead branch is redundant.

## 10. Closed

Move entries here when a deferral is genuinely resolved — not just
"implemented one phase later", but actively shipped with the invariant
closed and verification in place. Empty for now.

---

**Commit-message convention:** when you close or touch an item here,
include the ID in the commit body:

```
feat(pg-proto): close DEF-013 — SendBuf::Owned for StartupMessage

...
```

**Contract:** no architectural deferral is valid unless it is recorded
here. If you deferred something and did not add it to this file, you
broke the contract.
