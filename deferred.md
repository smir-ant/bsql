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
| DEF-062 | **CLOSED** (landed `cccc909`) — pre-dispatch filter in `feed_bytes` silently consumes tag `'N'` frames, mirroring the DEF-054 ParameterStatus pattern. Phase 1b form drops notices; future `Action::EmitNotice(...)` lands in Phase 1c+ when the wrapper surfaces notices. Regression test `notice_response_mid_flight_is_silently_consumed` in `ping_spec.rs`. |
| DEF-063 | **CLOSED — substantively met** by the post-DEF-097 + DEF-112 form. Current `ProtoState` variants (`ConnectingStartupTrust`, `ConnectingStartupScram`, `ConnectingScramAwaitServerFirst/Final/AuthOk`, `ConnectingPostAuthWaitKey/HaveKey`) ARE the handshake typestate chain — each variant carries exactly the step's required fields (DEF-097 trust/scram split; DEF-112 `ReplyId<StartupKind>` typing). The `match (state, tag)` exhaustive dispatch enforces tier-1 "step N dispatcher only reachable from step N-1 state". Extracting these into a separate `handshake` module with `fn step(Prev) -> (Next, Action)` signatures would reorganise code without further tier elevation. Revisit post Phase 1c if the query-flow work surfaces a clearer module boundary. |
| DEF-064 | **CLOSED** (landed `cccc909`) — `for _ in 0..MAX_ERROR_FIELDS` with `MAX_ERROR_FIELDS = 32` (≥ 2× PG's ~18 documented fields). Iteration is O(1) regardless of adversarial payload size. Beyond the cap, parsing stops and returns already-extracted fields — benign truncation. Tier-2 structural via the `for` range expression. |
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
| DEF-077 | **CLOSED** (superseded by DEF-061 + DEF-117) — `NonErroredState` refactor is architecturally unnecessary after the two composing DEFs. DEF-061 shrunk Errored to 1-byte `ErrorKind` (preservation clone is free). DEF-117 changed `fail_inflight_and_close` from `mem::take` to `mem::replace(&mut self.state, Errored(kind))` — the transient window no longer defaults to Idle; it IS the post-state. The "forget to restore Errored" class cannot arise at that site, and no other site exhibits the pattern. Tier-3 regression test `errored_cause_is_preserved_in_state_and_reply` stays as category-2 guard. | — | Closed 2026-04-20 (round-3 audit wave). |
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
- ✅ ~~**DEF-062**~~ — landed `cccc909` (pulled forward into Phase-1b hardening).
- ✅ ~~**DEF-064**~~ — landed `cccc909`.

**Registered, not blocking:**
- ✅ ~~**DEF-063**~~ — substantively met by DEF-097 + DEF-112 (handshake typestate already enforced via `ProtoState` variants + exhaustive dispatch). Module extraction is organisational, not tier-elevating.
- **DEF-065** — SCRAM message assembly writes into WriteBuf directly (perf + simplification). Superseded by DEF-107.
- **DEF-077** — `NonErroredState` typestate. **CLOSED — superseded** by DEF-061 + DEF-117. The original seam was "`mem::take(&mut self.state)` swaps Errored out and defaults to Idle in the transient window; forgetting to write Errored back silently re-opens the connection." DEF-061 made `Errored` a 1-byte `ErrorKind` (clone cost ≈ zero), and DEF-117 replaced the load-bearing `mem::take` in `fail_inflight_and_close` with `mem::replace(&mut self.state, ProtoState::Errored(kind))` — the transient window IS the post-state, so the "forget to restore Errored" class is architecturally impossible. A separate `NonErroredState` typestate would add ~80 LoC of wrapping for a seam that has already been closed at its root. Tier-3 `errored_cause_is_preserved_in_state_and_reply` test still pins the preservation behaviour as category-2 regression guard.

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

### Round 3 — additional escalations (2026-04-20)

Round 3 architect audit caught tier overclaims that rounds 1/2 missed.
All reaffirmations / escalations below.

| Commit | ID | One-liner |
|---|---|---|
| `324948f` | **DEF-115** | seal `FixedStrKind` / `Validated` (escalation of DEF-096) — external crates could impl these for their own tags; tier-4 hole closed by sealed supertrait. |
| `324948f` | **DEF-116** | sorted-array collision loop (escalation of DEF-111) — **blocked** on MSRV 1.95 (`<[T]>::get` not const-stable; `arr[i]` banned by `forbid(indexing_slicing)`). Pragmatic form: keep hand-unrolled const asserts + add `#[cfg(test)]` drift-guard tests walking parallel `*_FOR_RUNTIME_CHECK` arrays. Revisit when Rust stabilises `<[T]>::get` in const. |
| `324948f` | **DEF-117** | `core::mem::replace` instead of `take` in `fail_inflight_and_close` — eliminates the "`ProtoState::Default = Idle` is load-bearing for transient window safety" invariant. tier-3 → tier-1. |

### Round 3 — additional closures (perf / security wave)

| Commit | ID | One-liner |
|---|---|---|
| `3129d1e` | **DEF-102** | `base64ct` swap (constant-time SCRAM proof encoding; tier-3 audit → tier-1 RustCrypto-audited) |
| `5d2d03d` | **DEF-103** | `#[cold] #[inline] fn errored(...)` helper centralises 44 DispatchOutcome::Errored sites for LLVM block-layout cold-hinting |
| `f6313c5` | **DEF-106** | `SessionParams` per-field right-sized `BoundedStr<N>` — ~400 bytes saved in `PgProtocol`, Drop-free, overflow now truncates with `"…"` marker instead of silent value-drop |

### Round 3 — honestly skipped with rationale

- **DEF-104 — ErrorResponse field-kind table.** Architect: "architectural legibility, not perf — cold path". Current 6-arm match on `field_type` byte compiles to a byte-tree that LLVM folds. Separating into "byte→kind" + "kind→action" double-match would move the arm-body drift seam but not close it. Skip.
- **DEF-105 — `OutActions` shrink via `ServerErrorResponse.{message,detail,hint}` bounds tuning.** Reducing 128/96/64 → 96/64/48 saves ~64-144 bytes per `ProtocolError` but truncates real PG error messages more aggressively. Requires production error-length profile to justify. Skip without data.
- **DEF-108 — `std::simd::u8x32` XOR for ClientKey ⊕ ClientSignature.** `std::simd` portable SIMD is still unstable on MSRV 1.95 (tracked in rust-lang/rust#86656). Current zip-iterator form auto-vectorises via LLVM on x86-64-v2+ and aarch64. No stable-Rust path. Defer until portable SIMD lands on stable.
- **DEF-107 — SCRAM wire builders write-into-caller-buffer.** Architectural cleanup: eliminates the heapless::Vec → PodBytes copy step in `build_sasl_initial_response`. Cold path (SCRAM once per connection). Worth doing but scope-boundary fit is awkward (`build_client_first_message` is short-lived and fine returning heapless::Vec; only the two state-bound builders benefit). Schedule: revisit during Phase 1c if the SCRAM flow reshapes; standalone commit has low cost-benefit ratio.
- **DEF-109 / DEF-110 — `Severity::from_bytes` / `ProtocolError::kind` codegen review gates.** Pending `cargo asm` infrastructure. Not blocking Phase 1b.

### Round 3 — deferred with honest rationale

- **DEF-118 — `ParsedFrame<'_>` proof-token for parse_header → advance (P2.6).** Architect proposed tying `ReadBuf::advance`'s amount to `parse_header`'s output via a non-Clone typed token, so that a future refactor passing the wrong `total_len` becomes a compile error. **Two forms were explored:**

  - *Ambitious form* (generative lifetime): `HeaderParse<'a>::Ok { advance: FrameAdvance<'a> }`, with `consume_frame(&mut self, FrameAdvance<'_>)`. Real tier-1 compile gate. Cost: changes `HeaderParse` to non-Copy, lifetime-carrying; propagates to every caller (including external tests). Non-trivial rework.
  - *Minimal form*: `FrameAdvance { total_len: usize }` with `pub(crate)` constructor, added alongside existing `pub fn advance(usize)`. Inside `feed_bytes`, internal dispatchers use `consume_parsed(FrameAdvance)`; external tests retain `advance(usize)`. The tier claim becomes "internal dispatchers use the typed path" — **tier-3 audit (internal convention), not tier-1 compile**. Does not beat DEF-111's current form.

  Both forms punt against the current API shape. The real tier gain requires the ambitious form; the minimal form doesn't elevate. **Defer until Phase 1c pipelining reshapes the feed_bytes loop anyway**, at which point the ambitious form is a natural extension.

- **DEF-119 — `PgProtocol<Phase>` outer typestate (§2.1).** Architect: "single biggest tier elevation available". `PgProtocol<Idle>` accepts `push_command`; `PgProtocol<InFlight<K>>` does not. `push_command` while `AwaitingPingReply` → compile error instead of runtime `FailReply(UnexpectedFrame)`. Three classes of runtime failures (push-while-in-flight, startup-twice, command-after-close) → compile errors. Cost: ~150 LoC `PgProtocol` restructure + every test harness update; changes `push_command` signature to `self → Self` transition. **Schedule with Phase 1c pipelining** — the transition is natural there, and pipelining forces a new in-flight model anyway that DEF-119 naturally folds into.

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

## 17. Test audit — tier classification per reforge §4.11 (2026-04-20)

Walk-through of every test in `bsql-pg-proto` against the
category framework of reforge §4.11. Per the framework, a test
exists only if it defends one of:

- **(1) Spec conformance** — externally-observable API behaviour
  on valid/invalid input matches the PostgreSQL wire contract.
- **(2) Tier-3 invariant defense** — a narrow seam the compiler
  does not express structurally (arm-body drift, key→field
  routing, etc.).
- **(3) Compile-time invariant documentation** — `assert_send::`,
  `const _: () = assert!(…)`, `compile_fail` doctests.

Tier-1 and tier-2 invariants have no place in tests — they are
held architecturally (build failure or structural impossibility).

### Test inventory

82 tests total, spread across 5 integration files + 4 unit
modules. Each file's preamble documents its category scope
(rechecked during the round-3 audit):

| File | Tests | Category | Scope |
|---|---|---|---|
| `tests/bounded_buffers_spec.rs` | 15 | (1) | `ReadBuf` / `WriteBuf` / `CappedServerNonce` API contract: bounded-capacity overflow (exact `ReadBufFull` sizes), `advance` returns `AdvancePastEnd`, lazy-compact correctness. Every test is a contract pin that protects against silent drift in the buffer primitives the wire layer depends on. |
| `tests/frame_parse.rs` | 7 | (1) | `parse_header`'s observable contract: clean header → `HeaderParse::Ok {tag, declared_len, total_len}`, empty → `Empty`, incomplete → `Incomplete`, declared < 4 → `MalformedLength`, declared > cap → `FrameTooLarge`. Panic-freedom and no-index-panics are tier-1 closures held architecturally (forbid-bundle + slice patterns); not tested here. |
| `tests/ping_spec.rs` | 12 | (1) + (2) | Phase-1a end-to-end Ping flow against spec (A); bad-path coverage (ErrorResponse mid-flight, unsolicited ParameterStatus, extra RFQ in Idle, malformed RFQ, dropped ping at end). Category-2 coverage: the "silent reply loss" class (Drop-guard) and `PgProtocol: !Sync` compile-asserts in `src/lib.rs`. |
| `tests/startup_spec.rs` | 21 | (1) + (2) | Phase-1b full handshake: trust auth end-to-end, SCRAM-SHA-256 with RFC 7677 Appendix A vectors, NegotiateProtocolVersion rejection, pipelined-startup rejection, unsolicited ParameterStatus variants across every pre-auth state, SCRAM iteration-count-too-low, nonce-prefix-mismatch, unknown auth sub-code, ErrorResponse mid-handshake. Category-2: `errored_state_is_terminal_and_drops_subsequent_frames`, `startup_on_errored_state_fails_with_stored_cause`. |
| `tests/tier_seams_spec.rs` | 8 | (2) | Pure category-2 tier-3-shield tests. `session_params_set_key_routing_table` (S3 seam — key→field arm drift), `errored_cause_is_preserved_in_state_and_reply` (U3 seam — mem::replace preservation pinned even after DEF-117 made it structural, as category-2 regression guard), `database_name_validation`, `application_name_validation_allows_empty`, `backend_key_data_wrong_payload_size_is_classified`. |
| `src/dispatch.rs::parse_error_response_tests` | 7 | (1) | `parse_error_response`'s field-type arm coverage (S/V/C/M/D/H/unknown), empty payload, severity ordering (S wins over later V; V used when S absent), duplicate code handling, unterminated final field. Category-1 contract pins on the `ErrorResponse` parser's observable behaviour. |
| `src/protocol.rs::compute_push_tests` | 8 | (2) | Compute-push arm seams per pg-command × proto-state table: Ping from Errored/AwaitingPingReply/each Connecting* state, Startup from every non-Idle state, each Startup-chain variant preserves its `StartupKind` correlator. Closes the compute-push-arm drift class. |
| `src/protocol.rs::allows_unsolicited_param_status_tests` | 1 (`policy_per_variant`) | (2) | Exhaustive walk of every `ProtoState` variant through the `allows_unsolicited_param_status` predicate. Pins "which variants accept unsolicited PS" as tier-3 regression guard — adding a new variant that "forgot" which side of the predicate it sits on fails this test. |
| `src/reply_id.rs::reply_id_semantics` | 3 | (2) | `undelivered_drop_panics` (Drop-guard fires), `unrelated_panic_while_reply_id_alive_surfaces_original_message` (DEF-052 close: `cfg(test)` + `thread::panicking()` skip preserves original panic message), `debug_prints_kind_name` (DEF-112 `ReplyId<K>` Debug format pin). |
| `src/scram/crypto.rs::tests` | 1 | (1) | RFC 7677 Appendix A SCRAM-SHA-256 reference exchange bit-exact match (PBKDF2 + HMAC + XOR proof + server signature). Category-1 spec conformance. |

### Conclusions from the walk-through

- **Every test has a documented category.** No test defends a
  tier-1 or tier-2 invariant (those are held architecturally —
  build failure or structural impossibility).
- **Category-2 tests concentrate in `tier_seams_spec.rs`
  (8 tests), `src/protocol.rs` compute-push (8), and allowance
  predicate (1).** These are the surfaces where compiler cannot
  express the invariant — exactly where category-2 tests
  belong.
- **No test duplicates a tier-1 build-failure assertion.** The
  `assert_send::` / `size_of::` / `needs_drop::` /
  `assert_all_distinct!` calls in `src/lib.rs` and `src/wire.rs`
  are category (3), held at compile time — they are not
  replicated as runtime tests.
- **Three tests were removed in round-3.** DEF-116's macro-based
  pairwise distinctness obsoleted `collision_drift_guard` (3
  tests). Architectural move-up from tier-3 test to tier-1
  compile; tests disappeared as the surface that could drift
  disappeared. This matches §4.11's stated ideal.

### Pending audit items

- None. 82 tests, all categorised. `tests/bounded_buffers_spec.rs`
  and `tests/frame_parse.rs` could be shortened if their
  contracts are downgraded to "documented in rustdoc examples"
  instead of independent test assertions — but the dichotomy
  `rustdoc example || integration test` is itself a style choice
  rather than a tier gap. Hold as-is.

## 18. Phase 1c pre-work architectural findings (round 4, 2026-04-20)

Deeper architectural probe before 1c implementation begins. The
architect's 1c analysis recommended runtime-phase-field for
DEF-119 on "async ergonomics" grounds; this round-4 pass
challenged that and found a middle-ground pattern architect had
not enumerated. Plus 6 orthogonal angles on the 1c design
surface.

### DEF-119 re-evaluation — **witness-guard pattern**

**Prior framing (architect round-3).** Move-based typestate
(`PgProtocol<Phase>` consuming `self` on transitions) delivers
tier-1 compile on "cannot push when not Ready", but costs async
ergonomics: async tasks storing `PgProtocol` as a field cannot
update the field's type per operation without wrapping in an
enum (`PgProtocolAnyPhase`) that loses the tier-1 gate.

**Round-4 middle ground.** Witness-guard pattern: `PgProtocol`
stays one type; `ReadyGuard<'p>` / `PipeliningGuard<'p>` are
short-lived borrow-witnesses minted via `proto.as_ready()` /
`proto.as_pipelining()` (returning `Option<Guard<'_>>`).
`push_command` is a method of the guard, not of `PgProtocol`.

```rust
impl PgProtocol {
    pub fn as_ready(&mut self) -> Option<ReadyGuard<'_>>;
    // push_command is NOT on PgProtocol
}

impl<'p> ReadyGuard<'p> {
    pub fn push_command(self, cmd, wb) -> OutActions<'_, 'static>;
}
```

**Tier-1 claim delivered.** "Cannot call push_command without
proving current phase permits it" — `push_command` is not
callable on `PgProtocol` directly; obtaining a guard requires
handling `Option::None` (forced by `unused_must_use`).

**Async storage preserved.** `PgProtocol` is one type;
tokio tasks hold it as a regular field. Guards come and go
inside select! arms. No enum wrapper needed, no async
gymnastics.

**Honest tier boundary.** Witness-guard gives tier-1 compile on
"push requires guard" but tier-3 on "guard existence matches
phase" (runtime `Option` return). That's the same floor as full
typestate at the push-check seam — the typestate's extra tier
claim ("phase transitions carry across calls") is the part
async-incompatible anyway. Guards capture the real available
tier without paying the cost.

Will ship in **1c-5** (pipelining sub-phase).

### Six orthogonal 1c design findings

1. **`InFlightSlot` as sum enum, not u8-discriminant struct.**
   Architect's `InFlightHead { kind_tag: u8, reply_id, row_desc_ref: Option<u16> }`
   loses exhaustive-match discipline on slot kind. Replace with
   `enum InFlightSlot { Query{...}, Parse{...}, BindExecute{...},
   Close{...} }` — tier-2 structural via exhaustive match on slot
   variant. Per-slot size ≈ 12 bytes (8-slot queue → 96 bytes).

2. **Typed sealed newtypes for Sql / StmtName / PortalName.**
   Mirror the DEF-096 Ident/DatabaseName pattern via
   `FixedStr<N, Tag>` sealed. `fn bind(portal: PortalName, stmt:
   StmtName)` — swapped args = compile error. Free out of
   established machinery.

3. **Typed `CommandTag` parsed at ingest.** PG's
   `CommandComplete` body `"SELECT 5"` / `"INSERT 0 3"` parsed
   into `struct CommandTag { kind: CommandKind, rows_affected:
   Option<u64>, insert_oid: Option<u32> }`. User code does
   exhaustive match on `CommandKind` — tier-1 on "typo in
   command-name comparison".

4. **`Flush` vs `Sync` sequencing via guard consumption.**
   Extended-query chains end with `Sync`; `Flush` mid-stream is a
   different semantic. Guard-based: `PipeliningGuard::push_bind_execute(&mut self)`
   keeps guard alive; `PipeliningGuard::finish_with_sync(self)`
   consumes guard and emits terminal Sync. A chain ending on
   Flush alone doesn't consume the guard — cannot satisfy
   guard's Drop-invariant (or terminal transition). Tier-2
   structural via method placement.

5. **Text-format column rejection as typed error.** PG's
   RowDescription specifies per-column `format_code` (0=text,
   1=binary). 1c ships binary-only; text format must surface as
   typed error, not generic `MalformedFrame`:
   `ProtocolError::UnexpectedTextFormatColumn { column_idx }`.
   Tier-3 audit at server-interop layer; user gets clear
   diagnostic.

6. **Zero-copy param binding via `ParamsWriter` trait.** User's
   `(i32, &str, bool)` params should flow directly to WriteBuf
   without intermediate `Params { storage: heapless::Vec<u8,
   1024> }` copy. `trait ParamsWriter { const COUNT: u16; fn
   write_to(self, dst: &mut WriteBuf) -> Result<(), _>; }` +
   per-tuple impls. COUNT const-known → Bind frame's param-count
   field filled at compile time.

7. **`RowDescRef` generational arena guard.** 2-slot `[RowDescriptorBytes;
   2]` arena with simple `u16` index has a stale-ref class: slot
   freed, reused, old InFlightSlot's ref points at wrong row
   shape. Add generation counter: `RowDescRef { slot: u8,
   generation: u8 }` — 2 bytes, arena bumps generation on free.
   `arena.get(RowDescRef)` checks generation match — stale ref
   → None (classified as protocol error). Tier-2 runtime vs
   tier-3 audit miss.

### Sub-phase placement & status

| sub-phase | status | findings landed | scope |
|---|---|---|---|
| **1c-0 / 1c-1** | ✅ done | #2 typed newtypes (Sql/StmtName/PortalName); #3 CommandTag as BoundedStr<32> (typed-struct upgrade deferred to 1c-6) | SimpleQuery end-to-end, Action::StreamRow, DEF-121 gate |
| **1c-2** | ✅ done | #5 text-format rejection (UnexpectedFormatCode classification at parse) | RowDesc parse, DataRowRef + ColumnsIter, FromPgText primitives, oids module |
| **1c-3** | 🚧 in progress | #6 ParamsWriter zero-copy (1c-3b shipped) | Parse/Bind/Describe/Execute/Close extended-query flow + FromPgBinary parallel trait |
| **1c-3a** | ✅ done | — | `PgCommand::Parse` + Sync bundle, `ParseComplete → RFQ → Reply::ParseComplete` |
| **1c-3b** | ✅ done (2026-04-21) | #6 ParamsWriter closed | `push_bind_execute<P: ParamsWriter>` method, sealed `EncodeBinary`/`FromPgBinary` traits, 4 new BindExecute state variants, 12 new dispatch arms, `F19` schema-required shield preserved, `PortalSuspended` classified as UnexpectedFrame (1c-6 lifts) |
| **1c-3c** | ✅ done (2026-04-21) | — | `PgCommand::DescribeStatement` / `DescribePortal` — split command variants for tier-1 API (separate reply payload kinds); `DescribedRows` sum type over `Option<RowDesc>`; `ParamOids` bounded container; 5 new `ProtoState` variants (3 stmt + 2 portal); ~15 new dispatch arms; `DescribeTargetByte` typed wire byte with `b'S'`/`b'P'` drift pins; `TooManyParameters` + `MalformedParameterDescription` classifications; generalised `advance_to_drain_after_error<K: ReplyKind>` eliminating per-kind duplication |
| **1c-3d** | ⏳ pending | — | `Close` (statement or portal) |
| **1c-4** | ⏳ pending | — | BEGIN/COMMIT/ROLLBACK + tx_status tracking + SAVEPOINT |
| **1c-5** | ⏳ pending | #1 InFlightSlot sum enum; #4 Flush/Sync guard; #7 RowDescRef arena; **DEF-119 witness-guard** | Pipelining — biggest tier-lift of Phase 1c |
| **1c-6** | ⏳ pending | #3 typed CommandTag upgrade; DEF-109/110 cargo-asm validation | Hardening + fuzzing + proptest before 1d |

### 1c-1 landed (2026-04-20)

| commit | scope |
|---|---|
| `eaffe4e` | 1c-0 skeletons — wire tags (16 inbound / 9 outbound), typed newtypes (Sql / StmtName / PortalName), ReplyKind markers (QueryKind / ParseKind / CloseKind). |
| `4e3896b` | 1c-1a — `Action<'w, 'r>` two-lifetime refactor + `StreamRow` variant. `'w` for outbound, `'r` for inbound row slices borrowed from `ReadBuf`. |
| `14d386d` | 1c-1b — SimpleQuery dispatch end-to-end. Four new `ProtoState` variants, `StagedAction::StreamRowRange` (absolute coords into `ReadBuf::populated` — survives per-frame advance), `FrameCoords` dispatcher arg, query-level errors survive via `SimpleQueryDrainRfqAfterError`. `CommandInProgress` + `MalformedCommandComplete` error classifications added. `PgCommand` size cap 1344 → 2112 (Sql dominates). |
| `4c33eb0` | 1c-1c — 12 integration tests + generic `Truncating` sealed marker (now covers BoundedStrTag + SqlTag uniformly). Tests: 0-row SELECT, N-row SELECT streaming, DML, empty query, query-level error + connection survival, mid-stream error, in-flight push rejection, Errored-state push rejection, malformed CommandComplete teardown, unexpected-RFQ teardown, across-call row streaming, Q-frame wire layout drift-pin. |
| `6bc1744` | **DEF-121** fix + polish — per-iter budget gate before dispatch (tier-4 silent-reply-loss → tier-2 structural). `MAX_ACTIONS_PER_CALL` 4 → 8 + `WORST_CASE_PER_DISPATCH=2` named reserve. Three shared dispatch helpers (`advance_to_await_rfq`, `advance_to_drain_after_error`, `stream_row_or_errored`) centralise invariants previously duplicated across AwaitFirstResponse / StreamingRows arms. Regression test `overflow_backpressure_preserves_delivery_across_calls`. |

Finding 3 (typed CommandTag) — partially shipped: `QueryCompletePayload::command_tag: BoundedStr<32>` carries the raw PG tag verbatim. Upgrade to typed `CommandTag { kind: CommandKind, rows_affected: Option<u64>, insert_oid: Option<u32> }` deferred to 1c-6 hardening — the parse lives at the ingest boundary in `dispatch::parse_command_tag` so the upgrade is a local refactor.

Test count: 83 → 96 (+12 simple-query spec + DEF-121 regression).

### 1c-2 landed (2026-04-20)

| commit | scope |
|---|---|
| `8d3c5df` | 1c-2a — `decode` module, `RowDesc` + `ColumnDesc` + `FormatCode`, `parse_row_description`, `PgProtocol.row_desc` slot, `Action::StreamRow.desc: &'r RowDesc`, `Reply::QueryComplete.row_desc: Option<RowDesc>`, clear-on-new-push discipline, new error variants (`MalformedRowDescription`, `TooManyColumns`, `UnexpectedFormatCode` — round-4 #5 shipped), `Reply` size assert 64→320. 10 parse tests + `dml_after_select_clears_row_desc` regression. |
| `7513a09` | 1c-2b — `DataRowRef` + `ColumnsIter` zero-copy row-body parser with SQL NULL handling (`length = -1` → `Ok(None)`), `DecodeError` classification (`TruncatedRow` / `TruncatedColumnLen` / `NegativeColumnLength` / `TruncatedColumnData`), fused post-error semantics, `ExactSizeIterator` + `FusedIterator` impls. 11 unit tests + `stream_row_bytes_decode_via_data_row_ref` integration. |
| `3c3c0b2` | 1c-2c — `FromPgText<'a>` trait + impls for `i16`/`i32`/`i64`/`u32`/`bool`/`&str`, PG-strict bool (`"t"` / `"f"` only), UTF-8 + integer-parse validation, `DecodeError` `NonUtf8` / `IntParse` / `BoolParse` additions, `oids` module with catalog-pinned constants. 10 unit tests + `end_to_end_decode_typed_row` integration (full user pipeline push→schema→row→typed Rust values). |

Test count: 96 → 131 (+35 across all 1c-2 commits).

**Round-4 finding #5 closed** — text-format rejection ships as `ProtocolError::UnexpectedFormatCode { code }` in `parse_row_description`: format codes outside `{0, 1}` tear the connection down. 1c-3 will layer `FromPgBinary` on top of the same infrastructure (Extended Query selects binary per-column via Bind; decoder dispatch uses `ColumnDesc::format_code`).

### Architect-agent deep audit (2026-04-21)

Launched the `rust-senior-architect` agent for a systematic audit — 75 findings returned, each triple-checked by the agent. I then re-verified every "TAKE" recommendation with my own triple-check across three axes (real uplift? introduces fragility? conflicts with DEF-119 / future work?).

**Findings breakdown:**
- **35 "NO FINDING"** — agent verified the current code is optimal.
- **26 "DROP"** — marginal / risky / better handled in a bigger refactor.
- **6 "DEFER"** — reserved for DEF-119, DEF-053, 1c-6.
- **14 "TAKE"** — landed in 3 batches below.

**Agent recommendations I rejected on my triple-check:**
- **#33 StagedActions POD** — agent KEEP, I DROP. `heapless::Vec<StagedAction, N>` uses `MaybeUninit` internally, avoiding the 2.5KB stack init that a POD array would require. The `needs_drop = true` on `heapless::Vec<Copy, N>` is a nominal tier cost; LLVM elides the empty Drop body for Copy elements. Consistency-with-`OutActions` argument didn't outweigh the perf trade-off.
- **#2 payload_len usize → u16** — agent marginal TAKE, I DROP. Saves 6 bytes per variant in `ProtocolError`, but the size is budgeted (`ProtocolError ≤ 312`); the fallible-narrowing cost at every call site outweighs the marginal packing.

**Batch A — commit `c54b6b9` — visibility + overflow fidelity:**

| # | Change | Tier | LoC |
|---|--------|------|-----|
| #16 | `set_test_nonce` stays `pub` with explanatory doc (downgrade blocked by dead-code lint on the currently-unused test hook) | — | 4 |
| #17 | 8 SCRAM wire helpers `pub → pub(crate)` | tier-3 → tier-2 structural | 8 |
| #18 | `CappedServerNonce::try_from_bytes` stays `pub` (used by `tests/bounded_buffers_spec.rs`) | — | 3 |
| #19 | `ServerFirst` struct + `server_nonce`/`salt`/`iterations` fields → `pub(crate)` | tier-3 → tier-2 structural | 4 |
| #65 | `Encoding::from_bytes` over-length: silent `Other(empty)` drop → `from_truncated_bytes` with `"…"` marker | tier-4 → tier-2 structural | 35 |
| #73 | `OutActions.len` field comment updated ("currently 4" → "currently 8 post-1c-1b") | — | 2 |
| #75 | Removed stale `push_action` helper-removal note | — | 4 |

**Batch B — commit `f8e5e62` — `Option<Severity>`:**

| # | Change | Tier | LoC |
|---|--------|------|-----|
| #3 | `parse_error_response`: `severity: Severity::Unknown + severity_set: bool` pair → `severity: Option<Severity>` | tier-3 audit → tier-1 compile | 17 |

Desync between the bool flag and the enum value was a tier-3 audit seam. `Option<Severity>` makes the discriminator and the payload the same value — impossible to desync. Niche-packed (1 byte total, same as raw `Severity`) — no size cost.

**Batch C — commit `c46802d` — typed narrow ints:**

| # | Change | Tier | LoC |
|---|--------|------|-----|
| #1 | `UnsupportedAuthMethod.sub_code: u32 → AuthSubCodeClass { Unknown(u32) \| KnownButWrong(AuthSubCode) }` | tier-3 → tier-2 structural | 40 |
| #4 | `DecodeError::TruncatedColumnLen/NegativeColumnLength/TruncatedColumnData.column_idx: usize → u8` (MAX_ROW_COLUMNS=32 fits u8 with headroom) | tier-3 → tier-2 structural | 8 |
| #5 | `ColumnsIter.column_idx: usize → u8` (bundled with #4) | tier-3 → tier-2 | 4 |
| #66 | `SessionParams.application_name: BoundedStr<64> → BoundedStr<128>` for symmetric client↔server fidelity | tier-2 truncation-loss → tier-2 lossless | 3 |

`AuthSubCodeClass` preserves type information in the "server insisted on SASL on a Trust connection" diagnostic — downstream logging/wrapping renders the typed wire-method instead of widening back to an anonymous u32.

**Session total:** 14 architect-audit changes across 3 commits, 125 tests unchanged, clippy clean, forbid-bundle intact. Combined with the 86 individual tier-1 shields landed in prior 2026-04-21 work (Inbound/Outbound tags, TxStatus, FrameCoords typed construction, AuthSubCode enum, 48 const-asserted drift-pins), the crate now has **~100 compile-time shield points** covering wire-spec conformance, type-direction separation, size invariants, and exhaustive dispatch tables.

### Tier-1 uplift batch (2026-04-21)

User-driven aggressive tier-1 hunt. User clarified: *"никаких стеклянных архитектур"* meant **uplift must not introduce fragility**, NOT that uplift itself is bad — tier-1 IS the primary goal, zero-cost perf second, all-around safety third. Refactor decisions graded against: does the uplift ADD fragility? If no, do it.

Uplifts landed:

| uplift | tier change | mechanism | scope |
|---|---|---|---|
| Wire tag value drift-pin (26 tags + 4 auth codes + proto version) | tier-3 audit → **tier-1 compile** | `const _: () = { assert!(TAG_QUERY.byte() == b'Q'); … }` | 1 file, 30 const-asserts |
| OID catalog drift-pin (17 OIDs) | tier-3 audit → **tier-1 compile** | Same pattern inside `oids` module | 1 file, 17 const-asserts |
| MAX_SQL_LEN vs MAX_OWNED_SEND_LEN sizing | tier-3 audit → **tier-1 compile** | `const _: () = assert!(MAX_OWNED_SEND_LEN >= max_simple_query_message_size())` | 1 file, 1 const-assert; closed latent SimpleQuery WriteBuf overflow |
| `InboundTag` / `OutboundTag` newtypes (wire direction) | tier-3 audit → **tier-1 compile** | `#[repr(transparent)]` over `u8`; cross-direction assignment = build error | 5 files, ~200 LoC; 26 tag constants retyped |
| `TxStatus` enum (was raw u8 in Reply payloads) | tier-3 audit → **tier-1 compile** | `#[repr(u8)]` enum with `try_from_byte`; invalid bytes rejected at dispatch | 4 files, ~100 LoC |
| `UnexpectedFrame.tag: InboundTag` (was raw u8) | tier-3 audit → **tier-1 compile** | Typed wire-direction at the error level | 2 files, ~20 LoC |
| Ping-in-flight semantics (`UnexpectedFrame { tag: b'P' }` → `CommandInProgress`) | semantic correction | Drop the synthetic byte; use the proper push-path error kind | protocol.rs |

### 1c-2 tier-audit — explicit safety guarantees

User flagged: *"главное это ГАРАНТИИ безопасности и стабильности, а не тесты"*. Honest classification of every 1c-2 invariant by tier — what is compile-enforced, what is runtime-checked-and-structural, what is audit-enforced.

**Tier-1 compile (cannot be violated — the build fails):**

| Invariant | Mechanism |
|---|---|
| `MAX_ROW_COLUMNS = 32` bounds `RowDesc.columns` array | Fixed-size array type `[ColumnDesc; MAX_ROW_COLUMNS]` |
| `ColumnDesc` has exactly 2 fields — `type_oid: u32` + `format_code: FormatCode` | Struct definition |
| `FormatCode` is exactly `{Text=0, Binary=1}` | `#[repr(u8)]` enum with explicit discriminants |
| `RowDesc` is POD (Copy, no Drop) | `#[derive(Copy)]` + all fields Copy; `needs_drop::<RowDesc>() == false` asserted indirectly via `Reply` needs_drop contract |
| `Action::StreamRow.desc: &'r RowDesc` can never outlive `PgProtocol` | `'r` lifetime binds to `&'r mut self`; borrow checker rejects re-entry |
| `Action::StreamRow.row_bytes: &'r [u8]` can never outlive `PgProtocol.read_buf` | Same `'r` lifetime |
| `OutActions<'w, 'r>` cannot be held across `&mut PgProtocol` | Exclusive borrow prevents re-entry |
| `Reply::QueryComplete.row_desc: Option<RowDesc>` is owned (Copy) — safe to send across async boundary | No lifetime; owned by-value |
| `DataRowRef` fields are private; only constructor is `parse()` | Module privacy |
| `ColumnsIter` fields are private; only constructor is `DataRowRef::columns()` | Module privacy |
| PG OID constants match canonical `pg_type.dat` values (drift-pin) | `const _: () = assert!(BOOL == 16); ...` in `oids` module (tier-lifted from runtime test in this audit) |
| `FromPgText` trait return type enforces `Result<Self, DecodeError>` on every impl | Trait signature |
| `<&str>::from_pg_text` preserves borrow (zero-copy) | Lifetime parametrisation `FromPgText<'a> for &'a str` — compiler rejects any impl that copies |
| `FailReply + CloseSocket` vs `FailReply (recoverable)` never confused | Distinct `DispatchOutcome` variants (`Errored` vs `AdvancedWithAction`) |

**Tier-2 structural (runtime-checked; check-site is load-bearing architecturally):**

| Invariant | Mechanism |
|---|---|
| `RowDesc.n_columns ≤ MAX_ROW_COLUMNS` in all constructed values | `parse_row_description` rejects over-cap with `TooManyColumns`; no other constructor exposes `n_columns` |
| `row_desc cleared on new SimpleQuery push` — stale schema cannot leak into next query | Single clear site in `PgProtocol::push_command(SimpleQuery)`; regression test `dml_after_select_clears_row_desc` pins the site |
| `format_code ∈ {0, 1}` in every parsed `RowDesc` | Parser rejects otherwise with `UnexpectedFormatCode` (round-4 #5) |
| `ColumnsIter fuses after error` — no infinite loops, no post-error stale yields | Explicit `remaining = &[]; columns_left = 0` on error path |
| DEF-121 budget gate — no partial dispatch emission | Pre-dispatch `staged.len() + WORST_CASE_PER_DISPATCH > MAX` check before `mem::take(state)` |
| `Action::StreamRow.desc` points to live schema | When `StreamRowRange` is staged, `self.row_desc.is_some()` (enforced by T dispatch arm writing before StreamingRows state is entered); materialise's `unwrap_or(&EMPTY)` is structurally-dead fallback |

**Tier-3 audit (runtime-checked; correctness relies on code review):**

| Invariant | Mechanism | Why not uplift |
|---|---|---|
| `parse_row_description` correctly classifies every malformed input variant | Explicit match arms + slice patterns; unit tests pin the matrix | Parser operates on arbitrary server bytes; no type lifts runtime classification to compile |
| `DataRowRef::parse` + `ColumnsIter::next` correctly walk the wire layout | Same | Same |
| `FromPgText` impls correctly delegate to stdlib + map failures | Macro-generated impls with uniform shape | Runtime data |
| `parse_command_tag` correctly strips NUL and bounds-checks | Slice patterns + `BoundedStr` | Runtime data |

**Excluded from tier claims:**

- Stdlib behavior (integer overflow in `str::parse`, UTF-8 validation in `core::str::from_utf8`) — trusted library contract.
- Hardware-level invariants (bytes in RAM, etc).

**Test philosophy after 1c-2 audit:** one invariant → one test, with the test named after what breaks if the invariant fails. Tests for stdlib behavior or tier-1 compile guarantees are pruned (OID drift-pin moved from runtime test to `const _: () = assert!(...)` block). Tests for tier-3 audit invariants remain — they're the *only* shield.

Test count after audit: 131 → 124 (−7 merged/eliminated). Further reductions possible at 1c-6 hardening pass.

### DEF-121 — silent reply loss on `feed_bytes` loop overflow

Self-audit of 1c-1b found the hazard: `emit_actions!(..., on_overflow: break, ...)` bails out of the loop **after** the dispatcher has already consumed the `ReplyId` (via `deliver()` or `errored()`) and mutated `self.state`. The dropped action is silent — the caller's `oneshot::Sender` is orphaned forever, and the `ReplyId::Drop`-guard doesn't fire because the id was marked delivered.

**Before 1c-1b** this was dormant: Phase 1a (Ping, 1 action/call) and Phase 1b (Startup, ≤2 actions/call with the auth chain all `AdvancedSilent` except terminal Z) stayed under the `MAX_ACTIONS_PER_CALL=4` ceiling. 1c-1b row streaming made the hazard live — 5 `DataRow` frames + `CommandComplete` + `ReadyForQuery` in one chunk would overflow.

**Fix:** per-iteration budget gate in `feed_bytes`, checked BEFORE `core::mem::take(&mut self.state)`:

```rust
if staged.len().saturating_add(WORST_CASE_PER_DISPATCH) > MAX_ACTIONS_PER_CALL {
    break;  // frame stays in read buffer for the next call
}
```

`WORST_CASE_PER_DISPATCH = 2` is a named const so a future 3-action outcome causes a conspicuous bump, not a quiet overrun.

**Tier:** tier-4 (silent corruption) → tier-2 structural (the gate is a runtime check, but the check position — before any reply consumption or state mutation — is structurally load-bearing; the commit message makes this explicit).

Tier-1 would require the compiler to verify "every outcome has a slot" per iteration — possible via a dependent-types sketch but not on stable Rust. The gate is the honest ceiling.

### Round-4 discipline

Prior rounds (1/2/3) caught overclaims via stress-testing
tier-1 claims. Round 4 found a prior-round *underclaim* — the
"async ergonomics rejected" dismissal missed the witness-guard
pattern. Lesson: challenge not only "is the claim too strong"
but also "is the rejection too strong".

Future audits should probe `rejected` items with the same rigor
as `claimed` items. A rejection's rationale can itself be
wrong.

## 19. Architect-audit pass #5 findings (2026-04-21, post-Phase 1c-3a)

Fifth systematic architect pass (47 findings). Documented in full in
commit messages `802b411..6380f2a`. The landed items (F1, F6, F20,
F22, F30) are already described via their commit bodies. This section
records the remaining findings that were DEFERRED with their design
rationale, so a future implementer doesn't repeat the dead-end
analyses already performed.

### DEF-124 — F19: embed `RowDesc` in `StreamingRows` / `AwaitingRfq` variants (CLOSED 2026-04-21)

**Status: SHIPPED.** Implemented via option 2 (grow `Action::StreamRow`
to carry `RowDesc` by value). State variants now embed schema
directly — `SimpleQueryStreamingRows { reply, row_desc }` and
`SimpleQueryAwaitingRfq { reply, command_tag, row_desc }`. The
former `PgProtocol.row_desc: Option<RowDesc>` slot is removed. Tier
claim "StreamingRows implies schema" is now tier-1 compile via
struct-variant field requirement; "StreamRow action carries matching
schema" is tier-2 structural via `stream_row_or_errored` requiring
`RowDesc` arg from the pattern-matched state. Size budgets held
(`Action` stays ≤320 because DeliverReply was already bigger;
`PgProtocol` lost the slot). 135 tests pass, clippy clean.
See commit for full details.

(Original design analysis retained below for historical context.)

---

**Goal.** Eliminate the `PgProtocol.row_desc: Option<RowDesc>` slot;
move schema storage into the state variants themselves. Tier aim:
"StreamingRows state has a schema" becomes a structural tier-2
invariant (variant shape enforces it) rather than the current
tier-3 audit pairing (state + slot are two parallel representations
that could drift).

**Current design (tier-3 via paired facts).**
- `ProtoState::SimpleQueryStreamingRows(ReplyId<QueryKind>)` — reply only.
- `PgProtocol.row_desc: Option<RowDesc>` — separate slot.
- Dispatcher on `(AwaitingFirstResponse, 'T')` sets the slot to
  `Some(parsed_desc)` and transitions state to StreamingRows.
- `materialise` reads `self.row_desc.as_ref()` when producing
  `Action::StreamRow { desc: &'r RowDesc }`.
- Slot cleared on next `push_command(SimpleQuery)` — see protocol.rs:316.

**Drift risk.** A future dispatcher arm that enters StreamingRows
without populating the slot (or enters AwaitingRfq without a prior T)
produces `&RowDesc::EMPTY` silently instead of a classified error.

**Proposed target.**
```rust
enum ProtoState {
    /* ... */
    SimpleQueryStreamingRows {
        reply: ReplyId<QueryKind>,
        row_desc: RowDesc,                   // always present (tier-1)
    },
    SimpleQueryAwaitingRfq {
        reply: ReplyId<QueryKind>,
        command_tag: BoundedStr<32>,
        row_desc: Option<RowDesc>,           // Some for SELECT, None for DML/empty
    },
}
```

**Design challenge — lifetime of `Action::StreamRow.desc`.**

Currently `Action::StreamRow { desc: &'r RowDesc }` — a reference
(16 bytes). The `'r` lifetime ties to `ReadBuf`, but the actual
backing storage is `PgProtocol.row_desc` (the slot). Works today
because the slot outlives OutActions.

Post-F19 without the slot, materialisation runs at the END of
`feed_bytes` by which point `self.state` may be `Idle` (if the
terminal `Z` was processed) — so the schema is gone from state.

Three ways to resolve, each with a real trade:

1. **Per-frame materialisation** inside the dispatch loop (before
   state transitions further). Large refactor — changes the DEF-094
   staged-dispatch model. Rejected: too big a surface change for
   the tier win.

2. **Grow `Action::StreamRow` to carry `RowDesc` by value**
   (32 bytes → 292 bytes). `OutActions` size grows from ~2240 bytes
   to ~2336 bytes (+96 bytes on hot stack frame). Self-contained,
   no lifetime gymnastics. Tractable but a measurable perf cost.

3. **Keep the slot but tighten access** via a `row_desc_for_streaming()`
   method that matches state. Smaller tier win (tier-3 → tier-3 with
   gated access — no structural uplift; the slot-state desync hazard
   remains latent).

**Recommendation.** Option 2 (grow Action by-value). Rationale:
- Tier-3 → tier-2 structural is the primary goal; 96 bytes on a
  hot-path stack frame is acceptable given OutActions is already
  ~2KB and the state machine's happy path is few-frames-per-call.
- Avoids the DEF-094 refactor cost of option 1.
- Option 3 leaves the load-bearing invariant weak.

**Further scope.** Once embedded, `QueryCompletePayload.row_desc`
keeps `Option<RowDesc>` to preserve the current public API
distinction (`Some(empty_rowdesc)` = 0-row SELECT with schema;
`None` = DML / empty query). Collapsing these into "is_empty"
semantically loses the distinction — tests pin this.

**Touched files (estimated diff ~200 LoC).**
- `state.rs` — add `row_desc` to the two variants
- `dispatch.rs` — pattern changes; `advance_to_awaiting_rfq` grows
  an `Option<RowDesc>` arg; `row_desc_slot: &mut Option<RowDesc>`
  param removed from `dispatch()` signature
- `action.rs` — `StagedAction::StreamRowRange` carries `RowDesc`;
  `Action::StreamRow.desc: RowDesc` by value (not `&'r RowDesc`)
- `protocol.rs` — remove `self.row_desc` field + clear discipline;
  `materialise` gets `RowDesc` from staged action
- `state.rs::inflight_reply_raw_id` — update patterns
- `compute_push_*` rejection arms — update patterns
- Tests — `is_none()` checks still work (AwaitingRfq preserves Option)

**Why deferred from pass #5.** The lifetime analysis above
concluded option 2 needs to grow `Action::StreamRow`, which touches
the DEF-094 materialisation contract. Landing it mid-session
without deeper measurement of stack-frame impact (OutActions is
already the largest Copy-POD on the feed_bytes stack) risks a silent
perf regression the 1c test suite wouldn't catch. Ship F19 in a
dedicated commit series with a microbenchmark harness.

**Ship-order.** Can land before DEF-119 witness-guard (1c-5); the
two touch different axes (F19 = schema-state pairing; DEF-119 =
push-from-wrong-phase typestate). No ordering constraint.

### DEF-125 — F5: split `ProtoState` into `Active` / `Terminal` typestate (DEFERRED, paired with DEF-119)

**Problem.** `ProtoState::Errored(ErrorKind)` is a first-class
variant; `mem::take(&mut self.state)` in `push_command` replaces
it with `Idle` transiently, relying on the compute_push_* helpers'
explicit `Errored(prior_kind) => Errored(prior_kind)` arms to
preserve the terminal state. An `other @ (...)` catch-all that
omitted the Errored arm would not un-error the state (my triple-
check confirmed — architect's initial claim was overstated), BUT
would mis-classify the diagnostic as `CommandInProgress` /
`StartupAlreadyInProgress` instead of the correct
`ConnectionAlreadyClosed { prior_kind }`.

**Proposed.** Two-layer enum:
```rust
pub enum ProtoState {
    Active(ActiveState),  // current variants minus Errored
    Terminal(ErrorKind),
}
```
`mem::take` on `ProtoState::Terminal` would be a type error (no
`Default` for `Terminal`). `compute_push_*` helpers take
`ActiveState` by value and produce `ProtoState`; the Terminal branch
is handled once at the `push_command` entry point.

**Why deferred.** F5's tier claim is "silent un-error" — overstated
after triple-check. The real invariant (correct diagnostic
classification) is already tier-3 via the per-helper explicit arm
plus regression tests. Structural elevation to tier-2 requires the
full two-layer refactor, which overlaps heavily with DEF-119
witness-guard territory (both restructure `PgProtocol`'s state
API). Land together in 1c-5.

**Touched files.** state.rs (enum shape), protocol.rs
(push_command entry), all four compute_push_* (take ActiveState),
plus dispatch.rs top-level match, plus ProtoState Debug impl, plus
tests referring to `ProtoState::Errored(...)` as a direct match.

### DEF-126 — F2: replace UTF-8 ellipsis marker `"…"` with ASCII `~` (REJECTED 2026-04-21)

**Status: NOT DOING.** Investigated frequency + convention data, the
proposal turns out to be net-negative. Recording the analysis so
this doesn't resurface in future audits.

**Frequency data.** Truncation paths in the crate:

| Site | Cap | Typical len | Truncation rate |
|---|---|---|---|
| `CommandComplete` command_tag | 32 | 8-15 | Never (PG's longest doc'd tag ~23) |
| `Sql` user text | 2048 | 50-500 | Rare (only pathological ORM-generated) |
| `ErrorResponse.message` (M) | 64 | 30-80 | Moderate — "duplicate key value violates unique constraint \"users_email_key\"" = 66 bytes |
| `ErrorResponse.detail` (D) | 64 | 50-150 | Frequent — detail text is normally long |
| `ErrorResponse.hint` (H) | 64 | 20-60 | Rare |
| `OtherEncoding` | 96 | 5-10 | Never (encoding names short) |

Truncation is essentially error-path only. Quantitatively: pool
doing 100K query/sec with 0.1% errors and half of those producing
long detail text → 50 truncations/sec × 2 bytes saved = 100 bytes/sec.
Trivial vs the MB/sec wire traffic.

**Convention data.** Truncation markers across the ecosystem:

| System | Marker | Bytes |
|---|---|---|
| Python `textwrap.shorten` | `" [...]"` | 5 (ASCII) |
| Rust stdlib | no marker — caller decides | — |
| PostgreSQL internal logs | `...` | 3 (ASCII) |
| Unix logrotate / nginx access log | `...` | 3 (ASCII) |
| Chrome DevTools string preview | `…` | 3 (UTF-8) |
| VS Code peek-definition | `…` | 3 (UTF-8) |
| Typical CLI/TUI tools | `…` / `...` | 3 |

**`~` is NOT a recognised truncation-marker convention anywhere.**
It's semantically loaded with:
- Unix home directory (`~/path`)
- Bitwise NOT operator
- "Approximately" (`~100 rows`)
- Regex negation in some dialects

A user reading `"error: column \"foo\" does not exist~"` would not
instantly parse it as "truncated" — they'd wonder what the tilde
means. The diagnostic-confusion cost is real and cross-user.

**Byte savings are ~100 bytes/sec on a million-QPS pool — noise.**
F1's `N >= 3` bound isn't a real constraint anywhere in the crate
(no `BoundedStr<2>` exists or is planned); it's defensive. Relaxing
it to `N >= 1` is a theoretical tidy-up, not a practical win.

**Alternative considered:** `"…"` (3 UTF-8 bytes) → `"..."` (3 ASCII
bytes). Same length, convention universal, no encoding dependency
on the marker const. But the crate is fully UTF-8-aware already —
no portability concern for the marker — so even this swap has no
upside.

**Decision.** Keep `"…"`. Convention-standard (Chrome / VS Code /
modern UIs match), visually distinguishable in logs, no realistic
byte-pressure on error paths. Context comment in `ident.rs` near
`OVERFLOW_MARKER` records this so future audits don't re-litigate.

### DEF-127 — F9: collapse `Option<RowDesc>` to `RowDesc` with `is_empty()` (DEFERRED, subsumed by DEF-124)

**Status.** Superseded by DEF-124 decision to KEEP `Option<RowDesc>`
in `QueryCompletePayload` (preserves the "0-row SELECT vs DML"
distinction). The 8-byte `Option` tag cost is the price of the
distinction — record and close.

### DEF-128 — F32: `FormatCode::try_from_i16` centralised classifier (P2)

**Goal.** Replace the ad-hoc match on raw `i16` format codes
throughout decode with a `FormatCode::try_from_i16(n: i16) -> Result<Self, UnexpectedFormatCode>` helper. Currently only one call site
matches on the raw i16; future `Describe` + `BindExecute` paths
(1c-3b+) would add more sites.

**Why deferred.** Single call site today — premature DRY.
Implement when the 2nd-and-3rd sites show up in 1c-3b/c.

### DEF-129 — F33: `SYNC_WIRE_BYTES` visibility `pub` → `pub(crate)` (P3)

**Goal.** `wire::SYNC_WIRE_BYTES: [u8; 5]` is `pub` but has no
documented user-facing use-case. Protocol owns Sync frame
semantics; the const should be `pub(crate)`.

**Why deferred.** Pure visibility tightening; no behavioural
change. Trivial commit in a future hygiene pass.

### DEF-130 — F35: `record_param_status` silent-drop classification (P2)

**Goal.** `record_param_status` silently ignores malformed
ParameterStatus payloads (no NUL separator, over-length key/value,
etc.). The silent-drop is tier-3 because PG MUST NOT send malformed
PS — if it does, it's a server/proxy bug worth surfacing.

**Proposed.** Return a classified status:
```rust
enum ParamStatusRecordOutcome {
    Recorded,
    IgnoredUnknownKey,
    MalformedPayload { reason: ParamStatusMalformed },
}
```
Caller logs via an optional `Action::EmitPsAdvisory(...)` action
in Phase 1d+ — currently the wrapper has no channel for this.

**Why deferred.** Phase 1d introduces wrapper-visible advisories.
Today's wrapper silently drops same as the function does — no end-
to-end tier win in 1c scope.

### DEF-131 — F46: `FixedStr::PartialEq` compares full N-byte buffer (P2 perf)

**Goal.** `#[derive(PartialEq)]` on `FixedStr<N, Tag>` compares the
full `[u8; N]` buffer for equality, including the zeroed tail.
Wasted compare bytes: `N - self.len()`. For `Sql<2048>` with a
64-byte typical query, that's ~1984 wasted bytes per compare.

**Proposed.** Hand-written `PartialEq` that compares only
`[..self.len()]`:
```rust
impl<const N: usize, Tag> PartialEq for FixedStr<N, Tag> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.as_bytes() == other.as_bytes()
    }
}
```

**Why deferred.** Uncertain whether `FixedStr::PartialEq` is actually
called on hot paths — Sql compares are rare. Need `cargo asm` or
flamegraph data before optimising.

**Caveat.** Replacing `#[derive(PartialEq)]` with a manual impl
means we lose the `Hash` derive coherence (if we ever `#[derive(Hash)]`).
Document the trade when implementing.

### DEF-132 — F17+F38+F41: measurement pass on perf candidates (OPEN)

**Goal.** Both items are potentially valuable optimisations but
cannot be acted on without measurement data — committing blind
risks either no-op churn OR a regression LLVM had already solved.

**F17 — `ProtoState` (1248 bytes) passed by value into `compute_push_*`.**
LLVM typically applies NRVO (named-return-value-optimisation) to
elide the copy at in/out boundaries. If NRVO fires, by-value is
free. If it doesn't (e.g., the function's control flow prevents
NRVO), we're paying 2496 bytes of stack-frame memcpy per
`push_command` call.

**F41 — `fail_inflight_and_close` is currently `#[cold]` only.**
Adding `#[inline(never)]` would force an out-of-line call in every
case, potentially improving icache density on the hot `feed_bytes`
path (the cold body wouldn't pollute the hot-path prologue). OR it
might hurt if LLVM was inlining it in cases where the overhead of
the call was worse than the inlining.

**F38 — `memchr` crate for SIMD-accelerated NUL scan (3 sites).**

Three call sites currently use scalar byte scan:
- `ident.rs:679` — `try_from_str` NUL validation (per `push_command`,
  input 0-128 bytes).
- `protocol.rs:1227` — `record_param_status` NUL separator scan
  (per PS frame, input 10-100 bytes).
- `decode.rs:254` — per-column name NUL scan in RowDescription
  (per column, up to 32/row; input 5-30 bytes).

At 100K-QPS pool with ~5 cols/row avg, column-name scan fires
~500K times/sec — the highest-frequency site. Total savings if
memchr is ~2-3x faster than LLVM-optimised `contains(&0)` on
aarch64 for these small inputs: ~50ms/sec CPU, 0.5% of a saturated
core.

**BUT** — memchr's sweet spot is 1KB+ inputs; for 30-byte slices
its SIMD setup overhead may equal or exceed the win. LLVM 15+
autovectorises `iter().any(|b| *b == 0)` on aarch64 in many cases,
making `contains(&0)` already reasonably tight. Without measurement
we don't know which side wins. Plus dep cost (+1 crate, ~25 KB binary).

**Why deferred: need data.**

Measurement checklist for whoever picks this up:
1. **F17** — Run `cargo asm bsql-pg-proto::protocol::PgProtocol::push_command`
   — verify whether `ProtoState` copy in/out shows up in the generated
   assembly (look for large `memcpy` calls or repeated `mov`).
2. **F41** — Run `cargo asm bsql-pg-proto::protocol::PgProtocol::feed_bytes`
   — measure size of the hot-path function body in bytes; note
   whether `fail_inflight_and_close` is inlined.
3. **F38** — Run `cargo asm` on each of the three scan sites
   (`ident::FixedStr::try_from_str`, `protocol::record_param_status`,
   `decode::parse_row_description`) — check whether LLVM emitted
   SIMD instructions (`ld1`, `cmeq`, `umaxv` on aarch64 NEON) or fell
   back to a scalar loop. If scalar, memchr dep pays off.
4. Microbenchmark via `criterion` (or equivalent):
   - `push_command(Ping)` loop, 10M iterations → baseline for F17+F41
   - `parse_row_description` on typical / wide / edge-case schemas → F38 data
   Record ns/call.
5. Apply each candidate change independently, re-measure, record delta.
5. Commit only changes that show measurable improvement (or neutral
   + tier gain, e.g., `#[inline(never)]` that increases clarity).

**Dependency.** Measurement harness is itself a small project — likely
lives in `benches/push_command.rs` (new file, criterion dep gated
behind `[dev-dependencies]`). Criterion is already indirectly pulled
in by some crates; verify or add.

**Why not done now.** This session focused on tier uplifts where the
win/cost ratio was already clear. Perf optimizations without data
are speculation — the crate philosophy "каждая наносекунда на счету"
explicitly requires measurement, not guess.

### DEF-133 — finalise `scram::types` public API before v1.0 (OPEN)

**Status.** The 2026-04-21 visibility audit killed the concrete
"pub for tests" hacks (SYNC_WIRE_BYTES, base64_encode_to_buf,
CappedServerNonce::try_from_bytes, set_test_nonce — all now
`pub(crate)` with their tests moved / rewritten). Remaining
formally-public items in `scram::types` are:

- `pub struct CappedServerNonce` (constructor is `pub(crate)` —
  manufacture blocked, but type name is visible)
- `pub struct ServerNonceTooLong` (error struct — not currently
  surfaced through any `ScramError` variant)
- `pub const MAX_SERVER_NONCE_LEN: usize = 256;`
- `pub mod scram` / `pub mod types` (module visibility)

**Cost-benefit.** Users CANNOT actively use these — no public API
path produces a `CappedServerNonce`, no `ScramError` variant carries
`ServerNonceTooLong`. So the `pub` annotations cost:

- API versioning lock-in at v1.0 (every pub item freezes into the
  SemVer contract)
- docs.rs clutter — users see internal SCRAM plumbing with no clear
  "why this is here" context
- Lost freedom to restructure the SCRAM module in response to the
  Phase 1e wrapper crate's actual needs

Benefits of keeping them `pub` (today): zero. No user can
meaningfully depend on them.

**Why deferred, not fixed now.** Phase 1e (`bsql-driver-postgres`
wrapper) hasn't landed yet. When it does, the wrapper will need
SOME SCRAM types exposed (e.g., for structured logging of
handshake state, or for custom error variants carrying the typed
SCRAM failure). Deciding "what's public" before the wrapper
exists is speculation; after it ships, the answer becomes
mechanical.

**Pre-v1.0 checklist for whoever takes this up:**
1. Enumerate every `pub` item in `scram/`, `error::ScramError` /
   `ServerErrorResponse`, etc.
2. For each: is it transitively reachable from the wrapper crate's
   published API? If yes, keep `pub`. If no, `pub(crate)`.
3. Apply the same question to `wire::*` public items and all
   crate-root re-exports.
4. Commit as "final API surface freeze for v1.0" — one atomic
   pre-release audit.

**Non-goal.** Ship as pub-minimal version right now. Shrinking and
then re-expanding as Phase 1e needs emerge is churn; freeze once
at v1.0 cutover.

---

## 20. Architect-audit pass #6 findings (2026-04-21, post-1c-3b uplifts)

Sixth systematic audit pass, triggered by user pushback: "точно ли
нельзя tier поднять нигде? ... по тестам пройдись, я не уверен что
тут прям 150+ тестов должно быть". Agent returned 90+ findings
(F54..F84, BS1-BS9, CR1-CR6, MI1-MI10, test-audit). Triple-checked
each; documented in full in commit messages `5ad746b..b372399`.

**Landed pass #6:**

| ID | What | Commit |
|---|---|---|
| F54 | `hmac_sha256` Result (fail-closed, ScramError::HmacKeyRejected) | `5ad746b` |
| BS8 | `MAX_SCRAM_ITERATIONS = 10M` + `IterationsTooHigh` | `5ad746b` |
| F60 | Const-assert for `Option<T> ParamEncoder` OID dispatch | `5ad746b` |
| MI5/6/7/10 | `#[non_exhaustive]` on `Credentials`/`ScramError`/`Severity`/`AuthSubCode` | `5ad746b` |
| F83 | `FetchRows` enum (was `max_rows: u32`) — tier-3 docs → tier-1 compile | `bdd210e` |
| F55 | `SessionParams.set` non-UTF-8 → `from_bytes_lossy` | `b372399` |
| F61 | `with_length_prefix` / `with_i32_length_prefixed_body` explicit Err | `b372399` |
| F66 | `feed_bytes` early-return on Errored | `b372399` |

Plus: `FetchRows::All.as_wire_i32() == 0` drift-pin const-assert.

### DEF-134 — F78: cargo-fuzz harness (OPEN, own session)

**Target.** Differential fuzz against `feed_bytes` / `parse_header` /
`parse_row_description` / `parse_server_first`. 4 fuzz targets,
~150 LoC + cargo-fuzz setup. The forbid-bundle rules out panic-able
expressions at compile-time, but:
- Arithmetic edge cases from pathological frame-length sequences
- State-machine bugs from adversarial frame ordering (malicious
  proxies, protocol-desync attacks)
- Integer-overflow edge cases on NL-terminated string parsing

... are all observable only via fuzzing.

**Why deferred.** Own session. Setup + 4 targets + corpus + CI wiring
is ~half-day of focused work. No immediate critical gap (all
surfaces are tier-1/2 by current shields), but "unhandled panic-able
input = one CVE waiting" per audit rationale.

**Dependencies.** `cargo-fuzz` in `[dev-dependencies]`, separate
`fuzz/` directory with `Cargo.toml`. CI fuzz-nightly job.

### DEF-135 — F62: precomputed `ALL_BINARY_WIRE` const for Bind format-code emission (OPEN, blocked on DEF-132)

Current `build_bind_message` iterates `0..P::COUNT` and pushes
`u16_be(1)` per param. LLVM likely unrolls for N ≤ 16. F62 proposes
a precomputed `&'static [u8; 32]` (16 × `[0, 1]` pairs) + slice-take
to replace the loop with one `push_bytes` memcpy.

**Why deferred.** Perf-only; win is unclear without measurement.
Blocked on DEF-132 (measurement pass) — if LLVM already unrolls,
the precomputed const adds binary size without win.

### DEF-136 — F64: typed `OutboundSlice<'w>` wrapper for `Action::SendBytes` (DEFERRED to v1.0 freeze)

Current `Action::SendBytes(&'w [u8])` can carry any byte source. In
practice every emission routes through `StagedAction::SendBytesRange`
(outbound WriteBuf region) or `SendBytesStatic(&SYNC_WIRE_BYTES)` —
convention ensures only outbound bytes. A typed wrapper would make
it structural:

```rust
pub struct OutboundSlice<'w>(&'w [u8]);
Action::SendBytes(OutboundSlice<'w>)
```

**Why deferred.** Cost: every user-code pattern-match sprouts a
`.as_bytes()` call. ~40 test sites affected. Benefit: tier-2
structural shield on "SendBytes carries outbound-only bytes."
Worth at v1.0 freeze when we consolidate the public API.

### DEF-137 — F65: typed `StaticWirePayload` enum for `StagedAction::SendBytesStatic` (DEFERRED to 1c-5)

Current `SendBytesStatic(&'static [u8])` accepts any static bytes.
Only `SYNC_WIRE_BYTES` is used today. Narrow to enum:
```rust
enum StaticWirePayload { Sync }  // future: Flush, Terminate
```

**Why deferred.** Adding 1c-5 (`Flush` wire frame for pipelining)
will double the use-cases — natural trigger for the enum.

### DEF-138 — F72: rename `inflight_reply_raw_id` → `take_inflight_reply_raw_id` ✅ CLOSED (2026-04-21, pass-#8 polish batch)

Method name didn't reflect the `.consume()` side-effect. Renamed to
`take_*` per Rust-stdlib convention (`Option::take`, `Vec::drain`,
`core::mem::take`). Every call site reads the consumption intent
at a glance. Docstring expanded with the naming-convention rationale.

### DEF-139 — F81: `debug_assert!` in `materialise` on `range.apply` None ✅ CLOSED (2026-04-21, pass-#8 F-007)

Closed transitively by pass-#8 F-007: `debug_assert!` was added
INSIDE `NonEmptyRange::apply` itself (action.rs:183), which covers
both `materialise` call sites (`SendBytesRange` + `StreamRowRange`
resolution) in one shield. Release builds LLVM-elide; debug builds
fire on `buf shorter than emission-time bounds` wiring regression.

### DEF-140 — F82: `FromPgText` doctest freshness ✅ CLOSED (2026-04-21, pass-#8 polish batch)

Doctest rewritten to model the crate's own discipline: no
`.unwrap()` in the happy path, `let-else` + `?` for error
propagation, explicit `Option`/`Result` handling at each yield
boundary. `///` example is still `ignore`'d (no fixture mock for
`Action`) but now parses as idiomatic bsql code a reader could
copy-paste without picking up bad habits.

### Test count analysis — user's intuition rechecked

User's ask: "155+ тестов должно быть и что каждый из них необходим".
Agent's initial claim: 11 redundant. After my own re-analysis:

**Actually redundant (drop candidates):** 2 tests.
- `params::arity_sixteen_supported` — const-assert at module level
  already pins COUNT; runtime test adds nothing.
- `params::arity_three_oids_and_formats_coherent` — const-assert
  on OIDS pattern-matches `[INT4, TEXT, BOOL]`; runtime test overlaps.

**Not redundant (agent was wrong):** 9 tests.
- `frame_parse::trailing_bytes_do_not_affect_header_parse` — slice
  pattern is tier-1 but the TEST pins that parse_header returns
  `HeaderParse::Ok`, not some other variant. Tier-2 arm-body pin.
- `bounded_buffers::advance_zero_is_noop` — checks observable API
  (Ok(()) + state unchanged). Not provable structurally.
- `bounded_buffers::write_buf_push_u32_be_is_big_endian` +
  `_push_i32_be_is_big_endian` — pin wire-format BE convention at
  the crate's push methods. LLVM can't prove "our method emits BE";
  tests do.
- Other agent claims misidentify tier-2 pins as "trivial".

**Decision:** NOT worth dropping 2 tests for ~0 value. Keep all 156.

---

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

---

## 21. Architect-audit pass #7 findings (2026-04-21, post-1c-3c Describe)

Seventh systematic audit pass. 16 findings; 9 P1 tier/perf uplifts
landed in the 1c-3c audit-follow-up commit; 4 P2 "already optimal
/ defensible" findings documented here; 1 deferred for a future
infallible-writer refactor.

**Landed pass #7 (audit-follow-up to 1c-3c):**

| ID | What | Rationale |
|---|---|---|
| F1+F2 | `advance_to_drain_after_error` de-generic + `#[cold] #[inline]` | K-oblivious body; pre-consume at call sites (mirrors `errored()` pattern). Eliminates N monomorphisations; I-cache win. |
| F3 | `#[inline]` on `build_describe_message` | Tiny body, 2 call sites — cheap cross-crate inline. |
| F4 | `#[repr(C, align(4))]` on `ParamOids` + size/align/SIMD-wide const-asserts | Layout pin: drift in MAX_PARAMS_ARITY or field order fails the build. |
| F7 | `split_first_chunk::<4>()` in `parse_parameter_description` | Typed fixed-array ref instead of slice-pattern match with dead `_ =>` arm. Idiomatic. |
| F8 | `DescribedRows::from_row_desc` / `no_data` factory constructors | Intent-named construction at the 4 dispatch sites; swaps are less likely and named at code review. |
| F12 | Sealed `DescribeName` trait on `StmtName`/`PortalName` | Builder rejects raw `&[u8]` at compile. Tier-3 audit ("caller passes right bytes") → tier-1 compile ("builder accepts only these two types, sealed"). |
| F13 | `parse_rfq_payload` centralised classifier | 8 parallel `[tx_byte] / other` match bodies collapsed to one function call. Tier-3 audit → tier-2 structural. |
| F14 | `consume_state` test helpers delegate to `inflight_reply_raw_id` | Two 20-line parallel matches → 3-line calls. Adding a new variant classifies once in state.rs. |
| F15 | `max_describe_message_size()` decomposition drift-pin | Ties computed total to documented `'D' + len + target + name + NUL` sum. |
| F16 (short) | `frame_build_unreachable` centralised cold helper | 6 call sites × 10-line dead-branch body → 6 × 1-line calls routing to one `#[cold]` helper. Perf + code-size. |

### F5 / F9 / F11 — already optimal (no change, documented reasoning)

- **F5 `ParamOids::PartialEq` full-array eq** — 64-byte array compare is a single AVX2 register; populated-prefix would branch on length. Pinned via SIMD-wide size const-assert (documents the invariant).
- **F9 `param_oids` always present in `DescribeStatementComplete`** — not `Option<ParamOids>`; semantic intent is "describe-statement IS a parameter description, which may be empty". Option disambiguation would reintroduce the class DescribedRows eliminated.
- **F11 per-kind Describe state variants** — merging `DescribePortalAwaitingRowDescOrNoData` with `DescribeStatementAwaitingRowDescOrNoData` would require erasing the kind parameter on `ReplyId<K>`. DEF-112 structural pairing blocks the merge.

### F6 — rejected under forbid bundle (analysis captured inline)

Architect's "centralise `compute_push_*` catch-alls via
`is_busy_in_flight` + guard" proposal requires a `_ =>` fallback
arm because match guards don't prove exhaustiveness. Every
fallback option (`unreachable!()`, `panic!()`,
`unreachable_unchecked()` unsafe) collides with the forbid bundle.
Current design — explicit or-patterns at 5 call sites + exhaustive
match in `allows_unsolicited_param_status` — is tier-1 at every
site and forces classification on new variants. F6 would trade
tier-1 compile for tier-2 match guards + runtime fallback. Analysis
recorded at `protocol.rs:1786` (pre-`allows_unsolicited_param_status`).

### F10 — `DescribedRows::Rows` size: DEF-119 fix, no 1c-3c-scoped uplift

Rust cannot shrink an inline `RowDesc` (~260 B) without either
`Box` (needs alloc — banned) or an arena ref. The arena ref is
DEF-119's scope (1c-5 pipelining). Note in `action.rs:851-852`
documents the plan.

### DEF-141 — F16 long-term: infallible builder returns

**Goal.** Eliminate the `Result<NonEmptyRange, WriteBufFull>` return
across all `build_*_message` functions. Replace with an infallible
`NonEmptyRange` signature gated by a "capacity-proven writer
witness" type — the const-asserts in `write_buf.rs` already prove
no overflow; the type system should SURFACE that proof.

**Why deferred.** Requires a broader refactor of `WriteBuf`'s
push API. Current pass-#7 closed the cold-hint gap (F16 short-term)
by centralising `frame_build_unreachable`. The remaining step —
deleting the dead Err branch entirely — is the Tier-1 finish.

**Tier lift estimate.** Tier-2 runtime dead branch (architecturally
dead via const assert, syntactically present for match exhaustion)
→ tier-1 compile (type signature says Infallible).

**Touched files.** `write_buf.rs` (new witness trait or const-generic
capacity param on `WriteBuf`), `protocol.rs` (all 5 `build_*`
signatures), `error.rs` (keep `OutboundFrameBuildUnreachable` for
legacy or delete entirely).

**Ship-order.** Schedule for 1c-6 hardening sub-phase. Does not
block 1c-3d (Close) or 1c-4 (transactions).

**Test count after pass-#7.** 182 → 182. No new tests needed —
audit-follow-up refactors preserve semantics; existing 26 describe
tests + 156 pre-existing all pass.

---

## 22. Rust-unstable watchlist

Features we're currently working around because they aren't yet on
stable. Each entry cites the rust-lang tracking issue and the sites
in our codebase that would simplify the moment the feature lands on
stable. Revisit this section on every MSRV bump.

**Why a dedicated section.** Individual "when X stabilises…" notes
are scattered across source comments and commit messages — a
refactor done two years from now would have to grep blindly. Here
we centralise the dependency on the Rust stabilisation pipeline so
a single pass at each MSRV bump can sweep every site.

### RU-01 — `<[T]>::get` / `From<u16> for usize` / `u32::try_from` in const

**Tracking:** [rust-lang/rust#143874](https://github.com/rust-lang/rust/issues/143874)
(const-traits / const-impls of standard conversions)

**Status (MSRV 1.95):** not yet stable. `usize::from(u16_value)`,
`u32::try_from(usize_value)`, `<[T]>::get(i)` cannot appear inside
`const fn` bodies or `const _: () = { … }` blocks.

**Worked-around sites:**

| Site | Today's form | Flip-to |
|---|---|---|
| `src/decode.rs:140` `RowDesc::len` | `pub fn len(&self) -> usize` | `pub const fn len(&self) -> usize` |
| `src/frame.rs:50` `MAX_FRAME_LEN_FIELD` | Hard-coded literal `4095` | Derive via `const { u32::try_from(READ_BUF_CAP - 1).unwrap() }` |
| `src/wire.rs:519` `assert_all_distinct!` macro | Recursive macro expansion | Fold into `const fn walk(arr: &[u8]) -> bool` using `arr.get(i)` |
| `src/decode.rs` other `usize::from(u16)` sites | Non-const `fn` | Promote to `const fn` once stable |

**Action on stabilisation.** One-line keyword flips where possible;
the `assert_all_distinct!` macro collapse is medium-effort (rewrite
macro body as a const walker, retain call sites unchanged).

**Note (pass-#8 F-034).** Pre-verified that the flip is source-
compatible: MSRV bump alone enables the change.

### RU-02 — `<[T]>::split_once` with predicate

**Tracking:** [rust-lang/rust#112811](https://github.com/rust-lang/rust/issues/112811)
(slice `split_once_*` family)

**Status (MSRV 1.95):** not yet stable. Must use the `iter().position(pred)`
+ manual `split_at` idiom.

**Worked-around site:**

| Site | Today's form | Flip-to |
|---|---|---|
| `src/protocol.rs:1882` `record_param_status` | `payload.iter().position(\|b\| *b == 0)` + manual split | `payload.split_once(\|b\| *b == 0)` (single `.split_once` call) |

**Action on stabilisation.** Five-line tightening; net reduction
in `let Some(...) else` cascade.

### RU-03 — `generic_const_exprs` for capacity-witness patterns

**Tracking:** [rust-lang/rust#76560](https://github.com/rust-lang/rust/issues/76560)
(generic const expressions in type positions)

**Status (MSRV 1.95):** not yet stable. Prevents the proper
capacity-proven writer-witness refactor planned for **DEF-141**
(`build_*_message` infallible returns).

**Blocked work:**

- **DEF-141** — eliminate architecturally-dead `Err(WriteBufFull)`
  branches from every `build_*_message` via a type-level capacity
  witness. Current short-term hint: `frame_build_unreachable`
  centralised cold helper.
- **F-006 (pass-#8)** — same class: `OutActions::push_infallible<const IDX>`
  with compile-asserted `IDX < MAX_ACTIONS_PER_CALL`. Blocked on
  `generic_const_exprs` for the const-generic index bound check.

**Action on stabilisation.** Full-week refactor to thread witness
types through `WriteBuf` / `build_*_message` signatures; eliminates
~50 LoC of dead Err branches.

### RU-04 — `std::simd` portable SIMD

**Tracking:** [rust-lang/rust#86656](https://github.com/rust-lang/rust/issues/86656)
(portable SIMD abstractions)

**Status (MSRV 1.95):** `core::simd::u8x32` and friends remain
unstable.

**Blocked work:**

- **DEF-108** — `u8x32` XOR for ClientKey ⊕ ClientSignature in
  SCRAM client-proof. Current form (zip-iterator) auto-vectorises
  on x86-64-v2+ / aarch64 via LLVM, so the perf gap is near-zero
  today; portable SIMD would tighten the guarantee to ALL targets.

**Action on stabilisation.** Swap the zip-iterator form in
`scram/crypto.rs` for a `u8x32::from_slice(a) ^ u8x32::from_slice(b)`
one-liner. Preserves semantics; tightens perf on non-auto-vec
targets.

### RU-05 — `core::hint::unreachable_unchecked` as safe

**Tracking:** no tracking issue (architectural — the hint IS unsafe
by design today).

**Status (MSRV 1.95):** `unreachable_unchecked` exists but requires
`unsafe`. Since the crate is `#![forbid(unsafe_code)]`, we cannot
use it even for genuinely-impossible match arms.

**Worked-around sites:**

| Site | Today's form | Ideal-world form |
|---|---|---|
| Various `_ => {}` dead catch-alls | Explicit exhaustive or-patterns | Terminal `_ => core::hint::unreachable_unchecked()` |

**Action.** Unlikely to ever stabilise as safe — fundamentally unsafe
by spec. Watch for alternative "proved-unreachable via typestate"
language features (e.g., `never_type` stabilisation on stable).
Current form is optimal under the forbid bundle.

### Review cadence

Audit this section at each MSRV bump. When an entry's feature
stabilises, the entry turns into a work item (add to sub-phase
task list, implement, delete from here).

---

## 23. Closed DEFs from pass-#8

### DEF-138 — rename `inflight_reply_raw_id` → `take_inflight_reply_raw_id` ✅ CLOSED (2026-04-21)

See §21 entry (pass-#7 deferred record) — closed in pass-#8 polish
commit. Method receiver is `self` by value (consuming); `take_`
prefix mirrors `Option::take` / `Vec::drain` stdlib convention.

### DEF-139 — `debug_assert!` in `materialise` on `range.apply` None ✅ CLOSED (2026-04-21)

Closed transitively by pass-#8 F-007: `debug_assert!(slice.is_some())`
added INSIDE `NonEmptyRange::apply` (action.rs:183), covering both
`materialise` call sites through one shield.

### DEF-140 — `FromPgText` doctest freshness ✅ CLOSED (2026-04-21)

Doctest rewritten to model crate discipline — no `.unwrap()` in
happy path, `let-else` + `?` for error propagation.

### DEF-142 — ErrorKind split via StateErrorKind newtype ✅ CLOSED (2026-04-21)

**Origin.** Pass-#8 F-056 — `ErrorKind::AlreadyClosed` is
documented as a "pseudo-kind" that never reaches state, but the
invariant was tier-3 audit. A future refactor that accidentally
routed `AlreadyClosed` into state would produce nonsensical
`ConnectionAlreadyClosed { prior_kind: AlreadyClosed }` replies.

**Fix.** New `StateErrorKind(ErrorKind)` newtype with
`#[repr(transparent)]` — same 1-byte footprint, same niche-packed
`Option<_>`, but the constructor `try_from_kind` rejects
`AlreadyClosed`. `ProtoState::Errored(StateErrorKind)` and
`ProtocolError::ConnectionAlreadyClosed { prior_kind: StateErrorKind }`
both narrowed to the AlreadyClosed-free subset. Tier-3 audit →
tier-1 compile.

**Convenience.** `StateErrorKind::from_kind_or_internal(k)` is the
infallible fallback (maps `AlreadyClosed` to `Internal`) for test
fixtures. Production `fail_inflight_and_close` uses
`try_from_kind(k).unwrap_or(INTERNAL_FALLBACK)` at the one
architecturally-dead construction site.

**Touched files.** error.rs (+ newtype + 2 const-asserts),
state.rs (type change), protocol.rs (2 call sites + test helpers),
3 test files (pattern rewrites: nested `prior_kind:` patterns
unpacked to outer `match` + inner `as_kind()` compare because
guard-patterns-inside-patterns remain unstable). lib.rs re-export.

**Test count.** 188 → 188 (semantics preserved, no new tests
needed — existing tests exercise the new types).

## 24. Post-DEF-119 comprehensive audit (2026-04-22)

### Audit context

Architect session post-DEF-119 landing (commit `f356c88`). Raw
output lives at `audit.txt` (137 findings, gitignored); reviewer
verdicts at `audit_accepted.txt` (gitignored). This section
consolidates the actionable items into DEF tickets plus a
load-bearing architectural decision (H021).

Scope covered by the audit:
- Full `src/*.rs` (~12 KLoC), `src/scram/*.rs` (1300 LoC), all
  integration tests.
- Specifically re-audited DEF-119 schema arena and its consumers.
- Cross-referenced against the pre-existing DEF-001..DEF-142 set.

Review methodology: every finding spot-verified against source;
every rejection asked the forcing question "did our own docs /
naming / arch mislead the auditor?" → fixes tracked under DEF-163
(docs/naming hardening pass).

Count by disposition (reviewer decisions):
- MUST-DO (safety / load-bearing): 10 new DEFs (DEF-144..DEF-153).
- SHOULD-DO architectural (witness pattern): DEF-154.
- DEFERRED to future phases: DEF-155..DEF-167 (sketched below).
- Docs-batch consolidation: DEF-163.
- REJECTED outright: ~50 items (most self-rejected by the auditor
  during their own analysis; docs-fix items folded into DEF-163).

### H021 — decision: witness-guard pattern (variant C) selected

**The question.** Post-DEF-119, the crate remains strictly
single-in-flight. Pipelining (PG §52.5 — Parse+Bind+Execute+Sync
batched without waiting for replies) is a PG wire-protocol
feature, scheduled for 1c-5. The architectural question: **is
pipelining on a single connection a first-class feature of the
sans-I/O core, or is it a concern we push up into the pool
layer (`bsql-driver-postgres`)?**

**Three variants considered:**

| Variant | Shape | Tier | Async-friendly |
|---|---|---|---|
| A — stay single-shot forever, pool-layer multiplexing | Status quo | Simple — no new compile-gates | ✅ |
| B — typestate generic `PgProtocol<Phase>` | Each transition changes the concrete type | Tier-1 compile | ❌ (async field storage painful) |
| C — witness-guard pattern (`ReadyGuard<'p>` / `InFlightGuard<'p>`) minted via `proto.as_ready()` | One PgProtocol type, short-lived borrow-witnesses enforce capability | Tier-1 compile | ✅ |

**Decision: variant C.** Rationale:

1. Pipelining is a _nativepg-wire feature_. Delegating it
   entirely to the pool layer would surrender a 3-5× latency
   improvement on short-transaction workloads (batching on one
   connection beats round-tripping N times on N connections for
   correlated work). The sans-I/O kernel is exactly the layer
   that should own wire-level protocol capabilities.

2. Variant B's async friction is a real cost. The witness-guard
   form delivers the same tier-1 compile guarantee without
   making `PgProtocol` a type that shape-shifts over the life of
   an async task.

3. User directive (2026-04-22): "ломай api сколько потребуется,
   лишь бы чисто и надёжно на выходе" — preferring a clean
   durable foundation over backwards-compat hacks.

**Consequences for subsequent work:**

- **Arena design is fixed now with pipelining in mind.** DEF-148
  lands the final `SchemaRef` shape in _one pass_ —
  `NonZeroU8`-indexed slot _plus_ per-slot generation counter —
  so pipelining's concurrent-refs story is structurally ready.
- **DEF-154 buffer-witness** is the prototype for the general
  witness-guard infrastructure; DEF-158 (`ArenaWriter` /
  `ArenaReader` witness tokens) is its direct follow-on.
- **Public API freeze before v1.0** will finalise the
  `proto.as_ready() → ReadyGuard` vs `proto.as_pipelining() →
  PipeliningGuard` entry-point choice. Schedule a dedicated
  architect session after Phase α + β land.

**Explicitly NOT committed yet:** the exact shape of the
guard types, how `ReplyId<K>` integrates with multiple
in-flight slots, and the pool-side integration story. Those
are the witness-guard session's output.

### Phase α — MUST-DO batch (DEF-143..DEF-153)

Ten independent commits, each self-contained. Safe to ship in
parallel. No inter-dependencies that force ordering.

### DEF-143 — cargo-bench harness for per-frame throughput (OPEN, paired with DEF-134)

**Origin.** User directive post-DEF-119: "сделать и def 134 и
143, и лишь потом двигаться дальше". Audit H023/H024/H028 all
point to this.

**Scope.** Criterion-based bench targets for the hot paths:
- `parse_header` single-frame throughput
- `feed_bytes` loop on a synthetic 1000-row SELECT reply stream
- `push_command(Ping)` + `feed_bytes(RFQ)` round-trip
- `push_bind_execute` with 0/1/16 params

**Ship-order.** Pair with DEF-134 (cargo-fuzz). Both are infra
prerequisites for the CONSIDER bucket below (B013 LUT dispatch,
E005 RowDesc::eq prefix, E015 per-state split) — those items
require measured evidence before committing to restructures.

**Tier lift.** Not a tier lift — enables measurement-dependent
decisions.

### DEF-144 — `parse_header`: drop dead `NonZeroU32::new` branch (A015)

**Origin.** Audit A015. `frame.rs:178` returns early on
`declared < 4`; by line 208 `declared >= 4` is proved, so
`NonZeroU32::new(declared)` always returns Some and the else
branch (lines 213-216) is architecturally dead.

**Fix.** Remove the `NonZeroU32::new` check. Flow
`HeaderParse::Ok` directly after the declared-range guards.

**Tier lift.** Tier-3 audit-dead branch → absent. Hottest parser
on the wire path drops one conditional per frame.

**Touched files.** `src/frame.rs` (one function, ~10 LoC net).

**Test count.** 194 → 194. Existing `tests/frame_parse.rs`
exercises all four HeaderParse outcomes.

### DEF-145 — `nz(0)` test-helper hardening + centralise (A005)

**Origin.** Audit A005. `nz(n: u64) -> NonZeroU64` duplicated
across ~6 test files with
`NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)` — silently
coerces `0 → 1`. Two tests using `nz(0)` and `nz(1)` would
silently collide.

**Fix.** Centralise the helper in one place (e.g.
`src/test_util.rs` under `#[cfg(test)]`). Precede the
`unwrap_or` with `assert!(n > 0, "nz(0) is a test bug — use
nz(1..) for non-zero test correlators")`. Assert fires loudly
on misuse; `unwrap_or(MIN)` satisfies forbid-bundle.

**Tier lift.** Tier-4 silent-coerce → tier-2 runtime assertion.

**Touched files.** New `src/test_util.rs` (test-only module);
6 test files drop local `nz` definitions and import the shared
one.

**Test count.** 194 → 194.

### DEF-146 — `StatePushClass` classifier collapses 7× or-pattern duplication (B002)

**Origin.** Audit B002. Seven compute_push_* helpers at
`protocol.rs:949, 1071, 1161, 1311, 1497, 1592, 1804` each
enumerate the same ~18 ProtoState variants in identical
or-patterns for the `CommandInProgress` / similar catch-all
handling. Adding a new state variant today requires seven
synchronised edits.

**Fix.** New crate-private enum:
```rust
pub(crate) enum StatePushClass {
    Idle,
    Errored(StateErrorKind),
    Connecting,
    BusyQuery,
    PingAwaiting,
}

impl ProtoState {
    pub(crate) const fn push_class(&self) -> StatePushClass { ... }  // one exhaustive match
}
```

Each `compute_push_*` then matches `state.push_class()` — the
match is exhaustive on `StatePushClass` (no `_` fallback,
tier-1 compile preserved). Adding a ProtoState variant = 1 edit
(inside `push_class`); all 7 compute_push_* helpers flow
through automatically.

**Tier lift.** The pre-form is tier-1 exhaustive-match × 7
(correct but with 7× drift surface). The post-form is tier-1
exhaustive × 2 (push_class is exhaustive; each compute_push_*
match on StatePushClass is exhaustive). Strict improvement —
same compile-time safety at one-seventh of the drift surface.

**Critical constraint.** The happy-path Idle arm needs `state`
by value (moves into `compute_push_ping`'s happy-path emission);
the classifier works by-ref. Structure: match-on-class first,
then in the `StatePushClass::Idle` arm, re-destructure the
owned state. Single re-match is acceptable cost.

**Touched files.** `src/state.rs` (+classifier + exhaustive
match ~60 LoC), `src/protocol.rs` (7 compute_push_* bodies
collapse; net ~80 LoC deleted).

**Test count.** 194 → 194. The existing
`compute_push_tests` and `allows_unsolicited_param_status_tests`
continue to pin the behaviour table.

### DEF-147 — Narrow `FrameCoords` + `NonEmptyRange` from `usize` to `u16` (B005 + E019)

**Origin.** Audit B005 + E019. `FrameCoords` = 3×`usize` = 24 B.
`NonEmptyRange` = `usize + NonZeroUsize` = 16 B. `READ_BUF_CAP =
4096` fits `u16` with the existing drift guard at
`src/buf.rs:36-39` (`READ_BUF_CAP ≤ 65_535`).

**Fix.** Narrow all five newtypes (`AbsFrameStart`,
`FrameTotalLen`, `PopulatedLen`, plus the two `NonEmptyRange`
fields) to `u16` / `NonZeroU16`. Widen on access via
`usize::from` (infallible; zero-cost on 64-bit).

Sizes after:
- FrameCoords: 24 B → 8 B (aligned).
- NonEmptyRange: 16 B → 4 B.

**Throughput impact.** On a 1000-row SELECT: `NonEmptyRange`
emission × 1000 = 12 KB of stack traffic eliminated.

**Tier lift.** Not a tier change — pure byte compression.
Drift guard on `READ_BUF_CAP ≤ 65_535` already in place; extend
it to pin the u16 assumption explicitly.

**Touched files.** `src/dispatch.rs` (newtypes + widening at
call sites), `src/action.rs` (NonEmptyRange constructor +
apply).

**Test count.** 194 → 194.

### DEF-148 — `SchemaRef` one-pass final shape: `NonZeroU8` slot + generation counter (C001 + B001 + C006 + A009 classification prep)

**Origin.** Audit C001 (NonZeroU8 niche), B001 (ZERO sentinel
is a structural seam), C006 (generational counter),
A009 (stale ref silently → NoData). User directive 2026-04-22:
"если ты можешь за один проход — было бы здорово, чтобы к
этому вопросу не возвращаться". H021 decision: variant C
commits the crate to pipelining; SchemaRef must be
pipelining-ready now.

**Final shape.**

```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaRef {
    slot: NonZeroU8,    // encodes slot_index + 1; 1..=MAX_ARENA_SLOTS+1
    generation: u8,     // captured at alloc; checked at get
}
// sizeof::<SchemaRef>() == 2; Option<SchemaRef> niche-packs to 2 B
// (niche on slot field).

pub(crate) struct SchemaSlab {
    slots: [Option<RowDesc>; MAX_ARENA_SLOTS],
    generations: [u8; MAX_ARENA_SLOTS],    // per-slot; bumps on free/clear
    has_any: bool,                          // A007 fast-path for clear()
}
// sizeof::<SchemaSlab>() ≈ 2×264 + 2×1 + 1 = 531 bytes, padded ≈ 536.
```

**Semantics:**

- `alloc(desc)`: find free slot, write `Some(desc)`, capture
  current `generations[slot]`, set `has_any = true`, return
  `SchemaRef { slot: NonZeroU8::new(idx+1), generation }`.
- `get(r)`: if `generations[r.slot-1] == r.generation` AND
  `slots[r.slot-1].is_some()` → `Some(&desc)`; else `None`
  (stale ref — classifiable for A009).
- `free(r)`: if generation matches, bump that slot's
  generation, set slot to None, recompute `has_any`.
- `clear()`: if `!has_any`, return early (A007). Else: for each
  occupied slot, bump generation + set None; `has_any = false`.
- `generation: u8` wraps — 256-cycle period. At 1 cycle per
  query completion, that's 256 queries before the same
  (slot, generation) pair recurs. A **stale ref surviving 256
  full arena cycles** collides — architecturally impossible in
  the current flow (arena is cleared between every user-visible
  query boundary; a stale ref's lifetime ended long before the
  next query starts). Documented as a tier-3 invariant; if 1c-5
  pipelining reveals a collision window, promote to `u16`.

**Closed items (one commit):**
- C001 niche packing (Option<SchemaRef> = 2 B instead of 2 B
  from plain u8 discriminant — NOT a byte saving vs bare
  `Option<u8>`, because NonZeroU8 needs one more byte for
  generation. The saving vs the _combined_ MUST-DO "plain u8 +
  separate sentinel" design is the ZERO-sentinel elimination,
  not a raw byte win).
- B001 ZERO sentinel class retired (no valid SchemaRef can
  equal the test-fixture sentinel, because NonZeroU8 forbids
  zero entirely).
- C006 generational counter.
- A009 diagnostic path prep (stale ref → None is now
  classifiable because ALL `arena.get(r).is_none()` cases after
  successful dispatch are crate bugs; DEF-150's InternalCrateBug
  locus reads this).

**Tier lift.**
- Sentinel-that-overlaps-valid-handle: tier-3 structural → tier-1
  compile (impossible by type).
- Stale-ref silent-NoData-substitution: tier-4 "should not
  happen" → tier-2 classifiable diagnostic (via DEF-150).
- Pipelining concurrent-refs prep: tier-3 "future refactor"
  → tier-1 structurally ready now.

**Touched files.**
- `src/schema_arena.rs` — major refactor (~60 LoC net added).
- `src/dispatch.rs` — 3 alloc sites update (same arms, same
  match shape, different handle construction).
- `src/state.rs` — Option<SchemaRef> type unchanged textually.
- `src/action.rs` — SchemaRef usage unchanged textually.
- `src/protocol.rs` — test fixtures for SchemaRef construction
  update (no more `SchemaRef::ZERO`; use
  `arena.alloc(EMPTY).unwrap_or_else(...)` through the
  established forbid-compliant pattern).
- `src/lib.rs` — size pin for SchemaRef unchanged at 2 B;
  SchemaSlab pin bumps from 528 → 536.

**Test count.** 194 → 194+N (add unit tests for: generation
bump on clear invalidates old ref; generation bump on free
invalidates old ref; generation wraparound; `has_any`
fast-path correctness).

**Coordination.** Must land BEFORE DEF-150 (which classifies
stale ref via InternalCrateBug) and BEFORE DEF-159 (SCRAM
arena reuses the pattern).

### DEF-149 — `transition_to_errored` helper consolidates state-set + buffer-clear (A013)

**Origin.** Audit A013. Two sites in `protocol.rs` (lines
466-474 Errored early-return, and 784-786
fail_inflight_and_close) maintain the "set Errored +
read_buf.clear + build OutActions" pairing independently. No
bug today, but a tier-3 audit pairing (order consistent by
convention, not structurally).

**Fix.**
```rust
impl PgProtocol {
    fn transition_to_errored(&mut self, kind: StateErrorKind)
        -> Option<NonZeroU64> {
        let prev = core::mem::replace(
            &mut self.state,
            ProtoState::Errored(kind),
        );
        self.read_buf.clear();
        prev.take_inflight_reply_raw_id()
    }
}
```

Both sites delegate. Atomic state-set + buffer-clear is encoded
in the helper's single body.

**Tier lift.** Tier-3 audit-pairing → tier-2 structural (one
helper, two callers).

**Touched files.** `src/protocol.rs` (helper + 2 call-site
refactors).

**Test count.** 194 → 194.

### DEF-150 — `ProtocolError::InternalCrateBug { locus }` merge + `SchemaArenaAllocFull` + `StaleSchemaRef` loci (A001 + B009 + A009 classification)

**Origin.** Audit A001 (schema arena alloc-full classifies as
`RowRangeConstructionUnreachable` — the WRONG variant), B009
(three InternalCrateBug variants should merge), A009
(post-DEF-148 stale-ref is a classifiable event).

**Fix.** Single unified variant with inner locus enum:
```rust
pub enum ProtocolError {
    ...
    InternalCrateBug {
        locus: CrateBugLocus,
    },
    ...
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrateBugLocus {
    /// Frame builder returned Err despite const-asserted capacity.
    /// (was `ProtocolError::OutboundFrameBuildUnreachable { stage }`)
    OutboundFrameBuild { stage: FrameBuildStage },
    /// ReadBuf::advance returned Err despite pre-validated frame bounds.
    /// (was `ReadCursorAdvanceUnreachable`)
    ReadCursorAdvance,
    /// NonEmptyRange::new returned None despite pre-validated frame bounds.
    /// (was `RowRangeConstructionUnreachable`)
    RowRangeConstruction,
    /// Schema arena alloc returned None — arena full in a flow that
    /// shouldn't have more than MAX_ARENA_SLOTS concurrent schemas.
    /// NEW (was previously misclassified as RowRangeConstruction).
    SchemaArenaAllocFull,
    /// Arena get returned None on a ref that should be live (post
    /// successful dispatch, pre free/clear). Indicates generational
    /// drift — a crate bug in alloc/clear ordering.
    /// NEW (was previously silent NoData substitution).
    StaleSchemaRef,
}
```

All classify as `ErrorKind::Internal`. Display text is
uniform: `"internal crate bug at {locus:?}"`.

**Tier lift.**
- A001 diagnostic misdirection: classification lies → correct.
- B009 variant sprawl: 3 separate variants → 1 with
  discriminated locus (smaller enum, consistent Display).
- A009 silent-NoData class: runtime → classifiable +
  connection-fatal (closer to tier-2).

**Coordination.** Lands AFTER DEF-148 (SchemaRef with
generation; A009's stale-ref detection requires it).

**Touched files.** `src/error.rs` (variant merge + new loci),
`src/dispatch.rs` (3 arena-alloc arms), `src/action.rs` /
`src/protocol.rs` (stale-ref detection sites, DEF-119's
`unwrap_or(&EMPTY)` replaced with classified fail path).

**Test count.** 194 → 197 (add tests for:
`SchemaArenaAllocFull` path — requires a test fixture that
forces arena exhaustion; `StaleSchemaRef` path — forced
generation mismatch; existing `FrameBuildStage` / advance /
row-range tests migrate to new variant shape).

### DEF-151 — Tighten size budgets: exact pins on SchemaSlab + PgProtocol (C002 + C010)

**Origin.** Audit C002 + C010. Current assertions use `<=`
slack (SchemaSlab ≤ 540 vs actual ~528; PgProtocol ≤ 6336 vs
actual 6272). Slack hides regression-catching signal in both
directions (ADDED field vs REMOVED field both change size;
slack only catches additions).

**Fix.** Post-DEF-148 SchemaSlab grows to ~536 B (one extra
byte per slot + has_any). Post-DEF-147 PgProtocol size is ~net
zero change (u16 narrowing offsets other growth). Use tight
ranges to permit ±8 B cross-platform alignment slack:

```rust
const _: () = assert!(
    core::mem::size_of::<SchemaSlab>() <= 544
        && core::mem::size_of::<SchemaSlab>() >= 528,
    "SchemaSlab size drift — expected ~536 B post-DEF-148."
);
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol>() <= 6280
        && core::mem::size_of::<protocol::PgProtocol>() >= 6272,
    "PgProtocol size drift — expected ~6272 B post-DEF-148."
);
```

**Rationale.** Exact-size assertions would be fragile across
aarch64 / x86_64. Tight range catches drift in either direction
while tolerating platform alignment.

**Tier lift.** Regression-catching budget slack → tighter
structural pin.

**Touched files.** `src/lib.rs` (size-assert block).

**Test count.** 194 → 194 (const-asserts are build-time).

### DEF-152 — Arena `has_any` fast-path + debug-assertions invariant probes (A007 + C013)

**Origin.** Audit A007 + C013. `SchemaSlab::clear()` currently
unconditionally zeroes both slots even when the arena is
already empty — 528 B of wasted memset per push on the Ping
loop (state is always Idle between pings). `occupied_count()`
is `cfg(test)`-gated despite being useful for
debug-assertions-gated invariant probes.

**Fix.** Bundled with DEF-148 (SchemaSlab is already being
restructured; adding `has_any` is part of the same change).

Post-DEF-148, `clear()` checks `has_any` first:
```rust
pub(crate) fn clear(&mut self) {
    if !self.has_any { return; }
    for (slot, gen) in self.slots.iter_mut().zip(self.generations.iter_mut()) {
        if slot.is_some() {
            *gen = gen.wrapping_add(1);
            *slot = None;
        }
    }
    self.has_any = false;
}
```

`occupied_count()` switches from `cfg(test)` to
`cfg(debug_assertions)`. Protocol entry points add invariant
probes:
```rust
if matches!(self.state, ProtoState::Idle | ProtoState::Errored(_)) {
    self.schema_arena.clear();
    debug_assert_eq!(
        self.schema_arena.occupied_count(), 0,
        "clear() must leave arena empty",
    );
}
```

**Perf impact.** Ping loop: 528 B/iter memset eliminated. At 1M
pings/sec, ~1 GB/s of wasted memory bandwidth freed.

**Tier lift.** Debug-time invariant probe on clear()
correctness — catches future regressions where a refactor
breaks the has_any bookkeeping.

**Coordination.** LAND AS PART OF DEF-148 — mechanically
inseparable (same struct, same method signatures). Documented
here for reviewer clarity; single commit at DEF-148 time.

**Touched files.** Subsumed into DEF-148's touched-files list.

**Test count.** Subsumed into DEF-148 (existing arena tests
exercise clear-after-alloc and clear-on-empty paths).

### DEF-153 — SessionParams `n_malformed_bool_dropped` counter (A003)

**Origin.** Audit A003. `session_params.rs:361-373` silently
drops non-standard bool values (e.g. `is_superuser=yes` vs
PG's canonical `on`/`off`) — leaves field as `None`,
indistinguishable from "server never sent the parameter."

**Fix.** Mirror the existing F-074 `n_unknown_dropped` counter:
```rust
pub struct SessionParams {
    ...
    /// Count of ParameterStatus values that failed bool parsing
    /// (non-standard forms like "yes" / "1" instead of "on"/"off").
    /// Saturating u16.
    pub n_malformed_bool_dropped: u16,
    ...
}
```

Increment in the two bool-parsing arms (is_superuser,
integer_datetimes) when `parse_pg_bool` returns None.

**Tier lift.** Silent-drop → operator-visible counter. Same
class as F-074 (`n_unknown_dropped`).

**Touched files.** `src/session_params.rs` (+1 field + 2
increment sites).

**Test count.** 194 → 196 (add 2 tests: malformed
`is_superuser=yes` increments counter; parseable
`is_superuser=on` does not).

### Phase β — SHOULD-DO big piece (DEF-154)

### DEF-154 — Buffer-witness pattern for `apply` / `materialise` infallible access (A008 + A009 + C015 + E016 + B003 alignment)

**Origin.** Audit A008 (NonEmptyRange::apply returns Option
with silent `&[]` fallback), A009 (stale SchemaRef silently →
NoData — closed classifier side in DEF-150, open architectural
side here), C015 (arena.get unwrap_or EMPTY on stale ref),
E016 (per-row dead Err branch on range construction), plus
coordinates with DEF-141 (B003 — infallible builders).

**Problem class.** Five independent sites carry "architecturally
dead Option / Result" from a value produced inside the
protocol to a value consumed inside the protocol. Each site
falls back to a silent default on None; each is documented
as tier-3 audit-dead. Five drift surfaces, one semantic.

**Fix: witness-guard infrastructure.**

```rust
/// Proof token: this WriteBuf was reserved for N bytes;
/// constructible only via `WriteBuf::reserve::<N>()`. Moved into
/// each build_*_message as a capacity proof.
pub(crate) struct WriteReserved<'a, const N: usize> {
    buf: &'a mut WriteBuf,
    start: usize,
    // ...
}

/// Proof token: this NonEmptyRange was built from THIS buffer's
/// state. Can only be applied to the same buffer; a different
/// buffer is a compile error.
pub(crate) struct NonEmptyRange<'buf> {
    start: u16,
    len: NonZeroU16,
    _buf: PhantomData<&'buf ()>,
}
impl<'buf> NonEmptyRange<'buf> {
    pub(crate) fn apply(&self, buf: &'buf [u8]) -> &'buf [u8] {
        // INFALLIBLE — witness proves start + len <= buf.len().
        &buf[usize::from(self.start)..usize::from(self.start) + self.len.get() as usize]
    }
}

/// Proof token: this SchemaRef was built from THIS arena's
/// state. Combined with DEF-148's generation counter, this
/// gives compile-time + runtime invalidation. The compile-time
/// layer catches buffer-type confusion; the runtime layer
/// catches alloc/free ordering bugs.
pub(crate) struct ArenaReader<'arena> {
    slab: &'arena SchemaSlab,
}
impl<'arena> ArenaReader<'arena> {
    pub(crate) fn get(&self, r: SchemaRef) -> Option<&'arena RowDesc> {
        // ... generation check ...
    }
}
```

**Closed seams:**
- A008 (`apply` silent `&[]`): compile-time impossible —
  wrong-buffer is a type error.
- A009 classifier side (already in DEF-150) + architectural
  side: `arena.get` is now a method on `ArenaReader<'arena>`;
  only dispatch can construct `ArenaWriter`, only materialise
  gets `ArenaReader`.
- C015 silent EMPTY fallback: same as A008/A009.
- E016 per-row dead Err branch: witness-bound `NonEmptyRange`
  is infallible; the Result wrapper goes away.
- DEF-141 (B003) infallible builders: `WriteReserved<'_, N>`
  is exactly the capacity-witness it needs. Same infrastructure.

**Tier lift.** Five tier-3 audit-dead paths → tier-1
structurally-absent. Biggest durability win in this audit.

**Coordination.** Lands AFTER DEF-147 (u16 narrowing is a
prerequisite for the NonEmptyRange witness shape). Subsumes
DEF-141 (B003) — the capacity-witness branch of this work IS
DEF-141, so mark DEF-141 as "subsumed by DEF-154" at commit
time.

**Touched files.**
- `src/action.rs` — NonEmptyRange lifetime-bound.
- `src/write_buf.rs` — WriteReserved witness type.
- `src/schema_arena.rs` — ArenaReader/ArenaWriter split.
- `src/dispatch.rs` — dispatch takes ArenaWriter; passes
  NonEmptyRange-with-lifetime down to StreamRow staging.
- `src/protocol.rs` — materialise takes ArenaReader; 6
  build_*_message fns take WriteReserved<_, N>.
- All push-path call sites construct the witnesses.

**Effort.** L (200-400 LoC net). Biggest architectural piece
in this batch.

**Test count.** 194 → ~210 (witness misuse tests via
compile_fail doctests; existing tests flow through; new
classification tests for A009-via-DEF-150 complete the
coverage).

### Phase γ — DEFERRED (future phases, sketched)

Each item below has enough context to schedule when its phase
arrives. Deep DEF entries are written at implementation time.

### DEF-155 — Generational counter prep for 1c-5 pipelining

**Status.** SUBSUMED by DEF-148. The per-slot generation
counter is landed in DEF-148's final SchemaRef shape. No
separate DEF needed; kept here as a forwarding pointer for
audit traceability.

### DEF-156 — `materialise_push` vs `materialise_feed` type split (A014)

**Schedule.** 1c-6 hardening sub-phase.

**Origin.** Audit A014. Current `materialise` handles both
push-path (write_buf emission, no read_buf / arena) and
feed-path (full-featured). Push callers pass `&[]` as read_buf
— a tier-3 audit that push-path never emits StreamRowRange.

**Sketch.** Two staged enums: `PushStagedAction` (no
StreamRowRange variant) and `FeedStagedAction` (full). Two
materialise functions. Tier-3 → tier-1 compile ("arena only
touched by feed-path").

**Effort.** L. Ripples through compute_push / dispatch.

### DEF-157 — ProtoState sum-of-subsums restructure (B006)

**Schedule.** Post-1c-4 (transactions) when state shape is
stable.

**Origin.** Audit B006. 22 variants of ProtoState make the
dispatch match dense. Splitting into
`ProtoState::Ping(PingSubState)` / `::Startup(StartupSubState)`
/ `::Query(QuerySubState)` / etc. reduces cognitive load and
collapses dispatch match arms.

**Sketch.** Each sub-enum owns its variants; dispatcher
pattern-matches the outer, then the inner. Adding a variant
stays local to the sub-enum.

**Effort.** L. Does NOT shrink ProtoState size (Startup still
dominates at ~1224 B).

### DEF-158 — Arena witness typestate (C005) — bundled with DEF-154

**Status.** DEF-154 infrastructure already introduces
`ArenaWriter` / `ArenaReader`. DEF-158 is the name-pointer
for the "discipline is compile-enforced, not audited" claim
that DEF-154 makes possible. Track under DEF-154 at commit
time; no separate work.

### DEF-159 — SCRAM arena (D001)

**Schedule.** Post-1c-5 pipelining lands.

**Origin.** Audit D001. SCRAM state buffers (ScramSession
~1024 B + PodBytes<128> client_first_bare + PodBytes<48>
client_nonce_b64) dominate ProtoState at ~1200 B. Extracting
to `Option<ScramArena>` on PgProtocol drops the state-variant
cost to near-zero when not in SCRAM (which is most of
connection lifetime).

**Sketch.**
```rust
pub struct PgProtocol {
    ...
    scram_arena: Option<ScramArena>,  // Some only during SCRAM
}

pub(crate) struct ScramArena {
    session: ScramSession,
    client_first_bare: PodBytes<128>,
    client_nonce_b64: PodBytes<48>,
}
```

State variants carry a crate-internal `ScramArenaRef` (ZST or
bool — only one SCRAM ever in flight).

**Expected savings.** PgProtocol steady-state: ~6272 → ~5072 B
(−19%). SCRAM-active cost unchanged (arena pays the 1200 B
once, not per-state-variant).

**Constraint.** Must land AFTER H021 witness-guard shape is
finalised — the SCRAM arena access is part of the witness
story (mid-handshake transitions need typed access
through the guard).

**Effort.** M-L.

### DEF-160 — `PgCommand::Parse` carries `&'a str` SQL instead of owned 2 KB (D003)

**Schedule.** 1c-6 / API freeze pre-v1.0.

**Origin.** Audit D003. `PgCommand::Parse { sql: Sql, ... }`
inlines a 2050-byte buffer even for 10-byte queries. Borrow
form:
```rust
pub enum PgCommand<'a> {
    ...
    Parse {
        sql: &'a str,
        stmt_name: StmtName,
        reply: ReplyId<ParseKind>,
    },
    ...
}
```

**Tradeoff.** Adds lifetime to `PgCommand`. Ripples through
every consumer. Pre-v1.0 acceptable.

**Effort.** L (API break; ripples through tests and eventual
driver).

### DEF-161 — Error-body arena for `ProtocolError::ServerErrorResponse` (C008)

**Schedule.** Post-fuzz (DEF-134) if error path becomes a
bottleneck.

**Origin.** Audit C008. ServerErrorResponse carries 5 inline
BoundedStr totalling ~300 B. Externalising to an error arena
shrinks ProtocolError from 304 B → ~16 B; cascades through
FailReply.cause variants.

**Sketch.** `ErrorArena` on PgProtocol with `heapless::String<384>`
byte pool + ranges. ProtocolError::ServerErrorResponse carries
the ref.

**Tradeoff.** Error is currently cold-path. Complexity-to-win
ratio only favourable if fuzzing or prod telemetry shows
error-path load.

**Effort.** L.

### DEF-162 — cargo-mutants kill-rate target (H027)

**Schedule.** Infra batch alongside DEF-134 (fuzz) + DEF-143
(bench).

**Origin.** Audit H027. No current mutation-testing target.

**Sketch.** Run cargo-mutants; evaluate test coverage of
semantic edits. Kill-rate target ≥85%. Anything below pins a
missing test.

**Effort.** S infra setup + iterative test additions.

### DEF-163 — Docs / naming hardening pass (consolidated G-series + DOCS-FIX-NEEDED)

**Schedule.** After Phase α + β MUST-DO/SHOULD-DO batches land.

**Origin.** Consolidates all G-series audit items (docs/naming
polish) PLUS the DOCS-FIX-NEEDED items surfaced when reviewing
rejected findings. The latter are load-bearing: they prevent
future audits from repeating the same misreads.

**Items (flat list, one commit per group):**

*Cross-reference + status hygiene:*
- G001 — cross-reference "1c-5 pipelining" to reforge.md /
  deferred.md at docstring sites.
- G002 — split pre/post-DEF-119 size baselines into "CURRENT"
  vs "HISTORICAL" in lib.rs.
- G008 — sweep all "Tier-1 compile" claims in rustdoc for
  enforcement-mechanism citations (forbid bundle line, const
  assert line, exhaustive match, etc.).

*Naming corrections:*
- G004 — rename `SchemaSlab` → `SchemaArena` (slab overloads
  the kernel slab-allocator meaning).
- G011 — rename `DescribedRowsRef` → `DescribedRowsStaged`
  ("Ref" suffix conflicts with Rust's borrow-naming convention;
  the type is staged / lifetime-free, not a reference).

*Load-bearing docstring additions (from rejection DOCS-FIX-NEEDED):*
- A006 add to ReplyId: "ReplyId::value is a correlator, not a
  secret — intentionally NOT zeroized on drop."
- A011 add to `FixedStr::from_str_truncating`: "fit_end=0 is a
  valid terminal state; output is a 3-byte OVERFLOW_MARKER-only
  FixedStr, valid by MARKER.len() ≤ N (pinned by
  _TRUNCATING_N_MIN)."
- A012 add to dispatch.rs Errored arm: "Architecturally dead
  under current flow — feed_bytes short-circuits Errored
  before dispatch (protocol.rs:466-474). Kept for exhaustive
  match over (ProtoState, tag)."
- B011 add to `Action<'w, 'r>` docstring: "'w borrows write_buf
  (push-path emission); 'r borrows read_buf + arena (feed-path
  streaming). Two lifetimes required because push produces
  'static-to-'r and feed produces actual-'r; unifying forces
  'r='static and breaks feed-path row streaming."
- F001 sweep all `#[doc(hidden)] pub` comments to consistently
  cite "pub required by public `ReplyKind::StagedPayload`
  associated type (Rust language rule on pub-in-pub-trait)."

*Arch explanatory additions:*
- G007 — downgrade arena-discipline claim from "tier-2
  structural" to honest "tier-3 audit" (OR lift to tier-1 via
  DEF-158 witness — contingent on DEF-154's witness landing).
- G012 — add PgProtocol struct-level comment cross-referencing
  the size budget in lib.rs.
- G016 — ident.rs module-level ASCII diagram of the
  `FixedStrKind` trait hierarchy.
- G017 — scram/mod.rs RFC 5802 exchange-flow diagram.
- G018 — add top-of-file doc on compute_push vs dispatch
  naming convention.
- G021 — template `#[expect(clippy::result_large_err, reason = ...)]`
  reasons for grep-ability (same phrase everywhere: "no_alloc:
  Box unavailable; error path only").

**Tier lift.** Docs don't move tiers — but load-bearing
invariants currently buried in "subtle" places get surfaced,
reducing the chance a future auditor or contributor misreads
them again.

**Touched files.** Many — small edits across most .rs files.
Test suite unchanged.

**Effort.** M (mechanical but broad).

### DEF-164 — `ReplyId.delivered` debug-assertions-gated (B008 sub-idea)

**Schedule.** Later polish — only if release-build size
shows the delivered-flag as measurable (needs DEF-143 bench).

**Origin.** Audit B008. The typestate idea was rejected (Rust
Drop coherence). The sub-idea is to gate the runtime
`delivered: bool` field behind `cfg(debug_assertions)`:
release builds drop the flag (8 B saved per ReplyId), debug
builds keep the Drop-guard.

**Tradeoff.** Release builds lose the Drop-guard entirely. The
guard has caught real bugs; downgrading it is a real cost.

**Effort.** S but probably should not land — if we want tier-2
runtime Drop-guard, we want it in production too.

### DEF-165 — `ParamOids::n_params` narrow `u16` → `u8` (F005)

**Schedule.** Polish batch alongside DEF-147.

**Origin.** Audit F005. `n_params ≤ MAX_PARAMS_ARITY = 16` fits
u8 trivially. Widen on consumer access.

**Effort.** S.

### DEF-166 — `PodBytes<N>` visibility `pub → pub(crate)` via state-field privatize (F008)

**Schedule.** v1.0 API freeze.

**Origin.** Audit F008. Currently pub because
`ProtoState::ConnectingScramAwaitingServerFirst` destructures
`client_first_bare: PodBytes<...>` publicly.

**Sketch.** Privatise state-variant fields. External users can't
construct ProtoState anyway (no pub constructors for non-Idle).
Public matches on field names would break — acceptable pre-v1.0.

**Effort.** S.

### DEF-167 — Split `action.rs` / `dispatch.rs` into submodules (G014 + G015)

**Schedule.** Post-1c-4, once DEF-146 + DEF-157 restructures settle.

**Origin.** Audit G014 + G015. `action.rs` is 1397 LoC,
`dispatch.rs` is 1965 LoC. Post-DEF-146 classifier + DEF-157
sum-of-subsums, the natural submodule boundaries become visible.

**Effort.** M (file reorg).

### Phase δ — Architectural review session (H021 follow-on)

Schedule a dedicated rust-senior-architect session AFTER
Phase α + β land. Agenda:
- Finalise the `proto.as_ready() → ReadyGuard<'p>` vs
  `proto.as_pipelining() → PipeliningGuard<'p>` entry-point
  shape.
- Decide how `ReplyId<K>` pairs with multiple concurrent
  in-flight slots (does the raw u64 correlator stay alone, or
  does it carry a slot-tag?).
- Settle the public API shape before v1.0 freeze.
- Output: a dedicated DEF ticket with the definitive witness-
  guard design and migration sketch.

This is NOT a DEF ticket itself — it's the decision-making
event whose output is a DEF.

### Rejected items — docs-fix registry

The following audit items were REJECTED as code changes but
DID surface a misreading of our source by the auditor. Each
misreading is a symptom of a docstring / naming / arch comment
that needs a fix to prevent the same misread next time. All
the docs-fixes are consolidated into DEF-163 (above); this
subsection is the invariant-registry so future audits can
cross-check.

| Audit ID | Auditor's misread | Root cause | Docs-fix in DEF-163 |
|---|---|---|---|
| A002 | "Arena alloc-on-full is impossible in 1c — lift to infallible" | Missing pipelining-context note on `alloc` docstring | Add "Fallible shape intentional — pipelining flow lands here" |
| A004 | "record_param_status drops non-UTF8 keys" | No explicit spec-invariant comment on the byte-equal match | Add "PG §55.2.1 — keys are ASCII by spec" + const_assert |
| A006 | "ReplyId.value not zeroized" | Docstring doesn't explicitly say the field is a correlator, not a secret | Line added per G018 spec |
| A011 | "BoundedStr fit_end=0 pathological" | The invariant is subtle and not inline-documented | Inline invariant comment per G018 spec |
| A012 | "Errored dispatch arm silently consumes" | The arm looks real; it's actually dead-by-structure | Pinning comment referencing feed_bytes short-circuit |
| B001 | "SchemaRef::ZERO sentinel is a seam" | Sentinel's test-only purpose is cfg-gated but the overlap with valid SchemaRef(0) wasn't called out | Subsumed by DEF-148 (NonZeroU8 eliminates the possibility) |
| B011 | "Two lifetimes on Action<'w, 'r> look unnecessary" | The two-lifetime rationale buried in DEF-094/DEF-119 history, not inline | Inline rationale per B011 DOCS-FIX |
| C016 | "DescribedRows / DescribedRowsRef duplication" | "Ref" suffix semantically wrong | Rename to DescribedRowsStaged (G011) |
| F001 | "SchemaRef doc-hidden pub should be pub(crate)" | The "pub-in-pub-trait-associated-type" rationale is in ONE docstring but not consistently on siblings | Consistency sweep per F001 DOCS-FIX |
| G007 | "Arena discipline is tier-2 structural" — claim is actually tier-3 audit | Tier label overstated | Either downgrade (honest) or lift via DEF-158 witness |

These are the class of "audit surfaces the same issue twice"
risks. DEF-163's job is to close them all.

### Summary of Phase α-ε ship order

**Phase α (safe to ship in parallel, any order):**
- DEF-143 (benchmark harness — enables measurement-gated work)
- DEF-144 (parse_header dead branch)
- DEF-145 (nz(0) hardening)
- DEF-148 (SchemaRef final — bundles C001 + B001 + C006 + A007 + C013)
- DEF-149 (transition_to_errored helper)
- DEF-150 (InternalCrateBug merge — depends on DEF-148)
- DEF-151 (size budget pins — depends on DEF-148)
- DEF-153 (SessionParams counter)

**Phase β (sequenced):**
- DEF-147 (u16 narrowing — prereq for DEF-154)
- DEF-146 (StatePushClass classifier — independent)
- DEF-154 (buffer-witness pattern — subsumes DEF-141)

**Phase γ (deferred to specific future phases):**
- Listed above (DEF-156..DEF-167).

**Phase δ (architect session before 1c-5):**
- H021 witness-guard finalisation → produces post-session DEFs.

**Phase ε (after all above):**
- DEF-163 (docs/naming hardening pass).

**Infra (parallel, as bandwidth allows):**
- DEF-134 (cargo-fuzz) — ALREADY OPEN
- DEF-143 (benchmark) — NEW, listed in Phase α
- DEF-162 (cargo-mutants) — NEW

After Phase ε, the bsql-pg-proto core reaches 1c-5-ready state:
tier-1 gates on discipline invariants, witness infrastructure
for pipelining, arena shape final for pipelined flows,
measurement infra in place to validate CONSIDER-bucket items
with evidence.

## 25. Phase α (executed 2026-04-22) — Phase α2 (second-audit close)

### Phase α shipped (commits `1e8bb0a`..`55fe7b8`)

Ten DEFs from §24 Phase α landed in a single session:

| DEF | Commit | Item |
|-----|--------|------|
| DEF-144 | `1e8bb0a` | parse_header: dropped dead `NonZeroU32::new` branch |
| DEF-145 | `734a64c` | nz/raw test helpers: `assert!(n > 0)` + comment |
| DEF-146 | `55fe7b8` | `StatePushClass` classifier (7× or-pattern collapse) |
| DEF-147 | `f65f280` | `FrameCoords` + `NonEmptyRange` u16 narrowing |
| DEF-148 + DEF-152 | `6537cc9` | `SchemaRef` final shape (NonZeroU8 + u8 gen + has_any + debug probes) |
| DEF-149 | `0ba97f9` | `replace_state_errored_and_drain` atomic helper |
| DEF-150 | `dd76cc6` | `InternalCrateBug { locus }` merge + `SchemaArenaAllocFull` |
| DEF-151 | `768601c` | Tight-range size asserts (lib.rs) |
| DEF-153 | `22755d7` | `SessionParams.n_malformed_bool_dropped` counter |

### Phase α2 — second-pass audit follow-up (commits `0f152c9`..`5810661`)

Second `rust-senior-architect` pass (`audit2.txt`) found 39 findings
post Phase α landing, including 2 P0 items from my own DEF-149 /
DEF-148 refactors that were half-applied. Shipped in Phase α2:

| DEF | Commit | Item |
|-----|--------|------|
| DEF-168 + DEF-175 | `0f152c9` | feed_bytes dispatch-Errored arm: route through `replace_state_errored_and_drain` helper (closes DEF-149's atomicity claim) + explicit `debug_assert` on `StateErrorKind::try_from_kind` None branch (closes silent `unwrap_or(INTERNAL_FALLBACK)` shield) |
| DEF-169 | `60c3cdd` | `push_class` per-variant test module (27 variants × 1 assertion each) — category-1 shield for DEF-146's new classifier |
| DEF-170 | `2e841fa` | debug_assert shields at 3 stale-SchemaRef materialise sites (protocol.rs + 2× action.rs) — bridge to DEF-154 full structural closure |
| DEF-171 | `784c603` | Delete `has_any: bool` from SchemaSlab — derived-state fallback with silent-corruption failure mode; 2-slot walk equivalent codegen |
| DEF-172 | `0d35985` | `clear_arena_if_idle_or_errored` helper (collapse 3× duplication across entry points) |
| DEF-174 | `c17fd4b` | `FrameCoords::new` eliminate u16→usize→u16 identity round-trip + delete dead `.get()` accessors on 3 newtypes |
| DEF-176 | `092fc1a` | `ProtocolError::state_kind()` helper composing `kind() + try_from_kind` — closes the pair |
| DEF-177 | `ac39d55` | Cold helpers: `fail_read_cursor_advance` (4 sites in feed_bytes) + `internal_bug` (4 sites in dispatch) |
| DEF-173 | `15cefbb` | `CrateBugLocus` dedicated `Display` impl + 5 operator-string pin tests |
| DEF-178 | `d117dec` | Polish bundle (6 XS items: `#[inline]` on push_class, `const fn` dead_for_test, docs for derived-state coupling, etc.) |
| DEF-179 | `fb7dbcd` | 1c-5 pipelining blocker markers at 4 sites (WORST_CASE_PER_DISPATCH, take_inflight_reply_raw_id, fail_inflight_and_close, clear_arena helper) |
| DEF-180 | `5810661` | Delete `generation_wraps_around_at_256_cycles` test — architecturally-dead scenario documented with honest rationale on the `generations` field |

Net Phase α + α2 impact:
- Tests: 188 → 203 (+15).
- Clippy: clean throughout.
- Closed silent-corruption / silent-shield classes: A001 (wrong error variant), A005 (nz(0) coerce), A003 (malformed bool drop), A007 (Ping-loop memset waste), A001 (diagnostic misdirection), A010 stale-ref silent EMPTY (debug-time), A012 INTERNAL_FALLBACK silent, A002 has_any derived-state drift, dead `.get()` accessors, dead NonZeroU32 branch, dead `generation_wraps_around_at_256_cycles` test.
- Tier lifts: DEF-146 classifier (7×1), DEF-148 generation + NonZeroU8, DEF-149 + DEF-168 atomic-terminus helper, DEF-150 InternalCrateBug merge, DEF-171 derived-state elimination, DEF-174 type-round-trip elimination, DEF-176 match-pair consolidation.
- Code shrinkage: net −274 +99 in protocol.rs (DEF-146), deletion of 3 dead `.get()` accessors, deletion of 1 dead test.

### DEF-180 follow-on decision (deferred)

The `u8 generation` counter is retained as defence-in-depth for
(a) crate bugs that might leak a SchemaRef beyond its architectural
lifetime, (b) 1c-5 pipelining where concurrent inflight refs make
the stale-ref class real. Under current single-inflight the wrap
is architecturally unreachable (no SchemaRef can be live when
`clear()` runs, per DEF-180 commit's trace analysis).

**Potential future lift: `u8 generation → u16`.** Not done
preemptively because:
1. Under current flow the u8 horizon is architecturally-dead wide.
2. Widening preemptively is "growing a fallback for a null-class
   of bugs" — wrong direction under user's "минимизировать
   fallback" philosophy (user directive 2026-04-22).
3. Real pipelining collision analysis needs evidence — at H021
   witness-guard session, the actual concurrency shape will
   inform whether u8 is too tight or overprovisioned.

**Decision point:** defer the u8 vs u16 choice to the **H021
witness-guard architect session** (pre-1c-5), where pipelining's
concurrency shape will be designed and the collision-window
analysis can be made with evidence, not preemptively. If u16
turns out needed, lift at that time; otherwise u8 stays.

### Remaining audit2 items — re-classified

- **A008** (u8 → u16 generation): **deferred to H021** per above.
- **A018** (OutActions 2.5 KB eager sentinel fill): **architectural
  limit of forbid-bundle** (MaybeUninit / unsafe banned). Not
  "deferred" — not closable without relaxing forbid. Documented
  in-situ.
- **A026** (DispatchOutcome 320 B move per dispatch): **bench-gated
  by DEF-143**. Refactor cost is 60+ dispatch arm updates; needs
  measured evidence of gain before committing.
- **A013** (split SimpleQueryAwaitingRfq Dml/Select): **bundled
  with DEF-157** (ProtoState sum-of-subsums, already deferred to
  post-1c-4). Avoid duplicate work.

## 26. Phase γ — witness pattern (in progress)

Next architectural piece: **DEF-154 buffer-witness pattern** (big
refactor, subsumes DEF-141). Shipping incrementally:

### DEF-154 (A) — infallible builders via capacity witness (subsumes DEF-141)

**Goal.** Eliminate the `Result<NonEmptyRange, WriteBufFull>` return
shape on all 6 `build_*_message` functions. Replace with infallible
`NonEmptyRange` return guaranteed by type-level capacity proof.

**Mechanism.** Introduce `WriteReserved<'a>` token — constructible
only via `WriteBuf::reserve()` after `clear()` — guaranteeing
`MAX_OWNED_SEND_LEN` free capacity. Builders take `WriteReserved`
and return `NonEmptyRange` directly. Internal push methods on
`WriteReserved` shield the architecturally-dead overflow branch
via `debug_assert!` (release keeps the fallback, debug fires loud
on invariant break).

**Closed seams:**
- 6 dead `Err(WriteBufFull)` arms at compute_push_* call sites.
- `frame_build_unreachable` helper → DELETED.
- `CrateBugLocus::OutboundFrameBuild { stage }` variant → DELETED.
- `FrameBuildStage` enum → DELETED (only used by OutboundFrameBuild).
- Tier-3 const-assert-guarded dead Err paths → tier-2 structural
  (type-system-enforced capacity).

### DEF-154 (B) — NonEmptyRange lifetime binding

Scheduled after (A). Binds `NonEmptyRange<'buf>` to the specific
buffer's lifetime; `apply(buf)` becomes infallible via the
lifetime proving the buffer match. Closes A008, A009 arch side,
C015.

### DEF-154 (C) — ArenaReader / ArenaWriter witness tokens — SHIPPED

**Shipped** 2026-04-22. Chosen before (B) because (C) is self-
contained; (B) requires deeper lifetime design around generative
lifetimes (simple `&'buf` binding does not prove compile-time
buffer-identity).

**Goal.** Narrow the `&mut SchemaSlab` / `&SchemaSlab` borrows that
cross module boundaries (`dispatch(...)` / `materialise(...)`) to
witness types that expose only the method each actually uses.

**Mechanism.** Two thin wrappers on `SchemaSlab`:

- `ArenaReader<'r>(&'r SchemaSlab)` — exposes `get(SchemaRef) ->
  Option<&'r RowDesc>`. Nothing else. `Copy` for ergonomic value
  threading through sub-resolvers (`StagedReply::into_public`,
  `described_rows_ref_into_public`).
- `ArenaWriter<'a>(&'a mut SchemaSlab)` — exposes `alloc(RowDesc)
  -> Option<SchemaRef>`. Nothing else.

Constructed via `SchemaSlab::as_reader()` / `as_writer()`. Drift-
pinned at pointer size.

**Tier lift (C005).** The "dispatch only allocs; materialise only
reads" discipline was tier-3 code-review-enforced; post-(C) it is
tier-2 type-system-enforced. Dispatch cannot call `get` / `clear`
/ `free` because the type simply does not expose them; materialise
cannot call `alloc` / `clear` / `free` symmetrically.

**Closed seams:**
- Dispatch cannot accidentally read the arena (`get`) — closes a
  future-refactor drift surface.
- Dispatch cannot accidentally `clear()` mid-frame — closes a
  silent-correctness failure mode (clearing mid-query would
  invalidate the just-allocated ref that the new state variant
  carries).
- Materialise cannot accidentally `alloc()` — closes a future-
  refactor drift surface where a new StagedAction variant might
  need schema storage and be tempted to alloc in materialise.

**Tests added (4):**
- `writer_witness_alloc_matches_direct_alloc` — forwarding contract.
- `reader_witness_get_yields_live_desc` — reader forwarding.
- `reader_witness_stale_ref_returns_none` — generation-match preserved.
- `reader_witness_is_copy` — pins `Copy` derive against accidental removal.

**Along-the-way fix: DEF-181.** The pre-(C) release build was
silently broken: `SchemaSlab::occupied_count` was gated
`#[cfg(debug_assertions)]`, but its single caller `debug_assert_eq!`
expands to `if cfg!(debug_assertions) { assert_eq!(args) }` — the
`args` still type-check in release, and a cfg-gated-out method is
not in scope. Exposed when `(C)`'s verification ran `cargo build
--release` and errored. Fix: drop the cfg; LLVM DCEs the single
release-unused call. Same release-mode cost, compiles cleanly.

### DEF-154 (D) — stale-ref compile elimination

Scheduled after (B). Combines buffer-witness + arena-witness
(C shipped) to make stale SchemaRef detection compile-time (upgrade
DEF-170 debug_asserts from runtime-check to type-system-impossible).

### DEF-154 (B) Phase B4-W — SHIPPED

**Shipped** 2026-04-22. Scope: **write-side tier-1** (read-side
deferred to DEF-154 (E) — logical-cursor refactor).

**Mechanism.** Brand-lifetime generics threaded through
`StagedAction<'wb>`, `StagedActions<'wb>`, `DispatchOutcome<'wb>`,
all of `dispatch` + its sub-fns, all 6 builders + 5 `compute_push_*`
helpers, all 3 entry points (`push_command`, `push_bind_execute`,
`feed_bytes`). `materialise` signature becomes
`materialise<'w, 'r, 'wb>(StagedActions<'wb>, BrandedBytes<'wb, 'w>,
&'r [u8], ArenaReader<'r>) -> OutActions<'w, 'r>` — write-side
bytes are `BrandedBytes`; read-side stays unbranded `&[u8]`
(B4-W scope).

**Tier-1 wins** (DEF-182 shield elimination):
- Site 2 (`SendBytesRange(range).apply(write_buf)`) → **STRUCTURALLY CLOSED**.
  `WriteRange<'wb>::apply(BrandedBytes<'wb, 'w>) -> &'w [u8]` is
  infallible by the brand-identity + construction-bounds + API-narrow
  three-step argument.
- Site 1 (payload extraction) + Site 3 (`StreamRowRange.apply(read_buf)`):
  **retained** as DEF-182 tier-2 shields; lift to DEF-154 (E).

**Key design decisions** (from architect design spec):
- `_Phantom(PhantomData<fn(&'wb ()) -> &'wb ()>)` variant on
  `StagedAction` + `DispatchOutcome` to anchor `'wb` without
  phantom-per-variant noise. `#[doc(hidden)]`; never constructed.
  Match arms handle as neutral no-op (forbid-bundle bans
  `unreachable!`).
- `with_branded<'w, R, F>(&'w mut self, f: F)` — explicit `'w`
  propagation; elided `&mut self` would clip the brand's slice
  lifetime to the method-call reborrow scope (< `'w`).
- `into_bytes_branded(self) -> BrandedBytes<'brand, 'a>` — consuming
  form that yields the full outer `'a` lifetime. Required so
  `&'a [u8]` slices from `range.apply` escape the branded closure
  as `&'w [u8]` in `Action::SendBytes`.
- `BrandedBytes: Copy + Clone` — required for multi-iteration
  materialise loop (phantom + `&[u8]` = trivially Copy).
- SCRAM auth + `ParamsWriter` retain the `as_write_buf_mut`
  escape hatch. Migrating `scram::wire` across the module boundary
  produces zero tier win; branded `WriteRange<'wb>` wrapping at
  the enclosing scope preserves brand identity via
  `from_branded_write_span` post-push.
- Test observation via `StagedObs` (brand-free enum). Discovered
  mid-implementation: `ProtocolError` is `Copy + Clone`
  (error.rs:231), so `FailReply.cause` preserves full variant.
  `StagedObs::SendBytesRange` is a unit variant — tests only
  discriminate on kind, not range contents.
- Legacy `WriteReserved` + `WriteBuf::reserve()` + `NonEmptyRange::from_write_span`
  + `from_write_span_infallible` helper — DELETED (all paths go
  through branded form).
- `BrandedReadBuf` + `ReadRange::{new, apply, from_raw, inner}` +
  `BrandedBytes::{empty, from_slice_branded}` + `as_bytes_branded`
  — re-gated `#[cfg(test)]` pending DEF-154 (E).

**Verification:**
- `cargo check -p bsql-pg-proto --all-targets`: clean.
- `cargo clippy -p bsql-pg-proto --all-targets -- -D warnings`: clean.
- `cargo build -p bsql-pg-proto --release`: clean.
- `cargo test -p bsql-pg-proto`: 219 passed (unchanged from B3).

**Zero-cost perf.** Brand phantoms are ZST
(`PhantomData<fn(&'wb ()) -> &'wb ()>`); `with_branded` closures
are `#[inline]` and LLVM collapses to direct mutable-borrow pass.
Expected zero instruction-count delta vs pre-B4; systematic
verification deferred to DEF-143 (bench harness).

### DEF-154 (E) — read-side tier-1 via dispatch-loop logical cursor

Registered during Phase B4-W. The dispatch loop currently calls
`self.read_buf.advance(total_len)` mid-loop; a branded shared
borrow (`BrandedReadBuf`) conflicts with the mutating advance.
The fix is structural: convert physical `advance` to a logical
cursor (`frames_consumed: usize`) inside the branded scope,
apply the cumulative advance after the scope exits.

Complexity: ~+100 LoC in `feed_bytes`, interactions with
`ReadBufFull`-path + `Errored`-path early returns, plus the
`OutActions<'r>` borrow-checker constraint on when the advance
can legally run.

Scope gate: read-side tier-1 closure (DEF-182 sites 1 + 3) —
payload extraction + `StreamRowRange.apply` — becomes
structurally impossible via
`ReadRange<'rb>::apply(BrandedBytes<'rb, 'r>) -> &'r [u8]`.

Already-present scaffolding: `BrandedReadBuf` + `ReadBuf::with_branded`
(both `#[cfg(test)]`-gated in Phase B4-W), `ReadRange<'brand>::apply`
(cfg(test)).

### DEF-154 (B) Phase B4 — previous attempt (rolled back)

**Status.** Attempted 2026-04-22 same session as B1–B3 shipped.
Implementation ~500 LoC across action.rs / buf.rs / dispatch.rs /
protocol.rs / write_buf.rs. Reached "library compiles; tests don't"
state. Rolled back via `git stash` — stash message "Phase B4 attempt
— incomplete; revisit with architect redesign". Main branch remains
at Phase B3 scaffolding (219 tests, clean).

**Lessons learned (material for architect redesign):**

1. **Read-side branding requires a dispatch-loop logical cursor.**
   `feed_bytes` calls `self.read_buf.advance(total_len)` mid-loop;
   shared `BrandedReadBuf` borrow inside `with_branded` makes the
   `&mut` advance a borrow-checker conflict. The fix is to convert
   the physical advance to a logical cursor, applying the cumulative
   advance after the branded scope exits. This is a separate +100 LoC
   refactor. Scoping B4 to write-side-only + keeping read-side on
   DEF-182 tier-2 is the pragmatic split.

2. **`with_branded` must take `&'w mut self` (explicit `'w`), not
   `&mut self`.** The elided `&mut self` lifetime is ephemeral to
   the method call and shorter than the caller's `'w`. Slices
   derived from the branded buffer inside the closure cannot
   escape as `&'w [u8]` unless `'w` is explicitly propagated. Fix:
   sig = `fn with_branded<'w, R, F>(&'w mut self, f: F) -> R where
   F: for<'brand> FnOnce(BrandedWriteBuf<'brand, 'w>) -> R`.

3. **`into_bytes_branded(self) -> BrandedBytes<'brand, 'a>`
   needed alongside `as_bytes_branded(&self) -> BrandedBytes<'brand,
   '_>`.** The consuming form yields the full `'a` lifetime for
   materialise's unbranding boundary (where slices must be `&'w
   [u8]`); the borrowing form is kept for tests + multi-access in
   the same branded scope.

4. **`StagedAction` must gain a `<'wb>` brand parameter.** Every
   variant's construction site must thread the brand; `StagedActions`
   (alias) inherits it; `compute_push_*` all become
   `<'wb, 'rb>`-generic (or `<'wb>`-only for write-side-only B4).

5. **Dispatch + sub-functions + `DispatchOutcome` all become
   `<'wb>`-generic.** ~10 functions in dispatch.rs sign with `'wb`:
   `dispatch`, `errored`, `internal_bug`,
   `dispatch_auth_in_startup_{trust,scram}`,
   `dispatch_auth_sasl_{continue,final,ok_after_scram}`,
   `advance_to_{awaiting_rfq, bindexecute_awaiting_rfq_select,
   drain_after_error}`, `stream_row_or_errored`, plus
   `build_sasl_initial_response`. SCRAM auth builders migrate via
   the `as_write_buf_mut()` escape hatch (same pattern as
   `ParamsWriter` in `build_bind_message`).

6. **Test observation mechanism required — `StagedAction<'wb>`
   cannot leak out of its branded scope.** Options:
   - (a) `StagedObs` brand-free observation type that copies the
     visible data; requires `ProtocolError: Clone` or a kind-only
     discriminant for `FailReply`.
   - (b) Closure-based `compute_staged(cmd, state, |new_state,
     &staged| { /* assertions */ })` — ~20 test call sites
     rewrite.
   Option (b) is cleaner but larger surgery.

7. **`deliver<K>(id, payload) -> StagedAction<'_>` needs a
   phantom `'wb`** since none of its variants carry the brand but
   the enum does.

**Retry plan.**
- [ ] `architect` design pass covering (1)–(7) with concrete
      signatures before code work resumes.
- [ ] Accept write-side-only tier-1 scope for Phase B4; register a
      separate DEF-154 (E) for the dispatch-loop logical-cursor
      refactor that unblocks read-side tier-1.
- [ ] Pick test observation mechanism (a vs b) with architect
      guidance.
- [ ] Single atomic B4 commit once all pieces compile together.

**Stash preserved:**
`git stash list | grep "Phase B4 attempt"` recovers the
in-progress code. Don't try to un-stash onto main without the
design pass — the incremental path landed a half-migration that
the stashed work completed to a compiling lib but broken tests.

### DEF-182 — symmetric silent-fallback shields at NonEmptyRange.apply + payload extraction — SHIPPED

**Shipped** 2026-04-22 (same session as DEF-154 (C)).

**Context.** DEF-170 (audit2 A010) shielded 3 stale-SchemaRef
materialise sites with `debug_assert!(desc_opt.is_some())` before
their `unwrap_or(&RowDesc::EMPTY)` fallback — debug build fires
loud, release preserves the fallback (forbid-bundle bans `panic!`).

The pattern was asymmetrically applied — the `NonEmptyRange.apply`
and wire-payload `.get(HEADER_LEN..total_len)` call sites on the
SAME materialise/feed_bytes code paths retained unshielded
`unwrap_or(&[])` silent fallbacks. A drift discovered during the
DEF-154 (C) self-audit.

**Closed seams (3 sites):**

1. `protocol.rs:524` — payload extraction from `read_buf.unread()`.
   The preceding length-check + `parse_header` invariants prove
   the range `HEADER_LEN..total_len` valid. Unshielded
   `unwrap_or(&[])` would silently feed an empty payload to
   dispatch arms, misclassifying (empty DataRow → NoColumns, empty
   ErrorResponse → treated as OK).

2. `protocol.rs:1963` — `SendBytesRange(range).apply(write_buf)`.
   Range was constructor-validated at emission against THIS write
   buffer. Unshielded fallback would send a 0-byte frame where a
   multi-byte PG frame was required — malformed protocol
   downstream.

3. `protocol.rs:2010` — `StreamRowRange.row_range.apply(read_buf)`.
   Row range constructed from THIS read buffer this call; the `'r`
   borrow on OutActions blocks buffer mutation. Unshielded
   fallback would emit an empty row where a multi-column row was
   on the wire — user-boundary correctness break.

**Tier classification.** Tier-2 structural runtime. `debug_assert!`
panics in debug / tests (fires loud on the dead branch) and
compiles to nothing in release — the `unwrap_or(&[])` fallback
stays in release as the silent-but-typechecked closure. The
forbid-bundle's `panic` ban targets user-written `panic!` macros;
stdlib-internal panics via `debug_assert!` are allowed and
broadly used in the crate (DEF-170 is the prior precedent).
Full tier-1 compile-time closure of this class lands with
DEF-154 (B) buffer-witness-with-brand.

**Along-the-way fix.** Stale `has_any` references in `lib.rs`
comments (DEF-171 deleted the field but two size-bucket comments
still mentioned it). Corrected to reflect post-DEF-171 reality
(SchemaSlab ~520 B, PgProtocol ~6272 B DEF-119 baseline preserved).

### DEF-183 — Senior audit follow-up (P1-A/B/C + P2-A/B) — SHIPPED

**Shipped** 2026-04-22 (same session as DEF-154 (C) + DEF-182).

Independent `Senior` agent audit of the DEF-154 (C) + DEF-181 +
DEF-182 commits returned P0: Nothing material; P1: 3 findings;
P2: 2 actionable findings + 2 no-action. All 5 actionable items
shipped:

- **P1-A.** `push_bind_execute` at `protocol.rs:397` bypassed the
  DEF-154 (C) writer witness — `self.schema_arena.alloc(desc)`
  direct call asymmetric with dispatch's `ArenaWriter` routing.
  Fix: route via `self.schema_arena.as_writer().alloc(desc)`. Zero
  semantic change; closes the asymmetric drift surface where a
  future refactor that moves the alloc logic would inherit unfettered
  SchemaSlab access.
- **P1-B.** `reader_witness_is_copy` test pins Copy behaviourally
  (move-after-use produces a compile error) but not at the *trait*
  level. Fix: add `const _: fn() = || { const fn _assert_copy<T:
  Copy>() {} _assert_copy::<ArenaReader<'_>>(); };` — a trait-
  bound Copy assertion that fails with a clear error if a future
  refactor adds a non-Copy field.
- **P1-C.** `StagedReply::into_public` docstring called the stale-
  ref class "tier-3 crate bug = degraded diagnostic", but DEF-170
  already lifted the class to tier-2 via debug_assert shields 25
  lines below. Fix: rewrite the tier classification section to
  reflect post-DEF-170 reality (tier-2 debug/test, tier-4 release
  fallback, tier-1 compile closure scheduled in DEF-154 (D)).
- **P2-A.** DEF-181 cfg-fix docstring on `occupied_count` asserted
  LLVM DCE but didn't name the call site, making the argument
  fragile against future call-site additions. Fix: name the caller
  (`PgProtocol::clear_arena_if_idle_or_errored`) and add an
  explicit "Adding callers" advisory.
- **P2-B.** Drift-pin docstrings for `ArenaReader` /
  `ArenaWriter` sizes correct but terse. Fix: explain why `&SchemaSlab`
  is always thin (SchemaSlab is Sized; future `dyn Trait` conversion
  would trip the pin first).

No-action items: P2-C (DEF-182 site coverage complete — 3 hot-path
sites shielded; `dispatch.rs:1572` + `decode.rs:621` reachable-by-
design, not DEF-182 candidates) and P2-D (no codegen regression —
release build passes, wrappers are `#[inline]` + single-field).

### DEF-154 (F through V) — post-audit structural closure — SHIPPED

Series of 18 commits (DEF-154 F..V, commits `9674507`..`a2a7f42`)
closing all P0 silent-corruption findings identified by two
`architect` agent audits + user-reported issues. Full breakdown:

- **(F)** P0-1 use-after-clear on staged StreamRowRange after fatal
  path clear — 9-byte silent row corruption reproduced on
  `DataRow(valid) → CommandComplete(malformed)` sequence. Fix: remove
  `clear_scope_local` from in-scope fatal path; defer buffer clear
  to next `feed_bytes` via `is_errored` fast-path.
- **(G)** u16 narrowing silent-clamp — `AbsFrameStart::new(v_usize).unwrap_or(u16::MAX)`
  routed through classified `FrameTooLarge` at `parse_header` via
  `HeaderParse::Ok.total_len: u16`. No silent narrowing on ingress.
- **(H)** read-side brand `'rb` + `ReadRange<'brand>` + `BrandedReadBuf`
  entirely deleted (−585 LoC net). `StagedAction::StreamRowRange`
  carries `row_bytes: &'r [u8]` directly → tier-1 identity apply.
  Deferred-advance via `PgProtocol.pending_advance` to resolve the
  stage-time-borrow vs advance-time-mut conflict.
- **(I)** `ProtocolError::state_kind()` changed from `Option<StateErrorKind>`
  to total `StateErrorKind` via `from_kind_or_internal`. Deleted
  `INTERNAL_FALLBACK` const + 3 `debug_assert!(false, ...) +
  unwrap_or_else()` dead-branch shields.
- **(J)** Stale-SchemaRef silent fallback at materialise (`RowDesc::EMPTY` /
  `DescribedRows::NoData`) classified via `StagedReply::into_public`
  returning `Result<Reply<'r>, StaleSchemaRef>`; materialise emits
  `FailReply + CloseSocket` on Err instead of silent degraded payload.
- **(K)** User-reported double-panic in
  `zero_body_data_row_classified_as_malformed_data_row`: root causes
  were (a) DEF-154 (H) refactor lost the non-empty DataRow body
  check — `populated.get(5..5)` returns `Some(&[])` not `None`,
  routing to `StreamRow { row_bytes: &[] }` silently; (b)
  `ReplyId::drop` asserted delivery with `#[cfg(test)]` guard that
  fails to activate in integration-test crates → double-panic
  SIGABRT. Fixed by adding `start >= end` check in
  `stream_row_or_errored` + deleting the panic-in-Drop entirely
  (discipline enforced via `#[must_use]` + integration-test
  observation).
- **(L)** P0-1 materialise overflow via stale-ref fan-out: 6
  `.unwrap_or(())` sites on `out.push()` could silently drop
  terminal actions. Fix: split `MAX_ACTIONS_PER_CALL` into
  `MAX_STAGED_PER_CALL = 8` (dispatch-side) and
  `MAX_ACTIONS_PER_CALL = MAX_STAGED_PER_CALL * MAX_FANOUT_PER_STAGED = 16`
  (output-side), const-asserted. Materialise uses
  `push_within_fanout_budget` helper with explicit documented-dead
  Err arm. Also: 2× quick-win on SELECT-large (15 rows/call vs 7).
- **(M)** P0-3 `BrandedWriteReserved::push_*` 7 methods: silent
  `WriteBufFull` discard → `Result<(), WriteBufFull>` propagation
  through builders + `From<WriteBufFull> for ProtocolError` →
  `CrateBugLocus::BuilderCapacityOverflow` classification. Pre-(M)
  was bit-junk on wire with correct-looking length prefix on
  capacity drift.
- **(N)** P0-4 `WriteRange::apply` signature: `-> &[u8]` with
  debug_assert+unwrap_or silent fallback → `-> Option<&[u8]>`;
  materialise SendBytesRange arm routes None to `CloseSocket`
  emission (not silent empty SendBytes).
- **(O)** P1-5 `MAX_PASSWORD_LEN` 1024 → 512 B. `Password`/
  `Credentials::ScramPassword`/`ProtoState` SCRAM path shrink by
  512 B; `PgProtocol` 6272 B → 5760 B per instance. Lint
  suppression kept (Box forbidden by no_alloc; 514 B variant still
  exceeds clippy's 200 B threshold).
- **(P)** P0-5 `PgProtocol::unread()` `.unwrap_or(&[])` → explicit
  `split_at_checked` match. P0-6 `StagedObs::from_staged` variant
  collapse `StreamRowRange → CloseSocket` → distinct
  `StagedObs::StreamRowRangeUnexpected` sentinel (test regression
  signal).
- **(Q)** P1-6 `emit_actions!(..., on_overflow: break, ...)` form
  deleted. Dispatch gate already reserves slots; `break` on
  overflow could silently drop terminal Errored-arm FailReply +
  CloseSocket. Single infallible form remains with documented-
  dead Err arm.
- **(R)** P1-3 `rust,ignore` doc-tests: 1 converted to compile-
  checked (`decode.rs` FromPgText example); 4 reclassified to
  `text` prose (write_buf × 2, wire, ident — named crate-internal
  types not pub, or pseudo-code patterns).
- **(S)** P1-1 5 unshielded `unwrap_or(&[])` accessors:
  `OtherEncoding::as_bytes`, `Password::as_bytes`, `PodBytes::as_slice`,
  `ParamOids::oids`, + `DataRowRef::columns` → explicit
  `split_at_checked` match with documented-dead None arm.
- **(T)** P1-2 5 `u16::try_from(src.len()).unwrap_or(0)` narrowings
  (ident.rs × 4 + session_params.rs × 1): introduced
  `ident::narrow_len_u16(value, cap)` helper — Err arm saturates
  to `cap` (not `0`), surfacing "full buffer" on invariant break
  rather than "silently empty".
- **(U)** P2/P3 `SqlStateCode::as_str` `from_utf8(..).unwrap_or("")`
  → `if let Ok` form (escapes `clippy::manual_unwrap_or_default`
  which would push us back to the banned pattern). `DataRowRef`
  stores `body_after_count: &'a [u8]` directly (stripped at
  `parse` via `split_first_chunk::<2>()`) → `columns()` is tier-1
  identity field load.
- **(V)** Second-audit quick wins: `FrameCoords` drift-pin
  tightened `<= 8` → `== 4` (exact layout tier-1 compile);
  `pending_advance: u16` → `Option<NonZeroU16>` (niche-same-size +
  type-enforced "no pending" sentinel); round-trip compile-pins
  for `TxStatus`/`AuthSubCode`/`FormatCode`/`Severity` classifier
  pairs (body-swap drift caught at build time via
  `const _: () = { assert!(matches!(try_from(v.as_()), Ok(v))); }`).

Post-DEF-154 (V): 213 tests pass; all P0 silent-corruption classes
closed; all `unwrap_or(&[])` / `unwrap_or(0)` / `unwrap_or("")` /
`unwrap_or_else` production sites either deleted, restructured,
or converted to explicit-match form with documented-dead Err arm.
No debug_assert + silent-fallback pairs remain in production code.
No panic-in-Drop. Every classifier has round-trip compile pin.

### DEF-154 (W) — write-brand scaffolding deletion — IN PROGRESS

**Trigger.** Architect second-pass audit (post DEF-154 V) found:
DEF-154 (N) reverted `WriteRange::apply` to return
`Option<&[u8]>`. The write-brand's *only* load-bearing property
was "tier-1 infallible apply under buffer-identity proof" — that
property was silently deleted by (N). Post-(N) the brand remains
a 300+ LoC scaffolding (`WriteRange<'brand>`, `BrandedBytes<'brand, 'a>`,
`BrandedWriteBuf<'brand, 'a>`, `BrandedWriteReserved<'brand, 'a>`,
`with_branded` HRTB, 3× PhantomData<fn(&'brand ()) -> &'brand ()>`
invariance) that produces ZERO tier-1 guarantee. Classified by
the architect as "decorative tier-1" — fake compile check,
exactly what user banned via directive "эта проверка при
компиляции не была фейковой и про стеклянную архитектуру".

**Design (architect Option A).** Delete the brand phantom
across all 4 types (keep wrapper names for API-narrow tier-2:
prevents builders from calling `.clear()` / truncating ops mid-
scope). Strip `'brand` generic from ~188 occurrences across
5 files (write_buf.rs, action.rs, dispatch.rs, protocol.rs,
buf.rs). Delete `BrandedBytes` as a struct (replace with `&[u8]`
in materialise + apply signatures — the type is a wrapper over
`&[u8]` with phantom; without phantom it's pure aliasing
overhead). HRTB `for<'brand> FnOnce(...)` closures collapse to
plain `FnOnce(...)`.

**Tier impact.** Before: decorative tier-1 (claimed infallible
apply but delivers Option). After: honest tier-2 structural
(API-narrow on wrapper types prevents wrong-method calls;
classified runtime error on apply mismatch via the Option return
+ materialise's CloseSocket emission). Net: no safety regression;
removal of fake compile check; ~300-500 LoC deletion.

**Scope.** 188 `'brand` / `'wb` occurrences; 5 files; 500+ LoC
of doc + type signatures + phantom-field removal. Wholesale sed
is risky (a previous attempt this session created syntax artefacts
via regex greed); pointwise Edit is the correct approach.

**Verification.** 213 tests must still pass after the refactor.
`cargo clippy -D warnings` clean. No public API change (all brand
scaffolding is `pub(crate)`).

### DEF-154 (X) — pull-based RowStream API — SHIPPED (2026-04-22)

**Foundation: DEF-154 (W) SHIPPED** — write-brand scaffolding
deleted (commit `0236046`), −194 LoC. RowStream shipped atop the
simplified post-W type surface.

**What shipped (final design).**

Public surface:
```rust
pub struct RowStream<'p, 'w> { /* &mut PgProtocol + &mut WriteBuf */ }
pub enum StreamItem<'a> {
    Row { id: NonZeroU64, row_bytes: &'a [u8], desc: &'a RowDesc },
    Complete { id: NonZeroU64, value: Reply<'a> },
    SendBytes(&'a [u8]),
    FailReply { id: NonZeroU64, cause: ProtocolError },
    CloseSocket,
    NeedMore,
}
impl RowStream<'_, '_> {
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull>;
    pub fn next_event(&mut self) -> StreamItem<'_>;
}
impl PgProtocol {
    pub fn iter_rows<'p, 'w>(&'p mut self, wb: &'w mut WriteBuf)
        -> RowStream<'p, 'w>;
}
```

Dual-API: `iter_rows` is ADDITIVE; `feed_bytes` retained for
control-frame paths (startup/bind/describe). No public API break.

**Architecture: fast/slow path split with frame-bounded dispatch.**

1. **Fast path (the hot one).** `next_event` peeks the header at
   the read-buf cursor; if it's a DataRow AND the state is
   row-streaming, we extract the row body via direct slice math
   (cursor + HEADER_LEN..cursor + total_len) and emit
   `StreamItem::Row` with `row_bytes: &[u8]` aliasing the read
   buffer. **Zero `OutActions` allocation per row.**
2. **Slow path.** For T / C / Z / E frames (or DataRow outside
   a streaming state), we call the new
   `pub(crate) feed_bytes_bounded(bytes, wb, max_dispatches: u16)`
   with `max_dispatches = 1` — process exactly ONE actionable
   frame and return. Silent pre-dispatch skips
   (ParameterStatus / NoticeResponse) don't count against the
   budget.
3. **Terminal flush.** When slow-path emits a terminal action
   (DeliverReply / FailReply / CloseSocket), `flush_pending =
   true`. The NEXT `next_event` runs an unbounded `feed_bytes`
   to consume the trailing `Z` silent frame, then drains — so
   subsequent `push_command` finds `state = Idle` without
   requiring the caller to invoke `feed_bytes` manually.
4. **Pending-advance.** `feed_bytes_bounded` records a deferred
   cursor advance (DEF-154 (H) mechanism); `next_event` applies
   it at entry before any header peek, so fast and slow paths
   see the physical cursor in sync with the logical one.

**Files touched.**

- `src/row_stream.rs` (new, 354 LoC) — RowStream + StreamItem +
  action_to_stream_item mapping + full module docs (perf
  rationale + API + MVP-scope note).
- `src/protocol.rs` — `feed_bytes` becomes a 3-line wrapper
  around new `feed_bytes_bounded(bytes, wb, u16::MAX)`; inner
  loop gains `dispatches_this_call: u16` counter + entry-time
  budget gate. RowStream-support helpers added:
  `read_buf_append`, `read_buf_populated`, `read_buf_cursor_u16`,
  `read_buf_advance`, `schema_arena_reader`,
  `streaming_reply_id_and_schema`, `apply_pending_advance`,
  `state_is_errored`, `install_errored_malformed_data_row`,
  `iter_rows`.
- `src/lib.rs` — `pub mod row_stream;` + `pub use
  row_stream::{RowStream, StreamItem};`.
- `tests/row_stream_spec.rs` (new, 412 LoC) — six behavioural
  tests covering: multi-row happy path, drained-after-Complete
  NeedMore cascade, errored-state CloseSocket-once semantics,
  fast-path malformed-body MalformedDataRow, feed overflow
  returning tiny `ReadBufFull` (not ProtocolError — 4 B vs 300 B
  hot-path happy return), server ErrorResponse → FailReply via
  slow path.

**Perf rationale (per architect's (X) plan).** Pre-(X) every
`feed_bytes` call paid a 5008-byte zero-fill for
`OutActions`'s `[Action; MAX_ACTIONS_PER_CALL]` storage (no-
unsafe constraint forces the init). On 1M-row SELECT: ~130k
calls × 5 KB ≈ 650 MB of stack-zero traffic just to surface
rows. RowStream fast-path emits rows without touching
OutActions; 1M rows → 0 OutActions inits on the hot path. Only
T (1) and C Z terminal flush (1) hit slow-path → 2 OutActions
inits per query. **~300× reduction in stack bandwidth** on
the dominant hot path; architect's projection: **10-100×
end-to-end throughput** bounded by per-row decode work.

**Verification.**

- `cargo test -p bsql-pg-proto`: 218 tests pass (212 existing
  + 6 new row_stream_spec behavioural tests). Debug AND
  release.
- `cargo clippy --workspace --all-targets -D warnings`: clean.
- `cargo check --workspace --all-targets`: clean.
- Six bad-path tests in `row_stream_spec.rs` close the shield
  seams that would otherwise admit silent drops (drained
  double-delivery, post-terminal Z leftover, empty-body
  malformed row, read-buf overflow).

**What's intentionally NOT done here.**

- **Criterion benchmark harness** — the 300× reduction claim
  is predicted from the architecture (OutActions stack size ×
  call count), not measured in this session. Deferred to
  DEF-143 (cargo-bench harness, own session).
- **Pipelining (multiple concurrent queries on one RowStream)**
  — MVP supports one reply per `iter_rows` scope. Post-1c-5
  per-correlator stream objects address pipelining (separate
  phase).
- **`Action::StreamRow` deletion** — still used by the
  `feed_bytes` legacy path for callers that haven't migrated;
  retained for compat. Candidate for removal once
  `bsql-driver-postgres` fully migrates to `iter_rows`.
  **[CLOSED in DEF-154 (Y) — see below.]**

**Lessons from three failed mid-session attempts (pre-shipped).**
Earlier attempts hit Rust's NLL "conditional reborrow" limitation:
`slow_path_once` originally tried to recurse via
`self.next_event()` on NeedMore, which extended the returned
Action's borrow across the recursion's `&mut self.proto` and
failed E0499. Final design adopts a **caller-loop** pattern:
slow_path emits the first action OR NeedMore, caller re-enters
`next_event` until terminal — no recursion, no lifetime conflict.
One extra loop iter per silent transition (rare: 1 per query in
SELECT flow). Net wash.

### DEF-154 (Y) — `Action::StreamRow` full deletion — SHIPPED (2026-04-23)

**What shipped.** Complete deletion of the pre-`iter_rows` row
emission code path. Post-(Y), `feed_bytes` is strictly the
**control-path** API (startup / bind / describe / push-command
responses + DML `C Z`); all row-bearing responses MUST flow
through `iter_rows` → `StreamItem::Row`.

**Source-level deletions.**
- `Action::StreamRow { id, row_bytes, desc }` variant (~60 LoC
  + doc) from `action.rs`.
- `StagedAction::StreamRowRange { id, row_bytes, schema_ref }`
  variant (~25 LoC + doc) from `action.rs`. The `'r` lifetime
  on `StagedAction<'r>` / `StagedActions<'r>` /
  `DispatchOutcome<'r>` was the only consumer of `'r`; all
  three types become lifetime-free.
- `StagedObs::StreamRowRangeUnexpected` variant + its
  `from_staged` arm (~30 LoC) in `protocol.rs` tests.
- `stream_row_or_errored` helper (~60 LoC) in `dispatch.rs`.
- Three `TAG_DATA_ROW` dispatch arms (SimpleQueryStreamingRows,
  BindExecuteAwaitingDataOrCompleteSelect, BindExecuteStreamingRows)
  that constructed `StreamRowRange`; DataRow via `feed_bytes`
  now falls through the catch-all `other` arm → `UnexpectedFrame
  { tag: DataRow }`.
- `FrameCoords`, `AbsFrameStart`, `FrameTotalLen` newtypes +
  drift pin (~150 LoC) in `dispatch.rs`. These existed ONLY to
  pass frame coordinates into `stream_row_or_errored`; post-(Y)
  `dispatch()` no longer needs them. Its signature loses
  `coords: FrameCoords` and `populated: &'r [u8]` params.
- `materialise` fanout branch for `StreamRowRange` (~40 LoC) in
  `protocol.rs`. Only the `DeliverReply` stale-ref fanout
  remains — still reason for `MAX_FANOUT_PER_STAGED = 2`.
- The `'r` lifetime in several helper functions
  (`errored<'r>`, `internal_bug<'r>`, 8 dispatch_auth_* helpers,
  `compute_push_bind_execute<'rb>`) — all become bare fn.

**Test-level changes.** `Action::StreamRow` pattern matches
existed in 7 tests in `simple_query_spec.rs`:
- DELETED (redundant / obsolete): `select_multiple_rows_stream_then_deliver`,
  `zero_body_data_row_classified_as_malformed_data_row` (both
  covered by `row_stream_spec` equivalents),
  `overflow_backpressure_preserves_delivery_across_calls`
  (obsolete — `iter_rows` pulls one event per call, no output
  array overflow possible).
- MIGRATED to `row_stream_spec.rs` with `StreamItem::Row`
  pattern:
  - `rows_before_mid_stream_error_are_preserved` (from
    `error_after_some_rows_emits_stream_then_fail`).
  - `rows_preserved_when_command_complete_malformed` (from
    `data_row_then_malformed_command_complete_preserves_row_bytes`).
  - `rows_across_multiple_feed_calls` (from
    `rows_across_multiple_feed_bytes_calls`).
  - `row_bytes_decode_via_data_row_ref` (from
    `stream_row_bytes_decode_via_data_row_ref`).
  - `end_to_end_decode_typed_row` (from same name).
- `dml_after_select_clears_row_desc` retained — Q1 (SELECT
  with rows) converted to `iter_rows`; Q2 (DML) still uses
  `feed_bytes`.

**Doc-tests.** `decode::FromPgText` doc-test migrated from
`Action::StreamRow` to `StreamItem::Row` — compile-checked in
CI so future API drift fails at build.

**Why this is a real simplification, not a shuffle.**
- `'r` lifetime cascade stripped from `StagedAction`,
  `StagedActions`, `DispatchOutcome`, `dispatch()`, ~10 helper
  fns. The read-buf lifetime threading was a massive complexity
  source — post-(Y) it exists only in `Action<'w, 'r>` for
  `DeliverReply`'s `Reply<'r>` payload (arena refs).
- `materialise` loses its "early-if-let for fanout" branch —
  the remaining loop is a single clean `match sa { ... }`.
- `dispatch()` signature shrinks from 7 params to 5.
- `FrameCoords` + its two newtype arguments + the tier-1
  size drift-pin: 150 LoC of plumbing vanished. The typed-
  argument anti-swap invariant was shield for `stream_row_or_errored`'s
  coordinate handling; post-(Y) there's no coordinate handling.

**Verification.**
- `cargo test -p bsql-pg-proto`: 215 tests pass (218 pre-(Y)
  − 3 net: deleted 7 feed_bytes row tests, added 5 iter_rows
  equivalents + kept dml_after_select as hybrid).
- `cargo clippy --workspace --all-targets -D warnings`: clean.
- `cargo check --workspace --all-targets`: clean.
- Release build: clean.

**API contract change.** `feed_bytes` callers who send a row-
bearing query (SELECT, BindExecute with result rows) now get
`Action::FailReply { cause: UnexpectedFrame { tag: DataRow } }`
on the first DataRow — classified as an API misuse, not a
protocol desync. Callers MUST switch to `iter_rows`. In the
`sasql` workspace this affects only tests (migrated as above);
no external consumers.

**Deferred to separate pass.** Many doc comments still
reference `Action::StreamRow` / `StagedAction::StreamRowRange`
as historical context (e.g. in `schema_arena.rs`, `buf.rs`,
`reply_id.rs`). These don't block compile but read as stale to
a reader looking up current behaviour. A documentation sweep
to annotate these with "DEF-154 (Y): deleted — see row_stream"
or rewrite them entirely is a future polish pass.


### DEF-184 — post-(Y) двойной audit merge (A/B/C full catalog) — OPEN

**Статус:** каталог находок из 2-х architect-audit'ов pg-proto
после shipping (Y). Готов к батчевому исполнению. **Ничего не
отбрасывается** (CREDO §5).

**Источники:** audit #1 (без CREDO-context) и audit #2 (с CREDO
на руках, task: falsify audit #1 DONE + найти blind spots +
предложить crazy architectural ideas).

**Метрика:**
- 14 "net-new" A-findings + 6 A-reconfirmed-DONE (из audit #1).
- 29 B-findings (из audit #2): 3 falsified A "DONE", 2 false-positive,
  1 CONFIRMED DONE, 23 net-new.
- 6 C-crazy (из audit #2).
- Post-dedup: ~32 уникальных actionable item'а.
- **Null-result areas** (11): Cargo profile (уже lto-fat/cu1/opt3/
  panic=abort/strip=symbols); `#![forbid(unsafe_code)]` compliance;
  MSRV 1.95 + Edition 2024; tag distinctness invariants (wire.rs);
  `#[must_use]` propagation; endianness/alignment safety; frame
  parser purity; tests 215; `debug_assert!` sites (кроме B24/B25);
  `PgProtocol: !Sync` gate; cross-trait OID symmetry.

**Dedup-map:**
- A2 ≡ B1 ≡ B8 — `OutActions` POD → `heapless::Vec<Action, N>`.
- A4 ≡ B16 — cache-line-aware `PgProtocol` layout.
- A5 ≡ B10 — branchless `ColumnsIter` decode.
- A6 ≡ B13 — ASCII-int parser (no `from_utf8`).
- A10 ≡ B22 — SCRAM hot/cold split (extract SCRAM from `ProtoState`).
- A19 ≡ B6 — `feed_bytes_bounded(max)` gate dead overhead на
  production path.
- A20 ⊂ B1 + B3 — materialise `push_within_fanout_budget` cost.
- B21 ≡ C6 — `dispatch()` by-val 712 B memcpy per frame.
- A11 ⊂ C4 — SIMD column batch decode.

---

#### A-series findings (audit #1)

- **A1 — `ProtocolError` shrink через `ErrorArena`** — P0.
  - Files: `src/error.rs:346-361` (ServerErrorResponse variant),
    `src/action.rs:780-840` (Action.cause), `src/lib.rs:274-337`
    (size asserts), `src/row_stream.rs:103-139` (StreamItem cascade).
  - Current: `ProtocolError` cap 312 B, dominated by
    `ServerErrorResponse` с 5 in-place `BoundedStr<N>` = 288 B.
    Каскад: `Action::FailReply.cause` → `Action` 312 B →
    `OutActions = [Action; 16]` → **5008 B zero-fill/вызов**.
  - Proposed: move 5 bounded strings из `ServerErrorResponse` в
    `ErrorArena` (bounded slab + generational ref, тот же паттерн
    что `SchemaArena`). `ServerErrorResponse` становится 8-10 B
    (severity + code + ErrorRef).
  - Cascade: `ProtocolError` 312→~32 B; `Action` 312→~48 B;
    `OutActions` 5008→~784 B (**6.4× slow-path reduction**);
    `StreamItem` 320→~80 B (**4× per-row pull saving**).
  - Tier: tier-2 structural (arena ownership invariant, audited
    как в schema arena; `None` на stale-ref → `StaleErrorRef`
    InternalCrateBug).
  - LoC: 500-700. Risk: HIGH (user-facing через wrapper).
  - Depends on: A2 preferably shipped первым (проверяет паттерн
    `heapless::Vec<Action>`). Cascades в A13, A15.

- **A2 / B1 / B8 — `OutActions` POD → `heapless::Vec`** — P0.
  - File: `src/action.rs:584-715`.
  - Current: `OutActions.items: [Action; 16]` eagerly
    `Action::CloseSocket`-filled = **5008 B zero-fill/вызов
    feed_bytes/push_command/iter_rows.slow_path_once**.
  - Proposed: swap на `heapless::Vec<Action<'w,'r>, 16>`. Sibling
    `StagedActions` (action.rs:750) — **уже** `heapless::Vec`, Drop
    no-op для Copy payload, LLVM elides. "POD для NLL" в
    action.rs:559-583 — не актуально пост-(X) (каллеры
    `as_slice()` consume at one site).
  - Win: 5008 B → 32 B init/call = **~150× per-call reduction**.
    Комбинируется с A1 (cascade) до ~144 B.
  - Tier: tier-2 structural (capacity bound via `heapless`).
  - LoC: 40. Risk: LOW-MEDIUM.
  - Depends on: none standalone.

- **A3 — two-stage fn-ptr LUT dispatch** — P1.
  - File: `src/dispatch.rs:143-858` (~85-arm `match (state, tag)`).
  - Current: linear match chain → LLVM jump-table + nested chain,
    ~3-5 compares per frame.
  - Proposed: двухуровневая LUT. (1) `ProtoState → StateClass: u8`
    via `repr(u8)` discriminant + const LUT. (2)
    `const [[Option<fn>; 256]; N_STATES]` на `(state_class, tag)`.
    Hot path: `LUT[state.class()][tag.byte()]` → 1 load + 1 indirect.
  - Win: 5-15 ns/frame. 10-30 ms на 1M queries.
  - Tier: tier-1 exhaustiveness preserved via const construction
    с const-assert per state.
  - LoC: 300-400. Risk: HIGH (fn-ptr tables, const-fn generic
    lifetime erasure, coverage-complete LUT construction).
  - Depends on: A10/B22 (state shape shrinks → smaller LUT).
    Bench-driven decision to ship или skip.

- **A4 / B16 — cache-line-aware `PgProtocol` layout** — P1.
  - File: `src/protocol.rs:294-345` (struct layout).
  - Current: `state: 712 B / read_buf: 4098 B / session_params:
    420 B / schema_arena: 520 B`. Dispatch hot-loop touches `state`
    + `read_buf.cursor` — ~712 B apart, 2-4 L1 fetches.
  - Proposed: `#[repr(C)]` + explicit pad. Cache line 0 (64 B): hot
    dispatch state (`state_hot`, `pending_advance`, cursor cache).
    Cache line 1+: read_buf. Cold tail: arena, session_params,
    SCRAM state (per A10).
  - Win: 1 L1 fetch для whole dispatch-loop hot state. 3-5 ns/iter.
    3-5 ms на 1M rows.
  - Tier: tier-2 layout assertion + `mem::offset_of!` cross-platform
    pin.
  - LoC: 200-300. Risk: MEDIUM.
  - Depends on: A10/B22 (SCRAM extraction) — hot state должен быть
    мал чтоб влезть в 64 B cache line.

- **A5 / B10 — branchless ColumnsIter decode** — P1.
  - File: `src/decode.rs:705-768`.
  - Current: `ColumnsIter::next`: `split_first_chunk::<4>` (branch)
    + `from_be_bytes` + `if len == -1` (branch) + `if len < 0`
    (branch) + `usize::try_from(len)` (branch) + `split_at_checked`
    (branch). 5+ branches × 32 cols × 1M rows = 160M+ branches.
  - Proposed: `len_plus_one = len_i32.wrapping_add(1) as u32`.
    `len == -1 ⇔ plus_one == 0 = NULL`. `len < -1` → отдельный
    guard. `plus_one > 0` → data of length `plus_one - 1`.
    Collapse до 2 branches (null-check + truncation).
  - Win: ~2.5× per-column decode. 40-80 ns/row saved on 1M rows.
  - Tier: tier-3 preserved (same error classes).
  - LoC: 120. Risk: LOW.

- **A6 / B13 — ASCII-int parser без `from_utf8`** — P1.
  - File: `src/decode.rs:858-896` (`impl_from_pg_text_int!` macro).
  - Current: `str::parse::<T>()` preceded by `core::str::from_utf8`
    (full SSE2 walk) then `parse` re-walks with digit validation.
    Double-walk of 8-byte int cost ~16 ns.
  - Proposed: dedicated ASCII-digit int parser (20 LoC). Handle
    optional sign byte, branchless digit accumulator, checked
    arithmetic. PG text-int grammar = `[-+0-9]+` strict.
  - Win: ~2× int-heavy text-format rows.
  - Tier: tier-3 preserved (same `IntParse` error variant).
  - LoC: 80. Risk: LOW.

- **A7 — const LUT `[Option<InboundTagClass>; 256]` на tag byte** — P1.
  - File: `src/wire.rs:121-245`, `src/dispatch.rs` entry.
  - Current: `dispatch()`'s inner match descends через ~12-arm
    `tag == TAG_X` compare chain per state.
  - Proposed: `const INBOUND_TAG_LUT: [Option<InboundTagClass>; 256]
    = build()`. Dispatch: `match LUT[tag.byte() as usize]` → known
    class или `None` (UnexpectedFrame fallthrough). Tag distinctness
    уже const-asserted via `assert_all_distinct!` macro.
  - Win: 8-15 ns/frame.
  - Tier: tier-1 preserved, coverage const-asserted.
  - LoC: 150. Risk: LOW.

- **A8 — `usize → u16/u8` narrowing continuation** — P2.
  - Files: `src/dispatch.rs` intermediates, `src/decode.rs`
    `parse_row_description` (`n_columns_usize` хотя `≤ 32`),
    `src/action.rs:1521-1540` (`ParamOids.n_params` — already u16,
    good), `src/params.rs:153`.
  - Current: остаточные `usize` locals на per-row/per-column hot
    paths где ≤ u8 или ≤ u16 suffice.
  - Proposed: audit all `usize` hot-site locals, swap на `u8`/`u16`
    там где const-capped. Explicit `usize::from(..)` at slice
    indices.
  - Win: 4-6 B / stack frame × recursion depth. Cumulative.
  - Tier: tier-2 const-asserted caps.
  - LoC: 100. Risk: LOW.

- **A9 — const-fn classifiers** — RECLASSIFIED → B2.
  - См. B2.

- **A10 / B22 — SCRAM hot/cold split в `ProtoState`** — P1 BIG.
  - File: `src/state.rs:78-501`.
  - Current: `ProtoState` 712 B потому что
    `ConnectingScramAwaitingServerFirst` carries
    `PodBytes<MAX_CLIENT_FIRST_BARE_LEN>` + `PodBytes<MAX_CLIENT_NONCE_B64_LEN>`
    + `ScramSession`. Every `mem::take(state)` в `feed_bytes` →
    **712 B memcpy per dispatch iteration**.
  - Proposed: `ProtoStateHot` (max 48 B: все post-auth варианты +
    ZST `ScramCold` маркер для SCRAM). Фактический SCRAM state в
    `PgProtocol.scram_session: Option<ScramRef>` через
    `ScramSlab`.
  - Win: `mem::take(state)` 712 → 48 B. На 1M-row SELECT: 1M × 664
    B = **~664 MB memcpy bandwidth eliminated**. ~10-15× reduction
    на state-move cost.
  - Tier: tier-1 exhaustiveness сохраняется через exhaustive match.
    SCRAM-slot lifecycle → tier-2 (arena discipline).
  - LoC: 400-600. Risk: HIGH (state machine restructure).
  - Depends on: B1/A2 shipped (проверен паттерн `heapless::Vec`).
    Enables A3 и A4/B16.

- **A11 / C4 — SIMD / SWAR `ColumnsBatch<N>` decode** — P2 speculative.
  - File: `src/decode.rs:705-770` + new hot path.
  - Current: per-column `from_pg_text/binary` dispatch via
    vtable-style `ColumnDesc::format_code` match.
  - Proposed: для binary-format fixed-size int rows — pre-scan
    `[len; n] + [offset; n]` SoA arrays, затем `std::simd` gather
    до 8 column values/load. Или (MSRV 1.95): SWAR через u64
    arithmetic (stable).
  - Win: 3-10× on fixed-width binary int rows (analytics default).
  - Tier: tier-2 structural.
  - LoC: 800+. Risk: HIGH.
  - **Depends on: DEF-143 criterion bench baseline.** Ship AFTER
    measurement proves it's actually winning.

- **A12 — schema arena 2-slot → pipelining sizing** — P2 deferred.
  - File: `src/schema_arena.rs:126`, `src/lib.rs:317-324`.
  - Current: `MAX_ARENA_SLOTS = 2`. 1c-5 pipelining потребует ≥ 4.
  - Proposed: `#[cfg(feature = "pipelining")]` 8-slot variant.
    Cost: +264 B × 6 slots = ~1.6 KB на PgProtocol (6 KB → 7.6 KB).
  - LoC: 20. Risk: LOW.
  - **Deferred до 1c-5** — сейчас нет потребителей. Planning-only
    note в deferred (этом самом).

- **A13 — `StreamItem` shrink cascade** — P0 (auto from A1).
  - File: `src/row_stream.rs:107-135`.
  - Current: `StreamItem::FailReply.cause: ProtocolError` drives
    `StreamItem ≥ 312 B`. Every `next_event` returns this by value.
  - Proposed: automatic после A1 — `StreamItem` 320 → ~80 B.
  - Win: 240 B/call × 1M rows = 240 MB stack traffic saved.
  - LoC: cascades from A1. Risk: cascades.
  - Depends on: A1.

- **A14 — const-template Bind+Execute+Sync bundle** — P1.
  - File: `src/protocol.rs:1837-2150` (`build_bind_message`,
    `compute_push_bind_execute`).
  - Current: byte-by-byte `push_*` calls в `WriteBuf`, длина
    patch'ится в конце. Три frames (Bind + Execute + Sync) builds
    последовательно.
  - Proposed: const-template fixed-shape prefix
    (tag 'B' + len placeholder + NUL portal + NUL stmt +
    `n_param_formats=0u16_be` + `n_params=<runtime>u16_be`). Bulk
    `push_bytes(&prefix_template)` + patch только 2 байта n_params
    + 4 байта length. Scylla-rust-driver pattern.
  - Win: 20-40% encode CPU на 1-3 param queries.
  - Tier: tier-1 const-assert layout template bytes.
  - LoC: 150. Risk: LOW-MEDIUM.

- **A15 — `MAX_ACTIONS_PER_CALL` right-size 16 → 9** — P0.
  - File: `src/protocol.rs:246-257`.
  - Current: `MAX_ACTIONS_PER_CALL = MAX_STAGED_PER_CALL × MAX_FANOUT
    = 8 × 2 = 16`. Fanout 2× fires only на stale-schema-ref
    classification (crate-bug cold path).
  - Proposed: shrink до 9 = `MAX_STAGED + 1` (worst-case one
    stale-ref cascade/call). Если fanout = 1 strictly bounded, down
    до 8.
  - Win: 16 × Action → 9 × Action = ~37% `OutActions` savings
    (standalone). Complementary to A2/B1 — если A2 ship first,
    A15 становится правкой только const-assert, без runtime cost.
  - Tier: tier-1 const-assert.
  - LoC: 30. Risk: LOW.
  - Depends on: A2/B1 ideally первым.

- **A16 — `ParamOids` shape 68 B** — **CONFIRMED DONE (audit #2)**.
  - Justification: POD 68 B, SIMD-wide Eq под 64 B constraint,
    niche asymmetry с `RowDesc` объяснена. `#[MAX_PARAMS_ARITY = 16]`
    — phase 2 macro требование.

- **A17 — `parse_row_description` cold path** — **CONFIRMED DONE**.
  - Justification: `#[cold]` + `result_large_err` accepted.
    Per-query, not per-row. Verified #cold hint.

- **A18 — `build_query_message`** — **CONFIRMED DONE (audit #2)**.
  - Justification: branded builder + length-prefix closure +
    infallible writes. Tight code, no measurable fat. Vs A14's
    const-template: for Simple Query no 3-frame bundle to
    consolidate.

- **A19 — RowStream fast path optimal** — RECLASSIFIED → B6.
  - См. B6.

- **A20 — materialise no hot-path cost** — RECLASSIFIED → B1 + B3.
  - См. B1, B3.

---

#### B-series findings (audit #2)

- **B2 — 4 non-const `len()` методы** — P0.
  - Files: `src/decode.rs:172-176` (`RowDesc::len`),
    `src/decode.rs:629-631` (`DataRowRef::len`),
    `src/action.rs:1585` (`ParamOids::len`),
    `src/session_params.rs` (`OtherEncoding::len`).
  - Current: все 4 — `fn len(&self) -> usize`, каждая с
    комментарием "not const because `From<u16> for usize` not yet
    const-stable (rust-lang issue #143874)".
  - **Stale MSRV citation**: `usize::from(u16)` const-stable с
    Rust 1.87 (stabilised March 2025). Project MSRV = 1.95
    (April 2025). Citation устарела на момент MSRV bump.
  - Proposed: convert всех 4 на `const fn`, delete citation
    comment blocks.
  - Win: enables const evaluation на call sites → tier-2 runtime
    → tier-1 compile for downstream const bindings. Closes audit
    #1 A9 tail.
  - Tier elevation: 2 → 1.
  - LoC: 8 (modifications) + удаление comments. Risk: LOW.
  - Depends on: none. Standalone.

- **B3 — materialise fanout infallible push** — P0.
  - File: `src/protocol.rs:2215-2309` (`materialise`,
    `push_within_fanout_budget`).
  - Current: `push_within_fanout_budget` имеет `match Ok(()) | Err(_)`
    на every push. LLVM optimises в release к store+len-inc, но
    documented dead Err arm не eliminated типом.
  - Proposed: `OutActions::push_unchecked_bounded` gated by
    `where [(); N]: ArraySize` bound или через const-assert
    `MAX_ACTIONS ≥ worst-case`. Infallible push.
  - Win: removes 6 "dead-arm" sites, closes architectural doc
    drift.
  - Tier elevation: 3 → 1 (structural const-guarantee).
  - LoC: 60. Risk: MEDIUM.
  - Depends on: A2/B1 (OutActions shape) + A15 (cap right-sized).

- **B4 — `parse_error_response` O(N×fields) scan** — P0.
  - File: `src/dispatch.rs:1309-1416` (`parse_error_response`).
  - Current: `for _ in 0..MAX_ERROR_FIELDS` loop + inner
    `payload.iter().position(|b| *b == 0)` linear NUL scan per
    field. O(N × fields).
  - Proposed: chunk-of-4 scalar scan (SIMD-friendly; avoids
    `memchr` dep policy). Или keep pass as-is но mark
    `#[inline(always)]` + iterator `.copied()` vectorisation hint.
  - Win: ~3× speedup on ServerErrorResponse parsing hot path.
    ServerErrorResponse arrives на every failed query.
  - Tier: tier-2 preserved.
  - LoC: 30. Risk: MEDIUM.
  - **Missed by audit #1**.

- **B5 — narrow public API (`pub → pub(crate)`)** — P0.
  - File: `src/lib.rs:88-107` + module-root exports.
  - Current: 8 modules / types `pub` что могли быть `pub(crate)`.
    `StreamItem`-related internal types, narrow `reply_id::ReplyKind`
    trait visibility. ~20% API surface reduction potential.
  - Proposed: audit public API, tighten visibility where caller-
    unused.
  - Win: shrinks rust-analyzer footprint, reduces refactor surface
    cost, clearer API contract.
  - Tier: tier-1 (visibility = compile gate).
  - LoC: 30. Risk: LOW.

- **B6 — `feed_bytes_bounded(max)` split** — P1.
  - File: `src/protocol.rs:626-947` (`feed_bytes_bounded`).
  - Current: `feed_bytes` calls `feed_bytes_bounded(max=u16::MAX)`.
    Inner loop has `if dispatches_this_call >= max_dispatches
    { break; }` gate — dead on production paths (only `RowStream`
    passes anything other than MAX).
  - Proposed: split — `pub fn feed_bytes` → private
    `feed_bytes_unbounded` (no gate); `pub(crate) fn feed_bytes_bounded`
    (existing) для `RowStream`.
  - Win: eliminates 1 cmp+jge per frame parse на production hot
    path. 1M frames × 1 cycle = ~0.4 ms on 2.5 GHz.
  - Tier: tier-3 runtime check deleted → tier-2 structural (type-
    gated).
  - LoC: 20. Risk: LOW.
  - Reclassifies A19.

- **B7 — `#[inline(always)]` на `push_within_fanout_budget`** — P1.
  - File: `src/protocol.rs:2295-2309`.
  - Current: `#[inline]` (hint, not force). Every staged iter
    pays fn-call + match.
  - Proposed: `#[inline(always)]`, verify via `cargo asm`.
  - Win: 1 fn-call per staged action eliminated.
  - LoC: 1. Risk: LOW.
  - Note: becomes moot если B3 ships (push infallible).

- **B9 — `AuthSubCodeClass::Unknown(u32)` → NonZeroU32 niche** — P1.
  - File: `src/dispatch.rs:866-898` + `src/error.rs:232-254`.
  - Current: `AuthSubCodeClass::Unknown(u32)` — valid to be 0.
    `Option<AuthSubCodeClass>` не niche-packed.
  - Proposed: narrow до `Unknown(NonZeroU32)` где 0 становится
    sentinel. `Option<AuthSubCodeClass>` niche-packs в same
    bytes.
  - Win: 1 B saved на `ProtocolError::UnsupportedAuthMethod`.
  - Tier: tier-2 niche compile.
  - LoC: 5. Risk: LOW.

- **B11 — `ParamOids` `[u32; 16]` → `heapless::Vec`** — P1.
  - File: `src/action.rs:1525-1529` + `src/params.rs:153`.
  - Current: `ParamOids.oids: [u32; 16]` — always zero-filled tail.
    `parse_parameter_description` writes only populated prefix.
  - Proposed: `heapless::Vec<u32, 16>` тот же паттерн что B1.
  - Win: 60 B saved on describe hot path.
  - Tier: tier-2 structural.
  - LoC: 25. Risk: MEDIUM.
  - Note: conflicts с A16 CONFIRMED-DONE? Re-verify. Audit #2
    пришёл после audit #1's CONFIRMED — вероятно narrow scope
    here (heapless vs full shape retention).

- **B12 — SCRAM mechanism-list fast-path** — P1.
  - File: `src/dispatch.rs:989-998` (`mechanism_list_contains_scram`).
  - Current: linear scan через NUL-sep entries.
    `SCRAM_SHA_256_MECHANISM` known — fast-path possible.
  - Proposed: `payload.starts_with(b"SCRAM-SHA-256\0")` как
    pre-check перед полным scan.
  - Win: ~3× on happy path (single mechanism).
  - LoC: 5. Risk: LOW.

- **B14 — HList recursion для `ParamsWriter` tuple impls** — P1 HIGH.
  - File: `src/params.rs:245-285` + macro expansions.
  - Current: `ParamsWriter` tuple impls arity 0..=16 = **17
    monomorphised `write_params` bodies per call site**.
  - Proposed: HList recursion — single recursive `fn write_param_one`
    + trait `Nil` terminator. Pattern `frunk_core`-style re-rolled
    в ~50 LoC.
  - Win: removes 400+ LoC generated binary code → smaller I-cache
    footprint.
  - Tier: tier-1 type-level recursion.
  - LoC: 80. Risk: HIGH (ergonomic refactor of param binding API).

- **B15 — `with_length_prefix` as_chunks_mut::<4>()** — P1.
  - File: `src/write_buf.rs:383-414`.
  - Current: placeholder write → body → patch 4 bytes unaligned.
  - Proposed: `slice.as_chunks_mut::<4>()` (stable Rust 1.77) на
    known-aligned offset.
  - Win: branchless 4-byte write.
  - LoC: 10. Risk: LOW.

- **B17 — `record_param_status` inline hint** — P2.
  - File: `src/protocol.rs:2128-2147`.
  - Current: `payload.iter().position(|b| *b == 0)` — no inline
    hint.
  - Proposed: `#[inline(always)]` + `.copied()` vectorisation hint.
  - Win: minor loop-unroll.
  - LoC: 5. Risk: LOW.

- **B18 — lint bundle tighten** — P2.
  - File: `src/lib.rs:60-84`.
  - Current: forbid-bundle correct но missing
    `clippy::cast_possible_truncation`, `cast_sign_loss`,
    `cast_possible_wrap`, `float_cmp`.
  - Proposed: add 4 entries.
  - Win: catch edge cases where infallible `From`/`try_from` could
    be subtly wrong even after `as` ban.
  - Tier: tier-1 compile.
  - LoC: 4. Risk: LOW (may surface existing violations, each flagged
    must be fixed, not allowed).

- **B19 — `ParamOids::EMPTY` all-zeros Eq** — FALSE POSITIVE.
  - Justification per audit #2: current doc-safe. Full-array Eq is
    correct for POD shape; fresh-empty matches all-zeros populated
    fine.

- **B20 — `row_stream` inline hints** — P2.
  - File: `src/row_stream.rs:229-294` (`next_event`,
    `fast_path_data_row`, `slow_path_once`).
  - Current: no explicit `#[inline]`.
  - Proposed: `#[inline]` на три method'а.
  - Win: modest, LLVM already inlines в release.
  - LoC: 3. Risk: LOW.

- **B21 / C6 — `dispatch()` by-val → by-ref** — P1 BIG.
  - File: `src/dispatch.rs:143-149` (signature).
  - Current: `dispatch(prev: ProtoState, ...)` takes 712 B by value.
    Every dispatch call memcopies 712 B to stack.
  - Proposed: `dispatch(state: &mut ProtoState, ...)` + internal
    `mem::take` on transitions.
  - Win: **712 B × 1M frames = 712 MB stack traffic eliminated** на
    SELECT-heavy workloads.
  - Tier: tier-2 preserved (pure function semantics сохраняется
    через `&mut T → T` transformation).
  - LoC: 50. Risk: MEDIUM (touches all dispatch arms, test surface).

- **B23 — `CrateBugLocus` repr(u8) + niche** — P2.
  - File: `src/error.rs:582-672`.
  - Current: 7 variants, `#[repr(u8)]` не explicit;
    `Option<CrateBugLocus>` niche не const-asserted.
  - Proposed: add `#[repr(u8)]` + `const _: () = assert!(size_of::
    <Option<CrateBugLocus>>() == 1)`.
  - Win: 1 B saved + tier-1 drift pin.
  - Tier: tier-1.
  - LoC: 5. Risk: LOW.

- **B24 — `apply_pending_advance` silent Err discard** — P0.
  - File: `src/protocol.rs:1068-1074`.
  - Current: `let _result = self.read_buf.advance(usize::from(n.get()))`
    (line 1070) — Err architecturally dead but NO CLASSIFICATION.
    **Violates CREDO §1 "никакого silent fallback"** — tier-4.
  - Proposed: classify — if `advance` Err, transition to
    `ProtoState::Errored(ProtocolError::InternalCrateBug {
    locus: CrateBugLocus::ReadCursorAdvance })`. Tier-2 structural
    guarantee.
  - Win: closes tier-4 hole.
  - Tier elevation: 4 → 2.
  - LoC: 10. Risk: LOW.

- **B25 — `fast_path_data_row` silent Err discard** — P0.
  - File: `src/row_stream.rs:298-338`.
  - Current: `let _ = self.proto.read_buf_advance(total)` (line 321)
    — Err silently discarded. Same class как B24.
  - Proposed: same fix — classify Err via `Errored` state
    transition (install_errored_malformed_data_row pattern).
  - Tier elevation: 4 → 2.
  - LoC: 10. Risk: LOW.

- **B26 — `OutActions::as_slice` const-fold** — P3.
  - File: `src/action.rs:639-644`.
  - Current: `debug_assert!` + `.unwrap_or(&[])` — release builds
    pay fallback check.
  - Proposed: `const { assert!(MAX_ACTIONS_PER_CALL >= MIN_NEEDED) }`
    + `.get(..MIN).unwrap_or(&[])` — LLVM const-folds.
  - Win: minor cycles.
  - LoC: 5. Risk: LOW.

- **B27 — three-trait OID symmetry** — FALSE POSITIVE.
  - Already pinned at `src/decode.rs:1053-1071` + `:1177-1191`.
    Audit #1 не пропустил — comprehensive.

- **B28 — `SHA256_PROOF_B64_LEN` const-derive** — P3.
  - File: `src/scram/wire.rs:27-76`.
  - Current: `SHA256_PROOF_B64_LEN: usize = 44` — magic number.
  - Proposed: derive via `const fn` из `(SHA256_DIGEST_LEN * 4 + 2)
    / 3` formula.
  - Win: tier-1 drift guard.
  - Tier elevation: 2 → 1.
  - LoC: 10. Risk: LOW.

- **B29 — `SchemaSlab::occupied_count`** — CONFIRMED DONE.
  - Already optimal per DEF-171. 2-slot walk. DCE-safe (debug_assert
    path).

---

#### C-series crazy architectural

- **C1 — Typestate-driven `PgProtocol<S: State>`** — research spike.
  - Replace runtime `ProtoState` enum с compile-time `State` type
    parameter via sealed trait. 28 state variants → 28 type
    parameters. Invalid transitions = compile error.
  - Win: `ProtoState` 712 B → 0 B. Per-state monomorphic dispatch
    bodies. Tier-3 audit shield → tier-1 trivial.
  - Risk: HIGH (breaks public API catastrophically, threads state
    through caller, async wrapper потребует `Box<dyn Any>`-like
    pattern).
  - Deliverable: prototype (spike), decision ship/skip на основе
    usability benchmark'а в async wrapper.

- **C2 — Restore generative brands** — research spike.
  - Reintroduce `BrandedReadBuf<'brand>` + `BrandedWriteBuf<'brand>`
    + `WriteRange<'brand>` + `ReadRange<'brand>`. Apply infallible
    через HRTB `for<'brand>` closure.
  - Win: все 6 `.unwrap_or(&[])` / `match None => CloseSocket` в
    materialise collapse → tier-1 compile. 2 `CrateBugLocus`
    variants dead.
  - Risk: MEDIUM. User phapsed это в DEF-154 (W); revisit decision
    если infallible apply на самом деле elimirует >5 runtime checks.
  - Deliverable: spike → bench → decision.

- **C3 — `OutActions` as `impl Iterator<Item = Action>`** —
  research spike.
  - Generalise `RowStream` fast-path паттерн (X) на все actions.
    `push_command` / `feed_bytes` return iterator; action
    materialises на `.next()`.
  - Win: 5 KB `OutActions` init → 0 B на ALL caller paths.
  - Risk: MEDIUM (async wrapper адаптация, borrow enforces
    single-consume before next protocol call).
  - Overlaps A2; decision-point: A2 (heapless::Vec) vs C3 (iterator)
    — which wins on bench?

- **C4 — SWAR/SIMD `ColumnsBatch<N>`** — research spike (overlaps A11).
  - См. A11.

- **C5 — Bitpacked `StateErrorKind`** — P2 trivial.
  - File: `src/state.rs:ErrorKind`, `src/error.rs:StateErrorKind`.
  - Current: 2 bytes (discriminant + kind byte).
  - Proposed: bit-layout `[err_flag | kind_3bits | prior_gen_4bits]`
    in `NonZero<u8>`. Single-byte Errored variant.
  - Win: 1 B saved per connection. Compositional с B22.
  - Tier: tier-2 structural.
  - LoC: ~20. Risk: LOW.

- **C6 — `dispatch()` by-ref** — SAME AS B21.

---

### Dependency graph (execution ordering)

```
Batch 1 (tiny independent):
  B2, B5, B7, B18, B20

Batch 2 (classifier hygiene — closes tier-4 holes):
  B24, B25 [CRITICAL per CREDO §1], B23, B26, B28

Batch 3 (micro-perf independent):
  B9, B17, B15, B12, A8

Batch 4 (OutActions reshape foundation):
  A2/B1/B8 → A15 → B3 → B6

Batch 5 (decode hot-path):
  A5/B10, A6/B13, B4, B11

Batch 6 (encode optimization):
  A14

Batch 7 (dispatch perf):
  B21/C6 → A7 → [A3 post-bench]

Batch 8 (state refactor — enables A4):
  A10/B22 → A4/B16

Batch 9 (крупнейший каскад):
  A1+A13 (ErrorArena) → B14 (HList ParamsWriter)

Batch 10 (research spikes):
  A11/C4 [post-DEF-143 bench], C1, C2, C3, C5, A12 [deferred 1c-5]
```

### Execution policy

- Каждый batch — serial внутри, per-item коммит.
- Перед каждым batch — re-verify inputs/assumptions CREDO §3
  skepticism.
- После каждого batch — full test + release + clippy + memory
  leak spot-check (§9 zero-leak).
- Каждый item проходит §7 12 осей.
- Коммит-message обосновывает соответствие CREDO.
- Никакой batch не skip'ается целиком. Item отсев только по (a)
  doc-justified DONE эквивалент, (b) architecturally impossible без
  safety breach, (c) out of scope.
- Research-spike (Batch 10 C-series) — prototype first,
  decision-gate, затем полный ship если wins подтверждены.
- Параллельно собираем DEF-143 criterion bench harness (blocker
  для A11/C4 ship).

---

**Origin:** 2 architect subagent runs 2026-04-23.
