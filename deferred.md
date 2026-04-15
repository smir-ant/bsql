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
| DEF-001 | `ConnectingStartup(ReplyId)` | 1b | StartupMessage flow |
| DEF-002 | `ConnectingScram { nonce, step, … }` | 1b | SCRAM-SHA-256 auth |
| DEF-003 | `ConnectingPostAuthWaitKey(ReplyId)` | 1b | Post-auth handshake |
| DEF-004 | `ConnectingPostAuthHaveKey { pid, secret_key, … }` | 1b | BackendKeyData receipt |
| DEF-005 | `AwaitingQueryReply { reply, hash, columns }` | 1c | Query / Execute flow |
| DEF-006 | `StreamingRows { stream, hash, columns }` | 1d | QueryStream |
| DEF-007 | `InTransaction { level, depth }` | 1c | Begin/Commit/Rollback |
| DEF-008 | `Errored(ProtocolError)` | every flow | Added when its first legitimate entry path lands |
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
| DEF-012 | `MAX_OWNED_SEND_LEN` | not yet shipped | — | — | — (lands with SendBuf::Owned, DEF-013) |
| DEF-013 | `SendBuf::Owned` variant | not yet shipped | — | — | Lands with StartupMessage (1b) |
| DEF-014 | `WriteBuf` newtype | not yet shipped | — | — | Lands with runtime-built outbound frames (1b) |

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
| DEF-028 | **`ReplyId` consume discipline**: strip `Copy`/`Clone`; `#[must_use = "..."]`; private-mod ctor so only wrapper crate mints IDs; explicit `consume(self) -> u64` method. Add a `Drop` impl that `debug_assert!(false, "ReplyId dropped without delivery")` — in release under `panic = "abort"`, this aborts the process. **Honest tier label:** tier-1 runtime (process aborts on misuse), tier-3 compile-time (compiler cannot statically prove `consume` is always called; no linear types in stable Rust). | `bsql-pg-proto` — amend `reply_id.rs` | 1 runtime / 3 compile |
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

## 9. Closed

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
