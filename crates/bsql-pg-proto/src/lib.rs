//! PostgreSQL wire-protocol state machine — pure sync, `no_std`, no allocator.
//!
//! `bsql-pg-proto` is the **sans-I/O** core of bsql's PostgreSQL backend.
//! It contains zero I/O, zero `alloc`, zero async runtime. The same state
//! machine drives:
//!
//! - the production async wrapper (`bsql-driver-postgres`, Phase 1e), where
//!   it lives inside a tokio task that owns a `TcpStream` + `mpsc` channel;
//! - the proc-macro online client (Phase 2), where it lives inside a
//!   blocking helper executed during `cargo build`;
//! - test harnesses, where it is driven directly by feeding bytes.
//!
//! Architectural promises (CREDO §0):
//!
//! - **Cancellation-safety by construction.** A dropped user future cannot
//!   leave the wire dirty, because the wire-state lives in a task-owned
//!   state machine separate from the user-visible future. See reforge.md
//!   §7.1.
//! - **No panics.** The forbid-bundle below rejects every panic-able
//!   expression at compile time; bounded buffers replace `Vec` / `String`;
//!   `checked_*` arithmetic everywhere. See reforge.md §3.1.
//! - **No data races.** `#![forbid(unsafe_code)]` plus the borrow checker
//!   plus a `PhantomData<core::cell::Cell<()>>` field on [`PgProtocol`]
//!   guarantee `!Sync`. See reforge.md §52.
//!
//! # Phase 1a scope
//!
//! Only the **Ping** flow is implemented. The state machine starts in
//! [`ProtoState::Idle`] (assumed already authenticated by an upstream layer
//! that does not yet exist). A [`PgCommand::Ping`] emits a `Sync` frame on
//! the wire; the matching `ReadyForQuery` reply transitions back to `Idle`
//! and emits a [`Reply::Pong`] on the user's [`ReplyId`].
//!
//! Other variants of `ProtoState`, `PgCommand`, `Action`, and `Reply`
//! are **deliberately omitted**. Per reforge.md §3.5 / §4.6, manufactured
//! variants without entry/exit code are forbidden — they masquerade as
//! tier-1 invariants while delivering tier-4 ("happens not to fail")
//! protection. Variants land in the commit that implements their driving
//! code end-to-end.
//!
//! # Module layout
//!
//! - [`buf`] — sealed, bounded read buffer. Methods that could panic
//!   (`insert`, `resize`, indexing, etc.) physically absent from the API.
//! - [`frame`] — pure-function frame-header parser. Never panics on
//!   arbitrary bytes — tier-1 by forbid-bundle + slice patterns +
//!   checked arithmetic. See its docstring for the mechanism audit.
//! - [`wire`] — protocol byte constants, including the precomputed `Sync`
//!   message body.
//! - [`reply_id`] — opaque correlator for in-flight commands.
//! - [`command`] — user-pushed commands (`PgCommand`).
//! - [`action`] — protocol-emitted side-effects (`Action`, `SendBuf`,
//!   `Reply`).
//! - [`state`] — `ProtoState` enum (state-as-data, see reforge.md §7.2).
//! - [`error`] — classified protocol errors.
//! - [`protocol`] — `PgProtocol` itself; entry points `feed_bytes` and
//!   `push_command`; emits [`OutActions`].
//! - [`dispatch`] — internal `(state, header) → outcome` matcher.

#![no_std]
#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division,
    // DEF-184 (B18): even with `as` cast banned, infallible
    // `From`/`try_from` can be subtly wrong if a narrowing happens
    // at the type level (e.g. `i32 → u32` sign loss, `u64 → usize`
    // on 32-bit targets). Tier-1 compile guard catches these.
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    // DEF-211 SAFE-05 (audit 2026-05-04, 5th-pass architect-agent):
    // `clippy::let_underscore_must_use` catches `let _ = fn()` where
    // the result carries a `#[must_use]` contract. The sibling
    // `let_underscore_drop` was renamed/moved to a rustc lint —
    // see the `#![deny(let_underscore_drop)]` line below. Tier-1
    // closes the silent-discard class at build time; no production
    // callsites trip these today (`cargo clippy` clean post-add).
    clippy::let_underscore_must_use
)]
// DEF-211 SAFE-05 (continued): rustc-namespace `let_underscore_drop`
// moved out of `clippy::*` after Rust 1.69. Catches the explicit
// `let _ = drop_chain_value` form where the value's `Drop::drop`
// still fires (so it is NOT a "leak" of secrets — `ZeroizeOnDrop`
// chains still run) but the immediate discard is structurally
// suspicious (a `let _binding = ...` or `drop(value)` makes the
// intent explicit). Distinct from `unused_must_use` (which fires
// on the call expression, not the let-pattern).
#![deny(let_underscore_drop)]
#![deny(
    unused_must_use,
    unused_lifetimes,
    unused_variables,
    missing_docs,
    rust_2024_incompatible_pat
)]
#![warn(missing_debug_implementations, missing_copy_implementations)]

// DEF-187 (architect 2026-04-26): committed `alloc` baseline.
//
// `bsql-pg-proto` is `no_std + alloc`. The crate uses `Box<T>` once
// per connection during SCRAM-SHA-256 handshake to externalise
// password-bearing session data; this enables a 9× reduction in
// `ProtoState` size (712 → ~80 B) and corresponding hot-path latency
// improvement on row streaming. Embedded targets without an
// allocator should use Trust-auth (no Box allocated) or stay on a
// pre-DEF-187 release.
//
// Trade-off documented per CREDO §4 (user-land крейты могут зависеть
// от alloc, когда обоснованно). Feature-gating evaluated and rejected:
// doubles test surface for the embedded-SCRAM use case which doesn't
// exist in practice.
extern crate alloc;

// DEF-233 (2026-05-04): self-alias enables generated code from
// `bsql-pg-proto-derive` (e.g. `#[derive(Pristine)]`) to reference
// `::bsql_pg_proto::pristine::Pristine` via its absolute path —
// resolves the same way both inside this crate AND in downstream
// user crates. Standard Rust derive-pair convention (mirror
// `serde`'s `extern crate self as serde;` at its lib root).
extern crate self as bsql_pg_proto;

// DEF-211 SAFE-02 (audit 2026-05-04, 5th-pass architect-agent):
// **transitive-`unsafe` audit-trust chain**.
//
// `bsql-pg-proto` itself uses `#![forbid(unsafe_code)]` (line 61).
// Every line of crate-internal Rust is `unsafe`-free by build-time
// rejection — the crate's own surface contributes ZERO unsafe
// boundaries.
//
// Direct (Cargo.toml) and transitive runtime dependencies that
// contain `unsafe` blocks, ranked by audit-trust risk:
//
// 1. `simdutf8` (~v0.1) — SIMD-accelerated UTF-8 validation.
//    Used by `<&str as FromPgText>::from_pg_text` (decode.rs).
//    Surface: NEON / SSE intrinsic invocations + alignment-aware
//    chunking. Audit-trust class: **ecosystem-tested** (1M+
//    downloads/month, multiple production users including
//    `simd-json`). Behaviour parity with `core::str::from_utf8`
//    is property-tested upstream; we treat the validation
//    boundary as authoritative. Bench (DEF-202): 2-4× speedup
//    on Cyrillic / long-ASCII rows.
//    **Scope of trust**: validate `simdutf8::basic::from_utf8`
//    contract on every PG release. Failure mode: misclassified
//    text → `DecodeError::NonUtf8` (tier-3, classified). Never
//    UB on attacker-controlled bytes (per upstream property tests).
//
// 2. `heapless` (~v0.9) — bounded-capacity inline `Vec`/`String`.
//    Used for `OutActions`, `StagedActions`, multiple wire-format
//    builders. Surface: `MaybeUninit<[T; N]>` storage with
//    manual init-tracking. Audit-trust class: **ecosystem-tested**
//    (embedded-Rust standard, no_std staple).
//    **Scope of trust**: bounded-cap Vec must never write past
//    its declared `N`; we never construct `heapless::Vec` from
//    raw pointers. The crate's own const-asserts (`MAX_ACTIONS_PER_CALL`
//    >= per-site budgets) ensure the heapless capacity is
//    sufficient under our usage patterns.
//
//    **DEF-211 SAFE-01 / SAFE-01' REJECTED 2026-05-04** — see
//    `deferred.md §B "Verified load-bearing (architect's concern
//    falsified)"` entry for the full pre-implementation post-mortem.
//    Two structural blockers:
//    (a) Per-call init cost catastrophic — `[T; N]` POD-array storage
//        eagerly initialises all N slots at construction; for per-call
//        types (`StagedActions = [StagedAction; 8]` ≈ 704 B,
//        `OutActions = [Action; 9]` ≈ 792 B) this ships ~700 B memset
//        per push_command/feed_bytes call → projected +30-50% on
//        push_command/ping_amortised (10.28 → 13-15 ns), violates the
//        Q2 bench gate (max +3% on existing benches).
//    (b) `MaybeUninit`-based skip-init alternative requires
//        crate-internal `unsafe { assume_init_read }` — breaks
//        `#![forbid(unsafe_code)]` at the architectural-rule level
//        (CREDO §1 absolute commit). Net-zero or worse safety win
//        replacing ecosystem-trusted code (~1000 LoC well-audited
//        embedded-Rust standard) with locally-audited equivalent.
//
//    The companion comment at `action.rs:672+` ("Why heapless::Vec,
//    NOT the OutActions POD-array shape") covers the per-call type
//    perf rationale in detail. Future audits raising SAFE-01 again
//    require new measurement evidence per the deferred.md §B reopen
//    contract — without it, the ecosystem-trusted heapless choice
//    is the load-bearing decision.
//
// 3. RustCrypto: `sha2` + `hmac` + `pbkdf2` — SCRAM-SHA-256
//    cryptographic primitives. Surface: const-time arithmetic,
//    inline assembly on aarch64 hardware-CRC paths. Audit-trust
//    class: **expert-domain crypto** (CREDO §11 — never
//    hand-rolled; trust the ecosystem implementation, not our own).
//    **Scope of trust**: SHA-256 / HMAC-SHA-256 contract per
//    NIST FIPS 180-4. Behaviour parity with `openssl` /
//    `boringssl` reference is integration-tested.
//
// 4. `getrandom` — OS RNG bridge (`/dev/urandom`,
//    `getrandom(2)`, `RtlGenRandom` etc.). Used by SCRAM
//    client-nonce generator. Surface: per-platform syscalls.
//    Audit-trust class: **expert-domain ecosystem standard**.
//    **Scope of trust**: returns 16+ bytes of cryptographically
//    secure random per call. Failure mode: kernel RNG init
//    delay (rare) — bubbles up as `ScramError::ClientNonceUnavailable`,
//    classified.
//
// 5. `subtle` — constant-time comparison + select primitives.
//    Used in SCRAM server-signature verification.
//    Surface: black-box-`asm!` to thwart compiler optimisation
//    that would leak timing.
//    Audit-trust class: **ecosystem-standard cryptographic
//    primitive**, audited as part of RustCrypto governance.
//
// 6. `zeroize` — secret-bearing-type drop scrub. Trait + derive
//    macro. Surface: macro-level `unsafe` for the inline-asm
//    `compiler_fence` on the `Zeroize` impl for primitive types.
//    Audit-trust class: **ecosystem-standard cryptographic
//    hygiene primitive**. The `unsafe` block is a single-line
//    `compiler_fence(Ordering::SeqCst)` to prevent dead-store
//    elimination.
//
// **Audit-trust posture**: every transitive `unsafe` source is
// either (a) ecosystem-standard (1M+ downloads), (b) expert-
// domain (crypto / hardware), or (c) deliberately-tiny (zeroize
// fence). The crate-internal surface is `unsafe`-free by
// `#![forbid]`. CREDO §11 explicitly accepts this trust model
// for crypto + ecosystem-standard primitives. Replacement of
// any of these with hand-rolled equivalents would CREATE a new
// `unsafe` audit boundary inside the crate — net worse per
// CREDO §11.
//
// Per-PR requirement: when bumping any of these deps, audit
// the changelog for `unsafe` boundary changes (new `unsafe`
// blocks, new platform inline-asm). Today's audit (2026-05-04)
// confirmed all six sources at known-good versions.

#[cfg(test)]
extern crate std;

pub mod action;
pub mod bounded;
pub mod buf;
pub mod command;
pub mod decode;
mod dispatch;
pub mod error;
pub(crate) mod error_arena;
pub use error_arena::{ArenaError, DisplayError, ErrorPayload, ErrorRef};
pub mod frame;
pub mod guard;
pub mod ident;
pub(crate) mod md5;
pub mod params;
pub mod password;
pub mod protocol;
pub mod row_stream;
pub mod reply_id;
pub mod scram;
// DEF-188: schema_arena module DELETED — RowDesc lives inline in
// state variants; terminal-reply schema parks into
// PgProtocol::terminal_row_desc. See state.rs / protocol.rs for
// the post-arena flow.
// DEF-211 SAFE-06 (audit 2026-05-04): `SecretZeroize` trait —
// driver-side panic-hook integration contract for closing the
// `panic = "abort"` zeroize gap. See module docstring for the
// full treatment.
pub(crate) mod secret_zeroize;
pub mod sensitive;
pub mod session_params;
// DEF-211 INNO-01 / DEF-233: Pristine trait paired with
// `#[derive(Pristine)]` from `bsql-pg-proto-derive`. See module
// docstring for the BS-11 broad-scope tier-3 → tier-1 closure.
pub mod pristine;
pub mod state;
pub mod wire;
pub mod write_buf;

pub use action::{
    Action, CloseCompletePayload, DescribePortalCompletePayload,
    DescribeStatementCompletePayload, DescribedRows, FeedEvent, OutActions, ParamOids,
    ParseCompletePayload, PongPayload, PushFailure, QueryCompletePayload, Reply,
    StartupCompletePayload, TxStatus,
};
pub use bounded::{BoundedLen, BoundedU8, BoundedU16};
pub use buf::{AdvancePastEnd, ReadBuf, ReadBufFull, ReadBufN};
pub use command::{FetchRows, PgCommand};
pub use decode::{
    ColumnDesc, ColumnsIter, DataRowRef, DecodeError, FormatCode, FormatCodeSet, FromPgText,
    MAX_ROW_COLUMNS, OutOfRange, RowDesc, RowDescBorrow, RowDescColumnsIter, oids,
};
pub use error::{CrateBugLocus, ErrorKind, ProtocolError, StateErrorKind};
pub use frame::{HeaderParse, MAX_FRAME_LEN_FIELD, READ_BUF_CAP, parse_header};
pub use guard::{ConnectionStatus, ReadyGuard};
pub use ident::{
    ApplicationName, DatabaseName, Ident, IdentError, PortalName, SecretBoundedStr, Sql, StmtName,
};
pub use password::{Credentials, Password, PasswordError};
pub use protocol::{MAX_ACTIONS_PER_CALL, MAX_STAGED_PER_CALL, PgProtocol};
pub use reply_id::{
    CloseKind, DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind,
    ReplyId, ReplyKind, StartupKind,
};
pub use row_stream::{RowStream, StreamItem};
pub use sensitive::Sensitive;
pub use session_params::{Encoding, OtherEncoding, SessionParams};
// DEF-211 INNO-01: re-export the `Pristine` trait + matching derive
// macro under one name. Rust trait and derive macro live in DIFFERENT
// namespaces (type vs macro), so identical-name re-exports do NOT
// collide — `use bsql_pg_proto::Pristine` brings BOTH into scope:
// trait usage `impl Pristine for T` resolves to the type-namespace
// item, `#[derive(Pristine)]` resolves to the macro-namespace item.
// This mirrors `serde`'s `pub use serde_derive::{Serialize,
// Deserialize}` + `pub trait Serialize { ... }` pattern.
pub use bsql_pg_proto_derive::Pristine;
pub use pristine::Pristine;
pub use state::ProtoState;
// DEF-223 (2026-05-05): top-level re-export of the user-facing
// `Terminate` wire literal. Drivers (`bsql-driver-postgres`,
// async wrappers) write these bytes immediately before TCP
// close to signal graceful shutdown. Convention matches
// other top-level re-exports — wire-internal consts (e.g.
// `SYNC_WIRE_BYTES`) stay `pub(crate)`; user-facing wire
// primitives are re-exported here.
pub use wire::TERMINATE_WIRE_BYTES;
// DEF-214 (2026-05-05): top-level re-export of the user-facing
// `SSLRequest` wire literal. Phase 1e wrapper drivers write
// these bytes BEFORE `PgProtocol::new()` to negotiate TLS;
// the 1-byte server response is OOB (driver handles it
// outside the frame parser).
pub use wire::SSL_REQUEST_WIRE_BYTES;
pub use write_buf::{MAX_OWNED_SEND_LEN, WriteBuf, WriteBufFull};

// ---------------------------------------------------------------------
// Tier-1 compile gates on Send — every type that crosses a task
// boundary in the wrapper (`bsql-driver-postgres`) must be `Send`.
// A future refactor that introduces a non-Send field (`Rc<T>`, raw
// pointer, `MutexGuard`) into any of these types becomes a build
// error here rather than a silent regression downstream.
// ---------------------------------------------------------------------
const _: fn() = || {
    fn assert_send<T: Send>() {}
    // Phase 1a types. `Action<'_>` and `OutActions<'_>` carry a
    // lifetime (DEF-094); asserting for `'static` implies Send for
    // any shorter lifetime by covariance.
    assert_send::<action::Action<'static, 'static>>();
    assert_send::<action::OutActions<'static, 'static>>();
    assert_send::<action::Reply<'static>>();
    assert_send::<command::PgCommand>();
    assert_send::<error::ProtocolError>();
    assert_send::<protocol::PgProtocol>();
    // DEF-112: `ReplyId` is now generic over `K: ReplyKind`. The
    // nominal kind parameter is `PhantomData<fn() -> K>` (ZST,
    // unconditionally `Send + Sync`), so assert_send holds for
    // every `K`; checking one concrete `K` is sufficient.
    assert_send::<reply_id::ReplyId<reply_id::PingKind>>();
    assert_send::<reply_id::ReplyId<reply_id::StartupKind>>();
    assert_send::<reply_id::ReplyId<reply_id::QueryKind>>();
    assert_send::<state::ProtoState>();
    // Phase 1b types
    assert_send::<ident::Ident>();
    assert_send::<ident::DatabaseName>();
    assert_send::<ident::ApplicationName>();
    assert_send::<password::Password>();
    assert_send::<password::Credentials>();
    assert_send::<session_params::SessionParams>();
    assert_send::<write_buf::WriteBuf>();
    assert_send::<scram::types::SecretDigest>();
    assert_send::<scram::types::CappedServerNonce>();
    // Typestate wrappers (audit round 2 D1).
    assert_send::<scram::session::ScramSession>();
    assert_send::<sensitive::Sensitive<password::Password>>();
    // Error sentinels — small Copy-like types that must stay Send so
    // that Result<T, E> returned across a task boundary compiles.
    assert_send::<buf::AdvancePastEnd>();
    assert_send::<buf::ReadBufFull>();
    assert_send::<write_buf::WriteBufFull>();
    assert_send::<scram::types::ServerNonceTooLong>();
    assert_send::<ident::IdentError>();
    assert_send::<password::PasswordError>();
    assert_send::<frame::HeaderParse>();
    // DEF-198: witness-guard typestate.
    assert_send::<guard::ConnectionStatus>();
    // ReadyGuard<'a> is `&'a mut PgProtocol` — Send for 'static implies
    // Send for any shorter lifetime by covariance. Sync would defeat
    // its exclusive-access purpose, so only Send is asserted.
    assert_send::<guard::ReadyGuard<'static>>();
    // DEF-212: bytes-only push API (Phase 1) + per-event feed API (Phase 2).
    // PushFailure is the typed Err arm of ReadyGuard::push_command;
    // FeedEvent is the per-event return of advance_one_frame. Both
    // cross task boundaries in the async wrapper (Phase 1e) — Send
    // is load-bearing.
    assert_send::<action::PushFailure>();
    assert_send::<action::FeedEvent<'static, 'static>>();
};

// ---------------------------------------------------------------------
// Tier-1 compile gate on `!Sync` for `PgProtocol` (DEF-073).
//
// `PgProtocol` must be `!Sync` by construction so that concurrent
// `&mut PgProtocol` access is architecturally unreachable — only one
// task at a time holds the exclusive borrow (see reforge.md §16). The
// marker that achieves this today is the `PhantomData<Cell<()>>` field
// on the struct; since `Cell<T>` is `!Sync`, the struct inherits it.
//
// **Without this assertion**, removing the marker field compiles
// silently and `PgProtocol` becomes `Sync`. The downstream wrapper
// then would accept `&PgProtocol` across threads — a soundness break
// the compiler would no longer catch.
//
// # The ambiguous-impl trick
//
// We define a private trait with two overlapping blanket impls: one
// for every `T: ?Sized`, one for every `T: ?Sized + Sync`. For
// `T: Sync`, both impls match — method resolution is ambiguous and
// the build fails. For `T: !Sync`, only the first impl matches — the
// build succeeds. Calling `assert_not_sync::<PgProtocol>()` thus
// compiles iff `PgProtocol` is `!Sync`.
//
// No dev-dep on `static_assertions`; stable Rust, zero runtime cost
// (the const block resolves at typeck time and emits no code).
const _: fn() = || {
    trait AmbiguousIfSync<A> {
        fn assert_not_sync() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}

    // If `PgProtocol: Sync`, the two impls above collide on method
    // resolution — this line fails to compile.
    <protocol::PgProtocol as AmbiguousIfSync<_>>::assert_not_sync();
};

// ---------------------------------------------------------------------
// Tier-1 compile gates on enum / struct **size** (DEF-087).
//
// Background: an enum's `size_of` is governed by its largest variant.
// The `no_alloc` constraint forces variants to carry bounded inline
// buffers (heapless::Vec, heapless::String). A well-intentioned
// contributor could silently balloon an enum by bumping a
// `heapless::String<N>` capacity or adding a variant carrying a large
// inline buffer — the code compiles, but every call allocating that
// enum on stack now pays the bloated cost, and every move is a larger
// memcpy.
//
// Size bounds are **documentation in the type system**: they pin the
// current envelope and catch regressions. A legitimate size growth
// (e.g., adding a new state variant with its own bounded buffers) is
// normal — the bound adjusts in the same commit, making the memory
// cost part of the review surface instead of drifting silently.
//
// All bounds are generous — set slightly above current observed size
// to leave room for ordinary evolution, tight enough to catch obvious
// regression (2×, 4× blowups).
//
// ═══════════════════════════════════════════════════════════════════
// CURRENT (aarch64-apple-darwin, post-DEF-184 A10/B22, 2026-04-24)
// ═══════════════════════════════════════════════════════════════════
// The live size budget. All const_asserts below pin against THESE
// values. Any future refactor that changes a size must update the
// pin + shift the matching line here in the same commit.
//
//   Ident:             66  (FixedStr<63, IdentTag>)
//   DatabaseName:      66
//   ApplicationName:  130
//   ProtocolError:     72  (DEF-184 A1+A13 — ErrorArena cascade)
//   Action<'_,'_>:     88  (DEF-184 A1+A13 — Reply-bounded)
//   OutActions:       800  (DEF-184 — 9 × Action + len)
//   DispatchOutcome:   88  (DEF-184 B21/C6 — by-ref state)
//   Reply<'_>:         80  (DEF-119 — RowDesc externalised)
//   ReplyId:           16
//   PgCommand:      ≤2176  (Parse dominates)
//   ProtoState:      ~712  (SCRAM inline — tier-1 variant-carries-field;
//                            DEF-184 A10/B22 REVERTED 2026-04-24 per CREDO §1)
//   SchemaArena:      ~520
//   ErrorArena:      ~290
//   PgProtocol:    [6000, 6200]  (range tolerates alignment)
//
// ═══════════════════════════════════════════════════════════════════
// HISTORICAL (for context on why the budget looks the way it does)
// ═══════════════════════════════════════════════════════════════════
// Pre-DEF-060 (2026-04-20, x86_64 Linux):
//   ProtocolError:    856  (five heapless::String<N>, N<=256)
//   Action:           864  (SendBuf::Owned contains 512-byte vec)
// Post-DEF-095/096/097 (aarch64-apple-darwin):
//   Ident:             66  (was 72 — heapless::Vec<u8,63>+usize → POD FixedStr)
//   ProtocolError:    304  (DEF-060 typed variants + FixedStr tail)
//   PgCommand:       2136  (Parse dominates: StmtName + Sql + ReplyId)
// Post-DEF-119 (2026-04-21):
//   Reply<'_>:         80  (was ~340 — RowDesc externalised to arena)
//   Action<'_,'_>:    312  (was ~384 — FailReply.cause now dominant)
//   OutActions:      2504  (was ~3072 — 8 × Action shrunk)
//   ProtoState:      1224  (unchanged — SCRAM dominant)
//   PgProtocol:      6272  (added 528 B arena, other shrinkage offset net)
// Post-DEF-148 (2026-04-22):
//   SchemaArena:     ~520   (post-DEF-171 has_any deleted)
//   PgProtocol:      6272  (DEF-119 baseline preserved)
// Post-DEF-184 A1+A13 (2026-04-23):
//   ProtocolError:     72  (was 312 — ErrorArena cascade)
//   Action:            88  (was 312 — Reply-bounded via ErrorRef)
//   OutActions:       800  (was 2808 — 9 × Action shrink)
// Post-DEF-184 B21/C6 (2026-04-24):
//   DispatchOutcome:   88  (was 800 — by-ref state removes new_state payload)
// Post-DEF-184 A10/B22 REVERTED (2026-04-24):
//   ProtoState:       ~712  (SCRAM session restored INLINE in variant
//                            per CREDO §1 tier-1 variant-carries-field;
//                            safety > tier-1 > perf)
//   scram_state field: REMOVED (no correlation invariant to maintain,
//                            ZeroizeOnDrop fires on state transition
//                            automatically via variant drop glue)
// ---------------------------------------------------------------------
// DEF-151: tight-range size asserts. Bound BOTH directions to catch
// field additions (upper) AND accidental field removals (lower). The
// ±8 B slack tolerates cross-platform alignment differences; on
// aarch64-apple-darwin the actual is exactly at the lower bound, on
// other targets it may drift up to +8.
const _: () = assert!(
    core::mem::size_of::<error::ProtocolError>() == 72,
    "ProtocolError exact size — 72 B post-(A1+A13). \
     Pre-(184): 312 B dominated by ServerErrorResponse's 3 inline \
     BoundedStr<N> fields (288 B). Post-(184): ServerErrorResponse \
     carries `details_ref: ErrorRef` (2 B); bounded strings live in \
     PgProtocol::error_arena. Remaining 72 B dominated by other \
     large variants (e.g. Scram(ScramError), Malformed* with \
     BoundedStr<32>). \
     \
     Exact pin catches any variant growth / layout drift.",
);
const _: () = assert!(
    core::mem::size_of::<action::Action<'static, 'static>>() == 88,
    "Action<'_, '_> exact size — 88 B post-(A1+A13). \
     Pre-(184) was 312 B dominated by FailReply.cause ProtocolError; \
     post-(184) ProtocolError 72 B, so Action bounded by \
     max(Reply 72, FailReply 72) + discriminant + padding = 88 B. \
     Exact pin catches any variant growth.",
);
const _: () = assert!(
    core::mem::size_of::<action::Reply<'static>>() >= 72
        && core::mem::size_of::<action::Reply<'static>>() <= 96,
    "Reply<'r> size drift — post-DEF-119 actual is 80 B. Range [72, 96] \
     catches variant payload changes. Dominating variant is \
     DescribeStatementComplete (ParamOids ~68 B + DescribedRows ~16 B + \
     TxStatus + padding).",
);
const _: () = assert!(
    core::mem::size_of::<reply_id::ReplyId<reply_id::PingKind>>() <= 24,
    "ReplyId<K> size regression — the `PhantomData<fn() -> K>` kind \
     tag is zero-size; ReplyId's footprint is u64 value + bool \
     delivered + padding. Did a bookkeeping field get added?",
);
// DEF-189 (architect 2026-04-25): RowDesc moved to PgProtocol single
// slot; state variants no longer carry schema.
// DEF-210 SR-01 Path C/D (audit 2026-04-28): duplicate flags
// (`schema_present: bool` for SimpleQuery, `DescribedRowsStaged`
// enum for Describe paths) deleted — `PgProtocol::row_desc_slot.is_some()`
// is the single source of truth across all schema-bearing reply paths
// (tier-1 by-construction).
// DEF-210 SR-04 + REC-02 (audit 2026-04-28): tightened the prior
// `>= 16 && <= 96` range pin (80-byte slack window) into exact `==`
// value pin. Drift surface narrowed to a single arithmetic identity.
// Cross-platform note: pinned for reference target aarch64-apple-darwin;
// per-target `#[cfg(...)]` blocks set in the same commit that adds
// another target to CI per the §A "Cross-platform CI matrix" policy
// in `deferred.md`.
const _: () = assert!(
    core::mem::size_of::<state::ProtoState>() == 80,
    "ProtoState size post-DEF-189 (RowDesc externalised) + DEF-210 \
     SR-01 Path C/D (schema-presence flags deleted; row_desc_slot is \
     single source of truth) + DEF-210 SR-04 (range pin tightened to \
     exact). \
     \
     Layout on aarch64-apple-darwin: dominant variant is \
     `DescribeStatementAwaitingRfq` — `ReplyId<DescribeStatementKind>` \
     (8 B; NonZeroU64 + ZST PhantomData) + `ParamOids` (68 B; 4 B \
     `n_params: u16` + 2 B padding + 16 × 4 B oid array) + 1 B variant \
     discriminant + 3 B align(8) tail-pad → 80 B. \
     \
     Other notable variants: \
     - SCRAM `ConnectingScramAwaitingServerFirst` — 3 × Box (24 B) + \
       ReplyId (8 B) + discriminant + align-pad → ~40-48 B. \
     - `SimpleQueryAwaitingRfq` — ReplyId (8 B) + BoundedStr<32> \
       command_tag (~33 B) + discriminant + padding → ~48 B. \
     - Streaming variants — ReplyId (8 B) + discriminant → ~16 B. \
     \
     Pre-DEF-189: ProtoState ~320 B (dominant variant carried inline \
     RowDesc 264 B + reply + command_tag). Net cumulative win across \
     DEF-188/189 + DEF-210 SR-01 Path C/D: ~75% reduction. \
     \
     Per-row hot-path single state-projection retrieves just the \
     reply id; the descriptor is fetched via the protocol's \
     `current_row_desc` slot (one immutable borrow, no per-row \
     state match for the desc field). \
     \
     **The dominant constraint is `ParamOids` (68 B), not SCRAM.** A \
     refactor that wants to shrink ProtoState should target ParamOids \
     (16-OID arity) or split DescribeStatement* into a heap-boxed \
     payload variant (DEF-187 SCRAM precedent — but pay-vs-tier \
     tradeoff per CREDO §1). \
     \
     If a refactor changes this number on aarch64-apple-darwin, \
     update both the literal AND the layout comment above (drift-pin \
     CREDO §3 discipline).",
);
const _: () = assert!(
    core::mem::size_of::<command::PgCommand>() <= 2176,
    "PgCommand size regression — post-1c-3a budget is 2176 bytes. \
     Parse dominates: StmtName (66) + Sql (2050) + ReplyId<ParseKind> \
     (16) + discriminant + padding. Bumping MAX_SQL_LEN or \
     MAX_PG_NAME_LEN must move this limit in lockstep.",
);
// DEF-189 (architect 2026-04-25): RowDesc moved to single slot;
// state variants stripped of inline schema. DEF-194 (2026-04-27):
// bit-packed format_codes shrinks RowDesc 164 → 136 B; Option<RowDesc>
// 168 → 140 B (exact pin in `decode.rs` const-assert).
//
// Cross-platform stance (2026-04-27): exact `==` pin consistent with
// the rest of the crate (`ProtocolError == 72`, `Action == 88`,
// `OutActions == 800`, `DispatchOutcome == 88`, `RowDesc == 136`,
// etc.). Pre-DEF-194 follow-up I tried `5080 ±8 B` range under the
// "cross-platform alignment cushion" framing — but that was
// inconsistent with the project pattern AND permitted no-saving
// regressions silently. Reference target: aarch64-apple-darwin (where
// CI lives today). When CI matrix extends to x86_64-linux / riscv64 /
// etc., per-target cfg-gated pins land in the same commit that adds
// the target — not via permissive ranges. CREDO §3 skepticism: drift
// surface beats variance cushion every time.
// DEF-196 (2026-04-28): cold-path fields externalised into three
// independent lazy slots — each cold field allocates its Box
// independently only on first write. Hot `PgProtocol` shrinks
// 5080 B → 4352 B (−728 B inline).
//
// Layout breakdown:
//   ReadBuf:                4096 B (4 KiB)
//   state:                    64 B (post-DEF-189 strip)
//   row_desc_slot:           140 B (Option<RowDesc>)
//   session_params:            8 B (Option<Box<SessionParams>> niche)
//   error_arena:               8 B (Option<Box<ErrorArena>> niche)
//   malformed_frame_count:     4 B (inline u32 — too small to amortise
//                                   pointer indirection)
//   sync_marker:               0 B (PhantomData)
//   alignment padding:        ~32 B (to align(8))
//   total:                  4352 B
//
// Heap economics per connection pattern:
//   - Trust auth + no errors:        0 allocations.
//   - Startup auth + no errors:      1 alloc (Box<SessionParams> 436 B).
//   - Startup auth + errors:         2 allocs (~732 B total).
//   - Malformed frame teardown:      0 allocations (counter inline).
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol>() == 4352,
    "PgProtocol size exact pin (aarch64-apple-darwin reference). \
     \
     Budget: ReadBuf 4096 + state ~64 + row_desc_slot 140 + \
     cold (Option<Box<ColdFields>>) 8 + alignment to align(8) = 4344 B. \
     \
     Pre-DEF-196 was 5080 B (cold fields inline). DEF-196 saves \
     736 B per PgProtocol via heap-boxed cold storage. \
     \
     Cross-platform: when CI matrix extends, either (a) every target \
     lands at 4344 (most likely — alignment-stable types), or \
     (b) per-target cfg-gated pins land in the same commit. \
     Permissive ranges forbidden — drift surface > variance cushion \
     (CREDO §3 + §4.12).",
);
// DEF-184 (B21/C6): DispatchOutcome size pin — must stay ≤ 96 B
// post-`new_state` extraction. Pre-(B21/C6) each Advanced variant
// carried a `ProtoState` payload (712 B); the total enum was dominated
// by the Advanced variants at ~800 B. Post-(B21/C6) dispatch writes
// state directly via `&mut ProtoState`, and DispatchOutcome carries
// only the side-effect signal (StagedAction 88 B for WithAction,
// reply_id + ProtocolError 72 B for Errored).
const _: () = assert!(
    core::mem::size_of::<dispatch::DispatchOutcome>() == 88,
    "DispatchOutcome exact size — 88 B post-(B21/C6). \
     Dominated by AdvancedWithAction(StagedAction 88 B); the \
     discriminant + Errored variant payload fold into StagedAction's \
     alignment + niches. \
     \
     Pre-(B21/C6) was ~800 B dominated by the Advanced variants' \
     `new_state: ProtoState` payload. If this trips, either (a) a \
     new ProtocolError variant inflated the Errored payload, (b) \
     StagedAction grew (cascade into Action / OutActions), or (c) \
     `new_state` was re-added to an Advanced variant — regression \
     vs the B21/C6 refactor.",
);

const _: () = assert!(
    core::mem::size_of::<action::OutActions<'static, 'static>>() == 800,
    "OutActions<'_, '_> exact size drift — 800 B post-(A1+A13). \
     \
     = 9 (MAX_ACTIONS_PER_CALL) × 88 (Action) + 8 (usize len) = \
     800 B. \
     \
     Pre-(A1): 9 × 312 = 2808 B (ProtocolError-dominated). \
     \
     History: Post-(A15) MAX_ACTIONS_PER_CALL 16 → 9; \
     Post-(A2/B1/B8) ManuallyDrop<heapless::Vec>, zero init; \
     Post-(A1+A13) Action Reply-bounded via ErrorArena. \
     \
     Exact pin catches ANY layout drift. Change is decision-point \
     (not silent regression): audit Action size, OutActions cap, \
     ManuallyDrop shape.",
);

// ---------------------------------------------------------------------
// DEF-212 Phase 3 (M4 — architect-vetted impl plan, audit 2026-05-04):
// exact `==` size pins for the new bytes-only push API + per-event
// secondary feed API. Pinned per CREDO §III no-permissive-ranges
// policy: relative pins (e.g., `<= 96`) cushion silent drift; exact
// pins make every byte change a contributor decision-point.
// ---------------------------------------------------------------------

// `PushFailure` exact size — 80 B post-Phase-1.
//
// Layout: NonZeroU64 (8 B, 8-aligned) + ProtocolError (72 B) = 80 B
// total. Niche-packed: `Option<PushFailure>` is also 80 B (NonZeroU64
// niche absorbs the discriminant).
//
// Drift surface: a future `ProtocolError` variant addition that
// pushes the enum past 72 B would cascade here. The complementary
// `ProtocolError == 72` pin (line 456+) catches drift at the source;
// this pin catches the propagation.
const _: () = assert!(
    core::mem::size_of::<action::PushFailure>() == 80,
    "PushFailure exact size — 80 B (NonZeroU64 8 B + ProtocolError 72 B). \
     If this trips: (a) ProtocolError grew past 72 B (check sibling pin \
     at action::PushFailure docstring + error.rs ProtocolError pin), \
     or (b) NonZeroU64 alignment changed (architecturally impossible \
     under stable Rust). Cascade impact: Result<(), PushFailure> return \
     frame on push paths grows in lockstep — the DEF-212 Phase 1 -88% \
     headline (800 B → 80 B) erodes proportionally.",
);
const _: () = assert!(
    core::mem::size_of::<Option<action::PushFailure>>() == 80,
    "Option<PushFailure> niche-pack — must stay 80 B via the NonZeroU64 \
     niche on PushFailure.id. If this regresses to 88 B (or higher), \
     the niche optimisation was lost — likely cause: a non-niche field \
     added to PushFailure that consumed the discriminant slot.",
);

// `FeedEvent<'static, 'static>` exact size — 88 B post-Phase-2.
//
// Layout: max variant is `Deliver(NonZeroU64, Reply<'r>)` =
// 8 + 80 = 88 B; discriminant niche-optimised via NonZeroU64
// (zero bit-pattern reserved for variant tagging across the 7
// variants). `Option<FeedEvent>` also niche-packs to 88 B.
//
// Drift surface: a future `Reply<'r>` widening past 80 B (Reply pin
// is range [72, 96] currently — any growth toward 88+ regresses
// FeedEvent). A new variant carrying a payload > 80 B would also
// trip this.
const _: () = assert!(
    core::mem::size_of::<action::FeedEvent<'static, 'static>>() == 88,
    "FeedEvent<'wb, 'r> exact size — 88 B (max variant = Deliver: \
     NonZeroU64 8 B + Reply<'r> 80 B). Discriminant niche-optimised \
     via NonZeroU64. If this trips: (a) Reply grew past 80 B (check \
     sibling pin Reply<'r> in [72, 96] — tighten when Reply gets exact), \
     (b) a new FeedEvent variant carries a payload > 80 B (rare — \
     architectural change), or (c) niche optimisation lost. The DEF-212 \
     Phase 2 design budget assumes ≤ 88 B per per-event return frame — \
     larger means worse 1c-5 pipelining throughput per cycle.",
);
const _: () = assert!(
    core::mem::size_of::<Option<action::FeedEvent<'static, 'static>>>() == 88,
    "Option<FeedEvent> niche-pack — must stay 88 B via the NonZeroU64 \
     niche on Deliver.id / Fail.id. If this regresses, the niche was \
     lost — verify variant layout still routes the discriminant through \
     a NonZero* slot.",
);

// ---------------------------------------------------------------------
// Tier-1 compile gates on Drop semantics (DEF-093).
//
// `core::mem::needs_drop::<T>()` is a const fn that returns true iff
// T (or any of its fields transitively) has a non-trivial Drop impl.
// We assert this at compile time to pin invariants:
//
// - Types that carry secrets MUST have Drop (for zeroize-on-drop).
// - Types that are safety-net runtime guards MUST have Drop.
// - Small value types SHOULD NOT have Drop (Copy-able / move-friendly
//   / no hidden runtime cost on scope exit).
//
// A regression that removed Zeroize impls from Password or added a
// Drop to Reply would fail the build here. Zero runtime cost.
// ---------------------------------------------------------------------
const _: () = assert!(
    core::mem::needs_drop::<password::Password>(),
    "Password must have Drop for zeroize-on-drop (DEF-051 / secret scrub)",
);
const _: () = assert!(
    core::mem::needs_drop::<scram::types::SecretDigest>(),
    "SecretDigest must have Drop for zeroize-on-drop",
);
// DEF-154 (K): ReplyId<K> no longer has Drop. The panic-in-Drop
// "consume-discipline guard" double-panicked under integration-test
// unwind (SIGABRT masked original failure). Discipline enforced via
// `#[must_use]` + integration tests observing Action content. See
// `reply_id.rs` `// DEF-154 (K):` block for the full rationale.
const _: () = assert!(
    !core::mem::needs_drop::<reply_id::ReplyId<reply_id::PingKind>>(),
    "ReplyId<K> must stay drop-free — Drop was a footgun (see DEF-154 (K)).",
);
const _: () = assert!(
    !core::mem::needs_drop::<action::Reply<'static>>(),
    "Reply must stay drop-free — all variants are Copy-like (small value type). \
     DEF-119: Reply<'r> borrows &'r RowDesc from the schema arena; borrows \
     don't add Drop.",
);
const _: () = assert!(
    !core::mem::needs_drop::<frame::HeaderParse>(),
    "HeaderParse must stay drop-free — pure value type",
);
const _: () = assert!(
    !core::mem::needs_drop::<ident::IdentError>(),
    "IdentError must stay drop-free — enum of Copy variants",
);
const _: () = assert!(
    !core::mem::needs_drop::<password::PasswordError>(),
    "PasswordError must stay drop-free — enum of Copy variants",
);
// Audit round 2 E1 — expanded coverage. Positives: types carrying
// secrets / resources that MUST self-scrub. Negatives: small value
// types that MUST stay Copy-friendly / drop-free.
const _: () = assert!(
    core::mem::needs_drop::<scram::session::ScramSession>(),
    "ScramSession owns Sensitive<Password> — must Drop so the inner zeroize fires",
);
const _: () = assert!(
    core::mem::needs_drop::<sensitive::Sensitive<password::Password>>(),
    "Sensitive<Password> must Drop to trigger ZeroizeOnDrop on the inner",
);
// Note — Ident/DatabaseName/ApplicationName wrap heapless::Vec<u8, N>,
// which carries a blanket `Drop` impl (even for `T: Copy`) and thus
// trips `needs_drop`. No negative assert here — the ambient Drop has
// an empty body for `u8`, so there is no scrub contract to pin.
const _: () = assert!(
    !core::mem::needs_drop::<buf::ReadBufFull>(),
    "ReadBufFull must stay drop-free — error sentinel, Copy value",
);
const _: () = assert!(
    !core::mem::needs_drop::<buf::AdvancePastEnd>(),
    "AdvancePastEnd must stay drop-free — ZST-like error sentinel",
);
const _: () = assert!(
    !core::mem::needs_drop::<write_buf::WriteBufFull>(),
    "WriteBufFull must stay drop-free — error sentinel",
);
const _: () = assert!(
    !core::mem::needs_drop::<scram::types::ServerNonceTooLong>(),
    "ServerNonceTooLong must stay drop-free — error sentinel",
);
// DEF-094 follow-up: Action<'_> is Copy (post POD BoundedStr), so
// `needs_drop::<Action<'static>>()` must be false — that's what
// makes `OutActions<'buf>` releases-at-last-use under NLL (no
// explicit `drop(out)` needed in tests).
const _: () = assert!(
    !core::mem::needs_drop::<action::Action<'static, 'static>>(),
    "Action<'buf> must stay drop-free — POD BoundedStr + typed ProtocolError + Copy variants",
);
const _: () = assert!(
    !core::mem::needs_drop::<error::ProtocolError>(),
    "ProtocolError must stay drop-free — all variants' fields are Copy (DEF-060 POD BoundedStr)",
);
const _: () = assert!(
    !core::mem::needs_drop::<action::OutActions<'static, 'static>>(),
    "OutActions<'_> must stay drop-free. Post-DEF-184 A2/B1/B8: \
     inner heapless::Vec is wrapped in ManuallyDrop which inhibits \
     the Vec's Drop impl. Since Action<'w, 'r> is Copy (POD refs + \
     small payload), skipping inner Drop is sound (no-op body \
     anyway). This preserves pre-(184) NLL last-use borrow-release \
     semantics — the caller pattern `let out = proto.feed_bytes(..); \
     match out.as_slice() {{ .. }}; proto.state()` compiles without \
     explicit drop(out) between as_slice and next proto call.",
);
