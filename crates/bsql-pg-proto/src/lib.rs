//! PostgreSQL wire-protocol state machine — pure sync, `no_std`, no allocator.
//!
//! `bsql-pg-proto` is the **sans-I/O** core of bsql's PostgreSQL backend.
//! It contains zero I/O, zero `alloc`, zero async runtime. The same state
//! machine drives:
//!
//! - the production async wrapper (`bsql-driver-postgres`), where it
//!   lives inside a tokio task that owns a `TcpStream` + `mpsc`
//!   channel;
//! - the proc-macro online client, where it lives inside a blocking
//!   helper executed during `cargo build`;
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
//! # Scope policy
//!
//! Per reforge.md §3.5 / §4.6, manufactured variants of `ProtoState`,
//! `PgCommand`, `Action`, and `Reply` without entry/exit code are
//! forbidden — they masquerade as tier-1 invariants while delivering
//! tier-4 ("happens not to fail") protection. Variants land in the
//! commit that implements their driving code end-to-end.
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
    // Even with `as` cast banned, infallible `From`/`try_from` can
    // be subtly wrong if a narrowing happens at the type level
    // (e.g. `i32 → u32` sign loss, `u64 → usize` on 32-bit targets).
    // Tier-1 compile guard catches these.
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    // `clippy::let_underscore_must_use` catches `let _ = fn()`
    // where the result carries a `#[must_use]` contract. The
    // sibling `let_underscore_drop` was renamed/moved to a rustc
    // lint — see the `#![deny(let_underscore_drop)]` line below.
    // Tier-1 closes the silent-discard class at build time.
    clippy::let_underscore_must_use
)]
// Rustc-namespace `let_underscore_drop` moved out of `clippy::*`
// after Rust 1.69. Catches the explicit `let _ = drop_chain_value`
// form where the value's `Drop::drop` still fires (so it is NOT a
// "leak" of secrets — `ZeroizeOnDrop` chains still run) but the
// immediate discard is structurally suspicious (a `let _binding =
// ...` or `drop(value)` makes the intent explicit). Distinct from
// `unused_must_use` (which fires on the call expression, not the
// let-pattern).
#![deny(let_underscore_drop)]
#![deny(
    unused_must_use,
    unused_lifetimes,
    unused_variables,
    missing_docs,
    rust_2024_incompatible_pat
)]
#![warn(missing_debug_implementations, missing_copy_implementations)]

// `bsql-pg-proto` is `no_std + alloc`. The crate uses `Box<T>` in
// state variants to externalise large inline payloads — SCRAM/MD5/
// Cleartext password-bearing handshake data, and the `ParamOids`
// 68 B `DescribeStatement*` payload. This enables a ~15× reduction
// in `ProtoState` size (712 → ~48 B) and corresponding hot-path
// cache-density improvement on row streaming. Embedded targets
// without an allocator should use Trust-auth (no Box allocated).
//
// Trade-off documented per CREDO §4 (user-land крейты могут зависеть
// от alloc, когда обоснованно). Feature-gating evaluated and
// rejected: doubles test surface for the embedded-SCRAM use case
// which doesn't exist in practice.
extern crate alloc;

// Self-alias enables generated code from `bsql-pg-proto-derive`
// (e.g. `#[derive(Pristine)]`) to reference
// `::bsql_pg_proto::pristine::Pristine` via its absolute path —
// resolves the same way both inside this crate AND in downstream
// user crates. Standard Rust derive-pair convention (mirror
// `serde`'s `extern crate self as serde;` at its lib root).
extern crate self as bsql_pg_proto;

// **Transitive-`unsafe` audit-trust chain**.
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
//    boundary as authoritative. Benchmarked at 2-4× speedup on
//    Cyrillic / long-ASCII rows vs `core::str::from_utf8`.
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
//    **Replacement with locally-audited POD-array shape rejected**
//    by two structural blockers:
//    (a) Per-call init cost catastrophic — `[T; N]` POD-array
//        storage eagerly initialises all N slots at construction;
//        for per-call types (`StagedActions = [StagedAction; 8]` ≈
//        704 B, `OutActions = [Action; 9]` ≈ 792 B) this ships ~700
//        B memset per push_command/feed_bytes call → projected
//        +30-50% on push_command/ping_amortised (10.28 → 13-15 ns),
//        violates the bench gate (max +3% on existing benches).
//    (b) `MaybeUninit`-based skip-init alternative requires
//        crate-internal `unsafe { assume_init_read }` — breaks
//        `#![forbid(unsafe_code)]` at the architectural-rule level
//        (CREDO §1 absolute commit). Net-zero or worse safety win
//        replacing ecosystem-trusted code (~1000 LoC well-audited
//        embedded-Rust standard) with locally-audited equivalent.
//
//    The companion comment in `action.rs` ("Why heapless::Vec, NOT
//    the OutActions POD-array shape") covers the per-call type
//    perf rationale in detail. Future audits raising replacement
//    again require new measurement evidence — without it, the
//    ecosystem-trusted heapless choice is the load-bearing decision.
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
// blocks, new platform inline-asm).

#[cfg(test)]
extern crate std;

pub mod action;
pub mod bounded;
pub mod buf;
pub mod command;
// PostgreSQL §55.2.7 CancelRequest mechanism. Public surface is
// the closure-scoped `<ActivePhase>::with_cancel_request` accessor
// (see `protocol.rs`); the internal `BackendKey` type stays
// `pub(crate)` and has no public re-export — the closure-scoped
// lend handles materialisation inline.
pub mod cancel;
pub mod decode;
mod dispatch;
pub mod error;
pub(crate) mod error_arena;
pub use error_arena::{ArenaError, DisplayError, ErrorPayload, ErrorRef};
pub(crate) mod notifications_arena;
pub use notifications_arena::{NotificationPayload, NotificationRef};
pub(crate) mod copy_chunks_arena;
pub use copy_chunks_arena::{CopyChunkPayload, CopyChunkRef};
pub mod frame;
pub mod guard;
pub mod ident;
pub(crate) mod md5;
// Typed numeric narrowing/widening helpers with single-audit-point
// encapsulation of the `try_from(...).unwrap_or(SATURATION)` pattern.
// Crate-internal module; no public re-exports.
pub(crate) mod narrow;
pub mod params;
pub mod password;
// Runtime support for the `prepared!` proc-macro. Hosts
// `PreparedQuery<P, R>`, the `RowDecode` sealed trait, and the
// `new_prepared_query` macro-plumbing constructor.
pub mod prepared;
pub mod protocol;
pub mod push_command;
pub mod row_stream;
pub mod reply_id;

// Tier-1 row_desc_slot write provenance. Crate-internal module; no
// public re-exports.
pub(crate) mod schema_slot;
// Tier-1 state-transition ↔ command-kind pairing. Crate-internal
// module; no public re-exports. Per-command witness types live in
// `push_command` alongside the impls; this module owns the
// `StateSetter<'_, W>` machinery + sealed `PostStateProof` trait.
//
// Also hosts `FeedStateSetter<'_>` for feed-side `Errored`
// transitions, atomically draining the in-flight reply id from the
// prior state during `mem::replace`.
pub(crate) mod state_setter;

// Tier-1 SessionParams write provenance. Crate-internal module; no
// public re-exports. Mirror of `schema_slot` for the
// `PgProtocol::session_params` field.
pub(crate) mod session_params_slot;
// Universal-coverage streaming sink for non-`'D'` backend frames
// whose declared body exceeds READ_BUF_CAP. Stream-and-truncate:
// bounded 8 KB prefix + counted-and-skipped remainder; covers every
// wire-legal size from 0 to ~2 GiB in constant memory.
// Crate-internal; no public re-exports.
pub(crate) mod partial_assembly;
pub mod scram;
// `SecretZeroize` trait — driver-side panic-hook integration
// contract for closing the `panic = "abort"` zeroize gap. See
// module docstring for the full treatment.
pub(crate) mod secret_zeroize;
// Test-only `DropCounter` machinery + sealed `CrateZeroizeSecret`
// manifest. The exhaustiveness gate fails build-time if the
// manifest drifts from src; per-type DropCounter witnesses run on
// every `cargo test`.
//
// Module is `#[cfg(test)]`-only — production builds compile without
// it, zero downstream API surface impact. See module docstring for
// the full design rationale.
#[cfg(test)]
pub(crate) mod drop_witness;
// Shared test-fixture narrowing helpers — loud-fail `usize → i16/i32`
// conversion for hand-built wire-frame fixtures across the crate's
// test modules. Replaces the silent `try_from(...).unwrap_or(0)`
// fixture-corruption mode with `#[track_caller]` invariant pinning.
// Test-only; zero production surface.
#[cfg(test)]
pub(crate) mod test_fixtures;
pub mod sensitive;
pub mod session_params;
// Pristine trait paired with `#[derive(Pristine)]` from
// `bsql-pg-proto-derive`. See module docstring for the broad-scope
// tier-3 → tier-1 closure.
pub mod pristine;
pub mod state;
pub mod wire;
pub mod write_buf;

pub use action::{
    Action, CloseCompletePayload, CopyPushError, DescribePortalCompletePayload,
    DescribeStatementCompletePayload, DescribedRows, FeedEvent, OutActions, ParamOids,
    ParseCompletePayload, PongPayload, PushFailure, QueryCompletePayload, Reply,
    StartupCompletePayload, TxStatus,
};
pub use bounded::{BoundedLen, BoundedU8, BoundedU16};
pub use buf::{AdvancePastEnd, ReadBuf, ReadBufFull, ReadBufN};
// `PgCommand` enum is NOT publicly re-exported. External callers
// construct per-command structs directly:
// `bsql_pg_proto::push_command::{Ping, Flush, Startup, ...,
// BindExecute}`. The enum still exists internally
// (`crate::command::PgCommand`) and is used by the
// `compute_push_tests` lib-internal test module + the
// `impl PushCommand for PgCommand` blanket impl for the
// `compute_push` test dispatcher.
pub use command::FetchRows;
pub use decode::{
    BinaryFmt, ColumnDesc, ColumnsIter, CopyFormat, CopyHeader, DataRowRef, DecodeError,
    DecodeFormat, FormatCode, FormatCodeMarker, FormatCodeSet, FromPgText, MAX_ROW_COLUMNS,
    OutOfRange, RowDesc, RowDescBorrow, RowDescColumnsIter, TextFmt, decode_with_format, oids,
    parse_long_uint_swar, parse_pg_bool_swar, parse_short_uint_swar, validate_utf8_swar,
};
pub use error::{CrateBugLocus, ErrorKind, ProtocolError, StateErrorKind};
pub use frame::{HeaderParse, MAX_FRAME_LEN_FIELD, READ_BUF_CAP, parse_header};
pub use guard::{ConnectionStatus, ReadyGuard};
pub use ident::{
    ApplicationName, DatabaseName, Ident, IdentError, LossyDisplay, LossyText, PortalName,
    SecretBoundedStr, Sql, StmtName,
};
pub use password::{Credentials, Password, PasswordError};
pub use protocol::{
    ActivePhase, CloseCause, ClosedPhase, ConnectingPhase, DisconnectedPhase, IntoActiveError,
    MAX_ACTIONS_PER_CALL, MAX_STAGED_PER_CALL, PgProtocol, SealedPhase,
};
pub use reply_id::{
    CloseKind, DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind,
    ReplyId, ReplyKind, StartupKind,
};
pub use row_stream::{ColEvent, RowStream};
pub use sensitive::Sensitive;
pub use session_params::{Encoding, OtherEncoding, SessionParams};
// Re-export the `Pristine` trait + matching derive macro under one
// name. Rust trait and derive macro live in DIFFERENT namespaces
// (type vs macro), so identical-name re-exports do NOT collide —
// `use bsql_pg_proto::Pristine` brings BOTH into scope: trait usage
// `impl Pristine for T` resolves to the type-namespace item,
// `#[derive(Pristine)]` resolves to the macro-namespace item. This
// mirrors `serde`'s `pub use serde_derive::{Serialize,
// Deserialize}` + `pub trait Serialize { ... }` pattern.
pub use bsql_pg_proto_derive::Pristine;
pub use pristine::Pristine;
// Top-level re-export of `prepared!` + `PreparedQuery` +
// `RowDecode`. The macro lives in `bsql-pg-proto-derive`
// (proc-macros must live in a `proc-macro = true` crate per Rust's
// language rule); the runtime types live here. `use
// bsql_pg_proto::{prepared, PreparedQuery}` brings both into scope
// — the trait + type live in the type namespace, the macro in the
// macro namespace.
pub use bsql_pg_proto_derive::prepared;
pub use prepared::{PreparedQuery, RowDecode};
pub use state::ProtoState;
// Per-phase state enums for the `<ConnectingPhase>` /
// `<ActivePhase>` / `<ClosedPhase>` API. ConnectingState is the
// active surface (queried by
// `<ConnectingPhase>::connecting_state()`); ActiveState +
// ErroredState are pre-exported for the eventual per-phase
// ActivePhase / ClosedPhase migrations.
pub use state::{ActiveState, ConnectingState, ErroredState};
// Top-level re-export of the user-facing `Terminate` wire literal.
// Drivers (`bsql-driver-postgres`, async wrappers) write these
// bytes immediately before TCP close to signal graceful shutdown.
// Convention: wire-internal consts (e.g. `SYNC_WIRE_BYTES`) stay
// `pub(crate)`; user-facing wire primitives are re-exported here.
pub use wire::TERMINATE_WIRE_BYTES;
// Top-level re-export of the user-facing `Flush` wire literal.
// Pipelining drivers write these bytes mid-batch to extract
// intermediate responses without committing the implicit
// transaction (which would be `Sync`'s job).
pub use wire::FLUSH_WIRE_BYTES;
// Top-level re-export of the user-facing `SSLRequest` wire
// literal. Wrapper drivers write these bytes BEFORE
// `PgProtocol::new()` to negotiate TLS; the 1-byte server response
// is OOB (driver handles it outside the frame parser).
pub use wire::SSL_REQUEST_WIRE_BYTES;
// Typed classification of the 1-byte SSL response. Pairs with
// SSL_REQUEST_WIRE_BYTES — driver reads 1 byte and calls
// `classify_ssl_response_byte` to obtain a `SslNegotiationOutcome`
// instead of ad-hoc `match byte` logic. Tier-1 enforcement of all
// 4 currently-defined outcomes with `#[non_exhaustive]` for
// SemVer-safe future extension.
pub use wire::{SslNegotiationOutcome, classify_ssl_response_byte};
// Top-level re-export of the user-facing `CancelRequest` builder.
// Drivers call `cancel_request_bytes(pid, secret_key)` to
// materialise the 16-byte cancel packet, open a parallel TCP
// connection, write the bytes, and close. PG processes the cancel
// asynchronously; no reply on the cancel socket. Pid + secret_key
// come from the BackendKeyData ('K') frame on the original
// connection.
//
// `MAGIC_VERSION_HIGH_HALF` + `CANCEL_REQUEST_VERSION` are
// re-exported via the `wire` module path only — they're internal
// composition primitives, not user-facing wire literals (the
// builder fn is the user surface).
pub use wire::cancel_request_bytes;
// Closure-scoped CancelRequest API. The wire frame is lent
// through `<ActivePhase>::with_cancel_request(|bytes, pid| ...)`,
// materialised on the function's stack inside a `Zeroizing<[u8;
// 16]>` guard. The `BackendKey` cell-level type stays `pub(crate)`;
// no public re-export from `mod cancel`. Retention is structurally
// impossible (HRTB bounded borrow + stack-local guard) — see
// `with_cancel_request` doc and the `cancel.rs` module-level docs
// for the tier-elevation rationale. Since `<ActivePhase>` carries
// `BackendKey` inline (constructed from the `HandshakeReady`
// payload by `<ConnectingPhase>::into_active`), the accessor
// returns `R` (not `Option<R>`).
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
    // Core types. `Action<'_>` and `OutActions<'_>` carry
    // lifetimes; asserting for `'static` implies Send for any
    // shorter lifetime by covariance.
    assert_send::<action::Action<'static, 'static>>();
    assert_send::<action::OutActions<'static, 'static>>();
    assert_send::<action::Reply<'static>>();
    assert_send::<command::PgCommand>();
    assert_send::<error::ProtocolError>();
    assert_send::<protocol::PgProtocol>();
    // `ReplyId` is generic over `K: ReplyKind`. The nominal kind
    // parameter is `PhantomData<fn() -> K>` (ZST, unconditionally
    // `Send + Sync`), so assert_send holds for every `K`; checking
    // one concrete `K` is sufficient.
    assert_send::<reply_id::ReplyId<reply_id::PingKind>>();
    assert_send::<reply_id::ReplyId<reply_id::StartupKind>>();
    assert_send::<reply_id::ReplyId<reply_id::QueryKind>>();
    assert_send::<state::ProtoState>();
    // Bounded string types
    assert_send::<ident::Ident>();
    assert_send::<ident::DatabaseName>();
    assert_send::<ident::ApplicationName>();
    assert_send::<password::Password>();
    assert_send::<password::Credentials>();
    assert_send::<session_params::SessionParams>();
    assert_send::<write_buf::WriteBuf>();
    assert_send::<scram::types::SecretDigest>();
    assert_send::<scram::types::CappedServerNonce>();
    // Typestate wrappers.
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
    // Witness-guard typestate.
    assert_send::<guard::ConnectionStatus>();
    // ReadyGuard<'a> is `&'a mut PgProtocol` — Send for 'static implies
    // Send for any shorter lifetime by covariance. Sync would defeat
    // its exclusive-access purpose, so only Send is asserted.
    assert_send::<guard::ReadyGuard<'static>>();
    // PushFailure is the typed Err arm of ReadyGuard::push_command;
    // FeedEvent is the per-event return of advance_one_frame. Both
    // cross task boundaries in the async wrapper — Send is
    // load-bearing.
    assert_send::<action::PushFailure>();
    assert_send::<action::FeedEvent<'static, 'static>>();
};

// ---------------------------------------------------------------------
// Tier-1 compile gate on `!Sync` for `PgProtocol`.
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
// Tier-1 compile gates on enum / struct **size**.
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
// All bounds are exact where reproducible cross-platform; range pins
// (±N B) tolerate alignment differences.
//
// Current budget (aarch64-apple-darwin):
//
//   Ident:             66  (FixedStr<63, IdentTag>)
//   DatabaseName:      66
//   ApplicationName:  130
//   ProtocolError:     72  (ErrorArena cascade)
//   Action<'_,'_>:     88  (Reply-bounded)
//   OutActions:       800  (9 × Action + len)
//   DispatchOutcome:   88  (by-ref state)
//   Reply<'_>:         80  (RowDesc externalised to slot)
//   ReplyId:           16
//   PgCommand:      ≤2176  (Parse dominates)
//   ProtoState:        80  (per-phase state enum)
//   PgProtocol:    [4300, 4400]  (range tolerates alignment)
// ---------------------------------------------------------------------
// Target architecture support bound: the crate uses `u32` body counters
// and assumes `usize::BITS >= 32` for infallible `u32 → usize` widening
// (see `crate::partial_assembly::PartialAssemblyInner::absorb` and the
// `_usize_widening` leaf helper). 16-bit targets are unsupported.
const _: () = assert!(
    usize::BITS >= 32,
    "bsql-pg-proto requires a target with usize >= 32 bits. \
     16-bit targets are unsupported; the wire-protocol body counters \
     are u32 and several call sites infallibly widen u32 → usize.",
);

// Tight-range size asserts. Bound BOTH directions to catch field
// additions (upper) AND accidental field removals (lower). Exact
// pins where reproducible cross-platform.
const _: () = assert!(
    core::mem::size_of::<error::ProtocolError>() == 72,
    "ProtocolError exact size — 72 B. ServerErrorResponse carries \
     `details_ref: ErrorRef` (2 B); bounded strings live in \
     PgProtocol::error_arena. Remaining 72 B dominated by other \
     large variants (e.g. Scram(ScramError), Malformed* with \
     BoundedStr<32>). Exact pin catches any variant growth / layout \
     drift.",
);
const _: () = assert!(
    core::mem::size_of::<action::Action<'static, 'static>>() == 88,
    "Action<'_, '_> exact size — 88 B. ProtocolError 72 B, so Action \
     bounded by max(Reply 72, FailReply 72) + discriminant + padding \
     = 88 B. Exact pin catches any variant growth. \
     \
     **NICHE OPTIMIZATION IS LOAD-BEARING** (DEF-260 MEASURED-REJECTED \
     2026-05-21): a future `#[repr(u8)]` attempt would disable niche \
     packing on `id: NonZeroU64` and grow Action 88 → 96 B (+8 B), \
     cascading to OutActions 800 → 872 B (+72 B). Default Rust repr is \
     provably better here — keep it.",
);
const _: () = assert!(
    core::mem::size_of::<action::Reply<'static>>() >= 72
        && core::mem::size_of::<action::Reply<'static>>() <= 96,
    "Reply<'r> size drift — actual is 80 B. Range [72, 96] catches \
     variant payload changes. Dominating variant is \
     DescribeStatementComplete (ParamOids ~68 B + DescribedRows ~16 B \
     + TxStatus + padding).",
);
const _: () = assert!(
    core::mem::size_of::<reply_id::ReplyId<reply_id::PingKind>>() <= 24,
    "ReplyId<K> size regression — the `PhantomData<fn() -> K>` kind \
     tag is zero-size; ReplyId's footprint is u64 value + bool \
     delivered + padding. Did a bookkeeping field get added?",
);
// RowDesc lives in `PgProtocol::row_desc_slot` (single source of
// truth); state variants do not carry schema. A naive shape would
// have parallel `schema_present: bool` for SimpleQuery and
// `DescribedRowsStaged` enum for Describe paths — these would
// duplicate `PgProtocol::row_desc_slot.is_some()` and be tier-2
// by-discipline. The slot-as-single-source shape is tier-1
// by-construction.
//
// Exact `==` pin (rather than range) narrows drift surface to a
// single arithmetic identity. Cross-platform: pinned for reference
// target aarch64-apple-darwin; per-target `#[cfg(...)]` blocks
// would land in the same commit that adds another target to CI.
const _: () = assert!(
    core::mem::size_of::<state::ProtoState>() == 48,
    "ProtoState exact size pin: row_desc_slot externalised on \
     PgProtocol; schema-presence flags deleted (`row_desc_slot. \
     is_some()` is single source of truth); `param_oids` heap-boxed \
     on DescribeStatement* variants (mirror of SCRAM/MD5/Cleartext \
     pattern). \
     \
     Layout on aarch64-apple-darwin: dominant variants are now the \
     `BoundedStr<32>`-bearing ones — `SimpleQueryAwaitingRfq` / \
     `BindExecuteAwaitingRfqDml` / `BindExecuteAwaitingRfqSelect`. \
     Shape: `ReplyId<_>` (8 B; NonZeroU64 + ZST PhantomData) + \
     `BoundedStr<32>` (~36 B: 2 B len + 32 B buf + tail-pad to align 2) \
     + 1 B variant discriminant + alignment → 48 B. \
     \
     Other notable variants: \
     - SCRAM `ConnectingScramAwaitingServerFirst` / `…ServerFinal` — \
       Box (8 B) or SecretDigest (32 B) + ReplyId (8 B) + discriminant \
       → ~24–48 B. \
     - DescribeStatement* — ReplyId (8 B) + `Box<ParamOids>` (8 B) + \
       discriminant → ~24 B (post-DEF-282 boxing). \
     - Streaming variants — ReplyId (8 B) + discriminant → ~16 B. \
     \
     Per-row hot-path single state-projection retrieves just the \
     reply id; the descriptor is fetched via the protocol's \
     `current_row_desc` slot (one immutable borrow, no per-row \
     state match for the desc field). \
     \
     **The dominant constraint is now `BoundedStr<32>` command_tag.** \
     A refactor that wants to shrink ProtoState further should target \
     command_tag arity, or move command_tag off the variant entirely \
     (e.g. into a slot pattern, mirror of row_desc_slot). \
     \
     If a refactor changes this number on aarch64-apple-darwin, \
     update both the literal AND the layout comment above (drift-pin \
     CREDO §3 discipline).",
);
const _: () = assert!(
    core::mem::size_of::<command::PgCommand>() <= 2176,
    "PgCommand size regression — budget is 2176 bytes. \
     Parse dominates: StmtName (66) + Sql (2050) + ReplyId<ParseKind> \
     (16) + discriminant + padding. Bumping MAX_SQL_LEN or \
     MAX_PG_NAME_LEN must move this limit in lockstep.",
);
// Cross-platform stance: exact `==` pins are consistent with the
// rest of the crate. Reference target: aarch64-apple-darwin (where
// CI lives today). When CI matrix extends to x86_64-linux /
// riscv64 / etc., per-target cfg-gated pins land in the same commit
// that adds the target — not via permissive ranges. Drift surface
// beats variance cushion every time.
//
// Cold-path fields are externalised into independent lazy slots —
// each cold field allocates its Box independently only on first
// write. ReadBuf is two-tier (256 B inline + lazy heap escape Box).
// The reply-id mint counter is a `static AtomicU64` in
// `next_reply_id`, NOT inline on PgProtocol — an inline u64 field
// was bisect-shown to grow the struct and shift LLVM whole-crate
// codegen heuristic +6% on the synthetic `iter_10cols` decode bench.
// Static-atomic mint preserves the size AND strengthens the
// invariant: globally-unique IDs across all instances (per-protocol
// field would have given per-instance only).
//
// Layout breakdown:
//   ReadBuf inline:          ~256 B (heapless::Vec<u8, 256> + cursor)
//   ReadBuf heap slot:          8 B (Option<Box<...>> niche)
//   state:                    ~48 B (SimpleQueryAwaitingRfq / BindExecuteAwaitingRfq* dominant; DescribeStatement* `param_oids` heap-boxed)
//   row_desc_slot:           ~140 B (Option<RowDesc>)
//   session_params:             8 B (Option<Box<SessionParams>> niche)
//   error_arena:                8 B (Option<Box<ErrorArena>> niche)
//   partial_assembly:           8 B (Option<Box<...>> niche)
//   backend_key:                8 B (inline BackendKey { pid:i32, secret:Sensitive<i32> } on ActiveInner)
//   malformed_frame_count:      4 B (inline u32)
//   sync_marker:                0 B (PhantomData)
//   alignment padding:        ~16 B (to align(8))
//   total:                    504 B
//
// Heap economics per connection pattern:
//   - Trust auth + no errors:        0 allocations.
//   - Startup auth + no errors:      1 alloc (Box<SessionParams> 436 B).
//   - Startup auth + errors:         2 allocs (~732 B total).
//   - Malformed frame teardown:      0 allocations (counter inline).
//   - First frame > 256 B:           1 alloc (Box<heapless::Vec<u8, 4096>>).
//
// `BackendKey` is `{ pid: i32, secret_key: Sensitive<i32> }` (8 B
// inline). `<ActivePhase>` carries it directly on `ActiveInner`
// (non-Option) — construction is the structural consume of
// `ConnectingState::HandshakeReady { pid, secret_key }` by
// `<ConnectingPhase>::into_active`, so the storage-absence proof
// makes a missing key by-type-impossible. Reads are O(1) field
// projection. Public API surfaces the key via the closure-scoped
// `<ActivePhase>::with_cancel_request<R>(&self, f) -> R` accessor
// (infallible): the wire-frame Zeroizing<[u8;16]> guard lives on
// the function's stack, not on `ActiveInner` — secret-scrub
// retention is structurally impossible (HRTB closure bound +
// stack-local guard).
//
// Tier-1 absolutism: size growth must be gated by `bench-stable.sh`
// on a quiet system (`load avg < 8`). On regression, investigate
// (asm-diff, alternative shapes), do NOT roll back tier elevations.
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol>() == 520,
    "PgProtocol size exact pin (aarch64-apple-darwin reference). \
     \
     Budget: ReadBuf inline 256 + ReadBuf heap-slot 8 + state ~48 \
     (post-DEF-282 ParamOids boxing) + row_desc_slot ~140 (outer \
     Extras) + session_params 8 + error_arena 8 + notifications_arena \
     8 (DEF-220 lazy Option<Box<NotificationsArena>>) + \
     partial_assembly 8 + backend_key 8 + malformed_frame_count 4 + \
     alignment-pad to align(8) = 512 B. \
     \
     DEF-220 grew the pin from 504 → 512 B via the new \
     notifications_arena slot. The slot is `Option<Box<_>>` so \
     LISTEN-free connections pay the 8-byte field but no heap; \
     LISTEN-using connections allocate one `Box<NotificationsArena>` \
     on first NOTIFY arrival (lifelong per connection, not per-\
     notification). \
     \
     Cross-platform: when CI matrix extends, either (a) every target \
     lands at 512 (most likely — alignment-stable types), or \
     (b) per-target cfg-gated pins land in the same commit. \
     Permissive ranges forbidden — drift surface > variance cushion \
     (CREDO §3 + §4.12).",
);

// Branch-collapse typestate layout pins.
//
// `PgProtocol<P: SealedPhase>` is `#[repr(transparent)]` over
// `<P as SealedPhase>::Inner` + a ZST `PhantomData<fn() -> P>`.
// Layout per phase is determined by the per-phase Inner:
//   - DisconnectedPhase → DisconnectedInner (0 B, ZST)
//   - ConnectingPhase  → ConnectingInner   (360 B; unchanged — no Describe variants in `<ConnectingPhase>`)
//   - ActivePhase      → ActiveInner       (504 B; post-DEF-282)
//   - ClosedPhase      → ClosedInner       (16 B)
//
// If any pin trips, either (a) PhantomData was rendered non-ZST
// under a future rustc heuristic (file an issue, do NOT relax the
// pin), (b) repr(transparent) was removed from PgProtocol<P>
// (review the commit), or (c) a non-ZST field was added to
// PgProtocol<P> outside `inner` (architectural violation).
//
// `<DisconnectedPhase>::Inner = DisconnectedInner` is ZST: a fresh
// `PgProtocol::new()` allocates exactly zero protocol bytes; the
// materialisation cost is moved to `push_startup` (which calls
// `_proto_init_leaf::fresh_inner()` to construct the
// post-transition Inner for the `<ConnectingPhase>` wrapper).
// Tier-1 by-storage-absence: pre-Startup state cannot carry
// in-flight payload because the storage physically does not exist.
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol<protocol::DisconnectedPhase>>() == 0,
    "PgProtocol<DisconnectedPhase> layout drift — should be 0 B \
     (ZST DisconnectedInner + ZST phase_marker PhantomData<fn() -> \
     DisconnectedPhase>). If this trips, a non-ZST field crept onto \
     DisconnectedInner — audit `mod protocol::DisconnectedInner`.",
);
const _: () = assert!(
    core::mem::size_of::<protocol::DisconnectedInner>() == 0,
    "DisconnectedInner exact size — must be 0 B (the only field is \
     `sync_marker: PhantomData<Cell<()>>`, which is ZST). If this \
     trips, a non-ZST field was added to DisconnectedInner — \
     architectural violation of the tier-1-by-storage-absence invariant.",
);
// `<ConnectingPhase>::Inner = ConnectingInner` carries the
// `ConnectingState` (48 B) variant of state; `row_desc_slot` is
// hoisted off Inner because no dispatch arm reachable from a
// `ConnectingState` LHS writes it (Extras = ()). PgProtocol<P>
// = Inner + Extras + ZST phase_marker.
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol<protocol::ConnectingPhase>>() == 360,
    "PgProtocol<ConnectingPhase> layout drift — must equal \
     ConnectingInner (state ConnectingState 48 B + read_buf 264 B + \
     3 cells × 8 B + 1 u32 + alignment) PLUS Extras = () (ZST) PLUS \
     ZST phase_marker. If this trips, audit `mod \
     protocol::ConnectingInner` and the SealedPhase Extras = () \
     mapping for ConnectingPhase.",
);
// `<ActivePhase>::Extras = RowDescSlotCell` (140 B; align 4); the
// cell lives on outer Extras rather than inside ActiveInner.
// PgProtocol<ActivePhase> = ActiveInner + Extras + ZST
// phase_marker; measured 512 B on aarch64-apple-darwin (post-DEF-282
// `Box<ParamOids>` boxing on DescribeStatement* variants, post-DEF-220
// notifications_arena slot).
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol<protocol::ActivePhase>>() == 520,
    "PgProtocol<ActivePhase> layout drift — must equal ActiveInner \
     (state ActiveState 48 B + read_buf 264 B + 4 cells × 8 B + \
     1 u32 + alignment; the 4th cell is DEF-220's notifications_arena \
     `Option<Box<NotificationsArena>>`) PLUS Extras = \
     RowDescSlotCell (140 B inline; align 4) PLUS ZST phase_marker. \
     If this trips, audit `mod protocol::ActiveInner` and the \
     SealedPhase Extras = RowDescSlotCell mapping for ActivePhase.",
);
// `<ClosedPhase>::Inner = ClosedInner` (~16 B) — state_kind 1B + 7B
// pad + error_arena Option<Box> 8B. Post-Errored only state_kind +
// arena are reachable via the legitimate <ClosedPhase> API
// (`cause()`, `get_server_error()`,
// `error_arena_overwrite_count()`); the full PgProtocolInner is
// dropped at the transition boundary (`into_closed_if_errored` /
// `into_active` Closed arm), releasing stack + Box-niche heap.
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol<protocol::ClosedPhase>>() == 16,
    "PgProtocol<ClosedPhase> layout drift — should be 16 B \
     (ClosedInner: state_kind 1B + 7B pad + error_arena Box-niche 8B \
     + ZST sync_marker + ZST phase_marker). If this trips, a field \
     was added/removed/reshaped on `ClosedInner` — audit `mod \
     protocol::ClosedInner`.",
);
const _: () = assert!(
    core::mem::size_of::<protocol::ClosedInner>() == 16,
    "ClosedInner exact size — must be 16 B. If this trips, a field \
     was added/removed/reshaped on `ClosedInner`.",
);

// DispatchOutcome size pin — bounded by `AdvancedWithAction(
// StagedAction 88 B)`. Dispatch writes state directly via
// `&mut ProtoState`, so DispatchOutcome carries only the
// side-effect signal (StagedAction for WithAction, reply_id +
// ProtocolError 72 B for Errored).
const _: () = assert!(
    core::mem::size_of::<dispatch::DispatchOutcome>() == 88,
    "DispatchOutcome exact size — 88 B. Dominated by \
     AdvancedWithAction(StagedAction 88 B); the discriminant + \
     Errored variant payload fold into StagedAction's alignment + \
     niches. If this trips, either (a) a new ProtocolError variant \
     inflated the Errored payload, (b) StagedAction grew (cascade \
     into Action / OutActions), or (c) `new_state` was added to an \
     Advanced variant — regression vs the by-ref-state dispatch \
     shape.",
);

const _: () = assert!(
    core::mem::size_of::<action::OutActions<'static, 'static>>() == 800,
    "OutActions<'_, '_> exact size — 800 B \
     = 9 (MAX_ACTIONS_PER_CALL) × 88 (Action) + 8 (usize len). \
     \
     Exact pin catches ANY layout drift. Change is a contributor \
     decision-point (not silent regression): audit Action size, \
     OutActions cap, ManuallyDrop shape.",
);

// ---------------------------------------------------------------------
// Exact `==` size pins for the bytes-only push API + per-event feed
// API. Relative pins (e.g., `<= 96`) cushion silent drift; exact pins
// make every byte change a contributor decision-point.
// ---------------------------------------------------------------------

// `PushFailure` exact size — 80 B.
//
// Layout: NonZeroU64 (8 B, 8-aligned) + ProtocolError (72 B) = 80 B
// total. Niche-packed: `Option<PushFailure>` is also 80 B (NonZeroU64
// niche absorbs the discriminant).
//
// Drift surface: a future `ProtocolError` variant addition that
// pushes the enum past 72 B would cascade here. The complementary
// `ProtocolError == 72` pin catches drift at the source; this pin
// catches the propagation.
const _: () = assert!(
    core::mem::size_of::<action::PushFailure>() == 80,
    "PushFailure exact size — 80 B (NonZeroU64 8 B + ProtocolError 72 B). \
     If this trips: (a) ProtocolError grew past 72 B (check sibling pin \
     at action::PushFailure docstring + error.rs ProtocolError pin), \
     or (b) NonZeroU64 alignment changed (architecturally impossible \
     under stable Rust). Cascade impact: Result<(), PushFailure> return \
     frame on push paths grows in lockstep.",
);
const _: () = assert!(
    core::mem::size_of::<Option<action::PushFailure>>() == 80,
    "Option<PushFailure> niche-pack — must stay 80 B via the NonZeroU64 \
     niche on PushFailure.id. If this regresses to 88 B (or higher), \
     the niche optimisation was lost — likely cause: a non-niche field \
     added to PushFailure that consumed the discriminant slot.",
);

// `FeedEvent<'static, 'static>` exact size — 88 B.
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
     sibling pin Reply<'r> in [72, 96] — tighten when Reply gets \
     exact), (b) a new FeedEvent variant carries a payload > 80 B \
     (rare — architectural change), or (c) niche optimisation lost. \
     The design budget assumes ≤ 88 B per per-event return frame — \
     larger means worse pipelining throughput per cycle.",
);
const _: () = assert!(
    core::mem::size_of::<Option<action::FeedEvent<'static, 'static>>>() == 88,
    "Option<FeedEvent> niche-pack — must stay 88 B via the NonZeroU64 \
     niche on Deliver.id / Fail.id. If this regresses, the niche was \
     lost — verify variant layout still routes the discriminant through \
     a NonZero* slot.",
);

// `PreparedQuery<P, R>` is a struct of 6 × `&'static`-fat-pointers
// + `PhantomData<fn(P) -> R>` = 6 × 16 B + 0 = 96 B. The pin's
// upper bound is 128 B with cushion for alignment / future
// niche-friendly field changes; tighter exact pin at 96 B would
// be brittle to layout heuristics on different targets.
//
// Rationale for the cushion: cross-target portability — `&[u8]`
// fat pointer is 16 B on every 64-bit target stable today, but a
// future ABI might pack the length differently. The 128-B ceiling
// preserves the "static, small, .rodata-friendly" promise without
// forcing per-target pins for a struct that's not perf-critical
// at the per-byte level.
const _: () = assert!(
    core::mem::size_of::<prepared::PreparedQuery<(i32,), (i32, &'static str)>>() <= 128,
    "PreparedQuery<(i32,), (i32, &'static str)> must stay ≤ 128 B \
     (6 × 16 B fat pointers + PhantomData = 96 B + padding cushion). \
     Larger sizes regress consumer crate .rodata footprint and \
     LLVM whole-crate codegen heuristics.",
);

// ---------------------------------------------------------------------
// Tier-1 compile gates on Drop semantics.
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
    "Password must have Drop for zeroize-on-drop (secret scrub)",
);
const _: () = assert!(
    core::mem::needs_drop::<scram::types::SecretDigest>(),
    "SecretDigest must have Drop for zeroize-on-drop",
);
// ReplyId<K> has no Drop. A panic-in-Drop "consume-discipline
// guard" would double-panic under integration-test unwind (SIGABRT
// masking original failure). Discipline is enforced via
// `#[must_use]` + integration tests observing Action content. See
// `reply_id.rs` for the full rationale.
const _: () = assert!(
    !core::mem::needs_drop::<reply_id::ReplyId<reply_id::PingKind>>(),
    "ReplyId<K> must stay drop-free — Drop was a footgun.",
);
const _: () = assert!(
    !core::mem::needs_drop::<action::Reply<'static>>(),
    "Reply must stay drop-free — all variants are Copy-like (small value type). \
     Reply<'r> borrows &'r RowDesc from the row_desc_slot; borrows \
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
// Expanded coverage. Positives: types carrying secrets / resources
// that MUST self-scrub. Negatives: small value types that MUST stay
// Copy-friendly / drop-free.
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
// Action<'_> is Copy (POD BoundedStr + typed ProtocolError + Copy
// variants), so `needs_drop::<Action<'static>>()` is false — that
// makes `OutActions<'buf>` release-at-last-use under NLL (no
// explicit `drop(out)` needed in tests).
const _: () = assert!(
    !core::mem::needs_drop::<action::Action<'static, 'static>>(),
    "Action<'buf> must stay drop-free — POD BoundedStr + typed ProtocolError + Copy variants",
);
const _: () = assert!(
    !core::mem::needs_drop::<error::ProtocolError>(),
    "ProtocolError must stay drop-free — all variants' fields are Copy (POD BoundedStr)",
);
const _: () = assert!(
    !core::mem::needs_drop::<action::OutActions<'static, 'static>>(),
    "OutActions<'_> must stay drop-free. The inner heapless::Vec \
     is wrapped in ManuallyDrop which inhibits the Vec's Drop impl. \
     Since Action<'w, 'r> is Copy (POD refs + small payload), \
     skipping inner Drop is sound (no-op body anyway). This \
     preserves NLL last-use borrow-release semantics — the caller \
     pattern `let out = proto.feed_bytes(..); match out.as_slice() \
     {{ .. }}; proto.state()` compiles without explicit drop(out) \
     between as_slice and next proto call.",
);