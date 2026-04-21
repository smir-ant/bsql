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
    clippy::integer_division
)]
#![deny(
    unused_must_use,
    unused_lifetimes,
    unused_variables,
    missing_docs,
    rust_2024_incompatible_pat
)]
#![warn(missing_debug_implementations, missing_copy_implementations)]

#[cfg(test)]
extern crate std;

pub mod action;
pub mod buf;
pub mod command;
pub mod decode;
mod dispatch;
pub mod error;
pub mod frame;
pub mod ident;
pub mod params;
pub mod password;
pub mod protocol;
pub mod reply_id;
pub mod scram;
mod schema_arena;
pub mod sensitive;
pub mod session_params;
pub mod state;
pub mod wire;
pub mod write_buf;

pub use action::{
    Action, CloseCompletePayload, DescribePortalCompletePayload,
    DescribeStatementCompletePayload, DescribedRows, OutActions, ParamOids,
    ParseCompletePayload, PongPayload, QueryCompletePayload, Reply,
    StartupCompletePayload, TxStatus,
};
pub use buf::{AdvancePastEnd, ReadBuf, ReadBufFull};
pub use command::{FetchRows, PgCommand};
pub use decode::{
    ColumnDesc, ColumnsIter, DataRowRef, DecodeError, FormatCode, FromPgText, MAX_ROW_COLUMNS,
    RowDesc, oids,
};
pub use error::{ErrorKind, FrameBuildStage, ProtocolError, StateErrorKind};
pub use frame::{HeaderParse, MAX_FRAME_LEN_FIELD, READ_BUF_CAP, parse_header};
pub use ident::{
    ApplicationName, DatabaseName, Ident, IdentError, PortalName, Sql, StmtName,
};
pub use password::{Credentials, Password, PasswordError};
pub use protocol::{MAX_ACTIONS_PER_CALL, PgProtocol};
pub use reply_id::{
    CloseKind, DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind,
    ReplyId, ReplyKind, StartupKind,
};
pub use sensitive::Sensitive;
pub use session_params::{Encoding, OtherEncoding, SessionParams};
pub use state::ProtoState;
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
// Measurement baseline (x86_64 Linux, 2026-04-20):
//   ProtocolError:   856  (five heapless::String<N>, N<=256)
//   Action:          864  (SendBuf::Owned contains 512-byte vec)
// Post-DEF-095/096/097 measurements (aarch64-apple-darwin):
//   Ident:            66  (was 72 — heapless::Vec<u8,63>+usize → POD FixedStr)
//   DatabaseName:     66
//   ApplicationName: 130
//   ProtocolError:   304  (DEF-060 typed variants + FixedStr tail)
//   ReplyId:          16
//   PgCommand:      2136  (Parse dominates: StmtName + Sql + ReplyId)
// Post-DEF-119 measurements (aarch64-apple-darwin, 2026-04-21):
//   Reply<'_>:        80  (was ~340 — RowDesc externalised to arena)
//   Action<'_,'_>:   312  (was ~384 — FailReply.cause now dominant)
//   OutActions:     2504  (was ~3072 — 8 × Action shrunk)
//   ProtoState:     1224  (unchanged — SCRAM dominant, still 1224)
//   PgProtocol:     6272  (added 528 B arena, other shrinkage offset net)
// ---------------------------------------------------------------------
const _: () = assert!(
    core::mem::size_of::<error::ProtocolError>() <= 312,
    "ProtocolError size regression — post-DEF-060/061/096 budget is 312 bytes. \
     Did ServerErrorResponse.message/detail/hint bounds grow, or did a \
     variant add a large inline buffer?",
);
const _: () = assert!(
    core::mem::size_of::<action::Action<'static, 'static>>() <= 320,
    "Action<'_, '_> size regression — post-DEF-119 budget is 320 bytes. \
     DEF-119 externalised RowDesc to the schema arena: Reply<'r> \
     shrunk from ~340 B to ~80 B, making FailReply.cause (ProtocolError \
     ~304 B) the new dominant variant. `#[expect(large_enum_variant)]` \
     acknowledges FailReply as the cold-path dominator. If this trips, \
     check whether ProtocolError grew or a new variant added a large \
     inline buffer.",
);
const _: () = assert!(
    core::mem::size_of::<action::Reply<'static>>() <= 96,
    "Reply<'r> size regression — post-DEF-119 budget is 96 bytes. \
     DEF-119 replaced inline RowDesc (~264 B) with `&'r RowDesc` \
     borrowed from the schema arena — Reply shrunk from ~340 B to \
     ~80 B. Dominating variant is now DescribeStatementComplete \
     (ParamOids ~68 B + DescribedRows ~16 B + TxStatus + padding). \
     If this trips, check ParamOids capacity or Reply variant growth.",
);
const _: () = assert!(
    core::mem::size_of::<reply_id::ReplyId<reply_id::PingKind>>() <= 24,
    "ReplyId<K> size regression — the `PhantomData<fn() -> K>` kind \
     tag is zero-size; ReplyId's footprint is u64 value + bool \
     delivered + padding. Did a bookkeeping field get added?",
);
const _: () = assert!(
    core::mem::size_of::<state::ProtoState>() <= 1248,
    "ProtoState size regression — post-DEF-099 budget is 1248 bytes \
     (Scram path dominant at ~1224; Trust path just 24 bytes). DEF-119 \
     shrunk RowDesc-carrying variants (6 sites × ~260 B → 1 B SchemaRef) \
     but didn't touch the SCRAM dominator. Did a state variant add a \
     large buffer?",
);
const _: () = assert!(
    core::mem::size_of::<command::PgCommand>() <= 2176,
    "PgCommand size regression — post-1c-3a budget is 2176 bytes. \
     Parse dominates: StmtName (66) + Sql (2050) + ReplyId<ParseKind> \
     (16) + discriminant + padding. Bumping MAX_SQL_LEN or \
     MAX_PG_NAME_LEN must move this limit in lockstep.",
);
const _: () = assert!(
    core::mem::size_of::<protocol::PgProtocol>() <= 6336,
    "PgProtocol size regression — post-DEF-119 budget is 6336 bytes. \
     DEF-119 added the 528 B schema arena (SchemaSlab: 2 slots × \
     ~264 B). Other shrinkage (RowDesc out of state variants) offset \
     the addition — net size ~6272 B. Budget: ReadBuf 4096 + state \
     ~1224 + session_params ~420 + schema_arena 528 + padding.",
);
const _: () = assert!(
    core::mem::size_of::<action::OutActions<'static, 'static>>() <= 2560,
    "OutActions<'_, '_> size regression — post-DEF-119 budget is 2560 \
     bytes. 8 × sizeof(Action<'_, '_>) (now 312 B) + u8 len + padding \
     ≈ 2504 B. DEF-119 shrunk Action from 384 → 312 B by externalising \
     RowDesc from Reply; OutActions shrunk correspondingly.",
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
const _: () = assert!(
    core::mem::needs_drop::<reply_id::ReplyId<reply_id::PingKind>>(),
    "ReplyId<K> must have Drop for the consume-discipline guard (DEF-028 / \
     DEF-101 — same guarantee, now per-kind after DEF-112's type \
     parameterisation).",
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
    "OutActions<'_> must stay drop-free — custom POD array (not heapless::Vec); \
     this is what lets NLL release borrows at last use (no `drop()` calls needed in tests).",
);
