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
mod dispatch;
pub mod error;
pub mod frame;
pub mod ident;
pub mod password;
pub mod protocol;
pub mod reply_id;
pub mod scram;
pub mod sensitive;
pub mod session_params;
pub mod state;
pub mod wire;
pub mod write_buf;

pub use action::{Action, OutActions, Reply, SendBuf};
pub use buf::{AdvancePastEnd, ReadBuf, ReadBufFull};
pub use command::PgCommand;
pub use error::ProtocolError;
pub use frame::{HeaderParse, MAX_FRAME_LEN_FIELD, READ_BUF_CAP, parse_header};
pub use ident::{ApplicationName, DatabaseName, Ident, IdentError};
pub use password::{Credentials, Password, PasswordError};
pub use protocol::{MAX_ACTIONS_PER_CALL, PgProtocol};
pub use reply_id::ReplyId;
pub use sensitive::Sensitive;
pub use session_params::SessionParams;
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
    // Phase 1a types
    assert_send::<action::Action>();
    assert_send::<action::OutActions>();
    assert_send::<action::Reply>();
    assert_send::<action::SendBuf>();
    assert_send::<command::PgCommand>();
    assert_send::<error::ProtocolError>();
    assert_send::<protocol::PgProtocol>();
    assert_send::<reply_id::ReplyId>();
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
