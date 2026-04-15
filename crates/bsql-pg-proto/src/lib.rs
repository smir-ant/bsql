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
//! - [`frame`] — pure-function frame-header parser. Tested in isolation;
//!   never panics on arbitrary bytes (tier-3, verified by randomized
//!   fuzz harness).
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
pub mod protocol;
pub mod reply_id;
pub mod state;
pub mod wire;

pub use action::{Action, OutActions, Reply, SendBuf};
pub use buf::{AdvancePastEnd, ReadBuf, ReadBufFull};
pub use command::PgCommand;
pub use error::ProtocolError;
pub use frame::{HeaderParse, MAX_FRAME_LEN_FIELD, READ_BUF_CAP, parse_header};
pub use protocol::{MAX_ACTIONS_PER_CALL, PgProtocol};
pub use reply_id::ReplyId;
pub use state::ProtoState;
