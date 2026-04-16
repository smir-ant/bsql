//! Protocol state — state-as-data.
//!
//! Each variant carries its in-flight correlator inline (reforge.md
//! §7.2). Consequence: a transition out of [`ProtoState::AwaitingPingReply`]
//! that fails to consume the inner [`ReplyId`] is a build error — the
//! borrow / move checker forces every transition to handle the carried
//! data explicitly.
//!
//! Phase 1a ships only the variants the Ping flow drives. Per reforge.md
//! §3.5 / §4.6, manufactured variants ("ConnectingStartup", "InTransaction",
//! "Closed", …) are forbidden until their entry/exit code lands in a
//! later sub-phase.
//!
//! [`ProtoState::Errored`] is the one terminal variant — entered via
//! any classified failure in `feed_bytes` or `push_command`, never
//! left. Its presence prevents the state from lying about connection
//! health: a post-error frame arriving at `feed_bytes` observes
//! `Errored`, not `Idle`, and the dispatcher's arm keeps it that way
//! (no action, no state change — post-`CloseSocket` packet flushes
//! become true no-ops instead of silent mis-advances).

use crate::error::ProtocolError;
use crate::reply_id::ReplyId;

/// Where the protocol is right now.
///
/// **Internal-use enum.** Not `#[non_exhaustive]`: exhaustive `match` in
/// internal dispatch is the load-bearing tier-1 invariant — a missed
/// (state, tag) combination is a build failure.
///
/// `Default` is `Idle`, which lets [`core::mem::take`] swap the state
/// out for owned-pattern transitions without ceremony. `take` on an
/// [`Errored`][ProtoState::Errored] state is a genuine hazard (it would
/// lose the stored cause and replace it with `Idle`, re-opening the
/// connection for commands); every caller that uses `mem::take` on the
/// state must explicitly preserve the `Errored` case — see the
/// `fail_inflight_and_close` and `handle_push_ping` bodies in
/// `protocol.rs`.
// Deliberately **not** `Copy`: moving out of `AwaitingPingReply(id)`
// must consume the [`crate::ReplyId`] inline — the state-as-data
// invariant (reforge.md §7.2). `ProtoState` inherits non-Copy from
// the non-Copy `ReplyId` field, so the `missing_copy_implementations`
// lint does not fire here (there is no "could be Copy" suggestion to
// suppress).
#[derive(Debug, Default, PartialEq, Eq)]
pub enum ProtoState {
    /// Connection established and idle. Accepts new commands.
    ///
    /// **Note (Phase 1a):** the protocol *starts* in `Idle`.
    ///
    /// The startup + auth handshake that legitimately produces this
    /// state lives upstream and lands in 1b/1e. Until then, the test
    /// harness just constructs `PgProtocol::new()` and pushes commands.
    #[default]
    Idle,

    /// A `Sync` was sent; awaiting the matching `ReadyForQuery` reply.
    ///
    /// The carried [`ReplyId`] is the only way the inner correlator can
    /// be reached. Any transition that abandons it without forwarding
    /// to a [`crate::Action::DeliverReply`] / [`crate::Action::FailReply`]
    /// will leave the user's `oneshot::Receiver` permanently pending —
    /// that is exactly the bug class the state-as-data pattern makes
    /// impossible to write.
    AwaitingPingReply(ReplyId),

    /// Terminal: the connection has been classified as unrecoverable.
    ///
    /// Entered by any path that calls `fail_inflight_and_close` or
    /// returns `DispatchOutcome::Errored` — these paths also emit the
    /// matching `FailReply` + `CloseSocket` actions in the same call,
    /// so by the time the state is observable as `Errored` the wrapper
    /// has already been told to tear the transport down.
    ///
    /// Never left: no arm of `dispatch` or `handle_push_*` transitions
    /// out of `Errored`. New bytes arriving at `feed_bytes` after the
    /// transition are dropped silently (the TCP segment that provoked
    /// the transition may still have been in flight on a later reader
    /// call). New commands pushed via `push_command` are failed with
    /// the cached cause.
    ///
    /// The carried [`ProtocolError`] is the **root** cause — the
    /// first failure classified, not the most recent. Later failures
    /// on an already-errored state do not overwrite it.
    Errored(ProtocolError),
}
