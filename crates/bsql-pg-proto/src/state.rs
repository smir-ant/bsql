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
use crate::password::Credentials;
use crate::reply_id::ReplyId;
use crate::scram::session::ScramSession;
use crate::scram::types::SecretDigest;

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
#[derive(Default)]
pub enum ProtoState {
    /// Connection established and idle. Accepts new commands.
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

    // ---------------------------------------------------------------
    // Phase 1b: startup + auth handshake states (DEF-001..DEF-004)
    // ---------------------------------------------------------------

    /// A `StartupMessage` was sent; awaiting the server's authentication
    /// response. DEF-001.
    ///
    /// The carried `credentials` determine what authentication we can
    /// perform: `Trust` expects immediate `AuthenticationOk`;
    /// `ScramPassword` expects `AuthenticationSASL` offering
    /// `SCRAM-SHA-256`.
    ConnectingStartup {
        /// Correlator for the Startup command.
        reply: ReplyId,
        /// Credentials supplied by the user.
        credentials: Credentials,
    },

    /// SCRAM step 1 complete (client-first sent); awaiting
    /// `AuthenticationSASLContinue` (server-first-message). DEF-002.
    ConnectingScramAwaitServerFirst {
        /// Correlator for the Startup command.
        reply: ReplyId,
        /// SCRAM session (password bundle). Tier-1 typestate via
        /// [`ScramSession`] — the `Credentials::Trust` variant
        /// cannot appear here by construction (audit A2).
        scram: ScramSession,
        /// The `client-first-message-bare` (saved for AuthMessage).
        client_first_bare: heapless::Vec<u8, 128>,
        /// The client nonce (base64-encoded, for prefix validation).
        client_nonce_b64: heapless::Vec<u8, 48>,
    },

    /// SCRAM step 2 complete (client-final sent); awaiting
    /// `AuthenticationSASLFinal` (server-final-message). DEF-002.
    ConnectingScramAwaitServerFinal {
        /// Correlator for the Startup command.
        reply: ReplyId,
        /// Expected server signature for constant-time comparison.
        expected_server_sig: SecretDigest,
    },

    /// SCRAM step 3 complete (server signature verified); awaiting
    /// `AuthenticationOk`. DEF-002.
    ConnectingScramAwaitAuthOk(ReplyId),

    /// Authentication succeeded; waiting for `BackendKeyData`. DEF-003.
    ///
    /// `ParameterStatus` messages received in this state are recorded
    /// on [`crate::PgProtocol::session_params`] by the `feed_bytes`
    /// loop. `BackendKeyData` transitions to `ConnectingPostAuthHaveKey`.
    ConnectingPostAuthWaitKey(ReplyId),

    /// `BackendKeyData` received; waiting for `ReadyForQuery`. DEF-004.
    ///
    /// Additional `ParameterStatus` messages may arrive before RFQ.
    ConnectingPostAuthHaveKey {
        /// Correlator for the Startup command.
        reply: ReplyId,
        /// The backend process ID.
        pid: i32,
        /// The backend secret key (for cancel requests).
        secret_key: i32,
    },

    /// Terminal: the connection has been classified as unrecoverable.
    ///
    /// Entered by any path that calls `fail_inflight_and_close` or
    /// returns `DispatchOutcome::Errored` — these paths also emit the
    /// matching `FailReply` + `CloseSocket` actions in the same call,
    /// so by the time the state is observable as `Errored` the wrapper
    /// has already been told to tear the transport down.
    ///
    /// Never left. The carried [`ProtocolError`] is the **root** cause.
    Errored(ProtocolError),
}

impl core::fmt::Debug for ProtoState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Idle => f.write_str("Idle"),
            Self::AwaitingPingReply(id) => write!(f, "AwaitingPingReply({id:?})"),
            Self::ConnectingStartup { reply, credentials } => f
                .debug_struct("ConnectingStartup")
                .field("reply", reply)
                .field("credentials", credentials)
                .finish(),
            Self::ConnectingScramAwaitServerFirst { reply, .. } => f
                .debug_struct("ConnectingScramAwaitServerFirst")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingScramAwaitServerFinal { reply, .. } => f
                .debug_struct("ConnectingScramAwaitServerFinal")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingScramAwaitAuthOk(id) => {
                write!(f, "ConnectingScramAwaitAuthOk({id:?})")
            }
            Self::ConnectingPostAuthWaitKey(id) => {
                write!(f, "ConnectingPostAuthWaitKey({id:?})")
            }
            Self::ConnectingPostAuthHaveKey { reply, pid, .. } => f
                .debug_struct("ConnectingPostAuthHaveKey")
                .field("reply", reply)
                .field("pid", pid)
                .finish_non_exhaustive(),
            Self::Errored(cause) => write!(f, "Errored({cause:?})"),
        }
    }
}
