//! Connecting-phase protocol state — state-as-data.
//!
//! [`ConnectingState`] is the connecting-phase engine's state machine: each
//! variant carries its in-flight [`ReplyId`] correlator and any handshake
//! secret (SCRAM / MD5 / cleartext password material, post-auth backend key)
//! inline (reforge.md §7.2). Consequence: a transition that fails to consume
//! the carried data is a build error — the borrow / move checker forces every
//! transition to handle the carried data explicitly. The connecting-phase
//! engine dispatch ([`crate::engine`]) consumes and produces these variants
//! directly; there is no wider phase-agnostic state enum.

use crate::error::StateErrorKind;
use crate::reply_id::{ReplyId, StartupKind};
use crate::scram::session::ScramSession;
use crate::scram::types::SecretDigest;

/// State space reachable during the PostgreSQL connection handshake.
///
/// 11 handshake variants + 1 transient `Errored` (entered when a classified
/// failure fires mid-handshake). Each variant carries its in-flight
/// [`ReplyId<StartupKind>`] correlator inline; secret-bearing variants box
/// their SCRAM / MD5 / cleartext payload so the enum stays compact.
///
/// **Tier-1 closure**: a variant for a wrong phase (an active-phase verb
/// state) does not exist on this enum, so a state-in-wrong-phase write is
/// impossible by construction.
///
/// **Layout**: 24 B (pin-asserted below). Largest variant is
/// [`Self::StartupCleartext`].
///
/// **Manual `Debug` impl** — variants carrying SCRAM / MD5 / cleartext
/// password material or a `Sensitive<i32>` secret key use
/// `finish_non_exhaustive()` to elide the secret fields from the formatted
/// output; non-sensitive variants print all fields via `finish()` / `write!`.
#[allow(
    missing_docs,
    reason = "Each variant's field semantics are documented at the field level and at the connecting-phase dispatch that drives them; a per-variant docstring would duplicate that and create a drift surface."
)]
#[non_exhaustive]
pub enum ConnectingState {
    StartupTrust {
        reply: ReplyId<StartupKind>,
    },
    StartupCleartext {
        reply: ReplyId<StartupKind>,
        password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    },
    CleartextAwaitingAuthOk(ReplyId<StartupKind>),
    StartupMd5 {
        reply: ReplyId<StartupKind>,
        handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    },
    Md5AwaitingAuthOk(ReplyId<StartupKind>),
    StartupScram {
        reply: ReplyId<StartupKind>,
        scram: alloc::boxed::Box<ScramSession>,
    },
    ScramAwaitingServerFirst {
        reply: ReplyId<StartupKind>,
        scram: alloc::boxed::Box<ScramSession>,
    },
    ScramAwaitingServerFinal {
        reply: ReplyId<StartupKind>,
        expected_server_sig: alloc::boxed::Box<SecretDigest>,
    },
    ScramAwaitingAuthOk(ReplyId<StartupKind>),
    PostAuthAwaitingKey(ReplyId<StartupKind>),
    PostAuthHaveKey {
        reply: ReplyId<StartupKind>,
        pid: i32,
        secret_key: crate::sensitive::Sensitive<i32>,
    },
    /// Handshake complete — the backend key material is captured and the
    /// connection is ready to transition to the active phase. The
    /// `(pid, secret_key)` payload is consumed structurally when the active
    /// engine takes over.
    HandshakeReady {
        /// The backend process ID (wire-public; safe to print).
        pid: i32,
        /// The backend secret key for cancel requests.
        ///
        /// Wrapped in [`crate::sensitive::Sensitive`] for zero-on-drop scrub
        /// when this variant drops (e.g. on a state transition).
        secret_key: crate::sensitive::Sensitive<i32>,
    },
    /// A classified failure was observed mid-handshake; terminal.
    Errored(StateErrorKind),
}

/// Manual `Debug` for [`ConnectingState`] with secret-field redaction.
///
/// Variants carrying SCRAM `ScramSession` / `SecretDigest`, MD5
/// `Md5HandshakeState`, cleartext `Sensitive<Password>`, or post-auth
/// `Sensitive<i32>` secret keys use `finish_non_exhaustive()` to elide secret
/// fields from the formatted output. Non-sensitive variants print all fields.
impl core::fmt::Debug for ConnectingState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::StartupTrust { reply } => f
                .debug_struct("StartupTrust")
                .field("reply", reply)
                .finish(),
            Self::StartupCleartext { reply, .. } => f
                .debug_struct("StartupCleartext")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::CleartextAwaitingAuthOk(id) => {
                write!(f, "CleartextAwaitingAuthOk({id:?})")
            }
            Self::StartupMd5 { reply, .. } => f
                .debug_struct("StartupMd5")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::Md5AwaitingAuthOk(id) => {
                write!(f, "Md5AwaitingAuthOk({id:?})")
            }
            Self::StartupScram { reply, .. } => f
                .debug_struct("StartupScram")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ScramAwaitingServerFirst { reply, .. } => f
                .debug_struct("ScramAwaitingServerFirst")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ScramAwaitingServerFinal { reply, .. } => f
                .debug_struct("ScramAwaitingServerFinal")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ScramAwaitingAuthOk(id) => {
                write!(f, "ScramAwaitingAuthOk({id:?})")
            }
            Self::PostAuthAwaitingKey(id) => {
                write!(f, "PostAuthAwaitingKey({id:?})")
            }
            Self::PostAuthHaveKey { reply, pid, .. } => f
                .debug_struct("PostAuthHaveKey")
                .field("reply", reply)
                .field("pid", pid)
                .finish_non_exhaustive(),
            Self::HandshakeReady { pid, secret_key } => f
                .debug_struct("HandshakeReady")
                .field("pid", pid)
                .field("secret_key", secret_key)
                .finish(),
            Self::Errored(kind) => write!(f, "Errored({kind:?})"),
        }
    }
}

// Tier-1 size pin. ConnectingState's dominant variant settles at 24 B post
// SecretDigest boxing (ScramAwaitingServerFinal shrank via Box<SecretDigest>;
// new dominator is StartupCleartext). A layout drift is an E0080 build failure.
const _: () = assert!(
    core::mem::size_of::<ConnectingState>() == 24,
    "ConnectingState 24 B post-SecretDigest boxing. Dominator: StartupCleartext.",
);
