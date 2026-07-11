//! Connecting-phase protocol state — state-as-data.
//!
//! [`ConnectingState`] is the connecting-phase engine's state machine: each
//! variant carries any handshake secret (SCRAM / MD5 / cleartext password
//! material, post-auth backend key) inline. Consequence: a transition that
//! fails to consume the carried data is a build error — the borrow / move
//! checker forces every transition to handle the carried data explicitly. The
//! connecting-phase engine dispatch ([`crate::engine`]) consumes and produces
//! these variants directly; there is no wider phase-agnostic state enum.
//!
//! There is no request/reply correlator on these variants: the handshake is a
//! single, strictly serial, non-multiplexed exchange (exactly one in-flight
//! startup command at a time), so reply-to-request correlation is positional —
//! the current variant *is* the correlation — and no id is threaded.

use crate::error::StateErrorKind;
#[cfg(feature = "scram")]
use crate::scram::session::ScramSession;
#[cfg(feature = "scram")]
use crate::scram::types::SecretDigest;

/// State space reachable during the PostgreSQL connection handshake.
///
/// 11 handshake variants + 1 transient `Errored` (entered when a classified
/// failure fires mid-handshake). Secret-bearing variants box their SCRAM / MD5
/// / cleartext payload so the enum stays compact; the current variant is
/// itself the request/reply correlation (the handshake is strictly serial), so
/// no id is threaded through them.
///
/// **Tier-1 closure**: a variant for a wrong phase (an active-phase verb
/// state) does not exist on this enum, so a state-in-wrong-phase write is
/// impossible by construction.
///
/// **Layout**: pin-asserted below. Largest variants are the boxed-secret
/// arms and the post-auth key arm.
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
    StartupTrust,
    StartupCleartext {
        password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    },
    CleartextAwaitingAuthOk,
    #[cfg(feature = "md5-auth")]
    StartupMd5 {
        handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    },
    #[cfg(feature = "md5-auth")]
    Md5AwaitingAuthOk,
    #[cfg(feature = "scram")]
    StartupScram {
        scram: alloc::boxed::Box<ScramSession>,
    },
    #[cfg(feature = "scram")]
    ScramAwaitingServerFirst {
        scram: alloc::boxed::Box<ScramSession>,
    },
    #[cfg(feature = "scram")]
    ScramAwaitingServerFinal {
        expected_server_sig: alloc::boxed::Box<SecretDigest>,
    },
    #[cfg(feature = "scram")]
    ScramAwaitingAuthOk,
    PostAuthAwaitingKey,
    PostAuthHaveKey {
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
            Self::StartupTrust => write!(f, "StartupTrust"),
            Self::StartupCleartext { .. } => f
                .debug_struct("StartupCleartext")
                .finish_non_exhaustive(),
            Self::CleartextAwaitingAuthOk => {
                write!(f, "CleartextAwaitingAuthOk")
            }
            #[cfg(feature = "md5-auth")]
            Self::StartupMd5 { .. } => f
                .debug_struct("StartupMd5")
                .finish_non_exhaustive(),
            #[cfg(feature = "md5-auth")]
            Self::Md5AwaitingAuthOk => {
                write!(f, "Md5AwaitingAuthOk")
            }
            #[cfg(feature = "scram")]
            Self::StartupScram { .. } => f
                .debug_struct("StartupScram")
                .finish_non_exhaustive(),
            #[cfg(feature = "scram")]
            Self::ScramAwaitingServerFirst { .. } => f
                .debug_struct("ScramAwaitingServerFirst")
                .finish_non_exhaustive(),
            #[cfg(feature = "scram")]
            Self::ScramAwaitingServerFinal { .. } => f
                .debug_struct("ScramAwaitingServerFinal")
                .finish_non_exhaustive(),
            #[cfg(feature = "scram")]
            Self::ScramAwaitingAuthOk => {
                write!(f, "ScramAwaitingAuthOk")
            }
            Self::PostAuthAwaitingKey => {
                write!(f, "PostAuthAwaitingKey")
            }
            Self::PostAuthHaveKey { pid, .. } => f
                .debug_struct("PostAuthHaveKey")
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

// Tier-1 size pin. With no reply-id correlator threaded through the variants,
// the dominant payload is one 8 B pointer (the boxed-secret arms) or the 8 B
// `pid: i32` + `secret_key: Sensitive<i32>` pair; the enum adds an 8 B-aligned
// discriminant, settling at 16 B. A layout drift is an E0080 build failure.
// 64-bit-scoped (the pinned 16 B is a 64-bit-pointer figure) — on any other width
// the crate-root `compile_error!` is the single honest diagnostic.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    core::mem::size_of::<ConnectingState>() == 16,
    "ConnectingState 16 B — one 8 B boxed-secret pointer (or pid+secret pair) \
     plus an 8 B-aligned discriminant.",
);
