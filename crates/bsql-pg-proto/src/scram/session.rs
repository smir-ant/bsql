//! Typestate: credentials guaranteed to support SCRAM-SHA-256.
//!
//! [`ScramSession`] is a private typestate that eliminates the
//! `Trust`-vs-`ScramPassword` double-match seam (audit 2026-04-19
//! finding A2). Before this type, two independent sites in
//! `dispatch.rs` — the `AUTH_SASL` arm of `dispatch_auth_in_startup`
//! and the head of `build_sasl_initial_response` — each matched
//! [`Credentials`] directly and classified the `Trust` variant as
//! an error. A body swap between arms (e.g. classifying `Trust` as
//! success in one site and `ScramPassword` as failure) compiled
//! cleanly: the two sites had no structural linkage.
//!
//! With `ScramSession`, the discrimination exists exactly *once* —
//! at [`ScramSession::try_from_credentials`]. Every downstream site
//! (the SCRAM-state variant of [`crate::state::ProtoState`],
//! [`build_sasl_initial_response`], [`dispatch_auth_sasl_continue`])
//! takes [`&ScramSession`] or owns one. The `Trust` variant cannot
//! reach those sites — it was consumed at construction.
//!
//! Tier-1 compile: an arm-body drift in any downstream site becomes
//! a type error (the variant does not exist in `ScramSession`'s
//! shape), not silent semantic breakage.
//!
//! [`build_sasl_initial_response`]: crate::dispatch
//! [`dispatch_auth_sasl_continue`]: crate::dispatch
//! [`&ScramSession`]: ScramSession

use crate::password::{Credentials, Password};
use crate::sensitive::Sensitive;

/// Owned SCRAM session bundle — the password to use during the
/// SCRAM-SHA-256 exchange, guarded by [`Sensitive`] zeroisation.
///
/// Not [`Clone`]: matches `Sensitive<Password>`'s single-owner
/// invariant (the scrub surface stays at one). Debug-derived, but
/// leaf [`Sensitive`] redacts the password bytes, so the whole
/// struct prints as `ScramSession { password: <REDACTED> }`.
///
/// `pub` visibility with a fully `pub(crate)` surface: the type is
/// visible through [`crate::ProtoState::ConnectingScramAwaitServerFirst`]'s
/// `scram` field (which is `pub` by enum-variant rules), but every
/// constructor and accessor is crate-internal. External code sees
/// an opaque handle — matching `ConnectingScramAwaitServerFirst
/// { scram, .. }` binds a value they can do nothing with, which is
/// exactly the intent. The type is not part of the crate's
/// behavioural public surface.
#[derive(Debug)]
pub struct ScramSession {
    /// The password, zeroed on drop via the inner [`Sensitive`].
    password: Sensitive<Password>,
}

impl ScramSession {
    /// Try to build a `ScramSession` from user-supplied
    /// [`Credentials`]. Returns `Err(())` for any credentials that
    /// cannot drive SCRAM (today: `Credentials::Trust`).
    ///
    /// The `Err` branch is the unique site at which the
    /// `Trust`-vs-`ScramPassword` split is decided — after this
    /// function returns [`Ok`], the `Trust` variant is not in scope
    /// anywhere in the SCRAM path, by type.
    ///
    /// `Err(())` (zero-sized) rather than `Err(Credentials)` — the
    /// caller always classifies to a different error (`UnsupportedAuthMethod`)
    /// and does not need the original value. Zero-sized Err keeps
    /// the return type small (`sizeof ScramSession + 0` ≈ 32 bytes)
    /// and avoids `clippy::result_large_err`.
    pub(crate) fn try_from_credentials(credentials: Credentials) -> Result<Self, ()> {
        match credentials {
            Credentials::ScramPassword(password) => Ok(Self { password }),
            Credentials::Trust => Err(()),
        }
    }

    /// Borrow the password bytes for HMAC / PBKDF2 computation.
    ///
    /// The returned slice lives for the shared borrow of `self`;
    /// callers must not cache it past the call boundary (the
    /// underlying `Sensitive` may be zeroed at any later moment).
    #[inline]
    pub(crate) fn password_bytes(&self) -> &[u8] {
        self.password.get().as_bytes()
    }
}
