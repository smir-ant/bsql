//! Typestate: a credentialed session guaranteed to carry a password
//! for SCRAM-SHA-256.
//!
//! [`ScramSession`] is a private typestate that eliminates the
//! `Trust`-vs-`ScramPassword` seam.
//!
//! The `Trust`/`ScramPassword` discrimination happens inside
//! `crate::PgProtocol::push_command` — specifically the
//! `compute_push_startup` branch that routes a Trust command into
//! [`crate::state::ConnectingState::StartupTrust`] and a Scram
//! command into [`crate::state::ConnectingState::StartupScram`].
//! From that point on, the state machine has *two disjoint pre-auth
//! state variants* and the dispatcher's arms are type-split:
//! the Trust arm can only receive `AuthenticationOk`, the Scram arm
//! can only receive `AuthenticationSASL`. The "server sent the
//! wrong auth method for this credential" case is a type-level
//! impossibility rather than a runtime `UnsupportedAuthMethod`
//! classification.
//!
//! `ScramSession` has a single constructor
//! `ScramSession::from_password` — `Credentials::Trust` never
//! reaches this module.

use crate::password::Password;
use crate::scram::channel_binding::{ChannelBinding, SaslChoice};
use crate::sensitive::Sensitive;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Owned SCRAM session bundle — the password to use during the
/// SCRAM-SHA-256 exchange, guarded by [`Sensitive`] zeroisation.
///
/// Not [`Clone`]: matches `Sensitive<Password>`'s single-owner
/// invariant (the scrub surface stays at one). Debug-derived, but
/// leaf [`Sensitive`] redacts the password bytes, so the whole
/// struct prints as `ScramSession { password: <REDACTED> }`.
///
/// `pub` visibility with a fully `pub(crate)` surface: the type is
/// visible through `crate::ProtoState::ConnectingScramAwaitingServerFirst`'s
/// `scram` field (which is `pub` by enum-variant rules), but every
/// constructor and accessor is crate-internal. External code sees
/// an opaque handle — matching `ConnectingScramAwaitingServerFirst
/// { scram, .. }` binds a value they can do nothing with, which is
/// exactly the intent. The type is not part of the crate's
/// behavioural public surface.
///
/// # Explicit `Zeroize` / `ZeroizeOnDrop` derive
///
/// Relying solely on `Sensitive<Password>`'s Drop to scrub
/// transitively would be *field-order-dependent*: a future refactor
/// adding another secret-derived field (e.g., a cached intermediate
/// key) without a `Zeroize` impl would silently skip scrubbing. The
/// derive below forces every field to be `Zeroize` at compile time:
/// any non-Zeroize field added fails the build.
///
/// # Single-Box handshake state
///
/// `client-first-message-bare` and `client-nonce-b64` SCRAM-handshake
/// fields live INSIDE `ScramSession`, not in a separate boxed
/// `ScramHandshakeState` struct. Both
/// `ConnectingStartupScram` and `ConnectingScramAwaitingServerFirst`
/// carry the **same** `Box<ScramSession>` — the transition is a
/// state-discriminant flip with the Box pointer copy-moved across
/// variants (zero allocator ops). The `client-first-message-bare`
/// and `client-nonce-b64` start empty (`PodBytes::new()`) at
/// `from_password`; `dispatch_auth_sasl_begin`'s
/// `build_sasl_initial_response` populates them in-place via
/// `&mut ScramSession`. Per-handshake total: 1 alloc + 1 free
/// (ConnectingStartupScram alloc → ServerFinal drop), zero
/// transitions in between. The "one heap alloc per SCRAM connection"
/// invariant is literal.
///
/// `#[zeroize(skip)]` on the two `PodBytes` fields preserves the
/// scrub semantics: SCRAM `client-first-message-bare` and
/// `client-nonce-b64` are wire-public bytes (sent unencrypted over
/// TLS at session establishment), classified LOW-severity. The
/// password remains `Sensitive<Password>` and IS zeroized on drop.
/// Drop chain: `Box::drop` then `ScramSession::drop` then
/// `password.zeroize()` (the two `PodBytes` fields are skip-zeroed).
#[derive(Debug, Zeroize, ZeroizeOnDrop)]
pub struct ScramSession {
    /// The password, zeroed on drop via the inner [`Sensitive`].
    password: Sensitive<Password>,
    /// `client-first-message-bare` saved for the SCRAM AuthMessage
    /// at the ServerFirst → ServerFinal transition. Empty at
    /// `from_password`; populated in-place by
    /// `dispatch::build_sasl_initial_response` via `&mut ScramSession`
    /// before the StartupScram → ServerFirst transition.
    ///
    /// `#[zeroize(skip)]` — wire-public bytes (no PII / no secrets);
    /// see struct docstring for the LOW-severity classification.
    #[zeroize(skip)]
    pub(crate) client_first_bare:
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
    /// Client nonce (base64-encoded) saved for prefix validation
    /// of the server-first-message's `r=` field. Same lifecycle
    /// + scrub policy as `client_first_bare` above.
    #[zeroize(skip)]
    pub(crate) client_nonce_b64:
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
    /// The resolved channel-binding context (transport-derived): `Unbound` on a
    /// plaintext connection or under `channel_binding=disable`, or `Available`
    /// with the `tls-server-end-point` cert hash over TLS. Set at
    /// `from_password` from the credential and read at the
    /// `AuthenticationSASL` dispatch to decide the mechanism.
    ///
    /// `#[zeroize(skip)]` — the binding data is the server's PUBLIC certificate
    /// hash (sent base64 in the client-final `c=` value), not a secret.
    #[zeroize(skip)]
    pub(crate) channel_binding: ChannelBinding,
    /// The SCRAM mechanism + gs2 flag actually selected for this exchange.
    /// Initialised to [`SaslChoice::NoBinding`] and authoritatively set by
    /// `dispatch_startup_scram` from the server's mechanism offer + the
    /// `channel_binding` above, BEFORE the StartupScram → ServerFirst
    /// transition — exactly as `client_first_bare` / `client_nonce_b64` are
    /// populated there. Read at the client-final build to reconstruct the `c=`
    /// cbind-input.
    ///
    /// `#[zeroize(skip)]` — a Copy enum discriminant, no secret material.
    #[zeroize(skip)]
    pub(crate) sasl_choice: SaslChoice,
}

impl ScramSession {
    /// Build a `ScramSession` from an already-extracted password.
    /// Infallible — the caller has already discriminated away the
    /// `Credentials::Trust` variant at its own site.
    ///
    /// `client_first_bare` / `client_nonce_b64` start empty
    /// (`PodBytes::new()`); they're populated by
    /// `dispatch::build_sasl_initial_response` at the SASL Initial
    /// Response build, BEFORE the StartupScram → ServerFirst
    /// transition. The single `Box<ScramSession>` allocation is
    /// reused across both states.
    ///
    /// `channel_binding` is the transport-resolved binding (carried in from the
    /// SCRAM credential); `sasl_choice` starts at [`SaslChoice::NoBinding`] and
    /// is set from the server's mechanism offer at the same StartupScram step.
    #[inline]
    pub(crate) const fn from_password(
        password: Sensitive<Password>,
        channel_binding: ChannelBinding,
    ) -> Self {
        Self {
            password,
            client_first_bare: crate::ident::PodBytes::new(),
            client_nonce_b64: crate::ident::PodBytes::new(),
            channel_binding,
            sasl_choice: SaslChoice::NoBinding,
        }
    }

    /// Closure-scope password bytes for HMAC / PBKDF2 computation.
    /// The HRTB-quantified `&'a [u8]` borrow cannot escape the call.
    /// A plain `pub(crate) fn password_bytes(&self) -> &[u8]` would
    /// be tier-2 by-discipline (only a docstring "don't cache the
    /// borrow past the call boundary" stops abuse); routing through
    /// `Sensitive::with_inner` inherits its HRTB-scoped retention
    /// guarantee.
    #[inline]
    pub(crate) fn with_password_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        self.password.with_inner(|pwd| f(pwd.as_bytes()))
    }
}

#[cfg(test)]
mod drop_witness_tests {
    //! Tier-1-by-construction Drop-fire witness for [`ScramSession`]
    //! via [`crate::drop_witness::DropCounter`]. The test drops a
    //! `DropCounter<ScramSession>` and asserts the counter increments.
    //! By Rust drop-glue rules, the counter cannot increment unless
    //! `ScramSession::drop` (the ZeroizeOnDrop-derived impl) was
    //! reached, which transitively fires `Sensitive::drop` →
    //! `Password::drop`.
    //!
    //! Without this witness, ScramSession Drop is covered only by
    //! the `dropping_proto_mid_scram_handshake_runs_drop_glue` smoke
    //! test (drops a `PgProtocol` mid-SCRAM and asserts no panic) —
    //! no counter, no per-instance verification, no probe of the
    //! ZeroizeOnDrop chain firing on the inner `Sensitive<Password>`.

    use super::ScramSession;
    use crate::drop_witness::{DropCounter, DropProbe};
    use crate::password::Password;
    use crate::sensitive::Sensitive;

    /// `ScramSession::drop` fires the full ZeroizeOnDrop chain via
    /// the derive-generated Drop body. Counter increments on wrapper
    /// drop iff `ScramSession::drop` reached its body.
    #[test]
    fn scram_session_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let pw = match Password::try_from_bytes(b"scram-witness-magic") {
            Ok(p) => p,
            Err(_) => return,
        };
        let session = ScramSession::from_password(Sensitive::new(pw), crate::scram::channel_binding::ChannelBinding::Unbound);
        DropCounter::scoped(session, probe.clone(), || {
            assert_eq!(probe.fired(), 0, "session alive — counter is 0");
        });
        assert_eq!(
            probe.fired(),
            1,
            "ScramSession drop must fire exactly once on scope exit",
        );
    }

    /// Repeated `ScramSession` constructions and drops accumulate
    /// the counter. Pins per-instance witness rather than per-type.
    #[test]
    fn each_scram_session_drop_increments_counter() {
        let probe = DropProbe::new();
        for byte in 0..3_u8 {
            let pw_bytes = [b'p', byte, b'w'];
            let pw = match Password::try_from_bytes(&pw_bytes) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let session = ScramSession::from_password(Sensitive::new(pw), crate::scram::channel_binding::ChannelBinding::Unbound);
            DropCounter::scoped(session, probe.clone(), || {});
        }
        assert_eq!(probe.fired(), 3);
    }
}
