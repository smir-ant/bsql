//! Typestate: a credentialed session guaranteed to carry a password
//! for SCRAM-SHA-256.
//!
//! [`ScramSession`] is a private typestate that eliminates the
//! `Trust`-vs-`ScramPassword` seam (audit 2026-04-19 finding A2,
//! tightened by DEF-097).
//!
//! **DEF-097 update.** The `Trust`/`ScramPassword` discrimination
//! moved out of this module and into
//! [`crate::PgProtocol::push_command`] — specifically the
//! `compute_push_startup` branch that routes a Trust command into
//! [`crate::state::ProtoState::ConnectingStartupTrust`] and a Scram
//! command into [`crate::state::ProtoState::ConnectingStartupScram`].
//! From that point on, the state machine has *two disjoint pre-auth
//! state variants* and the dispatcher's arms are type-split:
//! the Trust arm can only receive `AuthenticationOk`, the Scram arm
//! can only receive `AuthenticationSASL`. The "server sent the
//! wrong auth method for this credential" case is a type-level
//! impossibility rather than a runtime `UnsupportedAuthMethod`
//! classification.
//!
//! `ScramSession` itself now only has a direct constructor
//! [`ScramSession::from_password`]; there is no longer a
//! `try_from_credentials` path because `Credentials::Trust` never
//! reaches this module.

use crate::password::Password;
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
/// visible through [`crate::ProtoState::ConnectingScramAwaitingServerFirst`]'s
/// `scram` field (which is `pub` by enum-variant rules), but every
/// constructor and accessor is crate-internal. External code sees
/// an opaque handle — matching `ConnectingScramAwaitingServerFirst
/// { scram, .. }` binds a value they can do nothing with, which is
/// exactly the intent. The type is not part of the crate's
/// behavioural public surface.
///
/// # F-026 (pass-#8): explicit `Zeroize` / `ZeroizeOnDrop` derive
///
/// Pre-F-026 the struct relied on `Sensitive<Password>`'s Drop to
/// scrub transitively — which works today but is *field-order-
/// dependent*. A future refactor adding another secret-derived
/// field (e.g., a cached intermediate key) without a `Zeroize` impl
/// would silently skip scrubbing. The derive below forces every
/// field to be `Zeroize` at compile time: any non-Zeroize field
/// added fails the build.
///
/// # DEF-210 PERF-02 (audit 2026-05-04): single-Box handshake state
///
/// Pre-PERF-02 the `client-first-message-bare` and `client-nonce-b64`
/// SCRAM-handshake fields lived in a separate `ScramHandshakeState`
/// struct boxed into `ConnectingScramAwaitingServerFirst`. The
/// `StartupScram → ServerFirst` transition incurred two allocator
/// ops (free old `Box<ScramSession>` + alloc new
/// `Box<ScramHandshakeState>`). The principal's documented invariant
/// "one heap alloc per SCRAM connection" was structurally violated
/// at the transition.
///
/// Post-PERF-02 these fields live INSIDE `ScramSession`. Both
/// `ConnectingStartupScram` and `ConnectingScramAwaitingServerFirst`
/// carry the **same** `Box<ScramSession>` — the transition is a
/// state-discriminant flip with the Box pointer copy-moved across
/// variants (zero allocator ops). The `client-first-message-bare`
/// and `client-nonce-b64` start empty (`PodBytes::new()`) at
/// `from_password`; `dispatch_auth_sasl_begin`'s
/// `build_sasl_initial_response` populates them in-place via
/// `&mut ScramSession`. Per-handshake total: 1 alloc + 1 free
/// (ConnectingStartupScram alloc → ServerFinal drop), zero
/// transitions in between. Invariant restored to literal accuracy.
///
/// `#[zeroize(skip)]` on the two `PodBytes` fields preserves the
/// pre-existing scrub semantics: SCRAM `client-first-message-bare`
/// and `client-nonce-b64` are wire-public bytes (sent unencrypted
/// over TLS at session establishment), classified LOW-severity by
/// DEF-205 step 4 / DEF-206 audit. The password remains
/// `Sensitive<Password>` and IS zeroized on drop. Drop chain:
/// `Box::drop` then `ScramSession::drop` then `password.zeroize()`
/// (the two `PodBytes` fields are skip-zeroed).
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
    /// see struct docstring for the DEF-205/206 audit reference.
    #[zeroize(skip)]
    pub(crate) client_first_bare:
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
    /// Client nonce (base64-encoded) saved for prefix validation
    /// of the server-first-message's `r=` field. Same lifecycle
    /// + scrub policy as `client_first_bare` above.
    #[zeroize(skip)]
    pub(crate) client_nonce_b64:
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
}

impl ScramSession {
    /// Build a `ScramSession` from an already-extracted password.
    /// Infallible — the caller has already discriminated away the
    /// `Credentials::Trust` variant at its own site (DEF-097).
    ///
    /// `client_first_bare` / `client_nonce_b64` start empty
    /// (`PodBytes::new()`); they're populated by
    /// `dispatch::build_sasl_initial_response` at the SASL Initial
    /// Response build, BEFORE the StartupScram → ServerFirst
    /// transition. The single `Box<ScramSession>` allocation is
    /// reused across both states (DEF-210 PERF-02).
    #[inline]
    pub(crate) const fn from_password(password: Sensitive<Password>) -> Self {
        Self {
            password,
            client_first_bare: crate::ident::PodBytes::new(),
            client_nonce_b64: crate::ident::PodBytes::new(),
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
