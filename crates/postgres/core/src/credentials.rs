//! The driver-side password-credential builder.
//!
//! PostgreSQL is server-driven: which authentication mechanism a connection uses
//! is decided by the server's `pg_hba.conf`, and the client learns it only from
//! the mid-handshake `Authentication*` frame. So the drivers cannot commit to
//! one mechanism at credential-construction time; they build a
//! [`Credentials::Password`] carrying EVERY form the server might ask for, and
//! the sans-IO engine ([`bsql_postgres_proto::state::ConnectingState::StartupPassword`])
//! answers the actual challenge.
//!
//! Both forms come from the SAME config password but differ by construction:
//! SCRAM-SHA-256 feeds `SASLprep(password)` to PBKDF2 (RFC 5802 mandates RFC
//! 4013 SASLprep), while MD5 and cleartext use the RAW password bytes VERBATIM
//! (SASLprep MUST NOT be applied there — a non-ASCII password whose SASLprep
//! form differs from its raw bytes would silently fail MD5/cleartext while
//! authenticating fine under SCRAM). This builder computes both once.
//!
//! # Feature honesty
//!
//! The builder exists only under a build that can satisfy a password challenge —
//! `scram` (SCRAM) or `md5-auth` (MD5) — since cleartext-over-TLS alone is not a
//! deployment worth widening the API for. With BOTH gated out, a supplied
//! password is a fail-loud [`DriverError::Config`] at the driver's credential
//! decision (never a silent Trust attempt), exactly as before. The `scram` form
//! (`scram_prepped` + the resolved channel binding) is included only under
//! `scram`; MD5 needs no extra form (it digests the raw password). So the
//! credential's answerable mechanisms track the compiled features: an
//! unanswerable challenge (SASL with `scram` off, MD5 with `md5-auth` off) is a
//! classified fail-loud at the engine, not a silent stall.
//!
//! The whole module is `#[cfg(any(scram, md5-auth))]` at its declaration in
//! `lib.rs`; with both off there is no password mechanism, so the driver's
//! credential decision returns the fail-loud config error inline.

use crate::error::DriverError;
use bsql_postgres_proto::{Credentials, Ident, Password, PasswordAuth, Sensitive};

/// Build a server-driven [`Credentials::Password`] from a config password.
///
/// `password` is the consumer's configured password; `user` MUST be the same
/// [`Ident`] used in the StartupMessage (it is the MD5 digest input);
/// `encrypted` reports whether the negotiated transport is TLS (it gates
/// cleartext). Under `scram`, `peer_cert` (the server's end-entity certificate
/// DER, if any) and `channel_binding_mode` resolve the SCRAM channel binding.
///
/// # Errors
///
/// [`DriverError::Config`] for an empty/over-length password, a password whose
/// SASLprep form is rejected by RFC 4013 (under `scram`), or a
/// `channel_binding=require` over a plaintext channel.
pub fn build_password_credentials(
    password: &str,
    user: &Ident,
    encrypted: bool,
    #[cfg(feature = "scram")] peer_cert: Option<&[u8]>,
    #[cfg(feature = "scram")] channel_binding_mode: crate::config::ChannelBindingMode,
) -> Result<Credentials, DriverError> {
    // The RAW password — MD5 + cleartext. Stored verbatim, never SASLprepped.
    let raw = Sensitive::new(
        Password::try_from_str(password).map_err(|_| DriverError::Config("invalid password"))?,
    );
    // The SASLprep form — SCRAM PBKDF2 input (RFC 5802 / RFC 4013). A prohibited
    // codepoint is a classified `DriverError::Config` inside `saslprep_password`.
    #[cfg(feature = "scram")]
    let scram_prepped = Sensitive::new(crate::scram_prep::saslprep_password(password)?);
    // Resolve SCRAM channel binding from the negotiated transport + policy: over
    // TLS this hashes the server certificate into the `tls-server-end-point`
    // binding data (so the engine can select SCRAM-SHA-256-PLUS);
    // `channel_binding=require` over plaintext fails closed here.
    #[cfg(feature = "scram")]
    let channel_binding =
        crate::config::resolve_channel_binding(encrypted, peer_cert, channel_binding_mode)?;

    let auth = PasswordAuth::new(
        raw,
        #[cfg(feature = "scram")]
        scram_prepped,
        #[cfg(feature = "scram")]
        channel_binding,
        *user,
        encrypted,
    );
    Ok(Credentials::Password(Box::new(auth)))
}
