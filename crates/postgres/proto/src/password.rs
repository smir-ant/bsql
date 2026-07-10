//! Password buffer and authentication credentials.
//!
//! [`Password`] is a bounded byte buffer for user passwords, wrapped in
//! [`Sensitive`] to guarantee zero-on-drop
//! and redacted debug. [`Credentials`] selects between trust auth (no
//! password) and password-based auth.
//!
//! # Security properties
//!
//! - Passwords are never exposed in `Debug` output (tier-1 via
//!   `Sensitive` wrapper + manual `Debug` on `Credentials`).
//! - Empty passwords are rejected at construction time — tier-1 via
//!   `Result` return.
//! - Password bytes are scrubbed on drop via `zeroize`.
//! - NUL bytes inside the password are allowed (PG supports binary
//!   passwords via md5/scram).

use crate::sensitive::Sensitive;
use core::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Maximum password length in bytes.
///
/// PostgreSQL does not impose a hard limit on password length, but
/// SCRAM-SHA-256 with PBKDF2 processes the full password on every
/// authentication. SCRAM uses SASLprep normalisation on the password
/// bytes; UTF-8 NFKC expansion is bounded at ~4× the input-char count
/// in the pathological case (combining marks). 512 B accommodates 128
/// normalized UTF-8 chars — a huge password for any realistic
/// workflow (industry practice: argon2 accepts arbitrary input but
/// real deployments cap at ~128; bcrypt truncates at 72).
///
/// Halves of this cap propagate through `Password` size →
/// `Sensitive<Password>` → `Credentials::ScramPassword` →
/// `PgCommand::Startup` enum payload (which pays the worst-case
/// cost on every caller-side allocation). `try_from_bytes` classifies
/// oversize as `PasswordError::TooLong`, surfacing a clean error at
/// construction rather than silently accepting.
pub const MAX_PASSWORD_LEN: usize = 512;

/// Compile-time drift guard: `MAX_PASSWORD_LEN` must fit the `u16`
/// length field on [`Password`]. `65_535` is hard-coded instead of
/// `u16::MAX as usize` because `as` casts are banned by the crate
/// forbid-bundle.
const _: () = assert!(
    MAX_PASSWORD_LEN <= 65_535,
    "MAX_PASSWORD_LEN must fit Password::len (u16)",
);

/// A bounded, zeroize-on-drop password buffer.
///
/// Constructed via [`Password::try_from_bytes`]. Rejects empty and
/// over-length inputs. NUL bytes are allowed.
///
/// The inner storage is a fixed-size array with a length field,
/// avoiding heap allocation. `#[derive(Zeroize, ZeroizeOnDrop)]`
/// scrubs the full array (not just the used portion) on drop —
/// self-zeroizing regardless of wrapper context. A compile-time
/// `const _: () = assert!(needs_drop::<Password>())` in `lib.rs`
/// enforces this invariant structurally.
///
/// `len` is a `u16` (not `usize`) because `MAX_PASSWORD_LEN` (512)
/// trivially fits; the narrower type saves 6 bytes per `Password`
/// instance which compounds through `Sensitive<Password>` →
/// `Credentials::ScramPassword` → `PgCommand::Startup` →
/// `ProtoState::ConnectingStartup`.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Password {
    /// Fixed-size backing store. The full array is zeroed on drop,
    /// not just `[..len]`.
    buf: [u8; MAX_PASSWORD_LEN],
    /// Number of valid bytes in `buf[..len]`.
    len: u16,
}

/// Errors from [`Password`] construction.
///
/// # `#[non_exhaustive]`
///
/// New rejection classes may land as future password validation
/// rules tighten (e.g. NUL-byte rejection mirroring [`crate::ident::Ident`],
/// UTF-8-only requirements for SCRAM normalisation). Sealing via
/// `non_exhaustive` forces downstream `match` callers to retain
/// a catch-all arm so a new variant cannot silently fall through
/// a downstream exhaustive match and lose its diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PasswordError {
    /// The password was empty. Empty passwords are rejected at
    /// construction as a tier-1 visible choice (via `Result`).
    Empty,
    /// The password exceeds [`MAX_PASSWORD_LEN`] bytes.
    TooLong {
        /// Actual byte length of the rejected input.
        len: usize,
    },
}

impl core::error::Error for PasswordError {}

// Footprint pin: sized by the `TooLong { len: usize }` variant (a usize payload
// plus the discriminant). A new variant carrying a wider payload would show up
// here.
crate::wire_pin!(PasswordError, size = 16, align = 8);

impl fmt::Display for PasswordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("password must not be empty"),
            Self::TooLong { len } => write!(
                f,
                "password too long: {len} bytes (max {MAX_PASSWORD_LEN})",
            ),
        }
    }
}

impl Password {
    /// Construct from raw bytes. Rejects empty and over-length inputs.
    pub fn try_from_bytes(input: &[u8]) -> Result<Self, PasswordError> {
        if input.is_empty() {
            return Err(PasswordError::Empty);
        }
        if input.len() > MAX_PASSWORD_LEN {
            return Err(PasswordError::TooLong { len: input.len() });
        }
        // `input.len() <= MAX_PASSWORD_LEN <= 65_535` (const-asserted),
        // so the narrowing is infallible in practice. `try_from` +
        // `map_err` keeps the forbid-bundle happy without `as`.
        let len = u16::try_from(input.len())
            .map_err(|_| PasswordError::TooLong { len: input.len() })?;
        let mut buf = [0u8; MAX_PASSWORD_LEN];
        let dest = match buf.get_mut(..input.len()) {
            Some(s) => s,
            None => return Err(PasswordError::TooLong { len: input.len() }),
        };
        dest.copy_from_slice(input);
        Ok(Self { buf, len })
    }

    /// Construct from a UTF-8 string. Convenience wrapper over
    /// [`Password::try_from_bytes`].
    pub fn try_from_str(s: &str) -> Result<Self, PasswordError> {
        Self::try_from_bytes(s.as_bytes())
    }

    /// Borrow the password bytes.
    ///
    /// `self.len ≤ MAX_PASSWORD_LEN ≤ self.buf.len()` by construction
    /// (see `try_from_bytes` bound check). `split_at_checked`'s `None`
    /// arm is architecturally unreachable; the empty-slice fallback
    /// matches semantically "no password bytes" (same surface as a
    /// zeroized post-drop).
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        let n = usize::from(self.len);
        match self.buf.split_at_checked(n) {
            Some((head, _)) => head,
            None => &[],
        }
    }
}

impl fmt::Debug for Password {
    /// Prints `"Password(<REDACTED>)"` — the password bytes never leak.
    ///
    /// # Test-pinned invariant
    ///
    /// Pinned by `tests/startup_spec.rs::password_debug_does_not_leak_bytes`
    /// which constructs a `Password` from `b"hunter2"` and asserts the
    /// Debug output contains `"REDACTED"` and does NOT contain the
    /// literal `"hunter2"`. Drift-shield against future Debug-derive
    /// refactors.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Password(<REDACTED>)")
    }
}

/// Authentication credentials for a PostgreSQL connection.
///
/// `Trust` means no password is sent — the server is configured to
/// accept the connection based on `pg_hba.conf` rules alone.
/// `ScramPassword` carries a password for SCRAM-SHA-256 authentication.
///
/// # Design: large enum variants are intentional
///
/// `Credentials` is a cold-path enum constructed once per connection,
/// `Password` is 512 B by design ([`MAX_PASSWORD_LEN`]), and boxing at
/// the variant layer would require allocation in user code — breaking
/// the no_alloc-from-outside contract. In the full build the
/// password-bearing variants (SCRAM / cleartext / MD5) are symmetric, so
/// `clippy::large_enum_variant` (which fires only when a single variant
/// dominates) does not warn. In the MINIMAL build with BOTH `scram` and
/// `md5-auth` gated out, only `CleartextPassword` and unit `Trust` remain,
/// so the heuristic flags a size gap that reflects the gated-out variants,
/// not a fixable layout — the scoped `expect` below documents that (boxing
/// to silence it would add a heap indirection to every credential on the
/// common path).
#[cfg_attr(
    not(any(feature = "scram", feature = "md5-auth")),
    expect(
        clippy::large_enum_variant,
        reason = "minimal build only: with both password features gated out, the inline 512-byte `Sensitive<Password>` in `CleartextPassword` is the lone large variant beside unit `Trust`; the full build's symmetric password variants close the gap, and boxing to appease the heuristic would cost a heap indirection on the common path"
    )
)]
#[non_exhaustive]
pub enum Credentials {
    /// Trust authentication — no password required.
    Trust,
    /// Password-based authentication (SCRAM-SHA-256).
    ///
    /// The password is wrapped in [`Sensitive`] for zero-on-drop and
    /// debug redaction. Present only under the default-on `scram` feature — with
    /// SCRAM off the crypto is not compiled, so this credential cannot be built
    /// (a driver given a password then fails LOUD at connect).
    ///
    /// The second field is the resolved
    /// [`ChannelBinding`](crate::scram::channel_binding::ChannelBinding): the
    /// driver computes it from the TLS transport + the consumer's
    /// `channel_binding` policy, so the SCRAM credential carries everything the
    /// exchange needs — the password AND whether/how to bind to the channel. It
    /// is [`ChannelBinding::Unbound`](crate::scram::channel_binding::ChannelBinding::Unbound)
    /// on a plaintext connection.
    #[cfg(feature = "scram")]
    ScramPassword(Sensitive<Password>, crate::scram::channel_binding::ChannelBinding),
    /// Cleartext password authentication (PG `AuthenticationCleartextPassword`,
    /// sub-code 3).
    ///
    /// The server requests the password as a NUL-terminated cleartext
    /// string in a `PasswordMessage`. Common in legacy on-prem PG
    /// configurations (PG ≤ 13 era).
    ///
    /// **Security**: cleartext password is sent as-is over the wire.
    /// The connection MUST be TLS-protected before the startup phase
    /// begins, otherwise the password leaks. `bsql-pg-proto` itself
    /// does not enforce the TLS gate; the driver wrapper is responsible
    /// for refusing cleartext-credential constructs on non-TLS
    /// connections.
    ///
    /// The password is wrapped in [`Sensitive`] for zero-on-drop and
    /// debug redaction; same Drop chain as [`Self::ScramPassword`].
    CleartextPassword(Sensitive<Password>),
    /// MD5 password authentication (PG `AuthenticationMD5Password`,
    /// sub-code 5).
    ///
    /// Server sends a 4-byte salt; client responds with
    /// `"md5" || md5_hex(md5_hex(password || username) || salt)` in
    /// a `PasswordMessage`. Common in PG ≤ 13 enterprise on-prem
    /// installs; PG 14 and newer default to SCRAM.
    ///
    /// **Security**: MD5 is cryptographically broken for collision-
    /// resistant uses; PG's salt+rehash construction provides only
    /// weak protection against passive observation, and offline
    /// password cracking with modern GPUs is fast. Where the server
    /// offers SCRAM as well, drivers SHOULD prefer SCRAM. Unlike
    /// cleartext, MD5 does not require TLS to defeat passive
    /// observation, but TLS is still strongly recommended (the
    /// digest leaks enough information for offline cracking).
    ///
    /// The password is wrapped in [`Sensitive`] for zero-on-drop and
    /// debug redaction. The MD5 computation is performed inside
    /// `crate::md5` which uses `Zeroizing<>` for every password-
    /// derived intermediate buffer.
    ///
    /// Present only under the default-on `md5-auth` feature — with MD5 auth off
    /// the `md-5` crate is not compiled, so this credential cannot be built (an
    /// MD5-demanding server then fails LOUD with `UnsupportedAuthMethod`).
    #[cfg(feature = "md5-auth")]
    Md5Password(Sensitive<Password>),
}

impl fmt::Debug for Credentials {
    /// Manual impl — exhaustive match per variant.
    ///
    /// # `#[non_exhaustive]` + exhaustive-inside-crate match
    ///
    /// `Credentials` carries `#[non_exhaustive]` so downstream crates
    /// MUST use a catch-all when matching. Inside THIS crate the
    /// match below is still exhaustive; adding a new variant is a
    /// build error HERE until the new variant's Debug path is
    /// declared. The combination is the tier-1 drift shield:
    /// a new internal variant cannot silently inherit a derived
    /// Debug that would leak secrets.
    ///
    /// # Test-pinned invariant
    ///
    /// Pinned by `tests/startup_spec.rs::credentials_debug_does_not_leak_password`
    /// which constructs a `Credentials::ScramPassword` from a known
    /// password string and asserts the Debug output contains
    /// `"REDACTED"` and does NOT contain the password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Trust => f.write_str("Credentials::Trust"),
            #[cfg(feature = "scram")]
            Self::ScramPassword(_, _) => f.write_str("Credentials::ScramPassword(<REDACTED>)"),
            Self::CleartextPassword(_) => {
                f.write_str("Credentials::CleartextPassword(<REDACTED>)")
            }
            #[cfg(feature = "md5-auth")]
            Self::Md5Password(_) => f.write_str("Credentials::Md5Password(<REDACTED>)"),
        }
    }
}

#[cfg(test)]
mod drop_witness_tests {
    //! Tier-1-by-construction Drop-fire witness for [`Password`] via
    //! [`crate::drop_witness::DropCounter`].
    //!
    //! The `DropCounter<Password>` wrapper observes the drop event via
    //! an atomic counter; the production `ZeroizeOnDrop` impl on
    //! `Password` fires unchanged. Both together: the counter
    //! increment witnesses that `ZeroizeOnDrop::drop` was reached
    //! (Rust drop-glue rules guarantee field drops fire on enclosing-
    //! struct drop).

    use super::Password;
    use crate::drop_witness::{DropCounter, DropProbe};

    /// Dropping `DropCounter<Password>` fires both the wrapper's
    /// counter and the inner `Password::drop` (`ZeroizeOnDrop`).
    #[test]
    fn password_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let pw = match Password::try_from_bytes(b"witness-magic-1234") {
            Ok(p) => p,
            Err(_) => return,
        };
        DropCounter::scoped(pw, probe.clone(), || {
            assert_eq!(probe.fired(), 0, "wrapper alive — counter is 0");
        });
        assert_eq!(
            probe.fired(),
            1,
            "Password drop must fire exactly once on scope exit",
        );
    }

    /// Multiple `Password` instances all wired to the same probe
    /// each contribute one count. Pins that the drop-fire signal is
    /// per-instance, not per-type.
    #[test]
    fn each_password_drop_increments_counter() {
        let probe = DropProbe::new();
        for i in 0..5_u8 {
            let pw = match Password::try_from_bytes(&[b'a', i]) {
                Ok(p) => p,
                Err(_) => continue,
            };
            DropCounter::scoped(pw, probe.clone(), || {});
            // wrapper drops at closure exit, before next iteration.
        }
        assert_eq!(
            probe.fired(),
            5,
            "five Password drops must yield exactly 5 counter increments",
        );
    }
}
