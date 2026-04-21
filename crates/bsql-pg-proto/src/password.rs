//! Password buffer and authentication credentials.
//!
//! [`Password`] is a bounded byte buffer for user passwords, wrapped in
//! [`Sensitive`](crate::sensitive::Sensitive) to guarantee zero-on-drop
//! and redacted debug. [`Credentials`] selects between trust auth (no
//! password) and password-based auth.
//!
//! # Security properties
//!
//! - Passwords are never exposed in `Debug` output (tier-1 via
//!   `Sensitive` wrapper + manual `Debug` on `Credentials`).
//! - Empty passwords are rejected at construction time (DEF-051) —
//!   tier-1 via `Result` return.
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
/// authentication. 1024 bytes is generous for any real-world password
/// while keeping the stack footprint bounded.
pub const MAX_PASSWORD_LEN: usize = 1024;

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
/// Constructed via [`Password::try_from_bytes`]. Rejects empty
/// (DEF-051) and over-length inputs. NUL bytes are allowed.
///
/// The inner storage is a fixed-size array with a length field,
/// avoiding heap allocation. `#[derive(Zeroize, ZeroizeOnDrop)]`
/// scrubs the full array (not just the used portion) on drop —
/// self-zeroizing regardless of wrapper context (DEF-093). A
/// compile-time `const _: () = assert!(needs_drop::<Password>())`
/// in `lib.rs` enforces this invariant structurally.
///
/// `len` is a `u16` (not `usize`) because `MAX_PASSWORD_LEN`
/// (1024) trivially fits; the narrower type saves 6 bytes per
/// `Password` instance which compounds through `Sensitive<Password>` →
/// `Credentials::ScramPassword` → `PgCommand::Startup` →
/// `ProtoState::ConnectingStartup`. (DEF-095)
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Password {
    /// Fixed-size backing store. The full array is zeroed on drop,
    /// not just `[..len]`.
    buf: [u8; MAX_PASSWORD_LEN],
    /// Number of valid bytes in `buf[..len]`.
    len: u16,
}

/// Errors from [`Password`] construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PasswordError {
    /// The password was empty. DEF-051: empty passwords are rejected
    /// at construction as a tier-1 visible choice (via `Result`).
    Empty,
    /// The password exceeds [`MAX_PASSWORD_LEN`] bytes.
    TooLong {
        /// Actual byte length of the rejected input.
        len: usize,
    },
}

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
    /// Construct from raw bytes.
    ///
    /// Rejects empty (DEF-051) and over-length passwords.
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

    /// Construct from a UTF-8 string.
    ///
    /// Convenience wrapper over [`Password::try_from_bytes`].
    pub fn try_from_str(s: &str) -> Result<Self, PasswordError> {
        Self::try_from_bytes(s.as_bytes())
    }

    /// Borrow the password bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        // `self.len <= MAX_PASSWORD_LEN` by constructor invariant.
        // `usize::from(u16)` is the forbid-bundle-safe widening.
        self.buf.get(..usize::from(self.len)).unwrap_or(&[])
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
/// accept the connection based on pg_hba.conf rules alone.
/// `ScramPassword` carries a password for SCRAM-SHA-256 authentication.
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box is unavailable; Password lives on the stack by design and Credentials is constructed once per connection, not per query")]
#[non_exhaustive]
pub enum Credentials {
    /// Trust authentication — no password required.
    Trust,
    /// Password-based authentication (SCRAM-SHA-256).
    ///
    /// The password is wrapped in [`Sensitive`] for zero-on-drop and
    /// debug redaction.
    ScramPassword(Sensitive<Password>),
}

impl fmt::Debug for Credentials {
    /// Manual impl — exhaustive match per variant. See module-level
    /// design rationale.
    ///
    /// # `#[non_exhaustive]` + exhaustive-inside-crate match
    ///
    /// `Credentials` carries `#[non_exhaustive]` so downstream crates
    /// MUST use a catch-all when matching. Inside THIS crate the
    /// match below is still exhaustive; adding a new variant is a
    /// build error HERE until the new variant's Debug path is
    /// declared. The combination is the tier-1 drift shield:
    /// a new internal variant cannot silently inherit a derived
    /// Debug that would leak secrets. DEF-048.
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
            Self::ScramPassword(_) => f.write_str("Credentials::ScramPassword(<REDACTED>)"),
        }
    }
}
