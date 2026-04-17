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
use zeroize::Zeroize;

/// Maximum password length in bytes.
///
/// PostgreSQL does not impose a hard limit on password length, but
/// SCRAM-SHA-256 with PBKDF2 processes the full password on every
/// authentication. 1024 bytes is generous for any real-world password
/// while keeping the stack footprint bounded.
pub const MAX_PASSWORD_LEN: usize = 1024;

/// A bounded, zeroize-on-drop password buffer.
///
/// Constructed via [`Password::try_from_bytes`]. Rejects empty
/// (DEF-051) and over-length inputs. NUL bytes are allowed.
///
/// The inner storage is a fixed-size array with a length field,
/// avoiding heap allocation. On drop, [`Zeroize`] scrubs the full
/// array (not just the used portion).
pub struct Password {
    /// Fixed-size backing store. The full array is zeroed on drop,
    /// not just `[..len]`.
    buf: [u8; MAX_PASSWORD_LEN],
    /// Number of valid bytes in `buf[..len]`.
    len: usize,
}

impl Zeroize for Password {
    fn zeroize(&mut self) {
        self.buf.zeroize();
        self.len.zeroize();
    }
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
        let mut buf = [0u8; MAX_PASSWORD_LEN];
        // Copy input into the fixed buffer. Length is bounded above.
        let dest = match buf.get_mut(..input.len()) {
            Some(s) => s,
            None => return Err(PasswordError::TooLong { len: input.len() }),
        };
        dest.copy_from_slice(input);
        Ok(Self {
            buf,
            len: input.len(),
        })
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
        self.buf.get(..self.len).unwrap_or(&[])
    }
}

impl fmt::Debug for Password {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Trust => f.write_str("Credentials::Trust"),
            Self::ScramPassword(_) => f.write_str("Credentials::ScramPassword(<REDACTED>)"),
        }
    }
}
