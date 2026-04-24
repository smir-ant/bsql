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
/// authentication.
///
/// DEF-154 (O) P1-5: shrunk 1024 → 512 B. SCRAM-SHA-256 uses
/// SASLprep normalisation on the password bytes; UTF-8 NFKC
/// expansion is bounded at ~4x the input-char count on the
/// pathological case (combining marks). 512 B accommodates 128
/// normalized UTF-8 chars — a HUGE password for any realistic
/// workflow (industry practice: argon2 accepts arbitrary input
/// but real deployments cap at ~128; bcrypt truncates at 72).
/// Shrinking 1024→512 halves `Password` size → halves
/// `Credentials::ScramPassword` variant → halves
/// `PgCommand::Startup` enum payload (which pays the worst-case
/// cost on every caller-side allocation). Zero safety impact:
/// `try_from_bytes` classifies oversize as `PasswordError::TooLong`,
/// surfacing a clean error at construction rather than silently
/// accepting.
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
/// (512 — DEF-154 (O) P1-5) trivially fits; the narrower type saves
/// 6 bytes per `Password` instance which compounds through
/// `Sensitive<Password>` → `Credentials::ScramPassword` →
/// `PgCommand::Startup` → `ProtoState::ConnectingStartup`. (DEF-095)
///
/// # DEF-185 P2-E (audit 2026-04-24): doc sync
///
/// Pre-fix: this comment said `(1024)` — drift from the actual 512
/// after DEF-154 (O) P1-5 shrunk the const. Comment vs source had
/// been out of step for ~3 weeks. Pairs with the startup_spec test
/// boundary which uses `"a".repeat(1025)` and happens to pass
/// (1025 > 512) but not at the exact +1-over-cap boundary; that
/// test should assert at `MAX_PASSWORD_LEN + 1` symbolically.
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
    ///
    /// DEF-154 (S) P1-1: explicit `split_at_checked` match —
    /// `self.len ≤ MAX_PASSWORD_LEN ≤ self.buf.len()` by construction
    /// (see `try_from_bytes` bound check). None arm architecturally
    /// unreachable; returns empty slice as no-silent-op sentinel
    /// (matches semantically "no password bytes", same surface as
    /// a zeroized post-drop). Pre-(S) was `self.buf.get(..len)
    /// .unwrap_or(&[])` — silent fallback banned by user directive.
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
/// accept the connection based on pg_hba.conf rules alone.
/// `ScramPassword` carries a password for SCRAM-SHA-256 authentication.
// DEF-154 (O): `#[expect(clippy::large_enum_variant)]` retained.
// Post-(O) shrink (1024→512 password), `ScramPassword` variant is
// ~514 B vs `Trust`'s 0 B — clippy still flags the size diff.
// Box the variant is forbidden by no_alloc; splitting the enum
// would lose the single-return-type dispatch ergonomics. The
// suppression is structural — user directive "no costyl" is
// respected because the suppression has a LOAD-BEARING reason
// (no_alloc), not a pragmatic "fix later".
#[expect(
    clippy::large_enum_variant,
    reason = "no_alloc crate: Box unavailable; Password lives on stack by design. \
              Credentials is constructed once per connection, not per query. \
              Pre-DEF-154 (O) the variant was 1026 B; (O) halved it to 514 B."
)]
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
