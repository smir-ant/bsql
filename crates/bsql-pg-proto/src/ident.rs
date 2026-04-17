//! Bounded, NUL-free string newtypes for PostgreSQL startup parameters.
//!
//! [`Ident`], [`ApplicationName`], and [`DatabaseName`] are fixed-capacity
//! string wrappers that reject embedded NUL bytes at construction time.
//! This makes "NUL byte reaching the PG wire" **tier-1 impossible** —
//! the StartupMessage builder accepts only these types, and their
//! constructors refuse NUL. DEF-041.
//!
//! # Capacity
//!
//! PostgreSQL's `NAMEDATALEN` is 64 bytes (63 chars + NUL terminator).
//! Identifiers (user, database) are capped at 63. Application name is
//! conventionally capped at 64 but has no hard server limit; we use 128
//! to accommodate common patterns like `myapp-worker-pod-abc123`.
//!
//! Over-length inputs are rejected with a typed error — no silent
//! truncation (Part V ban).

use core::fmt;

/// Maximum byte length for a PostgreSQL identifier (user / database).
///
/// PostgreSQL `NAMEDATALEN = 64`; usable chars = 63.
pub const MAX_IDENT_LEN: usize = 63;

/// Maximum byte length for an application name parameter.
///
/// No hard PG limit; 128 bytes accommodates deployment-tagged names
/// like `myapp-worker-pod-abc123def456`.
pub const MAX_APP_NAME_LEN: usize = 128;

/// A PostgreSQL identifier (user name).
///
/// Guaranteed: non-empty, no embedded NUL, at most [`MAX_IDENT_LEN`]
/// bytes. These properties are tier-1 by constructor rejection.
#[derive(Clone, PartialEq, Eq)]
pub struct Ident {
    buf: heapless::Vec<u8, MAX_IDENT_LEN>,
}

/// A PostgreSQL database name.
///
/// Same invariants and capacity as [`Ident`]; separate type for
/// call-site clarity (you cannot accidentally pass a user name where
/// a database name is expected).
#[derive(Clone, PartialEq, Eq)]
pub struct DatabaseName {
    buf: heapless::Vec<u8, MAX_IDENT_LEN>,
}

/// A PostgreSQL `application_name` parameter.
///
/// Guaranteed: no embedded NUL, at most [`MAX_APP_NAME_LEN`] bytes.
/// May be empty (PG allows it).
#[derive(Clone, PartialEq, Eq)]
pub struct ApplicationName {
    buf: heapless::Vec<u8, MAX_APP_NAME_LEN>,
}

/// Errors from identifier / name construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentError {
    /// The input was empty. PG identifiers must be non-empty.
    Empty,
    /// The input contains a NUL byte, which PG uses as a field
    /// terminator in the wire protocol. Tier-1 rejection.
    ContainsNul,
    /// The input exceeds the capacity bound.
    TooLong {
        /// Actual byte length of the rejected input.
        len: usize,
        /// Maximum allowed.
        max: usize,
    },
}

impl fmt::Display for IdentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("identifier must not be empty"),
            Self::ContainsNul => f.write_str("identifier must not contain NUL bytes"),
            Self::TooLong { len, max } => {
                write!(f, "identifier too long: {len} bytes (max {max})")
            }
        }
    }
}

/// Validate raw bytes: non-empty, no NUL, within `max_len`.
fn validate_ident(input: &[u8], max_len: usize, require_non_empty: bool) -> Result<(), IdentError> {
    if require_non_empty && input.is_empty() {
        return Err(IdentError::Empty);
    }
    if input.contains(&0) {
        return Err(IdentError::ContainsNul);
    }
    if input.len() > max_len {
        return Err(IdentError::TooLong {
            len: input.len(),
            max: max_len,
        });
    }
    Ok(())
}

impl Ident {
    /// Construct from a UTF-8 string.
    ///
    /// Rejects empty, NUL-containing, and over-length inputs.
    pub fn try_from_str(s: &str) -> Result<Self, IdentError> {
        validate_ident(s.as_bytes(), MAX_IDENT_LEN, true)?;
        let mut buf = heapless::Vec::new();
        // Length already validated <= MAX_IDENT_LEN = capacity.
        // extend_from_slice cannot fail.
        if buf.extend_from_slice(s.as_bytes()).is_err() {
            return Err(IdentError::TooLong {
                len: s.len(),
                max: MAX_IDENT_LEN,
            });
        }
        Ok(Self { buf })
    }

    /// Borrow the identifier as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Borrow the identifier as a UTF-8 string slice.
    ///
    /// Always valid UTF-8 because the constructor accepts only `&str`.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Constructor guarantees UTF-8; this cannot fail.
        // We use from_utf8 (fallible) because the forbid-bundle bans
        // unsafe and from_utf8_unchecked. The Err branch is dead.
        core::str::from_utf8(&self.buf).unwrap_or("")
    }
}

impl DatabaseName {
    /// Construct from a UTF-8 string.
    ///
    /// Rejects empty, NUL-containing, and over-length inputs.
    pub fn try_from_str(s: &str) -> Result<Self, IdentError> {
        validate_ident(s.as_bytes(), MAX_IDENT_LEN, true)?;
        let mut buf = heapless::Vec::new();
        if buf.extend_from_slice(s.as_bytes()).is_err() {
            return Err(IdentError::TooLong {
                len: s.len(),
                max: MAX_IDENT_LEN,
            });
        }
        Ok(Self { buf })
    }

    /// Borrow the database name as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Borrow the database name as a UTF-8 string slice.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf).unwrap_or("")
    }
}

impl ApplicationName {
    /// Construct from a UTF-8 string.
    ///
    /// Rejects NUL-containing and over-length inputs. Empty is allowed
    /// (PG accepts an empty `application_name`).
    pub fn try_from_str(s: &str) -> Result<Self, IdentError> {
        validate_ident(s.as_bytes(), MAX_APP_NAME_LEN, false)?;
        let mut buf = heapless::Vec::new();
        if buf.extend_from_slice(s.as_bytes()).is_err() {
            return Err(IdentError::TooLong {
                len: s.len(),
                max: MAX_APP_NAME_LEN,
            });
        }
        Ok(Self { buf })
    }

    /// Borrow the application name as a byte slice.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Borrow the application name as a UTF-8 string slice.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.buf).unwrap_or("")
    }
}

impl fmt::Debug for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ident(\"{}\")", self.as_str())
    }
}

impl fmt::Debug for DatabaseName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DatabaseName(\"{}\")", self.as_str())
    }
}

impl fmt::Debug for ApplicationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApplicationName(\"{}\")", self.as_str())
    }
}
