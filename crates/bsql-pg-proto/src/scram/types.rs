//! SCRAM-specific newtypes for secret values and bounded nonces.
//!
//! [`SecretDigest`] — 32-byte wrapper that deliberately omits `PartialEq`
//! / `Eq`, forcing all comparisons through constant-time `ct_eq`. DEF-039.
//!
//! [`CappedServerNonce`] — bounded byte buffer for the server's SCRAM
//! nonce. DEF-040.

use core::fmt;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A 32-byte secret-derived digest (SCRAM signature, proof, key).
///
/// **Deliberately does NOT implement `PartialEq` / `Eq`.** Any attempt
/// to `==`-compare two `SecretDigest` values is a compile error — the
/// only comparison path is [`SecretDigest::ct_eq`], which uses
/// [`subtle::ConstantTimeEq`] to prevent timing side-channels. DEF-039.
///
/// Scrubbed on drop via [`ZeroizeOnDrop`]. DEF-093: `#[repr(transparent)]`
/// for formal zero-cost ABI layout over `[u8; 32]`.
#[derive(Zeroize, ZeroizeOnDrop)]
#[repr(transparent)]
pub struct SecretDigest {
    bytes: [u8; 32],
}

impl SecretDigest {
    /// Wrap a 32-byte array.
    #[inline]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Constant-time comparison with another digest.
    ///
    /// Returns `subtle::Choice` (1 = equal, 0 = not equal). The
    /// comparison time is independent of the byte values.
    #[inline]
    #[must_use]
    pub fn ct_eq(&self, other: &Self) -> subtle::Choice {
        self.bytes.ct_eq(&other.bytes)
    }

    /// Borrow the raw bytes.
    ///
    /// Use sparingly — this is a secret value. Needed for XOR in
    /// client-proof computation and for base64-encoding the proof.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for SecretDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretDigest(<REDACTED>)")
    }
}

/// Maximum byte length for a server SCRAM nonce (raw, before base64).
///
/// RFC 5802 does not cap nonce length, but server nonces in practice
/// are <=128 bytes (PostgreSQL uses 18 random bytes, base64-encoded
/// to 24 chars, prefixed with the client nonce). 256 bytes accommodates
/// any reasonable server implementation.
pub const MAX_SERVER_NONCE_LEN: usize = 256;

/// A bounded server nonce for SCRAM authentication. DEF-040.
///
/// Constructible only via [`CappedServerNonce::try_from_bytes`], which
/// enforces the capacity bound. Downstream builders of
/// `client-final-message` accept only this type — an unbounded nonce
/// cannot reach the wire. DEF-093: `#[repr(transparent)]`.
#[derive(Clone)]
#[repr(transparent)]
pub struct CappedServerNonce {
    buf: heapless::Vec<u8, MAX_SERVER_NONCE_LEN>,
}

/// Error when a server nonce exceeds the capacity bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerNonceTooLong {
    /// Actual byte length of the rejected nonce.
    pub len: usize,
}

impl fmt::Display for ServerNonceTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "server nonce too long: {} bytes (max {})",
            self.len, MAX_SERVER_NONCE_LEN,
        )
    }
}

impl CappedServerNonce {
    /// Construct from raw bytes.
    ///
    /// Returns `Err` if the nonce exceeds [`MAX_SERVER_NONCE_LEN`].
    ///
    /// `pub` rather than `pub(crate)` because `tests/bounded_buffers_spec.rs`
    /// exercises the over-length rejection as a tier-2 structural
    /// regression shield.
    pub fn try_from_bytes(input: &[u8]) -> Result<Self, ServerNonceTooLong> {
        let mut buf = heapless::Vec::new();
        buf.extend_from_slice(input)
            .map_err(|_| ServerNonceTooLong { len: input.len() })?;
        Ok(Self { buf })
    }

    /// Borrow the nonce bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl fmt::Debug for CappedServerNonce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Nonce is not secret, but we truncate for readability.
        let len = self.buf.len();
        if len <= 32 {
            write!(f, "CappedServerNonce({len} bytes)")
        } else {
            write!(f, "CappedServerNonce({len} bytes, truncated)")
        }
    }
}
