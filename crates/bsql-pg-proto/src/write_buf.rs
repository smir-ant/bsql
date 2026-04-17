//! Bounded outbound frame builder.
//!
//! [`WriteBuf`] wraps `heapless::Vec<u8, MAX_OWNED_SEND_LEN>` with
//! PG-wire-aware helpers: [`push_u8`], [`push_u32_be`],
//! [`push_nul_terminated`], and [`with_length_prefix`] for the PG
//! "length includes itself but excludes tag" convention. Every mutator
//! returns `Result<(), WriteBufFull>` — no panic, no silent truncation.
//!
//! DEF-012/013/014: `MAX_OWNED_SEND_LEN`, `SendBuf::Owned`, `WriteBuf`.
//!
//! # `MAX_OWNED_SEND_LEN` sizing
//!
//! Worst-case Phase 1b outbound frame: `StartupMessage` with maximum-
//! length user (63), database (63), application_name (128), plus fixed
//! keys and separators:
//!
//! ```text
//! 4 bytes (length prefix)
//! 4 bytes (protocol version 196608 = 3.0)
//! "user\0" + user(63) + "\0"     =  5 + 63 + 1 =  69
//! "database\0" + db(63) + "\0"   =  9 + 63 + 1 =  73
//! "application_name\0" + app(128) + "\0" = 17 + 128 + 1 = 146
//! "\0" (terminator)              =  1
//! Total = 4 + 4 + 69 + 73 + 146 + 1 = 297
//! ```
//!
//! SASL messages are smaller (~200 bytes). We round up to 512 for
//! headroom against future startup parameters.

use core::fmt;

/// Maximum byte capacity for an owned outbound frame.
///
/// See module-level sizing math. 512 bytes covers worst-case Phase 1b
/// StartupMessage + SASLInitialResponse + SASLResponse with generous
/// margin.
pub const MAX_OWNED_SEND_LEN: usize = 512;

// Compile-time: the owned send buffer must hold at least the largest
// Phase 1b frame (297 bytes for a maxed StartupMessage).
const _: () = assert!(MAX_OWNED_SEND_LEN >= 297);

/// Bounded outbound frame buffer with PG wire builders.
///
/// See [module-level docs](self) for sizing rationale.
pub struct WriteBuf {
    inner: heapless::Vec<u8, MAX_OWNED_SEND_LEN>,
}

/// Returned when a write operation would exceed the buffer capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteBufFull;

impl fmt::Display for WriteBufFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("write buffer full")
    }
}

impl WriteBuf {
    /// Create an empty buffer.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: heapless::Vec::new(),
        }
    }

    /// Push a single byte.
    #[inline]
    pub fn push_u8(&mut self, byte: u8) -> Result<(), WriteBufFull> {
        self.inner.push(byte).map_err(|_| WriteBufFull)
    }

    /// Push a big-endian `u32`.
    pub fn push_u32_be(&mut self, val: u32) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push a big-endian `i32`.
    pub fn push_i32_be(&mut self, val: i32) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push raw bytes.
    pub fn push_bytes(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.inner
            .extend_from_slice(data)
            .map_err(|_| WriteBufFull)
    }

    /// Push a NUL-terminated string (bytes + `\0`).
    ///
    /// The input must not contain NUL — use [`Ident`] / [`ApplicationName`]
    /// / [`DatabaseName`] newtypes which guarantee this at construction.
    pub fn push_nul_terminated(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.push_bytes(data)?;
        self.push_u8(0)
    }

    /// Write a length-prefixed region using the PG convention:
    /// the 4-byte length field includes itself but excludes the tag.
    ///
    /// Writes a placeholder `[0,0,0,0]`, calls `body_fn(self)` to fill
    /// the body, then patches the placeholder with the actual length.
    ///
    /// Returns `Err(WriteBufFull)` if any write inside `body_fn` or the
    /// placeholder itself overflows.
    pub fn with_length_prefix(
        &mut self,
        body_fn: impl FnOnce(&mut Self) -> Result<(), WriteBufFull>,
    ) -> Result<(), WriteBufFull> {
        let start = self.inner.len();
        // Write placeholder length field (4 bytes of zeros).
        self.push_u32_be(0)?;
        // Let the caller fill the body.
        body_fn(self)?;
        // Compute the length: bytes written since start (includes the
        // 4-byte placeholder itself — PG convention).
        let body_len = self.inner.len().saturating_sub(start);
        let len_u32 = u32::try_from(body_len).map_err(|_| WriteBufFull)?;
        let len_bytes = len_u32.to_be_bytes();
        // Patch the placeholder at `start..start+4`.
        if let Some(slot) = self.inner.get_mut(start..start.saturating_add(4)) {
            slot.copy_from_slice(&len_bytes);
        }
        Ok(())
    }

    /// Current number of bytes written.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the buffer is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Consume the builder, returning the underlying `heapless::Vec`.
    #[inline]
    #[must_use]
    pub fn into_inner(self) -> heapless::Vec<u8, MAX_OWNED_SEND_LEN> {
        self.inner
    }

    /// Borrow the written bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }
}

impl Default for WriteBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for WriteBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteBuf")
            .field("len", &self.inner.len())
            .field("cap", &MAX_OWNED_SEND_LEN)
            .finish()
    }
}
