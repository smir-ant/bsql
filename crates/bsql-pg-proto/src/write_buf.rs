//! Bounded outbound frame builder.
//!
//! [`WriteBuf`] wraps `heapless::Vec<u8, MAX_OWNED_SEND_LEN>` with
//! PG-wire-aware helpers: [`push_u8`], [`push_u32_be`],
//! [`push_nul_terminated`], and [`with_length_prefix`] for the PG
//! "length includes itself but excludes tag" convention. Every mutator
//! returns `Result<(), WriteBufFull>` — no panic, no silent truncation.
//!
//! DEF-012/013/014: `MAX_OWNED_SEND_LEN`, `SendBuf`, `WriteBuf`.
//!
//! # `MAX_OWNED_SEND_LEN` sizing — const fn derivation (DEF-057)
//!
//! The cap is not a hardcoded magic number. [`max_startup_message_size`]
//! computes the worst-case `StartupMessage` byte length from its
//! components — the underlying `MAX_IDENT_LEN`, `MAX_APP_NAME_LEN`, and
//! fixed key names. The `const _` assert below ties the buffer cap to
//! those inputs: bumping `MAX_IDENT_LEN` or adding a StartupMessage
//! parameter without growing the cap fails the build.
//!
//! SASL frame sizes have analogous drift-guards in `scram::wire`.

use core::fmt;

/// Maximum byte capacity for an owned outbound frame.
///
/// Derived from the worst case across StartupMessage, SASLInitialResponse,
/// SASLResponse, **and SimpleQuery** (1c-1b). The cap is a const
/// computed from the worst-case contributing inputs; const asserts
/// below tie it to every frame-builder's size math so a future
/// change to any contributing constant (e.g. `MAX_SQL_LEN`) without
/// growing this cap becomes a build error, not a runtime overflow.
///
/// Size breakdown (current values):
/// - StartupMessage worst case: ~305 bytes.
/// - SASLInitialResponse worst case: ~147 bytes.
/// - SASLResponse worst case: ~389 bytes.
/// - **SimpleQuery (`Q`) worst case: 2054 bytes** (tag + length +
///   MAX_SQL_LEN + NUL). Dominates; drives the cap.
pub const MAX_OWNED_SEND_LEN: usize = 2112;

/// Worst-case byte size of a PostgreSQL `StartupMessage` frame.
///
/// `StartupMessage` has no tag byte; the 4-byte length prefix includes
/// itself per PG spec. The body is a fixed 4-byte protocol version
/// followed by NUL-terminated key/value pairs, ending in a single NUL
/// terminator.
///
/// # Drift-guarded inputs
///
/// - `user` (key `"user"`, 4 bytes): value up to [`crate::ident::MAX_IDENT_LEN`].
/// - `database` (key `"database"`, 8 bytes): value up to [`crate::ident::MAX_IDENT_LEN`].
/// - `application_name` (key `"application_name"`, 16 bytes): value
///   up to [`crate::ident::MAX_APP_NAME_LEN`].
///
/// Changing any of the inputs without growing [`MAX_OWNED_SEND_LEN`]
/// fails the `const _` assert below.
pub const fn max_startup_message_size() -> usize {
    // `saturating_add` keeps the const body clean of `+` operators;
    // the crate-root forbid-bundle bans `arithmetic_side_effects` even
    // in const context, and saturating arithmetic is the accepted form
    // across the rest of the crate.
    4usize // length prefix
        .saturating_add(4) // protocol version
        .saturating_add(4) // "user"
        .saturating_add(1) // NUL
        .saturating_add(crate::ident::MAX_IDENT_LEN)
        .saturating_add(1) // NUL
        .saturating_add(8) // "database"
        .saturating_add(1) // NUL
        .saturating_add(crate::ident::MAX_IDENT_LEN)
        .saturating_add(1) // NUL
        .saturating_add(16) // "application_name"
        .saturating_add(1) // NUL
        .saturating_add(crate::ident::MAX_APP_NAME_LEN)
        .saturating_add(1) // NUL
        .saturating_add(1) // trailing empty-key NUL
}

// DEF-057 drift guard. Bumping any contributing constant (MAX_IDENT_LEN,
// MAX_APP_NAME_LEN) or adding a StartupMessage parameter without
// growing MAX_OWNED_SEND_LEN fails the build here.
const _: () = assert!(MAX_OWNED_SEND_LEN >= max_startup_message_size());

/// Worst-case byte size of a PostgreSQL `Query` (`'Q'`) frame —
/// Simple Query protocol. 1c-1b.
///
/// Layout (PG §55.7 Simple Query):
/// - Tag: `'Q'` (1 byte)
/// - Length: `u32` BE including itself
/// - SQL text: up to [`crate::ident::MAX_SQL_LEN`] bytes
/// - NUL terminator (1 byte)
///
/// # Drift guard
///
/// Bumping [`crate::ident::MAX_SQL_LEN`] (truncation threshold on
/// `Sql::from_str_truncating`) without growing [`MAX_OWNED_SEND_LEN`]
/// fails the `const _` assert below — the overflow cannot silently
/// sneak in.
pub const fn max_simple_query_message_size() -> usize {
    1usize // tag 'Q'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(crate::ident::MAX_SQL_LEN)
        .saturating_add(1) // NUL terminator
}

// 1c-1b safety drift-pin: the `build_query_message` Err(WriteBufFull)
// branch is architecturally unreachable if and only if
// `MAX_OWNED_SEND_LEN >= max_simple_query_message_size()`. Previously
// this invariant was NOT asserted — a full-size SQL (`MAX_SQL_LEN=2048`)
// would in fact overflow a 512-byte WriteBuf at runtime, masquerading
// as `ProtocolError::ProtocolInvariantBroken`. Now: bumping
// `MAX_SQL_LEN` without growing the WriteBuf cap is a build error.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_simple_query_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case SimpleQuery ('Q') frame size — \
     full-size SQL would overflow the caller's WriteBuf. Grow \
     MAX_OWNED_SEND_LEN or shrink MAX_SQL_LEN in lockstep.",
);

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

    /// Reset the buffer length to zero without deallocating.
    ///
    /// DEF-094: called by [`crate::PgProtocol::push_command`] and
    /// [`crate::PgProtocol::feed_bytes`] at entry to reuse the
    /// caller-owned bounded storage across calls. Any previously
    /// issued `&[u8]` borrows into this buffer are invalidated — the
    /// borrow checker enforces that no such borrows exist at the
    /// point of `clear()` via the `&mut self` receiver.
    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
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
