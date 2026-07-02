//! Bounded outbound frame builder.
//!
//! [`WriteBuf`] wraps `heapless::Vec<u8, MAX_OWNED_SEND_LEN>` with
//! PG-wire-aware helpers: [`WriteBuf::push_u8`], [`WriteBuf::push_u32_be`],
//! [`WriteBuf::push_nul_terminated`], and [`WriteBuf::with_length_prefix`] for the PG
//! "length includes itself but excludes tag" convention. Every mutator
//! returns `Result<(), WriteBufFull>` — no panic, no silent truncation.
//!
//! # `MAX_OWNED_SEND_LEN` sizing — const fn derivation
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
/// Derived from the worst case across StartupMessage,
/// SASLInitialResponse, SASLResponse, SimpleQuery, and Parse. The cap
/// is a const computed from the worst-case contributing inputs;
/// const asserts below tie it to every frame-builder's size math so a
/// future change to any contributing constant (`MAX_SQL_LEN`,
/// `MAX_PG_NAME_LEN`) without growing this cap becomes a build error,
/// not a runtime overflow.
///
/// Size breakdown (current values):
/// - StartupMessage worst case: ~305 bytes.
/// - SASLInitialResponse worst case: ~147 bytes.
/// - SASLResponse worst case: ~389 bytes.
/// - SimpleQuery (`Q`) worst case: 2054 bytes.
/// - **Parse (`P`) worst case: 2120 bytes** (tag + length + stmt_name
///   + NUL + SQL + NUL + i16 param-type count). Dominates.
pub const MAX_OWNED_SEND_LEN: usize = 2176;

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
/// - `client_encoding` (key `"client_encoding"`, 15 bytes): fixed value
///   `"UTF8"` (4 bytes), always sent to pin the session to UTF-8.
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
        .saturating_add(15) // "client_encoding"
        .saturating_add(1) // NUL
        .saturating_add(4) // "UTF8"
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

// Drift guard: bumping any contributing constant (MAX_IDENT_LEN,
// MAX_APP_NAME_LEN) or adding a StartupMessage parameter without
// growing MAX_OWNED_SEND_LEN fails the build here.
const _: () = assert!(MAX_OWNED_SEND_LEN >= max_startup_message_size());

/// Worst-case byte size of a PostgreSQL `Query` (`'Q'`) frame —
/// Simple Query protocol.
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

// Safety drift-pin: the `build_query_message` Err(WriteBufFull)
// branch is architecturally unreachable iff
// `MAX_OWNED_SEND_LEN >= max_simple_query_message_size()`. Without
// this assert, a full-size SQL (`MAX_SQL_LEN=2048`) could overflow
// a smaller WriteBuf cap at runtime, masquerading as
// `ProtocolError::InternalCrateBug { locus: OutboundFrameBuild
// { stage: Query } }`. With it: bumping `MAX_SQL_LEN` without
// growing the WriteBuf cap is a build error.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_simple_query_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case SimpleQuery ('Q') frame size — \
     full-size SQL would overflow the caller's WriteBuf. Grow \
     MAX_OWNED_SEND_LEN or shrink MAX_SQL_LEN in lockstep.",
);

/// Worst-case byte size of a PostgreSQL `Parse` (`'P'`) frame —
/// Extended Query protocol.
///
/// Layout (PG §55.7 Parse):
/// - Tag: `'P'` (1 byte)
/// - Length: `u32` BE including itself
/// - Statement name: up to [`crate::ident::MAX_PG_NAME_LEN`] bytes + NUL
/// - SQL text: up to [`crate::ident::MAX_SQL_LEN`] bytes + NUL
/// - Parameter type-count: `i16` (currently always 0 — no hints)
/// - Parameter type OIDs: `i32` × count (currently 0)
///
/// # Drift guard
///
/// Bumping [`crate::ident::MAX_PG_NAME_LEN`] or
/// [`crate::ident::MAX_SQL_LEN`] without growing
/// [`MAX_OWNED_SEND_LEN`] fails the `const _` assert below.
/// Parameter type hints are not yet supported; when they are, this
/// size formula widens (+4 × MAX_PARAM_COUNT).
pub const fn max_parse_message_size() -> usize {
    1usize // tag 'P'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(crate::ident::MAX_PG_NAME_LEN)
        .saturating_add(1) // stmt_name NUL
        .saturating_add(crate::ident::MAX_SQL_LEN)
        .saturating_add(1) // sql NUL
        .saturating_add(2) // i16 param-type count
    // No per-param-type OIDs (count is zero — param hints not yet
    // supported).
}

// Drift-pin: same pattern as SimpleQuery above. Parse without
// param-type hints fits comfortably under MAX_OWNED_SEND_LEN; param
// hints would require a corresponding cap bump.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_parse_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case Parse ('P') frame size — \
     full-size stmt_name + SQL would overflow the caller's WriteBuf. \
     Grow MAX_OWNED_SEND_LEN or shrink MAX_PG_NAME_LEN / MAX_SQL_LEN \
     in lockstep.",
);

/// Drift-pin for `push_parse` Parse+Sync bundle. `push_parse`
/// appends `SYNC_WIRE_BYTES` inline to `WriteBuf` after the Parse
/// frame (bytes-only push), so the caller's buffer must fit both
/// simultaneously. Without this assert, bumping `MAX_PG_NAME_LEN` /
/// `MAX_SQL_LEN` could allow a Parse frame that fills the buffer,
/// leaving no room for the trailing 5-byte Sync — a tier-4 "happens
/// to fit" gap. With this assert: tier-1 build failure on drift.
///
/// Sibling to the Bind+Execute+Sync and Describe+Sync drift pins
/// below.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_parse_message_size().saturating_add(5),
    "MAX_OWNED_SEND_LEN below worst-case Parse+Sync bundle. \
     push_parse appends Sync inline (bytes-only push). \
     Grow MAX_OWNED_SEND_LEN or shrink MAX_PG_NAME_LEN / MAX_SQL_LEN \
     in lockstep. `5` here is `SYNC_WIRE_BYTES.len()`.",
);

/// Worst-case byte size of a PostgreSQL `Bind` (`'B'`) frame —
/// tag + length prefix + portal name + stmt name + n_param_formats
/// + format codes + n_params + per-param length+data + n_result_formats.
///
/// Per-param format codes: [`crate::params::MAX_PARAMS_ARITY`] × 2 bytes.
/// Per-param length prefixes: [`crate::params::MAX_PARAMS_ARITY`] × 4 bytes.
/// Per-param data bytes total: [`crate::params::MAX_PARAMS_DATA_TOTAL`].
///
/// Bumping either const without growing [`MAX_OWNED_SEND_LEN`]
/// fails the `const _` assert on the Bind+Execute+Sync bundle below.
pub const fn max_bind_message_size() -> usize {
    1usize // tag 'B'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(crate::ident::MAX_PG_NAME_LEN)
        .saturating_add(1) // portal NUL
        .saturating_add(crate::ident::MAX_PG_NAME_LEN)
        .saturating_add(1) // stmt NUL
        .saturating_add(2) // n_param_formats
        .saturating_add(crate::params::MAX_PARAMS_ARITY.saturating_mul(2)) // format codes
        .saturating_add(2) // n_params
        .saturating_add(crate::params::MAX_PARAMS_ARITY.saturating_mul(4)) // per-param length prefixes
        .saturating_add(crate::params::MAX_PARAMS_DATA_TOTAL) // param data
        .saturating_add(4) // n_result_formats + 1 code: worst case is the
        // prepared path's `1, [Binary]` (4 bytes); the non-macro path's
        // `n_result_formats = 0` (2 bytes) fits within this.
}

/// Worst-case byte size of a PostgreSQL `Execute` (`'E'`) frame —
/// tag + length + portal name NUL-terminated + max_rows i32.
pub const fn max_execute_message_size() -> usize {
    1usize // tag 'E'
        .saturating_add(4) // length prefix
        .saturating_add(crate::ident::MAX_PG_NAME_LEN)
        .saturating_add(1) // portal NUL
        .saturating_add(4) // max_rows i32
}

/// Drift-pin: the Bind + Execute + Sync bundle ships in a single
/// `push_bind_execute` call, so the caller's WriteBuf must fit all
/// three worst-case messages simultaneously. Bumping
/// `MAX_PARAMS_DATA_TOTAL` / `MAX_PARAMS_ARITY` / `MAX_PG_NAME_LEN`
/// without growing `MAX_OWNED_SEND_LEN` is a build failure.
///
/// `SYNC_WIRE_BYTES` is a 5-byte static const (tag 'S' + BE u32
/// length=4); hard-coded as `5` here instead of referencing the
/// const to avoid a module cycle (wire.rs imports nothing from
/// write_buf, keeping that direction clean).
const _: () = assert!(
    MAX_OWNED_SEND_LEN
        >= max_bind_message_size()
            .saturating_add(max_execute_message_size())
            .saturating_add(5),
    "MAX_OWNED_SEND_LEN below worst-case Bind+Execute+Sync bundle. \
     Grow MAX_OWNED_SEND_LEN or shrink params::MAX_PARAMS_ARITY / \
     MAX_PARAMS_DATA_TOTAL / MAX_PG_NAME_LEN.",
);

/// Worst-case byte size of a PostgreSQL `Describe` (`'D'`) frame —
/// Extended Query protocol.
///
/// Wire layout per PG §55.2.2:
///
/// ```text
/// 'D' | len_i32 | target_byte('S'|'P') | name NUL
/// ```
///
/// Same worst-case for both target kinds (statement vs portal): the
/// name is either a [`crate::ident::StmtName`] or a
/// [`crate::ident::PortalName`], both of which are
/// `FixedStr<MAX_PG_NAME_LEN, _>` aliases. Capacity is identical.
pub const fn max_describe_message_size() -> usize {
    1usize // tag 'D'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(1) // target byte 'S' or 'P'
        .saturating_add(crate::ident::MAX_PG_NAME_LEN)
        .saturating_add(1) // name NUL
}

/// Drift-pin: `push_describe_*` emits a `Describe + Sync` bundle,
/// so the caller's WriteBuf must fit `max_describe_message_size()
/// + 5` simultaneously. Bumping `MAX_PG_NAME_LEN` without growing
/// `MAX_OWNED_SEND_LEN` is a build failure.
///
/// `5` here is `SYNC_WIRE_BYTES.len()` (tag `'S'` + BE u32 length=4).
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_describe_message_size().saturating_add(5),
    "MAX_OWNED_SEND_LEN below worst-case Describe+Sync bundle. \
     Grow MAX_OWNED_SEND_LEN or shrink MAX_PG_NAME_LEN.",
);

/// Decomposition drift-pin: PG §55.2.2 Describe frame is
/// `'D' (1) + len (4) + target (1) + name (N) + NUL (1)`. A
/// refactor that dropped the NUL, removed the target byte, or
/// otherwise corrupted the layout formula inside
/// `max_describe_message_size` would silently produce a wrong size
/// without this pin. Ties the computed total to the literal sum of
/// its documented parts.
const _: () = assert!(
    max_describe_message_size() == 7usize.saturating_add(crate::ident::MAX_PG_NAME_LEN),
    "Describe frame layout drift — PG §55.2.2: \
     'D' (1) + len (4) + target (1) + name (N) + NUL (1) = 7 + N",
);

/// Worst-case byte size of a PostgreSQL `PasswordMessage` (`'p'`)
/// frame for **cleartext** auth. PG §55.7 "PasswordMessage" — the
/// body is the password bytes followed by a single NUL terminator.
///
/// Wire layout:
///
/// ```text
/// 'p' (1) + len_i32 (4) + password (≤ MAX_PASSWORD_LEN) + NUL (1)
/// ```
///
/// # Drift guard
///
/// Bumping [`crate::password::MAX_PASSWORD_LEN`] without growing
/// [`MAX_OWNED_SEND_LEN`] fails the const-assert below — the
/// overflow cannot silently sneak in.
pub const fn max_password_message_size_cleartext() -> usize {
    1usize // tag 'p'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(crate::password::MAX_PASSWORD_LEN)
        .saturating_add(1) // NUL terminator
}

/// Worst-case byte size of a PostgreSQL `PasswordMessage` (`'p'`)
/// frame for **MD5** auth. The body is a fixed 35-byte digest
/// response (`"md5"` + 32 lowercase hex chars) plus NUL terminator.
/// Always smaller than the cleartext case; the
/// [`max_password_message_size`] umbrella const takes the max of
/// both for the global drift-pin.
///
/// Wire layout:
///
/// ```text
/// 'p' (1) + len_i32 (4) + "md5" + 32 hex chars (35 total) + NUL (1)
/// ```
pub const fn max_password_message_size_md5() -> usize {
    1usize // tag 'p'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(35) // "md5" (3) + 32 hex chars
        .saturating_add(1) // NUL terminator
}

/// Maximum across the two `PasswordMessage` shapes (cleartext +
/// MD5). The cleartext form dominates because passwords can be up
/// to [`crate::password::MAX_PASSWORD_LEN`] bytes; MD5 is fixed at
/// 35-byte body. Used by the global [`MAX_OWNED_SEND_LEN`]
/// drift-pin to make the `WriteBufFull` arm in
/// `crate::dispatch::build_password_message` +
/// `crate::dispatch::build_md5_password_message`
/// **architecturally impossible** rather than tier-3 by-classification.
pub const fn max_password_message_size() -> usize {
    let cleartext = max_password_message_size_cleartext();
    let md5 = max_password_message_size_md5();
    if cleartext > md5 { cleartext } else { md5 }
}

// PasswordMessage drift-pin: the cleartext + MD5 PasswordMessage
// builders' `Err(WriteBufFull)` arms are **architecturally
// unreachable** iff `MAX_OWNED_SEND_LEN >=
// max_password_message_size()`. Without the pin the builders
// propagate `WriteBufFull` via `?` to `InternalCrateBug
// { BuilderCapacityOverflow }` (tier-3 by classification — defence
// in depth, but not formally pinned to be impossible). With it:
// bumping `MAX_PASSWORD_LEN` without growing `MAX_OWNED_SEND_LEN`
// is a build error. Tier-1 architectural-impossibility for the
// cleartext + MD5 outbound frame paths.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_password_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case PasswordMessage frame size — \
     full-size cleartext password (MAX_PASSWORD_LEN bytes) plus tag + \
     length-prefix + NUL would overflow the caller's WriteBuf. Grow \
     MAX_OWNED_SEND_LEN or shrink MAX_PASSWORD_LEN in lockstep.",
);

// PasswordMessage layout-decomposition drift-pin: pin the cleartext
// + MD5 size formulas to their literal documented summations. A
// refactor that dropped the NUL, the length-prefix, or the tag byte
// from the formula would silently produce a wrong total without
// this pin. Ties the computed totals to the architectural shapes
// per PG §55.7.
const _: () = assert!(
    max_password_message_size_cleartext()
        == 6usize.saturating_add(crate::password::MAX_PASSWORD_LEN),
    "Cleartext PasswordMessage layout drift — PG §55.7: \
     'p' (1) + len (4) + password (N) + NUL (1) = 6 + N",
);
const _: () = assert!(
    max_password_message_size_md5() == 41,
    "MD5 PasswordMessage layout drift — PG §55.7: \
     'p' (1) + len (4) + 'md5' (3) + 32 hex chars (32) + NUL (1) = 41",
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

// `core::error::Error` impl on the write-buf-overflow sentinel.
impl core::error::Error for WriteBufFull {}

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
    #[inline]
    pub fn push_u32_be(&mut self, val: u32) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push a big-endian `i32`.
    #[inline]
    pub fn push_i32_be(&mut self, val: i32) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push a big-endian `i16`. Parallel to [`push_i32_be`] /
    /// [`push_u32_be`] — used by Extended Query frame builders
    /// (Parse's `n_param_types`, Bind's per-column format codes,
    /// etc.).
    ///
    /// [`push_i32_be`]: Self::push_i32_be
    /// [`push_u32_be`]: Self::push_u32_be
    #[inline]
    pub fn push_i16_be(&mut self, val: i16) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push a `u16` in big-endian. Used for non-negative count fields
    /// in PG wire messages (e.g., `n_params` in Bind). Equivalent to
    /// `push_i16_be(val as i16)` when the value fits `i16::MAX`, but
    /// the crate bans `as` casts — call `push_u16_be` directly instead.
    #[inline]
    pub fn push_u16_be(&mut self, val: u16) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push an `i64` in big-endian — used by the PG binary-format
    /// encoder for `int8` / `bigint` columns.
    #[inline]
    pub fn push_i64_be(&mut self, val: i64) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push a `u64` in big-endian — reserved for future unsigned
    /// 8-byte PG wire fields (none currently in use, added for API
    /// symmetry with the `u32` / `u16` pair).
    #[inline]
    pub fn push_u64_be(&mut self, val: u64) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push raw bytes.
    #[inline]
    pub fn push_bytes(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.inner
            .extend_from_slice(data)
            .map_err(|_| WriteBufFull)
    }

    /// Push a NUL-terminated string (bytes + `\0`).
    ///
    /// The input must not contain NUL — use [`crate::ident::Ident`] / [`crate::ident::ApplicationName`]
    /// / [`crate::ident::DatabaseName`] newtypes which guarantee this at construction.
    #[inline]
    pub fn push_nul_terminated(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.push_bytes(data)?;
        self.push_u8(0)
    }

    /// Write a length-prefixed region using the PG convention:
    /// the 4-byte length field includes itself but excludes the tag.
    #[inline]
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
        //
        // Explicit Err on the architecturally-dead None branch. The
        // `push_u32_be(0)` above guarantees `inner.len() >= start
        // + 4` — so `get_mut(start..)` + chunk extraction cannot
        // return None unless a future refactor removes or reorders
        // the placeholder push. Converting the alternative silent
        // no-op (`if let Some(slot)`) into an explicit Err means the
        // refactor fails with a classified `WriteBufFull` at the
        // first test run, rather than producing wire frames with a
        // length field of `0` that the server would reject as
        // `MalformedFrameLength`.
        //
        // `first_chunk_mut::<4>()` returns `Option<&mut [u8; 4]>` —
        // direct array assignment `*slot = len_bytes` compiles to a
        // single 32-bit store on aligned targets, vs the alternative
        // `copy_from_slice(&len_bytes)` which internally bounds-
        // checks `src.len() == dst.len()`. Both bounds checks
        // (get_mut range + copy_from_slice) fold into one
        // `first_chunk_mut` check.
        let Some(slot) = self
            .inner
            .get_mut(start..)
            .and_then(|s| s.first_chunk_mut::<4>())
        else {
            return Err(WriteBufFull);
        };
        *slot = len_bytes;
        Ok(())
    }

    /// Write an `i32` length-prefixed body where the length counts
    /// ONLY the body bytes (not the 4-byte length field itself).
    ///
    /// PG Bind frame `per-param: len i32 + bytes` uses this shape
    /// (vs [`WriteBuf::with_length_prefix`] which uses the "length includes
    /// itself" convention for top-level frames).
    ///
    /// The placeholder is reserved, the body function runs, the
    /// placeholder is patched with the body-only byte count. If any
    /// write overflows the buffer, `Err(WriteBufFull)` propagates.
    ///
    /// Note: for PG's SQL NULL param (wire length `-1`, no body),
    /// callers should `push_i32_be(-1)` directly instead of using
    /// this helper.
    #[inline]
    pub fn with_i32_length_prefixed_body<F>(
        &mut self,
        body_fn: F,
    ) -> Result<(), WriteBufFull>
    where
        F: FnOnce(&mut Self) -> Result<(), WriteBufFull>,
    {
        let len_offset = self.inner.len();
        self.push_i32_be(0)?; // placeholder
        let body_start = self.inner.len();
        body_fn(self)?;
        let body_len = self.inner.len().saturating_sub(body_start);
        let body_len_i32 = i32::try_from(body_len).map_err(|_| WriteBufFull)?;
        let bytes = body_len_i32.to_be_bytes();
        // Explicit Err on the architecturally-dead None branch
        // (mirrors `with_length_prefix`). A future refactor that
        // removes the placeholder push would fail with a typed error
        // at build-time tests instead of silently producing frames
        // with bogus length fields.
        //
        // Same `first_chunk_mut::<4>()` → single 32-bit store as
        // `with_length_prefix`.
        let Some(slot) = self
            .inner
            .get_mut(len_offset..)
            .and_then(|s| s.first_chunk_mut::<4>())
        else {
            return Err(WriteBufFull);
        };
        *slot = bytes;
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
    /// Called by `crate::PgProtocol::push_command` and
    /// `crate::PgProtocol::feed_bytes` at entry to reuse the
    /// caller-owned bounded storage across calls. Any previously
    /// issued `&[u8]` borrows into this buffer are invalidated — the
    /// borrow checker enforces that no such borrows exist at the
    /// point of `clear()` via the `&mut self` receiver.
    ///
    /// # Zero-on-clear discipline
    ///
    /// `heapless::Vec::clear()` by itself only resets the length to 0
    /// — the backing bytes persist in the 2176-byte array until a
    /// later `push_*` call overwrites them. Without zeroize, the
    /// buffer would retain **password-correlated SCRAM SASLResponse
    /// frames** (including the base64-encoded ClientProof) physically
    /// in RAM between SCRAM dispatch and the next `feed_bytes()` /
    /// `push_command()` call. Core-dump attackers on a long-lived
    /// connection would also find plaintext SQL from prior queries
    /// (e.g. `UPDATE users SET password='...'`) sitting in this
    /// buffer.
    ///
    /// Mitigation: overwrite the occupied prefix with zeros before
    /// truncating the length. Unoccupied bytes don't need scrubbing
    /// — they were either zero-initialised (fresh buffer) or already
    /// zeroed by a previous clear. Cost: O(len) memset on the hot
    /// path, which is L1-cache resident (≤ 2 KiB) and negligible
    /// relative to the write system call that typically follows.
    ///
    /// Pairs with manual `Drop` below: the `clear()` path handles
    /// reuse; `Drop` handles stack-frame teardown.
    #[inline]
    pub fn clear(&mut self) {
        use zeroize::Zeroize;
        self.inner.as_mut_slice().zeroize();
        self.inner.clear();
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

/// Manual Drop impl zeroizes the occupied prefix on scope teardown.
///
/// Rationale: `heapless::Vec` does NOT implement `zeroize::Zeroize`
/// (the upstream crate bounds `Zeroize` on `Default + Copy`, which
/// Vec does not satisfy). Scrub manually via `Zeroize` on the mut
/// slice (impl'd for `[u8]`). Ensures that on normal Drop — e.g.
/// when a wrapper's connection handle goes out of scope — any
/// residual password-correlated bytes (SASLResponse ClientProof) are
/// scrubbed.
///
/// Caveat: under `panic = "abort"` in the release profile, Drop does
/// NOT run on panic paths. The zeroize claim here is "best-effort
/// on normal control flow"; hard memory hygiene under panic requires
/// either `panic = "unwind"` or `mlock` + manual scrub — handled by
/// the driver-side panic hook for secret-bearing types. For
/// defense-in-depth, prefer explicit `Zeroizing<T>` wrappers on
/// stack locals within secret-bearing call frames.
impl Drop for WriteBuf {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.inner.as_mut_slice().zeroize();
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


#[cfg(test)]
mod drop_witness_tests {
    //! Tier-1-by-construction Drop-fire witness for [`WriteBuf`] via
    //! [`crate::drop_witness::DropCounter`].
    //!
    //! `WriteBuf::drop` is a manual `impl Drop` that calls
    //! `inner.as_mut_slice().zeroize()` (no `ZeroizeOnDrop` derive
    //! because `heapless::Vec` doesn't implement `Zeroize` upstream).
    //! Every `cargo test` increments the counter when
    //! `WriteBuf::drop` reaches its zeroize body. Catches regressions
    //! that remove the manual Drop impl (which would silently retain
    //! SCRAM proof / SQL bytes in the freed buffer's memory).

    use super::WriteBuf;
    use crate::drop_witness::{DropCounter, DropProbe};

    /// `WriteBuf::drop` fires its manual `inner.as_mut_slice().zeroize()`
    /// body. Counter increments iff Drop was reached.
    #[test]
    fn write_buf_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let wb = WriteBuf::new();
        DropCounter::scoped(wb, probe.clone(), || {
            assert_eq!(probe.fired(), 0);
        });
        assert_eq!(
            probe.fired(),
            1,
            "WriteBuf drop must fire exactly once",
        );
    }

    /// Repeated `WriteBuf` drops accumulate the counter.
    #[test]
    fn each_write_buf_drop_increments_counter() {
        let probe = DropProbe::new();
        for _ in 0..4 {
            DropCounter::scoped(WriteBuf::new(), probe.clone(), || {});
        }
        assert_eq!(probe.fired(), 4);
    }
}

// ═════════════════════════════════════════════════════════════════════
