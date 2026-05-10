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
/// SASLResponse, SimpleQuery, **and Parse** (1c-3a). The cap is a
/// const computed from the worst-case contributing inputs; const
/// asserts below tie it to every frame-builder's size math so a
/// future change to any contributing constant (`MAX_SQL_LEN`,
/// `MAX_PG_NAME_LEN`) without growing this cap becomes a build
/// error, not a runtime overflow.
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
// as `ProtocolError::InternalCrateBug { locus: OutboundFrameBuild { stage: Query } }`
// (DEF-150). Now: bumping
// `MAX_SQL_LEN` without growing the WriteBuf cap is a build error.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_simple_query_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case SimpleQuery ('Q') frame size — \
     full-size SQL would overflow the caller's WriteBuf. Grow \
     MAX_OWNED_SEND_LEN or shrink MAX_SQL_LEN in lockstep.",
);

/// Worst-case byte size of a PostgreSQL `Parse` (`'P'`) frame —
/// Extended Query protocol, 1c-3a.
///
/// Layout (PG §55.7 Parse):
/// - Tag: `'P'` (1 byte)
/// - Length: `u32` BE including itself
/// - Statement name: up to [`crate::ident::MAX_PG_NAME_LEN`] bytes + NUL
/// - SQL text: up to [`crate::ident::MAX_SQL_LEN`] bytes + NUL
/// - Parameter type-count: `i16` (can be 0 — no hints)
/// - Parameter type OIDs: `i32` × count (0 for 1c-3a — no hints)
///
/// # Drift guard
///
/// Bumping [`crate::ident::MAX_PG_NAME_LEN`] or
/// [`crate::ident::MAX_SQL_LEN`] without growing
/// [`MAX_OWNED_SEND_LEN`] fails the `const _` assert below. 1c-3a
/// does not yet support parameter type hints; when 1c-3b adds them,
/// this size formula widens (+4 × MAX_PARAM_COUNT).
pub const fn max_parse_message_size() -> usize {
    1usize // tag 'P'
        .saturating_add(4) // length prefix (includes itself)
        .saturating_add(crate::ident::MAX_PG_NAME_LEN)
        .saturating_add(1) // stmt_name NUL
        .saturating_add(crate::ident::MAX_SQL_LEN)
        .saturating_add(1) // sql NUL
        .saturating_add(2) // i16 param-type count
    // No per-param-type OIDs in 1c-3a (count is zero).
}

// 1c-3a drift-pin: same pattern as SimpleQuery above. Parse without
// param-type hints fits comfortably under MAX_OWNED_SEND_LEN; param
// hints will be added in 1c-3b with a corresponding cap bump.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_parse_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case Parse ('P') frame size — \
     full-size stmt_name + SQL would overflow the caller's WriteBuf. \
     Grow MAX_OWNED_SEND_LEN or shrink MAX_PG_NAME_LEN / MAX_SQL_LEN \
     in lockstep.",
);

/// DEF-212 (M1, audit 2026-05-04, architect-vetted impl plan): drift-pin
/// for `push_parse` Parse+Sync bundle. Pre-DEF-212 `push_parse` emitted
/// `[Action::SendBytes(parse_frame), Action::SendBytes(SYNC_WIRE_BYTES)]`
/// — the static `SendBytesStatic` variant did not consume `WriteBuf`
/// capacity. Post-DEF-212 (Alt Y' bytes-only push) the SYNC_WIRE_BYTES
/// is APPENDED inline to `WriteBuf` after the Parse frame, so the
/// caller's buffer must fit both simultaneously. Without this assert,
/// bumping `MAX_PG_NAME_LEN`/`MAX_SQL_LEN` could allow a Parse frame
/// that fills the buffer, leaving no room for the trailing 5-byte
/// Sync — a tier-4 "happens to fit" gap. With this assert: tier-1
/// build failure on drift.
///
/// Sibling to the existing Bind+Execute+Sync (line 208-217) and
/// Describe+Sync (line 247-251) drift pins.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_parse_message_size().saturating_add(5),
    "DEF-212 (M1): MAX_OWNED_SEND_LEN below worst-case Parse+Sync bundle. \
     push_parse appends Sync inline post-DEF-212 (bytes-only push). \
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
        .saturating_add(2) // n_result_formats (= 0 in 1c-3b, but field is always present)
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

/// Drift-pin (1c-3b): the Bind + Execute + Sync bundle ships in
/// a single `push_bind_execute` call, so the caller's WriteBuf must
/// fit all three worst-case messages simultaneously. Bumping
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
    "MAX_OWNED_SEND_LEN below worst-case Bind+Execute+Sync bundle \
     (1c-3b: push_bind_execute emits all three). Grow MAX_OWNED_SEND_LEN \
     or shrink params::MAX_PARAMS_ARITY / MAX_PARAMS_DATA_TOTAL / \
     MAX_PG_NAME_LEN.",
);

/// Worst-case byte size of a PostgreSQL `Describe` (`'D'`) frame —
/// Extended Query protocol, 1c-3c.
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

/// Drift-pin (1c-3c): `push_describe_*` emits a
/// `Describe + Sync` bundle, so the caller's WriteBuf must fit
/// `max_describe_message_size() + 5` simultaneously. Bumping
/// `MAX_PG_NAME_LEN` without growing `MAX_OWNED_SEND_LEN` is a
/// build failure.
///
/// `5` here is `SYNC_WIRE_BYTES.len()` (tag `'S'` + BE u32 length=4).
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_describe_message_size().saturating_add(5),
    "MAX_OWNED_SEND_LEN below worst-case Describe+Sync bundle. \
     Grow MAX_OWNED_SEND_LEN or shrink MAX_PG_NAME_LEN.",
);

/// Decomposition drift-pin (pass-#7 F15): PG §55.2.2 Describe frame
/// is `'D' (1) + len (4) + target (1) + name (N) + NUL (1)`. A
/// refactor that dropped the NUL, removed the target byte, or
/// otherwise corrupted the layout formula inside
/// `max_describe_message_size` would silently produce a wrong
/// size without this pin. Ties the computed total to the literal
/// sum of its documented parts.
const _: () = assert!(
    max_describe_message_size() == 7usize.saturating_add(crate::ident::MAX_PG_NAME_LEN),
    "Describe frame layout drift — PG §55.2.2: \
     'D' (1) + len (4) + target (1) + name (N) + NUL (1) = 7 + N",
);

/// Worst-case byte size of a PostgreSQL `PasswordMessage` (`'p'`)
/// frame for **cleartext** auth (DEF-215, 2026-05-05). PG §55.7
/// "PasswordMessage" — the body is the password bytes followed
/// by a single NUL terminator.
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
/// frame for **MD5** auth (DEF-216, 2026-05-05). The body is a
/// fixed 35-byte digest response (`"md5"` + 32 lowercase hex
/// chars) plus NUL terminator. Always smaller than the cleartext
/// case; the [`max_password_message_size`] umbrella const takes
/// the max of both for the global drift-pin.
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
/// [`crate::dispatch::build_password_message`] +
/// [`crate::dispatch::build_md5_password_message`]
/// **architecturally impossible** rather than tier-3 by-classification.
pub const fn max_password_message_size() -> usize {
    let cleartext = max_password_message_size_cleartext();
    let md5 = max_password_message_size_md5();
    if cleartext > md5 { cleartext } else { md5 }
}

// DEF-215 + DEF-216 drift-pin (audit 2026-05-07): the cleartext +
// MD5 PasswordMessage builders' `Err(WriteBufFull)` arms are
// **architecturally unreachable** if and only if
// `MAX_OWNED_SEND_LEN >= max_password_message_size()`. Pre-pin the
// builders propagated `WriteBufFull` via `?` to
// `InternalCrateBug { BuilderCapacityOverflow }` (tier-3 by
// classification — defence in depth, but not formally pinned to
// be impossible). Post-pin: bumping `MAX_PASSWORD_LEN` without
// growing `MAX_OWNED_SEND_LEN` is a build error. Tier-1
// architectural-impossibility for the cleartext + MD5 outbound
// frame paths.
const _: () = assert!(
    MAX_OWNED_SEND_LEN >= max_password_message_size(),
    "MAX_OWNED_SEND_LEN below worst-case PasswordMessage frame size — \
     full-size cleartext password (MAX_PASSWORD_LEN bytes) plus tag + \
     length-prefix + NUL would overflow the caller's WriteBuf. Grow \
     MAX_OWNED_SEND_LEN or shrink MAX_PASSWORD_LEN in lockstep.",
);

// DEF-215 + DEF-216 layout-decomposition drift-pin: pin the
// cleartext + MD5 size formulas to their literal documented
// summations. A refactor that dropped the NUL, the length-prefix,
// or the tag byte from the formula would silently produce a wrong
// total without this pin. Ties the computed totals to the
// architectural shapes per PG §55.7.
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

    /// Push a big-endian `i16`. Parallel to [`push_i32_be`] /
    /// [`push_u32_be`] — used by Extended Query frame builders
    /// (Parse's `n_param_types`, Bind's per-column format codes,
    /// etc.). 1c-3a.
    ///
    /// [`push_i32_be`]: Self::push_i32_be
    /// [`push_u32_be`]: Self::push_u32_be
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
    pub fn push_u16_be(&mut self, val: u16) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push an `i64` in big-endian — used by the PG binary-format
    /// encoder for `int8` / `bigint` columns.
    pub fn push_i64_be(&mut self, val: i64) -> Result<(), WriteBufFull> {
        let bytes = val.to_be_bytes();
        self.inner
            .extend_from_slice(&bytes)
            .map_err(|_| WriteBufFull)
    }

    /// Push a `u64` in big-endian — reserved for future unsigned
    /// 8-byte PG wire fields (1c-3b has none, added for API symmetry
    /// with the `u32` / `u16` pair).
    pub fn push_u64_be(&mut self, val: u64) -> Result<(), WriteBufFull> {
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
        //
        // F61 (pass #6 audit): explicit Err on the architecturally-dead
        // None branch. The `push_u32_be(0)` above guarantees
        // `inner.len() >= start + 4` — so `get_mut(start..)` + chunk
        // extraction cannot return None unless a future refactor
        // removes or reorders the placeholder push. Converting the
        // former silent no-op (`if let Some(slot)`) into an explicit
        // Err means the refactor fails with a classified
        // `WriteBufFull` at the first test run, rather than producing
        // wire frames with a length field of `0` that the server would
        // reject as `MalformedFrameLength`.
        //
        // DEF-184 (B15): `first_chunk_mut::<4>()` returns
        // `Option<&mut [u8; 4]>` — direct array assignment `*slot
        // = len_bytes` compiles to a single 32-bit store on aligned
        // targets, vs the pre-(184) `copy_from_slice(&len_bytes)`
        // which internally bounds-checks src.len() == dst.len().
        // Both bounds checks (get_mut range + copy_from_slice) fold
        // into one `first_chunk_mut` check.
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
    /// (vs [`with_length_prefix`] which uses the "length includes
    /// itself" convention for top-level frames).
    ///
    /// The placeholder is reserved, the body function runs, the
    /// placeholder is patched with the body-only byte count. If any
    /// write overflows the buffer, `Err(WriteBufFull)` propagates.
    ///
    /// Note: for PG's SQL NULL param (wire length `-1`, no body),
    /// callers should `push_i32_be(-1)` directly instead of using
    /// this helper.
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
        // F61: explicit Err on the architecturally-dead None branch
        // (mirrors `with_length_prefix`). A future refactor that
        // removes the placeholder push would fail with a typed error
        // at build-time tests instead of silently producing frames
        // with bogus length fields.
        //
        // DEF-184 (B15): same `first_chunk_mut::<4>()` → single
        // 32-bit store as `with_length_prefix`.
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
    /// DEF-094: called by [`crate::PgProtocol::push_command`] and
    /// [`crate::PgProtocol::feed_bytes`] at entry to reuse the
    /// caller-owned bounded storage across calls. Any previously
    /// issued `&[u8]` borrows into this buffer are invalidated — the
    /// borrow checker enforces that no such borrows exist at the
    /// point of `clear()` via the `&mut self` receiver.
    ///
    /// # DEF-185 P0-B (audit 2026-04-24): zero-on-clear discipline
    ///
    /// `heapless::Vec::clear()` by itself only resets the length to 0 —
    /// the backing bytes persist in the 2176-byte array until a later
    /// `push_*` call overwrites them. This left **password-correlated
    /// SCRAM SASLResponse frames** (including the base64-encoded
    /// ClientProof) physically in RAM between SCRAM dispatch and the
    /// next `feed_bytes()` / `push_command()` call. Core-dump attackers
    /// on a long-lived connection would also find plaintext SQL from
    /// prior queries (e.g. `UPDATE users SET password='...'`) sitting
    /// in this buffer.
    ///
    /// Post-fix: overwrite the occupied prefix with zeros before
    /// truncating the length. Unoccupied bytes don't need scrubbing —
    /// they were either zero-initialised (fresh buffer) or already
    /// zeroed by a previous clear. Cost: O(len) memset on the hot path,
    /// which is L1-cache resident (≤ 2 KiB) and negligible relative to
    /// the write system call that typically follows.
    ///
    /// Pairs with `ZeroizeOnDrop` below: the `clear()` path handles
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

/// DEF-185 P0-B (audit 2026-04-24): manual Drop impl zeroizes the
/// occupied prefix on scope teardown.
///
/// Rationale: `heapless::Vec` does NOT implement `zeroize::Zeroize`
/// (the upstream crate bounds `Zeroize` on `Default + Copy`, which Vec
/// does not satisfy). We scrub manually via `Zeroize` on the mut slice
/// (impl'd for `[u8]`). Ensures that on normal Drop — e.g. when a
/// wrapper's connection handle goes out of scope — any residual
/// password-correlated bytes (SASLResponse ClientProof) are scrubbed.
///
/// Caveat per DEF-185 P0-A: under `panic = "abort"` in the release
/// profile, Drop does NOT run on panic paths. The zeroize claim here
/// is "best-effort on normal control flow"; hard memory hygiene under
/// panic requires either `panic = "unwind"` or `mlock`+manual scrub.
/// Current stance: accept abort's fail-fast benefits; document the
/// limitation honestly; prefer explicit `Zeroizing<T>` wrappers on
/// stack locals within secret-bearing call frames for defense-in-depth.
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

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (B) Phase B1 — generatively-branded buffer scaffolding
// ═════════════════════════════════════════════════════════════════════
//
// Goal: upgrade [`WriteReserved<'a>`] (DEF-154 (A)) from capacity-only
// witness to capacity + **buffer-identity** witness. The identity
// proof comes from a generative lifetime `'brand` (GhostCell pattern):
// two distinct `with_branded` closures produce disjoint brands that
// the type system refuses to unify, closing the "apply range from
// buffer A to buffer B of same lifetime" crossover seam that a plain
// `&[u8]` lifetime cannot prove.
//
// Phase B1 scope — foundational types only:
//   - [`BrandedBytes<'brand, 'a>`]       — invariant-branded slice view
//   - [`BrandedWriteBuf<'a>`]    — invariant-branded WriteBuf
//     mutable borrow
//   - [`BrandedWriteReserved<'a>`] — branded capacity witness
//   - [`WriteBuf::with_branded`]         — HRTB generative constructor
//
// Phase B1 does NOT:
//   - Add push methods to `BrandedWriteReserved` (those land in B3
//     when `NonEmptyRange<'brand>` consumers need them).
//   - Touch existing `WriteReserved` / builder call sites (stays
//     side-by-side until B3 swaps the builders to the branded form).
//   - Thread brands through `StagedAction` / `dispatch` / `materialise`
//     (those land in B3/B4/B5).
//
// # `#[cfg(test)]` gating rationale — temporary through Phase B2
//
// These types have NO production callers in B1 — production code
// still uses the unbranded `WriteReserved` path. The crate-root
// `-D warnings` flag turns rustc's `dead_code` warning into an
// error, so pub(crate) items with zero call sites in non-test
// builds would fail compilation.
//
// Each B1 branded item carries `#[cfg(test)]` individually below.
// The gate is TEMPORARY: Phase B3 removes it when branded
// builders land and push_* methods on `BrandedWriteReserved`
// become production-reachable. Do NOT swap for
// `#[allow(dead_code)]` or `#[expect(dead_code)]` — both are
// project-banned per the "fix root cause, no lint hiding" policy.
// `cfg(test)` is honest scaffolding; `allow/expect` is drift.
//
// # Invariance mechanism
//
// Each branded type carries `PhantomData<fn(&'brand ()) -> &'brand ()>`
// — `'brand` appears in BOTH input and output position of the
// phantom function pointer, making it **invariant**. Covariant
// phantoms (e.g. `PhantomData<&'brand ()>`) would let the borrow
// checker subtype `'brand` to `'static` or to a shorter lifetime,
// breaking the generativity seal. This is the single most common
// trap in the GhostCell-style pattern.
//
// # Generativity via HRTB
//
// [`WriteBuf::with_branded`] takes `F: for<'brand> FnOnce(...)` —
// the higher-rank trait bound means `f` must accept ANY `'brand`
// the caller picks, so the caller cannot pre-fix `'brand` to a
// value they share with another scope. Each call creates a fresh,
// disjoint brand.

// DEF-154 (W): `BrandedBytes<'brand, 'a>` struct + impls DELETED.
//
// Pre-(W) this was a `&'a [u8]` + `PhantomData<fn(&'brand())>`
// wrapper claiming tier-1 infallible apply via brand-identity
// proof. DEF-154 (N) reduced `WriteRange::apply` to
// `Option<&'a [u8]>` (runtime-checked) — the brand's only tier-1
// deliverable evaporated. Post-(W) callers use `&'a [u8]`
// directly; `BrandedWriteBuf::{as_bytes, into_bytes}` and
// `BrandedWriteReserved::as_bytes` return plain slices.

/// Generatively-branded mutable borrow of a [`WriteBuf`].
///
/// Constructed via [`WriteBuf::with_branded`]. Inside the closure,
/// `'brand` is fresh and unique — it cannot be unified with any
/// brand outside the closure. Calling [`Self::reserve`] yields a
/// [`BrandedWriteReserved<'_>`] that carries the same
/// brand; builders (Phase B3) take `&mut BrandedWriteReserved`
/// and return `WriteRange` ranges tied to this buffer.
///
/// See module-level "Invariance mechanism" and "Generativity via
/// HRTB" for the soundness argument.
pub(crate) struct BrandedWriteBuf<'a> {
    /// Underlying mutable WriteBuf borrow. Access via
    /// [`Self::reserve`] → [`BrandedWriteReserved`] for builder
    /// scope, or via [`Self::into_bytes`] for the final
    /// materialise slice view.
    buf: &'a mut WriteBuf,
    // DEF-154 (W): `_brand: PhantomData<fn(&'brand ()) -> &'brand ()>`
    // DELETED. See module-level DEF-154 (W) block for rationale.
}

impl<'a> BrandedWriteBuf<'a> {
    /// Reserve the buffer for a builder, producing a capacity +
    /// brand witness. Mirrors [`WriteBuf::reserve`] (DEF-154 (A))
    /// with the added brand binding.
    ///
    /// # Why `&mut self` (not `self`)
    ///
    /// Phase B4 uses the "build then apply in same scope" pattern
    /// (pseudo-code, reflecting crate-internal types):
    ///
    /// ```text
    /// wb.with_branded(|mut wb| {
    ///     let range = {
    ///         let mut reserved = wb.reserve();        // &mut wb
    ///         build_query_message(&mut reserved)       // → WriteRange
    ///         // reserved drops; &mut wb borrow ends
    ///     };
    ///     let bytes = wb.as_bytes_branded();           // & wb — now re-accessible
    ///     range.apply(bytes)                           // Option<&[u8]> (DEF-154 N)
    /// })
    /// ```
    ///
    /// DEF-154 (R) P1-3: reclassified from `rust,ignore` to
    /// `text` — the example names crate-internal types
    /// (`BrandedWriteBuf`, `build_query_message`, `WriteRange`)
    /// that aren't `pub`, so compile-checking requires crate-test
    /// wiring. The pattern itself is illustrative prose.
    ///
    /// A consuming `self` version would make `wb` unusable after
    /// `reserve()` — the apply step requires `as_bytes_branded()`
    /// after the build, so reserve takes `&mut self` and returns
    /// a reserved with a shorter lifetime.
    #[inline]
    #[must_use]
    pub(crate) fn reserve(&mut self) -> BrandedWriteReserved<'_> {
        debug_assert!(
            self.buf.is_empty(),
            "BrandedWriteBuf::reserve must be called on a freshly-cleared buffer \
             — same MAX_OWNED_SEND_LEN capacity invariant as WriteBuf::reserve.",
        );
        BrandedWriteReserved { buf: self.buf }
    }

    /// DEF-154 (W): test-only borrowing view of the buffer.
    /// Pre-(W) returned `BrandedBytes<'brand, '_>`; post-(W) plain
    /// `&[u8]`.
    #[cfg(test)]
    #[inline]
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.buf.as_bytes()
    }

    /// Consume the scope and yield the underlying bytes with the
    /// full outer `'a` lifetime — the materialise-boundary op.
    ///
    /// DEF-154 (W): pre-(W) returned `BrandedBytes<'brand, 'a>`
    /// claiming buffer-identity proof for tier-1 apply; DEF-154 (N)
    /// had already reduced apply to `Option<&[u8]>` runtime-
    /// checked. Post-(W) returns `&'a [u8]` directly.
    #[inline]
    #[must_use]
    pub(crate) fn into_bytes(self) -> &'a [u8] {
        self.buf.as_bytes()
    }
}

impl fmt::Debug for BrandedWriteBuf<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrandedWriteBuf")
            .field("len", &self.buf.len())
            .finish()
    }
}

/// Branded capacity witness — the Phase B3 successor to
/// [`WriteReserved`] (DEF-154 (A)). Phase B1 defines the shape;
/// push methods land when builders are migrated in Phase B3.
///
/// Constructed via [`BrandedWriteBuf::reserve`]; holds a mutable
/// borrow of the underlying buffer. Same capacity guarantee as
/// [`WriteReserved`] (const-asserted ≥ every builder's max message
/// size) plus the brand-identity binding to the source
/// [`BrandedWriteBuf`].
pub(crate) struct BrandedWriteReserved<'a> {
    /// Underlying buffer — API-narrow wrapper preventing truncating
    /// ops mid-builder-scope.
    buf: &'a mut WriteBuf,
    // DEF-154 (W): `_brand: PhantomData<...>` DELETED. See
    // module-level (W) block.
}

impl BrandedWriteReserved<'_> {
    /// Current buffer length. Used by builder bodies to compute
    /// emission-time range endpoints (the same pattern as
    /// [`WriteReserved::len`]).
    #[inline]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    /// Mutable access to underlying WriteBuf for branded push ops
    /// (see `push_*` methods below). Kept private — callers use the
    /// typed push methods that forward through this.
    #[inline]
    fn buf_mut(&mut self) -> &mut WriteBuf {
        self.buf
    }

    // DEF-154 (M) P0-3: every `push_*` returns `Result<(), WriteBufFull>`.
    // Pre-(M) these methods silently discarded Err under a
    // `debug_assert!` shield — tier-4 silent corruption on capacity
    // drift (frame header already written assuming body-bytes to
    // follow; truncated body + correct-looking length prefix =
    // server-side framing desync / bit-junk on wire). Post-(M)
    // builders `?` propagate, Err classifies as
    // `CrateBugLocus::BuilderCapacityOverflow` at the builder-return
    // boundary.

    /// Push a single byte — branded mirror of [`WriteReserved::push_u8`].
    #[inline]
    pub(crate) fn push_u8(&mut self, byte: u8) -> Result<(), WriteBufFull> {
        self.buf_mut().push_u8(byte)
    }

    /// Push big-endian u16 — branded mirror of [`WriteReserved::push_u16_be`].
    #[inline]
    pub(crate) fn push_u16_be(&mut self, val: u16) -> Result<(), WriteBufFull> {
        self.buf_mut().push_u16_be(val)
    }

    /// Push big-endian i16 — branded mirror of [`WriteReserved::push_i16_be`].
    #[inline]
    pub(crate) fn push_i16_be(&mut self, val: i16) -> Result<(), WriteBufFull> {
        self.buf_mut().push_i16_be(val)
    }

    /// Push big-endian u32 — branded mirror of [`WriteReserved::push_u32_be`].
    #[inline]
    pub(crate) fn push_u32_be(&mut self, val: u32) -> Result<(), WriteBufFull> {
        self.buf_mut().push_u32_be(val)
    }

    /// Push big-endian i32 — branded mirror of [`WriteReserved::push_i32_be`].
    #[inline]
    pub(crate) fn push_i32_be(&mut self, val: i32) -> Result<(), WriteBufFull> {
        self.buf_mut().push_i32_be(val)
    }

    /// Push NUL-terminated bytes — branded mirror of [`WriteReserved::push_nul_terminated`].
    #[inline]
    pub(crate) fn push_nul_terminated(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.buf_mut().push_nul_terminated(data)
    }

    /// Push raw bytes — branded mirror of [`WriteReserved::push_bytes`].
    ///
    /// DEF-184 (A14): used by `build_bind_message` for the compact
    /// `[0, 1, 0, 1]` format-code block (n_format_codes=1 +
    /// format[0]=Binary), one 4-byte bulk write replacing
    /// `push_u16_be(1) + push_u16_be(1)` pair.
    #[inline]
    pub(crate) fn push_bytes(&mut self, data: &[u8]) -> Result<(), WriteBufFull> {
        self.buf_mut().push_bytes(data)
    }

    /// Write a PG length-prefixed body via a nested branded closure.
    /// Branded mirror of [`WriteReserved::with_length_prefix`].
    ///
    /// The inner closure receives a short-lived branded reserved
    /// with the SAME `'brand` as the outer — the brand is captured
    /// from `self`'s `'brand` parameter, so ranges produced inside
    /// the closure (via subsequent builders) are compatible with
    /// the outer scope.
    ///
    /// DEF-154 (M): body_fn returns `Result<(), WriteBufFull>`;
    /// outer returns same. Builders `?`-propagate both the outer
    /// length-prefix emission and the inner body's push results.
    #[inline]
    pub(crate) fn with_length_prefix<F>(&mut self, body_fn: F) -> Result<(), WriteBufFull>
    where
        F: FnOnce(&mut BrandedWriteReserved<'_>) -> Result<(), WriteBufFull>,
    {
        self.buf_mut().with_length_prefix(|inner_buf| {
            let mut inner_reserved = BrandedWriteReserved { buf: inner_buf };
            body_fn(&mut inner_reserved)
        })
    }

    /// DEF-154 (B) Phase B4 escape hatch — access underlying buffer
    /// mutably for APIs predating the brand (e.g.
    /// `ParamsWriter::write_params` in `build_bind_message`). Mirrors
    /// [`WriteReserved::as_write_buf_mut`]. Caller is responsible
    /// for shielding any `Result` return via `debug_assert!`.
    ///
    /// Safe because the branded scope's capacity invariant still
    /// holds iff builders follow their `max_{kind}_message_size()`
    /// const-asserted worst-case.
    #[inline]
    pub(crate) fn as_write_buf_mut(&mut self) -> &mut WriteBuf {
        self.buf
    }

    // DEF-160 Z2 (2026-05-11): `as_bytes(&self) -> &[u8]` removed.
    // Pre-Z2 it surfaced the WriteBuf's bytes inside the branded scope
    // for `materialise_push`'s M5 verification. Post-Z2 push paths emit
    // the wire frame via `OutActions` chunks (one of which carries the
    // header range — verified by `materialise` directly against the
    // unbranded `wb.as_bytes()` slice), so the in-branded read view is
    // no longer reachable from any call site. Re-adding it requires
    // identifying a new caller and re-justifying the brand exposure.
}

impl fmt::Debug for BrandedWriteReserved<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrandedWriteReserved")
            .field("len", &self.buf.len())
            .finish()
    }
}

impl WriteBuf {
    /// Enter a generatively-branded scope.
    ///
    /// The HRTB `for<'brand>` in the closure bound forces the
    /// caller to accept ANY `'brand` the compiler chooses —
    /// producing a fresh, disjoint brand per call that cannot
    /// unify with any other scope's brand. Inside the closure,
    /// [`BrandedWriteBuf<'brand, '_>`] wraps `self` with that
    /// brand. Builders (Phase B3) take
    /// `&mut BrandedWriteReserved<'_>` and produce
    /// ranges tied to this brand; [`Self::as_bytes_branded`] at
    /// materialise time accepts only same-brand ranges.
    ///
    /// # Example (Phase B5 call site — illustrative)
    ///
    /// ```text
    /// self.write_buf.with_branded(|wb| {
    ///     let mut reserved = wb.reserve();
    ///     let range = build_query_message(&mut reserved, &cmd);
    ///     // range: WriteRange; wb.as_bytes_branded(): BrandedBytes<'brand, '_>
    ///     let bytes = range.apply(wb.as_bytes_branded())?; // Option<&[u8]> (DEF-154 N)
    ///     Action::SendBytes(bytes)  // &[u8] is unbranded — escapes the closure
    /// })
    /// ```
    ///
    /// DEF-154 (R) P1-3: reclassified from `rust,ignore` to
    /// `text` — the example names crate-internal types not `pub`.
    ///
    /// # Soundness argument
    ///
    /// Each call to [`Self::with_branded`] instantiates the `'brand`
    /// parameter with a FRESH existential lifetime (HRTB semantics).
    /// The invariant phantom `PhantomData<fn(&'brand ()) ->
    /// &'brand ()>` on [`BrandedWriteBuf`] / [`BrandedBytes`] /
    /// [`BrandedWriteReserved`] prevents the subtyping system from
    /// equating two distinct brands. Therefore a range built under
    /// brand X cannot be applied to bytes branded under Y — a
    /// compile error, not a runtime check.
    /// DEF-154 (W): HRTB `for<'brand>` stripped. Post-(W) no brand,
    /// the closure is plain `FnOnce(BrandedWriteBuf<'w>) -> R`.
    #[inline]
    pub(crate) fn with_branded<'w, R, F>(&'w mut self, f: F) -> R
    where
        F: FnOnce(BrandedWriteBuf<'w>) -> R,
    {
        f(BrandedWriteBuf { buf: self })
    }
}

#[cfg(test)]
mod drop_witness_tests {
    //! DEF-259 (2026-05-08): tier-1-by-construction Drop-fire witness
    //! for [`WriteBuf`] via [`crate::drop_witness::DropCounter`].
    //!
    //! Pre-DEF-259: `WriteBuf::drop` had no per-type witness — it is
    //! a manual `impl Drop` (`write_buf.rs:673`) that calls
    //! `inner.as_mut_slice().zeroize()` (no `ZeroizeOnDrop` derive
    //! because `heapless::Vec` doesn't implement `Zeroize` upstream).
    //! Verified only transitively via integration tests dropping
    //! `WriteBuf` mid-handshake.
    //!
    //! Post-DEF-259: every `cargo test` increments the counter when
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
        {
            let _w = DropCounter::new(wb, probe.clone());
            assert_eq!(probe.fired(), 0);
        }
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
            let _w = DropCounter::new(WriteBuf::new(), probe.clone());
        }
        assert_eq!(probe.fired(), 4);
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (B) Phase B1 — tests
// ═════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod phase_b1_tests {
    //! Phase B1 behavioural + structural pins.
    //!
    //! Compile-fail cases (two brands rejected at the type level)
    //! land in a trybuild harness during a later phase. Phase B1
    //! pins: the happy path round-trips, brand-bearing types are
    //! non-zero-size-neutral (phantom ZST), unbranding produces the
    //! underlying slice.
    use super::*;

    /// B1-1 (post DEF-154 W): `with_branded` round-trip — construct
    /// a buffer, wrap it in a BrandedWriteBuf scope, observe
    /// `as_bytes()` slice. Pre-(W) this exercised the brand's
    /// `as_slice()` discharge at the "unbranding boundary"; post-(W)
    /// `BrandedBytes` is gone and `as_bytes()` just returns `&[u8]`.
    #[test]
    fn with_branded_round_trip_empty() {
        let mut buf = WriteBuf::new();
        let slice_len = buf.with_branded(|wb| wb.as_bytes().len());
        assert_eq!(slice_len, 0, "fresh buffer has zero-length slice view");
    }

    /// B1-2 (post W): `with_branded` produces a BrandedWriteReserved
    /// via `reserve()` — capacity-witness path intact.
    #[test]
    fn branded_reserve_preserves_len() {
        let mut buf = WriteBuf::new();
        let reserved_len = buf.with_branded(|mut wb| wb.reserve().len());
        assert_eq!(reserved_len, 0);
    }

    // DEF-160 Z2 (2026-05-11): `branded_reserve_as_bytes_len_mirrors_buf`
    // test DELETED. It pinned `BrandedWriteReserved::as_bytes` which has
    // also been removed (orphan after `materialise_push` deletion — Z2
    // unifies push / feed materialisation through the single feed-side
    // `materialise` against the unbranded `wb.as_bytes()` slice).

    // DEF-154 (W): `branded_bytes_empty_is_empty` test DELETED —
    // it tested `BrandedBytes::empty()` factory which no longer
    // exists (struct deleted; callers use `&[]` literal directly).

    /// B1-4 (post W): drift pin on sizes — BrandedWriteBuf /
    /// BrandedWriteReserved are `&mut WriteBuf` sized; no phantom,
    /// no overhead.
    #[test]
    fn branded_wrapper_sizes_are_ref_only() {
        assert_eq!(
            core::mem::size_of::<BrandedWriteReserved<'_>>(),
            core::mem::size_of::<&mut WriteBuf>(),
            "BrandedWriteReserved must be &mut-WriteBuf-sized (phantom is ZST).",
        );
    }
}
