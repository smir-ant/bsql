//! Row-schema + row-body decoding primitives.
//!
//! `bsql-pg-proto` owns the raw wire encoding of a result-set: the
//! `RowDescription` frame tells us column count, type OIDs, and
//! per-column format codes; each `DataRow` frame carries the column
//! values. This module parses `RowDescription` into [`RowDesc`]
//! (shared between the row-streaming `ColEvent` API and
//! [`crate::Reply::QueryComplete`]) and hosts the typed-decoder
//! primitives that materialise column bytes into Rust types.
//!
//! # Why POD + bounded capacity
//!
//! The crate is `no_alloc`. `RowDesc` is a flat inline struct
//! holding a `[ColumnDesc; MAX_ROW_COLUMNS]` array alongside a
//! `u16` populated count — `Copy`, no `Drop`. Result-sets with
//! more than [`MAX_ROW_COLUMNS`] columns land in
//! [`crate::ProtocolError::TooManyColumns`] at parse time (tier-2
//! structural — the bound is enforced at construction, no silent
//! truncation).
//!
//! # Tier notes
//!
//! Schema ingest is **tier-2 structural**. The parser produces
//! `RowDesc` only on well-formed payloads
//! (`MalformedRowDescription` on framing errors,
//! `UnexpectedFormatCode` on values outside `{0, 1}`). A malformed
//! response tears the connection down via the usual `Errored`
//! outcome.
//!
//! Schema access is **tier-1 compile** on pairing:
//! `Action::StreamRow` carries `&'r RowDesc` — the `'r` lifetime
//! prevents the user from using a stale schema after the protocol
//! advances to a new query.

use core::fmt;

/// Maximum columns per result-set. Queries returning more columns
/// classify as [`crate::ProtocolError::TooManyColumns`] — the
/// connection stays alive (recoverable), the user retries with a
/// narrower projection.
///
/// 1600 matches PG's `MaxTupleAttributeNumber`. Since RowDesc is
/// now heap-allocated (`Box<[u32]>`, exact-size), this constant
/// only affects the parse-time rejection threshold — not storage.
///
/// # Effective wire limit
///
/// The RowDescription frame for >~140 typical columns exceeds
/// `READ_BUF_CAP` (4096 B) and enters partial-assembly mode
/// (`PREFIX_CAP` = 8 KB prefix + skip tail). Queries with
/// ~300-400 columns parse from the prefix; beyond ~400 the
/// prefix truncation causes `MalformedRowDescription`. The
/// RowDesc TYPE has no cap — only the wire infrastructure
/// constrains the effective maximum.
pub const MAX_ROW_COLUMNS: usize = 1600;

/// PostgreSQL wire format for one column's bytes.
///
/// - [`FormatCode::Text`] (wire code `0`) — ASCII-ish representation
///   (e.g., `"42"` for int4, `"t"`/`"f"` for bool). Simple Query always
///   uses text.
/// - [`FormatCode::Binary`] (wire code `1`) — PG's typed binary layout
///   (BE integers, fixed-width / length-prefixed strings). Selected
///   per-column in Extended Query via the Bind frame.
///
/// Any other wire value classifies as
/// [`crate::ProtocolError::UnexpectedFormatCode`].
///
/// # NOT `#[non_exhaustive]`
///
/// PG §55.2.2 defines exactly two format codes (`0` text, `1` binary)
/// and the wire-protocol enumeration is closed by spec. A third value
/// would be a major-protocol-version bump (PG 4.0?) — not a SemVer-
/// compatible addition. Sealing via `non_exhaustive` would force
/// downstream consumers to keep a catch-all arm for a case that
/// **cannot exist on a well-formed wire**; the dispatcher already
/// classifies any non-{0,1} byte as `UnexpectedFormatCode` BEFORE
/// constructing this enum. Closed-by-spec → exhaustive `match` is
/// the load-bearing tier-1 invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum FormatCode {
    /// Text format — `0` on the wire.
    #[default]
    Text = 0,
    /// Binary format — `1` on the wire.
    Binary = 1,
}

impl FormatCode {
    /// Classify a wire i16 format-code byte into the typed variant.
    ///
    /// PG §55.2.2 defines exactly two legal values: `0` (text) and
    /// `1` (binary). Any other value is a server-side wire violation
    /// and returns the offending code in `Err` for the caller to wrap
    /// into `ProtocolError::UnexpectedFormatCode`.
    ///
    /// # Single classifier
    ///
    /// Centralises the `{0, 1}` classification so consumers (Bind,
    /// Describe, BindExecute) that also parse format codes don't
    /// each rewrite the same match. A new illegal value surfaces
    /// with identical diagnostic across every callsite.
    #[inline]
    pub const fn try_from_wire_i16(code: i16) -> Result<Self, i16> {
        match code {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            other => Err(other),
        }
    }

    /// The wire `i16` representation. Centralises what would
    /// otherwise be `self as i16` (banned by the forbid bundle) in
    /// a match whose arms match the `try_from_wire_i16` literals
    /// exactly. A body-swap drift is caught by the round-trip
    /// const-assert below.
    #[inline]
    #[must_use]
    pub const fn as_wire_i16(self) -> i16 {
        match self {
            Self::Text => 0,
            Self::Binary => 1,
        }
    }
}

// Round-trip compile pin for FormatCode.
const _: () = {
    assert!(
        matches!(FormatCode::try_from_wire_i16(FormatCode::Text.as_wire_i16()), Ok(FormatCode::Text)),
        "FormatCode round-trip broken: Text",
    );
    assert!(
        matches!(FormatCode::try_from_wire_i16(FormatCode::Binary.as_wire_i16()), Ok(FormatCode::Binary)),
        "FormatCode round-trip broken: Binary",
    );
};

// ── RowDesc: exact-size heap-backed schema ────────────────────────
//
// Layout inside `Box<[u32]>`:
//   [n_columns, oid_0, ..., oid_{n-1}, fmt_word_0, ..., fmt_word_{k-1}]
// where k = (n + 31) / 32   (ceil division, 1 bit per column)
//
// Slice length = 1 + n + k.
// Single heap allocation. No fixed cap. No waste.

/// Per-column descriptor returned by [`RowDesc::get`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColumnDesc {
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Text or binary.
    pub format_code: FormatCode,
}

/// Result-set schema. Exact-size, no fixed cap, single heap allocation.
///
/// Parsed once from the server's `RowDescription` (`'T'`) frame.
/// Read-only thereafter. Accessed once per query via
/// `current_row_desc()` — NOT per-column during streaming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDesc {
    data: alloc::boxed::Box<[u32]>,
}

impl RowDesc {
    /// Construct from pre-parsed OIDs and format codes.
    pub(crate) fn from_parts(
        oids: &[u32],
        format_codes: &[FormatCode],
    ) -> Result<Self, crate::error::ProtocolError> {
        let n = oids.len();
        if n > MAX_ROW_COLUMNS {
            return Err(crate::error::ProtocolError::TooManyColumns {
                count: n,
                max: MAX_ROW_COLUMNS,
            });
        }
        let k = n.div_ceil(32);
        let total = 1usize.saturating_add(n).saturating_add(k);
        let mut v = alloc::vec![0u32; total];
        let Ok(n_u32) = u32::try_from(n) else {
            return Err(crate::error::ProtocolError::TooManyColumns {
                count: n,
                max: MAX_ROW_COLUMNS,
            });
        };
        if let Some(slot) = v.get_mut(0) {
            *slot = n_u32;
        }
        for (i, &oid) in oids.iter().enumerate() {
            if let Some(slot) = v.get_mut(1usize.saturating_add(i)) {
                *slot = oid;
            }
        }
        let bits_start = 1usize.saturating_add(n);
        for (i, &fc) in format_codes.iter().enumerate() {
            if matches!(fc, FormatCode::Binary) {
                let word_idx = i >> 5;
                let bit_idx = i & 31;
                if let Some(word) = v.get_mut(bits_start.saturating_add(word_idx)) {
                    *word |= 1u32 << bit_idx;
                }
            }
        }
        Ok(Self {
            data: v.into_boxed_slice(),
        })
    }

    /// Synthesise from a static OID list with all-text format codes.
    pub(crate) fn from_static_oids_text_format(
        oids: &[u32],
    ) -> Result<Self, crate::error::ProtocolError> {
        let n = oids.len();
        let k = n.div_ceil(32);
        let total = 1usize.saturating_add(n).saturating_add(k);
        let mut v = alloc::vec![0u32; total];
        let Ok(n_u32) = u32::try_from(n) else {
            return Err(crate::error::ProtocolError::TooManyColumns {
                count: n,
                max: MAX_ROW_COLUMNS,
            });
        };
        if let Some(slot) = v.get_mut(0) {
            *slot = n_u32;
        }
        for (i, &oid) in oids.iter().enumerate() {
            if let Some(slot) = v.get_mut(1usize.saturating_add(i)) {
                *slot = oid;
            }
        }
        Ok(Self {
            data: v.into_boxed_slice(),
        })
    }

    /// Empty descriptor (0 columns).
    pub fn empty() -> Self {
        Self {
            data: alloc::vec![0u32; 1].into_boxed_slice(),
        }
    }

    /// Number of populated columns.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.data
            .first()
            .map(|&v| {
                let Ok(u) = usize::try_from(v) else {
                    return 0;
                };
                u
            })
            .unwrap_or(0)
    }

    /// Number of populated columns as `u16`.
    #[inline]
    #[must_use]
    pub fn n_columns(&self) -> u16 {
        let n = self.len();
        let Ok(v) = u16::try_from(n) else {
            return 0;
        };
        v
    }

    /// Whether the descriptor carries any columns.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// PG type OID for column `idx`, or `None` if out of range.
    #[inline]
    #[must_use]
    pub fn type_oid(&self, idx: usize) -> Option<u32> {
        if idx >= self.len() {
            return None;
        }
        self.data.get(1usize.saturating_add(idx)).copied()
    }

    /// Format code for column `idx`, or `None` if out of range.
    #[inline]
    #[must_use]
    pub fn format_code(&self, idx: usize) -> Option<FormatCode> {
        let n = self.len();
        if idx >= n {
            return None;
        }
        let bits_start = 1usize.saturating_add(n);
        let word_idx = idx >> 5;
        let bit_idx = idx & 31;
        let word = self.data.get(bits_start.saturating_add(word_idx)).copied()?;
        if word & (1u32 << bit_idx) != 0 {
            Some(FormatCode::Binary)
        } else {
            Some(FormatCode::Text)
        }
    }

    /// Construct a `ColumnDesc` for column `idx`.
    #[inline]
    #[must_use]
    pub fn get(&self, idx: usize) -> Option<ColumnDesc> {
        Some(ColumnDesc {
            type_oid: self.type_oid(idx)?,
            format_code: self.format_code(idx)?,
        })
    }

    /// Iterate over populated columns.
    #[inline]
    #[must_use]
    pub fn columns_iter(&self) -> RowDescColumnsIter<'_> {
        RowDescColumnsIter {
            desc: self,
            idx: 0,
            len: self.len(),
        }
    }
}

/// Iterator yielded by [`RowDesc::columns_iter`].
#[derive(Debug, Clone)]
pub struct RowDescColumnsIter<'a> {
    desc: &'a RowDesc,
    idx: usize,
    len: usize,
}

impl Iterator for RowDescColumnsIter<'_> {
    type Item = ColumnDesc;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.len {
            return None;
        }
        let cd = self.desc.get(self.idx)?;
        self.idx = self.idx.saturating_add(1);
        Some(cd)
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.len.saturating_sub(self.idx);
        (n, Some(n))
    }
}
impl ExactSizeIterator for RowDescColumnsIter<'_> {}
impl core::iter::FusedIterator for RowDescColumnsIter<'_> {}


/// Lifetime-bound borrow of a [`RowDesc`].
#[derive(Debug, Clone, Copy)]
pub struct RowDescBorrow<'r> {
    inner: &'r RowDesc,
}

impl<'r> RowDescBorrow<'r> {
    #[inline]
    #[must_use]
    pub(crate) fn from_ref(inner: &'r RowDesc) -> Self {
        Self { inner }
    }

    /// Column count as u16.
    #[inline]
    #[must_use]
    pub fn n_columns(&self) -> u16 {
        self.inner.n_columns()
    }

    /// Column count as usize.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the descriptor is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// PG type OID for column `idx`.
    #[inline]
    #[must_use]
    pub fn type_oid(&self, idx: usize) -> Option<u32> {
        self.inner.type_oid(idx)
    }

    /// Format code for column `idx`.
    #[inline]
    #[must_use]
    pub fn format_code(&self, idx: usize) -> Option<FormatCode> {
        self.inner.format_code(idx)
    }

    /// Column descriptor for column `idx`.
    #[inline]
    #[must_use]
    pub fn get(&self, idx: usize) -> Option<ColumnDesc> {
        self.inner.get(idx)
    }

    /// Iterate over populated columns.
    #[inline]
    #[must_use]
    pub fn columns_iter(&self) -> RowDescColumnsIter<'_> {
        self.inner.columns_iter()
    }

    /// Clone the underlying `RowDesc` into an owned value.
    #[inline]
    #[must_use]
    pub fn to_owned(&self) -> RowDesc {
        self.inner.clone()
    }
}

impl PartialEq for RowDescBorrow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}
impl Eq for RowDescBorrow<'_> {}

impl fmt::Display for FormatCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => f.write_str("text"),
            Self::Binary => f.write_str("binary"),
        }
    }
}


/// Parse a `RowDescription` payload (body of the `'T'` frame, after
/// the 5-byte header) into a [`RowDesc`].
///
/// Wire layout (PG §55.7):
/// ```text
///   int16  column_count
///   for each column:
///     cstring  name           (NUL-terminated; not stored)
///     int32    table_oid      (dropped)
///     int16    attr_num       (dropped)
///     int32    type_oid       ← captured
///     int16    type_size      (dropped)
///     int32    type_mod       (dropped)
///     int16    format_code    ← captured (0 = Text, 1 = Binary)
/// ```
///
/// # Error classifications
///
/// - [`crate::ProtocolError::MalformedRowDescription`] — payload too
///   short, negative column count, missing name NUL, truncated
///   per-column metadata.
/// - [`crate::ProtocolError::TooManyColumns`] — column count exceeds
///   [`MAX_ROW_COLUMNS`] (result-set too wide for this crate's bounded
///   storage).
/// - [`crate::ProtocolError::UnexpectedFormatCode`] — wire value
///   not in `{0, 1}`.
#[cold]
pub(crate) fn parse_row_description(
    payload: &[u8],
) -> Result<RowDesc, crate::error::ProtocolError> {
    use crate::error::ProtocolError;
    let malformed = || ProtocolError::MalformedRowDescription {
        payload_len: payload.len(),
    };

    let (count_bytes, mut rest) = payload.split_first_chunk::<2>().ok_or_else(malformed)?;
    let n_columns_i16 = i16::from_be_bytes(*count_bytes);
    if n_columns_i16 < 0 {
        return Err(malformed());
    }
    let n_columns = u16::try_from(n_columns_i16).map_err(|_| malformed())?;
    let n_columns_usize = usize::from(n_columns);

    if n_columns_usize > MAX_ROW_COLUMNS {
        return Err(ProtocolError::TooManyColumns {
            count: n_columns_usize,
            max: MAX_ROW_COLUMNS,
        });
    }

    let mut oids = alloc::vec::Vec::with_capacity(n_columns_usize);
    let mut formats = alloc::vec::Vec::with_capacity(n_columns_usize);

    for _idx in 0..n_columns_usize {
        let nul_pos = rest.iter().position(|&b| b == 0).ok_or_else(malformed)?;
        let name_end = nul_pos.saturating_add(1);
        let after_name = rest.get(name_end..).ok_or_else(malformed)?;

        let (meta, next_cursor) = after_name
            .split_first_chunk::<18>()
            .ok_or_else(malformed)?;

        let &[
            _tbl0, _tbl1, _tbl2, _tbl3,
            _att0, _att1,
            toid0, toid1, toid2, toid3,
            _ts0, _ts1,
            _tm0, _tm1, _tm2, _tm3,
            fc0, fc1,
        ] = meta;
        let type_oid = u32::from_be_bytes([toid0, toid1, toid2, toid3]);
        let format_code_i16 = i16::from_be_bytes([fc0, fc1]);
        let format_code = FormatCode::try_from_wire_i16(format_code_i16)
            .map_err(|code| ProtocolError::UnexpectedFormatCode { code })?;

        oids.push(type_oid);
        formats.push(format_code);
        rest = next_cursor;
    }

    if !rest.is_empty() {
        return Err(malformed());
    }

    RowDesc::from_parts(&oids, &formats)
}

/// Extract column names from a `RowDescription` payload.
///
/// Same wire format as [`parse_row_description`] but only extracts
/// the NUL-terminated name strings, skipping OID/format metadata.
/// Returns one `String` per column in wire order.
pub fn parse_column_names(payload: &[u8]) -> alloc::vec::Vec<alloc::string::String> {
    let Some((count_bytes, mut rest)) = payload.split_first_chunk::<2>() else {
        return alloc::vec::Vec::new();
    };
    let n_i16 = i16::from_be_bytes(*count_bytes);
    let Ok(n) = usize::try_from(n_i16) else {
        return alloc::vec::Vec::new();
    };
    let mut names = alloc::vec::Vec::with_capacity(n);
    for _ in 0..n {
        let Some(nul_pos) = rest.iter().position(|&b| b == 0) else { break };
        let name_bytes = rest.get(..nul_pos).unwrap_or(&[]);
        names.push(alloc::string::String::from_utf8_lossy(name_bytes).into_owned());
        let skip = nul_pos.saturating_add(1).saturating_add(18);
        rest = rest.get(skip..).unwrap_or(&[]);
    }
    names
}

/// Parse a `ParameterDescription` payload (body of the `'t'`
/// frame, after the 5-byte header) into a
/// [`crate::action::ParamOids`].
///
/// Wire layout (PG §55.2.2):
/// ```text
///   int16  parameter_count
///   for each parameter:
///     int32  type_oid
/// ```
///
/// # Error classifications
///
/// - [`crate::error::ProtocolError::MalformedParameterDescription`] —
///   payload shorter than the 2-byte count header, negative count,
///   or body length does not match `count × 4`.
/// - [`crate::error::ProtocolError::TooManyParameters`] — count
///   exceeds [`crate::params::MAX_PARAMS_ARITY`] (16). A statement
///   with more placeholders can be Parsed by the server but cannot
///   be Bound against by this crate, so the describe result is
///   useless downstream — fail loudly at parse time.
///
/// Cold path — called once per statement-level Describe reply.
#[cold]
pub(crate) fn parse_parameter_description(
    payload: &[u8],
) -> Result<crate::action::ParamOids, crate::error::ProtocolError> {
    use crate::error::ProtocolError;
    let malformed = || ProtocolError::MalformedParameterDescription {
        payload_len: payload.len(),
    };

    // parameter_count: i16 BE at offset 0.
    let (count_bytes, rest) = payload.split_first_chunk::<2>().ok_or_else(malformed)?;
    let n_params_i16 = i16::from_be_bytes(*count_bytes);
    if n_params_i16 < 0 {
        return Err(malformed());
    }
    // `n_params_i16 >= 0`, so `u16::try_from` is infallible (widening
    // from non-negative i16). Keep Result chain for panic-ban
    // discipline.
    let n_params = u16::try_from(n_params_i16).map_err(|_| malformed())?;
    let n_params_usize = usize::from(n_params);

    // Body length must exactly equal `count × 4` (one i32 per OID).
    // Trailing bytes imply wire corruption; short body implies the
    // declared count lies. Both classify as framing error.
    let expected_body_len = n_params_usize.checked_mul(4).ok_or_else(malformed)?;
    if rest.len() != expected_body_len {
        return Err(malformed());
    }

    // `split_first_chunk::<4>()` returns a typed
    // `Option<(&[u8; 4], &[u8])>` — no dead `_ =>` fallback arm
    // needed. The `Option::None` path is architecturally dead (the
    // body-length check above proves remaining bytes suffice) yet
    // surfaces as `Err(malformed())` rather than `unreachable!()`
    // (forbid-bundle).
    let mut oids = alloc::vec::Vec::with_capacity(n_params_usize);
    let mut cursor = rest;
    for _i in 0..n_params_usize {
        let (chunk, tail) = cursor.split_first_chunk::<4>().ok_or_else(malformed)?;
        oids.push(u32::from_be_bytes(*chunk));
        cursor = tail;
    }

    Ok(crate::action::ParamOids::from_slice(&oids))
}

// ════════════════════════════════════════════════════════════════════
// COPY response header (, PG §55.2.6)
// ════════════════════════════════════════════════════════════════════

/// Typed COPY transfer-format enum. Wire byte: 0 = text, 1 = binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CopyFormat {
    /// Text-mode transfer — newline-separated rows, columns joined
    /// by configurable delimiter (default tab).
    Text = 0,
    /// Binary-mode transfer — PG binary tuple format with a 19-byte
    /// header signature, per-tuple field count + length-prefixed
    /// values, and an end-of-stream marker.
    Binary = 1,
}

/// COPY response header (PG §55.2.6) — shared shape for both
/// `CopyOutResponse` ('H') and `CopyInResponse` ('G') frames.
///
/// Wire body shape: `format: int8` (0 = text, 1 = binary) +
/// `n_cols: int16` + per-column `format_code: int16[]` array. Per-PG
/// spec the per-column codes MUST all equal the overall format byte
/// — wire-validated by `parse_copy_response_header`. Stored as
/// `(format, n_cols)` only; per-column codes are redundant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyHeader {
    /// Overall transfer format. All per-column codes in the wire
    /// frame MUST equal this byte per PG spec.
    pub format: CopyFormat,
    /// Number of columns in the transfer. Bounded by
    /// [`MAX_ROW_COLUMNS`] = 32.
    pub n_cols: u16,
}

/// Parse a `CopyOutResponse` or `CopyInResponse` body (PG §55.2.6).
///
/// Body shape: `format: int8` (0/1) + `n_cols: int16 BE` +
/// per-column `format_code: int16 BE × n_cols`.
///
/// Returns `Err(ProtocolError::MalformedCopyResponse)` when:
/// body shorter than 3 bytes, format byte not 0/1, n_cols negative,
/// body length inconsistent with declared n_cols, or per-column
/// format code disagrees with overall format byte.
///
/// Returns `Err(ProtocolError::TooManyColumns)` if
/// `n_cols > MAX_ROW_COLUMNS`.
#[cold]
pub(crate) fn parse_copy_response_header(
    payload: &[u8],
) -> Result<CopyHeader, crate::error::ProtocolError> {
    use crate::error::ProtocolError;
    let malformed = || ProtocolError::MalformedCopyResponse {
        payload_len: payload.len(),
    };

    let (format_byte, rest) = payload.split_first().ok_or_else(malformed)?;
    let format = match *format_byte {
        0 => CopyFormat::Text,
        1 => CopyFormat::Binary,
        _ => return Err(malformed()),
    };
    let format_as_i16 = i16::from(*format_byte);

    let (count_bytes, rest) = rest.split_first_chunk::<2>().ok_or_else(malformed)?;
    let n_cols_i16 = i16::from_be_bytes(*count_bytes);
    if n_cols_i16 < 0 {
        return Err(malformed());
    }
    let n_cols = match u16::try_from(n_cols_i16) {
        Ok(v) => v,
        Err(_) => return Err(malformed()),
    };
    let n_cols_usize = usize::from(n_cols);

    if n_cols_usize > MAX_ROW_COLUMNS {
        return Err(ProtocolError::TooManyColumns {
            count: n_cols_usize,
            max: MAX_ROW_COLUMNS,
        });
    }

    let expected_body_len = n_cols_usize.checked_mul(2).ok_or_else(malformed)?;
    if rest.len() != expected_body_len {
        return Err(malformed());
    }

    let mut cursor = rest;
    for _ in 0..n_cols_usize {
        let (code_bytes, tail) = cursor.split_first_chunk::<2>().ok_or_else(malformed)?;
        let code = i16::from_be_bytes(*code_bytes);
        if code != format_as_i16 {
            return Err(malformed());
        }
        cursor = tail;
    }

    Ok(CopyHeader { format, n_cols })
}

#[cfg(test)]
mod copy_header_tests {
    use super::*;

    fn build_copy_response_body(format: u8, n_cols: u16) -> std::vec::Vec<u8> {
        let mut body = std::vec::Vec::new();
        body.push(format);
        body.extend_from_slice(&(i16::try_from(n_cols).unwrap_or(0)).to_be_bytes());
        let code_as_i16 = i16::from(format);
        for _ in 0..n_cols {
            body.extend_from_slice(&code_as_i16.to_be_bytes());
        }
        body
    }

    #[test]
    fn parse_text_format_zero_cols() {
        let body = build_copy_response_body(0, 0);
        let res = parse_copy_response_header(&body);
        assert!(matches!(
            res,
            Ok(CopyHeader { format: CopyFormat::Text, n_cols: 0 })
        ));
    }

    #[test]
    fn parse_binary_format_three_cols() {
        let body = build_copy_response_body(1, 3);
        let res = parse_copy_response_header(&body);
        assert!(matches!(
            res,
            Ok(CopyHeader { format: CopyFormat::Binary, n_cols: 3 })
        ));
    }

    #[test]
    fn rejects_format_byte_two() {
        let body = build_copy_response_body(2, 0);
        let res = parse_copy_response_header(&body);
        assert!(matches!(
            res,
            Err(crate::error::ProtocolError::MalformedCopyResponse { .. })
        ));
    }

    #[test]
    fn rejects_per_col_format_mismatch() {
        // overall format = 0 (text), but per-col format = 1 (binary) — spec violation
        let mut body = std::vec::Vec::new();
        body.push(0); // overall text
        body.extend_from_slice(&1_i16.to_be_bytes()); // n_cols = 1
        body.extend_from_slice(&1_i16.to_be_bytes()); // per-col = binary (mismatch!)
        let res = parse_copy_response_header(&body);
        assert!(matches!(
            res,
            Err(crate::error::ProtocolError::MalformedCopyResponse { .. })
        ));
    }

    #[test]
    fn rejects_too_many_columns() {
        // n_cols = MAX + 1; body length doesn't matter (the cap check
        // fires first).
        let n = u16::try_from(MAX_ROW_COLUMNS.saturating_add(1)).unwrap_or(33);
        let body = build_copy_response_body(0, n);
        let res = parse_copy_response_header(&body);
        assert!(matches!(
            res,
            Err(crate::error::ProtocolError::TooManyColumns { .. })
        ));
    }
}

// ════════════════════════════════════════════════════════════════════
// DataRow parser + ColumnsIter
// ════════════════════════════════════════════════════════════════════

/// Decode-time errors — classify malformed row bodies independently
/// of wire-level [`crate::ProtocolError`].
///
/// A [`DecodeError`] means the caller tried to parse an individual row
/// or column and the bytes don't match the PG DataRow shape. These
/// are per-row diagnostic errors: the protocol state machine already
/// accepted the frame as well-formed at the framing layer (the D
/// tag + length were intact); the body's internal structure is the
/// issue.
///
/// **Why separate from `ProtocolError`**: `ProtocolError` tears down
/// the connection. `DecodeError` surfaces to the row consumer who
/// can choose to skip the row, fail the application query, or
/// classify as a driver bug (the server sent a malformed row body)
/// — the connection itself is still healthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// DataRow body too short to contain the 2-byte column count
    /// header. Malformed frame.
    TruncatedRow,
    /// DataRow's 2-byte column count header parses as a negative
    /// signed value (PG §55.7 requires a non-negative i16). Wire
    /// protocol violation — servers never send this under spec
    /// compliance; arrival implies a bug / corruption / adversarial
    /// frame.
    ///
    /// Split from [`Self::TruncatedRow`]: the latter means "body too
    /// short"; this means "column count is signed-invalid." Different
    /// classes, different operator diagnostics.
    InvalidColumnCount {
        /// The offending i16 count value (always negative; positive
        /// values are well-formed and don't reach this arm).
        count: i16,
    },
    /// A column's 4-byte length prefix is missing (fewer bytes
    /// remain than expected). `column_idx` is 0-based, bounded by
    /// [`MAX_ROW_COLUMNS`] = 32 — fits `u8` with headroom.
    TruncatedColumnLen {
        /// Zero-based column index where the truncation was detected.
        column_idx: u8,
    },
    /// A column's declared length prefix is negative and is not the
    /// sentinel `-1` (which encodes SQL `NULL`). Other negative
    /// values are wire-level invalid.
    NegativeColumnLength {
        /// Zero-based column index.
        column_idx: u8,
        /// The offending length value.
        length: i32,
    },
    /// A column's data region is shorter than the declared length
    /// prefix (partial row).
    TruncatedColumnData {
        /// Zero-based column index.
        column_idx: u8,
        /// Length declared by the prefix.
        declared_len: usize,
        /// Bytes actually remaining in the row body.
        remaining: usize,
    },
    /// Column bytes are not valid UTF-8. Applies to text-format
    /// columns (including `&str` and all integer decoders, which
    /// read ASCII digits).
    NonUtf8,
    /// Failed to parse a numeric text-format column into the target
    /// Rust integer type — bad digit, sign out of range, or
    /// overflow.
    IntParse,
    /// Failed to parse a boolean — PG text format emits `"t"` / `"f"`;
    /// anything else classifies here.
    BoolParse,
    /// A binary-format fixed-size column's byte length doesn't match
    /// the decoder's expectation (e.g. an `i32` decoder receiving 3
    /// bytes, or 5). Binary-path classification — separate from
    /// [`Self::TruncatedColumnData`] which reports row-scoped
    /// truncation with a column index. Binary decoders run per-column
    /// through [`FromPgBinary`] and don't know the column index at
    /// their call site; this variant is honest about that.
    BinaryLengthMismatch {
        /// Bytes the decoder expected (fixed-size for ints / bool).
        expected_len: u8,
        /// Bytes actually received.
        actual_len: u16,
    },
    /// Server emitted SQL NULL (len = -1) for a column the
    /// `prepared!` row tuple typed as non-Option. The macro infers
    /// non-NULL semantics from the Rust type (`i32` vs
    /// `Option<i32>`); if the schema admits NULL, the user types
    /// `Option<T>` in the row tuple. Wide-typed nullable support
    /// (`Option<T>` row impls) is a planned follow-up.
    NullInNonNullColumn,
}

// Additive `core::error::Error` impl; matches the crate-wide
// policy of implementing the canonical `core::error::Error` on
// every public error type so downstream `bsql-driver-postgres`
// can `?`-propagate through `Box<dyn Error>` boundaries.
impl core::error::Error for DecodeError {}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedRow => f.write_str("DataRow body too short for column count header"),
            Self::InvalidColumnCount { count } => write!(
                f,
                "DataRow column count header is negative ({count}); PG §55.7 requires a non-negative i16",
            ),
            Self::TruncatedColumnLen { column_idx } => {
                write!(f, "column {column_idx}: length prefix truncated")
            }
            Self::NegativeColumnLength { column_idx, length } => write!(
                f,
                "column {column_idx}: invalid negative length {length} (only -1 = SQL NULL is valid)",
            ),
            Self::TruncatedColumnData {
                column_idx,
                declared_len,
                remaining,
            } => write!(
                f,
                "column {column_idx}: data truncated — declared {declared_len} bytes, only {remaining} remain",
            ),
            Self::NonUtf8 => f.write_str("column bytes are not valid UTF-8"),
            Self::IntParse => f.write_str("column text is not a valid integer for the target type"),
            Self::BoolParse => f.write_str("column text is not a PG boolean (expected \"t\" or \"f\")"),
            Self::BinaryLengthMismatch { expected_len, actual_len } => write!(
                f,
                "binary column byte length mismatch: expected {expected_len}, got {actual_len}",
            ),
            Self::NullInNonNullColumn => f.write_str(
                "server emitted SQL NULL for a column the prepared! row tuple typed as non-Option \
                 — use Option<T> in the row tuple if the schema admits NULL",
            ),
        }
    }
}

/// Zero-copy reference to a `DataRow` frame body.
///
/// Wraps the body bytes (everything after the 5-byte frame header)
/// and parses the 2-byte column count header eagerly. Per-column
/// data is lazily iterated via [`DataRowRef::columns`].
///
/// # Lifetimes
///
/// `'a` borrows the body bytes. Typically obtained from
/// the row-streaming `ColEvent` pull API, in which case `'a` is
/// the `'r` lifetime of the owning [`crate::OutActions`]. The
/// iterator yields column slices that share this borrow — no
/// copying, no allocation.
#[derive(Debug, Clone, Copy)]
pub struct DataRowRef<'a> {
    /// Body bytes AFTER the 2-byte column-count header.
    ///
    /// Stores the post-header slice directly (stripped at `parse`
    /// time via `split_first_chunk::<2>()`). The column iterator
    /// starts from the stored slice — tier-1 infallible, no
    /// Option, no fallback. A naive shape that stored the full
    /// body and re-stripped the header via
    /// `self.body.get(2..).unwrap_or(&[])` would form the banned
    /// silent-fallback pattern.
    body_after_count: &'a [u8],
    /// Parsed column count.
    n_columns: u16,
}

impl<'a> DataRowRef<'a> {
    /// Parse a `DataRow` frame body. Returns the declared column count
    /// without walking the column payloads — that happens in
    /// [`Self::columns`].
    ///
    /// # Errors
    ///
    /// - [`DecodeError::TruncatedRow`] — body is shorter than 2 bytes,
    ///   or the count header decodes to a negative `i16` (invalid).
    #[inline]
    pub fn parse(body: &'a [u8]) -> Result<Self, DecodeError> {
        let (count_bytes, body_after_count) =
            body.split_first_chunk::<2>().ok_or(DecodeError::TruncatedRow)?;
        let n_columns_i16 = i16::from_be_bytes(*count_bytes);
        if n_columns_i16 < 0 {
            // Distinguish "body too short" (TruncatedRow) from
            // "count header signed-invalid" (InvalidColumnCount).
            // Different classes; different operator diagnostics.
            return Err(DecodeError::InvalidColumnCount { count: n_columns_i16 });
        }
        // `n_columns_i16 >= 0` (proved above) ⟹ `try_from`
        // infallible. The Err arm is architecturally dead, but
        // classified as `TruncatedRow` rather than silently
        // fabricating a 0-column row — if a future refactor of the
        // negative-check above introduces a seam, the dead arm
        // becomes honest diagnostic output instead of "empty row
        // with no error". Tier-2 structural: misfire classifies,
        // does not mask.
        let n_columns = u16::try_from(n_columns_i16).map_err(|_| DecodeError::TruncatedRow)?;
        Ok(Self { body_after_count, n_columns })
    }

    /// Declared column count.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        usize::from(self.n_columns)
    }

    /// Whether the row carries zero columns (unusual — typically DML
    /// responses have no DataRow; a 0-column DataRow is exotic).
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.n_columns == 0
    }

    /// Iterator over columns in declaration order.
    ///
    /// Each item is `Result<Option<&'a [u8]>, DecodeError>`:
    /// - `Ok(Some(bytes))` — non-NULL column; `bytes` is the raw
    ///   payload (length-prefix stripped).
    /// - `Ok(None)` — SQL `NULL` (wire-level length prefix = `-1`).
    /// - `Err(DecodeError)` — malformed row body; iteration should
    ///   stop.
    ///
    /// Body bytes are advanced by `4 + data_len` per column; the
    /// iterator stops after `n_columns` items or on the first error.
    #[inline]
    #[must_use]
    pub fn columns(&self) -> ColumnsIter<'a> {
        // Tier-1 — `body_after_count` is the post-header slice
        // stored at parse time. No runtime
        // `.get(2..).unwrap_or(&[])` fallback.
        ColumnsIter {
            remaining: self.body_after_count,
            columns_left: self.n_columns,
            column_idx: 0u8,
        }
    }
}

/// Lazy iterator over a [`DataRowRef`]'s columns.
///
/// Produced by [`DataRowRef::columns`]. Each call to [`Iterator::next`]
/// reads one `(length, data)` pair from the remaining body bytes.
///
/// # Iterator semantics
///
/// - Yields exactly `n_columns` items on a well-formed row (then
///   returns `None`).
/// - On the first [`DecodeError`], that error is yielded; subsequent
///   `.next()` calls yield `None` (fused after error via the
///   `columns_left` counter saturating-decrement — further iteration
///   stops cleanly).
#[derive(Debug, Clone)]
pub struct ColumnsIter<'a> {
    remaining: &'a [u8],
    columns_left: u16,
    /// Zero-based column index, bounded by [`MAX_ROW_COLUMNS`] = 32 —
    /// `u8` with headroom. Propagated into `DecodeError::TruncatedColumn*`.
    column_idx: u8,
}

impl<'a> ColumnsIter<'a> {
    /// Centralised fuse-and-error helper.
    ///
    /// A naive shape inlining `self.remaining = &[];
    /// self.columns_left = 0; return Some(Err(...))` at every
    /// error site (4+ in `next`) is drift-prone: a future refactor
    /// adding a new error arm and forgetting the fuse would let
    /// iteration continue past the error. This helper makes the
    /// fuse+error path a single expression and makes every new
    /// error arm structurally-fused by default.
    #[inline]
    fn fuse_and_error(&mut self, e: DecodeError) -> Option<Result<Option<&'a [u8]>, DecodeError>> {
        self.remaining = &[];
        self.columns_left = 0;
        Some(Err(e))
    }
}

impl<'a> Iterator for ColumnsIter<'a> {
    type Item = Result<Option<&'a [u8]>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.columns_left == 0 {
            return None;
        }
        let idx = self.column_idx;
        self.column_idx = idx.saturating_add(1);
        self.columns_left = self.columns_left.saturating_sub(1);

        // 4-byte length prefix.
        let (len_bytes, after_len) = match self.remaining.split_first_chunk::<4>() {
            Some(pair) => pair,
            None => return self.fuse_and_error(DecodeError::TruncatedColumnLen { column_idx: idx }),
        };
        let len = i32::from_be_bytes(*len_bytes);

        // Collapsed sign-path cascade. A naive shape would chain
        // three sequential sign checks:
        //   if len == -1 { NULL }
        //   if len < 0 { NegativeColumnLength }
        //   usize::try_from(len) { ... Err → NegativeColumnLength }
        // Three comparisons per column × 32 max cols × 1M rows =
        // ~96M redundant compares on row-heavy workloads.
        //
        // The collapsed form: single NULL shortcut + fold the
        // `< -1` case into the `usize::try_from` Err branch (which
        // also catches hypothetical i32→usize overflow on 16-bit
        // targets, even though MSRV implicitly disallows those).
        // Two compares: `len == -1` (null) and `usize::try_from`
        // (non-negative). LLVM fuses the try_from sign check with
        // the comparison.
        if len == -1 {
            // SQL NULL — no data bytes to consume.
            self.remaining = after_len;
            return Some(Ok(None));
        }
        let Ok(len_usize) = usize::try_from(len) else {
            // `len < -1` (wire violation) OR i32-that-doesn't-fit-
            // usize (architecturally impossible on 32+-bit MSRV
            // targets since i32 range ⊂ usize range). The audit's
            // proposed `wrapping_add(1) as u32` trick is blocked
            // by crate-wide `as_conversions` forbid — try_from is
            // the accepted substitute with LLVM fusing the
            // non-negative fast path.
            return self.fuse_and_error(DecodeError::NegativeColumnLength {
                column_idx: idx,
                length: len,
            });
        };

        match after_len.split_at_checked(len_usize) {
            Some((data, next)) => {
                self.remaining = next;
                Some(Ok(Some(data)))
            }
            None => {
                let remaining = after_len.len();
                self.fuse_and_error(DecodeError::TruncatedColumnData {
                    column_idx: idx,
                    declared_len: len_usize,
                    remaining,
                })
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = usize::from(self.columns_left);
        (n, Some(n))
    }
}

impl ExactSizeIterator for ColumnsIter<'_> {}
impl core::iter::FusedIterator for ColumnsIter<'_> {}

// ════════════════════════════════════════════════════════════════════
// Text-format decoders
// ════════════════════════════════════════════════════════════════════

/// PostgreSQL **text-format** column decoder for a Rust type.
///
/// PG's text format — the default for Simple Query — encodes all
/// values as ASCII-ish strings (`"42"`, `"t"`, `"hello"`). This
/// trait's implementations wrap `core::str::from_utf8` and
/// `FromStr`-style parses with type-specific error classification.
///
/// # Lifetime
///
/// `'a` ties the decoder's output to the input byte slice. For
/// `&str` the output borrows the input directly (zero-copy). For
/// owned types like `i32` / `bool`, `'a` is phantom.
///
/// # Usage
///
/// The example models the crate's own discipline — no `unwrap()`
/// / `panic!()` in the happy path.
/// `cols.next()` returns `Option<Result<Option<&[u8]>, DecodeError>>`
/// and is matched structurally via `let Some(...) else`. Real user
/// code can adapt to its own error strategy (`?` into custom errors,
/// slogged through the `query!` macro, etc.).
///
/// The doc-test below is COMPILE-CHECKED — a future refactor that
/// alters `DataRowRef::parse`, `ColumnsIter::next`, the `FromPgText`
/// trait shape, or `DecodeError` variants fails the build in CI.
/// The example operates directly on `row_bytes: &[u8]` — the raw
/// PostgreSQL DataRow body the protocol surfaces via its row-streaming
/// API (`RowStream::col_next`, etc.).
///
/// ```rust
/// use bsql_postgres_proto::{DataRowRef, DecodeError, FromPgText};
///
/// fn decode_id_and_name<'a>(row_bytes: &'a [u8])
///     -> Result<Option<(Option<i32>, Option<&'a str>)>, DecodeError>
/// {
///     let row = DataRowRef::parse(row_bytes)?;
///     let mut cols = row.columns();
///
///     // `Option::None` from `next()` = fewer columns than expected.
///     // `Option::None` from the inner `Ok(None)` = SQL NULL.
///     // Both surface via structural match, no `unwrap()`.
///     let Some(id_result) = cols.next() else { return Ok(None) };
///     let id: Option<i32> = id_result?.map(i32::from_pg_text).transpose()?;
///
///     let Some(name_result) = cols.next() else { return Ok(None) };
///     let name: Option<&'a str> = name_result?.map(<&'a str>::from_pg_text).transpose()?;
///
///     // Return the decoded pair (per-column NULL preserved via `Option`).
///     // The example never silently defaults — every absence is explicit
///     // in the return type.
///     Ok(Some((id, name)))
/// }
/// ```
///
/// # Error
///
/// [`DecodeError::NonUtf8`] for non-UTF-8 bytes on decoders that
/// genuinely require UTF-8 validation (`&str`, `Vec<u8>`).
/// Type-specific parse errors:
/// - integer types → [`DecodeError::IntParse`] (single-pass
///   ASCII-digit parser treats non-digit bytes uniformly;
///   non-ASCII/non-UTF-8 input classifies as `IntParse`, NOT
///   `NonUtf8`, because UTF-8 validation is skipped as redundant
///   for the strict-ASCII integer grammar).
/// - `bool` → [`DecodeError::BoolParse`]
///
/// # Binary format
///
/// For PG binary-format columns (selected via Bind in Extended
/// Query), the parallel [`FromPgBinary`] trait carries the binary
/// codec. Text vs binary dispatch at the caller level via
/// `ColumnDesc::format_code`.
//
// `FromPgText` is NOT sealed — downstream crates may implement it
// for their own types (e.g. `chrono::DateTime`, `uuid::Uuid`). The
// diagnostic still pays off: the bare bound failure routes a user
// who tries `let row: (MyType,) = ...;` (where `MyType` lacks the
// impl) to the standard extension contract.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be decoded from PG text-format bytes",
    label = "missing `impl<'a> FromPgText<'a> for {Self}`",
    note = "implement `FromPgText` for your type to use it as a decoded column value, or use one of the crate-provided primitive types (`i16`, `i32`, `i64`, `u32`, `bool`, `&'a str`) which already implement it"
)]
pub trait FromPgText<'a>: Sized {
    /// PG type OID this text decoder targets.
    ///
    /// Parallel to [`FromPgBinary::OID`] and [`EncodeBinary::OID`].
    /// Enables compile-time validation that a Rust type chosen by
    /// the user matches the PG catalog OID the server declared in
    /// `RowDescription` — independent of which format
    /// (text/binary) the column uses.
    const OID: u32;

    /// Decode the column's text-format bytes.
    fn from_pg_text(bytes: &'a [u8]) -> Result<Self, DecodeError>;
}

// Dedicated ASCII-digit integer parser.
//
// PG text-format integers are strictly `[-+]?[0-9]+` per PG §55.7
// — always ASCII. A naive `core::str::from_utf8(bytes)?.parse::<T>()`
// chain walks the bytes twice: `from_utf8` SSE2-scans for non-UTF8,
// then `str::parse` re-scans, validates digits, accumulates. UTF-8
// validation is redundant for strict-ASCII integer grammar (a
// non-digit byte is already an `IntParse` error; a non-ASCII byte
// is non-digit). The dedicated parser walks once with one
// classification path — ~2× on int-heavy text SELECT workloads.
//
// Accumulates into the correct-sign arm to avoid `i*::MIN`
// overflow (if it accumulated as positive then negated, `-32768`
// on i16 would trip). Each step uses `checked_mul` /
// `checked_add` / `checked_sub` per `clippy::arithmetic_side_effects`
// forbid.
//
// For i16/i32 the digit loop uses a **wider accumulator**
// (`parse_pg_int_signed_widened!`) so the per-digit
// `checked_mul + checked_add/sub` chain (2 overflow branches per
// iteration) collapses to `wrapping_mul(10) + wrapping_add(d)`
// (no per-digit overflow check). The pre-loop length bound + a
// single end-of-loop `try_from` validate the entire range. `i64`
// stays on the original checked-arithmetic macro because the
// next-wider native type (i128) compiles to multi-instruction
// sequences on 64-bit targets, losing the win.

/// Parse a signed ASCII-digit integer with overflow checked at
/// every digit. Used by `i64` (where the next-wider type would be
/// i128 — non-native on 64-bit, slower than the checked path).
macro_rules! parse_pg_int_signed {
    ($bytes:expr, $t:ty) => {{
        let (is_neg, digits) = match $bytes.split_first() {
            Some((&b'-', rest)) => (true, rest),
            Some((&b'+', rest)) => (false, rest),
            Some(_) => (false, $bytes),
            None => return Err(DecodeError::IntParse),
        };
        if digits.is_empty() {
            return Err(DecodeError::IntParse);
        }
        let mut acc: $t = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(DecodeError::IntParse);
            }
            // `b - b'0'` is 0..=9, always fits u8 → $t via From.
            let d = <$t>::from(b.saturating_sub(b'0'));
            acc = acc.checked_mul(10).ok_or(DecodeError::IntParse)?;
            if is_neg {
                acc = acc.checked_sub(d).ok_or(DecodeError::IntParse)?;
            } else {
                acc = acc.checked_add(d).ok_or(DecodeError::IntParse)?;
            }
        }
        Ok(acc)
    }};
}

/// Parse a signed ASCII-digit integer using a **wider** accumulator
/// type than the result. Branch-budget reduction for the i16/i32
/// hot loop on text-format integer
/// columns (the dominant cost on int-heavy SELECT analytics).
///
/// # How it removes branches
///
/// The classic `checked_mul + checked_add/sub` form has 2
/// overflow-detection branches per digit. With a wider
/// accumulator and a digit-count pre-check, **the wrapping
/// arithmetic cannot actually wrap during the loop** — the
/// pre-check bounds the maximum reachable value safely below
/// `$acc::MAX`. One end-of-loop `<$result>::try_from(signed_acc)`
/// validates against the result-type's range.
///
/// Per-digit branches: **1** (digit validation) — was 3 (digit +
/// 2× overflow).
///
/// # Constraints
///
/// - `$acc` MUST be wider than `$result` (e.g. `i32` for `i16`,
///   `i64` for `i32`). Signed.
/// - `$max_digits` MUST satisfy `9 * 10^$max_digits + 9 < $acc::MAX`
///   so `wrapping_mul(10).wrapping_add(9)` cannot wrap during
///   the loop. For:
///   - i16 result + i32 acc + 5 digits: max acc reach = 99_999;
///     i32::MAX = 2_147_483_647. ✓
///   - i32 result + i64 acc + 10 digits: max acc reach =
///     9_999_999_999; i64::MAX ≈ 9.22 × 10^18. ✓
///
/// # Sign handling
///
/// Accumulate as positive, apply `wrapping_neg` at end if
/// `is_neg`. `wrapping_neg` on the in-range values we care about
/// (≤ 10^10 for i32) is just regular negation; the wider
/// accumulator gives headroom that avoids the original
/// "accumulate-into-correct-sign" complication of the checked
/// form (where `-i16::MIN = 32768` would overflow the result type
/// before negation). Final `try_from` validates `signed_acc ∈
/// $result::MIN..=$result::MAX`.
macro_rules! parse_pg_int_signed_widened {
    ($bytes:expr, $result:ty, $acc:ty, $max_digits:expr) => {{
        // Sign strip — identical to the checked-arithmetic form.
        let (is_neg, digits) = match $bytes.split_first() {
            Some((&b'-', rest)) => (true, rest),
            Some((&b'+', rest)) => (false, rest),
            Some(_) => (false, $bytes),
            None => return Err(DecodeError::IntParse),
        };
        // Length pre-check — bounds the max accumulator reach so
        // `wrapping_mul(10).wrapping_add(9)` cannot actually wrap
        // during the loop. Empty digit run is also caught here.
        if digits.is_empty() || digits.len() > $max_digits {
            return Err(DecodeError::IntParse);
        }
        // Hot loop — single per-digit branch (digit valid?), no
        // overflow checks. Wrapping ops are always-defined; the
        // length bound above ensures the value stays below
        // `$acc::MAX` so wrapping never actually wraps for valid
        // input.
        let mut acc: $acc = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(DecodeError::IntParse);
            }
            // `b.saturating_sub(b'0')` ∈ 0..=9 on the valid path
            // (validated in the line above); identical semantics
            // to `b - b'0'` here, but lint-safe under
            // `clippy::arithmetic_side_effects` forbid.
            let d = <$acc>::from(b.saturating_sub(b'0'));
            acc = acc.wrapping_mul(10).wrapping_add(d);
        }
        // Sign at end. `wrapping_neg` is correct for all
        // in-range values; the impossible `acc == $acc::MIN`
        // edge would cycle back to itself but is unreachable
        // given the length pre-check.
        let signed: $acc = if is_neg { acc.wrapping_neg() } else { acc };
        // Final range check — validates `signed` fits in
        // `$result::MIN..=$result::MAX`. This is the SOLE overflow
        // check on the entire path.
        <$result>::try_from(signed).map_err(|_| DecodeError::IntParse)
    }};
}

/// Parse an unsigned ASCII-digit integer. Used for u32 (PG OID).
/// Rejects leading `-`; `+` prefix accepted as a no-op.
macro_rules! parse_pg_int_unsigned {
    ($bytes:expr, $t:ty) => {{
        let digits = match $bytes.split_first() {
            Some((&b'-', _)) => return Err(DecodeError::IntParse),
            Some((&b'+', rest)) => rest,
            Some(_) => $bytes,
            None => return Err(DecodeError::IntParse),
        };
        if digits.is_empty() {
            return Err(DecodeError::IntParse);
        }
        let mut acc: $t = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(DecodeError::IntParse);
            }
            let d = <$t>::from(b.saturating_sub(b'0'));
            acc = acc.checked_mul(10).ok_or(DecodeError::IntParse)?;
            acc = acc.checked_add(d).ok_or(DecodeError::IntParse)?;
        }
        Ok(acc)
    }};
}

/// SWAR (SIMD-Within-A-Register) fast-path for ASCII-decimal short
/// unsigned integers (0..=9999).
///
/// Pure scalar bit-trick over 4 packed ASCII bytes — no `unsafe`,
/// no platform intrinsics, no SIMD instructions. On valid 1-4
/// ASCII-digit input, ~3× faster than the generic
/// `parse_pg_int_signed_widened!` macro path. Caller invokes
/// EXPLICITLY when SQL type knowledge says the column is short.
///
/// # When to use
///
/// Use this when the caller knows the column value is ASCII-decimal,
/// at most 4 digits, **unsigned** (no leading sign). Typical shapes:
/// status flags (0..=9), port numbers (≤ 9999), small counts,
/// day-of-month (1..=31), HTTP-style status codes (200/404/500).
/// **Caller is responsible for** (a) applying any sign separately,
/// (b) validating against the target type's range via
/// [`i16::try_from`] (or similar) when narrowing.
///
/// For general integer decoding without short-int knowledge,
/// continue using `<T as FromPgText>::from_pg_text` — that path
/// preserves the common-value cache and the widened-accumulator
/// digit loop.
///
/// # Returns
///
/// - `Some(value)` for 1-4 ASCII-digit bytes (`b"0".."b"9999"`).
/// - `None` for: empty input, length > 4, leading `-` or `+`, any
///   non-digit byte at any position.
///
/// # Why this is opt-in (architectural rationale)
///
/// Two prior attempts embedded this fast-path INSIDE `<i32 as
/// FromPgText>::from_pg_text`:
///
/// - Attempt 1 (`#[inline(always)]`): 4-digit −38% but 8-digit
///   +5.2%, text +4-7% — icache pressure from the 252 B → 776 B
///   function-bloat blast radius.
/// - Attempt 2 (purely additive prologue): 4-digit −37% but the
///   `iter_5cols_decode_i32_common_values` bench regressed +31%
///   (+3.3 ns/row on cache hit). LLVM's `SimplifyCFG` merged the
///   SWAR length-dispatch with the common-value `match`,
///   pessimising the cache-hit prologue.
///
/// Decoupling SWAR placement from `from_pg_text`'s body size
/// eliminates the LLVM heuristic shift entirely. `from_pg_text`
/// stays byte-identical; the helper is a separate symbol the
/// caller invokes when type knowledge justifies the fast-path.
///
/// # Tier impact
///
/// Runtime classification is tier-3 by `Option::None` for invalid
/// input. Closed by exhaustive proptest grid over all 0..=9999
/// valid values plus non-digit-byte sweeps at every position.
/// `#![forbid(unsafe_code)]` and the workspace forbid-bundle
/// (`clippy::arithmetic_side_effects`, `clippy::as_conversions`,
/// `clippy::indexing_slicing`) keep the implementation tier-1
/// safe by construction.
#[must_use]
pub fn parse_short_uint_swar(bytes: &[u8]) -> Option<u32> {
    // Pad input to `[u8; 4]` with leading b'0' (a valid ASCII digit
    // contributing 0 in the MSB positions). Reject length > 4 and
    // length 0 via the wildcard fall-through arm. Slice patterns
    // are tier-1: the borrow-checker proves length-correctness at
    // compile time, no runtime bounds check, no panic surface.
    let buf: [u8; 4] = match *bytes {
        [d0]               => [b'0', b'0', b'0', d0],
        [d0, d1]           => [b'0', b'0', d0, d1],
        [d0, d1, d2]       => [b'0', d0, d1, d2],
        [d0, d1, d2, d3]   => [d0, d1, d2, d3],
        _ => return None,
    };
    let packed: u32 = u32::from_be_bytes(buf);
    // Lemire branch-free validation: for valid `b ∈ b'0'..=b'9'`,
    // both `(b - 0x30)` and `(0x39 - b)` are in `[0, 9]` (no high
    // bit set). If any byte is outside that range, the high bit
    // (0x80) appears in `lo` or `hi`, and the masked OR is nonzero
    // — single AND + branch rejects every invalid shape.
    let lo = packed.wrapping_sub(0x3030_3030);
    let hi = 0x3939_3939_u32.wrapping_sub(packed);
    if (lo | hi) & 0x8080_8080 != 0 {
        return None;
    }
    // Each byte of `lo` is now a digit value 0..=9. Decompose via
    // `to_be_bytes()` (statically-bounded `[u8; 4]` access — no
    // indexing-slicing lint trip) and recombine with the place
    // values 1000/100/10/1. `wrapping_*` is bit-exact: max acc
    // reach = 9*1000 + 9*100 + 9*10 + 9 = 9999, far below
    // `u32::MAX` (≈ 4.29 × 10^9), so no actual wrap occurs on the
    // valid path. `wrapping_*` is mandated by the
    // `clippy::arithmetic_side_effects` forbid (operator `+` / `*`
    // would not compile).
    let lo_bytes = lo.to_be_bytes(); // [thousands, hundreds, tens, ones]
    let value = u32::from(lo_bytes[0])
        .wrapping_mul(1000)
        .wrapping_add(u32::from(lo_bytes[1]).wrapping_mul(100))
        .wrapping_add(u32::from(lo_bytes[2]).wrapping_mul(10))
        .wrapping_add(u32::from(lo_bytes[3]));
    Some(value)
}

// ═════════════════════════════════════════════════════════════════
// SWAR extension — three additional opt-in helpers extending the
// `parse_short_uint_swar` precedent above. All caller-routed;
// NEVER embedded in shared dispatch (avoids the LLVM heuristic
// shifts that doomed earlier prologue-embedding attempts).
//
// Tier: caller-routed fast-paths. `Option::None` on invalid input
// is runtime-classified (tier-3 inherent — arbitrary network bytes
// only exist at runtime). Closure mechanism: exhaustive test grids
// over the full validity domain for each helper, plus byte-
// position sweeps covering rejection arms.
// ═════════════════════════════════════════════════════════════════

/// SWAR-style parser for 5-19 digit ASCII-decimal unsigned integers.
///
/// Extends [`parse_short_uint_swar`] (1-4 digits, `u32` range) to the
/// `u64`-representable middle band. Lengths < 5 should use the short
/// variant; lengths > 19 cannot be represented in `u64`
/// (`u64::MAX == 18_446_744_073_709_551_615`, 20 digits).
///
/// # Algorithm
///
/// 1. Reject `bytes.len() < 5` or `bytes.len() > 19` via length test.
/// 2. Left-pad input into a fixed-size `[u8; 24]` buffer with leading
///    `b'0'`. A digit value `0` contributes `0` to any place value, so
///    padded prefix bytes are invariant under accumulation.
/// 3. Load three `u64` chunks (big-endian) from the padded buffer.
/// 4. Lemire branch-free validation per chunk (same SWAR mask trick
///    as [`parse_short_uint_swar`]): for valid digit `b ∈ b'0'..=b'9'`
///    both `(b - 0x30)` and `(0x39 - b)` lie in `[0, 9]`. Any byte
///    outside the digit range sets the high bit on one of those
///    subtractions; ORed across three chunks, the masked
///    `0x8080…80` band rejects every invalid shape in one branch.
/// 5. Recombine via wrapping `*10 + digit`. Max value at 19 nines
///    is `9_999_999_999_999_999_999 < u64::MAX`, so the accumulation
///    is bit-exact on every valid input.
///
/// # Caller contract
///
/// - The helper accepts only the unsigned form. Leading `-` or `+`
///   bytes fall below `b'0'`, the Lemire mask rejects, and `None`
///   is returned. Callers decoding signed integers strip the sign
///   byte first, then call this helper, then re-apply the sign.
/// - For lengths 1-4 use [`parse_short_uint_swar`] — calling this
///   helper on shorter input returns `None`.
/// - Values 10^18 < v < u64::MAX are representable (19-digit window
///   spans into u64 headroom above i64::MAX). Callers needing i64
///   must verify `v <= i64::MAX as u64` after success.
///
/// # Tier
///
/// Runtime classification on invalid input is irreducibly tier-3
/// (arbitrary byte input). Closure: exhaustive boundary-length grid,
/// per-byte-position non-digit sweep, and boundary digit pin in
/// `parse_long_uint_swar_tests`. The crate-root `forbid(unsafe_code)`
/// plus the workspace forbid bundle keep the SWAR math tier-1 safe
/// by construction.
#[must_use]
pub fn parse_long_uint_swar(bytes: &[u8]) -> Option<u64> {
    let len = bytes.len();
    if !(5..=19).contains(&len) {
        return None;
    }
    // 24-byte buffer fits three u64 chunks. Left-pad with b'0'.
    let mut buf = [b'0'; 24];
    let pad_offset = 24usize.saturating_sub(len);
    // Safe suffix copy via slice indexing on `[u8; 24]` — bounds are
    // proven by `pad_offset + len == 24`; copy_from_slice asserts
    // length equality internally.
    let dst = buf.get_mut(pad_offset..)?;
    if dst.len() != len {
        return None; // architecturally dead — pad arithmetic above
    }
    dst.copy_from_slice(bytes);

    // Three u64 big-endian chunks. Slice-to-array via try_into avoids
    // the indexing-slicing lint trip while remaining tier-1 safe.
    let chunk0: [u8; 8] = buf.get(0..8)?.try_into().ok()?;
    let chunk1: [u8; 8] = buf.get(8..16)?.try_into().ok()?;
    let chunk2: [u8; 8] = buf.get(16..24)?.try_into().ok()?;
    let c0 = u64::from_be_bytes(chunk0);
    let c1 = u64::from_be_bytes(chunk1);
    let c2 = u64::from_be_bytes(chunk2);

    // Lemire SWAR validation per chunk. Single masked OR rejects
    // every byte outside `b'0'..=b'9'` across all three chunks.
    const ZEROS: u64 = 0x3030_3030_3030_3030;
    const NINES: u64 = 0x3939_3939_3939_3939;
    const HIBIT: u64 = 0x8080_8080_8080_8080;
    let l0 = c0.wrapping_sub(ZEROS);
    let h0 = NINES.wrapping_sub(c0);
    let l1 = c1.wrapping_sub(ZEROS);
    let h1 = NINES.wrapping_sub(c1);
    let l2 = c2.wrapping_sub(ZEROS);
    let h2 = NINES.wrapping_sub(c2);
    if ((l0 | h0) | (l1 | h1) | (l2 | h2)) & HIBIT != 0 {
        return None;
    }

    // Each byte of l0/l1/l2 now holds the digit value 0..=9.
    let b0 = l0.to_be_bytes();
    let b1 = l1.to_be_bytes();
    let b2 = l2.to_be_bytes();

    // Length-aware recombination via parallel-multiply per place
    // value. Three branches dispatch on the digit count class:
    //
    // - 5..=8  digits → only positions 16..=23 carry data; positions
    //   0..=15 are padded zeros (verified by the Lemire mask above).
    // - 9..=16 digits → positions 8..=23 carry data.
    // - 17..=19 digits → positions 5..=23 carry data.
    //
    // Each branch issues 8/16/19 INDEPENDENT `wrapping_mul` ops with
    // compile-time-constant place values; LLVM schedules them in
    // parallel, then sums via a tree reduction. Pre-fix (single
    // sequential Horner accumulator across 24 bytes) was a
    // 24-instruction dependency chain and benched 3.5× slower than
    // generic scalar decode at the 8-digit shape (113 ns/row vs
    // 31 ns/row on `iter_5cols_decode_i32_long_8digit_via_swar`).
    // The parallel form recovers the SWAR-promise speedup.
    Some(match len {
        5..=8 => {
            // Active positions 16..=23 (in b2).
            u64::from(b2[0]).wrapping_mul(10_000_000)
                .wrapping_add(u64::from(b2[1]).wrapping_mul(1_000_000))
                .wrapping_add(u64::from(b2[2]).wrapping_mul(100_000))
                .wrapping_add(u64::from(b2[3]).wrapping_mul(10_000))
                .wrapping_add(u64::from(b2[4]).wrapping_mul(1_000))
                .wrapping_add(u64::from(b2[5]).wrapping_mul(100))
                .wrapping_add(u64::from(b2[6]).wrapping_mul(10))
                .wrapping_add(u64::from(b2[7]))
        }
        9..=16 => {
            // Active positions 8..=23 (b1 + b2). Place values 10^15
            // .. 10^0. All within u64 range (10^15 < u64::MAX).
            u64::from(b1[0]).wrapping_mul(1_000_000_000_000_000)
                .wrapping_add(u64::from(b1[1]).wrapping_mul(100_000_000_000_000))
                .wrapping_add(u64::from(b1[2]).wrapping_mul(10_000_000_000_000))
                .wrapping_add(u64::from(b1[3]).wrapping_mul(1_000_000_000_000))
                .wrapping_add(u64::from(b1[4]).wrapping_mul(100_000_000_000))
                .wrapping_add(u64::from(b1[5]).wrapping_mul(10_000_000_000))
                .wrapping_add(u64::from(b1[6]).wrapping_mul(1_000_000_000))
                .wrapping_add(u64::from(b1[7]).wrapping_mul(100_000_000))
                .wrapping_add(u64::from(b2[0]).wrapping_mul(10_000_000))
                .wrapping_add(u64::from(b2[1]).wrapping_mul(1_000_000))
                .wrapping_add(u64::from(b2[2]).wrapping_mul(100_000))
                .wrapping_add(u64::from(b2[3]).wrapping_mul(10_000))
                .wrapping_add(u64::from(b2[4]).wrapping_mul(1_000))
                .wrapping_add(u64::from(b2[5]).wrapping_mul(100))
                .wrapping_add(u64::from(b2[6]).wrapping_mul(10))
                .wrapping_add(u64::from(b2[7]))
        }
        17..=19 => {
            // Active positions 5..=23 (b0[5..=7] + b1 + b2). Place
            // values 10^18 .. 10^0. 10^18 < u64::MAX = 18.44 × 10^18.
            u64::from(b0[5]).wrapping_mul(1_000_000_000_000_000_000)
                .wrapping_add(u64::from(b0[6]).wrapping_mul(100_000_000_000_000_000))
                .wrapping_add(u64::from(b0[7]).wrapping_mul(10_000_000_000_000_000))
                .wrapping_add(u64::from(b1[0]).wrapping_mul(1_000_000_000_000_000))
                .wrapping_add(u64::from(b1[1]).wrapping_mul(100_000_000_000_000))
                .wrapping_add(u64::from(b1[2]).wrapping_mul(10_000_000_000_000))
                .wrapping_add(u64::from(b1[3]).wrapping_mul(1_000_000_000_000))
                .wrapping_add(u64::from(b1[4]).wrapping_mul(100_000_000_000))
                .wrapping_add(u64::from(b1[5]).wrapping_mul(10_000_000_000))
                .wrapping_add(u64::from(b1[6]).wrapping_mul(1_000_000_000))
                .wrapping_add(u64::from(b1[7]).wrapping_mul(100_000_000))
                .wrapping_add(u64::from(b2[0]).wrapping_mul(10_000_000))
                .wrapping_add(u64::from(b2[1]).wrapping_mul(1_000_000))
                .wrapping_add(u64::from(b2[2]).wrapping_mul(100_000))
                .wrapping_add(u64::from(b2[3]).wrapping_mul(10_000))
                .wrapping_add(u64::from(b2[4]).wrapping_mul(1_000))
                .wrapping_add(u64::from(b2[5]).wrapping_mul(100))
                .wrapping_add(u64::from(b2[6]).wrapping_mul(10))
                .wrapping_add(u64::from(b2[7]))
        }
        // Architecturally dead — len was bounded by the initial test.
        _ => return None,
    })
}

/// SWAR ASCII fast-path for UTF-8 validation.
///
/// Scans `bytes` in 8-byte chunks. Returns `Some(())` if every byte
/// is `< 0x80` (pure ASCII — automatically valid UTF-8 by the
/// ASCII⊂UTF-8 spec). Returns `None` if any byte has the high bit
/// set — the caller MUST then validate via a full UTF-8 checker
/// (`simdutf8::basic::from_utf8` or `core::str::from_utf8`) to
/// distinguish legitimate multi-byte UTF-8 from invalid bytes.
///
/// # When to use
///
/// `simdutf8::basic::from_utf8` has constant per-call setup overhead
/// (lane initialisation, dispatcher selection on portable builds).
/// On very short strings (≤ ~16 B, dominated by ASCII identifiers
/// like column names, short enum tags) that overhead dominates.
/// This helper checks the "all-ASCII" hypothesis in one masked OR
/// per 8 bytes; on hit, the caller skips the full validator.
///
/// # Algorithm
///
/// 1. Process `bytes` in disjoint 8-byte chunks (no overlap).
/// 2. For each chunk, load as `u64` (little-endian — byte order is
///    irrelevant for high-bit-OR; LE just matches host on aarch64).
/// 3. Test `packed & 0x8080_8080_8080_8080 != 0`. Set bit → at least
///    one non-ASCII byte → return `None`.
/// 4. Tail bytes (0-7) handled bytewise.
///
/// # Tier
///
/// Tier-1 safe by construction (slice patterns, no indexing-slicing,
/// no unsafe). Tier-3 false-negative classification (returns `None`
/// on legal multi-byte UTF-8 — caller MUST follow up with full
/// validator; documented contract). Closure: byte-position sweep
/// over boundary values `0x7F` (highest ASCII, accepted) and `0x80`
/// (first non-ASCII, rejected) at every position within and across
/// chunk boundaries.
#[must_use]
pub fn validate_utf8_swar(bytes: &[u8]) -> Option<()> {
    const HIBIT: u64 = 0x8080_8080_8080_8080;
    let mut tail: &[u8] = bytes;
    while tail.len() >= 8 {
        let (chunk, rest) = tail.split_at(8);
        let arr: [u8; 8] = chunk.try_into().ok()?;
        let packed = u64::from_le_bytes(arr);
        if packed & HIBIT != 0 {
            return None;
        }
        tail = rest;
    }
    for &b in tail {
        if b >= 0x80 {
            return None;
        }
    }
    Some(())
}

/// Cache-hit fast-path for PostgreSQL boolean text literals.
///
/// Recognises the four PG-wire-legal text forms:
/// - `b"t"` → `Some(true)` (canonical SELECT output)
/// - `b"f"` → `Some(false)` (canonical SELECT output)
/// - `b"true"` → `Some(true)` (extended input form)
/// - `b"false"` → `Some(false)` (extended input form)
///
/// Returns `None` for every other byte slice. Caller falls back to
/// their generic decoder (typically [`FromPgText`] for `bool`).
///
/// # Why a dedicated helper
///
/// The standard [`FromPgText`] for `bool` matches only `b"t"`/`b"f"`
/// (PG's SELECT output format). Callers that ALSO need to accept
/// the longer literal forms (e.g. when decoding COPY-from-stdin
/// text streams, where PG accepts both short and long bool forms)
/// would otherwise hand-roll the same `match` chain. This helper
/// codifies the canonical four-form set in one place so callers
/// can dispatch once.
///
/// # Tier
///
/// Tier-1 safe by construction: slice patterns over a closed set
/// of byte-string literals. LLVM lowers the four patterns to an
/// optimal jump table on `bytes.len()` followed by a constant-cmp
/// per arm. No SWAR math needed — the optimal code IS the simple
/// `match`; the "_swar" suffix follows the SWAR opt-in naming
/// convention for helpers outside the shared dispatch.
#[must_use]
pub fn parse_pg_bool_swar(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [b't'] | b"true" => Some(true),
        [b'f'] | b"false" => Some(false),
        _ => None,
    }
}

// Common-literal fast-paths for i16/i32/i64 text decoders.
//
// # Why these three literals
//
// PG real-world workloads on integer columns concentrate value mass
// on a tiny set of literals:
//
// - `b"0"` — boolean coercions, status flags, NULL coalesce defaults,
//   counter resets. Dominant on heavy OLTP tables.
// - `b"1"` — boolean true coercions, single-row INSERT-and-return-id,
//   first-row pagination. Common on user-row tables.
// - `b"-1"` — sentinel "not found" / "unset" values, JSON-style
//   negative-id markers. Less common than 0/1 but still hit.
//
// Each fast-path is a single byte-slice equality check (LLVM folds
// to `cmp` against a constant). On a hit we return the literal value
// without invoking the digit loop. On a miss we fall through to the
// general parser at one extra branch (well-predicted on workloads
// where ANY of the three is the common case — modern branch predictors
// handle the bimodal/trimodal pattern fine).
//
// Tier neutrality: the fast-path returns the exact same `Result<T, DecodeError>`
// shape as the macro path. A digit-loop bug that miscomputed `0` /
// `1` / `-1` would now be caught by the fast-path identity test
// rather than depending on the macro's correctness — TIER-2 mutually-
// reinforcing checks (the byte-equality codifies the value-mass-is-here
// observation; the macro covers the rest).
//
// # Why not extend further (e.g. 2..=9, 100, common years)
//
// Each additional literal adds a branch on the miss path. Three
// branches is the "sweet spot" — bimodal/trimodal predictors hit
// the right arm cheaply, more-than-three would push toward the
// general digit loop's per-byte branches anyway. Bench evidence
// (the `iter_5cols_decode_i32_common_values` bench introduced
// alongside this change) measures the trade-off: hit case −1 to
// −2 ns/col, miss case +0 to +1 ns/col on the 8-digit `42_000_000`
// shape (within criterion noise).

impl FromPgText<'_> for i16 {
    const OID: u32 = oids::INT2;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        // Fast-paths: literal-byte equality bypasses digit loop on
        // the three most-common values. Match-on-slice folds to a
        // 3-arm jump table at -O2 / LTO=fat (workspace setting).
        match bytes {
            b"0" => return Ok(0),
            b"1" => return Ok(1),
            b"-1" => return Ok(-1),
            _ => {}
        }
        // Widened-accumulator path. i32 accumulator + 5-digit cap
        // (i16::MAX = 32767 = 5 digits). Max acc reach with 5
        // digits = 99_999 << i32::MAX ≈ 2.15B, so
        // wrapping_mul(10).wrapping_add(9) cannot wrap during the
        // loop. Single end-cast `i16::try_from` validates
        // i16::MIN..=i16::MAX.
        parse_pg_int_signed_widened!(bytes, i16, i32, 5)
    }
}

impl FromPgText<'_> for i32 {
    const OID: u32 = oids::INT4;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        // Fast-paths — see the family comment above i16's impl for
        // rationale.
        match bytes {
            b"0" => return Ok(0),
            b"1" => return Ok(1),
            b"-1" => return Ok(-1),
            _ => {}
        }
        // Widened-accumulator path. i64 accumulator + 10-digit cap
        // (i32::MAX = 2_147_483_647 = 10 digits). Max acc reach
        // with 10 digits = 9_999_999_999 << i64::MAX ≈ 9.22 ×
        // 10^18, so wrapping_mul(10) + wrapping_add(9) cannot wrap
        // during the loop. Single end-cast `i32::try_from`
        // validates i32::MIN..=i32::MAX.
        parse_pg_int_signed_widened!(bytes, i32, i64, 10)
    }
}

impl FromPgText<'_> for i64 {
    const OID: u32 = oids::INT8;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        // Fast-paths — see the family comment above i16's impl for
        // rationale.
        match bytes {
            b"0" => return Ok(0),
            b"1" => return Ok(1),
            b"-1" => return Ok(-1),
            _ => {}
        }
        // `i64` stays on the original checked-arithmetic macro.
        // The wider native accumulator (i128) compiles to multi-
        // instruction sequences on 64-bit targets — losing the
        // speed gain that motivates the widened-acc form for
        // i16/i32. Capping at 18 digits (skipping `i64::MAX`)
        // would be incorrect — `9_223_372_036_854_775_807` is a
        // valid 19-digit i64.
        parse_pg_int_signed!(bytes, i64)
    }
}

impl FromPgText<'_> for u32 {
    const OID: u32 = oids::OID;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes {
            b"0" => return Ok(0),
            b"1" => return Ok(1),
            _ => {}
        }
        parse_pg_int_unsigned!(bytes, u32)
    }
}

/// PG boolean text format: `"t"` = true, `"f"` = false. Anything
/// else (including `"true"`, `"TRUE"`, `"1"`, `"0"`) classifies as
/// [`DecodeError::BoolParse`] — PG is strict about its own format.
impl FromPgText<'_> for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes {
            b"t" => Ok(true),
            b"f" => Ok(false),
            _ => Err(DecodeError::BoolParse),
        }
    }
}

/// Text column as `&str` — zero-copy, validates UTF-8 only.
impl<'a> FromPgText<'a> for &'a str {
    const OID: u32 = oids::TEXT;
    /// SIMD-accelerated UTF-8 validation via `simdutf8`.
    ///
    /// `core::str::from_utf8` is scalar bytewise (with an ASCII
    /// fast-path that aborts on the first non-ASCII byte; cheap on
    /// short ASCII, expensive on multi-byte UTF-8).
    /// `simdutf8::basic::from_utf8` uses lane-wise vector shuffles
    /// + masks via NEON on aarch64.
    ///
    /// Bench evidence (aarch64-apple-darwin, 5-column rows):
    /// * **Long ASCII** (~200 B, descriptive text): −49.9% (~2×
    ///   faster). Realistic Postgres workload: log lines,
    ///   descriptions, JSON.
    /// * **Multi-byte UTF-8** (~78 B Cyrillic): −74.0% (~3.9×
    ///   faster). Internationalised content: non-Latin names,
    ///   free-form text.
    /// * **Short ASCII** (17 B `alice@example.com`): +9.9%.
    ///   Acceptable cost: 0.7 ns/col absolute regression on the
    ///   cheapest case (where total time is already 8 ns/col). A
    ///   length-threshold hybrid was tested and rejected — the
    ///   dispatch branch costs ~1.5 ns/col, exceeding the savings
    ///   on the short-ASCII path.
    ///
    /// Behaviour is byte-identical to `core::str::from_utf8`:
    /// both accept the same byte sequences, reject the same non-
    /// UTF-8 inputs, and produce the same `&str` for valid input.
    /// `simdutf8::basic::Utf8Error` is discriminator-only;
    /// collapsed to `DecodeError::NonUtf8` here.
    #[inline]
    fn from_pg_text(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        simdutf8::basic::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// ═════════════════════════════════════════════════════════════════
// FromPgBinary — parallel to FromPgText for PG binary-format
// columns (Extended Query Bind-selected per-parameter).
//
// Binary format byte layout matches PG §55.7 — fixed-size ints
// are big-endian two's complement, `bool` is a single byte 0/1,
// `text` is raw UTF-8 bytes. Every impl's `OID` const is drift-
// pinned against `oids::*` to catch type-mapping bugs at build
// time.
// ═════════════════════════════════════════════════════════════════

/// Decode a column's binary-format bytes into a typed Rust value.
///
/// Parallel to [`FromPgText`]; the caller dispatches between text
/// and binary decoders based on [`ColumnDesc::format_code`].
/// Extended Query selects binary via the Bind frame's per-param /
/// per-result format-code arrays; Simple Query always uses text.
///
/// # OID drift-pin
///
/// Every impl exposes a `const OID: u32` matching the PG type it
/// decodes. The crate's [`oids`] module is drift-pinned against the
/// canonical PG catalog (`pg_type.dat`); a const-assert per impl
/// verifies `<T as FromPgBinary>::OID == oids::X` at build time.
/// A future refactor that breaks the type↔OID mapping fails the
/// build, not at runtime.
///
/// # Sealed
///
/// The `sealed::FromPgBinarySealed` supertrait is module-private
/// — downstream crates cannot impl the trait for their own Rust
/// types. The binary-codec surface is a fixed set of primitives;
/// wider types (arrays, uuid, timestamp) land with their dedicated
/// follow-ups.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `FromPgBinary` (cannot decode from PG binary format)",
    label = "supported binary-decode types are `i16`, `i32`, `i64`, `bool`, `&str`",
    note = "`FromPgBinary` is sealed — extend by adding a `from_pg_binary_int!` invocation in `decode.rs`; downstream `impl FromPgBinary for ...` is forbidden by the sealed supertrait"
)]
pub trait FromPgBinary<'a>: Sized + sealed::FromPgBinarySealed {
    /// PG type OID this decoder handles. Drift-pinned against
    /// [`oids`] via const-assert.
    const OID: u32;

    /// Decode the column's binary-format bytes.
    ///
    /// # Errors
    ///
    /// - [`DecodeError::TruncatedColumnData`] — input length doesn't
    ///   match the type's fixed size (for fixed-size types).
    /// - [`DecodeError::BoolParse`] — byte outside `{0, 1}` for `bool`.
    /// - [`DecodeError::NonUtf8`] — non-UTF-8 bytes for `&str` / text.
    fn from_pg_binary(bytes: &'a [u8]) -> Result<Self, DecodeError>;
}

mod sealed {
    pub trait FromPgBinarySealed {}
    pub trait EncodeBinarySealed {}
}

// Fixed-size signed integer decoders: N bytes big-endian.
macro_rules! impl_from_pg_binary_int {
    ($($t:ty, $oid:expr, $n:literal),+ $(,)?) => {
        $(
            impl sealed::FromPgBinarySealed for $t {}
            impl FromPgBinary<'_> for $t {
                const OID: u32 = $oid;
                #[inline]
                fn from_pg_binary(bytes: &[u8]) -> Result<Self, DecodeError> {
                    // Binary fixed-size ints: exactly N bytes. Any
                    // other length is classified via
                    // `BinaryLengthMismatch` — a per-type honest error
                    // that doesn't lie about a column index the decoder
                    // can't know.
                    let arr: &[u8; $n] = bytes
                        .first_chunk::<$n>()
                        .filter(|_| bytes.len() == $n)
                        .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                            expected_len: $n,
                            // `bytes` is a column-body slice bounded
                            // by `READ_BUF_CAP <= u16::MAX`; the
                            // narrowing helper encapsulates the dead-
                            // arm landing pad.
                            actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
                        })?;
                    Ok(<$t>::from_be_bytes(*arr))
                }
            }
        )+
    };
}

impl_from_pg_binary_int!(
    i16, oids::INT2, 2,
    i32, oids::INT4, 4,
    i64, oids::INT8, 8,
    u32, oids::OID, 4,
);

/// PG binary `bool`: one byte — `0` = false, `1` = true.
/// Wrong byte length classifies as [`DecodeError::BinaryLengthMismatch`];
/// length-1 with an out-of-range byte classifies as
/// [`DecodeError::BoolParse`].
impl sealed::FromPgBinarySealed for bool {}
impl FromPgBinary<'_> for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn from_pg_binary(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            [_] => Err(DecodeError::BoolParse),
            _ => Err(DecodeError::BinaryLengthMismatch {
                expected_len: 1,
                // `bytes` is a column-body slice bounded by
                // `READ_BUF_CAP <= u16::MAX`.
                actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
            }),
        }
    }
}

/// PG binary `text`: raw UTF-8 bytes. Zero-copy borrow.
///
/// # UTF-8 validation cost
///
/// Every column read walks the column bytes to verify UTF-8 well-formedness
/// — `simdutf8::basic::from_utf8` (SIMD-accelerated, matching the text-format
/// path) is O(N). Under `#![forbid(unsafe_code)]` validation cannot be
/// skipped — `core::str::from_utf8_unchecked` is unsafe and inaccessible.
/// Callers who need to bypass should hold the bytes as `&[u8]` (via a
/// separate `FromPgBinary<Target = &[u8]>` impl — not implemented today)
/// and validate externally if / when they need a `&str`.
///
/// PG binary `text` is NOMINALLY UTF-8 per `client_encoding`; a buggy
/// server / misconfigured encoding setting could produce invalid bytes.
/// The Err path classifies as [`DecodeError::NonUtf8`] without
/// panicking — consistent with the column-level safety contract.
impl sealed::FromPgBinarySealed for &str {}
impl<'a> FromPgBinary<'a> for &'a str {
    const OID: u32 = oids::TEXT;
    #[inline]
    fn from_pg_binary(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        simdutf8::basic::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// Compile-time symmetry pins: text and binary decoders for the
// same Rust type MUST target the same PG type OID. A refactor that
// breaks this breaks the build.
//
// `FromPgText` carries `OID` too; the three traits
// (text / binary / encode) form a closed symmetry family. Adding
// a new Rust type that impls any ONE of these forces matching
// impls + identical OIDs across all three, verified here.
const _: () = {
    assert!(<i16 as FromPgBinary>::OID == oids::INT2);
    assert!(<i32 as FromPgBinary>::OID == oids::INT4);
    assert!(<i64 as FromPgBinary>::OID == oids::INT8);
    assert!(<u32 as FromPgBinary>::OID == oids::OID);
    assert!(<bool as FromPgBinary>::OID == oids::BOOL);
    assert!(<&str as FromPgBinary>::OID == oids::TEXT);
    // Text↔binary OID symmetry: the same Rust type MUST target the
    // same PG type OID across text and binary decoders. A refactor
    // that skewed one against the other would mean the same Rust
    // type decoded differently depending on `ColumnDesc::format_code`
    // — a classification bug. Pinned below.
    assert!(<i16 as FromPgText>::OID == <i16 as FromPgBinary>::OID);
    assert!(<i32 as FromPgText>::OID == <i32 as FromPgBinary>::OID);
    assert!(<i64 as FromPgText>::OID == <i64 as FromPgBinary>::OID);
    assert!(<u32 as FromPgText>::OID == <u32 as FromPgBinary>::OID);
    assert!(<bool as FromPgText>::OID == <bool as FromPgBinary>::OID);
    assert!(<&str as FromPgText>::OID == <&str as FromPgBinary>::OID);
};

// ═════════════════════════════════════════════════════════════════
// Compile-time FormatCode × Type matrix.
//
// Type-level encoding of which (FormatCode, Rust-type) pairs are
// valid for column decoding. Currently every primitive type
// (i16/i32/i64/u32/bool/&str) implements BOTH text and binary
// decoders — the matrix closes the runtime "is this (T, F) pair
// supported" classification at the type level so any future type
// with text-only OR binary-only support automatically rejects the
// missing-format dispatch at compile time.
//
// Tier impact: caller dispatch on (T, F) is **tier-1 by-
// construction** — a static `DecodeFormat<F>` bound is the
// type-system check; missing impl == compile error.
//
// Additive — does NOT replace [`FromPgText`] / [`FromPgBinary`].
// Both legacy traits remain (caller can still invoke
// `T::from_pg_text` or `T::from_pg_binary` directly). DecodeFormat
// is the new generic-F-parameterised dispatch surface; impls
// forward to the underlying legacy trait.
// ═════════════════════════════════════════════════════════════════

mod format_marker_sealed {
    pub trait FormatCodeMarkerSealed {}
    pub trait DecodeFormatSealed<F> {}
}

/// Type-level marker corresponding to a [`FormatCode`] wire variant.
///
/// Two implementors exist, sealed inside this crate: [`TextFmt`] and
/// [`BinaryFmt`]. The corresponding runtime [`FormatCode`] value is
/// available via the [`Self::WIRE`] constant — used by the runtime
/// dispatcher [`decode_with_format`] to bridge the runtime
/// `FormatCode` from `RowDescription` to the static format marker.
///
/// Downstream crates cannot implement this trait (sealed); the
/// closed set matches the PG wire spec (§55.2.2) which permits
/// only two format codes. A future PG major-version revision adding
/// a third format code would be a breaking-change major version
/// of this crate.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `FormatCodeMarker`",
    label = "valid markers are `TextFmt` (PG `FormatCode::Text`, wire byte 0) and `BinaryFmt` (PG `FormatCode::Binary`, wire byte 1)",
    note = "`FormatCodeMarker` is sealed — the closed set matches PG protocol spec §55.2.2 which permits exactly these two format codes; a third would be a major-version breaking change"
)]
pub trait FormatCodeMarker: format_marker_sealed::FormatCodeMarkerSealed {
    /// Runtime [`FormatCode`] value this marker corresponds to.
    const WIRE: FormatCode;
}

/// Type-level marker for [`FormatCode::Text`] (wire byte `0`).
///
/// Zero-sized; used as the format type parameter on
/// [`DecodeFormat`]. See [`FormatCodeMarker`] for the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TextFmt;

/// Type-level marker for [`FormatCode::Binary`] (wire byte `1`).
///
/// Zero-sized; used as the format type parameter on
/// [`DecodeFormat`]. See [`FormatCodeMarker`] for the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BinaryFmt;

impl format_marker_sealed::FormatCodeMarkerSealed for TextFmt {}
impl format_marker_sealed::FormatCodeMarkerSealed for BinaryFmt {}

impl FormatCodeMarker for TextFmt {
    const WIRE: FormatCode = FormatCode::Text;
}
impl FormatCodeMarker for BinaryFmt {
    const WIRE: FormatCode = FormatCode::Binary;
}

/// Type-level format-parameterised decoder.
///
/// Generic over `F: FormatCodeMarker` — the static format marker.
/// Implemented for each (Rust type, wire format) pair the crate
/// supports. Sealed via `format_marker_sealed::DecodeFormatSealed`;
/// downstream crates cannot add impls for their own types. Wider
/// type coverage (date, time, uuid, decimal) lands with future
/// follow-ups.
///
/// # Type-level pair check
///
/// Calling `<T as DecodeFormat<F>>::decode(bytes)` requires T to
/// implement DecodeFormat`<F>`. A missing pair (e.g. a hypothetical
/// type with only text support but caller tries
/// `<T as DecodeFormat<BinaryFmt>>::decode`) is a compile error,
/// NOT a runtime classification. This **closes** the runtime
/// "format-OID mismatch" classification at the type level.
///
/// # OID symmetry
///
/// Each impl's `OID` matches the corresponding [`FromPgText::OID`]
/// or [`FromPgBinary::OID`] for the same Rust type — pinned via
/// const-asserts after every impl block below. A drift in OID
/// between DecodeFormat and the legacy traits fails compilation.
///
/// # Forwarding
///
/// Each impl forwards to the matching legacy trait
/// ([`FromPgText`] for `F = TextFmt`, [`FromPgBinary`] for
/// `F = BinaryFmt`) — no behavior change, no new decode paths.
/// DecodeFormat is purely a dispatch-surface refinement.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `DecodeFormat<'_, {F}>`",
    label = "the (type, format) pair `({Self}, {F})` is not in the supported decode matrix",
    note = "`DecodeFormat` is sealed — supported pairs are the cartesian product of `{{i16, i32, i64, u32, bool, &str}} × {{TextFmt, BinaryFmt}}`. Extend by adding a `decode_format_impl!` invocation in `decode.rs`; downstream `impl DecodeFormat for ...` is forbidden by the sealed supertrait"
)]
pub trait DecodeFormat<'a, F: FormatCodeMarker>:
    Sized + format_marker_sealed::DecodeFormatSealed<F>
{
    /// PG type OID this (type, format) pair targets.
    ///
    /// Pinned via const-assert to match the corresponding
    /// [`FromPgText::OID`] / [`FromPgBinary::OID`].
    const OID: u32;

    /// Decode the column's bytes in the format specified by `F`.
    ///
    /// Forwards to [`FromPgText::from_pg_text`] (for `F = TextFmt`)
    /// or [`FromPgBinary::from_pg_binary`] (for `F = BinaryFmt`).
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError>;
}

// DecodeFormat impls — six primitive types × two format markers
// = 12 impls. Macro avoids 12 copies of the same boilerplate.
macro_rules! impl_decode_format_text {
    ($($t:ty),+ $(,)?) => {
        $(
            impl format_marker_sealed::DecodeFormatSealed<TextFmt> for $t {}
            impl<'a> DecodeFormat<'a, TextFmt> for $t {
                const OID: u32 = <$t as FromPgText<'a>>::OID;
                #[inline]
                fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
                    <$t as FromPgText<'a>>::from_pg_text(bytes)
                }
            }
        )+
    };
}

macro_rules! impl_decode_format_binary {
    ($($t:ty),+ $(,)?) => {
        $(
            impl format_marker_sealed::DecodeFormatSealed<BinaryFmt> for $t {}
            impl<'a> DecodeFormat<'a, BinaryFmt> for $t {
                const OID: u32 = <$t as FromPgBinary<'a>>::OID;
                #[inline]
                fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
                    <$t as FromPgBinary<'a>>::from_pg_binary(bytes)
                }
            }
        )+
    };
}

impl_decode_format_text!(i16, i32, i64, u32, bool);
impl_decode_format_binary!(i16, i32, i64, u32, bool);

// `&str` has a non-trivial lifetime in both legacy traits; macro
// substitution would tangle the `'a` bindings. Hand-rolled below.
impl format_marker_sealed::DecodeFormatSealed<TextFmt> for &str {}
impl<'a> DecodeFormat<'a, TextFmt> for &'a str {
    const OID: u32 = <&'a str as FromPgText<'a>>::OID;
    #[inline]
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        <&'a str as FromPgText<'a>>::from_pg_text(bytes)
    }
}

impl format_marker_sealed::DecodeFormatSealed<BinaryFmt> for &str {}
impl<'a> DecodeFormat<'a, BinaryFmt> for &'a str {
    const OID: u32 = <&'a str as FromPgBinary<'a>>::OID;
    #[inline]
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        <&'a str as FromPgBinary<'a>>::from_pg_binary(bytes)
    }
}

// Compile-time OID drift pin between DecodeFormat and the legacy
// FromPgText/FromPgBinary traits. A future refactor that touched
// one side without the other (or assigned a stale OID constant to
// a new DecodeFormat impl) fails the build.
const _: () = {
    // Text-format OID pins.
    assert!(<i16 as DecodeFormat<TextFmt>>::OID == <i16 as FromPgText>::OID);
    assert!(<i32 as DecodeFormat<TextFmt>>::OID == <i32 as FromPgText>::OID);
    assert!(<i64 as DecodeFormat<TextFmt>>::OID == <i64 as FromPgText>::OID);
    assert!(<u32 as DecodeFormat<TextFmt>>::OID == <u32 as FromPgText>::OID);
    assert!(<bool as DecodeFormat<TextFmt>>::OID == <bool as FromPgText>::OID);
    assert!(<&str as DecodeFormat<TextFmt>>::OID == <&str as FromPgText>::OID);

    // Binary-format OID pins.
    assert!(<i16 as DecodeFormat<BinaryFmt>>::OID == <i16 as FromPgBinary>::OID);
    assert!(<i32 as DecodeFormat<BinaryFmt>>::OID == <i32 as FromPgBinary>::OID);
    assert!(<i64 as DecodeFormat<BinaryFmt>>::OID == <i64 as FromPgBinary>::OID);
    assert!(<u32 as DecodeFormat<BinaryFmt>>::OID == <u32 as FromPgBinary>::OID);
    assert!(<bool as DecodeFormat<BinaryFmt>>::OID == <bool as FromPgBinary>::OID);
    assert!(<&str as DecodeFormat<BinaryFmt>>::OID == <&str as FromPgBinary>::OID);

    // Marker WIRE constants match the FormatCode variant they encode.
    assert!(matches!(<TextFmt as FormatCodeMarker>::WIRE, FormatCode::Text));
    assert!(matches!(<BinaryFmt as FormatCodeMarker>::WIRE, FormatCode::Binary));
};

/// Runtime [`FormatCode`] → static dispatch helper.
///
/// Bridges the runtime `FormatCode` value carried in
/// `RowDescription` / [`ColumnDesc::format_code`] to the
/// compile-time [`DecodeFormat`] dispatch surface. Requires `T`
/// to implement **both** [`DecodeFormat<TextFmt>`] **and**
/// [`DecodeFormat<BinaryFmt>`] — the common case for every
/// primitive type.
///
/// A future type with only one format impl cannot be dispatched
/// via this function (compile error at the trait-bound check),
/// closing the (T, F) pair-validity question at the type level:
/// either both impls exist and runtime dispatch is sound, or one
/// is missing and the call site fails to compile.
///
/// # Why not a `match` on `FormatCode`?
///
/// Caller could inline `match fmt { Text => T::decode::<TextFmt>(b),
/// Binary => T::decode::<BinaryFmt>(b) }` — that's exactly what
/// this helper centralises. The win is one canonical dispatch site
/// (per-callsite ad-hoc matches would diverge over time; one
/// helper stays drift-pinned).
///
/// # Exhaustive over [`FormatCode`]
///
/// [`FormatCode`] is closed-by-spec exhaustive (Text / Binary
/// only); a new variant would require a major PG protocol bump.
/// Adding a variant without updating this helper is a compile
/// error (the inner `match` is exhaustive, not `_ => `).
#[inline]
pub fn decode_with_format<'a, T>(
    bytes: &'a [u8],
    fmt: FormatCode,
) -> Result<T, DecodeError>
where
    T: DecodeFormat<'a, TextFmt> + DecodeFormat<'a, BinaryFmt>,
{
    match fmt {
        FormatCode::Text => <T as DecodeFormat<'a, TextFmt>>::decode(bytes),
        FormatCode::Binary => <T as DecodeFormat<'a, BinaryFmt>>::decode(bytes),
    }
}

// ═════════════════════════════════════════════════════════════════
// EncodeBinary — PG binary format write path (mirror of
// `FromPgBinary`). Used by `ParamsWriter` to serialise parameter
// values into the Bind frame's per-param length+bytes layout.
// ═════════════════════════════════════════════════════════════════

/// Encode a Rust value into PG binary format bytes, directly into
/// a [`crate::write_buf::WriteBuf`].
///
/// Parallel to [`FromPgBinary`] — the `OID` constants pair up
/// across the two traits so a future `query!` macro can check
/// param-type OIDs against the `Parse`-time schema fingerprint at
/// compile time.
///
/// Zero-alloc: writes directly into the caller's `WriteBuf`. No
/// intermediate heap buffer, no stack fixture — the caller owns
/// the output storage.
///
/// # Sealed
///
/// Same seal discipline as [`FromPgBinary`] — downstream crates
/// cannot add impls for their own types.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `EncodeBinary` (cannot encode to PG binary format)",
    label = "supported binary-encode types are `i16`, `i32`, `i64`, `bool`, `&str`",
    note = "`EncodeBinary` is sealed — extend by adding `impl EncodeBinary for ...` for the new type in `decode.rs` after extending the supported-OID matrix; downstream `impl EncodeBinary for ...` is forbidden by construction"
)]
pub trait EncodeBinary: sealed::EncodeBinarySealed {
    /// PG type OID this encoder produces. Drift-pinned against
    /// [`oids`] and cross-asserted against the matching
    /// [`FromPgBinary`] impl.
    const OID: u32;

    /// Write the encoded bytes into `dst`. The caller is responsible
    /// for the surrounding per-param length prefix (PG Bind frame
    /// layout); `encode_to` writes only the payload bytes.
    ///
    /// # Errors
    ///
    /// [`crate::write_buf::WriteBufFull`] if the buffer can't fit
    /// the encoded output — architecturally-bounded at the call
    /// site via the Bind-message size const-assert, but surfaced
    /// as a classified error rather than a panic.
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>;
}

macro_rules! impl_encode_binary_int {
    ($($t:ty, $oid:expr, $push:ident),+ $(,)?) => {
        $(
            impl sealed::EncodeBinarySealed for $t {}
            impl EncodeBinary for $t {
                const OID: u32 = $oid;
                #[inline]
                fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
                    -> Result<(), crate::write_buf::WriteBufFull>
                {
                    dst.$push(*self)
                }
            }
        )+
    };
}

impl_encode_binary_int!(
    i16, oids::INT2, push_i16_be,
    i32, oids::INT4, push_i32_be,
    u32, oids::OID, push_u32_be,
);

impl sealed::EncodeBinarySealed for i64 {}
impl EncodeBinary for i64 {
    const OID: u32 = oids::INT8;
    #[inline]
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i64_be(*self)
    }
}

/// `bool` encoder: `0x00` for `false`, `0x01` for `true`.
impl sealed::EncodeBinarySealed for bool {}
impl EncodeBinary for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_u8(u8::from(*self))
    }
}

/// `&str` encoder — raw UTF-8 bytes (Rust invariant guarantees
/// UTF-8 validity, nothing to check).
impl sealed::EncodeBinarySealed for &str {}
impl EncodeBinary for &str {
    const OID: u32 = oids::TEXT;
    #[inline]
    fn encode_to(&self, dst: &mut crate::write_buf::WriteBuf)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self.as_bytes())
    }
}

// Drift-pins: every EncodeBinary impl's OID matches the
// corresponding FromPgBinary impl AND the canonical `oids::*`
// constant. One const-block pins the whole set.
const _: () = {
    assert!(<i16 as EncodeBinary>::OID == oids::INT2);
    assert!(<i32 as EncodeBinary>::OID == oids::INT4);
    assert!(<i64 as EncodeBinary>::OID == oids::INT8);
    assert!(<u32 as EncodeBinary>::OID == oids::OID);
    assert!(<bool as EncodeBinary>::OID == oids::BOOL);
    assert!(<&str as EncodeBinary>::OID == oids::TEXT);
    // Cross-trait symmetry (text-format OID ≡ binary-format OID ≡ catalog OID).
    assert!(<i16 as EncodeBinary>::OID == <i16 as FromPgBinary>::OID);
    assert!(<i32 as EncodeBinary>::OID == <i32 as FromPgBinary>::OID);
    assert!(<i64 as EncodeBinary>::OID == <i64 as FromPgBinary>::OID);
    assert!(<u32 as EncodeBinary>::OID == <u32 as FromPgBinary>::OID);
    assert!(<bool as EncodeBinary>::OID == <bool as FromPgBinary>::OID);
    assert!(<&str as EncodeBinary>::OID == <&str as FromPgBinary>::OID);
};

/// PostgreSQL built-in type OID constants for the subset the
/// decoders cover. Full list at
/// `https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat`.
///
/// Callers match these against [`ColumnDesc::type_oid`] to
/// dispatch the right [`FromPgText`] impl. A future `query!`
/// macro consumes this mapping at compile time via generated
/// decoders.
///
/// # Tier-1 compile drift-pin
///
/// The `const _: () = { assert!(...) }` block below asserts every
/// constant against its canonical PG catalog value. A typo
/// (`INT4 = 32` instead of `23`) fails the build. No runtime test
/// required — the drift guard is the type system itself.
pub mod oids {
    /// `bool` (1-byte typtype `b`).
    pub const BOOL: u32 = 16;
    /// `bytea`.
    pub const BYTEA: u32 = 17;
    /// `"char"` — internal 1-byte char, not standard `char(n)`.
    pub const CHAR: u32 = 18;
    /// `name` — fixed 64-byte identifier (NAMEDATALEN).
    pub const NAME: u32 = 19;
    /// `int8` / `bigint`.
    pub const INT8: u32 = 20;
    /// `int2` / `smallint`.
    pub const INT2: u32 = 21;
    /// `int4` / `integer`.
    pub const INT4: u32 = 23;
    /// `text`.
    pub const TEXT: u32 = 25;
    /// `oid` — object identifier (u32).
    pub const OID: u32 = 26;
    /// `float4` / `real`.
    pub const FLOAT4: u32 = 700;
    /// `float8` / `double precision`.
    pub const FLOAT8: u32 = 701;
    /// `bpchar` — `char(n)`, blank-padded.
    pub const BPCHAR: u32 = 1042;
    /// `varchar` — `varchar(n)`.
    pub const VARCHAR: u32 = 1043;
    /// `timestamp` — timestamp without time zone.
    pub const TIMESTAMP: u32 = 1114;
    /// `timestamptz` — timestamp with time zone.
    pub const TIMESTAMPTZ: u32 = 1184;
    /// `uuid`.
    pub const UUID: u32 = 2950;
    /// `jsonb`.
    pub const JSONB: u32 = 3802;

    // Tier-1 compile drift-pin against the canonical PG catalog
    // (src/include/catalog/pg_type.dat). A typo in any constant
    // above breaks the build here — no runtime test needed.
    const _: () = {
        assert!(BOOL == 16, "oids::BOOL drift from pg_type.dat");
        assert!(BYTEA == 17, "oids::BYTEA drift from pg_type.dat");
        assert!(CHAR == 18, "oids::CHAR drift from pg_type.dat");
        assert!(NAME == 19, "oids::NAME drift from pg_type.dat");
        assert!(INT8 == 20, "oids::INT8 drift from pg_type.dat");
        assert!(INT2 == 21, "oids::INT2 drift from pg_type.dat");
        assert!(INT4 == 23, "oids::INT4 drift from pg_type.dat");
        assert!(TEXT == 25, "oids::TEXT drift from pg_type.dat");
        assert!(OID == 26, "oids::OID drift from pg_type.dat");
        assert!(FLOAT4 == 700, "oids::FLOAT4 drift from pg_type.dat");
        assert!(FLOAT8 == 701, "oids::FLOAT8 drift from pg_type.dat");
        assert!(BPCHAR == 1042, "oids::BPCHAR drift from pg_type.dat");
        assert!(VARCHAR == 1043, "oids::VARCHAR drift from pg_type.dat");
        assert!(TIMESTAMP == 1114, "oids::TIMESTAMP drift from pg_type.dat");
        assert!(TIMESTAMPTZ == 1184, "oids::TIMESTAMPTZ drift from pg_type.dat");
        assert!(UUID == 2950, "oids::UUID drift from pg_type.dat");
        assert!(JSONB == 3802, "oids::JSONB drift from pg_type.dat");
    };
}

#[cfg(test)]
mod parse_tests {
    //! `parse_row_description` conformance per PG §55.7 + bad-path
    //! classification. Category (1)/(B) per reforge.md §4.11 —
    //! spec-conformance table + tier-3 framing-error shield.
    //!
    //! Assertion style: every test uses `assert!(matches!(...))` +
    //! optional `assert_eq!` on destructured fields. The crate-root
    //! forbid bundle bans `panic!`, `.expect()`, `.unwrap()`, and
    //! `unreachable!()` even in unit tests, so the usual
    //! `expect_err("...")` idiom is replaced by `matches!(Err(...))`.
    //! Diagnostic messages on mismatch go into the `assert!` format
    //! string (evaluated only on failure).
    extern crate alloc;
    use super::*;
    use crate::error::ProtocolError;

    /// Build one RowDescription column block: name + NUL + 18 bytes of
    /// metadata (table_oid, attr_num, type_oid, type_size, type_mod,
    /// format_code).
    fn column_block(name: &[u8], type_oid: u32, format_code: i16) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        out.extend_from_slice(name);
        out.push(0);
        out.extend_from_slice(&0i32.to_be_bytes()); // table_oid
        out.extend_from_slice(&0i16.to_be_bytes()); // attr_num
        out.extend_from_slice(&type_oid.to_be_bytes());
        out.extend_from_slice(&(-1i16).to_be_bytes()); // type_size = variable
        out.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod = none
        out.extend_from_slice(&format_code.to_be_bytes());
        out
    }

    /// Build a full RowDescription body. `columns.len() ≤ i16::MAX`
    /// is guaranteed by `MAX_ROW_COLUMNS = 32 ≪ i16::MAX`. The
    /// `fixture_i16` helper asserts the bound and narrows;
    /// invariant breach is `#[track_caller]`-attributed loud-fail,
    /// not silent `unwrap_or(0)` fixture corruption.
    fn build(columns: &[(&[u8], u32, i16)]) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        let count = crate::test_fixtures::fixture_i16(columns.len());
        out.extend_from_slice(&count.to_be_bytes());
        for (name, oid, fc) in columns {
            out.extend_from_slice(&column_block(name, *oid, *fc));
        }
        out
    }

    /// Invariant (spec): a well-formed 2-column payload parses to the
    /// declared count, per-column OIDs, and text format codes.
    #[test]
    fn two_column_text_format_roundtrip() {
        let body = build(&[(b"id", 23, 0), (b"name", 25, 0)]);
        let result = parse_row_description(&body);
        let expected: [ColumnDesc; 2] = [
            ColumnDesc {
                type_oid: 23,
                format_code: FormatCode::Text,
            },
            ColumnDesc {
                type_oid: 25,
                format_code: FormatCode::Text,
            },
        ];
        // SoA storage; reconstruct AoS view via columns_iter().
        let actual: alloc::vec::Vec<ColumnDesc> = match &result {
            Ok(desc) => desc.columns_iter().collect(),
            Err(_) => alloc::vec::Vec::new(),
        };
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "expected 2-column text parse, got {result:?}",
        );
    }

    /// Invariant (spec): format code 1 parses as Binary.
    #[test]
    fn binary_format_parsed() {
        let body = build(&[(b"x", 23, 1)]);
        let result = parse_row_description(&body);
        assert!(
            matches!(
                &result,
                Ok(desc) if matches!(
                    desc.get(0),
                    Some(ColumnDesc { format_code: FormatCode::Binary, .. }),
                ),
            ),
            "expected Binary format first column, got {result:?}",
        );
    }

    /// Invariant (spec + round-4 #5): format code outside `{0, 1}`
    /// classifies as `UnexpectedFormatCode`, not a silent fallback.
    #[test]
    fn format_code_out_of_range_is_classified() {
        let body = build(&[(b"x", 23, 7)]);
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::UnexpectedFormatCode { code: 7 })),
            "expected UnexpectedFormatCode {{ code: 7 }}, got {result:?}",
        );
    }

    /// Invariant: negative column count classifies as malformed (not
    /// a usize wrap-around).
    #[test]
    fn negative_column_count_is_malformed() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&(-1i16).to_be_bytes());
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for negative count, got {result:?}",
        );
    }

    /// Invariant: a column count exceeding `MAX_ROW_COLUMNS` classifies
    /// as `TooManyColumns` with the actual counts — the caller can
    /// message the user clearly.
    #[test]
    fn column_count_exceeding_max_is_classified() {
        // Declare count = MAX + 1 (still fits i16); parser rejects
        // before per-column parsing.
        let over = MAX_ROW_COLUMNS.saturating_add(1);
        let count = crate::test_fixtures::fixture_i16(over);
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&count.to_be_bytes());
        let result = parse_row_description(&body);
        assert!(
            matches!(
                result,
                Err(ProtocolError::TooManyColumns { count: c, max }) if c == over && max == MAX_ROW_COLUMNS,
            ),
            "expected TooManyColumns {{ count: {over}, max: {MAX_ROW_COLUMNS} }}, got {result:?}",
        );
    }

    /// Invariant: payload too short for the column count header is
    /// malformed.
    #[test]
    fn payload_too_short_for_count_is_malformed() {
        for (label, buf) in [("empty", &[][..]), ("1-byte", &[0][..])] {
            let result = parse_row_description(buf);
            assert!(
                matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
                "{label} payload: expected MalformedRowDescription, got {result:?}",
            );
        }
    }

    /// Invariant: a column body missing the 18-byte metadata tail is
    /// malformed (spec framing desync).
    #[test]
    fn column_metadata_truncated_is_malformed() {
        // Declare 1 column but give only name + partial metadata.
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(b"x\0");
        body.extend_from_slice(&[0u8; 10]); // only 10 of 18 bytes
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for truncated metadata, got {result:?}",
        );
    }

    /// Invariant: a column name without NUL terminator is malformed.
    #[test]
    fn column_name_unterminated_is_malformed() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(b"no_nul_here_ever");
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for unterminated name, got {result:?}",
        );
    }

    /// Invariant: trailing bytes after the declared column count are a
    /// framing bug (shouldn't happen on a well-formed server), classified
    /// as malformed.
    #[test]
    fn trailing_bytes_after_columns_is_malformed() {
        let mut body = build(&[(b"x", 23, 0)]);
        body.push(0xAA); // stray trailing byte
        let result = parse_row_description(&body);
        assert!(
            matches!(result, Err(ProtocolError::MalformedRowDescription { .. })),
            "expected MalformedRowDescription for trailing bytes, got {result:?}",
        );
    }

    /// Invariant: exactly `MAX_ROW_COLUMNS` columns parses cleanly
    /// (boundary value).
    #[test]
    fn exactly_max_columns_parses() {
        let cols: alloc::vec::Vec<(&[u8], u32, i16)> = (0..MAX_ROW_COLUMNS)
            .map(|_i| (&b"c"[..], 23u32, 0i16))
            .collect();
        let body = build(&cols);
        let result = parse_row_description(&body);
        assert!(
            matches!(&result, Ok(desc) if desc.len() == MAX_ROW_COLUMNS),
            "expected MAX_ROW_COLUMNS parse, got {result:?}",
        );
    }
}

#[cfg(test)]
mod data_row_tests {
    //! `DataRowRef` + `ColumnsIter` spec-conformance per PG §55.7
    //! `DataRow` shape + bad-path classification.
    //!
    //! Body layout: i16 column-count + per-column `(i32 length,
    //! data-bytes)`. `length = -1` encodes SQL NULL.

    extern crate alloc;
    use super::*;

    /// Build a DataRow body: 2-byte count + per-column payloads.
    /// `None` = NULL, `Some(bytes)` = data. `fixture_i16` /
    /// `fixture_i32` enforce the wire-width bounds with
    /// `#[track_caller]` loud-fail; previous `unwrap_or(0)` silently
    /// emitted zero counts/lengths on overflow, corrupting fixtures.
    fn build(columns: &[Option<&[u8]>]) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        let count = crate::test_fixtures::fixture_i16(columns.len());
        out.extend_from_slice(&count.to_be_bytes());
        for col in columns {
            match col {
                Some(data) => {
                    let len = crate::test_fixtures::fixture_i32(data.len());
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(data);
                }
                None => {
                    out.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        out
    }

    /// Parse a body and return the row — with `assert` fail path
    /// that avoids the forbid-bundle's bans on `panic!`, `.unwrap()`,
    /// `.expect()`, `unreachable!()`, and `assert!(false)`.
    ///
    /// The `assert!(matches!(...))` ensures Ok on well-formed input;
    /// if it fires, the test fails before reaching the `else` branch,
    /// so the `return` is defensive dead code satisfying
    /// borrow-checker exhaustiveness on the post-assert decomposition.
    fn must_parse(body: &[u8]) -> DataRowRef<'_> {
        let result = DataRowRef::parse(body);
        assert!(
            result.is_ok(),
            "fixture parse should succeed, got {result:?}",
        );
        result.unwrap_or(DataRowRef {
            body_after_count: &[],
            n_columns: 0,
        })
    }

    /// Invariant (spec): a well-formed 2-column row yields both
    /// values in order; length + data round-trip verbatim.
    #[test]
    fn two_column_row_roundtrip() {
        let body = build(&[Some(b"hello"), Some(b"world")]);
        let row = must_parse(&body);
        assert_eq!(row.len(), 2);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert_eq!(items.len(), 2);
        assert!(matches!(items.first(), Some(Ok(Some(b"hello")))));
        assert!(matches!(items.get(1), Some(Ok(Some(b"world")))));
    }

    /// Invariant (spec): `length = -1` encodes SQL NULL, surfaced as
    /// `Ok(None)` — distinct from empty bytes `Ok(Some(b""))`.
    #[test]
    fn null_column_is_none() {
        let body = build(&[Some(b"x"), None, Some(b"y")]);
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert_eq!(items.len(), 3);
        assert!(matches!(items.first(), Some(Ok(Some(b"x")))));
        assert!(matches!(items.get(1), Some(Ok(None))));
        assert!(matches!(items.get(2), Some(Ok(Some(b"y")))));
    }

    /// Invariant: empty column (`length = 0`) surfaces as
    /// `Ok(Some(&[]))` — distinct from NULL.
    #[test]
    fn empty_column_is_not_null() {
        let body = build(&[Some(b""), Some(b"nonempty")]);
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert!(
            matches!(items.first(), Some(Ok(Some(s))) if s.is_empty()),
            "expected Ok(Some(empty)), got {:?}", items.first(),
        );
        assert!(matches!(items.get(1), Some(Ok(Some(b"nonempty")))));
    }

    /// Invariant: 0-column row parses — valid edge case.
    #[test]
    fn zero_column_row_parses() {
        let body = build(&[]);
        let row = must_parse(&body);
        assert!(row.is_empty());
        assert_eq!(row.columns().count(), 0);
    }

    /// Invariant: body shorter than the 2-byte count header is
    /// classified as `TruncatedRow`.
    #[test]
    fn truncated_count_header() {
        for buf in [&[][..], &[0][..]] {
            let result = DataRowRef::parse(buf);
            assert!(
                matches!(result, Err(DecodeError::TruncatedRow)),
                "expected TruncatedRow, got {result:?}",
            );
        }
    }

    /// Invariant: negative column count (i.e. count header decodes
    /// to a negative `i16`) is classified as
    /// `InvalidColumnCount { count }` with the offending i16
    /// preserved for diagnostics. Split out from the `TruncatedRow`
    /// "body too short" bucket to give operators distinct root
    /// causes.
    #[test]
    fn negative_column_count() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&(-3i16).to_be_bytes());
        let result = DataRowRef::parse(&body);
        assert!(
            matches!(result, Err(DecodeError::InvalidColumnCount { count: -3 })),
            "negative count: expected InvalidColumnCount {{ count: -3 }}, got {result:?}",
        );
    }

    /// Invariant: missing column length prefix surfaces as
    /// `TruncatedColumnLen`.
    #[test]
    fn missing_column_length_prefix() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&2i16.to_be_bytes()); // claim 2 columns
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(b"a"); // first column fine
        body.extend_from_slice(&[0, 0]); // partial length prefix for second
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert_eq!(items.len(), 2);
        assert!(matches!(items.first(), Some(Ok(Some(b"a")))));
        assert!(
            matches!(
                items.get(1),
                Some(Err(DecodeError::TruncatedColumnLen { column_idx: 1 })),
            ),
            "expected TruncatedColumnLen, got {:?}", items.get(1),
        );
    }

    /// Invariant: negative length that isn't `-1` classifies as
    /// `NegativeColumnLength`.
    #[test]
    fn negative_column_length_not_null_sentinel() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&(-7i32).to_be_bytes());
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert!(matches!(
            items.first(),
            Some(Err(DecodeError::NegativeColumnLength {
                column_idx: 0,
                length: -7,
            })),
        ));
    }

    /// Invariant: data region shorter than declared length classifies
    /// as `TruncatedColumnData` and identifies the shortage.
    #[test]
    fn truncated_column_data() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&1i16.to_be_bytes());
        body.extend_from_slice(&10i32.to_be_bytes()); // claim 10 bytes
        body.extend_from_slice(b"short"); // only 5 provided
        let row = must_parse(&body);
        let items: alloc::vec::Vec<_> = row.columns().collect();
        assert!(
            matches!(
                items.first(),
                Some(Err(DecodeError::TruncatedColumnData {
                    column_idx: 0,
                    declared_len: 10,
                    remaining: 5,
                })),
            ),
            "expected TruncatedColumnData, got {:?}", items.first(),
        );
    }

    /// Invariant: iterator is fused after an error — subsequent
    /// `.next()` calls return `None`, not re-yielding the error or
    /// advancing past broken bytes. Protects against infinite-loop
    /// consumers and double-processing.
    #[test]
    fn iterator_fuses_after_error() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&3i16.to_be_bytes()); // 3 columns claimed
        body.extend_from_slice(&(-99i32).to_be_bytes()); // invalid first col
        let row = must_parse(&body);
        let mut iter = row.columns();
        // First next: the error.
        assert!(matches!(iter.next(), Some(Err(_))));
        // Second next: fused None (not another error, not a stale value).
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    /// Invariant: `ExactSizeIterator::len()` reflects the declared
    /// column count pre-iteration and decrements with each `.next()`.
    #[test]
    fn exact_size_hint() {
        let body = build(&[Some(b"a"), Some(b"b"), Some(b"c")]);
        let row = must_parse(&body);
        let mut iter = row.columns();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        // Consume three items. Iterator yields Result; drop the
        // yielded Result via explicit match — no `let _ = next()`
        // per crate convention.
        match iter.next() {
            Some(_) | None => {}
        }
        assert_eq!(iter.size_hint(), (2, Some(2)));
        match iter.next() {
            Some(_) | None => {}
        }
        match iter.next() {
            Some(_) | None => {}
        }
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }
}

#[cfg(test)]
mod from_pg_text_tests {
    //! `FromPgText` impls — per-type text-format decoding plus the
    //! bad-path classification matrix (non-UTF-8, unparsable digits,
    //! overflow, non-canonical bool).

    use super::*;

    /// **One invariant, one test**: `i32::from_pg_text` correctly
    /// maps PG text representation into the Result<i32, DecodeError>
    /// contract — happy paths, overflow, malformed digits, non-ASCII.
    /// An arm-body swap in my impl (e.g., returning `NonUtf8` for
    /// overflow) fails this table.
    ///
    /// Non-ASCII/non-UTF-8 bytes classify as `IntParse` (not
    /// `NonUtf8`): the single-pass ASCII-digit parser treats ANY
    /// non-digit byte uniformly. The `NonUtf8` variant is reserved
    /// for `&str` / `Vec<u8>` decoders that genuinely require
    /// UTF-8 validation (arbitrary user text columns). A naive
    /// shape that ran `from_utf8` before `str::parse` would
    /// classify the same input as `NonUtf8` — duplicating work
    /// and splitting the diagnostic class.
    #[test]
    fn i32_decoder_matrix() {
        // Happy paths.
        assert!(matches!(i32::from_pg_text(b"0"), Ok(0)));
        assert!(matches!(i32::from_pg_text(b"42"), Ok(42)));
        assert!(matches!(i32::from_pg_text(b"-17"), Ok(-17)));
        assert!(matches!(i32::from_pg_text(b"+17"), Ok(17)));
        assert!(matches!(i32::from_pg_text(b"2147483647"), Ok(i32::MAX)));
        assert!(matches!(i32::from_pg_text(b"-2147483648"), Ok(i32::MIN)));

        // Overflow → IntParse.
        assert!(matches!(i32::from_pg_text(b"2147483648"), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b"-2147483649"), Err(DecodeError::IntParse)));

        // Garbage → IntParse (empty, non-digit, trailing, whitespace).
        assert!(matches!(i32::from_pg_text(b""), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b"abc"), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b"12a"), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(b" 12"), Err(DecodeError::IntParse)));

        // Non-ASCII bytes → IntParse (single-pass ASCII-digit
        // validator treats any non-digit byte uniformly).
        assert!(matches!(i32::from_pg_text(&[0xFF]), Err(DecodeError::IntParse)));
        assert!(matches!(i32::from_pg_text(&[0xC3, 0x28]), Err(DecodeError::IntParse)));
    }

    /// Tier-3 closure for the common-value fast-paths. The fast-
    /// path returns identical bytes to the macro path on the three
    /// covered literals; this test pins that equivalence so a
    /// future refactor that rewires fast-path to produce a DIFFERENT
    /// result (e.g., `b"-1"` accidentally → `1`) fails the build
    /// at test-time.
    ///
    /// **Why tier-3, not tier-1**: the fast-path and macro path
    /// produce identical observable results — Rust's type system
    /// cannot prove the equivalence statically (would need
    /// const-eval of the macro body, which is non-const). This
    /// test exercises the fast-path miss-then-hit interaction
    /// (each literal is the exact byte sequence the fast-path
    /// matches), with the same `Result<T, DecodeError>` contract.
    #[test]
    fn common_value_fast_paths_pin_correctness() {
        // i16 fast-paths.
        assert_eq!(i16::from_pg_text(b"0"), Ok(0i16));
        assert_eq!(i16::from_pg_text(b"1"), Ok(1i16));
        assert_eq!(i16::from_pg_text(b"-1"), Ok(-1i16));
        // i32 fast-paths.
        assert_eq!(i32::from_pg_text(b"0"), Ok(0i32));
        assert_eq!(i32::from_pg_text(b"1"), Ok(1i32));
        assert_eq!(i32::from_pg_text(b"-1"), Ok(-1i32));
        // i64 fast-paths.
        assert_eq!(i64::from_pg_text(b"0"), Ok(0i64));
        assert_eq!(i64::from_pg_text(b"1"), Ok(1i64));
        assert_eq!(i64::from_pg_text(b"-1"), Ok(-1i64));

        // Near-misses that MUST fall through to the digit loop and
        // return correctly (not the fast-path's literal). A bug
        // where fast-path matched too eagerly would break these.
        assert_eq!(i32::from_pg_text(b"01"), Ok(1i32)); // leading zero
        assert_eq!(i32::from_pg_text(b"10"), Ok(10i32));
        assert_eq!(i32::from_pg_text(b"-10"), Ok(-10i32));
        assert_eq!(i32::from_pg_text(b"+1"), Ok(1i32)); // explicit +
        assert_eq!(i32::from_pg_text(b"+0"), Ok(0i32));
    }

    /// **One invariant, one test**: parallel `i16` / `i64` / `u32`
    /// impls delegate to stdlib `FromStr` with per-type ranges and
    /// map failures to `IntParse`. Catches macro-expansion errors
    /// where a type's impl would mis-wire to another's range.
    #[test]
    fn other_integer_decoders_matrix() {
        // i16 boundaries.
        assert!(matches!(i16::from_pg_text(b"32767"), Ok(i16::MAX)));
        assert!(matches!(i16::from_pg_text(b"-32768"), Ok(i16::MIN)));
        assert!(matches!(i16::from_pg_text(b"32768"), Err(DecodeError::IntParse)));

        // i64 boundaries.
        assert!(matches!(i64::from_pg_text(b"9223372036854775807"), Ok(i64::MAX)));
        assert!(matches!(i64::from_pg_text(b"9223372036854775808"), Err(DecodeError::IntParse)));

        // u32 boundaries + negative rejection.
        assert!(matches!(u32::from_pg_text(b"0"), Ok(0)));
        assert!(matches!(u32::from_pg_text(b"4294967295"), Ok(u32::MAX)));
        assert!(matches!(u32::from_pg_text(b"4294967296"), Err(DecodeError::IntParse)));
        assert!(matches!(u32::from_pg_text(b"-1"), Err(DecodeError::IntParse)));
    }

    /// **One invariant, one test**: `bool::from_pg_text` accepts
    /// **exactly** PG's canonical `"t"` / `"f"` wire form — nothing
    /// else. PG server is strict on wire format; lax parsers that
    /// accept `"true"` / `"1"` / etc. would mask protocol desync if
    /// the server ever switched to a non-standard encoding.
    #[test]
    fn bool_decoder_matrix() {
        // Canonical accepts.
        assert!(matches!(bool::from_pg_text(b"t"), Ok(true)));
        assert!(matches!(bool::from_pg_text(b"f"), Ok(false)));

        // Every non-canonical form (including common false-friends
        // from SQL literal contexts) must classify as BoolParse, NOT
        // be coerced.
        for bad in [
            &b"true"[..], &b"false"[..], &b"TRUE"[..], &b"T"[..], &b"F"[..],
            &b"1"[..], &b"0"[..], &b"yes"[..], &b"no"[..], &b""[..],
        ] {
            assert!(
                matches!(bool::from_pg_text(bad), Err(DecodeError::BoolParse)),
                "expected BoolParse for {bad:?}",
            );
        }
    }

    /// **One invariant, one test**: `&str::from_pg_text` is a
    /// zero-copy UTF-8 validator. Output pointer must equal input
    /// pointer (no internal copy); non-UTF-8 input classifies as
    /// `NonUtf8`; empty input is valid.
    #[test]
    fn str_decoder_matrix() {
        let bytes: &[u8] = b"hello world";
        let result = <&str>::from_pg_text(bytes);
        assert!(matches!(result, Ok("hello world")));
        if let Ok(s) = result {
            // Zero-copy invariant — the returned &str borrows the
            // same memory region as the input &[u8].
            assert_eq!(s.as_ptr(), bytes.as_ptr());
        }

        // Empty is valid.
        assert!(matches!(<&str>::from_pg_text(b""), Ok("")));

        // Non-UTF-8 (lone continuation byte).
        assert!(matches!(<&str>::from_pg_text(&[0x80]), Err(DecodeError::NonUtf8)));
    }

    // OID drift-pin is tier-1 compile — see `decode::oids::const _`
    // block. Runtime test removed (was redundant with the
    // compile-time assertion).
}

#[cfg(test)]
mod format_code_set_tests {
    //! Bit-packed [`FormatCodeSet`] semantic + invariant tests.
    //!
    //! Every public-API surface is exercised. The 12 §7 axes (CREDO):
    //! - **Cardinality**: empty (0 cols), single, max (32), overflow (33+).
    //! - **Presence**: all-default, partial, all-set, alternating pattern.
    //! - **Temporal**: set→get round-trip, set→clear→get, multi-write.
    //! - **Size**: idx 0..32 valid; idx ≥ 32 → None / Err uniformly.
    //! - **State lifecycle**: empty seed, mid-populate, fully populated.
    //! - **Failure composition**: OutOfRange classifies, never silent.
    //! - **Memory-leak**: POD Copy, no Drop (covered by lib.rs needs_drop pin).
    //! - **Fallback**: every out-of-range path returns explicit None / Err.
    //!
    //! Concurrency / trust / platform / resource axes — not applicable
    //! (POD Copy, no I/O, branchless u32 ops portable across all
    //! supported targets).
    //!
    //! **Skepticism shield**: every test name pins a single inverse-swap
    //! the compiler would not catch. Removing any test = a compilable
    //! drift surface; `cargo test` is the only catcher.
    extern crate alloc;
    use super::*;
    use alloc::format;

    // Round-trip / boundary / independence / OutOfRange-field-
    // preservation / raw_bits round-trip are all verified at
    // compile time by the `const _: () = { ... }` blocks above the
    // test module (CREDO §4.11.1: tier-1 closure displaces
    // redundant tier-3 runtime tests).
    //
    // Tests retained below are tier-1-orthogonal — they cover
    // surfaces const-asserts can't pin: OutOfRange `.idx` field
    // Display surface, parser integration, and the wide-RowDesc
    // bit-pack round-trip.

    /// `OutOfRange::Display` carries the offending idx + max — used
    /// by future operator diagnostics. Pin the format so a body swap
    /// (idx vs max) is caught.
    // FormatCodeSet and OutOfRange deleted — format codes now stored
    // as trailing u32 words in RowDesc's Box<[u32]>.

    /// Wide-RowDesc bit-pack test. The bit-packed `FormatCodeSet`
    /// stores all 32 codes in a single u32. The narrow 2-column
    /// test below covers ordinary parser integration; THIS test
    /// pins the wide edge: 32 columns with alternating formats,
    /// closing the §4.11.1 "tier-1 on paper, broken on max inputs"
    /// seam.
    ///
    /// Specifically pins:
    /// - **Bit ordering**: column N writes bit N (not bit 31-N or some
    ///   other inversion). Pre-194 array layout had no ordering
    ///   ambiguity; bit-pack post-194 introduces a bit-position
    ///   semantic that must match column index linearly.
    /// - **All 32 bits independently settable**: max-cap row produces
    ///   a FormatCodeSet with the full alternating pattern preserved
    ///   end-to-end through parser → RowDesc → format_code(idx).
    /// - **Bit 31 (high bit) round-trip**: covers the boundary that
    ///   `mask_for_const(31) = 0x80000000` against future changes
    ///   that might accidentally use sign-flagged shift.
    #[test]
    fn wide_row_description_32_alternating_formats() {
        use alloc::vec::Vec;
        let mut frame: Vec<u8> = Vec::new();
        // MAX_ROW_COLUMNS = 32 fits i16 trivially; const-asserts in
        // this module pin the value. `fixture_i16` loud-fails on
        // overflow via `#[track_caller]` assert — replaces the prior
        // silent `return` (which would have masked a future widening
        // of MAX_ROW_COLUMNS past `i16::MAX`).
        let n_cols: i16 = crate::test_fixtures::fixture_i16(MAX_ROW_COLUMNS);
        frame.extend_from_slice(&n_cols.to_be_bytes());
        for idx in 0..MAX_ROW_COLUMNS {
            let name = format!("c{idx}");
            frame.extend_from_slice(name.as_bytes());
            frame.push(0);
            frame.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            frame.extend_from_slice(&0i16.to_be_bytes()); // attr_num
            frame.extend_from_slice(&25u32.to_be_bytes()); // type_oid (TEXT)
            frame.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
            frame.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
            // Even idx = Text (0), odd idx = Binary (1).
            let fmt: i16 = if idx % 2 == 0 { 0 } else { 1 };
            frame.extend_from_slice(&fmt.to_be_bytes());
        }

        let result = parse_row_description(&frame);
        assert!(result.is_ok(), "32-col parse must succeed, got {result:?}");
        if let Ok(desc) = result {
            assert_eq!(usize::from(desc.n_columns()), MAX_ROW_COLUMNS);
            for idx in 0..MAX_ROW_COLUMNS {
                let expected = if idx % 2 == 0 {
                    FormatCode::Text
                } else {
                    FormatCode::Binary
                };
                assert_eq!(
                    desc.format_code(idx),
                    Some(expected),
                    "column {idx}: expected {expected:?} (idx % 2 == {})",
                    idx % 2,
                );
            }
            // Boundary: format_code(MAX_ROW_COLUMNS) is None.
            assert_eq!(desc.format_code(MAX_ROW_COLUMNS), None);
        }
    }

    /// `RowDesc` end-to-end: setting columns through the parser
    /// path produces a descriptor whose `format_code(idx)` reflects
    /// the stored bit-pack. Validates the integration of `RowDesc`
    /// ← `FormatCodeSet::set`. Catches a parser-side regression
    /// that mis-wires the `format_codes.set(...)` call.
    #[test]
    fn row_desc_format_code_via_parser() {
        // Build a RowDescription frame body with two columns:
        // col 0: name="x", text format (code=0)
        // col 1: name="y", binary format (code=1)
        // PG layout: int16 count + per-column (cstring name + 18 bytes meta).
        let mut frame = alloc::vec::Vec::new();
        frame.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
        for (name, fmt) in [(b"x".as_ref(), 0i16), (b"y".as_ref(), 1i16)] {
            frame.extend_from_slice(name);
            frame.push(0); // NUL
            // table_oid(4) + attr_num(2) + type_oid(4) + type_size(2)
            // + type_mod(4) + format_code(2) = 18 bytes.
            frame.extend_from_slice(&0u32.to_be_bytes()); // table_oid
            frame.extend_from_slice(&0i16.to_be_bytes()); // attr_num
            frame.extend_from_slice(&25u32.to_be_bytes()); // type_oid (TEXT)
            frame.extend_from_slice(&(-1i16).to_be_bytes()); // type_size
            frame.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
            frame.extend_from_slice(&fmt.to_be_bytes()); // format_code
        }
        let result = parse_row_description(&frame);
        assert!(result.is_ok(), "parse must succeed, got {result:?}");
        if let Ok(desc) = result {
            assert_eq!(desc.n_columns(), 2);
            assert_eq!(desc.format_code(0), Some(FormatCode::Text));
            assert_eq!(desc.format_code(1), Some(FormatCode::Binary));
            // Trailing slots default to Text via FormatCodeSet::empty.
            for idx in 2..MAX_ROW_COLUMNS {
                assert_eq!(desc.format_code(idx), None, "idx {idx} >= n_columns");
            }
        }
    }
}

#[cfg(test)]
mod parse_short_uint_swar_tests {
    //! Tier-3 closure for the `parse_short_uint_swar` helper.
    //!
    //! `Option::None` on invalid input is a runtime classification
    //! (cannot be hoisted to tier-1 — invalid bytes only exist on
    //! arbitrary network input). The exhaustive grid over all
    //! 0..=9999 valid values, combined with non-digit-byte sweeps
    //! at every position, pins the SWAR mask + Lemire validation
    //! semantics. A future refactor that broke the bit-trick (e.g.
    //! a wrong shift, a flipped polarity in `lo | hi`, an
    //! off-by-one place value) fails this table at test-time.
    //!
    //! Why no fuzz harness here: the input domain is exhaustively
    //! enumerable in O(10⁴) — full grid covers EVERY representable
    //! 4-digit input. Fuzz adds nothing beyond what the grid
    //! already proves. The non-digit-byte sweep extends coverage
    //! into the rejection class with byte-position parity.
    use super::*;
    use alloc::format;

    /// Exhaustive: every 4-digit value 0..=9999 round-trips,
    /// including all natural lengths (1-3 digits without the
    /// leading-zero pad).
    #[test]
    fn swar_short_uint_exhaustive_4digit_grid() {
        for v in 0..=9999u32 {
            let s = format!("{v:04}"); // "0000" .. "9999"
            assert_eq!(
                parse_short_uint_swar(s.as_bytes()),
                Some(v),
                "4-digit padded: {s:?}",
            );
        }
        // Natural lengths (1-3 digits without leading-zero pad).
        for v in 0..=9u32 {
            let s = format!("{v}");
            assert_eq!(
                parse_short_uint_swar(s.as_bytes()),
                Some(v),
                "1-digit: {s:?}",
            );
        }
        for v in 10..=99u32 {
            let s = format!("{v}");
            assert_eq!(
                parse_short_uint_swar(s.as_bytes()),
                Some(v),
                "2-digit: {s:?}",
            );
        }
        for v in 100..=999u32 {
            let s = format!("{v}");
            assert_eq!(
                parse_short_uint_swar(s.as_bytes()),
                Some(v),
                "3-digit: {s:?}",
            );
        }
    }

    /// Boundary length cases: empty input and over-cap input must
    /// reject. The slice-pattern wildcard arm in the implementation
    /// is what enforces the cap; a future refactor that flipped
    /// the cap (e.g. accepted len 5) would fail these.
    #[test]
    fn swar_short_uint_length_boundaries() {
        assert_eq!(parse_short_uint_swar(b""), None, "empty");
        assert_eq!(parse_short_uint_swar(b"12345"), None, "5-digit (over cap)");
        assert_eq!(parse_short_uint_swar(b"99999"), None, "5-digit max");
    }

    /// Sign-rejection: leading `-` or `+` must reject. Caller
    /// applies any sign separately. The Lemire mask catches these
    /// because `b'-'` (0x2D) and `b'+'` (0x2B) both fall below
    /// `b'0'` (0x30), driving the high bit on `hi = 0x39 - byte`.
    #[test]
    fn swar_short_uint_rejects_leading_sign() {
        assert_eq!(parse_short_uint_swar(b"-1"), None);
        assert_eq!(parse_short_uint_swar(b"-100"), None);
        assert_eq!(parse_short_uint_swar(b"+1"), None);
        assert_eq!(parse_short_uint_swar(b"+100"), None);
    }

    /// Non-digit bytes at every byte position must reject. The
    /// Lemire mask is symmetric across all four packed bytes;
    /// this sweep proves the rejection has no positional weakness
    /// (e.g. a misplaced shift would leave one position unchecked).
    #[test]
    fn swar_short_uint_rejects_invalid_bytes_per_position() {
        let invalid_bytes: &[u8] = &[
            0x00,
            0x2F, // '/' — one below b'0'
            0x3A, // ':' — one above b'9'
            b'a',
            b'A',
            0x7F,
            0x80,
            0xFF,
        ];
        for &bad in invalid_bytes {
            // Position 0 of len-1.
            assert_eq!(
                parse_short_uint_swar(&[bad]),
                None,
                "len-1 invalid byte {bad:#x}",
            );
            // Position 0 of len-2.
            assert_eq!(
                parse_short_uint_swar(&[bad, b'5']),
                None,
                "len-2 invalid first byte {bad:#x}",
            );
            // Position 1 of len-2.
            assert_eq!(
                parse_short_uint_swar(&[b'5', bad]),
                None,
                "len-2 invalid second byte {bad:#x}",
            );
            // Position 2 of len-4.
            assert_eq!(
                parse_short_uint_swar(&[b'1', b'2', bad, b'4']),
                None,
                "len-4 invalid third byte {bad:#x}",
            );
            // Position 3 of len-4.
            assert_eq!(
                parse_short_uint_swar(&[b'1', b'2', b'3', bad]),
                None,
                "len-4 invalid fourth byte {bad:#x}",
            );
        }
    }

    /// Boundary digit values: `b'0'` and `b'9'` at every length.
    /// Pins the inclusive-bound semantics of the Lemire mask
    /// (`b'0' - 0x30 = 0`, `0x39 - b'9' = 0` — both still in the
    /// 0..=9 range, no high bit set). An off-by-one in the mask
    /// constants (e.g. `0x39` → `0x38`) would break b'9' acceptance.
    #[test]
    fn swar_short_uint_boundary_digits() {
        assert_eq!(parse_short_uint_swar(b"0"), Some(0));
        assert_eq!(parse_short_uint_swar(b"9"), Some(9));
        assert_eq!(parse_short_uint_swar(b"00"), Some(0));
        assert_eq!(parse_short_uint_swar(b"99"), Some(99));
        assert_eq!(parse_short_uint_swar(b"0000"), Some(0));
        assert_eq!(parse_short_uint_swar(b"9999"), Some(9999));
    }
}

#[cfg(test)]
mod parse_long_uint_swar_tests {
    //! SWAR extension — tier-3 closure for
    //! `parse_long_uint_swar` (5-19 digit u64-range parser).
    //!
    //! Strategy mirrors `parse_short_uint_swar_tests`:
    //! - representative grids at each natural length 5..=19,
    //! - boundary length tests (rejects len 4 and len 20),
    //! - sign rejection (the Lemire mask catches `-` / `+`),
    //! - non-digit byte sweep at every position (chunk-boundary parity),
    //! - boundary digit values `b'0'` / `b'9'` at each length,
    //! - representative high-u64 values incl. above i64::MAX.
    use super::*;
    use alloc::format;
    use alloc::string::ToString;
    use alloc::vec;
    use alloc::vec::Vec;

    /// Length-5 boundary: smallest input the helper accepts. The
    /// upper variant `parse_short_uint_swar` caps at length 4; this
    /// helper picks up at length 5 with no gap.
    #[test]
    fn def_266_long_swar_len5_grid_sampled() {
        // Full 10_000..=99_999 grid (90k iterations) is fast in
        // release tests but slow in debug; sample every 173rd value
        // to hit the byte-position coverage cheaply.
        let mut step: u64 = 10_000;
        while step <= 99_999 {
            let s = step.to_string();
            assert_eq!(
                parse_long_uint_swar(s.as_bytes()),
                Some(step),
                "5-digit: {s}",
            );
            step = step.saturating_add(173);
        }
        // Boundary endpoints exactly.
        assert_eq!(parse_long_uint_swar(b"10000"), Some(10_000));
        assert_eq!(parse_long_uint_swar(b"99999"), Some(99_999));
    }

    /// Lengths 6..=18 spot-checked with characteristic powers of 10
    /// and "all 9s" patterns. Pins the chunk-boundary recombination
    /// (8/16 byte u64 splits at lengths 9 and 17).
    #[test]
    fn def_266_long_swar_lengths_6_to_18_spot_checked() {
        let cases: &[(&[u8], u64)] = &[
            (b"100000",                  100_000_u64),                  // len 6
            (b"999999",                  999_999_u64),                  // len 6
            (b"1000000",                 1_000_000_u64),                // len 7
            (b"9999999",                 9_999_999_u64),                // len 7
            (b"10000000",                10_000_000_u64),               // len 8 (chunk boundary)
            (b"99999999",                99_999_999_u64),               // len 8
            (b"100000000",               100_000_000_u64),              // len 9
            (b"999999999",               999_999_999_u64),              // len 9
            (b"1000000000",              1_000_000_000_u64),            // len 10
            (b"9999999999",              9_999_999_999_u64),            // len 10
            (b"99999999999",             99_999_999_999_u64),           // len 11
            (b"999999999999",            999_999_999_999_u64),          // len 12
            (b"9999999999999",           9_999_999_999_999_u64),        // len 13
            (b"99999999999999",          99_999_999_999_999_u64),       // len 14
            (b"999999999999999",         999_999_999_999_999_u64),      // len 15
            (b"9999999999999999",        9_999_999_999_999_999_u64),    // len 16 (chunk boundary)
            (b"99999999999999999",       99_999_999_999_999_999_u64),   // len 17
            (b"999999999999999999",      999_999_999_999_999_999_u64),  // len 18
        ];
        for &(input, expected) in cases {
            assert_eq!(
                parse_long_uint_swar(input),
                Some(expected),
                // `String::from_utf8_lossy` (alloc) returns
                // `Cow<'_, str>` (Borrowed for valid UTF-8, no
                // alloc; Owned with `U+FFFD` substitution for
                // invalid bytes — stronger diagnostic preservation
                // than a naive `from_utf8(input).unwrap_or("?")`
                // fallback that would silently collapse all
                // non-ASCII to a literal `"?"`).
                // `core::str::from_utf8_lossy` doesn't exist
                // (`from_utf8_lossy` is alloc-only), so we go
                // through `alloc::string::String`.
                "input: {:?}",
                alloc::string::String::from_utf8_lossy(input),
            );
        }
    }

    /// Length-19 boundary: highest accepted length. Verifies `u64`
    /// headroom above `i64::MAX` is reachable (callers needing i64
    /// must verify range AFTER success).
    #[test]
    fn def_266_long_swar_len19_boundary() {
        // u64::MAX = 18_446_744_073_709_551_615 (20 digits) — rejected.
        // i64::MAX =  9_223_372_036_854_775_807 (19 digits) — accepted.
        // 19-nines  =  9_999_999_999_999_999_999             — accepted, > i64::MAX.
        assert_eq!(
            parse_long_uint_swar(b"9223372036854775807"),
            Some(9_223_372_036_854_775_807_u64),
            "i64::MAX",
        );
        assert_eq!(
            parse_long_uint_swar(b"9999999999999999999"),
            Some(9_999_999_999_999_999_999_u64),
            "19-nines (> i64::MAX, fits u64)",
        );
        assert_eq!(
            parse_long_uint_swar(b"1000000000000000000"),
            Some(1_000_000_000_000_000_000_u64),
            "10^18",
        );
    }

    /// Length boundaries: lengths outside `5..=19` must reject.
    #[test]
    fn def_266_long_swar_length_boundaries() {
        // Lengths below 5 (caller should use parse_short_uint_swar).
        assert_eq!(parse_long_uint_swar(b""), None);
        assert_eq!(parse_long_uint_swar(b"1"), None);
        assert_eq!(parse_long_uint_swar(b"12"), None);
        assert_eq!(parse_long_uint_swar(b"123"), None);
        assert_eq!(parse_long_uint_swar(b"1234"), None);
        // Length 20+ (exceeds u64 representability for safe overflow-free path).
        let twenty = b"99999999999999999999"; // 20 nines
        assert_eq!(parse_long_uint_swar(twenty), None);
        let twenty_one: Vec<u8> = vec![b'1'; 21];
        assert_eq!(parse_long_uint_swar(&twenty_one), None);
    }

    /// Sign rejection (mirror of short variant test). `b'-'` (0x2D)
    /// and `b'+'` (0x2B) sit below `b'0'` (0x30); the Lemire mask's
    /// `lo` term goes negative → high bit set → rejected.
    #[test]
    fn def_266_long_swar_rejects_leading_sign() {
        assert_eq!(parse_long_uint_swar(b"-10000"), None);
        assert_eq!(parse_long_uint_swar(b"+10000"), None);
        assert_eq!(parse_long_uint_swar(b"-9999999999"), None);
        assert_eq!(parse_long_uint_swar(b"+9999999999"), None);
    }

    /// Non-digit byte at every position within and across u64
    /// chunk boundaries (positions 0, 7, 8, 15, 16, 18 in a len-19
    /// input). Pins the chunked Lemire OR — a bug in chunk
    /// composition would let one position slip through.
    #[test]
    fn def_266_long_swar_rejects_invalid_bytes_per_position() {
        let invalid_bytes: &[u8] = &[
            0x00, 0x2F, // '/' — one below b'0'
            0x3A, // ':' — one above b'9'
            b'a', b'A', 0x7F, 0x80, 0xFF,
        ];
        // Construct len-19 inputs with the bad byte at each position.
        for &bad in invalid_bytes {
            for pos in 0..19usize {
                let mut buf: Vec<u8> = vec![b'5'; 19];
                if let Some(slot) = buf.get_mut(pos) {
                    *slot = bad;
                }
                assert_eq!(
                    parse_long_uint_swar(&buf),
                    None,
                    "len-19 bad byte {bad:#x} at pos {pos}",
                );
            }
        }
        // Position-0 of len-5 specifically (smallest valid length).
        for &bad in invalid_bytes {
            assert_eq!(
                parse_long_uint_swar(&[bad, b'1', b'2', b'3', b'4']),
                None,
                "len-5 bad byte {bad:#x} at pos 0",
            );
        }
    }

    /// Boundary digit values `b'0'` and `b'9'` at every length 5..=19.
    /// An off-by-one in the SWAR mask constants would break the
    /// inclusive-range semantic at one of these endpoints.
    #[test]
    fn def_266_long_swar_boundary_digits() {
        // Compile-time table of explicit `(len, all-nines-value)`
        // tuples — no `unwrap_or`, no `pow`, no `try_from`; each
        // case is a literal pin that a future digit-grouping
        // refactor cannot drift past. A naive
        // `(10_u64).pow(u32::try_from(len).unwrap_or(0)).saturating_sub(1)`
        // form would stack three layered fallbacks (try_from
        // usize→u32 Err, pow overflow, sub overflow) where every
        // Err arm is architecturally dead at len ≤ 19 but the call
        // sequence obscures the intent.
        let cases: &[(usize, u64)] = &[
            (5, 99_999),
            (6, 999_999),
            (7, 9_999_999),
            (8, 99_999_999),
            (9, 999_999_999),
            (10, 9_999_999_999),
            (11, 99_999_999_999),
            (12, 999_999_999_999),
            (13, 9_999_999_999_999),
            (14, 99_999_999_999_999),
            (15, 999_999_999_999_999),
            (16, 9_999_999_999_999_999),
            (17, 99_999_999_999_999_999),
            (18, 999_999_999_999_999_999),
            (19, 9_999_999_999_999_999_999),
        ];
        for &(len, expected) in cases {
            // All-zeros input is valid; value is 0 regardless of length.
            let zeros = "0".repeat(len);
            assert_eq!(
                parse_long_uint_swar(zeros.as_bytes()),
                Some(0),
                "len-{len} all zeros",
            );
            // All-nines input is valid; value = 10^len - 1.
            let nines = "9".repeat(len);
            assert_eq!(
                parse_long_uint_swar(nines.as_bytes()),
                Some(expected),
                "len-{len} all nines",
            );
        }
    }

    /// Leading-zero forms are accepted (the helper is value-equivalent;
    /// length-padded forms are also legal ASCII-decimal).
    #[test]
    fn def_266_long_swar_leading_zeros_accepted() {
        assert_eq!(parse_long_uint_swar(b"00001"), Some(1));
        assert_eq!(parse_long_uint_swar(b"0000000000000000001"), Some(1));
        assert_eq!(
            parse_long_uint_swar(b"0000000000000000099"),
            Some(99),
            "len-19 with leading zeros, value 99",
        );
    }

    /// Empty input behaviour matches the short variant.
    #[test]
    fn def_266_long_swar_empty_input_rejected() {
        assert_eq!(parse_long_uint_swar(b""), None);
    }

    /// Whitespace-padded input MUST reject (the helper requires
    /// clean digit bytes; trimming is caller responsibility).
    #[test]
    fn def_266_long_swar_rejects_whitespace() {
        assert_eq!(parse_long_uint_swar(b" 12345"), None);
        assert_eq!(parse_long_uint_swar(b"12345 "), None);
        assert_eq!(parse_long_uint_swar(b"\t12345"), None);
        assert_eq!(parse_long_uint_swar(b"123 4567"), None); // mid-string space
    }

    /// Random spot-check of u64 values up to ~10^18 to pin the
    /// recombination math beyond the boundary patterns above.
    #[test]
    fn def_266_long_swar_random_round_trip() {
        let cases: &[u64] = &[
            12_345_u64,
            999_999_999_u64,
            1_234_567_890_u64,
            42_000_000_000_u64,
            777_777_777_777_u64,
            1_000_000_000_000_000_u64,
            500_500_500_500_500_u64,
            9_223_372_036_854_775_807_u64,            // i64::MAX
            9_223_372_036_854_775_808_u64,            // i64::MAX + 1, fits u64
        ];
        for &v in cases {
            let s = format!("{v}");
            if s.len() >= 5 && s.len() <= 19 {
                assert_eq!(
                    parse_long_uint_swar(s.as_bytes()),
                    Some(v),
                    "round-trip {v}",
                );
            }
        }
    }
}

#[cfg(test)]
mod validate_utf8_swar_tests {
    //! SWAR extension — tier-3 closure for
    //! `validate_utf8_swar` (all-ASCII fast-path detector).
    //!
    //! The helper's contract is one-sided: `Some(())` ⇒ pure ASCII
    //! (thus valid UTF-8). `None` ⇒ at least one high-bit byte,
    //! caller defers to a full UTF-8 validator. Tests pin:
    //! - boundary ASCII values `b'\x7F'` (accepted, highest ASCII)
    //!   and `b'\x80'` (rejected, lowest non-ASCII),
    //! - chunk-boundary parity: high-bit byte at positions 0..15
    //!   across the 8-byte chunk + tail split,
    //! - empty-input behaviour,
    //! - representative long ASCII string acceptance.
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;

    /// Empty input is trivially all-ASCII.
    #[test]
    fn def_266_validate_utf8_empty_input_accepts() {
        assert_eq!(validate_utf8_swar(b""), Some(()));
    }

    /// All bytes `0x00..=0x7F` are ASCII; representative spot-checks
    /// at every length 1..=24 (covers ≥ 3 full 8-byte chunks).
    #[test]
    fn def_266_validate_utf8_pure_ascii_accepts_all_lengths() {
        for len in 1..=24usize {
            let buf: Vec<u8> = vec![b'A'; len];
            assert_eq!(
                validate_utf8_swar(&buf),
                Some(()),
                "len {len} all-ASCII 'A's",
            );
        }
        // Long-ASCII spot-check (realistic column-name length).
        assert_eq!(
            validate_utf8_swar(b"alice@example.com"),
            Some(()),
            "17-byte ASCII",
        );
        assert_eq!(
            validate_utf8_swar(b"the quick brown fox jumps over the lazy dog"),
            Some(()),
            "43-byte ASCII",
        );
    }

    /// Boundary byte value: `0x7F` is highest ASCII (accepted),
    /// `0x80` is first non-ASCII (rejected). Test both at every
    /// byte position within and across chunk boundaries.
    #[test]
    fn def_266_validate_utf8_boundary_bytes() {
        // 0x7F accepted at any position.
        for pos in 0..16usize {
            let mut buf: Vec<u8> = vec![b'A'; 16];
            if let Some(slot) = buf.get_mut(pos) {
                *slot = 0x7F;
            }
            assert_eq!(
                validate_utf8_swar(&buf),
                Some(()),
                "len-16 0x7F at pos {pos}",
            );
        }
        // 0x80 rejected at any position.
        for pos in 0..16usize {
            let mut buf: Vec<u8> = vec![b'A'; 16];
            if let Some(slot) = buf.get_mut(pos) {
                *slot = 0x80;
            }
            assert_eq!(
                validate_utf8_swar(&buf),
                None,
                "len-16 0x80 at pos {pos}",
            );
        }
    }

    /// Chunk-boundary parity: high-bit byte placed at positions 7
    /// (last byte of first chunk), 8 (first byte of second chunk),
    /// and 15 (last byte of second chunk).
    #[test]
    fn def_266_validate_utf8_chunk_boundary_rejection() {
        let mut buf = [b'A'; 16];
        buf[7] = 0xFF;
        assert_eq!(validate_utf8_swar(&buf), None, "0xFF at pos 7");
        let mut buf = [b'A'; 16];
        buf[8] = 0xFF;
        assert_eq!(validate_utf8_swar(&buf), None, "0xFF at pos 8");
        let mut buf = [b'A'; 16];
        buf[15] = 0xFF;
        assert_eq!(validate_utf8_swar(&buf), None, "0xFF at pos 15");
    }

    /// Tail bytes (length not multiple of 8) are processed bytewise.
    /// Pin the tail path with high-bit bytes at every tail position.
    #[test]
    fn def_266_validate_utf8_tail_handling() {
        // len 1..=7: only tail, no full chunk.
        for len in 1..=7usize {
            for pos in 0..len {
                let mut buf: Vec<u8> = vec![b'A'; len];
                if let Some(slot) = buf.get_mut(pos) {
                    *slot = 0x80;
                }
                assert_eq!(
                    validate_utf8_swar(&buf),
                    None,
                    "tail-only len {len} 0x80 at pos {pos}",
                );
            }
        }
        // len 9..=15: one chunk + tail. High bit in tail.
        for tail_len in 1..=7usize {
            let total = 8 + tail_len;
            let mut buf: Vec<u8> = vec![b'A'; total];
            if let Some(slot) = buf.get_mut(total.saturating_sub(1)) {
                *slot = 0xC0;
            }
            assert_eq!(
                validate_utf8_swar(&buf),
                None,
                "chunk+tail len {total} 0xC0 at last byte",
            );
        }
    }

    /// Legitimate multi-byte UTF-8 (e.g. Cyrillic) is correctly
    /// classified as "not pure ASCII" — caller MUST then use
    /// `simdutf8::basic::from_utf8` for true UTF-8 validation.
    /// This pins the contract: a `None` return is NOT a "invalid
    /// UTF-8" verdict; it is a "fast-path miss, defer to full
    /// validator" signal.
    #[test]
    fn def_266_validate_utf8_multibyte_signals_fast_path_miss() {
        // "Привет" (Cyrillic) — valid UTF-8 but contains 0xD0/0xD1
        // continuation lead bytes. Helper returns None correctly.
        assert_eq!(
            validate_utf8_swar("Привет".as_bytes()),
            None,
            "Cyrillic 'Привет' — multi-byte UTF-8, fast-path miss",
        );
        assert_eq!(
            validate_utf8_swar("日本語".as_bytes()),
            None,
            "Japanese — multi-byte UTF-8, fast-path miss",
        );
    }
}

#[cfg(test)]
mod parse_pg_bool_swar_tests {
    //! SWAR extension — tier-3 closure for
    //! `parse_pg_bool_swar` (4-form PG boolean cache-hit parser).
    //!
    //! Exhaustive — there are exactly four accepted shapes.
    //! Rejection-side covered with realistic miss cases.
    use super::*;

    /// All four accepted forms map to the correct value.
    #[test]
    fn def_266_pg_bool_swar_accepted_forms() {
        assert_eq!(parse_pg_bool_swar(b"t"), Some(true));
        assert_eq!(parse_pg_bool_swar(b"f"), Some(false));
        assert_eq!(parse_pg_bool_swar(b"true"), Some(true));
        assert_eq!(parse_pg_bool_swar(b"false"), Some(false));
    }

    /// Empty input must reject.
    #[test]
    fn def_266_pg_bool_swar_rejects_empty() {
        assert_eq!(parse_pg_bool_swar(b""), None);
    }

    /// Uppercase forms must reject (the helper is case-sensitive;
    /// PG SELECT output is always lowercase `t` / `f`).
    #[test]
    fn def_266_pg_bool_swar_rejects_uppercase() {
        assert_eq!(parse_pg_bool_swar(b"T"), None);
        assert_eq!(parse_pg_bool_swar(b"F"), None);
        assert_eq!(parse_pg_bool_swar(b"True"), None);
        assert_eq!(parse_pg_bool_swar(b"TRUE"), None);
        assert_eq!(parse_pg_bool_swar(b"False"), None);
        assert_eq!(parse_pg_bool_swar(b"FALSE"), None);
    }

    /// Single-byte non-bool literals reject.
    #[test]
    fn def_266_pg_bool_swar_rejects_other_single_bytes() {
        for byte in u8::MIN..=u8::MAX {
            if byte == b't' || byte == b'f' {
                continue;
            }
            assert_eq!(
                parse_pg_bool_swar(&[byte]),
                None,
                "single byte {byte:#x}",
            );
        }
    }

    /// Wrong-length 2-3 byte inputs reject.
    #[test]
    fn def_266_pg_bool_swar_rejects_wrong_length() {
        assert_eq!(parse_pg_bool_swar(b"tr"), None);
        assert_eq!(parse_pg_bool_swar(b"tru"), None);
        assert_eq!(parse_pg_bool_swar(b"fa"), None);
        assert_eq!(parse_pg_bool_swar(b"fal"), None);
        assert_eq!(parse_pg_bool_swar(b"fals"), None);
    }

    /// 4-byte non-"true" inputs reject (pins the slice-pattern
    /// constancy of the b"true" arm).
    #[test]
    fn def_266_pg_bool_swar_rejects_other_4_byte() {
        assert_eq!(parse_pg_bool_swar(b"trxe"), None);
        assert_eq!(parse_pg_bool_swar(b"trux"), None);
        assert_eq!(parse_pg_bool_swar(b"xrue"), None);
        assert_eq!(parse_pg_bool_swar(b"abcd"), None);
    }

    /// 5-byte non-"false" inputs reject.
    #[test]
    fn def_266_pg_bool_swar_rejects_other_5_byte() {
        assert_eq!(parse_pg_bool_swar(b"falsx"), None);
        assert_eq!(parse_pg_bool_swar(b"falxe"), None);
        assert_eq!(parse_pg_bool_swar(b"xalse"), None);
        assert_eq!(parse_pg_bool_swar(b"abcde"), None);
    }

    /// 6+ byte inputs reject regardless of content.
    #[test]
    fn def_266_pg_bool_swar_rejects_overlong() {
        assert_eq!(parse_pg_bool_swar(b"true "), None); // trailing space
        assert_eq!(parse_pg_bool_swar(b" true"), None); // leading space
        assert_eq!(parse_pg_bool_swar(b"truex"), None);
        assert_eq!(parse_pg_bool_swar(b"falses"), None);
    }
}

#[cfg(test)]
mod decode_format_tests {
    //! Type-level (T, F) pair dispatch via the generic-F
    //! `DecodeFormat<F>` trait.
    //!
    //! Tests cover:
    //! - each `(T, F)` pair round-trips correctly (12 cases:
    //!   6 primitive types × 2 format markers),
    //! - OID consistency between `DecodeFormat<F>::OID` and the
    //!   corresponding `FromPgText::OID` / `FromPgBinary::OID`
    //!   (additional to the compile-time const-asserts above; these
    //!   runtime checks pin the assert blocks against accidental
    //!   removal),
    //! - `FormatCodeMarker::WIRE` produces correct `FormatCode`,
    //! - `decode_with_format` dispatches the right impl on a
    //!   runtime `FormatCode`.
    use super::*;

    #[test]
    fn markers_wire_consts() {
        assert_eq!(<TextFmt as FormatCodeMarker>::WIRE, FormatCode::Text);
        assert_eq!(<BinaryFmt as FormatCodeMarker>::WIRE, FormatCode::Binary);
    }

    #[test]
    fn text_round_trips() {
        assert_eq!(<i16 as DecodeFormat<TextFmt>>::decode(b"42"), Ok(42_i16));
        assert_eq!(<i32 as DecodeFormat<TextFmt>>::decode(b"-1234567"), Ok(-1_234_567_i32));
        assert_eq!(<i64 as DecodeFormat<TextFmt>>::decode(b"9223372036854775807"), Ok(9_223_372_036_854_775_807_i64));
        assert_eq!(<u32 as DecodeFormat<TextFmt>>::decode(b"4294967295"), Ok(u32::MAX));
        assert_eq!(<bool as DecodeFormat<TextFmt>>::decode(b"t"), Ok(true));
        assert_eq!(<bool as DecodeFormat<TextFmt>>::decode(b"f"), Ok(false));
        assert_eq!(<&str as DecodeFormat<TextFmt>>::decode(b"hello"), Ok("hello"));
    }

    #[test]
    fn binary_round_trips() {
        assert_eq!(<i16 as DecodeFormat<BinaryFmt>>::decode(&42_i16.to_be_bytes()), Ok(42_i16));
        assert_eq!(<i32 as DecodeFormat<BinaryFmt>>::decode(&(-1_234_567_i32).to_be_bytes()), Ok(-1_234_567_i32));
        assert_eq!(<i64 as DecodeFormat<BinaryFmt>>::decode(&i64::MAX.to_be_bytes()), Ok(i64::MAX));
        assert_eq!(<u32 as DecodeFormat<BinaryFmt>>::decode(&u32::MAX.to_be_bytes()), Ok(u32::MAX));
        assert_eq!(<bool as DecodeFormat<BinaryFmt>>::decode(&[1]), Ok(true));
        assert_eq!(<bool as DecodeFormat<BinaryFmt>>::decode(&[0]), Ok(false));
        assert_eq!(<&str as DecodeFormat<BinaryFmt>>::decode(b"hello"), Ok("hello"));
    }

    #[test]
    fn oid_consistency_text() {
        // Runtime double-check of the compile-time const-asserts.
        // Removing the assert block would not be caught by compile,
        // but THIS test would still fail.
        assert_eq!(<i16 as DecodeFormat<TextFmt>>::OID, <i16 as FromPgText>::OID);
        assert_eq!(<i32 as DecodeFormat<TextFmt>>::OID, <i32 as FromPgText>::OID);
        assert_eq!(<i64 as DecodeFormat<TextFmt>>::OID, <i64 as FromPgText>::OID);
        assert_eq!(<u32 as DecodeFormat<TextFmt>>::OID, <u32 as FromPgText>::OID);
        assert_eq!(<bool as DecodeFormat<TextFmt>>::OID, <bool as FromPgText>::OID);
        assert_eq!(<&str as DecodeFormat<TextFmt>>::OID, <&str as FromPgText>::OID);
    }

    #[test]
    fn oid_consistency_binary() {
        assert_eq!(<i16 as DecodeFormat<BinaryFmt>>::OID, <i16 as FromPgBinary>::OID);
        assert_eq!(<i32 as DecodeFormat<BinaryFmt>>::OID, <i32 as FromPgBinary>::OID);
        assert_eq!(<i64 as DecodeFormat<BinaryFmt>>::OID, <i64 as FromPgBinary>::OID);
        assert_eq!(<u32 as DecodeFormat<BinaryFmt>>::OID, <u32 as FromPgBinary>::OID);
        assert_eq!(<bool as DecodeFormat<BinaryFmt>>::OID, <bool as FromPgBinary>::OID);
        assert_eq!(<&str as DecodeFormat<BinaryFmt>>::OID, <&str as FromPgBinary>::OID);
    }

    #[test]
    fn oid_text_binary_symmetry() {
        // Same Rust type → same PG type OID across text/binary.
        // (Already const-asserted on the legacy FromPgText/FromPgBinary
        // pair; mirrored here on the new DecodeFormat surface for
        // explicit runtime drift detection.)
        assert_eq!(
            <i16 as DecodeFormat<TextFmt>>::OID,
            <i16 as DecodeFormat<BinaryFmt>>::OID,
            "i16 OID skew between text and binary DecodeFormat impls",
        );
        assert_eq!(
            <i32 as DecodeFormat<TextFmt>>::OID,
            <i32 as DecodeFormat<BinaryFmt>>::OID,
        );
        assert_eq!(
            <i64 as DecodeFormat<TextFmt>>::OID,
            <i64 as DecodeFormat<BinaryFmt>>::OID,
        );
        assert_eq!(
            <u32 as DecodeFormat<TextFmt>>::OID,
            <u32 as DecodeFormat<BinaryFmt>>::OID,
        );
        assert_eq!(
            <bool as DecodeFormat<TextFmt>>::OID,
            <bool as DecodeFormat<BinaryFmt>>::OID,
        );
        assert_eq!(
            <&str as DecodeFormat<TextFmt>>::OID,
            <&str as DecodeFormat<BinaryFmt>>::OID,
        );
    }

    #[test]
    fn decode_with_format_dispatches_correctly() {
        // Text-side dispatch.
        let v: i32 = decode_with_format(b"42", FormatCode::Text).unwrap_or(0);
        assert_eq!(v, 42);
        // Binary-side dispatch.
        let v: i32 = decode_with_format(&42_i32.to_be_bytes(), FormatCode::Binary).unwrap_or(0);
        assert_eq!(v, 42);
        // bool — both formats.
        let v: bool = decode_with_format(b"t", FormatCode::Text).unwrap_or(false);
        assert!(v);
        let v: bool = decode_with_format(&[1], FormatCode::Binary).unwrap_or(false);
        assert!(v);
        // &str — both formats (text and binary are byte-equivalent for &str).
        let v: &str = decode_with_format(b"hello", FormatCode::Text).unwrap_or("");
        assert_eq!(v, "hello");
        let v: &str = decode_with_format(b"hello", FormatCode::Binary).unwrap_or("");
        assert_eq!(v, "hello");
    }

    #[test]
    fn decode_with_format_propagates_errors() {
        // Invalid text bool — `from_pg_text` returns `BoolParse`.
        let r: Result<bool, _> = decode_with_format(b"yes", FormatCode::Text);
        assert!(matches!(r, Err(DecodeError::BoolParse)));
        // Invalid binary i32 (wrong length) — `BinaryLengthMismatch`.
        let r: Result<i32, _> = decode_with_format(&[0, 1, 2], FormatCode::Binary);
        assert!(matches!(r, Err(DecodeError::BinaryLengthMismatch { expected_len: 4, .. })));
    }

    #[test]
    fn marker_zero_sized() {
        // ZST property — markers carry zero runtime cost.
        assert_eq!(core::mem::size_of::<TextFmt>(), 0);
        assert_eq!(core::mem::size_of::<BinaryFmt>(), 0);
    }
}

#[cfg(test)]
mod session_2025_05_25_tests {
    use super::*;

    #[test]
    fn u32_from_pg_text_common_value_zero() {
        assert!(matches!(<u32 as FromPgText>::from_pg_text(b"0"), Ok(0)));
    }

    #[test]
    fn u32_from_pg_text_common_value_one() {
        assert!(matches!(<u32 as FromPgText>::from_pg_text(b"1"), Ok(1)));
    }

    #[test]
    fn u32_from_pg_text_regular_value() {
        assert!(matches!(<u32 as FromPgText>::from_pg_text(b"42"), Ok(42)));
    }

    #[test]
    fn str_from_pg_binary_valid_utf8() {
        assert!(matches!(<&str as FromPgBinary>::from_pg_binary(b"hello"), Ok("hello")));
    }

    #[test]
    fn str_from_pg_binary_invalid_utf8() {
        let bytes: &[u8] = &[0xFF, 0xFE];
        assert!(<&str as FromPgBinary>::from_pg_binary(bytes).is_err());
    }
}

#[cfg(test)]
mod rowdesc_box_tests {
    use super::*;

    #[test]
    fn empty_rowdesc() {
        let rd = RowDesc::empty();
        assert_eq!(rd.len(), 0);
        assert!(rd.is_empty());
        assert!(rd.type_oid(0).is_none());
        assert!(rd.format_code(0).is_none());
    }

    #[test]
    fn five_column_round_trip() {
        let oids: &[u32] = &[23, 25, 16, 20, 701];
        let formats: &[FormatCode] = &[
            FormatCode::Text,
            FormatCode::Binary,
            FormatCode::Text,
            FormatCode::Text,
            FormatCode::Binary,
        ];
        let rd = RowDesc::from_parts(oids, formats);
        assert!(rd.is_ok());
        let rd = match rd { Ok(r) => r, Err(_) => return };
        assert_eq!(rd.len(), 5);
        assert!(matches!(rd.type_oid(0), Some(23)));
        assert!(matches!(rd.type_oid(4), Some(701)));
        assert!(matches!(rd.format_code(0), Some(FormatCode::Text)));
        assert!(matches!(rd.format_code(1), Some(FormatCode::Binary)));
        assert!(matches!(rd.format_code(4), Some(FormatCode::Binary)));
        assert!(rd.type_oid(5).is_none());
    }

    #[test]
    fn wide_table_50_columns() {
        let oids: alloc::vec::Vec<u32> = (1..=50).collect();
        let formats: alloc::vec::Vec<FormatCode> =
            (0..50).map(|i| if i & 1 == 0 { FormatCode::Text } else { FormatCode::Binary }).collect();
        let rd = RowDesc::from_parts(&oids, &formats);
        assert!(rd.is_ok());
        let rd = match rd { Ok(r) => r, Err(_) => return };
        assert_eq!(rd.len(), 50);
        assert!(matches!(rd.type_oid(49), Some(50)));
        assert!(matches!(rd.format_code(0), Some(FormatCode::Text)));
        assert!(matches!(rd.format_code(1), Some(FormatCode::Binary)));
        assert!(matches!(rd.format_code(33), Some(FormatCode::Binary)));
        assert!(matches!(rd.format_code(32), Some(FormatCode::Text)));
    }

    #[test]
    fn static_oids_all_text() {
        let oids: &[u32] = &[23, 25, 16];
        let rd = RowDesc::from_static_oids_text_format(oids);
        assert!(rd.is_ok());
        let rd = match rd { Ok(r) => r, Err(_) => return };
        assert_eq!(rd.len(), 3);
        assert!(matches!(rd.format_code(0), Some(FormatCode::Text)));
        assert!(matches!(rd.format_code(1), Some(FormatCode::Text)));
        assert!(matches!(rd.format_code(2), Some(FormatCode::Text)));
    }
}

#[cfg(test)]
mod parse_edge_case_tests {
    use super::*;

    #[test]
    fn parse_row_description_zero_columns() {
        let payload: &[u8] = &[0, 0]; // i16 BE = 0
        let result = parse_row_description(payload);
        assert!(result.is_ok());
        let rd = match result { Ok(r) => r, Err(_) => return };
        assert_eq!(rd.len(), 0);
        assert!(rd.is_empty());
    }

    #[test]
    fn parse_row_description_one_column() {
        let mut payload = alloc::vec::Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        payload.push(b'x'); // column name
        payload.push(0);    // NUL terminator
        payload.extend_from_slice(&0i32.to_be_bytes()); // table_oid
        payload.extend_from_slice(&0i16.to_be_bytes()); // attr_num
        payload.extend_from_slice(&23i32.to_be_bytes()); // type_oid = int4
        payload.extend_from_slice(&4i16.to_be_bytes());  // type_size
        payload.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
        payload.extend_from_slice(&0i16.to_be_bytes());  // format = text
        let result = parse_row_description(&payload);
        assert!(result.is_ok());
        let rd = match result { Ok(r) => r, Err(_) => return };
        assert_eq!(rd.len(), 1);
        assert!(matches!(rd.type_oid(0), Some(23)));
        assert!(matches!(rd.format_code(0), Some(FormatCode::Text)));
    }

    #[test]
    fn parse_row_description_negative_count_rejected() {
        let payload: &[u8] = &[0xFF, 0xFF]; // i16 BE = -1
        assert!(parse_row_description(payload).is_err());
    }

    #[test]
    fn parse_parameter_description_zero_params() {
        let payload: &[u8] = &[0, 0]; // i16 BE = 0
        let result = parse_parameter_description(payload);
        assert!(result.is_ok());
        let po = match result { Ok(p) => p, Err(_) => return };
        assert_eq!(po.len(), 0);
        assert!(po.is_empty());
    }

    #[test]
    fn parse_parameter_description_three_params() {
        let mut payload = alloc::vec::Vec::new();
        payload.extend_from_slice(&3i16.to_be_bytes());
        payload.extend_from_slice(&23u32.to_be_bytes());  // int4
        payload.extend_from_slice(&25u32.to_be_bytes());  // text
        payload.extend_from_slice(&16u32.to_be_bytes());  // bool
        let result = parse_parameter_description(&payload);
        assert!(result.is_ok());
        let po = match result { Ok(p) => p, Err(_) => return };
        assert_eq!(po.len(), 3);
        assert!(matches!(po.get(0), Some(23)));
        assert!(matches!(po.get(1), Some(25)));
        assert!(matches!(po.get(2), Some(16)));
    }
}
