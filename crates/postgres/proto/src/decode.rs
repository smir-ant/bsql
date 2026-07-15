//! Row-schema + row-body decoding primitives.
//!
//! `bsql-pg-proto` owns the raw wire encoding of a result-set: the
//! `RowDescription` frame tells us column count, type OIDs, and
//! per-column format codes; each `DataRow` frame carries the column
//! values. This module parses `RowDescription` into [`RowDesc`] (the
//! per-statement schema the active engine holds and surfaces to the
//! driver at each command boundary) and hosts the typed-decoder
//! primitives that materialise column bytes into Rust types.
//!
//! # Storage
//!
//! The crate is `no_std + alloc`. [`RowDesc`] is a SINGLE exact-size heap
//! allocation — a `Box<[u32]>` packing the column count, per-column type OIDs,
//! and a format-code bitset (not a fixed inline `[ColumnDesc; N]` array), so its
//! handle is a fat pointer regardless of column count. Result-sets with more than
//! [`MAX_ROW_COLUMNS`] columns are classified
//! [`crate::ProtocolError::TooManyColumns`] at parse time (no silent truncation);
//! see that variant for how the driver RECOVERS from an over-cap result rather
//! than tearing the connection down.
//!
//! # Tier notes
//!
//! Schema ingest is **tier-2 structural**. The parser produces `RowDesc` only on
//! well-formed payloads: a MALFORMED frame is `MalformedRowDescription` (framing
//! errors) or `UnexpectedFormatCode` (a value outside `{0, 1}`) and tears the
//! connection down (framing desync), while a well-formed but too-wide frame is
//! the RECOVERABLE `TooManyColumns` the driver drains from.
//!
//! Schema access is lifetime-scoped: the active engine hands the driver a
//! `RowDesc` / column view borrowed from its per-statement state, consumed at the
//! command boundary before the next pull resets it — a stale schema cannot be
//! read after the protocol advances to a new query.

use core::fmt;

use crate::pgtypes::{Date, Interval, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid};

/// Maximum columns per result-set. Queries returning more columns
/// classify as [`crate::ProtocolError::TooManyColumns`] — the
/// connection stays alive (recoverable), the user retries with a
/// narrower projection.
///
/// `1664` is PostgreSQL's `MaxTupleAttributeNumber` — the hard limit on the
/// number of entries in a target list (a query RESULT), which is exactly what a
/// `RowDescription` describes. PG accepts a 1664-column result and errors only at
/// 1665 (`target lists can have at most 1664 entries`), so this is the true
/// projection ceiling. It is NOT `MaxHeapAttributeNumber` (`1600`) — that is a
/// *table*'s column cap, a lower and different limit; a result set (joins,
/// computed columns, multiple tables) can legitimately be wider than any single
/// table. Since `RowDesc` is heap-allocated (`Box<[u32]>`, exact-size), this
/// constant only sets the parse-time rejection threshold — not storage.
///
/// # Effective wire limit
///
/// A `RowDescription` for more than ~140 typical columns exceeds `READ_BUF_CAP`
/// (4096 B) and enters the Sub-C oversize path, which ACCUMULATES the whole body
/// into a growable buffer (bounded by `MAX_ROW_DESC_ACCUM` = 1 MiB) and parses it
/// in full — so the effective wire maximum is `MAX_ROW_COLUMNS`, not a smaller
/// prefix-truncation limit. A worst-case 1664-column frame (~136 KiB) clears the
/// 1 MiB ceiling with wide margin.
pub const MAX_ROW_COLUMNS: usize = 1664;

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

// Footprint pin: a single `Box<[u32]>` (fat pointer = ptr + len). The whole
// column schema lives in one heap allocation; this pin keeps the handle a fat
// pointer and catches a field addition that would widen every prepared
// statement and row-stream that holds a RowDesc.
crate::wire_pin!(RowDesc, size = 16, align = 8);

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
// `pub` (not `pub(crate)`) so the `decoder_fuzz` total-function gate in
// `bsql-postgres-core` can drive it directly over the broad untrusted-byte sweep,
// exactly as it fuzzes the already-`pub` `parse_column_names` sibling below — both
// parse the same untrusted server `RowDescription` (`'T'`) payload. Visibility
// only; the parsing logic is untouched.
#[cold]
pub fn parse_row_description(
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
///
/// A non-UTF-8 column name is wire-malformed: the name feeds by-name lookups,
/// so a lossy substitution would silently corrupt the identifier. It is
/// classified as [`crate::error::ProtocolError::MalformedRowDescription`]
/// instead of being rewritten with replacement characters.
pub fn parse_column_names(
    payload: &[u8],
) -> Result<alloc::vec::Vec<alloc::string::String>, crate::error::ProtocolError> {
    let malformed = || crate::error::ProtocolError::MalformedRowDescription {
        payload_len: payload.len(),
    };
    let Some((count_bytes, mut rest)) = payload.split_first_chunk::<2>() else {
        return Ok(alloc::vec::Vec::new());
    };
    let n_i16 = i16::from_be_bytes(*count_bytes);
    let Ok(n) = usize::try_from(n_i16) else {
        return Ok(alloc::vec::Vec::new());
    };
    let mut names = alloc::vec::Vec::with_capacity(n);
    for _ in 0..n {
        let Some(nul_pos) = rest.iter().position(|&b| b == 0) else { break };
        let name_bytes = rest.get(..nul_pos).ok_or_else(malformed)?;
        let name = core::str::from_utf8(name_bytes).map_err(|_| malformed())?;
        names.push(alloc::string::String::from(name));
        let skip = nul_pos.saturating_add(1).saturating_add(18);
        rest = rest.get(skip..).unwrap_or(&[]);
    }
    Ok(names)
}

/// Extract the parameter-type OIDs from a `ParameterDescription` (`'t'`) payload.
///
/// Wire body shape (PG §55.7): `n_params: int16` followed by `n_params × int32`
/// parameter-type OIDs, in `$1..$n` order. A statement `Describe` answers with
/// this frame BEFORE its `RowDescription`/`NoData`, naming the type the server
/// inferred (or the client declared) for each `$N` placeholder. The driver
/// retains these on the driver's `PreparedStatement` so a
/// later `Bind` can VERIFY the caller's encoded parameter types against them —
/// the fixed-plan peer of the compile-checked path's OID pin.
///
/// TOTAL FUNCTION: on ANY input — truncated, over-count, hostile — it returns
/// `Some(oids)` (well-formed) or `None` (malformed → the caller tears the
/// connection down), NEVER a panic (fuzzed by `decoder_fuzz`). `None` is used
/// rather than a classified [`ProtocolError`](crate::error::ProtocolError)
/// because the sole caller DISCARDS the error and tears down — a distinct
/// variant would be dead classification. An OID `0` (`unspecified` — a param the
/// server could not infer, or one the client left to inference) is preserved
/// verbatim; the verify step treats `0` as unverifiable, not a type.
///
/// A count beyond [`MAX_ROW_COLUMNS`] is rejected (`None`) as a nonconforming /
/// hostile peer before allocating — the same reject-before-allocate ceiling
/// [`parse_row_description`] applies. A realistic prepared statement names far
/// fewer parameters (the typed tuple path caps at 32), so this ceiling is only a
/// hostile-input bound, never a real limit.
#[must_use]
pub fn parse_param_description(payload: &[u8]) -> Option<alloc::vec::Vec<u32>> {
    let (count_bytes, mut rest) = payload.split_first_chunk::<2>()?;
    let n_i16 = i16::from_be_bytes(*count_bytes);
    // A negative count is a framing violation (`try_from` rejects it).
    let n = usize::try_from(n_i16).ok()?;
    if n > MAX_ROW_COLUMNS {
        return None;
    }
    let mut oids = alloc::vec::Vec::with_capacity(n);
    for _ in 0..n {
        let (oid_bytes, next) = rest.split_first_chunk::<4>()?;
        oids.push(u32::from_be_bytes(*oid_bytes));
        rest = next;
    }
    // Trailing bytes after the declared OIDs signal a framing desync.
    if !rest.is_empty() {
        return None;
    }
    Some(oids)
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
    /// [`MAX_ROW_COLUMNS`] (1664).
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
    /// [`MAX_ROW_COLUMNS`] (1664) — a `u16` (the wire column count is a
    /// non-negative `i16`, so the widest index fits `u16` with headroom; a `u8`
    /// would silently saturate a real index past column 255).
    TruncatedColumnLen {
        /// Zero-based column index where the truncation was detected.
        column_idx: u16,
    },
    /// A column's declared length prefix is negative and is not the
    /// sentinel `-1` (which encodes SQL `NULL`). Other negative
    /// values are wire-level invalid.
    NegativeColumnLength {
        /// Zero-based column index.
        column_idx: u16,
        /// The offending length value.
        length: i32,
    },
    /// A column's data region is shorter than the declared length
    /// prefix (partial row).
    ///
    /// The two length counts are `u32`: a column's declared length comes from a
    /// non-negative `i32` length prefix (so `<= i32::MAX`), and the bytes
    /// remaining are a slice of the `<= 2 GB` (`i32`-framed) row body — both
    /// fit `u32` with headroom; with the `u16` column index that keeps this the
    /// widest `DecodeError` payload at 12 bytes rather than 24.
    TruncatedColumnData {
        /// Zero-based column index.
        column_idx: u16,
        /// Length declared by the prefix.
        declared_len: u32,
        /// Bytes actually remaining in the row body.
        remaining: u32,
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
    /// through [`Cell<BinaryFmt>`](Cell) and don't know the column
    /// index at their call site; this variant is honest about that.
    BinaryLengthMismatch {
        /// Bytes the decoder expected (fixed-size for ints / bool).
        expected_len: u8,
        /// Bytes actually received.
        actual_len: u16,
    },
    /// Server emitted SQL NULL (len = -1) for a column the `query!`
    /// row tuple typed as non-Option. The macro infers
    /// non-NULL semantics from the Rust type (`i32` vs
    /// `Option<i32>`); if the schema admits NULL, the user types
    /// `Option<T>` in the row tuple. Wide-typed nullable support
    /// (`Option<T>` row impls) is a planned follow-up.
    NullInNonNullColumn,
    /// A binary `jsonb` column's leading version header is invalid. The
    /// `jsonb` binary wire form is a single version byte (currently always
    /// `1`) followed by the UTF-8 text; `version` is the offending byte, or
    /// `None` when the body was empty (no version byte at all). A future
    /// PostgreSQL `jsonb` version would land here as `Some(v)` rather than
    /// being silently mis-decoded.
    JsonbHeaderInvalid {
        /// The leading byte found, or `None` for an empty body.
        version: Option<u8>,
    },
    /// A binary array column declares a dimensionality other than the
    /// supported forms. The `query!` array decoders model a ONE-dimensional
    /// array (`T[]`, decoding to `Vec<Option<T>>`); PostgreSQL sends `ndim = 0`
    /// for an empty array (accepted, decodes to an empty `Vec`) and `ndim = 1`
    /// for a populated one. Any other `ndim` (a multi-dimensional `int4[][]`,
    /// or a negative/garbage header) is classified here rather than silently
    /// flattened — a `2`-dimensional array is NOT the same value as a 1-D one,
    /// so mis-reading it would be a silently-wrong decode.
    ArrayMultiDim {
        /// The offending dimension count from the array header.
        ndim: i32,
    },
    /// A binary array column's header declares an element type OID that does
    /// not match the element type the row tuple decodes as. The array wire
    /// header carries the element OID explicitly; the decoder cross-checks it
    /// against `<T as ArrayElement>::OID` and refuses to decode a `text[]`
    /// payload as an `int4[]` (or any element mismatch) — a classified error,
    /// never a reinterpretation of the wrong element bytes.
    ArrayElemOidMismatch {
        /// The element OID the row tuple's element type expects.
        expected: u32,
        /// The element OID found in the array wire header.
        found: u32,
    },
    /// A binary array column's payload does not frame EXACTLY — a truncated or
    /// malformed array frame. Covers too FEW bytes (for the fixed header words,
    /// a per-dimension pair, an element's 4-byte length prefix, or an element
    /// body of the declared length), a negative element length other than the
    /// `-1` NULL sentinel, a negative dimension length, AND a length SURPLUS
    /// (trailing bytes past the last declared element, or past an empty array's
    /// header). Classified rather than yielding a partial / defaulted array or
    /// silently ignoring the surplus.
    ArrayTruncated,
    /// A binary `numeric` column's payload does not frame EXACTLY. The wire
    /// form is four `i16` header words (`ndigits`, `weight`, `sign`, `dscale`)
    /// followed by `ndigits` base-10000 digit groups (two bytes each); this
    /// covers a body too SHORT for the header or the declared digit count, AND
    /// a length SURPLUS (trailing bytes past the last digit group). Classified
    /// rather than yielding a partial or defaulted value — a numeric decode bug
    /// is silently-wrong money.
    NumericTruncated,
    /// A binary `numeric` column's sign word is not one of the recognised
    /// values (`0x0000` positive, `0x4000` negative, `0xC000` NaN, `0xD000`
    /// +Infinity, `0xF000` -Infinity). An unknown sign is classified, never
    /// mapped to a plausible-but-wrong value.
    NumericInvalidSign {
        /// The offending sign word from the wire header.
        sign: u16,
    },
    /// A binary `numeric` column carries a base-10000 digit group outside the
    /// valid range `0..=9999`. Each group must be a four-decimal-digit value; a
    /// larger group is a malformed / hostile frame, classified rather than
    /// producing a value with an impossible digit.
    NumericDigitOutOfRange {
        /// The offending digit-group value.
        digit: u16,
    },
    /// A binary `numeric` column's display scale carries a bit outside the
    /// 14-bit range PostgreSQL's wire format permits (`dscale & 0x3FFF !=
    /// dscale`). PostgreSQL's `numeric_recv` REJECTS such a value ("invalid
    /// scale in external \"numeric\" value") rather than masking it, so bsql
    /// classifies it too: a well-formed server never sends a scale beyond
    /// 16383, and silently masking a hostile high-bit scale would reinterpret
    /// it into a different (wrong) rendering — a silently-wrong decode.
    NumericInvalidScale {
        /// The offending display-scale word from the wire header.
        dscale: u16,
    },
    /// A user-defined `enum` column carried a label the generated Rust enum does
    /// not know — a value present in the LIVE database's enum but absent from
    /// the migration that the build catalog typed the query against (the enum
    /// gained a label out-of-band, without a corresponding migration file). A PG
    /// enum is sent as its label text; a label matching no generated variant is
    /// classified here rather than mapped to a plausible-but-wrong variant or
    /// panicking. Payload-free (the decoder has no context to carry the label
    /// bytes here without breaking the size pin); the classification is the
    /// signal — the fix is to add the migration that declares the new label.
    UnknownEnumLabel,
    /// A user-defined COMPOSITE (row-type) column's binary frame declared a
    /// field count that does not match the field count the migration's
    /// `CREATE TYPE name AS (...)` declared. The composite wire form leads with an
    /// `int32` field count; a mismatch means the LIVE database's composite has a
    /// different attribute set than the build catalog was typed against (an
    /// attribute added / dropped out-of-band, without a corresponding migration
    /// file), so the positional field decode would read the wrong bytes. A
    /// negative count (a malformed / hostile header) also lands here — it can
    /// never equal the declared count. Classified rather than mapped to a
    /// plausible-but-wrong record; the fix is to add the migration that evolves
    /// the composite. Payload carries both counts for the operator diagnostic.
    CompositeArityMismatch {
        /// The field count the migration `CREATE TYPE` declared.
        expected: u32,
        /// The field count the wire frame's leading `int32` declared (may be
        /// negative for a malformed header).
        found: i32,
    },
    /// A user-defined COMPOSITE (row-type) column's binary frame does not frame
    /// EXACTLY. The wire form is an `int32` field count then, per field, a
    /// `{uint32 type_oid, int32 len, byte[len]}` triple (`len = -1` = NULL);
    /// this covers a body too SHORT for the count header, a field's 8-byte
    /// `{oid, len}` header, or a field body of the declared length, a negative
    /// field length other than the `-1` NULL sentinel, AND a length SURPLUS
    /// (trailing bytes past the last declared field). Classified rather than
    /// yielding a partial / defaulted record or silently ignoring the surplus —
    /// mirroring the array and fixed-width scalar decoders.
    CompositeTruncated,
    /// A compile-checked `query!` RESULT column's RUNTIME type OID (from the
    /// server's `RowDescription`) does not match the carrier's COMPILE-TIME
    /// expected OID (the migration schema the query was typed against). This is
    /// the top-level peer of [`ArrayElemOidMismatch`](Self::ArrayElemOidMismatch):
    /// the typed decode is positional / const-offset, so a runtime column whose
    /// type DIFFERS from the baked schema — an out-of-band
    /// `ALTER TABLE ... ALTER COLUMN ... TYPE`, or a `CREATE TEMP TABLE` shadowing
    /// a migration table with a different column type — would silently mis-decode
    /// (a `text` decoder reading 4 `int4` bytes yields a plausible-but-wrong
    /// string). The guard verifies each column's runtime OID at the query's
    /// `RowDescription` (a fresh Parse — a cache MISS — where the resolved type
    /// can diverge; a plan REUSE that would change result type is refused by
    /// PostgreSQL itself as `0A000` "cached plan must not change result type"), so
    /// the mismatch is a classified error rather than a silent wrong value.
    /// SKIPPED for a user-defined type (a domain/enum/composite, whose runtime OID
    /// is server-assigned/dynamic — `found >= FIRST_NORMAL_OID`), matching the
    /// existing "no compile-time OID pin for user types" boundary; the text family
    /// (`text`/`varchar`/`bpchar`) is treated as one class (identical wire decode).
    ColumnOidMismatch {
        /// The zero-based projected result-column index.
        index: u16,
        /// The OID the carrier's compile-time row shape expects.
        expected: u32,
        /// The OID the server's `RowDescription` reported at runtime.
        found: u32,
    },
}

// Direct size pin. The two widest variants are `TruncatedColumnData { column_idx:
// u16, declared_len: u32, remaining: u32 }` and `ColumnOidMismatch { index: u16,
// expected: u32, found: u32 }` — both 12 B (two `u32`s + one `u16`, the enum
// discriminant packed into the trailing padding); every other variant is
// narrower. Pinned HERE — not merely capped transitively by `DriverError`'s
// 24 B pin, whose width is actually set by its 16-byte fat-pointer payloads —
// so a silent regrowth of `DecodeError` (e.g. a field widened back to `usize`)
// fails at THIS site with `E0080`, not unnoticed under `DriverError`'s slack.
crate::wire_pin!(DecodeError, size = 12, align = 4);

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
                "server emitted SQL NULL for a column the query! row tuple typed as non-Option \
                 — use Option<T> in the row tuple if the schema admits NULL",
            ),
            Self::JsonbHeaderInvalid { version: Some(v) } => write!(
                f,
                "jsonb binary version header byte is {v}, expected 1",
            ),
            Self::JsonbHeaderInvalid { version: None } => {
                f.write_str("jsonb binary body is empty (missing the version header byte)")
            }
            Self::ArrayMultiDim { ndim } => write!(
                f,
                "array column has {ndim} dimensions; only 1-D arrays (ndim 0 = empty, ndim 1) are supported",
            ),
            Self::ArrayElemOidMismatch { expected, found } => write!(
                f,
                "array element type OID mismatch: header declares {found}, decoder expects {expected}",
            ),
            Self::ArrayTruncated => {
                f.write_str("array column payload is truncated or malformed")
            }
            Self::NumericTruncated => {
                f.write_str("numeric column payload is truncated or malformed")
            }
            Self::NumericInvalidSign { sign } => write!(
                f,
                "numeric column sign word {sign:#06x} is not one of 0x0000/0x4000/0xC000/0xD000/0xF000",
            ),
            Self::NumericDigitOutOfRange { digit } => write!(
                f,
                "numeric column base-10000 digit group {digit} is out of range (must be 0..=9999)",
            ),
            Self::NumericInvalidScale { dscale } => write!(
                f,
                "numeric column display scale {dscale} exceeds the wire format's 14-bit range (0..=16383)",
            ),
            Self::UnknownEnumLabel => f.write_str(
                "enum column carried a label not declared by the migration the query was typed \
                 against (the live database's enum has a value the build catalog does not know)",
            ),
            Self::CompositeArityMismatch { expected, found } => write!(
                f,
                "composite column field count mismatch: the migration `CREATE TYPE` declared \
                 {expected} fields, the wire frame declares {found} (the live composite's \
                 attribute set differs from the build catalog)",
            ),
            Self::CompositeTruncated => {
                f.write_str("composite column payload is truncated or malformed")
            }
            Self::ColumnOidMismatch { index, expected, found } => write!(
                f,
                "result column {index} type OID mismatch: the query was typed against OID {expected} \
                 (the migration schema), but the server reported OID {found} at runtime — the live \
                 column type differs from the migration (an out-of-band ALTER COLUMN TYPE, or a \
                 TEMP TABLE shadowing the migration table)",
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
/// the `'r` lifetime of the owning `crate::OutActions`. The
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
            n_columns: self.n_columns,
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
    /// The row's ORIGINAL column count, captured once at construction and never
    /// mutated. The failing column's 0-based index is recovered LAZILY in the
    /// cold error arms as `n_columns - columns_left - 1` (see
    /// [`failing_column_idx`](Self::failing_column_idx)), so the happy path pays
    /// NO per-column index bookkeeping — no load / saturating-increment / store
    /// of a running index. A `u16`, the same width as the wire column count and
    /// bounded by [`MAX_ROW_COLUMNS`] (1664), so a real index past column 255 is
    /// never truncated in a `DecodeError::TruncatedColumn*`.
    n_columns: u16,
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

    /// The 0-based index of the column CURRENTLY being decoded, recovered for a
    /// cold error arm.
    ///
    /// `next` decrements `columns_left` for the current column BEFORE any error
    /// arm can fire, so at an error site `columns_left` already excludes the
    /// current column; `n_columns - columns_left - 1` is therefore that column's
    /// index. Must be read BEFORE [`fuse_and_error`](Self::fuse_and_error), which
    /// zeroes `columns_left`.
    ///
    /// Called ONLY from the (cold) error arms — never the happy path, which is
    /// exactly why the running index is not tracked per-column. Saturating
    /// throughout: the loop invariant guarantees `columns_left <= n_columns - 1`
    /// here, so neither subtraction underflows; the saturating form only
    /// satisfies the crate's `arithmetic_side_effects` forbid and can never
    /// yield a wrong-but-larger index.
    #[inline]
    fn failing_column_idx(&self) -> u16 {
        self.n_columns
            .saturating_sub(self.columns_left)
            .saturating_sub(1)
    }
}

impl<'a> Iterator for ColumnsIter<'a> {
    type Item = Result<Option<&'a [u8]>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.columns_left == 0 {
            return None;
        }
        // Advance the loop counter ONLY — no running column index is kept. The
        // current column's 0-based index is recovered on demand in the cold
        // error arms via `failing_column_idx` (`n_columns - columns_left - 1`),
        // so the happy path writes no per-column index field (the removed
        // load / saturating-increment / store).
        self.columns_left = self.columns_left.saturating_sub(1);

        // 4-byte length prefix.
        let (len_bytes, after_len) = match self.remaining.split_first_chunk::<4>() {
            Some(pair) => pair,
            None => {
                let column_idx = self.failing_column_idx();
                return self.fuse_and_error(DecodeError::TruncatedColumnLen { column_idx });
            }
        };
        let len = i32::from_be_bytes(*len_bytes);

        // Collapsed sign-path cascade. A naive shape would chain
        // three sequential sign checks:
        //   if len == -1 { NULL }
        //   if len < 0 { NegativeColumnLength }
        //   usize::try_from(len) { ... Err → NegativeColumnLength }
        // Three comparisons per column × up-to-MAX_ROW_COLUMNS cols × 1M rows =
        // tens of millions of redundant compares on row-heavy workloads.
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
            let column_idx = self.failing_column_idx();
            return self.fuse_and_error(DecodeError::NegativeColumnLength {
                column_idx,
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
                let column_idx = self.failing_column_idx();
                // `len_usize` came from a non-negative `i32` prefix and
                // `remaining` is a slice of the `<= 2 GB` row body, so both are
                // `<= i32::MAX < u32::MAX` — the narrow is structurally
                // infallible. The dead arm saturates the pure DIAGNOSTIC field
                // (the truncation is still classified `TruncatedColumnData`)
                // rather than a forbidden unwrap.
                self.fuse_and_error(DecodeError::TruncatedColumnData {
                    column_idx,
                    declared_len: u32::try_from(len_usize).unwrap_or(u32::MAX),
                    remaining: u32::try_from(remaining).unwrap_or(u32::MAX),
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
// Cell — unified value-decode trait (text + binary, one surface)
// ════════════════════════════════════════════════════════════════════
//
// `Cell<'a, F: Fmt>` is the SOLE column-value decode trait. It is
// keyed on a zero-sized format marker `F` (`TextFmt` / `BinaryFmt`)
// so one trait covers both PG wire formats. The body for each
// (Rust-type, format) pair lives directly in the corresponding
// `Cell` impl — there is no separate text-only / binary-only trait
// forwarded to. PG's text format (the default for Simple Query)
// encodes values as ASCII-ish strings; PG's binary format
// (Bind-selected in Extended Query) uses big-endian fixed-width
// ints, a single 0/1 byte for bool, and raw UTF-8 for text.
//
// Six primitive types implement `Cell` for BOTH markers:
// i16 / i32 / i64 / u32 / bool / &str.

mod format_marker_sealed {
    pub trait FmtSealed {}
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
    message = "`{Self}` is not a `Fmt` marker",
    label = "valid markers are `TextFmt` (PG `FormatCode::Text`, wire byte 0) and `BinaryFmt` (PG `FormatCode::Binary`, wire byte 1)",
    note = "`Fmt` is sealed — the closed set matches PG protocol spec §55.2.2 which permits exactly these two format codes; a third would be a major-version breaking change"
)]
pub trait Fmt: format_marker_sealed::FmtSealed {
    /// Runtime [`FormatCode`] value this marker corresponds to.
    const WIRE: FormatCode;
}

/// Type-level marker for [`FormatCode::Text`] (wire byte `0`).
///
/// Zero-sized; used as the format type parameter on [`Cell`].
/// See [`Fmt`] for the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TextFmt;

/// Type-level marker for [`FormatCode::Binary`] (wire byte `1`).
///
/// Zero-sized; used as the format type parameter on [`Cell`].
/// See [`Fmt`] for the closed set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BinaryFmt;

// Footprint anchors (tier-1, build-time): the format markers are pure
// type-level tags and MUST stay zero-sized — a `Cell<F>` carries `F`
// by value, so any accidental field on a marker would inflate every
// decoded cell. Pinned at module scope so the contract fires at
// `cargo check` (incl. for never-instantiated markers), not only under
// `cargo test`. Supersedes the former `assert_eq!(size_of) == 0` test
// pins (the const anchor is strictly stronger — build-time + align).
crate::wire_pin!(TextFmt, size = 0, align = 1);
crate::wire_pin!(BinaryFmt, size = 0, align = 1);

impl format_marker_sealed::FmtSealed for TextFmt {}
impl format_marker_sealed::FmtSealed for BinaryFmt {}

impl Fmt for TextFmt {
    const WIRE: FormatCode = FormatCode::Text;
}
impl Fmt for BinaryFmt {
    const WIRE: FormatCode = FormatCode::Binary;
}

/// Unified PG column-value decoder, generic over the wire format
/// marker `F`.
///
/// `Cell<'a, F>` is implemented in-crate for each (Rust type, wire
/// format) pair the crate supports out of the box — the cartesian
/// product of `{i16, i32, i64, u32, bool, &str} × {TextFmt, BinaryFmt}`.
/// `Cell` is **deliberately not sealed**: a downstream crate may add
/// `impl Cell<'a, F> for ItsOwnType` to decode `chrono`/`uuid`/`decimal`
/// (or any other) types, supplying its own `OID` + `decode`. The closed
/// set is the wire *format* (`Fmt` is sealed: only `TextFmt`/`BinaryFmt`),
/// not the type set. In-crate type coverage widens with follow-ups.
///
/// # Lifetime
///
/// `'a` ties the decoder's output to the input byte slice. For
/// `&str` the output borrows the input directly (zero-copy). For
/// owned types like `i32` / `bool`, `'a` is phantom.
///
/// # Type-level pair check
///
/// Calling `<T as Cell<F>>::decode(bytes)` requires `T` to implement
/// `Cell<F>`. A missing pair (e.g. a hypothetical type with only
/// text support but caller tries `<T as Cell<BinaryFmt>>::decode`)
/// is a compile error, NOT a runtime classification. This **closes**
/// the runtime "format-OID mismatch" classification at the type
/// level.
///
/// # OID drift-pin
///
/// Every impl exposes a `const OID: u32` matching the PG catalog
/// type it decodes (drift-pinned against [`oids`] via const-assert).
/// The same Rust type targets the SAME OID across both format
/// markers — a refactor that skewed text against binary fails the
/// build.
///
/// # Errors
///
/// - [`DecodeError::NonUtf8`] for non-UTF-8 bytes on decoders that
///   require UTF-8 validation (`&str`).
/// - integer types → [`DecodeError::IntParse`] (text) /
///   [`DecodeError::BinaryLengthMismatch`] (binary, wrong width).
/// - `bool` → [`DecodeError::BoolParse`].
///
/// The doc-test below is COMPILE-CHECKED — a future refactor that
/// alters `DataRowRef::parse`, `ColumnsIter::next`, the `Cell`
/// trait shape, or `DecodeError` variants fails the build. The
/// example operates directly on `row_bytes: &[u8]` — the raw
/// PostgreSQL DataRow body the protocol surfaces via its
/// row-streaming API (`RowStream::col_next`, etc.).
///
/// ```rust
/// use bsql_postgres_proto::{Cell, DataRowRef, DecodeError, TextFmt};
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
///     let id: Option<i32> =
///         id_result?.map(<i32 as Cell<TextFmt>>::decode).transpose()?;
///
///     let Some(name_result) = cols.next() else { return Ok(None) };
///     let name: Option<&'a str> =
///         name_result?.map(<&'a str as Cell<TextFmt>>::decode).transpose()?;
///
///     // Return the decoded pair (per-column NULL preserved via `Option`).
///     // The example never silently defaults — every absence is explicit
///     // in the return type.
///     Ok(Some((id, name)))
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be decoded from PG `{F}` bytes",
    label = "the (type, format) pair `({Self}, {F})` is not in the supported decode matrix",
    note = "the in-crate decode matrix covers `{{i16, i32, i64, u32, bool, &str}} × {{TextFmt, BinaryFmt}}`, plus `{{f32, f64, &[u8], Uuid, Timestamptz, Timestamp, Date, Time, Interval, Json, Jsonb, Numeric}}` for `BinaryFmt` only (the binary-uniform wire path); for other types add `impl Cell<'a, F> for {Self}` supplying its `OID` + `decode` (the trait is not sealed)"
)]
pub trait Cell<'a, F: Fmt>: Sized {
    /// PG type OID this (type, format) pair targets. Pinned via
    /// const-assert against [`oids`].
    const OID: u32;

    /// Decode the column's bytes in the format specified by `F`.
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError>;
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
/// continue using `<T as Cell<TextFmt>>::decode` — that path
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
/// Two prior attempts embedded this fast-path INSIDE
/// `<i32 as Cell<TextFmt>>::decode`:
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
/// Decoupling SWAR placement from the text decoder's body size
/// eliminates the LLVM heuristic shift entirely. The text decoder
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
/// Tier-1 safe by construction (`as_chunks` yields infallible fixed-size
/// arrays, no indexing-slicing, no unsafe). Tier-3 false-negative
/// classification (returns `None`
/// on legal multi-byte UTF-8 — caller MUST follow up with full
/// validator; documented contract). Closure: byte-position sweep
/// over boundary values `0x7F` (highest ASCII, accepted) and `0x80`
/// (first non-ASCII, rejected) at every position within and across
/// chunk boundaries.
#[must_use]
pub fn validate_utf8_swar(bytes: &[u8]) -> Option<()> {
    const HIBIT: u64 = 0x8080_8080_8080_8080;
    // `as_chunks::<8>()` (stable 1.88) splits into infallible `&[u8; 8]` chunks
    // plus a `< 8`-byte tail in one shot — no per-chunk `try_into().ok()?`
    // fallible convert (that edge was architecturally dead: a `>= 8`-byte
    // `split_at(8)` always yields exactly 8 bytes). Same SWAR, one fewer dead
    // branch.
    let (chunks, tail) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let packed = u64::from_le_bytes(*chunk);
        if packed & HIBIT != 0 {
            return None;
        }
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
/// their generic decoder (typically [`Cell<TextFmt>`](Cell) for `bool`).
///
/// # Why a dedicated helper
///
/// The standard [`Cell<TextFmt>`](Cell) for `bool` matches only `b"t"`/`b"f"`
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

impl Cell<'_, TextFmt> for i16 {
    const OID: u32 = oids::INT2;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
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

impl Cell<'_, TextFmt> for i32 {
    const OID: u32 = oids::INT4;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
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

impl Cell<'_, TextFmt> for i64 {
    const OID: u32 = oids::INT8;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
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

impl Cell<'_, TextFmt> for u32 {
    const OID: u32 = oids::OID;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
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
impl Cell<'_, TextFmt> for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes {
            b"t" => Ok(true),
            b"f" => Ok(false),
            _ => Err(DecodeError::BoolParse),
        }
    }
}

/// Text column as `&str` — zero-copy, validates UTF-8 only.
impl<'a> Cell<'a, TextFmt> for &'a str {
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
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        simdutf8::basic::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// ═════════════════════════════════════════════════════════════════
// Cell<BinaryFmt> — PG binary-format column decoders (Extended
// Query Bind-selected per-parameter).
//
// Binary format byte layout matches PG §55.7 — fixed-size ints
// are big-endian two's complement, `bool` is a single byte 0/1,
// `text` is raw UTF-8 bytes. Every impl's `OID` const is drift-
// pinned against `oids::*` to catch type-mapping bugs at build
// time.
//
// The caller dispatches between text and binary decoders based on
// [`ColumnDesc::format_code`]. Extended Query selects binary via
// the Bind frame's per-param / per-result format-code arrays;
// Simple Query always uses text.
//
// # OID drift-pin
//
// Every impl exposes a `const OID: u32` matching the PG type it
// decodes. The crate's [`oids`] module is drift-pinned against the
// canonical PG catalog (`pg_type.dat`); a const-assert per impl
// verifies `<T as Cell<BinaryFmt>>::OID == oids::X` at build time.
// A future refactor that breaks the type↔OID mapping fails the
// build, not at runtime.

mod sealed {
    pub trait EncodeBinarySealed {}
}

// Fixed-size big-endian scalar decoders: N bytes, reinterpreted via
// `from_be_bytes`. Covers the two's-complement integers AND the
// IEEE-754 floats (`f32`/`f64`) — the byte-level operation is
// identical (`from_be_bytes` reads exactly N big-endian bytes and
// reinterprets them; for floats that is the IEEE-754 bit pattern PG
// sends), so one macro serves both. A wrong length is classified,
// never silently truncated or widened.
macro_rules! impl_cell_binary_fixed_be {
    ($($t:ty, $oid:expr, $n:literal),+ $(,)?) => {
        $(
            impl Cell<'_, BinaryFmt> for $t {
                const OID: u32 = $oid;
                #[inline]
                fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
                    // Binary fixed-size scalar: exactly N bytes. Any
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

impl_cell_binary_fixed_be!(
    i16, oids::INT2, 2,
    i32, oids::INT4, 4,
    i64, oids::INT8, 8,
    u32, oids::OID, 4,
    // IEEE-754: PG `float4` is 4 big-endian bytes, `float8` is 8. Read
    // the EXACT width and reinterpret the bits — never a lossy `as`
    // narrowing between the two widths.
    f32, oids::FLOAT4, 4,
    f64, oids::FLOAT8, 8,
);

/// PG binary `bool`: one byte — `0` = false, `1` = true.
/// Wrong byte length classifies as [`DecodeError::BinaryLengthMismatch`];
/// length-1 with an out-of-range byte classifies as
/// [`DecodeError::BoolParse`].
impl Cell<'_, BinaryFmt> for bool {
    const OID: u32 = oids::BOOL;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
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
/// separate `Cell<'a, BinaryFmt, …>` impl projecting to `&[u8]` — not
/// implemented today) and validate externally if / when they need a
/// `&str`.
///
/// PG binary `text` is NOMINALLY UTF-8 per `client_encoding`; a buggy
/// server / misconfigured encoding setting could produce invalid bytes.
/// The Err path classifies as [`DecodeError::NonUtf8`] without
/// panicking — consistent with the column-level safety contract.
impl<'a> Cell<'a, BinaryFmt> for &'a str {
    const OID: u32 = oids::TEXT;
    #[inline]
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        simdutf8::basic::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

/// PG binary `bytea`: the raw column bytes, verbatim. Zero-copy borrow.
///
/// `bytea` has no internal structure on the binary wire — the column
/// body IS the byte string — so decode is the identity on the input
/// slice: every length (including empty) is valid and nothing is
/// validated, copied, or reinterpreted. This is the byte-string peer of
/// the `&str` decoder above, minus the UTF-8 check (`bytea` carries
/// arbitrary bytes, not text).
impl<'a> Cell<'a, BinaryFmt> for &'a [u8] {
    const OID: u32 = oids::BYTEA;
    #[inline]
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        Ok(bytes)
    }
}

// ════════════════════════════════════════════════════════════════════
// bsql-native semantic types — dependency-free `uuid` / `timestamptz` /
// `timestamp` decode (defined in `crate::pgtypes`). Each is a fixed-width
// binary payload, so a wrong length is classified via
// `BinaryLengthMismatch` exactly like the scalar integers above — never a
// silent truncation.
// ════════════════════════════════════════════════════════════════════

/// PG binary `uuid`: exactly 16 raw bytes. A wrong length is classified.
impl Cell<'_, BinaryFmt> for Uuid {
    const OID: u32 = oids::UUID;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let arr: &[u8; 16] = bytes
            .first_chunk::<16>()
            .filter(|_| bytes.len() == 16)
            .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                expected_len: 16,
                actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
            })?;
        Ok(Uuid::from_bytes(*arr))
    }
}

/// PG binary `timestamptz`: an `i64` microsecond count since the PG epoch
/// (2000-01-01 UTC), 8 big-endian bytes. A wrong length is classified.
impl Cell<'_, BinaryFmt> for Timestamptz {
    const OID: u32 = oids::TIMESTAMPTZ;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let arr: &[u8; 8] = bytes
            .first_chunk::<8>()
            .filter(|_| bytes.len() == 8)
            .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                expected_len: 8,
                actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
            })?;
        Ok(Timestamptz::from_micros(i64::from_be_bytes(*arr)))
    }
}

/// PG binary `timestamp` (naive): the same 8-byte `i64` micros as
/// `timestamptz`, but zone-less. A wrong length is classified.
impl Cell<'_, BinaryFmt> for Timestamp {
    const OID: u32 = oids::TIMESTAMP;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let arr: &[u8; 8] = bytes
            .first_chunk::<8>()
            .filter(|_| bytes.len() == 8)
            .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                expected_len: 8,
                actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
            })?;
        Ok(Timestamp::from_micros(i64::from_be_bytes(*arr)))
    }
}

/// PG binary `date` (`date_send`): an `i32` day count since 2000-01-01, 4
/// big-endian bytes (`i32::MAX` / `i32::MIN` are the `±infinity` sentinels,
/// wrapped faithfully). A wrong length is classified.
impl Cell<'_, BinaryFmt> for Date {
    const OID: u32 = oids::DATE;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let arr: &[u8; 4] = bytes
            .first_chunk::<4>()
            .filter(|_| bytes.len() == 4)
            .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                expected_len: 4,
                actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
            })?;
        Ok(Date::from_days(i32::from_be_bytes(*arr)))
    }
}

/// PG binary `time` (`time_send`): an `i64` microsecond count since midnight,
/// 8 big-endian bytes. A wrong length is classified.
impl Cell<'_, BinaryFmt> for Time {
    const OID: u32 = oids::TIME;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let arr: &[u8; 8] = bytes
            .first_chunk::<8>()
            .filter(|_| bytes.len() == 8)
            .ok_or_else(|| DecodeError::BinaryLengthMismatch {
                expected_len: 8,
                actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
            })?;
        Ok(Time::from_micros(i64::from_be_bytes(*arr)))
    }
}

/// PG binary `interval` (`interval_send`): three fixed fields IN WIRE ORDER —
/// `i64` microseconds, then `i32` days, then `i32` months (16 bytes). The
/// three fields are stored separately (never collapsed). A wrong length is
/// classified.
impl Cell<'_, BinaryFmt> for Interval {
    const OID: u32 = oids::INTERVAL;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mismatch = || DecodeError::BinaryLengthMismatch {
            expected_len: 16,
            actual_len: crate::narrow::u16_from_usize_under_u16_bound(bytes.len()),
        };
        // Reject any length but exactly 16 up front; the three field reads
        // below then cannot fail (their `ok_or` landing pads are dead).
        if bytes.len() != 16 {
            return Err(mismatch());
        }
        // Wire order: micros (i64), then days (i32), then months (i32).
        let (micros_be, rest) = bytes.split_first_chunk::<8>().ok_or_else(mismatch)?;
        let (days_be, months_be) = rest.split_first_chunk::<4>().ok_or_else(mismatch)?;
        let months_be = months_be.first_chunk::<4>().ok_or_else(mismatch)?;
        let micros = i64::from_be_bytes(*micros_be);
        let days = i32::from_be_bytes(*days_be);
        let months = i32::from_be_bytes(*months_be);
        Ok(Interval::new(months, days, micros))
    }
}

/// PG binary `json`: the raw UTF-8 JSON text (no framing). Validated as
/// UTF-8, then owned — bsql does not parse the JSON structure.
impl Cell<'_, BinaryFmt> for Json {
    const OID: u32 = oids::JSON;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let text = simdutf8::basic::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)?;
        Ok(Json::new(alloc::string::String::from(text)))
    }
}

/// PG binary `jsonb`: a leading version byte (must be `1`) followed by the
/// UTF-8 JSON text. The version byte is validated and stripped; a version
/// other than `1`, or an empty body, is classified — never silently
/// accepted or mis-read.
impl Cell<'_, BinaryFmt> for Jsonb {
    const OID: u32 = oids::JSONB;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        match bytes.split_first() {
            Some((&1, rest)) => {
                let text = simdutf8::basic::from_utf8(rest).map_err(|_| DecodeError::NonUtf8)?;
                Ok(Jsonb::new(alloc::string::String::from(text)))
            }
            Some((&version, _)) => {
                Err(DecodeError::JsonbHeaderInvalid { version: Some(version) })
            }
            None => Err(DecodeError::JsonbHeaderInvalid { version: None }),
        }
    }
}

/// PG binary `numeric` (`src/backend/utils/adt/numeric.c`, `numeric_recv`):
/// four `i16` header words — `ndigits`, `weight`, `sign`, `dscale` — followed
/// by `ndigits` base-10000 digit groups (two bytes each, `0..=9999`).
///
/// `ndigits` is read as an unsigned count (`u16`): a large value's group count
/// can exceed `i16::MAX`, and PostgreSQL itself reads it through a `uint16`, so
/// a signed read would misinterpret a valid huge numeric. The `sign` word
/// classifies the value (`0x0000` positive, `0x4000` negative, `0xC000` NaN,
/// `0xD000` +Infinity, `0xF000` -Infinity); an unknown sign, an out-of-range
/// digit group, and any length surplus / shortfall are classified — a numeric
/// decode bug is silently-wrong money.
impl Cell<'_, BinaryFmt> for Numeric {
    const OID: u32 = oids::NUMERIC;
    #[inline]
    fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (ndigits_be, rest) = bytes
            .split_first_chunk::<2>()
            .ok_or(DecodeError::NumericTruncated)?;
        let ndigits = u16::from_be_bytes(*ndigits_be);
        let (weight_be, rest) = rest
            .split_first_chunk::<2>()
            .ok_or(DecodeError::NumericTruncated)?;
        let weight = i16::from_be_bytes(*weight_be);
        let (sign_be, rest) = rest
            .split_first_chunk::<2>()
            .ok_or(DecodeError::NumericTruncated)?;
        let sign = u16::from_be_bytes(*sign_be);
        let (dscale_be, rest) = rest
            .split_first_chunk::<2>()
            .ok_or(DecodeError::NumericTruncated)?;
        let dscale = u16::from_be_bytes(*dscale_be);
        // Match `numeric_recv` EXACTLY: it REJECTS a display scale with any bit
        // outside the 14-bit `NUMERIC_DSCALE_MASK` range ("invalid scale in
        // external numeric value"), rather than masking it. A well-formed
        // server never sends such a scale; a hostile high-bit scale is a
        // classified error, never silently masked into a different rendering.
        // Applied for every value (finite AND special), as `numeric_recv` does.
        if dscale & NUMERIC_DSCALE_MASK != dscale {
            return Err(DecodeError::NumericInvalidScale { dscale });
        }

        // The non-finite specials carry no digit groups; a trailing body is a
        // malformed frame (no-swallow), not silently ignored.
        let negative = match sign {
            NUMERIC_SIGN_POS => false,
            NUMERIC_SIGN_NEG => true,
            NUMERIC_SIGN_NAN | NUMERIC_SIGN_PINF | NUMERIC_SIGN_NINF => {
                if !rest.is_empty() || ndigits != 0 {
                    return Err(DecodeError::NumericTruncated);
                }
                return Ok(match sign {
                    NUMERIC_SIGN_NAN => Numeric::nan(),
                    NUMERIC_SIGN_PINF => Numeric::infinity(),
                    // The remaining arm is `NUMERIC_SIGN_NINF` — the outer match
                    // already excluded every other value.
                    _ => Numeric::neg_infinity(),
                });
            }
            other => return Err(DecodeError::NumericInvalidSign { sign: other }),
        };

        let n = usize::from(ndigits);
        // Cap the reservation at the remaining byte count so a hostile
        // `ndigits` cannot trigger a huge speculative allocation: each group is
        // two bytes, so there can be no more groups than remaining bytes.
        let mut digits = alloc::vec::Vec::with_capacity(n.min(rest.len()));
        let mut cur = rest;
        for _ in 0..n {
            let (group_be, next) = cur
                .split_first_chunk::<2>()
                .ok_or(DecodeError::NumericTruncated)?;
            let group = u16::from_be_bytes(*group_be);
            if group >= NUMERIC_NBASE {
                return Err(DecodeError::NumericDigitOutOfRange { digit: group });
            }
            digits.push(group);
            cur = next;
        }
        // No-swallow: the digit groups must consume the payload EXACTLY.
        if !cur.is_empty() {
            return Err(DecodeError::NumericTruncated);
        }
        Ok(Numeric::finite(
            negative,
            weight,
            dscale,
            digits.into_boxed_slice(),
        ))
    }
}

/// The `sign` word values from `src/backend/utils/adt/numeric.h`.
const NUMERIC_SIGN_POS: u16 = 0x0000;
const NUMERIC_SIGN_NEG: u16 = 0x4000;
const NUMERIC_SIGN_NAN: u16 = 0xC000;
const NUMERIC_SIGN_PINF: u16 = 0xD000;
const NUMERIC_SIGN_NINF: u16 = 0xF000;
/// One past the largest base-10000 digit group (`NBASE`).
const NUMERIC_NBASE: u16 = 10_000;
/// The 14-bit mask PostgreSQL's `numeric_recv` validates the wire display scale
/// against (`NUMERIC_DSCALE_MASK`). A scale with any higher bit set is rejected.
const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
const _: () = assert!(NUMERIC_DSCALE_MASK == 16383);

// ════════════════════════════════════════════════════════════════════
// One-dimensional array decoders — `T[]` decoding to `Vec<Option<T>>`.
// ════════════════════════════════════════════════════════════════════
//
// PostgreSQL binary array layout (`src/backend/utils/adt/arrayfuncs.c`,
// `array_send`), for a one-dimensional array:
//
// ```text
//   ndim:        i32_be    (0 = empty array, 1 = one dimension)
//   flags:       i32_be    (bit 0 = has-null; IGNORED — NULL is detected
//                           per element from a `-1` length, never trusted
//                           from this flag)
//   element_oid: i32_be    (the scalar element type's OID)
//   per dim:     { dim_len: i32_be, lower_bound: i32_be }   (ndim pairs;
//                           NONE for ndim = 0; lower_bound IGNORED)
//   per element: { len: i32_be, body }   (len == -1 => SQL NULL => None)
// ```
//
// The element order in the `Vec` is the wire order; the lower bound is
// array metadata that does not change the element sequence, so it is read
// and discarded. A dimension count other than `{0, 1}` is a classified
// [`DecodeError::ArrayMultiDim`] — a multi-dimensional array is a distinct
// value and is never silently flattened.

/// An owned Rust type that can be one element of a `query!` array column.
///
/// The array `Cell` decoder (`Vec<Option<T>>`) is generic over this trait:
/// each element is decoded into an OWNED value (so the whole `Vec` owns its
/// contents and the borrowed / owned record twins carry the same field type),
/// the element's [`OID`](Self::OID) is cross-checked against the array wire
/// header, and the array's own OID comes from [`ARRAY_OID`](Self::ARRAY_OID).
///
/// # Sealed
///
/// The trait is sealed (`array_elem_sealed`): the element set is exactly the
/// `query!`-supported scalar types, and a downstream crate cannot introduce a
/// rogue array element type. Because the trait is not lifetime-parameterised,
/// the element is always owned — a `text[]` element is a `String`, a `bytea[]`
/// element a `Vec<u8>`; the value types are themselves.
pub trait ArrayElement: Sized + array_elem_sealed::Sealed {
    /// The element (scalar) type's PG OID — cross-checked against the array
    /// wire header's declared element OID. Drift-pinned against
    /// [`Cell<BinaryFmt>::OID`](Cell) for the borrowed peer type.
    const OID: u32;
    /// This element type's `T[]` array OID. Drift-pinned against the
    /// canonical `oids::*_ARRAY` constant.
    const ARRAY_OID: u32;
    /// Decode ONE array element body into an owned value. Reuses the scalar
    /// binary decoder, then owns the result where the scalar peer borrowed.
    fn decode_elem(bytes: &[u8]) -> Result<Self, DecodeError>;
}

mod array_elem_sealed {
    /// Module-private seal — only this crate's supported element types.
    pub trait Sealed {}
}

/// Decode a one-dimensional PG binary array body into `Vec<Option<T>>`.
/// Shared by every element type's array `Cell` impl. A wrong dimensionality,
/// an element-OID mismatch, or a truncated payload is a classified
/// [`DecodeError`] — never a partial, flattened, or defaulted array.
fn decode_array_1d<T: ArrayElement>(
    bytes: &[u8],
) -> Result<alloc::vec::Vec<Option<T>>, DecodeError> {
    let (ndim_bytes, rest) =
        bytes.split_first_chunk::<4>().ok_or(DecodeError::ArrayTruncated)?;
    let ndim = i32::from_be_bytes(*ndim_bytes);
    // `flags` (bit 0 = has-null) is read and discarded: NULL elements are
    // detected per element from a `-1` length, never trusted from this flag.
    let (_flags, rest) = rest.split_first_chunk::<4>().ok_or(DecodeError::ArrayTruncated)?;
    let (elem_oid_bytes, rest) =
        rest.split_first_chunk::<4>().ok_or(DecodeError::ArrayTruncated)?;
    let elem_oid = u32::from_be_bytes(*elem_oid_bytes);
    // The declared element OID must match the type the row tuple decodes as,
    // so a `text[]` payload is never reinterpreted as `int4[]` bytes. Checked
    // BEFORE the `ndim == 0` early return, so even an empty array enforces the
    // element-type contract (defense-in-depth symmetry — an empty `text[]`
    // header decoded as `int4[]` is a classified mismatch, not a silent
    // `Ok(empty)`).
    if elem_oid != <T as ArrayElement>::OID {
        return Err(DecodeError::ArrayElemOidMismatch {
            expected: <T as ArrayElement>::OID,
            found: elem_oid,
        });
    }
    // ndim 0 is PG's canonical empty array — no dimension pair, no elements.
    if ndim == 0 {
        // No-swallow: nothing may follow the three fixed header words of an
        // empty array; trailing bytes are a malformed frame, not ignored.
        if !rest.is_empty() {
            return Err(DecodeError::ArrayTruncated);
        }
        return Ok(alloc::vec::Vec::new());
    }
    if ndim != 1 {
        return Err(DecodeError::ArrayMultiDim { ndim });
    }
    let (dim_len_bytes, rest) =
        rest.split_first_chunk::<4>().ok_or(DecodeError::ArrayTruncated)?;
    let dim_len = i32::from_be_bytes(*dim_len_bytes);
    // The lower bound is array metadata; the element order is the wire order,
    // so it is read and discarded.
    let (_lower_bound, mut rest) =
        rest.split_first_chunk::<4>().ok_or(DecodeError::ArrayTruncated)?;
    // A negative dimension length is a malformed header (classified, never a
    // wrapped or saturated count).
    let n = usize::try_from(dim_len).map_err(|_| DecodeError::ArrayTruncated)?;
    // Pre-size, but cap the reservation at the remaining byte count so a
    // hostile `dim_len` cannot trigger a huge speculative allocation: every
    // element consumes at least its 4-byte length prefix, so there can be no
    // more elements than remaining bytes. `.min` avoids a division.
    let mut out = alloc::vec::Vec::with_capacity(n.min(rest.len()));
    for _ in 0..n {
        let (len_bytes, after_len) =
            rest.split_first_chunk::<4>().ok_or(DecodeError::ArrayTruncated)?;
        let elem_len = i32::from_be_bytes(*len_bytes);
        // -1 is the wire NULL sentinel — an honest `None` element.
        if elem_len == -1 {
            out.push(None);
            rest = after_len;
            continue;
        }
        // Any other negative length is malformed (classified).
        let elem_len = usize::try_from(elem_len).map_err(|_| DecodeError::ArrayTruncated)?;
        let (body, after_body) =
            after_len.split_at_checked(elem_len).ok_or(DecodeError::ArrayTruncated)?;
        out.push(Some(<T as ArrayElement>::decode_elem(body)?));
        rest = after_body;
    }
    // No-swallow: the element bodies must consume the payload EXACTLY. Trailing
    // bytes past the last declared element are a malformed / hostile frame, not
    // silently ignored — mirroring the fixed-width scalar decoders, which
    // reject any length surplus (never a partial read of a longer body).
    if !rest.is_empty() {
        return Err(DecodeError::ArrayTruncated);
    }
    Ok(out)
}

/// A `query!` array column decodes to an owned `Vec<Option<T>>`: the outer
/// `Vec` owns its elements, and each element is `Option<T>` because a PG array
/// may always contain NULL elements. The array's OID is the element type's
/// `T[]` OID; the header's declared element OID is cross-checked.
impl<'a, T: ArrayElement> Cell<'a, BinaryFmt> for alloc::vec::Vec<Option<T>> {
    const OID: u32 = <T as ArrayElement>::ARRAY_OID;
    #[inline]
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        decode_array_1d::<T>(bytes)
    }
}

// ── ArrayElement impls — the supported element set. Each reuses the scalar
//    binary decoder for the element body, owning the result where the scalar
//    peer borrows (`text` -> `String`, `bytea` -> `Vec<u8>`). ──

/// Generate `ArrayElement` for a value-typed element (`At = Self`): the
/// element decode is exactly the scalar `Cell<BinaryFmt>` decode.
macro_rules! impl_array_element_value {
    ($($t:ty => $array_oid:expr),+ $(,)?) => {
        $(
            impl array_elem_sealed::Sealed for $t {}
            impl ArrayElement for $t {
                const OID: u32 = <$t as Cell<'_, BinaryFmt>>::OID;
                const ARRAY_OID: u32 = $array_oid;
                #[inline]
                fn decode_elem(bytes: &[u8]) -> Result<Self, DecodeError> {
                    <$t as Cell<'_, BinaryFmt>>::decode(bytes)
                }
            }
        )+
    };
}

impl_array_element_value!(
    i16 => oids::INT2_ARRAY,
    i32 => oids::INT4_ARRAY,
    i64 => oids::INT8_ARRAY,
    u32 => oids::OID_ARRAY,
    bool => oids::BOOL_ARRAY,
    f32 => oids::FLOAT4_ARRAY,
    f64 => oids::FLOAT8_ARRAY,
    Uuid => oids::UUID_ARRAY,
    Timestamptz => oids::TIMESTAMPTZ_ARRAY,
    Timestamp => oids::TIMESTAMP_ARRAY,
    Json => oids::JSON_ARRAY,
    Jsonb => oids::JSONB_ARRAY,
    Numeric => oids::NUMERIC_ARRAY,
    Date => oids::DATE_ARRAY,
    Time => oids::TIME_ARRAY,
    Interval => oids::INTERVAL_ARRAY,
);

/// `text[]` element — the owned `String` peer of the borrowed `&str` scalar
/// decoder (the outer `Vec` allocates regardless, so the element owns its
/// UTF-8 bytes rather than borrowing the row body).
impl array_elem_sealed::Sealed for alloc::string::String {}
impl ArrayElement for alloc::string::String {
    const OID: u32 = <&str as Cell<'_, BinaryFmt>>::OID;
    const ARRAY_OID: u32 = oids::TEXT_ARRAY;
    #[inline]
    fn decode_elem(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(alloc::string::String::from(<&str as Cell<'_, BinaryFmt>>::decode(bytes)?))
    }
}

/// `bytea[]` element — the owned `Vec<u8>` peer of the borrowed `&[u8]` scalar
/// decoder.
impl array_elem_sealed::Sealed for alloc::vec::Vec<u8> {}
impl ArrayElement for alloc::vec::Vec<u8> {
    const OID: u32 = <&[u8] as Cell<'_, BinaryFmt>>::OID;
    const ARRAY_OID: u32 = oids::BYTEA_ARRAY;
    #[inline]
    fn decode_elem(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(<[u8]>::to_vec(<&[u8] as Cell<'_, BinaryFmt>>::decode(bytes)?))
    }
}

// Drift-pins: every array `Cell` impl's OID matches the canonical array
// `oids::*_ARRAY` constant, AND each `ArrayElement::OID` (the wire-header
// element check) matches the borrowed scalar peer's `Cell<BinaryFmt>::OID`.
// One const-block pins the whole set — a wrong array OID or a wrong element
// OID fails the build, not a live decode.
const _: () = {
    assert!(<alloc::vec::Vec<Option<i16>> as Cell<BinaryFmt>>::OID == oids::INT2_ARRAY);
    assert!(<alloc::vec::Vec<Option<i32>> as Cell<BinaryFmt>>::OID == oids::INT4_ARRAY);
    assert!(<alloc::vec::Vec<Option<i64>> as Cell<BinaryFmt>>::OID == oids::INT8_ARRAY);
    assert!(<alloc::vec::Vec<Option<u32>> as Cell<BinaryFmt>>::OID == oids::OID_ARRAY);
    assert!(<alloc::vec::Vec<Option<bool>> as Cell<BinaryFmt>>::OID == oids::BOOL_ARRAY);
    assert!(<alloc::vec::Vec<Option<f32>> as Cell<BinaryFmt>>::OID == oids::FLOAT4_ARRAY);
    assert!(<alloc::vec::Vec<Option<f64>> as Cell<BinaryFmt>>::OID == oids::FLOAT8_ARRAY);
    assert!(<alloc::vec::Vec<Option<Uuid>> as Cell<BinaryFmt>>::OID == oids::UUID_ARRAY);
    assert!(<alloc::vec::Vec<Option<Timestamptz>> as Cell<BinaryFmt>>::OID == oids::TIMESTAMPTZ_ARRAY);
    assert!(<alloc::vec::Vec<Option<Timestamp>> as Cell<BinaryFmt>>::OID == oids::TIMESTAMP_ARRAY);
    assert!(<alloc::vec::Vec<Option<Json>> as Cell<BinaryFmt>>::OID == oids::JSON_ARRAY);
    assert!(<alloc::vec::Vec<Option<Jsonb>> as Cell<BinaryFmt>>::OID == oids::JSONB_ARRAY);
    assert!(<alloc::vec::Vec<Option<Numeric>> as Cell<BinaryFmt>>::OID == oids::NUMERIC_ARRAY);
    assert!(<alloc::vec::Vec<Option<Date>> as Cell<BinaryFmt>>::OID == oids::DATE_ARRAY);
    assert!(<alloc::vec::Vec<Option<Time>> as Cell<BinaryFmt>>::OID == oids::TIME_ARRAY);
    assert!(<alloc::vec::Vec<Option<Interval>> as Cell<BinaryFmt>>::OID == oids::INTERVAL_ARRAY);
    assert!(<alloc::vec::Vec<Option<alloc::string::String>> as Cell<BinaryFmt>>::OID == oids::TEXT_ARRAY);
    assert!(<alloc::vec::Vec<Option<alloc::vec::Vec<u8>>> as Cell<BinaryFmt>>::OID == oids::BYTEA_ARRAY);
    // Element-OID (wire header check) ≡ borrowed scalar peer's Cell OID.
    assert!(<i32 as ArrayElement>::OID == <i32 as Cell<BinaryFmt>>::OID);
    assert!(<alloc::string::String as ArrayElement>::OID == <&str as Cell<BinaryFmt>>::OID);
    assert!(<alloc::vec::Vec<u8> as ArrayElement>::OID == <&[u8] as Cell<BinaryFmt>>::OID);
    assert!(<Uuid as ArrayElement>::OID == <Uuid as Cell<BinaryFmt>>::OID);
};

// Compile-time symmetry pins: text and binary decoders for the
// same Rust type MUST target the same PG type OID. A refactor that
// breaks this breaks the build.
//
// `Cell<TextFmt>` and `Cell<BinaryFmt>` both carry `OID`; the
// unified trait + `EncodeBinary` form a closed symmetry family.
// Adding a new Rust type forces matching impls + identical OIDs
// across all of them, verified here.
const _: () = {
    assert!(<i16 as Cell<BinaryFmt>>::OID == oids::INT2);
    assert!(<i32 as Cell<BinaryFmt>>::OID == oids::INT4);
    assert!(<i64 as Cell<BinaryFmt>>::OID == oids::INT8);
    assert!(<u32 as Cell<BinaryFmt>>::OID == oids::OID);
    assert!(<bool as Cell<BinaryFmt>>::OID == oids::BOOL);
    assert!(<&str as Cell<BinaryFmt>>::OID == oids::TEXT);
    // Binary-only types (no text-format twin — the compile-checked
    // query path is binary-uniform, and no shipped decoder reads these
    // from the simple-query text format).
    assert!(<f32 as Cell<BinaryFmt>>::OID == oids::FLOAT4);
    assert!(<f64 as Cell<BinaryFmt>>::OID == oids::FLOAT8);
    assert!(<&[u8] as Cell<BinaryFmt>>::OID == oids::BYTEA);
    // bsql-native semantic types (binary-only, no text-format twin).
    assert!(<Uuid as Cell<BinaryFmt>>::OID == oids::UUID);
    assert!(<Timestamptz as Cell<BinaryFmt>>::OID == oids::TIMESTAMPTZ);
    assert!(<Timestamp as Cell<BinaryFmt>>::OID == oids::TIMESTAMP);
    assert!(<Json as Cell<BinaryFmt>>::OID == oids::JSON);
    assert!(<Jsonb as Cell<BinaryFmt>>::OID == oids::JSONB);
    assert!(<Numeric as Cell<BinaryFmt>>::OID == oids::NUMERIC);
    assert!(<Date as Cell<BinaryFmt>>::OID == oids::DATE);
    assert!(<Time as Cell<BinaryFmt>>::OID == oids::TIME);
    assert!(<Interval as Cell<BinaryFmt>>::OID == oids::INTERVAL);
    // Text↔binary OID symmetry: the same Rust type MUST target the
    // same PG type OID across text and binary decoders. A refactor
    // that skewed one against the other would mean the same Rust
    // type decoded differently depending on `ColumnDesc::format_code`
    // — a classification bug. Pinned below.
    assert!(<i16 as Cell<TextFmt>>::OID == <i16 as Cell<BinaryFmt>>::OID);
    assert!(<i32 as Cell<TextFmt>>::OID == <i32 as Cell<BinaryFmt>>::OID);
    assert!(<i64 as Cell<TextFmt>>::OID == <i64 as Cell<BinaryFmt>>::OID);
    assert!(<u32 as Cell<TextFmt>>::OID == <u32 as Cell<BinaryFmt>>::OID);
    assert!(<bool as Cell<TextFmt>>::OID == <bool as Cell<BinaryFmt>>::OID);
    assert!(<&str as Cell<TextFmt>>::OID == <&str as Cell<BinaryFmt>>::OID);
};

// Marker WIRE constants match the FormatCode variant they encode.
const _: () = {
    assert!(matches!(<TextFmt as Fmt>::WIRE, FormatCode::Text));
    assert!(matches!(<BinaryFmt as Fmt>::WIRE, FormatCode::Binary));
};

/// Runtime [`FormatCode`] → static dispatch helper.
///
/// Bridges the runtime `FormatCode` value carried in
/// `RowDescription` / [`ColumnDesc::format_code`] to the
/// compile-time [`Cell`] dispatch surface. Requires `T` to
/// implement **both** [`Cell<TextFmt>`] **and** [`Cell<BinaryFmt>`]
/// — the common case for every primitive type.
///
/// A future type with only one format impl cannot be dispatched
/// via this function (compile error at the trait-bound check),
/// closing the (T, F) pair-validity question at the type level:
/// either both impls exist and runtime dispatch is sound, or one
/// is missing and the call site fails to compile.
///
/// # Why not a `match` on `FormatCode`?
///
/// Caller could inline `match fmt { Text => <T as Cell<TextFmt>>::decode(b),
/// Binary => <T as Cell<BinaryFmt>>::decode(b) }` — that's exactly
/// what this helper centralises. The win is one canonical dispatch
/// site (per-callsite ad-hoc matches would diverge over time; one
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
    T: Cell<'a, TextFmt> + Cell<'a, BinaryFmt>,
{
    match fmt {
        FormatCode::Text => <T as Cell<'a, TextFmt>>::decode(bytes),
        FormatCode::Binary => <T as Cell<'a, BinaryFmt>>::decode(bytes),
    }
}

// ═════════════════════════════════════════════════════════════════
// EncodeBinary — PG binary format write path (mirror of
// `Cell<BinaryFmt>`). Used by `ParamsWriter` to serialise parameter
// values into the Bind frame's per-param length+bytes layout.
// ═════════════════════════════════════════════════════════════════

/// Encode a Rust value into PG binary format bytes, directly into
/// a [`crate::write_buf::WriteBuf`].
///
/// Parallel to [`Cell<BinaryFmt>`](Cell) — the `OID` constants pair
/// up across the two traits so a future `query!` macro can check
/// param-type OIDs against the `Parse`-time schema fingerprint at
/// compile time.
///
/// Zero-alloc: writes directly into the caller's `WriteBuf`. No
/// intermediate heap buffer, no stack fixture — the caller owns
/// the output storage.
///
/// # Sealed
///
/// Same seal discipline as [`Cell`] — downstream crates cannot add
/// impls for their own types.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not implement `EncodeBinary` (cannot encode to PG binary format)",
    label = "supported binary-encode types are `i16`, `i32`, `i64`, `u32`, `bool`, `f32`, `f64`, `&str`, `&[u8]`, `Uuid`, `Timestamptz`, `Timestamp`, `Date`, `Time`, `Interval`, `Json`, `Jsonb`, `Numeric` (and, for the scalar wire types, their one-dimensional `&[T]` array forms)",
    note = "`EncodeBinary` is sealed — extend by adding `impl EncodeBinary for ...` for the new type in `decode.rs` after extending the supported-OID matrix; downstream `impl EncodeBinary for ...` is forbidden by construction"
)]
pub trait EncodeBinary: sealed::EncodeBinarySealed {
    /// PG type OID this encoder produces. Drift-pinned against
    /// [`oids`] and cross-asserted against the matching
    /// [`Cell<BinaryFmt>`](Cell) impl.
    const OID: u32;

    /// Write the encoded bytes into `dst`. The caller is responsible
    /// for the surrounding per-param length prefix (PG Bind frame
    /// layout); `encode_to` writes only the payload bytes.
    ///
    /// Generic over the [`crate::write_buf::FrameSink`] target: production
    /// binds stream onto the GROWABLE send buffer (so an arbitrarily large
    /// `jsonb` / `bytea` / array parameter is not capped), while tests and the
    /// byte-twin reference build into the bounded [`crate::write_buf::WriteBuf`]
    /// — the same code over both sinks, so their output cannot drift.
    ///
    /// # Errors
    ///
    /// [`crate::write_buf::WriteBufFull`] if the sink rejects the write — for
    /// the growable sink only the architecturally-dead case of a body exceeding
    /// the `u32` / `i32` wire length field, surfaced as a classified error
    /// rather than a panic.
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>;
}

macro_rules! impl_encode_binary_int {
    ($($t:ty, $oid:expr, $push:ident),+ $(,)?) => {
        $(
            impl sealed::EncodeBinarySealed for $t {}
            impl EncodeBinary for $t {
                const OID: u32 = $oid;
                #[inline]
                fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
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
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
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
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
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
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self.as_bytes())
    }
}

/// The runtime contract of a Rust type generated from a PostgreSQL
/// `CREATE TYPE ... AS ENUM` migration — the `bsql::user_types!()` macro emits
/// one `impl PgEnum` per migration enum, and the compile-checked `query!` path
/// decodes an enum column through [`from_wire_label`](PgEnum::from_wire_label)
/// and binds an enum parameter through [`as_label`](PgEnum::as_label).
///
/// A PostgreSQL enum value travels on the wire as its LABEL TEXT (in both the
/// text and binary formats — `enum_send`/`enum_recv` are the label bytes), so a
/// generated enum is decoded exactly like a `text` column plus a label→variant
/// match, and encoded as its label text. The label⟷variant mapping lives once,
/// in the generated `impl` — `query!` only names the type and calls these
/// methods, so the mapping cannot drift between decode and encode.
///
/// This trait is deliberately NOT sealed: the impl is emitted in the CONSUMER
/// crate (by `user_types!()`), so a seal would be unsatisfiable there. It grants
/// no wire capability a hand impl could abuse — a `PgEnum` value only ever binds
/// as an `unspecified`-typed (OID 0) label parameter the SERVER validates
/// against the real enum (an invalid label is a server error), and only ever
/// decodes a label the generated `from_wire_label` recognises (an unknown label
/// is a classified [`DecodeError::UnknownEnumLabel`]).
pub trait PgEnum: Copy + 'static {
    /// This value's PostgreSQL enum LABEL — the exact declared label text,
    /// case-sensitive. The inverse of [`from_wire_label`](Self::from_wire_label)
    /// over the recognised labels.
    fn wire_label(self) -> &'static str;

    /// Decode a wire enum LABEL into this Rust enum, or a classified error when
    /// the label matches no generated variant.
    ///
    /// # Errors
    ///
    /// [`DecodeError::UnknownEnumLabel`] when `label` is not one of the enum's
    /// declared labels — a value in the live database's enum that the migration
    /// the query was typed against did not declare. Never a panic or a
    /// plausible-but-wrong variant.
    fn from_wire_label(label: &str) -> Result<Self, DecodeError>;

    /// Wrap this value as a bind parameter for the compile-checked `query!`
    /// path. The returned [`EnumLabel`] carries the value's label and binds it
    /// as an `unspecified`-typed (OID 0) parameter the server coerces to the
    /// enum from context. Type-parameterised by the enum, so a `query!`
    /// expecting one enum rejects another enum's label at compile time.
    #[inline]
    #[must_use]
    fn as_label(self) -> EnumLabel<Self> {
        EnumLabel::new(self)
    }
}

/// A user-enum bind parameter: a [`PgEnum`] value's label text, bound as an
/// `unspecified`-typed (OID 0) binary parameter the server infers from context.
///
/// A PostgreSQL enum has NO implicit `text` cast, so a `text` (OID 25)
/// parameter against an enum column is a server error; declaring the parameter
/// UNSPECIFIED (0) instead lets the server resolve the enum type from the SQL
/// context and apply `enum_recv` to the label bytes. The phantom `E` makes the
/// parameter enum-SPECIFIC: `EnumLabel<Mood>` and `EnumLabel<Status>` are
/// distinct types, so a `query!` whose parameter is one enum rejects the other
/// enum's label with a compile error — the wrong-enum footgun is structural.
pub struct EnumLabel<E: PgEnum> {
    /// The value's declared label text (always `'static` — an enum's labels are
    /// generated string literals).
    label: &'static str,
    /// Phantom tie to the source enum, so `EnumLabel<A>` ≠ `EnumLabel<B>`.
    _enum: core::marker::PhantomData<E>,
}

impl<E: PgEnum> EnumLabel<E> {
    /// Wrap a [`PgEnum`] value's label as an `unspecified`-typed bind parameter.
    #[inline]
    #[must_use]
    pub fn new(value: E) -> Self {
        EnumLabel {
            label: value.wire_label(),
            _enum: core::marker::PhantomData,
        }
    }
}

impl<E: PgEnum> Clone for EnumLabel<E> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<E: PgEnum> Copy for EnumLabel<E> {}

impl<E: PgEnum> core::fmt::Debug for EnumLabel<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("EnumLabel").field(&self.label).finish()
    }
}

impl<E: PgEnum> sealed::EncodeBinarySealed for EnumLabel<E> {}
impl<E: PgEnum> EncodeBinary for EnumLabel<E> {
    // OID 0 (unspecified): the server infers the enum type from the parameter's
    // SQL context, then applies `enum_recv` to the label bytes below. A `text`
    // (25) OID would be rejected — PG has no implicit text→enum cast.
    const OID: u32 = oids::UNSPECIFIED;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self.label.as_bytes())
    }
}

/// A user-defined PostgreSQL COMPOSITE (row) type generated from a
/// `CREATE TYPE name AS (...)` migration. `query!` decodes a composite column
/// through [`decode_row`](PgComposite::decode_row).
///
/// A PostgreSQL composite value travels on the wire in BINARY as its row-type
/// frame — an `int32` field count, then per field a `{uint32 type_oid, int32 len
/// (-1 = NULL), byte[len] value}` triple, the value bytes being each field
/// type's OWN binary encoding. So a generated composite is decoded by walking
/// the frame ([`CompositeReader`]) and RECURSING into each field's own decoder
/// (a native `Cell<BinaryFmt>` scalar/array, a nested [`PgComposite`], or a
/// [`PgEnum`] label) — never a second copy of the scalar decoders. The
/// field⟷attribute mapping lives once, in the generated `impl`, so `query!` only
/// names the type and calls this method.
///
/// **Guarantee boundary (the runtime peer of the column-OID pin, matching the
/// enum's).** The composite's OID — and every field's wire OID — is
/// server-assigned / DYNAMIC (a domain or enum field carries its own dynamic OID,
/// not its base's), so there is NO static OID to pin: the wire field OID is READ
/// and IGNORED. The decode is validated instead by field POSITION + ARITY (the
/// frame's declared field count must equal the migration's — else
/// [`DecodeError::CompositeArityMismatch`]) + each field's OWN decode succeeding
/// (a fixed-width field rejects a wrong byte length; a nested composite re-checks
/// its own arity; an enum field rejects an unknown label). A same-width native
/// confusion (a field the migration declares `int4` that the LIVE composite was
/// ALTERed to `float4`) is NOT caught by length alone — but that requires an
/// out-of-band schema divergence from the migration FILES, exactly the documented
/// catalog boundary, and an attribute add/drop shifts the arity, which IS caught.
///
/// This trait is deliberately NOT sealed: the impl is emitted in the CONSUMER
/// crate (by `user_types!()`), so a seal would be unsatisfiable there. It grants
/// no wire capability a hand impl could abuse — a `PgComposite` only ever DECODES
/// a frame the generated `decode_row` walks, classifying any malformed / drifted
/// frame rather than panicking.
pub trait PgComposite: Sized {
    /// Decode a composite (row-type) binary frame into this Rust struct, or a
    /// classified error for a malformed / arity-drifted frame.
    ///
    /// # Errors
    ///
    /// [`DecodeError::CompositeArityMismatch`] when the frame's declared field
    /// count differs from the migration's; [`DecodeError::CompositeTruncated`]
    /// when the frame does not frame exactly; or the field's own decode error
    /// (e.g. [`DecodeError::UnknownEnumLabel`] for an enum field, a
    /// [`DecodeError::BinaryLengthMismatch`] for a wrong-width scalar field).
    fn decode_row(frame: &[u8]) -> Result<Self, DecodeError>;
}

/// A cursor over a PostgreSQL composite (row-type) BINARY frame, walking it
/// field-by-field so a generated [`PgComposite::decode_row`] never re-implements
/// the frame framing. TOTAL and panic-free: every read is a bounds-checked
/// `split_first_chunk` / `split_at_checked`, and any shortfall / surplus / bad
/// length is a classified [`DecodeError`] (`CompositeArityMismatch` /
/// `CompositeTruncated`), never a panic or a partial value.
///
/// Usage (as the generated code drives it): [`new`](CompositeReader::new) reads +
/// checks the field-count header, then exactly one [`next_field`](
/// CompositeReader::next_field) per declared field (in declared order), then
/// [`finish`](CompositeReader::finish) to reject any trailing surplus.
#[derive(Debug)]
pub struct CompositeReader<'a> {
    /// The unconsumed remainder of the frame (after the count header and every
    /// field read so far).
    rest: &'a [u8],
}

impl<'a> CompositeReader<'a> {
    /// Begin reading a composite frame, consuming and checking the leading
    /// `int32` field count against the migration's declared field count.
    ///
    /// # Errors
    ///
    /// [`DecodeError::CompositeTruncated`] when the frame is too short for the
    /// 4-byte count header; [`DecodeError::CompositeArityMismatch`] when the
    /// declared count differs from `expected_nfields` (including a negative
    /// count, which never equals a real field count).
    #[inline]
    pub fn new(frame: &'a [u8], expected_nfields: u32) -> Result<Self, DecodeError> {
        let (count_bytes, rest) = frame
            .split_first_chunk::<4>()
            .ok_or(DecodeError::CompositeTruncated)?;
        let nfields = i32::from_be_bytes(*count_bytes);
        // Compare via i64 so a negative wire count and a large declared count
        // both compare losslessly (no `as`, no fallible narrowing).
        if i64::from(nfields) != i64::from(expected_nfields) {
            return Err(DecodeError::CompositeArityMismatch {
                expected: expected_nfields,
                found: nfields,
            });
        }
        Ok(CompositeReader { rest })
    }

    /// Read the next field: its 4-byte type OID (read and IGNORED — dynamic,
    /// per the guarantee boundary) and 4-byte length, then its body. Returns
    /// `Ok(None)` for a NULL field (`len == -1`), `Ok(Some(body))` for a present
    /// field's binary value bytes.
    ///
    /// # Errors
    ///
    /// [`DecodeError::CompositeTruncated`] when the remainder is too short for
    /// the `{oid, len}` header or the declared body, or the length is negative
    /// and not the `-1` NULL sentinel.
    #[inline]
    pub fn next_field(&mut self) -> Result<Option<&'a [u8]>, DecodeError> {
        let (_oid_bytes, after_oid) = self
            .rest
            .split_first_chunk::<4>()
            .ok_or(DecodeError::CompositeTruncated)?;
        // The field's wire type OID is server-assigned / dynamic (a domain or
        // enum field carries its own OID), so it is READ past and NOT validated;
        // the decode is checked by position + arity + each field's own decode.
        let (len_bytes, after_len) = after_oid
            .split_first_chunk::<4>()
            .ok_or(DecodeError::CompositeTruncated)?;
        let len = i32::from_be_bytes(*len_bytes);
        // -1 is the wire NULL sentinel — an honest absent field.
        if len == -1 {
            self.rest = after_len;
            return Ok(None);
        }
        // Any other negative length is a malformed frame (classified, never a
        // wrapped / saturated count).
        let len_usize = usize::try_from(len).map_err(|_| DecodeError::CompositeTruncated)?;
        let (body, after_body) = after_len
            .split_at_checked(len_usize)
            .ok_or(DecodeError::CompositeTruncated)?;
        self.rest = after_body;
        Ok(Some(body))
    }

    /// Assert the frame is fully consumed. A trailing surplus past the last
    /// declared field is a malformed / hostile frame, not silently ignored.
    ///
    /// # Errors
    ///
    /// [`DecodeError::CompositeTruncated`] when unconsumed bytes remain.
    #[inline]
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::CompositeTruncated)
        }
    }
}

// IEEE-754 float encoders: the big-endian bit pattern PG expects
// (`float4` = 4 bytes, `float8` = 8), written verbatim. `to_be_bytes`
// is the exact inverse of the `from_be_bytes` decoders above, so a
// round-trip is bit-identical — no width coercion, no lossy `as`.
macro_rules! impl_encode_binary_float {
    ($($t:ty, $oid:expr),+ $(,)?) => {
        $(
            impl sealed::EncodeBinarySealed for $t {}
            impl EncodeBinary for $t {
                const OID: u32 = $oid;
                #[inline]
                fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
                    -> Result<(), crate::write_buf::WriteBufFull>
                {
                    dst.push_bytes(&self.to_be_bytes())
                }
            }
        )+
    };
}

impl_encode_binary_float!(f32, oids::FLOAT4, f64, oids::FLOAT8);

/// `&[u8]` encoder — raw `bytea` bytes, verbatim (the byte-string peer
/// of the `&str` encoder, without the UTF-8 assumption). Every length,
/// including empty, is a valid `bytea` body.
impl sealed::EncodeBinarySealed for &[u8] {}
impl EncodeBinary for &[u8] {
    const OID: u32 = oids::BYTEA;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self)
    }
}

// bsql-native semantic-type encoders — the mirror of the `Cell<BinaryFmt>`
// decoders above, so a `uuid` / `timestamptz` / `timestamp` value can bind
// as a `$N` parameter. The seal mirrors the scalar impls.

/// `uuid` encoder — the 16 raw bytes, verbatim.
impl sealed::EncodeBinarySealed for Uuid {}
impl EncodeBinary for Uuid {
    const OID: u32 = oids::UUID;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self.as_bytes())
    }
}

/// `timestamptz` encoder — the `i64` PG-epoch micros, big-endian.
impl sealed::EncodeBinarySealed for Timestamptz {}
impl EncodeBinary for Timestamptz {
    const OID: u32 = oids::TIMESTAMPTZ;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i64_be(self.as_micros())
    }
}

/// `timestamp` (naive) encoder — the `i64` PG-epoch micros, big-endian.
impl sealed::EncodeBinarySealed for Timestamp {}
impl EncodeBinary for Timestamp {
    const OID: u32 = oids::TIMESTAMP;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i64_be(self.as_micros())
    }
}

/// `date` encoder — the `i32` day count since 2000-01-01, big-endian (the
/// `±infinity` sentinels ride through unchanged).
impl sealed::EncodeBinarySealed for Date {}
impl EncodeBinary for Date {
    const OID: u32 = oids::DATE;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i32_be(self.to_days())
    }
}

/// `time` encoder — the `i64` microseconds-since-midnight, big-endian.
impl sealed::EncodeBinarySealed for Time {}
impl EncodeBinary for Time {
    const OID: u32 = oids::TIME;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i64_be(self.as_micros())
    }
}

/// `interval` encoder — the three fields IN WIRE ORDER (`i64` micros, `i32`
/// days, `i32` months), the exact inverse of the [`Cell<BinaryFmt>`](Cell)
/// decoder so a value round-trips bit-for-bit. The fields are never collapsed.
impl sealed::EncodeBinarySealed for Interval {}
impl EncodeBinary for Interval {
    const OID: u32 = oids::INTERVAL;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_i64_be(self.micros())?;
        dst.push_i32_be(self.days())?;
        dst.push_i32_be(self.months())
    }
}

/// `json` encoder — the raw UTF-8 JSON text, verbatim (no framing).
impl sealed::EncodeBinarySealed for Json {}
impl EncodeBinary for Json {
    const OID: u32 = oids::JSON;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_bytes(self.as_str().as_bytes())
    }
}

/// `jsonb` encoder — the leading version byte (`1`) followed by the UTF-8
/// JSON text, mirroring the decoder's header contract exactly.
impl sealed::EncodeBinarySealed for Jsonb {}
impl EncodeBinary for Jsonb {
    const OID: u32 = oids::JSONB;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        dst.push_u8(1)?;
        dst.push_bytes(self.as_str().as_bytes())
    }
}

/// `numeric` encoder — the four `i16` header words then the base-10000 digit
/// groups, the exact inverse of the [`Cell<BinaryFmt>`](Cell) decoder above, so
/// a value round-trips bit-for-bit. `ndigits` is written as `u16` (PostgreSQL
/// reads it through a `uint16`), `weight` as `i16`, and `sign` / `dscale` as
/// their wire words derived from the value's classification.
impl sealed::EncodeBinarySealed for Numeric {}
impl EncodeBinary for Numeric {
    const OID: u32 = oids::NUMERIC;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        let digits = self.base_10000_digits();
        // A group count past `u16::MAX` cannot be wire-encoded; that is a loud
        // `Err` (fail-closed), never a wrapped count. A `FromStr` / decoded
        // value can never reach it (its `weight` overflows `i16` first), so
        // this landing pad is dead in practice.
        let ndigits =
            u16::try_from(digits.len()).map_err(|_| crate::write_buf::WriteBufFull)?;
        let sign: u16 = if self.is_nan() {
            NUMERIC_SIGN_NAN
        } else if self.is_infinite() {
            if self.is_negative() {
                NUMERIC_SIGN_NINF
            } else {
                NUMERIC_SIGN_PINF
            }
        } else if self.is_negative() {
            NUMERIC_SIGN_NEG
        } else {
            NUMERIC_SIGN_POS
        };
        dst.push_u16_be(ndigits)?;
        dst.push_i16_be(self.weight())?;
        dst.push_u16_be(sign)?;
        // `dscale` is `0..=16383`, so its `u16` bytes are exactly the `i16`
        // dscale PostgreSQL reads (it never sets a sign bit).
        dst.push_u16_be(self.scale())?;
        for &group in digits {
            dst.push_u16_be(group)?;
        }
        Ok(())
    }
}

// ════════════════════════════════════════════════════════════════════
// One-dimensional array encoders — the wire form of a single array
// parameter passed to a `col = ANY($N)` in-list.
// ════════════════════════════════════════════════════════════════════
//
// PostgreSQL binary array layout (`src/backend/utils/adt/arrayfuncs.c`,
// `array_send`), for a one-dimensional array with lower bound 1:
//
// ```text
//   ndim:        i32_be = 1
//   has_null:    i32_be = 0   (this encoder never emits NULL elements)
//   element_oid: i32_be       (the scalar element type's OID)
//   dim_len:     i32_be = N   (the element count)
//   lower_bound: i32_be = 1
//   per element: { len_i32_be, body_bytes }   (no NULL: len >= 0)
// ```
//
// The outer per-parameter length prefix is NOT written here — the caller
// (`ParamEncoder::write_param`) wraps `encode_to` in
// `with_i32_length_prefixed_body`, exactly as for every scalar param, so
// the array param is binary-uniform with the rest of the Bind frame.

/// Write the one-dimensional PG binary array header + each element body,
/// length-prefixed. Shared by every element type's array `encode_to`.
/// `T: EncodeBinary` supplies both the per-element bytes and the element
/// type OID, so the header's `element_oid` can never disagree with the
/// bytes that follow.
#[inline]
fn encode_array_1d<T: EncodeBinary, S: crate::write_buf::FrameSink>(
    elems: &[T],
    dst: &mut S,
) -> Result<(), crate::write_buf::WriteBufFull> {
    // An empty array is PG's canonical zero-dimension form: `array_send`
    // writes `ndim = 0` with NO dimension or lower-bound words (verified
    // byte-for-byte against `array_send('{}'::int8[])`). Matching it keeps
    // this encoder a faithful `array_send` replica for every length.
    if elems.is_empty() {
        dst.push_i32_be(0)?; // ndim = 0
        dst.push_i32_be(0)?; // has_null = 0
        dst.push_u32_be(<T as EncodeBinary>::OID)?; // element OID
        return Ok(());
    }
    dst.push_i32_be(1)?; // ndim = 1
    dst.push_i32_be(0)?; // has_null = 0 (no NULL elements on this path)
    // element OID as 4 bytes; every supported element OID is < 2^31, so a
    // u32 write and an i32 write are byte-identical — `push_u32_be` keeps
    // the value un-cast.
    dst.push_u32_be(<T as EncodeBinary>::OID)?;
    // A slice longer than i32::MAX cannot be length-encoded; that is a
    // loud `Err` (it overflows the wire's i32 dim field), never a wrapped
    // length. In practice the bounded send buffer is exhausted long first.
    let dim_len = i32::try_from(elems.len()).map_err(|_| crate::write_buf::WriteBufFull)?;
    dst.push_i32_be(dim_len)?; // dimension length
    dst.push_i32_be(1)?; // lower bound = 1
    for elem in elems {
        dst.with_i32_length_prefixed_body(|w| elem.encode_to(w))?;
    }
    Ok(())
}

/// Generate `EncodeBinary` for `&[T]` over a fixed-width element `T`,
/// with the array's own OID. The seal mirrors the scalar impls so a
/// downstream crate cannot introduce its own array encoder.
macro_rules! impl_encode_binary_array {
    ($($elem:ty => $array_oid:expr),+ $(,)?) => {
        $(
            impl<'array> sealed::EncodeBinarySealed for &'array [$elem] {}
            impl<'array> EncodeBinary for &'array [$elem] {
                const OID: u32 = $array_oid;
                #[inline]
                fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
                    -> Result<(), crate::write_buf::WriteBufFull>
                {
                    encode_array_1d::<$elem, S>(self, dst)
                }
            }
        )+
    };
}

impl_encode_binary_array!(
    i16 => oids::INT2_ARRAY,
    i32 => oids::INT4_ARRAY,
    i64 => oids::INT8_ARRAY,
    u32 => oids::OID_ARRAY,
    bool => oids::BOOL_ARRAY,
    f32 => oids::FLOAT4_ARRAY,
    f64 => oids::FLOAT8_ARRAY,
    Numeric => oids::NUMERIC_ARRAY,
    Date => oids::DATE_ARRAY,
    Time => oids::TIME_ARRAY,
    Interval => oids::INTERVAL_ARRAY,
);

/// `text[]` array of borrowed strings. Each element is the same UTF-8
/// body the scalar `&str` encoder writes, length-prefixed by
/// `encode_array_1d`.
impl sealed::EncodeBinarySealed for &[&str] {}
impl EncodeBinary for &[&str] {
    const OID: u32 = oids::TEXT_ARRAY;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        encode_array_1d::<&str, S>(self, dst)
    }
}

/// `bytea[]` array of borrowed byte strings. Each element is the same
/// raw body the scalar `&[u8]` encoder writes, length-prefixed by
/// `encode_array_1d` — the byte-string peer of the `&[&str]` array.
impl sealed::EncodeBinarySealed for &[&[u8]] {}
impl EncodeBinary for &[&[u8]] {
    const OID: u32 = oids::BYTEA_ARRAY;
    #[inline]
    fn encode_to<S: crate::write_buf::FrameSink>(&self, dst: &mut S)
        -> Result<(), crate::write_buf::WriteBufFull>
    {
        encode_array_1d::<&[u8], S>(self, dst)
    }
}

// Drift-pins: every array EncodeBinary impl's OID matches the canonical
// array `oids::*` constant, and the header's element OID matches the
// scalar element's OID (so the declared element type can never disagree
// with the element bytes `encode_array_1d` writes).
const _: () = {
    assert!(<&[i16] as EncodeBinary>::OID == oids::INT2_ARRAY);
    assert!(<&[i32] as EncodeBinary>::OID == oids::INT4_ARRAY);
    assert!(<&[i64] as EncodeBinary>::OID == oids::INT8_ARRAY);
    assert!(<&[u32] as EncodeBinary>::OID == oids::OID_ARRAY);
    assert!(<&[bool] as EncodeBinary>::OID == oids::BOOL_ARRAY);
    assert!(<&[&str] as EncodeBinary>::OID == oids::TEXT_ARRAY);
    assert!(<&[f32] as EncodeBinary>::OID == oids::FLOAT4_ARRAY);
    assert!(<&[f64] as EncodeBinary>::OID == oids::FLOAT8_ARRAY);
    assert!(<&[&[u8]] as EncodeBinary>::OID == oids::BYTEA_ARRAY);
    assert!(<&[Numeric] as EncodeBinary>::OID == oids::NUMERIC_ARRAY);
    assert!(<&[Date] as EncodeBinary>::OID == oids::DATE_ARRAY);
    assert!(<&[Time] as EncodeBinary>::OID == oids::TIME_ARRAY);
    assert!(<&[Interval] as EncodeBinary>::OID == oids::INTERVAL_ARRAY);
};

// Drift-pins: every EncodeBinary impl's OID matches the
// corresponding `Cell<BinaryFmt>` impl AND the canonical `oids::*`
// constant. One const-block pins the whole set.
const _: () = {
    assert!(<i16 as EncodeBinary>::OID == oids::INT2);
    assert!(<i32 as EncodeBinary>::OID == oids::INT4);
    assert!(<i64 as EncodeBinary>::OID == oids::INT8);
    assert!(<u32 as EncodeBinary>::OID == oids::OID);
    assert!(<bool as EncodeBinary>::OID == oids::BOOL);
    assert!(<&str as EncodeBinary>::OID == oids::TEXT);
    assert!(<f32 as EncodeBinary>::OID == oids::FLOAT4);
    assert!(<f64 as EncodeBinary>::OID == oids::FLOAT8);
    assert!(<&[u8] as EncodeBinary>::OID == oids::BYTEA);
    // bsql-native semantic types.
    assert!(<Uuid as EncodeBinary>::OID == oids::UUID);
    assert!(<Timestamptz as EncodeBinary>::OID == oids::TIMESTAMPTZ);
    assert!(<Timestamp as EncodeBinary>::OID == oids::TIMESTAMP);
    assert!(<Json as EncodeBinary>::OID == oids::JSON);
    assert!(<Jsonb as EncodeBinary>::OID == oids::JSONB);
    assert!(<Numeric as EncodeBinary>::OID == oids::NUMERIC);
    assert!(<Date as EncodeBinary>::OID == oids::DATE);
    assert!(<Time as EncodeBinary>::OID == oids::TIME);
    assert!(<Interval as EncodeBinary>::OID == oids::INTERVAL);
    // Cross-trait symmetry (encode OID ≡ binary-decode OID ≡ catalog OID).
    assert!(<i16 as EncodeBinary>::OID == <i16 as Cell<BinaryFmt>>::OID);
    assert!(<i32 as EncodeBinary>::OID == <i32 as Cell<BinaryFmt>>::OID);
    assert!(<i64 as EncodeBinary>::OID == <i64 as Cell<BinaryFmt>>::OID);
    assert!(<u32 as EncodeBinary>::OID == <u32 as Cell<BinaryFmt>>::OID);
    assert!(<bool as EncodeBinary>::OID == <bool as Cell<BinaryFmt>>::OID);
    assert!(<&str as EncodeBinary>::OID == <&str as Cell<BinaryFmt>>::OID);
    assert!(<f32 as EncodeBinary>::OID == <f32 as Cell<BinaryFmt>>::OID);
    assert!(<f64 as EncodeBinary>::OID == <f64 as Cell<BinaryFmt>>::OID);
    assert!(<&[u8] as EncodeBinary>::OID == <&[u8] as Cell<BinaryFmt>>::OID);
    assert!(<Uuid as EncodeBinary>::OID == <Uuid as Cell<BinaryFmt>>::OID);
    assert!(<Timestamptz as EncodeBinary>::OID == <Timestamptz as Cell<BinaryFmt>>::OID);
    assert!(<Timestamp as EncodeBinary>::OID == <Timestamp as Cell<BinaryFmt>>::OID);
    assert!(<Json as EncodeBinary>::OID == <Json as Cell<BinaryFmt>>::OID);
    assert!(<Jsonb as EncodeBinary>::OID == <Jsonb as Cell<BinaryFmt>>::OID);
    assert!(<Numeric as EncodeBinary>::OID == <Numeric as Cell<BinaryFmt>>::OID);
    assert!(<Date as EncodeBinary>::OID == <Date as Cell<BinaryFmt>>::OID);
    assert!(<Time as EncodeBinary>::OID == <Time as Cell<BinaryFmt>>::OID);
    assert!(<Interval as EncodeBinary>::OID == <Interval as Cell<BinaryFmt>>::OID);
};

/// PostgreSQL built-in type OID constants for the subset the
/// decoders cover. Full list at
/// `https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat`.
///
/// Callers match these against [`ColumnDesc::type_oid`] to
/// dispatch the right [`Cell`] impl. A future `query!`
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
    /// PostgreSQL's `InvalidOid` (0) — an "unspecified type" marker, NOT a
    /// concrete type. Used as a Parse-frame parameter type OID to ask the server
    /// to INFER the parameter's type from its context in the SQL (exactly as an
    /// unquoted string literal is `unknown`-typed and coerced). This is how a
    /// user-defined `enum` parameter binds: a PG enum has no implicit `text`
    /// (OID 25) cast, so declaring the parameter `text` is rejected, but an
    /// unspecified (0) parameter is resolved to the enum type from context and
    /// its binary label bytes are accepted by `enum_recv`.
    pub const UNSPECIFIED: u32 = 0;
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
    /// `json` — JSON document stored as text.
    pub const JSON: u32 = 114;
    /// `uuid`.
    pub const UUID: u32 = 2950;
    /// `jsonb` — binary JSON (leading version byte + text).
    pub const JSONB: u32 = 3802;
    /// `numeric` / `decimal` — arbitrary-precision exact decimal.
    pub const NUMERIC: u32 = 1700;
    /// `date` — calendar day (days since 2000-01-01).
    pub const DATE: u32 = 1082;
    /// `time` — time of day without time zone (microseconds since midnight).
    pub const TIME: u32 = 1083;
    /// `interval` — a span of months / days / microseconds.
    pub const INTERVAL: u32 = 1186;

    // ── Array (`T[]`) type OIDs, for a single array parameter sent to a
    //    `col = ANY($N)` in-list. Each is the `typarray` of the matching
    //    scalar above (PG `pg_type.typarray`). The drift-pin block below
    //    asserts each against its canonical catalog value, so a swapped
    //    pair (e.g. `INT4_ARRAY` set to the `_int8` value) fails the build.

    /// `bool[]` (`_bool`).
    pub const BOOL_ARRAY: u32 = 1000;
    /// `bytea[]` (`_bytea`).
    pub const BYTEA_ARRAY: u32 = 1001;
    /// `int2[]` (`_int2`).
    pub const INT2_ARRAY: u32 = 1005;
    /// `int4[]` (`_int4`).
    pub const INT4_ARRAY: u32 = 1007;
    /// `text[]` (`_text`).
    pub const TEXT_ARRAY: u32 = 1009;
    /// `int8[]` (`_int8`).
    pub const INT8_ARRAY: u32 = 1016;
    /// `float4[]` (`_float4`).
    pub const FLOAT4_ARRAY: u32 = 1021;
    /// `float8[]` (`_float8`).
    pub const FLOAT8_ARRAY: u32 = 1022;
    /// `oid[]` (`_oid`).
    pub const OID_ARRAY: u32 = 1028;
    /// `timestamp[]` (`_timestamp`).
    pub const TIMESTAMP_ARRAY: u32 = 1115;
    /// `timestamptz[]` (`_timestamptz`).
    pub const TIMESTAMPTZ_ARRAY: u32 = 1185;
    /// `json[]` (`_json`).
    pub const JSON_ARRAY: u32 = 199;
    /// `jsonb[]` (`_jsonb`).
    pub const JSONB_ARRAY: u32 = 3807;
    /// `uuid[]` (`_uuid`).
    pub const UUID_ARRAY: u32 = 2951;
    /// `numeric[]` (`_numeric`).
    pub const NUMERIC_ARRAY: u32 = 1231;
    /// `date[]` (`_date`).
    pub const DATE_ARRAY: u32 = 1182;
    /// `time[]` (`_time`).
    pub const TIME_ARRAY: u32 = 1183;
    /// `interval[]` (`_interval`).
    pub const INTERVAL_ARRAY: u32 = 1187;
    /// `bpchar[]` (`_bpchar`) — the array of `char(n)`.
    pub const BPCHAR_ARRAY: u32 = 1014;
    /// `varchar[]` (`_varchar`) — the array of `varchar(n)`.
    pub const VARCHAR_ARRAY: u32 = 1015;

    /// PostgreSQL's `FirstNormalObjectId` — the boundary between BUILT-IN type
    /// OIDs (all `< 16384`, assigned in the bootstrap catalog) and USER-DEFINED
    /// type OIDs (all `>= 16384`, assigned by `CREATE TYPE`/`DOMAIN`). The
    /// result-column OID guard uses this to SKIP a user-defined type (an
    /// enum/domain/composite, whose OID is server-assigned/dynamic and carries no
    /// compile-time pin — the existing user-type boundary) while checking every
    /// built-in column against its baked expected OID. Stable across PostgreSQL
    /// versions.
    pub const FIRST_NORMAL_OID: u32 = 16384;

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
        assert!(JSON == 114, "oids::JSON drift from pg_type.dat");
        assert!(UUID == 2950, "oids::UUID drift from pg_type.dat");
        assert!(JSONB == 3802, "oids::JSONB drift from pg_type.dat");
        assert!(NUMERIC == 1700, "oids::NUMERIC drift from pg_type.dat");
        assert!(DATE == 1082, "oids::DATE drift from pg_type.dat");
        assert!(TIME == 1083, "oids::TIME drift from pg_type.dat");
        assert!(INTERVAL == 1186, "oids::INTERVAL drift from pg_type.dat");
        // Array OIDs, verified against `pg_type` (`typname` -> `typarray`)
        // on PostgreSQL 15.
        assert!(BOOL_ARRAY == 1000, "oids::BOOL_ARRAY drift from pg_type.dat");
        assert!(BYTEA_ARRAY == 1001, "oids::BYTEA_ARRAY drift from pg_type.dat");
        assert!(INT2_ARRAY == 1005, "oids::INT2_ARRAY drift from pg_type.dat");
        assert!(INT4_ARRAY == 1007, "oids::INT4_ARRAY drift from pg_type.dat");
        assert!(TEXT_ARRAY == 1009, "oids::TEXT_ARRAY drift from pg_type.dat");
        assert!(INT8_ARRAY == 1016, "oids::INT8_ARRAY drift from pg_type.dat");
        assert!(FLOAT4_ARRAY == 1021, "oids::FLOAT4_ARRAY drift from pg_type.dat");
        assert!(FLOAT8_ARRAY == 1022, "oids::FLOAT8_ARRAY drift from pg_type.dat");
        assert!(OID_ARRAY == 1028, "oids::OID_ARRAY drift from pg_type.dat");
        assert!(TIMESTAMP_ARRAY == 1115, "oids::TIMESTAMP_ARRAY drift from pg_type.dat");
        assert!(TIMESTAMPTZ_ARRAY == 1185, "oids::TIMESTAMPTZ_ARRAY drift from pg_type.dat");
        assert!(JSON_ARRAY == 199, "oids::JSON_ARRAY drift from pg_type.dat");
        assert!(JSONB_ARRAY == 3807, "oids::JSONB_ARRAY drift from pg_type.dat");
        assert!(UUID_ARRAY == 2951, "oids::UUID_ARRAY drift from pg_type.dat");
        assert!(NUMERIC_ARRAY == 1231, "oids::NUMERIC_ARRAY drift from pg_type.dat");
        assert!(DATE_ARRAY == 1182, "oids::DATE_ARRAY drift from pg_type.dat");
        assert!(TIME_ARRAY == 1183, "oids::TIME_ARRAY drift from pg_type.dat");
        assert!(INTERVAL_ARRAY == 1187, "oids::INTERVAL_ARRAY drift from pg_type.dat");
        assert!(BPCHAR_ARRAY == 1014, "oids::BPCHAR_ARRAY drift from pg_type.dat");
        assert!(VARCHAR_ARRAY == 1015, "oids::VARCHAR_ARRAY drift from pg_type.dat");
        assert!(FIRST_NORMAL_OID == 16384, "oids::FIRST_NORMAL_OID drift from PG FirstNormalObjectId");
    };
}

/// Classify a result column's RUNTIME type OID against the carrier's COMPILE-TIME
/// expected OID for the typed `query!` result-schema guard.
///
/// Returns `true` when the runtime column is safe to decode with the expected
/// type's decoder — i.e. NOT a silent mis-decode:
///
/// - `found >= FIRST_NORMAL_OID` — a user-defined type (enum/domain/composite).
///   Its OID is server-assigned/dynamic and carries no compile-time pin (a domain
///   column reports its OWN OID, not its base's), so it is SKIPPED, exactly the
///   existing user-type boundary (the runtime already validates an enum label /
///   composite arity). A native column shadowed at a MISS by a user-defined type
///   of a different base is the sole residual of this skip — extremely exotic.
/// - the `text` family (`text` / `varchar` / `bpchar`, and their arrays) shares
///   ONE wire decode (raw UTF-8 bytes), so a `varchar`/`bpchar` column decoded by
///   the `text` marker is CORRECT, not a mismatch — they canonicalize to one class.
/// - otherwise EXACT equality — any other runtime OID differing from the expected
///   is a real type divergence (the reproduced `text`↔`int4` shadow).
#[must_use]
pub(crate) fn result_oid_compatible(expected: u32, found: u32) -> bool {
    // A user-defined runtime type is dynamic-OID: skip (honest boundary).
    if found >= oids::FIRST_NORMAL_OID {
        return true;
    }
    result_oid_class(expected) == result_oid_class(found)
}

/// Canonicalize a built-in OID into its wire-decode equivalence class: the
/// blank-padded / varying CHAR family (`varchar` 1043, `bpchar` 1042) collapses
/// to `text` (25), and their arrays (`varchar[]` 1015, `bpchar[]` 1014) collapse
/// to `text[]` (1009), since all decode identically (raw UTF-8). Every other OID
/// is its own class (identity), so the guard is EXACT equality outside the CHAR
/// family.
#[must_use]
fn result_oid_class(oid: u32) -> u32 {
    match oid {
        oids::VARCHAR | oids::BPCHAR => oids::TEXT,
        oids::VARCHAR_ARRAY | oids::BPCHAR_ARRAY => oids::TEXT_ARRAY,
        other => other,
    }
}

#[cfg(test)]
mod parse_tests {
    //! `parse_row_description` conformance per PG §55.7 + bad-path
    //! classification. Category (1)/(B) —
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
    /// is guaranteed by `MAX_ROW_COLUMNS = 1664 ≪ i16::MAX`. The
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
    //! `Cell<TextFmt>` impls — per-type text-format decoding plus the
    //! bad-path classification matrix (non-UTF-8, unparsable digits,
    //! overflow, non-canonical bool).

    use super::*;

    /// **One invariant, one test**: `<i32 as Cell<TextFmt>>::decode`
    /// correctly maps PG text representation into the
    /// Result<i32, DecodeError> contract — happy paths, overflow,
    /// malformed digits, non-ASCII. An arm-body swap in my impl
    /// (e.g., returning `NonUtf8` for overflow) fails this table.
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
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"0"), Ok(0)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"42"), Ok(42)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"-17"), Ok(-17)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"+17"), Ok(17)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"2147483647"), Ok(i32::MAX)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"-2147483648"), Ok(i32::MIN)));

        // Overflow → IntParse.
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"2147483648"), Err(DecodeError::IntParse)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"-2147483649"), Err(DecodeError::IntParse)));

        // Garbage → IntParse (empty, non-digit, trailing, whitespace).
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b""), Err(DecodeError::IntParse)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"abc"), Err(DecodeError::IntParse)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b"12a"), Err(DecodeError::IntParse)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(b" 12"), Err(DecodeError::IntParse)));

        // Non-ASCII bytes → IntParse (single-pass ASCII-digit
        // validator treats any non-digit byte uniformly).
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(&[0xFF]), Err(DecodeError::IntParse)));
        assert!(matches!(<i32 as Cell<TextFmt>>::decode(&[0xC3, 0x28]), Err(DecodeError::IntParse)));
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
        assert_eq!(<i16 as Cell<TextFmt>>::decode(b"0"), Ok(0i16));
        assert_eq!(<i16 as Cell<TextFmt>>::decode(b"1"), Ok(1i16));
        assert_eq!(<i16 as Cell<TextFmt>>::decode(b"-1"), Ok(-1i16));
        // i32 fast-paths.
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"0"), Ok(0i32));
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"1"), Ok(1i32));
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"-1"), Ok(-1i32));
        // i64 fast-paths.
        assert_eq!(<i64 as Cell<TextFmt>>::decode(b"0"), Ok(0i64));
        assert_eq!(<i64 as Cell<TextFmt>>::decode(b"1"), Ok(1i64));
        assert_eq!(<i64 as Cell<TextFmt>>::decode(b"-1"), Ok(-1i64));

        // Near-misses that MUST fall through to the digit loop and
        // return correctly (not the fast-path's literal). A bug
        // where fast-path matched too eagerly would break these.
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"01"), Ok(1i32)); // leading zero
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"10"), Ok(10i32));
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"-10"), Ok(-10i32));
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"+1"), Ok(1i32)); // explicit +
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"+0"), Ok(0i32));
    }

    /// **One invariant, one test**: parallel `i16` / `i64` / `u32`
    /// impls delegate to stdlib `FromStr` with per-type ranges and
    /// map failures to `IntParse`. Catches macro-expansion errors
    /// where a type's impl would mis-wire to another's range.
    #[test]
    fn other_integer_decoders_matrix() {
        // i16 boundaries.
        assert!(matches!(<i16 as Cell<TextFmt>>::decode(b"32767"), Ok(i16::MAX)));
        assert!(matches!(<i16 as Cell<TextFmt>>::decode(b"-32768"), Ok(i16::MIN)));
        assert!(matches!(<i16 as Cell<TextFmt>>::decode(b"32768"), Err(DecodeError::IntParse)));

        // i64 boundaries.
        assert!(matches!(<i64 as Cell<TextFmt>>::decode(b"9223372036854775807"), Ok(i64::MAX)));
        assert!(matches!(<i64 as Cell<TextFmt>>::decode(b"9223372036854775808"), Err(DecodeError::IntParse)));

        // u32 boundaries + negative rejection.
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"0"), Ok(0)));
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"4294967295"), Ok(u32::MAX)));
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"4294967296"), Err(DecodeError::IntParse)));
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"-1"), Err(DecodeError::IntParse)));
    }

    /// **One invariant, one test**: `<bool as Cell<TextFmt>>::decode`
    /// accepts **exactly** PG's canonical `"t"` / `"f"` wire form —
    /// nothing else. PG server is strict on wire format; lax parsers
    /// that accept `"true"` / `"1"` / etc. would mask protocol desync
    /// if the server ever switched to a non-standard encoding.
    #[test]
    fn bool_decoder_matrix() {
        // Canonical accepts.
        assert!(matches!(<bool as Cell<TextFmt>>::decode(b"t"), Ok(true)));
        assert!(matches!(<bool as Cell<TextFmt>>::decode(b"f"), Ok(false)));

        // Every non-canonical form (including common false-friends
        // from SQL literal contexts) must classify as BoolParse, NOT
        // be coerced.
        for bad in [
            &b"true"[..], &b"false"[..], &b"TRUE"[..], &b"T"[..], &b"F"[..],
            &b"1"[..], &b"0"[..], &b"yes"[..], &b"no"[..], &b""[..],
        ] {
            assert!(
                matches!(<bool as Cell<TextFmt>>::decode(bad), Err(DecodeError::BoolParse)),
                "expected BoolParse for {bad:?}",
            );
        }
    }

    /// **One invariant, one test**: `<&str as Cell<TextFmt>>::decode`
    /// is a zero-copy UTF-8 validator. Output pointer must equal input
    /// pointer (no internal copy); non-UTF-8 input classifies as
    /// `NonUtf8`; empty input is valid.
    #[test]
    fn str_decoder_matrix() {
        let bytes: &[u8] = b"hello world";
        let result = <&str as Cell<TextFmt>>::decode(bytes);
        assert!(matches!(result, Ok("hello world")));
        if let Ok(s) = result {
            // Zero-copy invariant — the returned &str borrows the
            // same memory region as the input &[u8].
            assert_eq!(s.as_ptr(), bytes.as_ptr());
        }

        // Empty is valid.
        assert!(matches!(<&str as Cell<TextFmt>>::decode(b""), Ok("")));

        // Non-UTF-8 (lone continuation byte).
        assert!(matches!(<&str as Cell<TextFmt>>::decode(&[0x80]), Err(DecodeError::NonUtf8)));
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

    // `OutOfRange::Display` carries the offending idx + max — used
    // by future operator diagnostics. Pin the format so a body swap
    // (idx vs max) is caught.
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
    fn wide_row_description_max_cols_alternating_formats() {
        use alloc::vec::Vec;
        let mut frame: Vec<u8> = Vec::new();
        // MAX_ROW_COLUMNS = 1664 fits i16 trivially; const-asserts in
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
    //! Type-level (T, F) pair dispatch via the unified generic-F
    //! `Cell<F>` trait.
    //!
    //! Tests cover:
    //! - each `(T, F)` pair round-trips correctly (14 cases:
    //!   7 type slots × 2 format markers),
    //! - OID consistency between `Cell<F>::OID` and the canonical
    //!   `oids::*` constants (additional to the compile-time
    //!   const-asserts above; these runtime checks pin the assert
    //!   blocks against accidental removal),
    //! - `Fmt::WIRE` produces correct `FormatCode`,
    //! - `decode_with_format` dispatches the right impl on a
    //!   runtime `FormatCode`.
    use super::*;

    #[test]
    fn markers_wire_consts() {
        assert_eq!(<TextFmt as Fmt>::WIRE, FormatCode::Text);
        assert_eq!(<BinaryFmt as Fmt>::WIRE, FormatCode::Binary);
    }

    #[test]
    fn text_round_trips() {
        assert_eq!(<i16 as Cell<TextFmt>>::decode(b"42"), Ok(42_i16));
        assert_eq!(<i32 as Cell<TextFmt>>::decode(b"-1234567"), Ok(-1_234_567_i32));
        assert_eq!(<i64 as Cell<TextFmt>>::decode(b"9223372036854775807"), Ok(9_223_372_036_854_775_807_i64));
        assert_eq!(<u32 as Cell<TextFmt>>::decode(b"4294967295"), Ok(u32::MAX));
        assert_eq!(<bool as Cell<TextFmt>>::decode(b"t"), Ok(true));
        assert_eq!(<bool as Cell<TextFmt>>::decode(b"f"), Ok(false));
        assert_eq!(<&str as Cell<TextFmt>>::decode(b"hello"), Ok("hello"));
    }

    #[test]
    fn binary_round_trips() {
        assert_eq!(<i16 as Cell<BinaryFmt>>::decode(&42_i16.to_be_bytes()), Ok(42_i16));
        assert_eq!(<i32 as Cell<BinaryFmt>>::decode(&(-1_234_567_i32).to_be_bytes()), Ok(-1_234_567_i32));
        assert_eq!(<i64 as Cell<BinaryFmt>>::decode(&i64::MAX.to_be_bytes()), Ok(i64::MAX));
        assert_eq!(<u32 as Cell<BinaryFmt>>::decode(&u32::MAX.to_be_bytes()), Ok(u32::MAX));
        assert_eq!(<bool as Cell<BinaryFmt>>::decode(&[1]), Ok(true));
        assert_eq!(<bool as Cell<BinaryFmt>>::decode(&[0]), Ok(false));
        assert_eq!(<&str as Cell<BinaryFmt>>::decode(b"hello"), Ok("hello"));
    }

    #[test]
    fn binary_round_trips_float_and_bytea() {
        // f32 / f64: exact IEEE-754 big-endian bytes, compared BIT-for-bit (a
        // wire round-trip must be bit-identical — and the crate forbids the
        // float `==` a value comparison would need).
        assert_eq!(
            <f32 as Cell<BinaryFmt>>::decode(&1.5_f32.to_be_bytes()).map(f32::to_bits),
            Ok(1.5_f32.to_bits()),
        );
        assert_eq!(
            <f64 as Cell<BinaryFmt>>::decode(&1234.5_f64.to_be_bytes()).map(f64::to_bits),
            Ok(1234.5_f64.to_bits()),
        );
        // bytea: the identity on the column body — every length, including empty.
        assert_eq!(
            <&[u8] as Cell<BinaryFmt>>::decode(&[0xDE, 0xAD, 0xBE, 0xEF]),
            Ok(&[0xDE, 0xAD, 0xBE, 0xEF][..]),
        );
        assert_eq!(<&[u8] as Cell<BinaryFmt>>::decode(&[]), Ok(&[][..]));
    }

    #[test]
    fn binary_float_wrong_length_is_classified() {
        // A 4-byte body for an f64 (or 8 for an f32) is a classified length
        // error — never a silent width coercion between the two float widths.
        assert!(matches!(
            <f64 as Cell<BinaryFmt>>::decode(&[0, 0, 0, 0]),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 8, actual_len: 4 }),
        ));
        assert!(matches!(
            <f32 as Cell<BinaryFmt>>::decode(&[0, 0, 0, 0, 0, 0, 0, 0]),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 4, actual_len: 8 }),
        ));
    }

    #[test]
    fn encode_binary_float_and_bytea_round_trips() {
        // `encode_to` writes exactly the bytes the decoder reads back.
        let mut buf = crate::write_buf::WriteBuf::new();
        assert!(<f64 as EncodeBinary>::encode_to(&3.25_f64, &mut buf).is_ok());
        assert_eq!(
            <f64 as Cell<BinaryFmt>>::decode(buf.as_bytes()).map(f64::to_bits),
            Ok(3.25_f64.to_bits()),
        );

        let mut buf = crate::write_buf::WriteBuf::new();
        assert!(<f32 as EncodeBinary>::encode_to(&0.5_f32, &mut buf).is_ok());
        assert_eq!(
            <f32 as Cell<BinaryFmt>>::decode(buf.as_bytes()).map(f32::to_bits),
            Ok(0.5_f32.to_bits()),
        );

        let mut buf = crate::write_buf::WriteBuf::new();
        assert!(<&[u8] as EncodeBinary>::encode_to(&&[1u8, 2, 3][..], &mut buf).is_ok());
        assert_eq!(
            <&[u8] as Cell<BinaryFmt>>::decode(buf.as_bytes()),
            Ok(&[1u8, 2, 3][..]),
        );
    }

    #[test]
    fn oid_consistency_text() {
        // Runtime double-check of the compile-time const-asserts.
        // Removing the assert block would not be caught by compile,
        // but THIS test would still fail.
        assert_eq!(<i16 as Cell<TextFmt>>::OID, oids::INT2);
        assert_eq!(<i32 as Cell<TextFmt>>::OID, oids::INT4);
        assert_eq!(<i64 as Cell<TextFmt>>::OID, oids::INT8);
        assert_eq!(<u32 as Cell<TextFmt>>::OID, oids::OID);
        assert_eq!(<bool as Cell<TextFmt>>::OID, oids::BOOL);
        assert_eq!(<&str as Cell<TextFmt>>::OID, oids::TEXT);
    }

    #[test]
    fn oid_consistency_binary() {
        assert_eq!(<i16 as Cell<BinaryFmt>>::OID, oids::INT2);
        assert_eq!(<i32 as Cell<BinaryFmt>>::OID, oids::INT4);
        assert_eq!(<i64 as Cell<BinaryFmt>>::OID, oids::INT8);
        assert_eq!(<u32 as Cell<BinaryFmt>>::OID, oids::OID);
        assert_eq!(<bool as Cell<BinaryFmt>>::OID, oids::BOOL);
        assert_eq!(<&str as Cell<BinaryFmt>>::OID, oids::TEXT);
    }

    #[test]
    fn oid_text_binary_symmetry() {
        // Same Rust type → same PG type OID across text/binary.
        // (Already const-asserted on the `Cell<TextFmt>`/`Cell<BinaryFmt>`
        // pair; mirrored here for explicit runtime drift detection.)
        assert_eq!(
            <i16 as Cell<TextFmt>>::OID,
            <i16 as Cell<BinaryFmt>>::OID,
            "i16 OID skew between text and binary Cell impls",
        );
        assert_eq!(
            <i32 as Cell<TextFmt>>::OID,
            <i32 as Cell<BinaryFmt>>::OID,
        );
        assert_eq!(
            <i64 as Cell<TextFmt>>::OID,
            <i64 as Cell<BinaryFmt>>::OID,
        );
        assert_eq!(
            <u32 as Cell<TextFmt>>::OID,
            <u32 as Cell<BinaryFmt>>::OID,
        );
        assert_eq!(
            <bool as Cell<TextFmt>>::OID,
            <bool as Cell<BinaryFmt>>::OID,
        );
        assert_eq!(
            <&str as Cell<TextFmt>>::OID,
            <&str as Cell<BinaryFmt>>::OID,
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
        // Invalid text bool — `Cell<TextFmt>::decode` returns `BoolParse`.
        let r: Result<bool, _> = decode_with_format(b"yes", FormatCode::Text);
        assert!(matches!(r, Err(DecodeError::BoolParse)));
        // Invalid binary i32 (wrong length) — `BinaryLengthMismatch`.
        let r: Result<i32, _> = decode_with_format(&[0, 1, 2], FormatCode::Binary);
        assert!(matches!(r, Err(DecodeError::BinaryLengthMismatch { expected_len: 4, .. })));
    }

}

#[cfg(test)]
mod session_2025_05_25_tests {
    use super::*;

    #[test]
    fn u32_from_pg_text_common_value_zero() {
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"0"), Ok(0)));
    }

    #[test]
    fn u32_from_pg_text_common_value_one() {
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"1"), Ok(1)));
    }

    #[test]
    fn u32_from_pg_text_regular_value() {
        assert!(matches!(<u32 as Cell<TextFmt>>::decode(b"42"), Ok(42)));
    }

    #[test]
    fn str_from_pg_binary_valid_utf8() {
        assert!(matches!(<&str as Cell<BinaryFmt>>::decode(b"hello"), Ok("hello")));
    }

    #[test]
    fn str_from_pg_binary_invalid_utf8() {
        let bytes: &[u8] = &[0xFF, 0xFE];
        assert!(<&str as Cell<BinaryFmt>>::decode(bytes).is_err());
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
    fn parse_param_description_zero_params() {
        // A no-param prepared statement: count 0, no OIDs.
        assert_eq!(parse_param_description(&[0, 0]), Some(alloc::vec::Vec::new()));
    }

    #[test]
    fn parse_param_description_three_params() {
        let mut payload = alloc::vec::Vec::new();
        payload.extend_from_slice(&3i16.to_be_bytes());
        payload.extend_from_slice(&23u32.to_be_bytes()); // int4
        payload.extend_from_slice(&25u32.to_be_bytes()); // text
        payload.extend_from_slice(&0u32.to_be_bytes()); // unspecified (server could not infer)
        assert_eq!(parse_param_description(&payload), Some(alloc::vec![23, 25, 0]));
    }

    #[test]
    fn parse_param_description_negative_count_rejected() {
        assert_eq!(parse_param_description(&[0xFF, 0xFF]), None); // i16 = -1
    }

    #[test]
    fn parse_param_description_truncated_oid_rejected() {
        // Declares 1 param but only 3 of the 4 OID bytes present.
        let mut payload = alloc::vec::Vec::new();
        payload.extend_from_slice(&1i16.to_be_bytes());
        payload.extend_from_slice(&[0, 0, 23]);
        assert_eq!(parse_param_description(&payload), None);
    }

    #[test]
    fn parse_param_description_trailing_bytes_rejected() {
        // Declares 0 params but carries a trailing byte — framing desync.
        assert_eq!(parse_param_description(&[0, 0, 0xAB]), None);
    }

    #[test]
    fn parse_param_description_short_header_rejected() {
        assert_eq!(parse_param_description(&[0]), None);
        assert_eq!(parse_param_description(&[]), None);
    }

    #[test]
    fn parse_param_description_over_cap_rejected() {
        // A count beyond MAX_ROW_COLUMNS is rejected before allocating.
        let over = i16::try_from(MAX_ROW_COLUMNS + 1).unwrap_or(i16::MAX);
        let payload = over.to_be_bytes();
        assert_eq!(parse_param_description(&payload), None);
    }
}

#[cfg(test)]
mod semantic_type_decode_tests {
    //! Direct `Cell<BinaryFmt>::decode` bad-path classification for the
    //! bsql-native semantic types. Seals the no-swallow contract on every
    //! malformed arm BY TEST (not by inspection): each wrong-shape payload is
    //! a specific classified `DecodeError`, never a silent default, panic, or
    //! truncation. The crate-root forbid bundle bans `unwrap`/`expect`/`panic`
    //! even in tests, so classification is asserted via `matches!`.
    use super::{BinaryFmt, Cell, DecodeError};
    use crate::pgtypes::{Json, Jsonb, Timestamp, Timestamptz, Uuid};

    #[test]
    fn timestamptz_wrong_length_is_classified() {
        // 7 bytes for an 8-byte `i64` micros payload.
        let seven = [0u8; 7];
        let decoded = <Timestamptz as Cell<BinaryFmt>>::decode(&seven);
        assert!(matches!(
            decoded,
            Err(DecodeError::BinaryLengthMismatch { expected_len: 8, actual_len: 7 })
        ));
        // The naive `timestamp` shares the width contract.
        assert!(matches!(
            <Timestamp as Cell<BinaryFmt>>::decode(&seven),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 8, actual_len: 7 })
        ));
    }

    #[test]
    fn uuid_over_length_is_classified() {
        // 17 bytes: `first_chunk::<16>` SUCCEEDS, then the `bytes.len() == 16`
        // filter rejects — the distinct over-length branch (an under-length
        // 15-byte payload fails `first_chunk` instead).
        let seventeen = [0u8; 17];
        assert!(matches!(
            <Uuid as Cell<BinaryFmt>>::decode(&seventeen),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 16, actual_len: 17 })
        ));
        // Under-length is classified too (the `first_chunk` failure branch).
        let fifteen = [0u8; 15];
        assert!(matches!(
            <Uuid as Cell<BinaryFmt>>::decode(&fifteen),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 16, actual_len: 15 })
        ));
    }

    #[test]
    fn json_non_utf8_is_classified() {
        // A lone 0xFF continuation byte is not valid UTF-8.
        let bad = [0xFFu8, 0x00, 0x01];
        assert!(matches!(
            <Json as Cell<BinaryFmt>>::decode(&bad),
            Err(DecodeError::NonUtf8)
        ));
    }

    #[test]
    fn jsonb_empty_body_is_classified_missing_version() {
        // A zero-length body has no leading version byte at all.
        let empty: &[u8] = &[];
        assert!(matches!(
            <Jsonb as Cell<BinaryFmt>>::decode(empty),
            Err(DecodeError::JsonbHeaderInvalid { version: None })
        ));
    }

    #[test]
    fn jsonb_wrong_version_byte_is_classified() {
        // Leading byte 2 (a future/unknown jsonb version) — classified with
        // the offending byte, never silently decoded as text.
        let v2: &[u8] = &[0x02, b'{', b'}'];
        assert!(matches!(
            <Jsonb as Cell<BinaryFmt>>::decode(v2),
            Err(DecodeError::JsonbHeaderInvalid { version: Some(2) })
        ));
    }

    #[test]
    fn jsonb_non_utf8_after_version_is_classified() {
        // Valid version byte, but the text after it is not UTF-8.
        let bad: &[u8] = &[0x01, 0xFF, 0xFE];
        assert!(matches!(
            <Jsonb as Cell<BinaryFmt>>::decode(bad),
            Err(DecodeError::NonUtf8)
        ));
    }

    #[test]
    fn well_formed_payloads_decode() {
        // The happy paths, so the bad-path asserts above are not vacuously
        // green against a decoder that rejects everything.
        assert!(matches!(
            <Timestamptz as Cell<BinaryFmt>>::decode(&1_000_000_i64.to_be_bytes()),
            Ok(v) if v.as_micros() == 1_000_000
        ));
        assert!(<Uuid as Cell<BinaryFmt>>::decode(&[0u8; 16]).is_ok());
        assert!(matches!(
            <Jsonb as Cell<BinaryFmt>>::decode(&[0x01, b'{', b'}']),
            Ok(ref v) if v.as_str() == "{}"
        ));
    }
}

#[cfg(test)]
mod numeric_wire_tests {
    //! `numeric` binary wire round-trip + byte-layout + bad-path classification.
    //! Precision-critical: a numeric decode bug is silently-wrong money, so the
    //! encode/decode identity is proven for a WIDE battery (including
    //! arbitrary-precision past `i128`, `NaN`, `±Infinity`), the decode is
    //! pinned against hand-built PostgreSQL wire bytes (not just self-consistent
    //! with the encoder), and every malformed frame is a specific classified
    //! `DecodeError`, never a panic or silent value.
    extern crate alloc;
    use super::{BinaryFmt, Cell, DecodeError, EncodeBinary};
    use crate::pgtypes::Numeric;
    use crate::write_buf::WriteBuf;
    use alloc::string::ToString as _;
    use alloc::vec::Vec;
    use core::str::FromStr as _;

    /// Encode a `Numeric` to its binary wire bytes.
    fn encode(n: &Numeric) -> Vec<u8> {
        let mut buf = WriteBuf::new();
        assert!(n.encode_to(&mut buf).is_ok(), "encode fits the buffer");
        buf.as_bytes().to_vec()
    }

    /// Every battery value survives encode -> decode as the IDENTITY, and the
    /// decoded value renders the same string — the bit-exact round-trip proof,
    /// dependency-free.
    #[test]
    fn encode_decode_round_trips_identity() {
        for s in [
            "0",
            "1",
            "-1",
            "0.1",
            "0.0001",
            "3.14159265358979323846",
            "1.500",
            "0.000",
            "-123456789012345678901234567890", // > i128 magnitude
            "9999999999999999999999999999999999999999.0001",
            "100000001", // interior zero group
            "NaN",
            "Infinity",
            "-Infinity",
        ] {
            let original = Numeric::from_str(s).expect("battery value parses");
            let bytes = encode(&original);
            let decoded =
                <Numeric as Cell<BinaryFmt>>::decode(&bytes).expect("wire bytes decode");
            assert_eq!(decoded, original, "round-trip identity failed for {s}");
            assert_eq!(decoded.to_string(), s, "round-trip display failed for {s}");
        }
    }

    /// Decode is pinned against HAND-BUILT PostgreSQL wire bytes (`1.5` =
    /// ndigits 2, weight 0, sign 0x0000, dscale 1, digits [1, 5000]) — proving
    /// the byte layout matches `numeric_send`, not merely the encoder.
    #[test]
    fn decode_matches_hand_built_pg_bytes() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u16.to_be_bytes()); // ndigits
        bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
        bytes.extend_from_slice(&0x0000u16.to_be_bytes()); // sign = positive
        bytes.extend_from_slice(&1u16.to_be_bytes()); // dscale
        bytes.extend_from_slice(&1u16.to_be_bytes()); // digit group 0
        bytes.extend_from_slice(&5000u16.to_be_bytes()); // digit group 1
        let decoded = <Numeric as Cell<BinaryFmt>>::decode(&bytes).expect("decodes");
        assert_eq!(decoded.to_string(), "1.5");
        assert_eq!(decoded, Numeric::from_str("1.5").expect("parses"));
    }

    /// The special sign words decode to the correct non-finite value.
    #[test]
    fn decode_specials() {
        for (sign, expected) in [
            (0xC000u16, Numeric::nan()),
            (0xD000u16, Numeric::infinity()),
            (0xF000u16, Numeric::neg_infinity()),
        ] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&0u16.to_be_bytes()); // ndigits = 0
            bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
            bytes.extend_from_slice(&sign.to_be_bytes());
            bytes.extend_from_slice(&0u16.to_be_bytes()); // dscale
            let decoded = <Numeric as Cell<BinaryFmt>>::decode(&bytes).expect("decodes");
            assert_eq!(decoded, expected);
        }
    }

    /// Every malformed frame is a CLASSIFIED error, never a panic or a value
    /// with an impossible digit.
    #[test]
    fn decode_bad_paths_classified() {
        // Too short for the 8-byte header.
        assert!(matches!(
            <Numeric as Cell<BinaryFmt>>::decode(&[0, 0, 0, 0, 0]),
            Err(DecodeError::NumericTruncated)
        ));
        // Unknown sign word.
        let mut bad_sign = Vec::new();
        bad_sign.extend_from_slice(&0u16.to_be_bytes());
        bad_sign.extend_from_slice(&0i16.to_be_bytes());
        bad_sign.extend_from_slice(&0x1234u16.to_be_bytes());
        bad_sign.extend_from_slice(&0u16.to_be_bytes());
        assert!(matches!(
            <Numeric as Cell<BinaryFmt>>::decode(&bad_sign),
            Err(DecodeError::NumericInvalidSign { sign: 0x1234 })
        ));
        // A digit group >= 10000.
        let mut bad_digit = Vec::new();
        bad_digit.extend_from_slice(&1u16.to_be_bytes()); // ndigits = 1
        bad_digit.extend_from_slice(&0i16.to_be_bytes());
        bad_digit.extend_from_slice(&0u16.to_be_bytes());
        bad_digit.extend_from_slice(&0u16.to_be_bytes());
        bad_digit.extend_from_slice(&10_000u16.to_be_bytes()); // out of range
        assert!(matches!(
            <Numeric as Cell<BinaryFmt>>::decode(&bad_digit),
            Err(DecodeError::NumericDigitOutOfRange { digit: 10_000 })
        ));
        // Trailing surplus past the declared digit groups (no-swallow).
        let mut surplus = Vec::new();
        surplus.extend_from_slice(&0u16.to_be_bytes()); // ndigits = 0
        surplus.extend_from_slice(&0i16.to_be_bytes());
        surplus.extend_from_slice(&0u16.to_be_bytes());
        surplus.extend_from_slice(&0u16.to_be_bytes());
        surplus.extend_from_slice(&0xABu16.to_be_bytes()); // one group too many
        assert!(matches!(
            <Numeric as Cell<BinaryFmt>>::decode(&surplus),
            Err(DecodeError::NumericTruncated)
        ));
    }

    /// A display scale with a bit above the 14-bit `NUMERIC_DSCALE_MASK` range
    /// is REJECTED, exactly as PostgreSQL's `numeric_recv` does ("invalid scale
    /// in external numeric value") — never silently masked into a different
    /// rendering. `0x8001` has the high bit set, so it is classified rather than
    /// stored verbatim (which would render 32769 fractional digits) or masked to
    /// `0x0001` (which would silently accept a frame PG rejects).
    #[test]
    fn decode_rejects_high_bit_dscale() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_be_bytes()); // ndigits = 1
        bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
        bytes.extend_from_slice(&0x0000u16.to_be_bytes()); // sign = positive
        bytes.extend_from_slice(&0x8001u16.to_be_bytes()); // dscale, high bit set
        bytes.extend_from_slice(&5u16.to_be_bytes()); // one digit group
        assert!(matches!(
            <Numeric as Cell<BinaryFmt>>::decode(&bytes),
            Err(DecodeError::NumericInvalidScale { dscale: 0x8001 })
        ));
        // The largest in-range scale (16383) is accepted.
        let mut ok = Vec::new();
        ok.extend_from_slice(&0u16.to_be_bytes()); // ndigits = 0 (zero value)
        ok.extend_from_slice(&0i16.to_be_bytes());
        ok.extend_from_slice(&0x0000u16.to_be_bytes());
        ok.extend_from_slice(&0x3FFFu16.to_be_bytes()); // dscale = 16383
        assert!(<Numeric as Cell<BinaryFmt>>::decode(&ok).is_ok());
    }
}

#[cfg(test)]
mod temporal_wire_tests {
    //! `date` / `time` / `interval` binary wire round-trip + byte-layout +
    //! bad-path classification. A date off-by-one is a wrong calendar day, so
    //! the decode is pinned against HAND-BUILT PostgreSQL wire bytes (the exact
    //! `date_send` / `time_send` / `interval_send` layout), the encode/decode
    //! identity is proven across the sentinels and edge values, and a wrong
    //! length is a classified `BinaryLengthMismatch`, never a panic or a
    //! partial read.
    extern crate alloc;
    use super::{oids, BinaryFmt, Cell, DecodeError, EncodeBinary};
    use crate::pgtypes::{Date, Interval, Time};
    use crate::write_buf::WriteBuf;
    use alloc::vec::Vec;

    /// Encode any `EncodeBinary` value to its wire bytes.
    fn encode<T: EncodeBinary>(v: &T) -> Vec<u8> {
        let mut buf = WriteBuf::new();
        assert!(v.encode_to(&mut buf).is_ok(), "encode fits the buffer");
        buf.as_bytes().to_vec()
    }

    /// `date` encode/decode is the identity across ordinary days, the epoch,
    /// pre-epoch days, and BOTH `±infinity` sentinels — and the encoded bytes
    /// are exactly the 4-byte big-endian day count `date_send` emits.
    #[test]
    fn date_round_trips_and_matches_wire() {
        for days in [0_i32, 59, -1, -730_119, 2_921_939, i32::MAX, i32::MIN] {
            let d = Date::from_days(days);
            let bytes = encode(&d);
            assert_eq!(bytes, days.to_be_bytes(), "wire bytes for days {days}");
            let decoded = <Date as Cell<BinaryFmt>>::decode(&bytes).expect("decodes");
            assert_eq!(decoded, d, "round-trip identity for days {days}");
        }
        assert_eq!(<Date as Cell<BinaryFmt>>::OID, oids::DATE);
    }

    /// `time` encode/decode is the identity, and the bytes are the 8-byte
    /// big-endian microsecond count `time_send` emits.
    #[test]
    fn time_round_trips_and_matches_wire() {
        for micros in [0_i64, 45_296_789_012, 86_399_999_999, 86_400_000_000] {
            let t = Time::from_micros(micros);
            let bytes = encode(&t);
            assert_eq!(bytes, micros.to_be_bytes(), "wire bytes for {micros} micros");
            let decoded = <Time as Cell<BinaryFmt>>::decode(&bytes).expect("decodes");
            assert_eq!(decoded, t, "round-trip identity for {micros} micros");
        }
        assert_eq!(<Time as Cell<BinaryFmt>>::OID, oids::TIME);
    }

    /// `interval` decode is pinned against HAND-BUILT PostgreSQL wire bytes:
    /// `1 year 2 mons 3 days 04:05:06` is `interval_send` order micros (i64) =
    /// 14_706_000_000, days (i32) = 3, months (i32) = 14. The three fields are
    /// stored separately — never collapsed.
    #[test]
    fn interval_decode_matches_hand_built_pg_bytes() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&14_706_000_000_i64.to_be_bytes()); // micros
        bytes.extend_from_slice(&3_i32.to_be_bytes()); // days
        bytes.extend_from_slice(&14_i32.to_be_bytes()); // months
        let decoded = <Interval as Cell<BinaryFmt>>::decode(&bytes).expect("decodes");
        assert_eq!(decoded, Interval::new(14, 3, 14_706_000_000));
        // And the encoder reproduces those exact 16 bytes.
        assert_eq!(encode(&decoded), bytes);
    }

    /// `interval` encode/decode identity over positive / negative / mixed-sign
    /// field combinations (the three fields are independent).
    #[test]
    fn interval_round_trips_identity() {
        for (months, days, micros) in [
            (0_i32, 0_i32, 0_i64),
            (14, 3, 14_706_000_000),
            (0, -1, 0),
            (1200, 0, 0),
            (-14, 0, 0),
            (0, 1, -1),
            (10, 3, -14_706_000_000),
        ] {
            let i = Interval::new(months, days, micros);
            let decoded = <Interval as Cell<BinaryFmt>>::decode(&encode(&i)).expect("decodes");
            assert_eq!(decoded, i, "round-trip for ({months},{days},{micros})");
        }
        assert_eq!(<Interval as Cell<BinaryFmt>>::OID, oids::INTERVAL);
    }

    /// A wrong body length for each fixed-width temporal type is a classified
    /// `BinaryLengthMismatch` (no panic, no partial read), never accepted.
    #[test]
    fn wrong_length_is_classified() {
        // date wants 4 bytes.
        assert!(matches!(
            <Date as Cell<BinaryFmt>>::decode(&[0, 0, 0]),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 4, actual_len: 3 })
        ));
        // time wants 8 bytes.
        assert!(matches!(
            <Time as Cell<BinaryFmt>>::decode(&[0, 0, 0, 0]),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 8, actual_len: 4 })
        ));
        // interval wants 16 bytes — both too short and too long are rejected.
        assert!(matches!(
            <Interval as Cell<BinaryFmt>>::decode(&[0; 15]),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 16, actual_len: 15 })
        ));
        assert!(matches!(
            <Interval as Cell<BinaryFmt>>::decode(&[0; 17]),
            Err(DecodeError::BinaryLengthMismatch { expected_len: 16, actual_len: 17 })
        ));
    }
}

#[cfg(test)]
mod array_decode_tests {
    //! One-dimensional PG binary array decode (`Vec<Option<T>>`): the happy
    //! path with a NULL element, the empty `ndim = 0` form, and the classified
    //! bad paths (multi-dimensional, element-OID mismatch, truncated).

    extern crate alloc;
    use super::*;
    use alloc::string::String;
    use alloc::vec::Vec;

    /// Append an `i32` big-endian word.
    fn push_i32(out: &mut Vec<u8>, v: i32) {
        out.extend_from_slice(&v.to_be_bytes());
    }

    /// Build a 1-D PG binary array body from `(elem_oid, elements)`, each
    /// element `Some(bytes)` (non-NULL) or `None` (wire `-1`). `ndim` and
    /// `flags` are supplied explicitly so bad-path fixtures can set them.
    fn build_array(ndim: i32, elem_oid: u32, elems: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = Vec::new();
        push_i32(&mut out, ndim);
        push_i32(&mut out, 0); // flags — ignored by the decoder
        out.extend_from_slice(&elem_oid.to_be_bytes());
        if ndim == 0 {
            return out; // empty array: no dimension pair, no elements
        }
        push_i32(&mut out, crate::test_fixtures::fixture_i32(elems.len())); // dim_len
        push_i32(&mut out, 1); // lower bound
        for elem in elems {
            match elem {
                Some(body) => {
                    push_i32(&mut out, crate::test_fixtures::fixture_i32(body.len()));
                    out.extend_from_slice(body);
                }
                None => push_i32(&mut out, -1),
            }
        }
        out
    }

    #[test]
    fn int4_array_happy_with_null_element() {
        let one = 1i32.to_be_bytes();
        let three = 3i32.to_be_bytes();
        let wire = build_array(1, oids::INT4, &[Some(&one), None, Some(&three)]);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(
            got,
            Ok(ref v) if v.as_slice() == [Some(1i32), None, Some(3i32)]
        ));
    }

    #[test]
    fn text_array_owned_elements_with_null() {
        let wire = build_array(1, oids::TEXT, &[Some(b"hi"), None, Some(b"z")]);
        let got = <Vec<Option<String>> as Cell<BinaryFmt>>::decode(&wire);
        // Owned `String` elements, with the middle element an honest `None`.
        let expected: Vec<Option<String>> =
            alloc::vec![Some(String::from("hi")), None, Some(String::from("z"))];
        assert!(matches!(got, Ok(ref v) if *v == expected));
    }

    #[test]
    fn empty_array_ndim_zero() {
        let wire = build_array(0, oids::INT4, &[]);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(got, Ok(ref v) if v.is_empty()));
    }

    #[test]
    fn multidim_array_is_classified() {
        let wire = build_array(2, oids::INT4, &[]);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(got, Err(DecodeError::ArrayMultiDim { ndim: 2 })));
    }

    #[test]
    fn element_oid_mismatch_is_classified() {
        // Header says `text` (25) but we decode as `int4[]`.
        let wire = build_array(1, oids::TEXT, &[Some(b"x")]);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(
            got,
            Err(DecodeError::ArrayElemOidMismatch { expected, found })
                if expected == oids::INT4 && found == oids::TEXT
        ));
    }

    #[test]
    fn truncated_header_is_classified() {
        // Only two of the three fixed header words present.
        let mut wire = Vec::new();
        push_i32(&mut wire, 1);
        push_i32(&mut wire, 0);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(got, Err(DecodeError::ArrayTruncated)));
    }

    #[test]
    fn truncated_element_body_is_classified() {
        // Declares a 4-byte element but supplies only 2 bytes.
        let mut wire = build_array(1, oids::INT4, &[]);
        // Overwrite dim_len (0) with 1 and append a bogus short element.
        // Rebuild cleanly instead: header + one element claiming 4 bytes.
        wire.clear();
        push_i32(&mut wire, 1);
        push_i32(&mut wire, 0);
        wire.extend_from_slice(&oids::INT4.to_be_bytes());
        push_i32(&mut wire, 1); // dim_len
        push_i32(&mut wire, 1); // lower bound
        push_i32(&mut wire, 4); // element declares 4 bytes...
        wire.extend_from_slice(&[0u8, 0u8]); // ...but only 2 present
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(got, Err(DecodeError::ArrayTruncated)));
    }

    #[test]
    fn wrong_element_width_is_classified() {
        // A well-framed array whose element body is the wrong width for the
        // element decoder (`int4` needs 4 bytes, gets 2) surfaces the scalar
        // decoder's classified length mismatch — never a silent truncation.
        let mut wire = Vec::new();
        push_i32(&mut wire, 1);
        push_i32(&mut wire, 0);
        wire.extend_from_slice(&oids::INT4.to_be_bytes());
        push_i32(&mut wire, 1);
        push_i32(&mut wire, 1);
        push_i32(&mut wire, 2); // element is 2 bytes (present)
        wire.extend_from_slice(&[0u8, 1u8]);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(matches!(
            got,
            Err(DecodeError::BinaryLengthMismatch { expected_len: 4, actual_len: 2 })
        ));
    }

    #[test]
    fn trailing_bytes_past_last_element_are_classified() {
        // A fully-valid 1-D array with EXTRA bytes after the last declared
        // element is a surplus / malformed frame — classified, not silently
        // ignored (the no-swallow guarantee on the array path).
        let ten = 10i32.to_be_bytes();
        let mut wire = build_array(1, oids::INT4, &[Some(&ten)]);
        wire.extend_from_slice(&[0xDE, 0xAD]); // surplus trailing bytes
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(
            matches!(got, Err(DecodeError::ArrayTruncated)),
            "trailing bytes past the last element must be classified, got {got:?}"
        );
    }

    #[test]
    fn empty_array_with_mismatched_elem_oid_is_classified() {
        // An EMPTY array (ndim 0) whose header declares a `text` element OID,
        // decoded as `int4[]`, enforces the element-type contract even though
        // it carries no element bytes — a classified mismatch, not `Ok(empty)`.
        let wire = build_array(0, oids::TEXT, &[]);
        let got = <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&wire);
        assert!(
            matches!(
                got,
                Err(DecodeError::ArrayElemOidMismatch { expected, found })
                    if expected == oids::INT4 && found == oids::TEXT
            ),
            "an empty text[] header decoded as int4[] must be classified, got {got:?}"
        );
    }
}

#[cfg(test)]
mod composite_reader_tests {
    use super::*;
    use alloc::vec::Vec;

    /// Build a composite (row-type) binary frame from `(oid, Option<body>)`
    /// fields — the exact `record_send` wire form: `int32 nfields`, then per
    /// field `{uint32 oid, int32 len (-1 = NULL), byte[len]}`.
    fn build_composite(fields: &[(u32, Option<&[u8]>)]) -> Vec<u8> {
        let mut out = Vec::new();
        let nfields = i32::try_from(fields.len()).expect("test arity");
        out.extend_from_slice(&nfields.to_be_bytes());
        for (oid, body) in fields {
            out.extend_from_slice(&oid.to_be_bytes());
            match body {
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(bytes) => {
                    let len = i32::try_from(bytes.len()).expect("test len");
                    out.extend_from_slice(&len.to_be_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
        out
    }

    #[test]
    fn reads_present_and_null_fields_then_finishes() {
        // The empirically-captured `ROW('main st', 5)::addr` shape: a text field
        // then an int4 field.
        let five = 5i32.to_be_bytes();
        let frame = build_composite(&[(oids::TEXT, Some(b"main st")), (oids::INT4, Some(&five))]);
        let mut r = CompositeReader::new(&frame, 2).expect("arity 2");
        assert_eq!(r.next_field(), Ok(Some(&b"main st"[..])));
        assert_eq!(r.next_field(), Ok(Some(&five[..])));
        assert_eq!(r.finish(), Ok(()));
    }

    #[test]
    fn null_field_reads_as_none() {
        // `ROW(NULL, 5)::addr` — the first field carries len = -1.
        let five = 5i32.to_be_bytes();
        let frame = build_composite(&[(oids::TEXT, None), (oids::INT4, Some(&five))]);
        let mut r = CompositeReader::new(&frame, 2).expect("arity 2");
        assert_eq!(r.next_field(), Ok(None));
        assert_eq!(r.next_field(), Ok(Some(&five[..])));
        assert_eq!(r.finish(), Ok(()));
    }

    #[test]
    fn wrong_arity_is_classified() {
        let five = 5i32.to_be_bytes();
        let frame = build_composite(&[(oids::INT4, Some(&five))]); // 1 field
        let got = CompositeReader::new(&frame, 2); // expected 2
        assert!(
            matches!(
                got,
                Err(DecodeError::CompositeArityMismatch { expected: 2, found: 1 })
            ),
            "a 1-field frame decoded as arity 2 must be classified, got {got:?}"
        );
    }

    #[test]
    fn negative_field_count_is_classified_as_arity() {
        // A malformed / hostile negative field count can never equal a real
        // arity, so it classifies as a mismatch (found carries the negative).
        let mut frame = Vec::new();
        frame.extend_from_slice(&(-3i32).to_be_bytes());
        let got = CompositeReader::new(&frame, 2);
        assert!(
            matches!(
                got,
                Err(DecodeError::CompositeArityMismatch { expected: 2, found: -3 })
            ),
            "a negative field count must be classified, got {got:?}"
        );
    }

    #[test]
    fn truncated_count_header_is_classified() {
        // Fewer than the 4 header bytes.
        let got = CompositeReader::new(&[0x00, 0x00], 1);
        assert!(matches!(got, Err(DecodeError::CompositeTruncated)), "got {got:?}");
    }

    #[test]
    fn truncated_field_header_is_classified() {
        // Declares 1 field but the {oid,len} header is short.
        let mut frame = Vec::new();
        frame.extend_from_slice(&1i32.to_be_bytes());
        frame.extend_from_slice(&[0x00, 0x00, 0x00]); // 3 of 8 header bytes
        let mut r = CompositeReader::new(&frame, 1).expect("arity 1");
        assert!(matches!(r.next_field(), Err(DecodeError::CompositeTruncated)));
    }

    #[test]
    fn field_body_shorter_than_declared_is_classified() {
        // A field whose declared len exceeds the remaining bytes.
        let mut frame = Vec::new();
        frame.extend_from_slice(&1i32.to_be_bytes());
        frame.extend_from_slice(&oids::INT4.to_be_bytes());
        frame.extend_from_slice(&4i32.to_be_bytes()); // len 4
        frame.extend_from_slice(&[0x00, 0x00]); // only 2 body bytes
        let mut r = CompositeReader::new(&frame, 1).expect("arity 1");
        assert!(matches!(r.next_field(), Err(DecodeError::CompositeTruncated)));
    }

    #[test]
    fn negative_field_length_other_than_null_is_classified() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&1i32.to_be_bytes());
        frame.extend_from_slice(&oids::INT4.to_be_bytes());
        frame.extend_from_slice(&(-2i32).to_be_bytes()); // -2 is not the -1 NULL sentinel
        let mut r = CompositeReader::new(&frame, 1).expect("arity 1");
        assert!(matches!(r.next_field(), Err(DecodeError::CompositeTruncated)));
    }

    #[test]
    fn trailing_surplus_is_classified_by_finish() {
        // A fully-valid single field with extra trailing bytes: finish() rejects.
        let five = 5i32.to_be_bytes();
        let mut frame = build_composite(&[(oids::INT4, Some(&five))]);
        frame.extend_from_slice(&[0xDE, 0xAD]);
        let mut r = CompositeReader::new(&frame, 1).expect("arity 1");
        assert_eq!(r.next_field(), Ok(Some(&five[..])));
        assert!(matches!(r.finish(), Err(DecodeError::CompositeTruncated)));
    }
}
