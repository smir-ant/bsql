//! Row-schema + row-body decoding primitives. Phase 1c-2a.
//!
//! `bsql-pg-proto` owns the raw wire encoding of a result-set: the
//! `RowDescription` frame tells us column count, type OIDs, and per-column
//! format codes; each `DataRow` frame carries the column values. This
//! module parses `RowDescription` into [`RowDesc`] (shared between
//! [`crate::Action::StreamRow`] and [`crate::Reply::QueryComplete`]) and
//! will host the `DataRow` body parser + typed decoders in 1c-2b/c.
//!
//! # Why POD + bounded capacity
//!
//! The crate is `no_alloc`. `RowDesc` is a flat inline struct holding
//! a `[ColumnDesc; MAX_ROW_COLUMNS]` array alongside a `u16` populated
//! count — `Copy`, no `Drop`. Result-sets with more than
//! [`MAX_ROW_COLUMNS`] columns land in
//! [`crate::ProtocolError::TooManyColumns`] at parse time (tier-2
//! structural — the bound is enforced at construction, no silent
//! truncation).
//!
//! # Tier notes
//!
//! Schema ingest is **tier-2 structural**. The parser produces `RowDesc`
//! only on well-formed payloads (`MalformedRowDescription` on framing
//! errors, `UnexpectedFormatCode` on values outside `{0, 1}` — round-4
//! finding #5). A malformed response tears the connection down via the
//! usual `Errored` outcome.
//!
//! Schema access is **tier-1 compile** on pairing:
//! `Action::StreamRow` carries `&'r RowDesc` — the `'r` lifetime
//! prevents the user from using a stale schema after the protocol
//! advances to a new query.

use core::fmt;

/// Maximum columns per result-set supported by 1c-2. Queries returning
/// more columns classify as [`crate::ProtocolError::TooManyColumns`] —
/// the connection stays alive (recoverable), the user retries with a
/// narrower projection.
///
/// 32 covers typical application queries with headroom. Widening this
/// bound grows [`RowDesc`] linearly and propagates up through
/// [`crate::Reply::QueryComplete`].
pub const MAX_ROW_COLUMNS: usize = 32;

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
/// [`crate::ProtocolError::UnexpectedFormatCode`] (round-4 finding #5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum FormatCode {
    /// Text format — `0` on the wire.
    #[default]
    Text = 0,
    /// Binary format — `1` on the wire.
    Binary = 1,
}

/// Per-column metadata from a `RowDescription` frame.
///
/// Carries the load-bearing fields for row decoding: the PG type OID
/// (which tells the caller what Rust type to decode into) and the
/// format code (which tells the decoder whether to parse text or
/// binary representation).
///
/// **Fields dropped vs PG spec**: `table_oid`, `attr_num`, `type_size`,
/// `type_mod`, column name. Names can be restored in 1c-6 if
/// runtime-reflection tooling requires them; the macro layer (Phase 2)
/// resolves names at compile time and does not need the runtime copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColumnDesc {
    /// PostgreSQL type OID (e.g. `23` = `int4`, `25` = `text`). Match
    /// via the constants in [`crate::oids`].
    pub type_oid: u32,
    /// Text or binary.
    pub format_code: FormatCode,
}

/// Schema of a result-set's rows.
///
/// POD layout: a `[ColumnDesc; MAX_ROW_COLUMNS]` + `u16` populated
/// count. `Copy`, no `Drop`. Equality compares only the populated
/// prefix — trailing slots are default-filled and semantically
/// invisible.
#[derive(Debug, Clone, Copy)]
pub struct RowDesc {
    n_columns: u16,
    columns: [ColumnDesc; MAX_ROW_COLUMNS],
}

impl RowDesc {
    /// Empty descriptor (0 columns). Used as an architecturally-unreachable
    /// safe fallback in [`crate::action::Action::StreamRow`]
    /// materialisation when protocol state is inconsistent.
    pub(crate) const EMPTY: Self = Self {
        n_columns: 0,
        columns: [ColumnDesc {
            type_oid: 0,
            format_code: FormatCode::Text,
        }; MAX_ROW_COLUMNS],
    };

    /// Number of populated columns.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        // Infallible widening via `From` (bans on `as` casts per
        // crate forbid-bundle).
        usize::from(self.n_columns)
    }

    /// Whether the descriptor carries any columns.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.n_columns == 0
    }

    /// Borrow the populated columns as a slice.
    #[inline]
    #[must_use]
    pub fn columns(&self) -> &[ColumnDesc] {
        self.columns.get(..self.len()).unwrap_or(&[])
    }

    /// Get a single column by index, or `None` if out of range.
    #[inline]
    #[must_use]
    pub fn get(&self, idx: usize) -> Option<&ColumnDesc> {
        if idx >= self.len() {
            return None;
        }
        self.columns.get(idx)
    }
}

// RowDesc uses full-array Eq (tail ColumnDesc slots are default-filled
// during construction and never mutated thereafter, so byte-equality of
// the arrays implies logical equality of populated-prefix semantics).
impl PartialEq for RowDesc {
    fn eq(&self, other: &Self) -> bool {
        self.n_columns == other.n_columns && self.columns == other.columns
    }
}
impl Eq for RowDesc {}

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
///     cstring  name           (NUL-terminated; not stored — 1c-2 MVP)
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
/// - [`crate::ProtocolError::UnexpectedFormatCode`] — wire value not in
///   `{0, 1}` (round-4 finding #5).
#[cold]
#[expect(
    clippy::result_large_err,
    reason = "no_alloc: Box unavailable; RowDescription parse is cold (once per result-set)"
)]
pub(crate) fn parse_row_description(
    payload: &[u8],
) -> Result<RowDesc, crate::error::ProtocolError> {
    use crate::error::ProtocolError;
    let malformed = || ProtocolError::MalformedRowDescription {
        payload_len: payload.len(),
    };

    // column_count: i16 BE at offset 0.
    let (count_bytes, mut rest) = payload.split_first_chunk::<2>().ok_or_else(malformed)?;
    let n_columns_i16 = i16::from_be_bytes(*count_bytes);
    if n_columns_i16 < 0 {
        return Err(malformed());
    }
    // `n_columns_i16 >= 0`, so `u16::try_from` is infallible (just a
    // bit-width narrowing from a non-negative i16). Keep the Result
    // chain for the crate's no-panic discipline.
    let n_columns = u16::try_from(n_columns_i16).map_err(|_| malformed())?;
    let n_columns_usize = usize::from(n_columns);

    // Tier-2 structural: reject results too wide for inline storage.
    if n_columns_usize > MAX_ROW_COLUMNS {
        return Err(ProtocolError::TooManyColumns {
            count: n_columns_usize,
            max: MAX_ROW_COLUMNS,
        });
    }

    // Per-column parse. `columns` starts as `[default; MAX]`; populated
    // slots get overwritten with real values.
    let mut columns = [ColumnDesc::default(); MAX_ROW_COLUMNS];
    for slot in columns.iter_mut().take(n_columns_usize) {
        // Name: cstring (NUL-terminated). We skip the bytes; round-4
        // finding #2 typed-newtypes already covers identifier discipline
        // elsewhere.
        let nul_pos = rest.iter().position(|&b| b == 0).ok_or_else(malformed)?;
        let name_end = nul_pos.saturating_add(1);
        let after_name = rest.get(name_end..).ok_or_else(malformed)?;

        // 18 bytes of metadata after name: table_oid(4) + attr_num(2) +
        // type_oid(4) + type_size(2) + type_mod(4) + format_code(2).
        let (meta, next_cursor) = after_name
            .split_first_chunk::<18>()
            .ok_or_else(malformed)?;

        // Destructure into the two fields we keep. Slice-pattern makes
        // the offsets readable inline (no magic-index arithmetic).
        let &[
            _tbl0, _tbl1, _tbl2, _tbl3,        // table_oid
            _att0, _att1,                      // attr_num
            toid0, toid1, toid2, toid3,        // type_oid
            _ts0, _ts1,                        // type_size
            _tm0, _tm1, _tm2, _tm3,            // type_mod
            fc0, fc1,                          // format_code
        ] = meta;
        let type_oid = u32::from_be_bytes([toid0, toid1, toid2, toid3]);
        let format_code_i16 = i16::from_be_bytes([fc0, fc1]);
        let format_code = match format_code_i16 {
            0 => FormatCode::Text,
            1 => FormatCode::Binary,
            other => return Err(ProtocolError::UnexpectedFormatCode { code: other }),
        };

        *slot = ColumnDesc {
            type_oid,
            format_code,
        };
        rest = next_cursor;
    }

    // Trailing bytes after the declared column count are a framing
    // bug; `rest` must be empty at this point.
    if !rest.is_empty() {
        return Err(malformed());
    }

    Ok(RowDesc {
        n_columns,
        columns,
    })
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
    /// is guaranteed by `MAX_ROW_COLUMNS = 32 ≪ i16::MAX`; the
    /// `unwrap_or(0)` branch below is architecturally dead but
    /// honours the forbid-bundle ban on `unwrap()`.
    fn build(columns: &[(&[u8], u32, i16)]) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        let count = i16::try_from(columns.len()).unwrap_or(0);
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
        let expected = [
            ColumnDesc {
                type_oid: 23,
                format_code: FormatCode::Text,
            },
            ColumnDesc {
                type_oid: 25,
                format_code: FormatCode::Text,
            },
        ];
        assert!(
            matches!(
                &result,
                Ok(desc) if desc.columns() == expected.as_slice(),
            ),
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
                    Some(&ColumnDesc { format_code: FormatCode::Binary, .. }),
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
        let count = i16::try_from(over).unwrap_or(0);
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
