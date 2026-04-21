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

impl FormatCode {
    /// Classify a wire i16 format-code byte into the typed variant.
    ///
    /// PG §55.2.2 defines exactly two legal values: `0` (text) and
    /// `1` (binary). Any other value is a server-side wire violation
    /// and returns the offending code in `Err` for the caller to wrap
    /// into [`ProtocolError::UnexpectedFormatCode`].
    ///
    /// # F32 (2026-04-21)
    ///
    /// Centralises the `{0, 1}` classification so future extended-query
    /// sub-phases (1c-3b Describe / 1c-3c BindExecute) that also parse
    /// format codes don't each rewrite the same match. A new illegal
    /// value surfaces with identical diagnostic across every callsite.
    #[inline]
    pub const fn try_from_wire_i16(code: i16) -> Result<Self, i16> {
        match code {
            0 => Ok(Self::Text),
            1 => Ok(Self::Binary),
            other => Err(other),
        }
    }
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
    /// Empty descriptor (0 columns). Used to populate the `row_desc`
    /// field of `SimpleQueryAwaitingRfq` on the empty-query
    /// ([`crate::wire::TAG_EMPTY_QUERY_RESPONSE`]) transition where
    /// no `RowDescription` precedes — and as a test fixture.
    pub const EMPTY: Self = Self {
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
        let format_code = FormatCode::try_from_wire_i16(format_code_i16)
            .map_err(|code| ProtocolError::UnexpectedFormatCode { code })?;

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

/// Parse a `ParameterDescription` payload (body of the `'t'` frame,
/// after the 5-byte header) into a [`crate::action::ParamOids`].
/// 1c-3c.
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
#[expect(
    clippy::result_large_err,
    reason = "no_alloc: Box unavailable; ParameterDescription parse is cold (once per Describe)"
)]
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

    // Tier-2 structural: reject counts too high for inline storage.
    // MAX_PARAMS_ARITY matches the Bind-side cap — receiving more
    // OIDs than we can ever Bind against means the describe result
    // is useless downstream.
    if n_params_usize > crate::params::MAX_PARAMS_ARITY {
        return Err(ProtocolError::TooManyParameters {
            count: n_params_usize,
            max: crate::params::MAX_PARAMS_ARITY,
        });
    }

    // Body length must exactly equal `count × 4` (one i32 per OID).
    // Trailing bytes imply wire corruption; short body implies the
    // declared count lies. Both classify as framing error.
    let expected_body_len = n_params_usize.checked_mul(4).ok_or_else(malformed)?;
    if rest.len() != expected_body_len {
        return Err(malformed());
    }

    // F7 (pass-#7 audit): `split_first_chunk::<4>()` returns typed
    // `Option<(&[u8; 4], &[u8])>` — the typed fixed-array ref
    // replaces the `chunks_exact(4)` + `[a,b,c,d]` slice-pattern
    // approach. No dead `_ =>` fallback arm needed; the Option::None
    // path is architecturally dead (body_len check above proves
    // remaining bytes suffice) yet surfaces as `Err(malformed())`
    // rather than `unreachable!()` (forbid-bundle).
    let mut oids = [0u32; crate::params::MAX_PARAMS_ARITY];
    let mut cursor = rest;
    for slot in oids.iter_mut().take(n_params_usize) {
        let (chunk, tail) = cursor.split_first_chunk::<4>().ok_or_else(malformed)?;
        *slot = u32::from_be_bytes(*chunk);
        cursor = tail;
    }

    Ok(crate::action::ParamOids::from_parts(n_params, oids))
}

// ════════════════════════════════════════════════════════════════════
// 1c-2b — DataRow parser + ColumnsIter
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
    /// read ASCII digits). 1c-2c.
    NonUtf8,
    /// Failed to parse a numeric text-format column into the target
    /// Rust integer type — bad digit, sign out of range, or
    /// overflow. 1c-2c.
    IntParse,
    /// Failed to parse a boolean — PG text format emits `"t"` / `"f"`;
    /// anything else classifies here. 1c-2c.
    BoolParse,
    /// A binary-format fixed-size column's byte length doesn't match
    /// the decoder's expectation (e.g. an `i32` decoder receiving 3
    /// bytes, or 5). 1c-3b binary-path classification — separate from
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
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedRow => f.write_str("DataRow body too short for column count header"),
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
/// [`crate::Action::StreamRow::row_bytes`], in which case `'a` is
/// the `'r` lifetime of the owning [`crate::OutActions`]. The
/// iterator yields column slices that share this borrow — no
/// copying, no allocation.
#[derive(Debug, Clone, Copy)]
pub struct DataRowRef<'a> {
    /// Full body, including the 2-byte count header.
    body: &'a [u8],
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
        let (count_bytes, _) = body.split_first_chunk::<2>().ok_or(DecodeError::TruncatedRow)?;
        let n_columns_i16 = i16::from_be_bytes(*count_bytes);
        if n_columns_i16 < 0 {
            return Err(DecodeError::TruncatedRow);
        }
        // `n_columns_i16 >= 0` (proved above) ⟹ `try_from` infallible.
        // The Err arm is architecturally dead, but classified as
        // `TruncatedRow` rather than silently fabricating a 0-column
        // row — if a future refactor of the negative-check above
        // introduces a seam, the dead arm becomes honest diagnostic
        // output instead of "empty row with no error". Tier-3 audit
        // → tier-2 structural: misfire classifies, does not mask.
        let n_columns = u16::try_from(n_columns_i16).map_err(|_| DecodeError::TruncatedRow)?;
        Ok(Self { body, n_columns })
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
        let remaining = self.body.get(2..).unwrap_or(&[]);
        ColumnsIter {
            remaining,
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
            None => {
                // Fuse: drop remaining bytes so subsequent `next` is None.
                self.remaining = &[];
                self.columns_left = 0;
                return Some(Err(DecodeError::TruncatedColumnLen { column_idx: idx }));
            }
        };
        let len = i32::from_be_bytes(*len_bytes);

        if len == -1 {
            // SQL NULL — no data bytes to consume.
            self.remaining = after_len;
            return Some(Ok(None));
        }

        if len < 0 {
            self.remaining = &[];
            self.columns_left = 0;
            return Some(Err(DecodeError::NegativeColumnLength {
                column_idx: idx,
                length: len,
            }));
        }

        // `len >= 0` ⟹ `try_from` infallible; defensive fallback.
        let len_usize = match usize::try_from(len) {
            Ok(v) => v,
            Err(_) => {
                self.remaining = &[];
                self.columns_left = 0;
                return Some(Err(DecodeError::NegativeColumnLength {
                    column_idx: idx,
                    length: len,
                }));
            }
        };

        match after_len.split_at_checked(len_usize) {
            Some((data, next)) => {
                self.remaining = next;
                Some(Ok(Some(data)))
            }
            None => {
                let remaining = after_len.len();
                self.remaining = &[];
                self.columns_left = 0;
                Some(Err(DecodeError::TruncatedColumnData {
                    column_idx: idx,
                    declared_len: len_usize,
                    remaining,
                }))
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
// 1c-2c — Text-format decoders
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
/// ```ignore
/// use bsql_pg_proto::{Action, DataRowRef, FromPgText};
///
/// let Action::StreamRow { row_bytes, .. } = action else { return };
/// let row = DataRowRef::parse(row_bytes)?;
/// let mut cols = row.columns();
/// let id: i32 = cols.next().unwrap()?.map(i32::from_pg_text).transpose()?
///     .ok_or("id cannot be NULL")?;
/// let name: &str = cols.next().unwrap()?.map(<&str>::from_pg_text).transpose()?
///     .ok_or("name cannot be NULL")?;
/// ```
///
/// # Error
///
/// [`DecodeError::NonUtf8`] for non-UTF-8 bytes; type-specific
/// parse errors:
/// - integer types → [`DecodeError::IntParse`]
/// - `bool` → [`DecodeError::BoolParse`]
///
/// # Binary format
///
/// For PG binary-format columns (selected via Bind in Extended
/// Query, 1c-3), a parallel `FromPgBinary` trait lands alongside
/// the binary codec. Text vs binary dispatch at the caller level
/// via `ColumnDesc::format_code`.
pub trait FromPgText<'a>: Sized {
    /// Decode the column's text-format bytes.
    fn from_pg_text(bytes: &'a [u8]) -> Result<Self, DecodeError>;
}

// Integer decoders: ASCII digits (optionally leading `-`). Uses
// stdlib `FromStr` which validates range + rejects trailing
// garbage.
macro_rules! impl_from_pg_text_int {
    ($($t:ty),+ $(,)?) => {
        $(
            impl FromPgText<'_> for $t {
                #[inline]
                fn from_pg_text(bytes: &[u8]) -> Result<Self, DecodeError> {
                    let s = core::str::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)?;
                    s.parse::<$t>().map_err(|_| DecodeError::IntParse)
                }
            }
        )+
    };
}

impl_from_pg_text_int!(i16, i32, i64, u32);

/// PG boolean text format: `"t"` = true, `"f"` = false. Anything
/// else (including `"true"`, `"TRUE"`, `"1"`, `"0"`) classifies as
/// [`DecodeError::BoolParse`] — PG is strict about its own format.
impl FromPgText<'_> for bool {
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
    #[inline]
    fn from_pg_text(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        core::str::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// ═════════════════════════════════════════════════════════════════
// FromPgBinary — parallel to FromPgText for PG binary-format
// columns (1c-3b: Bind-selected binary format per-parameter).
//
// Binary format byte layout matches PG §55.7 — fixed-size ints are
// big-endian two's complement, `bool` is a single byte 0/1, `text`
// is raw UTF-8 bytes. Every impl's `OID` const is drift-pinned
// against `oids::*` to catch type-mapping bugs at build time.
// ═════════════════════════════════════════════════════════════════

/// Decode a column's binary-format bytes into a typed Rust value.
///
/// Parallel to [`FromPgText`]; the caller dispatches between text
/// and binary decoders based on [`ColumnDesc::format_code`]. Extended
/// Query (1c-3b) selects binary via the Bind frame's per-param /
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
/// The [`sealed::FromPgBinarySealed`] supertrait is module-private
/// (DEF-115-class seal). Downstream crates cannot impl the trait
/// for their own Rust types — the binary-codec surface is a fixed
/// set of primitives in 1c-3b; wider types land with their
/// dedicated sub-phases (arrays 1c-6, uuid / timestamp Phase 2+).
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
                            actual_len: u16::try_from(bytes.len()).unwrap_or(u16::MAX),
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
                actual_len: u16::try_from(bytes.len()).unwrap_or(u16::MAX),
            }),
        }
    }
}

/// PG binary `text`: raw UTF-8 bytes. Zero-copy borrow.
impl sealed::FromPgBinarySealed for &str {}
impl<'a> FromPgBinary<'a> for &'a str {
    const OID: u32 = oids::TEXT;
    #[inline]
    fn from_pg_binary(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        core::str::from_utf8(bytes).map_err(|_| DecodeError::NonUtf8)
    }
}

// Compile-time symmetry pins: text and binary decoders for the
// same Rust type MUST target the same PG type OID. A refactor that
// breaks this breaks the build.
const _: () = {
    assert!(<i16 as FromPgBinary>::OID == oids::INT2);
    assert!(<i32 as FromPgBinary>::OID == oids::INT4);
    assert!(<i64 as FromPgBinary>::OID == oids::INT8);
    assert!(<u32 as FromPgBinary>::OID == oids::OID);
    assert!(<bool as FromPgBinary>::OID == oids::BOOL);
    assert!(<&str as FromPgBinary>::OID == oids::TEXT);
};

// ═════════════════════════════════════════════════════════════════
// EncodeBinary — PG binary format write path (mirror of FromPgBinary).
// Used by ParamsWriter (1c-3b) to serialise parameter values into
// the Bind frame's per-param length+bytes layout.
// ═════════════════════════════════════════════════════════════════

/// Encode a Rust value into PG binary format bytes, directly into
/// a [`crate::write_buf::WriteBuf`].
///
/// Parallel to [`FromPgBinary`] — the `OID` constants pair up
/// across the two traits so the Phase 2 `query!` macro can check
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

/// PostgreSQL built-in type OID constants for the subset 1c-2
/// decoders cover. Full list at
/// `https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat`.
///
/// Callers match these against [`ColumnDesc::type_oid`] to
/// dispatch the right [`FromPgText`] impl. The macro layer
/// (Phase 2) consumes this mapping at compile time via
/// `query!`-generated decoders.
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
    /// `None` = NULL, `Some(bytes)` = data.
    fn build(columns: &[Option<&[u8]>]) -> alloc::vec::Vec<u8> {
        let mut out = alloc::vec::Vec::new();
        let count = i16::try_from(columns.len()).unwrap_or(0);
        out.extend_from_slice(&count.to_be_bytes());
        for col in columns {
            match col {
                Some(data) => {
                    let len = i32::try_from(data.len()).unwrap_or(0);
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
            body: &[],
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

    /// Invariant: negative column count (i.e. count header decodes to
    /// a negative `i16`) is classified as `TruncatedRow`.
    #[test]
    fn negative_column_count() {
        let mut body = alloc::vec::Vec::new();
        body.extend_from_slice(&(-3i16).to_be_bytes());
        let result = DataRowRef::parse(&body);
        assert!(
            matches!(result, Err(DecodeError::TruncatedRow)),
            "negative count: expected TruncatedRow, got {result:?}",
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
    /// contract — happy paths, overflow, malformed digits, non-UTF-8.
    /// An arm-body swap in my impl (e.g., returning `NonUtf8` for
    /// overflow) fails this table.
    #[test]
    fn i32_decoder_matrix() {
        // Happy paths.
        assert!(matches!(i32::from_pg_text(b"0"), Ok(0)));
        assert!(matches!(i32::from_pg_text(b"42"), Ok(42)));
        assert!(matches!(i32::from_pg_text(b"-17"), Ok(-17)));
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

        // Non-UTF-8 → NonUtf8 (distinct from IntParse — classification
        // tells the caller whether retry would help).
        assert!(matches!(i32::from_pg_text(&[0xFF]), Err(DecodeError::NonUtf8)));
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
