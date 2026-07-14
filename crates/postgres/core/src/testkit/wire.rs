//! Server-frame builders for the in-memory fake — pure functions over the
//! PUBLIC wire vocabulary ([`bsql_postgres_proto::wire`]`::TAG_*`), no engine
//! state. Each builder names the PostgreSQL message it produces, so a fake
//! reply reads like the wire trace the real engine parses.
//!
//! Unlike a dev-only fixture builder, these are consumer-facing (a testkit is
//! shipped): an input that cannot fit a fixed-width wire length field is a
//! [`FakeEncodeError`] returned to the caller, never a panic and never a
//! silent truncation. The realistic testkit inputs (a handful of small rows)
//! never approach the limits; the `Result` keeps the impossible case honest.

use std::vec::Vec;

use bsql_postgres_proto::decode::{oids, EncodeBinary};
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE, TAG_DATA_ROW,
    TAG_ERROR_RESPONSE, TAG_NOTIFICATION_RESPONSE, TAG_PARAMETER_STATUS, TAG_PARSE_COMPLETE,
    TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use bsql_postgres_proto::WriteBuf;

/// Reinterpret a PostgreSQL type OID (`u32`, from the proto [`oids`] table) as
/// the `i32` the `RowDescription` type-oid field carries. Every catalog OID is
/// far below `2^31`, so the bit reinterpretation is value-preserving; routing
/// it through `to_le_bytes`/`from_le_bytes` keeps the crate-root forbid-bundle
/// satisfied (no `as` cast, no arithmetic) and lets each `OID_*` below be
/// single-sourced from `oids::*` — a wire OID cannot drift from the decoder's.
const fn oid_i32(oid: u32) -> i32 {
    i32::from_le_bytes(oid.to_le_bytes())
}

/// PostgreSQL type OID for `int8` (`bigint`) — the wire type a scripted `i64`
/// column advertises. Single-sourced from [`oids::INT8`].
pub const OID_INT8: i32 = oid_i32(oids::INT8);
/// PostgreSQL type OID for `int4` (`integer`). Single-sourced from [`oids::INT4`].
pub const OID_INT4: i32 = oid_i32(oids::INT4);
/// PostgreSQL type OID for `text`. Single-sourced from [`oids::TEXT`].
pub const OID_TEXT: i32 = oid_i32(oids::TEXT);
/// PostgreSQL type OID for `bool`. Single-sourced from [`oids::BOOL`].
pub const OID_BOOL: i32 = oid_i32(oids::BOOL);
/// PostgreSQL type OID for `float4` (`real`). Single-sourced from [`oids::FLOAT4`].
pub const OID_FLOAT4: i32 = oid_i32(oids::FLOAT4);
/// PostgreSQL type OID for `float8` (`double precision`). Single-sourced from [`oids::FLOAT8`].
pub const OID_FLOAT8: i32 = oid_i32(oids::FLOAT8);
/// PostgreSQL type OID for `bytea`. Single-sourced from [`oids::BYTEA`].
pub const OID_BYTEA: i32 = oid_i32(oids::BYTEA);
/// PostgreSQL type OID for `uuid`. Single-sourced from [`oids::UUID`].
pub const OID_UUID: i32 = oid_i32(oids::UUID);
/// PostgreSQL type OID for `numeric` (`decimal`). Single-sourced from [`oids::NUMERIC`].
pub const OID_NUMERIC: i32 = oid_i32(oids::NUMERIC);
/// PostgreSQL type OID for `timestamptz`. Single-sourced from [`oids::TIMESTAMPTZ`].
pub const OID_TIMESTAMPTZ: i32 = oid_i32(oids::TIMESTAMPTZ);
/// PostgreSQL type OID for `timestamp`. Single-sourced from [`oids::TIMESTAMP`].
pub const OID_TIMESTAMP: i32 = oid_i32(oids::TIMESTAMP);
/// PostgreSQL type OID for `date`. Single-sourced from [`oids::DATE`].
pub const OID_DATE: i32 = oid_i32(oids::DATE);
/// PostgreSQL type OID for `time`. Single-sourced from [`oids::TIME`].
pub const OID_TIME: i32 = oid_i32(oids::TIME);
/// PostgreSQL type OID for `interval`. Single-sourced from [`oids::INTERVAL`].
pub const OID_INTERVAL: i32 = oid_i32(oids::INTERVAL);
/// PostgreSQL type OID for `json`. Single-sourced from [`oids::JSON`].
pub const OID_JSON: i32 = oid_i32(oids::JSON);
/// PostgreSQL type OID for `jsonb`. Single-sourced from [`oids::JSONB`].
pub const OID_JSONB: i32 = oid_i32(oids::JSONB);

// One-dimensional array type OIDs — the OID a `RowDescription` advertises for a
// `T[]` column, one per scriptable scalar element type. Single-sourced from the
// `oids::*_ARRAY` table so an array OID can never drift from the scalar it
// wraps. The wire HEADER of an array carries the SCALAR element OID (e.g.
// `OID_INT4`); these `_ARRAY` OIDs are the ARRAY column's own type OID.

/// PostgreSQL type OID for `bigint[]` (`int8[]`). Single-sourced from [`oids::INT8_ARRAY`].
pub const OID_INT8_ARRAY: i32 = oid_i32(oids::INT8_ARRAY);
/// PostgreSQL type OID for `integer[]` (`int4[]`). Single-sourced from [`oids::INT4_ARRAY`].
pub const OID_INT4_ARRAY: i32 = oid_i32(oids::INT4_ARRAY);
/// PostgreSQL type OID for `text[]`. Single-sourced from [`oids::TEXT_ARRAY`].
pub const OID_TEXT_ARRAY: i32 = oid_i32(oids::TEXT_ARRAY);
/// PostgreSQL type OID for `boolean[]`. Single-sourced from [`oids::BOOL_ARRAY`].
pub const OID_BOOL_ARRAY: i32 = oid_i32(oids::BOOL_ARRAY);
/// PostgreSQL type OID for `real[]` (`float4[]`). Single-sourced from [`oids::FLOAT4_ARRAY`].
pub const OID_FLOAT4_ARRAY: i32 = oid_i32(oids::FLOAT4_ARRAY);
/// PostgreSQL type OID for `double precision[]` (`float8[]`). Single-sourced from [`oids::FLOAT8_ARRAY`].
pub const OID_FLOAT8_ARRAY: i32 = oid_i32(oids::FLOAT8_ARRAY);
/// PostgreSQL type OID for `bytea[]`. Single-sourced from [`oids::BYTEA_ARRAY`].
pub const OID_BYTEA_ARRAY: i32 = oid_i32(oids::BYTEA_ARRAY);
/// PostgreSQL type OID for `uuid[]`. Single-sourced from [`oids::UUID_ARRAY`].
pub const OID_UUID_ARRAY: i32 = oid_i32(oids::UUID_ARRAY);
/// PostgreSQL type OID for `numeric[]` (`decimal[]`). Single-sourced from [`oids::NUMERIC_ARRAY`].
pub const OID_NUMERIC_ARRAY: i32 = oid_i32(oids::NUMERIC_ARRAY);
/// PostgreSQL type OID for `timestamptz[]`. Single-sourced from [`oids::TIMESTAMPTZ_ARRAY`].
pub const OID_TIMESTAMPTZ_ARRAY: i32 = oid_i32(oids::TIMESTAMPTZ_ARRAY);
/// PostgreSQL type OID for `timestamp[]`. Single-sourced from [`oids::TIMESTAMP_ARRAY`].
pub const OID_TIMESTAMP_ARRAY: i32 = oid_i32(oids::TIMESTAMP_ARRAY);
/// PostgreSQL type OID for `date[]`. Single-sourced from [`oids::DATE_ARRAY`].
pub const OID_DATE_ARRAY: i32 = oid_i32(oids::DATE_ARRAY);
/// PostgreSQL type OID for `time[]`. Single-sourced from [`oids::TIME_ARRAY`].
pub const OID_TIME_ARRAY: i32 = oid_i32(oids::TIME_ARRAY);
/// PostgreSQL type OID for `interval[]`. Single-sourced from [`oids::INTERVAL_ARRAY`].
pub const OID_INTERVAL_ARRAY: i32 = oid_i32(oids::INTERVAL_ARRAY);
/// PostgreSQL type OID for `json[]`. Single-sourced from [`oids::JSON_ARRAY`].
pub const OID_JSON_ARRAY: i32 = oid_i32(oids::JSON_ARRAY);
/// PostgreSQL type OID for `jsonb[]`. Single-sourced from [`oids::JSONB_ARRAY`].
pub const OID_JSONB_ARRAY: i32 = oid_i32(oids::JSONB_ARRAY);

/// Map a scalar element type OID (e.g. [`OID_INT4`]) to its one-dimensional
/// `T[]` array OID (e.g. [`OID_INT4_ARRAY`]) — the type OID a `RowDescription`
/// advertises for an array column. `None` for an OID that is not one of the
/// fake's scriptable scalar element types (an array OID itself has no entry, so
/// an array-of-arrays cannot be assigned an OID — multi-dimensional arrays are
/// not modelled). Single-sourced from the `_ARRAY` constants above, so it
/// cannot drift from the decoder's supported element set.
#[must_use]
pub const fn array_oid_for_element(element_oid: i32) -> Option<i32> {
    Some(match element_oid {
        OID_INT8 => OID_INT8_ARRAY,
        OID_INT4 => OID_INT4_ARRAY,
        OID_TEXT => OID_TEXT_ARRAY,
        OID_BOOL => OID_BOOL_ARRAY,
        OID_FLOAT4 => OID_FLOAT4_ARRAY,
        OID_FLOAT8 => OID_FLOAT8_ARRAY,
        OID_BYTEA => OID_BYTEA_ARRAY,
        OID_UUID => OID_UUID_ARRAY,
        OID_NUMERIC => OID_NUMERIC_ARRAY,
        OID_TIMESTAMPTZ => OID_TIMESTAMPTZ_ARRAY,
        OID_TIMESTAMP => OID_TIMESTAMP_ARRAY,
        OID_DATE => OID_DATE_ARRAY,
        OID_TIME => OID_TIME_ARRAY,
        OID_INTERVAL => OID_INTERVAL_ARRAY,
        OID_JSON => OID_JSON_ARRAY,
        OID_JSONB => OID_JSONB_ARRAY,
        _ => return None,
    })
}

/// A human-facing `T[]` type name for a scalar element OID — used only in the
/// fail-closed simple-query (text) error an array column raises. Defaults to
/// the generic `"array"` for an OID outside the scriptable scalar set.
#[must_use]
pub const fn array_type_name(element_oid: i32) -> &'static str {
    match element_oid {
        OID_INT8 => "bigint[]",
        OID_INT4 => "int4[]",
        OID_TEXT => "text[]",
        OID_BOOL => "boolean[]",
        OID_FLOAT4 => "float4[]",
        OID_FLOAT8 => "float8[]",
        OID_BYTEA => "bytea[]",
        OID_UUID => "uuid[]",
        OID_NUMERIC => "numeric[]",
        OID_TIMESTAMPTZ => "timestamptz[]",
        OID_TIMESTAMP => "timestamp[]",
        OID_DATE => "date[]",
        OID_TIME => "time[]",
        OID_INTERVAL => "interval[]",
        OID_JSON => "json[]",
        OID_JSONB => "jsonb[]",
        _ => "array",
    }
}

/// Transaction-status byte for `ReadyForQuery`: `I` = idle (not in a
/// transaction block) — the only status the MVP fake reports.
pub const TX_IDLE: u8 = b'I';

/// Why a fake server reply could not be encoded.
///
/// Every variant is an input that overflows a fixed-width PostgreSQL wire
/// field. None occurs for a realistic testkit script; the type exists so the
/// impossible case surfaces as a classified error rather than a panic or a
/// silently truncated (wire-illegal) frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FakeEncodeError {
    /// A frame body exceeded the `u32` wire length field.
    FrameTooLarge,
    /// A column or row count exceeded the `i16` wire count field.
    CountTooLarge,
    /// A single cell value exceeded the `i32` wire length field.
    CellTooLarge,
    /// A value's BINARY encoding overflowed the bounded encode buffer the
    /// real outbound frame builder uses (its fixed capacity). Only reachable
    /// by a value routed through the real [`EncodeBinary`] encoder — in
    /// practice a `numeric` with thousands of significant digits, which no
    /// realistic testkit fixture holds. The raw-byte types (`bytea`, `json`,
    /// `jsonb`) encode into an UNBOUNDED buffer and never reach it.
    EncodeBufferFull,
}

impl core::fmt::Display for FakeEncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let msg = match self {
            Self::FrameTooLarge => "fake reply frame body exceeds the u32 wire length field",
            Self::CountTooLarge => "fake reply column/row count exceeds the i16 wire field",
            Self::CellTooLarge => "fake reply cell value exceeds the i32 wire length field",
            Self::EncodeBufferFull => {
                "fake reply value's binary encoding exceeds the bounded encode buffer"
            }
        };
        f.write_str(msg)
    }
}

impl std::error::Error for FakeEncodeError {}

/// Wrap `body` in a PostgreSQL frame: tag byte + 4-byte big-endian length (the
/// length counts itself but not the tag) + body.
///
/// # Errors
///
/// [`FakeEncodeError::FrameTooLarge`] if `body.len() + 4` exceeds `u32::MAX`.
pub fn frame(tag: u8, body: &[u8]) -> Result<Vec<u8>, FakeEncodeError> {
    let len = u32::try_from(body.len().saturating_add(4)).map_err(|_| FakeEncodeError::FrameTooLarge)?;
    let mut out = Vec::with_capacity(body.len().saturating_add(5));
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

/// `AuthenticationOk`: tag `R`, sub-code 0 (trust auth accepted).
///
/// # Errors
///
/// Never in practice — the body is a fixed 4 bytes; the `Result` mirrors
/// [`frame`].
pub fn auth_ok() -> Result<Vec<u8>, FakeEncodeError> {
    frame(TAG_AUTHENTICATION.byte(), &0i32.to_be_bytes())
}

/// `ParameterStatus`: tag `S`, `key\0value\0`.
///
/// # Errors
///
/// [`FakeEncodeError::FrameTooLarge`] for a pathologically large key/value.
pub fn parameter_status(key: &str, value: &str) -> Result<Vec<u8>, FakeEncodeError> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(TAG_PARAMETER_STATUS.byte(), &body)
}

/// `BackendKeyData`: tag `K`, 8-byte payload (pid + secret key).
///
/// # Errors
///
/// Never in practice — the body is a fixed 8 bytes.
pub fn backend_key_data(pid: i32, secret_key: i32) -> Result<Vec<u8>, FakeEncodeError> {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&secret_key.to_be_bytes());
    frame(TAG_BACKEND_KEY_DATA.byte(), &body)
}

/// `ReadyForQuery`: tag `Z`, 1-byte transaction status.
///
/// # Errors
///
/// Never in practice — the body is a single byte.
pub fn ready_for_query(tx_status: u8) -> Result<Vec<u8>, FakeEncodeError> {
    frame(TAG_READY_FOR_QUERY.byte(), &[tx_status])
}

/// The fixed on-wire size a PostgreSQL type OID advertises in
/// `RowDescription`, or `-1` for a variable-length type. Real PG sends the true
/// fixed width (`int8`=8, `int4`=4, `bool`=1) and `-1` only for variable types
/// (`text`); mirroring that keeps the fake faithful even though bsql's decoder
/// ignores the field.
const fn type_size_for_oid(oid: i32) -> i16 {
    match oid {
        OID_INT8 | OID_FLOAT8 | OID_TIMESTAMPTZ | OID_TIMESTAMP | OID_TIME => 8,
        OID_INT4 | OID_FLOAT4 | OID_DATE => 4,
        OID_BOOL => 1,
        OID_UUID | OID_INTERVAL => 16,
        // `text`, `bytea`, `numeric`, `json`, `jsonb` (varlena) and anything
        // else the fake does not model as fixed-width.
        _ => -1,
    }
}

/// `RowDescription`: tag `T`, one entry per column. Each entry is
/// `name\0` + table-oid(0) + attnum(i16) + type-oid(i32) + type-size +
/// type-mod(-1) + format(0 = text). The type-size is the type's true fixed
/// width (or `-1` for variable types). Simple-query results are always text
/// format, so every column advertises format 0.
///
/// # Errors
///
/// [`FakeEncodeError::CountTooLarge`] if the column count exceeds `i16::MAX`,
/// or [`FakeEncodeError::FrameTooLarge`] for an oversized frame.
pub fn row_description(columns: &[(String, i32)]) -> Result<Vec<u8>, FakeEncodeError> {
    let n = i16::try_from(columns.len()).map_err(|_| FakeEncodeError::CountTooLarge)?;
    let mut body = Vec::new();
    body.extend_from_slice(&n.to_be_bytes());
    for (i, (name, type_oid)) in columns.iter().enumerate() {
        let attnum = i16::try_from(i.saturating_add(1)).map_err(|_| FakeEncodeError::CountTooLarge)?;
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes()); // table oid
        body.extend_from_slice(&attnum.to_be_bytes());
        body.extend_from_slice(&type_oid.to_be_bytes());
        body.extend_from_slice(&type_size_for_oid(*type_oid).to_be_bytes()); // type size
        body.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        body.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

/// `DataRow`: tag `D`, column count + per-column `(len i32, bytes)`. A `None`
/// cell is the SQL-NULL sentinel length `-1`.
///
/// # Errors
///
/// [`FakeEncodeError::CountTooLarge`] if the column count exceeds `i16::MAX`,
/// [`FakeEncodeError::CellTooLarge`] if a cell exceeds `i32::MAX`, or
/// [`FakeEncodeError::FrameTooLarge`] for an oversized frame.
pub fn data_row(cells: &[Option<Vec<u8>>]) -> Result<Vec<u8>, FakeEncodeError> {
    let n = i16::try_from(cells.len()).map_err(|_| FakeEncodeError::CountTooLarge)?;
    let mut body = Vec::new();
    body.extend_from_slice(&n.to_be_bytes());
    for cell in cells {
        match cell {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len = i32::try_from(bytes.len()).map_err(|_| FakeEncodeError::CellTooLarge)?;
                body.extend_from_slice(&len.to_be_bytes());
                body.extend_from_slice(bytes);
            }
        }
    }
    frame(TAG_DATA_ROW.byte(), &body)
}

/// `CommandComplete`: tag `C`, NUL-terminated tag string (e.g. `"SELECT 2"`).
///
/// # Errors
///
/// [`FakeEncodeError::FrameTooLarge`] for a pathologically large tag.
pub fn command_complete(tag: &str) -> Result<Vec<u8>, FakeEncodeError> {
    let mut body = Vec::from(tag.as_bytes());
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

/// `ErrorResponse`: tag `E`, `S<severity>\0C<sqlstate>\0M<message>\0` then a
/// terminating `\0`. The real engine parses this into a `DbError` the driver
/// surfaces as `DriverError::Db`.
///
/// # Errors
///
/// [`FakeEncodeError::FrameTooLarge`] for a pathologically large message.
pub fn error_response(severity: &str, sqlstate: &str, message: &str) -> Result<Vec<u8>, FakeEncodeError> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0); // field-list terminator
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

/// `NotificationResponse`: tag `A`, `[i32 pid][channel\0][payload\0]` — the
/// asynchronous `LISTEN`/`NOTIFY` frame. Spliced into a query's reply stream, it
/// scripts a notification arriving DURING a query (the interleaving the real
/// backend does), so a test can prove the driver captures it rather than dropping
/// it. The body layout is exactly what
/// [`parse_notification`](crate::materialize::parse_notification) reads.
///
/// # Errors
///
/// [`FakeEncodeError::FrameTooLarge`] for a pathologically large channel/payload.
pub fn notification_response(pid: i32, channel: &str, payload: &str) -> Result<Vec<u8>, FakeEncodeError> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(channel.as_bytes());
    body.push(0);
    body.extend_from_slice(payload.as_bytes());
    body.push(0);
    frame(TAG_NOTIFICATION_RESPONSE.byte(), &body)
}

/// Concatenate several frames into one server-reply byte stream.
#[must_use]
pub fn concat(frames: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frames {
        out.extend_from_slice(f);
    }
    out
}

// ═══════════════════════════════════════════════════════════════════════
// Binary result-format cells.
//
// The compile-checked `query!` path is binary-uniform: its `Bind` elects
// binary results for every column, and its emitted decoder reads each cell
// via `Cell<BinaryFmt>`. So an extended-query `DataRow` the fake serves must
// carry BINARY cell bytes, not the text bytes the simple-query path uses.
// The [`data_row`] frame itself is format-agnostic (a column count plus each
// cell's `(len, bytes)`); only the CELL bytes differ, so these helpers render
// the bytes and [`data_row`] wraps them unchanged.
//
// Each encoder mirrors the exact layout `Cell<BinaryFmt>::decode` reads
// (`<T>::from_be_bytes` for the fixed-width scalars). The round-trip tests
// below prove that by decoding the produced bytes with the REAL decoder — a
// wrong layout is a failing test, never a silently-wrong fake.
// ═══════════════════════════════════════════════════════════════════════

/// PG binary `int8` (`bigint`): 8 big-endian bytes — the bytes
/// `Cell<BinaryFmt> for i64` reads via `i64::from_be_bytes`.
#[must_use]
pub fn binary_int8(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// PG binary `int4` (`integer`): 4 big-endian bytes — the bytes
/// `Cell<BinaryFmt> for i32` reads via `i32::from_be_bytes`.
#[must_use]
pub fn binary_int4(v: i32) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// PG binary `bool`: one byte, `0` = false, `1` = true — exactly what
/// `Cell<BinaryFmt> for bool` accepts.
#[must_use]
pub fn binary_bool(v: bool) -> Vec<u8> {
    std::vec![u8::from(v)]
}

/// PG binary `text`: raw UTF-8 bytes, verbatim — what
/// `Cell<BinaryFmt> for &str` borrows (it only validates UTF-8). Identical
/// to the text-format rendering for `text`, but named for the binary path so
/// the call site reads honestly.
#[must_use]
pub fn binary_text(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Encode a value's PG BINARY body through the crate's REAL [`EncodeBinary`]
/// impl — the identical encoder the compile-checked `query!` parameter path
/// uses. Routing the fake's bytes through it makes them byte-identical to what
/// a real server round-trips BY CONSTRUCTION, not merely by test: a fixed-width
/// scalar (`uuid`, `timestamptz`, `date`, …) or the grouped `numeric` layout
/// can never drift from the [`bsql_postgres_proto::Cell`]`<BinaryFmt>` decoder
/// that reads it, because there is one encoder, not two. This is the preferred
/// source for every fixed-width / non-trivially-laid-out type; the raw-byte
/// types (`bytea`, `json`, `jsonb`) keep their own unbounded helpers below so a
/// multi-kilobyte fixture stays representable.
///
/// # Errors
///
/// [`FakeEncodeError::EncodeBufferFull`] if the encoding overflows the bounded
/// [`WriteBuf`] the real outbound builder uses — reachable only by a `numeric`
/// with thousands of significant digits, never a realistic fixture. Surfaced as
/// a classified error, never a panic and never a truncated (wire-illegal) body.
pub fn binary_via_encoder<T: EncodeBinary>(value: &T) -> Result<Vec<u8>, FakeEncodeError> {
    let mut buf = WriteBuf::new();
    value
        .encode_to(&mut buf)
        .map_err(|_| FakeEncodeError::EncodeBufferFull)?;
    Ok(buf.as_bytes().to_vec())
}

/// PG binary `bytea`: the raw bytes, verbatim — what `Cell<BinaryFmt> for
/// &[u8]` borrows (every length, including empty, is a valid body). The
/// byte-string peer of [`binary_text`], and like it UNBOUNDED, so a
/// multi-kilobyte fixture is representable (unlike the bounded
/// [`binary_via_encoder`] path).
#[must_use]
pub fn binary_bytea(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}

/// PG binary `json`: the raw UTF-8 JSON text, verbatim (no framing) — exactly
/// what `Cell<BinaryFmt> for Json` reads. Unbounded, like [`binary_text`].
#[must_use]
pub fn binary_json(text: &str) -> Vec<u8> {
    text.as_bytes().to_vec()
}

/// PG binary `jsonb`: the leading version byte `1` then the UTF-8 JSON text,
/// mirroring the decoder's header contract exactly — what `Cell<BinaryFmt> for
/// Jsonb` reads (a version byte other than `1` is a classified decode error).
/// The version byte is the ONLY difference from [`binary_json`]. Unbounded.
#[must_use]
pub fn binary_jsonb(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len().saturating_add(1));
    out.push(1);
    out.extend_from_slice(text.as_bytes());
    out
}

/// Encode a one-dimensional PostgreSQL binary array body from PRE-RENDERED
/// element bodies — the exact wire form the `query!` array decoder
/// (`Cell<BinaryFmt> for Vec<Option<T>>`) reads.
///
/// `element_oid` is the SCALAR element type OID written into the array header
/// (the decoder cross-checks it against the row tuple's element type, refusing
/// a `text[]` payload decoded as `int4[]`). Each `Some(bytes)` is one element's
/// binary body — produced by the SCALAR binary encoder ([`binary_int4`],
/// [`binary_via_encoder`], [`binary_text`], …) and passed through VERBATIM, so
/// the element bytes can never drift from the scalar decoder that reads them —
/// and each `None` is a SQL-NULL element (the wire `-1` length sentinel).
///
/// Mirrors PostgreSQL's `array_send`: an EMPTY array is the canonical
/// `ndim = 0` form (three header words: `ndim`, `flags`, `element_oid` — no
/// dimension pair, nothing after, so an empty array decodes to an empty `Vec`);
/// a populated one is `ndim = 1`, a `flags` word whose bit 0 is set iff any
/// element is NULL (the real backend's `has-null` bit — the decoder ignores it
/// and detects NULL per element, but the fake sets it faithfully), the element
/// OID, the dimension length, a lower bound of `1`, then each element's
/// `(len, body)`.
///
/// # Errors
///
/// [`FakeEncodeError::CellTooLarge`] if an element body — or the element count —
/// overflows an `i32` wire length/dimension field. Unreachable for a realistic
/// fixture (the enclosing `DataRow` cell caps the whole array against the same
/// `i32` limit long first); the `Result` keeps the impossible case honest,
/// never a wrapped length or a silent truncation.
pub fn binary_array(
    element_oid: i32,
    elements: &[Option<Vec<u8>>],
) -> Result<Vec<u8>, FakeEncodeError> {
    let mut body = Vec::new();
    // PG's canonical empty array: `ndim = 0`, no dimension or lower-bound words.
    // The decoder's `ndim == 0` path requires NOTHING follows these three header
    // words, so an empty array is exactly 12 bytes.
    if elements.is_empty() {
        body.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0
        body.extend_from_slice(&0i32.to_be_bytes()); // flags = 0
        body.extend_from_slice(&element_oid.to_be_bytes());
        return Ok(body);
    }
    let has_null = elements.iter().any(Option::is_none);
    let flags: i32 = if has_null { 1 } else { 0 };
    let dim_len = i32::try_from(elements.len()).map_err(|_| FakeEncodeError::CellTooLarge)?;
    body.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
    body.extend_from_slice(&flags.to_be_bytes());
    body.extend_from_slice(&element_oid.to_be_bytes());
    body.extend_from_slice(&dim_len.to_be_bytes()); // dimension length
    body.extend_from_slice(&1i32.to_be_bytes()); // lower bound = 1
    for elem in elements {
        match elem {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let len = i32::try_from(bytes.len()).map_err(|_| FakeEncodeError::CellTooLarge)?;
                body.extend_from_slice(&len.to_be_bytes());
                body.extend_from_slice(bytes);
            }
        }
    }
    Ok(body)
}

// ═══════════════════════════════════════════════════════════════════════
// Extended-protocol acknowledgement frames.
//
// Each is a bodyless frame the server sends to acknowledge one extended-query
// frontend message: `ParseComplete` after a `Parse`, `BindComplete` after a
// `Bind`, `CloseComplete` after a `Close`. The fake emits one per matching
// message, so an extended batch's reply is byte-for-byte what the real engine
// expects at each step.
// ═══════════════════════════════════════════════════════════════════════

/// `ParseComplete`: tag `1`, empty body — sent after a successful `Parse`.
///
/// # Errors
///
/// Never in practice — the body is empty; the `Result` mirrors [`frame`].
pub fn parse_complete() -> Result<Vec<u8>, FakeEncodeError> {
    frame(TAG_PARSE_COMPLETE.byte(), &[])
}

/// `BindComplete`: tag `2`, empty body — sent after a successful `Bind`.
///
/// # Errors
///
/// Never in practice — the body is empty; the `Result` mirrors [`frame`].
pub fn bind_complete() -> Result<Vec<u8>, FakeEncodeError> {
    frame(TAG_BIND_COMPLETE.byte(), &[])
}

/// `CloseComplete`: tag `3`, empty body — sent after a successful `Close`.
///
/// The tag byte is the literal `b'3'` (PostgreSQL protocol §55.7): proto's
/// `TAG_CLOSE_COMPLETE` is `pub(crate)`, so it is not importable here, and
/// this keeps the proto crate byte-untouched. The `TAG_CLOSE_COMPLETE.byte()
/// == b'3'` drift-pin in proto guards the constant on its side.
///
/// # Errors
///
/// Never in practice — the body is empty; the `Result` mirrors [`frame`].
pub fn close_complete() -> Result<Vec<u8>, FakeEncodeError> {
    frame(b'3', &[])
}

/// `NoData`: tag `n`, empty body — the extended-protocol `Describe(portal)`
/// answer for a portal that returns no rows (a scripted error / no-column query).
/// A fixed 5-byte frame (`b'n'`, length 4, no body), so it is built directly
/// rather than through the fallible [`frame`] path.
#[must_use]
pub fn no_data() -> Vec<u8> {
    vec![b'n', 0, 0, 0, 4]
}

#[cfg(test)]
mod tests {
    //! The fake's binary cell bytes MUST be exactly what the flagship
    //! `query!` decoder reads. These offline round-trips prove that by
    //! decoding each encoder's output with the REAL `Cell<BinaryFmt>` — no
    //! engine, no network. A wrong byte layout fails here, so a wire-incorrect
    //! binary encoding is impossible to ship.

    use bsql_postgres_proto::{
        BinaryFmt, Cell, Date, DecodeError, Interval, Json, Jsonb, Numeric, Time, Timestamp,
        Timestamptz, Uuid,
    };

    use super::{
        array_oid_for_element, array_type_name, binary_array, binary_bool, binary_bytea,
        binary_int4, binary_int8, binary_json, binary_jsonb, binary_text, binary_via_encoder,
        OID_BYTEA, OID_INT4, OID_INT4_ARRAY, OID_NUMERIC, OID_TEXT, OID_TEXT_ARRAY, OID_UUID,
        OID_UUID_ARRAY,
    };

    #[test]
    fn binary_int8_round_trips_through_the_real_decoder() {
        for v in [0_i64, 1, -1, 42, i64::MIN, i64::MAX] {
            let bytes = binary_int8(v);
            assert_eq!(bytes.len(), 8, "int8 is 8 wire bytes");
            assert_eq!(<i64 as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_int4_round_trips_through_the_real_decoder() {
        for v in [0_i32, 1, -1, 42, i32::MIN, i32::MAX] {
            let bytes = binary_int4(v);
            assert_eq!(bytes.len(), 4, "int4 is 4 wire bytes");
            assert_eq!(<i32 as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_bool_round_trips_through_the_real_decoder() {
        for v in [false, true] {
            let bytes = binary_bool(v);
            assert_eq!(bytes.len(), 1, "bool is 1 wire byte");
            assert_eq!(<bool as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_text_round_trips_through_the_real_decoder() {
        for v in ["", "alice", "hello world", "über"] {
            let bytes = binary_text(v);
            assert_eq!(<&str as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_floats_round_trip_through_the_real_decoder() {
        for v in [0.0_f32, 1.5, -2.5, f32::MIN, f32::MAX] {
            let bytes = binary_via_encoder(&v).expect("f32 encodes");
            assert_eq!(bytes.len(), 4, "float4 is 4 wire bytes");
            assert_eq!(<f32 as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
        for v in [0.0_f64, 1.5, -2.5, f64::MIN, f64::MAX] {
            let bytes = binary_via_encoder(&v).expect("f64 encodes");
            assert_eq!(bytes.len(), 8, "float8 is 8 wire bytes");
            assert_eq!(<f64 as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_uuid_round_trips_through_the_real_decoder() {
        for raw in [[0_u8; 16], [0xFF; 16], *b"0123456789abcdef"] {
            let v = Uuid::from_bytes(raw);
            let bytes = binary_via_encoder(&v).expect("uuid encodes");
            assert_eq!(bytes.len(), 16, "uuid is 16 wire bytes");
            assert_eq!(<Uuid as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_numeric_round_trips_through_the_real_decoder() {
        // A finite decimal, a whole number, zero, a negative, and the special
        // sentinels — every classification the grouped wire layout carries.
        let finite = "3.14".parse::<Numeric>().expect("parse 3.14");
        let big = "12345678901234567890.987654321".parse::<Numeric>().expect("parse big");
        let neg = "-42".parse::<Numeric>().expect("parse -42");
        let zero = "0".parse::<Numeric>().expect("parse 0");
        for v in [finite, big, neg, zero, Numeric::nan(), Numeric::infinity(), Numeric::neg_infinity()] {
            let bytes = binary_via_encoder(&v).expect("numeric encodes");
            assert_eq!(<Numeric as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_temporal_round_trips_through_the_real_decoder() {
        for v in [Timestamptz::from_micros(1_000_000), Timestamptz::from_micros(i64::MIN), Timestamptz::from_micros(i64::MAX)] {
            let bytes = binary_via_encoder(&v).expect("timestamptz encodes");
            assert_eq!(bytes.len(), 8, "timestamptz is 8 wire bytes");
            assert_eq!(<Timestamptz as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
        for v in [Timestamp::from_micros(0), Timestamp::from_micros(-1), Timestamp::from_micros(123_456_789)] {
            let bytes = binary_via_encoder(&v).expect("timestamp encodes");
            assert_eq!(bytes.len(), 8, "timestamp is 8 wire bytes");
            assert_eq!(<Timestamp as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
        for v in [Date::from_days(0), Date::from_days(59), Date::from_days(-1), Date::infinity(), Date::neg_infinity()] {
            let bytes = binary_via_encoder(&v).expect("date encodes");
            assert_eq!(bytes.len(), 4, "date is 4 wire bytes");
            assert_eq!(<Date as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
        for v in [Time::from_micros(0), Time::from_micros(45_296_789_012)] {
            let bytes = binary_via_encoder(&v).expect("time encodes");
            assert_eq!(bytes.len(), 8, "time is 8 wire bytes");
            assert_eq!(<Time as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
        for v in [Interval::new(0, 0, 0), Interval::new(14, 3, 14_706_000_000), Interval::new(-1, -2, -3)] {
            let bytes = binary_via_encoder(&v).expect("interval encodes");
            assert_eq!(bytes.len(), 16, "interval is 16 wire bytes");
            assert_eq!(<Interval as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_bytea_round_trips_through_the_real_decoder() {
        for v in [b"".as_slice(), b"\xDE\xAD\xBE\xEF", b"\x00\x01\x02"] {
            let bytes = binary_bytea(v);
            assert_eq!(<&[u8] as Cell<BinaryFmt>>::decode(&bytes), Ok(v));
        }
    }

    #[test]
    fn binary_json_round_trips_through_the_real_decoder() {
        for text in [r#"{"k":1}"#, "[1,2,3]", "null", r#""über""#] {
            let bytes = binary_json(text);
            assert_eq!(
                <Json as Cell<BinaryFmt>>::decode(&bytes),
                Ok(Json::new(text.to_owned()))
            );
        }
    }

    #[test]
    fn binary_jsonb_round_trips_through_the_real_decoder() {
        for text in [r#"{"k":1}"#, "[1,2,3]", "null"] {
            let bytes = binary_jsonb(text);
            assert_eq!(bytes.first(), Some(&1_u8), "jsonb carries the leading version byte 1");
            assert_eq!(
                <Jsonb as Cell<BinaryFmt>>::decode(&bytes),
                Ok(Jsonb::new(text.to_owned()))
            );
        }
    }

    // ── array round-trips: the fake's `binary_array` bytes MUST be exactly what
    //    the flagship `query!` array decoder (`Cell<BinaryFmt> for
    //    Vec<Option<T>>`) reads. Each test encodes an array from the SAME scalar
    //    element encoders the fake uses, then decodes it with the REAL decoder —
    //    a wrong header, element order, or NULL sentinel fails HERE, so a
    //    wire-incorrect fake array is impossible to ship. ──

    #[test]
    fn binary_array_int4_round_trips_with_a_null_element() {
        // `{10, NULL, 30}` — a populated int4[] with an interior NULL element.
        let elements = [Some(binary_int4(10)), None, Some(binary_int4(30))];
        let bytes = binary_array(OID_INT4, &elements).expect("int4[] encodes");
        assert_eq!(
            <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&bytes),
            Ok(vec![Some(10), None, Some(30)])
        );
    }

    #[test]
    fn binary_array_text_round_trips_with_a_null_element() {
        // `{"a", NULL, "c"}` — decoded elements own their bytes (`String`).
        let elements = [Some(binary_text("a")), None, Some(binary_text("c"))];
        let bytes = binary_array(OID_TEXT, &elements).expect("text[] encodes");
        assert_eq!(
            <Vec<Option<String>> as Cell<BinaryFmt>>::decode(&bytes),
            Ok(vec![Some("a".to_owned()), None, Some("c".to_owned())])
        );
    }

    #[test]
    fn binary_array_numeric_round_trips_with_a_null_element() {
        // `{3.14, NULL}` — each element is the grouped `numeric` binary body from
        // the real encoder, so a wire mislayout fails the decode.
        let pi = "3.14".parse::<Numeric>().expect("parse 3.14");
        let elements = [Some(binary_via_encoder(&pi).expect("numeric elem encodes")), None];
        let bytes = binary_array(OID_NUMERIC, &elements).expect("numeric[] encodes");
        assert_eq!(
            <Vec<Option<Numeric>> as Cell<BinaryFmt>>::decode(&bytes),
            Ok(vec![Some("3.14".parse::<Numeric>().expect("parse 3.14")), None])
        );
    }

    #[test]
    fn binary_array_uuid_round_trips() {
        // A populated uuid[] — 16-byte fixed-width elements from the real encoder.
        let a = Uuid::from_bytes([0x11; 16]);
        let b = Uuid::from_bytes([0x22; 16]);
        let elements = [
            Some(binary_via_encoder(&a).expect("uuid elem encodes")),
            Some(binary_via_encoder(&b).expect("uuid elem encodes")),
        ];
        let bytes = binary_array(OID_UUID, &elements).expect("uuid[] encodes");
        assert_eq!(
            <Vec<Option<Uuid>> as Cell<BinaryFmt>>::decode(&bytes),
            Ok(vec![Some(a), Some(b)])
        );
    }

    #[test]
    fn binary_array_empty_round_trips_to_an_empty_vec() {
        // PG's canonical empty array (`ndim = 0`) — three header words, nothing
        // after — decodes to an empty `Vec`.
        let empty: [Option<Vec<u8>>; 0] = [];
        let bytes = binary_array(OID_INT4, &empty).expect("empty int4[] encodes");
        assert_eq!(bytes.len(), 12, "an empty array is exactly the 3 header words");
        assert_eq!(
            <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&bytes),
            Ok(vec![])
        );
    }

    #[test]
    fn binary_array_all_null_elements_round_trips() {
        // Every element NULL — the `has-null` flag is set and each element is a
        // `-1` sentinel, all decoded as `None`.
        let elements = [None, None];
        let bytes = binary_array(OID_INT4, &elements).expect("all-NULL int4[] encodes");
        assert_eq!(
            <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&bytes),
            Ok(vec![None, None])
        );
    }

    #[test]
    fn binary_array_wrong_element_oid_is_classified_by_the_real_decoder() {
        // The header's element OID is written FAITHFULLY: encode with a `text`
        // element OID but int4 bodies, then decode as int4[] — the real decoder
        // rejects the element-OID mismatch (proving the fake writes the header's
        // element OID, and the decoder cross-checks it), never reinterprets.
        let elements = [Some(binary_int4(1))];
        let bytes = binary_array(OID_TEXT, &elements).expect("encodes");
        assert_eq!(
            <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(&bytes),
            Err(DecodeError::ArrayElemOidMismatch { expected: 23, found: 25 })
        );
    }

    #[test]
    fn array_oid_mapping_is_single_sourced() {
        // The scalar->array OID map routes each scriptable element to its `_ARRAY`
        // constant (itself single-sourced from `oids`), and refuses an OID that
        // is not a scriptable scalar element (an array OID has no entry, so an
        // array-of-arrays cannot be assigned an OID).
        assert_eq!(array_oid_for_element(OID_INT4), Some(OID_INT4_ARRAY));
        assert_eq!(array_oid_for_element(OID_TEXT), Some(OID_TEXT_ARRAY));
        assert_eq!(array_oid_for_element(OID_UUID), Some(OID_UUID_ARRAY));
        assert_eq!(array_oid_for_element(OID_INT4_ARRAY), None);
        assert_eq!(array_type_name(OID_INT4), "int4[]");
        assert_eq!(array_type_name(OID_BYTEA), "bytea[]");
    }
}
