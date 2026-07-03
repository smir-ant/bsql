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

use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE, TAG_DATA_ROW,
    TAG_ERROR_RESPONSE, TAG_NOTIFICATION_RESPONSE, TAG_PARAMETER_STATUS, TAG_PARSE_COMPLETE,
    TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};

/// PostgreSQL type OID for `int8` (`bigint`) — the wire type a scripted `i64`
/// column advertises.
pub const OID_INT8: i32 = 20;
/// PostgreSQL type OID for `int4` (`integer`).
pub const OID_INT4: i32 = 23;
/// PostgreSQL type OID for `text`.
pub const OID_TEXT: i32 = 25;
/// PostgreSQL type OID for `bool`.
pub const OID_BOOL: i32 = 16;

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
}

impl core::fmt::Display for FakeEncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let msg = match self {
            Self::FrameTooLarge => "fake reply frame body exceeds the u32 wire length field",
            Self::CountTooLarge => "fake reply column/row count exceeds the i16 wire field",
            Self::CellTooLarge => "fake reply cell value exceeds the i32 wire length field",
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
        OID_INT8 => 8,
        OID_INT4 => 4,
        OID_BOOL => 1,
        // `text` and anything else the fake does not model as fixed-width.
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

#[cfg(test)]
mod tests {
    //! The fake's binary cell bytes MUST be exactly what the flagship
    //! `query!` decoder reads. These offline round-trips prove that by
    //! decoding each encoder's output with the REAL `Cell<BinaryFmt>` — no
    //! engine, no network. A wrong byte layout fails here, so a wire-incorrect
    //! binary encoding is impossible to ship.

    use bsql_postgres_proto::{BinaryFmt, Cell};

    use super::{binary_bool, binary_int4, binary_int8, binary_text};

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
}
