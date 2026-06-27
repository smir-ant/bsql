//! Server-frame builder vocabulary — pure functions over the PUBLIC wire
//! constants (`bsql_postgres_proto::wire::TAG_*`), no engine state. Each
//! builder names the PostgreSQL message it produces so a fixture reads like
//! the wire trace it emulates.
//!
//! These are fixture builders: a length that cannot fit the wire's fixed-width
//! field is a malformed-fixture programming error, so they panic loudly (the
//! sanctioned test-failure signal) rather than silently truncating — never a
//! production data path.

// Fixture-builder loud-fail: a body too large for the wire length field is a
// corpus authoring bug, surfaced as an immediate panic, not a silent
// saturating fallback. This is a dev-only verification crate; the builders
// have no production caller.
#![allow(
    clippy::panic,
    reason = "fixture builders panic on malformed synthetic input (a body exceeding a fixed-width wire length field) as the loud test-failure signal; this is a dev-only verification crate with no production data path"
)]

use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE,
    TAG_COMMAND_COMPLETE, TAG_COPY_DATA, TAG_COPY_DONE, TAG_COPY_OUT_RESPONSE, TAG_DATA_ROW,
    TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE, TAG_NO_DATA, TAG_NOTICE_RESPONSE,
    TAG_NOTIFICATION_RESPONSE, TAG_PARAMETER_DESCRIPTION, TAG_PARAMETER_STATUS, TAG_PARSE_COMPLETE,
    TAG_PORTAL_SUSPENDED, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};

/// PostgreSQL `int4` OID for the `text` type — the column type used by the
/// minimal `RowDescription` builder.
pub const OID_TEXT: i32 = 25;
/// PostgreSQL `int4` OID for the `int4` type.
pub const OID_INT4: i32 = 23;

/// Transaction-status byte for `ReadyForQuery`: `I` = idle (not in a
/// transaction block).
pub const TX_IDLE: u8 = b'I';
/// Transaction-status byte for `ReadyForQuery`: `T` = in a transaction block.
pub const TX_IN_TX: u8 = b'T';
/// Transaction-status byte for `ReadyForQuery`: `E` = failed transaction
/// (commands are rejected until `ROLLBACK`).
pub const TX_FAILED: u8 = b'E';

/// `CopyData` body format byte: `0` = textual COPY (the common
/// `COPY … TO STDOUT` default).
pub const COPY_FORMAT_TEXT: u8 = 0;

/// Wrap `body` in a PG frame: tag byte + 4-byte big-endian length (the length
/// counts itself but not the tag) + body.
#[must_use]
pub fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("corpus fixture: frame body too large for the u32 wire length field");
    };
    let mut out = Vec::with_capacity(body.len().saturating_add(5));
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// `AuthenticationOk`: tag `R`, length 8, sub-code 0.
#[must_use]
pub fn auth_ok() -> Vec<u8> {
    frame(TAG_AUTHENTICATION.byte(), &0i32.to_be_bytes())
}

/// `BackendKeyData`: tag `K`, 8-byte payload (pid + secret key).
#[must_use]
pub fn backend_key_data(pid: i32, secret_key: i32) -> Vec<u8> {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&secret_key.to_be_bytes());
    frame(TAG_BACKEND_KEY_DATA.byte(), &body)
}

/// `ReadyForQuery`: tag `Z`, length 5, 1-byte transaction status.
#[must_use]
pub fn ready_for_query(tx_status: u8) -> Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), &[tx_status])
}

/// `ParameterStatus`: tag `S`, `key\0value\0`.
#[must_use]
pub fn parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(TAG_PARAMETER_STATUS.byte(), &body)
}

/// `RowDescription`: tag `T`, one entry per column. Each entry is
/// `name\0` + table-oid(i32=0) + attnum(i16) + type-oid(i32) +
/// type-size(i16=-1) + type-mod(i32=-1) + format(i16=0 text).
#[must_use]
pub fn row_description(columns: &[(&str, i32)]) -> Vec<u8> {
    let Ok(n) = i16::try_from(columns.len()) else {
        panic!("corpus fixture: RowDescription column count exceeds i16::MAX");
    };
    let mut body = Vec::new();
    body.extend_from_slice(&n.to_be_bytes());
    for (i, (name, type_oid)) in columns.iter().enumerate() {
        let Ok(attnum) = i16::try_from(i.saturating_add(1)) else {
            panic!("corpus fixture: RowDescription attnum exceeds i16::MAX");
        };
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes()); // table oid
        body.extend_from_slice(&attnum.to_be_bytes());
        body.extend_from_slice(&type_oid.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes()); // type size
        body.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        body.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

/// `DataRow`: tag `D`, column count + per-column `(len i32, bytes)` where a
/// `None` cell is encoded as the SQL-NULL sentinel length `-1`.
#[must_use]
pub fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let Ok(n) = i16::try_from(cells.len()) else {
        panic!("corpus fixture: DataRow column count exceeds i16::MAX");
    };
    let mut body = Vec::new();
    body.extend_from_slice(&n.to_be_bytes());
    for cell in cells {
        match cell {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(bytes) => {
                let Ok(len) = i32::try_from(bytes.len()) else {
                    panic!("corpus fixture: DataRow cell exceeds i32::MAX");
                };
                body.extend_from_slice(&len.to_be_bytes());
                body.extend_from_slice(bytes);
            }
        }
    }
    frame(TAG_DATA_ROW.byte(), &body)
}

/// `CommandComplete`: tag `C`, NUL-terminated tag string (e.g. `"SELECT 1"`).
#[must_use]
pub fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::from(tag.as_bytes());
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

/// `EmptyQueryResponse`: tag `I`, empty body.
#[must_use]
pub fn empty_query_response() -> Vec<u8> {
    frame(TAG_EMPTY_QUERY_RESPONSE.byte(), &[])
}

/// `ParseComplete`: tag `1`, empty body.
#[must_use]
pub fn parse_complete() -> Vec<u8> {
    frame(TAG_PARSE_COMPLETE.byte(), &[])
}

/// `BindComplete`: tag `2`, empty body.
#[must_use]
pub fn bind_complete() -> Vec<u8> {
    frame(TAG_BIND_COMPLETE.byte(), &[])
}

/// `NoData`: tag `n`, empty body (a described statement returns no rows).
#[must_use]
pub fn no_data() -> Vec<u8> {
    frame(TAG_NO_DATA.byte(), &[])
}

/// `CloseComplete`: tag `3`, empty body. (The outbound Close request uses tag
/// byte `C`; the inbound CloseComplete frame is the distinct byte `3`.)
#[must_use]
pub fn close_complete() -> Vec<u8> {
    // The outbound Close request reuses tag byte 'C'; the inbound
    // CloseComplete frame is tag byte '3'. Use the byte literal directly: the
    // public wire module exposes the outbound Close tag, not a distinct
    // CloseComplete constant.
    frame(b'3', &[])
}

/// `ParameterDescription`: tag `t`, param count + one type-OID (i32) each.
#[must_use]
pub fn parameter_description(param_oids: &[i32]) -> Vec<u8> {
    let Ok(n) = i16::try_from(param_oids.len()) else {
        panic!("corpus fixture: ParameterDescription count exceeds i16::MAX");
    };
    let mut body = Vec::new();
    body.extend_from_slice(&n.to_be_bytes());
    for oid in param_oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    frame(TAG_PARAMETER_DESCRIPTION.byte(), &body)
}

/// Build the field body shared by `ErrorResponse` / `NoticeResponse`:
/// `S<severity>\0C<sqlstate>\0M<message>\0` then a terminating `\0`.
fn diagnostic_body(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
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
    body
}

/// `ErrorResponse`: tag `E`, `S`/`C`/`M` fields.
#[must_use]
pub fn error_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(
        TAG_ERROR_RESPONSE.byte(),
        &diagnostic_body(severity, sqlstate, message),
    )
}

/// `NoticeResponse`: tag `N`, `S`/`C`/`M` fields.
#[must_use]
pub fn notice_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(
        TAG_NOTICE_RESPONSE.byte(),
        &diagnostic_body(severity, sqlstate, message),
    )
}

/// `NotificationResponse`: tag `A`, pid(i32) + `channel\0payload\0`.
#[must_use]
pub fn notification_response(pid: i32, channel: &str, payload: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(channel.as_bytes());
    body.push(0);
    body.extend_from_slice(payload.as_bytes());
    body.push(0);
    frame(TAG_NOTIFICATION_RESPONSE.byte(), &body)
}

/// `PortalSuspended`: tag `s`, empty body (PG §55.2.7 — a row-limited
/// `Execute` paused at its `max_rows` cap before the portal exhausted).
#[must_use]
pub fn portal_suspended() -> Vec<u8> {
    frame(TAG_PORTAL_SUSPENDED.byte(), &[])
}

/// `CopyOutResponse`: tag `H`, `format` byte + column count(i16) + per-column
/// format(i16). The minimal textual COPY header has format `0` and `n_cols`
/// columns each with format `0`.
#[must_use]
pub fn copy_out_response(n_cols: i16) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(COPY_FORMAT_TEXT);
    body.extend_from_slice(&n_cols.to_be_bytes());
    for _ in 0..n_cols.max(0) {
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(TAG_COPY_OUT_RESPONSE.byte(), &body)
}

/// `CopyData`: tag `d`, the raw chunk body verbatim (one row of textual COPY
/// output, or an arbitrary binary slice).
#[must_use]
pub fn copy_data(bytes: &[u8]) -> Vec<u8> {
    frame(TAG_COPY_DATA.byte(), bytes)
}

/// `CopyDone`: tag `c`, empty body — the server signals no more `CopyData`
/// follows.
#[must_use]
pub fn copy_done() -> Vec<u8> {
    frame(TAG_COPY_DONE.byte(), &[])
}

/// Build an `ErrorResponse` / `NoticeResponse` body from an explicit list of
/// `(field_byte, text)` pairs plus the terminating `\0`. Lets a fixture carry
/// the full PG §55.7 diagnostic field set (`D` detail, `H` hint, `P` position,
/// `s` schema, `t` table, `c` column, `n` constraint, …) in wire order.
#[must_use]
pub fn diagnostic_fields(fields: &[(u8, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (tag, text) in fields {
        body.push(*tag);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0); // field-list terminator
    body
}

/// `ErrorResponse` with an explicit field list (for fixtures exercising the
/// full diagnostic field set). Tag `E`.
#[must_use]
pub fn error_response_fields(fields: &[(u8, &str)]) -> Vec<u8> {
    frame(TAG_ERROR_RESPONSE.byte(), &diagnostic_fields(fields))
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
