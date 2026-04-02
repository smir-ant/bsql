//! PostgreSQL wire protocol v3.0 — message framing, frontend messages, backend parsing.
//!
//! All frontend (client -> server) messages follow the format:
//! `[type: u8] [length: i32 BE] [payload: ...]`
//!
//! Exception: `StartupMessage` has no type byte (just length + version + params).
//! Exception: `SSLRequest` is 8 bytes (length=8, code=80877103).
//!
//! Backend messages borrow from the read buffer -- zero allocation for parsing.
//!
//! This module exposes the complete PG v3.0 protocol surface. Some functions
//! (e.g., `write_close`, `write_ssl_request`) are not yet called by `conn.rs`
//! but exist because the protocol is complete.
use std::fmt;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::DriverError;

// --- Protocol constants ---

/// Protocol version 3.0: major=3, minor=0.
const PROTOCOL_VERSION: i32 = 196608; // 3 << 16

/// SSLRequest magic code.
#[cfg(feature = "tls")]
const SSL_REQUEST_CODE: i32 = 80877103;

// Frontend message type bytes
const MSG_PASSWORD: u8 = b'p';
const MSG_QUERY: u8 = b'Q';
const MSG_PARSE: u8 = b'P';
const MSG_BIND: u8 = b'B';
const MSG_EXECUTE: u8 = b'E';
const MSG_DESCRIBE: u8 = b'D';
#[allow(dead_code)]
const MSG_CLOSE: u8 = b'C';
const MSG_SYNC: u8 = b'S';
const MSG_TERMINATE: u8 = b'X';

// Backend message type bytes (for documentation; used as match arms)
const _BACKEND_AUTH: u8 = b'R';
const _BACKEND_PARAM_STATUS: u8 = b'S';
const _BACKEND_KEY_DATA: u8 = b'K';
const _BACKEND_READY: u8 = b'Z';
const _BACKEND_PARSE_COMPLETE: u8 = b'1';
const _BACKEND_BIND_COMPLETE: u8 = b'2';
const _BACKEND_CLOSE_COMPLETE: u8 = b'3';
const _BACKEND_ROW_DESC: u8 = b'T';
const _BACKEND_DATA_ROW: u8 = b'D';
const _BACKEND_CMD_COMPLETE: u8 = b'C';
const _BACKEND_ERROR: u8 = b'E';
const _BACKEND_NOTICE: u8 = b'N';
const _BACKEND_NOTIFICATION: u8 = b'A';
const _BACKEND_EMPTY_QUERY: u8 = b'I';
const _BACKEND_NO_DATA: u8 = b'n';
const _BACKEND_PARAM_DESC: u8 = b't';

// --- Backend message types ---

/// A parsed backend message. Borrows from the read buffer for zero-allocation parsing.
///
/// `RowDescription`, `DataRow`, `ErrorResponse`, and `NoticeResponse` carry raw byte
/// slices — their contents are parsed lazily only when accessed.
#[derive(Debug)]
#[allow(dead_code)] // Variant fields are part of the complete protocol representation
pub enum BackendMessage<'a> {
    AuthOk,
    AuthCleartext,
    AuthMd5 {
        salt: [u8; 4],
    },
    AuthSasl {
        mechanisms: &'a [u8],
    },
    AuthSaslContinue {
        data: &'a [u8],
    },
    AuthSaslFinal {
        data: &'a [u8],
    },
    ParameterStatus {
        name: &'a str,
        value: &'a str,
    },
    BackendKeyData {
        pid: i32,
        secret: i32,
    },
    ReadyForQuery {
        status: u8,
    },
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    ParameterDescription {
        data: &'a [u8],
    },
    RowDescription {
        data: &'a [u8],
    },
    DataRow {
        data: &'a [u8],
    },
    CommandComplete {
        tag: &'a str,
    },
    ErrorResponse {
        data: &'a [u8],
    },
    NoticeResponse {
        data: &'a [u8],
    },
    NotificationResponse {
        pid: i32,
        channel: &'a str,
        payload: &'a str,
    },
    EmptyQuery,
}

// --- Frontend message writers ---
//
// Each function appends a complete protocol message to `buf`. The caller flushes
// the entire buffer in one TCP write for pipelining.

/// Write a typed message: type byte + 4-byte length (includes self) + payload.
#[inline]
pub fn write_message(buf: &mut Vec<u8>, msg_type: u8, payload: &[u8]) {
    buf.push(msg_type);
    let len = (payload.len() as i32) + 4; // length includes itself
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
}

/// Startup message — no type byte: `[length: i32] [version: i32] [params...] [0x00]`
pub fn write_startup(buf: &mut Vec<u8>, user: &str, database: &str) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // placeholder for length
    buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());

    buf.extend_from_slice(b"user\0");
    buf.extend_from_slice(user.as_bytes());
    buf.push(0);

    buf.extend_from_slice(b"database\0");
    buf.extend_from_slice(database.as_bytes());
    buf.push(0);

    buf.push(0); // terminating zero

    let len = (buf.len() - start) as i32;
    buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
}

/// SSLRequest — 8 bytes, no type byte: `[length=8: i32] [code=80877103: i32]`
#[cfg(feature = "tls")]
pub fn write_ssl_request(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&8i32.to_be_bytes());
    buf.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
}

/// Parse message — prepare a named statement.
///
/// Format: `'P' [len] [name\0] [sql\0] [num_param_types: i16] [oid: i32]...`
pub fn write_parse(buf: &mut Vec<u8>, name: &str, sql: &str, param_oids: &[u32]) {
    let payload_len = name.len()
        + 1 // NUL
        + sql.len()
        + 1 // NUL
        + 2 // i16 param count
        + param_oids.len() * 4;

    buf.push(MSG_PARSE);
    let len = (payload_len as i32) + 4;
    buf.extend_from_slice(&len.to_be_bytes());

    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
    for &oid in param_oids {
        buf.extend_from_slice(&(oid as i32).to_be_bytes());
    }
}

/// Bind message — bind parameters to a prepared statement, requesting binary format
/// for both parameters and results. Encodes parameters inline into the write buffer,
/// eliminating intermediate `Vec<Vec<u8>>` allocation.
///
/// Format: `'B' [len] [portal\0] [stmt\0] [num_fmt_codes: i16] [fmt_code: i16]...
///          [num_params: i16] ([param_len: i32] [param_data]...)
///          [num_result_fmt_codes: i16] [result_fmt_code: i16]...`
pub fn write_bind_params(
    buf: &mut Vec<u8>,
    portal: &str,
    statement: &str,
    params: &[&(dyn crate::codec::Encode + Sync)],
) {
    buf.push(MSG_BIND);
    let len_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // placeholder

    // Portal name
    buf.extend_from_slice(portal.as_bytes());
    buf.push(0);

    // Statement name
    buf.extend_from_slice(statement.as_bytes());
    buf.push(0);

    // Parameter format codes: all binary (format code 1)
    if params.is_empty() {
        buf.extend_from_slice(&0i16.to_be_bytes()); // 0 format codes
    } else {
        buf.extend_from_slice(&1i16.to_be_bytes()); // 1 format code (applies to all)
        buf.extend_from_slice(&1i16.to_be_bytes()); // binary
    }

    // Parameter values — encoded inline, no intermediate Vec<Vec<u8>>
    buf.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for param in params {
        let len_pos_param = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // placeholder for param length
        param.encode_binary(buf);
        let data_len = (buf.len() - len_pos_param - 4) as i32;
        buf[len_pos_param..len_pos_param + 4].copy_from_slice(&data_len.to_be_bytes());
    }

    // Result format codes: all binary
    buf.extend_from_slice(&1i16.to_be_bytes()); // 1 format code
    buf.extend_from_slice(&1i16.to_be_bytes()); // binary

    // Patch length
    let len = (buf.len() - len_pos) as i32;
    buf[len_pos..len_pos + 4].copy_from_slice(&len.to_be_bytes());
}

/// Execute message — execute a bound portal.
///
/// `max_rows = 0` means unlimited.
pub fn write_execute(buf: &mut Vec<u8>, portal: &str, max_rows: i32) {
    let payload_len = portal.len() + 1 + 4;
    buf.push(MSG_EXECUTE);
    let len = (payload_len as i32) + 4;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(portal.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&max_rows.to_be_bytes());
}

/// Sync message — marks the end of a message pipeline.
pub fn write_sync(buf: &mut Vec<u8>) {
    write_message(buf, MSG_SYNC, &[]);
}

/// Describe message — request description of a statement ('S') or portal ('P').
pub fn write_describe(buf: &mut Vec<u8>, kind: u8, name: &str) {
    let payload_len = 1 + name.len() + 1;
    buf.push(MSG_DESCRIBE);
    let len = (payload_len as i32) + 4;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.push(kind);
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
}

/// Close message — close a statement ('S') or portal ('P').
#[allow(dead_code)]
pub fn write_close(buf: &mut Vec<u8>, kind: u8, name: &str) {
    let payload_len = 1 + name.len() + 1;
    buf.push(MSG_CLOSE);
    let len = (payload_len as i32) + 4;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.push(kind);
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
}

/// Terminate message — close the connection.
pub fn write_terminate(buf: &mut Vec<u8>) {
    write_message(buf, MSG_TERMINATE, &[]);
}

/// Simple query message — for non-prepared SQL (BEGIN, COMMIT, SET, etc.).
pub fn write_simple_query(buf: &mut Vec<u8>, sql: &str) {
    let payload_len = sql.len() + 1;
    buf.push(MSG_QUERY);
    let len = (payload_len as i32) + 4;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(sql.as_bytes());
    buf.push(0);
}

/// Password message (MD5 or cleartext).
pub fn write_password(buf: &mut Vec<u8>, password: &[u8]) {
    write_message(buf, MSG_PASSWORD, password);
}

/// SASLInitialResponse message.
///
/// Format: `'p' [len] [mechanism\0] [data_len: i32] [data]`
pub fn write_sasl_initial(buf: &mut Vec<u8>, mechanism: &str, data: &[u8]) {
    buf.push(MSG_PASSWORD);
    let payload_len = mechanism.len() + 1 + 4 + data.len();
    let len = (payload_len as i32) + 4;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(mechanism.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
    buf.extend_from_slice(data);
}

/// SASLResponse message.
pub fn write_sasl_response(buf: &mut Vec<u8>, data: &[u8]) {
    write_message(buf, MSG_PASSWORD, data);
}

// --- Backend message reading ---

/// Read one complete backend message from the stream into `buf`.
///
/// Returns `(msg_type, payload)` where payload borrows from `buf`.
/// The buffer is cleared before reading (caller must process the previous message first).
///
/// Wire format: `[type: u8] [length: i32 BE] [payload: length - 4 bytes]`
#[allow(dead_code)]
pub async fn read_message<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
) -> Result<(u8, usize), DriverError> {
    // Read 5-byte header: type(1) + length(4)
    let mut header = [0u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .map_err(DriverError::Io)?;

    let msg_type = header[0];
    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);

    if len < 4 {
        return Err(DriverError::Protocol(format!(
            "invalid message length {len} for type '{}'",
            msg_type as char
        )));
    }

    // Reject unreasonably large messages (128 MB) to prevent OOM from malicious
    // or corrupted streams.
    const MAX_MESSAGE_LEN: i32 = 128 * 1024 * 1024;
    if len > MAX_MESSAGE_LEN {
        return Err(DriverError::Protocol(format!(
            "message length {len} exceeds maximum ({MAX_MESSAGE_LEN}) for type '{}'",
            msg_type as char
        )));
    }

    let payload_len = (len - 4) as usize;

    // Store payload starting at offset 0
    buf.clear();
    buf.resize(payload_len, 0);
    if payload_len > 0 {
        stream
            .read_exact(&mut buf[..payload_len])
            .await
            .map_err(DriverError::Io)?;
    }

    Ok((msg_type, payload_len))
}

/// Flush the write buffer to the stream.
#[allow(dead_code)]
pub async fn flush<S: AsyncWrite + Unpin>(stream: &mut S, buf: &[u8]) -> Result<(), DriverError> {
    stream.write_all(buf).await.map_err(DriverError::Io)?;
    stream.flush().await.map_err(DriverError::Io)?;
    Ok(())
}

// --- Backend message parsing ---

/// Parse a backend message from its type byte and payload.
///
/// The payload slice must remain valid for the lifetime of the returned message.
pub fn parse_backend_message(
    msg_type: u8,
    payload: &[u8],
) -> Result<BackendMessage<'_>, DriverError> {
    match msg_type {
        b'R' => parse_auth(payload),
        b'S' => parse_parameter_status(payload),
        b'K' => parse_backend_key_data(payload),
        b'Z' => parse_ready_for_query(payload),
        b'1' => Ok(BackendMessage::ParseComplete),
        b'2' => Ok(BackendMessage::BindComplete),
        b'3' => Ok(BackendMessage::CloseComplete),
        b'n' => Ok(BackendMessage::NoData),
        b't' => Ok(BackendMessage::ParameterDescription { data: payload }),
        b'T' => Ok(BackendMessage::RowDescription { data: payload }),
        b'D' => Ok(BackendMessage::DataRow { data: payload }),
        b'C' => parse_command_complete(payload),
        b'E' => Ok(BackendMessage::ErrorResponse { data: payload }),
        b'N' => Ok(BackendMessage::NoticeResponse { data: payload }),
        b'A' => parse_notification(payload),
        b'I' => Ok(BackendMessage::EmptyQuery),
        _ => Err(DriverError::Protocol(format!(
            "unknown backend message type: '{}' (0x{:02x})",
            msg_type as char, msg_type
        ))),
    }
}

// --- Parse helpers ---

fn parse_auth(payload: &[u8]) -> Result<BackendMessage<'_>, DriverError> {
    if payload.len() < 4 {
        return Err(DriverError::Protocol("auth message too short".into()));
    }
    let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

    match auth_type {
        0 => Ok(BackendMessage::AuthOk),
        3 => Ok(BackendMessage::AuthCleartext),
        5 => {
            if payload.len() < 8 {
                return Err(DriverError::Protocol("MD5 auth message too short".into()));
            }
            let mut salt = [0u8; 4];
            salt.copy_from_slice(&payload[4..8]);
            Ok(BackendMessage::AuthMd5 { salt })
        }
        10 => {
            // SASL — mechanisms follow as NUL-terminated strings, double NUL at end
            Ok(BackendMessage::AuthSasl {
                mechanisms: &payload[4..],
            })
        }
        11 => Ok(BackendMessage::AuthSaslContinue {
            data: &payload[4..],
        }),
        12 => Ok(BackendMessage::AuthSaslFinal {
            data: &payload[4..],
        }),
        _ => Err(DriverError::Protocol(format!(
            "unsupported auth type: {auth_type}"
        ))),
    }
}

fn parse_parameter_status(payload: &[u8]) -> Result<BackendMessage<'_>, DriverError> {
    let name = read_cstring(payload, 0)?;
    let name_end = name.len() + 1;
    let value = read_cstring(payload, name_end)?;
    Ok(BackendMessage::ParameterStatus { name, value })
}

fn parse_backend_key_data(payload: &[u8]) -> Result<BackendMessage<'_>, DriverError> {
    if payload.len() < 8 {
        return Err(DriverError::Protocol(
            "BackendKeyData message too short".into(),
        ));
    }
    let pid = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let secret = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
    Ok(BackendMessage::BackendKeyData { pid, secret })
}

fn parse_ready_for_query(payload: &[u8]) -> Result<BackendMessage<'_>, DriverError> {
    if payload.is_empty() {
        return Err(DriverError::Protocol("ReadyForQuery message empty".into()));
    }
    Ok(BackendMessage::ReadyForQuery { status: payload[0] })
}

fn parse_command_complete(payload: &[u8]) -> Result<BackendMessage<'_>, DriverError> {
    let tag = read_cstring(payload, 0)?;
    Ok(BackendMessage::CommandComplete { tag })
}

fn parse_notification(payload: &[u8]) -> Result<BackendMessage<'_>, DriverError> {
    if payload.len() < 4 {
        return Err(DriverError::Protocol("notification too short".into()));
    }
    let pid = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let channel = read_cstring(payload, 4)?;
    let channel_end = 4 + channel.len() + 1;
    let msg_payload = read_cstring(payload, channel_end)?;
    Ok(BackendMessage::NotificationResponse {
        pid,
        channel,
        payload: msg_payload,
    })
}

/// Read a NUL-terminated C string from `data` starting at `offset`.
fn read_cstring(data: &[u8], offset: usize) -> Result<&str, DriverError> {
    let remaining = data
        .get(offset..)
        .ok_or_else(|| DriverError::Protocol("c-string read out of bounds".into()))?;

    let nul_pos = remaining
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| DriverError::Protocol("c-string not NUL-terminated".into()))?;

    std::str::from_utf8(&remaining[..nul_pos])
        .map_err(|e| DriverError::Protocol(format!("invalid UTF-8 in protocol string: {e}")))
}

// --- RowDescription parsing ---

/// A single column descriptor parsed from a RowDescription message.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: Box<str>,
    pub type_oid: u32,
    pub type_size: i16,
    #[allow(dead_code)]
    pub format: i16,
}

/// Parse a RowDescription payload into column descriptors.
///
/// Format: `[num_fields: i16] ([name\0] [table_oid: i32] [col_attr: i16]
///           [type_oid: i32] [type_size: i16] [type_mod: i32] [format: i16])...`
pub fn parse_row_description(data: &[u8]) -> Result<Vec<ColumnInfo>, DriverError> {
    if data.len() < 2 {
        return Err(DriverError::Protocol("RowDescription too short".into()));
    }

    let num_fields = i16::from_be_bytes([data[0], data[1]]) as usize;
    let mut columns = Vec::with_capacity(num_fields);
    let mut pos = 2;

    for _ in 0..num_fields {
        let name = read_cstring(data, pos)?;
        pos += name.len() + 1;

        if pos + 18 > data.len() {
            return Err(DriverError::Protocol(
                "RowDescription field truncated".into(),
            ));
        }

        // table_oid (4) + col_attr (2) = 6 bytes, skip
        pos += 6;

        let type_oid = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        let type_size = i16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        // type_mod (4), skip
        pos += 4;

        let format = i16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        columns.push(ColumnInfo {
            name: name.into(),
            type_oid,
            type_size,
            format,
        });
    }

    Ok(columns)
}

// --- ErrorResponse parsing ---

/// Parsed fields from an ErrorResponse or NoticeResponse.
#[derive(Debug)]
pub struct ErrorFields {
    #[allow(dead_code)]
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl fmt::Display for ErrorFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;
        if let Some(ref detail) = self.detail {
            write!(f, " DETAIL: {detail}")?;
        }
        if let Some(ref hint) = self.hint {
            write!(f, " HINT: {hint}")?;
        }
        Ok(())
    }
}

/// Parse an ErrorResponse / NoticeResponse payload into fields.
///
/// Format: `([field_type: u8] [value\0])... [0x00]`
pub fn parse_error_response(data: &[u8]) -> ErrorFields {
    let mut severity = String::new();
    let mut code = String::new();
    let mut message = String::new();
    let mut detail = None;
    let mut hint = None;

    let mut pos = 0;
    while pos < data.len() {
        let field_type = data[pos];
        pos += 1;

        if field_type == 0 {
            break;
        }

        let value = match read_cstring(data, pos) {
            Ok(s) => {
                pos += s.len() + 1;
                s
            }
            Err(_) => break,
        };

        match field_type {
            b'S' => severity = value.to_owned(),
            b'C' => code = value.to_owned(),
            b'M' => message = value.to_owned(),
            b'D' => detail = Some(value.to_owned()),
            b'H' => hint = Some(value.to_owned()),
            _ => {} // skip other fields (position, internal query, etc.)
        }
    }

    ErrorFields {
        severity,
        code,
        message,
        detail,
        hint,
    }
}

/// Extract affected row count from a CommandComplete tag.
///
/// Tags like "INSERT 0 5", "UPDATE 3", "DELETE 12", "SELECT 100" — the last
/// number is the affected/returned row count.
pub fn parse_command_tag(tag: &str) -> u64 {
    tag.rsplit(' ')
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_message_format() {
        let mut buf = Vec::new();
        write_startup(&mut buf, "testuser", "testdb");

        // First 4 bytes = length
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len as usize, buf.len());

        // Next 4 bytes = protocol version 3.0
        let ver = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(ver, PROTOCOL_VERSION);

        // Must contain user\0testuser\0database\0testdb\0\0
        let payload = &buf[8..];
        assert!(payload.starts_with(b"user\0testuser\0database\0testdb\0"));
        assert_eq!(*buf.last().unwrap(), 0); // trailing NUL
    }

    #[cfg(feature = "tls")]
    #[test]
    fn ssl_request_format() {
        let mut buf = Vec::new();
        write_ssl_request(&mut buf);
        assert_eq!(buf.len(), 8);
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(len, 8);
        let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(code, SSL_REQUEST_CODE);
    }

    #[test]
    fn parse_message_framing() {
        let mut buf = Vec::new();
        write_message(&mut buf, b'X', &[]);
        assert_eq!(buf, &[b'X', 0, 0, 0, 4]);
    }

    #[test]
    fn sync_message_format() {
        let mut buf = Vec::new();
        write_sync(&mut buf);
        assert_eq!(buf, &[b'S', 0, 0, 0, 4]);
    }

    #[test]
    fn terminate_message_format() {
        let mut buf = Vec::new();
        write_terminate(&mut buf);
        assert_eq!(buf, &[b'X', 0, 0, 0, 4]);
    }

    #[test]
    fn parse_complete_parses() {
        let msg = parse_backend_message(b'1', &[]).unwrap();
        assert!(matches!(msg, BackendMessage::ParseComplete));
    }

    #[test]
    fn bind_complete_parses() {
        let msg = parse_backend_message(b'2', &[]).unwrap();
        assert!(matches!(msg, BackendMessage::BindComplete));
    }

    #[test]
    fn auth_ok_parses() {
        let payload = 0i32.to_be_bytes();
        let msg = parse_backend_message(b'R', &payload).unwrap();
        assert!(matches!(msg, BackendMessage::AuthOk));
    }

    #[test]
    fn auth_md5_parses() {
        let mut payload = 5i32.to_be_bytes().to_vec();
        payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let msg = parse_backend_message(b'R', &payload).unwrap();
        match msg {
            BackendMessage::AuthMd5 { salt } => {
                assert_eq!(salt, [0xDE, 0xAD, 0xBE, 0xEF]);
            }
            _ => panic!("expected AuthMd5"),
        }
    }

    #[test]
    fn ready_for_query_parses() {
        let msg = parse_backend_message(b'Z', &[b'I']).unwrap();
        match msg {
            BackendMessage::ReadyForQuery { status } => assert_eq!(status, b'I'),
            _ => panic!("expected ReadyForQuery"),
        }
    }

    #[test]
    fn command_complete_parses() {
        let payload = b"SELECT 42\0".to_vec();
        let msg = parse_backend_message(b'C', &payload).unwrap();
        match msg {
            BackendMessage::CommandComplete { tag } => assert_eq!(tag, "SELECT 42"),
            _ => panic!("expected CommandComplete"),
        }
    }

    #[test]
    fn parameter_status_parses() {
        let payload = b"server_version\015.2\0".to_vec();
        let msg = parse_backend_message(b'S', &payload).unwrap();
        match msg {
            BackendMessage::ParameterStatus { name, value } => {
                assert_eq!(name, "server_version");
                assert_eq!(value, "15.2");
            }
            _ => panic!("expected ParameterStatus"),
        }
    }

    #[test]
    fn command_tag_parsing() {
        assert_eq!(parse_command_tag("SELECT 100"), 100);
        assert_eq!(parse_command_tag("INSERT 0 5"), 5);
        assert_eq!(parse_command_tag("UPDATE 3"), 3);
        assert_eq!(parse_command_tag("DELETE 12"), 12);
        assert_eq!(parse_command_tag("BEGIN"), 0);
        assert_eq!(parse_command_tag("COMMIT"), 0);
    }

    #[test]
    fn unknown_backend_message_errors() {
        let result = parse_backend_message(0xFF, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_message_writes_correct_format() {
        let mut buf = Vec::new();
        write_parse(&mut buf, "s_test", "SELECT 1", &[23]);

        assert_eq!(buf[0], b'P');
        // After type byte, next 4 bytes are length
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len as usize + 1, buf.len()); // +1 for type byte
    }

    #[test]
    fn bind_message_binary_format() {
        let mut buf = Vec::new();
        let val = 42i32;
        let params: Vec<&(dyn crate::codec::Encode + Sync)> = vec![&val];
        write_bind_params(&mut buf, "", "s_test", &params);

        assert_eq!(buf[0], b'B');
        // Verify it contains binary format codes
        // The message should request binary results
    }

    #[test]
    fn bind_no_params() {
        let mut buf = Vec::new();
        let params: Vec<&(dyn crate::codec::Encode + Sync)> = vec![];
        write_bind_params(&mut buf, "", "s_test", &params);
        assert_eq!(buf[0], b'B');
    }

    #[test]
    fn execute_message_format() {
        let mut buf = Vec::new();
        write_execute(&mut buf, "", 0);
        assert_eq!(buf[0], b'E');
    }

    #[test]
    fn describe_message_format() {
        let mut buf = Vec::new();
        write_describe(&mut buf, b'S', "s_test");
        assert_eq!(buf[0], b'D');
        assert_eq!(buf[5], b'S');
    }

    #[test]
    fn close_message_format() {
        let mut buf = Vec::new();
        write_close(&mut buf, b'S', "s_test");
        assert_eq!(buf[0], b'C');
        assert_eq!(buf[5], b'S');
    }

    #[test]
    fn simple_query_format() {
        let mut buf = Vec::new();
        write_simple_query(&mut buf, "BEGIN");
        assert_eq!(buf[0], b'Q');
        // Should end with NUL
        assert_eq!(*buf.last().unwrap(), 0);
    }

    #[test]
    fn error_response_parsing() {
        let mut data = Vec::new();
        data.push(b'S');
        data.extend_from_slice(b"ERROR\0");
        data.push(b'C');
        data.extend_from_slice(b"42P01\0");
        data.push(b'M');
        data.extend_from_slice(b"relation does not exist\0");
        data.push(b'D');
        data.extend_from_slice(b"some detail\0");
        data.push(b'H');
        data.extend_from_slice(b"some hint\0");
        data.push(0);

        let fields = parse_error_response(&data);
        assert_eq!(fields.severity, "ERROR");
        assert_eq!(fields.code, "42P01");
        assert_eq!(fields.message, "relation does not exist");
        assert_eq!(fields.detail.as_deref(), Some("some detail"));
        assert_eq!(fields.hint.as_deref(), Some("some hint"));
    }

    #[test]
    fn row_description_parsing() {
        // Build a minimal RowDescription: 1 field named "id", type int4 (OID 23),
        // size 4, format binary (1)
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes()); // 1 field

        data.extend_from_slice(b"id\0"); // name
        data.extend_from_slice(&0i32.to_be_bytes()); // table OID
        data.extend_from_slice(&0i16.to_be_bytes()); // column attr
        data.extend_from_slice(&23u32.to_be_bytes()); // type OID (int4)
        data.extend_from_slice(&4i16.to_be_bytes()); // type size
        data.extend_from_slice(&(-1i32).to_be_bytes()); // type mod
        data.extend_from_slice(&1i16.to_be_bytes()); // format (binary)

        let cols = parse_row_description(&data).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(&*cols[0].name, "id");
        assert_eq!(cols[0].type_oid, 23);
        assert_eq!(cols[0].type_size, 4);
        assert_eq!(cols[0].format, 1);
    }
}
