//! Shared types used by both async `Connection` and sync `SyncConnection`.
//!
//! Extracted from `conn.rs` to avoid duplication between the async and sync
//! code paths. Contains configuration, result types, row views, and helpers.

use std::sync::Arc;

use rapidhash::quality::RapidHasher;

use crate::arena::Arena;
use crate::DriverError;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Implements Drop to zeroize the password field, minimizing the
/// window where plaintext credentials live in memory.
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub ssl: SslMode,
    /// PG-side statement timeout in seconds. Default: 30. 0 = no timeout.
    ///
    /// After connecting, the driver sends `SET statement_timeout = '{N}s'`.
    /// If a query exceeds this duration, PostgreSQL kills it and returns an error.
    pub statement_timeout_secs: u32,
}

/// Zeroize password on drop to minimize credential lifetime in memory.
impl Drop for Config {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.password.zeroize();
    }
}

/// SSL/TLS connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Never use TLS.
    Disable,
    /// Try TLS, fall back to plain if server says 'N'.
    Prefer,
    /// Require TLS, fail if server says 'N'.
    Require,
}

impl Config {
    /// Parse a PostgreSQL connection URL.
    ///
    /// Format: `postgres://user:password@host:port/database?sslmode=prefer`
    ///
    /// # Unix domain sockets
    ///
    /// Use the `host` query parameter to specify a UDS directory (libpq convention):
    /// ```text
    /// postgres://user@localhost/dbname?host=/tmp
    /// postgres:///dbname?host=/var/run/postgresql
    /// ```
    /// When `host` starts with `/`, the driver connects via Unix domain socket at
    /// `{host}/.s.PGSQL.{port}` instead of TCP. TLS is skipped for UDS connections.
    pub fn from_url(url: &str) -> Result<Self, DriverError> {
        let url = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .ok_or_else(|| DriverError::Protocol("URL must start with postgres://".into()))?;

        // Split user:password@host:port/database
        let (userinfo, rest) = url
            .split_once('@')
            .ok_or_else(|| DriverError::Protocol("missing @ in connection URL".into()))?;

        let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));

        // Split host:port/database?params
        let (hostport, rest) = rest.split_once('/').unwrap_or((rest, ""));
        let (database, params) = rest.split_once('?').unwrap_or((rest, ""));

        let (host, port) = if let Some((h, p)) = hostport.split_once(':') {
            let port = p
                .parse::<u16>()
                .map_err(|_| DriverError::Protocol(format!("invalid port: {p}")))?;
            (h.to_owned(), port)
        } else {
            (hostport.to_owned(), 5432)
        };

        let mut ssl = SslMode::Prefer;
        let mut statement_timeout_secs: u32 = 30;
        let mut host_override: Option<String> = None;
        for param in params.split('&') {
            if param.is_empty() {
                continue;
            }
            if let Some(val) = param.strip_prefix("sslmode=") {
                // A typo like "sslmode=require" (missing 'e') would go unencrypted.
                ssl = match val {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    _ => {
                        return Err(DriverError::Protocol(format!(
                            "unknown sslmode: '{val}' (expected: disable, prefer, require)"
                        )));
                    }
                };
            } else if let Some(val) = param.strip_prefix("statement_timeout=") {
                statement_timeout_secs = val.parse::<u32>().unwrap_or(30);
            } else if let Some(val) = param.strip_prefix("host=") {
                host_override = Some(url_decode(val)?);
            }
        }

        // If ?host=/path was specified, override the URL hostname with it.
        // This follows the libpq convention: host=/tmp means UDS.
        let final_host = if let Some(h) = host_override {
            h
        } else {
            url_decode(&host)?
        };

        let config = Config {
            host: final_host,
            port,
            user: url_decode(user)?,
            password: url_decode(password)?,
            database: if database.is_empty() {
                url_decode(user)?
            } else {
                url_decode(database)?
            },
            ssl,
            statement_timeout_secs,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration fields before attempting a connection.
    ///
    /// Called automatically by `from_url()`. Call manually if constructing
    /// a `Config` by hand.
    pub fn validate(&self) -> Result<(), DriverError> {
        if self.host.is_empty() {
            return Err(DriverError::Protocol("host cannot be empty".into()));
        }
        if self.user.is_empty() {
            return Err(DriverError::Protocol("user cannot be empty".into()));
        }
        if self.database.is_empty() {
            return Err(DriverError::Protocol("database cannot be empty".into()));
        }
        Ok(())
    }

    /// Returns `true` if the host is a Unix domain socket directory path.
    ///
    /// libpq convention: if `host` starts with `/`, the connection uses a
    /// Unix domain socket at `{host}/.s.PGSQL.{port}`.
    pub fn host_is_uds(&self) -> bool {
        self.host.starts_with('/')
    }

    /// Returns the Unix domain socket path: `{host}/.s.PGSQL.{port}`.
    ///
    /// Only meaningful when [`host_is_uds()`](Self::host_is_uds) returns `true`.
    pub fn uds_path(&self) -> String {
        format!("{}/.s.PGSQL.{}", self.host, self.port)
    }
}

// ---------------------------------------------------------------------------
// url_decode / hex_val
// ---------------------------------------------------------------------------

/// Minimal percent-decoding for connection URL components.
///
/// Decodes `%XX` hex sequences into raw bytes, then validates as UTF-8.
/// This correctly handles multi-byte UTF-8 characters that are percent-encoded
/// byte-by-byte (e.g. `%C3%A9` for 'e').
fn url_decode(s: &str) -> Result<String, DriverError> {
    let mut bytes = Vec::with_capacity(s.len());
    let input = s.as_bytes();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' {
            if i + 2 >= input.len() {
                return Err(DriverError::Protocol(format!(
                    "malformed percent-encoding in URL: '{s}'"
                )));
            }
            let hi = hex_val(input[i + 1]).ok_or_else(|| {
                DriverError::Protocol(format!(
                    "invalid hex digit '{}' in URL: '{s}'",
                    input[i + 1] as char
                ))
            })?;
            let lo = hex_val(input[i + 2]).ok_or_else(|| {
                DriverError::Protocol(format!(
                    "invalid hex digit '{}' in URL: '{s}'",
                    input[i + 2] as char
                ))
            })?;
            bytes.push(hi * 16 + lo);
            i += 3;
        } else {
            bytes.push(input[i]);
            i += 1;
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| DriverError::Protocol(format!("invalid UTF-8 in URL: '{s}'")))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// StartupAction
// ---------------------------------------------------------------------------

/// Owned action from a startup message, avoiding borrow conflicts with `self.read_buf`.
pub(crate) enum StartupAction {
    AuthOk,
    AuthCleartext,
    AuthMd5([u8; 4]),
    AuthSasl(Vec<u8>),
    ParameterStatus(Box<str>, Box<str>),
    BackendKeyData(i32, i32),
    ReadyForQuery(u8),
    Error(String),
    Notice,
}

// ---------------------------------------------------------------------------
// ColumnDesc / PrepareResult / SimpleRow / Notification
// ---------------------------------------------------------------------------

/// Description of a result column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// Column name from the query.
    pub name: Box<str>,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Type size in bytes (-1 for variable-length).
    pub type_size: i16,
    /// OID of the source table (0 if not a table column, e.g. computed).
    pub table_oid: u32,
    /// Column number within the source table (0 if not a table column).
    pub column_id: i16,
}

/// Result of a `prepare_describe` call -- column and parameter metadata
/// without executing the query.
#[derive(Debug, Clone)]
pub struct PrepareResult {
    /// Output columns (empty for INSERT/UPDATE/DELETE without RETURNING).
    pub columns: Vec<ColumnDesc>,
    /// PostgreSQL OIDs of the expected parameter types.
    pub param_oids: Vec<u32>,
}

/// A single row of text values returned by `simple_query_rows`.
///
/// Each field is `None` for SQL NULL, `Some(text)` otherwise.
/// Only used for compile-time schema introspection queries.
pub type SimpleRow = Vec<Option<String>>;

/// A notification received during normal query processing.
///
/// When the read loop encounters a NotificationResponse during queries,
/// it is buffered here instead of being dropped. Call
/// [`Connection::drain_notifications`] to retrieve and clear the buffer.
#[derive(Debug, Clone)]
pub struct Notification {
    /// Backend process ID that sent the notification.
    pub pid: i32,
    /// Channel name.
    pub channel: String,
    /// Payload string (may be empty).
    pub payload: String,
}

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

/// Collected result of a query: all rows' column offsets plus metadata.
///
/// Data lives in an [`Arena`]; this struct holds only the offset/length
/// bookkeeping. Access rows via [`row()`](Self::row) or [`rows()`](Self::rows).
///
/// # Example
///
/// ```ignore
/// for row in result.rows(&arena) {
///     // Access columns by index
/// }
/// ```
pub struct QueryResult {
    /// All rows' column (arena_offset, length) pairs, contiguous.
    /// length = -1 means NULL.
    pub(crate) all_col_offsets: Vec<(usize, i32)>,
    /// Number of columns per row.
    pub(crate) num_cols: usize,
    pub(crate) columns: Arc<[ColumnDesc]>,
    pub(crate) affected_rows: u64,
}

impl QueryResult {
    /// Construct a `QueryResult` from its constituent parts.
    ///
    /// Used by `bsql-core`'s streaming layer to assemble per-chunk results.
    pub fn from_parts(
        all_col_offsets: Vec<(usize, i32)>,
        num_cols: usize,
        columns: Arc<[ColumnDesc]>,
        affected_rows: u64,
    ) -> Self {
        Self {
            all_col_offsets,
            num_cols,
            columns,
            affected_rows,
        }
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        if self.num_cols == 0 {
            return 0;
        }
        self.all_col_offsets.len() / self.num_cols
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.all_col_offsets.is_empty()
    }

    /// Number of affected rows (for INSERT/UPDATE/DELETE).
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Column descriptors.
    pub fn columns(&self) -> &[ColumnDesc] {
        &self.columns
    }

    /// Get a row by index. The returned `Row` borrows from the arena.
    pub fn row<'a>(&'a self, idx: usize, arena: &'a Arena) -> Row<'a> {
        let start = idx * self.num_cols;
        let end = start + self.num_cols;
        Row {
            arena,
            col_offsets: &self.all_col_offsets[start..end],
            columns: &self.columns,
        }
    }

    /// Take the `col_offsets` vec out of this result, leaving it empty.
    ///
    /// Used by `QueryStream` to reclaim and reuse the allocation between chunks
    /// instead of allocating a new `Vec` per chunk.
    pub fn take_col_offsets(&mut self) -> Vec<(usize, i32)> {
        std::mem::take(&mut self.all_col_offsets)
    }

    /// Iterate over rows.
    pub fn rows<'a>(&'a self, arena: &'a Arena) -> impl Iterator<Item = Row<'a>> {
        let num_cols = self.num_cols;
        let columns = &self.columns;
        self.all_col_offsets
            // .max(1) prevents a panic from chunks(0) when num_cols is 0
            // (e.g., commands with no columns like INSERT without RETURNING).
            .chunks(num_cols.max(1))
            .map(move |chunk| Row {
                arena,
                col_offsets: chunk,
                columns,
            })
    }
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

/// A view into a single result row, borrowing data from the arena.
///
/// Column values are accessed by index. NULL values return `None`.
/// Decode errors (protocol violations from a malicious server) are treated
/// as `None` rather than panicking -- a compliant PostgreSQL server always
/// sends correctly-sized data for the declared type.
pub struct Row<'a> {
    arena: &'a Arena,
    col_offsets: &'a [(usize, i32)],
    columns: &'a [ColumnDesc],
}

impl<'a> Row<'a> {
    /// Get the raw bytes for a column, or `None` if NULL.
    pub fn get_raw(&self, idx: usize) -> Option<&'a [u8]> {
        let (offset, len) = self.col_offsets[idx];
        if len < 0 {
            None
        } else {
            Some(self.arena.get(offset, len as usize))
        }
    }

    /// Whether a column is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        self.col_offsets[idx].1 < 0
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.col_offsets.len()
    }

    /// Get a boolean column value. Returns `None` on NULL or decode error.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_bool(data).ok())
    }

    /// Get an i16 column value. Returns `None` on NULL or decode error.
    pub fn get_i16(&self, idx: usize) -> Option<i16> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i16(data).ok())
    }

    /// Get an i32 column value. Returns `None` on NULL or decode error.
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i32(data).ok())
    }

    /// Get an i64 column value. Returns `None` on NULL or decode error.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i64(data).ok())
    }

    /// Get an f32 column value. Returns `None` on NULL or decode error.
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f32(data).ok())
    }

    /// Get an f64 column value. Returns `None` on NULL or decode error.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f64(data).ok())
    }

    /// Get a string column value. Returns `None` on NULL or decode error.
    pub fn get_str(&self, idx: usize) -> Option<&'a str> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_str(data).ok())
    }

    /// Get a byte slice column value.
    pub fn get_bytes(&self, idx: usize) -> Option<&'a [u8]> {
        self.get_raw(idx)
    }

    /// Get the column name by index.
    pub fn column_name(&self, idx: usize) -> &str {
        &self.columns[idx].name
    }

    /// Get the column type OID by index.
    pub fn column_type_oid(&self, idx: usize) -> u32 {
        self.columns[idx].type_oid
    }
}

// ---------------------------------------------------------------------------
// PgDataRow (zero-copy row view for for_each)
// ---------------------------------------------------------------------------

/// A temporary view of a single PostgreSQL DataRow message.
///
/// Reads columns directly from the wire buffer -- no arena copy.
/// Column offsets are pre-computed on construction using a `SmallVec`
/// that is stack-allocated for up to 16 columns (zero heap allocation
/// for the common case).
///
/// Lifetime `'a` borrows from `Connection::read_buf`.
pub struct PgDataRow<'a> {
    data: &'a [u8],
    /// Pre-scanned `(byte_offset, wire_len)` pairs for each column.
    /// `wire_len = -1` means NULL.
    offsets: smallvec::SmallVec<[(usize, i32); 16]>,
}

impl<'a> PgDataRow<'a> {
    /// Parse column boundaries from a raw DataRow payload.
    ///
    /// `data` is the DataRow message payload (after the 'D' type byte and
    /// 4-byte length prefix have been stripped by the framing layer).
    pub fn new(data: &'a [u8]) -> Result<Self, DriverError> {
        if data.len() < 2 {
            return Err(DriverError::Protocol("DataRow too short".into()));
        }
        let num_cols = i16::from_be_bytes([data[0], data[1]]);
        if num_cols < 0 {
            return Err(DriverError::Protocol(
                "DataRow: negative column count".into(),
            ));
        }
        let num_cols = num_cols as usize;
        let mut offsets = smallvec::SmallVec::<[(usize, i32); 16]>::with_capacity(num_cols);
        let mut pos = 2usize;
        for _ in 0..num_cols {
            if pos + 4 > data.len() {
                return Err(DriverError::Protocol("DataRow truncated".into()));
            }
            let col_len =
                i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            offsets.push((pos, col_len));
            if col_len > 0 {
                pos += col_len as usize;
            }
        }
        Ok(Self { data, offsets })
    }

    /// Get the raw bytes for a column, or `None` if NULL.
    #[inline]
    pub fn get_raw(&self, idx: usize) -> Option<&'a [u8]> {
        let (offset, len) = self.offsets[idx];
        if len < 0 {
            None
        } else {
            Some(&self.data[offset..offset + len as usize])
        }
    }

    /// Whether a column is NULL.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        self.offsets[idx].1 < 0
    }

    /// Number of columns.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.offsets.len()
    }

    /// Get a boolean column value. Returns `None` on NULL or decode error.
    #[inline]
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_bool(data).ok())
    }

    /// Get an i16 column value.
    #[inline]
    pub fn get_i16(&self, idx: usize) -> Option<i16> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i16(data).ok())
    }

    /// Get an i32 column value.
    #[inline]
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i32(data).ok())
    }

    /// Get an i64 column value.
    #[inline]
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i64(data).ok())
    }

    /// Get an f32 column value.
    #[inline]
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f32(data).ok())
    }

    /// Get an f64 column value.
    #[inline]
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f64(data).ok())
    }

    /// Get a string column value (zero-copy borrow from the wire buffer).
    #[inline]
    pub fn get_str(&self, idx: usize) -> Option<&'a str> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_str(data).ok())
    }

    /// Get a byte slice column value (zero-copy borrow from the wire buffer).
    #[inline]
    pub fn get_bytes(&self, idx: usize) -> Option<&'a [u8]> {
        self.get_raw(idx)
    }
}

// ---------------------------------------------------------------------------
// parse_data_row_flat
// ---------------------------------------------------------------------------

/// Parse a DataRow message into the flat column offset storage.
///
/// Appends `(arena_offset, length)` pairs for each column to `out`.
/// `length = -1` indicates NULL.
///
/// DataRow format: `[num_columns: i16] ([col_len: i32] [col_data: col_len bytes])...`
pub(crate) fn parse_data_row_flat(
    data: &[u8],
    arena: &mut Arena,
    out: &mut Vec<(usize, i32)>,
) -> Result<(), DriverError> {
    if data.len() < 2 {
        return Err(DriverError::Protocol("DataRow too short".into()));
    }

    let num_cols_raw = i16::from_be_bytes([data[0], data[1]]);
    if num_cols_raw < 0 {
        return Err(DriverError::Protocol(
            "DataRow: negative column count".into(),
        ));
    }
    let num_cols = num_cols_raw as usize;
    out.reserve(num_cols);
    let mut pos = 2;

    for _ in 0..num_cols {
        if pos + 4 > data.len() {
            return Err(DriverError::Protocol("DataRow truncated".into()));
        }

        let col_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        if col_len < 0 {
            // NULL
            out.push((0, -1));
        } else {
            let len = col_len as usize;
            if pos + len > data.len() {
                return Err(DriverError::Protocol(
                    "DataRow column data truncated".into(),
                ));
            }

            let offset = arena.alloc_copy(&data[pos..pos + len]);
            out.push((offset, col_len));
            pos += len;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// hash_sql
// ---------------------------------------------------------------------------

/// Compute a rapidhash of a SQL string.
///
/// Uses `str::hash()` via the `Hash` trait, matching `bsql_core::rapid_hash_str`.
pub fn hash_sql(sql: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = RapidHasher::default();
    sql.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    // ===================================================================
    // Config tests
    // ===================================================================

    #[test]
    fn config_parse_full_url() {
        let cfg = Config::from_url("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "mydb");
    }

    #[test]
    fn config_parse_default_port() {
        let cfg = Config::from_url("postgres://user:pass@localhost/mydb").unwrap();
        assert_eq!(cfg.port, 5432);
    }

    #[test]
    fn config_parse_no_password() {
        let cfg = Config::from_url("postgres://user@localhost/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "");
    }

    #[test]
    fn config_parse_empty_database() {
        let cfg = Config::from_url("postgres://user:pass@localhost").unwrap();
        // database defaults to user
        assert_eq!(cfg.database, "user");
    }

    #[test]
    fn config_parse_sslmode() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    #[test]
    fn config_parse_percent_encoding() {
        let cfg = Config::from_url("postgres://user%40domain:p%40ss@localhost/db").unwrap();
        assert_eq!(cfg.user, "user@domain");
        assert_eq!(cfg.password, "p@ss");
    }

    #[test]
    fn config_rejects_bad_scheme() {
        let result = Config::from_url("mysql://user:pass@localhost/db");
        assert!(result.is_err());
    }

    /// Unknown sslmode should error, not silently default to Prefer.
    #[test]
    fn config_rejects_unknown_sslmode() {
        let result = Config::from_url("postgres://user:pass@localhost/db?sslmode=requre");
        assert!(result.is_err(), "typo 'requre' should be rejected");
        let result = Config::from_url("postgres://user:pass@localhost/db?sslmode=REQUIRE");
        assert!(result.is_err(), "uppercase should be rejected");
        let result = Config::from_url("postgres://user:pass@localhost/db?sslmode=bogus");
        assert!(result.is_err(), "bogus value should be rejected");
    }

    /// Valid sslmodes should still work.
    #[test]
    fn config_accepts_valid_sslmodes() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=disable").unwrap();
        assert_eq!(cfg.ssl, SslMode::Disable);
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=prefer").unwrap();
        assert_eq!(cfg.ssl, SslMode::Prefer);
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    // #68: Config with postgresql:// scheme
    #[test]
    fn config_parse_postgresql_scheme() {
        let cfg = Config::from_url("postgresql://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "mydb");
    }

    // #69: Config URL without password
    #[test]
    fn config_parse_no_password_standalone() {
        let cfg = Config::from_url("postgres://admin@db.example.com/myapp").unwrap();
        assert_eq!(cfg.user, "admin");
        assert_eq!(cfg.password, "");
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.database, "myapp");
    }

    // #70: Config URL with empty database (falls back to user)
    #[test]
    fn config_empty_database_falls_back_to_user() {
        let cfg = Config::from_url("postgres://testuser:pass@localhost").unwrap();
        assert_eq!(cfg.database, "testuser");
    }

    // #71: Config URL with unknown sslmode error
    #[test]
    fn config_unknown_sslmode_error() {
        let result = Config::from_url("postgres://u:p@h/d?sslmode=verify-full");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown sslmode"),
            "should describe unknown sslmode: {err}"
        );
    }

    // #72: Config URL with multiple query params
    #[test]
    fn config_multiple_query_params() {
        let cfg = Config::from_url(
            "postgres://user:pass@localhost/db?sslmode=disable&statement_timeout=60",
        )
        .unwrap();
        assert_eq!(cfg.ssl, SslMode::Disable);
        assert_eq!(cfg.statement_timeout_secs, 60);
    }

    // Config validation: empty host
    #[test]
    fn config_validate_empty_host() {
        let cfg = Config {
            host: String::new(),
            port: 5432,
            user: "user".into(),
            password: "pass".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.validate().is_err());
    }

    // Config validation: empty user
    #[test]
    fn config_validate_empty_user() {
        let cfg = Config {
            host: "localhost".into(),
            port: 5432,
            user: String::new(),
            password: "pass".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.validate().is_err());
    }

    // Config validation: empty database
    #[test]
    fn config_validate_empty_database() {
        let cfg = Config {
            host: "localhost".into(),
            port: 5432,
            user: "user".into(),
            password: "pass".into(),
            database: String::new(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.validate().is_err());
    }

    // Config missing @ in URL
    #[test]
    fn config_missing_at_sign() {
        let result = Config::from_url("postgres://userpasslocalhost/db");
        assert!(result.is_err());
    }

    // Config with custom port
    #[test]
    fn config_custom_port() {
        let cfg = Config::from_url("postgres://user:pass@localhost:5433/db").unwrap();
        assert_eq!(cfg.port, 5433);
    }

    // Config with invalid port
    #[test]
    fn config_invalid_port() {
        let result = Config::from_url("postgres://user:pass@localhost:notaport/db");
        assert!(result.is_err());
    }

    // #76: Config SslMode::Require without tls feature
    #[cfg(not(feature = "tls"))]
    #[test]
    fn config_sslmode_require_without_tls_feature() {
        // The config parses fine, but validate doesn't check this.
        // The error occurs at connection time. Just verify parsing works.
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    #[test]
    fn config_statement_timeout_default() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db").unwrap();
        assert_eq!(cfg.statement_timeout_secs, 30);
    }

    #[test]
    fn config_statement_timeout_custom() {
        let cfg =
            Config::from_url("postgres://user:pass@localhost/db?statement_timeout=120").unwrap();
        assert_eq!(cfg.statement_timeout_secs, 120);
    }

    #[test]
    fn config_statement_timeout_zero() {
        let cfg =
            Config::from_url("postgres://user:pass@localhost/db?statement_timeout=0").unwrap();
        assert_eq!(cfg.statement_timeout_secs, 0);
    }

    #[test]
    fn config_statement_timeout_invalid_falls_back() {
        let cfg =
            Config::from_url("postgres://user:pass@localhost/db?statement_timeout=notanumber")
                .unwrap();
        assert_eq!(cfg.statement_timeout_secs, 30); // fallback
    }

    #[test]
    fn config_uds_path_format() {
        let cfg = Config::from_url("postgres://user@localhost/db?host=/tmp").unwrap();
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5432");
    }

    #[test]
    fn config_uds_path_custom_port() {
        let cfg = Config::from_url("postgres://user@localhost:5433/db?host=/tmp").unwrap();
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5433");
    }

    // ===================================================================
    // UDS (Unix domain socket) tests
    // ===================================================================

    #[test]
    fn config_host_is_uds_absolute_path() {
        let cfg = Config {
            host: "/tmp".into(),
            port: 5432,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5432");
    }

    #[test]
    fn config_host_is_uds_var_run() {
        let cfg = Config {
            host: "/var/run/postgresql".into(),
            port: 5433,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.uds_path(), "/var/run/postgresql/.s.PGSQL.5433");
    }

    #[test]
    fn config_host_is_not_uds_for_hostname() {
        let cfg = Config {
            host: "localhost".into(),
            port: 5432,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(!cfg.host_is_uds());
    }

    #[test]
    fn config_host_is_not_uds_for_ip() {
        let cfg = Config {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(!cfg.host_is_uds());
    }

    #[test]
    fn config_parse_uds_host_query_param() {
        let cfg = Config::from_url("postgres://user@localhost/mydb?host=/tmp").unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5432");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.user, "user");
    }

    #[test]
    fn config_parse_uds_host_query_param_custom_port() {
        let cfg = Config::from_url("postgres://user@localhost:5433/mydb?host=/var/run/postgresql")
            .unwrap();
        assert_eq!(cfg.host, "/var/run/postgresql");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.uds_path(), "/var/run/postgresql/.s.PGSQL.5433");
    }

    #[test]
    fn config_parse_uds_host_with_other_params() {
        let cfg = Config::from_url(
            "postgres://user@localhost/db?host=/tmp&sslmode=disable&statement_timeout=60",
        )
        .unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.ssl, SslMode::Disable);
        assert_eq!(cfg.statement_timeout_secs, 60);
    }

    #[test]
    fn config_parse_uds_host_percent_encoded() {
        // %2F = '/'
        let cfg = Config::from_url("postgres://user@localhost/db?host=%2Ftmp").unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
    }

    #[test]
    fn config_parse_tcp_host_not_overridden_without_param() {
        // No ?host= param: hostname from URL is used (TCP)
        let cfg = Config::from_url("postgres://user@myserver/db").unwrap();
        assert_eq!(cfg.host, "myserver");
        assert!(!cfg.host_is_uds());
    }

    #[test]
    fn config_parse_uds_host_overrides_url_hostname() {
        // ?host= overrides even an explicit hostname
        let cfg = Config::from_url("postgres://user@db.example.com/mydb?host=/var/run/postgresql")
            .unwrap();
        assert_eq!(cfg.host, "/var/run/postgresql");
        assert!(cfg.host_is_uds());
    }

    #[test]
    fn config_parse_uds_empty_url_host() {
        // postgres:///dbname?host=/tmp -- empty hostname before /, host from param
        let cfg = Config::from_url("postgres://user@/mydb?host=/tmp").unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.database, "mydb");
    }

    // ===================================================================
    // url_decode tests
    // ===================================================================

    #[test]
    fn url_decode_works() {
        assert_eq!(url_decode("hello%20world").unwrap(), "hello world");
        assert_eq!(url_decode("no%20escape").unwrap(), "no escape");
        assert_eq!(url_decode("plain").unwrap(), "plain");
        assert_eq!(url_decode("a%40b").unwrap(), "a@b");
    }

    #[test]
    fn url_decode_malformed_percent_trailing() {
        // Truncated percent sequence at end of string
        let result = url_decode("abc%2");
        assert!(result.is_err(), "truncated %2 should error");
    }

    #[test]
    fn url_decode_malformed_percent_no_digits() {
        // % followed by no digits at all
        let result = url_decode("abc%");
        assert!(result.is_err(), "bare % at end should error");
    }

    #[test]
    fn url_decode_invalid_hex_digit() {
        // %GG -- 'G' is not a valid hex digit
        let result = url_decode("abc%GG");
        assert!(result.is_err(), "%GG should error");
    }

    #[test]
    fn url_decode_invalid_hex_second_digit() {
        // %2Z -- 'Z' is not a valid hex digit
        let result = url_decode("abc%2Z");
        assert!(result.is_err(), "%2Z should error");
    }

    /// url_decode with invalid UTF-8 from percent-decoded bytes
    #[test]
    fn url_decode_invalid_utf8_percent() {
        // %80%81 are not valid UTF-8 start bytes
        let result = url_decode("%80%81");
        assert!(result.is_err(), "invalid UTF-8 bytes should error");
    }

    /// url_decode with percent-encoded chars in all positions
    #[test]
    fn url_decode_percent_everywhere() {
        assert_eq!(url_decode("%41%42%43").unwrap(), "ABC");
        assert_eq!(url_decode("%61").unwrap(), "a");
        assert_eq!(url_decode("x%2Fy%2Fz").unwrap(), "x/y/z");
    }

    /// url_decode with bare percent at various positions
    #[test]
    fn url_decode_bare_percent_middle() {
        assert!(url_decode("a%b").is_err(), "bare % in middle should error");
    }

    /// T-02: url_decode with multi-byte UTF-8 (%C3%A9 -> e with acute)
    #[test]
    fn url_decode_multibyte_utf8() {
        let result = url_decode("caf%C3%A9").unwrap();
        assert_eq!(result, "caf\u{00e9}"); // cafe with accent
    }

    // #73: url_decode with invalid percent (%ZZ)
    #[test]
    fn url_decode_invalid_percent_zz() {
        let result = url_decode("abc%ZZ");
        assert!(result.is_err(), "%ZZ should error");
    }

    // #74: url_decode with truncated percent (trailing %)
    #[test]
    fn url_decode_truncated_percent_trailing() {
        let result = url_decode("abc%");
        assert!(result.is_err(), "trailing % should error");
    }

    // #75: url_decode producing invalid UTF-8
    #[test]
    fn url_decode_invalid_utf8() {
        // 0x80 alone is not valid UTF-8
        let result = url_decode("%80");
        assert!(result.is_err(), "invalid UTF-8 should error");
    }

    #[test]
    fn url_decode_empty_string() {
        assert_eq!(url_decode("").unwrap(), "");
    }

    #[test]
    fn url_decode_no_encoding() {
        assert_eq!(url_decode("hello").unwrap(), "hello");
    }

    #[test]
    fn url_decode_all_ascii_hex() {
        // Uppercase hex
        assert_eq!(url_decode("%2F").unwrap(), "/");
        assert_eq!(url_decode("%2f").unwrap(), "/");
    }

    // ===================================================================
    // hash_sql tests
    // ===================================================================

    #[test]
    fn hash_sql_deterministic() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 1");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_sql_different_queries() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_sql_empty() {
        let _h = hash_sql(""); // should not panic
    }

    #[test]
    fn hash_sql_whitespace_only() {
        let h = hash_sql("   ");
        assert_ne!(h, hash_sql(""));
    }

    #[test]
    fn hash_sql_very_long() {
        let long_sql = "SELECT ".to_string() + &"x".repeat(10_000);
        let h = hash_sql(&long_sql);
        assert_eq!(h, hash_sql(&long_sql));
    }

    #[test]
    fn hash_sql_unicode() {
        let h = hash_sql("SELECT '\u{1F600}'");
        assert_ne!(h, hash_sql("SELECT 'x'"));
    }

    // ===================================================================
    // Notification tests
    // ===================================================================

    #[test]
    fn notification_struct_fields() {
        let n = Notification {
            pid: 42,
            channel: "test_chan".to_owned(),
            payload: "hello".to_owned(),
        };
        assert_eq!(n.pid, 42);
        assert_eq!(n.channel, "test_chan");
        assert_eq!(n.payload, "hello");
    }

    #[test]
    fn notification_clone() {
        let n = Notification {
            pid: 1,
            channel: "c".to_owned(),
            payload: "p".to_owned(),
        };
        let n2 = n.clone();
        assert_eq!(n2.pid, 1);
        assert_eq!(n2.channel, "c");
    }

    #[test]
    fn notification_debug() {
        let n = Notification {
            pid: 1,
            channel: "c".to_owned(),
            payload: "p".to_owned(),
        };
        let dbg = format!("{n:?}");
        assert!(dbg.contains("Notification"));
    }

    // ===================================================================
    // QueryResult tests
    // ===================================================================

    #[test]
    fn query_result_empty() {
        let result = QueryResult {
            all_col_offsets: vec![],
            num_cols: 0,
            columns: Arc::from(Vec::new()),
            affected_rows: 0,
        };
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn query_result_from_parts() {
        let result = QueryResult::from_parts(vec![(0, 4), (0, -1)], 2, Arc::from(Vec::new()), 5);
        assert_eq!(result.len(), 1);
        assert_eq!(result.num_cols, 2);
        assert_eq!(result.affected_rows, 5);
    }

    #[test]
    fn query_result_affected_rows() {
        let result = QueryResult {
            all_col_offsets: vec![],
            num_cols: 0,
            columns: Arc::from(Vec::new()),
            affected_rows: 42,
        };
        assert_eq!(result.affected_rows, 42);
        assert!(result.is_empty());
    }

    // ===================================================================
    // parse_data_row_flat tests
    // ===================================================================

    #[test]
    fn data_row_parsing() {
        let mut arena = Arena::new();
        let mut out = Vec::new();

        // Build a DataRow with 2 columns: i32(42) and NULL
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 columns

        // Column 1: i32 = 42
        data.extend_from_slice(&4i32.to_be_bytes()); // length = 4
        data.extend_from_slice(&42i32.to_be_bytes()); // value

        // Column 2: NULL
        data.extend_from_slice(&(-1i32).to_be_bytes()); // length = -1

        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 2);

        // First column should have length 4
        assert_eq!(out[0].1, 4);

        // Second column should be NULL
        assert_eq!(out[1].1, -1);
    }

    #[test]
    fn data_row_empty() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let data = 0i16.to_be_bytes();
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn data_row_flat_all_null() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&4i16.to_be_bytes());
        for _ in 0..4 {
            data.extend_from_slice(&(-1i32).to_be_bytes());
        }
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 4);
        for (_, len) in &out {
            assert_eq!(*len, -1);
        }
    }

    #[test]
    fn data_row_flat_long_text() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let long = vec![b'A'; 1024];
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&(long.len() as i32).to_be_bytes());
        data.extend_from_slice(&long);
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out[0].1, 1024);
        let stored = arena.get(out[0].0, 1024);
        assert!(stored.iter().all(|&b| b == b'A'));
    }

    #[test]
    fn data_row_flat_empty_text() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes()); // 0-length, not null
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out[0].1, 0);
    }

    // ===================================================================
    // PgDataRow tests
    // ===================================================================

    /// Build a DataRow payload: [i16 num_cols] ([i32 len] [bytes])...
    /// len = -1 for NULL
    fn make_data_row(columns: &[Option<&[u8]>]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for col in columns {
            match col {
                Some(data) => {
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
                None => {
                    buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        buf
    }

    #[test]
    fn pg_data_row_get_i32() {
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), Some(42));
        assert_eq!(row.column_count(), 1);
    }

    #[test]
    fn pg_data_row_get_i64() {
        let data = make_data_row(&[Some(&12345i64.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i64(0), Some(12345));
    }

    #[test]
    fn pg_data_row_get_str() {
        let data = make_data_row(&[Some(b"hello")]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_str(0), Some("hello"));
    }

    #[test]
    fn pg_data_row_get_bytes() {
        let data = make_data_row(&[Some(&[0xDE, 0xAD, 0xBE, 0xEF])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bytes(0), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn pg_data_row_get_bool() {
        let data = make_data_row(&[Some(&[1u8])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bool(0), Some(true));

        let data = make_data_row(&[Some(&[0u8])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bool(0), Some(false));
    }

    #[test]
    fn pg_data_row_get_f64() {
        let data = make_data_row(&[Some(&3.14f64.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!((row.get_f64(0).unwrap() - 3.14).abs() < 1e-10);
    }

    #[test]
    fn pg_data_row_null_column() {
        let data = make_data_row(&[None]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(row.is_null(0));
        assert_eq!(row.get_i32(0), None);
        assert_eq!(row.get_str(0), None);
    }

    #[test]
    fn pg_data_row_multiple_columns() {
        let data = make_data_row(&[
            Some(&42i32.to_be_bytes()),
            Some(b"alice"),
            Some(b"alice@example.com"),
            Some(&[1u8]),
            Some(&3.14f64.to_be_bytes()),
        ]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 5);
        assert_eq!(row.get_i32(0), Some(42));
        assert_eq!(row.get_str(1), Some("alice"));
        assert_eq!(row.get_str(2), Some("alice@example.com"));
        assert_eq!(row.get_bool(3), Some(true));
        assert!((row.get_f64(4).unwrap() - 3.14).abs() < 1e-10);
    }

    #[test]
    fn pg_data_row_mixed_null() {
        let data = make_data_row(&[Some(&42i32.to_be_bytes()), None, Some(b"text")]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), Some(42));
        assert!(row.is_null(1));
        assert_eq!(row.get_str(1), None);
        assert_eq!(row.get_str(2), Some("text"));
    }

    #[test]
    fn pg_data_row_empty() {
        let data = make_data_row(&[]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 0);
    }

    #[test]
    fn pg_data_row_too_short() {
        let data = vec![0u8]; // only 1 byte, need at least 2
        assert!(PgDataRow::new(&data).is_err());
    }

    #[test]
    fn pg_data_row_truncated() {
        // Declare 2 columns but only include 1
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&42i32.to_be_bytes());
        // Missing second column
        assert!(PgDataRow::new(&data).is_err());
    }

    #[test]
    fn pg_data_row_get_i16() {
        let data = make_data_row(&[Some(&7i16.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i16(0), Some(7));
    }

    #[test]
    fn pg_data_row_get_f32() {
        let data = make_data_row(&[Some(&2.5f32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!((row.get_f32(0).unwrap() - 2.5).abs() < 1e-6);
    }

    #[test]
    fn pg_data_row_get_raw_null() {
        let data = make_data_row(&[None]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_raw(0), None);
    }

    #[test]
    fn pg_data_row_get_raw_data() {
        let data = make_data_row(&[Some(&[1, 2, 3])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_raw(0), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn pg_data_row_stack_alloc_16_columns() {
        // SmallVec<16> should not heap-allocate for <= 16 columns
        let cols: Vec<Option<&[u8]>> = (0..16).map(|_| Some(&[0u8][..])).collect();
        let data = make_data_row(&cols);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 16);
        // All columns should be accessible
        for i in 0..16 {
            assert_eq!(row.get_raw(i), Some(&[0u8][..]));
        }
    }

    // --- Inline sequential decode tests (validates the raw-bytes pattern) ---

    /// Validate inline sequential decode of a 5-column DataRow
    /// (i32, str, str, bool, f64) -- the same pattern the generated code uses.
    #[test]
    fn inline_sequential_decode_five_columns() {
        let data = make_data_row(&[
            Some(&42i32.to_be_bytes()),
            Some(b"alice"),
            Some(b"alice@example.com"),
            Some(&[1u8]),
            Some(&3.14f64.to_be_bytes()),
        ]);

        // Simulate generated inline decode
        let mut pos: usize = 2; // skip i16 num_cols

        // Column 0: i32
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 4);
        let id = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert_eq!(id, 42);

        // Column 1: str
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 5);
        let name = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        assert_eq!(name, "alice");

        // Column 2: str
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let email = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        assert_eq!(email, "alice@example.com");

        // Column 3: bool
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 1);
        let active = data[pos] != 0;
        pos += len as usize;
        assert!(active);

        // Column 4: f64
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 8);
        let score = f64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;
        assert!((score - 3.14).abs() < 1e-10);
        assert_eq!(pos, data.len());
    }

    /// Validate inline decode with NULL columns.
    #[test]
    fn inline_sequential_decode_with_nulls() {
        let data = make_data_row(&[
            Some(&42i32.to_be_bytes()),
            None, // NULL name
            Some(b"text"),
        ]);

        let mut pos: usize = 2;

        // Column 0: i32 NOT NULL
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let id = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert_eq!(id, 42);

        // Column 1: str NULLABLE -> None
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let name: Option<&str> = if len < 0 {
            None
        } else {
            let s = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
            pos += len as usize;
            Some(s)
        };
        assert!(name.is_none());

        // Column 2: str NOT NULL
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let txt = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        assert_eq!(txt, "text");
        assert_eq!(pos, data.len());
    }

    /// Validate inline decode with all supported scalar types.
    #[test]
    fn inline_sequential_decode_all_scalar_types() {
        let data = make_data_row(&[
            Some(&[1u8]),                  // bool
            Some(&7i16.to_be_bytes()),     // i16
            Some(&42i32.to_be_bytes()),    // i32
            Some(&12345i64.to_be_bytes()), // i64
            Some(&2.5f32.to_be_bytes()),   // f32
            Some(&3.14f64.to_be_bytes()),  // f64
        ]);

        let mut pos: usize = 2;

        // bool
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_bool = data[pos] != 0;
        pos += len as usize;
        assert!(v_bool);

        // i16
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_i16 = i16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += len as usize;
        assert_eq!(v_i16, 7);

        // i32
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_i32 = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert_eq!(v_i32, 42);

        // i64
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_i64 = i64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;
        assert_eq!(v_i64, 12345);

        // f32
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_f32 = f32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert!((v_f32 - 2.5).abs() < 1e-6);

        // f64
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_f64 = f64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;
        assert!((v_f64 - 3.14).abs() < 1e-10);
        assert_eq!(pos, data.len());
    }

    /// Validate PgDataRow::new is public (callable from external code).
    #[test]
    fn pg_data_row_new_is_public() {
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        // This compiles because PgDataRow::new is pub.
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), Some(42));
    }

    /// Inline decode produces identical results to PgDataRow for mixed data.
    #[test]
    fn inline_decode_matches_pgdatarow() {
        let data = make_data_row(&[
            Some(&99i32.to_be_bytes()),
            Some(b"hello world"),
            None,
            Some(&[0u8]),
            Some(&1.23f64.to_be_bytes()),
        ]);

        // PgDataRow results
        let row = PgDataRow::new(&data).unwrap();
        let dr_i32 = row.get_i32(0);
        let dr_str = row.get_str(1);
        let dr_null = row.get_str(2);
        let dr_bool = row.get_bool(3);
        let dr_f64 = row.get_f64(4);

        // Inline results
        let mut pos: usize = 2;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_i32 = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_str = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_null: Option<&str> = if len < 0 { None } else { unreachable!() };

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_bool = data[pos] != 0;
        pos += len as usize;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_f64 = f64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;

        // Both paths must produce identical results
        assert_eq!(dr_i32, Some(in_i32));
        assert_eq!(dr_str, Some(in_str));
        assert_eq!(dr_null, in_null);
        assert_eq!(dr_bool, Some(in_bool));
        assert!((dr_f64.unwrap() - in_f64).abs() < 1e-15);
        assert_eq!(pos, data.len());
    }

    // ===================================================================
    // PgDataRow -- comprehensive tests
    // ===================================================================

    #[test]
    fn pg_data_row_all_null_columns() {
        let data = make_data_row(&[None, None, None, None, None]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 5);
        for i in 0..5 {
            assert!(row.is_null(i), "column {i} should be null");
            assert_eq!(row.get_raw(i), None);
            assert_eq!(row.get_i32(i), None);
            assert_eq!(row.get_i64(i), None);
            assert_eq!(row.get_str(i), None);
            assert_eq!(row.get_bool(i), None);
            assert_eq!(row.get_f64(i), None);
        }
    }

    #[test]
    fn pg_data_row_very_long_text() {
        let long_text = "x".repeat(2048);
        let data = make_data_row(&[Some(long_text.as_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_str(0), Some(long_text.as_str()));
    }

    #[test]
    fn pg_data_row_empty_text() {
        let data = make_data_row(&[Some(b"")]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(!row.is_null(0));
        assert_eq!(row.get_str(0), Some(""));
        assert_eq!(row.get_bytes(0), Some(&[][..]));
    }

    #[test]
    fn pg_data_row_20_columns_exceeds_inline() {
        let col_data: Vec<[u8; 4]> = (0..20).map(|i: i32| i.to_be_bytes()).collect();
        let cols: Vec<Option<&[u8]>> = col_data.iter().map(|b| Some(b.as_slice())).collect();
        let data = make_data_row(&cols);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 20);
        for i in 0..20 {
            assert_eq!(row.get_i32(i), Some(i as i32));
        }
    }

    #[test]
    fn pg_data_row_is_null_each_position() {
        // 3 columns: data, null, data
        let data = make_data_row(&[Some(&1i32.to_be_bytes()), None, Some(&3i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(!row.is_null(0));
        assert!(row.is_null(1));
        assert!(!row.is_null(2));
    }

    #[test]
    fn pg_data_row_negative_column_count() {
        let data = (-1i16).to_be_bytes();
        assert!(PgDataRow::new(&data).is_err());
    }

    #[test]
    fn pg_data_row_get_str_invalid_utf8() {
        let invalid_utf8 = &[0xFF, 0xFE, 0x80];
        let data = make_data_row(&[Some(invalid_utf8)]);
        let row = PgDataRow::new(&data).unwrap();
        // get_str returns None for invalid UTF-8, but get_bytes returns the raw data
        assert_eq!(row.get_str(0), None);
        assert_eq!(row.get_bytes(0), Some(&[0xFF, 0xFE, 0x80][..]));
    }

    #[test]
    fn pg_data_row_get_i32_wrong_length() {
        // i32 needs exactly 4 bytes; give it 2
        let data = make_data_row(&[Some(&7i16.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), None); // 2 bytes != 4 bytes
        assert_eq!(row.get_i16(0), Some(7)); // but i16 works
    }

    #[test]
    fn pg_data_row_get_i64_wrong_length() {
        // i64 needs 8 bytes; give it 4
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i64(0), None);
    }

    #[test]
    fn pg_data_row_get_f64_wrong_length() {
        let data = make_data_row(&[Some(&2.5f32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f64(0), None); // 4 bytes != 8 bytes
    }

    #[test]
    fn pg_data_row_get_f32_wrong_length() {
        let data = make_data_row(&[Some(&3.14f64.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f32(0), None); // 8 bytes != 4 bytes
    }

    #[test]
    fn pg_data_row_get_bool_wrong_length() {
        // bool needs 1 byte; give it 4
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bool(0), None);
    }

    #[test]
    fn pg_data_row_unicode_text() {
        let texts = [
            "\u{1F600}\u{1F4A9}\u{1F680}", // emoji
            "\u{4e16}\u{754c}",            // CJK
            "\u{0645}\u{0631}\u{062D}",    // Arabic
            "\u{1F468}\u{200D}\u{1F469}",  // ZWJ
        ];
        for text in &texts {
            let data = make_data_row(&[Some(text.as_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_str(0), Some(*text));
        }
    }

    #[test]
    fn pg_data_row_i32_boundary_values() {
        for &val in &[i32::MIN, -1, 0, 1, i32::MAX] {
            let data = make_data_row(&[Some(&val.to_be_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_i32(0), Some(val), "failed for {val}");
        }
    }

    #[test]
    fn pg_data_row_i64_boundary_values() {
        for &val in &[i64::MIN, -1, 0, 1, i64::MAX] {
            let data = make_data_row(&[Some(&val.to_be_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_i64(0), Some(val), "failed for {val}");
        }
    }

    #[test]
    fn pg_data_row_f64_special_values() {
        let data = make_data_row(&[Some(&f64::INFINITY.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f64(0), Some(f64::INFINITY));

        let data = make_data_row(&[Some(&f64::NEG_INFINITY.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f64(0), Some(f64::NEG_INFINITY));

        let data = make_data_row(&[Some(&f64::NAN.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(row.get_f64(0).unwrap().is_nan());
    }

    #[test]
    fn pg_data_row_f32_special_values() {
        let data = make_data_row(&[Some(&f32::INFINITY.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f32(0), Some(f32::INFINITY));

        let data = make_data_row(&[Some(&f32::NAN.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(row.get_f32(0).unwrap().is_nan());
    }

    #[test]
    fn pg_data_row_i16_boundary_values() {
        for &val in &[i16::MIN, -1, 0, 1, i16::MAX] {
            let data = make_data_row(&[Some(&val.to_be_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_i16(0), Some(val));
        }
    }
}
