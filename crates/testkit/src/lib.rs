#![forbid(unsafe_code)]

//! Deterministic in-memory fake PostgreSQL for testing bsql driver code.
//!
//! A [`FakePostgres`] scripts query replies; [`FakePostgres::connect`] hands
//! back a REAL [`bsql_postgres_async::Connection`] — the same concrete type a
//! socket `connect` returns — backed by an in-memory fake. Driver code under
//! test then runs unchanged (`query_sql`, transactions, the whole decode path)
//! against the fake, with **no network, no socket, no PostgreSQL** — fully
//! deterministic.
//!
//! This is possible because the driver's engine is sans-IO: it drives a
//! `Transport` seam, and the fake implements that seam over in-memory buffers.
//! The bytes the fake serves are real PostgreSQL wire frames the real engine
//! parses, so a passing test proves genuine end-to-end behaviour, not a mock.
//!
//! ```no_run
//! use bsql_testkit::{rows, FakePostgres};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let mut fake = FakePostgres::new();
//! fake.on("SELECT id FROM users").returns(rows![[1_i64], [2_i64]]);
//!
//! let mut conn = fake.connect().await?;
//! let result = conn.query_sql("SELECT id FROM users").await?;
//!
//! assert_eq!(result.rows.len(), 2);
//! # Ok(())
//! # }
//! ```
//!
//! # Scope (MVP)
//!
//! Handles the trust-auth handshake, scripted **simple** queries
//! ([`query_sql`](bsql_postgres_async::Connection::query_sql)), and scripted
//! errors. An unscripted query or an extended-protocol message (the
//! compile-checked `query!` path) is answered with a loud, classified
//! `ErrorResponse` — never a silent empty result or a hang. Multi-query
//! scripting, expectations (`assert_all_queried`), COPY/LISTEN, and the sync
//! driver are not yet supported.

use bsql_postgres_async::Connection;
use bsql_postgres_core::testkit::wire::{
    self, FakeEncodeError, OID_BOOL, OID_INT4, OID_INT8, OID_TEXT, TX_IDLE,
};
use bsql_postgres_core::testkit::{FakeScript, FakeTransport};
use bsql_postgres_core::DriverError;

/// A single scripted column value, rendered to PostgreSQL text wire format.
///
/// Construct through the [`From`] impls (or the [`rows!`] macro), e.g.
/// `FakeValue::from(1_i64)`. `Option<T>` maps `None` to a SQL `NULL`.
#[derive(Debug, Clone)]
pub enum FakeValue {
    /// A `bigint` (`int8`) value.
    Int8(i64),
    /// An `integer` (`int4`) value.
    Int4(i32),
    /// A `text` value.
    Text(String),
    /// A `boolean` value.
    Bool(bool),
    /// A SQL `NULL`.
    Null,
}

impl From<i64> for FakeValue {
    fn from(v: i64) -> Self {
        Self::Int8(v)
    }
}
impl From<i32> for FakeValue {
    fn from(v: i32) -> Self {
        Self::Int4(v)
    }
}
impl From<&str> for FakeValue {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}
impl From<String> for FakeValue {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<bool> for FakeValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl<T: Into<FakeValue>> From<Option<T>> for FakeValue {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

impl FakeValue {
    /// The PostgreSQL type OID this value advertises in `RowDescription`.
    fn oid(&self) -> i32 {
        match self {
            Self::Int8(_) => OID_INT8,
            Self::Int4(_) => OID_INT4,
            Self::Text(_) => OID_TEXT,
            Self::Bool(_) => OID_BOOL,
            Self::Null => OID_TEXT,
        }
    }

    /// The value in PostgreSQL text wire format, or `None` for a SQL `NULL`.
    fn render(&self) -> Option<Vec<u8>> {
        match self {
            Self::Int8(v) => Some(v.to_string().into_bytes()),
            Self::Int4(v) => Some(v.to_string().into_bytes()),
            Self::Text(s) => Some(s.clone().into_bytes()),
            Self::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
            Self::Null => None,
        }
    }
}

/// A scripted result set — a grid of [`FakeValue`] rows. Build it with the
/// [`rows!`] macro.
#[derive(Debug, Clone)]
pub struct ScriptedRows {
    rows: Vec<Vec<FakeValue>>,
}

impl ScriptedRows {
    /// Build from a grid of rows (each an equal-length list of cells). Prefer
    /// the [`rows!`] macro.
    #[must_use]
    pub fn from_rows(rows: Vec<Vec<FakeValue>>) -> Self {
        Self { rows }
    }
}

/// Build a [`ScriptedRows`] from row literals: `rows![[1_i64], [2_i64]]`.
///
/// Each inner `[...]` is one row; each element is any value with a
/// [`FakeValue`] `From` impl (`i64`, `i32`, `&str`, `String`, `bool`,
/// `Option<T>` for a `NULL`).
#[macro_export]
macro_rules! rows {
    ( $( [ $( $cell:expr ),* $(,)? ] ),* $(,)? ) => {
        $crate::ScriptedRows::from_rows(::std::vec![
            $( ::std::vec![ $( $crate::FakeValue::from($cell) ),* ] ),*
        ])
    };
}

/// A scripted reply to one query: either a result set or a server error.
#[derive(Debug, Clone)]
enum ScriptedReply {
    Rows(ScriptedRows),
    Error { sqlstate: String, message: String },
}

/// Why building or connecting a [`FakePostgres`] failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum TestkitError {
    /// A scripted reply could not be encoded to wire bytes (an oversized value).
    Encode(FakeEncodeError),
    /// A scripted result set had rows of differing column counts, which the
    /// wire's single-width `RowDescription` cannot represent.
    RaggedRows {
        /// The column count established by the first row.
        expected: usize,
        /// The differing column count of a later row.
        found: usize,
    },
    /// The driver failed to connect over the fake (a malformed scripted
    /// handshake, surfaced by the real engine).
    Driver(DriverError),
}

impl core::fmt::Display for TestkitError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "fake reply encode error: {e}"),
            Self::RaggedRows { expected, found } => write!(
                f,
                "scripted rows have differing column counts: expected {expected}, found {found}"
            ),
            Self::Driver(e) => write!(f, "fake connect failed: {e}"),
        }
    }
}

impl std::error::Error for TestkitError {}

impl From<FakeEncodeError> for TestkitError {
    fn from(e: FakeEncodeError) -> Self {
        Self::Encode(e)
    }
}
impl From<DriverError> for TestkitError {
    fn from(e: DriverError) -> Self {
        Self::Driver(e)
    }
}

/// A deterministic in-memory fake PostgreSQL server.
///
/// Script replies with [`on`](Self::on), then obtain a real connection with
/// [`connect`](Self::connect). One fake can back several connections (each call
/// replays the same script).
#[derive(Debug, Clone)]
pub struct FakePostgres {
    responses: Vec<(String, ScriptedReply)>,
    server_version: String,
    backend_pid: i32,
}

impl FakePostgres {
    /// A fresh fake with no scripted queries.
    #[must_use]
    pub fn new() -> Self {
        Self {
            responses: Vec::new(),
            server_version: "17.0 (bsql-testkit)".to_owned(),
            backend_pid: 1,
        }
    }

    /// Set the `server_version` the fake reports at connect time.
    #[must_use]
    pub fn with_server_version(mut self, version: impl Into<String>) -> Self {
        self.server_version = version.into();
        self
    }

    /// Begin scripting a reply for a simple query. The SQL is matched by exact
    /// text after trimming surrounding whitespace. Finish with
    /// [`Responder::returns`] or [`Responder::returns_error`].
    pub fn on(&mut self, sql: impl Into<String>) -> Responder<'_> {
        Responder {
            fake: self,
            sql: sql.into(),
        }
    }

    /// Open a real [`Connection`] backed by this fake — no socket, no network.
    ///
    /// # Errors
    ///
    /// [`TestkitError`] if a scripted reply cannot be encoded (an oversized
    /// value or ragged rows) or the driver rejects the fake handshake.
    pub async fn connect(&self) -> Result<Connection, TestkitError> {
        let script = self.build_script()?;
        let conn = Connection::connect_fake(FakeTransport::new(script)).await?;
        Ok(conn)
    }

    /// Encode the whole script to the pre-built reply bytes the fake serves.
    fn build_script(&self) -> Result<FakeScript, TestkitError> {
        let handshake = encode_handshake(&self.server_version, self.backend_pid)?;
        let mut queries = Vec::with_capacity(self.responses.len());
        for (sql, reply) in &self.responses {
            let bytes = match reply {
                ScriptedReply::Rows(rows) => encode_rows(rows)?,
                ScriptedReply::Error { sqlstate, message } => encode_error(sqlstate, message)?,
            };
            queries.push((sql.trim().to_owned(), bytes));
        }
        let scripted = self
            .responses
            .iter()
            .map(|(sql, _)| format!("{sql:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let unmatched = encode_error(
            "XX000",
            &format!(
                "bsql-testkit: no scripted reply for the received query. \
                 Scripted queries: [{scripted}]. \
                 Add fake.on(<sql>).returns(...) to script it."
            ),
        )?;
        // The extended-protocol error is served WITHOUT a trailing
        // ReadyForQuery: the fake emits it once for the batch's first message,
        // then supplies the single `ready_for_query` when the batch's Sync
        // arrives (PostgreSQL's error-then-skip-to-Sync recovery), so a failed
        // extended op leaves the connection clean.
        let unsupported_error = wire::error_response(
            "ERROR",
            "0A000",
            "bsql-testkit: this in-memory fake supports the simple-query protocol \
             (query_sql) only; the compile-checked query! (extended query) protocol \
             is not supported yet.",
        )?;
        let ready_for_query = wire::ready_for_query(TX_IDLE)?;
        Ok(FakeScript {
            handshake,
            queries,
            unmatched_reply: unmatched,
            unsupported_error,
            ready_for_query,
        })
    }
}

impl Default for FakePostgres {
    fn default() -> Self {
        Self::new()
    }
}

/// A pending scripted reply for one query. Finish it with
/// [`returns`](Self::returns) or [`returns_error`](Self::returns_error).
#[derive(Debug)]
#[must_use = "call .returns(...) or .returns_error(...) to record the scripted reply"]
pub struct Responder<'a> {
    fake: &'a mut FakePostgres,
    sql: String,
}

impl Responder<'_> {
    /// Script this query to return the given rows.
    pub fn returns(self, rows: ScriptedRows) {
        self.fake.responses.push((self.sql, ScriptedReply::Rows(rows)));
    }

    /// Script this query to fail with a PostgreSQL `ErrorResponse` — the driver
    /// surfaces it as `DriverError::Db`.
    pub fn returns_error(self, sqlstate: impl Into<String>, message: impl Into<String>) {
        self.fake.responses.push((
            self.sql,
            ScriptedReply::Error {
                sqlstate: sqlstate.into(),
                message: message.into(),
            },
        ));
    }
}

/// Derive the `(name, oid)` columns for a result set from its rows: the width
/// is the first row's; each column's OID is the first non-NULL cell's type
/// (defaulting to `text` when a column is entirely NULL).
fn columns(rows: &[Vec<FakeValue>]) -> Vec<(String, i32)> {
    let width = match rows.first() {
        Some(first) => first.len(),
        None => 0,
    };
    (0..width)
        .map(|col| {
            let oid = match rows.iter().find_map(|row| match row.get(col) {
                Some(cell) if !matches!(cell, FakeValue::Null) => Some(cell.oid()),
                _ => None,
            }) {
                Some(found) => found,
                None => OID_TEXT,
            };
            (format!("col{col}"), oid)
        })
        .collect()
}

/// Encode a scripted result set: `RowDescription` + `DataRow`s +
/// `CommandComplete` + `ReadyForQuery`.
fn encode_rows(rows: &ScriptedRows) -> Result<Vec<u8>, TestkitError> {
    let cols = columns(&rows.rows);
    for row in &rows.rows {
        if row.len() != cols.len() {
            return Err(TestkitError::RaggedRows {
                expected: cols.len(),
                found: row.len(),
            });
        }
    }
    let mut frames = Vec::with_capacity(rows.rows.len().saturating_add(3));
    frames.push(wire::row_description(&cols)?);
    for row in &rows.rows {
        let cells: Vec<Option<Vec<u8>>> = row.iter().map(FakeValue::render).collect();
        frames.push(wire::data_row(&cells)?);
    }
    frames.push(wire::command_complete(&format!("SELECT {}", rows.rows.len()))?);
    frames.push(wire::ready_for_query(TX_IDLE)?);
    Ok(wire::concat(&frames))
}

/// Encode a scripted `ErrorResponse` + `ReadyForQuery`.
fn encode_error(sqlstate: &str, message: &str) -> Result<Vec<u8>, TestkitError> {
    let frames = [
        wire::error_response("ERROR", sqlstate, message)?,
        wire::ready_for_query(TX_IDLE)?,
    ];
    Ok(wire::concat(&frames))
}

/// Encode the trust-auth handshake chain the fake serves for the startup packet.
fn encode_handshake(server_version: &str, backend_pid: i32) -> Result<Vec<u8>, TestkitError> {
    let frames = [
        wire::auth_ok()?,
        wire::parameter_status("server_version", server_version)?,
        wire::backend_key_data(backend_pid, 0)?,
        wire::ready_for_query(TX_IDLE)?,
    ];
    Ok(wire::concat(&frames))
}
