//! Connection pool — thin wrapper over `bsql_driver_postgres::Pool`.
//!
//! Delegates all connection management, fail-fast semantics, and LIFO ordering
//! to the driver. This layer adds only the bsql error type conversions.

use std::time::Duration;

use bsql_driver_postgres::arena::acquire_arena;
use bsql_driver_postgres::codec::Encode;

use crate::error::{BsqlError, BsqlResult};
use crate::stream::QueryStream;
use crate::transaction::Transaction;

/// A row of text values from a raw (unvalidated) SQL query.
///
/// All values are strings — PostgreSQL's simple query protocol returns
/// everything as text. Use [`get`](RawRow::get) to access columns by index.
#[derive(Debug, Clone)]
pub struct RawRow(Vec<Option<String>>);

impl RawRow {
    /// Get a column value by index. Returns `None` for SQL NULL.
    pub fn get(&self, idx: usize) -> Option<&str> {
        self.0.get(idx)?.as_deref()
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate over column values.
    pub fn iter(&self) -> impl Iterator<Item = Option<&str>> {
        self.0.iter().map(|v| v.as_deref())
    }
}

/// Convert a binary-encoded column value to its text representation.
///
/// Uses the column type OID to dispatch to the correct decoder. For types
/// not explicitly handled, falls back to hex-encoded raw bytes.
fn binary_col_to_text(row: &bsql_driver_postgres::Row<'_>, idx: usize, type_oid: u32) -> String {
    match type_oid {
        16 => row // bool
            .get_bool(idx)
            .map(|v| if v { "t" } else { "f" })
            .unwrap_or("")
            .to_owned(),
        21 => row.get_i16(idx).map(|v| v.to_string()).unwrap_or_default(), // int2
        23 => row.get_i32(idx).map(|v| v.to_string()).unwrap_or_default(), // int4
        20 => row.get_i64(idx).map(|v| v.to_string()).unwrap_or_default(), // int8
        26 => row
            .get_i32(idx)
            .map(|v| (v as u32).to_string())
            .unwrap_or_default(), // oid
        700 => row.get_f32(idx).map(|v| v.to_string()).unwrap_or_default(), // float4
        701 => row.get_f64(idx).map(|v| v.to_string()).unwrap_or_default(), // float8
        25 | 1042 | 1043 | 114 | 142 | 3802 => {
            // text, char, varchar, json, xml, jsonb — all UTF-8 text
            row.get_str(idx).unwrap_or("").to_owned()
        }
        17 => {
            // bytea — hex-encode with \x prefix (PG default hex output)
            match row.get_bytes(idx) {
                Some(bytes) => {
                    let mut s = String::with_capacity(2 + bytes.len() * 2);
                    s.push_str("\\x");
                    for b in bytes {
                        use std::fmt::Write;
                        let _ = write!(s, "{b:02x}");
                    }
                    s
                }
                None => String::new(),
            }
        }
        _ => {
            // Fallback: try UTF-8 text decode first (handles many PG types
            // like inet, uuid, timestamps that are text-representable),
            // then fall back to hex-encoded raw bytes.
            if let Some(s) = row.get_str(idx) {
                s.to_owned()
            } else if let Some(bytes) = row.get_bytes(idx) {
                let mut s = String::with_capacity(2 + bytes.len() * 2);
                s.push_str("\\x");
                for b in bytes {
                    use std::fmt::Write;
                    let _ = write!(s, "{b:02x}");
                }
                s
            } else {
                String::new()
            }
        }
    }
}

/// A PostgreSQL connection pool.
///
/// Created via [`Pool::connect`] or [`Pool::builder`]. The pool manages a set
/// of connections, automatically acquires/releases them for each query, and
/// supports optional read/write splitting with a replica.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::Pool;
///
/// let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;
///
/// // Or configure via builder:
/// let pool = Pool::builder()
///     .url("postgres://user:pass@localhost/mydb")
///     .lifetime_secs(900)
///     .timeout_secs(5)
///     .build()?;
/// ```
pub struct Pool {
    pub(crate) inner: bsql_driver_postgres::Pool,
    /// Optional read replica pool. When present, `query_raw_readonly` routes here.
    pub(crate) read_pool: Option<bsql_driver_postgres::Pool>,
}

/// Builder for configuring a connection pool.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::Pool;
///
/// let pool = Pool::builder()
///     .url("postgres://user:pass@localhost/mydb")
///     .max_size(20)
///     .lifetime_secs(900)
///     .timeout_secs(5)
///     .min_idle(2)
///     .build()?;
/// ```
pub struct PoolBuilder {
    url: Option<String>,
    max_size: usize,
    max_lifetime: Option<Option<Duration>>,
    acquire_timeout: Option<Option<Duration>>,
    min_idle: Option<usize>,
    /// Optional URL for a read replica. When set, `query_raw_readonly`
    /// routes to this pool instead of the primary.
    replica_url: Option<String>,
    /// Max pool size for the replica pool. Defaults to same as `max_size`.
    replica_max_size: Option<usize>,
    /// Maximum idle duration before a connection is considered stale.
    stale_timeout: Option<Duration>,
    /// Maximum number of cached prepared statements per connection.
    max_stmt_cache_size: Option<usize>,
}

impl PoolBuilder {
    /// Configure the pool from a PostgreSQL connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub fn url(mut self, url: &str) -> Self {
        self.url = Some(url.into());
        self
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set the maximum lifetime of a connection. Connections older than this
    /// are discarded when returned to the pool. Default: 30 minutes.
    ///
    /// Pass `None` for unlimited lifetime.
    pub fn max_lifetime(mut self, d: Option<Duration>) -> Self {
        self.max_lifetime = Some(d);
        self
    }

    /// Set the maximum lifetime in seconds. Convenience for
    /// `max_lifetime(Some(Duration::from_secs(secs)))`.
    pub fn max_lifetime_secs(self, secs: u64) -> Self {
        self.max_lifetime(Some(Duration::from_secs(secs)))
    }

    /// Shorthand for [`max_lifetime_secs`](Self::max_lifetime_secs).
    pub fn lifetime_secs(self, secs: u64) -> Self {
        self.max_lifetime_secs(secs)
    }

    /// Set the maximum time to wait for a connection when the pool is
    /// exhausted. Default: 5 seconds.
    ///
    /// Pass `None` for fail-fast behavior (no waiting, immediate error).
    pub fn acquire_timeout(mut self, d: Option<Duration>) -> Self {
        self.acquire_timeout = Some(d);
        self
    }

    /// Set the acquire timeout in seconds. Convenience for
    /// `acquire_timeout(Some(Duration::from_secs(secs)))`.
    pub fn acquire_timeout_secs(self, secs: u64) -> Self {
        self.acquire_timeout(Some(Duration::from_secs(secs)))
    }

    /// Shorthand for [`acquire_timeout_secs`](Self::acquire_timeout_secs).
    pub fn timeout_secs(self, secs: u64) -> Self {
        self.acquire_timeout_secs(secs)
    }

    /// Set the minimum number of idle connections to maintain. Default: 0.
    ///
    /// When greater than 0, a background task creates connections as needed
    /// to maintain this idle floor.
    pub fn min_idle(mut self, n: usize) -> Self {
        self.min_idle = Some(n);
        self
    }

    /// Set a read replica URL for read/write splitting.
    ///
    /// When configured, `query_raw_readonly` (used by SELECT queries)
    /// routes to the replica pool. All writes go to the primary.
    /// When no replica is configured, all queries use the primary.
    pub fn replica_url(mut self, url: &str) -> Self {
        self.replica_url = Some(url.into());
        self
    }

    /// Set the max pool size for the replica pool.
    /// Defaults to the same value as `max_size`.
    pub fn replica_max_size(mut self, size: usize) -> Self {
        self.replica_max_size = Some(size);
        self
    }

    /// Set the maximum idle duration before a connection is considered stale.
    /// Default: 30 seconds. Connections idle longer than this are dropped on
    /// acquire instead of being reused.
    pub fn stale_timeout(mut self, timeout: Duration) -> Self {
        self.stale_timeout = Some(timeout);
        self
    }

    /// Set the maximum number of cached prepared statements per connection.
    /// Default: 256. When the cache exceeds this size, the least recently
    /// used statement is evicted.
    pub fn max_stmt_cache_size(mut self, size: usize) -> Self {
        self.max_stmt_cache_size = Some(size);
        self
    }

    pub async fn build(self) -> BsqlResult<Pool> {
        let url = self.url.ok_or_else(|| {
            BsqlError::from(bsql_driver_postgres::DriverError::Pool(
                "pool builder requires a URL".into(),
            ))
        })?;
        let mut builder = bsql_driver_postgres::Pool::builder()
            .url(&url)
            .max_size(self.max_size);

        if let Some(lt) = self.max_lifetime {
            builder = builder.max_lifetime(lt);
        }
        if let Some(at) = self.acquire_timeout {
            builder = builder.acquire_timeout(at);
        }
        if let Some(mi) = self.min_idle {
            builder = builder.min_idle(mi);
        }
        if let Some(st) = self.stale_timeout {
            builder = builder.stale_timeout(st);
        }
        if let Some(msc) = self.max_stmt_cache_size {
            builder = builder.max_stmt_cache_size(msc);
        }

        let inner = builder.build().map_err(BsqlError::from)?;

        // Build replica pool if configured
        let read_pool = if let Some(replica_url) = &self.replica_url {
            let replica_size = self.replica_max_size.unwrap_or(self.max_size);
            let mut rbuilder = bsql_driver_postgres::Pool::builder()
                .url(replica_url)
                .max_size(replica_size);
            if let Some(lt) = self.max_lifetime {
                rbuilder = rbuilder.max_lifetime(lt);
            }
            if let Some(at) = self.acquire_timeout {
                rbuilder = rbuilder.acquire_timeout(at);
            }
            Some(rbuilder.build().map_err(BsqlError::from)?)
        } else {
            None
        };

        Ok(Pool { inner, read_pool })
    }
}

impl Pool {
    /// Connect to PostgreSQL using a connection URL.
    ///
    /// Creates the pool (parses URL, allocates pool structures). Actual TCP/UDS
    /// connections are established lazily on first `acquire()`.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let inner = bsql_driver_postgres::Pool::connect(url).map_err(BsqlError::from)?;
        Ok(Pool {
            inner,
            read_pool: None,
        })
    }

    /// Create a pool builder for fine-grained configuration.
    pub fn builder() -> PoolBuilder {
        PoolBuilder {
            url: None,
            max_size: 10,
            max_lifetime: None,
            acquire_timeout: None,
            min_idle: None,
            replica_url: None,
            replica_max_size: None,
            stale_timeout: None,
            max_stmt_cache_size: None,
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available (unless `acquire_timeout` is configured).
    pub async fn acquire(&self) -> BsqlResult<PoolConnection> {
        let guard = self.inner.acquire().map_err(BsqlError::from)?;
        Ok(PoolConnection { inner: guard })
    }

    /// Begin a new transaction.
    ///
    /// Acquires a connection and sends BEGIN immediately.
    pub async fn begin(&self) -> BsqlResult<Transaction> {
        let tx = self.inner.begin().map_err(BsqlError::from)?;
        Ok(Transaction::from_driver(tx))
    }

    /// Execute a query and return a stream of rows.
    ///
    /// Acquires a connection from the pool and returns a [`QueryStream`]
    /// that holds the connection alive until the stream is consumed or dropped.
    ///
    /// Uses true PG-level streaming via `Execute(max_rows=64)`. Only 64 rows
    /// are in memory at a time. The stream fetches additional chunks on demand
    /// via the `PortalSuspended` / re-`Execute` protocol.
    pub async fn query_stream(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<QueryStream> {
        let mut guard = self.inner.acquire().map_err(BsqlError::from)?;
        let mut arena = acquire_arena();

        // chunk_size=64 rows per Execute call
        const CHUNK_SIZE: i32 = 64;

        let (columns, _) = guard
            .query_streaming_start(sql, sql_hash, params, CHUNK_SIZE)
            .map_err(BsqlError::from)?;

        let num_cols = columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> =
            Vec::with_capacity(num_cols * CHUNK_SIZE as usize);

        let more = guard
            .streaming_next_chunk(&mut arena, &mut all_col_offsets)
            .map_err(BsqlError::from)?;

        let first_result = bsql_driver_postgres::QueryResult::from_parts(
            all_col_offsets,
            num_cols,
            columns.clone(),
            0,
        );

        Ok(QueryStream::new(guard, arena, first_result, columns, !more))
    }

    /// Set the SQL statements to pre-PREPARE on new connections.
    ///
    /// Each SQL string is PREPAREd on new connections before they are returned
    /// from `acquire()`. This eliminates first-use Parse overhead for hot queries.
    ///
    /// Warmup errors are silently ignored — a bad warmup SQL does not prevent
    /// the connection from being usable.
    pub fn set_warmup_sqls<S: Into<Box<str>>>(&self, sqls: impl IntoIterator<Item = S>) {
        self.inner.set_warmup_sqls(sqls);
    }

    /// Execute arbitrary SQL and return text rows.
    ///
    /// Uses PostgreSQL's simple query protocol — all values returned as strings.
    /// This bypasses bsql's compile-time SQL validation entirely.
    ///
    /// Use for DDL, ad-hoc queries, migrations, or the rare dynamic SQL that
    /// cannot be expressed via `query!`. For type-safe queries, use `query!`.
    pub async fn raw_query(&self, sql: &str) -> BsqlResult<Vec<RawRow>> {
        let mut guard = self.inner.acquire().map_err(BsqlError::from)?;
        let rows = guard
            .simple_query_rows(sql)
            .map_err(BsqlError::from_driver_query)?;
        Ok(rows.into_iter().map(RawRow).collect())
    }

    /// Execute arbitrary SQL without returning rows.
    ///
    /// Uses PostgreSQL's simple query protocol. Useful for DDL (CREATE TABLE,
    /// ALTER, DROP), SET commands, or any statement where you don't need results.
    pub async fn raw_execute(&self, sql: &str) -> BsqlResult<()> {
        let mut guard = self.inner.acquire().map_err(BsqlError::from)?;
        guard
            .simple_query(sql)
            .map_err(BsqlError::from_driver_query)?;
        Ok(())
    }

    /// Execute parameterized SQL and return text rows.
    ///
    /// Uses PostgreSQL's extended query protocol (Parse+Bind+Execute) with
    /// positional `$1, $2, ...` parameter placeholders. Results are converted
    /// from binary format to text strings.
    ///
    /// This is the parameterized equivalent of [`raw_query`](Self::raw_query).
    /// Use when you need parameter binding but don't want compile-time
    /// validation via `query!`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let rows = pool.raw_query_params(
    ///     "SELECT $1::int4 + $2::int4 AS sum",
    ///     &[&1i32, &2i32],
    /// ).await?;
    /// assert_eq!(rows[0].get(0), Some("3"));
    /// ```
    pub async fn raw_query_params(
        &self,
        sql: &str,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<Vec<RawRow>> {
        let sql_hash = crate::rapid_hash_str(sql);
        let mut guard = self.inner.acquire().map_err(BsqlError::from)?;
        let result = guard
            .query(sql, sql_hash, params)
            .map_err(BsqlError::from_driver_query)?;
        let arena = bsql_driver_postgres::Arena::empty();
        let columns = result.columns();
        let num_rows = result.len();
        let mut rows = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let row = result.row(i, &arena);
            let num_cols = row.column_count();
            let mut values = Vec::with_capacity(num_cols);
            for (c, col) in columns.iter().enumerate().take(num_cols) {
                if row.is_null(c) {
                    values.push(None);
                } else {
                    let text = binary_col_to_text(&row, c, col.type_oid);
                    values.push(Some(text));
                }
            }
            rows.push(RawRow(values));
        }
        Ok(rows)
    }

    /// Bulk copy data INTO a table from an iterator of text rows.
    ///
    /// Each row is a tab-separated string (TSV format, matching PostgreSQL's
    /// default COPY text format). Returns the number of rows copied.
    ///
    /// This is 10-100x faster than individual INSERTs for bulk data loading.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let rows = vec!["alice\talice@example.com", "bob\tbob@example.com"];
    /// let count = pool.copy_in("users", &["name", "email"], rows.iter().map(|s| s.as_str())).await?;
    /// ```
    pub async fn copy_in<'a, I>(&self, table: &str, columns: &[&str], rows: I) -> BsqlResult<u64>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut guard = self.inner.acquire().map_err(BsqlError::from)?;
        guard
            .copy_in(table, columns, rows)
            .map_err(BsqlError::from_driver_query)
    }

    /// Bulk copy data OUT of a table or query result to a writer.
    ///
    /// Data is written in PostgreSQL's text format (tab-separated columns,
    /// newline-terminated rows). Returns the number of rows copied.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut buf = Vec::new();
    /// let count = pool.copy_out("SELECT name, email FROM users", &mut buf).await?;
    /// ```
    pub async fn copy_out<W: std::io::Write>(
        &self,
        query: &str,
        writer: &mut W,
    ) -> BsqlResult<u64> {
        let mut guard = self.inner.acquire().map_err(BsqlError::from)?;
        guard
            .copy_out(query, writer)
            .map_err(BsqlError::from_driver_query)
    }

    /// Pool status metrics: idle, active, open, and max_size.
    ///
    /// Returns detailed pool utilization metrics from the driver.
    pub fn status(&self) -> PoolStatus {
        let driver_status = self.inner.status();
        PoolStatus {
            idle: driver_status.idle,
            active: driver_status.active,
            open: driver_status.open,
            max_size: driver_status.max_size,
        }
    }

    /// Gracefully close the pool (and replica pool if configured).
    ///
    /// No new connections can be acquired after this call. All idle connections
    /// are closed immediately. Active connections are closed when returned to
    /// the pool.
    pub fn close(&self) {
        self.inner.close();
        if let Some(ref rp) = self.read_pool {
            rp.close();
        }
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Whether a read replica pool is configured.
    pub fn has_replica(&self) -> bool {
        self.read_pool.is_some()
    }

    /// Whether this pool uses sync connections via Unix domain sockets.
    ///
    /// When `true`, the pool automatically uses `SyncConnection` (blocking I/O)
    /// internally, eliminating async overhead for sub-microsecond UDS I/O.
    /// The user API is identical — this is purely a performance optimization.
    pub fn is_uds(&self) -> bool {
        self.inner.is_uds()
    }

    /// Process each row directly from the wire buffer via a closure.
    ///
    /// Acquires a connection, calls `Connection::for_each`, and releases.
    /// Zero arena allocation — the closure reads columns directly from
    /// the DataRow message bytes.
    ///
    /// When `readonly` is true and a replica pool is configured, routes
    /// to the replica pool; otherwise uses the primary.
    pub async fn for_each_raw<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        readonly: bool,
        mut f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(bsql_driver_postgres::PgDataRow<'_>) -> BsqlResult<()>,
    {
        let pool = if readonly {
            self.read_pool.as_ref().unwrap_or(&self.inner)
        } else {
            &self.inner
        };
        let mut guard = pool.acquire().map_err(BsqlError::from)?;
        // Bridge BsqlError from the user closure into DriverError for the
        // driver-level for_each. Any closure error is stashed in `user_err`
        // and re-surfaced after the driver returns.
        let mut user_err: Option<BsqlError> = None;
        let driver_result = guard.for_each(sql, sql_hash, params, |row| match f(row) {
            Ok(()) => Ok(()),
            Err(e) => {
                user_err = Some(e);
                Err(bsql_driver_postgres::DriverError::Protocol(
                    "for_each closure error".into(),
                ))
            }
        });
        // If the user closure produced an error, return it directly.
        if let Some(e) = user_err {
            return Err(e);
        }
        driver_result.map_err(BsqlError::from_driver_query)
    }

    /// Process each DataRow as raw bytes via inline sequential decode.
    ///
    /// Like `for_each_raw` but passes the raw `&[u8]` DataRow payload directly
    /// to the closure — no `PgDataRow` construction, no SmallVec pre-scan.
    /// The generated macro code decodes columns inline by advancing a position
    /// cursor through the bytes.
    #[doc(hidden)]
    pub async fn __for_each_raw_bytes<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        readonly: bool,
        mut f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(&[u8]) -> BsqlResult<()>,
    {
        let pool = if readonly {
            self.read_pool.as_ref().unwrap_or(&self.inner)
        } else {
            &self.inner
        };
        let mut guard = pool.acquire().map_err(BsqlError::from)?;
        let mut user_err: Option<BsqlError> = None;
        let driver_result = guard.for_each_raw(sql, sql_hash, params, |data| match f(data) {
            Ok(()) => Ok(()),
            Err(e) => {
                user_err = Some(e);
                Err(bsql_driver_postgres::DriverError::Protocol(
                    "for_each closure error".into(),
                ))
            }
        });
        if let Some(e) = user_err {
            return Err(e);
        }
        driver_result.map_err(BsqlError::from_driver_query)
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Pool {
            inner: self.inner.clone(),
            read_pool: self.read_pool.clone(),
        }
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("status", &self.status())
            .finish()
    }
}

/// A connection borrowed from the pool.
///
/// Provides exclusive (`&mut`) access to the underlying `PoolGuard` — no
/// `Mutex` needed. Generated code converts `&mut PoolConnection` into a
/// [`QueryTarget`](crate::executor::QueryTarget) for dispatch.
///
/// Returned to the pool when dropped.
pub struct PoolConnection {
    pub(crate) inner: bsql_driver_postgres::PoolGuard,
}

impl std::fmt::Debug for PoolConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConnection").finish()
    }
}

/// Snapshot of pool utilization.
#[derive(Debug, Clone, Copy)]
pub struct PoolStatus {
    /// Number of idle connections in the pool.
    pub idle: usize,
    /// Number of connections currently in use.
    pub active: usize,
    /// Total open connections (idle + active).
    pub open: usize,
    /// Maximum pool size.
    pub max_size: usize,
}

impl std::fmt::Display for PoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "idle={}, active={}, open={}, max={}",
            self.idle, self.active, self.open, self.max_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = Pool::builder();
        assert_eq!(b.max_size, 10);
        assert!(b.max_lifetime.is_none());
        assert!(b.acquire_timeout.is_none());
        assert!(b.min_idle.is_none());
    }

    #[test]
    fn builder_max_lifetime() {
        let b = Pool::builder().max_lifetime(Some(Duration::from_secs(60)));
        assert_eq!(b.max_lifetime, Some(Some(Duration::from_secs(60))));
    }

    #[test]
    fn builder_max_lifetime_none_disables() {
        let b = Pool::builder().max_lifetime(None);
        assert_eq!(b.max_lifetime, Some(None));
    }

    #[test]
    fn builder_acquire_timeout() {
        let b = Pool::builder().acquire_timeout(Some(Duration::from_secs(3)));
        assert_eq!(b.acquire_timeout, Some(Some(Duration::from_secs(3))));
    }

    #[test]
    fn builder_acquire_timeout_none_disables() {
        let b = Pool::builder().acquire_timeout(None);
        assert_eq!(b.acquire_timeout, Some(None));
    }

    #[test]
    fn builder_min_idle() {
        let b = Pool::builder().min_idle(5);
        assert_eq!(b.min_idle, Some(5));
    }

    // --- Convenience methods ---

    #[test]
    fn builder_max_lifetime_secs() {
        let b = Pool::builder().max_lifetime_secs(1800);
        assert_eq!(b.max_lifetime, Some(Some(Duration::from_secs(1800))));
    }

    #[test]
    fn builder_acquire_timeout_secs() {
        let b = Pool::builder().acquire_timeout_secs(5);
        assert_eq!(b.acquire_timeout, Some(Some(Duration::from_secs(5))));
    }

    // --- Shorthand aliases ---

    #[test]
    fn builder_lifetime_secs_shorthand() {
        let b = Pool::builder().lifetime_secs(900);
        assert_eq!(b.max_lifetime, Some(Some(Duration::from_secs(900))));
    }

    #[test]
    fn builder_timeout_secs_shorthand() {
        let b = Pool::builder().timeout_secs(3);
        assert_eq!(b.acquire_timeout, Some(Some(Duration::from_secs(3))));
    }

    // --- Task 2: Read/write splitting ---

    #[test]
    fn builder_defaults_no_replica() {
        let b = Pool::builder();
        assert!(b.replica_url.is_none());
        assert!(b.replica_max_size.is_none());
    }

    #[test]
    fn builder_replica_url() {
        let b = Pool::builder().replica_url("postgres://replica:5432/db");
        assert_eq!(b.replica_url.as_deref(), Some("postgres://replica:5432/db"));
    }

    #[test]
    fn builder_replica_max_size() {
        let b = Pool::builder().replica_max_size(20);
        assert_eq!(b.replica_max_size, Some(20));
    }

    #[tokio::test]
    async fn pool_connect_has_no_replica() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert!(!pool.has_replica());
    }

    // --- Auto-UDS sync connection tests ---

    #[tokio::test]
    async fn pool_is_uds_false_for_tcp() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_is_uds_true_for_unix_socket() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/tmp")
            .await
            .unwrap();
        assert!(pool.is_uds());
    }

    #[tokio::test]
    async fn pool_is_uds_false_for_ip() {
        let pool = Pool::connect("postgres://user:pass@127.0.0.1/db")
            .await
            .unwrap();
        assert!(!pool.is_uds());
    }

    // --- PoolStatus Display ---

    #[test]
    fn pool_status_display() {
        let status = PoolStatus {
            idle: 3,
            active: 2,
            open: 5,
            max_size: 10,
        };
        assert_eq!(status.to_string(), "idle=3, active=2, open=5, max=10");
    }

    #[test]
    fn pool_status_display_zeros() {
        let status = PoolStatus {
            idle: 0,
            active: 0,
            open: 0,
            max_size: 0,
        };
        assert_eq!(status.to_string(), "idle=0, active=0, open=0, max=0");
    }

    // --- PoolConnection Debug ---

    #[test]
    fn pool_connection_debug() {
        // PoolConnection wraps a PoolGuard, Debug should not panic
        let dbg_str = "PoolConnection";
        assert!(!dbg_str.is_empty());
        // We can't construct a PoolConnection without a real pool guard,
        // but we verify the impl exists at compile time through the trait bound.
        fn _assert_debug<T: std::fmt::Debug>() {}
        _assert_debug::<PoolConnection>();
    }

    // --- Pool Debug ---

    #[tokio::test]
    async fn pool_debug() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        let dbg = format!("{pool:?}");
        assert!(dbg.contains("Pool"), "Debug should show Pool: {dbg}");
    }

    // --- Pool Clone ---

    #[tokio::test]
    async fn pool_clone_is_cheap() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        let pool2 = pool.clone();
        assert_eq!(pool.status().max_size, pool2.status().max_size);
        assert!(!pool.has_replica());
        assert!(!pool2.has_replica());
    }

    // --- Send + Sync assertions ---

    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    #[test]
    fn pool_is_send_and_sync() {
        _assert_send::<Pool>();
        _assert_sync::<Pool>();
    }

    #[test]
    fn pool_connection_is_send() {
        _assert_send::<PoolConnection>();
    }

    #[test]
    fn pool_status_is_send_and_sync() {
        _assert_send::<PoolStatus>();
        _assert_sync::<PoolStatus>();
    }

    // --- Builder without URL ---

    #[tokio::test]
    async fn builder_build_without_url_errors() {
        let result = Pool::builder().build().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("URL"), "error should mention URL: {err}");
    }

    // --- PoolBuilder chaining ---

    #[test]
    fn builder_chaining() {
        let b = Pool::builder()
            .url("postgres://u@localhost/db")
            .max_size(20)
            .lifetime_secs(600)
            .timeout_secs(3)
            .min_idle(2)
            .replica_url("postgres://u@replica/db")
            .replica_max_size(10);
        assert_eq!(b.max_size, 20);
        assert_eq!(b.min_idle, Some(2));
        assert_eq!(b.replica_max_size, Some(10));
    }

    // --- RawRow ---

    #[test]
    fn raw_row_get() {
        let row = RawRow(vec![Some("hello".into()), None, Some("42".into())]);
        assert_eq!(row.get(0), Some("hello"));
        assert_eq!(row.get(1), None);
        assert_eq!(row.get(2), Some("42"));
        assert_eq!(row.get(99), None);
        assert_eq!(row.len(), 3);
    }

    #[test]
    fn raw_row_is_empty() {
        let empty = RawRow(vec![]);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);

        let non_empty = RawRow(vec![Some("x".into())]);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn raw_row_iter() {
        let row = RawRow(vec![Some("a".into()), None, Some("b".into())]);
        let vals: Vec<_> = row.iter().collect();
        assert_eq!(vals, vec![Some("a"), None, Some("b")]);
    }

    #[test]
    fn raw_row_clone() {
        let row = RawRow(vec![Some("hello".into()), None]);
        let cloned = row.clone();
        assert_eq!(cloned.get(0), Some("hello"));
        assert_eq!(cloned.get(1), None);
        assert_eq!(cloned.len(), 2);
    }

    #[test]
    fn raw_row_debug() {
        let row = RawRow(vec![Some("x".into())]);
        let dbg = format!("{row:?}");
        assert!(dbg.contains("RawRow"), "Debug should show RawRow: {dbg}");
    }

    // --- RawRow additional edge cases ---

    #[test]
    fn raw_row_all_null_values() {
        let row = RawRow(vec![None, None, None]);
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        assert_eq!(row.get(0), None);
        assert_eq!(row.get(1), None);
        assert_eq!(row.get(2), None);
        // iter should produce all None
        let vals: Vec<_> = row.iter().collect();
        assert_eq!(vals, vec![None, None, None]);
    }

    #[test]
    fn raw_row_empty_string_values() {
        let row = RawRow(vec![Some(String::new()), Some("".into())]);
        assert_eq!(row.len(), 2);
        // Empty string is Some(""), not None
        assert_eq!(row.get(0), Some(""));
        assert_eq!(row.get(1), Some(""));
    }

    #[test]
    fn raw_row_get_out_of_bounds() {
        let row = RawRow(vec![Some("only".into())]);
        assert_eq!(row.get(0), Some("only"));
        assert_eq!(row.get(1), None);
        assert_eq!(row.get(100), None);
        assert_eq!(row.get(usize::MAX), None);
    }

    #[test]
    fn raw_row_iter_empty() {
        let row = RawRow(vec![]);
        let vals: Vec<_> = row.iter().collect();
        assert!(vals.is_empty());
    }

    #[test]
    fn raw_row_iter_mixed() {
        let row = RawRow(vec![
            Some("hello".into()),
            None,
            Some("world".into()),
            None,
            Some("".into()),
        ]);
        let vals: Vec<_> = row.iter().collect();
        assert_eq!(
            vals,
            vec![Some("hello"), None, Some("world"), None, Some("")]
        );
    }

    #[test]
    fn raw_row_single_null() {
        let row = RawRow(vec![None]);
        assert_eq!(row.len(), 1);
        assert!(!row.is_empty());
        assert_eq!(row.get(0), None);
    }

    // --- PoolBuilder stale_timeout ---

    #[test]
    fn builder_stale_timeout() {
        let b = Pool::builder().stale_timeout(Duration::from_secs(15));
        assert_eq!(b.stale_timeout, Some(Duration::from_secs(15)));
    }

    #[test]
    fn builder_stale_timeout_default_is_none() {
        let b = Pool::builder();
        assert!(b.stale_timeout.is_none());
    }

    // --- PoolBuilder max_stmt_cache_size ---

    #[test]
    fn builder_max_stmt_cache_size() {
        let b = Pool::builder().max_stmt_cache_size(512);
        assert_eq!(b.max_stmt_cache_size, Some(512));
    }

    #[test]
    fn builder_max_stmt_cache_size_default_is_none() {
        let b = Pool::builder();
        assert!(b.max_stmt_cache_size.is_none());
    }

    // --- Pool close / is_closed ---

    #[tokio::test]
    async fn pool_close_and_is_closed() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert!(!pool.is_closed());
        pool.close();
        assert!(pool.is_closed());
    }

    // --- Pool status on fresh pool ---

    #[tokio::test]
    async fn pool_status_on_fresh_pool() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        let status = pool.status();
        assert_eq!(status.idle, 0, "fresh pool should have 0 idle");
        assert_eq!(status.active, 0, "fresh pool should have 0 active");
        assert_eq!(status.open, 0, "fresh pool should have 0 open");
        assert_eq!(status.max_size, 10, "default max_size should be 10");
    }

    // --- PoolStatus Clone and Copy ---

    #[test]
    fn pool_status_clone_and_copy() {
        let status = PoolStatus {
            idle: 1,
            active: 2,
            open: 3,
            max_size: 10,
        };
        let cloned = status;
        assert_eq!(cloned.idle, 1);
        assert_eq!(cloned.active, 2);
        assert_eq!(cloned.open, 3);
        assert_eq!(cloned.max_size, 10);
    }

    // --- PoolStatus Debug ---

    #[test]
    fn pool_status_debug() {
        let status = PoolStatus {
            idle: 1,
            active: 2,
            open: 3,
            max_size: 10,
        };
        let dbg = format!("{status:?}");
        assert!(
            dbg.contains("PoolStatus"),
            "Debug should show PoolStatus: {dbg}"
        );
    }

    // --- Builder max_size ---

    #[test]
    fn builder_max_size() {
        let b = Pool::builder().max_size(50);
        assert_eq!(b.max_size, 50);
    }

    // --- Builder url ---

    #[test]
    fn builder_url_stored() {
        let b = Pool::builder().url("postgres://localhost/test");
        assert_eq!(b.url.as_deref(), Some("postgres://localhost/test"));
    }

    // --- RawRow unicode content ---

    #[test]
    fn raw_row_unicode_content() {
        let row = RawRow(vec![
            Some("\u{1F600}".into()),                                // emoji
            Some("\u{0645}\u{0631}\u{062D}\u{0628}\u{0627}".into()), // Arabic
            Some("\u{00E9}\u{00E8}\u{00EA}".into()),                 // French accents
        ]);
        assert_eq!(row.get(0), Some("\u{1F600}"));
        assert_eq!(row.len(), 3);
    }

    // --- RawRow large column count ---

    #[test]
    fn raw_row_many_columns() {
        let cols: Vec<Option<String>> = (0..100).map(|i| Some(format!("val_{i}"))).collect();
        let row = RawRow(cols);
        assert_eq!(row.len(), 100);
        assert_eq!(row.get(0), Some("val_0"));
        assert_eq!(row.get(99), Some("val_99"));
        assert_eq!(row.get(100), None);
    }

    // --- Pool has_replica false by default ---

    #[tokio::test]
    async fn pool_has_replica_false_default() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert!(!pool.has_replica());
    }

    // --- Builder complete chaining returns correct state ---

    #[test]
    fn builder_full_chain() {
        let b = Pool::builder()
            .url("postgres://u@localhost/db")
            .max_size(32)
            .lifetime_secs(600)
            .timeout_secs(3)
            .min_idle(4)
            .stale_timeout(Duration::from_secs(30))
            .max_stmt_cache_size(128)
            .replica_url("postgres://u@replica/db")
            .replica_max_size(16);
        assert_eq!(b.max_size, 32);
        assert_eq!(b.min_idle, Some(4));
        assert_eq!(b.stale_timeout, Some(Duration::from_secs(30)));
        assert_eq!(b.max_stmt_cache_size, Some(128));
        assert_eq!(b.replica_max_size, Some(16));
    }

    // --- RawRow construction ---

    #[test]
    fn raw_row_with_nulls() {
        let row = RawRow(vec![Some("hello".into()), None, Some("world".into())]);
        assert_eq!(row.len(), 3);
        assert_eq!(row.get(0), Some("hello"));
        assert_eq!(row.get(1), None);
        assert_eq!(row.get(2), Some("world"));
    }

    #[test]
    fn raw_row_all_nulls() {
        let row = RawRow(vec![None, None]);
        assert_eq!(row.len(), 2);
        assert_eq!(row.get(0), None);
        assert_eq!(row.get(1), None);
    }
}
