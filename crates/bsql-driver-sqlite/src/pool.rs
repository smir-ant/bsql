//! Synchronous connection pool — Mutex-guarded connections, WAL reader/writer split.
//!
//! SQLite connections are opened with `SQLITE_OPEN_NOMUTEX` (multi-thread mode),
//! disabling SQLite's internal serialization for maximum performance. The pool
//! wraps each connection in a `std::sync::Mutex` to prevent interleaved `step()`
//! calls — external serialization replaces SQLite's internal locking.
//!
//! # Architecture
//!
//! - **Writer**: one `Mutex<SqliteConnection>` with a read-write connection.
//!   All INSERT/UPDATE/DELETE/DDL goes here.
//! - **Readers**: N `Mutex<SqliteConnection>` with read-only connections.
//!   SELECT queries are round-robin distributed across readers.
//! - **No threads**: connections are accessed directly via mutex lock.
//!   No crossbeam channels, no dedicated threads, no async runtime.
//!
//! # Fail-fast
//!
//! Per CREDO #17, `busy_timeout = 0` on all connections. If the writer is busy,
//! SQLite returns SQLITE_BUSY immediately. The pool does not queue or retry.
//!
//! # Performance
//!
//! For single-row queries (`fetch_one_direct`), the entire path is:
//! ```text
//! lock mutex → cache lookup → bind → step → read columns directly → reset → unlock
//! ```
//! Zero arena allocation, zero channel overhead, zero thread switching.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bsql_arena::{acquire_arena, Arena};
use smallvec::SmallVec;

use crate::codec::SqliteEncode;
use crate::conn::{hash_sql, QueryResult, SqliteConnection, StreamingQuery};
use crate::ffi::StmtHandle;
use crate::SqliteError;

/// Validate a savepoint name to prevent SQL injection.
///
/// Must be 1-63 chars, start with letter or underscore, contain only
/// alphanumeric + underscore. This matches the bsql-core validation.
fn validate_savepoint_name(name: &str) -> Result<(), SqliteError> {
    if name.is_empty() || name.len() > 63 {
        return Err(SqliteError::Internal(
            "savepoint name must be 1-63 characters".into(),
        ));
    }
    let first = name.as_bytes()[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return Err(SqliteError::Internal(
            "savepoint name must start with letter or underscore".into(),
        ));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(SqliteError::Internal(
            "savepoint name must contain only alphanumeric characters and underscores".into(),
        ));
    }
    Ok(())
}

// --- ParamValue ---

/// Pre-serialized parameter for pool API compatibility.
///
/// Typical queries (<=8 params) fit in `SmallVec<[ParamValue; 8]>` with
/// zero heap allocation for the array.
#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Bool(bool),
}

impl SqliteEncode for ParamValue {
    fn bind(&self, stmt: &StmtHandle, idx: i32) -> Result<(), SqliteError> {
        match self {
            ParamValue::Null => stmt.bind_null(idx),
            ParamValue::Int(v) => stmt.bind_int64(idx, *v),
            ParamValue::Real(v) => stmt.bind_double(idx, *v),
            ParamValue::Text(v) => stmt.bind_text(idx, v.as_str()),
            ParamValue::Blob(v) => stmt.bind_blob(idx, v.as_slice()),
            ParamValue::Bool(v) => stmt.bind_int64(idx, if *v { 1 } else { 0 }),
        }
    }
}

// --- StreamingState ---

/// Streaming query state passed between pool and caller.
///
/// Contains the `StreamingQuery` metadata needed to step the next chunk,
/// plus the reader index so subsequent chunks go to the same connection.
pub struct StreamingState {
    /// The streaming query metadata.
    pub inner: StreamingQuery,
}

// SAFETY: StreamingState is Send because StreamingQuery contains only scalar
// values (u64, usize, bool) — no raw pointers, references, or Rc.
unsafe impl Send for StreamingState {}

// --- SqlitePool ---

/// Synchronous connection pool — one writer + N readers, mutex-guarded.
///
/// Read queries are round-robin distributed across reader connections.
/// Write queries (INSERT/UPDATE/DELETE/DDL) go to the single writer.
///
/// # Thread safety
///
/// `SqlitePool` is `Send + Sync`. Each connection is wrapped in a
/// `Mutex<SqliteConnection>` and opened with `SQLITE_OPEN_FULLMUTEX`.
///
/// # Example
///
/// ```no_run
/// use bsql_driver_sqlite::pool::SqlitePool;
///
/// let pool = SqlitePool::connect("/tmp/test.db").unwrap();
/// // Read queries go to reader connections
/// // Write queries go to the writer connection
/// pool.close();
/// ```
pub struct SqlitePool {
    writer: Mutex<SqliteConnection>,
    readers: Vec<Mutex<SqliteConnection>>,
    closed: Arc<AtomicBool>,
    /// Round-robin counter for distributing read queries across readers.
    reader_idx: AtomicUsize,
}

impl SqlitePool {
    /// Open a pool with default settings (4 readers).
    pub fn connect(path: &str) -> Result<Self, SqliteError> {
        SqlitePoolBuilder::new().path(path).build()
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> SqlitePoolBuilder {
        SqlitePoolBuilder::new()
    }

    /// Acquire the next reader connection (round-robin).
    fn acquire_reader(&self) -> Result<MutexGuard<'_, SqliteConnection>, SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers[idx]
            .lock()
            .map_err(|_| SqliteError::Pool("reader mutex poisoned".into()))
    }

    /// Acquire the writer connection.
    fn acquire_writer(&self) -> Result<MutexGuard<'_, SqliteConnection>, SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        self.writer
            .lock()
            .map_err(|_| SqliteError::Pool("writer mutex poisoned".into()))
    }

    /// Acquire a reader connection. Returns a MutexGuard.
    ///
    /// # Doc-hidden
    ///
    /// Used by generated code from `bsql::query!`. Not part of the public API.
    #[doc(hidden)]
    #[inline]
    pub fn __acquire_reader(&self) -> Result<MutexGuard<'_, SqliteConnection>, SqliteError> {
        self.acquire_reader()
    }

    /// Acquire the writer connection. Returns a MutexGuard.
    ///
    /// # Doc-hidden
    ///
    /// Used by generated code from `bsql::query!`. Not part of the public API.
    #[doc(hidden)]
    #[inline]
    pub fn __acquire_writer(&self) -> Result<MutexGuard<'_, SqliteConnection>, SqliteError> {
        self.acquire_writer()
    }

    /// Route a read query to a reader (round-robin), returning results in an arena.
    pub fn query_readonly(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<(QueryResult, Arena), SqliteError> {
        let mut conn = self.acquire_reader()?;
        let mut arena = acquire_arena();
        let param_refs: SmallVec<[&dyn SqliteEncode; 8]> =
            params.iter().map(|p| p as &dyn SqliteEncode).collect();
        let result = conn.query(sql, sql_hash, &param_refs, &mut arena)?;
        Ok((result, arena))
    }

    /// Route a write query to the writer, returning results in an arena.
    pub fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<(QueryResult, Arena), SqliteError> {
        let mut conn = self.acquire_writer()?;
        let mut arena = acquire_arena();
        let param_refs: SmallVec<[&dyn SqliteEncode; 8]> =
            params.iter().map(|p| p as &dyn SqliteEncode).collect();
        let result = conn.query(sql, sql_hash, &param_refs, &mut arena)?;
        Ok((result, arena))
    }

    /// Execute a write statement (INSERT/UPDATE/DELETE), return affected row count.
    pub fn execute(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
    ) -> Result<u64, SqliteError> {
        let mut conn = self.acquire_writer()?;
        let param_refs: SmallVec<[&dyn SqliteEncode; 8]> =
            params.iter().map(|p| p as &dyn SqliteEncode).collect();
        conn.execute(sql, sql_hash, &param_refs)
    }

    /// Execute a simple SQL statement on the writer (PRAGMA, DDL).
    pub fn simple_exec(&self, sql: &str) -> Result<(), SqliteError> {
        let conn = self.acquire_writer()?;
        conn.exec(sql)
    }

    /// Fetch exactly one row via direct decode — zero arena overhead.
    ///
    /// The `decode` closure reads columns directly from the stepped statement.
    /// This is the fastest path for single-row queries.
    #[inline]
    pub fn fetch_one_direct<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> Result<T, SqliteError>
    where
        F: FnOnce(&StmtHandle) -> Result<T, SqliteError>,
    {
        if is_write {
            let mut conn = self.acquire_writer()?;
            conn.fetch_one_direct(sql, sql_hash, params, decode)
        } else {
            let mut conn = self.acquire_reader()?;
            conn.fetch_one_direct(sql, sql_hash, params, decode)
        }
    }

    /// Fetch zero or one row via direct decode — zero arena overhead.
    #[inline]
    pub fn fetch_optional_direct<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> Result<Option<T>, SqliteError>
    where
        F: FnOnce(&StmtHandle) -> Result<T, SqliteError>,
    {
        if is_write {
            let mut conn = self.acquire_writer()?;
            conn.fetch_optional_direct(sql, sql_hash, params, decode)
        } else {
            let mut conn = self.acquire_reader()?;
            conn.fetch_optional_direct(sql, sql_hash, params, decode)
        }
    }

    /// Fetch all rows via direct decode — zero arena overhead.
    ///
    /// The `decode` closure reads columns directly from the stepped statement
    /// for each row. This is the fastest path for multi-row queries.
    #[inline]
    pub fn fetch_all_direct<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> Result<Vec<T>, SqliteError>
    where
        F: Fn(&StmtHandle) -> Result<T, SqliteError>,
    {
        if is_write {
            let mut conn = self.acquire_writer()?;
            conn.fetch_all_direct(sql, sql_hash, params, decode)
        } else {
            let mut conn = self.acquire_reader()?;
            conn.fetch_all_direct(sql, sql_hash, params, decode)
        }
    }

    /// Fetch all rows into an arena-backed result — zero per-row heap allocation
    /// for text and blob columns. See [`SqliteConnection::fetch_all_arena`] for details.
    #[inline]
    pub fn fetch_all_arena<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> Result<bsql_arena::ArenaRows<T>, SqliteError>
    where
        F: Fn(&StmtHandle, &mut bsql_arena::Arena) -> Result<T, SqliteError>,
    {
        if is_write {
            let mut conn = self.acquire_writer()?;
            conn.fetch_all_arena(sql, sql_hash, params, decode)
        } else {
            let mut conn = self.acquire_reader()?;
            conn.fetch_all_arena(sql, sql_hash, params, decode)
        }
    }

    /// Process each row in-place via a closure. Zero-copy -- text columns
    /// borrow directly from SQLite's internal buffer.
    ///
    /// Routes to writer or reader based on `is_write`.
    #[inline]
    pub fn for_each<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        is_write: bool,
        f: F,
    ) -> Result<(), SqliteError>
    where
        F: FnMut(&StmtHandle) -> Result<(), SqliteError>,
    {
        if is_write {
            let mut conn = self.acquire_writer()?;
            conn.for_each(sql, sql_hash, params, f)
        } else {
            let mut conn = self.acquire_reader()?;
            conn.for_each(sql, sql_hash, params, f)
        }
    }

    /// Process each row in-place, collecting results into a `Vec`.
    ///
    /// Routes to writer or reader based on `is_write`.
    #[inline]
    pub fn for_each_collect<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        is_write: bool,
        f: F,
    ) -> Result<Vec<T>, SqliteError>
    where
        F: FnMut(&StmtHandle) -> Result<T, SqliteError>,
    {
        if is_write {
            let mut conn = self.acquire_writer()?;
            conn.for_each_collect(sql, sql_hash, params, f)
        } else {
            let mut conn = self.acquire_reader()?;
            conn.for_each_collect(sql, sql_hash, params, f)
        }
    }

    /// Execute a statement via direct param binding — zero arena/ParamValue overhead.
    ///
    /// Takes `&[&dyn SqliteEncode]` directly instead of `SmallVec<ParamValue>`.
    #[inline]
    pub fn execute_direct(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
    ) -> Result<u64, SqliteError> {
        let mut conn = self.acquire_writer()?;
        conn.execute_direct(sql, sql_hash, params)
    }

    /// Execute the same statement N times with different parameter sets.
    ///
    /// Acquires the writer once for the entire batch. Prepares the statement
    /// once, then binds+steps+resets for each parameter set. Returns the total
    /// number of affected rows across all executions.
    pub fn execute_batch(
        &self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&dyn SqliteEncode]],
    ) -> Result<u64, SqliteError> {
        let mut conn = self.acquire_writer()?;
        conn.execute_batch(sql, sql_hash, param_sets)
    }

    /// Start a streaming query on a reader connection.
    ///
    /// Returns `(result, arena, state, reader_idx)`. The `reader_idx` must be
    /// passed to `streaming_next` / `streaming_drop` so subsequent chunks are
    /// routed to the same reader that owns the open statement.
    pub fn query_streaming(
        &self,
        sql: &str,
        sql_hash: u64,
        params: SmallVec<[ParamValue; 8]>,
        chunk_size: usize,
    ) -> Result<(QueryResult, Arena, StreamingState, usize), SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let mut conn = self.readers[idx]
            .lock()
            .map_err(|_| SqliteError::Pool("reader mutex poisoned".into()))?;

        let param_refs: SmallVec<[&dyn SqliteEncode; 8]> =
            params.iter().map(|p| p as &dyn SqliteEncode).collect();
        let streaming = conn.query_streaming(sql, sql_hash, &param_refs, chunk_size)?;

        let mut arena = acquire_arena();
        let mut sq = StreamingQuery {
            sql_hash: streaming.sql_hash,
            col_count: streaming.col_count,
            chunk_size: streaming.chunk_size,
            finished: streaming.finished,
        };
        let qr = conn.streaming_next_chunk(&mut sq, &mut arena)?;
        let state = StreamingState {
            inner: StreamingQuery {
                sql_hash: sq.sql_hash,
                col_count: sq.col_count,
                chunk_size: sq.chunk_size,
                finished: sq.finished || (qr.row_count < sq.chunk_size),
            },
        };
        Ok((qr, arena, state, idx))
    }

    /// Fetch the next chunk from a streaming query.
    pub fn streaming_next(
        &self,
        mut state: StreamingState,
        reader_idx: usize,
    ) -> Result<(QueryResult, Arena, StreamingState), SqliteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SqliteError::Pool("pool is closed".into()));
        }
        let idx = reader_idx % self.readers.len();
        let mut conn = self.readers[idx]
            .lock()
            .map_err(|_| SqliteError::Pool("reader mutex poisoned".into()))?;
        let mut arena = acquire_arena();
        let result = conn.streaming_next_chunk(&mut state.inner, &mut arena)?;
        Ok((result, arena, state))
    }

    /// Drop a streaming query, resetting the statement for reuse.
    pub fn streaming_drop(&self, state: StreamingState, reader_idx: usize) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let idx = reader_idx % self.readers.len();
        if let Ok(mut conn) = self.readers[idx].lock() {
            if !state.inner.finished {
                conn.reset_streaming(&state.inner);
            }
        }
    }

    /// Begin a transaction on the writer connection.
    pub fn begin_transaction(&self) -> Result<(), SqliteError> {
        let conn = self.acquire_writer()?;
        conn.exec("BEGIN")
    }

    /// Commit the current transaction on the writer connection.
    pub fn commit_transaction(&self) -> Result<(), SqliteError> {
        let conn = self.acquire_writer()?;
        conn.exec("COMMIT")
    }

    /// Rollback the current transaction on the writer connection.
    pub fn rollback_transaction(&self) -> Result<(), SqliteError> {
        let conn = self.acquire_writer()?;
        conn.exec("ROLLBACK")
    }

    /// Create a savepoint within the current transaction.
    ///
    /// Name must be a valid SQL identifier (alphanumeric + underscore, starts
    /// with letter or underscore, max 63 chars). This is validated to prevent
    /// SQL injection — the name is interpolated into `SAVEPOINT {name}`.
    pub fn savepoint(&self, name: &str) -> Result<(), SqliteError> {
        validate_savepoint_name(name)?;
        let conn = self.acquire_writer()?;
        conn.exec(&format!("SAVEPOINT {name}"))
    }

    /// Release a savepoint.
    pub fn release_savepoint(&self, name: &str) -> Result<(), SqliteError> {
        validate_savepoint_name(name)?;
        let conn = self.acquire_writer()?;
        conn.exec(&format!("RELEASE SAVEPOINT {name}"))
    }

    /// Rollback to a savepoint.
    pub fn rollback_to(&self, name: &str) -> Result<(), SqliteError> {
        validate_savepoint_name(name)?;
        let conn = self.acquire_writer()?;
        conn.exec(&format!("ROLLBACK TO SAVEPOINT {name}"))
    }

    /// Pre-prepare statements on all connections (warmup).
    pub fn warmup(&self, sqls: &[&str]) {
        for sql in sqls {
            let sql_hash = hash_sql(sql);
            // Warmup on writer
            if let Ok(mut conn) = self.writer.lock() {
                let _ = conn.prepare_only(sql, sql_hash);
            }
            // Warmup on all readers
            for reader in &self.readers {
                if let Ok(mut conn) = reader.lock() {
                    let _ = conn.prepare_only(sql, sql_hash);
                }
            }
        }
    }

    /// Number of reader connections.
    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Close the pool.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

// --- SqlitePoolBuilder ---

/// Builder for configuring a SQLite connection pool.
pub struct SqlitePoolBuilder {
    path: Option<String>,
    reader_count: usize,
    busy_timeout_ms: u32,
}

impl SqlitePoolBuilder {
    fn new() -> Self {
        Self {
            path: None,
            reader_count: 4,
            busy_timeout_ms: 0,
        }
    }

    /// Set the database file path.
    pub fn path(mut self, path: &str) -> Self {
        self.path = Some(path.to_owned());
        self
    }

    /// Set the number of reader connections. Default: 4.
    pub fn reader_count(mut self, count: usize) -> Self {
        self.reader_count = count;
        self
    }

    /// Set the SQLite `busy_timeout` in milliseconds. Default: 0 (fail-fast).
    ///
    /// When set to 0, SQLite returns `SQLITE_BUSY` immediately if the database
    /// is locked. With a positive value, SQLite will wait up to that many
    /// milliseconds before returning `SQLITE_BUSY`.
    pub fn busy_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.busy_timeout_ms = timeout.as_millis() as u32;
        self
    }

    /// Build and open the pool. Opens connections directly (no thread spawning).
    pub fn build(self) -> Result<SqlitePool, SqliteError> {
        let path = self
            .path
            .ok_or_else(|| SqliteError::Pool("pool builder requires a path".into()))?;

        let reader_count = if self.reader_count == 0 {
            1 // need at least one reader
        } else {
            self.reader_count
        };

        // Open writer first (creates the database if needed, sets WAL mode)
        let writer = SqliteConnection::open(&path)?;

        // Apply configured busy_timeout (overrides the default 0 set in open())
        if self.busy_timeout_ms > 0 {
            writer.exec(&format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms))?;
        }

        // Force WAL initialization: on some system SQLite builds (e.g. macOS 3.51+),
        // read-only connections cannot open a WAL database until the WAL/SHM files
        // are materialized on disk. Executing a trivial write-then-rollback forces
        // SQLite to create these files before any reader connections open.
        writer.exec("BEGIN IMMEDIATE; ROLLBACK")?;

        // Open readers
        let mut readers = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            let reader = SqliteConnection::open_readonly(&path)?;
            if self.busy_timeout_ms > 0 {
                reader.exec(&format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms))?;
            }
            readers.push(Mutex::new(reader));
        }

        Ok(SqlitePool {
            writer: Mutex::new(writer),
            readers,
            closed: Arc::new(AtomicBool::new(false)),
            reader_idx: AtomicUsize::new(0),
        })
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        format!("{}/bsql_test_pool_{}_{}.db", dir.display(), pid, id)
    }

    // ---- connect / close ----

    #[test]
    fn pool_connect_and_close() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        assert!(!pool.is_closed());
        assert_eq!(pool.reader_count(), 4);
        pool.close();
        assert!(pool.is_closed());
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_connect_creates_file() {
        let path = temp_db_path();
        assert!(!std::path::Path::new(&path).exists());
        let pool = SqlitePool::connect(&path).unwrap();
        assert!(std::path::Path::new(&path).exists());
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_close_then_is_closed() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        assert!(!pool.is_closed());
        pool.close();
        assert!(pool.is_closed());
        // Close again is idempotent
        pool.close();
        assert!(pool.is_closed());
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_drop_without_close() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        // Just drop -- should not panic
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- builder ----

    #[test]
    fn pool_builder_custom_readers() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(2)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 2);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_requires_path() {
        let result = SqlitePoolBuilder::new().build();
        assert!(result.is_err());
        match result {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("path")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn pool_builder_zero_readers_becomes_one() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(0)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 1);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_one_reader() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(1)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 1);
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_many_readers() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(8)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 8);
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        for _ in 0..16 {
            let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
            assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        }
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_busy_timeout_configurable() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .busy_timeout(std::time::Duration::from_millis(5000))
            .build()
            .unwrap();

        // Verify the writer has the configured busy_timeout
        let sql = "PRAGMA busy_timeout";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readwrite(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(5000));

        // Verify readers also have the configured busy_timeout
        let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(5000));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_builder_busy_timeout_default_zero() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new().path(&path).build().unwrap();

        let sql = "PRAGMA busy_timeout";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readwrite(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(0));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- simple_exec ----

    #[test]
    fn pool_simple_exec() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_simple_exec_error() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        let result = pool.simple_exec("NOT VALID SQL");
        assert!(result.is_err());
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_simple_exec_pragma() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("PRAGMA cache_size = -32000").unwrap();
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- execute / query ----

    #[test]
    fn pool_execute_and_query() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1, ?2)";
        let hash_ins = hash_sql(sql_ins);
        let params: SmallVec<[ParamValue; 8]> =
            smallvec::smallvec![ParamValue::Int(1), ParamValue::Text("alice".into())];
        let affected = pool.execute(sql_ins, hash_ins, params).unwrap();
        assert_eq!(affected, 1);

        let params2: SmallVec<[ParamValue; 8]> =
            smallvec::smallvec![ParamValue::Int(2), ParamValue::Text("bob".into())];
        pool.execute(sql_ins, hash_ins, params2).unwrap();

        let sql_sel = "SELECT id, name FROM t ORDER BY id";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("alice"));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));
        assert_eq!(result.get_str(1, 1, &arena), Some("bob"));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_query_readwrite() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (42)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readwrite(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_update() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, val TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2, 'b')").unwrap();

        let sql = "UPDATE t SET val = ?1";
        let hash = hash_sql(sql);
        let affected = pool
            .execute(
                sql,
                hash,
                smallvec::smallvec![ParamValue::Text("new".into())],
            )
            .unwrap();
        assert_eq!(affected, 2);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_delete() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (3)").unwrap();

        let sql = "DELETE FROM t WHERE id > ?1";
        let hash = hash_sql(sql);
        let affected = pool
            .execute(sql, hash, smallvec::smallvec![ParamValue::Int(1)])
            .unwrap();
        assert_eq!(affected, 2);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- closed pool rejects ----

    #[test]
    fn pool_closed_rejects_queries() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();

        let result = pool.query_readonly("SELECT 1", 0, SmallVec::new());
        assert!(result.is_err());
        match result {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }

        let result = pool.query_readwrite("SELECT 1", 0, SmallVec::new());
        assert!(result.is_err());

        let result = pool.execute("SELECT 1", 0, SmallVec::new());
        assert!(result.is_err());

        let result = pool.simple_exec("SELECT 1");
        assert!(result.is_err());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_closed_query_readonly_error_message() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();
        match pool.query_readonly("SELECT 1", 0, SmallVec::new()) {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_closed_query_readwrite_error_message() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();
        match pool.query_readwrite("SELECT 1", 0, SmallVec::new()) {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_closed_execute_error_message() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();
        match pool.execute("SELECT 1", 0, SmallVec::new()) {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_closed_simple_exec_error_message() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();
        match pool.simple_exec("SELECT 1") {
            Err(SqliteError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- round-robin ----

    #[test]
    fn pool_round_robin_readers() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(2)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        for _ in 0..4 {
            let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
            assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        }

        let idx = pool.reader_idx.load(Ordering::Relaxed);
        assert_eq!(idx, 4);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_round_robin_wraps() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(3)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        // Issue 7 queries over 3 readers => indices 0,1,2,0,1,2,0
        for _ in 0..7 {
            let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
            assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        }

        let idx = pool.reader_idx.load(Ordering::Relaxed);
        assert_eq!(idx, 7);
        // 7 % 3 == 1, so next query would go to reader 1

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- warmup ----

    #[test]
    fn pool_warmup() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        pool.warmup(&["SELECT id FROM t WHERE id = ?1"]);

        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        let sql = "SELECT id FROM t WHERE id = ?1";
        let hash = hash_sql(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::smallvec![ParamValue::Int(1)])
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_warmup_multiple_statements() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        pool.warmup(&[
            "SELECT id FROM t WHERE id = ?1",
            "SELECT name FROM t WHERE id = ?1",
            "INSERT INTO t VALUES (?1, ?2)",
        ]);

        // Verify the warmed-up statements can be used
        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let hash = hash_sql(sql);
        pool.execute(
            sql,
            hash,
            smallvec::smallvec![ParamValue::Int(1), ParamValue::Text("a".into())],
        )
        .unwrap();

        let sql2 = "SELECT name FROM t WHERE id = ?1";
        let hash2 = hash_sql(sql2);
        let (result, arena) = pool
            .query_readonly(sql2, hash2, smallvec::smallvec![ParamValue::Int(1)])
            .unwrap();
        assert_eq!(result.get_str(0, 0, &arena), Some("a"));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_warmup_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        // Warmup with empty list should not fail
        pool.warmup(&[]);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- ParamValue types ----

    #[test]
    fn pool_null_params() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let hash = hash_sql(sql);
        let params: SmallVec<[ParamValue; 8]> =
            smallvec::smallvec![ParamValue::Int(1), ParamValue::Null];
        pool.execute(sql, hash, params).unwrap();

        let sql_sel = "SELECT id, name FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert!(result.is_null(0, 1));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_value_all_types() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec(
            "CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BLOB, e INTEGER, f INTEGER)",
        )
        .unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
        let hash = hash_sql(sql);
        let params: SmallVec<[ParamValue; 8]> = smallvec::smallvec![
            ParamValue::Int(99),
            ParamValue::Real(1.5),
            ParamValue::Text("test".into()),
            ParamValue::Blob(vec![0xAB, 0xCD]),
            ParamValue::Bool(true),
            ParamValue::Null,
        ];
        pool.execute(sql, hash, params).unwrap();

        let sql_sel = "SELECT a, b, c, d, e, f FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();

        assert_eq!(result.get_i64(0, 0, &arena), Some(99));
        assert!((result.get_f64(0, 1, &arena).unwrap() - 1.5).abs() < f64::EPSILON);
        assert_eq!(result.get_str(0, 2, &arena), Some("test"));
        assert_eq!(result.get_bytes(0, 3, &arena), Some(&[0xAB, 0xCD][..]));
        assert_eq!(result.get_bool(0, 4, &arena), Some(true));
        assert!(result.is_null(0, 5));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_int_min_max() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        pool.execute(sql, hash, smallvec::smallvec![ParamValue::Int(i64::MIN)])
            .unwrap();
        pool.execute(sql, hash, smallvec::smallvec![ParamValue::Int(i64::MAX)])
            .unwrap();

        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(i64::MIN));
        assert_eq!(result.get_i64(1, 0, &arena), Some(i64::MAX));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_real_nan() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (val REAL)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        pool.execute(sql, hash, smallvec::smallvec![ParamValue::Real(f64::NAN)])
            .unwrap();
        // NaN stored -- does not crash. Just verify we can read it back.
        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let _ = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_text_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (val TEXT)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        pool.execute(
            sql,
            hash,
            smallvec::smallvec![ParamValue::Text(String::new())],
        )
        .unwrap();

        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, _arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert!(!result.is_null(0, 0));
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_text_unicode() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (val TEXT)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        let unicode = "\u{1F600}\u{4e16}\u{754c}";
        pool.execute(
            sql,
            hash,
            smallvec::smallvec![ParamValue::Text(unicode.into())],
        )
        .unwrap();

        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.get_str(0, 0, &arena), Some(unicode));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_blob_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (val BLOB)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        pool.execute(sql, hash, smallvec::smallvec![ParamValue::Blob(vec![])])
            .unwrap();

        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, _arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert!(!result.is_null(0, 0));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_param_bool_true_false() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        pool.execute(sql, hash, smallvec::smallvec![ParamValue::Bool(true)])
            .unwrap();
        pool.execute(sql, hash, smallvec::smallvec![ParamValue::Bool(false)])
            .unwrap();

        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.get_bool(0, 0, &arena), Some(true));
        assert_eq!(result.get_bool(1, 0, &arena), Some(false));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- ParamValue derives ----

    #[test]
    fn param_value_debug() {
        let p = ParamValue::Int(42);
        assert!(format!("{p:?}").contains("42"));

        let p = ParamValue::Null;
        assert!(format!("{p:?}").contains("Null"));

        let p = ParamValue::Text("hello".into());
        assert!(format!("{p:?}").contains("hello"));

        let p = ParamValue::Real(3.14);
        assert!(format!("{p:?}").contains("3.14"));

        let p = ParamValue::Blob(vec![1, 2]);
        assert!(format!("{p:?}").contains("Blob"));

        let p = ParamValue::Bool(true);
        assert!(format!("{p:?}").contains("true"));
    }

    #[test]
    fn param_value_clone() {
        let p = ParamValue::Text("hello".into());
        let p2 = p.clone();
        match p2 {
            ParamValue::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("expected Text"),
        }
    }

    // ---- concurrent reads ----

    #[test]
    fn pool_concurrent_reads() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(4)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        // Insert some data
        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        for i in 1..=100 {
            pool.execute(sql_ins, hash_ins, smallvec::smallvec![ParamValue::Int(i)])
                .unwrap();
        }

        let pool = Arc::new(pool);
        let mut handles = Vec::new();

        // Spawn 8 threads all reading concurrently
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            let handle = std::thread::spawn(move || {
                let sql = "SELECT COUNT(*) FROM t";
                let hash = hash_sql(sql);
                let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
                assert_eq!(result.get_i64(0, 0, &arena), Some(100));
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        // Pool is in Arc, close and drop
        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // ---- pool status ----

    #[test]
    fn pool_reader_count() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(3)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 3);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_is_closed_initially_false() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        assert!(!pool.is_closed());
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- pool builder() shortcut ----

    #[test]
    fn pool_builder_shortcut() {
        let path = temp_db_path();
        let pool = SqlitePool::builder()
            .path(&path)
            .reader_count(2)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 2);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- DDL then DML via pool ----

    #[test]
    fn pool_ddl_then_dml() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'hello')")
            .unwrap();
        pool.simple_exec("ALTER TABLE t ADD COLUMN extra TEXT")
            .unwrap();

        let sql = "UPDATE t SET extra = ?1 WHERE id = ?2";
        let hash = hash_sql(sql);
        pool.execute(
            sql,
            hash,
            smallvec::smallvec![ParamValue::Text("world".into()), ParamValue::Int(1)],
        )
        .unwrap();

        let sql_sel = "SELECT id, name, extra FROM t";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("hello"));
        assert_eq!(result.get_str(0, 2, &arena), Some("world"));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- pool handles SQL error gracefully ----

    #[test]
    fn pool_sql_error_does_not_break_pool() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        // Cause an error
        let result = pool.simple_exec("INSERT INTO nonexistent VALUES (1)");
        assert!(result.is_err());

        // Pool should still work
        pool.simple_exec("INSERT INTO t VALUES (42)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // --- Transactions ---

    #[test]
    fn transaction_commit() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        pool.begin_transaction().unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2)").unwrap();
        pool.commit_transaction().unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let h = hash_sql(sql);
        let (result, arena) = pool.query_readonly(sql, h, SmallVec::new()).unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_rollback() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        pool.begin_transaction().unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();
        pool.rollback_transaction().unwrap();

        let sql = "SELECT id FROM t";
        let h = hash_sql(sql);
        let (result, _arena) = pool.query_readonly(sql, h, SmallVec::new()).unwrap();
        assert_eq!(result.row_count, 0);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_savepoint() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        pool.begin_transaction().unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        pool.savepoint("sp1").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2)").unwrap();

        // Rollback to savepoint — should undo the second insert
        pool.rollback_to("sp1").unwrap();
        pool.commit_transaction().unwrap();

        let sql = "SELECT id FROM t";
        let h = hash_sql(sql);
        let (result, arena) = pool.query_readonly(sql, h, SmallVec::new()).unwrap();
        assert_eq!(result.row_count, 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_savepoint_release() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        pool.begin_transaction().unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        pool.savepoint("sp1").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2)").unwrap();
        pool.release_savepoint("sp1").unwrap();

        pool.commit_transaction().unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let h = hash_sql(sql);
        let (result, arena) = pool.query_readonly(sql, h, SmallVec::new()).unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // --- Streaming ---

    #[test]
    fn pool_streaming_query() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        for i in 0..10 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }

        let sql = "SELECT id FROM t ORDER BY id";
        let h = hash_sql(sql);
        let (first_result, first_arena, mut state, reader_idx) =
            pool.query_streaming(sql, h, SmallVec::new(), 3).unwrap();

        assert!(first_result.row_count > 0);
        assert_eq!(first_result.get_i64(0, 0, &first_arena), Some(0));

        // Continue fetching until done
        let mut total_rows = first_result.row_count;
        while !state.inner.finished {
            let (result, _arena, new_state) = pool.streaming_next(state, reader_idx).unwrap();
            total_rows += result.row_count;
            state = new_state;
        }
        assert_eq!(total_rows, 10);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // --- fetch_one_direct ---

    #[test]
    fn pool_fetch_one_direct() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (42, 'hello')")
            .unwrap();

        let sql = "SELECT id, name FROM t WHERE id = ?1";
        let sql_hash = hash_sql(sql);
        let id: i64 = 42;
        let result: (i64, String) = pool
            .fetch_one_direct(sql, sql_hash, &[&id], false, |stmt| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned())
                    .ok_or_else(|| SqliteError::Internal("decode error".into()))?;
                Ok((id, name))
            })
            .unwrap();
        assert_eq!(result.0, 42);
        assert_eq!(result.1, "hello");

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_one_direct_no_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t WHERE id = ?1";
        let sql_hash = hash_sql(sql);
        let id: i64 = 999;
        let result =
            pool.fetch_one_direct(
                sql,
                sql_hash,
                &[&id],
                false,
                |stmt| Ok(stmt.column_int64(0)),
            );
        assert!(result.is_err());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_optional_direct() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (7)").unwrap();

        let sql = "SELECT id FROM t WHERE id = ?1";
        let sql_hash = hash_sql(sql);

        // Found
        let id: i64 = 7;
        let result =
            pool.fetch_optional_direct(sql, sql_hash, &[&id], false, |stmt| {
                Ok(stmt.column_int64(0))
            })
            .unwrap();
        assert_eq!(result, Some(7));

        // Not found
        let id: i64 = 999;
        let result =
            pool.fetch_optional_direct(sql, sql_hash, &[&id], false, |stmt| {
                Ok(stmt.column_int64(0))
            })
            .unwrap();
        assert_eq!(result, None);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // --- fetch_all_direct ---

    #[test]
    fn pool_fetch_all_direct_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql = "SELECT id, name FROM t";
        let sql_hash = hash_sql(sql);
        let rows: Vec<(i64, String)> = pool
            .fetch_all_direct(sql, sql_hash, &[], false, |stmt| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned())
                    .ok_or_else(|| SqliteError::Internal("decode error".into()))?;
                Ok((id, name))
            })
            .unwrap();
        assert!(rows.is_empty());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_single_row() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'alice')")
            .unwrap();

        let sql = "SELECT id, name FROM t";
        let sql_hash = hash_sql(sql);
        let rows: Vec<(i64, String)> = pool
            .fetch_all_direct(sql, sql_hash, &[], false, |stmt| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned())
                    .ok_or_else(|| SqliteError::Internal("decode error".into()))?;
                Ok((id, name))
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (1, "alice".to_owned()));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_100_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        for i in 0..100 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }

        let sql = "SELECT id FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);
        let rows: Vec<i64> = pool
            .fetch_all_direct(sql, sql_hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(rows.len(), 100);
        assert_eq!(rows[0], 0);
        assert_eq!(rows[99], 99);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_10k_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("BEGIN").unwrap();
        for i in 0..10_000 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }
        pool.simple_exec("COMMIT").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);
        let rows: Vec<i64> = pool
            .fetch_all_direct(sql, sql_hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(rows.len(), 10_000);
        assert_eq!(rows[0], 0);
        assert_eq!(rows[9_999], 9_999);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_null_columns() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, NULL)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (NULL, 'bob')")
            .unwrap();

        let sql = "SELECT id, name FROM t ORDER BY rowid";
        let sql_hash = hash_sql(sql);
        let rows: Vec<(Option<i64>, Option<String>)> = pool
            .fetch_all_direct(sql, sql_hash, &[], false, |stmt| {
                use libsqlite3_sys as raw;
                let id = if stmt.column_type(0) == raw::SQLITE_NULL {
                    None
                } else {
                    Some(stmt.column_int64(0))
                };
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned());
                Ok((id, name))
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (Some(1), None));
        assert_eq!(rows[1], (None, Some("bob".to_owned())));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_mixed_types() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (i INTEGER, r REAL, t TEXT, b BLOB)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (42, 3.14, 'hello', X'DEADBEEF')")
            .unwrap();

        let sql = "SELECT i, r, t, b FROM t";
        let sql_hash = hash_sql(sql);
        let rows: Vec<(i64, f64, String, Vec<u8>)> = pool
            .fetch_all_direct(sql, sql_hash, &[], false, |stmt| {
                let i = stmt.column_int64(0);
                let r = stmt.column_double(1);
                let t = stmt
                    .column_text(2)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned())
                    .ok_or_else(|| SqliteError::Internal("decode error".into()))?;
                let b = stmt.column_blob(3).to_vec();
                Ok((i, r, t, b))
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 42);
        assert!((rows[0].1 - 3.14).abs() < f64::EPSILON);
        assert_eq!(rows[0].2, "hello");
        assert_eq!(rows[0].3, vec![0xDE, 0xAD, 0xBE, 0xEF]);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_with_params() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        for i in 0..5 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }

        let sql = "SELECT id FROM t WHERE id >= ?1 ORDER BY id";
        let sql_hash = hash_sql(sql);
        let min_id: i64 = 3;
        let rows: Vec<i64> = pool
            .fetch_all_direct(sql, sql_hash, &[&min_id], false, |stmt| {
                Ok(stmt.column_int64(0))
            })
            .unwrap();
        assert_eq!(rows, vec![3, 4]);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_direct_writer() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let sql_hash = hash_sql(sql);
        let rows: Vec<i64> = pool
            .fetch_all_direct(sql, sql_hash, &[], true, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(rows, vec![1]);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // --- execute_direct ---

    #[test]
    fn pool_execute_direct_insert() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let sql_hash = hash_sql(sql);
        let id: i64 = 42;
        let name = "alice";
        let affected = pool.execute_direct(sql, sql_hash, &[&id, &name]).unwrap();
        assert_eq!(affected, 1);

        let sql_sel = "SELECT id FROM t";
        let rows: Vec<i64> = pool
            .fetch_all_direct(sql_sel, hash_sql(sql_sel), &[], false, |stmt| {
                Ok(stmt.column_int64(0))
            })
            .unwrap();
        assert_eq!(rows, vec![42]);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_direct_update() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, val TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2, 'b')").unwrap();

        let sql = "UPDATE t SET val = ?1 WHERE id > ?2";
        let sql_hash = hash_sql(sql);
        let new_val = "new";
        let min_id: i64 = 1;
        let affected = pool
            .execute_direct(sql, sql_hash, &[&new_val, &min_id])
            .unwrap();
        assert_eq!(affected, 1);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_direct_no_params() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "DELETE FROM t";
        let sql_hash = hash_sql(sql);
        let affected = pool.execute_direct(sql, sql_hash, &[]).unwrap();
        assert_eq!(affected, 1);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- fetch_all_arena pool tests ----

    #[test]
    fn pool_fetch_all_arena_text() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'alice')")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);

        #[allow(dead_code)]
        struct Row {
            id: i64,
            name: String,
        }

        let rows = pool
            .fetch_all_arena(sql, sql_hash, &[], false, |stmt, _arena| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .ok_or_else(|| SqliteError::Internal("null".into()))
                    .and_then(|b| {
                        std::str::from_utf8(b).map_err(|_| SqliteError::Internal("bad utf8".into()))
                    })?
                    .to_owned();
                Ok(Row { id, name })
            })
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].name, "alice");
        assert_eq!(rows[1].id, 2);
        assert_eq!(rows[1].name, "bob");

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_arena_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (name TEXT)").unwrap();

        let sql = "SELECT name FROM t";
        let sql_hash = hash_sql(sql);
        let rows = pool
            .fetch_all_arena(sql, sql_hash, &[], false, |stmt, _arena| {
                let s = stmt
                    .column_text(0)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok(s)
            })
            .unwrap();
        assert!(rows.is_empty());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_arena_writer() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'test')")
            .unwrap();

        // is_write = true routes to writer
        let sql = "SELECT id, name FROM t";
        let sql_hash = hash_sql(sql);
        let rows = pool
            .fetch_all_arena(sql, sql_hash, &[], true, |stmt, _arena| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok((id, name))
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1, "test");

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_fetch_all_arena_1000_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("BEGIN").unwrap();
        for i in 0..1000 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i}, 'name_{i}')"))
                .unwrap();
        }
        pool.simple_exec("COMMIT").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);

        #[allow(dead_code)]
        struct Row {
            id: i64,
            name: String,
        }

        let rows = pool
            .fetch_all_arena(sql, sql_hash, &[], false, |stmt, _arena| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok(Row { id, name })
            })
            .unwrap();

        assert_eq!(rows.len(), 1000);
        assert_eq!(rows[0].name, "name_0");
        assert_eq!(rows[999].name, "name_999");

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- for_each ----

    #[test]
    fn pool_for_each_zero_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let mut count = 0usize;
        pool.for_each(sql, hash, &[], false, |_stmt| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_for_each_multiple_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        for i in 0..10 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i}, 'name_{i}')"))
                .unwrap();
        }

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let mut count = 0usize;
        pool.for_each(sql, hash, &[], false, |stmt| {
            let id = stmt.column_int64(0);
            assert_eq!(id, count as i64);
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 10);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_for_each_writer_route() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("INSERT INTO t VALUES (42)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let mut found = 0i64;
        pool.for_each(sql, hash, &[], true, |stmt| {
            found = stmt.column_int64(0);
            Ok(())
        })
        .unwrap();
        assert_eq!(found, 42);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- for_each_collect ----

    #[test]
    fn pool_for_each_collect_builds_vec() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (1, 'alice')")
            .unwrap();
        pool.simple_exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let results: Vec<i64> = pool
            .for_each_collect(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(results, vec![1, 2]);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_for_each_collect_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let results: Vec<i64> = pool
            .for_each_collect(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert!(results.is_empty());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_for_each_closed_pool_errors() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.close();

        let sql = "SELECT 1";
        let hash = hash_sql(sql);
        let result = pool.for_each(sql, hash, &[], false, |_| Ok(()));
        assert!(result.is_err());

        let result: Result<Vec<i64>, _> =
            pool.for_each_collect(sql, hash, &[], false, |_| Ok(0i64));
        assert!(result.is_err());

        let _ = std::fs::remove_file(&path);
    }

    // ---- Gap: concurrent 2 readers simultaneously ----

    #[test]
    fn pool_concurrent_two_readers_simultaneously() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(4)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, val TEXT)")
            .unwrap();
        for i in 0..50 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                .unwrap();
        }

        let pool = Arc::new(pool);
        let barrier = std::sync::Barrier::new(2);
        let barrier = Arc::new(barrier);
        let mut handles = Vec::new();

        for _ in 0..2 {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            let handle = std::thread::spawn(move || {
                barrier.wait(); // both readers start at the same time
                let sql = "SELECT COUNT(*) FROM t";
                let hash = hash_sql(sql);
                let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
                assert_eq!(result.get_i64(0, 0, &arena), Some(50));
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // ---- Gap: write during concurrent reads ----

    #[test]
    fn pool_write_during_concurrent_reads() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(4)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        for i in 0..100 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }

        let pool = Arc::new(pool);
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut handles = Vec::new();

        // 4 reader threads
        for _ in 0..4 {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    let sql = "SELECT COUNT(*) FROM t";
                    let hash = hash_sql(sql);
                    let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
                    // Count may be 100 or more (writer may have inserted)
                    let count = result.get_i64(0, 0, &arena).unwrap();
                    assert!(count >= 100, "count should be >= 100, got {count}");
                }
            }));
        }

        // 1 writer thread
        {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let sql = "INSERT INTO t VALUES (?1)";
                let hash = hash_sql(sql);
                for i in 100..150 {
                    // Writer may get SQLITE_BUSY with busy_timeout=0 — that's OK.
                    let _ = pool.execute(sql, hash, smallvec::smallvec![ParamValue::Int(i)]);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // ---- Gap: very large result 10K rows via pool ----

    #[test]
    fn pool_query_10k_rows() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.simple_exec("BEGIN").unwrap();
        for i in 0..10_000 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }
        pool.simple_exec("COMMIT").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let (result, arena) = pool.query_readonly(sql, hash, SmallVec::new()).unwrap();
        assert_eq!(result.len(), 10_000);
        assert_eq!(result.get_i64(0, 0, &arena), Some(0));
        assert_eq!(result.get_i64(9_999, 0, &arena), Some(9_999));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- Gap: NULL handling for all types via pool ----

    #[test]
    fn pool_null_handling_all_types() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec(
            "CREATE TABLE t (
                a INTEGER, b REAL, c TEXT, d BLOB, e INTEGER
            )",
        )
        .unwrap();

        // Insert a row where all columns are NULL
        let sql_ins = "INSERT INTO t VALUES (?1, ?2, ?3, ?4, ?5)";
        let hash_ins = hash_sql(sql_ins);
        pool.execute(
            sql_ins,
            hash_ins,
            smallvec::smallvec![
                ParamValue::Null,
                ParamValue::Null,
                ParamValue::Null,
                ParamValue::Null,
                ParamValue::Null,
            ],
        )
        .unwrap();

        // Also insert a row with real values to confirm mixed handling
        pool.execute(
            sql_ins,
            hash_ins,
            smallvec::smallvec![
                ParamValue::Int(42),
                ParamValue::Real(3.14),
                ParamValue::Text("hello".into()),
                ParamValue::Blob(vec![0xDE, 0xAD]),
                ParamValue::Bool(true),
            ],
        )
        .unwrap();

        let sql_sel = "SELECT a, b, c, d, e FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let (result, arena) = pool
            .query_readonly(sql_sel, hash_sel, SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 2);

        // Row 0: all NULLs
        for col in 0..5 {
            assert!(result.is_null(0, col), "row 0, col {col} should be NULL");
        }
        assert_eq!(result.get_i64(0, 0, &arena), None);
        assert_eq!(result.get_f64(0, 1, &arena), None);
        assert_eq!(result.get_str(0, 2, &arena), None);
        assert_eq!(result.get_bytes(0, 3, &arena), None);
        assert_eq!(result.get_bool(0, 4, &arena), None);

        // Row 1: real values
        assert_eq!(result.get_i64(1, 0, &arena), Some(42));
        let f = result.get_f64(1, 1, &arena).unwrap();
        assert!((f - 3.14).abs() < f64::EPSILON);
        assert_eq!(result.get_str(1, 2, &arena), Some("hello"));
        assert_eq!(result.get_bytes(1, 3, &arena), Some(&[0xDE, 0xAD][..]));
        // Bool is stored as integer: 1 = true
        assert_eq!(result.get_bool(1, 4, &arena), Some(true));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    // ---- Gap: concurrent stress with many readers ----

    #[test]
    fn pool_concurrent_8_threads_read_stress() {
        let path = temp_db_path();
        let pool = SqlitePoolBuilder::new()
            .path(&path)
            .reader_count(4)
            .build()
            .unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, val TEXT)")
            .unwrap();
        pool.simple_exec("BEGIN").unwrap();
        for i in 0..500 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i}, 'value_{i}')"))
                .unwrap();
        }
        pool.simple_exec("COMMIT").unwrap();

        let pool = Arc::new(pool);
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let mut handles = Vec::new();

        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let sql = "SELECT id, val FROM t WHERE id = ?1";
                    let hash = hash_sql(sql);
                    let tid_hash = {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        std::thread::current().id().hash(&mut h);
                        h.finish()
                    };
                    let row_id = (tid_hash % 500) as i64;
                    let (result, _arena) = pool
                        .query_readonly(sql, hash, smallvec::smallvec![ParamValue::Int(row_id)])
                        .unwrap();
                    // May get 0 or 1 rows depending on the thread ID
                    assert!(result.len() <= 1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // ---- execute_batch ----

    #[test]
    fn pool_execute_batch_inserts() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let sql_hash = hash_sql(sql);

        let id1: i64 = 1;
        let name1: &str = "alice";
        let id2: i64 = 2;
        let name2: &str = "bob";
        let id3: i64 = 3;
        let name3: &str = "carol";

        let param_sets: &[&[&dyn SqliteEncode]] =
            &[&[&id1, &name1], &[&id2, &name2], &[&id3, &name3]];

        let total_affected = pool.execute_batch(sql, sql_hash, param_sets).unwrap();
        assert_eq!(total_affected, 3);

        let sel = "SELECT id, name FROM t ORDER BY id";
        let sel_hash = hash_sql(sel);
        let (result, arena) = pool.query_readonly(sel, sel_hash, SmallVec::new()).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("alice"));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));
        assert_eq!(result.get_str(1, 1, &arena), Some("bob"));
        assert_eq!(result.get_i64(2, 0, &arena), Some(3));
        assert_eq!(result.get_str(2, 1, &arena), Some("carol"));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_batch_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let sql_hash = hash_sql(sql);
        let param_sets: &[&[&dyn SqliteEncode]] = &[];
        let total = pool.execute_batch(sql, sql_hash, param_sets).unwrap();
        assert_eq!(total, 0);

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_batch_single_set() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let sql_hash = hash_sql(sql);
        let id: i64 = 42;
        let param_sets: &[&[&dyn SqliteEncode]] = &[&[&id]];
        let total = pool.execute_batch(sql, sql_hash, param_sets).unwrap();
        assert_eq!(total, 1);

        let sel = "SELECT id FROM t";
        let sel_hash = hash_sql(sel);
        let (result, arena) = pool.query_readonly(sel, sel_hash, SmallVec::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_batch_100_inserts() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let sql_hash = hash_sql(sql);

        let ids: Vec<i64> = (0..100).collect();
        let param_sets: Vec<Vec<&dyn SqliteEncode>> =
            ids.iter().map(|id| vec![id as &dyn SqliteEncode]).collect();
        let param_refs: Vec<&[&dyn SqliteEncode]> =
            param_sets.iter().map(|v| v.as_slice()).collect();

        let total = pool.execute_batch(sql, sql_hash, &param_refs).unwrap();
        assert_eq!(total, 100);

        let sel = "SELECT COUNT(*) FROM t";
        let sel_hash = hash_sql(sel);
        let (result, arena) = pool.query_readonly(sel, sel_hash, SmallVec::new()).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(100));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pool_execute_batch_closed_pool_errors() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER)").unwrap();
        pool.close();

        let sql = "INSERT INTO t VALUES (?1)";
        let sql_hash = hash_sql(sql);
        let id: i64 = 1;
        let param_sets: &[&[&dyn SqliteEncode]] = &[&[&id]];
        let result = pool.execute_batch(sql, sql_hash, param_sets);
        assert!(result.is_err());

        let _ = std::fs::remove_file(&path);
    }

    // --- Savepoint name validation (SQL injection prevention) ---

    #[test]
    fn savepoint_name_valid() {
        assert!(validate_savepoint_name("sp1").is_ok());
        assert!(validate_savepoint_name("_sp").is_ok());
        assert!(validate_savepoint_name("my_savepoint_123").is_ok());
        assert!(validate_savepoint_name("a").is_ok());
        assert!(validate_savepoint_name("_").is_ok());
    }

    #[test]
    fn savepoint_name_rejects_empty() {
        assert!(validate_savepoint_name("").is_err());
    }

    #[test]
    fn savepoint_name_rejects_too_long() {
        let long = "a".repeat(64);
        assert!(validate_savepoint_name(&long).is_err());
    }

    #[test]
    fn savepoint_name_accepts_max_length() {
        let max = "a".repeat(63);
        assert!(validate_savepoint_name(&max).is_ok());
    }

    #[test]
    fn savepoint_name_rejects_digit_start() {
        assert!(validate_savepoint_name("1sp").is_err());
    }

    #[test]
    fn savepoint_name_rejects_special_chars() {
        assert!(validate_savepoint_name("sp-1").is_err());
        assert!(validate_savepoint_name("sp.1").is_err());
        assert!(validate_savepoint_name("sp 1").is_err());
        assert!(validate_savepoint_name("sp;1").is_err());
        assert!(validate_savepoint_name("sp'1").is_err());
        assert!(validate_savepoint_name("sp\"1").is_err());
    }

    #[test]
    fn savepoint_name_rejects_sql_injection() {
        assert!(validate_savepoint_name("sp; DROP TABLE users").is_err());
        assert!(validate_savepoint_name("sp'--").is_err());
        assert!(validate_savepoint_name("sp\"; DROP TABLE t; --").is_err());
    }

    #[test]
    fn savepoint_name_rejects_unicode() {
        assert!(validate_savepoint_name("sp_\u{00e9}").is_err());
        assert!(validate_savepoint_name("\u{0410}").is_err());
    }
}
