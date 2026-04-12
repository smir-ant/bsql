//! SQLite connection pool — synchronous wrapper over `bsql_driver_sqlite::pool::SqlitePool`.
//!
//! The driver pool uses `Mutex<SqliteConnection>` for direct synchronous access.
//! This wrapper provides bsql error types and a clean public API.
//!
//! No tokio. No async. No block_in_place. Just direct sync calls.

use std::sync::Arc;

use crate::error::{BsqlError, BsqlResult};

/// A SQLite connection pool.
///
/// Created via [`SqlitePool::open`] or [`SqlitePool::builder`]. Uses a single
/// writer connection plus N reader connections (default 4). All operations are
/// synchronous -- no async runtime required.
///
/// bsql automatically configures WAL mode, mmap, and page cache for optimal
/// performance.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::SqlitePool;
///
/// // Simple: open with defaults (4 readers)
/// let pool = SqlitePool::open("./myapp.db")?;
///
/// // Advanced: configure via builder
/// let pool = SqlitePool::builder()
///     .path("./myapp.db")
///     .reader_count(8)
///     .build()?;
/// ```
pub struct SqlitePool {
    inner: Arc<bsql_driver_sqlite::pool::SqlitePool>,
}

/// Builder for configuring a SQLite connection pool.
pub struct SqlitePoolBuilder {
    path: Option<String>,
    reader_count: usize,
}

impl SqlitePoolBuilder {
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

    /// Build and open the pool.
    pub fn build(self) -> BsqlResult<SqlitePool> {
        let path = self.path.ok_or_else(|| {
            BsqlError::Connect(crate::error::ConnectError {
                message: "SQLite pool builder requires a path".into(),
                source: None,
            })
        })?;

        let inner = bsql_driver_sqlite::pool::SqlitePool::builder()
            .path(&path)
            .reader_count(self.reader_count)
            .build()
            .map_err(BsqlError::from_sqlite)?;

        Ok(SqlitePool {
            inner: Arc::new(inner),
        })
    }
}

impl SqlitePool {
    /// Access the inner driver pool.
    ///
    /// # Doc-hidden
    ///
    /// Used by generated code from `bsql::query!`. Not part of the public API.
    #[doc(hidden)]
    #[inline]
    pub fn __inner(&self) -> &bsql_driver_sqlite::pool::SqlitePool {
        &self.inner
    }

    /// Open a SQLite pool with default settings (4 reader connections).
    ///
    /// Alias: [`open`](Self::open) — same behavior, friendlier name for file-backed databases.
    pub fn connect(path: &str) -> BsqlResult<Self> {
        let inner =
            bsql_driver_sqlite::pool::SqlitePool::connect(path).map_err(BsqlError::from_sqlite)?;
        Ok(SqlitePool {
            inner: Arc::new(inner),
        })
    }

    /// Open a SQLite pool with default settings (4 reader connections).
    ///
    /// Identical to [`connect`](Self::connect). Provided because `open` reads
    /// more naturally for file-backed databases:
    ///
    /// ```rust,ignore
    /// let pool = SqlitePool::open("./data.db")?;
    /// ```
    pub fn open(path: &str) -> BsqlResult<Self> {
        Self::connect(path)
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> SqlitePoolBuilder {
        SqlitePoolBuilder {
            path: None,
            reader_count: 4,
        }
    }

    /// Execute a read-only query, returning the `QueryResult` and its `Arena`.
    pub fn query_readonly(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        self.inner
            .query_readonly(sql, sql_hash, params)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute a read-write query, returning the `QueryResult` and its `Arena`.
    pub fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        self.inner
            .query_readwrite(sql, sql_hash, params)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute a write statement (INSERT/UPDATE/DELETE), return affected row count.
    pub fn execute_sql(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<u64> {
        self.inner
            .execute(sql, sql_hash, params)
            .map_err(BsqlError::from_sqlite)
    }

    /// Fetch exactly one row via direct decode — zero arena overhead.
    ///
    /// The `decode` closure reads columns directly from the stepped statement.
    #[inline]
    pub fn fetch_one_direct<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> BsqlResult<T>
    where
        F: FnOnce(
            &bsql_driver_sqlite::ffi::StmtHandle,
        ) -> Result<T, bsql_driver_sqlite::SqliteError>,
    {
        self.inner
            .fetch_one_direct(sql, sql_hash, params, is_write, decode)
            .map_err(BsqlError::from_sqlite)
    }

    /// Fetch zero or one row via direct decode — zero arena overhead.
    #[inline]
    pub fn fetch_optional_direct<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> BsqlResult<Option<T>>
    where
        F: FnOnce(
            &bsql_driver_sqlite::ffi::StmtHandle,
        ) -> Result<T, bsql_driver_sqlite::SqliteError>,
    {
        self.inner
            .fetch_optional_direct(sql, sql_hash, params, is_write, decode)
            .map_err(BsqlError::from_sqlite)
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
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> BsqlResult<Vec<T>>
    where
        F: Fn(&bsql_driver_sqlite::ffi::StmtHandle) -> Result<T, bsql_driver_sqlite::SqliteError>,
    {
        self.inner
            .fetch_all_direct(sql, sql_hash, params, is_write, decode)
            .map_err(BsqlError::from_sqlite)
    }

    /// Fetch all rows into an arena-backed result — zero per-row heap allocation
    /// for text/blob columns. See [`bsql_driver_sqlite::conn::SqliteConnection::fetch_all_arena`].
    #[inline]
    pub fn fetch_all_arena<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
        is_write: bool,
        decode: F,
    ) -> BsqlResult<bsql_arena::ArenaRows<T>>
    where
        F: Fn(
            &bsql_driver_sqlite::ffi::StmtHandle,
            &mut bsql_arena::Arena,
        ) -> Result<T, bsql_driver_sqlite::SqliteError>,
    {
        self.inner
            .fetch_all_arena(sql, sql_hash, params, is_write, decode)
            .map_err(BsqlError::from_sqlite)
    }

    /// Process each row in-place via a closure. Zero-copy -- text columns
    /// borrow directly from SQLite's internal buffer.
    #[inline]
    pub fn for_each<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
        is_write: bool,
        f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(
            &bsql_driver_sqlite::ffi::StmtHandle,
        ) -> Result<(), bsql_driver_sqlite::SqliteError>,
    {
        self.inner
            .for_each(sql, sql_hash, params, is_write, f)
            .map_err(BsqlError::from_sqlite)
    }

    /// Process each row in-place, collecting results into a `Vec`.
    #[inline]
    pub fn for_each_collect<F, T>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
        is_write: bool,
        f: F,
    ) -> BsqlResult<Vec<T>>
    where
        F: FnMut(
            &bsql_driver_sqlite::ffi::StmtHandle,
        ) -> Result<T, bsql_driver_sqlite::SqliteError>,
    {
        self.inner
            .for_each_collect(sql, sql_hash, params, is_write, f)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute a statement via direct param binding — zero arena/ParamValue overhead.
    ///
    /// Takes `&[&dyn SqliteEncode]` directly instead of `SmallVec<ParamValue>`.
    #[inline]
    pub fn execute_direct(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn bsql_driver_sqlite::codec::SqliteEncode],
    ) -> BsqlResult<u64> {
        self.inner
            .execute_direct(sql, sql_hash, params)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute the same statement N times with different parameter sets.
    ///
    /// Acquires the writer once for the entire batch. Returns the total
    /// number of affected rows across all executions.
    pub fn execute_batch(
        &self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&dyn bsql_driver_sqlite::codec::SqliteEncode]],
    ) -> BsqlResult<u64> {
        self.inner
            .execute_batch(sql, sql_hash, param_sets)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute arbitrary runtime SQL on the writer connection (DDL, PRAGMA, SET).
    ///
    /// This bypasses bsql's compile-time SQL validation entirely.
    /// Use for DDL with runtime-computed identifiers (schema names, table
    /// names) and connection-level session commands. For normal queries,
    /// always prefer `query!()`.
    ///
    /// Named with the `raw_` prefix to make it visually obvious that
    /// you're leaving the compile-time-safe zone.
    pub fn raw_execute(&self, sql: &str) -> BsqlResult<()> {
        self.inner.raw_execute(sql).map_err(BsqlError::from_sqlite)
    }


    /// Begin a transaction on the writer connection.
    ///
    /// Returns a `SqliteTransaction` that must be committed or rolled back.
    /// If dropped without committing, the transaction is automatically rolled back.
    pub fn begin(&self) -> BsqlResult<SqliteTransaction> {
        self.inner
            .begin_transaction()
            .map_err(BsqlError::from_sqlite)?;

        Ok(SqliteTransaction {
            pool: Arc::clone(&self.inner),
            finished: false,
        })
    }

    /// Execute a read-only streaming query.
    ///
    /// Returns the first chunk and a `SqliteStreamingQuery` to continue.
    pub fn query_streaming(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
        chunk_size: usize,
    ) -> BsqlResult<SqliteStreamingQuery> {
        let (first_result, first_arena, state, reader_idx) = self
            .inner
            .query_streaming(sql, sql_hash, params, chunk_size)
            .map_err(BsqlError::from_sqlite)?;

        Ok(SqliteStreamingQuery {
            pool: Arc::clone(&self.inner),
            state: Some(state),
            current_result: Some(first_result),
            current_arena: Some(first_arena),
            position: 0,
            reader_idx,
        })
    }

    /// Pre-prepare statements on all connections (warmup).
    pub fn warmup(&self, sqls: &[&str]) {
        self.inner.warmup(sqls);
    }

    /// Number of reader connections.
    pub fn reader_count(&self) -> usize {
        self.inner.reader_count()
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Close the pool.
    pub fn close(&self) {
        self.inner.close();
    }
}

// ===========================================================================
// SqliteTransaction
// ===========================================================================

/// A SQLite transaction.
///
/// Created by [`SqlitePool::begin()`]. Must be explicitly committed via
/// [`commit()`](SqliteTransaction::commit). If dropped without `commit()`,
/// the transaction is automatically rolled back.
///
/// All write operations during a transaction are routed to the pool's
/// single writer connection.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::SqlitePool;
///
/// let pool = SqlitePool::open("./myapp.db")?;
/// let tx = pool.begin()?;
///
/// // Execute writes within the transaction...
/// bsql::query!("INSERT INTO log (msg) VALUES ($msg: &str)")
///     .execute(&tx)?;
///
/// tx.commit()?;  // or drop to auto-rollback
/// ```
pub struct SqliteTransaction {
    pool: Arc<bsql_driver_sqlite::pool::SqlitePool>,
    finished: bool,
}

impl SqliteTransaction {
    /// Commit the transaction.
    pub fn commit(mut self) -> BsqlResult<()> {
        self.finished = true;
        self.pool
            .commit_transaction()
            .map_err(BsqlError::from_sqlite)
    }

    /// Explicitly roll back the transaction.
    pub fn rollback(mut self) -> BsqlResult<()> {
        self.finished = true;
        self.pool
            .rollback_transaction()
            .map_err(BsqlError::from_sqlite)
    }

    /// Create a savepoint within the transaction.
    pub fn savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        self.pool.savepoint(name).map_err(BsqlError::from_sqlite)
    }

    /// Release (destroy) a savepoint, keeping its effects.
    pub fn release_savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        self.pool
            .release_savepoint(name)
            .map_err(BsqlError::from_sqlite)
    }

    /// Roll back to a savepoint.
    pub fn rollback_to(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        self.pool.rollback_to(name).map_err(BsqlError::from_sqlite)
    }

    /// Execute a write query within the transaction.
    pub fn execute_sql(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<u64> {
        self.pool
            .execute(sql, sql_hash, params)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute the same statement N times with different parameter sets
    /// within the transaction.
    ///
    /// Holds the writer for the entire batch. Returns the total affected rows.
    pub fn execute_batch(
        &self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&dyn bsql_driver_sqlite::codec::SqliteEncode]],
    ) -> BsqlResult<u64> {
        self.pool
            .execute_batch(sql, sql_hash, param_sets)
            .map_err(BsqlError::from_sqlite)
    }

    /// Execute a query within the transaction (writer connection).
    pub fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        self.pool
            .query_readwrite(sql, sql_hash, params)
            .map_err(BsqlError::from_sqlite)
    }
}

impl std::fmt::Debug for SqliteTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteTransaction")
            .field("finished", &self.finished)
            .finish()
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if !self.finished {
            log::warn!(
                "bsql: SqliteTransaction dropped without commit() or rollback() — \
                 rolling back automatically."
            );
            let _ = self.pool.rollback_transaction();
        }
    }
}

// ===========================================================================
// SqliteStreamingQuery
// ===========================================================================

/// A streaming SQLite query result.
///
/// Rows are fetched in chunks. Call `next_chunk()` to get the next batch,
/// or use the `next_row()` helper for row-by-row iteration.
pub struct SqliteStreamingQuery {
    pool: Arc<bsql_driver_sqlite::pool::SqlitePool>,
    state: Option<bsql_driver_sqlite::pool::StreamingState>,
    current_result: Option<bsql_driver_sqlite::conn::QueryResult>,
    current_arena: Option<bsql_arena::Arena>,
    position: usize,
    reader_idx: usize,
}

impl SqliteStreamingQuery {
    /// Fetch the next chunk of rows from SQLite.
    ///
    /// Returns `true` if a new chunk was fetched, `false` if all rows
    /// have been consumed.
    pub fn fetch_next_chunk(&mut self) -> BsqlResult<bool> {
        let state = match self.state.take() {
            Some(s) if !s.inner.finished => s,
            Some(s) => {
                self.state = Some(s);
                return Ok(false);
            }
            None => return Ok(false),
        };

        let (result, arena, new_state) = self
            .pool
            .streaming_next(state, self.reader_idx)
            .map_err(BsqlError::from_sqlite)?;

        let has_rows = result.row_count > 0;
        self.current_result = Some(result);
        self.current_arena = Some(arena);
        self.position = 0;
        self.state = Some(new_state);
        Ok(has_rows)
    }

    /// Get the current result and arena for row decoding.
    pub fn current(
        &self,
    ) -> Option<(
        &bsql_driver_sqlite::conn::QueryResult,
        &bsql_arena::Arena,
        usize,
    )> {
        match (&self.current_result, &self.current_arena) {
            (Some(result), Some(arena)) if self.position < result.row_count => {
                Some((result, arena, self.position))
            }
            _ => None,
        }
    }

    /// Advance to the next row in the current chunk.
    pub fn advance(&mut self) {
        self.position += 1;
    }

    /// Whether there are more rows in the current chunk.
    pub fn has_current_row(&self) -> bool {
        self.current_result
            .as_ref()
            .is_some_and(|r| self.position < r.row_count)
    }

    /// Whether all rows have been consumed (no more chunks).
    pub fn is_finished(&self) -> bool {
        !self.has_current_row() && self.state.as_ref().is_none_or(|s| s.inner.finished)
    }
}

impl Drop for SqliteStreamingQuery {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            if !state.inner.finished {
                self.pool.streaming_drop(state, self.reader_idx);
            }
        }
    }
}

/// Delegate to shared savepoint name validator.
fn validate_savepoint_name(name: &str) -> BsqlResult<()> {
    crate::util::validate_savepoint_name(name)
}

impl Clone for SqlitePool {
    fn clone(&self) -> Self {
        SqlitePool {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for SqlitePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePool")
            .field("reader_count", &self.inner.reader_count())
            .field("closed", &self.inner.is_closed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        format!("{}/bsql_test_sqlite_pool_{}_{}.db", dir.display(), pid, id)
    }

    // --- SqlitePool::open alias ---

    #[test]
    fn open_is_alias_for_connect() {
        let path = temp_db_path();
        let pool = SqlitePool::open(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        // Verify the pool is usable
        assert_eq!(pool.reader_count(), 4);
        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Transaction tests ---

    #[test]
    fn transaction_commit() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(2)],
        )
        .unwrap();
        tx.commit().unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_rollback() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .unwrap();
        tx.rollback().unwrap();

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, _arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 0);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_savepoint() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .unwrap();

        tx.savepoint("sp1").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(2)],
        )
        .unwrap();

        tx.rollback_to("sp1").unwrap();
        tx.commit().unwrap();

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_drop_auto_rollback() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        {
            let tx = pool.begin().unwrap();
            tx.execute_sql(
                "INSERT INTO t VALUES (?1)",
                crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
                smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
            )
            .unwrap();
            // Drop without commit or rollback
            drop(tx);
        }

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, _arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 0);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Streaming tests ---

    #[test]
    fn streaming_query() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        for i in 0..10 {
            pool.raw_execute(&format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let mut stream = pool
            .query_streaming(sql, hash, smallvec::SmallVec::new(), 3)
            .unwrap();

        // Should have initial rows
        assert!(stream.has_current_row());
        assert!(!stream.is_finished());

        // Read all rows
        let mut total = 0;
        loop {
            if stream.has_current_row() {
                let (result, arena, pos) = stream.current().unwrap();
                let _id = result.get_i64(pos, 0, arena);
                stream.advance();
                total += 1;
            } else if !stream.is_finished() {
                let fetched = stream.fetch_next_chunk().unwrap();
                if !fetched {
                    break;
                }
            } else {
                break;
            }
        }
        assert_eq!(total, 10);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Savepoint validation ---

    #[test]
    fn savepoint_name_validation() {
        assert!(validate_savepoint_name("sp1").is_ok());
        assert!(validate_savepoint_name("_sp").is_ok());
        assert!(validate_savepoint_name("my_savepoint_123").is_ok());

        assert!(validate_savepoint_name("").is_err());
        assert!(validate_savepoint_name("1sp").is_err());
        assert!(validate_savepoint_name("sp-1").is_err());
        assert!(validate_savepoint_name("sp 1").is_err());

        let long = "a".repeat(64);
        assert!(validate_savepoint_name(&long).is_err());
        let max = "a".repeat(63);
        assert!(validate_savepoint_name(&max).is_ok());
    }

    // --- Builder tests ---

    #[test]
    fn builder_requires_path() {
        let result = SqlitePool::builder().build();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("path"), "error should mention path: {err}");
    }

    #[test]
    fn builder_default_reader_count() {
        let b = SqlitePool::builder();
        assert_eq!(b.reader_count, 4);
    }

    #[test]
    fn builder_custom_reader_count() {
        let path = temp_db_path();
        let pool = SqlitePool::builder()
            .path(&path)
            .reader_count(2)
            .build()
            .unwrap();
        assert_eq!(pool.reader_count(), 2);
        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Debug impls ---

    #[test]
    fn sqlite_pool_debug() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        let dbg = format!("{pool:?}");
        assert!(
            dbg.contains("SqlitePool"),
            "Debug should show SqlitePool: {dbg}"
        );
        assert!(
            dbg.contains("reader_count"),
            "Debug should show reader_count: {dbg}"
        );
        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn sqlite_transaction_debug() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        let tx = pool.begin().unwrap();
        let dbg = format!("{tx:?}");
        assert!(
            dbg.contains("SqliteTransaction"),
            "Debug should show SqliteTransaction: {dbg}"
        );
        assert!(
            dbg.contains("finished"),
            "Debug should show finished field: {dbg}"
        );
        tx.rollback().unwrap();
        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Clone ---

    #[test]
    fn sqlite_pool_clone() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        let pool2 = pool.clone();
        assert_eq!(pool.reader_count(), pool2.reader_count());
        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- close / is_closed ---

    #[test]
    fn sqlite_pool_close_and_is_closed() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        assert!(!pool.is_closed());
        pool.close();
        assert!(pool.is_closed());
        let _ = std::fs::remove_file(&path);
    }

    // --- execute_batch ---

    #[test]
    fn execute_batch_multiple() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::rapid_hash_str(sql);

        let v1 = 1i64;
        let v2 = 2i64;
        let v3 = 3i64;
        let params1: &[&dyn bsql_driver_sqlite::codec::SqliteEncode] = &[&v1];
        let params2: &[&dyn bsql_driver_sqlite::codec::SqliteEncode] = &[&v2];
        let params3: &[&dyn bsql_driver_sqlite::codec::SqliteEncode] = &[&v3];

        let total = pool
            .execute_batch(sql, hash, &[params1, params2, params3])
            .unwrap();
        assert_eq!(total, 3);

        let select = "SELECT id FROM t ORDER BY id";
        let select_hash = crate::rapid_hash_str(select);
        let (result, _arena) = pool
            .query_readonly(select, select_hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 3);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- execute_direct ---

    #[test]
    fn execute_direct_insert() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::rapid_hash_str(sql);
        let v: i64 = 42;
        let affected = pool
            .execute_direct(
                sql,
                hash,
                &[&v as &dyn bsql_driver_sqlite::codec::SqliteEncode],
            )
            .unwrap();
        assert_eq!(affected, 1);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Send + Sync assertions ---

    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    #[test]
    fn sqlite_pool_is_send_and_sync() {
        _assert_send::<SqlitePool>();
        _assert_sync::<SqlitePool>();
    }

    // SqliteTransaction is NOT Send (holds Arc to pool with Mutex<SqliteConnection>
    // which is !Send due to raw FFI pointers), but it IS usable from a single thread.
    // We do NOT assert Send/Sync for SqliteTransaction — it is intentionally !Send.

    // --- Nested savepoints (3+ levels) ---

    #[test]
    fn transaction_nested_savepoints_three_levels() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        // Insert row at transaction level
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .unwrap();

        // Level 1 savepoint
        tx.savepoint("sp1").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(2)],
        )
        .unwrap();

        // Level 2 savepoint
        tx.savepoint("sp2").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(3)],
        )
        .unwrap();

        // Level 3 savepoint
        tx.savepoint("sp3").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(4)],
        )
        .unwrap();

        // Rollback to sp2 (undoes sp3 insert and sp2 insert)
        tx.rollback_to("sp2").unwrap();
        tx.commit().unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        // Only rows 1 and 2 should survive (sp2 rollback undoes id=3 and id=4)
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Savepoint with same name twice ---

    #[test]
    fn transaction_savepoint_same_name_twice() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .unwrap();

        tx.savepoint("sp1").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(2)],
        )
        .unwrap();

        // Second savepoint with same name overwrites the first
        tx.savepoint("sp1").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(3)],
        )
        .unwrap();

        // Rolling back to sp1 should undo id=3 but keep id=2
        tx.rollback_to("sp1").unwrap();
        tx.commit().unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Release savepoint ---

    #[test]
    fn transaction_release_savepoint() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        tx.savepoint("sp1").unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .unwrap();

        // Release keeps the changes
        tx.release_savepoint("sp1").unwrap();
        tx.commit().unwrap();

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, _arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 1);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- SqlitePool warmup ---

    #[test]
    fn sqlite_pool_warmup() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        // Warmup should not panic even with valid SQL
        pool.warmup(&["SELECT id FROM t"]);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- SqlitePool for_each ---

    #[test]
    fn sqlite_pool_for_each() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        pool.raw_execute("INSERT INTO t VALUES (1)").unwrap();
        pool.raw_execute("INSERT INTO t VALUES (2)").unwrap();
        pool.raw_execute("INSERT INTO t VALUES (3)").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let mut ids = Vec::new();
        pool.for_each(sql, hash, &[], false, |stmt| {
            let id = stmt.column_int64(0);
            ids.push(id);
            Ok(())
        })
        .unwrap();

        assert_eq!(ids, vec![1, 2, 3]);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- SqlitePool for_each_collect ---

    #[test]
    fn sqlite_pool_for_each_collect() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        pool.raw_execute("INSERT INTO t VALUES (10)").unwrap();
        pool.raw_execute("INSERT INTO t VALUES (20)").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let ids: Vec<i64> = pool
            .for_each_collect(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert_eq!(ids, vec![10, 20]);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- SqlitePool fetch_one_direct ---

    #[test]
    fn sqlite_pool_fetch_one_direct() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        pool.raw_execute("INSERT INTO t VALUES (42)").unwrap();

        let sql = "SELECT id FROM t LIMIT 1";
        let hash = crate::rapid_hash_str(sql);
        let id: i64 = pool
            .fetch_one_direct(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert_eq!(id, 42);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- SqlitePool fetch_optional_direct ---

    #[test]
    fn sqlite_pool_fetch_optional_direct_some() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        pool.raw_execute("INSERT INTO t VALUES (7)").unwrap();

        let sql = "SELECT id FROM t LIMIT 1";
        let hash = crate::rapid_hash_str(sql);
        let id: Option<i64> = pool
            .fetch_optional_direct(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert_eq!(id, Some(7));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn sqlite_pool_fetch_optional_direct_none() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let sql = "SELECT id FROM t LIMIT 1";
        let hash = crate::rapid_hash_str(sql);
        let id: Option<i64> = pool
            .fetch_optional_direct(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert_eq!(id, None);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- SqlitePool fetch_all_direct ---

    #[test]
    fn sqlite_pool_fetch_all_direct() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();
        pool.raw_execute("INSERT INTO t VALUES (1)").unwrap();
        pool.raw_execute("INSERT INTO t VALUES (2)").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let ids: Vec<i64> = pool
            .fetch_all_direct(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert_eq!(ids, vec![1, 2]);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn sqlite_pool_fetch_all_direct_empty() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let ids: Vec<i64> = pool
            .fetch_all_direct(sql, hash, &[], false, |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert!(ids.is_empty());

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Transaction: query within transaction sees uncommitted data ---

    #[test]
    fn transaction_read_own_writes() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(42)],
        )
        .unwrap();

        // Read within transaction should see the uncommitted row
        let (result, arena) = tx
            .query_readwrite(
                "SELECT id FROM t",
                crate::rapid_hash_str("SELECT id FROM t"),
                smallvec::SmallVec::new(),
            )
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        tx.rollback().unwrap();

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Transaction execute_batch within tx ---

    #[test]
    fn transaction_execute_batch() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.raw_execute("CREATE TABLE t (id INTEGER NOT NULL)")
            .unwrap();

        let tx = pool.begin().unwrap();
        let sql = "INSERT INTO t VALUES (?1)";
        let hash = crate::rapid_hash_str(sql);

        let v1 = 1i64;
        let v2 = 2i64;
        let params1: &[&dyn bsql_driver_sqlite::codec::SqliteEncode] = &[&v1];
        let params2: &[&dyn bsql_driver_sqlite::codec::SqliteEncode] = &[&v2];

        let total = tx.execute_batch(sql, hash, &[params1, params2]).unwrap();
        assert_eq!(total, 2);

        tx.commit().unwrap();

        let select = "SELECT id FROM t ORDER BY id";
        let select_hash = crate::rapid_hash_str(select);
        let (result, _) = pool
            .query_readonly(select, select_hash, smallvec::SmallVec::new())
            .unwrap();
        assert_eq!(result.len(), 2);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }
}
