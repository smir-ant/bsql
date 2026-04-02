//! SQLite connection pool — async wrapper over `bsql_driver_sqlite::pool::SqlitePool`.
//!
//! The driver pool uses dedicated OS threads and crossbeam channels. This
//! wrapper provides an async-compatible API by spawning blocking tasks on
//! tokio's blocking thread pool.

use std::sync::Arc;

use crate::error::{BsqlError, BsqlResult};

/// A SQLite connection pool.
///
/// Wraps `bsql_driver_sqlite::pool::SqlitePool` with bsql error types
/// and an async-compatible API.
///
/// The driver pool is `Send + Sync` (asserted in bsql-driver-sqlite)
/// because it communicates with its threads via crossbeam channels and
/// atomic flags only.
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

    /// Set the number of reader threads. Default: 4.
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
    /// Open a SQLite pool with default settings (4 reader threads).
    pub fn connect(path: &str) -> BsqlResult<Self> {
        let inner =
            bsql_driver_sqlite::pool::SqlitePool::connect(path).map_err(BsqlError::from_sqlite)?;
        Ok(SqlitePool {
            inner: Arc::new(inner),
        })
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> SqlitePoolBuilder {
        SqlitePoolBuilder {
            path: None,
            reader_count: 4,
        }
    }

    /// Execute a read-only query via the async wrapper.
    ///
    /// Routes to a reader thread in the pool. Returns the `QueryResult`
    /// and its associated `Arena`.
    pub async fn query_readonly(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.query_readonly(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a read-write query via the async wrapper.
    pub async fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.query_readwrite(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a write statement (INSERT/UPDATE/DELETE), return affected row count.
    pub async fn execute_sql(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<u64> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.execute(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a simple SQL statement on the writer (PRAGMA, DDL).
    pub async fn simple_exec(&self, sql: &str) -> BsqlResult<()> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || pool.simple_exec(&sql).map_err(BsqlError::from_sqlite))
            .await
            .map_err(|e| {
                BsqlError::Query(crate::error::QueryError {
                    message: format!("SQLite task panicked: {e}").into(),
                    pg_code: None,
                    source: None,
                })
            })?
    }

    /// Begin a transaction on the writer connection.
    ///
    /// Returns a `SqliteTransaction` that must be committed or rolled back.
    /// If dropped without committing, the transaction is automatically rolled back.
    pub async fn begin(&self) -> BsqlResult<SqliteTransaction> {
        let pool = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            pool.begin_transaction().map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })??;

        Ok(SqliteTransaction {
            pool: Arc::clone(&self.inner),
            finished: false,
        })
    }

    /// Execute a read-only streaming query.
    ///
    /// Returns the first chunk and a `SqliteStreamingQuery` to continue.
    pub async fn query_streaming(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
        chunk_size: usize,
    ) -> BsqlResult<SqliteStreamingQuery> {
        let pool = Arc::clone(&self.inner);
        let sql = sql.to_owned();
        let (first_result, first_arena, state, reader_idx) =
            tokio::task::spawn_blocking(move || {
                let (result, arena, state, idx) = pool
                    .query_streaming(&sql, sql_hash, params, chunk_size)
                    .map_err(BsqlError::from_sqlite)?;
                Ok::<_, BsqlError>((result, arena, state, idx))
            })
            .await
            .map_err(|e| {
                BsqlError::Query(crate::error::QueryError {
                    message: format!("SQLite task panicked: {e}").into(),
                    pg_code: None,
                    source: None,
                })
            })??;

        Ok(SqliteStreamingQuery {
            pool: Arc::clone(&self.inner),
            state: Some(state),
            current_result: Some(first_result),
            current_arena: Some(first_arena),
            position: 0,
            reader_idx,
        })
    }

    /// Pre-prepare statements on all threads (warmup).
    pub fn warmup(&self, sqls: &[&str]) {
        self.inner.warmup(sqls);
    }

    /// Number of reader threads.
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
/// single writer thread.
pub struct SqliteTransaction {
    pool: Arc<bsql_driver_sqlite::pool::SqlitePool>,
    finished: bool,
}

impl SqliteTransaction {
    /// Commit the transaction.
    ///
    /// Consumes `self` — the transaction cannot be used after commit.
    pub async fn commit(mut self) -> BsqlResult<()> {
        self.finished = true;
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn_blocking(move || {
            pool.commit_transaction().map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Explicitly roll back the transaction.
    ///
    /// Consumes `self` — the transaction cannot be used after rollback.
    pub async fn rollback(mut self) -> BsqlResult<()> {
        self.finished = true;
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn_blocking(move || {
            pool.rollback_transaction().map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Create a savepoint within the transaction.
    ///
    /// The `name` must be a valid SQL identifier.
    pub async fn savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let pool = Arc::clone(&self.pool);
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || pool.savepoint(&name).map_err(BsqlError::from_sqlite))
            .await
            .map_err(|e| {
                BsqlError::Query(crate::error::QueryError {
                    message: format!("SQLite task panicked: {e}").into(),
                    pg_code: None,
                    source: None,
                })
            })?
    }

    /// Release (destroy) a savepoint, keeping its effects.
    pub async fn release_savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let pool = Arc::clone(&self.pool);
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.release_savepoint(&name)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Roll back to a savepoint.
    pub async fn rollback_to(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let pool = Arc::clone(&self.pool);
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || pool.rollback_to(&name).map_err(BsqlError::from_sqlite))
            .await
            .map_err(|e| {
                BsqlError::Query(crate::error::QueryError {
                    message: format!("SQLite task panicked: {e}").into(),
                    pg_code: None,
                    source: None,
                })
            })?
    }

    /// Execute a write query within the transaction.
    pub async fn execute_sql(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<u64> {
        let pool = Arc::clone(&self.pool);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.execute(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
    }

    /// Execute a query within the transaction (writer thread).
    pub async fn query_readwrite(
        &self,
        sql: &str,
        sql_hash: u64,
        params: smallvec::SmallVec<[bsql_driver_sqlite::pool::ParamValue; 8]>,
    ) -> BsqlResult<(bsql_driver_sqlite::conn::QueryResult, bsql_arena::Arena)> {
        let pool = Arc::clone(&self.pool);
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            pool.query_readwrite(&sql, sql_hash, params)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })?
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
            eprintln!(
                "bsql: SqliteTransaction dropped without commit() or rollback() — \
                 rolling back automatically."
            );
            // Best-effort rollback. Cannot block in Drop, so use a blocking
            // call directly on the pool (which goes to the writer thread).
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
    pub async fn fetch_next_chunk(&mut self) -> BsqlResult<bool> {
        let state = match self.state.take() {
            Some(s) if !s.inner.finished => s,
            Some(s) => {
                self.state = Some(s);
                return Ok(false);
            }
            None => return Ok(false),
        };

        let pool = Arc::clone(&self.pool);
        let reader_idx = self.reader_idx;
        let (result, arena, new_state) = tokio::task::spawn_blocking(move || {
            pool.streaming_next(state, reader_idx)
                .map_err(BsqlError::from_sqlite)
        })
        .await
        .map_err(|e| {
            BsqlError::Query(crate::error::QueryError {
                message: format!("SQLite task panicked: {e}").into(),
                pg_code: None,
                source: None,
            })
        })??;

        let has_rows = result.row_count > 0;
        self.current_result = Some(result);
        self.current_arena = Some(arena);
        self.position = 0;
        self.state = Some(new_state);
        Ok(has_rows)
    }

    /// Get the current result and arena for row decoding.
    ///
    /// Returns `None` if no chunk is loaded or all rows in the current chunk
    /// have been consumed.
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

    // --- Transaction tests ---

    #[tokio::test]
    async fn transaction_commit() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER NOT NULL)")
            .await
            .unwrap();

        let tx = pool.begin().await.unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .await
        .unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(2)],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn transaction_rollback() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER NOT NULL)")
            .await
            .unwrap();

        let tx = pool.begin().await.unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, _arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .await
            .unwrap();
        assert_eq!(result.len(), 0);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn transaction_savepoint() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER NOT NULL)")
            .await
            .unwrap();

        let tx = pool.begin().await.unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
        )
        .await
        .unwrap();

        tx.savepoint("sp1").await.unwrap();
        tx.execute_sql(
            "INSERT INTO t VALUES (?1)",
            crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
            smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(2)],
        )
        .await
        .unwrap();

        tx.rollback_to("sp1").await.unwrap();
        tx.commit().await.unwrap();

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn transaction_drop_auto_rollback() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER NOT NULL)")
            .await
            .unwrap();

        {
            let tx = pool.begin().await.unwrap();
            tx.execute_sql(
                "INSERT INTO t VALUES (?1)",
                crate::rapid_hash_str("INSERT INTO t VALUES (?1)"),
                smallvec::smallvec![bsql_driver_sqlite::pool::ParamValue::Int(1)],
            )
            .await
            .unwrap();
            // Drop without commit or rollback
            drop(tx);
        }

        let sql = "SELECT id FROM t";
        let hash = crate::rapid_hash_str(sql);
        let (result, _arena) = pool
            .query_readonly(sql, hash, smallvec::SmallVec::new())
            .await
            .unwrap();
        assert_eq!(result.len(), 0);

        pool.close();
        let _ = std::fs::remove_file(&path);
    }

    // --- Streaming tests ---

    #[tokio::test]
    async fn streaming_query() {
        let path = temp_db_path();
        let pool = SqlitePool::connect(&path).unwrap();
        pool.simple_exec("CREATE TABLE t (id INTEGER NOT NULL)")
            .await
            .unwrap();
        for i in 0..10 {
            pool.simple_exec(&format!("INSERT INTO t VALUES ({i})"))
                .await
                .unwrap();
        }

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = crate::rapid_hash_str(sql);
        let mut stream = pool
            .query_streaming(sql, hash, smallvec::SmallVec::new(), 3)
            .await
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
                let fetched = stream.fetch_next_chunk().await.unwrap();
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
}
