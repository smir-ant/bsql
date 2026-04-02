//! SQLite connection — open, prepare, step, column decode, statement cache.
//!
//! `SqliteConnection` owns a [`DbHandle`] and implements statement caching
//! with an identity-hashed `HashMap` (same pattern as the PG driver). Statements are
//! cached by `rapidhash` of the SQL text. On first use, the statement is prepared; on
//! subsequent uses, it is reused after `sqlite3_reset`.
//!
//! All row data is copied into an `Arena` during the step loop, making the result
//! independent of the SQLite statement lifetime.
//!
//! This module contains **zero** `unsafe` code — all FFI interaction goes through
//! the safe [`DbHandle`] and [`StmtHandle`] wrapper types in [`crate::ffi`].

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use bsql_arena::Arena;
use libsqlite3_sys as raw;
use rapidhash::quality::RapidHasher;

use crate::SqliteError;
use crate::codec::SqliteEncode;
use crate::ffi::{DbHandle, StepResult, StmtHandle};

// --- Identity hasher (same pattern as PG driver) ---

/// Identity hasher for pre-hashed u64 keys. Avoids SipHash overhead
/// on keys that are already well-distributed rapidhash values.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
    #[inline]
    fn write(&mut self, _: &[u8]) {
        debug_assert!(false, "IdentityHasher only supports u64 keys");
        self.0 = 0;
    }
}

type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;
type StmtCache = HashMap<u64, CachedStmt, IdentityBuildHasher>;

/// Cached prepared statement with pre-computed metadata.
struct CachedStmt {
    handle: StmtHandle,
    /// Column count cached at prepare time. Used for pre-allocation.
    #[allow(dead_code)]
    column_count: i32,
}

/// Hash a SQL string with rapidhash. Used as the statement cache key.
///
/// # Example
///
/// ```
/// let hash = bsql_driver_sqlite::conn::hash_sql("SELECT 1");
/// assert_ne!(hash, 0);
/// ```
pub fn hash_sql(sql: &str) -> u64 {
    use std::hash::Hash;
    let mut hasher = RapidHasher::default();
    sql.hash(&mut hasher);
    hasher.finish()
}

// --- SqliteConnection ---

/// A single SQLite database connection with a statement cache.
///
/// `SqliteConnection` is **not** `Send` or `Sync` — the underlying `DbHandle`
/// wraps a raw `sqlite3*` handle opened with `SQLITE_OPEN_NOMUTEX` (no internal
/// mutexing). The pool module handles thread affinity by opening connections on
/// dedicated threads.
pub struct SqliteConnection {
    db: DbHandle,
    stmts: StmtCache,
}

impl SqliteConnection {
    /// Open a read-write database connection with optimal PRAGMAs.
    ///
    /// Sets WAL mode, synchronous=NORMAL, 256MB mmap, 64MB cache, and
    /// busy_timeout=0 (fail-fast per CREDO #17).
    pub fn open(path: &str) -> Result<Self, SqliteError> {
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(path, flags)?;

        db.exec("PRAGMA journal_mode = WAL")?;
        db.exec("PRAGMA synchronous = NORMAL")?;
        db.exec("PRAGMA mmap_size = 268435456")?; // 256MB
        db.exec("PRAGMA cache_size = -64000")?; // 64MB
        db.exec("PRAGMA busy_timeout = 0")?; // fail-fast (CREDO #17)
        db.exec("PRAGMA temp_store = MEMORY")?;
        db.exec("PRAGMA foreign_keys = ON")?;

        Ok(Self {
            db,
            stmts: StmtCache::default(),
        })
    }

    /// Open a read-only database connection.
    ///
    /// Used by reader threads in the pool. Does not set journal_mode (only
    /// the writer sets that).
    pub fn open_readonly(path: &str) -> Result<Self, SqliteError> {
        let flags = raw::SQLITE_OPEN_READONLY | raw::SQLITE_OPEN_NOMUTEX;
        let db = DbHandle::open(path, flags)?;

        db.exec("PRAGMA synchronous = NORMAL")?;
        db.exec("PRAGMA mmap_size = 268435456")?;
        db.exec("PRAGMA cache_size = -64000")?;
        db.exec("PRAGMA busy_timeout = 0")?;
        db.exec("PRAGMA temp_store = MEMORY")?;
        db.exec("PRAGMA foreign_keys = ON")?;

        Ok(Self {
            db,
            stmts: StmtCache::default(),
        })
    }

    /// Execute a query and return results in an arena.
    ///
    /// Row data is copied into the arena during the step loop. Integer and float
    /// values are stored as little-endian bytes; text and blob are copied verbatim.
    /// NULL values are indicated by `len == -1` in the column offset array.
    pub fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        arena: &mut Arena,
    ) -> Result<QueryResult, SqliteError> {
        let stmt = self.get_or_prepare(sql, sql_hash)?;

        // Bind parameters
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }

        // Step loop — read all rows into arena
        let col_count = stmt.column_count() as usize;
        let mut col_offsets: Vec<(usize, i32)> = Vec::with_capacity(col_count * 8);
        let mut row_count: usize = 0;

        loop {
            match stmt.step()? {
                StepResult::Done => break,
                StepResult::Row => {
                    for col in 0..col_count as i32 {
                        let col_type = stmt.column_type(col);
                        match col_type {
                            raw::SQLITE_NULL => {
                                col_offsets.push((0, -1));
                            }
                            raw::SQLITE_INTEGER => {
                                let val = stmt.column_int64(col);
                                let bytes = val.to_le_bytes();
                                let offset = arena.alloc_copy(&bytes);
                                col_offsets.push((offset, 8));
                            }
                            raw::SQLITE_FLOAT => {
                                let val = stmt.column_double(col);
                                let bytes = val.to_le_bytes();
                                let offset = arena.alloc_copy(&bytes);
                                col_offsets.push((offset, 8));
                            }
                            raw::SQLITE_TEXT => {
                                let data = stmt.column_text(col);
                                match data {
                                    Some(bytes) => {
                                        let offset = arena.alloc_copy(bytes);
                                        col_offsets.push((offset, bytes.len() as i32));
                                    }
                                    None => {
                                        col_offsets.push((0, -1));
                                    }
                                }
                            }
                            _ => {
                                // SQLITE_BLOB or unknown type — treat as blob
                                let data = stmt.column_blob(col);
                                if data.is_empty() {
                                    col_offsets.push((arena.global_offset(), 0));
                                } else {
                                    let offset = arena.alloc_copy(data);
                                    col_offsets.push((offset, data.len() as i32));
                                }
                            }
                        }
                    }
                    row_count += 1;
                }
            }
        }

        // Reset statement for reuse
        stmt.reset()?;

        Ok(QueryResult {
            col_count,
            row_count,
            col_offsets,
        })
    }

    /// Execute a statement (INSERT/UPDATE/DELETE), return affected row count.
    pub fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
    ) -> Result<u64, SqliteError> {
        let stmt = self.get_or_prepare(sql, sql_hash)?;

        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        stmt.step()?;
        stmt.reset()?;
        Ok(self.db.changes())
    }

    /// Execute a query and return a streaming iterator.
    ///
    /// Unlike `query()`, this does not step all rows into an arena upfront.
    /// Instead, it prepares the statement and returns a `StreamingQuery` that
    /// steps `chunk_size` rows at a time into an arena on each call to `next_chunk()`.
    pub fn query_streaming(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        chunk_size: usize,
    ) -> Result<StreamingQuery, SqliteError> {
        let stmt = self.get_or_prepare(sql, sql_hash)?;

        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }

        let col_count = stmt.column_count() as usize;

        Ok(StreamingQuery {
            sql_hash,
            col_count,
            chunk_size,
            finished: false,
        })
    }

    /// Step the streaming query's statement `chunk_size` rows.
    ///
    /// Returns the rows in a `QueryResult` + `Arena`. When all rows are
    /// consumed, returns a result with `row_count == 0`.
    pub fn streaming_next_chunk(
        &mut self,
        streaming: &mut StreamingQuery,
        arena: &mut Arena,
    ) -> Result<QueryResult, SqliteError> {
        if streaming.finished {
            return Ok(QueryResult {
                col_count: streaming.col_count,
                row_count: 0,
                col_offsets: Vec::new(),
            });
        }

        let stmt = self
            .stmts
            .get(&streaming.sql_hash)
            .map(|c| &c.handle)
            .ok_or_else(|| {
                SqliteError::Internal("streaming query: statement not in cache".into())
            })?;

        let col_count = streaming.col_count;
        let mut col_offsets: Vec<(usize, i32)> =
            Vec::with_capacity(col_count * streaming.chunk_size);
        let mut row_count: usize = 0;

        for _ in 0..streaming.chunk_size {
            match stmt.step()? {
                StepResult::Done => {
                    streaming.finished = true;
                    break;
                }
                StepResult::Row => {
                    for col in 0..col_count as i32 {
                        let col_type = stmt.column_type(col);
                        match col_type {
                            raw::SQLITE_NULL => {
                                col_offsets.push((0, -1));
                            }
                            raw::SQLITE_INTEGER => {
                                let val = stmt.column_int64(col);
                                let bytes = val.to_le_bytes();
                                let offset = arena.alloc_copy(&bytes);
                                col_offsets.push((offset, 8));
                            }
                            raw::SQLITE_FLOAT => {
                                let val = stmt.column_double(col);
                                let bytes = val.to_le_bytes();
                                let offset = arena.alloc_copy(&bytes);
                                col_offsets.push((offset, 8));
                            }
                            raw::SQLITE_TEXT => {
                                let data = stmt.column_text(col);
                                match data {
                                    Some(bytes) => {
                                        let offset = arena.alloc_copy(bytes);
                                        col_offsets.push((offset, bytes.len() as i32));
                                    }
                                    None => {
                                        col_offsets.push((0, -1));
                                    }
                                }
                            }
                            _ => {
                                let data = stmt.column_blob(col);
                                if data.is_empty() {
                                    col_offsets.push((arena.global_offset(), 0));
                                } else {
                                    let offset = arena.alloc_copy(data);
                                    col_offsets.push((offset, data.len() as i32));
                                }
                            }
                        }
                    }
                    row_count += 1;
                }
            }
        }

        if streaming.finished {
            // Reset statement for reuse now that we're done
            let stmt = self
                .stmts
                .get(&streaming.sql_hash)
                .map(|c| &c.handle)
                .ok_or_else(|| {
                    SqliteError::Internal("streaming query: statement not in cache".into())
                })?;
            stmt.reset()?;
        }

        Ok(QueryResult {
            col_count,
            row_count,
            col_offsets,
        })
    }

    /// Reset a streaming query's statement without stepping to completion.
    ///
    /// Called when a `StreamingQuery` is dropped before all rows are consumed.
    pub fn reset_streaming(&mut self, streaming: &StreamingQuery) {
        if let Some(cached) = self.stmts.get(&streaming.sql_hash) {
            let _ = cached.handle.reset();
        }
    }

    /// Prepare a statement without executing it (cache warmup).
    pub fn prepare_only(&mut self, sql: &str, sql_hash: u64) -> Result<(), SqliteError> {
        self.get_or_prepare(sql, sql_hash)?;
        Ok(())
    }

    /// Execute a simple SQL string (PRAGMA, DDL). No parameters, no results.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        self.db.exec(sql)
    }

    /// Validate a SQL statement and extract column metadata.
    ///
    /// Used by the proc macro for compile-time validation. Prepares the
    /// statement, extracts column info (name, declared type, source table/
    /// column for nullability), then drops the statement.
    ///
    /// Returns `(column_info_vec, param_count)`.
    pub fn compile_validate(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<CompileColumnInfo>, usize), SqliteError> {
        let stmt = self.db.prepare(sql)?;

        let col_count = stmt.column_count() as usize;
        let param_count = stmt.bind_parameter_count() as usize;

        let mut columns = Vec::with_capacity(col_count);
        for i in 0..col_count as i32 {
            let name = stmt.column_name(i).unwrap_or("?column?").to_owned();
            let declared_type = stmt.column_decltype(i).map(|s| s.to_owned());
            let table_name = stmt.column_table_name(i).map(|s| s.to_owned());
            let origin_name = stmt.column_origin_name(i).map(|s| s.to_owned());

            // Resolve nullability from table schema
            let is_nullable = match (&table_name, &origin_name) {
                (Some(table), Some(column)) => self.resolve_column_nullable(table, column),
                _ => true, // expression/aggregate -> nullable (safe default)
            };

            columns.push(CompileColumnInfo {
                name,
                declared_type,
                table_name,
                origin_name,
                is_nullable,
            });
        }

        // stmt is finalized on drop

        Ok((columns, param_count))
    }

    /// Query PRAGMA table_info to determine if a column is nullable.
    fn resolve_column_nullable(&mut self, table: &str, column: &str) -> bool {
        let pragma_sql = format!("PRAGMA table_info(\"{}\")", table);
        let pragma_hash = hash_sql(&pragma_sql);
        let mut arena = Arena::new();

        match self.query(&pragma_sql, pragma_hash, &[], &mut arena) {
            Ok(result) => {
                // PRAGMA table_info columns: cid(0), name(1), type(2), notnull(3), dflt_value(4), pk(5)
                for row in 0..result.row_count {
                    if let Some(col_name) = result.get_str(row, 1, &arena) {
                        if col_name == column {
                            // notnull flag: 1 = NOT NULL, 0 = nullable
                            return result.get_i64(row, 3, &arena) != Some(1);
                        }
                    }
                }
                true // column not found -> assume nullable
            }
            Err(_) => true, // error -> assume nullable (safe)
        }
    }

    /// Get a cached statement or prepare a new one.
    fn get_or_prepare(&mut self, sql: &str, sql_hash: u64) -> Result<&StmtHandle, SqliteError> {
        if !self.stmts.contains_key(&sql_hash) {
            let handle = self.db.prepare(sql)?;
            let col_count = handle.column_count();
            self.stmts.insert(
                sql_hash,
                CachedStmt {
                    handle,
                    column_count: col_count,
                },
            );
        }
        Ok(&self.stmts.get(&sql_hash).unwrap().handle)
    }
}

// No manual Drop needed — `DbHandle` and `StmtHandle` handle cleanup via
// their own Drop impls. The `StmtCache` entries (each containing a `StmtHandle`)
// are dropped before `DbHandle`, which is correct because Rust drops struct
// fields in declaration order.

// --- QueryResult ---

/// Column metadata extracted during compile-time validation.
///
/// Used by the proc macro to generate typed Rust code for SQLite queries.
#[derive(Debug, Clone)]
pub struct CompileColumnInfo {
    /// Column name (or alias) in the result set.
    pub name: String,
    /// Declared type from CREATE TABLE (e.g. "INTEGER", "TEXT").
    /// `None` for expressions without a declared type.
    pub declared_type: Option<String>,
    /// Source table name, if this column comes from a table column.
    pub table_name: Option<String>,
    /// Origin column name in the source table.
    pub origin_name: Option<String>,
    /// Whether this column can be NULL.
    pub is_nullable: bool,
}

/// State for a streaming query. Tracks position across `next_chunk()` calls.
///
/// Created by [`SqliteConnection::query_streaming`]. The statement remains
/// bound and positioned between chunks — only `chunk_size` rows are stepped
/// per call to [`SqliteConnection::streaming_next_chunk`].
pub struct StreamingQuery {
    /// Hash of the SQL text, used to look up the cached statement.
    pub sql_hash: u64,
    /// Column count per row.
    pub col_count: usize,
    /// Rows per chunk.
    pub chunk_size: usize,
    /// Whether `sqlite3_step` returned DONE.
    pub finished: bool,
}

/// Result of a query execution. Row data lives in the associated `Arena`.
///
/// Column data is addressed by `(offset, length)` pairs into the arena.
/// A length of `-1` indicates a SQL NULL value.
pub struct QueryResult {
    /// Number of columns per row.
    pub col_count: usize,
    /// Number of rows returned.
    pub row_count: usize,
    /// Flat array of `(arena_offset, byte_length)` for every cell.
    /// Layout: `[row0_col0, row0_col1, ..., row1_col0, ...]`.
    /// A `byte_length` of `-1` indicates NULL.
    pub col_offsets: Vec<(usize, i32)>,
}

impl QueryResult {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Get the `(offset, length)` for a specific cell.
    ///
    /// # Panics
    ///
    /// Panics if `row >= row_count` or `col >= col_count`.
    pub fn cell(&self, row: usize, col: usize) -> (usize, i32) {
        debug_assert!(
            row < self.row_count,
            "row {row} out of range (max {})",
            self.row_count
        );
        debug_assert!(
            col < self.col_count,
            "col {col} out of range (max {})",
            self.col_count
        );
        self.col_offsets[row * self.col_count + col]
    }

    /// Check if a cell is NULL.
    pub fn is_null(&self, row: usize, col: usize) -> bool {
        self.cell(row, col).1 == -1
    }

    /// Get the raw bytes for a cell from the arena. Returns `None` for NULL.
    pub fn get_bytes<'a>(&self, row: usize, col: usize, arena: &'a Arena) -> Option<&'a [u8]> {
        let (offset, len) = self.cell(row, col);
        if len == -1 {
            return None;
        }
        Some(arena.get(offset, len as usize))
    }

    /// Get an i64 value from the arena. Returns `None` for NULL.
    pub fn get_i64(&self, row: usize, col: usize, arena: &Arena) -> Option<i64> {
        let bytes = self.get_bytes(row, col, arena)?;
        if bytes.len() == 8 {
            Some(i64::from_le_bytes(bytes.try_into().unwrap()))
        } else {
            None
        }
    }

    /// Get an f64 value from the arena. Returns `None` for NULL.
    pub fn get_f64(&self, row: usize, col: usize, arena: &Arena) -> Option<f64> {
        let bytes = self.get_bytes(row, col, arena)?;
        if bytes.len() == 8 {
            Some(f64::from_le_bytes(bytes.try_into().unwrap()))
        } else {
            None
        }
    }

    /// Get a text value from the arena. Returns `None` for NULL or invalid UTF-8.
    pub fn get_str<'a>(&self, row: usize, col: usize, arena: &'a Arena) -> Option<&'a str> {
        let bytes = self.get_bytes(row, col, arena)?;
        std::str::from_utf8(bytes).ok()
    }

    /// Get a bool value (stored as i64, 0=false, nonzero=true). Returns `None` for NULL.
    pub fn get_bool(&self, row: usize, col: usize, arena: &Arena) -> Option<bool> {
        self.get_i64(row, col, arena).map(|v| v != 0)
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
        format!("{}/bsql_test_conn_{}_{}.db", dir.display(), pid, id)
    }

    // ---- open ----

    #[test]
    fn open_and_query() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.col_count, 2);
        assert!(!result.is_empty());

        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("alice"));
        assert_eq!(result.get_i64(1, 0, &arena), Some(2));
        assert_eq!(result.get_str(1, 1, &arena), Some("bob"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_readonly() {
        let path = temp_db_path();
        {
            let conn = SqliteConnection::open(&path).unwrap();
            conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
            conn.exec("INSERT INTO t VALUES (42)").unwrap();
        }

        let mut conn = SqliteConnection::open_readonly(&path).unwrap();
        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_readonly_nonexistent_file() {
        let result = SqliteConnection::open_readonly("/tmp/bsql_no_such_db_ever.db");
        assert!(result.is_err());
    }

    #[test]
    fn open_sets_wal_mode() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        let mut arena = Arena::new();
        let sql = "PRAGMA journal_mode";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_str(0, 0, &arena), Some("wal"));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_sets_mmap_size() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        let mut arena = Arena::new();
        let sql = "PRAGMA mmap_size";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(268_435_456));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_sets_foreign_keys() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        let mut arena = Arena::new();
        let sql = "PRAGMA foreign_keys";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- query ----

    #[test]
    fn query_with_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t WHERE id = ?1";
        let hash = hash_sql(sql);
        let id: i64 = 2;
        let result = conn.query(sql, hash, &[&id], &mut arena).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result.get_i64(0, 0, &arena), Some(2));
        assert_eq!(result.get_str(0, 1, &arena), Some("bob"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_zero_params_when_sql_expects_some() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id FROM t WHERE id = ?1";
        let hash = hash_sql(sql);
        // Not binding params -- SQLite treats unbound params as NULL
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.len(), 0); // NULL != 1
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_insert_without_returning() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let mut arena = Arena::new();
        let sql = "INSERT INTO t VALUES (1)";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert!(result.is_empty());
        assert_eq!(result.col_count, 0);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_empty_where_false() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id FROM t WHERE 1 = 0";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert!(result.is_empty());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_large_result_set() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..10_000 {
            conn.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.len(), 10_000);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_large_text() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();

        let big = "a".repeat(100_000);
        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        conn.execute(sql_ins, hash_ins, &[&big.as_str()]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_str(0, 0, &arena), Some(big.as_str()));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_large_blob() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val BLOB)").unwrap();

        let big: Vec<u8> = (0..=255).cycle().take(100_000).collect();
        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let blob_ref: &[u8] = &big;
        conn.execute(sql_ins, hash_ins, &[&blob_ref]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_bytes(0, 0, &arena), Some(&big[..]));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_unicode_text() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();

        let texts = [
            "\u{1F600}\u{1F4A9}\u{1F680}", // emoji
            "\u{4e16}\u{754c}",            // CJK
            "\u{0645}\u{0631}\u{062D}",    // Arabic
            "\u{1F468}\u{200D}\u{1F469}",  // ZWJ
        ];
        for t in &texts {
            let sql_ins = "INSERT INTO t VALUES (?1)";
            let hash_ins = hash_sql(sql_ins);
            conn.execute(sql_ins, hash_ins, &[t]).unwrap();
        }

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        for (i, t) in texts.iter().enumerate() {
            assert_eq!(result.get_str(i, 0, &arena), Some(*t));
        }
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- statement cache ----

    #[test]
    fn statement_cache_hit() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        let mut arena = Arena::new();
        conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(conn.stmts.len(), 1);

        arena.reset();
        conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(conn.stmts.len(), 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn statement_cache_miss_different_sql() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'a')").unwrap();

        let sql1 = "SELECT id FROM t";
        let hash1 = hash_sql(sql1);
        let sql2 = "SELECT name FROM t";
        let hash2 = hash_sql(sql2);

        let mut arena = Arena::new();
        conn.query(sql1, hash1, &[], &mut arena).unwrap();
        assert_eq!(conn.stmts.len(), 1);

        arena.reset();
        conn.query(sql2, hash2, &[], &mut arena).unwrap();
        assert_eq!(conn.stmts.len(), 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- execute ----

    #[test]
    fn execute_returns_affected_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'c')").unwrap();

        let sql = "DELETE FROM t WHERE id > ?1";
        let hash = hash_sql(sql);
        let threshold: i64 = 1;
        let affected = conn.execute(sql, hash, &[&threshold]).unwrap();
        assert_eq!(affected, 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_on_select_succeeds() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        // execute() on a SELECT does not error -- it steps the statement once
        // and returns db.changes(), which may reflect the previous DML.
        let affected = conn.execute(sql, hash, &[]).unwrap();
        let _ = affected; // just ensure it does not crash
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_insert_returns_one() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        let val: i64 = 42;
        let affected = conn.execute(sql, hash, &[&val]).unwrap();
        assert_eq!(affected, 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_update() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'old')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'old')").unwrap();

        let sql = "UPDATE t SET val = ?1";
        let hash = hash_sql(sql);
        let new_val: &str = "new";
        let affected = conn.execute(sql, hash, &[&new_val]).unwrap();
        assert_eq!(affected, 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- null handling ----

    #[test]
    fn null_handling() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, NULL)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result.is_null(0, 0));
        assert!(result.is_null(0, 1));
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), None);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_in_first_column() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a TEXT, b INTEGER, c TEXT)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (NULL, 1, 'x')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT a, b, c FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert!(result.is_null(0, 0));
        assert_eq!(result.get_i64(0, 1, &arena), Some(1));
        assert_eq!(result.get_str(0, 2, &arena), Some("x"));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_in_middle_column() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (1, NULL, 3)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT a, b, c FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert!(result.is_null(0, 1));
        assert_eq!(result.get_i64(0, 2, &arena), Some(3));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn all_columns_null() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a TEXT, b INTEGER, c REAL)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (NULL, NULL, NULL)")
            .unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT a, b, c FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        for col in 0..3 {
            assert!(result.is_null(0, col));
        }
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- types ----

    #[test]
    fn float_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();
        conn.exec("INSERT INTO t VALUES (3.14)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        let val = result.get_f64(0, 0, &arena).unwrap();
        assert!((val - 3.14).abs() < f64::EPSILON);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn blob_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (data BLOB)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let blob: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        conn.execute(sql_ins, hash_ins, &[&blob]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT data FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(
            result.get_bytes(0, 0, &arena),
            Some(&[0xDE, 0xAD, 0xBE, 0xEF][..])
        );
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bool_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (flag INTEGER)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let flag = true;
        conn.execute(sql_ins, hash_ins, &[&flag]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT flag FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_bool(0, 0, &arena), Some(true));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bool_false_value() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (flag INTEGER)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let flag = false;
        conn.execute(sql_ins, hash_ins, &[&flag]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT flag FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_bool(0, 0, &arena), Some(false));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bool_nonzero_is_true() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (flag INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (42)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT flag FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_bool(0, 0, &arena), Some(true));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_string_vs_null() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let empty: &str = "";
        conn.execute(sql_ins, hash_ins, &[&empty]).unwrap();
        let none: Option<&str> = None;
        conn.execute(sql_ins, hash_ins, &[&none]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        // Row 0: empty string
        assert!(!result.is_null(0, 0));
        assert_eq!(result.get_str(0, 0, &arena), Some(""));
        // Row 1: NULL
        assert!(result.is_null(1, 0));
        assert_eq!(result.get_str(1, 0, &arena), None);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_blob_vs_null() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val BLOB)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let empty_blob: &[u8] = &[];
        conn.execute(sql_ins, hash_ins, &[&empty_blob]).unwrap();
        let none: Option<Vec<u8>> = None;
        conn.execute(sql_ins, hash_ins, &[&none]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        // Row 0: empty blob (not null, length 0)
        assert!(!result.is_null(0, 0));
        assert_eq!(result.get_bytes(0, 0, &arena), Some(&[][..]));
        // Row 1: NULL
        assert!(result.is_null(1, 0));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- boundary values ----

    #[test]
    fn i64_boundary_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        for &v in &[i64::MIN, -1, 0, 1, i64::MAX] {
            conn.execute(sql_ins, hash_ins, &[&v]).unwrap();
        }

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(i64::MIN));
        assert_eq!(result.get_i64(1, 0, &arena), Some(-1));
        assert_eq!(result.get_i64(2, 0, &arena), Some(0));
        assert_eq!(result.get_i64(3, 0, &arena), Some(1));
        assert_eq!(result.get_i64(4, 0, &arena), Some(i64::MAX));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn f64_special_values() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        let infinity: f64 = f64::INFINITY;
        conn.execute(sql_ins, hash_ins, &[&infinity]).unwrap();
        let neg_inf: f64 = f64::NEG_INFINITY;
        conn.execute(sql_ins, hash_ins, &[&neg_inf]).unwrap();
        let zero: f64 = 0.0;
        conn.execute(sql_ins, hash_ins, &[&zero]).unwrap();
        let neg_zero: f64 = -0.0;
        conn.execute(sql_ins, hash_ins, &[&neg_zero]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t ORDER BY rowid";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_f64(0, 0, &arena), Some(f64::INFINITY));
        assert_eq!(result.get_f64(1, 0, &arena), Some(f64::NEG_INFINITY));
        assert_eq!(result.get_f64(2, 0, &arena), Some(0.0));
        assert_eq!(result.get_f64(3, 0, &arena), Some(0.0)); // -0.0 stored as 0.0
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- cache / prepare_only ----

    #[test]
    fn prepare_only_warmup() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        conn.prepare_only(sql, hash).unwrap();
        assert_eq!(conn.stmts.len(), 1);

        let mut arena = Arena::new();
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(conn.stmts.len(), 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_only_multiple_statements() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql1 = "SELECT id FROM t";
        let sql2 = "SELECT name FROM t";
        conn.prepare_only(sql1, hash_sql(sql1)).unwrap();
        conn.prepare_only(sql2, hash_sql(sql2)).unwrap();
        assert_eq!(conn.stmts.len(), 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- exec / DDL ----

    #[test]
    fn exec_ddl() {
        let path = temp_db_path();
        let conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t1 (id INTEGER)").unwrap();
        conn.exec("CREATE TABLE t2 (id INTEGER)").unwrap();
        conn.exec("DROP TABLE t1").unwrap();
        conn.exec("INSERT INTO t2 VALUES (1)").unwrap();
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_transaction() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("BEGIN").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();
        conn.exec("INSERT INTO t VALUES (2)").unwrap();
        conn.exec("COMMIT").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT COUNT(*) FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(2));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_transaction_rollback() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("BEGIN").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();
        conn.exec("ROLLBACK").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT COUNT(*) FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(0));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- empty result ----

    #[test]
    fn empty_result_set() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- hash_sql ----

    #[test]
    fn hash_sql_deterministic() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 1");
        let h3 = hash_sql("SELECT 2");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn hash_sql_case_sensitive() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("select 1");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_sql_whitespace_matters() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT  1");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_sql_empty() {
        let _h = hash_sql("");
        // Just ensure no panic.
    }

    // ---- QueryResult accessors ----

    #[test]
    fn query_result_cell_accessor() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER, b TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (10, 'x')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT a, b FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        let (_, len) = result.cell(0, 0);
        assert_eq!(len, 8);
        let (_, len) = result.cell(0, 1);
        assert_eq!(len, 1);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_result_get_bytes_null() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (NULL)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_bytes(0, 0, &arena), None);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_result_get_i64_wrong_length() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES ('abc')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        // Text data is 3 bytes, get_i64 needs 8
        assert_eq!(result.get_i64(0, 0, &arena), None);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_result_get_f64_wrong_length() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES ('abc')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_f64(0, 0, &arena), None);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_result_get_bool_null() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (NULL)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_bool(0, 0, &arena), None);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- multiple params ----

    #[test]
    fn multiple_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
            .unwrap();

        let sql_ins = "INSERT INTO t VALUES (?1, ?2, ?3)";
        let hash_ins = hash_sql(sql_ins);
        let a: i64 = 7;
        let b: &str = "hello";
        let c: f64 = 2.718;
        conn.execute(sql_ins, hash_ins, &[&a, &b, &c]).unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT a, b, c FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();

        assert_eq!(result.get_i64(0, 0, &arena), Some(7));
        assert_eq!(result.get_str(0, 1, &arena), Some("hello"));
        let c_val = result.get_f64(0, 2, &arena).unwrap();
        assert!((c_val - 2.718).abs() < f64::EPSILON);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- multiple queries on same connection ----

    #[test]
    fn multiple_queries_same_connection() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t1 (id INTEGER)").unwrap();
        conn.exec("CREATE TABLE t2 (name TEXT)").unwrap();
        conn.exec("INSERT INTO t1 VALUES (1)").unwrap();
        conn.exec("INSERT INTO t2 VALUES ('a')").unwrap();

        let mut arena = Arena::new();
        let sql1 = "SELECT id FROM t1";
        let hash1 = hash_sql(sql1);
        let r1 = conn.query(sql1, hash1, &[], &mut arena).unwrap();
        assert_eq!(r1.get_i64(0, 0, &arena), Some(1));

        arena.reset();
        let sql2 = "SELECT name FROM t2";
        let hash2 = hash_sql(sql2);
        let r2 = conn.query(sql2, hash2, &[], &mut arena).unwrap();
        assert_eq!(r2.get_str(0, 0, &arena), Some("a"));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- connection usable after error ----

    #[test]
    fn connection_usable_after_error() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let result = conn.exec("INSERT INTO nonexistent VALUES (1)");
        assert!(result.is_err());

        conn.exec("INSERT INTO t VALUES (42)").unwrap();
        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(42));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- compile_validate ----

    #[test]
    fn compile_validate_simple() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE users (id INTEGER NOT NULL, name TEXT, age INTEGER NOT NULL)")
            .unwrap();

        let (cols, param_count) = conn
            .compile_validate("SELECT id, name, age FROM users WHERE id = ?1")
            .unwrap();

        assert_eq!(param_count, 1);
        assert_eq!(cols.len(), 3);

        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].declared_type.as_deref(), Some("INTEGER"));
        assert!(!cols[0].is_nullable); // NOT NULL

        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].declared_type.as_deref(), Some("TEXT"));
        assert!(cols[1].is_nullable); // nullable

        assert_eq!(cols[2].name, "age");
        assert!(!cols[2].is_nullable); // NOT NULL

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compile_validate_expression_columns() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let (cols, _) = conn
            .compile_validate("SELECT val + 1 AS incremented, COUNT(*) AS cnt FROM t")
            .unwrap();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "incremented");
        assert!(cols[0].declared_type.is_none());
        assert!(cols[0].is_nullable); // expression -> nullable by default

        assert_eq!(cols[1].name, "cnt");
        assert!(cols[1].is_nullable); // aggregate -> nullable by default

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compile_validate_no_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let (_, param_count) = conn.compile_validate("SELECT id FROM t").unwrap();
        assert_eq!(param_count, 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compile_validate_invalid_sql() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();

        let result = conn.compile_validate("NOT VALID SQL");
        assert!(result.is_err());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- IdentityHasher ----

    #[test]
    fn identity_hasher_roundtrip() {
        let mut h = IdentityHasher::default();
        h.write_u64(12345);
        assert_eq!(h.finish(), 12345);
    }

    #[test]
    fn identity_hasher_write_bytes_debug() {
        // In debug mode, write(&[u8]) would panic, but we don't
        // call it in practice. In release mode it just sets 0.
        // We verify the default state.
        let h = IdentityHasher::default();
        assert_eq!(h.finish(), 0);
    }

    // --- Streaming ---

    #[test]
    fn streaming_query_basic() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL, name TEXT NOT NULL)")
            .unwrap();
        for i in 0..10 {
            conn.exec(&format!("INSERT INTO t VALUES ({i}, 'row_{i}')"))
                .unwrap();
        }

        let sql = "SELECT id, name FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);
        let mut streaming = conn.query_streaming(sql, sql_hash, &[], 3).unwrap();
        assert!(!streaming.finished);
        assert_eq!(streaming.chunk_size, 3);

        // First chunk: 3 rows
        let mut arena = Arena::new();
        let chunk = conn
            .streaming_next_chunk(&mut streaming, &mut arena)
            .unwrap();
        assert_eq!(chunk.row_count, 3);
        assert_eq!(chunk.get_i64(0, 0, &arena), Some(0));
        assert_eq!(chunk.get_str(0, 1, &arena), Some("row_0"));
        assert_eq!(chunk.get_i64(2, 0, &arena), Some(2));

        // Second chunk: 3 rows
        let mut arena2 = Arena::new();
        let chunk2 = conn
            .streaming_next_chunk(&mut streaming, &mut arena2)
            .unwrap();
        assert_eq!(chunk2.row_count, 3);
        assert_eq!(chunk2.get_i64(0, 0, &arena2), Some(3));

        // Third chunk: 3 rows
        let mut arena3 = Arena::new();
        let chunk3 = conn
            .streaming_next_chunk(&mut streaming, &mut arena3)
            .unwrap();
        assert_eq!(chunk3.row_count, 3);

        // Fourth chunk: 1 row (final)
        let mut arena4 = Arena::new();
        let chunk4 = conn
            .streaming_next_chunk(&mut streaming, &mut arena4)
            .unwrap();
        assert_eq!(chunk4.row_count, 1);
        assert_eq!(chunk4.get_i64(0, 0, &arena4), Some(9));
        assert!(streaming.finished);

        // Fifth chunk: 0 rows (done)
        let mut arena5 = Arena::new();
        let chunk5 = conn
            .streaming_next_chunk(&mut streaming, &mut arena5)
            .unwrap();
        assert_eq!(chunk5.row_count, 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn streaming_query_empty_result() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let sql_hash = hash_sql(sql);
        let mut streaming = conn.query_streaming(sql, sql_hash, &[], 10).unwrap();

        let mut arena = Arena::new();
        let chunk = conn
            .streaming_next_chunk(&mut streaming, &mut arena)
            .unwrap();
        assert_eq!(chunk.row_count, 0);
        assert!(streaming.finished);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn streaming_query_exact_chunk_boundary() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        for i in 0..6 {
            conn.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        let sql = "SELECT id FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);
        let mut streaming = conn.query_streaming(sql, sql_hash, &[], 3).unwrap();

        let mut arena1 = Arena::new();
        let chunk1 = conn
            .streaming_next_chunk(&mut streaming, &mut arena1)
            .unwrap();
        assert_eq!(chunk1.row_count, 3);
        assert!(!streaming.finished);

        let mut arena2 = Arena::new();
        let chunk2 = conn
            .streaming_next_chunk(&mut streaming, &mut arena2)
            .unwrap();
        assert_eq!(chunk2.row_count, 3);
        // After stepping exactly 6 rows with chunk_size=3, the last step may or may not
        // have seen DONE yet depending on SQLite's behavior.

        let mut arena3 = Arena::new();
        let chunk3 = conn
            .streaming_next_chunk(&mut streaming, &mut arena3)
            .unwrap();
        // Either 0 rows (finished) or already finished
        assert!(chunk3.row_count == 0 || streaming.finished);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn streaming_reset_on_drop() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        for i in 0..10 {
            conn.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }

        let sql = "SELECT id FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);

        // Start streaming, read one chunk, then reset
        let mut streaming = conn.query_streaming(sql, sql_hash, &[], 3).unwrap();
        let mut arena = Arena::new();
        let chunk = conn
            .streaming_next_chunk(&mut streaming, &mut arena)
            .unwrap();
        assert_eq!(chunk.row_count, 3);
        assert!(!streaming.finished);

        // Reset the streaming statement
        conn.reset_streaming(&streaming);

        // Should be able to start a new streaming query on the same SQL
        let mut streaming2 = conn.query_streaming(sql, sql_hash, &[], 5).unwrap();
        let mut arena2 = Arena::new();
        let chunk2 = conn
            .streaming_next_chunk(&mut streaming2, &mut arena2)
            .unwrap();
        assert_eq!(chunk2.row_count, 5);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
