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
//! All FFI interaction goes through the safe [`DbHandle`] and [`StmtHandle`]
//! wrapper types in [`crate::ffi`]. No `unsafe` in user-facing APIs — text
//! columns are batch-validated via `String::from_utf8` (SIMD-accelerated in std).

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

/// Cached prepared statement.
struct CachedStmt {
    handle: StmtHandle,
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
/// `SqliteConnection` is `Send` because the underlying `DbHandle` is opened
/// with `SQLITE_OPEN_NOMUTEX` (multi-thread mode). The sync pool wraps each
/// connection in `Mutex<SqliteConnection>` to prevent interleaved step() calls.
pub struct SqliteConnection {
    db: DbHandle,
    stmts: StmtCache,
}

impl SqliteConnection {
    /// Open a read-write database connection with optimal PRAGMAs.
    ///
    /// Sets WAL mode, synchronous=NORMAL, 256MB mmap, 64MB cache, and
    /// busy_timeout=0 (fail-fast per CREDO #17).
    ///
    /// Connections are opened with `SQLITE_OPEN_NOMUTEX` (multi-thread mode).
    /// Thread safety is provided by `Mutex<SqliteConnection>` in the pool —
    /// SQLite's internal locking is redundant and adds ~15-20ns per API call.
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
    /// Used by readers in the pool. Does not set journal_mode (only
    /// the writer sets that).
    ///
    /// Connections are opened with `SQLITE_OPEN_NOMUTEX` (multi-thread mode).
    /// Thread safety is provided by `Mutex<SqliteConnection>` in the pool.
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

    /// Fetch exactly one row, decoding directly from the statement handle.
    ///
    /// This is the zero-overhead path for single-row queries: no arena
    /// allocation, no QueryResult construction, no column copying. The
    /// `decode` closure reads columns directly from the stepped statement.
    ///
    /// Returns an error if the query produces 0 rows.
    #[inline]
    pub fn fetch_one_direct<F, T>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        decode: F,
    ) -> Result<T, SqliteError>
    where
        F: FnOnce(&StmtHandle) -> Result<T, SqliteError>,
    {
        let stmt = self.get_or_prepare(sql, sql_hash)?;
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        match stmt.step()? {
            StepResult::Row => {
                let result = decode(stmt)?;
                stmt.reset()?;
                Ok(result)
            }
            StepResult::Done => {
                stmt.reset()?;
                Err(SqliteError::Internal("expected 1 row, got 0".into()))
            }
        }
    }

    /// Fetch zero or one row, decoding directly from the statement handle.
    ///
    /// Same zero-overhead path as `fetch_one_direct`, but returns `None`
    /// instead of an error when the query produces 0 rows.
    #[inline]
    pub fn fetch_optional_direct<F, T>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        decode: F,
    ) -> Result<Option<T>, SqliteError>
    where
        F: FnOnce(&StmtHandle) -> Result<T, SqliteError>,
    {
        let stmt = self.get_or_prepare(sql, sql_hash)?;
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        match stmt.step()? {
            StepResult::Row => {
                let result = decode(stmt)?;
                stmt.reset()?;
                Ok(Some(result))
            }
            StepResult::Done => {
                stmt.reset()?;
                Ok(None)
            }
        }
    }

    /// Fetch all rows, decoding directly from the statement handle.
    ///
    /// This is the zero-overhead path for multi-row queries: no arena
    /// allocation, no QueryResult construction, no column copying. The
    /// `decode` closure reads columns directly from the stepped statement
    /// for each row and returns a decoded struct that is pushed to the result Vec.
    ///
    /// One tight loop: step -> decode -> push.
    #[inline]
    pub fn fetch_all_direct<F, T>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        decode: F,
    ) -> Result<Vec<T>, SqliteError>
    where
        F: Fn(&StmtHandle) -> Result<T, SqliteError>,
    {
        let stmt = self.get_or_prepare(sql, sql_hash)?;
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        let mut results = Vec::new();
        while let StepResult::Row = stmt.step()? {
            results.push(decode(stmt)?);
        }
        stmt.reset()?;
        Ok(results)
    }

    /// Fetch all rows into an arena-backed result.
    ///
    /// The `decode` closure receives `(&StmtHandle, &mut Arena)` and should
    /// store scalar columns directly and blob columns via `arena.alloc_copy`.
    /// No unsafe is involved — `ArenaRows::new` is fully safe.
    #[inline]
    pub fn fetch_all_arena<F, T>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        decode: F,
    ) -> Result<bsql_arena::ArenaRows<T>, SqliteError>
    where
        F: Fn(&StmtHandle, &mut Arena) -> Result<T, SqliteError>,
    {
        let stmt = self.get_or_prepare(sql, sql_hash)?;
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        let mut arena = bsql_arena::acquire_arena();
        let mut results = Vec::new();
        while let StepResult::Row = stmt.step()? {
            results.push(decode(stmt, &mut arena)?);
        }
        stmt.reset()?;
        Ok(bsql_arena::ArenaRows::new(results, arena))
    }

    /// Process each row in-place via a closure. Zero-copy -- text columns
    /// borrow directly from SQLite's internal buffer. No arena, no allocation.
    ///
    /// The closure receives a `&StmtHandle` for direct column access.
    /// Column pointers are valid only within the closure -- they are invalidated
    /// by the next `step()`.
    #[inline]
    pub fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        mut f: F,
    ) -> Result<(), SqliteError>
    where
        F: FnMut(&StmtHandle) -> Result<(), SqliteError>,
    {
        let stmt = self.get_or_prepare(sql, sql_hash)?;
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        while let StepResult::Row = stmt.step()? {
            f(stmt)?;
        }
        stmt.reset()?;
        Ok(())
    }

    /// Process each row in-place, collecting results into a `Vec`.
    ///
    /// Same zero-copy semantics as [`for_each`](Self::for_each), but the closure
    /// returns a value that is pushed to the result vector.
    #[inline]
    pub fn for_each_collect<F, T>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&dyn SqliteEncode],
        mut f: F,
    ) -> Result<Vec<T>, SqliteError>
    where
        F: FnMut(&StmtHandle) -> Result<T, SqliteError>,
    {
        let stmt = self.get_or_prepare(sql, sql_hash)?;
        stmt.clear_bindings();
        for (i, param) in params.iter().enumerate() {
            param.bind(stmt, (i + 1) as i32)?;
        }
        let mut results = Vec::new();
        while let StepResult::Row = stmt.step()? {
            results.push(f(stmt)?);
        }
        stmt.reset()?;
        Ok(results)
    }

    /// Execute a statement via direct param binding. Returns affected row count.
    ///
    /// Same as `execute()` but takes `&[&dyn SqliteEncode]` directly instead
    /// of requiring the caller to build a `SmallVec<ParamValue>`.
    #[inline]
    pub fn execute_direct(
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

    /// Execute the same statement N times with different parameter sets.
    ///
    /// Prepares the statement once, then for each parameter set: clear bindings,
    /// bind parameters, step, reset. This avoids the per-iteration overhead of
    /// `get_or_prepare` (hash lookup + contains_key check) and holds the
    /// connection for the entire batch without releasing and re-acquiring it.
    ///
    /// Returns the total number of affected rows across all executions.
    pub fn execute_batch(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&dyn SqliteEncode]],
    ) -> Result<u64, SqliteError> {
        if param_sets.is_empty() {
            return Ok(0);
        }
        // Ensure statement is prepared (one hash lookup for the entire batch).
        let _ = self.get_or_prepare(sql, sql_hash)?;
        let mut total_affected = 0u64;
        for params in param_sets {
            // Re-borrow stmt from the cache each iteration. The hash lookup is
            // fast (identity hasher on pre-hashed key) and the borrow is scoped
            // so db.changes() can be called after stmt.reset().
            let stmt = &self.stmts[&sql_hash].handle;
            stmt.clear_bindings();
            for (i, param) in params.iter().enumerate() {
                param.bind(stmt, (i + 1) as i32)?;
            }
            stmt.step()?;
            stmt.reset()?;
            total_affected += self.db.changes();
        }
        Ok(total_affected)
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
    #[inline]
    fn get_or_prepare(&mut self, sql: &str, sql_hash: u64) -> Result<&StmtHandle, SqliteError> {
        // Use the entry API to avoid a redundant lookup. The fallible prepare
        // must happen outside the entry closure (closures cannot return Result
        // with the entry API), so we do a contains_key + insert pattern but
        // avoid the unwrap by using an infallible index after the insert.
        if !self.stmts.contains_key(&sql_hash) {
            let handle = self.db.prepare(sql)?;
            self.stmts.insert(sql_hash, CachedStmt { handle });
        }
        // SAFETY invariant: the key was just inserted above if it was missing,
        // so this index is infallible. We use `expect` with a detailed message
        // instead of `unwrap` to aid debugging if the invariant is ever broken.
        Ok(&self
            .stmts
            .get(&sql_hash)
            .expect("BUG: stmt cache insert-then-get failed — key was just inserted")
            .handle)
    }

    /// Get or prepare a cached statement. Returns a reference to the StmtHandle.
    ///
    /// # Doc-hidden
    ///
    /// Used by generated code from `bsql::query!`. Not part of the public API.
    #[doc(hidden)]
    #[inline]
    pub fn __get_or_prepare(
        &mut self,
        sql: &str,
        sql_hash: u64,
    ) -> Result<&StmtHandle, SqliteError> {
        self.get_or_prepare(sql, sql_hash)
    }

    /// Get number of changes from the last INSERT/UPDATE/DELETE.
    ///
    /// # Doc-hidden
    ///
    /// Used by generated code from `bsql::query!`. Not part of the public API.
    #[doc(hidden)]
    #[inline]
    pub fn __changes(&self) -> u64 {
        self.db.changes()
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
        assert!(
            row < self.row_count,
            "row {row} out of range (max {})",
            self.row_count
        );
        assert!(
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
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(i64::from_le_bytes(arr))
    }

    /// Get an f64 value from the arena. Returns `None` for NULL.
    pub fn get_f64(&self, row: usize, col: usize, arena: &Arena) -> Option<f64> {
        let bytes = self.get_bytes(row, col, arena)?;
        let arr: [u8; 8] = bytes.try_into().ok()?;
        Some(f64::from_le_bytes(arr))
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
#[allow(clippy::approx_constant)]
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

    // ---- fetch_all_direct ----

    #[test]
    fn fetch_all_direct_empty() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let rows: Vec<(i64, String)> = conn
            .fetch_all_direct(sql, hash, &[], |stmt| {
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

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_single_row() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let rows: Vec<(i64, String)> = conn
            .fetch_all_direct(sql, hash, &[], |stmt| {
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

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_100_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, val REAL)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..100 {
            conn.exec(&format!("INSERT INTO t VALUES ({i}, {}.5)", i))
                .unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let sql = "SELECT id, val FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let rows: Vec<(i64, f64)> = conn
            .fetch_all_direct(sql, hash, &[], |stmt| {
                Ok((stmt.column_int64(0), stmt.column_double(1)))
            })
            .unwrap();
        assert_eq!(rows.len(), 100);
        for (i, (id, val)) in rows.iter().enumerate() {
            assert_eq!(*id, i as i64);
            assert!((val - (i as f64 + 0.5)).abs() < f64::EPSILON);
        }

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_10k_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..10_000 {
            conn.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let rows: Vec<i64> = conn
            .fetch_all_direct(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(rows.len(), 10_000);
        assert_eq!(rows[0], 0);
        assert_eq!(rows[9_999], 9_999);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_null_columns() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, NULL)").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();
        conn.exec("INSERT INTO t VALUES (NULL, 'carol')").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let rows: Vec<(Option<i64>, Option<String>)> = conn
            .fetch_all_direct(sql, hash, &[], |stmt| {
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

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], (Some(1), None));
        assert_eq!(rows[1], (Some(2), Some("bob".to_owned())));
        assert_eq!(rows[2], (None, Some("carol".to_owned())));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_mixed_types() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (i INTEGER, r REAL, t TEXT, b BLOB)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (42, 3.14, 'hello', X'DEADBEEF')")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (-1, 0.0, '', X'')")
            .unwrap();

        let sql = "SELECT i, r, t, b FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let rows: Vec<(i64, f64, String, Vec<u8>)> = conn
            .fetch_all_direct(sql, hash, &[], |stmt| {
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

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 42);
        assert!((rows[0].1 - 3.14).abs() < f64::EPSILON);
        assert_eq!(rows[0].2, "hello");
        assert_eq!(rows[0].3, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(rows[1].0, -1);
        assert!((rows[1].1 - 0.0).abs() < f64::EPSILON);
        assert_eq!(rows[1].2, "");
        assert!(rows[1].3.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_with_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'c')").unwrap();

        let sql = "SELECT id, name FROM t WHERE id > ?1 ORDER BY id";
        let hash = hash_sql(sql);
        let min_id: i64 = 1;
        let rows: Vec<(i64, String)> = conn
            .fetch_all_direct(sql, hash, &[&min_id], |stmt| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned())
                    .ok_or_else(|| SqliteError::Internal("decode error".into()))?;
                Ok((id, name))
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (2, "b".to_owned()));
        assert_eq!(rows[1], (3, "c".to_owned()));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_direct_decode_error() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result: Result<Vec<i64>, SqliteError> =
            conn.fetch_all_direct(sql, hash, &[], |_stmt| {
                Err(SqliteError::Internal("forced decode error".into()))
            });
        assert!(result.is_err());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- fetch_all_arena ----

    #[test]
    fn fetch_all_arena_empty() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let rows = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
                let id = stmt.column_int64(0);
                let text = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok((id, text))
            })
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(rows.len(), 0);

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_arena_text_columns() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'charlie')").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);

        struct Row {
            id: i64,
            name: String,
        }

        let rows = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .ok_or_else(|| SqliteError::Internal("null name".into()))
                    .and_then(|b| {
                        std::str::from_utf8(b).map_err(|_| SqliteError::Internal("bad utf8".into()))
                    })?
                    .to_owned();
                Ok(Row { id, name })
            })
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].id, 1);
        assert_eq!(rows[0].name, "alice");
        assert_eq!(rows[1].id, 2);
        assert_eq!(rows[1].name, "bob");
        assert_eq!(rows[2].id, 3);
        assert_eq!(rows[2].name, "charlie");

        // Verify we can iterate
        let names: Vec<&str> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["alice", "bob", "charlie"]);

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_arena_1000_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..1000 {
            conn.exec(&format!("INSERT INTO t VALUES ({i}, 'user_{i}')"))
                .unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);

        struct Row {
            id: i64,
            name: String,
        }

        let rows = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
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
        assert_eq!(rows[0].name, "user_0");
        assert_eq!(rows[999].name, "user_999");
        assert_eq!(rows[500].id, 500);

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_arena_blob_columns() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, data BLOB)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, X'DEADBEEF')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, X'CAFEBABE')").unwrap();

        let sql = "SELECT id, data FROM t ORDER BY id";
        let hash = hash_sql(sql);

        #[allow(dead_code)]
        struct Row {
            id: i64,
            data: Vec<u8>,
        }

        let rows = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
                let id = stmt.column_int64(0);
                let data = stmt.column_blob(1).to_vec();
                Ok(Row { id, data })
            })
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].data, &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(rows[1].data, &[0xCA, 0xFE, 0xBA, 0xBE]);

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_arena_null_text_handling() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'present')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, NULL)").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'also_present')")
            .unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);

        #[allow(dead_code)]
        struct Row {
            id: i64,
            name: Option<String>,
        }

        let rows = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_owned());
                Ok(Row { id, name })
            })
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].name, Some("present".to_owned()));
        assert_eq!(rows[1].name, None);
        assert_eq!(rows[2].name, Some("also_present".to_owned()));

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_arena_decode_error() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result: Result<bsql_arena::ArenaRows<i64>, SqliteError> =
            conn.fetch_all_arena(sql, hash, &[], |_stmt, _arena| {
                Err(SqliteError::Internal("forced error".into()))
            });
        assert!(result.is_err(), "should propagate decode error");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_all_arena_integers_only() {
        // Verify arena path works for integer-only queries too (no text columns)
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 10)").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 20)").unwrap();

        let sql = "SELECT a, b FROM t ORDER BY a";
        let hash = hash_sql(sql);
        let rows = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
                Ok((stmt.column_int64(0), stmt.column_int64(1)))
            })
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], (1, 10));
        assert_eq!(rows[1], (2, 20));
        // Arena should have zero allocated bytes (no text)
        assert_eq!(rows.arena_allocated(), 0);

        drop(rows);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- execute_direct ----

    #[test]
    fn execute_direct_insert() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let hash = hash_sql(sql);
        let id: i64 = 42;
        let name = "alice";
        let affected = conn.execute_direct(sql, hash, &[&id, &name]).unwrap();
        assert_eq!(affected, 1);

        // Verify it was inserted
        let rows: Vec<(i64, String)> = conn
            .fetch_all_direct(
                "SELECT id, name FROM t",
                hash_sql("SELECT id, name FROM t"),
                &[],
                |stmt| {
                    let id = stmt.column_int64(0);
                    let name = stmt
                        .column_text(1)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .map(|s| s.to_owned())
                        .ok_or_else(|| SqliteError::Internal("decode error".into()))?;
                    Ok((id, name))
                },
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], (42, "alice".to_owned()));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_direct_update() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'b')").unwrap();

        let sql = "UPDATE t SET val = ?1";
        let hash = hash_sql(sql);
        let new_val = "new";
        let affected = conn.execute_direct(sql, hash, &[&new_val]).unwrap();
        assert_eq!(affected, 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_direct_no_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();
        conn.exec("INSERT INTO t VALUES (2)").unwrap();

        let sql = "DELETE FROM t";
        let hash = hash_sql(sql);
        let affected = conn.execute_direct(sql, hash, &[]).unwrap();
        assert_eq!(affected, 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- execute_batch ----

    #[test]
    fn execute_batch_inserts_multiple_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let hash = hash_sql(sql);
        let id1: i64 = 1;
        let name1: &str = "alice";
        let id2: i64 = 2;
        let name2: &str = "bob";
        let id3: i64 = 3;
        let name3: &str = "charlie";

        let p1: [&dyn SqliteEncode; 2] = [&id1, &name1];
        let p2: [&dyn SqliteEncode; 2] = [&id2, &name2];
        let p3: [&dyn SqliteEncode; 2] = [&id3, &name3];
        let params: Vec<&[&dyn SqliteEncode]> = vec![&p1, &p2, &p3];
        let total = conn.execute_batch(sql, hash, &params).unwrap();
        assert_eq!(total, 3);

        // Verify all rows were inserted
        let mut arena = Arena::new();
        let sel_sql = "SELECT id, name FROM t ORDER BY id";
        let sel_hash = hash_sql(sel_sql);
        let result = conn.query(sel_sql, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("alice"));
        assert_eq!(result.get_i64(2, 0, &arena), Some(3));
        assert_eq!(result.get_str(2, 1, &arena), Some("charlie"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_batch_empty_returns_zero() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        let total = conn.execute_batch(sql, hash, &[]).unwrap();
        assert_eq!(total, 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_batch_reuses_cached_statement() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);

        // First call prepares the statement
        let v1: i64 = 1;
        let p1: [&dyn SqliteEncode; 1] = [&v1];
        let total = conn.execute_batch(sql, hash, &[&p1]).unwrap();
        assert_eq!(total, 1);

        // Second call reuses the cached statement
        let v2: i64 = 2;
        let v3: i64 = 3;
        let p2: [&dyn SqliteEncode; 1] = [&v2];
        let p3: [&dyn SqliteEncode; 1] = [&v3];
        let total = conn.execute_batch(sql, hash, &[&p2, &p3]).unwrap();
        assert_eq!(total, 2);

        // Verify all 3 rows exist
        let mut arena = Arena::new();
        let sel = "SELECT COUNT(*) FROM t";
        let result = conn.query(sel, hash_sql(sel), &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(3));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn execute_batch_with_update() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'a')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'b')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'c')").unwrap();

        let sql = "UPDATE t SET val = ?1 WHERE id = ?2";
        let hash = hash_sql(sql);
        let new1: &str = "x";
        let id1: i64 = 1;
        let new2: &str = "y";
        let id2: i64 = 2;
        let p1: [&dyn SqliteEncode; 2] = [&new1, &id1];
        let p2: [&dyn SqliteEncode; 2] = [&new2, &id2];
        let params: Vec<&[&dyn SqliteEncode]> = vec![&p1, &p2];
        let total = conn.execute_batch(sql, hash, &params).unwrap();
        assert_eq!(total, 2);

        // Verify
        let rows: Vec<(i64, String)> = conn
            .fetch_all_direct(
                "SELECT id, val FROM t ORDER BY id",
                hash_sql("SELECT id, val FROM t ORDER BY id"),
                &[],
                |stmt| {
                    let id = stmt.column_int64(0);
                    let val = stmt
                        .column_text(1)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .map(|s| s.to_owned())
                        .unwrap_or_default();
                    Ok((id, val))
                },
            )
            .unwrap();
        assert_eq!(rows[0], (1, "x".to_owned()));
        assert_eq!(rows[1], (2, "y".to_owned()));
        assert_eq!(rows[2], (3, "c".to_owned()));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: cell bounds checking ---

    #[test]
    #[should_panic(expected = "row 0 out of range")]
    fn cell_row_out_of_bounds() {
        let result = QueryResult {
            col_count: 1,
            row_count: 0,
            col_offsets: vec![],
        };
        result.cell(0, 0);
    }

    #[test]
    #[should_panic(expected = "col 1 out of range")]
    fn cell_col_out_of_bounds() {
        let result = QueryResult {
            col_count: 1,
            row_count: 1,
            col_offsets: vec![(0, 8)],
        };
        result.cell(0, 1);
    }

    // --- Audit: NULL handling ---

    #[test]
    fn null_values_in_query_result() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, NULL)").unwrap();
        conn.exec("INSERT INTO t VALUES (NULL, 'bob')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id, name FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.len(), 2);

        // Row 0: id=1, name=NULL
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert!(result.is_null(0, 1));
        assert_eq!(result.get_str(0, 1, &arena), None);

        // Row 1: id=NULL, name='bob'
        assert!(result.is_null(1, 0));
        assert_eq!(result.get_i64(1, 0, &arena), None);
        assert_eq!(result.get_str(1, 1, &arena), Some("bob"));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: empty result set ---

    #[test]
    fn audit_empty_result_set() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.len(), 0);
        assert!(result.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: bool value decode ---

    #[test]
    fn query_result_get_bool() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (0)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();
        conn.exec("INSERT INTO t VALUES (42)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        assert_eq!(result.get_bool(0, 0, &arena), Some(false));
        assert_eq!(result.get_bool(1, 0, &arena), Some(true));
        assert_eq!(result.get_bool(2, 0, &arena), Some(true)); // nonzero = true

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: f64 value decode ---

    #[test]
    fn query_result_get_f64() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();
        conn.exec("INSERT INTO t VALUES (3.14)").unwrap();
        conn.exec("INSERT INTO t VALUES (-1.0)").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT val FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        let v0 = result.get_f64(0, 0, &arena).unwrap();
        assert!((v0 - 3.14).abs() < 0.001);
        let v1 = result.get_f64(1, 0, &arena).unwrap();
        assert!((v1 - (-1.0)).abs() < 0.001);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: blob value decode ---

    #[test]
    fn query_result_blob() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (data BLOB)").unwrap();
        conn.exec("INSERT INTO t VALUES (X'DEADBEEF')").unwrap();
        conn.exec("INSERT INTO t VALUES (X'')").unwrap();

        let mut arena = Arena::new();
        let sql = "SELECT data FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena).unwrap();

        let bytes0 = result.get_bytes(0, 0, &arena).unwrap();
        assert_eq!(bytes0, &[0xDE, 0xAD, 0xBE, 0xEF]);

        let bytes1 = result.get_bytes(1, 0, &arena).unwrap();
        assert!(bytes1.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: fetch_all_arena ---

    #[test]
    fn fetch_all_arena_basic() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL, name TEXT NOT NULL)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();

        struct Row {
            id: i64,
            name: String,
        }

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);

        let ar = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok(Row { id, name })
            })
            .unwrap();

        assert_eq!(ar.len(), 2);
        assert_eq!(ar[0].id, 1);
        assert_eq!(ar[0].name, "alice");
        assert_eq!(ar[1].id, 2);
        assert_eq!(ar[1].name, "bob");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: fetch_all_arena with empty result ---

    #[test]
    fn audit_fetch_all_arena_empty() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER NOT NULL)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);

        let ar: bsql_arena::ArenaRows<i64> = conn
            .fetch_all_arena(sql, hash, &[], |stmt, _arena| Ok(stmt.column_int64(0)))
            .unwrap();

        assert!(ar.is_empty());
        assert_eq!(ar.len(), 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: fetch_one_direct error for 0 rows ---

    #[test]
    fn fetch_one_direct_no_rows_errors() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.fetch_one_direct(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)));

        assert!(result.is_err());
        match result {
            Err(SqliteError::Internal(msg)) => {
                assert!(msg.contains("expected 1 row, got 0"));
            }
            other => panic!("expected Internal error, got: {other:?}"),
        }

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: fetch_optional_direct returns None for 0 rows ---

    #[test]
    fn fetch_optional_direct_no_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn
            .fetch_optional_direct(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)))
            .unwrap();

        assert!(result.is_none());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // --- Audit: get_i64 with wrong-sized data returns None ---

    #[test]
    fn get_i64_wrong_size_returns_none() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(&[1, 2, 3]); // 3 bytes, not 8
        let result = QueryResult {
            col_count: 1,
            row_count: 1,
            col_offsets: vec![(offset, 3)],
        };
        // Should return None because data is 3 bytes, not 8
        assert_eq!(result.get_i64(0, 0, &arena), None);
    }

    // --- Audit: get_f64 with wrong-sized data returns None ---

    #[test]
    fn get_f64_wrong_size_returns_none() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(&[1, 2, 3, 4]); // 4 bytes, not 8
        let result = QueryResult {
            col_count: 1,
            row_count: 1,
            col_offsets: vec![(offset, 4)],
        };
        assert_eq!(result.get_f64(0, 0, &arena), None);
    }

    // ---- for_each ----

    #[test]
    fn for_each_zero_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let mut count = 0usize;
        conn.for_each(sql, hash, &[], |_stmt| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_one_row() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let mut count = 0usize;
        let mut found_id = 0i64;
        let mut found_name = String::new();
        conn.for_each(sql, hash, &[], |stmt| {
            count += 1;
            found_id = stmt.column_int64(0);
            found_name = stmt
                .column_text(1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .unwrap_or("")
                .to_owned();
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(found_id, 1);
        assert_eq!(found_name, "alice");

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_1000_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..1000 {
            conn.exec(&format!("INSERT INTO t VALUES ({i}, 'name_{i}')"))
                .unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let mut count = 0usize;
        conn.for_each(sql, hash, &[], |stmt| {
            let id = stmt.column_int64(0);
            assert_eq!(id, count as i64);
            let name_bytes = stmt.column_text(1).unwrap();
            let name = std::str::from_utf8(name_bytes).unwrap();
            assert_eq!(name, format!("name_{count}"));
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1000);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_text_columns_zero_copy() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'hello world')")
            .unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        conn.for_each(sql, hash, &[], |stmt| {
            // column_text returns &[u8] borrowed directly from SQLite
            let bytes = stmt.column_text(1).unwrap();
            let s = std::str::from_utf8(bytes).unwrap();
            assert_eq!(s, "hello world");
            // Verify it's a valid pointer (not copied) by checking length
            assert_eq!(bytes.len(), 11);
            Ok(())
        })
        .unwrap();

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_counting_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..50 {
            conn.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let mut count = 0u64;
        conn.for_each(sql, hash, &[], |_stmt| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 50);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ---- for_each_collect ----

    #[test]
    fn for_each_collect_empty() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();

        let sql = "SELECT id, name FROM t";
        let hash = hash_sql(sql);
        let results: Vec<(i64, String)> = conn
            .for_each_collect(sql, hash, &[], |stmt| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok((id, name))
            })
            .unwrap();
        assert!(results.is_empty());

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_collect_builds_vec() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'charlie')").unwrap();

        let sql = "SELECT id, name FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let results: Vec<(i64, String)> = conn
            .for_each_collect(sql, hash, &[], |stmt| {
                let id = stmt.column_int64(0);
                let name = stmt
                    .column_text(1)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("")
                    .to_owned();
                Ok((id, name))
            })
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (1, "alice".to_owned()));
        assert_eq!(results[1], (2, "bob".to_owned()));
        assert_eq!(results[2], (3, "charlie".to_owned()));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_collect_1000_rows() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("BEGIN").unwrap();
        for i in 0..1000 {
            conn.exec(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        conn.exec("COMMIT").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let results: Vec<i64> = conn
            .for_each_collect(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(results.len(), 1000);
        for (i, &id) in results.iter().enumerate() {
            assert_eq!(id, i as i64);
        }

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_closure_error_propagates() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();
        conn.exec("INSERT INTO t VALUES (2)").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let hash = hash_sql(sql);
        let result = conn.for_each(sql, hash, &[], |stmt| {
            if stmt.column_int64(0) == 2 {
                return Err(SqliteError::Internal("stop at 2".into()));
            }
            Ok(())
        });
        assert!(result.is_err());
        match result {
            Err(SqliteError::Internal(msg)) => assert!(msg.contains("stop at 2")),
            other => panic!("expected Internal error, got: {other:?}"),
        }

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn for_each_with_params() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, 'alice')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, 'bob')").unwrap();
        conn.exec("INSERT INTO t VALUES (3, 'charlie')").unwrap();

        let sql = "SELECT id, name FROM t WHERE id > ?1";
        let hash = hash_sql(sql);
        let limit: i64 = 1;
        let mut count = 0usize;
        conn.for_each(sql, hash, &[&limit], |_stmt| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 2);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // for_each — NULL values in various positions
    // ===============================================================

    #[test]
    fn for_each_null_values_in_all_positions() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (a TEXT, b INTEGER, c REAL, d BLOB)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (NULL, NULL, NULL, NULL)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES ('x', NULL, 1.5, NULL)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (NULL, 42, NULL, X'DEADBEEF')")
            .unwrap();

        let sql = "SELECT a, b, c, d FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let mut rows_seen = 0usize;
        conn.for_each(sql, hash, &[], |stmt| {
            match rows_seen {
                0 => {
                    // All NULL
                    assert!(
                        stmt.column_text(0).is_none() || stmt.column_type(0) == raw::SQLITE_NULL
                    );
                }
                1 => {
                    // 'x', NULL, 1.5, NULL
                    let text = stmt.column_text(0);
                    assert_eq!(text, Some(b"x" as &[u8]));
                }
                2 => {
                    // NULL, 42, NULL, blob
                    assert_eq!(stmt.column_int64(1), 42);
                }
                _ => {}
            }
            rows_seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(rows_seen, 3);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // for_each — mixed types in one query
    // ===============================================================

    #[test]
    fn for_each_mixed_types() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (i INTEGER, t TEXT, b BLOB, f REAL, bo INTEGER)")
            .unwrap();
        conn.exec("INSERT INTO t VALUES (42, 'hello', X'CAFE', 3.14, 1)")
            .unwrap();

        let sql = "SELECT i, t, b, f, bo FROM t";
        let hash = hash_sql(sql);
        conn.for_each(sql, hash, &[], |stmt| {
            assert_eq!(stmt.column_int64(0), 42);
            assert_eq!(stmt.column_text(1), Some(b"hello" as &[u8]));
            assert_eq!(stmt.column_blob(2), &[0xCA, 0xFE]);
            let f = stmt.column_double(3);
            assert!((f - 3.14).abs() < 1e-10);
            assert_eq!(stmt.column_int64(4), 1);
            Ok(())
        })
        .unwrap();
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // for_each_collect with transformation
    // ===============================================================

    #[test]
    fn for_each_collect_with_transform() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (name TEXT)").unwrap();
        conn.exec("INSERT INTO t VALUES ('alice')").unwrap();
        conn.exec("INSERT INTO t VALUES ('bob')").unwrap();
        conn.exec("INSERT INTO t VALUES ('charlie')").unwrap();

        let sql = "SELECT name FROM t ORDER BY rowid";
        let hash = hash_sql(sql);
        let upper: Vec<String> = conn
            .for_each_collect(sql, hash, &[], |stmt| {
                let name = stmt
                    .column_text(0)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("");
                Ok(name.to_uppercase())
            })
            .unwrap();
        assert_eq!(upper, vec!["ALICE", "BOB", "CHARLIE"]);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // for_each — zero rows
    // ===============================================================

    #[test]
    fn for_each_zero_rows_closure_not_called() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let mut count = 0usize;
        conn.for_each(sql, hash, &[], |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // Error paths
    // ===============================================================

    #[test]
    fn exec_invalid_sql_error() {
        let path = temp_db_path();
        let conn = SqliteConnection::open(&path).unwrap();
        let result = conn.exec("NOT VALID SQL AT ALL");
        assert!(result.is_err());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn query_invalid_sql_error() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        let mut arena = Arena::new();
        let sql = "SELECTTTT GARBAGE";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[], &mut arena);
        assert!(result.is_err());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn connection_usable_after_query_error() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        // First: trigger an error
        let mut arena = Arena::new();
        let bad_sql = "SELECT nonexistent_column FROM nonexistent_table";
        let bad_hash = hash_sql(bad_sql);
        let result = conn.query(bad_sql, bad_hash, &[], &mut arena);
        assert!(result.is_err());

        // Then: verify connection is still usable
        arena.reset();
        let good_sql = "SELECT 1";
        let good_hash = hash_sql(good_sql);
        let result = conn.query(good_sql, good_hash, &[], &mut arena);
        assert!(result.is_ok());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_one_direct_zero_rows_error() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn.fetch_one_direct(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)));
        assert!(result.is_err());
        match result {
            Err(SqliteError::Internal(msg)) => {
                assert!(msg.contains("expected 1 row, got 0"));
            }
            other => panic!("expected Internal error, got: {other:?}"),
        }
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_optional_direct_zero_rows_none() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn
            .fetch_optional_direct(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert!(result.is_none());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fetch_optional_direct_one_row_some() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (42)").unwrap();

        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        let result = conn
            .fetch_optional_direct(sql, hash, &[], |stmt| Ok(stmt.column_int64(0)))
            .unwrap();
        assert_eq!(result, Some(42));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // Edge cases — SQL content
    // ===============================================================

    #[test]
    fn empty_sql_string_exec() {
        let path = temp_db_path();
        let conn = SqliteConnection::open(&path).unwrap();
        // Empty SQL is valid in SQLite (no-op)
        let result = conn.exec("");
        assert!(result.is_ok());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn whitespace_only_sql_exec() {
        let path = temp_db_path();
        let conn = SqliteConnection::open(&path).unwrap();
        let result = conn.exec("   ");
        assert!(result.is_ok());
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn very_long_sql() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();
        // Build a 10KB SQL string
        let long_val = "x".repeat(10_000);
        let sql_ins = "INSERT INTO t VALUES (?1)";
        let hash_ins = hash_sql(sql_ins);
        conn.execute(sql_ins, hash_ins, &[&long_val.as_str()])
            .unwrap();

        let mut arena = Arena::new();
        let sql_sel = "SELECT val FROM t";
        let hash_sel = hash_sql(sql_sel);
        let result = conn.query(sql_sel, hash_sel, &[], &mut arena).unwrap();
        assert_eq!(result.get_str(0, 0, &arena), Some(long_val.as_str()));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // Edge cases — parameter values
    // ===============================================================

    #[test]
    fn param_empty_string() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();
        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        let empty: &str = "";
        conn.execute(sql, hash, &[&empty]).unwrap();

        let mut arena = Arena::new();
        let sel = "SELECT val FROM t";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert!(!result.is_null(0, 0));
        assert_eq!(result.get_str(0, 0, &arena), Some(""));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn param_unicode_emoji_zwj_rtl() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val TEXT)").unwrap();

        let texts = [
            "\u{1F600}\u{1F4A9}\u{1F680}",                 // emoji
            "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}", // ZWJ family
            "\u{0645}\u{0631}\u{062D}\u{0628}\u{0627}",    // Arabic (RTL)
            "\u{202E}reversed\u{202C}",                    // RTL override
        ];
        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        for text in &texts {
            conn.execute(sql, hash, &[text]).unwrap();
        }

        let mut arena = Arena::new();
        let sel = "SELECT val FROM t ORDER BY rowid";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        for (i, text) in texts.iter().enumerate() {
            assert_eq!(result.get_str(i, 0, &arena), Some(*text), "row {i}");
        }
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn param_i64_min_max() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();
        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        conn.execute(sql, hash, &[&i64::MIN]).unwrap();
        conn.execute(sql, hash, &[&i64::MAX]).unwrap();

        let mut arena = Arena::new();
        let sel = "SELECT val FROM t ORDER BY rowid";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(i64::MIN));
        assert_eq!(result.get_i64(1, 0, &arena), Some(i64::MAX));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn param_f64_nan_infinity() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val REAL)").unwrap();
        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        let nan: f64 = f64::NAN;
        conn.execute(sql, hash, &[&nan]).unwrap();
        let inf: f64 = f64::INFINITY;
        conn.execute(sql, hash, &[&inf]).unwrap();
        let neg_inf: f64 = f64::NEG_INFINITY;
        conn.execute(sql, hash, &[&neg_inf]).unwrap();

        let mut arena = Arena::new();
        let sel = "SELECT val FROM t ORDER BY rowid";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        // NaN behavior: SQLite may store NaN as NULL
        // Infinity and neg infinity should round-trip
        let nan_val = result.get_f64(0, 0, &arena);
        if let Some(v) = nan_val {
            assert!(v.is_nan());
        }
        // else: SQLite stored NaN as NULL, which is acceptable
        assert_eq!(result.get_f64(1, 0, &arena), Some(f64::INFINITY));
        assert_eq!(result.get_f64(2, 0, &arena), Some(f64::NEG_INFINITY));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn param_null_option() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (val INTEGER)").unwrap();
        let sql = "INSERT INTO t VALUES (?1)";
        let hash = hash_sql(sql);
        let null_val: Option<i64> = None;
        conn.execute(sql, hash, &[&null_val]).unwrap();
        let some_val: Option<i64> = Some(42);
        conn.execute(sql, hash, &[&some_val]).unwrap();

        let mut arena = Arena::new();
        let sel = "SELECT val FROM t ORDER BY rowid";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert!(result.is_null(0, 0));
        assert_eq!(result.get_i64(1, 0, &arena), Some(42));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // execute_direct
    // ===============================================================

    #[test]
    fn execute_direct_insert_and_verify() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, val TEXT)").unwrap();

        let sql = "INSERT INTO t VALUES (?1, ?2)";
        let hash = hash_sql(sql);
        let id: i64 = 1;
        let val: &str = "hello";
        let affected = conn.execute_direct(sql, hash, &[&id, &val]).unwrap();
        assert_eq!(affected, 1);

        let mut arena = Arena::new();
        let sel = "SELECT id, val FROM t";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(1));
        assert_eq!(result.get_str(0, 1, &arena), Some("hello"));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // QueryResult boundary assertions
    // ===============================================================

    #[test]
    #[should_panic(expected = "row")]
    fn query_result_cell_out_of_range_row() {
        let result = QueryResult {
            col_count: 1,
            row_count: 1,
            col_offsets: vec![(0, 8)],
        };
        result.cell(5, 0); // row 5 out of range
    }

    #[test]
    #[should_panic(expected = "col")]
    fn query_result_cell_out_of_range_col() {
        let result = QueryResult {
            col_count: 1,
            row_count: 1,
            col_offsets: vec![(0, 8)],
        };
        result.cell(0, 5); // col 5 out of range
    }

    // ===============================================================
    // fetch_all_arena
    // ===============================================================

    #[test]
    fn fetch_all_arena_with_blobs() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, data BLOB)").unwrap();
        conn.exec("INSERT INTO t VALUES (1, X'CAFE')").unwrap();
        conn.exec("INSERT INTO t VALUES (2, X'BEEF')").unwrap();

        let sql = "SELECT id, data FROM t ORDER BY id";
        let hash = hash_sql(sql);

        #[derive(Debug)]
        #[allow(dead_code)]
        struct Row {
            id: i64,
            data_offset: usize,
            data_len: usize,
        }

        let ar = conn
            .fetch_all_arena(sql, hash, &[], |stmt, arena| {
                let id = stmt.column_int64(0);
                let blob = stmt.column_blob(1);
                let offset = arena.alloc_copy(blob);
                Ok(Row {
                    id,
                    data_offset: offset,
                    data_len: blob.len(),
                })
            })
            .unwrap();

        assert_eq!(ar.len(), 2);
        assert_eq!(ar[0].id, 1);
        assert_eq!(ar[1].id, 2);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // streaming — chunk_size=1
    // ===============================================================

    #[test]
    fn streaming_chunk_size_one() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (10)").unwrap();
        conn.exec("INSERT INTO t VALUES (20)").unwrap();
        conn.exec("INSERT INTO t VALUES (30)").unwrap();

        let sql = "SELECT id FROM t ORDER BY id";
        let sql_hash = hash_sql(sql);
        let mut streaming = conn.query_streaming(sql, sql_hash, &[], 1).unwrap();

        for expected in [10i64, 20, 30] {
            let mut arena = Arena::new();
            let chunk = conn
                .streaming_next_chunk(&mut streaming, &mut arena)
                .unwrap();
            assert_eq!(chunk.row_count, 1);
            assert_eq!(chunk.get_i64(0, 0, &arena), Some(expected));
        }

        let mut arena_end = Arena::new();
        let chunk_end = conn
            .streaming_next_chunk(&mut streaming, &mut arena_end)
            .unwrap();
        assert_eq!(chunk_end.row_count, 0);
        assert!(streaming.finished);
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // Multiple prepare_only calls
    // ===============================================================

    #[test]
    fn prepare_only_idempotent() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER)").unwrap();
        let sql = "SELECT id FROM t";
        let hash = hash_sql(sql);
        conn.prepare_only(sql, hash).unwrap();
        assert_eq!(conn.stmts.len(), 1);
        conn.prepare_only(sql, hash).unwrap();
        assert_eq!(conn.stmts.len(), 1); // no duplicate
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    // ===============================================================
    // Multiple mixed operations on same connection
    // ===============================================================

    #[test]
    fn mixed_operations_stress() {
        let path = temp_db_path();
        let mut conn = SqliteConnection::open(&path).unwrap();
        conn.exec("CREATE TABLE t (id INTEGER, name TEXT, val REAL)")
            .unwrap();

        // Insert
        let ins = "INSERT INTO t VALUES (?1, ?2, ?3)";
        let ins_hash = hash_sql(ins);
        for i in 0..100 {
            let id: i64 = i;
            let name = format!("row_{i}");
            let val: f64 = i as f64 * 1.5;
            conn.execute(ins, ins_hash, &[&id, &name.as_str(), &val])
                .unwrap();
        }

        // Query
        let mut arena = Arena::new();
        let sel = "SELECT id, name, val FROM t ORDER BY id";
        let sel_hash = hash_sql(sel);
        let result = conn.query(sel, sel_hash, &[], &mut arena).unwrap();
        assert_eq!(result.len(), 100);

        // Update
        let upd = "UPDATE t SET name = ?1 WHERE id = ?2";
        let upd_hash = hash_sql(upd);
        let new_name: &str = "updated";
        let id: i64 = 50;
        let affected = conn.execute(upd, upd_hash, &[&new_name, &id]).unwrap();
        assert_eq!(affected, 1);

        // Delete
        let del = "DELETE FROM t WHERE id > ?1";
        let del_hash = hash_sql(del);
        let threshold: i64 = 90;
        let affected = conn.execute(del, del_hash, &[&threshold]).unwrap();
        assert_eq!(affected, 9);

        // Verify final count
        arena.reset();
        let count_sql = "SELECT COUNT(*) FROM t";
        let count_hash = hash_sql(count_sql);
        let result = conn.query(count_sql, count_hash, &[], &mut arena).unwrap();
        assert_eq!(result.get_i64(0, 0, &arena), Some(91));

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
