//! Safe wrappers over the `libsqlite3-sys` C API.
//!
//! This module is the **only** place in the crate that contains `unsafe` code.
//! It exposes two safe wrapper types — [`DbHandle`] and [`StmtHandle`] — that
//! encapsulate the raw `*mut sqlite3` and `*mut sqlite3_stmt` pointers. All
//! other modules in the crate use these safe types exclusively.

use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::ptr;

use libsqlite3_sys as raw;

use crate::SqliteError;

/// Return the `SQLITE_TRANSIENT` destructor sentinel.
///
/// In C this is `((sqlite3_destructor_type)-1)` -- a sentinel function pointer
/// that tells SQLite to make its own copy of the data immediately. We cannot
/// define this as a `const` in Rust because the compiler rejects a non-null
/// function pointer with value -1 as UB in const context. Instead we produce
/// it at runtime via transmute, which is sound because SQLite never calls
/// this "pointer" as a function -- it only checks if the value equals -1.
#[inline]
fn sqlite_transient() -> raw::sqlite3_destructor_type {
    // SAFETY: SQLITE_TRANSIENT is the well-known sentinel value -1 cast to a
    // function pointer. SQLite checks this value with pointer comparison and
    // never dereferences or calls it. This is the standard way to express it
    // from Rust when the crate does not re-export the macro.
    Some(unsafe { std::mem::transmute::<isize, unsafe extern "C" fn(*mut std::ffi::c_void)>(-1) })
}

/// Get the last error message from a database connection.
///
/// # Safety
///
/// `db` must be a valid (possibly errored) database handle, or null.
unsafe fn error_message(db: *mut raw::sqlite3) -> String {
    if db.is_null() {
        return "unknown error (null db handle)".into();
    }
    let ptr = unsafe { raw::sqlite3_errmsg(db) };
    if ptr.is_null() {
        return "unknown error".into();
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

// ---------------------------------------------------------------------------
// StepResult
// ---------------------------------------------------------------------------

/// Result of stepping a prepared statement.
///
/// SQLite's `sqlite3_step` returns one of two success states:
///
/// - [`StepResult::Row`] -- a new row of data is available for reading via
///   the `column_*` methods on [`StmtHandle`].
/// - [`StepResult::Done`] -- the statement has finished executing (no more rows).
///
/// Any other return code from `sqlite3_step` is translated into a
/// [`SqliteError::Sqlite`] error.
///
/// # Example
///
/// ```no_run
/// use bsql_driver_sqlite::ffi::{DbHandle, StepResult};
/// // After preparing and binding a SELECT:
/// // match stmt.step()? {
/// //     StepResult::Row => { /* read columns */ }
/// //     StepResult::Done => { /* no more rows */ }
/// // }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// A new row of data is ready for reading.
    Row,
    /// The statement has finished executing.
    Done,
}

// ---------------------------------------------------------------------------
// DbHandle
// ---------------------------------------------------------------------------

/// Safe wrapper around a SQLite database handle (`*mut sqlite3`).
///
/// `DbHandle` owns the raw `sqlite3*` pointer returned by `sqlite3_open_v2`
/// and closes it automatically on drop via `sqlite3_close`. All prepared
/// statements created from this handle ([`StmtHandle`]) must be dropped
/// before the handle itself -- Rust's struct drop order guarantees this
/// when `StmtHandle` values are held in the same struct.
///
/// # Thread safety
///
/// `DbHandle` is intentionally **not** `Send` or `Sync`. The underlying
/// connection is opened with `SQLITE_OPEN_NOMUTEX` (no internal mutexing),
/// so it must only be used from the thread that created it. The pool module
/// enforces this by running each connection on a dedicated OS thread.
///
/// # Lifecycle
///
/// 1. [`DbHandle::open`] -- open or create a database file.
/// 2. [`DbHandle::exec`] / [`DbHandle::prepare`] -- execute SQL or prepare statements.
/// 3. `Drop` -- `sqlite3_close` is called automatically.
///
/// # Example
///
/// ```no_run
/// use bsql_driver_sqlite::ffi::DbHandle;
/// use libsqlite3_sys as raw;
///
/// let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
/// let db = DbHandle::open("/tmp/test.db", flags).unwrap();
/// db.exec("CREATE TABLE t (id INTEGER)").unwrap();
/// // db is closed when dropped
/// ```
pub struct DbHandle {
    ptr: *mut raw::sqlite3,
    /// Prevent Send+Sync auto-derivation.
    _marker: PhantomData<*mut ()>,
}

impl DbHandle {
    /// Open a database.
    pub fn open(path: &str, flags: i32) -> Result<Self, SqliteError> {
        let c_path =
            CString::new(path).map_err(|_| SqliteError::Internal("null byte in path".into()))?;
        let mut db: *mut raw::sqlite3 = ptr::null_mut();
        // SAFETY: We pass valid CString pointer, a mutable pointer to receive the
        // handle, valid flags, and null VFS name. If open fails we close the handle.
        let rc = unsafe { raw::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, ptr::null()) };
        if rc != raw::SQLITE_OK {
            let msg = unsafe { error_message(db) };
            if !db.is_null() {
                // SAFETY: db was allocated by sqlite3_open_v2, close releases it.
                unsafe {
                    raw::sqlite3_close(db);
                }
            }
            return Err(SqliteError::Sqlite {
                code: rc,
                message: msg,
            });
        }
        Ok(Self {
            ptr: db,
            _marker: PhantomData,
        })
    }

    /// Prepare a SQL statement.
    pub fn prepare(&self, sql: &str) -> Result<StmtHandle, SqliteError> {
        let mut stmt: *mut raw::sqlite3_stmt = ptr::null_mut();
        // SAFETY: self.ptr is a valid open database handle. We pass the SQL bytes
        // with their length (no NUL terminator required for _v2).
        let rc = unsafe {
            raw::sqlite3_prepare_v2(
                self.ptr,
                sql.as_ptr().cast::<i8>(),
                sql.len() as i32,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if rc != raw::SQLITE_OK {
            let msg = unsafe { error_message(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: msg,
            });
        }
        Ok(StmtHandle {
            ptr: stmt,
            _marker: PhantomData,
        })
    }

    /// Execute a simple SQL string (PRAGMA, DDL). No parameters, no results.
    pub fn exec(&self, sql: &str) -> Result<(), SqliteError> {
        let c_sql =
            CString::new(sql).map_err(|_| SqliteError::Internal("null byte in SQL".into()))?;
        // SAFETY: self.ptr is a valid open database handle. We pass a NUL-terminated
        // C string, no callback, no callback context, and no error string output.
        let rc = unsafe {
            raw::sqlite3_exec(
                self.ptr,
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if rc != raw::SQLITE_OK {
            let msg = unsafe { error_message(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: msg,
            });
        }
        Ok(())
    }

    /// Get number of changes from last INSERT/UPDATE/DELETE.
    pub fn changes(&self) -> u64 {
        // SAFETY: self.ptr is a valid open database handle.
        // Clamp negative values to 0 to avoid wrapping when cast to u64.
        (unsafe { raw::sqlite3_changes(self.ptr) }).max(0) as u64
    }

    /// Get last error message.
    pub fn error_message(&self) -> String {
        // SAFETY: self.ptr is a valid open database handle.
        unsafe { error_message(self.ptr) }
    }
}

impl Drop for DbHandle {
    fn drop(&mut self) {
        // SAFETY: self.ptr is a valid database handle. All statements must have
        // been finalized before this point (ensured by the caller / StmtHandle Drop).
        unsafe {
            raw::sqlite3_close(self.ptr);
        }
    }
}

// ---------------------------------------------------------------------------
// StmtHandle
// ---------------------------------------------------------------------------

/// Safe wrapper around a SQLite prepared statement (`*mut sqlite3_stmt`).
///
/// `StmtHandle` owns the raw `sqlite3_stmt*` pointer returned by
/// `sqlite3_prepare_v2` and finalizes it automatically on drop via
/// `sqlite3_finalize`.
///
/// # Lifecycle
///
/// 1. Created by [`DbHandle::prepare`].
/// 2. Bind parameters via `bind_*` methods (1-based indices).
/// 3. Step the statement with [`StmtHandle::step`].
/// 4. Read columns with `column_*` methods when `step` returns [`StepResult::Row`].
/// 5. Reset with [`StmtHandle::reset`] for reuse, or drop to finalize.
///
/// # Pointer invalidation
///
/// Column data pointers (from `column_text`, `column_blob`) are invalidated
/// by the next call to `step`, `reset`, or when the statement is dropped.
/// The `conn` module copies all data into an arena during the step loop.
///
/// # Thread safety
///
/// Not `Send` or `Sync` -- must be used on the same thread as the parent
/// `DbHandle`.
pub struct StmtHandle {
    ptr: *mut raw::sqlite3_stmt,
    /// Prevent Send+Sync auto-derivation.
    _marker: PhantomData<*mut ()>,
}

impl StmtHandle {
    // --- Binding (safe methods) ---

    /// Bind an i64 parameter at 1-based index.
    pub fn bind_int64(&self, idx: i32, val: i64) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_bind_int64(self.ptr, idx, val) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind a f64 parameter at 1-based index.
    pub fn bind_double(&self, idx: i32, val: f64) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_bind_double(self.ptr, idx, val) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind a text parameter at 1-based index.
    ///
    /// Uses `SQLITE_TRANSIENT` — SQLite copies the data immediately.
    pub fn bind_text(&self, idx: i32, val: &str) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle. SQLITE_TRANSIENT
        // tells SQLite to copy the data, so the Rust reference need not outlive this call.
        let rc = unsafe {
            raw::sqlite3_bind_text(
                self.ptr,
                idx,
                val.as_ptr().cast::<i8>(),
                val.len() as i32,
                sqlite_transient(),
            )
        };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind a blob parameter at 1-based index.
    ///
    /// Uses `SQLITE_TRANSIENT` — SQLite copies the data immediately.
    pub fn bind_blob(&self, idx: i32, val: &[u8]) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle. SQLITE_TRANSIENT
        // tells SQLite to copy the data.
        let rc = unsafe {
            raw::sqlite3_bind_blob(
                self.ptr,
                idx,
                val.as_ptr().cast::<std::ffi::c_void>(),
                val.len() as i32,
                sqlite_transient(),
            )
        };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Bind NULL at 1-based index.
    pub fn bind_null(&self, idx: i32) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_bind_null(self.ptr, idx) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    /// Clear all bindings on this statement.
    pub fn clear_bindings(&self) {
        // SAFETY: self.ptr is a valid prepared statement handle.
        // sqlite3_clear_bindings always returns SQLITE_OK.
        unsafe {
            raw::sqlite3_clear_bindings(self.ptr);
        }
    }

    // --- Stepping ---

    /// Step the statement. Returns `StepResult::Row` or `StepResult::Done`.
    pub fn step(&self) -> Result<StepResult, SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_step(self.ptr) };
        match rc {
            raw::SQLITE_ROW => Ok(StepResult::Row),
            raw::SQLITE_DONE => Ok(StepResult::Done),
            _ => {
                let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
                let msg = unsafe { error_message(db) };
                Err(SqliteError::Sqlite {
                    code: rc,
                    message: msg,
                })
            }
        }
    }

    /// Reset the statement for reuse (clears the step state, keeps bindings).
    pub fn reset(&self) -> Result<(), SqliteError> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let rc = unsafe { raw::sqlite3_reset(self.ptr) };
        if rc != raw::SQLITE_OK {
            let db = unsafe { raw::sqlite3_db_handle(self.ptr) };
            return Err(SqliteError::Sqlite {
                code: rc,
                message: unsafe { error_message(db) },
            });
        }
        Ok(())
    }

    // --- Column reading ---

    /// Get the number of columns in the result set.
    pub fn column_count(&self) -> i32 {
        // SAFETY: self.ptr is a valid prepared statement handle.
        unsafe { raw::sqlite3_column_count(self.ptr) }
    }

    /// Get the storage type of a column (SQLITE_INTEGER, SQLITE_FLOAT,
    /// SQLITE_TEXT, SQLITE_BLOB, or SQLITE_NULL).
    pub fn column_type(&self, idx: i32) -> i32 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_type(self.ptr, idx) }
    }

    /// Get a column value as i64.
    pub fn column_int64(&self, idx: i32) -> i64 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_int64(self.ptr, idx) }
    }

    /// Get a column value as f64.
    pub fn column_double(&self, idx: i32) -> f64 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_double(self.ptr, idx) }
    }

    /// Get a column value as text (UTF-8 bytes).
    ///
    /// Returns `None` if the column is NULL. The returned slice is valid until
    /// the next `step`, `reset`, or the statement is dropped.
    pub fn column_text(&self, idx: i32) -> Option<&[u8]> {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        // The returned pointer is valid until the next step/reset/finalize.
        let ptr = unsafe { raw::sqlite3_column_text(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        let len = unsafe { raw::sqlite3_column_bytes(self.ptr, idx) } as usize;
        Some(unsafe { std::slice::from_raw_parts(ptr, len) })
    }

    /// Get a column value as blob (raw bytes).
    ///
    /// Returns an empty slice if the column is NULL or has zero length.
    /// The returned slice is valid until the next `step`, `reset`, or the
    /// statement is dropped.
    pub fn column_blob(&self, idx: i32) -> &[u8] {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        let len = unsafe { raw::sqlite3_column_bytes(self.ptr, idx) } as usize;
        if len == 0 {
            return &[];
        }
        let ptr = unsafe { raw::sqlite3_column_blob(self.ptr, idx) };
        if ptr.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }
    }

    /// Get the byte length of a column value.
    pub fn column_bytes(&self, idx: i32) -> i32 {
        // SAFETY: self.ptr is a valid statement that has been stepped to SQLITE_ROW.
        unsafe { raw::sqlite3_column_bytes(self.ptr, idx) }
    }

    /// Get the column name as a UTF-8 string.
    pub fn column_name(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_name(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the column declared type (from CREATE TABLE schema).
    pub fn column_decltype(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_decltype(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the table name for a column in the result set.
    ///
    /// Returns `None` if the column is an expression (not backed by a table)
    /// or if the index is out of range.
    pub fn column_table_name(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_table_name(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the origin column name (the actual column name in CREATE TABLE) for
    /// a column in the result set.
    ///
    /// Returns `None` if the column is an expression (not backed by a table column)
    /// or if the index is out of range.
    pub fn column_origin_name(&self, idx: i32) -> Option<&str> {
        // SAFETY: self.ptr is a valid prepared statement handle.
        let ptr = unsafe { raw::sqlite3_column_origin_name(self.ptr, idx) };
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }

    /// Get the number of parameters in this prepared statement.
    pub fn bind_parameter_count(&self) -> i32 {
        // SAFETY: self.ptr is a valid prepared statement handle.
        unsafe { raw::sqlite3_bind_parameter_count(self.ptr) }
    }
}

impl Drop for StmtHandle {
    fn drop(&mut self) {
        // SAFETY: self.ptr is a valid statement handle. After finalize, the
        // pointer is invalid — but we never use it again (we are in drop).
        // We intentionally ignore the return code; sqlite3_finalize returns the
        // error from the most recent step, which we already handled.
        unsafe {
            raw::sqlite3_finalize(self.ptr);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        format!("{}/bsql_test_ffi_{}_{}.db", dir.display(), pid, id)
    }

    fn rw_flags() -> i32 {
        raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX
    }

    // ---- open ----

    #[test]
    fn open_and_close() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).expect("open failed");
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_invalid_path_with_null() {
        let result = DbHandle::open("path\0with_null", rw_flags());
        assert!(result.is_err());
        match result {
            Err(SqliteError::Internal(msg)) => assert!(msg.contains("null")),
            Err(e) => panic!("expected Internal error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn open_nonexistent_directory() {
        let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_CREATE | raw::SQLITE_OPEN_NOMUTEX;
        let result = DbHandle::open("/no/such/directory/db.sqlite", flags);
        assert!(result.is_err());
    }

    #[test]
    fn open_readonly_nonexistent_file() {
        let flags = raw::SQLITE_OPEN_READONLY | raw::SQLITE_OPEN_NOMUTEX;
        let result = DbHandle::open("/tmp/bsql_does_not_exist_ever.db", flags);
        assert!(result.is_err());
    }

    #[test]
    fn open_path_with_spaces() {
        let dir = std::env::temp_dir();
        let path = format!(
            "{}/bsql test spaces {}.db",
            dir.display(),
            std::process::id()
        );
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_path_with_unicode() {
        let dir = std::env::temp_dir();
        let path = format!(
            "{}/bsql_тест_юникод_{}.db",
            dir.display(),
            std::process::id()
        );
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_path_with_special_chars() {
        let dir = std::env::temp_dir();
        let path = format!("{}/bsql_test#@!_{}.db", dir.display(), std::process::id());
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_empty_path() {
        let result = DbHandle::open("", rw_flags());
        // SQLite may succeed with empty string (in-memory), or may error.
        // We just ensure it does not crash.
        drop(result);
    }

    // ---- prepare ----

    #[test]
    fn prepare_and_finalize() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        let _stmt = db
            .prepare("INSERT INTO t (id, name) VALUES (?1, ?2)")
            .unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_invalid_sql() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let result = db.prepare("NOT VALID SQL STATEMENT");
        assert!(result.is_err());
        match &result {
            Err(SqliteError::Sqlite { code, message }) => {
                assert_ne!(*code, 0);
                assert!(!message.is_empty());
            }
            _ => panic!("expected Sqlite error"),
        }
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_sql_referencing_nonexistent_table() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        // prepare succeeds for SELECT from missing table in SQLite (deferred)
        // but step will fail. Let's verify prepare itself.
        let result = db.prepare("SELECT * FROM no_such_table");
        assert!(result.is_err());
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_empty_sql() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        // Empty SQL should not crash -- sqlite3_prepare_v2 with empty string
        // returns OK with a null statement pointer.
        let _result = db.prepare("");
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_with_pragma() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let stmt = db.prepare("PRAGMA journal_mode").unwrap();
        let rc = stmt.step().unwrap();
        assert_eq!(rc, StepResult::Row);
        // PRAGMA journal_mode returns a text value
        assert!(stmt.column_text(0).is_some());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- bind errors ----

    #[test]
    fn bind_parameter_out_of_range_zero() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val INTEGER)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        // Index 0 is out of range (1-based)
        let result = stmt.bind_int64(0, 42);
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_parameter_out_of_range_high() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val INTEGER)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        // Index 100 is out of range (only 1 param)
        let result = stmt.bind_int64(100, 42);
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_to_stmt_with_no_params() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val INTEGER)").unwrap();
        let stmt = db.prepare("SELECT val FROM t").unwrap();
        assert_eq!(stmt.bind_parameter_count(), 0);
        // Binding index 1 when there are zero parameters -> error
        let result = stmt.bind_int64(1, 42);
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_text_out_of_range() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let result = stmt.bind_text(0, "test");
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_blob_out_of_range() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val BLOB)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let result = stmt.bind_blob(0, &[1, 2, 3]);
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_double_out_of_range() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val REAL)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let result = stmt.bind_double(0, 3.14);
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_null_out_of_range() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let result = stmt.bind_null(0);
        assert!(result.is_err());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- step and column reads ----

    #[test]
    fn bind_step_and_read_columns() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER, name TEXT, score REAL, data BLOB)")
            .unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1, ?2, ?3, ?4)").unwrap();
        ins.bind_int64(1, 42).unwrap();
        ins.bind_text(2, "hello").unwrap();
        ins.bind_double(3, 3.14).unwrap();
        ins.bind_blob(4, &[0xDE, 0xAD]).unwrap();
        let rc = ins.step().unwrap();
        assert_eq!(rc, StepResult::Done);
        drop(ins);

        let sel = db.prepare("SELECT id, name, score, data FROM t").unwrap();
        assert_eq!(sel.column_count(), 4);

        let rc = sel.step().unwrap();
        assert_eq!(rc, StepResult::Row);

        assert_eq!(sel.column_type(0), raw::SQLITE_INTEGER);
        assert_eq!(sel.column_type(1), raw::SQLITE_TEXT);
        assert_eq!(sel.column_type(2), raw::SQLITE_FLOAT);
        assert_eq!(sel.column_type(3), raw::SQLITE_BLOB);

        assert_eq!(sel.column_int64(0), 42);
        let text = sel.column_text(1).unwrap();
        assert_eq!(text, b"hello");
        assert!((sel.column_double(2) - 3.14).abs() < f64::EPSILON);
        let blob = sel.column_blob(3);
        assert_eq!(blob, &[0xDE, 0xAD]);

        assert_eq!(sel.column_name(0), Some("id"));
        assert_eq!(sel.column_name(1), Some("name"));

        let rc = sel.step().unwrap();
        assert_eq!(rc, StepResult::Done);

        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_null_and_read_null() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_null(1).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_type(0), raw::SQLITE_NULL);
        assert!(sel.column_text(0).is_none());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_in_first_column() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (a TEXT, b INTEGER, c TEXT)")
            .unwrap();
        db.exec("INSERT INTO t VALUES (NULL, 1, 'x')").unwrap();
        let sel = db.prepare("SELECT a, b, c FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_type(0), raw::SQLITE_NULL);
        assert_eq!(sel.column_int64(1), 1);
        assert_eq!(sel.column_text(2).unwrap(), b"x");
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_in_middle_column() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)")
            .unwrap();
        db.exec("INSERT INTO t VALUES (1, NULL, 3)").unwrap();
        let sel = db.prepare("SELECT a, b, c FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 1);
        assert_eq!(sel.column_type(1), raw::SQLITE_NULL);
        assert_eq!(sel.column_int64(2), 3);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_in_last_column() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
            .unwrap();
        db.exec("INSERT INTO t VALUES (1, 'x', NULL)").unwrap();
        let sel = db.prepare("SELECT a, b, c FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 1);
        assert_eq!(sel.column_text(1).unwrap(), b"x");
        assert_eq!(sel.column_type(2), raw::SQLITE_NULL);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn all_columns_null() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (a TEXT, b INTEGER, c REAL)")
            .unwrap();
        db.exec("INSERT INTO t VALUES (NULL, NULL, NULL)").unwrap();
        let sel = db.prepare("SELECT a, b, c FROM t").unwrap();
        sel.step().unwrap();
        for i in 0..3 {
            assert_eq!(sel.column_type(i), raw::SQLITE_NULL);
        }
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- reset and reuse ----

    #[test]
    fn reset_and_reuse_statement() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();

        ins.bind_int64(1, 1).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();

        ins.bind_int64(1, 2).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();

        drop(ins);

        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 2);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reset_multiple_times() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();

        for i in 1..=10 {
            ins.bind_int64(1, i).unwrap();
            ins.step().unwrap();
            ins.reset().unwrap();
            ins.clear_bindings();
        }
        drop(ins);

        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 10);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- exec ----

    #[test]
    fn exec_pragma() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("PRAGMA journal_mode = WAL").unwrap();
        db.exec("PRAGMA synchronous = NORMAL").unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_invalid_sql() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let result = db.exec("NOT VALID SQL");
        assert!(result.is_err());
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_null_in_sql() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let result = db.exec("SELECT\01");
        assert!(result.is_err());
        match result {
            Err(SqliteError::Internal(msg)) => assert!(msg.contains("null")),
            Err(e) => panic!("expected Internal error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_with_select_ignores_rows() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        // exec with SELECT -- does not error, just ignores the result rows
        db.exec("SELECT * FROM t").unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_multi_statement() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);")
            .unwrap();

        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 2);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exec_syntax_error_returns_sqlite_error() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let result = db.exec("CREATE TABL invalid_syntax");
        match result {
            Err(SqliteError::Sqlite { code, message }) => {
                assert_ne!(code, 0);
                assert!(!message.is_empty());
            }
            Ok(_) => panic!("expected error"),
            Err(e) => panic!("expected Sqlite error, got: {e:?}"),
        }
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- changes ----

    #[test]
    fn changes_count() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.exec("INSERT INTO t VALUES (2)").unwrap();
        db.exec("INSERT INTO t VALUES (3)").unwrap();
        db.exec("DELETE FROM t WHERE id > 1").unwrap();
        assert_eq!(db.changes(), 2);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn changes_count_after_insert() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(db.changes(), 1);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn changes_count_zero_after_select() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.exec("SELECT * FROM t").unwrap();
        // changes() still reports last DML, not 0 after SELECT
        // but after CREATE TABLE, changes is 0
        assert!(db.changes() <= 1);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- column metadata ----

    #[test]
    fn column_decltype_returns_type() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
            .unwrap();
        let stmt = db.prepare("SELECT id, name, score FROM t").unwrap();
        assert_eq!(stmt.column_decltype(0), Some("INTEGER"));
        assert_eq!(stmt.column_decltype(1), Some("TEXT"));
        assert_eq!(stmt.column_decltype(2), Some("REAL"));
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_decltype_expression_returns_none() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val INTEGER)").unwrap();
        let stmt = db.prepare("SELECT val + 1 FROM t").unwrap();
        // Expressions don't have a declared type
        assert!(stmt.column_decltype(0).is_none());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_blob_null_returns_empty() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (data BLOB)").unwrap();
        db.exec("INSERT INTO t VALUES (NULL)").unwrap();
        let sel = db.prepare("SELECT data FROM t").unwrap();
        sel.step().unwrap();
        let blob = sel.column_blob(0);
        assert!(blob.is_empty());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_blob_empty_returns_empty() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (data BLOB)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_blob(1, &[]).unwrap();
        ins.step().unwrap();
        drop(ins);
        let sel = db.prepare("SELECT data FROM t").unwrap();
        sel.step().unwrap();
        let blob = sel.column_blob(0);
        assert!(blob.is_empty());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_bytes_len() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (name TEXT)").unwrap();
        db.exec("INSERT INTO t VALUES ('abcdef')").unwrap();
        let sel = db.prepare("SELECT name FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_bytes(0), 6);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_table_name() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE users (name TEXT)").unwrap();
        let stmt = db.prepare("SELECT name FROM users").unwrap();
        assert_eq!(stmt.column_table_name(0), Some("users"));
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_table_name_expression_returns_none() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let stmt = db.prepare("SELECT 1 + 1").unwrap();
        assert!(stmt.column_table_name(0).is_none());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_origin_name() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE users (name TEXT)").unwrap();
        let stmt = db.prepare("SELECT name AS n FROM users").unwrap();
        assert_eq!(stmt.column_name(0), Some("n"));
        assert_eq!(stmt.column_origin_name(0), Some("name"));
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn column_origin_name_expression_returns_none() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let stmt = db.prepare("SELECT 1 + 1 AS sum").unwrap();
        assert_eq!(stmt.column_name(0), Some("sum"));
        assert!(stmt.column_origin_name(0).is_none());
        drop(stmt);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_parameter_count() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (a INT, b TEXT, c REAL)").unwrap();
        let stmt = db.prepare("INSERT INTO t VALUES (?1, ?2, ?3)").unwrap();
        assert_eq!(stmt.bind_parameter_count(), 3);
        drop(stmt);

        let stmt2 = db.prepare("SELECT * FROM t").unwrap();
        assert_eq!(stmt2.bind_parameter_count(), 0);
        drop(stmt2);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- boundary values ----

    #[test]
    fn bind_i64_min_max() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_int64(1, i64::MIN).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();
        ins.bind_int64(1, i64::MAX).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t ORDER BY val").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), i64::MIN);
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), i64::MAX);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_f64_infinity() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val REAL)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_double(1, f64::INFINITY).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();
        ins.bind_double(1, f64::NEG_INFINITY).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t ORDER BY rowid").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_double(0), f64::INFINITY);
        sel.step().unwrap();
        assert_eq!(sel.column_double(0), f64::NEG_INFINITY);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_f64_nan() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val REAL)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_double(1, f64::NAN).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        // SQLite stores NaN as NULL
        // Check that it doesn't crash; value might be NULL or NaN
        let _val = sel.column_double(0);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_f64_positive_negative_zero() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val REAL)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_double(1, 0.0_f64).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();
        ins.bind_double(1, -0.0_f64).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t ORDER BY rowid").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_double(0), 0.0);
        sel.step().unwrap();
        // -0.0 is stored as 0.0 in SQLite
        assert_eq!(sel.column_double(0), 0.0);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- text encodings ----

    #[test]
    fn bind_text_unicode_emoji() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let emoji = "Hello \u{1F600}\u{1F4A9}\u{1F680}";
        ins.bind_text(1, emoji).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_text(0).unwrap(), emoji.as_bytes());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_text_cjk() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let cjk = "\u{4e16}\u{754c}\u{4f60}\u{597d}"; // 世界你好
        ins.bind_text(1, cjk).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_text(0).unwrap(), cjk.as_bytes());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_text_rtl_arabic() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        let rtl = "\u{0645}\u{0631}\u{062D}\u{0628}\u{0627}"; // مرحبا
        ins.bind_text(1, rtl).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_text(0).unwrap(), rtl.as_bytes());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_text_zero_width_joiner() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        // Family emoji with ZWJ sequences
        let zwj = "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}";
        ins.bind_text(1, zwj).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_text(0).unwrap(), zwj.as_bytes());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bind_text_empty_string() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_text(1, "").unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val, typeof(val) FROM t").unwrap();
        sel.step().unwrap();
        // Empty string is NOT null
        assert_eq!(sel.column_type(0), raw::SQLITE_TEXT);
        assert_eq!(sel.column_text(0).unwrap(), b"");
        assert_eq!(sel.column_text(1).unwrap(), b"text");
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_string_vs_null() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_text(1, "").unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();
        ins.bind_null(1).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db
            .prepare("SELECT val, typeof(val) FROM t ORDER BY rowid")
            .unwrap();
        // Row 1: empty string
        sel.step().unwrap();
        assert_eq!(sel.column_type(0), raw::SQLITE_TEXT);
        assert!(sel.column_text(0).is_some());
        // Row 2: NULL
        sel.step().unwrap();
        assert_eq!(sel.column_type(0), raw::SQLITE_NULL);
        assert!(sel.column_text(0).is_none());
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- large data ----

    #[test]
    fn large_text_value() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val TEXT)").unwrap();

        let large_text = "x".repeat(1_000_000); // 1MB
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_text(1, &large_text).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_bytes(0), 1_000_000);
        assert_eq!(sel.column_text(0).unwrap().len(), 1_000_000);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn large_blob_value() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val BLOB)").unwrap();

        let large_blob: Vec<u8> = (0..=255).cycle().take(1_000_000).collect();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_blob(1, &large_blob).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_bytes(0), 1_000_000);
        assert_eq!(sel.column_blob(0), &large_blob[..]);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- blob with null bytes ----

    #[test]
    fn blob_with_null_bytes() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val BLOB)").unwrap();

        let data = vec![0x00, 0x01, 0x00, 0xFF, 0x00];
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_blob(1, &data).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db.prepare("SELECT val FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_blob(0), &data[..]);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- empty blob vs null ----

    #[test]
    fn empty_blob_vs_null() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val BLOB)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        ins.bind_blob(1, &[]).unwrap();
        ins.step().unwrap();
        ins.reset().unwrap();
        ins.clear_bindings();
        ins.bind_null(1).unwrap();
        ins.step().unwrap();
        drop(ins);

        let sel = db
            .prepare("SELECT typeof(val), length(val) FROM t ORDER BY rowid")
            .unwrap();
        // Row 1: empty blob -- type is blob, length is 0
        sel.step().unwrap();
        let ty = sel.column_text(0).unwrap();
        assert_eq!(ty, b"blob");
        assert_eq!(sel.column_int64(1), 0);
        // Row 2: NULL -- type is null
        sel.step().unwrap();
        let ty = sel.column_text(0).unwrap();
        assert_eq!(ty, b"null");
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- transaction ----

    #[test]
    fn transaction_commit() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();

        db.exec("BEGIN").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.exec("INSERT INTO t VALUES (2)").unwrap();
        db.exec("COMMIT").unwrap();

        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 2);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transaction_rollback() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();

        db.exec("BEGIN").unwrap();
        db.exec("INSERT INTO t VALUES (1)").unwrap();
        db.exec("INSERT INTO t VALUES (2)").unwrap();
        db.exec("ROLLBACK").unwrap();

        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 0);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- error_message ----

    #[test]
    fn error_message_after_success() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        // After success, error message is "not an error"
        let msg = db.error_message();
        assert_eq!(msg, "not an error");
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn error_message_after_failure() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        let _ = db.exec("NOT VALID SQL");
        let msg = db.error_message();
        assert!(!msg.is_empty());
        assert_ne!(msg, "not an error");
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- DDL then DML ----

    #[test]
    fn ddl_then_dml() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        db.exec("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.exec("ALTER TABLE t ADD COLUMN extra TEXT").unwrap();
        db.exec("UPDATE t SET extra = 'world' WHERE id = 1")
            .unwrap();

        let sel = db.prepare("SELECT id, name, extra FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 1);
        assert_eq!(sel.column_text(1).unwrap(), b"hello");
        assert_eq!(sel.column_text(2).unwrap(), b"world");
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- connection usable after error ----

    #[test]
    fn connection_usable_after_error() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();

        // Cause an error
        let result = db.exec("INSERT INTO nonexistent VALUES (1)");
        assert!(result.is_err());

        // Connection should still work
        db.exec("INSERT INTO t VALUES (42)").unwrap();
        let sel = db.prepare("SELECT id FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 42);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- WAL mode ----

    #[test]
    fn wal_mode_verified() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("PRAGMA journal_mode = WAL").unwrap();

        let sel = db.prepare("PRAGMA journal_mode").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_text(0).unwrap(), b"wal");
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn mmap_size_verified() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("PRAGMA mmap_size = 268435456").unwrap();

        let sel = db.prepare("PRAGMA mmap_size").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 268_435_456);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- many rows ----

    #[test]
    fn many_rows() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (id INTEGER)").unwrap();
        db.exec("BEGIN").unwrap();
        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        for i in 0..10_000 {
            ins.bind_int64(1, i).unwrap();
            ins.step().unwrap();
            ins.reset().unwrap();
            ins.clear_bindings();
        }
        drop(ins);
        db.exec("COMMIT").unwrap();

        let sel = db.prepare("SELECT COUNT(*) FROM t").unwrap();
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 10_000);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }

    // ---- StepResult derives ----

    #[test]
    fn step_result_equality() {
        assert_eq!(StepResult::Row, StepResult::Row);
        assert_eq!(StepResult::Done, StepResult::Done);
        assert_ne!(StepResult::Row, StepResult::Done);
    }

    #[test]
    fn step_result_debug() {
        assert_eq!(format!("{:?}", StepResult::Row), "Row");
        assert_eq!(format!("{:?}", StepResult::Done), "Done");
    }

    #[test]
    fn step_result_clone() {
        let a = StepResult::Row;
        let b = a;
        assert_eq!(a, b);
    }

    // ---- boolean semantics ----

    #[test]
    fn boolean_integers() {
        let path = temp_db_path();
        let db = DbHandle::open(&path, rw_flags()).unwrap();
        db.exec("CREATE TABLE t (val INTEGER)").unwrap();

        let ins = db.prepare("INSERT INTO t VALUES (?1)").unwrap();
        for v in [0i64, 1, -1, 42, i64::MAX] {
            ins.bind_int64(1, v).unwrap();
            ins.step().unwrap();
            ins.reset().unwrap();
            ins.clear_bindings();
        }
        drop(ins);

        let sel = db.prepare("SELECT val FROM t ORDER BY rowid").unwrap();
        // 0 = false
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 0);
        // 1 = true
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 1);
        // -1 = true (nonzero)
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), -1);
        // 42 = true (nonzero)
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), 42);
        // i64::MAX = true
        sel.step().unwrap();
        assert_eq!(sel.column_int64(0), i64::MAX);
        drop(sel);
        drop(db);
        let _ = std::fs::remove_file(&path);
    }
}
