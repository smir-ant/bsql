//! PILOT probe — a thin OWN-FFI layer over `libsqlite3-sys` (bundled), measured
//! head-to-head against the rebuild's rusqlite path, IN THE SAME PROCESS on the
//! SAME engine, for the regressed small-read SQLite scenarios.
//!
//! # What this answers
//!
//! The rebuild rides `rusqlite`; the ORIGINAL bsql had its own thin FFI layer
//! over `sqlite3` (`crates/bsql-driver-sqlite/src/ffi.rs`, 62 unsafe sites,
//! plain-prepare statement reuse). The original's recorded by-PK read was
//! **1.73 µs**; the rebuild's streaming by-PK is ~2.8 µs (uncached prepare) and
//! its eager by-PK ~2.05 µs (cached). This probe reconstructs the original's
//! PROVEN shape — open `READWRITE|NOMUTEX` + WAL + `synchronous=NORMAL`, prepare
//! ONCE, then per iteration `bind → step → read every column directly → reset`,
//! byte-for-byte mirroring `bench/c/sqlite_bench_rf.c`'s `consume_rows` — and
//! measures it beside the rebuild's own verbs.
//!
//! # Why same-process, same-engine is the clean comparison
//!
//! Both this probe's `libsqlite3-sys` (bundled) AND the rebuild's `rusqlite`
//! resolve to the SAME `libsqlite3-sys 0.35.0` (SQLite **3.50.2**), so exactly
//! ONE `sqlite3` is linked into this binary. The own-FFI-vs-rebuild delta is
//! therefore PURE wrapper overhead — no engine-version or compile-flag confound.
//! The C reference (`sqlite_bench_rf`, run separately by the harness) links the
//! SYSTEM SQLite **3.51.0** (homebrew, no `SQLITE_ENABLE_API_ARMOR`,
//! `-march=native`), so the probe-vs-C gap isolates the bundled-vs-system
//! engine-build delta — reported honestly, not hidden.
//!
//! This is a `publish = false` bench binary in the STANDALONE `bench` workspace.
//! `unsafe` is sanctioned here exactly as the sibling `libc` peak-RSS FFI is;
//! every block carries a `SAFETY:` note. It NEVER touches a shipped crate.
//!
//! Env:
//!   BENCH_SQLITE_PATH   path to the bench database file (REQUIRED)

use std::hint::black_box;
use std::ops::ControlFlow;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use bsql::sqlite::{BorrowedRow, Connection, SqliteError, ValueRef};

const ITERS: u64 = 10_000;

fn report(label: &str, elapsed: Duration, iters: u64) {
    let ns = elapsed.as_nanos() / u128::from(iters);
    println!("{label}: {ns} ns/op  ({iters} iters)\tKV\t{label}\t{ns}\t{iters}");
}

// ===========================================================================
// The thin OWN-FFI layer — the whole pilot hot path.
//
// This module is the entire complexity price of a driver cutover's read path:
// a raw `*mut sqlite3` / `*mut sqlite3_stmt` behind two `Send` handles that
// finalize/close on drop, and the direct column readers. It mirrors the
// original bsql `ffi.rs` shape, trimmed to what the READ benchmarks touch
// (int/limit bind + every column type read). Count of hot-path unsafe blocks
// is reported at the bottom of this comment for the verdict's complexity line.
// ===========================================================================
mod ffi {
    use std::ffi::{CStr, CString};
    use std::ptr;

    use libsqlite3_sys as raw;

    /// Owned `*mut sqlite3`; closes on drop.
    pub struct Db {
        ptr: *mut raw::sqlite3,
    }

    // SAFETY: opened with SQLITE_OPEN_NOMUTEX and used single-threaded here
    // (the probe never shares a handle across threads). Send is asserted only
    // so the handle can live in a local; it is never actually sent.
    unsafe impl Send for Db {}

    /// A raw error carrying the sqlite result code + message.
    pub struct FfiError {
        pub code: i32,
        pub message: String,
    }

    impl std::fmt::Display for FfiError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "[{}] {}", self.code, self.message)
        }
    }

    fn errmsg(db: *mut raw::sqlite3) -> String {
        if db.is_null() {
            return "null db handle".to_owned();
        }
        // SAFETY: `db` is a live handle (or null, handled above);
        // sqlite3_errmsg returns a NUL-terminated string owned by sqlite.
        let p = unsafe { raw::sqlite3_errmsg(db) };
        if p.is_null() {
            return "unknown error".to_owned();
        }
        // SAFETY: `p` is a live NUL-terminated string valid until the next
        // sqlite API call on `db`; we copy it out immediately.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }

    impl Db {
        /// Open with `READWRITE | NOMUTEX` — byte-identical to the C reference.
        pub fn open(path: &str) -> Result<Self, FfiError> {
            let cpath = CString::new(path).map_err(|_| FfiError {
                code: -1,
                message: "NUL byte in path".to_owned(),
            })?;
            let mut db: *mut raw::sqlite3 = ptr::null_mut();
            let flags = raw::SQLITE_OPEN_READWRITE | raw::SQLITE_OPEN_NOMUTEX;
            // SAFETY: cpath is a live NUL-terminated C string; &mut db receives
            // the handle; flags are valid open flags; null VFS = default.
            let rc = unsafe { raw::sqlite3_open_v2(cpath.as_ptr(), &mut db, flags, ptr::null()) };
            if rc != raw::SQLITE_OK {
                let m = errmsg(db);
                if !db.is_null() {
                    // SAFETY: db was allocated by sqlite3_open_v2 (open can
                    // return a handle even on failure); close releases it.
                    unsafe {
                        raw::sqlite3_close(db);
                    }
                }
                return Err(FfiError { code: rc, message: m });
            }
            Ok(Self { ptr: db })
        }

        /// Run a parameterless statement (PRAGMA). Ignores any result rows.
        pub fn exec(&self, sql: &str) -> Result<(), FfiError> {
            let csql = CString::new(sql).map_err(|_| FfiError {
                code: -1,
                message: "NUL byte in SQL".to_owned(),
            })?;
            // SAFETY: self.ptr is a live handle; csql is a live NUL-terminated
            // C string; no callback, context, or errmsg-out requested.
            let rc = unsafe {
                raw::sqlite3_exec(self.ptr, csql.as_ptr(), None, ptr::null_mut(), ptr::null_mut())
            };
            if rc != raw::SQLITE_OK {
                return Err(FfiError {
                    code: rc,
                    message: errmsg(self.ptr),
                });
            }
            Ok(())
        }

        /// Prepare a statement ONCE (plain, non-persistent — the shape that
        /// leaves SQLite's lookaside pool live, unlike a PERSISTENT cache).
        pub fn prepare(&self, sql: &str) -> Result<Stmt, FfiError> {
            let mut stmt: *mut raw::sqlite3_stmt = ptr::null_mut();
            let csql = CString::new(sql).map_err(|_| FfiError {
                code: -1,
                message: "NUL byte in SQL".to_owned(),
            })?;
            // SAFETY: self.ptr is a live handle; csql is a live NUL-terminated
            // C string, -1 length = read to the NUL; &mut stmt / null tail out.
            let rc = unsafe {
                raw::sqlite3_prepare_v2(self.ptr, csql.as_ptr(), -1, &mut stmt, ptr::null_mut())
            };
            if rc != raw::SQLITE_OK {
                return Err(FfiError {
                    code: rc,
                    message: errmsg(self.ptr),
                });
            }
            Ok(Stmt { ptr: stmt })
        }
    }

    impl Drop for Db {
        fn drop(&mut self) {
            // SAFETY: self.ptr is a live handle; all Stmt created from it are
            // dropped first (they are locals finalized before the Db local).
            unsafe {
                raw::sqlite3_close(self.ptr);
            }
        }
    }

    /// Owned `*mut sqlite3_stmt`; finalizes on drop.
    pub struct Stmt {
        ptr: *mut raw::sqlite3_stmt,
    }

    // SAFETY: tied to a NOMUTEX Db, used single-threaded; see Db's note.
    unsafe impl Send for Stmt {}

    impl Stmt {
        /// Bind an i64 at a 1-based index. (The read scenarios bind only the
        /// id / LIMIT integer, so this is the only bind the hot path needs.)
        #[inline]
        pub fn bind_int64(&self, idx: i32, val: i64) {
            // SAFETY: self.ptr is a live prepared statement; idx is in range
            // (1 = the sole `?1` param); a bind on a reset statement is valid.
            unsafe {
                raw::sqlite3_bind_int64(self.ptr, idx, val);
            }
        }

        /// Number of result columns.
        #[inline]
        pub fn column_count(&self) -> i32 {
            // SAFETY: self.ptr is a live prepared statement.
            unsafe { raw::sqlite3_column_count(self.ptr) }
        }

        /// Step; `true` = a row is ready, `false` = done.
        #[inline]
        pub fn step_row(&self) -> bool {
            // SAFETY: self.ptr is a live prepared statement.
            unsafe { raw::sqlite3_step(self.ptr) == raw::SQLITE_ROW }
        }

        /// Reset for reuse (keeps bindings; clears the step cursor).
        #[inline]
        pub fn reset(&self) {
            // SAFETY: self.ptr is a live prepared statement.
            unsafe {
                raw::sqlite3_reset(self.ptr);
            }
        }

        /// Read + touch every column of the current row exactly as the C
        /// reference's `consume_rows` does: type-dispatch, then the matching
        /// `column_*` reader (TEXT/BLOB also read `column_bytes`, 2 calls).
        #[inline]
        pub fn touch_all_columns(&self, ncols: i32) {
            let mut i = 0;
            while i < ncols {
                // SAFETY: self.ptr is a live statement stepped to a row; i is in
                // [0, ncols); each column_* reader below matches the reported
                // type, and the returned text/blob pointer is read (not stored
                // past the next step/reset).
                unsafe {
                    match raw::sqlite3_column_type(self.ptr, i) {
                        raw::SQLITE_INTEGER => {
                            black_box_i64(raw::sqlite3_column_int64(self.ptr, i));
                        }
                        raw::SQLITE_FLOAT => {
                            black_box_f64(raw::sqlite3_column_double(self.ptr, i));
                        }
                        raw::SQLITE_TEXT => {
                            black_box_ptr(raw::sqlite3_column_text(self.ptr, i).cast());
                            black_box_i32(raw::sqlite3_column_bytes(self.ptr, i));
                        }
                        raw::SQLITE_BLOB => {
                            black_box_ptr(raw::sqlite3_column_blob(self.ptr, i).cast());
                            black_box_i32(raw::sqlite3_column_bytes(self.ptr, i));
                        }
                        _ => {}
                    }
                }
                i += 1;
            }
        }

        /// The ORIGINAL bsql's read shape: for TEXT, derive the length by a
        /// NUL scan of `sqlite3_column_text` (a NUL-terminated string) via
        /// `CStr::from_ptr`, SKIPPING the extra `sqlite3_column_bytes` FFI call
        /// (one fewer C call per text column — the original's documented
        /// micro-opt for short strings). Otherwise identical to `touch_all_columns`.
        #[inline]
        pub fn touch_all_columns_nulscan(&self, ncols: i32) {
            let mut i = 0;
            while i < ncols {
                // SAFETY: as `touch_all_columns`, plus: for TEXT, sqlite guarantees
                // `sqlite3_column_text` returns a NUL-terminated string (or null),
                // so `CStr::from_ptr` reads a valid C string; the bytes are read,
                // not stored past the next step/reset.
                unsafe {
                    match raw::sqlite3_column_type(self.ptr, i) {
                        raw::SQLITE_INTEGER => {
                            black_box_i64(raw::sqlite3_column_int64(self.ptr, i));
                        }
                        raw::SQLITE_FLOAT => {
                            black_box_f64(raw::sqlite3_column_double(self.ptr, i));
                        }
                        raw::SQLITE_TEXT => {
                            let p = raw::sqlite3_column_text(self.ptr, i);
                            if !p.is_null() {
                                let bytes = CStr::from_ptr(p.cast()).to_bytes();
                                black_box_ptr(bytes.as_ptr());
                                black_box_i32(bytes.len() as i32);
                            }
                        }
                        raw::SQLITE_BLOB => {
                            black_box_ptr(raw::sqlite3_column_blob(self.ptr, i).cast());
                            black_box_i32(raw::sqlite3_column_bytes(self.ptr, i));
                        }
                        _ => {}
                    }
                }
                i += 1;
            }
        }
    }

    impl Drop for Stmt {
        fn drop(&mut self) {
            // SAFETY: self.ptr is a live statement; finalize releases it and we
            // never touch the pointer again.
            unsafe {
                raw::sqlite3_finalize(self.ptr);
            }
        }
    }

    // Opaque sinks so the optimizer cannot elide the column reads (mirroring
    // C's `(void)` casts). Kept out of the unsafe blocks for clarity.
    #[inline]
    fn black_box_i64(v: i64) {
        std::hint::black_box(v);
    }
    #[inline]
    fn black_box_f64(v: f64) {
        std::hint::black_box(v);
    }
    #[inline]
    fn black_box_i32(v: i32) {
        std::hint::black_box(v);
    }
    #[inline]
    fn black_box_ptr(v: *const u8) {
        std::hint::black_box(v);
    }
}

// ---------------------------------------------------------------------------
// OWN-FFI cells (prepare once, reuse — the original's proven shape).
// ---------------------------------------------------------------------------

fn ffi_fetch_one(db: &ffi::Db) -> Result<(), ffi::FfiError> {
    let stmt = db.prepare("SELECT id, name, email FROM bench_users WHERE id = ?1")?;
    let ncols = stmt.column_count();
    // warm up
    stmt.bind_int64(1, 42);
    while stmt.step_row() {
        stmt.touch_all_columns(ncols);
    }
    stmt.reset();

    let start = Instant::now();
    for _ in 0..ITERS {
        stmt.bind_int64(1, black_box(42));
        while stmt.step_row() {
            stmt.touch_all_columns(ncols);
        }
        stmt.reset();
    }
    report("ffi_fetch_one", start.elapsed(), ITERS);
    Ok(())
}

fn ffi_fetch_many(db: &ffi::Db, limit: i64) -> Result<(), ffi::FfiError> {
    let stmt = db
        .prepare("SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1")?;
    let ncols = stmt.column_count();
    stmt.bind_int64(1, limit);
    while stmt.step_row() {
        stmt.touch_all_columns(ncols);
    }
    stmt.reset();

    let start = Instant::now();
    for _ in 0..ITERS {
        stmt.bind_int64(1, black_box(limit));
        while stmt.step_row() {
            stmt.touch_all_columns(ncols);
        }
        stmt.reset();
    }
    report(&format!("ffi_fetch_many/{limit}"), start.elapsed(), ITERS);
    Ok(())
}

/// Own-FFI by-PK with the original's NUL-scan text read (one fewer C call per
/// text column). Isolates the read-shape delta at the 1-row fixed cost.
fn ffi_fetch_one_ns(db: &ffi::Db) -> Result<(), ffi::FfiError> {
    let stmt = db.prepare("SELECT id, name, email FROM bench_users WHERE id = ?1")?;
    let ncols = stmt.column_count();
    stmt.bind_int64(1, 42);
    while stmt.step_row() {
        stmt.touch_all_columns_nulscan(ncols);
    }
    stmt.reset();

    let start = Instant::now();
    for _ in 0..ITERS {
        stmt.bind_int64(1, black_box(42));
        while stmt.step_row() {
            stmt.touch_all_columns_nulscan(ncols);
        }
        stmt.reset();
    }
    report("ffi_fetch_one_ns", start.elapsed(), ITERS);
    Ok(())
}

/// Own-FFI multi-row with the original's NUL-scan text read — where saving a
/// `column_bytes` FFI call per text column per row actually accumulates.
fn ffi_fetch_many_ns(db: &ffi::Db, limit: i64) -> Result<(), ffi::FfiError> {
    let stmt = db
        .prepare("SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1")?;
    let ncols = stmt.column_count();
    stmt.bind_int64(1, limit);
    while stmt.step_row() {
        stmt.touch_all_columns_nulscan(ncols);
    }
    stmt.reset();

    let start = Instant::now();
    for _ in 0..ITERS {
        stmt.bind_int64(1, black_box(limit));
        while stmt.step_row() {
            stmt.touch_all_columns_nulscan(ncols);
        }
        stmt.reset();
    }
    report(&format!("ffi_fetch_many_ns/{limit}"), start.elapsed(), ITERS);
    Ok(())
}

// ---------------------------------------------------------------------------
// REBUILD cells (bsql::sqlite over rusqlite) — the SAME verbs `parity_sqlite`
// uses, run here IN-PROCESS on the SAME engine for a same-run baseline.
// ---------------------------------------------------------------------------

fn touch_all(row: &BorrowedRow<'_>) -> ControlFlow<SqliteError> {
    let n = row.column_count();
    for col in 0..n {
        match row.value_ref(col) {
            Ok(v) => {
                black_box(v);
            }
            Err(e) => return ControlFlow::Break(e),
        }
    }
    ControlFlow::Continue(())
}

/// Streaming by-PK (`query_each_params`) — the rebuild's zero-copy path that
/// pays a per-call `prepare` (uncached by design; see `parity_sqlite.rs`).
fn rebuild_fetch_one_stream(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email FROM bench_users WHERE id = ?1";
    let p = [ValueRef::Integer(42)];
    conn.query_each_params(sql, &p, |r| touch_all(&r))?;
    let start = Instant::now();
    for _ in 0..ITERS {
        if let Some(e) = conn.query_each_params(black_box(sql), &p, |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report("rebuild_fetch_one_stream", start.elapsed(), ITERS);
    Ok(())
}

/// Eager at-most-one by-PK (`query_params_one`) — the rebuild's cached path.
fn rebuild_fetch_one_eager(conn: &Connection) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email FROM bench_users WHERE id = ?1";
    let p = [ValueRef::Integer(42)];
    let cols = conn.query_params_one(sql, &p)?.column_count();
    let start = Instant::now();
    for _ in 0..ITERS {
        let row = conn.query_params_one(black_box(sql), &p)?;
        for col in 0..cols {
            black_box(row.value_ref(col)?);
        }
    }
    report("rebuild_fetch_one_eager", start.elapsed(), ITERS);
    Ok(())
}

fn rebuild_fetch_many(conn: &Connection, limit: i64) -> Result<(), SqliteError> {
    let sql = "SELECT id, name, email, active, score FROM bench_users ORDER BY id LIMIT ?1";
    let p = [ValueRef::Integer(limit)];
    conn.query_each_params(sql, &p, |r| touch_all(&r))?;
    let start = Instant::now();
    for _ in 0..ITERS {
        if let Some(e) = conn.query_each_params(black_box(sql), &p, |r| touch_all(&r))? {
            return Err(e);
        }
    }
    report(&format!("rebuild_fetch_many/{limit}"), start.elapsed(), ITERS);
    Ok(())
}

fn run() -> Result<(), String> {
    let path = std::env::var("BENCH_SQLITE_PATH")
        .map_err(|_| "BENCH_SQLITE_PATH must be set".to_owned())?;

    println!("=== ffi_probe: own-FFI vs rebuild (bundled SQLite 3.50.2) ===");
    println!("path={path}\n");

    // OWN-FFI first.
    let db = ffi::Db::open(&path).map_err(|e| format!("ffi open: {e}"))?;
    db.exec("PRAGMA journal_mode=WAL")
        .map_err(|e| format!("ffi wal: {e}"))?;
    db.exec("PRAGMA synchronous=NORMAL")
        .map_err(|e| format!("ffi sync: {e}"))?;
    ffi_fetch_one(&db).map_err(|e| format!("ffi_fetch_one: {e}"))?;
    ffi_fetch_one_ns(&db).map_err(|e| format!("ffi_fetch_one_ns: {e}"))?;
    ffi_fetch_many(&db, 10).map_err(|e| format!("ffi_fetch_many/10: {e}"))?;
    ffi_fetch_many_ns(&db, 10).map_err(|e| format!("ffi_fetch_many_ns/10: {e}"))?;
    ffi_fetch_many(&db, 100).map_err(|e| format!("ffi_fetch_many/100: {e}"))?;
    ffi_fetch_many_ns(&db, 100).map_err(|e| format!("ffi_fetch_many_ns/100: {e}"))?;
    drop(db);

    // REBUILD (rusqlite) in the SAME process, SAME engine.
    let conn = Connection::open(&path).map_err(|e| format!("rebuild open: {e}"))?;
    conn.execute_raw("PRAGMA synchronous=NORMAL")
        .map_err(|e| format!("rebuild sync: {e}"))?;
    rebuild_fetch_one_stream(&conn).map_err(|e| format!("rebuild_fetch_one_stream: {e}"))?;
    rebuild_fetch_one_eager(&conn).map_err(|e| format!("rebuild_fetch_one_eager: {e}"))?;
    rebuild_fetch_many(&conn, 10).map_err(|e| format!("rebuild_fetch_many/10: {e}"))?;
    rebuild_fetch_many(&conn, 100).map_err(|e| format!("rebuild_fetch_many/100: {e}"))?;

    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ffi_probe: {e}");
            ExitCode::FAILURE
        }
    }
}
