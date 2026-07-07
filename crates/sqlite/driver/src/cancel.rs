//! Interrupt-based query cancellation for the SQLite driver:
//! [`SqliteCancelToken`].
//!
//! The cross-backend twin of the PostgreSQL driver's `CancelToken`: it reads the
//! SAME on both backends — `conn.cancel_token()` mints a detached token, and
//! `token.cancel()` requests cancellation of the query in flight — so a consumer
//! carries ONE mental model across PostgreSQL and SQLite.
//!
//! Where PostgreSQL cancels OUT-OF-BAND (a second network connection), SQLite
//! cancels IN-PROCESS via `sqlite3_interrupt`: the token wraps rusqlite's
//! [`InterruptHandle`](rusqlite::InterruptHandle) (a `Send + Sync` handle sharing
//! the same `sqlite3*`), and `cancel()` is INFALLIBLE — there is no socket to
//! open, no packet to deliver, so it returns `()` rather than a `Result`. The
//! `unsafe impl Send/Sync` that makes the handle thread-shareable lives inside
//! trusted rusqlite; this driver stays `#![forbid(unsafe_code)]`.

/// A detached capability to interrupt the query in flight on the
/// [`Connection`](crate::Connection) it was minted from — the SQLite twin of the
/// PostgreSQL `CancelToken`.
///
/// Minted by [`Connection::cancel_token`](crate::Connection::cancel_token), it is
/// `Send + Sync + 'static` and borrows NOTHING from the connection, so the
/// canonical use is to obtain it before a long query and hand it to another
/// thread that calls [`cancel`](Self::cancel) mid-query:
///
/// ```no_run
/// # fn demo(conn: &bsql_sqlite::Connection) {
/// let token = conn.cancel_token();            // obtained BEFORE the long query
/// let canceller = std::thread::spawn(move || {
///     std::thread::sleep(std::time::Duration::from_millis(50));
///     token.cancel();                          // from another thread, mid-query
/// });
/// // A long/compute-bound query returns `Err(SqliteError::Interrupted)`.
/// let _ = conn.query_sql("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c");
/// canceller.join().ok();
/// # }
/// ```
///
/// # Effect
///
/// `cancel()` makes the running (or next) `sqlite3_step` on that connection
/// return `SQLITE_INTERRUPT`, which the driver classifies as
/// [`SqliteError::Interrupted`](crate::SqliteError::Interrupted). The connection
/// is REUSABLE afterward — the interrupt aborts the statement, not the
/// connection. A cancel with no query running is harmless (it interrupts the
/// next step if one starts promptly, else is a no-op). Unlike the best-effort
/// network PostgreSQL cancel, `sqlite3_interrupt` reliably stops the targeted
/// statement.
pub struct SqliteCancelToken {
    handle: rusqlite::InterruptHandle,
}

impl core::fmt::Debug for SqliteCancelToken {
    /// `rusqlite::InterruptHandle` is not `Debug`, and the raw `sqlite3*` it wraps
    /// is not meaningful to print, so the handle is elided.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SqliteCancelToken").finish_non_exhaustive()
    }
}

// Footprint pin: the token is exactly rusqlite's `InterruptHandle` (an `Arc` to
// the shared db handle) — one word. A widened token trips this.
crate::footprint_pin!(SqliteCancelToken, size = 8, align = 8);

// Tier-1 static assertion: the token is a DETACHED capability — Send + Sync +
// 'static so it can move to another thread and interrupt the owning connection's
// query in flight (rusqlite's `unsafe impl Send/Sync for InterruptHandle` makes
// this sound; this driver stays `#![forbid(unsafe_code)]`).
const _: () = {
    const fn assert_send_sync_static<T: Send + Sync + 'static>() {}
    assert_send_sync_static::<SqliteCancelToken>();
};

impl SqliteCancelToken {
    /// Wrap an interrupt handle. The driver-facing seam (`#[doc(hidden)]`),
    /// called by [`Connection::cancel_token`](crate::Connection::cancel_token).
    #[doc(hidden)]
    #[must_use]
    pub fn new(handle: rusqlite::InterruptHandle) -> Self {
        Self { handle }
    }

    /// Interrupt the in-flight query on the owning connection.
    ///
    /// Infallible (no I/O): the running `sqlite3_step` returns `SQLITE_INTERRUPT`,
    /// surfaced by the driver as
    /// [`SqliteError::Interrupted`](crate::SqliteError::Interrupted). Callable
    /// from any thread; a double cancel is harmless.
    pub fn cancel(&self) {
        self.handle.interrupt();
    }
}
