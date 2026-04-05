//! Database transactions with commit/rollback.
//!
//! Created via [`Pool::begin()`](crate::pool::Pool::begin). A transaction
//! holds a single connection from the pool for its entire lifetime. Queries
//! executed through the `Executor` trait run within the transaction.
//!
//! # Drop behavior
//!
//! If a `Transaction` is dropped without calling [`commit()`](Transaction::commit)
//! or [`rollback()`](Transaction::rollback), the driver discards the connection
//! from the pool. PostgreSQL auto-rollbacks when the connection closes. A warning
//! is emitted via `eprintln!` to help detect forgotten commits during development.

use std::fmt;
use std::sync::Mutex;

use bsql_driver_postgres::arena::acquire_arena;
use bsql_driver_postgres::codec::Encode;

use crate::error::{BsqlError, BsqlResult, QueryError};
use crate::executor::OwnedResult;

/// Transaction isolation levels supported by PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl IsolationLevel {
    /// SQL representation for `SET TRANSACTION ISOLATION LEVEL ...`.
    fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_sql())
    }
}

/// A database transaction.
///
/// Created by [`Pool::begin()`](crate::pool::Pool::begin). Must be explicitly
/// committed via [`commit()`](Transaction::commit). If dropped without
/// `commit()`, the connection is discarded from the pool and a warning is logged.
///
/// Use `.defer(&tx)` on queries to buffer writes, then `tx.commit()` to flush
/// them all in a single pipeline. Use `.run(&tx)` or `.fetch(&tx)` for immediate
/// execution within the transaction.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::Pool;
///
/// let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;
/// let tx = pool.begin()?;
///
/// // Buffer writes with .defer() — nothing hits the network yet
/// bsql::query!("INSERT INTO log (msg) VALUES ($msg: &str)")
///     .defer(&tx)?;
///
/// // Or execute immediately within the transaction
/// bsql::query!("UPDATE accounts SET balance = 0 WHERE id = $id: i32")
///     .run(&tx)?;
///
/// // commit() flushes all deferred operations, then commits
/// tx.commit()?;
/// ```
pub struct Transaction {
    inner: Mutex<Option<bsql_driver_postgres::Transaction>>,
    /// Set to true when commit() or rollback() is called.
    finished: bool,
}

impl Transaction {
    /// Wrap a driver-level transaction.
    pub(crate) fn from_driver(tx: bsql_driver_postgres::Transaction) -> Self {
        Self {
            inner: Mutex::new(Some(tx)),
            finished: false,
        }
    }

    /// Return a "transaction already consumed" error.
    fn consumed_error() -> BsqlError {
        BsqlError::Query(QueryError {
            message: "transaction already consumed".into(),
            pg_code: None,
            source: None,
        })
    }

    /// Commit the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after commit.
    pub fn commit(mut self) -> BsqlResult<()> {
        self.finished = true;
        let tx = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(Self::consumed_error)?;
        tx.commit().map_err(BsqlError::from)
    }

    /// Explicitly roll back the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after rollback.
    pub fn rollback(mut self) -> BsqlResult<()> {
        self.finished = true;
        let tx = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(Self::consumed_error)?;
        tx.rollback().map_err(BsqlError::from)
    }

    /// Create a savepoint within the transaction.
    ///
    /// The `name` must be a valid SQL identifier: ASCII alphanumeric and
    /// underscores only, starting with a letter or underscore. Maximum 63 characters.
    pub fn savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let sql = format!("SAVEPOINT {name}");
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql).map_err(BsqlError::from_driver_query)
    }

    /// Release (destroy) a savepoint, keeping its effects.
    ///
    /// The `name` must match a previously created savepoint.
    pub fn release_savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let sql = format!("RELEASE SAVEPOINT {name}");
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql).map_err(BsqlError::from_driver_query)
    }

    /// Roll back to a savepoint, undoing changes made after it was created.
    ///
    /// The savepoint remains valid after this call (can be rolled back to again).
    pub fn rollback_to(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let sql = format!("ROLLBACK TO SAVEPOINT {name}");
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql).map_err(BsqlError::from_driver_query)
    }

    /// Set the isolation level for this transaction.
    ///
    /// Must be called before the first query in the transaction (immediately
    /// after `begin()`). PostgreSQL rejects `SET TRANSACTION` after any
    /// data-modifying statement.
    pub fn set_isolation(&self, level: IsolationLevel) -> BsqlResult<()> {
        let sql = format!("SET TRANSACTION ISOLATION LEVEL {}", level.as_sql());
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql).map_err(BsqlError::from_driver_query)
    }

    /// Execute a query within the transaction (used by Executor impl).
    pub(crate) fn query_inner(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<OwnedResult> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        let mut arena = acquire_arena();
        let result = tx
            .query(sql, sql_hash, params, &mut arena)
            .map_err(BsqlError::from_driver_query)?;
        Ok(OwnedResult::new(result, arena))
    }

    /// Execute without result rows within the transaction (used by Executor impl).
    pub(crate) fn execute_inner(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<u64> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.execute(sql, sql_hash, params)
            .map_err(BsqlError::from_driver_query)
    }

    /// Execute the same statement N times with different params in one pipeline.
    ///
    /// Sends all N Bind+Execute messages + one Sync. One round-trip for
    /// N operations within the transaction. Returns the affected row count
    /// for each parameter set.
    pub fn execute_pipeline(
        &self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> BsqlResult<Vec<u64>> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.execute_pipeline(sql, sql_hash, param_sets)
            .map_err(BsqlError::from_driver_query)
    }

    // --- Deferred pipeline API ---

    /// Buffer an execute for deferred pipeline flush.
    ///
    /// The operation is not sent to the server immediately. Instead, the
    /// Bind+Execute message bytes are buffered internally. The buffered
    /// operations are sent as a single pipeline on [`commit()`](Self::commit)
    /// or [`flush_deferred()`](Self::flush_deferred).
    ///
    /// If the statement has not been prepared yet, a single round-trip is
    /// made to prepare it. After that, the Bind+Execute bytes are buffered
    /// with no I/O.
    ///
    /// Any read operation (`query_inner`, `for_each_raw`, `simple_query`, etc.)
    /// automatically flushes deferred operations first to ensure
    /// read-your-writes consistency.
    #[doc(hidden)]
    pub fn defer_execute(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<()> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.defer_execute(sql, sql_hash, params)
            .map_err(BsqlError::from_driver_query)
    }

    /// Flush all deferred operations as a single pipeline.
    ///
    /// Sends all buffered Bind+Execute messages + one Sync in a single TCP write.
    /// Returns the affected row count for each deferred operation.
    #[doc(hidden)]
    pub fn flush_deferred(&self) -> BsqlResult<Vec<u64>> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.flush_deferred().map_err(BsqlError::from_driver_query)
    }

    /// Number of operations currently buffered for deferred execution.
    ///
    /// This is a diagnostic method primarily for testing. Most users should
    /// not need to call this -- deferred operations are flushed automatically
    /// on commit or before any read.
    #[doc(hidden)]
    pub fn deferred_count(&self) -> usize {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_ref() {
            Some(tx) => tx.deferred_count(),
            None => 0,
        }
    }

    /// Process each row directly from the wire buffer within this transaction.
    ///
    /// Zero arena allocation — the closure receives a `PgDataRow` that reads
    /// columns directly from the DataRow message bytes.
    pub fn for_each_raw<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(bsql_driver_postgres::PgDataRow<'_>) -> BsqlResult<()>,
    {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        let mut user_err: Option<BsqlError> = None;
        let driver_result = tx.for_each(sql, sql_hash, params, |row| match f(row) {
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

    /// Process each DataRow as raw bytes within this transaction.
    ///
    /// Like `for_each_raw` but passes the raw `&[u8]` DataRow payload directly
    /// to the closure — no `PgDataRow` construction, no SmallVec pre-scan.
    #[doc(hidden)]
    pub fn __for_each_raw_bytes<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(&[u8]) -> BsqlResult<()>,
    {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        let mut user_err: Option<BsqlError> = None;
        let driver_result = tx.for_each_raw(sql, sql_hash, params, |data| match f(data) {
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

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("finished", &self.finished)
            .finish()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            // The transaction was dropped without commit() or rollback().
            // The driver-level Transaction::drop discards the connection from the
            // pool — PG server auto-rollbacks when it sees the disconnect.
            // Log a warning to help catch forgotten commits during development.
            eprintln!(
                "bsql: Transaction dropped without commit() or rollback() — \
                 connection discarded from pool. This is safe but wasteful."
            );
        }
    }
}

/// Delegate to shared savepoint name validator.
fn validate_savepoint_name(name: &str) -> BsqlResult<()> {
    crate::util::validate_savepoint_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_savepoint_name_valid() {
        assert!(validate_savepoint_name("sp1").is_ok());
        assert!(validate_savepoint_name("_sp").is_ok());
        assert!(validate_savepoint_name("my_savepoint_123").is_ok());
    }

    #[test]
    fn validate_savepoint_name_empty() {
        assert!(validate_savepoint_name("").is_err());
    }

    #[test]
    fn validate_savepoint_name_too_long() {
        let long = "a".repeat(64);
        assert!(validate_savepoint_name(&long).is_err());
    }

    #[test]
    fn validate_savepoint_name_max_length() {
        let max = "a".repeat(63);
        assert!(validate_savepoint_name(&max).is_ok());
    }

    #[test]
    fn validate_savepoint_name_starts_with_digit() {
        assert!(validate_savepoint_name("1sp").is_err());
    }

    #[test]
    fn validate_savepoint_name_starts_with_underscore() {
        assert!(validate_savepoint_name("_sp").is_ok());
    }

    #[test]
    fn validate_savepoint_name_special_chars() {
        assert!(validate_savepoint_name("sp-1").is_err());
        assert!(validate_savepoint_name("sp.1").is_err());
        assert!(validate_savepoint_name("sp 1").is_err());
        assert!(validate_savepoint_name("sp;1").is_err());
        assert!(validate_savepoint_name("sp'1").is_err());
    }

    #[test]
    fn isolation_level_display() {
        assert_eq!(
            IsolationLevel::ReadUncommitted.to_string(),
            "READ UNCOMMITTED"
        );
        assert_eq!(IsolationLevel::ReadCommitted.to_string(), "READ COMMITTED");
        assert_eq!(
            IsolationLevel::RepeatableRead.to_string(),
            "REPEATABLE READ"
        );
        assert_eq!(IsolationLevel::Serializable.to_string(), "SERIALIZABLE");
    }

    // --- IsolationLevel traits ---

    #[test]
    fn isolation_level_clone() {
        let level = IsolationLevel::Serializable;
        let cloned = level;
        assert_eq!(level, cloned);
    }

    #[test]
    fn isolation_level_debug() {
        let level = IsolationLevel::RepeatableRead;
        let dbg = format!("{level:?}");
        assert!(
            dbg.contains("RepeatableRead"),
            "Debug should show variant name: {dbg}"
        );
    }

    #[test]
    fn isolation_level_eq() {
        assert_eq!(IsolationLevel::Serializable, IsolationLevel::Serializable);
        assert_ne!(IsolationLevel::Serializable, IsolationLevel::ReadCommitted);
    }

    // --- Transaction Debug ---

    #[test]
    fn transaction_debug_shows_finished_false() {
        // Transaction cannot be constructed in tests without a driver,
        // but we verify the Debug impl exists at compile time.
        fn _assert_debug<T: std::fmt::Debug>() {}
        _assert_debug::<Transaction>();
    }

    // --- Send + Sync assertions ---

    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    #[test]
    fn transaction_is_send() {
        _assert_send::<Transaction>();
    }

    #[test]
    fn transaction_is_sync() {
        _assert_sync::<Transaction>();
    }

    #[test]
    fn isolation_level_is_send_and_sync() {
        _assert_send::<IsolationLevel>();
        _assert_sync::<IsolationLevel>();
    }
}
