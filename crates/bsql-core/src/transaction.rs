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

use bsql_driver_postgres::arena::acquire_arena;
use bsql_driver_postgres::codec::Encode;
use tokio::sync::Mutex;

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
/// Uses `tokio::sync::Mutex` for interior mutability because the driver's
/// `Transaction` requires `&mut self` but the `Executor` trait takes `&self`.
/// The mutex is uncontended in practice — a transaction is used by one task
/// at a time. `tokio::sync::Mutex` (over `RefCell`) is required because the
/// future holding the lock must be `Send` for tokio task migration.
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
    pub async fn commit(mut self) -> BsqlResult<()> {
        self.finished = true;
        let tx = self
            .inner
            .lock()
            .await
            .take()
            .ok_or_else(Self::consumed_error)?;
        tx.commit().await.map_err(BsqlError::from)
    }

    /// Explicitly roll back the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after rollback.
    pub async fn rollback(mut self) -> BsqlResult<()> {
        self.finished = true;
        let tx = self
            .inner
            .lock()
            .await
            .take()
            .ok_or_else(Self::consumed_error)?;
        tx.rollback().await.map_err(BsqlError::from)
    }

    /// Create a savepoint within the transaction.
    ///
    /// The `name` must be a valid SQL identifier: ASCII alphanumeric and
    /// underscores only, starting with a letter or underscore. Maximum 63 characters.
    pub async fn savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let sql = format!("SAVEPOINT {name}");
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql)
            .await
            .map_err(BsqlError::from_driver_query)
    }

    /// Release (destroy) a savepoint, keeping its effects.
    ///
    /// The `name` must match a previously created savepoint.
    pub async fn release_savepoint(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let sql = format!("RELEASE SAVEPOINT {name}");
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql)
            .await
            .map_err(BsqlError::from_driver_query)
    }

    /// Roll back to a savepoint, undoing changes made after it was created.
    ///
    /// The savepoint remains valid after this call (can be rolled back to again).
    pub async fn rollback_to(&self, name: &str) -> BsqlResult<()> {
        validate_savepoint_name(name)?;
        let sql = format!("ROLLBACK TO SAVEPOINT {name}");
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql)
            .await
            .map_err(BsqlError::from_driver_query)
    }

    /// Set the isolation level for this transaction.
    ///
    /// Must be called before the first query in the transaction (immediately
    /// after `begin()`). PostgreSQL rejects `SET TRANSACTION` after any
    /// data-modifying statement.
    pub async fn set_isolation(&self, level: IsolationLevel) -> BsqlResult<()> {
        let sql = format!("SET TRANSACTION ISOLATION LEVEL {}", level.as_sql());
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.simple_query(&sql)
            .await
            .map_err(BsqlError::from_driver_query)
    }

    /// Execute a query within the transaction (used by Executor impl).
    pub(crate) async fn query_inner(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<OwnedResult> {
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        let mut arena = acquire_arena();
        let result = tx
            .query(sql, sql_hash, params, &mut arena)
            .await
            .map_err(BsqlError::from_driver_query)?;
        Ok(OwnedResult::new(result, arena))
    }

    /// Execute without result rows within the transaction (used by Executor impl).
    pub(crate) async fn execute_inner(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<u64> {
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        tx.execute(sql, sql_hash, params)
            .await
            .map_err(BsqlError::from_driver_query)
    }

    /// Process each row directly from the wire buffer within this transaction.
    ///
    /// Zero arena allocation — the closure receives a `PgDataRow` that reads
    /// columns directly from the DataRow message bytes.
    pub async fn for_each_raw<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(bsql_driver_postgres::PgDataRow<'_>) -> BsqlResult<()>,
    {
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        let mut user_err: Option<BsqlError> = None;
        let driver_result = tx
            .for_each(sql, sql_hash, params, |row| match f(row) {
                Ok(()) => Ok(()),
                Err(e) => {
                    user_err = Some(e);
                    Err(bsql_driver_postgres::DriverError::Protocol(
                        "for_each closure error".into(),
                    ))
                }
            })
            .await;
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
    pub async fn __for_each_raw_bytes<F>(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> BsqlResult<()>
    where
        F: FnMut(&[u8]) -> BsqlResult<()>,
    {
        let mut guard = self.inner.lock().await;
        let tx = guard.as_mut().ok_or_else(Self::consumed_error)?;
        let mut user_err: Option<BsqlError> = None;
        let driver_result = tx
            .for_each_raw(sql, sql_hash, params, |data| match f(data) {
                Ok(()) => Ok(()),
                Err(e) => {
                    user_err = Some(e);
                    Err(bsql_driver_postgres::DriverError::Protocol(
                        "for_each closure error".into(),
                    ))
                }
            })
            .await;
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
}
