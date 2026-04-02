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
//! from the pool. PostgreSQL auto-rollbacks when the connection closes.

use std::fmt;

use bsql_driver::arena::acquire_arena;
use bsql_driver::codec::Encode;
use tokio::sync::Mutex;

use crate::error::{BsqlError, BsqlResult};
use crate::executor::OwnedResult;

/// A database transaction.
///
/// Created by [`Pool::begin()`](crate::pool::Pool::begin). Must be explicitly
/// committed via [`commit()`](Transaction::commit). If dropped without
/// `commit()`, the connection is discarded from the pool.
pub struct Transaction {
    inner: Mutex<bsql_driver::Transaction>,
}

impl Transaction {
    /// Wrap a driver-level transaction.
    pub(crate) fn from_driver(tx: bsql_driver::Transaction) -> Self {
        Self {
            inner: Mutex::new(tx),
        }
    }

    /// Commit the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after commit.
    pub async fn commit(self) -> BsqlResult<()> {
        let tx = self.inner.into_inner();
        tx.commit().await.map_err(BsqlError::from)
    }

    /// Explicitly roll back the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after rollback.
    pub async fn rollback(self) -> BsqlResult<()> {
        let tx = self.inner.into_inner();
        tx.rollback().await.map_err(BsqlError::from)
    }

    /// Execute a query within the transaction (used by Executor impl).
    pub(crate) async fn query_inner(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<OwnedResult> {
        let mut tx = self.inner.lock().await;
        let mut arena = acquire_arena();
        let result = tx
            .query(sql, sql_hash, params, &mut arena)
            .await
            .map_err(BsqlError::from)?;
        Ok(OwnedResult::new(result, arena))
    }

    /// Execute without result rows within the transaction (used by Executor impl).
    pub(crate) async fn execute_inner(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<u64> {
        let mut tx = self.inner.lock().await;
        tx.execute(sql, sql_hash, params)
            .await
            .map_err(BsqlError::from)
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction").finish()
    }
}
