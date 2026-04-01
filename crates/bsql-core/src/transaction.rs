//! Database transactions with commit/rollback and drop-guard semantics.
//!
//! Created via [`Pool::begin()`](crate::pool::Pool::begin). A transaction
//! holds a single connection from the pool for its entire lifetime. Queries
//! executed through the `Executor` trait run within the transaction.
//!
//! # Drop behavior
//!
//! If a `Transaction` is dropped without calling [`commit()`](Transaction::commit)
//! or [`rollback()`](Transaction::rollback), the connection is permanently detached
//! from the pool via `Object::take()` and closed. A warning is logged to stderr.
//! `Drop` is synchronous and cannot send an async `ROLLBACK`, so the connection
//! must be discarded to prevent reuse in a dirty state.
//!
//! Always call `commit()` or `rollback()` explicitly.

use std::fmt;

use crate::error::{BsqlError, BsqlResult};
use crate::pool::PoolConnection;

/// A database transaction.
///
/// Created by [`Pool::begin()`](crate::pool::Pool::begin). Must be explicitly
/// committed via [`commit()`](Transaction::commit). If dropped without
/// `commit()`, the connection is discarded from the pool.
pub struct Transaction {
    /// `None` after `commit()` or `rollback()` consumes the connection.
    /// Since both methods take `self`, user code cannot observe `None` —
    /// this is only `None` during `Drop` after a successful commit.
    conn: Option<PoolConnection>,
    committed: bool,
}

impl Transaction {
    /// Create a new transaction. Called by `Pool::begin()`.
    pub(crate) fn new(conn: PoolConnection) -> Self {
        Self {
            conn: Some(conn),
            committed: false,
        }
    }

    /// Commit the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after commit.
    pub async fn commit(mut self) -> BsqlResult<()> {
        let conn = self
            .conn
            .as_ref()
            .expect("bsql bug: Transaction::commit called but connection already taken");
        match conn.inner.batch_execute("COMMIT").await {
            Ok(()) => {
                self.committed = true;
                // conn drops with self, returning to pool (clean after COMMIT)
                Ok(())
            }
            Err(e) => {
                // COMMIT failed — connection is dirty (aborted transaction).
                // Detach it from the pool so nobody else gets it.
                if let Some(conn) = self.conn.take() {
                    let _ = deadpool_postgres::Object::take(conn.inner);
                }
                self.committed = true; // suppress Drop warning — we handled it
                Err(BsqlError::from(e))
            }
        }
    }

    /// Explicitly roll back the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after rollback.
    pub async fn rollback(mut self) -> BsqlResult<()> {
        let conn = self
            .conn
            .as_ref()
            .expect("bsql bug: Transaction::rollback called but connection already taken");
        match conn.inner.batch_execute("ROLLBACK").await {
            Ok(()) => {
                self.committed = true; // suppress Drop warning — rollback is intentional
                // conn drops with self, returning to pool (clean after ROLLBACK)
                Ok(())
            }
            Err(e) => {
                // ROLLBACK failed — connection is broken. Detach from pool.
                if let Some(conn) = self.conn.take() {
                    let _ = deadpool_postgres::Object::take(conn.inner);
                }
                self.committed = true;
                Err(BsqlError::from(e))
            }
        }
    }

    /// Access the inner connection for `Executor` implementation.
    pub(crate) fn connection(&self) -> &PoolConnection {
        self.conn
            .as_ref()
            .expect("bsql bug: Transaction used after commit/rollback")
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("active", &self.conn.is_some())
            .field("committed", &self.committed)
            .finish()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(conn) = self.conn.take() {
                // Connection has an uncommitted transaction. We cannot send
                // ROLLBACK because Drop is synchronous and ROLLBACK is async.
                //
                // Detach the connection from the pool permanently via
                // Object::take(). This prevents the dirty connection from
                // being handed to the next caller. RecyclingMethod::Fast
                // does NOT run a health-check query, so without this the
                // connection would be reused in an aborted-transaction state.
                //
                // The returned ClientWrapper drops here, closing the TCP
                // connection. The pool slot is freed and a fresh connection
                // will be created on the next acquire().
                let _ = deadpool_postgres::Object::take(conn.inner);
                #[cfg(debug_assertions)]
                eprintln!(
                    "bsql: transaction dropped without commit() or rollback() \
                     — connection discarded from pool"
                );
            }
        }
    }
}
