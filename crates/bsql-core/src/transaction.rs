//! Database transactions with commit/rollback, drop-guard, and lazy-BEGIN.
//!
//! Created via [`Pool::begin()`](crate::pool::Pool::begin). A transaction
//! holds a single connection from the pool for its entire lifetime. Queries
//! executed through the `Executor` trait run within the transaction.
//!
//! # Lazy BEGIN
//!
//! `Pool::begin()` acquires a connection but does NOT send `BEGIN` to
//! PostgreSQL. The `BEGIN` is sent lazily on the first query inside the
//! transaction (via `ensure_begun`). This saves one PG round-trip per
//! transaction, which adds up under high throughput.
//!
//! The lazy approach is transparent to callers: PostgreSQL guarantees
//! read-committed isolation within a transaction regardless of when
//! `BEGIN` is issued relative to the first statement.
//!
//! If a `Transaction` is created and then committed or dropped without
//! executing any queries, no `BEGIN`/`COMMIT`/`ROLLBACK` is sent at all.
//! The connection returns to the pool cleanly.
//!
//! # Drop behavior
//!
//! If a `Transaction` is dropped without calling [`commit()`](Transaction::commit)
//! or [`rollback()`](Transaction::rollback):
//!
//! - **If `BEGIN` was never sent** (no queries executed): the connection is
//!   clean and returns to the pool normally.
//! - **If `BEGIN` was sent**: the connection is dirty. It is permanently
//!   detached from the pool via `Object::take()` and closed. `Drop` is
//!   synchronous and cannot send an async `ROLLBACK`, so the connection
//!   must be discarded to prevent reuse in an aborted-transaction state.
//!
//! Always call `commit()` or `rollback()` explicitly.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::{BsqlError, BsqlResult};
use crate::pool::PoolConnection;

/// A database transaction.
///
/// Created by [`Pool::begin()`](crate::pool::Pool::begin). Must be explicitly
/// committed via [`commit()`](Transaction::commit). If dropped without
/// `commit()`, the connection is discarded from the pool (unless no queries
/// were executed, in which case `BEGIN` was never sent and the connection
/// is clean).
pub struct Transaction {
    /// `None` after `commit()` or `rollback()` consumes the connection.
    /// Since both methods take `self`, user code cannot observe `None` —
    /// this is only `None` during `Drop` after a successful commit.
    conn: Option<PoolConnection>,
    committed: bool,
    /// Whether `BEGIN` has been sent to PostgreSQL. `AtomicBool` provides
    /// interior mutability for `&self` methods (the Executor trait takes
    /// `&self`) while keeping Transaction `Send + Sync`.
    ///
    /// Relaxed ordering suffices: a Transaction is never shared between
    /// tasks (PG connections are not multiplexed), so the only observer
    /// of this flag is the single task that owns the transaction. The
    /// atomic is used solely for interior mutability through `&self`.
    begun: AtomicBool,
}

impl Transaction {
    /// Create a new transaction. Called by `Pool::begin()`.
    ///
    /// Does NOT send `BEGIN` — that is deferred to the first query
    /// via [`ensure_begun`](Transaction::ensure_begun).
    pub(crate) fn new(conn: PoolConnection) -> Self {
        Self {
            conn: Some(conn),
            committed: false,
            begun: AtomicBool::new(false),
        }
    }

    /// Send `BEGIN` to PostgreSQL if not already sent.
    ///
    /// Called at the start of every `Executor` method. The first call
    /// sends `BEGIN`; subsequent calls are a no-op (relaxed atomic load).
    pub(crate) async fn ensure_begun(&self) -> BsqlResult<()> {
        if !self.begun.load(Ordering::Relaxed) {
            self.conn
                .as_ref()
                .expect("bsql bug: Transaction used after commit/rollback")
                .inner
                .batch_execute("BEGIN")
                .await
                .map_err(BsqlError::from)?;
            self.begun.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Commit the transaction and return the connection to the pool.
    ///
    /// Consumes `self` — the transaction cannot be used after commit.
    ///
    /// If no queries were executed (`BEGIN` was never sent), this is a
    /// no-op: no `COMMIT` is sent and the connection returns cleanly.
    pub async fn commit(mut self) -> BsqlResult<()> {
        if !self.begun.load(Ordering::Relaxed) {
            // BEGIN was never sent — nothing to commit.
            // Connection is clean; let it return to the pool via Drop.
            self.committed = true;
            return Ok(());
        }

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
    ///
    /// If no queries were executed (`BEGIN` was never sent), this is a
    /// no-op: no `ROLLBACK` is sent and the connection returns cleanly.
    pub async fn rollback(mut self) -> BsqlResult<()> {
        if !self.begun.load(Ordering::Relaxed) {
            // BEGIN was never sent — nothing to roll back.
            // Connection is clean; let it return to the pool via Drop.
            self.committed = true;
            return Ok(());
        }

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
            .field("begun", &self.begun.load(Ordering::Relaxed))
            .finish()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            if !self.begun.load(Ordering::Relaxed) {
                // BEGIN was never sent — connection is clean.
                // Let it return to the pool normally (conn drops with self).
                return;
            }

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
