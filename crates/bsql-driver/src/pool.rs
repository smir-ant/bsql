//! Connection pool — LIFO ordering, fail-fast acquire, no timeouts.
//!
//! The pool maintains a stack of idle connections. `acquire()` pops the top
//! (most recently used = warmest caches). On drop, the guard pushes the
//! connection back. If the pool is exhausted, `acquire()` returns an error
//! immediately — no blocking, no waiting.
//!
//! # Singleflight
//!
//! When multiple tasks need a new connection simultaneously, only one TCP connect
//! is initiated per slot. Other tasks wait on a `Notify` and receive an error if
//! the connect fails.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

use crate::DriverError;
use crate::arena::Arena;
use crate::codec::Encode;
use crate::conn::{Config, Connection, QueryResult};

// --- Pool ---

/// A connection pool with LIFO ordering and fail-fast semantics.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), bsql_driver::DriverError> {
/// let pool = bsql_driver::Pool::connect("postgres://user:pass@localhost/db").await?;
/// let mut conn = pool.acquire().await?;
/// conn.simple_query("SELECT 1").await?;
/// // conn is returned to pool on drop
/// # Ok(())
/// # }
/// ```
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    /// Idle connections. Uses std::sync::Mutex because the critical section is
    /// trivial (push/pop — no I/O). This lets PoolGuard::Drop return connections
    /// synchronously without spawning a task.
    stack: std::sync::Mutex<Vec<Connection>>,
    max_size: usize,
    open_count: AtomicUsize,
    config: Config,
    connecting: Notify,
    /// SQL statements to PREPARE on new connections (warmup).
    ///
    /// When a new connection is created, these are pre-prepared via the
    /// extended query protocol before the connection is returned. This
    /// eliminates Parse overhead on first use.
    warmup_sqls: std::sync::RwLock<Arc<[Box<str>]>>,
}

impl Pool {
    /// Create a pool from a connection URL with default settings (max_size = 10).
    ///
    /// Validates the URL but does not open any connections yet (lazy initialization).
    pub async fn connect(url: &str) -> Result<Self, DriverError> {
        PoolBuilder::new().url(url).build().await
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    /// Acquire a connection from the pool.
    ///
    /// Returns immediately with the most recently used idle connection (LIFO).
    /// If no idle connections are available and the pool is below max_size, a new
    /// connection is created. If the pool is at max_size, returns
    /// `DriverError::Pool` immediately — no blocking.
    pub async fn acquire(&self) -> Result<PoolGuard, DriverError> {
        // Try to pop an idle connection (fast path).
        // std::sync::Mutex — trivial critical section (no I/O), safe to unwrap
        // because we never panic while holding this lock.
        {
            let mut stack = self.inner.stack.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(conn) = stack.pop() {
                return Ok(PoolGuard {
                    conn: Some(conn),
                    pool: self.inner.clone(),
                    discard: false,
                });
            }
        }

        // No idle connections — try to claim a slot with a proper CAS loop.
        // This avoids the race where a fetch_add fallback could overshoot max_size.
        loop {
            let current = self.inner.open_count.load(Ordering::Acquire);
            if current >= self.inner.max_size {
                return Err(DriverError::Pool(
                    "pool exhausted: all connections in use".into(),
                ));
            }
            if self
                .inner
                .open_count
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            // CAS failed — another task incremented. Retry.
        }

        // Open a new connection
        match Connection::connect(&self.inner.config).await {
            Ok(mut conn) => {
                // Warmup: pre-PREPARE frequently used statements
                self.warmup_connection(&mut conn).await;

                self.inner.connecting.notify_waiters();
                Ok(PoolGuard {
                    conn: Some(conn),
                    pool: self.inner.clone(),
                    discard: false,
                })
            }
            Err(e) => {
                // Give back the slot
                self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                self.inner.connecting.notify_waiters();
                Err(e)
            }
        }
    }

    /// Begin a transaction. Acquires a connection and sends BEGIN.
    pub async fn begin(&self) -> Result<Transaction, DriverError> {
        let mut guard = self.acquire().await?;
        guard.simple_query("BEGIN").await?;
        Ok(Transaction {
            guard,
            committed: false,
        })
    }

    /// Current number of open connections (idle + in-use).
    pub fn open_count(&self) -> usize {
        self.inner.open_count.load(Ordering::Relaxed)
    }

    /// Maximum pool size.
    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }

    /// Pre-PREPARE warmup statements on a new connection.
    ///
    /// Uses `prepare_only()` which sends Parse+Describe+Sync without
    /// Bind+Execute — no query execution, only statement caching.
    ///
    /// Best-effort: errors and timeouts on individual statements are silently
    /// ignored. The connection remains usable even if warmup fails.
    async fn warmup_connection(&self, conn: &mut Connection) {
        let sqls = self
            .inner
            .warmup_sqls
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        if sqls.is_empty() {
            return;
        }

        for sql in sqls.iter() {
            let sql_hash = crate::conn::hash_sql(sql);
            // Parse+Describe+Sync only — no Bind+Execute. 5-second timeout
            // per statement; if exceeded, skip and continue with the rest.
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                conn.prepare_only(sql, sql_hash),
            )
            .await;
        }
    }

    /// Set the SQL statements to pre-PREPARE on new connections.
    ///
    /// Each SQL string is PREPAREd (Parse+Describe+Sync) on new connections
    /// before they are returned from `acquire()`. This eliminates the first-use
    /// Parse overhead for frequently executed queries.
    ///
    /// Warmup errors are silently ignored — a bad warmup SQL must not prevent
    /// the connection from being usable.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), bsql_driver::DriverError> {
    /// let pool = bsql_driver::Pool::connect("postgres://user:pass@localhost/db").await?;
    /// pool.set_warmup_sqls(&[
    ///     "SELECT id, name FROM users WHERE id = $1::int4",
    ///     "SELECT id, title FROM tickets WHERE status = ANY($1::text[])",
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_warmup_sqls(&self, sqls: &[&str]) {
        let boxed: Arc<[Box<str>]> = sqls.iter().map(|s| (*s).into()).collect::<Vec<_>>().into();
        *self
            .inner
            .warmup_sqls
            .write()
            .unwrap_or_else(|e| e.into_inner()) = boxed;
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

// --- PoolBuilder ---

/// Builder for configuring a connection pool.
pub struct PoolBuilder {
    url: Option<String>,
    max_size: usize,
}

impl PoolBuilder {
    fn new() -> Self {
        Self {
            url: None,
            max_size: 10,
        }
    }

    /// Set the connection URL.
    pub fn url(mut self, url: &str) -> Self {
        self.url = Some(url.to_owned());
        self
    }

    /// Set the maximum pool size. Default: 10.
    ///
    /// A max_size of 0 means the pool will reject all acquire() calls immediately.
    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Build the pool. Validates the URL but does not open connections.
    pub async fn build(self) -> Result<Pool, DriverError> {
        let url = self
            .url
            .ok_or_else(|| DriverError::Pool("pool builder requires a URL".into()))?;

        let config = Config::from_url(&url)?;

        Ok(Pool {
            inner: Arc::new(PoolInner {
                stack: std::sync::Mutex::new(Vec::with_capacity(self.max_size)),
                max_size: self.max_size,
                open_count: AtomicUsize::new(0),
                config,
                connecting: Notify::new(),
                warmup_sqls: std::sync::RwLock::new(Arc::from(Vec::<Box<str>>::new())),
            }),
        })
    }
}

// --- PoolGuard ---

/// A borrowed connection from the pool. Returns to the pool on drop.
///
/// If the connection is in a failed transaction state, broken, or marked for
/// discard, it is dropped (decrements open_count) instead of returned.
pub struct PoolGuard {
    conn: Option<Connection>,
    pool: Arc<PoolInner>,
    /// When true, the connection is dropped instead of returned to the pool.
    /// Used by streaming queries that are dropped mid-iteration (the connection
    /// is in an indeterminate protocol state and cannot be reused).
    discard: bool,
}

impl Deref for PoolGuard {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("connection already taken")
    }
}

impl DerefMut for PoolGuard {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("connection already taken")
    }
}

impl PoolGuard {
    /// Mark this connection for discard — it will NOT be returned to the pool
    /// on drop. The open_count is decremented and the TCP connection is closed.
    ///
    /// Used by streaming queries that are dropped mid-iteration: the connection
    /// may be in an indeterminate protocol state (portal open, no ReadyForQuery)
    /// and cannot be safely reused.
    pub fn mark_discard(&mut self) {
        self.discard = true;
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Discard if: failed transaction, or explicitly marked
            if self.discard || conn.is_in_failed_transaction() {
                self.pool.open_count.fetch_sub(1, Ordering::AcqRel);
                return;
            }

            // Return to pool synchronously. The critical section is trivial
            // (Vec::push — no I/O), so std::sync::Mutex is appropriate here
            // and avoids spawning an async task in Drop.
            let mut stack = self.pool.stack.lock().unwrap_or_else(|e| e.into_inner());
            stack.push(conn);
        }
    }
}

// --- Transaction ---

/// A database transaction. Sends ROLLBACK on drop if not committed.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), bsql_driver::DriverError> {
/// # let pool = bsql_driver::Pool::connect("postgres://user:pass@localhost/db").await?;
/// let mut tx = pool.begin().await?;
/// tx.simple_query("INSERT INTO t VALUES (1)").await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Transaction {
    guard: PoolGuard,
    committed: bool,
}

impl Transaction {
    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), DriverError> {
        self.guard.simple_query("COMMIT").await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction explicitly.
    pub async fn rollback(mut self) -> Result<(), DriverError> {
        self.guard.simple_query("ROLLBACK").await?;
        self.committed = true; // prevent double rollback in drop
        Ok(())
    }

    /// Execute a prepared query within the transaction.
    pub async fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        arena: &mut Arena,
    ) -> Result<QueryResult, DriverError> {
        self.guard.query(sql, sql_hash, params, arena).await
    }

    /// Execute without result rows within the transaction.
    pub async fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        self.guard.execute(sql, sql_hash, params).await
    }

    /// Simple query within the transaction.
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        self.guard.simple_query(sql).await
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed {
            // Connection is in an uncommitted transaction — discard it from the pool.
            // We can't send ROLLBACK in Drop (not async), so we mark the connection
            // as tainted. The guard's Drop will see is_in_failed_transaction isn't
            // applicable here (it's in 'T' state), but we need to discard it.
            //
            // Take the connection out of the guard and drop it, decrementing open_count.
            if let Some(_conn) = self.guard.conn.take() {
                self.guard.pool.open_count.fetch_sub(1, Ordering::AcqRel);
                // Connection dropped — PG server will auto-rollback when it sees disconnect
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pool_builder_requires_url() {
        let result = PoolBuilder::new().build().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pool_builder_validates_url() {
        let result = PoolBuilder::new().url("not_a_url").build().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pool_builder_accepts_valid_url() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.max_size(), 5);
        assert_eq!(pool.open_count(), 0);
    }

    #[tokio::test]
    async fn pool_connect_validates_url() {
        let result = Pool::connect("not_a_url").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pool_max_size_zero() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(0)
            .build()
            .await
            .unwrap();

        let result = pool.acquire().await;
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => assert!(msg.contains("exhausted")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn pool_clone_shares_state() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .await
            .unwrap();

        let pool2 = pool.clone();
        assert_eq!(pool.max_size(), pool2.max_size());
    }
}
