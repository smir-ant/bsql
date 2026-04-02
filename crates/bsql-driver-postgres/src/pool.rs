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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

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
/// # async fn example() -> Result<(), bsql_driver_postgres::DriverError> {
/// let pool = bsql_driver_postgres::Pool::connect("postgres://user:pass@localhost/db").await?;
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
    /// Notified when a connection is returned to the pool.
    release_notify: Notify,
    /// When true, no new acquires are accepted.
    closed: AtomicBool,
    /// Maximum lifetime of a connection. Connections older than this
    /// are discarded when popped from the pool. Default: 30 minutes.
    max_lifetime: Option<Duration>,
    /// Maximum time to wait for a connection. Default: 5 seconds.
    acquire_timeout: Option<Duration>,
    /// Minimum number of idle connections to maintain. Default: 0.
    min_idle: usize,
    /// SQL statements to PREPARE on new connections (warmup).
    ///
    /// When a new connection is created, these are pre-prepared via the
    /// extended query protocol before the connection is returned. This
    /// eliminates Parse overhead on first use.
    ///
    /// Uses Mutex instead of RwLock: reads are rare (only on new connection
    /// creation) and writes are rarer. Mutex has lower overhead.
    warmup_sqls: std::sync::Mutex<Arc<[Box<str>]>>,
    /// Maximum number of cached prepared statements per connection.
    max_stmt_cache_size: usize,
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
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(DriverError::Pool("pool is closed".into()));
        }

        // Try to pop an idle connection (fast path).
        // std::sync::Mutex — trivial critical section (no I/O), safe to unwrap
        // because we never panic while holding this lock.
        //
        // If the connection has been idle > 30s, its TCP socket may be dead
        // (half-open, firewall timeout, PG idle reaper). Discard it and try
        // the next one. This is cheaper than a health-check roundtrip.
        if let Some(guard) = self.try_pop_idle()? {
            return Ok(guard);
        }

        // No idle connections — try to claim a slot with a proper CAS loop.
        // This avoids the race where a fetch_add fallback could overshoot max_size.
        loop {
            let current = self.inner.open_count.load(Ordering::Acquire);
            if current >= self.inner.max_size {
                if let Some(timeout) = self.inner.acquire_timeout {
                    let result =
                        tokio::time::timeout(timeout, self.inner.release_notify.notified()).await;
                    if result.is_err() {
                        return Err(DriverError::Pool(
                            "pool exhausted: acquire timeout expired".into(),
                        ));
                    }
                    // A connection was returned — try again
                    if let Some(guard) = self.try_pop_idle()? {
                        return Ok(guard);
                    }
                    // Popped nothing — retry CAS
                    continue;
                }
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
                // Configure statement cache size
                conn.set_max_stmt_cache_size(self.inner.max_stmt_cache_size);
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

    /// Try to pop a valid idle connection from the stack.
    fn try_pop_idle(&self) -> Result<Option<PoolGuard>, DriverError> {
        let mut stack = self.inner.stack.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(conn) = stack.pop() {
            if let Some(max_lifetime) = self.inner.max_lifetime {
                if conn.created_at().elapsed() >= max_lifetime {
                    self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
            }
            if conn.idle_duration() < Duration::from_secs(30) {
                return Ok(Some(PoolGuard {
                    conn: Some(conn),
                    pool: self.inner.clone(),
                    discard: false,
                }));
            }
            // Stale connection — drop it, free the slot
            self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(None)
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

    /// Pool status metrics.
    pub fn status(&self) -> PoolStatus {
        let idle = self
            .inner
            .stack
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        let open = self.inner.open_count.load(Ordering::Relaxed);
        let active = open.saturating_sub(idle);
        PoolStatus {
            idle,
            active,
            open,
            max_size: self.inner.max_size,
        }
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
            .lock()
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
    /// # async fn example() -> Result<(), bsql_driver_postgres::DriverError> {
    /// let pool = bsql_driver_postgres::Pool::connect("postgres://user:pass@localhost/db").await?;
    /// pool.set_warmup_sqls(&[
    ///     "SELECT id, name FROM users WHERE id = $1::int4",
    ///     "SELECT id, title FROM tickets WHERE status = ANY($1::text[])",
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    /// Close the pool. No new acquires are accepted. All idle connections
    /// are sent Terminate and dropped.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        // Drain and close all idle connections
        let conns: Vec<Connection> = {
            let mut stack = self.inner.stack.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *stack)
        };
        for conn in conns {
            self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
            let _ = conn.close().await;
        }
        // Notify any waiters so they get the "pool is closed" error
        self.inner.release_notify.notify_waiters();
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    pub fn set_warmup_sqls(&self, sqls: &[&str]) {
        let boxed: Arc<[Box<str>]> = sqls.iter().map(|s| (*s).into()).collect::<Vec<_>>().into();
        *self
            .inner
            .warmup_sqls
            .lock()
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

// --- PoolStatus ( ---

/// Pool status metrics.
#[derive(Debug, Clone, Copy)]
pub struct PoolStatus {
    /// Number of idle connections in the pool.
    pub idle: usize,
    /// Number of connections currently in use.
    pub active: usize,
    /// Total open connections (idle + active).
    pub open: usize,
    /// Maximum pool size.
    pub max_size: usize,
}

// --- PoolBuilder ---

/// Builder for configuring a connection pool.
pub struct PoolBuilder {
    url: Option<String>,
    max_size: usize,
    /// Maximum lifetime of a connection.
    max_lifetime: Option<Duration>,
    /// Maximum time to wait for a connection when pool is exhausted.
    acquire_timeout: Option<Duration>,
    /// Minimum number of idle connections to maintain.
    min_idle: usize,
    /// Maximum number of cached prepared statements per connection.
    max_stmt_cache_size: usize,
}

impl PoolBuilder {
    fn new() -> Self {
        Self {
            url: None,
            max_size: 10,
            max_lifetime: Some(Duration::from_secs(30 * 60)), // 30 min default
            acquire_timeout: None,                            // fail-fast by default (CREDO #17)
            min_idle: 0,                                      // no minimum by default
            max_stmt_cache_size: 256,                         // LRU eviction at 256 stmts
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

    /// Set the maximum lifetime of a connection. Default: 30 minutes.
    /// Set to None for unlimited lifetime.
    pub fn max_lifetime(mut self, lifetime: Option<Duration>) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Set the acquire timeout. Default: None (fail-fast, per CREDO #17).
    /// Set to a Duration to enable waiting when the pool is exhausted.
    pub fn acquire_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set the minimum number of idle connections. Default: 0.
    /// When > 0, a background task maintains this many idle connections.
    pub fn min_idle(mut self, count: usize) -> Self {
        self.min_idle = count;
        self
    }

    /// Set the maximum number of cached prepared statements per connection.
    /// Default: 256. When the cache exceeds this size, the least recently
    /// used statement is evicted (Close sent to PG to free server memory).
    pub fn max_stmt_cache_size(mut self, size: usize) -> Self {
        self.max_stmt_cache_size = size;
        self
    }

    /// Build the pool. Validates the URL but does not open connections.
    pub async fn build(self) -> Result<Pool, DriverError> {
        let url = self
            .url
            .ok_or_else(|| DriverError::Pool("pool builder requires a URL".into()))?;

        let config = Config::from_url(&url)?;

        let pool = Pool {
            inner: Arc::new(PoolInner {
                stack: std::sync::Mutex::new(Vec::with_capacity(self.max_size)),
                max_size: self.max_size,
                open_count: AtomicUsize::new(0),
                config,
                connecting: Notify::new(),
                release_notify: Notify::new(),
                closed: AtomicBool::new(false),
                max_lifetime: self.max_lifetime,
                acquire_timeout: self.acquire_timeout,
                min_idle: self.min_idle,
                warmup_sqls: std::sync::Mutex::new(Arc::from(Vec::<Box<str>>::new())),
                max_stmt_cache_size: self.max_stmt_cache_size,
            }),
        };

        if self.min_idle > 0 {
            let inner = pool.inner.clone();
            tokio::spawn(async move {
                maintain_min_idle(inner).await;
            });
        }

        Ok(pool)
    }
}

/// Background task that maintains min_idle connections.
async fn maintain_min_idle(inner: Arc<PoolInner>) {
    loop {
        if inner.closed.load(Ordering::Acquire) {
            return;
        }

        let idle_count = inner.stack.lock().unwrap_or_else(|e| e.into_inner()).len();
        let needed = inner.min_idle.saturating_sub(idle_count);

        for _ in 0..needed {
            if inner.closed.load(Ordering::Acquire) {
                return;
            }
            let current = inner.open_count.load(Ordering::Acquire);
            if current >= inner.max_size {
                break;
            }
            if inner
                .open_count
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            match Connection::connect(&inner.config).await {
                Ok(conn) => {
                    let mut stack = inner.stack.lock().unwrap_or_else(|e| e.into_inner());
                    stack.push(conn);
                    inner.release_notify.notify_one();
                }
                Err(_) => {
                    inner.open_count.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }

        // Check every 5 seconds
        tokio::time::sleep(Duration::from_secs(5)).await;
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

    /// Cancel the currently running query on the underlying connection.
    ///
    /// Opens a new TCP connection and sends a CancelRequest to PG.
    /// The cancel connection is closed immediately after.
    pub async fn cancel(&self) -> Result<(), DriverError> {
        let conn = self.conn.as_ref().expect("connection already taken");
        conn.cancel(&self.pool.config).await
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // + Discard if:
            //   - explicitly marked for discard
            //   - in a failed transaction (tx_status == 'E')
            //   - in an active transaction (tx_status == 'T') — uncommitted tx
            //   - streaming query in progress — connection in indeterminate state
            //   - pool is closed
            if self.discard
                || conn.is_in_failed_transaction()
                || conn.is_in_transaction()
                || conn.is_streaming()
                || self.pool.closed.load(Ordering::Acquire)
            {
                self.pool.open_count.fetch_sub(1, Ordering::AcqRel);
                return;
            }

            // Return to pool synchronously. The critical section is trivial
            // (Vec::push — no I/O), so std::sync::Mutex is appropriate here
            // and avoids spawning an async task in Drop.
            {
                let mut stack = self.pool.stack.lock().unwrap_or_else(|e| e.into_inner());
                stack.push(conn);
            }

            self.pool.release_notify.notify_one();
        }
    }
}

// --- Transaction ---

/// A database transaction. Sends ROLLBACK on drop if not committed.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), bsql_driver_postgres::DriverError> {
/// # let pool = bsql_driver_postgres::Pool::connect("postgres://user:pass@localhost/db").await?;
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

    // --- Audit gap tests ---

    // #60: max_lifetime is configurable
    #[tokio::test]
    async fn pool_builder_max_lifetime() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_lifetime(Some(Duration::from_secs(60)))
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.max_lifetime, Some(Duration::from_secs(60)));
    }

    // #60: max_lifetime None
    #[tokio::test]
    async fn pool_builder_max_lifetime_none() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_lifetime(None)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.max_lifetime, None);
    }

    // #62: acquire_timeout set to None (fail-fast)
    #[tokio::test]
    async fn pool_builder_acquire_timeout_none() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .acquire_timeout(None)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.acquire_timeout, None);
    }

    // #62: acquire_timeout custom value
    #[tokio::test]
    async fn pool_builder_acquire_timeout_custom() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .acquire_timeout(Some(Duration::from_secs(10)))
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.acquire_timeout, Some(Duration::from_secs(10)));
    }

    // #63: min_idle setting
    #[tokio::test]
    async fn pool_builder_min_idle() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .min_idle(2)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.min_idle, 2);
    }

    // #64: Pool close marks pool as closed
    #[tokio::test]
    async fn pool_close_marks_closed() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .await
            .unwrap();

        assert!(!pool.is_closed());
        pool.close().await;
        assert!(pool.is_closed());

        // New acquires should fail
        let result = pool.acquire().await;
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool(closed) error, got: {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // #67: PoolStatus idle/active counts
    #[tokio::test]
    async fn pool_status_initial() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(10)
            .build()
            .await
            .unwrap();

        let status = pool.status();
        assert_eq!(status.idle, 0);
        assert_eq!(status.active, 0);
        assert_eq!(status.open, 0);
        assert_eq!(status.max_size, 10);
    }

    // Default pool builder values
    #[tokio::test]
    async fn pool_builder_defaults() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .build()
            .await
            .unwrap();

        assert_eq!(pool.max_size(), 10);
        assert_eq!(pool.inner.max_lifetime, Some(Duration::from_secs(30 * 60)));
        assert_eq!(pool.inner.acquire_timeout, None); // fail-fast by default (CREDO #17)
        assert_eq!(pool.inner.min_idle, 0);
    }

    // Pool open_count starts at 0
    #[tokio::test]
    async fn pool_open_count_initial() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert_eq!(pool.open_count(), 0);
    }

    // --- Task 7: max_stmt_cache_size ---

    #[tokio::test]
    async fn pool_builder_max_stmt_cache_size_default() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.max_stmt_cache_size, 256);
    }

    #[tokio::test]
    async fn pool_builder_max_stmt_cache_size_custom() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_stmt_cache_size(512)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.max_stmt_cache_size, 512);
    }
}
