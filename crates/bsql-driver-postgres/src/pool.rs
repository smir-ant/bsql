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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use crate::DriverError;
use crate::arena::Arena;
use crate::codec::Encode;
use crate::conn::Connection;
use crate::types::{Config, PgDataRow, QueryResult};
#[cfg(unix)]
use crate::sync_conn::SyncConnection;

// --- PoolSlot: async Connection or sync SyncConnection ---

/// Internal enum for connections in the pool.
///
/// When the pool URL points to a Unix domain socket (`host` starts with `/`),
/// connections are created as `Sync` variants using `SyncConnection` (blocking
/// I/O). For TCP connections, the `Async` variant uses tokio's async `Connection`.
///
/// This is an implementation detail — callers interact with `PoolGuard` which
/// dispatches transparently.
enum PoolSlot {
    Async(Connection),
    #[cfg(unix)]
    Sync(SyncConnection),
}

impl PoolSlot {
    fn created_at(&self) -> std::time::Instant {
        match self {
            PoolSlot::Async(c) => c.created_at(),
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.created_at(),
        }
    }

    fn idle_duration(&self) -> Duration {
        match self {
            PoolSlot::Async(c) => c.idle_duration(),
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.idle_duration(),
        }
    }

    fn is_in_failed_transaction(&self) -> bool {
        match self {
            PoolSlot::Async(c) => c.is_in_failed_transaction(),
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.is_in_failed_transaction(),
        }
    }

    fn is_in_transaction(&self) -> bool {
        match self {
            PoolSlot::Async(c) => c.is_in_transaction(),
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.is_in_transaction(),
        }
    }

    fn is_streaming(&self) -> bool {
        match self {
            PoolSlot::Async(c) => c.is_streaming(),
            // SyncConnection has no streaming mode
            #[cfg(unix)]
            PoolSlot::Sync(_) => false,
        }
    }

    fn set_max_stmt_cache_size(&mut self, size: usize) {
        match self {
            PoolSlot::Async(c) => c.set_max_stmt_cache_size(size),
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.set_max_stmt_cache_size(size),
        }
    }

    async fn close(self) -> Result<(), DriverError> {
        match self {
            PoolSlot::Async(c) => c.close().await,
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.close(),
        }
    }

    /// Whether this slot holds a sync (UDS) connection.
    #[cfg(unix)]
    fn is_sync(&self) -> bool {
        matches!(self, PoolSlot::Sync(_))
    }

    /// Update the last-used timestamp. Called once when the connection is
    /// returned to the pool, replacing per-query `Instant::now()` calls
    /// (~20-40ns per call on macOS).
    fn touch(&mut self) {
        match self {
            PoolSlot::Async(c) => c.touch(),
            #[cfg(unix)]
            PoolSlot::Sync(c) => c.touch(),
        }
    }
}

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
    stack: std::sync::Mutex<Vec<PoolSlot>>,
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

        // Open a new connection — sync for UDS, async for TCP
        let slot_result = self.open_new_connection().await;
        match slot_result {
            Ok(mut slot) => {
                // Configure statement cache size
                slot.set_max_stmt_cache_size(self.inner.max_stmt_cache_size);
                // Warmup: pre-PREPARE frequently used statements
                self.warmup_slot(&mut slot).await;

                self.inner.connecting.notify_waiters();
                Ok(PoolGuard {
                    conn: Some(slot),
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
        while let Some(slot) = stack.pop() {
            if let Some(max_lifetime) = self.inner.max_lifetime {
                if slot.created_at().elapsed() >= max_lifetime {
                    self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
            }
            if slot.idle_duration() < Duration::from_secs(30) {
                return Ok(Some(PoolGuard {
                    conn: Some(slot),
                    pool: self.inner.clone(),
                    discard: false,
                }));
            }
            // Stale connection — drop it, free the slot
            self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(None)
    }

    /// Open a new connection — sync for UDS, async for TCP.
    ///
    /// When `config.host_is_uds()` is true (Unix), creates a `SyncConnection`
    /// using blocking I/O wrapped in `block_in_place`. For TCP, creates an
    /// async `Connection` as before.
    async fn open_new_connection(&self) -> Result<PoolSlot, DriverError> {
        open_new_connection_inner(&self.inner.config).await
    }

    /// Whether this pool uses sync (UDS) connections.
    ///
    /// Returns `true` when the pool URL points to a Unix domain socket.
    /// On non-Unix platforms, always returns `false`.
    pub fn is_uds(&self) -> bool {
        #[cfg(unix)]
        {
            self.inner.config.host_is_uds()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    /// Begin a transaction. Acquires a connection and sends BEGIN.
    pub async fn begin(&self) -> Result<Transaction, DriverError> {
        let mut guard = self.acquire().await?;
        guard.simple_query("BEGIN").await?;
        Ok(Transaction {
            guard,
            committed: false,
            deferred_buf: Vec::new(),
            deferred_count: 0,
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

    /// Pre-PREPARE warmup statements on a new connection slot.
    ///
    /// Uses `prepare_only()` which sends Parse+Describe+Sync without
    /// Bind+Execute — no query execution, only statement caching.
    ///
    /// Best-effort: errors and timeouts on individual statements are silently
    /// ignored. The connection remains usable even if warmup fails.
    async fn warmup_slot(&self, slot: &mut PoolSlot) {
        let sqls = self
            .inner
            .warmup_sqls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        if sqls.is_empty() {
            return;
        }

        match slot {
            PoolSlot::Async(conn) => {
                for sql in sqls.iter() {
                    let sql_hash = crate::conn::hash_sql(sql);
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        conn.prepare_only(sql, sql_hash),
                    )
                    .await;
                }
            }
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| {
                    for sql in sqls.iter() {
                        let sql_hash = crate::conn::hash_sql(sql);
                        let _ = conn.prepare_only(sql, sql_hash);
                    }
                });
            }
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
        let slots: Vec<PoolSlot> = {
            let mut stack = self.inner.stack.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *stack)
        };
        for slot in slots {
            self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
            let _ = slot.close().await;
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

            let slot_result = open_new_connection_inner(&inner.config).await;
            match slot_result {
                Ok(slot) => {
                    let mut stack = inner.stack.lock().unwrap_or_else(|e| e.into_inner());
                    stack.push(slot);
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

/// Open a new connection — sync for UDS, async for TCP.
/// Free function so `maintain_min_idle` can use it without a `Pool` reference.
async fn open_new_connection_inner(config: &Config) -> Result<PoolSlot, DriverError> {
    #[cfg(unix)]
    if config.host_is_uds() {
        let config = config.clone();
        return tokio::task::block_in_place(|| {
            SyncConnection::connect(&config).map(PoolSlot::Sync)
        });
    }

    Connection::connect(config).await.map(PoolSlot::Async)
}

// --- PoolGuard ---

/// A borrowed connection from the pool. Returns to the pool on drop.
///
/// If the connection is in a failed transaction state, broken, or marked for
/// discard, it is dropped (decrements open_count) instead of returned.
///
/// `PoolGuard` dispatches query methods to either the async `Connection` or
/// the sync `SyncConnection` depending on the underlying slot type. For sync
/// connections, blocking I/O is wrapped in `tokio::task::block_in_place`.
pub struct PoolGuard {
    conn: Option<PoolSlot>,
    pool: Arc<PoolInner>,
    /// When true, the connection is dropped instead of returned to the pool.
    /// Used by streaming queries that are dropped mid-iteration (the connection
    /// is in an indeterminate protocol state and cannot be reused).
    discard: bool,
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
        let slot = self
            .conn
            .as_ref()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.cancel(&self.pool.config).await,
            // SyncConnection does not support cancel (no separate TCP channel).
            // Return an error so callers know.
            #[cfg(unix)]
            PoolSlot::Sync(_) => Err(DriverError::Pool(
                "cancel not supported on sync UDS connections".into(),
            )),
        }
    }

    // --- Introspection dispatch methods ---

    /// Get the backend process ID for this connection.
    pub fn pid(&self) -> i32 {
        match self.conn.as_ref().expect("connection taken") {
            PoolSlot::Async(conn) => conn.pid(),
            #[cfg(unix)]
            PoolSlot::Sync(conn) => conn.pid(),
        }
    }

    /// Whether the connection is idle (not in a transaction).
    pub fn is_idle(&self) -> bool {
        match self.conn.as_ref().expect("connection taken") {
            PoolSlot::Async(conn) => conn.is_idle(),
            #[cfg(unix)]
            PoolSlot::Sync(conn) => conn.is_idle(),
        }
    }

    /// Whether the connection is inside a transaction.
    pub fn is_in_transaction(&self) -> bool {
        match self.conn.as_ref().expect("connection taken") {
            PoolSlot::Async(conn) => conn.is_in_transaction(),
            #[cfg(unix)]
            PoolSlot::Sync(conn) => conn.is_in_transaction(),
        }
    }

    // --- Query dispatch methods ---

    /// Execute a prepared query and return rows in arena-allocated storage.
    pub async fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        arena: &mut Arena,
    ) -> Result<QueryResult, DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.query(sql, sql_hash, params, arena).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| conn.query(sql, sql_hash, params, arena))
            }
        }
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    ///
    /// For sync UDS connections, calls `execute_monolithic` directly — the
    /// entire send+receive path is inlined in one function.
    pub async fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.execute(sql, sql_hash, params).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| conn.execute_monolithic(sql, sql_hash, params))
            }
        }
    }

    /// Execute the same statement N times with different params in one pipeline.
    ///
    /// Sends all N Bind+Execute messages + one Sync. One round-trip for N operations.
    /// Returns the affected row count for each parameter set.
    pub async fn execute_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> Result<Vec<u64>, DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.execute_pipeline(sql, sql_hash, param_sets).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| conn.execute_pipeline(sql, sql_hash, param_sets))
            }
        }
    }

    /// Execute a simple (unprepared) query.
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.simple_query(sql).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => tokio::task::block_in_place(|| conn.simple_query(sql)),
        }
    }

    /// Process each row via a closure with zero-copy `PgDataRow`.
    pub async fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(PgDataRow<'_>) -> Result<(), DriverError>,
    {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.for_each(sql, sql_hash, params, f).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| conn.for_each(sql, sql_hash, params, f))
            }
        }
    }

    /// Process each DataRow as raw bytes — fastest path.
    ///
    /// For sync UDS connections, calls `for_each_raw_monolithic` directly — the
    /// entire send+receive path is inlined in one function.
    pub async fn for_each_raw<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.for_each_raw(sql, sql_hash, params, f).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => tokio::task::block_in_place(|| {
                conn.for_each_raw_monolithic(sql, sql_hash, params, f)
            }),
        }
    }

    // --- Streaming (async-only) ---

    /// Start a streaming query. Only available on async connections.
    ///
    /// Returns an error if called on a sync UDS connection (streaming requires
    /// the async protocol's portal suspend/resume mechanism).
    pub async fn query_streaming_start(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        chunk_size: i32,
    ) -> Result<(std::sync::Arc<[crate::conn::ColumnDesc]>, bool), DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => {
                conn.query_streaming_start(sql, sql_hash, params, chunk_size)
                    .await
            }
            #[cfg(unix)]
            PoolSlot::Sync(_) => Err(DriverError::Pool(
                "streaming queries not supported on sync UDS connections".into(),
            )),
        }
    }

    /// Send Execute+Flush for a streaming query (2nd+ chunks).
    pub async fn streaming_send_execute(&mut self, chunk_size: i32) -> Result<(), DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.streaming_send_execute(chunk_size).await,
            #[cfg(unix)]
            PoolSlot::Sync(_) => Err(DriverError::Pool(
                "streaming queries not supported on sync UDS connections".into(),
            )),
        }
    }

    /// Read the next chunk of rows from an in-progress streaming query.
    pub async fn streaming_next_chunk(
        &mut self,
        arena: &mut Arena,
        all_col_offsets: &mut Vec<(usize, i32)>,
    ) -> Result<bool, DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.streaming_next_chunk(arena, all_col_offsets).await,
            #[cfg(unix)]
            PoolSlot::Sync(_) => Err(DriverError::Pool(
                "streaming queries not supported on sync UDS connections".into(),
            )),
        }
    }

    /// Whether this guard holds a sync (UDS) connection.
    ///
    /// Useful for callers that need to know the connection type (e.g., to
    /// choose between streaming and non-streaming query paths).
    pub fn is_sync(&self) -> bool {
        #[cfg(unix)]
        if let Some(slot) = &self.conn {
            return slot.is_sync();
        }
        false
    }

    // --- Deferred pipeline support ---

    /// Ensure a statement is prepared and cached.
    ///
    /// Returns the cached statement name. No-op if already cached.
    pub(crate) async fn ensure_stmt_prepared(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<Box<str>, DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.ensure_stmt_prepared(sql, sql_hash, params).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| conn.ensure_stmt_prepared(sql, sql_hash, params))
            }
        }
    }

    /// Write Bind+Execute bytes for a prepared statement into an external buffer.
    ///
    /// The statement must already be prepared via `ensure_stmt_prepared`.
    pub(crate) fn write_deferred_bind_execute(
        &self,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        buf: &mut Vec<u8>,
    ) {
        let slot = self.conn.as_ref().expect("connection taken");
        match slot {
            PoolSlot::Async(conn) => conn.write_deferred_bind_execute(sql_hash, params, buf),
            #[cfg(unix)]
            PoolSlot::Sync(conn) => conn.write_deferred_bind_execute(sql_hash, params, buf),
        }
    }

    /// Flush a buffer of deferred Bind+Execute messages as a single pipeline.
    pub(crate) async fn flush_deferred_pipeline(
        &mut self,
        buf: &mut Vec<u8>,
        count: usize,
    ) -> Result<Vec<u64>, DriverError> {
        let slot = self
            .conn
            .as_mut()
            .ok_or_else(|| DriverError::Pool("connection already taken".into()))?;
        match slot {
            PoolSlot::Async(conn) => conn.flush_deferred_pipeline(buf, count).await,
            #[cfg(unix)]
            PoolSlot::Sync(conn) => {
                tokio::task::block_in_place(|| conn.flush_deferred_pipeline(buf, count))
            }
        }
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        if let Some(mut slot) = self.conn.take() {
            // + Discard if:
            //   - explicitly marked for discard
            //   - in a failed transaction (tx_status == 'E')
            //   - in an active transaction (tx_status == 'T') — uncommitted tx
            //   - streaming query in progress — connection in indeterminate state
            //   - pool is closed
            if self.discard
                || slot.is_in_failed_transaction()
                || slot.is_in_transaction()
                || slot.is_streaming()
                || self.pool.closed.load(Ordering::Acquire)
            {
                self.pool.open_count.fetch_sub(1, Ordering::AcqRel);
                return;
            }

            // Stamp the last-used time once on pool return, instead of on
            // every query. Saves ~20-40ns per query on macOS (one fewer
            // mach_absolute_time syscall per query).
            slot.touch();

            // Return to pool synchronously. The critical section is trivial
            // (Vec::push — no I/O), so std::sync::Mutex is appropriate here
            // and avoids spawning an async task in Drop.
            {
                let mut stack = self.pool.stack.lock().unwrap_or_else(|e| e.into_inner());
                stack.push(slot);
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
    /// Accumulated Bind+Execute message bytes for deferred operations.
    deferred_buf: Vec<u8>,
    /// Number of deferred operations buffered.
    deferred_count: usize,
}

impl Transaction {
    /// Commit the transaction.
    ///
    /// Automatically flushes any deferred operations before committing.
    pub async fn commit(mut self) -> Result<(), DriverError> {
        if self.deferred_count > 0 {
            self.flush_deferred().await?;
        }
        self.guard.simple_query("COMMIT").await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction explicitly.
    ///
    /// Discards any deferred operations without sending them.
    pub async fn rollback(mut self) -> Result<(), DriverError> {
        self.deferred_buf.clear();
        self.deferred_count = 0;
        self.guard.simple_query("ROLLBACK").await?;
        self.committed = true; // prevent double rollback in drop
        Ok(())
    }

    /// Execute a prepared query within the transaction.
    ///
    /// Automatically flushes any deferred operations before executing the query,
    /// ensuring read-your-writes consistency.
    pub async fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        arena: &mut Arena,
    ) -> Result<QueryResult, DriverError> {
        if self.deferred_count > 0 {
            self.flush_deferred().await?;
        }
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

    /// Execute the same statement N times with different params in one pipeline.
    ///
    /// All N Bind+Execute messages are sent with one Sync at the end.
    /// One round-trip for N operations within the transaction.
    pub async fn execute_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> Result<Vec<u64>, DriverError> {
        self.guard.execute_pipeline(sql, sql_hash, param_sets).await
    }

    /// Process each row directly from the wire buffer within a transaction.
    ///
    /// Automatically flushes any deferred operations first.
    pub async fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(crate::conn::PgDataRow<'_>) -> Result<(), DriverError>,
    {
        if self.deferred_count > 0 {
            self.flush_deferred().await?;
        }
        self.guard.for_each(sql, sql_hash, params, f).await
    }

    /// Process each DataRow as raw bytes within a transaction.
    ///
    /// The closure receives the raw DataRow message payload. Generated code
    /// decodes columns sequentially inline — no PgDataRow, no SmallVec.
    ///
    /// Automatically flushes any deferred operations first.
    pub async fn for_each_raw<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        if self.deferred_count > 0 {
            self.flush_deferred().await?;
        }
        self.guard.for_each_raw(sql, sql_hash, params, f).await
    }

    /// Simple query within the transaction.
    ///
    /// Automatically flushes any deferred operations first.
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        if self.deferred_count > 0 {
            self.flush_deferred().await?;
        }
        self.guard.simple_query(sql).await
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
    /// made to prepare it (Parse+Describe+Sync). After that, the Bind+Execute
    /// bytes are buffered with no I/O.
    ///
    /// **Note**: Because execution is deferred, the affected row count is not
    /// available until flush. Use `flush_deferred()` if you need per-operation
    /// counts, or `commit()` if you only need correctness.
    ///
    /// Any read operation (`query`, `for_each`, `for_each_raw`, `simple_query`)
    /// automatically flushes deferred operations first to ensure
    /// read-your-writes consistency.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), bsql_driver_postgres::DriverError> {
    /// # let pool = bsql_driver_postgres::Pool::connect("postgres://u:p@localhost/db").await?;
    /// let mut tx = pool.begin().await?;
    /// let sql = "INSERT INTO t (v) VALUES ($1)";
    /// let hash = bsql_driver_postgres::hash_sql(sql);
    ///
    /// // These are buffered, not sent:
    /// tx.defer_execute(sql, hash, &[&1i32]).await?;
    /// tx.defer_execute(sql, hash, &[&2i32]).await?;
    /// tx.defer_execute(sql, hash, &[&3i32]).await?;
    ///
    /// // commit() flushes all 3 as one pipeline + COMMIT = 2 round-trips total
    /// tx.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn defer_execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<(), DriverError> {
        if params.len() > i16::MAX as usize {
            return Err(DriverError::Protocol(format!(
                "parameter count {} exceeds maximum {}",
                params.len(),
                i16::MAX
            )));
        }

        // Ensure statement is prepared (may require one round-trip on first call)
        self.guard
            .ensure_stmt_prepared(sql, sql_hash, params)
            .await?;

        // Buffer the Bind+Execute bytes — no I/O
        self.guard
            .write_deferred_bind_execute(sql_hash, params, &mut self.deferred_buf);
        self.deferred_count += 1;
        Ok(())
    }

    /// Flush all deferred operations as a single pipeline.
    ///
    /// Sends all buffered Bind+Execute messages + one Sync in a single TCP write.
    /// Returns the affected row count for each deferred operation.
    ///
    /// After this call, the deferred buffer is empty and new operations can be
    /// deferred again.
    pub async fn flush_deferred(&mut self) -> Result<Vec<u64>, DriverError> {
        let count = self.deferred_count;
        self.deferred_count = 0;
        self.guard
            .flush_deferred_pipeline(&mut self.deferred_buf, count)
            .await
    }

    /// Number of operations currently buffered for deferred execution.
    pub fn deferred_count(&self) -> usize {
        self.deferred_count
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

    // --- Auto-UDS detection tests ---

    #[tokio::test]
    async fn pool_is_uds_false_for_tcp() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_is_uds_true_for_unix_socket() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/tmp")
            .await
            .unwrap();
        assert!(pool.is_uds());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_is_uds_true_for_var_run_socket() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/var/run/postgresql")
            .await
            .unwrap();
        assert!(pool.is_uds());
    }

    #[tokio::test]
    async fn pool_is_uds_false_for_ip_address() {
        let pool = Pool::connect("postgres://user:pass@127.0.0.1/db")
            .await
            .unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_slot_sync_created_for_uds_config() {
        // Verify that PoolSlot::Sync is created for UDS configs.
        // We can't actually connect (no PG running on /tmp), but we can
        // verify the detection logic.
        let config = Config::from_url("postgres://user@localhost/db?host=/tmp").unwrap();
        assert!(config.host_is_uds());
    }

    #[test]
    fn pool_slot_async_created_for_tcp_config() {
        let config = Config::from_url("postgres://user:pass@localhost/db").unwrap();
        assert!(!config.host_is_uds());
    }

    // ===============================================================
    // Pool::is_uds — extended tests
    // ===============================================================

    #[tokio::test]
    async fn pool_is_uds_false_for_hostname() {
        let pool = Pool::connect("postgres://user:pass@db.example.com/db")
            .await
            .unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_is_uds_true_for_tmp() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/tmp")
            .await
            .unwrap();
        assert!(pool.is_uds());
    }

    // ===============================================================
    // Pool close semantics
    // ===============================================================

    #[tokio::test]
    async fn pool_close_then_acquire_fails() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .await
            .unwrap();
        pool.close().await;
        let result = pool.acquire().await;
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => {
                assert!(msg.contains("closed"), "should say closed: {msg}")
            }
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn pool_is_closed_before_and_after() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        assert!(!pool.is_closed());
        pool.close().await;
        assert!(pool.is_closed());
    }

    // ===============================================================
    // Pool exhaustion (fail-fast without timeout)
    // ===============================================================

    #[tokio::test]
    async fn pool_exhausted_no_timeout() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(0)
            .acquire_timeout(None) // fail-fast
            .build()
            .await
            .unwrap();
        let result = pool.acquire().await;
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => {
                assert!(msg.contains("exhausted"), "should say exhausted: {msg}")
            }
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    // ===============================================================
    // PoolBuilder validation
    // ===============================================================

    #[tokio::test]
    async fn pool_builder_no_url_error() {
        let result = PoolBuilder::new().max_size(5).build().await;
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => {
                assert!(msg.contains("URL"), "should mention URL: {msg}")
            }
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn pool_builder_invalid_url_error() {
        let result = PoolBuilder::new().url("ftp://something").build().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pool_builder_stmt_cache_size_zero() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_stmt_cache_size(0)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.inner.max_stmt_cache_size, 0);
    }

    // ===============================================================
    // PoolStatus
    // ===============================================================

    #[tokio::test]
    async fn pool_status_reflects_max_size() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(20)
            .build()
            .await
            .unwrap();
        let status = pool.status();
        assert_eq!(status.max_size, 20);
        assert_eq!(status.idle, 0);
        assert_eq!(status.active, 0);
        assert_eq!(status.open, 0);
    }

    // ===============================================================
    // Pool clone
    // ===============================================================

    #[tokio::test]
    async fn pool_clone_shares_config() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(7)
            .build()
            .await
            .unwrap();
        let p2 = pool.clone();
        assert_eq!(pool.max_size(), 7);
        assert_eq!(p2.max_size(), 7);
        assert_eq!(pool.open_count(), p2.open_count());
    }

    // ===============================================================
    // set_warmup_sqls
    // ===============================================================

    #[tokio::test]
    async fn pool_set_warmup_sqls_empty() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        pool.set_warmup_sqls(&[]);
        let sqls = pool
            .inner
            .warmup_sqls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert!(sqls.is_empty());
    }

    #[tokio::test]
    async fn pool_set_warmup_sqls_multiple() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        pool.set_warmup_sqls(&["SELECT 1", "SELECT 2", "SELECT 3"]);
        let sqls = pool
            .inner
            .warmup_sqls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(sqls.len(), 3);
        assert_eq!(&*sqls[0], "SELECT 1");
        assert_eq!(&*sqls[1], "SELECT 2");
        assert_eq!(&*sqls[2], "SELECT 3");
    }

    #[tokio::test]
    async fn pool_set_warmup_sqls_overwrite() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        pool.set_warmup_sqls(&["SELECT 1"]);
        pool.set_warmup_sqls(&["SELECT 99"]);
        let sqls = pool
            .inner
            .warmup_sqls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert_eq!(sqls.len(), 1);
        assert_eq!(&*sqls[0], "SELECT 99");
    }

    // ===============================================================
    // PoolStatus Debug
    // ===============================================================

    #[tokio::test]
    async fn pool_status_debug() {
        let pool = Pool::connect("postgres://user:pass@localhost/db")
            .await
            .unwrap();
        let status = pool.status();
        let dbg = format!("{status:?}");
        assert!(dbg.contains("PoolStatus"));
        assert!(dbg.contains("idle"));
        assert!(dbg.contains("active"));
        assert!(dbg.contains("open"));
        assert!(dbg.contains("max_size"));
    }

    // ===============================================================
    // Config host_is_uds via pool (structural tests)
    // ===============================================================

    #[test]
    fn config_host_is_uds_returns_true_for_slash() {
        let config = Config::from_url("postgres://user@localhost/db?host=/tmp").unwrap();
        assert!(config.host_is_uds());
    }

    #[test]
    fn config_host_is_uds_returns_false_for_tcp() {
        let config = Config::from_url("postgres://user:pass@localhost/db").unwrap();
        assert!(!config.host_is_uds());
    }

    #[test]
    fn config_host_is_uds_returns_false_for_ip() {
        let config = Config::from_url("postgres://user:pass@192.168.1.1/db").unwrap();
        assert!(!config.host_is_uds());
    }

    // ===============================================================
    // PoolBuilder chaining
    // ===============================================================

    #[tokio::test]
    async fn pool_builder_full_chain() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(3)
            .max_lifetime(Some(Duration::from_secs(600)))
            .acquire_timeout(Some(Duration::from_secs(5)))
            .min_idle(1)
            .max_stmt_cache_size(128)
            .build()
            .await
            .unwrap();
        assert_eq!(pool.max_size(), 3);
        assert_eq!(pool.inner.max_lifetime, Some(Duration::from_secs(600)));
        assert_eq!(pool.inner.acquire_timeout, Some(Duration::from_secs(5)));
        assert_eq!(pool.inner.min_idle, 1);
        assert_eq!(pool.inner.max_stmt_cache_size, 128);
    }

    // --- Audit: PoolGuard drop discards connections in bad state ---

    #[tokio::test]
    async fn pool_max_size_zero_rejects_all_acquires() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(0)
            .build()
            .await
            .unwrap();
        let result = pool.acquire().await;
        assert!(result.is_err());
        match &result {
            Err(DriverError::Pool(msg)) => assert!(msg.contains("exhausted")),
            _ => panic!("expected pool exhausted error"),
        }
    }

    // --- Audit: URL parsing edge cases ---

    #[test]
    fn url_parse_unknown_sslmode_returns_error() {
        let result = Config::from_url("postgres://u:p@h/d?sslmode=bogus");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("unknown sslmode"));
    }

    #[test]
    fn url_parse_invalid_port_returns_error() {
        let result = Config::from_url("postgres://u:p@h:abc/d");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("invalid port"));
    }

    #[test]
    fn url_parse_missing_at_sign_returns_error() {
        let result = Config::from_url("postgres://u:plocalhost/d");
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("missing @"));
    }

    #[test]
    fn url_parse_empty_host_returns_error() {
        let result = Config::from_url("postgres://u:p@/d");
        assert!(result.is_err());
    }

    #[test]
    fn url_parse_empty_user_returns_error() {
        let result = Config::from_url("postgres://:p@h/d");
        assert!(result.is_err());
    }

    #[test]
    fn url_parse_statement_timeout_invalid_uses_default() {
        let config = Config::from_url("postgres://u:p@h/d?statement_timeout=notnum").unwrap();
        assert_eq!(config.statement_timeout_secs, 30);
    }

    #[test]
    fn url_parse_malformed_percent_encoding() {
        let result = Config::from_url("postgres://u%:p@h/d");
        assert!(result.is_err());
    }

    #[test]
    fn url_parse_invalid_hex_in_percent_encoding() {
        let result = Config::from_url("postgres://u%ZZ:p@h/d");
        assert!(result.is_err());
    }
}
