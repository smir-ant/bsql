//! Connection pool — LIFO ordering, Condvar-based waiting.
//!
//! The pool maintains a stack of idle connections. `acquire()` pops the top
//! (most recently used = warmest caches). On drop, the guard pushes the
//! connection back. If the pool is exhausted, callers wait on a `Condvar`
//! up to `acquire_timeout` (default: 5 seconds). Set `acquire_timeout` to
//! `None` for fail-fast behavior (immediate error when exhausted).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use crate::DriverError;
use crate::arena::Arena;
use crate::codec::Encode;
use crate::conn::Connection;
use crate::types::{Config, PgDataRow, QueryResult, SimpleRow};

#[cfg(feature = "async")]
use crate::async_conn::AsyncConnection;

// --- PoolSlot ---

/// A connection slot — either sync (UDS/TCP) or async (TCP only).
///
/// The pool auto-detects: UDS hosts get sync `Connection`, TCP hosts get
/// `AsyncConnection` (when the `async` feature is enabled). When `async`
/// is disabled, all connections are sync.
pub(crate) enum PoolSlot {
    /// Sync connection (UDS or TCP without async feature).
    Sync(Connection),
    /// Async TCP connection (requires async feature + tokio runtime).
    #[cfg(feature = "async")]
    Async(AsyncConnection),
}

// --- N+1 Detection ---

/// Tracks sequential repeats of the same `sql_hash` on a single connection
/// checkout. When the same hash fires more than `threshold` times in a row,
/// a warning is emitted. Fully `cfg`-gated — zero cost when disabled.
#[cfg(feature = "detect-n-plus-one")]
pub(crate) struct NPlusOneDetector {
    last_query_hash: u64,
    repeat_count: u16,
    threshold: u16,
}

#[cfg(feature = "detect-n-plus-one")]
impl NPlusOneDetector {
    /// Create a new detector with the given warning threshold.
    pub(crate) fn new(threshold: u16) -> Self {
        Self {
            last_query_hash: 0,
            repeat_count: 0,
            threshold,
        }
    }

    /// Track a query execution. Call this at the start of every query method.
    #[inline]
    pub(crate) fn track(&mut self, sql_hash: u64) {
        if sql_hash == self.last_query_hash {
            self.repeat_count = self.repeat_count.saturating_add(1);
        } else {
            // Check previous run before resetting
            self.emit_warning();
            self.last_query_hash = sql_hash;
            self.repeat_count = 1;
        }
    }

    /// Check the final sequence on drop / connection return.
    /// Returns `Some((hash, count))` if a warning should be emitted.
    pub(crate) fn check_final(&self) -> Option<(u64, u16)> {
        if self.repeat_count > self.threshold && self.last_query_hash != 0 {
            Some((self.last_query_hash, self.repeat_count))
        } else {
            None
        }
    }

    /// Emit a log warning if the current run exceeds the threshold.
    #[cold]
    #[inline(never)]
    fn emit_warning(&self) {
        if let Some((hash, count)) = self.check_final() {
            log::warn!(
                "[bsql] potential N+1 detected: sql_hash={:#018x} repeated {} times (threshold: {})",
                hash,
                count,
                self.threshold,
            );
        }
    }

    /// Emit the final warning (called on drop).
    #[cold]
    #[inline(never)]
    pub(crate) fn emit_final_warning(&self) {
        self.emit_warning();
    }
}

// --- Pool ---

/// A connection pool with LIFO ordering and fail-fast semantics.
///
/// # Example
///
/// ```no_run
/// # fn example() -> Result<(), bsql_driver_postgres::DriverError> {
/// let pool = bsql_driver_postgres::Pool::connect("postgres://user:pass@localhost/db")?;
/// let mut conn = pool.acquire()?;
/// conn.simple_query("SELECT 1")?;
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
    /// synchronously.
    stack: std::sync::Mutex<Vec<PoolSlot>>,
    max_size: usize,
    open_count: AtomicUsize,
    config: Arc<Config>,
    /// When true, no new acquires are accepted.
    closed: AtomicBool,
    /// Condvar pair for release notification. Waiters block on the Condvar
    /// when the pool is exhausted and `acquire_timeout` is set.
    release_pair: (std::sync::Mutex<()>, std::sync::Condvar),
    /// Maximum lifetime of a connection. Connections older than this
    /// are discarded when popped from the pool. Default: 30 minutes.
    max_lifetime: Option<Duration>,
    /// Maximum time to wait for a connection. Default: None (fail-fast).
    acquire_timeout: Option<Duration>,
    /// Minimum number of idle connections to maintain. Default: 0.
    min_idle: usize,
    /// SQL statements to PREPARE on new connections (warmup).
    warmup_sqls: std::sync::Mutex<Arc<Vec<Box<str>>>>,
    /// Maximum number of cached prepared statements per connection.
    max_stmt_cache_size: usize,
    /// Maximum idle duration before a connection is considered stale and discarded.
    /// Connections idle longer than this are dropped on acquire. Default: 30 seconds.
    stale_timeout: Duration,
    /// Threshold for N+1 detection. When the same sql_hash fires more than
    /// this many times sequentially on a single checkout, a warning is logged.
    #[cfg(feature = "detect-n-plus-one")]
    n_plus_one_threshold: u16,
}

impl Pool {
    /// Create a pool from a connection URL with default settings (max_size = 10).
    ///
    /// Validates the URL but does not open any connections yet (lazy initialization).
    pub fn connect(url: &str) -> Result<Self, DriverError> {
        PoolBuilder::new().url(url).build()
    }

    /// Create a pool builder for custom configuration.
    pub fn builder() -> PoolBuilder {
        PoolBuilder::new()
    }

    /// Acquire a connection from the pool.
    ///
    /// Returns immediately with the most recently used idle connection (LIFO).
    /// If no idle connections are available and the pool is below max_size, a new
    /// connection is created. If the pool is at max_size and no `acquire_timeout`
    /// is set, returns `DriverError::Pool` immediately. If `acquire_timeout` is
    /// set, blocks until a connection is returned or the timeout expires.
    #[inline]
    pub fn acquire(&self) -> Result<PoolGuard, DriverError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(DriverError::Pool("pool is closed".into()));
        }

        // Try to pop an idle connection (fast path).
        if let Some(guard) = self.try_pop_idle()? {
            return Ok(guard);
        }

        // No idle connections — try to claim a slot with a proper CAS loop.
        loop {
            let current = self.inner.open_count.load(Ordering::Acquire);
            if current >= self.inner.max_size {
                if let Some(timeout) = self.inner.acquire_timeout {
                    let (lock, cvar) = &self.inner.release_pair;
                    let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                    let (_guard, result) = cvar
                        .wait_timeout(guard, timeout)
                        .unwrap_or_else(|e| e.into_inner());
                    if result.timed_out() {
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
            // CAS failed — another thread incremented. Retry.
        }

        // Open a new connection
        let conn_result = Connection::connect_arc(self.inner.config.clone());
        match conn_result {
            Ok(mut conn) => {
                // Configure statement cache size
                conn.set_max_stmt_cache_size(self.inner.max_stmt_cache_size);
                // Warmup: pre-PREPARE frequently used statements
                self.warmup_conn(&mut conn);

                Ok(PoolGuard {
                    conn: Some(PoolSlot::Sync(conn)),
                    pool: self.inner.clone(),
                    discard: false,
                    #[cfg(feature = "detect-n-plus-one")]
                    detector: NPlusOneDetector::new(self.inner.n_plus_one_threshold),
                })
            }
            Err(e) => {
                // Give back the slot
                self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }

    /// Try to pop a valid idle connection from the stack.
    ///
    /// Performs lifetime and stale checks. For connections idle > 5 seconds
    /// (but within the stale timeout), sends an empty query as a health check
    /// to verify the connection is still alive before returning it.
    #[inline]
    fn try_pop_idle(&self) -> Result<Option<PoolGuard>, DriverError> {
        // Pop a candidate slot under the lock, performing only non-I/O checks
        // (lifetime, stale timeout). The health check (network round-trip) happens
        // AFTER the lock is released so other threads aren't blocked.
        loop {
            let (mut slot, needs_health_check) = {
                let mut stack = self.inner.stack.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    let Some(slot) = stack.pop() else {
                        return Ok(None);
                    };
                    let (created_at, idle_dur) = match &slot {
                        PoolSlot::Sync(conn) => (conn.created_at(), conn.idle_duration()),
                        #[cfg(feature = "async")]
                        PoolSlot::Async(conn) => (conn.created_at(), conn.idle_duration()),
                    };
                    if let Some(max_lifetime) = self.inner.max_lifetime {
                        if created_at.elapsed() >= max_lifetime {
                            self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                            continue;
                        }
                    }
                    if idle_dur >= self.inner.stale_timeout {
                        // Stale connection — drop it, free the slot
                        self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                        continue;
                    }
                    break (slot, idle_dur > Duration::from_secs(5));
                }
            };
            // Lock is now released — health check happens outside the critical section.
            // Sends an empty query — PG returns EmptyQueryResponse + ReadyForQuery.
            // Fast: one round-trip, ~15us on UDS. Skip for hot connections.
            if needs_health_check {
                let alive = match &mut slot {
                    PoolSlot::Sync(conn) => conn.simple_query("").is_ok(),
                    #[cfg(feature = "async")]
                    PoolSlot::Async(_) => true, // async connections are checked at I/O time
                };
                if !alive {
                    self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                    continue; // retry — re-acquire lock and pop next slot
                }
            }
            return Ok(Some(PoolGuard {
                conn: Some(slot),
                pool: self.inner.clone(),
                discard: false,
                #[cfg(feature = "detect-n-plus-one")]
                detector: NPlusOneDetector::new(self.inner.n_plus_one_threshold),
            }));
        }
    }

    /// Whether this pool uses UDS connections.
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
    pub fn begin(&self) -> Result<Transaction, DriverError> {
        let mut guard = self.acquire()?;
        guard.simple_query("BEGIN")?;
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

    /// Pre-PREPARE warmup statements on a new connection.
    ///
    /// Uses `prepare_only()` which sends Parse+Describe+Sync without
    /// Bind+Execute — no query execution, only statement caching.
    ///
    /// Best-effort: errors on individual statements are silently ignored.
    /// The connection remains usable even if warmup fails.
    fn warmup_conn(&self, conn: &mut Connection) {
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
            let sql_hash = crate::types::hash_sql(sql);
            let _ = conn.prepare_only(sql, sql_hash);
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
    /// # fn example() -> Result<(), bsql_driver_postgres::DriverError> {
    /// let pool = bsql_driver_postgres::Pool::connect("postgres://user:pass@localhost/db")?;
    /// pool.set_warmup_sqls(&[
    ///     "SELECT id, name FROM users WHERE id = $1::int4",
    ///     "SELECT id, title FROM tickets WHERE status = ANY($1::text[])",
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_warmup_sqls(&self, sqls: &[&str]) {
        let boxed: Arc<Vec<Box<str>>> =
            Arc::new(sqls.iter().map(|s| (*s).into()).collect::<Vec<_>>());
        *self
            .inner
            .warmup_sqls
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = boxed;
    }

    /// Close the pool. No new acquires are accepted. All idle connections
    /// are sent Terminate and dropped.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        // Drain and close all idle connections
        let slots: Vec<PoolSlot> = {
            let mut stack = self.inner.stack.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *stack)
        };
        for slot in slots {
            self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
            match slot {
                PoolSlot::Sync(conn) => {
                    let _ = conn.close();
                }
                #[cfg(feature = "async")]
                PoolSlot::Async(_conn) => {
                    // AsyncConnection::close() is async — we can't await in sync close().
                    // Drop will close the TCP socket, PG auto-cleans up.
                }
            }
        }
        // Notify any waiters so they get the "pool is closed" error
        let (_, cvar) = &self.inner.release_pair;
        cvar.notify_all();
    }

    /// Whether the pool has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Acquire a connection from the pool (async).
    ///
    /// Auto-detects transport: UDS hosts get a sync `Connection`, TCP hosts
    /// get an `AsyncConnection`. If the `async` feature is disabled, always
    /// creates sync connections.
    ///
    /// Returns immediately with the most recently used idle connection (LIFO).
    /// If no idle connections are available and the pool is below max_size, a new
    /// connection is created.
    #[cfg(feature = "async")]
    pub async fn acquire_async(&self) -> Result<PoolGuard, DriverError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(DriverError::Pool("pool is closed".into()));
        }

        // Try to pop an idle connection (fast path).
        if let Some(guard) = self.try_pop_idle()? {
            return Ok(guard);
        }

        // No idle connections — try to claim a slot with a proper CAS loop.
        loop {
            let current = self.inner.open_count.load(Ordering::Acquire);
            if current >= self.inner.max_size {
                if let Some(timeout) = self.inner.acquire_timeout {
                    let (lock, cvar) = &self.inner.release_pair;
                    let guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                    let (_guard, result) = cvar
                        .wait_timeout(guard, timeout)
                        .unwrap_or_else(|e| e.into_inner());
                    if result.timed_out() {
                        return Err(DriverError::Pool(
                            "pool exhausted: acquire timeout expired".into(),
                        ));
                    }
                    if let Some(guard) = self.try_pop_idle()? {
                        return Ok(guard);
                    }
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
        }

        // Open a new connection — auto-detect UDS vs TCP
        if self.inner.config.host_is_uds() {
            // UDS — use sync Connection
            let conn_result = Connection::connect_arc(self.inner.config.clone());
            match conn_result {
                Ok(mut conn) => {
                    conn.set_max_stmt_cache_size(self.inner.max_stmt_cache_size);
                    self.warmup_conn(&mut conn);
                    Ok(PoolGuard {
                        conn: Some(PoolSlot::Sync(conn)),
                        pool: self.inner.clone(),
                        discard: false,
                        #[cfg(feature = "detect-n-plus-one")]
                        detector: NPlusOneDetector::new(self.inner.n_plus_one_threshold),
                    })
                }
                Err(e) => {
                    self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                    Err(e)
                }
            }
        } else {
            // TCP — use AsyncConnection
            let conn_result = AsyncConnection::connect_arc(self.inner.config.clone()).await;
            match conn_result {
                Ok(mut conn) => {
                    conn.set_max_stmt_cache_size(self.inner.max_stmt_cache_size);
                    Ok(PoolGuard {
                        conn: Some(PoolSlot::Async(conn)),
                        pool: self.inner.clone(),
                        discard: false,
                        #[cfg(feature = "detect-n-plus-one")]
                        detector: NPlusOneDetector::new(self.inner.n_plus_one_threshold),
                    })
                }
                Err(e) => {
                    self.inner.open_count.fetch_sub(1, Ordering::AcqRel);
                    Err(e)
                }
            }
        }
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

// --- PoolStatus ---

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
    /// Maximum idle duration before a connection is considered stale.
    stale_timeout: Duration,
    /// Threshold for N+1 detection warnings.
    #[cfg(feature = "detect-n-plus-one")]
    n_plus_one_threshold: Option<u16>,
}

impl PoolBuilder {
    fn new() -> Self {
        Self {
            url: None,
            max_size: 10,
            max_lifetime: Some(Duration::from_secs(30 * 60)), // 30 min default
            acquire_timeout: Some(Duration::from_secs(5)), // 5s default (matches common pool defaults)
            min_idle: 0,                                   // no minimum by default
            max_stmt_cache_size: 256,                      // LRU eviction at 256 stmts
            stale_timeout: Duration::from_secs(30),        // 30s default
            #[cfg(feature = "detect-n-plus-one")]
            n_plus_one_threshold: None,
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

    /// Set the acquire timeout. Default: 5 seconds.
    /// Set to None for fail-fast behavior when the pool is exhausted.
    pub fn acquire_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set the minimum number of idle connections. Default: 0.
    /// When > 0, a background thread maintains this many idle connections.
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

    /// Set the maximum idle duration before a connection is considered stale.
    /// Default: 30 seconds. Connections idle longer than this are dropped on
    /// acquire instead of being reused.
    pub fn stale_timeout(mut self, timeout: Duration) -> Self {
        self.stale_timeout = timeout;
        self
    }

    /// Set the threshold for N+1 detection warnings.
    ///
    /// When the same `sql_hash` fires more than this many times sequentially
    /// on a single connection checkout, a warning is logged. Default: 10.
    #[cfg(feature = "detect-n-plus-one")]
    pub fn n_plus_one_threshold(mut self, n: u16) -> Self {
        self.n_plus_one_threshold = Some(n);
        self
    }

    /// Build the pool. Validates the URL but does not open connections.
    pub fn build(self) -> Result<Pool, DriverError> {
        let url = self
            .url
            .ok_or_else(|| DriverError::Pool("pool builder requires a URL".into()))?;

        let config = Arc::new(Config::from_url(&url)?);

        let pool = Pool {
            inner: Arc::new(PoolInner {
                stack: std::sync::Mutex::new(Vec::with_capacity(self.max_size)),
                max_size: self.max_size,
                open_count: AtomicUsize::new(0),
                config,
                closed: AtomicBool::new(false),
                release_pair: (std::sync::Mutex::new(()), std::sync::Condvar::new()),
                max_lifetime: self.max_lifetime,
                acquire_timeout: self.acquire_timeout,
                min_idle: self.min_idle,
                warmup_sqls: std::sync::Mutex::new(Arc::new(Vec::new())),
                max_stmt_cache_size: self.max_stmt_cache_size,
                stale_timeout: self.stale_timeout,
                #[cfg(feature = "detect-n-plus-one")]
                n_plus_one_threshold: self.n_plus_one_threshold.unwrap_or(10),
            }),
        };

        if self.min_idle > 0 {
            let inner = pool.inner.clone();
            std::thread::spawn(move || {
                maintain_min_idle(inner);
            });
        }

        Ok(pool)
    }
}

/// Background thread that maintains min_idle connections.
fn maintain_min_idle(inner: Arc<PoolInner>) {
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

            match Connection::connect_arc(inner.config.clone()) {
                Ok(conn) => {
                    let mut stack = inner.stack.lock().unwrap_or_else(|e| e.into_inner());
                    stack.push(PoolSlot::Sync(conn));
                    let (_, cvar) = &inner.release_pair;
                    cvar.notify_one();
                }
                Err(_) => {
                    inner.open_count.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }

        // Check every 1 second. Shorter interval ensures the thread exits promptly
        // when pool.closed is set (worst-case 1s delay instead of 5s).
        std::thread::sleep(Duration::from_secs(1));
    }
}

// --- PoolGuard ---

/// A borrowed connection from the pool. Returns to the pool on drop.
///
/// If the connection is in a failed transaction state, broken, or marked for
/// discard, it is dropped (decrements open_count) instead of returned.
pub struct PoolGuard {
    conn: Option<PoolSlot>,
    pool: Arc<PoolInner>,
    /// When true, the connection is dropped instead of returned to the pool.
    discard: bool,
    /// Tracks sequential repeats of the same sql_hash for N+1 detection.
    #[cfg(feature = "detect-n-plus-one")]
    detector: NPlusOneDetector,
}

impl PoolGuard {
    /// Get a reference to the inner sync connection. Panics if the slot
    /// holds an async connection.
    #[inline]
    fn sync_conn(&self) -> Result<&Connection, DriverError> {
        match self.conn.as_ref() {
            Some(PoolSlot::Sync(conn)) => Ok(conn),
            #[cfg(feature = "async")]
            Some(PoolSlot::Async(_)) => Err(DriverError::Pool(
                "expected sync connection, got async; use async methods".into(),
            )),
            None => Err(DriverError::Pool("connection already taken".into())),
        }
    }

    /// Get a mutable reference to the inner sync connection.
    #[inline]
    fn sync_conn_mut(&mut self) -> Result<&mut Connection, DriverError> {
        match self.conn.as_mut() {
            Some(PoolSlot::Sync(conn)) => Ok(conn),
            #[cfg(feature = "async")]
            Some(PoolSlot::Async(_)) => Err(DriverError::Pool(
                "expected sync connection, got async; use async methods".into(),
            )),
            None => Err(DriverError::Pool("connection already taken".into())),
        }
    }

    /// Mark this connection for discard — it will NOT be returned to the pool
    /// on drop. The open_count is decremented and the TCP connection is closed.
    pub fn mark_discard(&mut self) {
        self.discard = true;
    }

    /// Cancel the currently running query on the underlying connection.
    ///
    /// Opens a new TCP connection and sends a CancelRequest to PG.
    /// The cancel connection is closed immediately after.
    pub fn cancel(&self) -> Result<(), DriverError> {
        self.sync_conn()?.cancel()
    }

    // --- Introspection dispatch methods ---

    /// Get the backend process ID for this connection.
    pub fn pid(&self) -> i32 {
        match self.conn.as_ref().expect("connection taken") {
            PoolSlot::Sync(conn) => conn.pid(),
            #[cfg(feature = "async")]
            PoolSlot::Async(conn) => conn.pid(),
        }
    }

    /// Whether the connection is idle (not in a transaction).
    pub fn is_idle(&self) -> bool {
        match self.conn.as_ref().expect("connection taken") {
            PoolSlot::Sync(conn) => conn.is_idle(),
            #[cfg(feature = "async")]
            PoolSlot::Async(conn) => conn.is_idle(),
        }
    }

    /// Whether the connection is inside a transaction.
    pub fn is_in_transaction(&self) -> bool {
        match self.conn.as_ref().expect("connection taken") {
            PoolSlot::Sync(conn) => conn.is_in_transaction(),
            #[cfg(feature = "async")]
            PoolSlot::Async(conn) => conn.is_in_transaction(),
        }
    }

    // --- Sync query dispatch methods ---

    /// Execute a prepared query and return rows.
    #[inline]
    pub fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<QueryResult, DriverError> {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        self.sync_conn_mut()?.query(sql, sql_hash, params)
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    #[inline]
    pub fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        self.sync_conn_mut()?.execute(sql, sql_hash, params)
    }

    /// Execute the same statement N times with different params in one pipeline.
    ///
    /// Sends all N Bind+Execute messages + one Sync. One round-trip for N operations.
    /// Returns the affected row count for each parameter set.
    pub fn execute_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> Result<Vec<u64>, DriverError> {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        self.sync_conn_mut()?
            .execute_pipeline(sql, sql_hash, param_sets)
    }

    /// Execute a simple (unprepared) query.
    pub fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        self.sync_conn_mut()?.simple_query(sql)
    }

    /// Execute a simple query and return rows as text.
    ///
    /// Uses PostgreSQL's simple query protocol — all values are strings.
    pub fn simple_query_rows(&mut self, sql: &str) -> Result<Vec<SimpleRow>, DriverError> {
        self.sync_conn_mut()?.simple_query_rows(sql)
    }

    /// Process each row via a closure with zero-copy `PgDataRow`.
    pub fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(PgDataRow<'_>) -> Result<(), DriverError>,
    {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        self.sync_conn_mut()?.for_each(sql, sql_hash, params, f)
    }

    /// Process each DataRow as raw bytes — fastest path.
    pub fn for_each_raw<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        self.sync_conn_mut()?.for_each_raw(sql, sql_hash, params, f)
    }

    // --- Streaming ---

    /// Start a streaming query.
    pub fn query_streaming_start(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        chunk_size: i32,
    ) -> Result<(std::sync::Arc<[crate::types::ColumnDesc]>, bool), DriverError> {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        self.sync_conn_mut()?
            .query_streaming_start(sql, sql_hash, params, chunk_size)
    }

    /// Send Execute+Flush for a streaming query (2nd+ chunks).
    pub fn streaming_send_execute(&mut self, chunk_size: i32) -> Result<(), DriverError> {
        self.sync_conn_mut()?.streaming_send_execute(chunk_size)
    }

    /// Read the next chunk of rows from an in-progress streaming query.
    pub fn streaming_next_chunk(
        &mut self,
        arena: &mut Arena,
        all_col_offsets: &mut Vec<(usize, i32)>,
    ) -> Result<bool, DriverError> {
        self.sync_conn_mut()?
            .streaming_next_chunk(arena, all_col_offsets)
    }

    // --- COPY protocol ---

    /// Bulk copy data INTO a table from an iterator of text rows.
    ///
    /// Each row is a tab-separated string (TSV format). Returns the row count.
    pub fn copy_in<'a, I>(
        &mut self,
        table: &str,
        columns: &[&str],
        rows: I,
    ) -> Result<u64, DriverError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.sync_conn_mut()?.copy_in(table, columns, rows)
    }

    /// Bulk copy data OUT of a table/query to a writer.
    ///
    /// Writes TSV-formatted rows. Returns the row count.
    pub fn copy_out<W: std::io::Write>(
        &mut self,
        query: &str,
        writer: &mut W,
    ) -> Result<u64, DriverError> {
        self.sync_conn_mut()?.copy_out(query, writer)
    }

    /// Whether this guard holds a sync connection.
    pub fn is_sync(&self) -> bool {
        matches!(self.conn.as_ref(), Some(PoolSlot::Sync(_)))
    }

    /// Whether this guard holds an async connection.
    #[cfg(feature = "async")]
    pub fn is_async(&self) -> bool {
        matches!(self.conn.as_ref(), Some(PoolSlot::Async(_)))
    }

    // --- Async query dispatch methods ---

    /// Execute a prepared query and return rows (async).
    ///
    /// Auto-dispatches: sync connections use blocking I/O, async connections
    /// use tokio I/O. Returns an error if the guard holds a sync connection
    /// and this method is called.
    #[cfg(feature = "async")]
    pub async fn query_async(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<QueryResult, DriverError> {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        match self.conn.as_mut() {
            Some(PoolSlot::Sync(conn)) => conn.query(sql, sql_hash, params),
            Some(PoolSlot::Async(conn)) => conn.query(sql, sql_hash, params).await,
            None => Err(DriverError::Pool("connection already taken".into())),
        }
    }

    /// Execute without result rows (async).
    #[cfg(feature = "async")]
    pub async fn execute_async(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.track(sql_hash);
        match self.conn.as_mut() {
            Some(PoolSlot::Sync(conn)) => conn.execute(sql, sql_hash, params),
            Some(PoolSlot::Async(conn)) => conn.execute(sql, sql_hash, params).await,
            None => Err(DriverError::Pool("connection already taken".into())),
        }
    }

    /// Execute a simple query (async).
    #[cfg(feature = "async")]
    pub async fn simple_query_async(&mut self, sql: &str) -> Result<(), DriverError> {
        match self.conn.as_mut() {
            Some(PoolSlot::Sync(conn)) => conn.simple_query(sql),
            Some(PoolSlot::Async(conn)) => conn.simple_query(sql).await,
            None => Err(DriverError::Pool("connection already taken".into())),
        }
    }

    // --- Deferred pipeline support ---

    /// Ensure a statement is prepared and cached.
    pub(crate) fn ensure_stmt_prepared(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<[u8; 18], DriverError> {
        self.sync_conn_mut()?
            .ensure_stmt_prepared(sql, sql_hash, params)
    }

    /// Write Bind+Execute bytes for a prepared statement into an external buffer.
    pub(crate) fn write_deferred_bind_execute(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        buf: &mut Vec<u8>,
    ) {
        let conn = self
            .sync_conn()
            .expect("sync_conn failed in write_deferred");
        conn.write_deferred_bind_execute(sql, sql_hash, params, buf);
    }

    /// Flush a buffer of deferred Bind+Execute messages as a single pipeline.
    pub(crate) fn flush_deferred_pipeline(
        &mut self,
        buf: &mut Vec<u8>,
        count: usize,
    ) -> Result<Vec<u64>, DriverError> {
        self.sync_conn_mut()?.flush_deferred_pipeline(buf, count)
    }
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        #[cfg(feature = "detect-n-plus-one")]
        self.detector.emit_final_warning();

        if let Some(slot) = self.conn.take() {
            // Check discard conditions based on slot type.
            let should_discard = self.discard
                || self.pool.closed.load(Ordering::Acquire)
                || match &slot {
                    PoolSlot::Sync(conn) => {
                        conn.is_in_failed_transaction()
                            || conn.is_in_transaction()
                            || conn.is_streaming()
                    }
                    #[cfg(feature = "async")]
                    PoolSlot::Async(conn) => {
                        conn.is_in_failed_transaction() || conn.is_in_transaction()
                    }
                };

            if should_discard {
                self.pool.open_count.fetch_sub(1, Ordering::AcqRel);
                return;
            }

            // Stamp last-used time for idle connection tracking.
            // Amortized: only call Instant::now() every 64 returns.
            let mut slot = slot;
            match &mut slot {
                PoolSlot::Sync(conn) => {
                    if conn.query_counter() & 63 == 0 {
                        conn.touch();
                    }
                }
                #[cfg(feature = "async")]
                PoolSlot::Async(conn) => {
                    if conn.query_counter() & 63 == 0 {
                        conn.touch();
                    }
                }
            }

            // Return to pool
            {
                let mut stack = self.pool.stack.lock().unwrap_or_else(|e| e.into_inner());
                stack.push(slot);
            }

            // Notify waiters only if pool was exhausted (someone might be waiting).
            if self.pool.open_count.load(Ordering::Relaxed) >= self.pool.max_size {
                let (_, cvar) = &self.pool.release_pair;
                cvar.notify_one();
            }
        }
    }
}

// --- Transaction ---

/// A database transaction. Sends ROLLBACK on drop if not committed.
///
/// # Example
///
/// ```no_run
/// # fn example() -> Result<(), bsql_driver_postgres::DriverError> {
/// # let pool = bsql_driver_postgres::Pool::connect("postgres://user:pass@localhost/db")?;
/// let mut tx = pool.begin()?;
/// tx.simple_query("INSERT INTO t VALUES (1)")?;
/// tx.commit()?;
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
    pub fn commit(mut self) -> Result<(), DriverError> {
        if self.deferred_count > 0 {
            self.flush_deferred()?;
        }
        self.guard.simple_query("COMMIT")?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction explicitly.
    ///
    /// Discards any deferred operations without sending them.
    pub fn rollback(mut self) -> Result<(), DriverError> {
        self.deferred_buf.clear();
        self.deferred_count = 0;
        self.guard.simple_query("ROLLBACK")?;
        self.committed = true; // prevent double rollback in drop
        Ok(())
    }

    /// Execute a prepared query within the transaction.
    ///
    /// Automatically flushes any deferred operations before executing the query,
    /// ensuring read-your-writes consistency.
    pub fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<QueryResult, DriverError> {
        if self.deferred_count > 0 {
            self.flush_deferred()?;
        }
        self.guard.query(sql, sql_hash, params)
    }

    /// Execute without result rows within the transaction.
    pub fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        self.guard.execute(sql, sql_hash, params)
    }

    /// Execute the same statement N times with different params in one pipeline.
    pub fn execute_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> Result<Vec<u64>, DriverError> {
        self.guard.execute_pipeline(sql, sql_hash, param_sets)
    }

    /// Process each row directly from the wire buffer within a transaction.
    ///
    /// Automatically flushes any deferred operations first.
    pub fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(crate::types::PgDataRow<'_>) -> Result<(), DriverError>,
    {
        if self.deferred_count > 0 {
            self.flush_deferred()?;
        }
        self.guard.for_each(sql, sql_hash, params, f)
    }

    /// Process each DataRow as raw bytes within a transaction.
    ///
    /// Automatically flushes any deferred operations first.
    pub fn for_each_raw<F>(
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
            self.flush_deferred()?;
        }
        self.guard.for_each_raw(sql, sql_hash, params, f)
    }

    /// Simple query within the transaction.
    ///
    /// Automatically flushes any deferred operations first.
    pub fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        if self.deferred_count > 0 {
            self.flush_deferred()?;
        }
        self.guard.simple_query(sql)
    }

    // --- Deferred pipeline API ---

    /// Buffer an execute for deferred pipeline flush.
    ///
    /// The operation is not sent to the server immediately. Instead, the
    /// Bind+Execute message bytes are buffered internally. The buffered
    /// operations are sent as a single pipeline on [`commit()`](Self::commit)
    /// or [`flush_deferred()`](Self::flush_deferred).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn example() -> Result<(), bsql_driver_postgres::DriverError> {
    /// # let pool = bsql_driver_postgres::Pool::connect("postgres://u:p@localhost/db")?;
    /// let mut tx = pool.begin()?;
    /// let sql = "INSERT INTO t (v) VALUES ($1)";
    /// let hash = bsql_driver_postgres::hash_sql(sql);
    ///
    /// // These are buffered, not sent:
    /// tx.defer_execute(sql, hash, &[&1i32])?;
    /// tx.defer_execute(sql, hash, &[&2i32])?;
    /// tx.defer_execute(sql, hash, &[&3i32])?;
    ///
    /// // commit() flushes all 3 as one pipeline + COMMIT = 2 round-trips total
    /// tx.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn defer_execute(
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
        self.guard.ensure_stmt_prepared(sql, sql_hash, params)?;

        // Buffer the Bind+Execute bytes — no I/O
        self.guard
            .write_deferred_bind_execute(sql, sql_hash, params, &mut self.deferred_buf);
        self.deferred_count += 1;
        Ok(())
    }

    /// Flush all deferred operations as a single pipeline.
    ///
    /// Sends all buffered Bind+Execute messages + one Sync in a single TCP write.
    /// Returns the affected row count for each deferred operation.
    pub fn flush_deferred(&mut self) -> Result<Vec<u64>, DriverError> {
        let count = self.deferred_count;
        self.deferred_count = 0;
        self.guard
            .flush_deferred_pipeline(&mut self.deferred_buf, count)
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
            // Take the connection out of the guard and drop it, decrementing open_count.
            if let Some(_slot) = self.guard.conn.take() {
                self.guard.pool.open_count.fetch_sub(1, Ordering::AcqRel);
                // Connection dropped — PG server will auto-rollback when it sees disconnect
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_builder_requires_url() {
        let result = PoolBuilder::new().build();
        assert!(result.is_err());
    }

    #[test]
    fn pool_builder_validates_url() {
        let result = PoolBuilder::new().url("not_a_url").build();
        assert!(result.is_err());
    }

    #[test]
    fn pool_builder_accepts_valid_url() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .unwrap();
        assert_eq!(pool.max_size(), 5);
        assert_eq!(pool.open_count(), 0);
    }

    #[test]
    fn pool_connect_validates_url() {
        let result = Pool::connect("not_a_url");
        assert!(result.is_err());
    }

    #[test]
    fn pool_max_size_zero() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(0)
            .build()
            .unwrap();

        let result = pool.acquire();
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => assert!(msg.contains("exhausted")),
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn pool_clone_shares_state() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .unwrap();

        let pool2 = pool.clone();
        assert_eq!(pool.max_size(), pool2.max_size());
    }

    // --- Audit gap tests ---

    // #60: max_lifetime is configurable
    #[test]
    fn pool_builder_max_lifetime() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_lifetime(Some(Duration::from_secs(60)))
            .build()
            .unwrap();
        assert_eq!(pool.inner.max_lifetime, Some(Duration::from_secs(60)));
    }

    // #60: max_lifetime None
    #[test]
    fn pool_builder_max_lifetime_none() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_lifetime(None)
            .build()
            .unwrap();
        assert_eq!(pool.inner.max_lifetime, None);
    }

    // #62: acquire_timeout set to None (fail-fast)
    #[test]
    fn pool_builder_acquire_timeout_none() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .acquire_timeout(None)
            .build()
            .unwrap();
        assert_eq!(pool.inner.acquire_timeout, None);
    }

    // #62: acquire_timeout custom value
    #[test]
    fn pool_builder_acquire_timeout_custom() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .acquire_timeout(Some(Duration::from_secs(10)))
            .build()
            .unwrap();
        assert_eq!(pool.inner.acquire_timeout, Some(Duration::from_secs(10)));
    }

    // #63: min_idle setting
    #[test]
    fn pool_builder_min_idle() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .min_idle(2)
            .build()
            .unwrap();
        assert_eq!(pool.inner.min_idle, 2);
    }

    // #64: Pool close marks pool as closed
    #[test]
    fn pool_close_marks_closed() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .unwrap();

        assert!(!pool.is_closed());
        pool.close();
        assert!(pool.is_closed());

        // New acquires should fail
        let result = pool.acquire();
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => assert!(msg.contains("closed")),
            Err(e) => panic!("expected Pool(closed) error, got: {e:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    // #67: PoolStatus idle/active counts
    #[test]
    fn pool_status_initial() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(10)
            .build()
            .unwrap();

        let status = pool.status();
        assert_eq!(status.idle, 0);
        assert_eq!(status.active, 0);
        assert_eq!(status.open, 0);
        assert_eq!(status.max_size, 10);
    }

    // Default pool builder values
    #[test]
    fn pool_builder_defaults() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .build()
            .unwrap();

        assert_eq!(pool.max_size(), 10);
        assert_eq!(pool.inner.max_lifetime, Some(Duration::from_secs(30 * 60)));
        assert_eq!(pool.inner.acquire_timeout, Some(Duration::from_secs(5)));
        assert_eq!(pool.inner.min_idle, 0);
    }

    // Pool open_count starts at 0
    #[test]
    fn pool_open_count_initial() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
        assert_eq!(pool.open_count(), 0);
    }

    // --- Task 7: max_stmt_cache_size ---

    #[test]
    fn pool_builder_max_stmt_cache_size_default() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .build()
            .unwrap();
        assert_eq!(pool.inner.max_stmt_cache_size, 256);
    }

    #[test]
    fn pool_builder_max_stmt_cache_size_custom() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_stmt_cache_size(512)
            .build()
            .unwrap();
        assert_eq!(pool.inner.max_stmt_cache_size, 512);
    }

    // --- Auto-UDS detection tests ---

    #[test]
    fn pool_is_uds_false_for_tcp() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[test]
    fn pool_is_uds_true_for_unix_socket() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/tmp").unwrap();
        assert!(pool.is_uds());
    }

    #[cfg(unix)]
    #[test]
    fn pool_is_uds_true_for_var_run_socket() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/var/run/postgresql").unwrap();
        assert!(pool.is_uds());
    }

    #[test]
    fn pool_is_uds_false_for_ip_address() {
        let pool = Pool::connect("postgres://user:pass@127.0.0.1/db").unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[test]
    fn pool_slot_sync_created_for_uds_config() {
        let config = Config::from_url("postgres://user@localhost/db?host=/tmp").unwrap();
        assert!(config.host_is_uds());
    }

    #[test]
    fn pool_slot_tcp_config() {
        let config = Config::from_url("postgres://user:pass@localhost/db").unwrap();
        assert!(!config.host_is_uds());
    }

    // ===============================================================
    // Pool::is_uds — extended tests
    // ===============================================================

    #[test]
    fn pool_is_uds_false_for_hostname() {
        let pool = Pool::connect("postgres://user:pass@db.example.com/db").unwrap();
        assert!(!pool.is_uds());
    }

    #[cfg(unix)]
    #[test]
    fn pool_is_uds_true_for_tmp() {
        let pool = Pool::connect("postgres://user@localhost/db?host=/tmp").unwrap();
        assert!(pool.is_uds());
    }

    // ===============================================================
    // Pool close semantics
    // ===============================================================

    #[test]
    fn pool_close_then_acquire_fails() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(5)
            .build()
            .unwrap();
        pool.close();
        let result = pool.acquire();
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => {
                assert!(msg.contains("closed"), "should say closed: {msg}")
            }
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn pool_is_closed_before_and_after() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
        assert!(!pool.is_closed());
        pool.close();
        assert!(pool.is_closed());
    }

    // ===============================================================
    // Pool exhaustion (fail-fast without timeout)
    // ===============================================================

    #[test]
    fn pool_exhausted_no_timeout() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(0)
            .acquire_timeout(None) // fail-fast
            .build()
            .unwrap();
        let result = pool.acquire();
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

    #[test]
    fn pool_builder_no_url_error() {
        let result = PoolBuilder::new().max_size(5).build();
        assert!(result.is_err());
        match result {
            Err(DriverError::Pool(msg)) => {
                assert!(msg.contains("URL"), "should mention URL: {msg}")
            }
            Err(e) => panic!("expected Pool error, got: {e:?}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn pool_builder_invalid_url_error() {
        let result = PoolBuilder::new().url("ftp://something").build();
        assert!(result.is_err());
    }

    #[test]
    fn pool_builder_stmt_cache_size_zero() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_stmt_cache_size(0)
            .build()
            .unwrap();
        assert_eq!(pool.inner.max_stmt_cache_size, 0);
    }

    // ===============================================================
    // PoolStatus
    // ===============================================================

    #[test]
    fn pool_status_reflects_max_size() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(20)
            .build()
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

    #[test]
    fn pool_clone_shares_config() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(7)
            .build()
            .unwrap();
        let p2 = pool.clone();
        assert_eq!(pool.max_size(), 7);
        assert_eq!(p2.max_size(), 7);
        assert_eq!(pool.open_count(), p2.open_count());
    }

    // ===============================================================
    // set_warmup_sqls
    // ===============================================================

    #[test]
    fn pool_set_warmup_sqls_empty() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
        pool.set_warmup_sqls(&[]);
        let sqls = pool
            .inner
            .warmup_sqls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        assert!(sqls.is_empty());
    }

    #[test]
    fn pool_set_warmup_sqls_multiple() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
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

    #[test]
    fn pool_set_warmup_sqls_overwrite() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
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

    #[test]
    fn pool_status_debug() {
        let pool = Pool::connect("postgres://user:pass@localhost/db").unwrap();
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

    #[test]
    fn pool_builder_full_chain() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(3)
            .max_lifetime(Some(Duration::from_secs(600)))
            .acquire_timeout(Some(Duration::from_secs(5)))
            .min_idle(1)
            .max_stmt_cache_size(128)
            .build()
            .unwrap();
        assert_eq!(pool.max_size(), 3);
        assert_eq!(pool.inner.max_lifetime, Some(Duration::from_secs(600)));
        assert_eq!(pool.inner.acquire_timeout, Some(Duration::from_secs(5)));
        assert_eq!(pool.inner.min_idle, 1);
        assert_eq!(pool.inner.max_stmt_cache_size, 128);
    }

    // --- Audit: PoolGuard drop discards connections in bad state ---

    #[test]
    fn pool_max_size_zero_rejects_all_acquires() {
        let pool = PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .max_size(0)
            .build()
            .unwrap();
        let result = pool.acquire();
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

// --- N+1 detector tests ---

#[cfg(all(test, feature = "detect-n-plus-one"))]
mod n_plus_one_tests {
    use super::NPlusOneDetector;

    #[test]
    fn below_threshold_no_warning() {
        let mut d = NPlusOneDetector::new(10);
        for _ in 0..10 {
            d.track(42);
        }
        assert!(d.check_final().is_none());
    }

    #[test]
    fn above_threshold_warns() {
        let mut d = NPlusOneDetector::new(10);
        for _ in 0..11 {
            d.track(42);
        }
        let w = d.check_final().unwrap();
        assert_eq!(w, (42, 11));
    }

    #[test]
    fn exact_threshold_no_warning() {
        let mut d = NPlusOneDetector::new(5);
        for _ in 0..5 {
            d.track(99);
        }
        assert!(d.check_final().is_none(), "> not >=");
    }

    #[test]
    fn threshold_plus_one_warns() {
        let mut d = NPlusOneDetector::new(5);
        for _ in 0..6 {
            d.track(99);
        }
        assert_eq!(d.check_final(), Some((99, 6)));
    }

    #[test]
    fn alternating_hashes_no_warning() {
        let mut d = NPlusOneDetector::new(2);
        for i in 0..100 {
            d.track(if i % 2 == 0 { 1 } else { 2 });
        }
        assert!(d.check_final().is_none());
    }

    #[test]
    fn single_query_no_warning() {
        let mut d = NPlusOneDetector::new(10);
        d.track(42);
        assert!(d.check_final().is_none());
    }

    #[test]
    fn no_queries_no_warning() {
        let d = NPlusOneDetector::new(10);
        assert!(d.check_final().is_none());
    }

    #[test]
    fn threshold_zero_warns_on_second() {
        let mut d = NPlusOneDetector::new(0);
        d.track(42);
        // count=1, threshold=0 -> 1 > 0 -> warn
        assert_eq!(d.check_final(), Some((42, 1)));
    }

    #[test]
    fn threshold_max_never_warns() {
        let mut d = NPlusOneDetector::new(u16::MAX);
        for _ in 0..1000 {
            d.track(42);
        }
        assert!(d.check_final().is_none());
    }

    #[test]
    fn saturating_add_no_overflow() {
        let mut d = NPlusOneDetector::new(10);
        d.last_query_hash = 42;
        d.repeat_count = u16::MAX - 1;
        d.track(42); // saturating_add -> MAX
        d.track(42); // saturating_add -> still MAX
        assert_eq!(d.repeat_count, u16::MAX);
    }

    #[test]
    fn different_hash_resets() {
        let mut d = NPlusOneDetector::new(100);
        for _ in 0..50 {
            d.track(1);
        }
        d.track(2); // resets
        assert_eq!(d.repeat_count, 1);
        assert_eq!(d.last_query_hash, 2);
    }

    #[test]
    fn multiple_n_plus_one_sequences() {
        let mut d = NPlusOneDetector::new(3);
        // First sequence: hash=1, 5 times (>3 -> warning on switch)
        for _ in 0..5 {
            d.track(1);
        }
        // Switch triggers warning for hash=1
        // Second sequence: hash=2, 4 times (>3 -> check_final catches it)
        for _ in 0..4 {
            d.track(2);
        }
        // check_final sees hash=2, count=4 > 3
        assert_eq!(d.check_final(), Some((2, 4)));
    }

    #[test]
    fn warning_emitted_on_hash_switch() {
        let mut d = NPlusOneDetector::new(2);
        d.track(10);
        d.track(10);
        d.track(10); // count=3 > 2
        // Switch hash — this internally calls emit_warning for hash=10
        d.track(20);
        // Now tracking hash=20, count=1
        assert_eq!(d.last_query_hash, 20);
        assert_eq!(d.repeat_count, 1);
    }

    #[test]
    fn hash_zero_treated_normally() {
        let mut d = NPlusOneDetector::new(2);
        d.track(0);
        d.track(0);
        d.track(0);
        // hash=0 but check_final requires hash != 0 — no warning
        assert!(d.check_final().is_none());
    }

    #[test]
    fn long_sequence_correct_count() {
        let mut d = NPlusOneDetector::new(10);
        for _ in 0..500 {
            d.track(42);
        }
        assert_eq!(d.check_final(), Some((42, 500)));
    }

    #[test]
    fn two_queries_below_threshold() {
        let mut d = NPlusOneDetector::new(10);
        d.track(1);
        d.track(1);
        assert!(d.check_final().is_none());
    }

    #[test]
    fn interleaved_then_burst() {
        let mut d = NPlusOneDetector::new(3);
        // Interleaved: no trigger
        d.track(1);
        d.track(2);
        d.track(1);
        d.track(2);
        // Burst: hash=5, 5 times
        for _ in 0..5 {
            d.track(5);
        }
        assert_eq!(d.check_final(), Some((5, 5)));
    }

    // --- Builder threshold wiring ---

    #[test]
    fn pool_builder_n_plus_one_threshold_default() {
        let pool = super::PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .build()
            .unwrap();
        assert_eq!(pool.inner.n_plus_one_threshold, 10);
    }

    #[test]
    fn pool_builder_n_plus_one_threshold_custom() {
        let pool = super::PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .n_plus_one_threshold(5)
            .build()
            .unwrap();
        assert_eq!(pool.inner.n_plus_one_threshold, 5);
    }

    #[test]
    fn pool_builder_n_plus_one_threshold_zero() {
        let pool = super::PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .n_plus_one_threshold(0)
            .build()
            .unwrap();
        assert_eq!(pool.inner.n_plus_one_threshold, 0);
    }

    #[test]
    fn pool_builder_n_plus_one_threshold_max() {
        let pool = super::PoolBuilder::new()
            .url("postgres://user:pass@localhost/db")
            .n_plus_one_threshold(u16::MAX)
            .build()
            .unwrap();
        assert_eq!(pool.inner.n_plus_one_threshold, u16::MAX);
    }

    #[test]
    fn one_then_different_no_warning() {
        let mut d = NPlusOneDetector::new(10);
        d.track(1);
        d.track(2);
        // hash=1 had count=1 (below 10), hash=2 has count=1 (below 10)
        assert!(d.check_final().is_none());
    }

    #[test]
    fn nonzero_hash_after_zero_init() {
        // First call with nonzero hash: else branch (0 != hash),
        // emit_warning for old (hash=0, count=0) - nothing.
        // Set last=hash, count=1.
        let mut d = NPlusOneDetector::new(0);
        d.track(42);
        let w = d.check_final().unwrap();
        assert_eq!(w, (42, 1));
    }
}
