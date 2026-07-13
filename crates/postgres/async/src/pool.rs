//! A bounded, self-resetting async connection pool.
//!
//! # Isolation (reset-on-acquire)
//!
//! A pooled connection carries session state — GUCs (`search_path`, …), temp
//! tables, `LISTEN` channels, advisory locks, an open transaction. Handing a
//! used connection to the next logical user unchanged leaks that state (a real
//! cross-user isolation bug). This pool RESETS a reused connection on acquire,
//! before handing it out: [`Connection::reset_session`] clears every bleedable
//! item while DELIBERATELY KEEPING prepared statements (content-addressed query
//! plans, safe to share — so the server-side plan reuse survives across
//! checkouts). A freshly created connection is already clean and skips the
//! reset. A reset that FAILS evicts the connection (it is never handed out
//! un-reset); the pool then tries the next idle connection or creates a fresh
//! one. `Drop` cannot `.await`, so the (awaitable) reset runs on acquire rather
//! than on return.
//!
//! # Backpressure (acquire timeout)
//!
//! [`get`](Pool::get) waits at most the pool's configured acquire deadline for a
//! permit; on exhaustion it returns [`DriverError::PoolTimeout`] rather than
//! blocking forever. [`get_timeout`](Pool::get_timeout) overrides the deadline
//! per call.
//!
//! # Liveness (a dead peer can never hang a checkout)
//!
//! The acquire deadline above bounds only the semaphore WAIT. The post-acquire
//! health-gate reset is bounded SEPARATELY, by its own liveness deadline: on a
//! silently-vanished peer (a half-open socket — a NAT idle-drop, a cable pull, an
//! AZ partition — where no FIN/RST ever arrives) [`Connection::reset_session`]
//! arms an absolute read deadline (the connection's `connect_timeout`), so a
//! reset that would otherwise block for the kernel's `tcp_retries2` budget
//! (~15 min) ELAPSES into a classified error within a bounded wall-clock. That
//! routes into the eviction arm below (`Err(_evict) => drop; continue`), so a
//! dead pooled connection is evicted and the caller gets a FRESH connection — or,
//! if the whole budget is spent, a classified acquire-timeout — never a
//! multi-minute hang. So `get()` as a WHOLE is bounded, not merely its permit
//! wait.
//!
//! # Cancellation safety
//!
//! The capacity permit is an owned [`OwnedSemaphorePermit`] moved into the
//! returned [`PooledConnection`] and released by its `Drop`. If a `get` future is
//! dropped mid-`.await` (an outer timeout, a `select!` loser, a cancelled
//! request) — at the acquire, the reset, or the connect — the owned permit's own
//! `Drop` returns the capacity automatically. There is no manual
//! `forget` + `add_permits` accounting, so there is no window in which a permit
//! can be permanently lost.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use bsql_postgres_core::{ConnectConfig, DiagEvent, Diagnostics, DriverError, PoolStats};
use crate::connection::Connection;

/// An idle pooled connection plus its lifecycle stamps, so a checkout can REAP a
/// connection that outlived `max_lifetime` (age since it was established) or
/// `idle_timeout` (since it last returned) BEFORE handing it out — minting a
/// fresh one instead of reusing a stale one.
struct Idle {
    conn: Connection,
    /// When the underlying connection was first established (for `max_lifetime`).
    created: Instant,
    /// When it last returned to the idle set (for `idle_timeout`).
    returned: Instant,
}

/// Whether an idle connection must be REAPED at checkout: it has outlived
/// `max_lifetime` (age since `created`) or `idle_timeout` (idle since
/// `returned`). Both bounds are opt-in (`None` = disabled), so the default pool
/// reaps nothing and reuses every healthy connection — no behaviour change for an
/// existing consumer. `saturating_duration_since` is total (an `Instant` is
/// monotonic, so `now >= created`, but the saturating form needs no proof).
fn is_stale(
    created: Instant,
    returned: Instant,
    now: Instant,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
) -> bool {
    if let Some(max) = max_lifetime
        && now.saturating_duration_since(created) > max
    {
        return true;
    }
    if let Some(idle) = idle_timeout
        && now.saturating_duration_since(returned) > idle
    {
        return true;
    }
    false
}

/// The default acquire deadline when a caller constructs the pool with
/// [`Pool::new`] and does not override it per call via [`Pool::get_timeout`].
/// Finite by construction: the pool never blocks forever.
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// A cloneable handle to a bounded pool of [`Connection`]s.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: ConnectConfig,
    connections: Mutex<VecDeque<Idle>>,
    /// The capacity gate, as an `Arc` so a checkout can take an
    /// [`OwnedSemaphorePermit`] (`'static`) that rides in the guard and returns
    /// capacity on the guard's — or a cancelled `get` future's — `Drop`.
    semaphore: Arc<Semaphore>,
    max_size: usize,
    acquire_timeout: Duration,
    /// The structured-diagnostics configuration installed on every connection the
    /// pool mints (via [`Connection::connect_with`]), and the sink the pool's own
    /// saturation events emit through. `Default` (off) unless the pool was built
    /// through [`Pool::builder`]. Fixed for the pool's life, so a clone/checkout
    /// never races a reconfiguration.
    diagnostics: Diagnostics,
    /// Monotonic count of checkouts that waited out their acquire deadline.
    acquire_timeouts: AtomicU64,
    /// Monotonic count of pooled connections evicted on checkout (a failed
    /// health-gate reset).
    connections_evicted: AtomicU64,
    /// Concurrent checkouts currently blocked waiting for a permit (bracketed by
    /// [`WaiterGuard`], cancellation-safe), used only to feed `waiters_high_water`.
    current_waiters: AtomicU64,
    /// High-water mark of [`current_waiters`](Self::current_waiters).
    waiters_high_water: AtomicU64,
    /// Maximum lifetime of a pooled connection (since it was established), or
    /// `None` (disabled). At checkout, a connection older than this is reaped and
    /// replaced. Fixed for the pool's life.
    max_lifetime: Option<Duration>,
    /// Maximum idle time of a pooled connection (since it last returned), or
    /// `None` (disabled). At checkout, a connection idle longer than this is
    /// reaped and replaced. Fixed for the pool's life.
    idle_timeout: Option<Duration>,
}

/// A cancellation-safe bracket around the semaphore acquire: it bumps the pool's
/// concurrent-waiter gauge (and its high-water mark) on creation and decrements
/// on drop — so even a `get` future dropped mid-`.await` (an outer timeout, a
/// `select!` loser) restores the gauge, never leaking a phantom waiter.
struct WaiterGuard<'a> {
    current: &'a AtomicU64,
}

impl<'a> WaiterGuard<'a> {
    fn new(current: &'a AtomicU64, high_water: &'a AtomicU64) -> Self {
        // `fetch_add` returns the PRIOR value; the count including self is +1.
        let now = current.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        // Raise the high-water mark to at least `now` (a no-op if already higher).
        high_water.fetch_max(now, Ordering::Relaxed);
        Self { current }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("max_size", &self.inner.max_size)
            .field("idle", &self.idle_count())
            .field("acquire_timeout", &self.inner.acquire_timeout)
            .finish()
    }
}

impl Pool {
    /// Create a pool over `config` holding at most `max_size` connections, with
    /// the default 30s acquire deadline.
    ///
    /// Connections are created lazily on demand. To set a non-default deadline at
    /// construction use [`with_acquire_timeout`](Self::with_acquire_timeout); to
    /// override it for a single checkout use [`get_timeout`](Self::get_timeout).
    #[must_use]
    pub fn new(config: ConnectConfig, max_size: usize) -> Self {
        Self::with_acquire_timeout(config, max_size, DEFAULT_ACQUIRE_TIMEOUT)
    }

    /// Create a pool with an explicit default acquire deadline, fixed for the
    /// pool's lifetime.
    ///
    /// The deadline is set at construction (not mutated later), so a
    /// clone/checkout never races an accounting change and no connections are
    /// ever discarded to reconfigure it. Construction is a pure, infallible field
    /// set — connections are lazy (nothing is dialed here), so this is NOT `async`
    /// and does NOT return a `Result` (that would be defensive error handling for
    /// an impossible event); it mirrors the sync driver's `Pool::new` exactly.
    #[must_use]
    pub fn with_acquire_timeout(
        config: ConnectConfig,
        max_size: usize,
        acquire_timeout: Duration,
    ) -> Self {
        // `None`/`None`: no lifetime/idle reaping by default (existing behaviour).
        Self::from_parts(config, max_size, acquire_timeout, Diagnostics::default(), None, None)
    }

    /// Start building a pool with structured diagnostics (a
    /// [`DiagSink`](bsql_postgres_core::DiagSink) + a slow-query threshold): the
    /// installed configuration rides every connection the pool mints, and the
    /// pool's own saturation events (acquire timeout, connection eviction) emit
    /// through the same sink.
    ///
    /// Diagnostics is NOT a [`ConnectConfig`] field, so the config footprint is
    /// untouched; it is a pool-level (not per-connection) setting installed here.
    ///
    /// ```no_run
    /// # use bsql_postgres_async::Pool;
    /// # use bsql_postgres_core::{ConnectConfig, DiagEvent};
    /// # use std::time::Duration;
    /// let pool = Pool::builder(ConnectConfig::new("localhost", "u"), 16)
    ///     .acquire_timeout(Duration::from_secs(5))
    ///     .on_diagnostic(|ev: &DiagEvent<'_>| eprintln!("{ev:?}"))
    ///     .slow_query_threshold(Duration::from_millis(200))
    ///     .build();
    /// # let _ = pool;
    /// ```
    pub fn builder(config: ConnectConfig, max_size: usize) -> PoolBuilder {
        PoolBuilder {
            config,
            max_size,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            diagnostics: Diagnostics::default(),
            max_lifetime: None,
            idle_timeout: None,
        }
    }

    /// The one construction point every constructor + the builder route through,
    /// so the field set cannot drift between them.
    fn from_parts(
        config: ConnectConfig,
        max_size: usize,
        acquire_timeout: Duration,
        diagnostics: Diagnostics,
        max_lifetime: Option<Duration>,
        idle_timeout: Option<Duration>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(VecDeque::with_capacity(max_size)),
                semaphore: Arc::new(Semaphore::new(max_size)),
                max_size,
                acquire_timeout,
                diagnostics,
                acquire_timeouts: AtomicU64::new(0),
                connections_evicted: AtomicU64::new(0),
                current_waiters: AtomicU64::new(0),
                waiters_high_water: AtomicU64::new(0),
                max_lifetime,
                idle_timeout,
            }),
        }
    }

    /// Check out a connection, waiting up to the pool's configured acquire
    /// deadline. A reused connection is reset before it is handed out; a fresh
    /// one is created clean.
    ///
    /// # Errors
    ///
    /// [`DriverError::PoolTimeout`] if no permit becomes free within the
    /// deadline; a connect / reset error otherwise.
    pub async fn get(&self) -> Result<PooledConnection, DriverError> {
        self.get_timeout(self.inner.acquire_timeout).await
    }

    /// Like [`get`](Self::get) but with an explicit acquire deadline for this
    /// call, overriding the pool default.
    pub async fn get_timeout(&self, timeout: Duration) -> Result<PooledConnection, DriverError> {
        // Bounded wait for an OWNED permit: exhaustion past the deadline is
        // classified backpressure, never an infinite block. The owned permit
        // rides the returned guard and is released on its `Drop`; if THIS future
        // is dropped at any `.await` below (acquire, reset, or connect), the
        // permit's own `Drop` returns the capacity — no manual accounting, no
        // leak window.
        //
        let permit = {
            // FAST PATH: a permit is immediately available → the checkout never
            // BLOCKS, so no waiter is counted (parity with the sync pool, which
            // counts only queued waiters; `waiters_high_water` stays `0` when
            // nobody had to wait — as its doc promises).
            match Arc::clone(&self.inner.semaphore).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_would_block) => {
                    // CONTENDED: no permit is free, so this checkout WILL block on
                    // the semaphore — NOW count it as a waiter. The `WaiterGuard`
                    // feeds the concurrent-waiter gauge + high-water mark and drops
                    // (restoring the gauge) when this scope ends OR the future is
                    // cancelled — cancellation-safe like the permit.
                    let _waiter = WaiterGuard::new(
                        &self.inner.current_waiters,
                        &self.inner.waiters_high_water,
                    );
                    match tokio::time::timeout(
                        timeout,
                        Arc::clone(&self.inner.semaphore).acquire_owned(),
                    )
                    .await
                    {
                        Ok(Ok(permit)) => permit,
                        // The semaphore is never closed by this pool; classify defensively.
                        Ok(Err(_closed)) => {
                            return Err(DriverError::Io(std::io::Error::other("pool closed")));
                        }
                        Err(_elapsed) => {
                            // Saturation: count it and surface a structured event
                            // before returning the classified backpressure error.
                            self.inner.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
                            self.inner
                                .diagnostics
                                .emit(&DiagEvent::PoolAcquireTimeout { waited: timeout });
                            return Err(DriverError::PoolTimeout);
                        }
                    }
                }
            }
        };

        // Drain stale/broken idle connections and, on failure, create a fresh
        // one — all riding the single owned permit acquired above.
        loop {
            let reused = {
                // Mutex poison recovery, not a data fallback: a poisoned lock means
                // another thread panicked while holding it; `into_inner` recovers
                // the guard so the pool keeps operating rather than propagating the
                // panic. The connection set it guards is observed as normal.
                #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let mut conns = self.inner.connections.lock().unwrap_or_else(|e| e.into_inner());
                // A connection enters the idle set ONLY when healthy (`Drop` guards
                // on `is_healthy`; a fatal verb or an explicit `close` evicts at
                // RETURN time), and nothing runs on an idle pooled connection to
                // flip its health, so the front is always reusable — pop it
                // directly. Genuine idle-death (the server closed the socket while
                // idle) is `is_healthy()==true` and is caught by the reset failing
                // on acquire below (evict + retry), not by a pop-time probe.
                conns.pop_front()
            };

            match reused {
                Some(idle) => {
                    let Idle { mut conn, created, returned } = idle;
                    // LAZY REAPER — before the liveness reset, drop a connection
                    // that outlived `max_lifetime` (age) or `idle_timeout` (idle):
                    // gracefully close it (bounded Terminate) and mint a FRESH one
                    // instead of reusing a stale plan/socket. Reaping-at-checkout is
                    // chosen over a background timer task (which would add a runtime
                    // dependency + lifecycle complexity, and cannot even spawn from a
                    // pool built outside a runtime) and over reap-on-return (which
                    // cannot catch a connection that ages past a bound WHILE idle,
                    // and whose Drop cannot `.await` a graceful close). Disabled by
                    // default (`None`/`None`), so this is a no-op unless configured.
                    //
                    // ZERO-COST WHEN OFF: read the clock (and run the staleness
                    // check) ONLY if a bound is set. Rust's `&&` short-circuits, so
                    // a default pool (both `None`) never evaluates `Instant::now()`
                    // here — no per-checkout timing work, matching the
                    // `slow_query_armed` / `n1-detect` zero-cost-off discipline.
                    if (self.inner.max_lifetime.is_some() || self.inner.idle_timeout.is_some())
                        && is_stale(
                            created,
                            returned,
                            Instant::now(),
                            self.inner.max_lifetime,
                            self.inner.idle_timeout,
                        )
                    {
                        self.inner.connections_evicted.fetch_add(1, Ordering::Relaxed);
                        self.inner.diagnostics.emit(&DiagEvent::PoolConnectionEvicted);
                        conn.close_graceful().await;
                        drop(conn);
                        // Retry with the next idle connection / a fresh connect,
                        // retaining the owned permit.
                        continue;
                    }
                    // EXACTLY-ONCE LIVENESS GATE — do NOT fuse this reset into the
                    // user's first verb to save its round trip. The pre-verb reset
                    // is the proof the connection is still alive BEFORE the user's
                    // verb is sent: if it fails, recovery is transparent (evict +
                    // reconnect + retry) precisely because the verb has NOT run yet,
                    // so a non-idempotent verb (INSERT, …) is never at risk of double
                    // execution. Fusing the reset into the first verb would widen the
                    // ambiguous-failure window to idle-deaths — a fused failure
                    // cannot distinguish "the verb ran" from "it never arrived" (the
                    // two-generals problem, irreducible), forcing either a
                    // double-execution risk or a user-visible error where recovery is
                    // invisible today. The ~10–25µs local RTT is the minimum price of
                    // exactly-once, not an optimization gap.
                    //
                    // Reset a REUSED connection before handing it out. A reset
                    // failure evicts it (drop) and tries the next idle one — never
                    // hand out an un-reset (dirty) connection. The owned permit is
                    // retained across the retry. `created` rides into the guard so a
                    // reused connection keeps its ORIGINAL birth time (max_lifetime
                    // measures true age, not age-since-last-checkout).
                    match conn.pool_reset_session().await {
                        Ok(()) => {
                            return Ok(PooledConnection {
                                conn: Some(conn),
                                pool: Arc::clone(&self.inner),
                                permit,
                                created,
                            });
                        }
                        Err(_evict) => {
                            // A dead pooled connection: count the eviction and
                            // surface it (a steady stream is a reconnect storm),
                            // then drop it and retry with the next idle one / a
                            // fresh connect.
                            self.inner.connections_evicted.fetch_add(1, Ordering::Relaxed);
                            self.inner.diagnostics.emit(&DiagEvent::PoolConnectionEvicted);
                            drop(conn);
                            continue;
                        }
                    }
                }
                // No reusable idle connection: create a FRESH one (already clean,
                // no reset needed) carrying the pool's diagnostics. It rides the
                // permit already held; a connect failure drops the permit
                // (returning capacity) on the way out.
                None => match Connection::connect_with(
                    &self.inner.config,
                    &self.inner.diagnostics,
                )
                .await
                {
                    Ok(conn) => {
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: Arc::clone(&self.inner),
                            permit,
                            // Birth time of a freshly-established connection.
                            created: Instant::now(),
                        });
                    }
                    Err(e) => return Err(e),
                },
            }
        }
    }

    /// The number of idle (checked-in) connections currently held.
    #[must_use]
    pub fn idle_count(&self) -> usize {
        // Mutex poison recovery (see `get_timeout`), not a data fallback.
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let conns = self.inner.connections.lock().unwrap_or_else(|e| e.into_inner());
        conns.len()
    }

    /// The maximum number of connections the pool will hold.
    #[must_use]
    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }

    /// A snapshot of the pool's operational counters (idle/max plus the monotonic
    /// acquire-timeout, eviction, and waiter high-water gauges) — the pull-style
    /// complement to the push-style [`DiagEvent`] pool events. Cheap: a few
    /// relaxed atomic loads plus the idle count under the lock.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        PoolStats::new(
            self.idle_count(),
            self.inner.max_size,
            self.inner.acquire_timeouts.load(Ordering::Relaxed),
            self.inner.connections_evicted.load(Ordering::Relaxed),
            self.inner.waiters_high_water.load(Ordering::Relaxed),
        )
    }

    /// GRACEFULLY DRAIN the pool: send a protocol `Terminate` to every
    /// currently-IDLE pooled connection so the server sees a CLEAN disconnect,
    /// then close each socket.
    ///
    /// Without this, dropping the pool drops each pooled connection's socket
    /// bare — an RST/FIN with no `Terminate` — and PostgreSQL logs an "unexpected
    /// EOF on client connection" per connection (an error-log flood at shutdown
    /// for a large pool). `close` replaces that with the graceful `Terminate` the
    /// protocol defines for a clean disconnect.
    ///
    /// # Consumes the pool (use-after-close is a COMPILE error)
    ///
    /// `close` takes `self` by value, so this handle cannot be used afterward:
    ///
    /// ```compile_fail
    /// # async fn demo(pool: bsql_postgres_async::Pool) {
    /// pool.close().await;
    /// pool.get().await.unwrap();   // ERROR[E0382]: `pool` was moved into `close`
    /// # }
    /// ```
    ///
    /// # Bounded on a dead peer
    ///
    /// Each `Terminate` rides [`Connection::close_graceful`], whose write is
    /// bounded by the connection's `connect_timeout`, so a black-hole peer (a
    /// half-open socket with a full send buffer) cannot hang the drain for the
    /// kernel's `tcp_retries2` budget (~15 min). Best-effort: a per-connection
    /// failure is swallowed and the drain continues to the next connection.
    ///
    /// # Scope
    ///
    /// Drains only connections IDLE at the moment of the call. A connection
    /// CHECKED OUT right now returns to the (detached) idle set on its own `Drop`
    /// and closes with a bare socket drop when the last pool `Arc` releases — this
    /// is a graceful drain of the idle set, not a barrier over in-flight checkouts.
    /// The `Pool` is a cloneable handle; if other clones survive, they still
    /// observe the now-empty idle set and dial fresh on their next `get`.
    pub async fn close(self) {
        // Move the idle connections OUT from under the lock and RELEASE the lock
        // before any `.await`: a `std::sync::Mutex` guard must never be held
        // across an await point (it is not async-aware — holding it would risk a
        // deadlock and trips `clippy::await_holding_lock`). `mem::take` swaps in an
        // empty deque, so the guard's scope contains no I/O.
        let idle: VecDeque<Idle> = {
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut conns = self.inner.connections.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *conns)
        };
        for entry in idle {
            let mut conn = entry.conn;
            conn.close_graceful().await;
        }
    }
}

/// A builder for a [`Pool`] with an acquire deadline and structured diagnostics.
///
/// Obtained from [`Pool::builder`]. All settings beyond `config` + `max_size`
/// are optional; [`build`](Self::build) is infallible (connections are lazy, so
/// nothing is dialed here).
#[derive(Debug)]
#[must_use = "a PoolBuilder does nothing until `.build()` is called"]
pub struct PoolBuilder {
    config: ConnectConfig,
    max_size: usize,
    acquire_timeout: Duration,
    diagnostics: Diagnostics,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
}

impl PoolBuilder {
    /// Set the default acquire deadline (overridable per checkout via
    /// [`Pool::get_timeout`]). Defaults to 30s.
    pub fn acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// Set the maximum LIFETIME of a pooled connection (age since it was
    /// established). At checkout, a connection older than this is gracefully
    /// closed (a bounded `Terminate`) and replaced with a fresh one, so a
    /// long-lived pool rotates its connections — bounding server-side per-backend
    /// memory growth and letting a rolling credential / DNS change take effect.
    /// `None` (the default) disables the bound: connections live until they break.
    ///
    /// Reaping is LAZY (checked at checkout), so it adds no background task — see
    /// [`Pool::get`] for why lazy-at-checkout is chosen over a timer task.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Self {
        self.max_lifetime = max_lifetime;
        self
    }

    /// Set the IDLE timeout of a pooled connection (time since it last returned to
    /// the pool). At checkout, a connection idle longer than this is gracefully
    /// closed and replaced, so a pool that went quiet sheds connections the server
    /// may itself time out rather than handing out a likely-dead one. `None` (the
    /// default) disables the bound.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Install a diagnostics callback closure — the sink every minted connection
    /// carries and the pool's saturation events emit through.
    pub fn on_diagnostic(
        mut self,
        sink: impl Fn(&bsql_postgres_core::DiagEvent<'_>) + Send + Sync + 'static,
    ) -> Self {
        self.diagnostics = self.diagnostics.on_event(sink);
        self
    }

    /// Install a complete [`Diagnostics`] configuration (a pre-built sink +
    /// slow-query threshold), replacing whatever this builder held.
    pub fn diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// Set the slow-query threshold: a query whose server round trip meets or
    /// exceeds it emits a slow-query event (off by default — no timing cost).
    pub fn slow_query_threshold(mut self, threshold: Duration) -> Self {
        self.diagnostics = self.diagnostics.slow_query_threshold(threshold);
        self
    }

    /// Build the pool. Infallible — connections are created lazily on first
    /// [`Pool::get`].
    #[must_use]
    pub fn build(self) -> Pool {
        Pool::from_parts(
            self.config,
            self.max_size,
            self.acquire_timeout,
            self.diagnostics,
            self.max_lifetime,
            self.idle_timeout,
        )
    }
}

/// A checked-out connection that returns itself (and its capacity permit) to the
/// pool on drop.
///
/// # Accessing the connection
///
/// Access is through [`conn`](Self::conn) / [`conn_mut`](Self::conn_mut), which
/// return the borrowed [`Connection`]. They cannot be `Deref`/`DerefMut`: the
/// held connection is an `Option` (the only `#![forbid(unsafe_code)]`-safe way
/// for `Drop` to move it back into the pool is `Option::take`), and an
/// infallible `Deref` would need a panic on the `None` arm. That `None` is
/// reachable ONLY transiently inside `Drop` after the connection has been taken,
/// where no accessor can run — so the accessors never observe it in practice,
/// but classify it as [`DriverError::NotReady`] rather than panicking if the
/// invariant were ever violated.
pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<PoolInner>,
    /// The underlying connection's birth time, preserved across the checkout so
    /// its `Drop` restamps the returned [`Idle`] with the ORIGINAL `created` — so
    /// `max_lifetime` measures true age, not age since the last checkout.
    created: Instant,
    /// The capacity permit, held for its `Drop`. It is dropped AFTER
    /// [`PooledConnection::drop`] returns the connection to the idle set (struct
    /// fields drop after the explicit `Drop::drop` body), so capacity is released
    /// only once the connection is back in the pool — a waking waiter always sees
    /// the returned connection, never an empty idle set with a free permit.
    #[expect(dead_code, reason = "held for its Drop, which returns the acquire permit to the semaphore; never read directly")]
    permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("checked_out", &self.conn.is_some())
            .finish()
    }
}

impl PooledConnection {
    /// Borrow the underlying connection.
    ///
    /// # Errors
    ///
    /// [`DriverError::NotReady`] only in the structurally-unreachable case that
    /// the connection was already taken (see the type docs) — never in practice.
    pub fn conn(&self) -> Result<&Connection, DriverError> {
        self.conn.as_ref().ok_or(DriverError::NotReady)
    }

    /// Mutably borrow the underlying connection to run a command.
    ///
    /// # Errors
    ///
    /// See [`conn`](Self::conn).
    pub fn conn_mut(&mut self) -> Result<&mut Connection, DriverError> {
        self.conn.as_mut().ok_or(DriverError::NotReady)
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // `Option::take` is the only forbid-unsafe way to move the connection out
        // of a `Drop` type. A healthy connection returns to the idle set (dirty —
        // it is reset on the NEXT acquire, since Drop cannot `.await`); an
        // unhealthy one is dropped (evicted). The capacity `permit` field then
        // drops after this body, returning the slot to the semaphore.
        if let Some(conn) = self.conn.take()
            && conn.is_healthy()
        {
            // Mutex poison recovery (see `Pool::get_timeout`), not a data
            // fallback: return the connection rather than silently discarding it.
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut conns = self.pool.connections.lock().unwrap_or_else(|e| e.into_inner());
            // Restamp `returned` for `idle_timeout` — but read the clock ONLY when
            // idle reaping is enabled (ZERO-COST WHEN OFF). When `idle_timeout` is
            // `None`, `returned` is never consumed by the checkout `is_stale` gate,
            // so reuse the clock-free `created` stamp instead of an `Instant::now()`.
            // `created` is always preserved (for `max_lifetime`), so age is measured
            // from birth.
            let returned = if self.pool.idle_timeout.is_some() {
                Instant::now()
            } else {
                self.created
            };
            conns.push_back(Idle {
                conn,
                created: self.created,
                returned,
            });
        }
    }
}

#[cfg(test)]
mod reaper_tests {
    //! Offline witness for the lazy reaper's staleness gate. Documents the
    //! invariant the zero-cost-off short-circuit relies on: with BOTH bounds
    //! `None`, `is_stale` is unconditionally `false` — so the checkout gate
    //! `max_lifetime.is_some() || idle_timeout.is_some()` can skip the
    //! `Instant::now()` read entirely (the elided branch would have returned
    //! `false` anyway).

    use super::is_stale;
    use std::time::{Duration, Instant};

    #[test]
    fn disabled_bounds_are_never_stale() {
        let birth = Instant::now();
        let much_later = birth + Duration::from_secs(1_000_000);
        // No bound set → never stale regardless of the stamps → the disabled
        // checkout path is safe to skip the clock read.
        assert!(!is_stale(birth, birth, much_later, None, None));
    }

    #[test]
    fn each_bound_triggers_independently() {
        let birth = Instant::now();
        let now = birth + Duration::from_secs(10);
        // max_lifetime = age since `created`.
        assert!(is_stale(birth, now, now, Some(Duration::from_secs(5)), None));
        assert!(!is_stale(birth, now, now, Some(Duration::from_secs(50)), None));
        // idle_timeout = time since `returned`.
        assert!(is_stale(now, birth, now, None, Some(Duration::from_secs(5))));
        assert!(!is_stale(now, birth, now, None, Some(Duration::from_secs(50))));
    }
}
