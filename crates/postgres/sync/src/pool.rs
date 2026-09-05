//! A bounded, self-resetting blocking connection pool.
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
//! one. The reset (a round trip) runs outside the pool lock, so it never blocks
//! other threads.
//!
//! # Backpressure (acquire timeout)
//!
//! [`get`](Pool::get) waits at most the pool's configured acquire deadline for a
//! slot; on exhaustion it returns [`DriverError::PoolTimeout`] rather than
//! blocking forever. [`get_timeout`](Pool::get_timeout) overrides the deadline
//! per call.
//!
//! # Liveness (a dead peer can never hang a checkout)
//!
//! The acquire deadline above bounds only the FIFO slot WAIT. The post-acquire
//! health-gate reset is bounded SEPARATELY, by its own liveness deadline: on a
//! silently-vanished peer (a half-open socket — a NAT idle-drop, a cable pull, an
//! AZ partition — where no FIN/RST ever arrives) [`Connection::reset_session`]
//! arms a bounded socket read+write timeout (the connection's `connect_timeout`),
//! so a reset that would otherwise block for the kernel's `tcp_retries2` budget
//! (~15 min) ELAPSES into a classified error within a bounded wall-clock. That
//! routes into the eviction arm below (`Err(_evict)` → evict + retry), so a dead
//! pooled connection is evicted and the caller gets a FRESH connection — or, if
//! the whole budget is spent, a classified acquire-timeout — never a multi-minute
//! hang. So `get()` as a WHOLE is bounded, not merely its slot wait.
//!
//! # Fairness (FIFO hand-off)
//!
//! Checkouts are served in FIFO arrival order — the same fairness the async pool
//! gets for free from `tokio::sync::Semaphore`. A plain `std::sync::Condvar` +
//! `notify_one` does NOT give this: `notify_one` wakes a parked waiter, but a
//! FRESH `get()` caller can win the mutex race before the woken waiter re-locks,
//! steal the just-returned connection, and leave — the woken waiter re-locks,
//! finds nothing, and re-waits, having burned its wakeup. Under sustained
//! arrivals one waiter can lose that race many times in a row (unbounded tail;
//! eventually a false `PoolTimeout` while connections are actively cycling). So
//! each blocked waiter instead enqueues its OWN [`Condvar`] as a ticket in a
//! `VecDeque` and is served ONLY when it reaches the FRONT — a fresh caller that
//! finds a non-empty queue must enqueue BEHIND it (no barging by construction),
//! and a freed slot wakes exactly the front waiter (an O(1) hand-off, not a
//! thundering `notify_all`). The uncontended path allocates no ticket: an empty
//! queue with a free slot is taken directly.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bsql_postgres_core::{ConnectConfig, DiagEvent, Diagnostics, DriverError, PoolStats};
use crate::connection::Connection;

/// An idle pooled connection plus its lifecycle stamps, so a checkout can REAP a
/// connection that outlived `max_lifetime` (age since it was established) or
/// `idle_timeout` (since it last returned) BEFORE handing it out — minting a
/// fresh one instead of reusing a stale one. The blocking twin of the async
/// pool's `Idle`.
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
/// existing consumer.
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

/// The default acquire deadline when a caller does not specify one via
/// [`Pool::get_timeout`] or [`Pool::acquire_timeout`]. Finite by construction:
/// the pool never blocks forever.
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// A cloneable handle to a bounded pool of [`Connection`]s.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

/// Rate-limits concurrent new-connection handshakes in the sync pool.
///
/// When a pool is cold or recovering from an outage, establishing many connections
/// simultaneously saturates the server with TLS handshakes and SCRAM PBKDF2 compute.
/// This limiter bounds concurrent dials to `limit`. Waiters block on `condvar`
/// until an in-flight handshake finishes (or their checkout deadline expires).
struct HandshakeLimiter {
    state: Mutex<usize>,
    condvar: Condvar,
    limit: usize,
}

impl HandshakeLimiter {
    fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(0),
            condvar: Condvar::new(),
            limit: limit.max(1),
        }
    }

    fn acquire<'a>(&'a self, deadline: Instant) -> Result<HandshakeGuard<'a>, DriverError> {
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let mut active = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if *active < self.limit {
                *active = active.saturating_add(1);
                return Ok(HandshakeGuard { limiter: self });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(DriverError::PoolTimeout);
            }
            let remaining = deadline.saturating_duration_since(now);
            #[allow(clippy::disallowed_methods, reason = "condvar poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let (recovered, _) = self
                .condvar
                .wait_timeout(active, remaining)
                .unwrap_or_else(|e| e.into_inner());
            active = recovered;
        }
    }
}

struct HandshakeGuard<'a> {
    limiter: &'a HandshakeLimiter,
}

impl Drop for HandshakeGuard<'_> {
    fn drop(&mut self) {
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let mut active = self.limiter.state.lock().unwrap_or_else(|e| e.into_inner());
        *active = active.saturating_sub(1);
        self.limiter.condvar.notify_one();
    }
}

struct PoolInner {
    config: ConnectConfig,
    state: Mutex<PoolState>,
    max_size: usize,
    acquire_timeout: Duration,
    /// The structured-diagnostics configuration installed on every connection the
    /// pool mints (via [`Connection::connect_with`]), and the sink the pool's own
    /// saturation events emit through. `Default` (off) unless the pool was built
    /// through [`Pool::builder`]. Fixed for the pool's life.
    diagnostics: Diagnostics,
    /// Monotonic count of checkouts that waited out their acquire deadline.
    acquire_timeouts: AtomicU64,
    /// Monotonic count of pooled connections evicted on checkout (a failed
    /// health-gate reset).
    connections_evicted: AtomicU64,
    /// High-water mark of the FIFO waiter queue depth (updated under the state
    /// lock as a waiter enqueues).
    waiters_high_water: AtomicU64,
    /// Maximum lifetime of a pooled connection (since it was established), or
    /// `None` (disabled). At checkout, a connection older than this is reaped and
    /// replaced. Fixed for the pool's life.
    max_lifetime: Option<Duration>,
    /// Maximum idle time of a pooled connection (since it last returned), or
    /// `None` (disabled). At checkout, a connection idle longer than this is
    /// reaped and replaced. Fixed for the pool's life.
    idle_timeout: Option<Duration>,
    /// Thundering-herd gate: bounds how many threads can dial new connections
    /// concurrently during pool warmup or post-outage recovery.
    handshake_limiter: HandshakeLimiter,
    /// Condvar notified when all in-flight checkouts are returned during graceful drain.
    drain_condvar: Condvar,
}

struct PoolState {
    connections: VecDeque<Idle>,
    checked_out: usize,
    /// FIFO queue of blocked waiters, each parked on its OWN [`Condvar`]
    /// (identified by `Arc` pointer). The FRONT is the next to serve; a freed
    /// slot wakes exactly it. Empty on the uncontended path.
    waiters: VecDeque<Arc<Condvar>>,
    /// Set when [`Pool::drain`] starts, preventing new checkouts and waking existing waiters.
    draining: bool,
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

/// Reserve a slot for the caller if one is free, incrementing `checked_out`.
///
/// `Some(Some(idle))` — reuse the popped idle connection (check staleness + reset
/// before handing out); `Some(None)` — a fresh slot was reserved (create a new
/// connection); `None` — the pool is at capacity with nothing idle (the caller
/// must wait). The caller must already be entitled to the slot (queue empty, or at
/// the FIFO front) — this does NOT check fairness.
fn try_take(state: &mut PoolState, max_size: usize) -> Option<Option<Idle>> {
    // A connection enters the idle deque ONLY when healthy (`Drop` guards on
    // `is_healthy`; a fatal verb or an explicit `close` evicts at RETURN time,
    // never adding it), and nothing runs on an idle pooled connection to flip its
    // health, so the front is always reusable — pop it directly. Genuine
    // idle-death (the server closed the socket while idle) is `is_healthy()==true`
    // and is caught by the reset failing on acquire (evict + retry), not by a
    // pop-time probe — a pop-time `is_healthy()` filter would be dead code.
    if let Some(idle) = state.connections.pop_front() {
        // `checked_out` counts handed-out slots; it is bounded by `max_size`
        // (a `usize`) so this cannot overflow. `saturating_add` is the
        // forbid-bundle-compliant total form (this fn returns `Option`, not
        // `Result`, so there is no channel to carry an overflow error) and is
        // behavior-identical in the reachable domain.
        state.checked_out = state.checked_out.saturating_add(1);
        return Some(Some(idle));
    }
    if state.connections.len().saturating_add(state.checked_out) < max_size {
        state.checked_out = state.checked_out.saturating_add(1);
        return Some(None);
    }
    None
}

/// Wake the FIFO front waiter, if any, so it re-checks and takes its turn. An
/// O(1) hand-off (one `notify_one` on the front's own condvar), never a
/// thundering `notify_all`.
fn wake_front(state: &PoolState) {
    if let Some(front) = state.waiters.front() {
        front.notify_one();
    }
}

/// Remove `ticket` from the waiter queue (on timeout) — it may be anywhere, not
/// only the front, so it is located by `Arc` pointer identity.
fn remove_ticket(state: &mut PoolState, ticket: &Arc<Condvar>) {
    if let Some(pos) = state.waiters.iter().position(|w| Arc::ptr_eq(w, ticket)) {
        state.waiters.remove(pos);
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
    /// ever discarded to reconfigure it.
    #[must_use]
    pub fn with_acquire_timeout(
        config: ConnectConfig,
        max_size: usize,
        acquire_timeout: Duration,
    ) -> Self {
        // `None`/`None`: no lifetime/idle reaping by default (existing behaviour).
        Self::from_parts(
            config,
            max_size,
            acquire_timeout,
            Diagnostics::default(),
            None,
            None,
            None,
        )
    }

    /// Start building a pool with structured diagnostics (a
    /// [`DiagSink`](bsql_postgres_core::DiagSink) + a slow-query threshold): the
    /// installed configuration rides every connection the pool mints, and the
    /// pool's own saturation events (acquire timeout, connection eviction) emit
    /// through the same sink. The blocking twin of the async `Pool::builder`.
    ///
    /// Diagnostics is NOT a [`ConnectConfig`] field, so the config footprint is
    /// untouched.
    pub fn builder(config: ConnectConfig, max_size: usize) -> PoolBuilder {
        PoolBuilder {
            config,
            max_size,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            diagnostics: Diagnostics::default(),
            max_lifetime: None,
            idle_timeout: None,
            max_concurrent_handshakes: None,
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
        max_concurrent_handshakes: Option<usize>,
    ) -> Self {
        let handshake_limit = match max_concurrent_handshakes {
            Some(m) => m,
            None => std::cmp::min(max_size, 2).max(1),
        };
        Self {
            inner: Arc::new(PoolInner {
                config,
                state: Mutex::new(PoolState {
                    connections: VecDeque::with_capacity(max_size),
                    checked_out: 0,
                    waiters: VecDeque::new(),
                    draining: false,
                }),
                max_size,
                acquire_timeout,
                diagnostics,
                acquire_timeouts: AtomicU64::new(0),
                connections_evicted: AtomicU64::new(0),
                waiters_high_water: AtomicU64::new(0),
                max_lifetime,
                idle_timeout,
                handshake_limiter: HandshakeLimiter::new(handshake_limit),
                drain_condvar: Condvar::new(),
            }),
        }
    }

    /// Check out a connection, waiting up to the pool's configured acquire
    /// deadline. A reused connection is reset before it is handed out; a fresh
    /// one is created clean.
    ///
    /// # Errors
    ///
    /// [`DriverError::PoolTimeout`] if no slot becomes free within the deadline;
    /// a connect / reset error otherwise.
    pub fn get(&self) -> Result<PooledConnection, DriverError> {
        self.get_timeout(self.inner.acquire_timeout)
    }

    /// Like [`get`](Self::get) but with an explicit acquire deadline for this
    /// call, overriding the pool default.
    pub fn get_timeout(&self, timeout: Duration) -> Result<PooledConnection, DriverError> {
        // Absolute deadline; a timeout so large it overflows the clock is a
        // classified error, never an infinite block.
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(DriverError::TimeoutOverflow)?;

        // Outer loop: retry after evicting a connection whose reset failed.
        loop {
            // ── Phase 1: under the lock, reserve a slot FAIRLY (FIFO), or wait
            // until the deadline. Reserving increments `checked_out`; the
            // reservation is released on any Phase-2 failure below. `Some(idle)` =
            // a popped idle connection to REUSE (reap-check + reset before handing
            // out); `None` = the slot is reserved for a FRESH connection.
            let reused: Option<Idle> = {
                // Mutex poison recovery, not a data fallback: a poisoned lock means
                // another thread panicked while holding it; `into_inner` recovers
                // the guarded pool state so the pool keeps operating.
                #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.draining {
                    return Err(DriverError::PoolTimeout);
                }
                // FAST PATH: no one is queued ahead of us AND a slot is free → take
                // it directly, with NO ticket allocation (the uncontended common
                // case). If someone is already waiting, we MUST queue behind them
                // even if a slot is momentarily free — that is the anti-barging
                // guarantee (FIFO).
                if state.waiters.is_empty() {
                    match try_take(&mut state, self.inner.max_size) {
                        Some(taken) => taken,
                        None => self.wait_in_line(state, deadline, timeout)?,
                    }
                } else {
                    self.wait_in_line(state, deadline, timeout)?
                }
            };

            // ── Phase 2: outside the lock (never block other threads on I/O).
            //
            // EXACTLY-ONCE LIVENESS GATE — do NOT fuse this reset into the user's
            // first verb to save its round trip. The pre-verb reset is the proof
            // the connection is still alive BEFORE the user's verb is sent: if it
            // fails, recovery is transparent (evict + reconnect + retry) precisely
            // because the verb has NOT run yet, so a non-idempotent verb (INSERT, …)
            // is never at risk of double execution. Fusing the reset into the first
            // verb would widen the ambiguous-failure window to idle-deaths — a fused
            // failure cannot distinguish "the verb ran" from "it never arrived" (the
            // two-generals problem, irreducible), forcing either a double-execution
            // risk or a user-visible error where recovery is invisible today. The
            // ~10–25µs local RTT is the minimum price of exactly-once, not an
            // optimization gap.
            match reused {
                Some(idle) => {
                    let Idle { mut conn, created, returned } = idle;
                    // LAZY REAPER — before the liveness reset, drop a connection
                    // that outlived `max_lifetime` (age) or `idle_timeout` (idle):
                    // gracefully close it (bounded Terminate), release its slot, and
                    // loop to mint a FRESH one instead of reusing a stale
                    // plan/socket. Reaping-at-checkout is chosen over a background
                    // timer thread (extra lifecycle + a per-pool thread) and over
                    // reap-on-return (which cannot catch a connection that ages past
                    // a bound WHILE idle). Disabled by default (`None`/`None`).
                    //
                    // ZERO-COST WHEN OFF: read the clock (and run the staleness
                    // check) ONLY if a bound is set. Rust's `&&` short-circuits, so
                    // a default pool (both `None`) never evaluates `Instant::now()`
                    // here — no per-checkout timing work.
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
                        conn.close_graceful();
                        // Fairness nuance (intentional, benign): on reaping under
                        // contention the sync pool RELEASES its slot and re-queues
                        // (the fresh connect re-competes at the FIFO back), whereas
                        // the async pool RETAINS its permit and `continue`s. Both
                        // correctly hand the caller a FRESH connection; the
                        // difference is internal machinery only, not observable
                        // behaviour.
                        self.release_slot();
                        drop(conn);
                    } else {
                        match conn.pool_reset_session() {
                            Ok(()) => {
                                return Ok(PooledConnection {
                                    conn: Some(conn),
                                    pool: self.inner.clone(),
                                    // Preserve the ORIGINAL birth time so
                                    // `max_lifetime` measures true age.
                                    created,
                                });
                            }
                            // Reset failed: evict this connection, release its slot,
                            // and retry — never hand out an un-reset connection.
                            Err(_evict) => {
                                // Count + surface the eviction (a steady stream is a
                                // reconnect storm — server-side churn made visible).
                                self.inner.connections_evicted.fetch_add(1, Ordering::Relaxed);
                                self.inner.diagnostics.emit(&DiagEvent::PoolConnectionEvicted);
                                self.release_slot();
                                drop(conn);
                            }
                        }
                    }
                }
                // No reusable idle connection: create a FRESH one (already clean,
                // no reset needed) carrying the pool's diagnostics, into the
                // reserved slot.
                None => {
                    // Thundering-herd gate: rate-limit concurrent connection dials.
                    // Only up to `max_concurrent_handshakes` threads establish connections
                    // simultaneously. Remaining threads wait here.
                    let handshake = match self.inner.handshake_limiter.acquire(deadline) {
                        Ok(guard) => guard,
                        Err(e) => {
                            self.release_slot();
                            return Err(e);
                        }
                    };

                    // Double-check the idle queue! While waiting for the handshake permit,
                    // an earlier concurrent connection may have finished its query and
                    // returned to the idle set. If so, release our reservation and loop to reuse it.
                    let has_idle = {
                        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        !state.connections.is_empty()
                    };
                    if has_idle {
                        drop(handshake);
                        self.release_slot();
                        continue;
                    }

                    match Connection::connect_with(&self.inner.config, &self.inner.diagnostics) {
                        Ok(conn) => {
                            drop(handshake);
                            return Ok(PooledConnection {
                                conn: Some(conn),
                                pool: self.inner.clone(),
                                // Birth time of a freshly-established connection.
                                created: Instant::now(),
                            });
                        }
                        Err(e) => {
                            drop(handshake);
                            self.release_slot();
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Join the FIFO waiter queue and block until we reach the FRONT and a slot
    /// is free, or the deadline elapses.
    ///
    /// Consumes the lock guard (the wait must own it) and returns the reserved
    /// outcome — `Some(idle)` to reuse (reap-check + reset before handing out),
    /// `None` for a fresh slot — or [`DriverError::PoolTimeout`]. Serving increments
    /// `checked_out` and dequeues our ticket; a coalesced multi-return that freed
    /// more than one slot is propagated by waking the NEW front on the way out.
    fn wait_in_line(
        &self,
        mut state: MutexGuard<'_, PoolState>,
        deadline: Instant,
        timeout: Duration,
    ) -> Result<Option<Idle>, DriverError> {
        // Our OWN condvar, enqueued as a ticket. Allocated only on the contended
        // path (a fresh caller that found a slot free never reaches here).
        let ticket = Arc::new(Condvar::new());
        state.waiters.push_back(Arc::clone(&ticket));
        // Update the waiter-depth high-water mark under the lock (the queue length
        // now includes us). A count that overflows a u64 is impossible, so the
        // dead `else` simply skips the update — never a panic or a cast.
        if let Ok(depth) = u64::try_from(state.waiters.len()) {
            self.inner.waiters_high_water.fetch_max(depth, Ordering::Relaxed);
        }
        loop {
            if state.draining {
                remove_ticket(&mut state, &ticket);
                wake_front(&state);
                return Err(DriverError::PoolTimeout);
            }
            // Serve ONLY when we are the front — never barge past an earlier
            // waiter even if a slot is momentarily free.
            if state.waiters.front().is_some_and(|w| Arc::ptr_eq(w, &ticket))
                && let Some(taken) = try_take(&mut state, self.inner.max_size)
            {
                // Took our turn: leave the queue and hand off to the next waiter
                // — a coalesced multi-return may have freed more than the one slot
                // our single wakeup accounted for.
                state.waiters.pop_front();
                wake_front(&state);
                return Ok(taken);
            }
            let now = Instant::now();
            if now >= deadline {
                // Give up our place. If a slot was handed to us AS we timed out
                // (a return raced our deadline), the next waiter must be woken to
                // claim it — never a lost hand-off.
                remove_ticket(&mut state, &ticket);
                wake_front(&state);
                // RELEASE the state lock BEFORE invoking the consumer sink:
                // `std::sync::Mutex` is non-reentrant, so a sink that inspects the
                // pool (`pool.stats()`, the exact pattern this event invites) would
                // DEADLOCK if it ran under the guard. `remove_ticket` + `wake_front`
                // are the last lock users, so dropping here is safe — and it also
                // avoids serializing the whole pool behind a slow (file/network)
                // sink.
                drop(state);
                // Saturation: count + surface it before returning the classified
                // backpressure error.
                self.inner.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
                self.inner
                    .diagnostics
                    .emit(&DiagEvent::PoolAcquireTimeout { waited: timeout });
                return Err(DriverError::PoolTimeout);
            }
            let remaining = deadline.saturating_duration_since(now);
            // Wait on OUR ticket, not a shared condvar: a freed slot wakes exactly
            // the front, so a wakeup we consume is always ours to act on (no
            // burned wakeup, the barging root cause). Condvar poison recovery, same
            // reasoning as the lock in `get_timeout`.
            #[allow(clippy::disallowed_methods, reason = "condvar poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let (recovered, _timed_out) = ticket
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner());
            // Re-check at the loop top (handles spurious wakeups + our
            // not-yet-at-front case); the deadline guard bounds the total wait.
            state = recovered;
        }
    }

    /// Release a reserved slot (decrement `checked_out`) and hand off to the FIFO
    /// front waiter. Used when a reserved connection could not be delivered
    /// (reset / connect failure).
    fn release_slot(&self) {
        // Mutex poison recovery (see `get_timeout`), not a data fallback.
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        // `checked_out` was incremented once for this reservation; a checked
        // decrement fails loud in debug rather than silently wrapping.
        match state.checked_out.checked_sub(1) {
            Some(n) => state.checked_out = n,
            None => debug_assert!(false, "pool checked_out underflow on slot release"),
        }
        if state.checked_out == 0 && state.draining {
            self.inner.drain_condvar.notify_all();
        }
        // Freeing this slot may let the front waiter proceed — wake exactly it.
        wake_front(&state);
    }

    /// The number of idle (checked-in) connections currently held.
    #[must_use]
    pub fn idle_count(&self) -> usize {
        // Mutex poison recovery (see `get_timeout`), not a data fallback.
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.connections.len()
    }

    /// The maximum number of connections the pool will hold.
    #[must_use]
    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }

    /// A snapshot of the pool's operational counters (idle/max plus the monotonic
    /// acquire-timeout, eviction, and waiter high-water gauges) — the pull-style
    /// complement to the push-style [`DiagEvent`] pool events.
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

    /// Gracefully drain the pool: wait for all in-flight checkouts to finish and
    /// return their connections, then gracefully terminate all connections with
    /// [`close_graceful`](Connection::close_graceful).
    ///
    /// Unlike [`close`](Self::close) which only closes connections currently idle,
    /// `drain` acts as a complete barrier: it blocks until all checked out connections
    /// are returned, ensuring zero in-flight operations are abandoned.
    ///
    /// # Consumes the pool (use-after-drain is a COMPILE error)
    ///
    /// `drain` takes `self` by value, making use-after-drain statically impossible:
    ///
    /// ```compile_fail
    /// # fn demo(pool: bsql_postgres_sync::Pool) {
    /// pool.drain();
    /// pool.get().unwrap();   // ERROR[E0382]: `pool` was moved into `drain`
    /// # }
    /// ```
    pub fn drain(self) {
        {
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.draining = true;
            for waiter in &state.waiters {
                waiter.notify_one();
            }
            while state.checked_out > 0 {
                #[allow(clippy::disallowed_methods, reason = "condvar poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let recovered = self
                    .inner
                    .drain_condvar
                    .wait(state)
                    .unwrap_or_else(|e| e.into_inner());
                state = recovered;
            }
        }
        self.close();
    }

    /// GRACEFULLY DRAIN the pool: send a protocol `Terminate` to every
    /// currently-IDLE pooled connection so the server sees a CLEAN disconnect,
    /// then close each socket. The blocking twin of the async `Pool::close`.
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
    /// # fn demo(pool: bsql_postgres_sync::Pool) {
    /// pool.close();
    /// pool.get().unwrap();   // ERROR[E0382]: `pool` was moved into `close`
    /// # }
    /// ```
    ///
    /// # Bounded on a dead peer
    ///
    /// Each `Terminate` rides [`Connection::close_graceful`], whose write is
    /// bounded by the connection's `connect_timeout` (an armed `SO_SNDTIMEO`), so
    /// a black-hole peer cannot hang the drain for the kernel's `tcp_retries2`
    /// budget (~15 min). Best-effort: a per-connection failure is swallowed and
    /// the drain continues.
    ///
    /// # Scope
    ///
    /// Drains only connections IDLE at the moment of the call — see the async
    /// `Pool::close` for the checked-out / surviving-clone semantics.
    pub fn close(self) {
        // Move the idle connections OUT from under the state lock BEFORE any
        // blocking `Terminate` I/O, so the drain never serializes other threads
        // behind the pool state lock (mirrors the reset running outside the lock).
        let idle: VecDeque<Idle> = {
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut state.connections)
        };
        for entry in idle {
            let mut conn = entry.conn;
            conn.close_graceful();
        }
    }
}

/// A builder for a [`Pool`] with an acquire deadline and structured diagnostics.
///
/// Obtained from [`Pool::builder`]; the blocking twin of the async `PoolBuilder`.
/// All settings beyond `config` + `max_size` are optional; [`build`](Self::build)
/// is infallible (connections are lazy).
#[derive(Debug)]
#[must_use = "a PoolBuilder does nothing until `.build()` is called"]
pub struct PoolBuilder {
    config: ConnectConfig,
    max_size: usize,
    acquire_timeout: Duration,
    diagnostics: Diagnostics,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
    max_concurrent_handshakes: Option<usize>,
}

impl PoolBuilder {
    /// Set the default acquire deadline (overridable per checkout via
    /// [`Pool::get_timeout`]). Defaults to 30s.
    pub fn acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// Set the maximum number of concurrent new-connection handshakes allowed.
    ///
    /// When the pool is cold or recovering from an outage, this bounds how many
    /// threads can simultaneously initiate TCP dials, TLS handshakes, and SCRAM
    /// authentications against the server, protecting PostgreSQL from connection
    /// storms / thundering herd. Defaults to `min(max_size, 2)` (at least 1).
    pub fn max_concurrent_handshakes(mut self, max: usize) -> Self {
        self.max_concurrent_handshakes = Some(max);
        self
    }

    /// Set the maximum LIFETIME of a pooled connection (age since it was
    /// established). At checkout, a connection older than this is gracefully
    /// closed (a bounded `Terminate`) and replaced with a fresh one, so a
    /// long-lived pool rotates its connections — bounding server-side per-backend
    /// memory growth and letting a rolling credential / DNS change take effect.
    /// `None` (the default) disables the bound. Reaping is LAZY (at checkout), so
    /// it adds no background thread — see [`Pool::get`] for the rationale.
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
            self.max_concurrent_handshakes,
        )
    }
}

/// A checked-out connection that returns itself to the pool on drop.
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
        // it is reset on the NEXT acquire); an unhealthy one is dropped (evicted).
        if let Some(conn) = self.conn.take() {
            // Mutex poison recovery (see `Pool::get_timeout`), not a data fallback.
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut state = self.pool.state.lock().unwrap_or_else(|e| e.into_inner());
            // `checked_out` is incremented once per checkout and decremented once
            // here per drop, so it is always >= 1 at this point. A checked
            // decrement with a debug assertion fails loud in debug builds rather
            // than silently saturating; Drop cannot return an error, so release
            // builds hold the floor at zero rather than underflow.
            match state.checked_out.checked_sub(1) {
                Some(n) => state.checked_out = n,
                None => debug_assert!(false, "pool checked_out underflow on connection return"),
            }
            if state.checked_out == 0 && state.draining {
                self.pool.drain_condvar.notify_all();
            }
            if conn.is_healthy() {
                // Restamp `returned` for `idle_timeout` — but read the clock ONLY
                // when idle reaping is enabled (ZERO-COST WHEN OFF). When
                // `idle_timeout` is `None`, `returned` is never consumed by the
                // checkout `is_stale` gate, so reuse the clock-free `created` stamp
                // instead of an `Instant::now()`. `created` is always preserved (for
                // `max_lifetime`), so age is measured from birth.
                let returned = if self.pool.idle_timeout.is_some() {
                    Instant::now()
                } else {
                    self.created
                };
                state.connections.push_back(Idle {
                    conn,
                    created: self.created,
                    returned,
                });
            }
            // Hand off to the FIFO front waiter (an added idle connection or a
            // freed slot may let exactly it proceed). Woken while holding the lock:
            // the waiter blocks on re-lock until this guard drops at scope end.
            wake_front(&state);
        }
    }
}

#[cfg(test)]
mod fairness_tests {
    //! FIFO fairness witness: under saturation, blocked checkouts are served in
    //! strict ARRIVAL order (the same guarantee the async pool gets for free from
    //! `tokio::sync::Semaphore`), never barged. A barging `notify_one` + shared
    //! `Condvar` would serve them in an arbitrary scheduler-decided order.
    //!
    //! DETERMINISTIC, not timing-dependent: the queue is ordered by SPINNING on
    //! the private waiter count until each worker has actually parked, so arrival
    //! order does not depend on spawn/scheduler timing; and once the primed
    //! connection is released, the hand-off is a causal chain (worker `i` records
    //! its id and only THEN releases, which alone can wake worker `i+1`), so the
    //! recorded service order is a total order fixed by construction. Needs a live
    //! PG (a real `Connection`), so `#[ignore]`.

    use super::*;
    use std::sync::mpsc;
    use std::thread;

    fn test_config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant").database("postgres".to_string())
    }

    /// The private FIFO waiter count — the barrier the arrival ordering spins on.
    fn waiter_count(pool: &Pool) -> usize {
        #[allow(clippy::disallowed_methods, reason = "test-only mutex poison recovery")]
        let state = pool.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.waiters.len()
    }

    #[test]
    #[ignore = "requires local PG"]
    fn saturated_checkouts_are_served_in_fifo_arrival_order() {
        const N: usize = 8;
        // Pool size 1: one connection, so every worker but the holder must queue.
        let pool = Pool::new(test_config(), 1);
        // Hold the only connection so all N workers block in the FIFO queue.
        let held = pool.get().expect("prime the single connection");

        let (order_tx, order_rx) = mpsc::channel::<usize>();
        let mut handles = Vec::new();
        for id in 0..N {
            let p = pool.clone();
            let tx = order_tx.clone();
            handles.push(thread::spawn(move || {
                // On acquire, record our id (the SERVICE order), then release —
                // dropping the connection alone can wake the next front waiter, so
                // this send strictly precedes the next worker's.
                let conn = p.get().expect("worker acquires");
                tx.send(id).expect("record service order");
                drop(conn);
            }));
            // Enforce ARRIVAL order deterministically: block until THIS worker has
            // actually enqueued its ticket before spawning the next.
            while waiter_count(&pool) < id + 1 {
                thread::yield_now();
            }
        }
        assert_eq!(waiter_count(&pool), N, "all workers queued in arrival order");

        // Release the primed connection: the FIFO hand-off now serves the queue
        // strictly front-to-back.
        drop(held);

        let mut served = Vec::with_capacity(N);
        for _ in 0..N {
            served.push(order_rx.recv().expect("a worker was served"));
        }
        for h in handles {
            h.join().expect("worker thread");
        }
        // FIFO: served in the EXACT arrival order. A barging pool scrambles this.
        assert_eq!(
            served,
            (0..N).collect::<Vec<_>>(),
            "saturated checkouts must be served in FIFO arrival order"
        );
    }
}

#[cfg(test)]
mod reaper_tests {
    //! Offline witness for the lazy reaper's staleness gate (twin of the async
    //! pool's). Documents the invariant the zero-cost-off short-circuit relies
    //! on: with BOTH bounds `None`, `is_stale` is unconditionally `false`, so the
    //! checkout gate can skip the `Instant::now()` read entirely.

    use super::is_stale;
    use std::time::{Duration, Instant};

    #[test]
    fn disabled_bounds_are_never_stale() {
        let birth = Instant::now();
        let much_later = birth + Duration::from_secs(1_000_000);
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

#[cfg(test)]
mod handshake_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn handshake_limiter_bounds_concurrency() {
        let limiter = Arc::new(HandshakeLimiter::new(2));
        let active_count = Arc::new(AtomicUsize::new(0));
        let max_concurrent_seen = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..6 {
            let l = Arc::clone(&limiter);
            let active = Arc::clone(&active_count);
            let max_seen = Arc::clone(&max_concurrent_seen);
            handles.push(thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(2);
                let guard = l.acquire(deadline).expect("acquire permit");
                let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(cur, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(20));
                active.fetch_sub(1, Ordering::SeqCst);
                drop(guard);
            }));
        }

        for h in handles {
            h.join().expect("join thread");
        }

        assert!(
            max_concurrent_seen.load(Ordering::SeqCst) <= 2,
            "concurrent handshakes must never exceed the configured limit of 2"
        );
    }

    #[test]
    fn handshake_limiter_times_out() {
        let limiter = HandshakeLimiter::new(1);
        let deadline = Instant::now() + Duration::from_secs(1);
        let guard = limiter.acquire(deadline).expect("first acquire succeeds");

        let short_deadline = Instant::now() + Duration::from_millis(20);
        let result = limiter.acquire(short_deadline);
        assert!(matches!(result, Err(DriverError::PoolTimeout)));
        drop(guard);

        // After dropping, acquisition succeeds again:
        let result = limiter.acquire(Instant::now() + Duration::from_secs(1));
        assert!(result.is_ok());
    }

    #[test]
    fn empty_pool_drain_completes() {
        let pool = Pool::new(
            ConnectConfig::new("127.0.0.1", "postgres").database("postgres".to_string()),
            5,
        );
        pool.drain();
    }
}
