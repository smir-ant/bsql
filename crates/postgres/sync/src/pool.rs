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

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use bsql_postgres_core::{ConnectConfig, DriverError};
use crate::connection::Connection;

/// The default acquire deadline when a caller does not specify one via
/// [`Pool::get_timeout`] or [`Pool::acquire_timeout`]. Finite by construction:
/// the pool never blocks forever.
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// A cloneable handle to a bounded pool of [`Connection`]s.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: ConnectConfig,
    state: Mutex<PoolState>,
    available: Condvar,
    max_size: usize,
    acquire_timeout: Duration,
}

struct PoolState {
    connections: VecDeque<Connection>,
    checked_out: usize,
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

/// Pop the first HEALTHY idle connection, evicting (dropping) any that died
/// while idle. Returns `None` when the idle set holds no healthy connection.
fn pop_healthy(state: &mut PoolState) -> Option<Connection> {
    while let Some(conn) = state.connections.pop_front() {
        if conn.is_healthy() {
            return Some(conn);
        }
        // else: dead — drop it (evict) and keep looking.
    }
    None
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
        Self {
            inner: Arc::new(PoolInner {
                config,
                state: Mutex::new(PoolState {
                    connections: VecDeque::with_capacity(max_size),
                    checked_out: 0,
                }),
                available: Condvar::new(),
                max_size,
                acquire_timeout,
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
            // ── Phase 1: under the lock, reserve a slot, or wait until the
            // deadline. Reserving increments `checked_out`; the reservation is
            // released on any Phase-2 failure below. `Some(conn)` = a popped idle
            // connection to REUSE (reset before handing out); `None` = the slot is
            // reserved for a FRESH connection (already clean).
            let reused: Option<Connection> = {
                // Mutex poison recovery, not a data fallback: a poisoned lock means
                // another thread panicked while holding it; `into_inner` recovers
                // the guarded pool state so the pool keeps operating.
                #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    if let Some(conn) = pop_healthy(&mut state) {
                        state.checked_out += 1;
                        break Some(conn);
                    }
                    if state.connections.len() + state.checked_out < self.inner.max_size {
                        state.checked_out += 1;
                        break None;
                    }
                    // At capacity, nothing idle: wait until the deadline.
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(DriverError::PoolTimeout);
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    // Condvar poison recovery, same reasoning as the lock above.
                    #[allow(clippy::disallowed_methods, reason = "condvar poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                    let (recovered, _timed_out) = self
                        .inner
                        .available
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|e| e.into_inner());
                    // Re-check the condition at the top of the loop (handles
                    // spurious wakeups); the `now >= deadline` guard above bounds
                    // the total wait.
                    state = recovered;
                }
            };

            // ── Phase 2: outside the lock (never block other threads on I/O).
            match reused {
                Some(mut conn) => match conn.reset_session() {
                    Ok(()) => {
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: self.inner.clone(),
                        });
                    }
                    // Reset failed: evict this connection, release its slot, and
                    // retry — never hand out an un-reset (dirty) connection.
                    Err(_evict) => {
                        self.release_slot();
                        drop(conn);
                    }
                },
                // No reusable idle connection: create a FRESH one (already clean,
                // no reset needed) into the reserved slot.
                None => match Connection::connect(&self.inner.config) {
                    Ok(conn) => {
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: self.inner.clone(),
                        });
                    }
                    Err(e) => {
                        self.release_slot();
                        return Err(e);
                    }
                },
            }
        }
    }

    /// Release a reserved slot (decrement `checked_out`) and wake one waiter.
    /// Used when a reserved connection could not be delivered (reset / connect
    /// failure).
    fn release_slot(&self) {
        {
            // Mutex poison recovery (see `get_timeout`), not a data fallback.
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            // `checked_out` was incremented once for this reservation; a checked
            // decrement fails loud in debug rather than silently wrapping.
            match state.checked_out.checked_sub(1) {
                Some(n) => state.checked_out = n,
                None => debug_assert!(false, "pool checked_out underflow on slot release"),
            }
        }
        self.inner.available.notify_one();
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
            if conn.is_healthy() {
                state.connections.push_back(conn);
            }
            drop(state);
            self.pool.available.notify_one();
        }
    }
}
