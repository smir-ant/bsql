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
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
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
    max_size: usize,
    acquire_timeout: Duration,
}

struct PoolState {
    connections: VecDeque<Connection>,
    checked_out: usize,
    /// FIFO queue of blocked waiters, each parked on its OWN [`Condvar`]
    /// (identified by `Arc` pointer). The FRONT is the next to serve; a freed
    /// slot wakes exactly it. Empty on the uncontended path.
    waiters: VecDeque<Arc<Condvar>>,
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
/// `Some(Some(conn))` — reuse the popped idle connection (reset before handing
/// out); `Some(None)` — a fresh slot was reserved (create a new connection);
/// `None` — the pool is at capacity with nothing idle (the caller must wait).
/// The caller must already be entitled to the slot (queue empty, or at the
/// FIFO front) — this does NOT check fairness.
fn try_take(state: &mut PoolState, max_size: usize) -> Option<Option<Connection>> {
    // A connection enters the idle deque ONLY when healthy (`Drop` guards on
    // `is_healthy`; a fatal verb or an explicit `close` evicts at RETURN time,
    // never adding it), and nothing runs on an idle pooled connection to flip its
    // health, so the front is always reusable — pop it directly. Genuine
    // idle-death (the server closed the socket while idle) is `is_healthy()==true`
    // and is caught by the reset failing on acquire (evict + retry), not by a
    // pop-time probe — a pop-time `is_healthy()` filter would be dead code.
    if let Some(conn) = state.connections.pop_front() {
        // `checked_out` counts handed-out slots; it is bounded by `max_size`
        // (a `usize`) so this cannot overflow. `saturating_add` is the
        // forbid-bundle-compliant total form (this fn returns `Option`, not
        // `Result`, so there is no channel to carry an overflow error) and is
        // behavior-identical in the reachable domain.
        state.checked_out = state.checked_out.saturating_add(1);
        return Some(Some(conn));
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
        Self {
            inner: Arc::new(PoolInner {
                config,
                state: Mutex::new(PoolState {
                    connections: VecDeque::with_capacity(max_size),
                    checked_out: 0,
                    waiters: VecDeque::new(),
                }),
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
            // ── Phase 1: under the lock, reserve a slot FAIRLY (FIFO), or wait
            // until the deadline. Reserving increments `checked_out`; the
            // reservation is released on any Phase-2 failure below. `Some(conn)` =
            // a popped idle connection to REUSE (reset before handing out); `None`
            // = the slot is reserved for a FRESH connection (already clean).
            let reused: Option<Connection> = {
                // Mutex poison recovery, not a data fallback: a poisoned lock means
                // another thread panicked while holding it; `into_inner` recovers
                // the guarded pool state so the pool keeps operating.
                #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                // FAST PATH: no one is queued ahead of us AND a slot is free → take
                // it directly, with NO ticket allocation (the uncontended common
                // case). If someone is already waiting, we MUST queue behind them
                // even if a slot is momentarily free — that is the anti-barging
                // guarantee (FIFO).
                if state.waiters.is_empty() {
                    match try_take(&mut state, self.inner.max_size) {
                        Some(taken) => taken,
                        None => self.wait_in_line(state, deadline)?,
                    }
                } else {
                    self.wait_in_line(state, deadline)?
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

    /// Join the FIFO waiter queue and block until we reach the FRONT and a slot
    /// is free, or the deadline elapses.
    ///
    /// Consumes the lock guard (the wait must own it) and returns the reserved
    /// outcome — `Some(conn)` to reuse (reset before handing out), `None` for a
    /// fresh slot — or [`DriverError::PoolTimeout`]. Serving increments
    /// `checked_out` and dequeues our ticket; a coalesced multi-return that freed
    /// more than one slot is propagated by waking the NEW front on the way out.
    fn wait_in_line(
        &self,
        mut state: MutexGuard<'_, PoolState>,
        deadline: Instant,
    ) -> Result<Option<Connection>, DriverError> {
        // Our OWN condvar, enqueued as a ticket. Allocated only on the contended
        // path (a fresh caller that found a slot free never reaches here).
        let ticket = Arc::new(Condvar::new());
        state.waiters.push_back(Arc::clone(&ticket));
        loop {
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
