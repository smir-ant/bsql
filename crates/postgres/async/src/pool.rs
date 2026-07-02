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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use bsql_postgres_core::{ConnectConfig, DriverError};
use crate::connection::Connection;

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
    connections: Mutex<VecDeque<Connection>>,
    /// The capacity gate, as an `Arc` so a checkout can take an
    /// [`OwnedSemaphorePermit`] (`'static`) that rides in the guard and returns
    /// capacity on the guard's — or a cancelled `get` future's — `Drop`.
    semaphore: Arc<Semaphore>,
    max_size: usize,
    acquire_timeout: Duration,
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
    pub async fn new(config: ConnectConfig, max_size: usize) -> Result<Self, DriverError> {
        Self::with_acquire_timeout(config, max_size, DEFAULT_ACQUIRE_TIMEOUT).await
    }

    /// Create a pool with an explicit default acquire deadline, fixed for the
    /// pool's lifetime.
    ///
    /// The deadline is set at construction (not mutated later), so a
    /// clone/checkout never races an accounting change and no connections are
    /// ever discarded to reconfigure it.
    pub async fn with_acquire_timeout(
        config: ConnectConfig,
        max_size: usize,
        acquire_timeout: Duration,
    ) -> Result<Self, DriverError> {
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(VecDeque::with_capacity(max_size)),
                semaphore: Arc::new(Semaphore::new(max_size)),
                max_size,
                acquire_timeout,
            }),
        })
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
        let permit = match tokio::time::timeout(
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
            Err(_elapsed) => return Err(DriverError::PoolTimeout),
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
                // Skip connections that died while idle (evict, do not reset).
                loop {
                    match conns.pop_front() {
                        Some(conn) if conn.is_healthy() => break Some(conn),
                        Some(_dead) => continue,
                        None => break None,
                    }
                }
            };

            match reused {
                Some(mut conn) => {
                    // Reset a REUSED connection before handing it out. A reset
                    // failure evicts it (drop) and tries the next idle one — never
                    // hand out an un-reset (dirty) connection. The owned permit is
                    // retained across the retry.
                    match conn.reset_session().await {
                        Ok(()) => {
                            return Ok(PooledConnection {
                                conn: Some(conn),
                                pool: Arc::clone(&self.inner),
                                permit,
                            });
                        }
                        Err(_evict) => {
                            drop(conn);
                            continue;
                        }
                    }
                }
                // No reusable idle connection: create a FRESH one (already clean,
                // no reset needed). It rides the permit already held; a connect
                // failure drops the permit (returning capacity) on the way out.
                None => match Connection::connect(&self.inner.config).await {
                    Ok(conn) => {
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: Arc::clone(&self.inner),
                            permit,
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
            conns.push_back(conn);
        }
    }
}
