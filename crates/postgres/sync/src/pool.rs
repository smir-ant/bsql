use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex};

use bsql_postgres_core::{ConnectConfig, DriverError};
use crate::connection::Connection;

#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: ConnectConfig,
    state: Mutex<PoolState>,
    available: Condvar,
    max_size: usize,
}

struct PoolState {
    connections: VecDeque<Connection>,
    checked_out: usize,
}

impl Pool {
    pub fn new(config: ConnectConfig, max_size: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                state: Mutex::new(PoolState {
                    connections: VecDeque::with_capacity(max_size),
                    checked_out: 0,
                }),
                available: Condvar::new(),
                max_size,
            }),
        }
    }

    pub fn get(&self) -> Result<PooledConnection, DriverError> {
        let should_create = {
            // Mutex poison recovery, not a data fallback: a poisoned lock means
            // another thread panicked while holding it; `into_inner` recovers
            // the guarded pool state so the pool keeps operating.
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut state = self.inner.state.lock()
                .unwrap_or_else(|e| e.into_inner());
            loop {
                while let Some(conn) = state.connections.pop_front() {
                    if conn.is_healthy() {
                        state.checked_out += 1;
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: self.inner.clone(),
                        });
                    }
                }

                if state.connections.len() + state.checked_out < self.inner.max_size {
                    state.checked_out += 1;
                    break true;
                }

                // Condvar poison recovery, not a data fallback: same reasoning
                // as the lock above — recover the guarded state and keep waiting.
                #[allow(clippy::disallowed_methods, reason = "condvar poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let recovered = self.inner.available.wait(state)
                    .unwrap_or_else(|e| e.into_inner());
                state = recovered;
            }
        };

        if should_create {
            match Connection::connect(&self.inner.config) {
                Ok(conn) => Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self.inner.clone(),
                }),
                Err(e) => {
                    // Mutex poison recovery (see `get` entry), not a data fallback.
                    #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                    let mut state = self.inner.state.lock()
                        .unwrap_or_else(|e| e.into_inner());
                    state.checked_out -= 1;
                    self.inner.available.notify_one();
                    Err(e)
                }
            }
        } else {
            unreachable!()
        }
    }

    pub fn idle_count(&self) -> usize {
        // Mutex poison recovery (see `get`), not a data fallback.
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let state = self.inner.state.lock()
            .unwrap_or_else(|e| e.into_inner());
        state.connections.len()
    }

    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }
}

pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<PoolInner>,
}

impl Deref for PooledConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        match self.conn.as_ref() {
            Some(c) => c,
            None => unreachable!("PooledConnection used after drop"),
        }
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        match self.conn.as_mut() {
            Some(c) => c,
            None => unreachable!("PooledConnection used after drop"),
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Mutex poison recovery (see `get`), not a data fallback.
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut state = self.pool.state.lock()
                .unwrap_or_else(|e| e.into_inner());
            // `checked_out` is incremented once per checkout and decremented
            // once here per drop, so it is always >= 1 at this point. A checked
            // decrement with a debug assertion makes any accounting bug fail
            // loud in debug builds instead of being masked by a silent
            // saturate-to-zero; Drop cannot return an error, so release builds
            // hold the floor at zero rather than underflow.
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
