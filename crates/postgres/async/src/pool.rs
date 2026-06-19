use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

use bsql_postgres_core::{ConnectConfig, DriverError};
use crate::connection::Connection;

#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: ConnectConfig,
    connections: Mutex<VecDeque<Connection>>,
    semaphore: Semaphore,
    max_size: usize,
}

impl Pool {
    pub async fn new(config: ConnectConfig, max_size: usize) -> Result<Self, DriverError> {
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(VecDeque::with_capacity(max_size)),
                semaphore: Semaphore::new(max_size),
                max_size,
            }),
        })
    }

    pub async fn get(&self) -> Result<PooledConnection, DriverError> {
        let permit = self.inner.semaphore.acquire().await
            .map_err(|_| DriverError::Io(std::io::Error::other("pool closed")))?;
        permit.forget();

        {
            // Mutex poison recovery, not a data fallback: a poisoned lock means
            // another thread panicked while holding it; `into_inner` recovers the
            // guard so the pool keeps operating rather than propagating the panic.
            // The connection set it guards is observed and acted on as normal.
            #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
            let mut conns = self.inner.connections.lock()
                .unwrap_or_else(|e| e.into_inner());
            while let Some(conn) = conns.pop_front() {
                if conn.is_healthy() {
                    return Ok(PooledConnection {
                        conn: Some(conn),
                        pool: self.inner.clone(),
                    });
                }
            }
        }

        let conn = match Connection::connect(&self.inner.config).await {
            Ok(c) => c,
            Err(e) => {
                self.inner.semaphore.add_permits(1);
                return Err(e);
            }
        };
        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
        })
    }

    pub fn idle_count(&self) -> usize {
        // Mutex poison recovery (see `get`), not a data fallback.
        #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
        let conns = self.inner.connections.lock()
            .unwrap_or_else(|e| e.into_inner());
        conns.len()
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
        // Invariant: conn is Some until Drop. After Drop, Rust prevents
        // calling deref (the value is consumed). The None branch is
        // structurally unreachable — tier-2 by Rust ownership rules.
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
            if conn.is_healthy() {
                // Recover from a poisoned lock the same way the rest of the pool
                // does, so a healthy connection is always returned (never
                // silently discarded on poison while the permit is added back,
                // which would diverge capacity from the connection set).
                #[allow(clippy::disallowed_methods, reason = "mutex poison recovery — reclaims the guard after another thread panicked; not a silent data fallback")]
                let mut conns = self.pool.connections.lock()
                    .unwrap_or_else(|e| e.into_inner());
                conns.push_back(conn);
            }
            self.pool.semaphore.add_permits(1);
        }
    }
}
