use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use crate::config::ConnectConfig;
use crate::connection::Connection;
use crate::error::DriverError;

/// Connection pool for PostgreSQL.
///
/// Manages a set of reusable connections. `pool.get()` returns a
/// `PooledConnection` that auto-returns to the pool on Drop.
///
/// Unhealthy connections (Errored state) are discarded on return
/// rather than recycled — the next `get()` creates a fresh one.
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
        let pool = Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(VecDeque::with_capacity(max_size)),
                semaphore: Semaphore::new(max_size),
                max_size,
            }),
        };
        Ok(pool)
    }

    pub async fn get(&self) -> Result<PooledConnection, DriverError> {
        let permit = self.inner.semaphore.acquire().await
            .map_err(|_| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "pool closed",
            )))?;
        permit.forget();

        {
            let mut conns = self.inner.connections.lock().await;
            while let Some(conn) = conns.pop_front() {
                if conn.is_healthy() {
                    return Ok(PooledConnection {
                        conn: Some(conn),
                        pool: self.inner.clone(),
                    });
                }
            }
        }

        let conn = Connection::connect(&self.inner.config).await?;
        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
        })
    }

    pub async fn idle_count(&self) -> usize {
        self.inner.connections.lock().await.len()
    }

    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }
}

/// A connection checked out from a [`Pool`].
///
/// Implements `Deref<Target = Connection>` so all Connection
/// methods are available directly. Returns to the pool on Drop
/// if the connection is healthy; discards it otherwise.
pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<PoolInner>,
}

impl Deref for PooledConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("connection taken")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("connection taken")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                if conn.is_healthy() {
                    pool.connections.lock().await.push_back(conn);
                }
                pool.semaphore.add_permits(1);
            });
        }
    }
}
