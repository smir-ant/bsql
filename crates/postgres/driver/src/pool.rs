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
/// ```ignore
/// let pool = Pool::new(config, 10).await?;
/// let mut conn = pool.get().await?;
/// conn.query("SELECT 1").await?;
/// // conn drops → returned to pool
/// ```
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
    /// Create a new pool. Connects `initial_size` connections eagerly.
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

    /// Get a connection from the pool.
    ///
    /// If a free connection is available, returns it immediately.
    /// Otherwise creates a new one (up to max_size). Blocks if
    /// max_size connections are all in use.
    pub async fn get(&self) -> Result<PooledConnection, DriverError> {
        let permit = self.inner.semaphore.acquire().await
            .map_err(|_| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "pool closed",
            )))?;
        permit.forget();

        let mut conns = self.inner.connections.lock().await;
        if let Some(conn) = conns.pop_front() {
            drop(conns);
            return Ok(PooledConnection {
                conn: Some(conn),
                pool: self.inner.clone(),
            });
        }
        drop(conns);

        let conn = Connection::connect(&self.inner.config).await?;
        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
        })
    }

    /// Number of idle connections in the pool.
    pub async fn idle_count(&self) -> usize {
        self.inner.connections.lock().await.len()
    }

    /// Maximum pool size.
    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }
}

/// A connection checked out from a [`Pool`].
///
/// Implements `Deref<Target = Connection>` so all Connection
/// methods are available directly. Returns to the pool on Drop.
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
                let mut conns = pool.connections.lock().await;
                conns.push_back(conn);
                pool.semaphore.add_permits(1);
            });
        }
    }
}
