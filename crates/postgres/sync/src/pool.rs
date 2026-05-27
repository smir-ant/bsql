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
    connections: Mutex<VecDeque<Connection>>,
    available: Condvar,
    max_size: usize,
    checked_out: Mutex<usize>,
}

impl Pool {
    pub fn new(config: ConnectConfig, max_size: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(VecDeque::with_capacity(max_size)),
                available: Condvar::new(),
                max_size,
                checked_out: Mutex::new(0),
            }),
        }
    }

    pub fn get(&self) -> Result<PooledConnection, DriverError> {
        loop {
            {
                let mut conns = self.inner.connections.lock()
                    .unwrap_or_else(|e| e.into_inner());
                while let Some(conn) = conns.pop_front() {
                    if conn.is_healthy() {
                        let mut out = self.inner.checked_out.lock()
                            .unwrap_or_else(|e| e.into_inner());
                        *out += 1;
                        return Ok(PooledConnection {
                            conn: Some(conn),
                            pool: self.inner.clone(),
                        });
                    }
                }
            }

            let out = self.inner.checked_out.lock()
                .unwrap_or_else(|e| e.into_inner());
            let total_conns = {
                let conns = self.inner.connections.lock()
                    .unwrap_or_else(|e| e.into_inner());
                conns.len() + *out
            };
            drop(out);

            if total_conns < self.inner.max_size {
                let conn = Connection::connect(&self.inner.config)?;
                let mut out = self.inner.checked_out.lock()
                    .unwrap_or_else(|e| e.into_inner());
                *out += 1;
                return Ok(PooledConnection {
                    conn: Some(conn),
                    pool: self.inner.clone(),
                });
            }

            let conns = self.inner.connections.lock()
                .unwrap_or_else(|e| e.into_inner());
            let _conns = self.inner.available.wait(conns)
                .unwrap_or_else(|e| e.into_inner());
        }
    }

    pub fn idle_count(&self) -> usize {
        self.inner.connections.lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
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
            let mut out = self.pool.checked_out.lock()
                .unwrap_or_else(|e| e.into_inner());
            *out = out.saturating_sub(1);

            if conn.is_healthy()
                && let Ok(mut conns) = self.pool.connections.lock() {
                    conns.push_back(conn);
                }
            self.pool.available.notify_one();
        }
    }
}
