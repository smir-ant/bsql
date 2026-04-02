//! Connection pool — thin wrapper over `bsql_driver::Pool`.
//!
//! Delegates all connection management, fail-fast semantics, and LIFO ordering
//! to the driver. This layer adds only the bsql error type conversions.

use bsql_driver::arena::acquire_arena;
use bsql_driver::codec::Encode;
use tokio::sync::Mutex;

use crate::error::{BsqlError, BsqlResult};
use crate::stream::QueryStream;
use crate::transaction::Transaction;

/// A PostgreSQL connection pool.
///
/// Wraps `bsql_driver::Pool` with bsql error types and the `Executor` trait.
pub struct Pool {
    pub(crate) inner: bsql_driver::Pool,
}

/// Builder for configuring a connection pool.
pub struct PoolBuilder {
    url: Option<String>,
    max_size: usize,
}

impl PoolBuilder {
    /// Configure the pool from a PostgreSQL connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub fn url(mut self, url: &str) -> Self {
        self.url = Some(url.into());
        self
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    pub async fn build(self) -> BsqlResult<Pool> {
        let url = self.url.ok_or_else(|| {
            BsqlError::from(bsql_driver::DriverError::Pool(
                "pool builder requires a URL".into(),
            ))
        })?;
        let inner = bsql_driver::Pool::builder()
            .url(&url)
            .max_size(self.max_size)
            .build()
            .await
            .map_err(BsqlError::from)?;
        Ok(Pool { inner })
    }
}

impl Pool {
    /// Connect to PostgreSQL using a connection URL.
    ///
    /// Format: `postgres://user:password@host:port/dbname`
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let inner = bsql_driver::Pool::connect(url)
            .await
            .map_err(BsqlError::from)?;
        Ok(Pool { inner })
    }

    /// Create a pool builder for fine-grained configuration.
    pub fn builder() -> PoolBuilder {
        PoolBuilder {
            url: None,
            max_size: 10,
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// **Fail-fast**: returns `BsqlError::Pool` immediately if no connections
    /// are available. Does not wait.
    pub async fn acquire(&self) -> BsqlResult<PoolConnection> {
        let guard = self.inner.acquire().await.map_err(BsqlError::from)?;
        Ok(PoolConnection {
            inner: Mutex::new(guard),
        })
    }

    /// Begin a new transaction.
    ///
    /// Acquires a connection and sends BEGIN immediately.
    pub async fn begin(&self) -> BsqlResult<Transaction> {
        let tx = self.inner.begin().await.map_err(BsqlError::from)?;
        Ok(Transaction::from_driver(tx))
    }

    /// Execute a query and return a stream of rows.
    ///
    /// Acquires a connection from the pool and returns a [`QueryStream`]
    /// that holds the connection alive until the stream is consumed or dropped.
    ///
    /// Uses true PG-level streaming via `Execute(max_rows=64)`. Only 64 rows
    /// are in memory at a time. The stream fetches additional chunks on demand
    /// via the `PortalSuspended` / re-`Execute` protocol.
    pub async fn query_stream(
        &self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> BsqlResult<QueryStream> {
        let mut guard = self.inner.acquire().await.map_err(BsqlError::from)?;
        let mut arena = acquire_arena();

        // chunk_size=64 rows per Execute call
        const CHUNK_SIZE: i32 = 64;

        let (columns, _) = guard
            .query_streaming_start(sql, sql_hash, params, CHUNK_SIZE)
            .await
            .map_err(BsqlError::from)?;

        let num_cols = columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> =
            Vec::with_capacity(num_cols * CHUNK_SIZE as usize);

        let more = guard
            .streaming_next_chunk(&mut arena, &mut all_col_offsets)
            .await
            .map_err(BsqlError::from)?;

        let first_result =
            bsql_driver::QueryResult::from_parts(all_col_offsets, num_cols, columns.clone(), 0);

        Ok(QueryStream::new(guard, arena, first_result, columns, !more))
    }

    /// Set the SQL statements to pre-PREPARE on new connections.
    ///
    /// Each SQL string is PREPAREd on new connections before they are returned
    /// from `acquire()`. This eliminates first-use Parse overhead for hot queries.
    ///
    /// Warmup errors are silently ignored — a bad warmup SQL does not prevent
    /// the connection from being usable.
    pub fn set_warmup_sqls(&self, sqls: &[&str]) {
        self.inner.set_warmup_sqls(sqls);
    }

    /// Current pool status: open connections and max size.
    pub fn status(&self) -> PoolStatus {
        PoolStatus {
            available: 0, // driver pool doesn't expose idle count
            size: self.inner.open_count(),
            max_size: self.inner.max_size(),
        }
    }
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("status", &self.status())
            .finish()
    }
}

/// A connection borrowed from the pool.
///
/// Uses `tokio::sync::Mutex` for interior mutability because the driver's
/// `Connection` requires `&mut self` for queries, but the `Executor` trait
/// takes `&self`. The mutex is uncontended in practice (a single connection
/// is never shared between concurrent tasks).
///
/// Returned to the pool when dropped.
pub struct PoolConnection {
    pub(crate) inner: Mutex<bsql_driver::PoolGuard>,
}

/// Snapshot of pool utilization.
#[derive(Debug, Clone, Copy)]
pub struct PoolStatus {
    pub available: usize,
    pub size: usize,
    pub max_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = Pool::builder();
        assert_eq!(b.max_size, 10);
    }
}
