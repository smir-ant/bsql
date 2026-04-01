//! Streaming query results.
//!
//! [`QueryStream`] wraps a `RowStream` from tokio-postgres alongside the
//! `PoolConnection` that produced it, keeping the connection alive for the
//! lifetime of the stream. When the stream is dropped, the connection returns
//! to the pool.
//!
//! Users consume the stream via `futures_core::Stream` (or `tokio_stream`).
//! The generated `fetch_stream` method wraps this with typed row decoding.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio_postgres::RowStream;

use crate::error::{BsqlError, BsqlResult};
use crate::pool::PoolConnection;

/// A stream of `tokio_postgres::Row` values that keeps its connection alive.
///
/// Created by [`Pool::query_stream`](crate::pool::Pool::query_stream).
/// Implements `Stream<Item = BsqlResult<tokio_postgres::Row>>`.
///
/// The `PoolConnection` is held until the stream is fully consumed or dropped,
/// at which point it returns to the pool.
pub struct QueryStream {
    /// Held to keep the connection alive while streaming. Drops after `stream`.
    _conn: PoolConnection,
    /// The underlying row stream. Boxed because `RowStream` is `!Unpin`.
    stream: Pin<Box<RowStream>>,
}

impl QueryStream {
    /// Create a new `QueryStream` from a pool connection and a raw row stream.
    ///
    /// The connection must be the same one that produced the `RowStream`.
    pub(crate) fn new(conn: PoolConnection, stream: RowStream) -> Self {
        Self {
            _conn: conn,
            stream: Box::pin(stream),
        }
    }
}

impl Stream for QueryStream {
    type Item = BsqlResult<tokio_postgres::Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|r| r.map_err(BsqlError::from)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // RowStream does not know total count upfront
        (0, None)
    }
}
