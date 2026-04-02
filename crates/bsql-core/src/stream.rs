//! Streaming query results.
//!
//! [`QueryStream`] wraps a `QueryResult` and its `Arena` alongside the
//! `PoolGuard` that produced them, keeping the connection alive for the
//! lifetime of the stream. When the stream is dropped, the connection returns
//! to the pool and the arena is recycled.
//!
//! NOTE: With bsql-driver, all rows are loaded into the arena before
//! iteration begins. This is not true streaming (all rows in memory), but
//! it maintains API compatibility with the old `futures_core::Stream` interface.

use bsql_driver::arena::release_arena;
use bsql_driver::{Arena, QueryResult};

/// A stream of rows that keeps its connection and arena alive.
///
/// Created by [`Pool::query_stream`](crate::pool::Pool::query_stream).
///
/// The `PoolGuard` is held until the stream is fully consumed or dropped,
/// at which point it returns to the pool.
pub struct QueryStream {
    /// Held to keep the connection alive while streaming. Drops after `result`/`arena`.
    _guard: bsql_driver::PoolGuard,
    arena: Option<Arena>,
    result: QueryResult,
    position: usize,
}

impl QueryStream {
    /// Create a new `QueryStream` from a pool guard, arena, and query result.
    pub(crate) fn new(guard: bsql_driver::PoolGuard, arena: Arena, result: QueryResult) -> Self {
        Self {
            _guard: guard,
            arena: Some(arena),
            result,
            position: 0,
        }
    }

    /// Get the next row from the stream.
    ///
    /// Returns `None` when all rows have been consumed.
    pub fn next_row(&mut self) -> Option<bsql_driver::Row<'_>> {
        let arena = self.arena.as_ref()?;
        if self.position >= self.result.len() {
            return None;
        }
        let row = self.result.row(self.position, arena);
        self.position += 1;
        Some(row)
    }

    /// Number of remaining rows.
    pub fn remaining(&self) -> usize {
        self.result.len().saturating_sub(self.position)
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            release_arena(arena);
        }
    }
}
