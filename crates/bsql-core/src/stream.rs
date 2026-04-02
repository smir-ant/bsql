//! True PG-level streaming query results.
//!
//! [`QueryStream`] uses the extended query protocol's `Execute(max_rows=N)`
//! to fetch rows in chunks from PostgreSQL. Only one chunk is in memory at a
//! time — the arena is reset between chunks.
//!
//! The connection is held for the lifetime of the stream. When the stream is
//! dropped (whether fully consumed or not), the connection returns to the pool.
//! If the stream is dropped mid-iteration, the connection is discarded (not
//! returned to the pool) because the portal may still be open on the server.

use std::sync::Arc;

use bsql_driver::arena::release_arena;
use bsql_driver::{Arena, ColumnDesc, QueryResult};

/// Default chunk size for streaming queries.
///
/// 64 rows per Execute call balances network round-trip overhead against
/// memory consumption. Each chunk is parsed into the arena, decoded into
/// owned values, then the arena is recycled.
const STREAM_CHUNK_SIZE: i32 = 64;

/// A stream of rows backed by true PG-level chunked fetching.
///
/// Created by [`Pool::query_stream`](crate::pool::Pool::query_stream).
///
/// The `PoolGuard` is held until the stream is fully consumed or dropped.
/// Rows are fetched in chunks of 64 via `Execute(max_rows=64)`.
pub struct QueryStream {
    /// Held to keep the connection alive while streaming.
    guard: Option<bsql_driver::PoolGuard>,
    arena: Option<Arena>,
    /// Current chunk's row metadata.
    current_result: Option<QueryResult>,
    /// Position within the current chunk.
    position: usize,
    /// Column descriptors (shared across all chunks).
    columns: Arc<[ColumnDesc]>,
    /// Whether all rows have been consumed from the server.
    finished: bool,
    /// Whether we need to send Execute+Sync before reading the next chunk.
    /// True after the first chunk (since query_streaming_start already sent
    /// the first Execute).
    needs_execute: bool,
}

impl QueryStream {
    /// Create a new `QueryStream`.
    ///
    /// `first_result` is the first chunk of rows (from the initial Execute).
    /// `finished` is true if the first chunk was the only chunk (CommandComplete
    /// received).
    pub(crate) fn new(
        guard: bsql_driver::PoolGuard,
        arena: Arena,
        first_result: QueryResult,
        columns: Arc<[ColumnDesc]>,
        finished: bool,
    ) -> Self {
        Self {
            guard: Some(guard),
            arena: Some(arena),
            current_result: Some(first_result),
            position: 0,
            columns,
            finished,
            needs_execute: !finished, // if not finished, next call needs Execute+Sync
        }
    }

    /// Get the next row from the stream.
    ///
    /// Returns `None` when all rows have been consumed.
    ///
    /// Rows borrow from the arena, which is reset between chunks. Each row
    /// must be fully decoded (into owned types) before calling `next_row()`
    /// again. The generated code already does this — it decodes into owned
    /// struct fields.
    pub fn next_row(&mut self) -> Option<bsql_driver::Row<'_>> {
        // Check if current chunk has more rows
        if let Some(ref result) = self.current_result {
            if self.position < result.len() {
                let arena = self.arena.as_ref()?;
                let row = result.row(self.position, arena);
                self.position += 1;
                return Some(row);
            }
        }

        // Current chunk exhausted — cannot fetch more synchronously.
        // The async fetch is done via `fetch_next_chunk()`.
        None
    }

    /// Whether more rows might be available (either in the current chunk or
    /// from the server).
    pub fn has_more(&self) -> bool {
        if let Some(ref result) = self.current_result {
            if self.position < result.len() {
                return true;
            }
        }
        !self.finished
    }

    /// Fetch the next chunk from the server asynchronously.
    ///
    /// Returns `true` if a new chunk was fetched (call `next_row()` to iterate
    /// it). Returns `false` if all rows have been consumed.
    ///
    /// The arena is reset before fetching the new chunk, invalidating any
    /// previous `Row` references. The generated code always decodes rows into
    /// owned fields before calling this.
    pub async fn fetch_next_chunk(&mut self) -> Result<bool, crate::error::BsqlError> {
        if self.finished {
            return Ok(false);
        }

        let guard = self.guard.as_mut().ok_or_else(|| {
            crate::error::BsqlError::from(bsql_driver::DriverError::Pool(
                "stream guard already taken".into(),
            ))
        })?;

        let arena = self.arena.as_mut().ok_or_else(|| {
            crate::error::BsqlError::from(bsql_driver::DriverError::Pool(
                "stream arena already taken".into(),
            ))
        })?;

        // Reset arena for the new chunk
        arena.reset();

        // Send Execute+Sync if needed (2nd+ chunks)
        if self.needs_execute {
            guard
                .streaming_send_execute(STREAM_CHUNK_SIZE)
                .await
                .map_err(crate::error::BsqlError::from)?;
        }

        let num_cols = self.columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> =
            Vec::with_capacity(num_cols * STREAM_CHUNK_SIZE as usize);

        let more = guard
            .streaming_next_chunk(arena, &mut all_col_offsets)
            .await
            .map_err(crate::error::BsqlError::from)?;

        if !more {
            self.finished = true;
        }
        self.needs_execute = more; // if more rows, next call needs Execute+Sync

        if all_col_offsets.is_empty() && !more {
            self.current_result = None;
            self.position = 0;
            return Ok(false);
        }

        self.current_result = Some(QueryResult::from_parts(
            all_col_offsets,
            num_cols,
            self.columns.clone(),
            0,
        ));
        self.position = 0;

        Ok(true)
    }

    /// Number of remaining rows in the current chunk.
    pub fn remaining(&self) -> usize {
        match self.current_result {
            Some(ref result) => result.len().saturating_sub(self.position),
            None => 0,
        }
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            release_arena(arena);
        }
        // If the stream was not fully consumed, the connection is in an
        // indeterminate protocol state (portal open, no ReadyForQuery sent).
        // We cannot send Close+Sync in Drop (requires async I/O), so we
        // mark the guard for discard to prevent it from being returned to
        // the pool. The TCP disconnect causes PG to clean up the portal.
        if !self.finished {
            if let Some(mut guard) = self.guard.take() {
                guard.mark_discard();
                drop(guard);
            }
        }
    }
}
