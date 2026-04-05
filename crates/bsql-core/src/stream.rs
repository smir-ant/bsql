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

use bsql_driver_postgres::arena::release_arena;
use bsql_driver_postgres::{Arena, ColumnDesc, QueryResult};

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
///
/// # Usage
///
/// Use [`advance()`](QueryStream::advance) + [`next_row()`](QueryStream::next_row)
/// for row-by-row iteration:
///
/// ```rust,ignore
/// let mut stream = pool.query_stream(sql, hash, &[])?;
/// while stream.advance()? {
///     let row = stream.next_row().unwrap();
///     let id: i32 = row.get_i32(0).unwrap();
///     // decode before next advance() — row borrows from arena
/// }
/// ```
pub struct QueryStream {
    /// Held to keep the connection alive while streaming.
    guard: Option<bsql_driver_postgres::PoolGuard>,
    arena: Option<Arena>,
    /// Current chunk's row metadata.
    current_result: Option<QueryResult>,
    /// Position within the current chunk.
    position: usize,
    /// Column descriptors (shared across all chunks via Arc).
    /// Passed by reference to `QueryResult::from_parts` to avoid Arc
    /// refcount increments per chunk.
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
        guard: bsql_driver_postgres::PoolGuard,
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

    /// Get the next row from the current in-memory chunk.
    ///
    /// Returns `None` when the current chunk is exhausted. Call
    /// [`fetch_next_chunk()`](QueryStream::fetch_next_chunk) to load more rows
    /// from the server, or use [`advance()`](QueryStream::advance) which
    /// handles chunk management automatically.
    ///
    /// Rows borrow from the arena, which is reset between chunks. Each row
    /// must be fully decoded (into owned types) before calling `next_row()`
    /// again. The generated code already does this — it decodes into owned
    /// struct fields.
    pub fn next_row(&mut self) -> Option<bsql_driver_postgres::Row<'_>> {
        // Check if current chunk has more rows
        if let Some(ref result) = self.current_result {
            if self.position < result.len() {
                let arena = self.arena.as_ref()?;
                let row = result.row(self.position, arena);
                self.position += 1;
                return Some(row);
            }
        }

        // Current chunk exhausted — cannot fetch more synchronously here.
        // Use `fetch_next_chunk()` to load the next chunk.
        None
    }

    /// Ensure the current chunk has rows available for `next_row()`.
    ///
    /// If the current chunk is exhausted but more rows exist on the server,
    /// fetches the next chunk. Returns `true` if rows are available (call
    /// `next_row()` next), `false` if all rows have been consumed.
    ///
    /// This is the complement to `next_row()`. Together they form
    /// the primary iteration pattern:
    ///
    /// ```rust,ignore
    /// while stream.advance()? {
    ///     let row = stream.next_row().unwrap();
    ///     let id: i32 = row.get_i32(0).unwrap();
    ///     // decode before next advance() — row borrows from arena
    /// }
    /// ```
    pub fn advance(&mut self) -> Result<bool, crate::error::BsqlError> {
        // Fast path: current chunk still has rows
        if let Some(ref result) = self.current_result {
            if self.position < result.len() {
                return Ok(true);
            }
        }

        // Current chunk exhausted
        if self.finished {
            return Ok(false);
        }

        // Fetch the next chunk
        self.fetch_next_chunk()?;

        // Check if the new chunk has rows
        if let Some(ref result) = self.current_result {
            if self.position < result.len() {
                return Ok(true);
            }
        }

        Ok(false)
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

    /// Fetch the next chunk from the server.
    ///
    /// Returns `true` if a new chunk was fetched (call `next_row()` to iterate
    /// it). Returns `false` if all rows have been consumed.
    ///
    /// The arena is reset before fetching the new chunk, invalidating any
    /// previous `Row` references. The generated code always decodes rows into
    /// owned fields before calling this.
    pub fn fetch_next_chunk(&mut self) -> Result<bool, crate::error::BsqlError> {
        if self.finished {
            return Ok(false);
        }

        let guard = self.guard.as_mut().ok_or_else(|| {
            crate::error::BsqlError::from(bsql_driver_postgres::DriverError::Pool(
                "stream guard already taken".into(),
            ))
        })?;

        let arena = self.arena.as_mut().ok_or_else(|| {
            crate::error::BsqlError::from(bsql_driver_postgres::DriverError::Pool(
                "stream arena already taken".into(),
            ))
        })?;

        // Reset arena for the new chunk
        arena.reset();

        // Send Execute+Sync if needed (2nd+ chunks)
        if self.needs_execute {
            guard
                .streaming_send_execute(STREAM_CHUNK_SIZE)
                .map_err(crate::error::BsqlError::from_driver_query)?;
        }

        let num_cols = self.columns.len();

        // Reclaim the Vec from the previous chunk result to reuse its allocation.
        let mut col_offsets = match self.current_result.as_mut() {
            Some(result) => {
                let mut v = result.take_col_offsets();
                v.clear();
                v
            }
            None => Vec::with_capacity(num_cols * STREAM_CHUNK_SIZE as usize),
        };

        let more = guard
            .streaming_next_chunk(arena, &mut col_offsets)
            .map_err(crate::error::BsqlError::from_driver_query)?;

        if !more {
            self.finished = true;
        }
        self.needs_execute = more; // if more rows, next call needs Execute+Sync

        if col_offsets.is_empty() && !more {
            self.current_result = None;
            self.position = 0;
            return Ok(false);
        }

        // Pass Arc::clone of columns. The Arc is shared across all chunks —
        // this is a single refcount increment per chunk, not per row.
        self.current_result = Some(QueryResult::from_parts(
            col_offsets,
            num_cols,
            Arc::clone(&self.columns),
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

    /// Column descriptors for the result set.
    pub fn columns(&self) -> &[ColumnDesc] {
        &self.columns
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        if let Some(arena) = self.arena.take() {
            release_arena(arena);
        }
        // If the stream was not fully consumed, the connection is in an
        // indeterminate protocol state (portal open, no ReadyForQuery sent).
        // We cannot send Close+Sync in Drop (requires I/O), so we
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

#[cfg(test)]
mod tests {
    use super::*;
    use bsql_driver_postgres::arena::acquire_arena;
    use bsql_driver_postgres::{ColumnDesc, QueryResult};

    /// Build a QueryResult with `num_rows` rows and the given columns.
    /// Each cell is NULL (offset=0, len=-1) which is fine for structural tests.
    fn make_result(num_rows: usize, columns: &Arc<[ColumnDesc]>) -> QueryResult {
        let num_cols = columns.len();
        let col_offsets = vec![(0usize, -1i32); num_rows * num_cols];
        QueryResult::from_parts(col_offsets, num_cols, Arc::clone(columns), 0)
    }

    fn sample_columns(n: usize) -> Arc<[ColumnDesc]> {
        (0..n)
            .map(|i| ColumnDesc {
                name: format!("col{i}").into(),
                type_oid: 23,
                type_size: 4,
                table_oid: 0,
                column_id: 0,
            })
            .collect::<Vec<_>>()
            .into()
    }

    /// Build a QueryStream without a real PoolGuard.
    /// guard=None means fetch_next_chunk will fail, but structural methods work.
    fn make_stream(num_rows: usize, num_cols: usize, finished: bool) -> QueryStream {
        let columns = sample_columns(num_cols);
        let result = make_result(num_rows, &columns);
        let arena = acquire_arena();
        QueryStream {
            guard: None,
            arena: Some(arena),
            current_result: Some(result),
            position: 0,
            columns,
            finished,
            needs_execute: !finished,
        }
    }

    // --- next_row returns rows from buffer ---

    #[test]
    fn next_row_returns_rows() {
        let mut stream = make_stream(3, 2, true);
        assert!(stream.next_row().is_some());
        assert!(stream.next_row().is_some());
        assert!(stream.next_row().is_some());
    }

    #[test]
    fn next_row_returns_none_when_exhausted() {
        let mut stream = make_stream(2, 1, true);
        assert!(stream.next_row().is_some());
        assert!(stream.next_row().is_some());
        assert!(stream.next_row().is_none());
    }

    #[test]
    fn next_row_returns_none_for_empty_result() {
        let mut stream = make_stream(0, 1, true);
        assert!(stream.next_row().is_none());
    }

    // --- has_more ---

    #[test]
    fn has_more_true_when_rows_in_buffer() {
        let stream = make_stream(2, 1, true);
        assert!(stream.has_more());
    }

    #[test]
    fn has_more_false_when_exhausted_and_finished() {
        let mut stream = make_stream(1, 1, true);
        let _ = stream.next_row();
        assert!(!stream.has_more());
    }

    #[test]
    fn has_more_true_when_exhausted_but_not_finished() {
        let mut stream = make_stream(1, 1, false);
        let _ = stream.next_row();
        // Buffer exhausted but server may have more
        assert!(stream.has_more());
    }

    // --- remaining ---

    #[test]
    fn remaining_full_buffer() {
        let stream = make_stream(5, 2, true);
        assert_eq!(stream.remaining(), 5);
    }

    #[test]
    fn remaining_after_consuming() {
        let mut stream = make_stream(3, 1, true);
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 2);
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 1);
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 0);
    }

    #[test]
    fn remaining_empty_result() {
        let stream = make_stream(0, 1, true);
        assert_eq!(stream.remaining(), 0);
    }

    // --- columns ---

    #[test]
    fn columns_returns_descriptors() {
        let stream = make_stream(1, 3, true);
        let cols = stream.columns();
        assert_eq!(cols.len(), 3);
        assert_eq!(&*cols[0].name, "col0");
        assert_eq!(&*cols[1].name, "col1");
        assert_eq!(&*cols[2].name, "col2");
    }

    // --- finished flag ---

    #[test]
    fn finished_stream_has_more_false_after_drain() {
        let mut stream = make_stream(1, 1, true);
        let _ = stream.next_row();
        assert!(!stream.has_more());
    }

    // --- fetch_next_chunk requires guard ---

    #[test]
    fn fetch_next_chunk_without_guard_errors() {
        let mut stream = make_stream(0, 1, false);
        let result = stream.fetch_next_chunk();
        assert!(result.is_err(), "should error without guard");
    }

    #[test]
    fn fetch_next_chunk_when_finished_returns_false() {
        let mut stream = make_stream(0, 1, true);
        let result = stream.fetch_next_chunk().unwrap();
        assert!(!result, "finished stream should return false");
    }

    // --- advance ---

    #[test]
    fn advance_returns_true_when_rows_available() {
        let mut stream = make_stream(2, 1, true);
        let has = stream.advance().unwrap();
        assert!(has);
    }

    #[test]
    fn advance_returns_false_when_finished_and_exhausted() {
        let mut stream = make_stream(1, 1, true);
        let _ = stream.next_row(); // consume the one row
        let has = stream.advance().unwrap();
        assert!(!has);
    }

    // --- Drop releases arena ---

    #[test]
    fn drop_releases_arena() {
        let stream = make_stream(3, 2, true);
        drop(stream);
        // If arena was released back to pool, acquire should succeed
        let arena = acquire_arena();
        bsql_driver_postgres::arena::release_arena(arena);
    }

    // --- fetch_next_chunk without arena errors ---

    #[test]
    fn fetch_next_chunk_without_arena_errors() {
        let columns = sample_columns(1);
        let result = make_result(0, &columns);
        let mut stream = QueryStream {
            guard: None,
            arena: None, // no arena
            current_result: Some(result),
            position: 0,
            columns,
            finished: false,
            needs_execute: false,
        };
        let res = stream.fetch_next_chunk();
        assert!(res.is_err(), "should error without arena");
    }

    // --- advance when not finished but fetch fails ---

    #[test]
    fn advance_fetch_fails_propagates_error() {
        // Stream with 0 rows, not finished, no guard -> advance triggers fetch -> error
        let mut stream = make_stream(0, 1, false);
        let res = stream.advance();
        assert!(res.is_err(), "advance should propagate fetch error");
    }

    // --- remaining on None result ---

    #[test]
    fn remaining_with_none_result() {
        let columns = sample_columns(1);
        let arena = acquire_arena();
        let stream = QueryStream {
            guard: None,
            arena: Some(arena),
            current_result: None,
            position: 0,
            columns,
            finished: true,
            needs_execute: false,
        };
        assert_eq!(stream.remaining(), 0);
    }

    // --- has_more with None result and finished ---

    #[test]
    fn has_more_with_none_result_finished() {
        let columns = sample_columns(1);
        let arena = acquire_arena();
        let stream = QueryStream {
            guard: None,
            arena: Some(arena),
            current_result: None,
            position: 0,
            columns,
            finished: true,
            needs_execute: false,
        };
        assert!(!stream.has_more());
    }

    // --- columns returns correct count ---

    #[test]
    fn columns_zero_columns() {
        let stream = make_stream(0, 0, true);
        assert_eq!(stream.columns().len(), 0);
    }

    // --- Creation patterns ---

    #[test]
    fn make_stream_finished_true_needs_execute_false() {
        let stream = make_stream(3, 2, true);
        assert!(
            !stream.needs_execute,
            "finished stream should not need execute"
        );
        assert!(stream.finished);
    }

    #[test]
    fn make_stream_not_finished_needs_execute_true() {
        let stream = make_stream(3, 2, false);
        assert!(
            stream.needs_execute,
            "unfinished stream should need execute"
        );
        assert!(!stream.finished);
    }

    #[test]
    fn make_stream_zero_rows_zero_cols_remaining_zero() {
        let stream = make_stream(0, 0, true);
        assert_eq!(stream.remaining(), 0);
        assert!(!stream.has_more());
    }

    // --- remaining/has_more interaction after partial consumption ---

    #[test]
    fn remaining_and_has_more_consistency() {
        let mut stream = make_stream(3, 1, true);
        // Before consuming: remaining=3, has_more=true
        assert_eq!(stream.remaining(), 3);
        assert!(stream.has_more());
        // Consume 1: remaining=2, has_more=true
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 2);
        assert!(stream.has_more());
        // Consume all: remaining=0, has_more=false (finished=true)
        let _ = stream.next_row();
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 0);
        assert!(!stream.has_more());
    }

    #[test]
    fn remaining_and_has_more_when_not_finished() {
        let mut stream = make_stream(1, 1, false);
        // Before consuming: remaining=1, has_more=true
        assert_eq!(stream.remaining(), 1);
        assert!(stream.has_more());
        // After consuming last row: remaining=0, but has_more=true (server may have more)
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 0);
        assert!(
            stream.has_more(),
            "unfinished stream should report has_more even with empty buffer"
        );
    }

    // --- Drop behavior ---

    #[test]
    fn drop_finished_stream_does_not_panic() {
        let mut stream = make_stream(1, 1, true);
        let _ = stream.next_row();
        // Stream is finished and consumed, drop should be clean
        drop(stream);
    }

    #[test]
    fn drop_unfinished_stream_does_not_panic() {
        // Unfinished stream (guard=None so mark_discard is not called)
        let stream = make_stream(5, 2, false);
        drop(stream);
    }

    // --- advance with rows already available ---

    #[test]
    fn advance_does_not_consume_row() {
        let mut stream = make_stream(2, 1, true);
        assert!(stream.advance().unwrap());
        // advance should not advance position, just check availability
        assert_eq!(stream.remaining(), 2);
        // next_row consumes
        let _ = stream.next_row();
        assert_eq!(stream.remaining(), 1);
    }
}
