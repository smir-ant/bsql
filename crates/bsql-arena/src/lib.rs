#![forbid(unsafe_code)]
#![deny(clippy::all)]

//! Bump allocator for row data — one allocation per query result.
//!
//! All row data (strings, byte arrays) from a single query is allocated into a
//! contiguous arena. When the result is dropped, one deallocation frees everything.
//!
//! # Thread-local recycling
//!
//! Arenas are recycled from a thread-local pool (LIFO, up to 4 per thread).
//! The arena object itself is never heap-allocated fresh on the hot path.
//!
//! # Chunk growth
//!
//! Initial chunk: 8KB. Growth: double the previous chunk size (capped at 1MB).
//! On `reset()`, chunks larger than 64KB are discarded to prevent long-term bloat.

use std::cell::RefCell;

/// Initial chunk size: 8KB.
///
/// Arena is used only for streaming queries (portal-based chunked fetch).
/// Regular queries store data in QueryResult.data_buf (a Vec<u8>), not arena.
/// 8KB is sufficient for streaming chunks and keeps thread-local pool light
/// (4 arenas × 8KB = 32KB per thread).
const INITIAL_CHUNK_SIZE: usize = 8 * 1024;

/// Maximum chunk size: 1MB cap to prevent runaway growth.
const MAX_CHUNK_SIZE: usize = 1024 * 1024;

/// Maximum number of arenas in the thread-local pool.
const MAX_POOL_SIZE: usize = 4;

/// Shrink threshold: chunks larger than this are discarded on reset.
const SHRINK_THRESHOLD: usize = 64 * 1024;

/// A bump allocator for row data.
///
/// Memory is allocated in contiguous chunks. Each `alloc` call bumps a pointer
/// forward. There is no per-allocation deallocation — the entire arena is freed
/// at once via `reset()` or `Drop`.
///
/// # Example
///
/// ```
/// use bsql_arena::Arena;
///
/// let mut arena = Arena::new();
/// let offset = arena.alloc_copy(b"hello");
/// assert_eq!(arena.get(offset, 5), b"hello");
/// arena.reset();
/// ```
pub struct Arena {
    chunks: Vec<Vec<u8>>,
    /// Cached cumulative chunk capacities for O(1) offset resolution.
    /// `prefix_sums[i]` = sum of capacities of chunks 0..i.
    prefix_sums: Vec<usize>,
    current: usize,
    offset: usize,
}

impl Arena {
    /// Create a new arena with an 8KB initial chunk.
    pub fn new() -> Self {
        let chunk = Vec::with_capacity(INITIAL_CHUNK_SIZE);
        Self {
            chunks: vec![chunk],
            prefix_sums: vec![0],
            current: 0,
            offset: 0,
        }
    }

    /// Create an empty arena with zero allocation.
    ///
    /// No memory is allocated until `alloc` or `alloc_copy` is called.
    /// Used by query paths that store data in `QueryResult.data_buf` instead.
    pub fn empty() -> Self {
        Self {
            chunks: Vec::new(),
            prefix_sums: Vec::new(),
            current: 0,
            offset: 0,
        }
    }

    /// Allocate `len` bytes, returning a mutable slice into the arena.
    ///
    /// The returned slice is zeroed. For copying data in, prefer `alloc_copy`.
    pub fn alloc(&mut self, len: usize) -> &mut [u8] {
        if len == 0 {
            return &mut [];
        }

        self.ensure_capacity(len);

        let chunk = &mut self.chunks[self.current];
        let start = self.offset;
        let new_len = start + len;

        // Extend the chunk's length (capacity is guaranteed by ensure_capacity)
        // vec![0u8; N].resize(N, 0) zeroes new bytes. This is the cost of safe
        // Rust — the kernel already zeroes mmap'd pages, so the real overhead is
        // only on reused capacity. Cannot avoid without unsafe.
        if new_len > chunk.len() {
            chunk.resize(new_len, 0);
        }

        self.offset = new_len;
        &mut chunk[start..new_len]
    }

    /// Copy `data` into the arena and return the global offset.
    ///
    /// The offset can be used with `get()` to retrieve the data later.
    #[inline(always)]
    pub fn alloc_copy(&mut self, data: &[u8]) -> usize {
        let len = data.len();
        if len == 0 {
            return self.global_offset();
        }

        // Fast path: data fits in current chunk's remaining capacity.
        // No function calls, no branches — just memcpy and bump.
        // Guard: empty arena (lazy init) falls through to slow path.
        if let Some(chunk) = self.chunks.get_mut(self.current) {
            let remaining = chunk.capacity() - self.offset;
            if remaining >= len {
                let start = self.offset;
                // Append directly — extend_from_slice is the fastest way to
                // copy data into a Vec when we know capacity is sufficient.
                // It compiles to a single memcpy + length update.
                if start == chunk.len() {
                    chunk.extend_from_slice(data);
                } else {
                    let new_len = start + len;
                    if new_len > chunk.len() {
                        chunk.resize(new_len, 0);
                    }
                    chunk[start..start + len].copy_from_slice(data);
                }
                let global = self.prefix_sums[self.current] + start;
                self.offset = start + len;
                return global;
            }
        }

        // Slow path: need a new chunk (or first allocation for lazy arena).
        self.alloc_copy_slow(data)
    }

    /// Slow path for alloc_copy — allocates a new chunk.
    #[cold]
    #[inline(never)]
    fn alloc_copy_slow(&mut self, data: &[u8]) -> usize {
        self.ensure_capacity(data.len());

        let chunk = &mut self.chunks[self.current];
        let start = self.offset;
        chunk.extend_from_slice(data);

        let global = self.prefix_sums[self.current] + start;
        self.offset = start + data.len();
        global
    }

    /// Retrieve a slice from the arena by global offset and length.
    ///
    /// # Panics
    ///
    /// Panics if the offset + length exceeds the arena's allocated range.
    pub fn get(&self, global_offset: usize, len: usize) -> &[u8] {
        if len == 0 {
            return &[];
        }

        let (chunk_idx, local_offset) = self.resolve_offset(global_offset);
        &self.chunks[chunk_idx][local_offset..local_offset + len]
    }

    /// Retrieve a str slice from the arena. Returns `None` if not valid UTF-8.
    ///
    /// Uses SIMD-accelerated UTF-8 validation via `simdutf8`.
    pub fn get_str(&self, global_offset: usize, len: usize) -> Option<&str> {
        if len == 0 {
            return Some("");
        }
        simdutf8::basic::from_utf8(self.get(global_offset, len)).ok()
    }

    /// Reset the arena for reuse. Keeps allocated memory but resets the bump pointer.
    ///
    /// Chunks larger than 64KB are discarded to prevent long-term bloat.
    pub fn reset(&mut self) {
        // Discard oversized chunks, keep small ones
        self.chunks.retain(|c| c.capacity() <= SHRINK_THRESHOLD);

        if self.chunks.is_empty() {
            self.chunks.push(Vec::with_capacity(INITIAL_CHUNK_SIZE));
        }

        // Clear all chunks (set len to 0, keep capacity)
        for chunk in &mut self.chunks {
            chunk.clear();
        }

        // Rebuild prefix_sums
        self.rebuild_prefix_sums();

        self.current = 0;
        self.offset = 0;
    }

    /// Total bytes allocated in this arena (across all chunks).
    pub fn allocated(&self) -> usize {
        if self.chunks.is_empty() {
            return 0;
        }
        // Fast path: single chunk (common case — most queries fit in 8KB).
        if self.current == 0 {
            return self.offset;
        }
        // Multi-chunk: sum filled chunks + current offset.
        let mut total = self.offset;
        for chunk in &self.chunks[..self.current] {
            total += chunk.len();
        }
        total
    }

    /// Total capacity of all chunks (for diagnostics).
    pub fn capacity(&self) -> usize {
        self.chunks.iter().map(|c| c.capacity()).sum()
    }

    // --- Internal ---

    /// Ensure the current chunk has room for `len` bytes. If not, allocate a new chunk.
    fn ensure_capacity(&mut self, len: usize) {
        // Lazy arena: first allocation creates the initial chunk.
        if self.chunks.is_empty() {
            let cap = INITIAL_CHUNK_SIZE.max(len);
            self.chunks.push(Vec::with_capacity(cap));
            self.prefix_sums.push(0);
            self.current = 0;
            self.offset = 0;
            return;
        }

        let chunk = &self.chunks[self.current];
        let remaining = chunk.capacity().saturating_sub(self.offset);

        if remaining >= len {
            return;
        }

        // Need a new chunk. Size = max(double previous capacity, len, INITIAL_CHUNK_SIZE)
        let prev_cap = chunk.capacity();
        let new_cap = prev_cap
            .saturating_mul(2)
            .max(len)
            .max(INITIAL_CHUNK_SIZE)
            .min(MAX_CHUNK_SIZE.max(len)); // allow exceeding MAX for single large allocs

        // Check if the next chunk already exists and has enough capacity
        let next_idx = self.current + 1;
        if next_idx < self.chunks.len() && self.chunks[next_idx].capacity() >= len {
            self.current = next_idx;
            self.offset = 0;
            return;
        }

        // Allocate a new chunk and update prefix_sums
        let new_chunk = Vec::with_capacity(new_cap);
        let prefix = self.prefix_sums[self.chunks.len() - 1]
            + self.chunks.last().map_or(0, |c| c.capacity());
        if next_idx < self.chunks.len() {
            self.chunks[next_idx] = new_chunk;
            // Rebuild prefix_sums since a chunk capacity changed
            self.rebuild_prefix_sums();
        } else {
            self.chunks.push(new_chunk);
            self.prefix_sums.push(prefix);
        }
        self.current = next_idx;
        self.offset = 0;
    }

    /// Rebuild the prefix_sums cache from current chunk capacities.
    fn rebuild_prefix_sums(&mut self) {
        self.prefix_sums.clear();
        let mut sum = 0;
        for chunk in &self.chunks {
            self.prefix_sums.push(sum);
            sum += chunk.capacity();
        }
    }

    /// Compute the global offset for the current position.
    pub fn global_offset(&self) -> usize {
        if self.chunks.is_empty() {
            return 0;
        }
        self.global_offset_at(self.current, self.offset)
    }

    /// Compute a global offset from chunk index and local offset.
    /// O(1) using cached prefix_sums.
    fn global_offset_at(&self, chunk_idx: usize, local_offset: usize) -> usize {
        self.prefix_sums[chunk_idx] + local_offset
    }

    /// Resolve a global offset to (chunk_index, local_offset).
    /// O(log n) using binary search on prefix_sums.
    fn resolve_offset(&self, global_offset: usize) -> (usize, usize) {
        // for the common case (most queries fit in one 8KB chunk).
        if self.chunks.len() == 1 {
            debug_assert!(
                global_offset < self.chunks[0].capacity(),
                "arena offset {global_offset} out of bounds in single chunk (cap={})",
                self.chunks[0].capacity()
            );
            return (0, global_offset);
        }

        // Binary search: find the last chunk whose prefix_sum <= global_offset
        let idx = match self.prefix_sums.binary_search(&global_offset) {
            Ok(i) => i,
            Err(0) => 0, // guard against underflow when global_offset < prefix_sums[0]
            Err(i) => i - 1,
        };
        let local = global_offset - self.prefix_sums[idx];
        debug_assert!(
            local < self.chunks[idx].capacity(),
            "arena offset {global_offset} out of bounds in chunk {idx} (cap={})",
            self.chunks[idx].capacity()
        );
        (idx, local)
    }
}

impl Default for Arena {
    /// Default creates a lazy arena (zero upfront allocation).
    /// The first `alloc_copy` call allocates the initial 8KB chunk.
    fn default() -> Self {
        Self::empty()
    }
}

// --- Thread-local arena pool ---

thread_local! {
    static ARENA_POOL: RefCell<Vec<Arena>> = const { RefCell::new(Vec::new()) };
}

/// Acquire an arena from the thread-local pool, or create a new one.
///
/// LIFO ordering: returns the most recently released arena (warmest cache).
///
/// # Example
///
/// ```
/// use bsql_arena::{acquire_arena, release_arena};
///
/// let mut arena = acquire_arena();
/// let offset = arena.alloc_copy(b"data");
/// // ... use arena ...
/// release_arena(arena);
/// ```
pub fn acquire_arena() -> Arena {
    ARENA_POOL
        .with(|pool| pool.borrow_mut().pop())
        .unwrap_or_default()
}

/// Return an arena to the thread-local pool for reuse.
///
/// The arena is reset (bump pointer zeroed, oversized chunks discarded).
/// If the pool is full (4 arenas), the arena is dropped instead.
pub fn release_arena(mut arena: Arena) {
    arena.reset();
    ARENA_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < MAX_POOL_SIZE {
            pool.push(arena);
        }
        // else: drop the arena (too many in pool)
    });
}

// ---------------------------------------------------------------------------
// ArenaRows — arena-backed row storage with borrowed strings
// ---------------------------------------------------------------------------

/// A collection of decoded rows backed by an arena.
///
/// Text and blob columns in `T` are `&'static str` / `&'static [u8]` whose
/// memory actually lives in the arena stored alongside them. The `'static`
/// lifetime is a fiction — the data is valid for as long as this struct lives.
///
/// # Safety contract
///
/// The `Vec<T>` is dropped **before** the `Arena` (Rust drops fields in
/// declaration order). The `&'static str` / `&'static [u8]` references
/// inside `T` are never dereferenced after the arena is freed.
///
/// # Drop order guarantee
///
/// Rust guarantees fields are dropped in declaration order (RFC 1857).
/// `rows` is declared before `arena`, so all `T` values (and their borrowed
/// pointers) are dropped before the arena memory is freed.
pub struct ArenaRows<T> {
    rows: Vec<T>,
    arena: Arena,
}

impl<T> ArenaRows<T> {
    /// Build `ArenaRows` from an arena and a row vector.
    ///
    /// `T` should contain only Copy types (integers, floats, bools) and
    /// byte-range indices into a separately validated text buffer. No
    /// `&'static str` transmute is involved.
    pub fn new(rows: Vec<T>, arena: Arena) -> Self {
        Self { rows, arena }
    }

    /// Number of rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the result set is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get a row by index.
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&T> {
        self.rows.get(idx)
    }

    /// Iterate over rows by reference.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.rows.iter()
    }

    /// Consume into the inner `Vec<T>` and arena.
    ///
    /// Returns both so the caller can decide what to do with the arena.
    pub fn into_parts(self) -> (Vec<T>, Arena) {
        (self.rows, self.arena)
    }

    /// Total bytes allocated in the backing arena.
    #[inline]
    pub fn arena_allocated(&self) -> usize {
        self.arena.allocated()
    }
}

impl<T> std::ops::Deref for ArenaRows<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.rows
    }
}

impl<'a, T> IntoIterator for &'a ArenaRows<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ArenaRows<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaRows")
            .field("len", &self.rows.len())
            .field("arena_allocated", &self.arena.allocated())
            .field("rows", &self.rows)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ValidatedRows — batch-validated text, zero unsafe
// ---------------------------------------------------------------------------

/// A collection of decoded rows with batch-validated text data.
///
/// Text columns are stored as byte ranges `(u32, u32)` into a shared,
/// batch-validated `String` buffer. Blob columns are stored as byte ranges
/// into the `Arena`. Scalar columns (i64, f64, bool) are stored directly.
///
/// # Zero unsafe
///
/// The text buffer is validated once via `String::from_utf8` (SIMD-accelerated
/// in std on modern CPUs). No `from_utf8_unchecked`, no `transmute`, no
/// lifetime extension.
///
/// # Usage pattern
///
/// The codegen generates an "inner" struct with byte ranges and a "view" struct
/// with `&str`. `ValidatedRows::iter()` maps inner -> view by slicing the
/// validated text buffer.
pub struct ValidatedRows<T> {
    rows: Vec<T>,
    text_buf: String,
    blob_arena: Arena,
}

impl<T> ValidatedRows<T> {
    /// Build `ValidatedRows` from a text buffer (already validated as UTF-8),
    /// a blob arena, and the decoded inner rows.
    pub fn new(rows: Vec<T>, text_buf: String, blob_arena: Arena) -> Self {
        Self {
            rows,
            text_buf,
            blob_arena,
        }
    }

    /// Get the validated text buffer.
    #[inline]
    pub fn text(&self) -> &str {
        &self.text_buf
    }

    /// Get a text slice by byte range. Panics if range is out of bounds
    /// or not on a UTF-8 char boundary (impossible if ranges were recorded
    /// correctly during the step loop).
    #[inline]
    pub fn text_slice(&self, start: u32, end: u32) -> &str {
        &self.text_buf[start as usize..end as usize]
    }

    /// Get a blob slice from the arena by global offset and length.
    #[inline]
    pub fn blob_slice(&self, offset: u32, len: u32) -> &[u8] {
        self.blob_arena.get(offset as usize, len as usize)
    }

    /// Number of rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the result set is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get an inner row by index.
    #[inline]
    pub fn get_inner(&self, idx: usize) -> Option<&T> {
        self.rows.get(idx)
    }

    /// Iterate over inner rows by reference.
    #[inline]
    pub fn iter_inner(&self) -> std::slice::Iter<'_, T> {
        self.rows.iter()
    }

    /// Total bytes in the text buffer.
    #[inline]
    pub fn text_len(&self) -> usize {
        self.text_buf.len()
    }

    /// Total bytes allocated in the blob arena.
    #[inline]
    pub fn blob_allocated(&self) -> usize {
        self.blob_arena.allocated()
    }

    /// Total bytes allocated (text + blobs).
    #[inline]
    pub fn arena_allocated(&self) -> usize {
        self.text_buf.len() + self.blob_arena.allocated()
    }
}

impl<T> std::ops::Deref for ValidatedRows<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.rows
    }
}

impl<'a, T> IntoIterator for &'a ValidatedRows<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ValidatedRows<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatedRows")
            .field("len", &self.rows.len())
            .field("text_len", &self.text_buf.len())
            .field("blob_allocated", &self.blob_arena.allocated())
            .field("rows", &self.rows)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_alloc_and_get() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(b"hello");
        assert_eq!(arena.get(offset, 5), b"hello");
    }

    #[test]
    fn multiple_allocs() {
        let mut arena = Arena::new();
        let o1 = arena.alloc_copy(b"foo");
        let o2 = arena.alloc_copy(b"bar");
        let o3 = arena.alloc_copy(b"baz");

        assert_eq!(arena.get(o1, 3), b"foo");
        assert_eq!(arena.get(o2, 3), b"bar");
        assert_eq!(arena.get(o3, 3), b"baz");
    }

    #[test]
    fn alloc_str_retrieval() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(b"hello world");
        assert_eq!(arena.get_str(offset, 11), Some("hello world"));
    }

    #[test]
    fn zero_length_alloc() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(b"");
        let data = arena.get(offset, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn alloc_returns_zeroed_slice() {
        let mut arena = Arena::new();
        let slice = arena.alloc(16);
        assert!(slice.iter().all(|&b| b == 0));
    }

    #[test]
    fn reset_allows_reuse() {
        let mut arena = Arena::new();
        let _o1 = arena.alloc_copy(b"before reset");
        assert_eq!(arena.allocated(), 12);

        arena.reset();
        assert_eq!(arena.allocated(), 0);

        let o2 = arena.alloc_copy(b"after reset");
        assert_eq!(arena.get(o2, 11), b"after reset");
    }

    #[test]
    fn chunk_growth() {
        let mut arena = Arena::new();

        // Fill the initial 8KB chunk
        let big = vec![0xAA; INITIAL_CHUNK_SIZE + 1];
        let offset = arena.alloc_copy(&big);
        assert_eq!(arena.get(offset, big.len())[0], 0xAA);
        assert!(
            arena.chunks.len() >= 2,
            "should have grown to a second chunk"
        );
    }

    #[test]
    fn large_single_alloc() {
        let mut arena = Arena::new();
        let data = vec![0x42; 2 * MAX_CHUNK_SIZE];
        let offset = arena.alloc_copy(&data);
        let result = arena.get(offset, data.len());
        assert!(result.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn one_hundred_rows_in_one_chunk() {
        let mut arena = Arena::new();
        let row_data = b"typical row data, about 50 bytes of text content.";

        let mut offsets = Vec::new();
        for _ in 0..100 {
            offsets.push(arena.alloc_copy(row_data));
        }

        // 100 * 50 = 5000 bytes, fits in 8KB initial chunk
        assert_eq!(arena.chunks.len(), 1);

        for &offset in &offsets {
            assert_eq!(arena.get(offset, row_data.len()), row_data);
        }
    }

    #[test]
    fn reset_discards_oversized_chunks() {
        let mut arena = Arena::new();

        // Allocate a chunk larger than SHRINK_THRESHOLD
        let big = vec![0xFF; SHRINK_THRESHOLD + 1];
        arena.alloc_copy(&big);

        let _chunks_before = arena.chunks.len();
        arena.reset();

        // Oversized chunks should be discarded
        for chunk in &arena.chunks {
            assert!(
                chunk.capacity() <= SHRINK_THRESHOLD,
                "oversized chunk not discarded: capacity={}",
                chunk.capacity()
            );
        }
    }

    #[test]
    fn thread_local_pool_acquire_release() {
        let mut arena = acquire_arena();
        arena.alloc_copy(b"test data");
        release_arena(arena);

        // Second acquire should get the recycled arena
        let arena2 = acquire_arena();
        assert_eq!(arena2.allocated(), 0); // should be reset
        release_arena(arena2);
    }

    #[test]
    fn thread_local_pool_max_size() {
        // Release MAX_POOL_SIZE + 1 arenas, only MAX_POOL_SIZE should be kept
        for _ in 0..MAX_POOL_SIZE + 2 {
            let arena = Arena::new();
            release_arena(arena);
        }

        ARENA_POOL.with(|pool| {
            assert!(pool.borrow().len() <= MAX_POOL_SIZE);
        });
    }

    #[test]
    fn capacity_reports_total() {
        let arena = Arena::new();
        assert!(arena.capacity() >= INITIAL_CHUNK_SIZE);
    }

    #[test]
    fn allocated_tracks_usage() {
        let mut arena = Arena::new();
        assert_eq!(arena.allocated(), 0);
        arena.alloc_copy(b"12345");
        assert_eq!(arena.allocated(), 5);
        arena.alloc_copy(b"67890");
        assert_eq!(arena.allocated(), 10);
    }

    #[test]
    fn alloc_at_exact_8kb_boundary() {
        let mut arena = Arena::new();

        // Fill exactly to the 8KB boundary
        let filler = vec![0xAA; INITIAL_CHUNK_SIZE];
        let o1 = arena.alloc_copy(&filler);
        assert_eq!(arena.get(o1, INITIAL_CHUNK_SIZE)[0], 0xAA);
        assert_eq!(arena.chunks.len(), 1);

        // Next alloc (even 1 byte) must trigger a new chunk
        let o2 = arena.alloc_copy(b"x");
        assert_eq!(arena.get(o2, 1), b"x");
        assert!(arena.chunks.len() >= 2, "should have grown past 8KB chunk");

        // Data from both chunks must still be accessible
        assert_eq!(arena.get(o1, INITIAL_CHUNK_SIZE)[0], 0xAA);
        assert_eq!(
            arena.get(o1, INITIAL_CHUNK_SIZE)[INITIAL_CHUNK_SIZE - 1],
            0xAA
        );
    }

    #[test]
    fn prefix_sums_correct_after_multi_chunk() {
        let mut arena = Arena::new();
        let mut offsets = Vec::new();

        // Force 4 chunks
        for i in 0..4 {
            let data = vec![i as u8; INITIAL_CHUNK_SIZE + 1];
            offsets.push((arena.alloc_copy(&data), data.len()));
        }

        // Verify all data is retrievable (exercises prefix_sums-based resolve_offset)
        for (idx, &(offset, len)) in offsets.iter().enumerate() {
            let data = arena.get(offset, len);
            assert!(data.iter().all(|&b| b == idx as u8));
        }
    }

    #[test]
    fn prefix_sums_correct_after_reset() {
        let mut arena = Arena::new();

        // Force a second chunk
        let big = vec![0xBB; INITIAL_CHUNK_SIZE + 1];
        arena.alloc_copy(&big);
        assert!(arena.chunks.len() >= 2);

        arena.reset();

        // After reset, alloc should work correctly with rebuilt prefix_sums
        let o = arena.alloc_copy(b"after reset");
        assert_eq!(arena.get(o, 11), b"after reset");
    }

    /// T-01: resolve_offset with global_offset=0 must return (0, 0)
    #[test]
    fn resolve_offset_zero() {
        let arena = Arena::new();
        let (chunk_idx, local) = arena.resolve_offset(0);
        assert_eq!(chunk_idx, 0);
        assert_eq!(local, 0);
    }

    /// Single-chunk fast-path in resolve_offset.
    #[test]
    fn resolve_offset_single_chunk_fast_path() {
        let mut arena = Arena::new();
        // Stay within one chunk
        let o1 = arena.alloc_copy(b"hello");
        let o2 = arena.alloc_copy(b"world");
        assert_eq!(arena.chunks.len(), 1, "should be single chunk");

        // resolve_offset uses fast-path
        assert_eq!(arena.get(o1, 5), b"hello");
        assert_eq!(arena.get(o2, 5), b"world");
    }

    // --- Audit gap tests ---

    // #56: get_str with invalid UTF-8
    #[test]
    fn get_str_invalid_utf8_returns_none() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(&[0xFF, 0xFE, 0xFD]);
        assert_eq!(arena.get_str(offset, 3), None);
    }

    // #56 extra: get_str with valid UTF-8
    #[test]
    fn get_str_valid_utf8() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy("hello".as_bytes());
        assert_eq!(arena.get_str(offset, 5), Some("hello"));
    }

    // #56 extra: get_str with empty string
    #[test]
    fn get_str_empty_returns_some_empty() {
        let arena = Arena::new();
        assert_eq!(arena.get_str(0, 0), Some(""));
    }

    // #57: get() with offset beyond bounds panics
    #[test]
    #[should_panic]
    fn get_out_of_bounds_panics() {
        let arena = Arena::new();
        // Try to read beyond the arena (capacity is 8KB but nothing allocated)
        arena.get(INITIAL_CHUNK_SIZE + 100, 1);
    }

    // #58: ensure_capacity reusing existing next chunk
    #[test]
    fn ensure_capacity_reuses_next_chunk() {
        let mut arena = Arena::new();

        // Fill first chunk to force a second
        let big = vec![0xAA; INITIAL_CHUNK_SIZE + 1];
        arena.alloc_copy(&big);
        assert!(arena.chunks.len() >= 2);

        // Reset (keeps small chunks)
        arena.reset();
        assert_eq!(arena.current, 0);
        assert_eq!(arena.offset, 0);

        // Now fill first chunk again — second alloc should reuse existing chunk
        let filler = vec![0xBB; INITIAL_CHUNK_SIZE];
        arena.alloc_copy(&filler);
        // Next alloc should reuse the existing second chunk if capacity is sufficient
        let o = arena.alloc_copy(b"reuse check");
        assert_eq!(arena.get(o, 11), b"reuse check");
    }

    // #59: Multi-thread safety: acquire on thread A, release on thread B
    #[test]
    fn arena_cross_thread_no_crash() {
        // Thread-local pools are per-thread, so this just verifies
        // Arena is Send (can move between threads) without crashing.
        let mut arena = Arena::new();
        arena.alloc_copy(b"test data");

        let handle = std::thread::spawn(move || {
            // Arena moved to another thread — should not crash
            assert_eq!(arena.get(0, 9), b"test data");
            arena.reset();
            arena
        });

        let arena = handle.join().unwrap();
        // Release on the original thread's pool
        release_arena(arena);
    }

    // --- ArenaRows tests (safe) ---

    #[test]
    fn arena_rows_basic() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![42], arena);
        assert_eq!(ar.len(), 1);
        assert!(!ar.is_empty());
        assert_eq!(ar[0], 42);
        assert_eq!(ar.get(0), Some(&42));
    }

    #[test]
    fn arena_rows_empty() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![], arena);
        assert!(ar.is_empty());
        assert_eq!(ar.len(), 0);
        assert!(ar.get(0).is_none());
    }

    #[test]
    fn arena_rows_iter() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![10, 20, 30], arena);
        let vals: Vec<&i64> = ar.iter().collect();
        assert_eq!(vals, vec![&10, &20, &30]);
    }

    #[test]
    fn arena_rows_deref() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![1, 2, 3], arena);
        let slice: &[i64] = &ar;
        assert_eq!(slice, &[1, 2, 3]);
    }

    #[test]
    fn arena_rows_for_loop() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![10, 20], arena);
        let mut sum = 0;
        for &val in &ar {
            sum += val;
        }
        assert_eq!(sum, 30);
    }

    #[test]
    fn arena_rows_debug() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![42], arena);
        let dbg = format!("{ar:?}");
        assert!(dbg.contains("ArenaRows"));
        assert!(dbg.contains("42"));
    }

    #[test]
    fn arena_rows_arena_allocated() {
        let mut arena = Arena::new();
        arena.alloc_copy(b"some data");
        let allocated = arena.allocated();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![], arena);
        assert_eq!(ar.arena_allocated(), allocated);
    }

    #[test]
    fn arena_rows_into_parts() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![1, 2, 3], arena);
        let (v, _arena) = ar.into_parts();
        assert_eq!(v, vec![1, 2, 3]);
    }

    #[test]
    fn arena_rows_into_parts_empty() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![], arena);
        let (v, _arena) = ar.into_parts();
        assert!(v.is_empty());
    }

    #[test]
    fn arena_rows_get_out_of_bounds() {
        let arena = Arena::new();
        let ar: ArenaRows<i64> = ArenaRows::new(vec![42], arena);
        assert_eq!(ar.get(0), Some(&42));
        assert_eq!(ar.get(1), None);
        assert_eq!(ar.get(999), None);
    }

    // --- ValidatedRows tests ---

    #[test]
    fn validated_rows_basic() {
        let text_buf = String::from("alicebob");
        let blob_arena = Arena::new();

        #[derive(Debug)]
        #[allow(dead_code)]
        struct Inner {
            id: i64,
            name_start: u32,
            name_end: u32,
        }

        let rows = vec![
            Inner {
                id: 1,
                name_start: 0,
                name_end: 5,
            },
            Inner {
                id: 2,
                name_start: 5,
                name_end: 8,
            },
        ];
        let vr = ValidatedRows::new(rows, text_buf, blob_arena);

        assert_eq!(vr.len(), 2);
        assert!(!vr.is_empty());
        assert_eq!(vr.text_slice(vr[0].name_start, vr[0].name_end), "alice");
        assert_eq!(vr.text_slice(vr[1].name_start, vr[1].name_end), "bob");
    }

    #[test]
    fn validated_rows_empty() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![], String::new(), Arena::new());
        assert!(vr.is_empty());
        assert_eq!(vr.len(), 0);
        assert_eq!(vr.text_len(), 0);
    }

    #[test]
    fn validated_rows_blob() {
        let mut blob_arena = Arena::new();
        let off = blob_arena.alloc_copy(&[0xDE, 0xAD]);

        #[derive(Debug)]
        struct Inner {
            blob_off: u32,
            blob_len: u32,
        }

        let rows = vec![Inner {
            blob_off: off as u32,
            blob_len: 2,
        }];
        let vr = ValidatedRows::new(rows, String::new(), blob_arena);

        assert_eq!(vr.blob_slice(vr[0].blob_off, vr[0].blob_len), &[0xDE, 0xAD]);
    }

    #[test]
    fn validated_rows_arena_allocated() {
        let mut blob_arena = Arena::new();
        blob_arena.alloc_copy(&[1, 2, 3]);
        let text_buf = String::from("hello");

        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![], text_buf, blob_arena);
        assert_eq!(vr.arena_allocated(), 5 + 3); // text_len + blob_allocated
    }

    #[test]
    fn validated_rows_debug() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![42], String::new(), Arena::new());
        let dbg = format!("{vr:?}");
        assert!(dbg.contains("ValidatedRows"));
        assert!(dbg.contains("42"));
    }

    #[test]
    fn validated_rows_deref() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![1, 2, 3], String::new(), Arena::new());
        let slice: &[i64] = &vr;
        assert_eq!(slice, &[1, 2, 3]);
    }

    #[test]
    fn validated_rows_iter() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![10, 20], String::new(), Arena::new());
        let mut sum = 0;
        for &val in &vr {
            sum += val;
        }
        assert_eq!(sum, 30);
    }

    // --- alloc zero length slice ---

    #[test]
    fn alloc_zero_returns_empty_slice() {
        let mut arena = Arena::new();
        let slice = arena.alloc(0);
        assert!(slice.is_empty());
    }

    // --- get_str with zero length ---

    #[test]
    fn get_str_zero_len_returns_empty() {
        let arena = Arena::new();
        assert_eq!(arena.get_str(0, 0), Some(""));
    }

    // ===============================================================
    // ValidatedRows — comprehensive tests
    // ===============================================================

    #[test]
    fn validated_rows_empty_text_buf() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![1, 2, 3], String::new(), Arena::new());
        assert_eq!(vr.text(), "");
        assert_eq!(vr.text_len(), 0);
        assert_eq!(vr.len(), 3);
    }

    #[test]
    fn validated_rows_blob_only_no_text() {
        let mut blob_arena = Arena::new();
        let o1 = blob_arena.alloc_copy(&[0x01, 0x02, 0x03]);
        let o2 = blob_arena.alloc_copy(&[0xAA, 0xBB]);

        #[derive(Debug)]
        struct Inner {
            off: u32,
            len: u32,
        }

        let rows = vec![
            Inner {
                off: o1 as u32,
                len: 3,
            },
            Inner {
                off: o2 as u32,
                len: 2,
            },
        ];
        let vr = ValidatedRows::new(rows, String::new(), blob_arena);
        assert_eq!(vr.text_len(), 0);
        assert_eq!(vr.blob_slice(vr[0].off, vr[0].len), &[0x01, 0x02, 0x03]);
        assert_eq!(vr.blob_slice(vr[1].off, vr[1].len), &[0xAA, 0xBB]);
    }

    #[test]
    #[should_panic]
    fn validated_rows_text_slice_out_of_bounds() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![], String::from("hi"), Arena::new());
        // end is beyond the text buffer
        vr.text_slice(0, 100);
    }

    #[test]
    #[should_panic]
    fn validated_rows_blob_slice_out_of_bounds() {
        let blob_arena = Arena::new();
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![], String::new(), blob_arena);
        // nothing allocated in blob arena
        vr.blob_slice(0, 100);
    }

    #[test]
    fn validated_rows_large_10k_rows() {
        let mut text_buf = String::new();
        let blob_arena = Arena::new();

        #[derive(Debug)]
        struct Inner {
            start: u32,
            end: u32,
        }

        let mut rows = Vec::with_capacity(10_000);
        for i in 0..10_000u32 {
            let start = text_buf.len() as u32;
            text_buf.push_str(&format!("row_{i}"));
            let end = text_buf.len() as u32;
            rows.push(Inner { start, end });
        }

        let vr = ValidatedRows::new(rows, text_buf, blob_arena);
        assert_eq!(vr.len(), 10_000);
        assert_eq!(vr.text_slice(vr[0].start, vr[0].end), "row_0");
        assert_eq!(vr.text_slice(vr[9999].start, vr[9999].end), "row_9999");
    }

    #[test]
    fn validated_rows_text_slice_empty_range() {
        let vr: ValidatedRows<i64> =
            ValidatedRows::new(vec![], String::from("hello"), Arena::new());
        assert_eq!(vr.text_slice(0, 0), "");
        assert_eq!(vr.text_slice(3, 3), "");
    }

    #[test]
    fn validated_rows_get_inner() {
        let vr: ValidatedRows<i64> =
            ValidatedRows::new(vec![10, 20, 30], String::new(), Arena::new());
        assert_eq!(vr.get_inner(0), Some(&10));
        assert_eq!(vr.get_inner(1), Some(&20));
        assert_eq!(vr.get_inner(2), Some(&30));
        assert_eq!(vr.get_inner(3), None);
    }

    #[test]
    fn validated_rows_iter_inner() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![5, 10], String::new(), Arena::new());
        let vals: Vec<&i64> = vr.iter_inner().collect();
        assert_eq!(vals, vec![&5, &10]);
    }

    #[test]
    fn validated_rows_blob_allocated_zero() {
        let vr: ValidatedRows<i64> = ValidatedRows::new(vec![], String::new(), Arena::new());
        assert_eq!(vr.blob_allocated(), 0);
    }

    // ===============================================================
    // Arena — additional edge cases
    // ===============================================================

    #[test]
    fn arena_get_zero_len() {
        let arena = Arena::new();
        let data = arena.get(0, 0);
        assert!(data.is_empty());
    }

    #[test]
    fn arena_alloc_copy_zero_len() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(b"");
        assert_eq!(arena.get(offset, 0), &[]);
    }

    #[test]
    fn arena_global_offset_initial() {
        let arena = Arena::new();
        assert_eq!(arena.global_offset(), 0);
    }

    #[test]
    fn arena_global_offset_advances() {
        let mut arena = Arena::new();
        arena.alloc_copy(b"12345");
        assert_eq!(arena.global_offset(), 5);
        arena.alloc_copy(b"67890");
        assert_eq!(arena.global_offset(), 10);
    }

    #[test]
    fn arena_multiple_resets() {
        let mut arena = Arena::new();
        for _ in 0..10 {
            arena.alloc_copy(b"data");
            assert_eq!(arena.allocated(), 4);
            arena.reset();
            assert_eq!(arena.allocated(), 0);
        }
    }

    #[test]
    fn arena_get_str_unicode() {
        let texts = [
            "\u{1F600}\u{1F4A9}",         // emoji
            "\u{4e16}\u{754c}",           // CJK
            "caf\u{00e9}",                // accented
            "\u{1F468}\u{200D}\u{1F469}", // ZWJ
        ];
        for text in &texts {
            let mut arena = Arena::new();
            let offset = arena.alloc_copy(text.as_bytes());
            assert_eq!(
                arena.get_str(offset, text.len()),
                Some(*text),
                "failed for text: {text}"
            );
        }
    }

    #[test]
    fn arena_get_str_partial_utf8_returns_none() {
        // 0xC3 is the start of a 2-byte UTF-8 sequence, incomplete without the second byte
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(&[0xC3]);
        assert_eq!(arena.get_str(offset, 1), None);
    }

    #[test]
    fn arena_default_is_empty() {
        let a = Arena::default();
        assert_eq!(a.allocated(), 0);
        assert_eq!(a.capacity(), 0);
    }

    // ===============================================================
    // ArenaRows — additional edge cases
    // ===============================================================

    #[test]
    fn arena_rows_large() {
        let arena = Arena::new();
        let rows: Vec<i64> = (0..1000).collect();
        let ar = ArenaRows::new(rows, arena);
        assert_eq!(ar.len(), 1000);
        assert_eq!(ar[0], 0);
        assert_eq!(ar[999], 999);
    }

    #[test]
    fn arena_rows_with_arena_data() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(b"stored data");

        #[derive(Debug)]
        #[allow(dead_code)]
        struct Inner {
            off: usize,
            len: usize,
        }

        let ar = ArenaRows::new(
            vec![Inner {
                off: offset,
                len: 11,
            }],
            arena,
        );
        assert_eq!(ar.len(), 1);
    }

    // ===============================================================
    // Thread-local pool edge cases
    // ===============================================================

    #[test]
    fn thread_local_pool_acquire_fresh() {
        // Drain the pool first
        ARENA_POOL.with(|pool| pool.borrow_mut().clear());
        let arena = acquire_arena();
        assert_eq!(arena.allocated(), 0);
        release_arena(arena);
    }

    #[test]
    fn thread_local_pool_recycle_resets() {
        let mut arena = Arena::new();
        arena.alloc_copy(b"something");
        assert!(arena.allocated() > 0);
        release_arena(arena);

        let arena2 = acquire_arena();
        assert_eq!(arena2.allocated(), 0, "recycled arena should be reset");
        release_arena(arena2);
    }

    // --- Audit: arena cannot return stale data after reset ---

    #[test]
    fn arena_reset_clears_data_positions() {
        let mut arena = Arena::new();
        let o1 = arena.alloc_copy(b"first query data");
        assert_eq!(arena.get(o1, 16), b"first query data");

        arena.reset();
        assert_eq!(arena.allocated(), 0);
        assert_eq!(arena.current, 0);
        assert_eq!(arena.offset, 0);

        // After reset, a new alloc should produce offset 0 (same as o1)
        // but the data is different. No stale data leaks.
        let o2 = arena.alloc_copy(b"second query dat");
        assert_eq!(o2, 0, "first alloc after reset should be at offset 0");
        assert_eq!(arena.get(o2, 16), b"second query dat");
    }

    #[test]
    fn arena_reset_discards_oversized_chunks() {
        let mut arena = Arena::new();
        // Allocate a 128KB blob (> SHRINK_THRESHOLD of 64KB)
        let big = vec![0xAA; 128 * 1024];
        arena.alloc_copy(&big);
        let cap_before = arena.capacity();
        assert!(cap_before >= 128 * 1024);

        arena.reset();
        let cap_after = arena.capacity();
        // Oversized chunks should be discarded — capacity should shrink
        assert!(
            cap_after < cap_before,
            "oversized chunks should be discarded on reset: before={cap_before}, after={cap_after}"
        );
    }

    // --- Audit: alloc_copy zero-length returns stable offset ---

    #[test]
    fn alloc_copy_zero_length_returns_valid_offset() {
        let mut arena = Arena::new();
        let o1 = arena.alloc_copy(b"");
        let o2 = arena.alloc_copy(b"hello");
        // Zero-length alloc should return a valid global offset
        // without advancing the bump pointer.
        assert_eq!(o1, o2, "zero-length alloc should not advance offset");
        assert_eq!(arena.get(o2, 5), b"hello");
    }

    // --- Audit: get with zero length returns empty slice ---

    #[test]
    fn get_zero_length_returns_empty() {
        let arena = Arena::new();
        assert_eq!(arena.get(0, 0), &[]);
        assert_eq!(arena.get(9999, 0), &[]);
    }

    // --- Arena::empty() edge cases ---

    // Arena::empty() is designed for query paths that use data_buf instead of arena.
    // Direct alloc on an empty arena (no chunks) panics because it indexes into
    // an empty chunks vec. Call reset() first to initialize a chunk.

    #[test]
    fn arena_empty_lazy_init_alloc_copy() {
        let mut arena = Arena::empty();
        assert_eq!(arena.chunks.len(), 0);
        // Lazy init: first alloc_copy creates the initial chunk.
        let offset = arena.alloc_copy(b"lazy");
        assert_eq!(arena.chunks.len(), 1);
        assert_eq!(arena.get(offset, 4), b"lazy");
    }

    #[test]
    fn arena_empty_lazy_init_alloc() {
        let mut arena = Arena::empty();
        assert_eq!(arena.chunks.len(), 0);
        // Lazy init: first alloc creates the initial chunk.
        let slice = arena.alloc(8);
        assert_eq!(slice.len(), 8);
        assert_eq!(arena.chunks.len(), 1);
    }

    #[test]
    fn arena_empty_reset_does_not_panic() {
        let mut arena = Arena::empty();
        // reset on empty arena should not crash
        arena.reset();
        // After reset, arena should be usable with a fresh chunk
        assert!(
            !arena.chunks.is_empty(),
            "reset should create initial chunk if empty"
        );
        assert_eq!(arena.allocated(), 0);
        assert_eq!(arena.current, 0);
        assert_eq!(arena.offset, 0);
    }

    #[test]
    fn arena_empty_reset_then_alloc() {
        let mut arena = Arena::empty();
        arena.reset();
        let offset = arena.alloc_copy(b"after reset on empty");
        assert_eq!(arena.get(offset, 20), b"after reset on empty");
    }

    #[test]
    fn arena_empty_capacity_is_zero() {
        let arena = Arena::empty();
        assert_eq!(arena.capacity(), 0);
        assert_eq!(arena.allocated(), 0);
    }

    #[test]
    fn arena_empty_reset_then_multiple_allocs() {
        let mut arena = Arena::empty();
        arena.reset(); // initialize a chunk
        let o1 = arena.alloc_copy(b"first");
        let o2 = arena.alloc_copy(b"second");
        assert_eq!(arena.get(o1, 5), b"first");
        assert_eq!(arena.get(o2, 6), b"second");
    }

    // --- alloc_copy exactly at chunk boundary ---

    #[test]
    fn alloc_copy_exactly_fills_chunk() {
        let mut arena = Arena::new();
        // Fill exactly to the 8KB boundary
        let data = vec![0xCC; INITIAL_CHUNK_SIZE];
        let offset = arena.alloc_copy(&data);
        assert_eq!(arena.chunks.len(), 1, "should still be one chunk");
        assert_eq!(arena.get(offset, INITIAL_CHUNK_SIZE)[0], 0xCC);
        assert_eq!(arena.allocated(), INITIAL_CHUNK_SIZE);

        // Allocating 0 bytes should not trigger a new chunk
        let o2 = arena.alloc_copy(b"");
        assert_eq!(arena.get(o2, 0), &[]);
    }

    // --- get with offset=0, len=0 on empty Arena ---

    #[test]
    fn arena_empty_get_zero_len() {
        // Even on Arena::empty(), get with len=0 should return empty slice
        // without panicking (no chunks to index into)
        let arena = Arena::empty();
        assert_eq!(arena.get(0, 0), &[]);
    }

    // --- Gap: get_str with unicode multi-byte ---

    #[test]
    fn get_str_unicode_multibyte() {
        let mut arena = Arena::new();
        let text = "\u{1F600}\u{00E9}";
        let offset = arena.alloc_copy(text.as_bytes());
        assert_eq!(arena.get_str(offset, text.len()), Some(text));
    }

    // --- Gap: release_arena of Arena::empty() does not panic ---

    #[test]
    fn release_arena_empty_does_not_panic() {
        let arena = Arena::empty();
        release_arena(arena); // Should not panic even though no chunks
    }

    // --- Gap: ValidatedRows blob_slice ---

    #[test]
    fn validated_rows_blob_slice_roundtrip() {
        let mut arena = Arena::new();
        let offset = arena.alloc_copy(&[0xDE, 0xAD]);
        let rows: ValidatedRows<u32> =
            ValidatedRows::new(vec![offset as u32], String::new(), arena);
        let blob = rows.blob_slice(offset as u32, 2);
        assert_eq!(blob, &[0xDE, 0xAD]);
    }

    // --- Gap: ValidatedRows deref and into_iter ---

    #[test]
    fn validated_rows_deref_and_for_loop() {
        let arena = Arena::new();
        let rows: ValidatedRows<i32> = ValidatedRows::new(vec![1, 2, 3], String::new(), arena);
        let slice: &[i32] = &rows;
        assert_eq!(slice, &[1, 2, 3]);

        let mut sum = 0;
        for &v in &rows {
            sum += v;
        }
        assert_eq!(sum, 6);
    }
}
