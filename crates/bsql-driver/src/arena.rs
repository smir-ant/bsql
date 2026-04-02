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

/// Initial chunk size: 8KB covers most result sets.
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
/// use bsql_driver::Arena;
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
        if new_len > chunk.len() {
            chunk.resize(new_len, 0);
        }

        self.offset = new_len;
        &mut chunk[start..new_len]
    }

    /// Copy `data` into the arena and return the global offset.
    ///
    /// The offset can be used with `get()` to retrieve the data later.
    pub fn alloc_copy(&mut self, data: &[u8]) -> usize {
        if data.is_empty() {
            return self.global_offset();
        }

        self.ensure_capacity(data.len());

        let chunk = &mut self.chunks[self.current];
        let start = self.offset;
        let new_len = start + data.len();

        if new_len > chunk.len() {
            chunk.resize(new_len, 0);
        }
        chunk[start..new_len].copy_from_slice(data);

        let global = self.global_offset_at(self.current, start);
        self.offset = new_len;
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
    pub fn get_str(&self, global_offset: usize, len: usize) -> Option<&str> {
        if len == 0 {
            return Some("");
        }
        std::str::from_utf8(self.get(global_offset, len)).ok()
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
        let mut total = 0;
        for (i, chunk) in self.chunks.iter().enumerate() {
            if i < self.current {
                total += chunk.len();
            } else if i == self.current {
                total += self.offset;
            }
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
    fn global_offset(&self) -> usize {
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
        // Binary search: find the last chunk whose prefix_sum <= global_offset
        let idx = match self.prefix_sums.binary_search(&global_offset) {
            Ok(i) => i,
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
    fn default() -> Self {
        Self::new()
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
/// use bsql_driver::arena::{acquire_arena, release_arena};
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
}
