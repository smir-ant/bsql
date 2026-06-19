//! Dev-only measurement and verification harness for the bsql workspace.
//!
//! This crate is the **single sanctioned home for `unsafe`** in the
//! whole workspace. It hosts two dev-only building blocks that
//! inherently require `unsafe`:
//!
//! - [`CountingAllocator`] — a `#[global_allocator]`-installable wrapper
//!   around the platform allocator that counts `alloc` / `dealloc` /
//!   bytes. The allocation-traffic bench installs it to report
//!   deterministic per-scenario alloc counts.
//! - [`probe_bytes`] — a raw-pointer read of `len` bytes at `ptr`,
//!   used by the zeroize-verification tests to observe the bytes left
//!   in a stack slot after a secret-bearing value has been dropped.
//!
//! # Why this crate exists
//!
//! Every SHIPPED crate carries `#![forbid(unsafe_code)]` at its own crate
//! root, so all production code is unsafe-free. Two populations of
//! dev-only `unsafe` live outside production: the alloc bench's counting
//! allocator and the zeroize tests' post-drop memory probe. Consolidating
//! both here — into a `publish = false` member that does **not** set
//! `lints.workspace = true` and is therefore exempt from the workspace
//! lint floor — gives the workspace a single audited `unsafe` location:
//! the alloc bench becomes unsafe-free, and the five zeroize tests share
//! one reviewed [`probe_bytes`] instead of five identical copies.
//!
//! (`unsafe_code` is itself kept OUT of the workspace forbid floor: the
//! zeroize tests' probe is `unsafe` at the call site — it dereferences a
//! caller-held raw pointer into dropped storage, which has no sound safe
//! wrapper — so those test crates keep `#![allow(unsafe_code)]`, which a
//! `forbid` floor would reject with `E0453`. See the root `Cargo.toml`
//! `[workspace.lints]` comment.)
//!
//! This crate is `publish = false` and never linked into a shipped
//! artifact: benches and tests depend on it as a dev-dependency only.

#![forbid(clippy::unwrap_used, clippy::expect_used)]
#![warn(missing_docs)]
// `unsafe` is permitted here ON PURPOSE — this is the workspace's
// quarantine for the two dev-only unsafe building blocks. It is NOT
// `forbid(unsafe_code)`, because that is exactly the lint this crate
// is built to absorb on behalf of the rest of the workspace.

extern crate alloc;

use alloc::vec::Vec;
use core::alloc::{GlobalAlloc, Layout};
use core::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::System;

/// A snapshot of the [`CountingAllocator`] counters at one instant.
#[derive(Copy, Clone, Debug)]
pub struct AllocSnapshot {
    /// Cumulative number of `alloc` / `alloc_zeroed` / `realloc` calls.
    pub allocs: usize,
    /// Cumulative number of `dealloc` / `realloc` calls.
    pub deallocs: usize,
    /// Cumulative bytes requested across all allocating calls.
    pub bytes_allocated: usize,
}

impl AllocSnapshot {
    /// The per-scenario delta between this snapshot and an earlier one.
    ///
    /// Subtraction saturates at zero: a snapshot is always taken after
    /// `prior`, so the monotonic counters never decrease, and the
    /// saturating floor only guards against a caller passing the
    /// arguments in the wrong order (it never masks real data).
    #[must_use]
    pub fn delta(self, prior: Self) -> AllocDelta {
        AllocDelta {
            allocs: self.allocs.saturating_sub(prior.allocs),
            deallocs: self.deallocs.saturating_sub(prior.deallocs),
            bytes: self.bytes_allocated.saturating_sub(prior.bytes_allocated),
        }
    }
}

/// The difference between two [`AllocSnapshot`]s — the allocation cost
/// attributable to the code run between them.
#[derive(Copy, Clone, Debug)]
pub struct AllocDelta {
    /// Allocating calls in the measured window.
    pub allocs: usize,
    /// Freeing calls in the measured window.
    pub deallocs: usize,
    /// Bytes requested in the measured window.
    pub bytes: usize,
}

/// A `#[global_allocator]`-installable wrapper around the platform
/// `System` allocator that counts allocation traffic.
///
/// Install with `#[global_allocator] static A: CountingAllocator =
/// CountingAllocator::new();` in a bench target, then bracket a
/// scenario with [`CountingAllocator::snapshot`] before and after to
/// read its [`AllocDelta`].
///
/// `Relaxed` ordering is used throughout: the counters are independent
/// side effects whose only requirement is mutual atomicity, never an
/// ordering relationship with the forwarded `System` call.
#[derive(Debug)]
pub struct CountingAllocator {
    inner: System,
    allocs: AtomicUsize,
    deallocs: AtomicUsize,
    bytes_allocated: AtomicUsize,
}

impl CountingAllocator {
    /// Construct a zeroed counter. `const` so it can initialise a
    /// `static` for `#[global_allocator]`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: System,
            allocs: AtomicUsize::new(0),
            deallocs: AtomicUsize::new(0),
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    /// Read the current counter values.
    #[must_use]
    pub fn snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            allocs: self.allocs.load(Ordering::Relaxed),
            deallocs: self.deallocs.load(Ordering::Relaxed),
            bytes_allocated: self.bytes_allocated.load(Ordering::Relaxed),
        }
    }
}

impl Default for CountingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: `CountingAllocator` forwards every alloc / dealloc / realloc
// call to `System` unchanged. The atomic counters are a pure side
// effect (Relaxed loads/stores cannot reorder w.r.t. the System call
// in any way that affects allocator semantics). The only requirement
// on a `GlobalAlloc` impl is that alloc/dealloc honor the `Layout` —
// that obligation is delegated entirely to `System`.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: `layout` is forwarded unchanged from the caller, who
        // is responsible for its validity per the `GlobalAlloc` contract.
        unsafe { self.inner.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.deallocs.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `ptr` + `layout` pair forwarded unchanged from caller.
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: same as `alloc` — `Layout` forwarded unchanged.
        unsafe { self.inner.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A realloc counts as one alloc + one dealloc at the allocator
        // API; bytes only grow by the delta when the block expands.
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.deallocs.fetch_add(1, Ordering::Relaxed);
        if new_size > layout.size() {
            self.bytes_allocated
                .fetch_add(new_size - layout.size(), Ordering::Relaxed);
        }
        // SAFETY: `ptr` + `layout` + `new_size` triple forwarded unchanged.
        unsafe { self.inner.realloc(ptr, layout, new_size) }
    }
}

/// Read `len` bytes starting at `ptr` into an owned `Vec`.
///
/// The zeroize-verification tests use this to observe the bytes left in
/// a stack slot after a secret-bearing value has been dropped, proving
/// the slot was actually overwritten with zeros (rather than merely
/// trusting `Drop` ran). The read is purely observational — no memory
/// is mutated.
///
/// Returns a fresh `Vec`; the caller compares it against an
/// all-zero expectation.
///
/// # Safety
///
/// The caller must guarantee, for the whole read:
/// - `ptr` is valid for reads of `len` consecutive bytes, i.e. it
///   points into a single allocated object (here, a live stack frame)
///   that has not been freed or had its slot reused.
/// - The `len` bytes are within that object's bounds.
/// - No concurrent mutation of those bytes occurs during the read.
///
/// Reading bytes whose logical owner has been dropped is permitted: the
/// drop runs the destructor but does not invalidate the stack storage,
/// which is exactly the post-drop residue the tests want to inspect.
#[must_use]
pub unsafe fn probe_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        // SAFETY: by the function contract, `ptr.add(i)` is in bounds of
        // a single live allocation for every `i < len`, and reading one
        // byte from it is valid and free of concurrent mutation.
        let byte = unsafe { ptr.add(i).read() };
        out.push(byte);
    }
    out
}
