//! Allocation proof for `query!`'s borrowed decode path.
//!
//! Installs the workspace counting allocator as this test binary's
//! `#[global_allocator]` and brackets each decode with snapshots. The
//! borrowed record's `decode` must allocate NOTHING: an all-fixed row
//! reads primitives at const offsets, and a `text` cell borrows the input
//! bytes as `&str`. The owned twin's `decode`, by contrast, copies each
//! `text` cell into a `String`, so it DOES allocate — the contrast proves
//! the borrowed path's zero-copy claim is real, not incidental.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global: it counts allocations on
//! EVERY thread. `cargo test` runs a binary's `#[test]` fns in parallel,
//! so two test fns each bracketing a window would have one window count
//! the other's allocations. All measurements therefore live in a SINGLE
//! `#[test]` fn, run sequentially, so no concurrent test thread can
//! allocate inside a measured window. (Other test BINARIES are separate
//! processes with their own allocator instance, so they cannot interfere.)

use std::hint::black_box;

use bsql_devgates::CountingAllocator;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// Same shapes as the decode fixtures: an all-fixed-width row and a
// NOT-NULL text row.
bsql_query_macros::query!(OrderKey, "SELECT id, user_id FROM orders");
bsql_query_macros::query!(UserNames, "SELECT id, email FROM users");

const ORDER_KEY_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 42, // id = 42
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 7, // user_id = 7
];

const USER_NAMES_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 1, // id = 1
    0x00, 0x00, 0x00, 0x06, b'a', b'@', b'b', b'.', b'c', b'o', // email
];

#[test]
fn borrowed_decode_is_zero_alloc_owned_text_allocates() {
    // (1) Borrowed all-fixed decode (vectorized fast path) — zero allocs.
    let before = ALLOC.snapshot();
    let fixed = OrderKey::decode(black_box(ORDER_KEY_ROW));
    let after = ALLOC.snapshot();
    black_box(&fixed);
    let fixed_allocs = after.delta(before).allocs;

    // (2) Borrowed text decode — borrows the bytes as `&str`, zero allocs.
    let before = ALLOC.snapshot();
    let borrowed_text = UserNames::decode(black_box(USER_NAMES_ROW));
    let after = ALLOC.snapshot();
    black_box(&borrowed_text);
    let borrowed_text_allocs = after.delta(before).allocs;

    // (3) Owned text decode — copies the `text` cell into a `String`, so
    // it must allocate. The deliberate contrast with (2).
    let before = ALLOC.snapshot();
    let owned_text = UserNamesOwned::decode(black_box(USER_NAMES_ROW));
    let after = ALLOC.snapshot();
    black_box(&owned_text);
    let owned_text_allocs = after.delta(before).allocs;

    // (4) The const wire artifact lives entirely in `.rodata`: reading the
    // pre-baked Parse template, Bind prefix, and OID lists off
    // `<Name>Query::PREPARED` borrows `&'static` slices and allocates
    // nothing. (The `const` is materialised at compile time.)
    let before = ALLOC.snapshot();
    let q = black_box(OrderKeyQuery::PREPARED);
    let wire_len = q.parse_template_for_test().len()
        + q.bind_execute_prefix_for_test().len()
        + q.param_oids().len()
        + q.row_oids().len();
    let after = ALLOC.snapshot();
    black_box(wire_len);
    let wire_allocs = after.delta(before).allocs;

    assert_eq!(
        fixed_allocs, 0,
        "borrowed all-fixed decode must not allocate (got {fixed_allocs})"
    );
    assert_eq!(
        borrowed_text_allocs, 0,
        "borrowed text decode must borrow, not allocate (got {borrowed_text_allocs})"
    );
    assert!(
        owned_text_allocs >= 1,
        "owned text decode is expected to allocate the String (got {owned_text_allocs})"
    );
    assert_eq!(
        wire_allocs, 0,
        "reading the const .rodata wire artifact must not allocate (got {wire_allocs})"
    );
}
