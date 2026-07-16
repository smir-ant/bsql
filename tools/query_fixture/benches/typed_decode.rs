//! Criterion ns/op bench for the `query!` carrier's typed row decode.
//!
//! This lives in the fixture crate (not the proto crate's `hot_paths`) because
//! the decode being measured is EMITTED by the `query!` macro against this
//! crate's build catalog: an all-fixed row (`int8, int8`) decodes via a
//! CONST-OFFSET fast path — one bounds check, primitives read at fixed offsets —
//! while a row with a variable-width `text` column decodes per cell. The two
//! groups make that contrast measurable:
//!
//! - `all_fixed_const_offset` — `OrderKey::decode` over an `(int8, int8)` row.
//!   The vectorized const-offset fast path; the `query_alloc` test already pins
//!   it at zero allocations, this pins its ns.
//! - `per_cell_borrowed` — `UserNames::decode` over an `(int8, text)` row. The
//!   per-cell path (borrows the `text` as `&str`), still zero-alloc.
//! - `per_cell_owned` — `UserNames::decode` over the same row. Copies the
//!   `text` cell into a `String`; the deliberate allocating contrast.
//!
//! Post-LTO codegen is inspected with the `asm-linked-diff` tooling on the
//! `bench` branch (`PKG=bsql-query-fixture … typed_decode`).

#![allow(
    missing_docs,
    reason = "bench harness — criterion's macro-generated wrappers don't take doc comments uniformly; the module docstring and descriptive bench-fn names cover intent"
)]
#![allow(
    clippy::expect_used,
    reason = "bench harness — expect() is the loud fixture-failure signal; a bench is never a #[test] context, so the floor's allow-in-tests carve-out cannot reach it"
)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

// Same shapes as the decode/alloc fixtures. `orders.id` / `orders.user_id` are
// both `int8 NOT NULL` (the all-fixed row); `users.id` / `users.email` are
// `int8`/`text NOT NULL` (the per-cell row). Both validate against the build
// catalog replayed from this crate's migrations.
bsql::query!(OrderKey, "SELECT id, user_id FROM orders");
bsql::query!(UserNames, "SELECT id, email FROM users");

const ORDER_KEY_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 42, // id = 42
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 7, // user_id = 7
];

const USER_NAMES_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 1, // id = 1
    0x00, 0x00, 0x00, 0x0e, // email length = 14
    b'a', b'l', b'i', b'c', b'e', b'@', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'!',
];

fn bench_all_fixed(c: &mut Criterion) {
    c.bench_function("decode/all_fixed_const_offset", |b| {
        b.iter(|| {
            let row = OrderKey::decode(black_box(ORDER_KEY_ROW)).expect("well-formed row");
            black_box(row)
        });
    });
}

fn bench_per_cell_borrowed(c: &mut Criterion) {
    c.bench_function("decode/per_cell_borrowed", |b| {
        b.iter(|| {
            let row = UserNames::decode(black_box(USER_NAMES_ROW)).expect("well-formed row");
            black_box(row)
        });
    });
}

fn bench_per_cell_owned(c: &mut Criterion) {
    c.bench_function("decode/per_cell_owned", |b| {
        b.iter(|| {
            let row = UserNames::decode(black_box(USER_NAMES_ROW)).expect("well-formed row");
            black_box(row)
        });
    });
}

criterion_group!(
    typed_decode,
    bench_all_fixed,
    bench_per_cell_borrowed,
    bench_per_cell_owned
);
criterion_main!(typed_decode);
