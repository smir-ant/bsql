// BUILD-TIME FOOTPRINT PIN (tier-1 degradation). The `columns!` macro
// emits, per column, exactly this assertion:
//
//   const _: () = assert!(size_of::<$col>() == 0, "... must be a ZST");
//
// so any regression that makes a column marker field-bearing is E0080 —
// fired at const-eval for a type that need never be instantiated. This
// case reproduces the emitted pin against a field-bearing type to lock
// the diagnostic (mirrors proto's `footprint_drift` reproducing
// `wire_pin!`).

#![allow(dead_code)]

// A column marker that regressed to carry a field (no longer zero-sized).
struct Regressed {
    leaked: u32,
}

const _: () = assert!(
    ::core::mem::size_of::<Regressed>() == 0,
    "column identifier `Regressed` must be a zero-sized type"
);

fn main() {}
