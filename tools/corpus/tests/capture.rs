//! Dev-only capture harness: prints the current engine's observed result for
//! each seed fixture so the pinned `expect` values in `corpus.rs` can be baked
//! from REAL observations (never assumed). It replays each fixture as defined
//! in `corpus::seed()` through BOTH twins and prints the real `ObservedRun`
//! (and whether the twins agree), so re-baking is a copy of the printed value.
//! Run with:
//!   cargo test -p bsql-corpus --test capture -- --ignored --nocapture

#![allow(clippy::panic, reason = "dev capture harness — prints actuals; not production")]
#![allow(clippy::print_stdout, reason = "dev capture harness — reporting is its job")]

use bsql_corpus::{Adapter, SansIoAdapter, corpus};

#[test]
#[ignore = "dev capture harness"]
fn capture_all() {
    let sync = SansIoAdapter::sync();
    let async_twin = SansIoAdapter::async_twin();
    for t in corpus::seed() {
        let a = sync.run(&t);
        let b = async_twin.run(&t);
        println!("=== {} === twins_agree={}", t.name, a == b);
        println!("{a:#?}");
        if a != b {
            println!("ASYNC DIVERGES:\n{b:#?}");
        }
    }
}
