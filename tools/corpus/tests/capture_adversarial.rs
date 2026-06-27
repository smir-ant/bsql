//! Dev-only capture for the 3 mandatory adversarial fixtures. Replays each
//! fixture as defined in `corpus::adversarial()` through both twins and prints
//! the real `ObservedRun` for re-baking. Run with:
//!   cargo test -p bsql-corpus --test capture_adversarial -- --ignored --nocapture

#![allow(clippy::panic, reason = "dev capture harness — prints actuals; not production")]
#![allow(clippy::print_stdout, reason = "dev capture harness — reporting is its job")]

use bsql_corpus::{Adapter, SansIoAdapter, corpus};

#[test]
#[ignore = "dev capture harness"]
fn capture_adversarial() {
    let sync = SansIoAdapter::sync();
    let async_twin = SansIoAdapter::async_twin();
    for t in corpus::adversarial() {
        let a = sync.run(&t);
        let b = async_twin.run(&t);
        println!("=== {} === twins_agree={}", t.name, a == b);
        println!("{a:#?}");
        if a != b {
            println!("ASYNC DIVERGES:\n{b:#?}");
        }
    }
}
