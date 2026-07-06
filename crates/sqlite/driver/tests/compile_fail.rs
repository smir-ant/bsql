//! Compile-fail wall for the streaming read path.
//!
//! `query_each` lends each row through a `for<'r>` callback so a borrowed value
//! cannot outlive the row step. The `ui/query_each_escape.rs` fixture tries to
//! stash such a borrow in an outer `Vec`; the committed `.stderr` golden pins
//! the exact borrow-checker rejection, so any regression that lets a streamed
//! borrow escape is a loud failure.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "trybuild harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

#[test]
fn escape_is_rejected() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/query_each_escape.rs");
}

/// The transaction guard exposes only data verbs: a nested `transaction` or any
/// manual lifecycle verb (`begin`/`commit`/`rollback`/`close`) on the handed
/// `&Transaction` is an E0599, pinned by the committed `.stderr` golden — so the
/// runtime "transaction within a transaction" is a compile error instead.
#[test]
fn transaction_guard_rejects_lifecycle_verbs() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/tx_guard_nested.rs");
}
