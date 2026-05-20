//! Probe **P-D280E-1** — `Sensitive::get` is method-absent.
//!
//! `Sensitive<T>` exposes `pub fn with_inner<R>(&self, f: impl
//! FnOnce(&T) -> R) -> R` only (tier-1 by-construction —
//! HRTB-scoped borrow cannot escape). A naive `pub const fn
//! get(&self) -> &T` shape would push the no-retention contract
//! onto a docstring discipline («the borrow is intentionally short-
//! lived — the caller must not store the reference beyond the
//! immediate computation», tier-2 by-discipline).
//!
//! This probe pins the absence: calling `.get()` on a `Sensitive`
//! must fail with E0599 (method-absent). External callers MUST go
//! through `.with_inner(|inner| ...)`.

extern crate bsql_pg_proto;

use bsql_pg_proto::Sensitive;

fn main() {
    let s: Sensitive<i32> = Sensitive::new(42);
    // .get() does not exist.
    let _ = s.get();
}
