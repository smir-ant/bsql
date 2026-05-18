//! DEF-280 Bundle E Phase 1 probe **P-D280E-1** — `Sensitive::get`
//! is method-absent post-Bundle-E.
//!
//! Pre-Bundle E `Sensitive<T>` had `pub const fn get(&self) -> &T`
//! with a docstring saying «the borrow is intentionally short-lived
//! — the caller must not store the reference beyond the immediate
//! computation» (tier-2 by-discipline). Bundle E migrated this to
//! `pub fn with_inner<R>(&self, f: impl FnOnce(&T) -> R) -> R`
//! (tier-1 by-construction — HRTB-scoped borrow cannot escape).
//!
//! This probe pins the migration: calling `.get()` on a Sensitive
//! must fail with E0599 (method-absent). External callers MUST go
//! through `.with_inner(|inner| ...)`.

extern crate bsql_pg_proto;

use bsql_pg_proto::Sensitive;

fn main() {
    let s: Sensitive<i32> = Sensitive::new(42);
    // .get() does NOT exist post-Bundle E.
    let _ = s.get();
}
