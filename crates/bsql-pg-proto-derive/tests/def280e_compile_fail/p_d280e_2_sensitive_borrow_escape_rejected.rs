//! DEF-280 Bundle E Phase 1 probe **P-D280E-2** — the HRTB-scoped
//! `&T` borrow lent into `Sensitive::with_inner`'s closure cannot
//! escape past the call.
//!
//! This is the **retention-impossibility guarantee** that Bundle E
//! lands. Pre-Bundle E `Sensitive::get(&self) -> &T` returned a
//! borrow tied to `&self`'s lifetime — Rust's borrow checker
//! prevented use-after-Drop but the borrow itself could propagate
//! through arbitrary value-shapes (struct fields, async future
//! captures, etc.) up to `Sensitive`'s scope-exit. The docstring
//! asked callers «not to store the reference beyond the immediate
//! computation» (tier-2 by-discipline).
//!
//! Post-Bundle E `with_inner<R>(&self, f: impl FnOnce(&T) -> R)
//! -> R` desugars to `for<'a> FnOnce(&'a T) -> R` (HRTB-quantified
//! `'a`). `R` is a single concrete type that does NOT depend on
//! `'a`. So if the closure tries to return the inner borrow itself
//! (`R = &'a T`), `R` must encompass `&'a` for ALL `'a` — the only
//! such type is `&'static T`, but the closure body's `inner` does
//! not have `'static` lifetime. Type error.
//!
//! This probe attempts the retention attack and pins that rustc
//! rejects it.

extern crate bsql_pg_proto;

use bsql_pg_proto::Sensitive;

fn main() {
    let s: Sensitive<i32> = Sensitive::new(42);
    // Try to retain the inner borrow past the closure scope by
    // having the closure return it. The HRTB-quantified `'a`
    // (lifetime of `&'a T`) is universal; `R` in `with_inner<R>`
    // is concrete; so the closure cannot return `&'a T` (would
    // require `'a: 'static` which the HRTB does not satisfy).
    let _leaked: &i32 = s.with_inner(|inner: &i32| inner);
}
