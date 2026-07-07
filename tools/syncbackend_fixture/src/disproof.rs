//! The DISPROOF record: what does NOT unify naively, recorded as REAL code with
//! the VERBATIM compiler error it produces (captured on the pinned toolchain).
//! The whole module is gated `#[cfg(any())]` — it is never compiled (so the
//! crate builds), but the code is preserved verbatim for inspection. Un-gate the
//! function to reproduce its error.
//!
//! This is the reason `RunsOn<B>` keys `Owned` (and `Params`) on the backend
//! rather than naming them off a bare two-trait bound.
//!
//! # A note that was here, now RESOLVED — the parameterized param-wall
//!
//! An earlier probe recorded a SECOND disproof: that a PARAMETERIZED query did
//! not unify across the backends, because PG's typed verb took the compile-
//! checked `Q::Params` tuple while SQLite's took an untyped `&[ValueRef]` slice —
//! two incompatible param vocabularies with no bridge, so `sqlite.query::<Q>(pg_params)`
//! was a hard `E0308`. That wall is **GONE**: the SQLite typed verbs now take the
//! SAME typed `Q::Params` tuple (the SQLite `$N` param-bridge), so ONE
//! parameterized signature serves both backends. The former disproof is therefore
//! deleted, and its replacement is a PASSING witness — the real, executing
//! `super::user_by_id<B>` (`RunsOn<B, Params = (i64,), ..>`), which runs on SQLite
//! in the offline tests and on live PostgreSQL in `tests/live_pg.rs`. The record
//! stays honest: no wall is claimed that no longer exists.

#![cfg(any())]

use bsql::sqlite;
use bsql::{RunsOn, SyncBackend, SyncQueries};

// ── Disproof A — why `RunsOn<B>` keys `Owned`/`Params` on the backend ────────
//
// The naive unification bounds a method `Q: TypedQuery + SqliteTypedQuery` and
// names `Q::Owned`. Because a carrier implements BOTH traits and each declares
// its own `Owned` associated type, `Q::Owned` is AMBIGUOUS — even though the
// macro makes both the SAME concrete `FooOwned`, the trait solver does not know
// that. Verbatim:
//
//   error[E0221]: ambiguous associated type `Owned` in bounds of `Q`
//    --> tools/syncbackend_fixture/src/disproof.rs
//     | pub fn ambiguous_owned<Q: bsql::TypedQuery + sqlite::SqliteTypedQuery>(x: Q::Owned) -> Q::Owned {
//     |                                                                           ^^^^^^^^ ambiguous associated type `Owned`
//     = note: associated type `Owned` could derive from `SqliteTypedQuery`
//     = note: associated type `Owned` could derive from `TypedQuery`
//
// The fix a consumer would be forced to write EVERY time is a fully-qualified
// `<Q as TypedQuery>::Owned` (or an `Owned = ...` equality bound) — which is
// exactly the noise `RunsOn<B>::Owned` dissolves in `lib.rs`.
pub fn ambiguous_owned<Q: bsql::TypedQuery + sqlite::SqliteTypedQuery>(x: Q::Owned) -> Q::Owned {
    x
}

// ── Disproof B — the tx-guard TYPED-fetch scope limit ───────────────────────
//
// The `transaction` combinator ships and the guard `B::Tx<'t>` genuinely
// implements `SyncQueries`, so generic RAW-SQL grouping in a transaction is
// clean (`wipe_in_tx` in `lib.rs`). But running a generic TYPED `fetch_*` on the
// guard needs a higher-ranked bound over the guard's lifetime:
//
//   for<'t> UserRowQuery: RunsOn<B::Tx<'t>, Params = (), Owned = UserRowOwned>
//
// This bound is provable when ASSUMED — the generic function DEFINITION below
// type-checks. But at a CALL site the trait solver cannot INFER `B` through the
// higher-ranked bound, so it fails to discharge it. Verbatim (calling
// `load_users_in_tx` with a concrete `sqlite::Connection`, no turbofish):
//
//   error[E0277]: the trait bound `for<'t> UserRowQuery: RunsOn<<_ as SyncBackend>::Tx<'t>>` is not satisfied
//    --> tools/syncbackend_fixture/src/disproof.rs
//     |     let _ = load_users_in_tx(&mut conn);
//     |             ---------------- ^^^^^^^^^ the trait `for<'t> RunsOn<<_ as SyncBackend>::Tx<'t>>` is not implemented for `UserRowQuery`
//     |             |
//     |             required by a bound introduced by this call
//     |
//   note: this is a known limitation of the trait solver that will be lifted in the future
//     |     add turbofish arguments to this call to specify the types manually, even if it's redundant
//
// A turbofish (`load_users_in_tx::<sqlite::Connection>(&mut conn)`) DOES compile
// and run, but forcing the concrete backend at every call — on top of the HRTB
// bound — is exactly the higher-ranked "soup" that is off the flagship's clean
// `B: SyncBackend + Q: RunsOn<B, ..>` shape. So a generic typed fetch on the tx
// guard is NOT offered; typed reads stay at connection level and the transaction
// carries the raw-SQL grouping. This is the tx-guard's honest scope limit.

// The DEFINITION type-checks (the bound is assumed here).
pub fn load_users_in_tx<B>(conn: &mut B) -> Result<Vec<super::UserRowOwned>, B::Error>
where
    B: SyncBackend,
    for<'t> super::UserRowQuery: RunsOn<B::Tx<'t>, Params = (), Owned = super::UserRowOwned>,
{
    conn.transaction(|tx| tx.fetch_all::<super::UserRowQuery>(()))
}

// The CALL is where it fails (E0277 above) — the solver cannot infer `B`.
pub fn force_the_e0277(conn: &mut sqlite::Connection) -> Result<Vec<super::UserRowOwned>, sqlite::SqliteError> {
    load_users_in_tx(conn)
}
