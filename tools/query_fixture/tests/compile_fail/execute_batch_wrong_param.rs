//! `execute_batch` element-type probe — feeding a batch whose parameter tuples do
//! NOT match the carrier's compile-time `Params` is a type error at the call, never
//! a value that reaches the wire.
//!
//! `execute_batch<'p, Q, I>(params: I)` binds `I: IntoIterator<Item = Q::Params<'p>>`.
//! The `query!` below updates by the `int8` PK with an `int8` increment, so
//! `Q::Params = (i64, i64)`; passing a `Vec<(&str, i64)>` is `error[E0271]` (the
//! iterator's `Item` is nominally distinct from the carrier's tuple). So a bulk write
//! batch cannot carry a mistyped parameter set — the typed guarantee holds at the
//! batch boundary exactly as it does for a single `execute`.

use bsql_postgres_sync::Connection;

bsql::query!(
    EbMismatch,
    "UPDATE accounts SET balance = $2::int8 WHERE id = $1 RETURNING id"
);

fn probe(c: &mut Connection) {
    // Feed `(&str, i64)` to a carrier expecting `(i64, i64)`: E0271.
    let _ = c.execute_batch::<EbMismatchQuery, _>(vec![("hostile", 1_i64)]);
}

fn main() {}
