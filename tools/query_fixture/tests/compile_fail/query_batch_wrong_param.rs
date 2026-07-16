//! `query_batch` element-type probe — feeding a batch whose parameter tuples do NOT
//! match the carrier's compile-time `Params` is a type error at the call, never a
//! value that reaches the wire.
//!
//! `query_batch<'p, Q, I>(params: I)` binds `I: IntoIterator<Item = Q::Params<'p>>`.
//! The `query!` below selects by the `int8` PK, so `Q::Params = (i64,)`; passing a
//! `Vec<(&str,)>` is `error[E0271]` (the iterator's `Item` is nominally distinct from
//! the carrier's tuple). So a bulk QUERY batch cannot carry a mistyped parameter set —
//! the typed guarantee holds at the batch boundary exactly as it does for a single
//! `query` (and for the `execute_batch` twin).

use bsql_postgres_sync::Connection;

bsql::query!(QbMismatch, "SELECT id FROM accounts WHERE id = $1");

fn probe(c: &mut Connection) {
    // Feed `(&str,)` to a carrier expecting `(i64,)`: E0271.
    let _ = c.query_batch::<QbMismatch, _>(vec![("hostile",)]);
}

fn main() {}
