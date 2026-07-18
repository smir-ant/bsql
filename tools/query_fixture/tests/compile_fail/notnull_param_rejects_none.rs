//! Nullable-parameter inference is SCOPED to NULLABLE assignment targets. A `$N`
//! bound as a bare value into a NOT NULL column keeps the base type `T`, NOT
//! `Option<T>` — so binding `None` there is a type error, never a silent SQL
//! NULL that the NOT NULL constraint would reject at run time.
//!
//! `np_rows.id` is `INT4 PRIMARY KEY` (NOT NULL), so `$1` is `i32`; passing
//! `None` is `error[E0308]` (`None` is an `Option<i32>`, the param is `i32`).
//! (`note` / `score` ARE nullable, so their `$2` / `$3` params are `Option<..>`
//! and correctly take `Some(..)` here — only the NOT NULL `id` rejects `None`.)

use bsql_postgres_sync::Connection;

bsql::query!(
    NpNn,
    "INSERT INTO np_rows (id, note, score) VALUES ($1, $2, $3) RETURNING id"
);

fn probe(c: &mut Connection) {
    // `None` into the NOT NULL `id` param (`i32`): E0308.
    let _ = c.execute::<NpNn>((None, Some("x"), Some(1_i32)));
}

fn main() {}
