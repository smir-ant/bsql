// CLOSED COLUMN-TYPE SET (via `columns!`): a column declared with an
// unsupported value type is E0277 — `ColType` is not satisfied for
// `f64`/`u64`. The column-type set is exactly {i16, i32, i64, u32, bool,
// Text}.

bsql_postgres_core::columns! {
    bad => [ amount: f64, big: u64 ]
}

fn main() {}
