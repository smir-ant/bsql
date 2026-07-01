//! Seal probe — reading a `pub(crate)` field of a `PreparedQuery` from
//! outside the crate is `error[E0616]`. A caller cannot project the raw SQL
//! to splice it into a fresh query string. The `sql()` accessor exists for
//! diagnostics (it returns the already-validated `&'static str`), but the
//! field itself is private. The query is built by the compile-checked
//! `query!` macro — the seal holds for the sanctioned builder's output.

bsql_query_macros::query!(SealFieldRead, "SELECT id FROM users");

fn main() {
    let _hostile: &str = SealFieldReadQuery::PREPARED.sql;
    let _ = _hostile;
}
