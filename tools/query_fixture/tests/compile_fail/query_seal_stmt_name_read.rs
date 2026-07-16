//! Seal probe — reading the `pub(crate)` `stmt_name` field from outside the
//! crate is `error[E0616]`. The `stmt_name()` accessor exposes the
//! content-addressed name for legitimate diagnostics; the raw field stays
//! private so a caller cannot harvest it to splice a hostile parallel query.
//! Built by `query!`, whose statement name is the SHA-256-96 content address
//! of the SQL (`bsql_q_<24 hex>`); its length is pinned by the `query!` wire
//! tests.

bsql::query!(SealStmtNameRead, "SELECT id FROM users");

fn main() {
    let _hostile: &str = SealStmtNameRead::PREPARED.stmt_name;
    let _ = _hostile;
}
