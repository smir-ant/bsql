// A dynamic OPTIONAL(...) toggle filter whose enabled form (`$1 IS NULL OR
// name = $1`) forces a full-table scan in SQLite (the OR defeats every
// index). It is NOT acknowledged, so the conformance cross-check makes it a
// loud compile error. Adding a `/* bsql:allow-scan: <reason> */` marker
// (see the compile_pass case) would accept it.
fn main() {
    bsql::query!(Row, "SELECT id FROM users WHERE OPTIONAL(name = $1)");
}
