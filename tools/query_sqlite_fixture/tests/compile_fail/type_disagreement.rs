// `widgets.thing_id` is a PostgreSQL `oid` (Rust `u32`): the inference
// lattice types it fine, but SQLite has no `oid` equivalent. The SQLite
// conformance cross-check turns that leaf-map divergence into a loud
// compile error — the query is not portable to SQLite.
fn main() {
    bsql::query!(Row, "SELECT thing_id FROM widgets");
}
