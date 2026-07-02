// `nope` is not a table in any migration — the inference lattice (which the
// SQLite path conforms to) rejects it before SQLite is consulted.
fn main() {
    bsql::query!(Row, "SELECT id FROM nope");
}
