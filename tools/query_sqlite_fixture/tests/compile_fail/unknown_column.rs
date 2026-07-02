// `users` exists but has no `nope` column — rejected by the inference
// lattice the SQLite path conforms to.
fn main() {
    bsql::query!(Row, "SELECT nope FROM users");
}
