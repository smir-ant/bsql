// A migration defines a `ledger` table but not `ledgar` — a one-key typo.
// The "unknown table or alias" error is enriched with the nearest known
// table name from the catalog: "did you mean `ledger`?".
fn main() {
    bsql::query!(Row, "SELECT id FROM ledgar");
}
