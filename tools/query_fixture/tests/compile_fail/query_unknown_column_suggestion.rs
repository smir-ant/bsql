// `users` has an `email` column but no `emial` — a classic transposition
// typo. The inference engine's "no such column" error is enriched with the
// nearest known column name (a restricted Damerau-Levenshtein match, which
// counts the adjacent transposition as ONE edit): "did you mean `email`?".
fn main() {
    bsql::query!(Row, "SELECT emial FROM users");
}
