fn main() {
    let _ = bsql::query!(
        "SELECT id FROM users WHERE id = $1"
    );
}
