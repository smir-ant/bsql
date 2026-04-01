fn main() {
    let id = "not_an_int";
    let _ = bsql::query!(
        "SELECT id FROM users WHERE id = $id: &str"
    );
}
