fn main() {
    let id = 1i32;
    let _ = sasql::query!(
        "SELECT id FROM users WHERE id = $id"
    );
}
