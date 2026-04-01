// i64 cannot be used for int4 column — no implicit widening
fn main() {
    let id = 1i64;
    let _ = bsql::query!(
        "SELECT id FROM users WHERE id = $id: i64"
    );
}
