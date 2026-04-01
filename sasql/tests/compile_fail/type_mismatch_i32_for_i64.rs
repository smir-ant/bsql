// i32 cannot be used for int2 column — no implicit widening
fn main() {
    let id = 1i32;
    let _ = sasql::query!(
        "SELECT id FROM users WHERE score = $id: i32"
    );
}
