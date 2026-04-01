// i32 cannot be used for text column
fn main() {
    let login = 42i32;
    let _ = sasql::query!(
        "SELECT id FROM users WHERE login = $login: i32"
    );
}
