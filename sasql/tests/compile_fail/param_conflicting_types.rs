fn main() {
    let x = 1i32;
    let _ = sasql::query!(
        "SELECT id FROM users WHERE id = $x: i32 AND login = $x: &str"
    );
}
