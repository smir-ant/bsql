// &str cannot be used for int4 column
fn main() {
    let name = "alice";
    let _ = sasql::query!(
        "SELECT id FROM users WHERE id = $name: &str"
    );
}
