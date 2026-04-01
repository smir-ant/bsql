fn main() {
    let _ = sasql::query!("SELECT 1; DROP TABLE users");
}
