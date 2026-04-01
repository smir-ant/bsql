fn main() {
    let _ = bsql::query!("SELECT 1; DROP TABLE users");
}
