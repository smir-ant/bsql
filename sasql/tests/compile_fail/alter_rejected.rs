fn main() {
    let _ = sasql::query!("ALTER TABLE users ADD COLUMN x INT");
}
