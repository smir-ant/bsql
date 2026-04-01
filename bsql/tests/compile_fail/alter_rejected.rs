fn main() {
    let _ = bsql::query!("ALTER TABLE users ADD COLUMN x INT");
}
