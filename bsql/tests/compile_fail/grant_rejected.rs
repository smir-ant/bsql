fn main() {
    let _ = bsql::query!("GRANT SELECT ON users TO public");
}
