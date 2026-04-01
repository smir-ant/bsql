fn main() {
    let _ = sasql::query!("GRANT SELECT ON users TO public");
}
