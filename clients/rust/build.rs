fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit_catalog("migrations")
}
