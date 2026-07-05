// EXPECT: the tuple field of `SafeTable` is private, so it cannot be initialized
// from outside the crate — a `SafeTable` cannot be fabricated bypassing
// `SafeTable::validate`, its SOLE (validating) constructor. The COPY-in /
// COPY-out table splice therefore cannot be handed an unvalidated table name.
use bsql_postgres_core::SafeTable;

fn main() {
    // Direct tuple construction touches the private field — inaccessible here.
    let _ = SafeTable("users; DROP TABLE users --");
}
