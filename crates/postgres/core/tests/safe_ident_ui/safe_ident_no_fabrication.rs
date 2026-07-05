// EXPECT: the tuple field of `SafeIdent` is private, so it cannot be initialized
// from outside the crate — a `SafeIdent` cannot be fabricated bypassing
// `SafeIdent::validate`, its SOLE (validating) constructor. This is what makes
// "the type is the proof": every `SafeIdent` in existence passed the injection
// check.
use bsql_postgres_core::SafeIdent;

fn main() {
    // Direct tuple construction touches the private field — inaccessible here.
    let _ = SafeIdent("events; DROP TABLE users --");
}
