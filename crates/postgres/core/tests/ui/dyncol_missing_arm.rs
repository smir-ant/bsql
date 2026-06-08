// COLUMN-COMPLETENESS AT THE MATCH SITE. `DynCol` is deliberately NOT
// `#[non_exhaustive]`, so a downstream `match` that forgets a column is
// E0004 (non-exhaustive patterns). Adding a column is a breaking change —
// the correct tradeoff for a closed vocabulary the compiler can prove
// complete.

bsql_postgres_core::columns! { t => [ id: i32, name: i64, age: i16 ] }

fn classify(c: t::DynCol) -> &'static str {
    match c {
        t::DynCol::id => "id",
        t::DynCol::name => "name",
        // `age` is forgotten — E0004.
    }
}

fn main() {
    let _ = classify(t::DynCol::id);
}
