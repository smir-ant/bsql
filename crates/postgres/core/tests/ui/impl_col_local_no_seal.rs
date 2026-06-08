// LEG-A COMPANION: a local un-sealed type cannot be a `Col`.
// `impl Col for Bogus` is E0277 — the private `col_seal::Sealed`
// supertrait is not satisfied. rustc lists the declared columns as the
// only impls of the seal.

use bsql_postgres_core::col::Col;

bsql_postgres_core::columns! { t => [ id: i32 ] }

#[derive(Clone, Copy)]
struct Bogus;

impl Col for Bogus {
    type Ty = i32;
    fn as_sql(&self) -> &'static str {
        "evil"
    }
}

fn main() {}
