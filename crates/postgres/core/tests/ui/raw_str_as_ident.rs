// A raw `&str` is unusable where an identifier (`AsIdent`) is required.
// `&str: AsIdent` is E0277 because `&str: Col` is not satisfied (the
// blanket `impl<C: Col> AsIdent for C` is the only route to `AsIdent`).

use bsql_postgres_core::col::AsIdent;

bsql_postgres_core::columns! { t => [ id: i32 ] }

fn needs_ident<I: AsIdent>(i: I) -> &'static str {
    i.ident()
}

fn main() {
    // Passing a runtime string where an identifier is required:
    let _ = needs_ident("name; DROP TABLE t; --");
}
