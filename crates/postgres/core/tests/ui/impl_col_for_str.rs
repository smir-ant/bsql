// LEG-A HEADLINE: a raw `&str` is not a column identifier.
// `impl Col for &str` is E0117 (orphan rule — `str` is foreign).
// There is no raw-`&str` -> identifier path.

use bsql_postgres_core::col::Col;

// Bring a vocabulary into scope so `Col` is in active use.
bsql_postgres_core::columns! { t => [ id: i32, name: i64 ] }

impl Col for &str {
    type Ty = i32;
    fn as_sql(&self) -> &'static str {
        "evil"
    }
}

fn main() {}
