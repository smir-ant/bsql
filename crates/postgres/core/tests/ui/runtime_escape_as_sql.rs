// THE MOAT CEILING (load-bearing). The injection guarantee rests on
// `Col::as_sql` returning `&'static str`. A hostile downstream impl that
// reaches the doc-hidden seal still cannot smuggle a *runtime* string
// into identifier position: returning a local `String`'s slice is E0515
// (cannot return a value referencing a local variable).
//
// This is why the guarantee does NOT depend on seal unforgeability —
// it depends on the return type.

use bsql_postgres_core::col::{col_seal, Col};

#[derive(Clone, Copy)]
struct Rogue;

impl col_seal::Sealed for Rogue {}

impl Col for Rogue {
    type Ty = i32;
    fn as_sql(&self) -> &'static str {
        let runtime = String::from("injected ; DROP TABLE users; --");
        runtime.as_str()
    }
}

fn main() {}
