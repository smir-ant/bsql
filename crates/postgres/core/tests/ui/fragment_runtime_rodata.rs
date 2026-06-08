// THE INJECTION WALL = E0597. The doc-hidden `__from_chunks` /
// `Chunk::Rodata` are reachable cross-crate (caller hygiene), but
// `Chunk::Rodata` holds a `&'static str`: a runtime `String`'s slice
// cannot enter it. This is the moat — and it holds cross-crate (a
// `tests/ui` file is a separate crate from `bsql_postgres_core`).

use bsql_postgres_core::fragment::{Chunk, Fragment};

fn main() {
    let runtime = String::from("DROP TABLE users");
    let _ = Fragment::__from_chunks(vec![Chunk::Rodata(&runtime)]);
}
