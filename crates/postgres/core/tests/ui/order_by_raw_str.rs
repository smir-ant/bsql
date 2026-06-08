// NO-RAW-STR WALL = E0277. `order_by` accepts `impl AsIdent` — a `Col`
// marker or a `DynCol`, never a raw `&str`. A runtime string in ordering
// position does not implement `AsIdent` and is a compile error (with the
// custom `#[diagnostic::on_unimplemented]` note). There is no raw-`&str`
// -> identifier path.

use bsql_postgres_core::fragment::{Chunk, Fragment};

fn main() {
    let f = Fragment::__from_chunks(vec![Chunk::Rodata("SELECT 1")]);
    let _ = f.order_by("id; DROP TABLE users");
}
