// NO-RAW-STR WALL = E0277. An owned `String` is likewise not an `AsIdent`;
// there is no runtime-string -> identifier path through `order_by`.

use bsql_postgres_core::fragment::{Chunk, Fragment};

fn main() {
    let f = Fragment::__from_chunks(vec![Chunk::Rodata("SELECT 1")]);
    let s = String::from("id");
    let _ = f.order_by(s);
}
