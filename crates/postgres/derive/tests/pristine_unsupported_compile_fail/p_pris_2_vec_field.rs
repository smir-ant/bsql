//! P-PRIS-2: `#[derive(Pristine)]` on a struct with a `Vec<u8>` field
//! must reject at compile time per `synthesise_check`.

#![forbid(unsafe_code)]

use bsql_postgres_proto::Pristine;

#[derive(Default, Pristine)]
struct WithVecField {
    bad: alloc::vec::Vec<u8>,
}

fn main() {
    let _ = <WithVecField as Pristine>::is_pristine(&WithVecField::default());
}
