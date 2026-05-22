//! P-PRIS-1: `#[derive(Pristine)]` on a struct with a `String` field
//! must reject at compile time per `synthesise_check`.

#![forbid(unsafe_code)]

use bsql_pg_proto::Pristine;

#[derive(Default, Pristine)]
struct WithStringField {
    bad: alloc::string::String,
}

fn main() {
    let _ = <WithStringField as Pristine>::is_pristine(&WithStringField::default());
}
