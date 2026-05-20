//! Pin the `bytea` rejection at macro-expand. Tracks
//! When `DecodeFormat<TextFmt>` for `&[u8]` (or the
//! canonical `Bytea` newtype) lands, the runtime crate gains the
//! decode/encode pair, the macro's typemap grows the `bytea` entry,
//! and **this file must be deleted in the same commit**. Drift
//! detection: if the runtime supports `bytea` but this file still
//! exists, trybuild fails the assertion that the type is rejected.

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(&'static [u8],), ()> = prepared!(
    "SELECT $1::bytea"
);

fn main() {
    let _ = Q;
}
