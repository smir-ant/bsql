//! DEF-244 — pin the `varchar` rejection at macro-expand. Tracks
//! DEF-228: when `DecodeFormat<TextFmt>` impls land for `varchar`
//! (likely re-using the `text` decoder; OIDs differ but the byte
//! representation is identical text-format), the macro grows the
//! `varchar` entry and **this file must be deleted in the same
//! commit**.

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(&'static str,), ()> = prepared!(
    "SELECT $1::varchar"
);

fn main() {
    let _ = Q;
}
