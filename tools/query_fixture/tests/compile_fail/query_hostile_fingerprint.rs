//! Layer 2 of the seal: even a hand-written fingerprint carrier cannot
//! mint a LYING `PreparedQuery`. A hostile `impl QueryFingerprint` whose
//! wire bytes disagree with its declared `Params` / `Row` is forced
//! through the validating constructor by the proto-owned `run` boundary,
//! so it fails const-evaluation (`error[E0080]`).
//!
//! The param / row OID lists are no longer carrier consts — they are SOURCED
//! from `Params` / `Row` by `new_prepared_query`, so a carrier can no longer
//! even STATE a mismatching OID list (that channel is gone). The one remaining
//! way a carrier can lie about its parameter types is the independently-baked
//! `PARSE_TEMPLATE` wire bytes — and those are still cross-checked against
//! `<Params as ParamsWriter>::OIDS`. Here `Params = (i32,)` (`int4` = 23) but
//! the baked `Parse` frame announces OID `99`, so `run::<Hostile>()` rejects
//! it. The carrier is uninhabited; the only path to a `PreparedQuery` is
//! `run::<Hostile>()`, which validates the wire against the declared types.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::prepared::run;
use bsql_postgres_proto::{PreparedQuery, QueryFingerprint};

enum Hostile {}

// A structurally valid `Parse` frame for one parameter over SQL "q" / stmt "s"
// EXCEPT its OID word is `99` instead of the declared `int4` = 23.
// Layout: `b'P' | len_i32_be | "s" | NUL | "q" | NUL | n_params=1 | oid=99`.
const HOSTILE_PARSE: &[u8] = &[
    b'P', 0, 0, 0, 14, b's', 0, b'q', 0, 0, 1, 0, 0, 0, 99,
];

impl QueryFingerprint for Hostile {
    type Params = (i32,);
    type Row = ();
    const SQL: &'static str = "q";
    const STMT_NAME: &'static str = "s";
    const PARSE_TEMPLATE: &'static [u8] = HOSTILE_PARSE;
    const BIND_EXECUTE_PREFIX: &'static [u8] = &[0, 0];
}

const Q: PreparedQuery<(i32,), ()> = run::<Hostile>();

fn main() {
    let _ = &Q;
}
