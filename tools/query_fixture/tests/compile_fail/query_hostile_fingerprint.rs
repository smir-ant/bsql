//! Layer 2 of the seal: even a hand-written fingerprint carrier cannot
//! mint a LYING `PreparedQuery`. A hostile `impl QueryFingerprint` whose
//! wire bytes disagree with its declared `Params` / `Row` is forced
//! through the validating constructor by the proto-owned `run` boundary,
//! so it fails const-evaluation (`error[E0080]`).
//!
//! Here `Params = (i32,)` (declared OID `int4` = 23) but `PARAM_OIDS`
//! claims `99`. The carrier is uninhabited; the only path to a
//! `PreparedQuery` is `run::<Hostile>()`, which validates.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::prepared::run;
use bsql_postgres_proto::{PreparedQuery, QueryFingerprint};

enum Hostile {}

impl QueryFingerprint for Hostile {
    type Params = (i32,);
    type Row = ();
    const SQL: &'static str = "SELECT $1";
    const STMT_NAME: &'static str = "bsql_q_hostile";
    const PARAM_OIDS: &'static [u32] = &[99];
    const ROW_OIDS: &'static [u32] = &[];
    const PARSE_TEMPLATE: &'static [u8] = &[];
    const BIND_EXECUTE_PREFIX: &'static [u8] = &[0, 0];
}

const Q: PreparedQuery<(i32,), ()> = run::<Hostile>();

fn main() {
    let _ = &Q;
}
