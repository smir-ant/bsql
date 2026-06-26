//! Layer 1 of the seal: a hostile caller cannot fabricate a
//! `PreparedQuery` by writing its struct literal directly. Every field
//! is `pub(crate)`, so an external struct-literal construction is
//! `error[E0451]` (private fields) — there is no way to mint an artifact
//! carrying attacker-chosen SQL outside the validating constructor.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;
use core::marker::PhantomData;

fn main() {
    let _hostile: PreparedQuery<(), ()> = PreparedQuery {
        sql: "DROP TABLE users; --",
        stmt_name: "x",
        param_oids: &[],
        row_oids: &[],
        parse_template: &[],
        bind_execute_prefix: &[],
        _phantom: PhantomData,
    };
}
