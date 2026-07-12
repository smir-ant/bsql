//! SCHEMA_PIN drift: the param OID list is now SOURCED from the declared
//! tuple (`(i32,)` → `int4` = 23), but the pre-baked `Parse`-frame
//! template embeds a DIFFERENT OID (`99`) in its trailing parameter-type
//! section. The validating constructor cross-checks the independently-baked
//! wire bytes against `<Params as ParamsWriter>::OIDS`, so a template that
//! lies about the parameter types it announces to the server is
//! `error[E0080]` — the baked wire cannot drift from the type it declares.
//! This is the one genuinely-distinct OID cross-check that survives sourcing
//! the OID lists from the tuple types: the `Parse` template is a SEPARATE
//! representation (raw big-endian bytes for the zero-cost wire), not a
//! restatement of the OID list, so it is a real check, not a tautology.
//!
//! The template below is a structurally valid `Parse` frame for one
//! `int4` parameter EXCEPT that its OID word is `99` instead of `23`.
//! Layout: `b'P' | len_i32_be | "s" | NUL | "q" | NUL | n_params=1 |
//! oid=99`.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::PreparedQuery;

const PARSE_TEMPLATE: &[u8] = &[
    b'P', // tag
    0, 0, 0, 14, // length field (self-inclusive, excludes tag)
    b's', 0, // stmt_name + NUL
    b'q', 0, // sql + NUL
    0, 1, // n_param_types = 1
    0, 0, 0, 99, // OID = 99 (drifted; the declared int4 is 23)
];

const Q: PreparedQuery<(i32,), ()> = new_prepared_query::<(i32,), ()>(
    "q",
    "s",
    PARSE_TEMPLATE,
    &[0, 0],
);

fn main() {
    let _ = &Q;
}
