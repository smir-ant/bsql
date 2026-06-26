//! SCHEMA_PIN drift: the parameter OID list agrees with the declared
//! tuple, but the pre-baked `Parse`-frame template embeds a DIFFERENT
//! OID in its trailing parameter-type section. The validating
//! constructor cross-checks the wire bytes against the declared OIDs, so
//! a template that lies about the parameter types it announces to the
//! server is `error[E0080]` — the baked wire cannot drift from the
//! schema-pinned shape.
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
    &[bsql_postgres_proto::oids::INT4],
    &[],
    PARSE_TEMPLATE,
    &[0, 0],
);

fn main() {
    let _ = &Q;
}
