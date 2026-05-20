//! Probe **P-E-8** — `PgProtocol`'s `inner` and
//! `phase_marker` fields are module-private; struct-literal
//! construction outside `mod protocol` fails with E0451 (private
//! fields cannot be named in a literal).

extern crate bsql_pg_proto;

use bsql_pg_proto::PgProtocol;
use core::marker::PhantomData;

fn main() {
    // E0451: cannot construct `PgProtocol` because both `inner` and
    // `phase_marker` are module-private (no visibility modifier on
    // either field within `mod protocol`).
    let _: PgProtocol = PgProtocol {
        inner: panic!("never reached — the literal is structurally rejected"),
        phase_marker: PhantomData,
    };
}
