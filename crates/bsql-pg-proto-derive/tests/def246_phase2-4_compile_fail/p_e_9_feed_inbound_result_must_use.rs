//! Probe **P-E-9** — discarding the
//! `Result<(), ProtocolError>` returned by `feed_inbound` trips
//! `unused_must_use` + `-D warnings` = compile error.
//!
//! Pre-Phase-4 the return type was `Result<(), ReadBufFull>` with
//! `#[must_use]` already present; the change to a richer error type
//! preserves the contract.

#![deny(unused_must_use)]
#![forbid(unsafe_code)]

extern crate bsql_pg_proto;

use bsql_pg_proto::{Credentials, Ident, PgProtocol, WriteBuf};

fn main() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply = proto.next_reply_id();
    let user = match Ident::try_from_str("u") {
        Ok(u) => u,
        Err(_) => return,
    };
    let (_, mut connecting) = match proto.push_startup(
        user,
        None,
        None,
        Credentials::Trust,
        reply,
        &mut wb,
    ) {
        Ok(p) => p,
        Err(_) => return,
    };
    // E (unused_must_use): the returned `Result<(), ProtocolError>`
    // is silently discarded. `#[deny(unused_must_use)]` at file scope
    // upgrades the warning to an error.
    connecting.feed_inbound(&[]);
}
