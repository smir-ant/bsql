//! DEF-278 Bundle D probe **P-D278D-5** — `CancelRequestCredentials`'s
//! `secret_key` field is private to `mod cancel`. External code
//! cannot read or write it directly (E0616).
//!
//! Tier-1 by construction: the public surface for the credentials
//! is `pid()`, `encode()`, `Debug` (which redacts the secret), and
//! Drop (which zeroizes). Direct field access is rejected at compile
//! time so a downstream contributor cannot accidentally bypass the
//! `Sensitive<i32>` redaction by reading the raw `secret_key` bytes.
//!
//! Mirror of the cell-payload privacy invariant (`BackendKeyCell.inner`
//! / `BackendKey.secret_key` are also field-private to `mod cancel`,
//! but those types themselves are `pub(crate)` and thus unreachable
//! from an external crate). This probe targets the only externally-
//! reachable type in the chain — `CancelRequestCredentials` — and
//! ensures its `secret_key` field is private.

extern crate bsql_pg_proto;

use bsql_pg_proto::CancelRequestCredentials;

fn _force_use(c: CancelRequestCredentials) {
    // Direct field access — E0616 expected because secret_key is
    // private to `mod cancel`. The field's type is
    // `Sensitive<i32>` internally, but the privacy gate fires before
    // any type-resolution issue — E0616 surfaces first.
    let _stolen = c.secret_key;
    let _ = _stolen;
}

fn main() {}
