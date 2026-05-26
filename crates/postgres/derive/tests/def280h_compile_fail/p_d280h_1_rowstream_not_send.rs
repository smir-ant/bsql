//! Probe **P-D280H-1** — `RowStream` is `!Send`.
//!
//! Tier-1 by construction: `RowStream` carries a
//! `PhantomData<*const ()>` field; `*const ()` is the canonical
//! non-`Send` witness in `core::marker::Send`'s auto-trait rules.
//! Any function bound on `T: Send` rejects `RowStream` at type-check
//! time (E0277). This prevents the family of bugs where a caller
//! attempts to `tokio::spawn` (or any other `Send + 'static`
//! boundary-crosser) over `&mut RowStream` captured inside an
//! `iter_rows` closure — Drop ordering across thread boundaries
//! cannot race with the protocol's state-machine state, because
//! the `!Send` mark makes the capture refuse to compile.
//!
//! The auto-`!Send` propagates through `&mut RowStream` (and
//! through any container holding a RowStream-typed field) per
//! Rust's auto-trait propagation rules.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::RowStream;

fn require_send<T: Send>() {}

fn main() {
    // RowStream is `!Send` — this must fail to compile with E0277.
    require_send::<RowStream<'static, 'static>>();
}
