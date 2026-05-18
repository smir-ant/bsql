//! Tier-3 audit #33 probe **P-E-10** — `DispatchContext<'state, 'r>`
//! is `pub(in crate::protocol)` so external crates cannot name the
//! type, cannot import it, cannot construct it. Attempting to
//! reference the type from outside the crate fails with E0603
//! (private type) or E0432 (unresolved import). Pins the within-
//! crate tier-1 closure of dispatch-context construction (the only
//! legitimate path is via `_dispatch_context_leaf` mint helpers).

extern crate bsql_pg_proto;

fn main() {
    // E0603 / E0432: `DispatchContext` is `pub(in crate::protocol)`
    // — not nameable from outside the crate. The type does not
    // appear in `bsql_pg_proto`'s public API at all; this import
    // must fail. If a future refactor accidentally widens the
    // visibility to `pub`, this probe stops failing and the
    // trybuild gate fires.
    use bsql_pg_proto::protocol::DispatchContext;
    let _: Option<DispatchContext<'_, '_>> = None;
}
