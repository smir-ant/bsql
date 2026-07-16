//! Pipeline batch-element probe — binding a carrier with the WRONG parameter
//! tuple is a compile error at the `bind`, never a value that reaches the wire.
//!
//! `BindExt::bind<'p>(params: Self::Params<'p>)` ties the bound tuple to the
//! carrier's compile-time `Params`. The `query!` below binds the `int8` PK
//! (`$1`), so `Params = (i64,)`; binding `("hostile",)` is `error[E0308]` (tuple
//! types are nominally distinct). So a heterogeneous `pipeline((...))` cannot
//! carry a mistyped command — the typed-per-element guarantee holds at the batch
//! boundary exactly as it does for a single `query`.

use bsql::BindExt;

bsql::query!(PipeMismatch, "SELECT id FROM users WHERE id = $1");

fn main() {
    // Bind a `(&str,)` to a carrier expecting `(i64,)`: E0308.
    let _bound = PipeMismatch::bind(("hostile",));
}
