// CLOSED CARRIER = E0599. `BoundValue` is a closed enum over exactly the
// six bindable types — there is no `Raw`/text-passthrough variant, so a
// value can only ever become a binary `$N` block, never spine text.

use bsql_postgres_core::BoundValue;

fn main() {
    let _ = BoundValue::Raw(String::from("1; DROP"));
}
