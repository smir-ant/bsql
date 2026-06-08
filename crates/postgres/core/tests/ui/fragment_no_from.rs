// NO From<&str> for Fragment. `Fragment::from(s)` resolves the reflexive
// `impl<T> From<T> for T` (expects a `Fragment`), so a `&str` arg is E0308
// mismatched types — there is no `From<&str>`; no raw-str -> SQL path.

use bsql_postgres_core::Fragment;

fn main() {
    let r = String::from("DROP TABLE users");
    let _f: Fragment = Fragment::from(r.as_str());
}
