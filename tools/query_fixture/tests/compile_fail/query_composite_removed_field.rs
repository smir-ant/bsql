// The `0017_composites.sql` migration declares
// `CREATE TYPE addr AS (street text, zip int4)`, so `bsql::user_types!()`
// generates a `struct Addr { street: Option<String>, zip: Option<i32> }` with
// EXACTLY those fields. Naming a field the migration did not declare — the same
// situation as a field a later migration RENAMED, DROPPED, or whose ATTRIBUTE
// was retyped (`ALTER TYPE ... {DROP|RENAME|ALTER} ATTRIBUTE`) — is a compile
// error at the use site: the field SET is the migration's, enforced by the
// compiler. This is the composite drift guarantee (a renamed/dropped attribute
// breaks the build) — the exact peer of the enum's variant-set guarantee.
bsql::user_types!();

fn main() {
    let _ = |a: Addr| a.county;
}
