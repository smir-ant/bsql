// The `0014_moods.sql` migration declares
// `CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'in_progress')`, so
// `bsql::user_types!()` generates a `Mood` enum with EXACTLY those variants.
// Naming a variant the migration did not declare — the same situation as a
// variant a later migration RENAMED or DELETED — is a compile error at the use
// site: the variant SET is the migration's, enforced by the compiler. This is
// the drift guarantee (a renamed/deleted enum label breaks the build) no other
// library offers from the DDL alone.
bsql::user_types!();

fn main() {
    let _ = Mood::Ecstatic;
}
