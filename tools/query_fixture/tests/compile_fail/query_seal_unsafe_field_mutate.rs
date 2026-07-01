//! Seal probe — mutating a `PreparedQuery`'s SQL through a raw pointer is
//! rejected two ways: the `sql` field is `pub(crate)` (`error[E0616]`), and
//! the `unsafe` block needed for the raw-pointer write is barred by this
//! file's `#![forbid(unsafe_code)]`. The query is built by `query!`; the
//! forbid is the language-level half of the OS-boundary closure (`.rodata`
//! is read-only at runtime, so the write would segfault even without it).

#![forbid(unsafe_code)]

bsql_query_macros::query!(SealUnsafeMutate, "SELECT id FROM users");

fn main() {
    unsafe {
        let sql_ptr = &SealUnsafeMutateQuery::PREPARED.sql as *const &str as *mut &str;
        sql_ptr.write("DROP TABLE users; --");
    }
}
