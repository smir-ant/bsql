//! OFFLINE proof of the `copy!` flagship: the macro expands against the build
//! catalog into a `TypedCopyIn` carrier whose `SQL` is the exact PGCOPY binary
//! COPY command and whose `Row<'q>` tuple is the pinned column encode types. No
//! PostgreSQL — the whole point is the COMPILE-TIME contract (the live
//! round-trip is `copy_typed_live_{async,sync}.rs`, `#[ignore]`).

#![forbid(unsafe_code)]

// `copy_bulk` (migration 0014): id BIGINT NOT NULL, label TEXT NOT NULL,
// note TEXT (nullable), amount INTEGER (nullable).
bsql::copy!(BulkRow, "copy_bulk", (id, label, note, amount));

#[test]
fn sql_is_the_exact_binary_copy_command() {
    assert_eq!(
        <BulkRow as bsql::TypedCopyIn>::SQL,
        "COPY copy_bulk (id, label, note, amount) FROM STDIN WITH (FORMAT binary)",
        "the baked COPY command must name the catalog table + columns and request \
         the binary format",
    );
}

#[test]
fn row_tuple_is_the_pinned_column_types() {
    // A compile-time proof of the `Row<'q>` shape: a `NOT NULL` column is `T`
    // (`id: i64`, `label: &str`), a nullable column is `Option<T>`
    // (`note: Option<&str>`, `amount: Option<i32>`). A wrong type / arity here
    // would be `error[E0308]` — the same wall the `copy_in_typed` call site hits.
    let row: <BulkRow as bsql::TypedCopyIn>::Row<'_> = (7i64, "hi", Some("note"), None);

    // …and that tuple is a `ParamsWriter` (arity 4) — the exact bound the
    // `copy_in_typed` verb needs to stream it through the shared binary encoders.
    fn assert_params<P: bsql::ParamsWriter>(_: &P) {}
    assert_params(&row);
}

#[test]
fn borrowed_text_field_takes_a_non_static_lifetime() {
    // The `text` field is `&'q str`, so a bulk load borrows the caller's data —
    // a runtime-owned `String` lends `&str` with no per-field copy. Proven by
    // building the row from a local (non-`'static`) `String`.
    let owned_label = String::from("borrowed");
    let owned_note = String::from("also borrowed");
    let row: <BulkRow as bsql::TypedCopyIn>::Row<'_> =
        (1i64, owned_label.as_str(), Some(owned_note.as_str()), Some(42i32));
    fn assert_params<P: bsql::ParamsWriter>(_: &P) {}
    assert_params(&row);
}
