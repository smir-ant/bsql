//! `query!` exact `numeric` / `decimal` support: a `numeric` column decodes to
//! the dep-free arbitrary-precision `bsql::Numeric`, a nullable one to
//! `Option<bsql::Numeric>`, and a `numeric[]` column to
//! `Vec<Option<bsql::Numeric>>`, from hand-built `DataRow` payloads (no live
//! server).
//!
//! Each `query!` below types AT COMPILE TIME against the `ledger` table in
//! `migrations/` — the mere fact these compile is the proof that a `numeric` /
//! `numeric[]` column is no longer a `compile_error!`. The `_field_types`
//! function is a compile-time assertion that each record field has exactly the
//! expected `bsql::Numeric` shape. The decode assertions prove the numeric wire
//! bytes materialise into the exact value (precision-critical: a decode bug is
//! silently-wrong money), including a NULL element and a NULL whole column.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "offline test fixture — expect surfaces a malformed hand-built fixture loudly; not a production fallback"
)]

use bsql::Numeric;

// A SELECT over every `ledger` column: `id` (`int4`), `amount` (`numeric` NOT
// NULL), `fee` (nullable `decimal`), `tranche` (`numeric[]` NOT NULL). No column
// borrows the input (numeric is self-owning), so the borrowed record carries no
// lifetime.
bsql::query!(LedgerRow, "SELECT id, amount, fee, tranche FROM ledger");

// A literal `numeric` cast (no table) — proves the cast path types to the same
// `bsql::Numeric` as a catalog column.
bsql::query!(NumericCast, "SELECT '3.14'::numeric AS n");

/// Compile-time assertions that each record field has the exact expected type.
#[allow(dead_code, reason = "compile-time field-type assertion; never called")]
fn _field_types(r: LedgerRow) {
    let _id: i32 = r.id;
    let _amount: Numeric = r.amount;
    let _fee: Option<Numeric> = r.fee;
    let _tranche: Vec<Option<Numeric>> = r.tranche;
}

// ── numeric wire builders ──────────────────────────────────────────────────

/// Build a `numeric` binary wire body: `ndigits · weight · sign · dscale` then
/// the base-10000 digit groups.
fn numeric_body(weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&u16::try_from(digits.len()).expect("ndigits").to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for &d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

/// Wrap a column body in its 4-byte `DataRow` length prefix; `None` -> the
/// `-1` SQL-NULL sentinel.
fn column(body: Option<&[u8]>) -> Vec<u8> {
    let mut out = Vec::new();
    match body {
        Some(b) => {
            out.extend_from_slice(&i32::try_from(b.len()).expect("len").to_be_bytes());
            out.extend_from_slice(b);
        }
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    out
}

/// Build a 1-D PG binary `numeric[]` array body from per-element numeric bodies
/// (`None` -> a NULL element).
fn numeric_array_body(elems: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    let has_null = i32::from(elems.iter().any(Option::is_none));
    out.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
    out.extend_from_slice(&has_null.to_be_bytes());
    out.extend_from_slice(&1700u32.to_be_bytes()); // element OID = numeric
    out.extend_from_slice(&i32::try_from(elems.len()).expect("len").to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
    for elem in elems {
        out.extend_from_slice(&column(elem.as_deref()));
    }
    out
}

/// A full `LedgerRow` `DataRow` body: `id`, `amount`, `fee`, `tranche`.
fn ledger_row(id: i32, amount: &[u8], fee: Option<&[u8]>, tranche: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&4i16.to_be_bytes()); // 4 columns
    out.extend_from_slice(&column(Some(&id.to_be_bytes())));
    out.extend_from_slice(&column(Some(amount)));
    out.extend_from_slice(&column(fee));
    out.extend_from_slice(&column(Some(tranche)));
    out
}

#[test]
fn ledger_row_decodes_exact_numeric_values() {
    // amount = 1.5 (weight 0, dscale 1, digits [1, 5000]).
    let amount = numeric_body(0, 0x0000, 1, &[1, 5000]);
    // tranche = { 1.50, NULL, 100 }.
    let one_five_zero = numeric_body(0, 0x0000, 2, &[1, 5000]); // 1.50 (dscale 2)
    let hundred = numeric_body(0, 0x0000, 0, &[100]); // 100
    let tranche = numeric_array_body(&[
        Some(one_five_zero),
        None,
        Some(hundred),
    ]);
    // fee = SQL NULL.
    let body = ledger_row(1, &amount, None, &tranche);

    let row = LedgerRow::decode(&body).expect("row decodes");
    assert_eq!(row.id, 1);
    assert_eq!(row.amount.to_string(), "1.5");
    assert!(row.fee.is_none(), "fee is SQL NULL");
    let rendered: Vec<Option<String>> = row
        .tranche
        .iter()
        .map(|e| e.as_ref().map(ToString::to_string))
        .collect();
    assert_eq!(
        rendered,
        vec![Some("1.50".to_string()), None, Some("100".to_string())],
        "tranche renders 1.50, NULL, 100",
    );
}

#[test]
fn numeric_cast_column_decodes() {
    // A one-column row for `SELECT '3.14'::numeric AS n`: 3.14 = weight 0,
    // dscale 2, digits [3, 1400].
    let mut body = Vec::new();
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    body.extend_from_slice(&column(Some(&numeric_body(0, 0x0000, 2, &[3, 1400]))));
    let row = NumericCast::decode(&body).expect("row decodes");
    assert_eq!(row.n.to_string(), "3.14");
}

#[test]
fn numeric_null_in_not_null_column_is_classified() {
    // `amount` is NOT NULL; a SQL NULL there is a classified decode error on the
    // record, never a silent default.
    let tranche = numeric_array_body(&[]);
    let mut with_null = Vec::new();
    with_null.extend_from_slice(&4i16.to_be_bytes());
    with_null.extend_from_slice(&column(Some(&1i32.to_be_bytes()))); // id
    with_null.extend_from_slice(&column(None)); // amount = NULL (NOT NULL column)
    with_null.extend_from_slice(&column(None)); // fee = NULL
    with_null.extend_from_slice(&column(Some(&tranche))); // tranche

    assert!(
        LedgerRow::decode(&with_null).is_err(),
        "a NULL in the NOT-NULL numeric column must be a classified error",
    );
}
