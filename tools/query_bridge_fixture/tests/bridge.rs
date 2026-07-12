//! The external-type bridge WITNESS: `query!` over bridged columns emits the
//! consumer's chosen target types (a dep-free fixture-local `MyTs` stand-in AND
//! the real `uuid::Uuid`), decodes canned `DataRow` bytes into them through the
//! infallible converter free functions, and the const OID validator still holds
//! WITH the bridges present.
//!
//! Everything here is structurally offline (hand-built `DataRow` bodies, no live
//! server). The `bridge::MyTs` / `bridge::to_myts` / `bridge::to_uuid` paths the
//! build.rs registered resolve because this integration-test crate names the
//! fixture crate by its package name.
//!
//! The FIELD-TYPE assertions are compile-time: each `let _: Target = record.f;`
//! type-checks ONLY because the macro emitted the bare target type. The value
//! assertions prove the converter is applied. `_force_prepared` monomorphizes
//! each carrier's `PREPARED`, which runs the proto-owned const validator — a
//! wrong OID would be an `error[E0080]` (the guarantee rides the native pivot,
//! untouched by the bridge).

use bsql_query_bridge_fixture::bridge::{MyDate, MyDecimal, MyTs};

// All-fixed-width, all-NOT-NULL, BOTH columns bridged: `id` (uuid, 16 bytes)
// and `created` (timestamptz, 8 bytes). This exercises the vectorized fast path
// with the converter applied per column.
bsql::query!(TwoFixed, "SELECT id, created FROM events");

// The full row: scalar-bridged (`id` uuid, `created` timestamptz), nullable
// bridged (`updated` -> Option<MyTs>), array-element bridged (`tstamps` ->
// Vec<Option<MyTs>>), and an UNBRIDGED native text column (`label`). Exercises
// the per-cell path, per-element conversion, and bridged/native coexistence.
bsql::query!(
    FullRow,
    "SELECT id, created, updated, tstamps, label FROM events"
);

// The numeric-bridged columns: scalar (`amount` -> MyDecimal), nullable
// (`refund` -> Option<MyDecimal>), and array-element (`rates` ->
// Vec<Option<MyDecimal>>). Proves the variable-width, arbitrary-precision
// `bsql::Numeric` pivot reshapes into the consumer's decimal type.
bsql::query!(Decimals, "SELECT amount, refund, rates FROM events");

// The `date`-bridged column: the native `bsql::Date` pivot reshapes into the
// consumer's calendar-date type (`MyDate`) via the civil conversion — proving
// the temporal pivot bridges with bsql forcing no calendar crate.
bsql::query!(Dated, "SELECT day FROM events");

// ── compile-time FIELD-TYPE assertions ──────────────────────────────────
// Each binding type-checks only because the emitted field type is the BARE
// target — no `.0`, no `.into()`, no annotation at any `query!` site.

#[allow(dead_code, reason = "compile-time type witnesses; never called")]
fn _field_types_two_fixed(r: TwoFixedOwned) {
    let _id: uuid::Uuid = r.id;
    let _created: MyTs = r.created;
}

#[allow(dead_code, reason = "compile-time type witnesses; never called")]
fn _field_types_full_row(r: FullRowOwned) {
    let _id: uuid::Uuid = r.id;
    let _created: MyTs = r.created;
    let _updated: Option<MyTs> = r.updated;
    let _tstamps: Vec<Option<MyTs>> = r.tstamps;
    let _label: String = r.label;
}

#[allow(dead_code, reason = "compile-time type witnesses; never called")]
fn _field_types_decimals(r: DecimalsOwned) {
    let _amount: MyDecimal = r.amount;
    let _refund: Option<MyDecimal> = r.refund;
    let _rates: Vec<Option<MyDecimal>> = r.rates;
}

#[allow(dead_code, reason = "compile-time type witnesses; never called")]
fn _field_types_dated(r: DatedOwned) {
    // The `date` column decodes DIRECTLY into the consumer's `MyDate` — no
    // `.0`, no `.into()`, no annotation at the `query!` site.
    let _day: MyDate = r.day;
}

// ── byte builders ────────────────────────────────────────────────────────

fn i32be(n: i32) -> [u8; 4] {
    n.to_be_bytes()
}

/// A non-NULL column cell: 4-byte length prefix + payload.
fn cell(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&i32be(payload.len() as i32));
    out.extend_from_slice(payload);
    out
}

/// The 8-byte big-endian timestamptz wire payload for a raw PG-epoch micros.
fn tstz(micros: i64) -> [u8; 8] {
    micros.to_be_bytes()
}

/// A 1-D `timestamptz[]` binary array payload with the given elements
/// (`None` = a NULL element). `ndim=1`, element OID 1184 (timestamptz).
fn tstz_array(elems: &[Option<i64>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&i32be(1)); // ndim
    out.extend_from_slice(&i32be(0)); // flags (hasnull; ignored on decode)
    out.extend_from_slice(&i32be(1184)); // element OID = timestamptz
    out.extend_from_slice(&i32be(elems.len() as i32)); // dim length
    out.extend_from_slice(&i32be(1)); // lower bound
    for elem in elems {
        match elem {
            Some(micros) => {
                out.extend_from_slice(&i32be(8));
                out.extend_from_slice(&tstz(*micros));
            }
            None => out.extend_from_slice(&i32be(-1)),
        }
    }
    out
}

const UUID_BYTES: [u8; 16] = [
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
];
const CREATED_MICROS: i64 = 1_700_000_000_000_000;

fn two_fixed_row() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(2i16).to_be_bytes()); // 2 columns
    body.extend_from_slice(&cell(&UUID_BYTES)); // id
    body.extend_from_slice(&cell(&tstz(CREATED_MICROS))); // created
    body
}

fn full_row() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(5i16).to_be_bytes()); // 5 columns
    body.extend_from_slice(&cell(&UUID_BYTES)); // id
    body.extend_from_slice(&cell(&tstz(CREATED_MICROS))); // created (NOT NULL)
    body.extend_from_slice(&i32be(-1)); // updated = SQL NULL
    body.extend_from_slice(&cell(&tstz_array(&[Some(111), None]))); // tstamps
    body.extend_from_slice(&cell(b"hi")); // label
    body
}

// ── decode witnesses ─────────────────────────────────────────────────────

#[test]
fn two_fixed_decodes_into_the_bridged_targets_on_the_fast_path() {
    let row = TwoFixedOwned::decode(&two_fixed_row()).expect("decodes");
    // The BARE target types, values reshaped by the converters.
    assert_eq!(row.id, uuid::Uuid::from_bytes(UUID_BYTES));
    assert_eq!(row.created, MyTs(CREATED_MICROS));
}

#[test]
fn two_fixed_borrowed_twin_also_decodes() {
    // Both columns are self-owning bridged targets, so the borrowed twin has no
    // lifetime and decodes identically.
    let row = TwoFixed::decode(&two_fixed_row()).expect("decodes");
    assert_eq!(row.id, uuid::Uuid::from_bytes(UUID_BYTES));
    assert_eq!(row.created, MyTs(CREATED_MICROS));
}

#[test]
fn full_row_decodes_scalar_nullable_array_and_native() {
    let row = FullRowOwned::decode(&full_row()).expect("decodes");
    assert_eq!(row.id, uuid::Uuid::from_bytes(UUID_BYTES));
    assert_eq!(row.created, MyTs(CREATED_MICROS));
    // Nullable bridged column: SQL NULL -> None (never a defaulted target).
    assert_eq!(row.updated, None);
    // Array-element bridge: the converter is applied per present element; a
    // NULL element stays None.
    assert_eq!(row.tstamps, vec![Some(MyTs(111)), None]);
    // The unbridged native text column is untouched.
    assert_eq!(row.label, "hi");
}

#[test]
fn null_in_not_null_bridged_column_is_still_classified() {
    // A SQL NULL in the NOT-NULL bridged `created` is a classified decode error
    // — the bridge does not swallow it into a defaulted target.
    let mut body = Vec::new();
    body.extend_from_slice(&(2i16).to_be_bytes());
    body.extend_from_slice(&cell(&UUID_BYTES)); // id
    body.extend_from_slice(&i32be(-1)); // created = NULL (NOT NULL column)
    let err = TwoFixedOwned::decode(&body).expect_err("NULL in NOT NULL is loud");
    assert!(matches!(
        err,
        bsql_postgres_proto::DecodeError::NullInNonNullColumn
    ));
}

// ── the OID-drift guarantee still holds WITH bridges present ──────────────
// Monomorphizing each carrier's `PREPARED` runs the proto-owned const
// validator (formats-binary + the `Parse`-template OID pin against the param
// tuple's `ParamsWriter::OIDS`). The ROW OID list is SOURCED from
// `<Row as RowDecode>::OIDS`, which rides the NATIVE pivot (uuid OID 2950,
// timestamptz OID 1184), UNCHANGED by the bridge — the bridge reshapes only
// the record FIELD, not the row-tuple marker, so a wrong row type would be
// `error[E0308]` at the record. That this compiles proves the native pivot
// (and the surviving wire pin) hold with a bridge in effect.

#[test]
fn oid_validator_runs_with_bridges_present() {
    let _two = TwoFixedQuery::PREPARED;
    let _full = FullRowQuery::PREPARED;
    let _dec = DecimalsQuery::PREPARED;
}

// ── numeric bridge witnesses ─────────────────────────────────────────────

/// A `numeric` binary wire payload: `ndigits · weight · sign · dscale` then the
/// base-10000 digit groups.
fn numeric(weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(digits.len() as u16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for &d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

/// A 1-D `numeric[]` binary array payload (`None` = a NULL element).
fn numeric_array(elems: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&i32be(1)); // ndim
    out.extend_from_slice(&i32be(0)); // flags
    out.extend_from_slice(&i32be(1700)); // element OID = numeric
    out.extend_from_slice(&i32be(elems.len() as i32));
    out.extend_from_slice(&i32be(1)); // lower bound
    for elem in elems {
        match elem {
            Some(body) => out.extend_from_slice(&cell(body)),
            None => out.extend_from_slice(&i32be(-1)),
        }
    }
    out
}

#[test]
fn numeric_columns_decode_into_the_bridged_decimal() {
    // amount = 1.50, refund = NULL, rates = { 0.0001, NULL, 100 }.
    let amount = numeric(0, 0x0000, 2, &[1, 5000]); // 1.50
    let rate_a = numeric(-1, 0x0000, 4, &[1]); // 0.0001
    let rate_c = numeric(0, 0x0000, 0, &[100]); // 100
    let rates = numeric_array(&[Some(rate_a), None, Some(rate_c)]);

    let mut body = Vec::new();
    body.extend_from_slice(&(3i16).to_be_bytes()); // 3 columns
    body.extend_from_slice(&cell(&amount)); // amount (NOT NULL)
    body.extend_from_slice(&i32be(-1)); // refund = SQL NULL
    body.extend_from_slice(&cell(&rates)); // rates

    let row = DecimalsOwned::decode(&body).expect("decodes");
    // The BARE target type, exact decimal text reshaped by the converter.
    assert_eq!(row.amount, MyDecimal("1.50".to_string()));
    // Nullable bridged column: SQL NULL -> None.
    assert_eq!(row.refund, None);
    // Array-element bridge: the converter is applied per present element.
    assert_eq!(
        row.rates,
        vec![
            Some(MyDecimal("0.0001".to_string())),
            None,
            Some(MyDecimal("100".to_string())),
        ],
    );
}

#[test]
fn date_column_decodes_into_the_bridged_calendar_type() {
    // day = 2000-02-29 (59 days after the PG date epoch). The 4-byte i32 day
    // count decodes to the native `bsql::Date`, then the converter reshapes it
    // into the consumer's `MyDate` via the civil conversion.
    let mut body = Vec::new();
    body.extend_from_slice(&(1i16).to_be_bytes()); // 1 column
    body.extend_from_slice(&cell(&59i32.to_be_bytes())); // day = 59 -> 2000-02-29

    let row = DatedOwned::decode(&body).expect("decodes");
    assert_eq!(
        row.day,
        MyDate {
            year: 2000,
            month: 2,
            day: 29,
        },
        "date bridges into the consumer's calendar type via the civil conversion",
    );
}
