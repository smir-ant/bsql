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

use bsql_query_bridge_fixture::bridge::MyTs;

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
// validator: `ROW_OIDS == <Row as RowDecode>::OIDS`. Both ride the NATIVE
// pivot (uuid OID 2950, timestamptz OID 1184), UNCHANGED by the bridge — a
// wrong OID would be `error[E0080]`. That this compiles proves the validator
// runs and passes with a bridge in effect.

#[test]
fn oid_validator_runs_with_bridges_present() {
    let _two = TwoFixedQuery::PREPARED;
    let _full = FullRowQuery::PREPARED;
}
