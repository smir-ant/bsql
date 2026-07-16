//! `query!` dep-free temporal support: a `date` column decodes to `bsql::Date`,
//! a `time` to `bsql::Time`, an `interval` to `bsql::Interval`, a nullable one
//! to its `Option<..>`, and an `interval[]` column to
//! `Vec<Option<bsql::Interval>>`, from hand-built `DataRow` payloads (no live
//! server).
//!
//! Each `query!` below types AT COMPILE TIME against the `schedule` table in
//! `migrations/` — the mere fact these compile is the proof that a `date` /
//! `time` / `interval` (+ array) column is no longer a `compile_error!`. The
//! `_field_types` function is a compile-time assertion that each record field
//! has exactly the expected shape. The decode assertions prove the wire bytes
//! materialise into the exact value and render PostgreSQL's own text (a `date`
//! off-by-one is a wrong calendar day).
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "offline test fixture — expect surfaces a malformed hand-built fixture loudly; not a production fallback"
)]

use bsql::{Date, Interval, Time};

// A SELECT over every `schedule` column. No column borrows the input (every
// temporal type is self-owning), so the borrowed record carries no lifetime.
bsql::query!(
    ScheduleRow,
    "SELECT id, day, at, span, deadline, windows FROM schedule"
);

// Literal temporal casts (no table) — prove the cast path types to the same
// dep-free types as a catalog column.
bsql::query!(DateCast, "SELECT '2000-02-29'::date AS d");
bsql::query!(TimeCast, "SELECT '12:34:56.789012'::time AS t");
bsql::query!(IntervalCast, "SELECT '1 year 2 mons 3 days 04:05:06'::interval AS i");

/// Compile-time assertions that each record field has the exact expected type.
#[allow(dead_code, reason = "compile-time field-type assertion; never called")]
fn _field_types(r: ScheduleRow) {
    let _id: i32 = r.id;
    let _day: Date = r.day;
    let _at: Time = r.at;
    let _span: Interval = r.span;
    let _deadline: Option<Date> = r.deadline;
    let _windows: Vec<Option<Interval>> = r.windows;
}

// ── temporal wire builders ─────────────────────────────────────────────────

/// A `date` binary body: 4-byte big-endian day count since 2000-01-01.
fn date_body(days: i32) -> Vec<u8> {
    days.to_be_bytes().to_vec()
}

/// A `time` binary body: 8-byte big-endian microseconds since midnight.
fn time_body(micros: i64) -> Vec<u8> {
    micros.to_be_bytes().to_vec()
}

/// An `interval` binary body: `interval_send` order — i64 micros, i32 days,
/// i32 months (16 bytes).
fn interval_body(months: i32, days: i32, micros: i64) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&micros.to_be_bytes());
    out.extend_from_slice(&days.to_be_bytes());
    out.extend_from_slice(&months.to_be_bytes());
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

/// Build a 1-D PG binary `interval[]` array body (`None` -> a NULL element).
fn interval_array_body(elems: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    let has_null = i32::from(elems.iter().any(Option::is_none));
    out.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
    out.extend_from_slice(&has_null.to_be_bytes());
    out.extend_from_slice(&1186u32.to_be_bytes()); // element OID = interval
    out.extend_from_slice(&i32::try_from(elems.len()).expect("len").to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
    for elem in elems {
        out.extend_from_slice(&column(elem.as_deref()));
    }
    out
}

/// A full `ScheduleRow` `DataRow` body.
fn schedule_row(
    id: i32,
    day: &[u8],
    at: &[u8],
    span: &[u8],
    deadline: Option<&[u8]>,
    windows: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&6i16.to_be_bytes()); // 6 columns
    out.extend_from_slice(&column(Some(&id.to_be_bytes())));
    out.extend_from_slice(&column(Some(day)));
    out.extend_from_slice(&column(Some(at)));
    out.extend_from_slice(&column(Some(span)));
    out.extend_from_slice(&column(deadline));
    out.extend_from_slice(&column(Some(windows)));
    out
}

#[test]
fn schedule_row_decodes_exact_temporal_values() {
    // day = 2000-02-29 (leap day, 59 days after the epoch).
    let day = date_body(59);
    // at = 12:34:56.789012.
    let at = time_body(45_296_789_012);
    // span = 1 year 2 mons 3 days 04:05:06 -> months 14, days 3, micros 14_706_000_000.
    let span = interval_body(14, 3, 14_706_000_000);
    // windows = { '01:02:03', NULL, '-1 day' }.
    let one_two_three = interval_body(0, 0, 3_723_000_000);
    let neg_day = interval_body(0, -1, 0);
    let windows = interval_array_body(&[Some(one_two_three), None, Some(neg_day)]);
    // deadline = SQL NULL.
    let body = schedule_row(1, &day, &at, &span, None, &windows);

    let row = ScheduleRow::decode(&body).expect("row decodes");
    assert_eq!(row.id, 1);
    assert_eq!(row.day.to_string(), "2000-02-29");
    assert_eq!(row.at.to_string(), "12:34:56.789012");
    assert_eq!(row.span.to_string(), "1 year 2 mons 3 days 04:05:06");
    assert!(row.deadline.is_none(), "deadline is SQL NULL");
    let rendered: Vec<Option<String>> = row
        .windows
        .iter()
        .map(|e| e.as_ref().map(ToString::to_string))
        .collect();
    assert_eq!(
        rendered,
        vec![
            Some("01:02:03".to_string()),
            None,
            Some("-1 days".to_string()),
        ],
        "windows renders 01:02:03, NULL, -1 days",
    );
}

#[test]
fn temporal_cast_columns_decode() {
    // '2000-02-29'::date -> day 59.
    let mut d = Vec::new();
    d.extend_from_slice(&1i16.to_be_bytes());
    d.extend_from_slice(&column(Some(&date_body(59))));
    assert_eq!(DateCast::decode(&d).expect("decodes").d.to_string(), "2000-02-29");

    // '12:34:56.789012'::time.
    let mut t = Vec::new();
    t.extend_from_slice(&1i16.to_be_bytes());
    t.extend_from_slice(&column(Some(&time_body(45_296_789_012))));
    assert_eq!(TimeCast::decode(&t).expect("decodes").t.to_string(), "12:34:56.789012");

    // '1 year 2 mons 3 days 04:05:06'::interval.
    let mut i = Vec::new();
    i.extend_from_slice(&1i16.to_be_bytes());
    i.extend_from_slice(&column(Some(&interval_body(14, 3, 14_706_000_000))));
    assert_eq!(
        IntervalCast::decode(&i).expect("decodes").i.to_string(),
        "1 year 2 mons 3 days 04:05:06",
    );
}

#[test]
fn temporal_null_in_not_null_column_is_classified() {
    // `day` is NOT NULL; a SQL NULL there is a classified decode error on the
    // record, never a silent default.
    let windows = interval_array_body(&[]);
    let mut with_null = Vec::new();
    with_null.extend_from_slice(&6i16.to_be_bytes());
    with_null.extend_from_slice(&column(Some(&1i32.to_be_bytes()))); // id
    with_null.extend_from_slice(&column(None)); // day = NULL (NOT NULL column)
    with_null.extend_from_slice(&column(Some(&time_body(0)))); // at
    with_null.extend_from_slice(&column(Some(&interval_body(0, 0, 0)))); // span
    with_null.extend_from_slice(&column(None)); // deadline = NULL
    with_null.extend_from_slice(&column(Some(&windows))); // windows

    assert!(
        ScheduleRow::decode(&with_null).is_err(),
        "a NULL in the NOT-NULL date column must be a classified error",
    );
}
