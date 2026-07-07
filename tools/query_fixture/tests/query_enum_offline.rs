//! OFFLINE proof of the generated user-enum codegen — no PostgreSQL.
//!
//! `bsql::user_types!()` turns the `0014_moods.sql` migration
//! (`CREATE TYPE mood AS ENUM ('happy', 'sad', 'ok', 'in_progress')`) into a
//! real Rust `enum Mood`. This exercises the GENERATED mapping directly, with
//! NO live server — the codegen itself is the unit under test:
//!
//!   * label/variant round-trips (`wire_label` / `from_wire_label`),
//!   * an unknown label is a classified `UnknownEnumLabel`, never a panic,
//!   * snake_case labels PascalCase to variants (`in_progress` -> `InProgress`),
//!   * the derived `Ord` follows the DECLARED (PostgreSQL sort) order.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test harness — expect/unwrap surface failures loudly"
)]

use bsql::{DecodeError, PgEnum as _};

bsql::user_types!();

#[test]
fn generated_enum_round_trips_every_label() {
    // Declared order is preserved as the variant order.
    let pairs = [
        (Mood::Happy, "happy"),
        (Mood::Sad, "sad"),
        (Mood::Ok, "ok"),
        (Mood::InProgress, "in_progress"),
    ];
    for (variant, label) in pairs {
        assert_eq!(variant.label(), label, "wire_label maps the variant to its label");
        assert_eq!(
            Mood::from_wire_label(label).expect("known label decodes"),
            variant,
            "from_wire_label is the inverse over known labels"
        );
    }
}

#[test]
fn unknown_label_is_classified_never_a_panic() {
    match Mood::from_wire_label("ecstatic") {
        Err(DecodeError::UnknownEnumLabel) => {}
        other => panic!("expected a classified UnknownEnumLabel, got {other:?}"),
    }
    // A label off by case is NOT the same enum member (labels are
    // case-sensitive, unlike identifiers) — also classified.
    assert!(matches!(
        Mood::from_wire_label("Happy"),
        Err(DecodeError::UnknownEnumLabel)
    ));
}

#[test]
fn snake_case_label_pascal_cases_to_a_variant() {
    // `in_progress` -> `InProgress`, and it round-trips to the exact label.
    assert_eq!(Mood::InProgress.label(), "in_progress");
}

#[test]
fn derived_ord_follows_declared_postgres_sort_order() {
    // The declared label order IS PostgreSQL's enum sort order; the derived
    // `Ord` mirrors it (variant order = declaration order).
    assert!(Mood::Happy < Mood::Sad);
    assert!(Mood::Sad < Mood::Ok);
    assert!(Mood::Ok < Mood::InProgress);
    let mut all = [Mood::InProgress, Mood::Happy, Mood::Ok, Mood::Sad];
    all.sort();
    assert_eq!(all, [Mood::Happy, Mood::Sad, Mood::Ok, Mood::InProgress]);
}

#[test]
fn alter_type_evolution_reaches_the_generated_enum() {
    // `priority` (0016): CREATE ('low','high'), ADD 'medium' AFTER 'low', ADD
    // 'urgent' (append), RENAME 'high' -> 'critical' => [low, medium, critical,
    // urgent]. A silent ALTER-TYPE drop would leave the generated enum missing
    // the added variant / carrying the pre-rename label — this proves it did NOT.
    assert_eq!(Priority::Low.label(), "low");
    assert_eq!(Priority::Medium.label(), "medium"); // the ADD VALUE variant
    assert_eq!(Priority::Critical.label(), "critical"); // the RENAME VALUE variant
    assert_eq!(Priority::Urgent.label(), "urgent");

    // The added / renamed labels decode BACK into the generated variants (not
    // UnknownEnumLabel) — exactly the drift the silent-skip bug would have caused.
    assert!(matches!(
        Priority::from_wire_label("medium"),
        Ok(Priority::Medium)
    ));
    assert!(matches!(
        Priority::from_wire_label("critical"),
        Ok(Priority::Critical)
    ));
    // The PRE-rename label is gone — a live row still carrying 'high' is caught
    // loudly, never silently mis-mapped.
    assert!(matches!(
        Priority::from_wire_label("high"),
        Err(DecodeError::UnknownEnumLabel)
    ));

    // Declared order is preserved through the ALTERs (it feeds the derived Ord).
    assert!(Priority::Low < Priority::Medium);
    assert!(Priority::Medium < Priority::Critical);
    assert!(Priority::Critical < Priority::Urgent);

    // RENAME TO: `tshirt` -> `garment_size` — the type exists under its NEW name
    // with its labels intact.
    assert_eq!(GarmentSize::S.label(), "s");
    assert_eq!(GarmentSize::M.label(), "m");
    assert_eq!(GarmentSize::L.label(), "l");
}

#[test]
fn as_label_binds_the_enum_specifically() {
    // `as_label` yields an `EnumLabel<Mood>` — a distinct type per enum, so a
    // query expecting one enum cannot be handed another's label (a compile-time
    // guarantee; here we just witness the value constructs and is Copy).
    let label = Mood::Happy.as_label();
    let copied = label;
    let _ = (label, copied);
}
