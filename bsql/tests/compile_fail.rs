#[test]
fn compile_fail_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}

/// These tests verify that using a PG type without the required feature
/// produces a clear compile error. They only run when the feature is
/// NOT enabled — otherwise the code compiles successfully (which is correct).
#[test]
#[cfg(not(feature = "time"))]
fn compile_fail_missing_time_feature() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail_features/missing_time.rs");
}

#[test]
#[cfg(not(feature = "uuid"))]
fn compile_fail_missing_uuid_feature() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail_features/missing_uuid.rs");
}

#[test]
#[cfg(not(feature = "decimal"))]
fn compile_fail_missing_decimal_feature() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail_features/missing_decimal.rs");
}
