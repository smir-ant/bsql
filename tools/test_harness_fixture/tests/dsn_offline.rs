#![forbid(unsafe_code)]

//! Offline witness: an unset (or malformed) `BSQL_TEST_DSN` produces a loud,
//! actionable error naming the variable — never a silent skip. Deterministic
//! and env-independent (it feeds the pure resolver an explicit lookup result),
//! so it runs under `cargo test` without a server and without racing parallel
//! tests over the global environment.

use std::env::VarError;

#[test]
fn missing_dsn_is_a_loud_named_error() {
    match bsql::__test_rt::resolve_base_config(Err(VarError::NotPresent)) {
        Ok(_) => panic!("a missing BSQL_TEST_DSN must not resolve to a config"),
        Err(msg) => {
            assert!(
                msg.contains(bsql::__test_rt::DSN_ENV),
                "the missing-DSN error must name the env var, got: {msg}",
            );
            assert!(
                msg.contains("postgres://"),
                "the missing-DSN error should show an example DSN, got: {msg}",
            );
        }
    }
}

#[test]
fn malformed_dsn_is_a_loud_named_error() {
    match bsql::__test_rt::resolve_base_config(Ok("this is not a dsn".to_string())) {
        Ok(_) => panic!("a malformed BSQL_TEST_DSN must not resolve to a config"),
        Err(msg) => assert!(
            msg.contains(bsql::__test_rt::DSN_ENV),
            "the malformed-DSN error must name the env var, got: {msg}",
        ),
    }
}
