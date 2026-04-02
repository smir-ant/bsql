//! Shared utility functions used across bsql-core modules.

use crate::error::{BsqlResult, ConnectError};

/// Validate a savepoint name: must be a valid SQL identifier.
///
/// Rules:
/// - Non-empty, at most 63 characters (PG's `NAMEDATALEN - 1`)
/// - Starts with an ASCII letter or underscore
/// - Contains only ASCII letters, digits, and underscores
pub fn validate_savepoint_name(name: &str) -> BsqlResult<()> {
    if name.is_empty() {
        return Err(ConnectError::create("savepoint name must not be empty"));
    }
    if name.len() > 63 {
        return Err(ConnectError::create(
            "savepoint name must not exceed 63 characters",
        ));
    }
    let first = name.as_bytes()[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return Err(ConnectError::create(
            "savepoint name must start with a letter or underscore",
        ));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(ConnectError::create(
            "savepoint name must contain only ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_savepoint_name_valid() {
        assert!(validate_savepoint_name("sp1").is_ok());
        assert!(validate_savepoint_name("_sp").is_ok());
        assert!(validate_savepoint_name("my_savepoint_123").is_ok());
    }

    #[test]
    fn validate_savepoint_name_empty() {
        assert!(validate_savepoint_name("").is_err());
    }

    #[test]
    fn validate_savepoint_name_too_long() {
        let long = "a".repeat(64);
        assert!(validate_savepoint_name(&long).is_err());
    }

    #[test]
    fn validate_savepoint_name_max_length() {
        let max = "a".repeat(63);
        assert!(validate_savepoint_name(&max).is_ok());
    }

    #[test]
    fn validate_savepoint_name_starts_with_digit() {
        assert!(validate_savepoint_name("1sp").is_err());
    }

    #[test]
    fn validate_savepoint_name_starts_with_underscore() {
        assert!(validate_savepoint_name("_sp").is_ok());
    }

    #[test]
    fn validate_savepoint_name_special_chars() {
        assert!(validate_savepoint_name("sp-1").is_err());
        assert!(validate_savepoint_name("sp.1").is_err());
        assert!(validate_savepoint_name("sp 1").is_err());
        assert!(validate_savepoint_name("sp;1").is_err());
        assert!(validate_savepoint_name("sp'1").is_err());
    }
}
