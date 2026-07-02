//! Validation of SQL identifiers spliced directly into statement text.
//!
//! A few driver verbs must interpolate a caller-supplied identifier straight
//! into SQL: `COPY <table> FROM STDIN` and `LISTEN <channel>` have no
//! parameterized form for the identifier itself. An un-validated splice is a
//! SQL-injection vector — a `table` of `users; DROP TABLE x --` would run as a
//! second statement.
//!
//! These validators accept ONLY a plain unquoted PostgreSQL identifier
//! (optionally schema-qualified for a table): a leading ASCII letter or `_`,
//! then ASCII letters / digits / `_` / `$`, at most 63 bytes per component.
//! Everything else — spaces, `;`, quotes, comment markers, dots beyond one
//! separator, empty components — is rejected as a classified
//! [`DriverError::Config`], so an injection-shaped string can never reach the
//! interpolated SQL. An identifier that must be double-quoted (special
//! characters) is deliberately rejected rather than quote-escaped: rejection is
//! injection-proof by construction, and no driver path needs quoted names.

use crate::error::DriverError;

/// Maximum bytes in one identifier component (PG `NAMEDATALEN - 1`).
const MAX_COMPONENT_LEN: usize = 63;

/// Whether `s` is a single unquoted PG identifier: non-empty, ≤ 63 bytes, a
/// leading ASCII letter or `_`, then ASCII letters / digits / `_` / `$`.
fn is_unquoted_identifier(s: &str) -> bool {
    let bytes = s.as_bytes();
    let Some((&first, rest)) = bytes.split_first() else {
        return false; // empty
    };
    if bytes.len() > MAX_COMPONENT_LEN {
        return false;
    }
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    rest.iter().all(|&b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

/// Validate a single unquoted SQL identifier (e.g. a `LISTEN` / `UNLISTEN`
/// channel) for safe interpolation into statement text.
///
/// # Errors
///
/// [`DriverError::Config`] if `name` is not a plain unquoted identifier.
pub fn validate_identifier(name: &str) -> Result<(), DriverError> {
    if is_unquoted_identifier(name) {
        Ok(())
    } else {
        Err(DriverError::Config(
            "invalid SQL identifier: expected an unquoted name (a letter or '_' \
             followed by letters, digits, '_', or '$', at most 63 bytes)",
        ))
    }
}

/// Validate an optionally schema-qualified table identifier (`table` or
/// `schema.table`) for safe interpolation into a `COPY` statement.
///
/// # Errors
///
/// [`DriverError::Config`] if `table` is not one or two unquoted identifier
/// components separated by a single `.`.
pub fn validate_table(table: &str) -> Result<(), DriverError> {
    let mut parts = table.split('.');
    let ok = match (parts.next(), parts.next(), parts.next()) {
        (Some(one), None, _) => is_unquoted_identifier(one),
        (Some(schema), Some(name), None) => {
            is_unquoted_identifier(schema) && is_unquoted_identifier(name)
        }
        // Empty input, or more than two dot-separated components.
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(DriverError::Config(
            "invalid COPY table identifier: expected an unquoted `table` or \
             `schema.table` (each component a letter or '_' followed by letters, \
             digits, '_', or '$', at most 63 bytes)",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_identifier, validate_table};
    use crate::error::DriverError;

    fn is_config_err<T: core::fmt::Debug>(r: Result<T, DriverError>) -> bool {
        matches!(r, Err(DriverError::Config(_)))
    }

    #[test]
    fn plain_identifiers_are_accepted() {
        for ok in ["users", "_hidden", "t1", "my_table", "col$x", "A", "Table123"] {
            assert!(validate_identifier(ok).is_ok(), "{ok:?} should be a valid identifier");
            assert!(validate_table(ok).is_ok(), "{ok:?} should be a valid table");
        }
    }

    #[test]
    fn schema_qualified_tables_are_accepted() {
        assert!(validate_table("public.my_table").is_ok());
        assert!(validate_table("s.t").is_ok());
        // …but a schema-qualified name is NOT a single identifier.
        assert!(is_config_err(validate_identifier("public.my_table")));
    }

    #[test]
    fn injection_shaped_identifiers_are_rejected() {
        for bad in [
            "users; DROP TABLE x --",
            "\"; DROP TABLE x; --",
            "users WHERE 1=1",
            "a b",
            "a'b",
            "a\"b",
            "a;b",
            "a--b",
            "a\nb",
            "1abc",  // leading digit
            "$x",    // leading '$'
            "",      // empty
            ".",     // empty components
            "a.",    // trailing empty component
            ".a",    // leading empty component
            "a.b.c", // three components
        ] {
            assert!(is_config_err(validate_table(bad)), "{bad:?} must be rejected as a table");
            assert!(
                is_config_err(validate_identifier(bad)),
                "{bad:?} must be rejected as an identifier",
            );
        }
    }

    #[test]
    fn over_length_component_is_rejected() {
        let too_long = "a".repeat(64);
        assert!(is_config_err(validate_identifier(&too_long)));
        assert!(is_config_err(validate_table(&too_long)));
        // Exactly 63 bytes is the boundary and is accepted.
        let at_cap = "a".repeat(63);
        assert!(validate_identifier(&at_cap).is_ok());
    }
}
