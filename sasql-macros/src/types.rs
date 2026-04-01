//! Extended OID-to-Rust-type resolution with feature-gate awareness.
//!
//! The proc macro must decide which Rust type to use for a given PostgreSQL OID.
//! Base types (i32, String, etc.) are always available. Feature-gated types
//! (time::OffsetDateTime, uuid::Uuid, etc.) require the corresponding feature
//! flag to be enabled.
//!
//! This module lives in the proc macro crate because `cfg!(feature = "...")` is
//! evaluated at the proc macro's own compile time — which is controlled by the
//! feature chain: `sasql/time` -> `sasql-macros/time`.

/// Resolve a PostgreSQL OID to a Rust type string, considering enabled features.
///
/// Returns `Ok(type_string)` for recognized OIDs, or `Err(message)` if the OID
/// requires an unabled feature or is completely unsupported.
pub fn resolve_rust_type(oid: u32) -> Result<&'static str, String> {
    // Base types — always available
    if let Some(t) = sasql_core::types::rust_type_for_oid(oid) {
        return Ok(t);
    }

    match oid {
        // --- Timestamp with time zone (TIMESTAMPTZ) ---
        1184 => {
            if cfg!(feature = "time") {
                Ok("::time::OffsetDateTime")
            } else if cfg!(feature = "chrono") {
                Ok("::chrono::DateTime<::chrono::Utc>")
            } else {
                Err(feature_error("TIMESTAMPTZ", oid, &["time", "chrono"]))
            }
        }
        // --- Timestamp without time zone (TIMESTAMP) ---
        1114 => {
            if cfg!(feature = "time") {
                Ok("::time::PrimitiveDateTime")
            } else if cfg!(feature = "chrono") {
                Ok("::chrono::NaiveDateTime")
            } else {
                Err(feature_error("TIMESTAMP", oid, &["time", "chrono"]))
            }
        }
        // --- Date ---
        1082 => {
            if cfg!(feature = "time") {
                Ok("::time::Date")
            } else if cfg!(feature = "chrono") {
                Ok("::chrono::NaiveDate")
            } else {
                Err(feature_error("DATE", oid, &["time", "chrono"]))
            }
        }
        // --- Time (without time zone) ---
        1083 => {
            if cfg!(feature = "time") {
                Ok("::time::Time")
            } else if cfg!(feature = "chrono") {
                Ok("::chrono::NaiveTime")
            } else {
                Err(feature_error("TIME", oid, &["time", "chrono"]))
            }
        }
        // --- Time with time zone (TIMETZ) ---
        // 1266 is TIMETZ, rare but handle it
        1266 => {
            Err(format!(
                "column type is TIMETZ (OID {oid}) which is not supported by sasql. \
                 Use TIME or TIMESTAMPTZ instead."
            ))
        }
        // --- UUID ---
        2950 => {
            if cfg!(feature = "uuid") {
                Ok("::uuid::Uuid")
            } else {
                Err(feature_error("UUID", oid, &["uuid"]))
            }
        }
        // --- NUMERIC / DECIMAL ---
        1700 => {
            if cfg!(feature = "decimal") {
                Ok("::rust_decimal::Decimal")
            } else {
                Err(feature_error("NUMERIC", oid, &["decimal"]))
            }
        }
        // --- INTERVAL ---
        1186 => {
            Err(format!(
                "column type is INTERVAL (OID {oid}) which is not yet supported by sasql. \
                 Cast to a supported type or track the sasql issue for INTERVAL support."
            ))
        }
        // --- Timestamp/Date/Time arrays ---
        1115 => resolve_array("TIMESTAMP[]", 1114, oid),
        1185 => resolve_array("TIMESTAMPTZ[]", 1184, oid),
        1182 => resolve_array("DATE[]", 1082, oid),
        1183 => resolve_array("TIME[]", 1083, oid),
        2951 => resolve_array("UUID[]", 2950, oid),
        1231 => resolve_array("NUMERIC[]", 1700, oid),
        _ => {
            let name = sasql_core::types::pg_name_for_oid(oid).unwrap_or("unknown");
            Err(format!(
                "unsupported PostgreSQL type `{name}` (OID {oid}). \
                 Enable the appropriate feature flag in sasql or cast to a supported type."
            ))
        }
    }
}

/// Resolve an array type by delegating to the element type.
fn resolve_array(pg_name: &str, element_oid: u32, array_oid: u32) -> Result<&'static str, String> {
    match element_oid {
        1184 => {
            if cfg!(feature = "time") {
                Ok("Vec<::time::OffsetDateTime>")
            } else if cfg!(feature = "chrono") {
                Ok("Vec<::chrono::DateTime<::chrono::Utc>>")
            } else {
                Err(feature_error(pg_name, array_oid, &["time", "chrono"]))
            }
        }
        1114 => {
            if cfg!(feature = "time") {
                Ok("Vec<::time::PrimitiveDateTime>")
            } else if cfg!(feature = "chrono") {
                Ok("Vec<::chrono::NaiveDateTime>")
            } else {
                Err(feature_error(pg_name, array_oid, &["time", "chrono"]))
            }
        }
        1082 => {
            if cfg!(feature = "time") {
                Ok("Vec<::time::Date>")
            } else if cfg!(feature = "chrono") {
                Ok("Vec<::chrono::NaiveDate>")
            } else {
                Err(feature_error(pg_name, array_oid, &["time", "chrono"]))
            }
        }
        1083 => {
            if cfg!(feature = "time") {
                Ok("Vec<::time::Time>")
            } else if cfg!(feature = "chrono") {
                Ok("Vec<::chrono::NaiveTime>")
            } else {
                Err(feature_error(pg_name, array_oid, &["time", "chrono"]))
            }
        }
        2950 => {
            if cfg!(feature = "uuid") {
                Ok("Vec<::uuid::Uuid>")
            } else {
                Err(feature_error(pg_name, array_oid, &["uuid"]))
            }
        }
        1700 => {
            if cfg!(feature = "decimal") {
                Ok("Vec<::rust_decimal::Decimal>")
            } else {
                Err(feature_error(pg_name, array_oid, &["decimal"]))
            }
        }
        _ => {
            Err(format!(
                "unsupported PostgreSQL array type `{pg_name}` (OID {array_oid})."
            ))
        }
    }
}

/// Check whether a user-declared Rust parameter type is compatible with a PG OID.
///
/// Extends `sasql_core::types::is_param_compatible` with feature-gated types.
pub fn is_param_compatible_extended(rust_type: &str, pg_oid: u32) -> bool {
    // Base types first
    if sasql_core::types::is_param_compatible(rust_type, pg_oid) {
        return true;
    }

    match (rust_type, pg_oid) {
        // --- time crate types ---
        ("::time::OffsetDateTime" | "time::OffsetDateTime" | "OffsetDateTime", 1184) => {
            cfg!(feature = "time")
        }
        ("::time::PrimitiveDateTime" | "time::PrimitiveDateTime" | "PrimitiveDateTime", 1114) => {
            cfg!(feature = "time")
        }
        ("::time::Date" | "time::Date", 1082) => cfg!(feature = "time"),
        ("::time::Time" | "time::Time", 1083) => cfg!(feature = "time"),

        // --- chrono types ---
        ("::chrono::DateTime<::chrono::Utc>" | "chrono::DateTime<chrono::Utc>" | "chrono::DateTime<Utc>", 1184) => {
            cfg!(feature = "chrono")
        }
        ("::chrono::NaiveDateTime" | "chrono::NaiveDateTime" | "NaiveDateTime", 1114) => {
            cfg!(feature = "chrono")
        }
        ("::chrono::NaiveDate" | "chrono::NaiveDate" | "NaiveDate", 1082) => {
            cfg!(feature = "chrono")
        }
        ("::chrono::NaiveTime" | "chrono::NaiveTime" | "NaiveTime", 1083) => {
            cfg!(feature = "chrono")
        }

        // --- uuid ---
        ("::uuid::Uuid" | "uuid::Uuid" | "Uuid", 2950) => cfg!(feature = "uuid"),

        // --- rust_decimal ---
        ("::rust_decimal::Decimal" | "rust_decimal::Decimal" | "Decimal", 1700) => {
            cfg!(feature = "decimal")
        }

        _ => false,
    }
}

/// Build a clear error message for a type that requires an unabled feature.
fn feature_error(pg_type: &str, oid: u32, features: &[&str]) -> String {
    let features_str = features
        .iter()
        .map(|f| format!("\"{f}\""))
        .collect::<Vec<_>>()
        .join(" or ");
    format!(
        "column type is {pg_type} (OID {oid}) — enable feature {features_str} in sasql"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_types_resolve() {
        assert_eq!(resolve_rust_type(23).unwrap(), "i32");
        assert_eq!(resolve_rust_type(25).unwrap(), "String");
        assert_eq!(resolve_rust_type(16).unwrap(), "bool");
    }

    #[test]
    fn unknown_oid_errors() {
        assert!(resolve_rust_type(99999).is_err());
    }

    // Feature-dependent tests: these pass based on which features are active
    // during `cargo test` of the proc macro crate.

    #[cfg(feature = "time")]
    #[test]
    fn timestamptz_resolves_to_time() {
        assert_eq!(resolve_rust_type(1184).unwrap(), "::time::OffsetDateTime");
        assert_eq!(resolve_rust_type(1114).unwrap(), "::time::PrimitiveDateTime");
        assert_eq!(resolve_rust_type(1082).unwrap(), "::time::Date");
        assert_eq!(resolve_rust_type(1083).unwrap(), "::time::Time");
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn uuid_resolves() {
        assert_eq!(resolve_rust_type(2950).unwrap(), "::uuid::Uuid");
    }

    #[cfg(not(any(feature = "time", feature = "chrono")))]
    #[test]
    fn timestamptz_errors_without_feature() {
        let err = resolve_rust_type(1184).unwrap_err();
        assert!(err.contains("TIMESTAMPTZ"), "unexpected error: {err}");
        assert!(err.contains("time"), "should suggest time feature: {err}");
    }

    #[cfg(not(feature = "uuid"))]
    #[test]
    fn uuid_errors_without_feature() {
        let err = resolve_rust_type(2950).unwrap_err();
        assert!(err.contains("UUID"), "unexpected error: {err}");
        assert!(err.contains("uuid"), "should suggest uuid feature: {err}");
    }

    #[test]
    fn base_param_compat_still_works() {
        assert!(is_param_compatible_extended("i32", 23));
        assert!(is_param_compatible_extended("&str", 25));
        assert!(!is_param_compatible_extended("i32", 25));
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn uuid_param_compat() {
        assert!(is_param_compatible_extended("::uuid::Uuid", 2950));
        assert!(is_param_compatible_extended("uuid::Uuid", 2950));
        assert!(is_param_compatible_extended("Uuid", 2950));
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_param_compat() {
        assert!(is_param_compatible_extended("::time::OffsetDateTime", 1184));
        assert!(is_param_compatible_extended("time::OffsetDateTime", 1184));
        assert!(is_param_compatible_extended("OffsetDateTime", 1184));
        assert!(is_param_compatible_extended("::time::Date", 1082));
    }

    #[cfg(all(feature = "chrono", not(feature = "time")))]
    #[test]
    fn timestamptz_resolves_to_chrono() {
        assert_eq!(resolve_rust_type(1184).unwrap(), "::chrono::DateTime<::chrono::Utc>");
        assert_eq!(resolve_rust_type(1114).unwrap(), "::chrono::NaiveDateTime");
        assert_eq!(resolve_rust_type(1082).unwrap(), "::chrono::NaiveDate");
        assert_eq!(resolve_rust_type(1083).unwrap(), "::chrono::NaiveTime");
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn chrono_param_compat() {
        assert!(is_param_compatible_extended("::chrono::NaiveDate", 1082));
        assert!(is_param_compatible_extended("chrono::NaiveDate", 1082));
        assert!(is_param_compatible_extended("NaiveDate", 1082));
        assert!(is_param_compatible_extended("::chrono::NaiveDateTime", 1114));
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn numeric_resolves_to_decimal() {
        assert_eq!(resolve_rust_type(1700).unwrap(), "::rust_decimal::Decimal");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_param_compat() {
        assert!(is_param_compatible_extended("::rust_decimal::Decimal", 1700));
        assert!(is_param_compatible_extended("rust_decimal::Decimal", 1700));
        assert!(is_param_compatible_extended("Decimal", 1700));
    }

    #[cfg(not(feature = "decimal"))]
    #[test]
    fn numeric_errors_without_feature() {
        let err = resolve_rust_type(1700).unwrap_err();
        assert!(err.contains("NUMERIC"), "unexpected error: {err}");
        assert!(err.contains("decimal"), "should suggest decimal feature: {err}");
    }
}
