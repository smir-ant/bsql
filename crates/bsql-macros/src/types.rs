//! Extended OID-to-Rust-type resolution with feature-gate awareness.
//!
//! The proc macro must decide which Rust type to use for a given PostgreSQL OID.
//! Base types (i32, String, etc.) are always available. Feature-gated types
//! (time::OffsetDateTime, uuid::Uuid, etc.) require the corresponding feature
//! flag to be enabled.
//!
//! This module lives in the proc macro crate because `cfg!(feature = "...")` is
//! evaluated at the proc macro's own compile time — which is controlled by the
//! feature chain: `bsql/time` -> `bsql-macros/time`.

/// Resolve a PostgreSQL OID to a Rust type string, considering enabled features.
///
/// Returns `Ok(type_string)` for recognized OIDs, or `Err(message)` if the OID
/// requires an unabled feature or is completely unsupported.
pub fn resolve_rust_type(oid: u32) -> Result<&'static str, String> {
    // Base types — always available
    if let Some(t) = bsql_core::types::rust_type_for_oid(oid) {
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
        1266 => Err(format!(
            "column type is TIMETZ (OID {oid}) which is not supported by bsql. \
                 Use TIME or TIMESTAMPTZ instead."
        )),
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
        // --- Timestamp/Date/Time arrays ---
        1115 => resolve_array("TIMESTAMP[]", 1114, oid),
        1185 => resolve_array("TIMESTAMPTZ[]", 1184, oid),
        1182 => resolve_array("DATE[]", 1082, oid),
        1183 => resolve_array("TIME[]", 1083, oid),
        2951 => resolve_array("UUID[]", 2950, oid),
        1231 => resolve_array("NUMERIC[]", 1700, oid),
        // --- Network type arrays ---
        1041 => Ok("Vec<String>"), // inet[]
        651 => Ok("Vec<String>"),  // cidr[]
        1040 => Ok("Vec<String>"), // macaddr[]
        // --- Interval array ---
        1187 => Ok("Vec<String>"), // interval[]
        705 => Err(
            "PostgreSQL could not determine the type of this expression (OID 705 = unknown). \
             This usually happens with untyped parameters inside functions like unnest(). \
             Add an explicit cast: unnest($param::int[]), unnest($param::text[]), etc."
                .into(),
        ),
        _ => {
            let name = bsql_core::types::pg_name_for_oid(oid).unwrap_or("unknown");
            Err(format!(
                "unsupported PostgreSQL type `{name}` (OID {oid}). \
                 Enable the appropriate feature flag in bsql or cast to a supported type."
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
        _ => Err(format!(
            "unsupported PostgreSQL array type `{pg_name}` (OID {array_oid})."
        )),
    }
}

/// Check whether a user-declared Rust parameter type is compatible with a PG OID.
///
/// Extends `bsql_core::types::is_param_compatible` with feature-gated types.
pub fn is_param_compatible_extended(rust_type: &str, pg_oid: u32) -> bool {
    // Base types first (is_param_compatible strips Option<> internally)
    if bsql_core::types::is_param_compatible(rust_type, pg_oid) {
        return true;
    }

    // Strip Option<> wrapper — Option<T> is nullable T, same PG type as T.
    let rust_type = strip_option(rust_type);
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
        (
            "::chrono::DateTime<::chrono::Utc>"
            | "chrono::DateTime<chrono::Utc>"
            | "chrono::DateTime<Utc>",
            1184,
        ) => {
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

/// Strip `Option<...>` wrapper from a type string, returning the inner type.
/// If the type is not `Option<T>`, returns it unchanged.
fn strip_option(ty: &str) -> &str {
    if let Some(inner) = ty.strip_prefix("Option<") {
        if let Some(inner) = inner.strip_suffix('>') {
            return inner;
        }
    }
    ty
}

/// Build a clear error message for a type that requires an unabled feature.
fn feature_error(pg_type: &str, oid: u32, features: &[&str]) -> String {
    let features_str = features
        .iter()
        .map(|f| format!("\"{f}\""))
        .collect::<Vec<_>>()
        .join(" or ");
    format!("column type is {pg_type} (OID {oid}) — enable feature {features_str} in bsql")
}

/// Returns true if the Rust type is a known scalar/array type that is
/// provably incompatible with PG enum parameters.
///
/// Used by `validate.rs` to reject obviously wrong types for PG enum columns
/// (e.g. `$status: i32` on an enum column) while still allowing unknown types
/// that might be `#[bsql::pg_enum]` user enums.
pub fn is_known_non_enum_type(rust_type: &str) -> bool {
    matches!(
        rust_type,
        "bool"
            | "i16"
            | "i32"
            | "i64"
            | "f32"
            | "f64"
            | "u32"
            | "Vec<u8>"
            | "&[u8]"
            | "Vec<bool>"
            | "Vec<i16>"
            | "Vec<i32>"
            | "Vec<i64>"
            | "Vec<f32>"
            | "Vec<f64>"
            | "Vec<String>"
            | "&[bool]"
            | "&[i16]"
            | "&[i32]"
            | "&[i64]"
            | "&[f32]"
            | "&[f64]"
            | "&[&str]"
            | "&[String]"
    ) || rust_type.starts_with("::time::")
        || rust_type.starts_with("time::")
        || rust_type.starts_with("::chrono::")
        || rust_type.starts_with("chrono::")
        || rust_type.starts_with("::uuid::")
        || rust_type.starts_with("uuid::")
        || rust_type.starts_with("::rust_decimal::")
        || rust_type.starts_with("rust_decimal::")
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
        assert_eq!(
            resolve_rust_type(1114).unwrap(),
            "::time::PrimitiveDateTime"
        );
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
        assert_eq!(
            resolve_rust_type(1184).unwrap(),
            "::chrono::DateTime<::chrono::Utc>"
        );
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
        assert!(is_param_compatible_extended(
            "::chrono::NaiveDateTime",
            1114
        ));
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn numeric_resolves_to_decimal() {
        assert_eq!(resolve_rust_type(1700).unwrap(), "::rust_decimal::Decimal");
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_param_compat() {
        assert!(is_param_compatible_extended(
            "::rust_decimal::Decimal",
            1700
        ));
        assert!(is_param_compatible_extended("rust_decimal::Decimal", 1700));
        assert!(is_param_compatible_extended("Decimal", 1700));
    }

    #[cfg(not(feature = "decimal"))]
    #[test]
    fn numeric_errors_without_feature() {
        let err = resolve_rust_type(1700).unwrap_err();
        assert!(err.contains("NUMERIC"), "unexpected error: {err}");
        assert!(
            err.contains("decimal"),
            "should suggest decimal feature: {err}"
        );
    }

    // --- is_known_non_enum_type ---

    #[test]
    fn known_non_enum_scalars() {
        assert!(is_known_non_enum_type("bool"));
        assert!(is_known_non_enum_type("i32"));
        assert!(is_known_non_enum_type("i64"));
        assert!(is_known_non_enum_type("f64"));
        assert!(is_known_non_enum_type("u32"));
    }

    #[test]
    fn known_non_enum_arrays() {
        assert!(is_known_non_enum_type("Vec<u8>"));
        assert!(is_known_non_enum_type("Vec<i32>"));
        assert!(is_known_non_enum_type("&[i64]"));
        assert!(is_known_non_enum_type("&[&str]"));
    }

    #[test]
    fn known_non_enum_crate_types() {
        assert!(is_known_non_enum_type("::time::OffsetDateTime"));
        assert!(is_known_non_enum_type("time::Date"));
        assert!(is_known_non_enum_type("::chrono::NaiveDate"));
        assert!(is_known_non_enum_type("chrono::NaiveDateTime"));
        assert!(is_known_non_enum_type("::uuid::Uuid"));
        assert!(is_known_non_enum_type("uuid::Uuid"));
        assert!(is_known_non_enum_type("::rust_decimal::Decimal"));
        assert!(is_known_non_enum_type("rust_decimal::Decimal"));
    }

    #[test]
    fn str_and_string_not_known_non_enum() {
        assert!(!is_known_non_enum_type("&str"));
        assert!(!is_known_non_enum_type("String"));
    }

    #[test]
    fn unknown_custom_type_not_known_non_enum() {
        assert!(!is_known_non_enum_type("TicketStatus"));
        assert!(!is_known_non_enum_type("MyEnum"));
    }

    #[test]
    fn json_resolves_to_string() {
        assert_eq!(resolve_rust_type(114).unwrap(), "String");
    }

    #[test]
    fn jsonb_resolves_to_string() {
        assert_eq!(resolve_rust_type(3802).unwrap(), "String");
    }

    #[test]
    fn json_array_resolves_to_vec_string() {
        assert_eq!(resolve_rust_type(199).unwrap(), "Vec<String>");
    }

    #[test]
    fn jsonb_array_resolves_to_vec_string() {
        assert_eq!(resolve_rust_type(3807).unwrap(), "Vec<String>");
    }

    #[test]
    fn interval_resolves_to_string() {
        assert_eq!(resolve_rust_type(1186).unwrap(), "String");
    }

    #[test]
    fn inet_resolves_to_string() {
        assert_eq!(resolve_rust_type(869).unwrap(), "String");
    }

    #[test]
    fn cidr_resolves_to_string() {
        assert_eq!(resolve_rust_type(650).unwrap(), "String");
    }

    #[test]
    fn macaddr_resolves_to_string() {
        assert_eq!(resolve_rust_type(829).unwrap(), "String");
    }

    #[test]
    fn json_param_compat() {
        assert!(is_param_compatible_extended("&str", 114));
        assert!(is_param_compatible_extended("String", 114));
        assert!(is_param_compatible_extended("&str", 3802));
        assert!(is_param_compatible_extended("String", 3802));
    }

    #[test]
    fn interval_param_compat() {
        assert!(is_param_compatible_extended("&str", 1186));
        assert!(is_param_compatible_extended("String", 1186));
    }

    #[test]
    fn inet_param_compat() {
        assert!(is_param_compatible_extended("&str", 869));
        assert!(is_param_compatible_extended("String", 869));
    }

    #[test]
    fn inet_array_resolves() {
        assert_eq!(resolve_rust_type(1041).unwrap(), "Vec<String>");
    }

    #[test]
    fn interval_array_resolves() {
        assert_eq!(resolve_rust_type(1187).unwrap(), "Vec<String>");
    }

    #[cfg(feature = "time")]
    #[test]
    fn timestamp_resolves_to_primitive_datetime() {
        assert_eq!(
            resolve_rust_type(1114).unwrap(),
            "::time::PrimitiveDateTime"
        );
    }

    #[cfg(all(feature = "chrono", not(feature = "time")))]
    #[test]
    fn timestamp_resolves_to_naive_datetime() {
        assert_eq!(resolve_rust_type(1114).unwrap(), "::chrono::NaiveDateTime");
    }

    // --- Audit gap tests ---

    // #111: OID 114 (json) resolves to String via resolve_rust_type
    #[test]
    fn resolve_json_oid_114() {
        assert_eq!(resolve_rust_type(114).unwrap(), "String");
    }

    // #113: Array OIDs resolve correctly
    #[test]
    fn resolve_inet_array_oid() {
        assert_eq!(resolve_rust_type(1041).unwrap(), "Vec<String>");
    }

    #[test]
    fn resolve_cidr_array_oid() {
        assert_eq!(resolve_rust_type(651).unwrap(), "Vec<String>");
    }

    #[test]
    fn resolve_macaddr_array_oid() {
        assert_eq!(resolve_rust_type(1040).unwrap(), "Vec<String>");
    }

    #[test]
    fn resolve_interval_array_oid() {
        assert_eq!(resolve_rust_type(1187).unwrap(), "Vec<String>");
    }

    #[test]
    fn resolve_json_array_oid() {
        assert_eq!(resolve_rust_type(199).unwrap(), "Vec<String>");
    }

    #[test]
    fn resolve_jsonb_array_oid() {
        assert_eq!(resolve_rust_type(3807).unwrap(), "Vec<String>");
    }

    // TIMETZ (1266) is unsupported
    #[test]
    fn timetz_oid_unsupported() {
        let err = resolve_rust_type(1266).unwrap_err();
        assert!(err.contains("TIMETZ"), "should mention TIMETZ: {err}");
    }

    // cidr and macaddr resolve to String
    #[test]
    fn resolve_cidr_oid() {
        assert_eq!(resolve_rust_type(650).unwrap(), "String");
    }

    #[test]
    fn resolve_macaddr_oid() {
        assert_eq!(resolve_rust_type(829).unwrap(), "String");
    }

    // is_known_non_enum_type: &[u8] is known non-enum
    #[test]
    fn known_non_enum_byteslice() {
        assert!(is_known_non_enum_type("&[u8]"));
    }

    // is_param_compatible_extended for interval/inet/cidr/macaddr
    #[test]
    fn param_compat_extended_network_types() {
        assert!(is_param_compatible_extended("&str", 869)); // inet
        assert!(is_param_compatible_extended("String", 650)); // cidr
        assert!(is_param_compatible_extended("&str", 829)); // macaddr
        assert!(is_param_compatible_extended("String", 1186)); // interval
    }
}
