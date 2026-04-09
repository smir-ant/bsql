//! Single-source OID mapping: Rust type string -> PostgreSQL OID.
//!
//! This is the canonical source of truth for the Rust type -> PG OID mapping.
//! `bsql-core::types::default_pg_oid_for_rust_type` delegates to this function.

/// Map a Rust type string to the default PostgreSQL OID for that type.
///
/// Returns the OID that a Rust type would naturally correspond to in PG.
/// Returns 0 for unknown types (let PG infer from context).
///
/// Single source of truth for Rust type -> PostgreSQL OID mapping.
///
/// Used by:
/// - `Encode::pg_type_oid()` and `Encode::type_oid()` (runtime delegation)
/// - The proc macro's two-phase PREPARE (Phase 1 sends these OIDs, via bsql-core re-export)
///
/// Every OID value in the codebase should originate from this function.
pub fn default_pg_oid_for_rust_type(rust_type: &str) -> u32 {
    // Strip Option<> wrapper — Option<T> is nullable T, same PG type as T.
    let ty = strip_option_wrapper(rust_type);
    match ty {
        // Scalars
        "bool" => 16,
        "i16" => 21,
        "i32" => 23,
        "i64" => 20,
        "f32" => 700,
        "f64" => 701,
        "&str" | "String" => 25,
        "u32" => 26,
        "&[u8]" | "Vec<u8>" => 17,
        // Arrays
        "&[bool]" | "Vec<bool>" => 1000,
        "&[i16]" | "Vec<i16>" => 1005,
        "&[i32]" | "Vec<i32>" => 1007,
        "&[i64]" | "Vec<i64>" => 1016,
        "&[f32]" | "Vec<f32>" => 1021,
        "&[f64]" | "Vec<f64>" => 1022,
        "&[&str]" | "Vec<String>" | "&[String]" => 1009,
        "&[&[u8]]" | "Vec<Vec<u8>>" => 1001,
        // Feature-gated types
        "uuid::Uuid" | "Uuid" => 2950,
        "time::OffsetDateTime" | "OffsetDateTime" => 1184,
        "time::Date" => 1082,
        "time::Time" => 1083,
        "time::PrimitiveDateTime" | "PrimitiveDateTime" => 1114,
        "chrono::NaiveDateTime" | "NaiveDateTime" => 1114,
        "chrono::DateTime<chrono::Utc>" | "DateTime<Utc>" => 1184,
        "chrono::NaiveDate" | "NaiveDate" => 1082,
        "chrono::NaiveTime" | "NaiveTime" => 1083,
        "rust_decimal::Decimal" | "Decimal" => 1700,
        _ => 0, // unknown → let PG infer
    }
}

/// Strip `Option<...>` wrapper from a type string, returning the inner type.
/// If the type is not `Option<T>`, returns it unchanged.
fn strip_option_wrapper(ty: &str) -> &str {
    if let Some(inner) = ty.strip_prefix("Option<") {
        if let Some(inner) = inner.strip_suffix('>') {
            return inner;
        }
    }
    ty
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pg_oid_scalars() {
        assert_eq!(default_pg_oid_for_rust_type("bool"), 16);
        assert_eq!(default_pg_oid_for_rust_type("i16"), 21);
        assert_eq!(default_pg_oid_for_rust_type("i32"), 23);
        assert_eq!(default_pg_oid_for_rust_type("i64"), 20);
        assert_eq!(default_pg_oid_for_rust_type("f32"), 700);
        assert_eq!(default_pg_oid_for_rust_type("f64"), 701);
        assert_eq!(default_pg_oid_for_rust_type("&str"), 25);
        assert_eq!(default_pg_oid_for_rust_type("String"), 25);
        assert_eq!(default_pg_oid_for_rust_type("u32"), 26);
        assert_eq!(default_pg_oid_for_rust_type("&[u8]"), 17);
        assert_eq!(default_pg_oid_for_rust_type("Vec<u8>"), 17);
    }

    #[test]
    fn default_pg_oid_arrays() {
        assert_eq!(default_pg_oid_for_rust_type("&[bool]"), 1000);
        assert_eq!(default_pg_oid_for_rust_type("Vec<bool>"), 1000);
        assert_eq!(default_pg_oid_for_rust_type("&[i16]"), 1005);
        assert_eq!(default_pg_oid_for_rust_type("Vec<i16>"), 1005);
        assert_eq!(default_pg_oid_for_rust_type("&[i32]"), 1007);
        assert_eq!(default_pg_oid_for_rust_type("Vec<i32>"), 1007);
        assert_eq!(default_pg_oid_for_rust_type("&[i64]"), 1016);
        assert_eq!(default_pg_oid_for_rust_type("Vec<i64>"), 1016);
        assert_eq!(default_pg_oid_for_rust_type("&[f32]"), 1021);
        assert_eq!(default_pg_oid_for_rust_type("Vec<f32>"), 1021);
        assert_eq!(default_pg_oid_for_rust_type("&[f64]"), 1022);
        assert_eq!(default_pg_oid_for_rust_type("Vec<f64>"), 1022);
        assert_eq!(default_pg_oid_for_rust_type("&[&str]"), 1009);
        assert_eq!(default_pg_oid_for_rust_type("Vec<String>"), 1009);
        assert_eq!(default_pg_oid_for_rust_type("&[String]"), 1009);
        assert_eq!(default_pg_oid_for_rust_type("&[&[u8]]"), 1001);
        assert_eq!(default_pg_oid_for_rust_type("Vec<Vec<u8>>"), 1001);
    }

    #[test]
    fn default_pg_oid_feature_gated() {
        assert_eq!(default_pg_oid_for_rust_type("uuid::Uuid"), 2950);
        assert_eq!(default_pg_oid_for_rust_type("Uuid"), 2950);
        assert_eq!(default_pg_oid_for_rust_type("time::OffsetDateTime"), 1184);
        assert_eq!(default_pg_oid_for_rust_type("time::Date"), 1082);
        assert_eq!(default_pg_oid_for_rust_type("time::Time"), 1083);
        assert_eq!(
            default_pg_oid_for_rust_type("time::PrimitiveDateTime"),
            1114
        );
        assert_eq!(default_pg_oid_for_rust_type("chrono::NaiveDateTime"), 1114);
        assert_eq!(
            default_pg_oid_for_rust_type("chrono::DateTime<chrono::Utc>"),
            1184
        );
        assert_eq!(default_pg_oid_for_rust_type("chrono::NaiveDate"), 1082);
        assert_eq!(default_pg_oid_for_rust_type("chrono::NaiveTime"), 1083);
        assert_eq!(default_pg_oid_for_rust_type("rust_decimal::Decimal"), 1700);
    }

    #[test]
    fn default_pg_oid_option_wrapper() {
        assert_eq!(default_pg_oid_for_rust_type("Option<bool>"), 16);
        assert_eq!(default_pg_oid_for_rust_type("Option<i32>"), 23);
        assert_eq!(default_pg_oid_for_rust_type("Option<String>"), 25);
    }

    #[test]
    fn default_pg_oid_unknown() {
        assert_eq!(default_pg_oid_for_rust_type("unknown"), 0);
        assert_eq!(default_pg_oid_for_rust_type(""), 0);
    }
}
