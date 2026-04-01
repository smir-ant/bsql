//! PostgreSQL OID to Rust type mapping.
//!
//! This table is the single source of truth for how PostgreSQL types map to Rust
//! types in sasql v0.1. Both the proc macro (at compile time) and the runtime
//! (for error messages) reference this mapping.

/// A single entry in the PG-to-Rust type map.
#[derive(Debug, Clone, Copy)]
pub struct TypeMapping {
    /// The PostgreSQL OID for this type.
    pub pg_oid: u32,
    /// The PostgreSQL type name (e.g. `"int4"`, `"text"`).
    pub pg_name: &'static str,
    /// The Rust type string used in generated code (e.g. `"i32"`, `"String"`).
    pub rust_type: &'static str,
    /// Whether this is an array type.
    pub is_array: bool,
}

/// Base type mappings for sasql v0.1. No feature-gated types.
///
/// OIDs sourced from `pg_type.dat` in the PostgreSQL source tree.
/// <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>
pub const BASE_TYPE_MAP: &[TypeMapping] = &[
    // Scalar types
    TypeMapping { pg_oid: 16,   pg_name: "bool",    rust_type: "bool",       is_array: false },
    TypeMapping { pg_oid: 21,   pg_name: "int2",    rust_type: "i16",        is_array: false },
    TypeMapping { pg_oid: 23,   pg_name: "int4",    rust_type: "i32",        is_array: false },
    TypeMapping { pg_oid: 20,   pg_name: "int8",    rust_type: "i64",        is_array: false },
    TypeMapping { pg_oid: 700,  pg_name: "float4",  rust_type: "f32",        is_array: false },
    TypeMapping { pg_oid: 701,  pg_name: "float8",  rust_type: "f64",        is_array: false },
    TypeMapping { pg_oid: 25,   pg_name: "text",    rust_type: "String",     is_array: false },
    TypeMapping { pg_oid: 1043, pg_name: "varchar",  rust_type: "String",    is_array: false },
    TypeMapping { pg_oid: 1042, pg_name: "bpchar",   rust_type: "String",    is_array: false },
    TypeMapping { pg_oid: 17,   pg_name: "bytea",    rust_type: "Vec<u8>",   is_array: false },
    TypeMapping { pg_oid: 26,   pg_name: "oid",      rust_type: "u32",       is_array: false },
    TypeMapping { pg_oid: 2278, pg_name: "void",     rust_type: "()",        is_array: false },
    // Array types
    TypeMapping { pg_oid: 1000, pg_name: "_bool",    rust_type: "Vec<bool>",       is_array: true },
    TypeMapping { pg_oid: 1005, pg_name: "_int2",    rust_type: "Vec<i16>",        is_array: true },
    TypeMapping { pg_oid: 1007, pg_name: "_int4",    rust_type: "Vec<i32>",        is_array: true },
    TypeMapping { pg_oid: 1016, pg_name: "_int8",    rust_type: "Vec<i64>",        is_array: true },
    TypeMapping { pg_oid: 1021, pg_name: "_float4",  rust_type: "Vec<f32>",        is_array: true },
    TypeMapping { pg_oid: 1022, pg_name: "_float8",  rust_type: "Vec<f64>",        is_array: true },
    TypeMapping { pg_oid: 1009, pg_name: "_text",    rust_type: "Vec<String>",     is_array: true },
    TypeMapping { pg_oid: 1015, pg_name: "_varchar",  rust_type: "Vec<String>",    is_array: true },
    TypeMapping { pg_oid: 1001, pg_name: "_bytea",    rust_type: "Vec<Vec<u8>>",   is_array: true },
];

/// Look up the Rust type for a PostgreSQL OID.
///
/// Returns `None` for unrecognized OIDs. The caller should emit a compile error
/// suggesting the user enable the appropriate feature flag.
pub fn rust_type_for_oid(oid: u32) -> Option<&'static str> {
    BASE_TYPE_MAP.iter().find(|m| m.pg_oid == oid).map(|m| m.rust_type)
}

/// Look up the PostgreSQL type name for an OID (for error messages).
pub fn pg_name_for_oid(oid: u32) -> Option<&'static str> {
    BASE_TYPE_MAP.iter().find(|m| m.pg_oid == oid).map(|m| m.pg_name)
}

/// Check whether a user-declared Rust parameter type is compatible with a PG OID.
///
/// This is used by the proc macro to verify that `$id: i32` matches the column
/// type PostgreSQL expects. The check is intentionally strict — no implicit
/// widening (i32 does not match int8).
pub fn is_param_compatible(rust_type: &str, pg_oid: u32) -> bool {
    matches!(
        (rust_type, pg_oid),
        // Exact matches
        ("bool", 16)
        | ("i16", 21)
        | ("i32", 23)
        | ("i64", 20)
        | ("f32", 700)
        | ("f64", 701)
        // String params: &str and String both accepted for text-like columns
        | ("&str", 25) | ("&str", 1043) | ("&str", 1042)
        | ("String", 25) | ("String", 1043) | ("String", 1042)
        // Byte params: &[u8] and Vec<u8> both accepted for bytea
        | ("&[u8]", 17)
        | ("Vec<u8>", 17)
        // OID
        | ("u32", 26)
        // Array params
        | ("&[bool]", 1000) | ("Vec<bool>", 1000)
        | ("&[i16]", 1005)  | ("Vec<i16>", 1005)
        | ("&[i32]", 1007)  | ("Vec<i32>", 1007)
        | ("&[i64]", 1016)  | ("Vec<i64>", 1016)
        | ("&[f32]", 1021)  | ("Vec<f32>", 1021)
        | ("&[f64]", 1022)  | ("Vec<f64>", 1022)
        | ("&[&str]", 1009) | ("&[String]", 1009) | ("Vec<String>", 1009)
        | ("&[&str]", 1015) | ("&[String]", 1015) | ("Vec<String>", 1015)
    )
}

/// A `String` that can be deserialized from PostgreSQL enum types.
///
/// PostgreSQL sends enum values as text (UTF-8 byte slices), but `String`'s
/// `FromSql` implementation only accepts `TEXT`, `VARCHAR`, `BPCHAR`, `NAME`,
/// and `UNKNOWN`. This wrapper adds `Kind::Enum` acceptance.
///
/// Generated code uses this for PG enum columns mapped to `String`. Users
/// who want typed enums should use `#[sasql::pg_enum]` instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EnumString(pub String);

impl std::fmt::Display for EnumString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for EnumString {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for EnumString {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for EnumString {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl<'a> postgres_types::FromSql<'a> for EnumString {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = std::str::from_utf8(raw)?;
        Ok(EnumString(s.to_owned()))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        // Accept both text-like types AND custom enum types
        matches!(
            *ty,
            postgres_types::Type::VARCHAR
                | postgres_types::Type::TEXT
                | postgres_types::Type::BPCHAR
                | postgres_types::Type::NAME
                | postgres_types::Type::UNKNOWN
        ) || matches!(ty.kind(), postgres_types::Kind::Enum(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn all_oids_are_unique() {
        let mut seen = HashSet::new();
        for m in BASE_TYPE_MAP {
            assert!(
                seen.insert(m.pg_oid),
                "duplicate OID {} ({})",
                m.pg_oid,
                m.pg_name
            );
        }
    }

    #[test]
    fn lookup_scalar_types() {
        assert_eq!(rust_type_for_oid(16), Some("bool"));
        assert_eq!(rust_type_for_oid(21), Some("i16"));
        assert_eq!(rust_type_for_oid(23), Some("i32"));
        assert_eq!(rust_type_for_oid(20), Some("i64"));
        assert_eq!(rust_type_for_oid(700), Some("f32"));
        assert_eq!(rust_type_for_oid(701), Some("f64"));
        assert_eq!(rust_type_for_oid(25), Some("String"));
        assert_eq!(rust_type_for_oid(1043), Some("String"));
        assert_eq!(rust_type_for_oid(17), Some("Vec<u8>"));
        assert_eq!(rust_type_for_oid(26), Some("u32"));
    }

    #[test]
    fn lookup_array_types() {
        assert_eq!(rust_type_for_oid(1007), Some("Vec<i32>"));
        assert_eq!(rust_type_for_oid(1009), Some("Vec<String>"));
        assert_eq!(rust_type_for_oid(1001), Some("Vec<Vec<u8>>"));
    }

    #[test]
    fn unknown_oid_returns_none() {
        assert_eq!(rust_type_for_oid(99999), None);
    }

    #[test]
    fn pg_name_lookup() {
        assert_eq!(pg_name_for_oid(23), Some("int4"));
        assert_eq!(pg_name_for_oid(25), Some("text"));
        assert_eq!(pg_name_for_oid(99999), None);
    }

    #[test]
    fn param_compatibility_exact_match() {
        assert!(is_param_compatible("i32", 23));
        assert!(is_param_compatible("i64", 20));
        assert!(is_param_compatible("bool", 16));
        assert!(is_param_compatible("f64", 701));
    }

    #[test]
    fn param_compatibility_string_types() {
        assert!(is_param_compatible("&str", 25));
        assert!(is_param_compatible("&str", 1043));
        assert!(is_param_compatible("String", 25));
    }

    #[test]
    fn param_compatibility_byte_types() {
        assert!(is_param_compatible("&[u8]", 17));
        assert!(is_param_compatible("Vec<u8>", 17));
    }

    #[test]
    fn param_compatibility_array_types() {
        assert!(is_param_compatible("&[i32]", 1007));
        assert!(is_param_compatible("Vec<i32>", 1007));
        assert!(is_param_compatible("&[&str]", 1009));
    }

    #[test]
    fn param_incompatible_rejects_wrong_type() {
        assert!(!is_param_compatible("&str", 23));   // &str for int4
        assert!(!is_param_compatible("i32", 20));    // i32 for int8 (no widening)
        assert!(!is_param_compatible("i64", 23));    // i64 for int4 (no narrowing)
        assert!(!is_param_compatible("bool", 25));   // bool for text
    }

    #[test]
    fn param_incompatible_unknown_oid() {
        assert!(!is_param_compatible("i32", 99999));
    }
}
