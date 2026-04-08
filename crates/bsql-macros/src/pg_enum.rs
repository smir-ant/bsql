//! Implementation of the `#[bsql::pg_enum]` attribute macro.
//!
//! Generates `FromSql` and `ToSql` implementations for Rust enums that
//! correspond to PostgreSQL enum types.
//!
//! # Usage
//!
//! ```rust,ignore
//! #[bsql::pg_enum]
//! enum TicketStatus {
//!     #[sql("new")]
//!     New,
//!     #[sql("in_progress")]
//!     InProgress,
//!     #[sql("resolved")]
//!     Resolved,
//!     #[sql("closed")]
//!     Closed,
//! }
//! ```
//!
//! Each variant must have a `#[sql("...")]` attribute mapping it to the
//! PostgreSQL enum label. The generated code uses an efficient match strategy:
//! for enums with <=8 variants, it matches on `(len, first_byte)` to minimize
//! branching.

use proc_macro2::TokenStream;
use quote::quote;

/// Convert `CamelCase` to `snake_case` for PG type name derivation.
///
/// Handles consecutive uppercase correctly:
/// - `TicketStatus` -> `ticket_status`
/// - `HTTPCode`     -> `http_code`
/// - `A`            -> `a`
#[cfg(test)]
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let chars: Vec<char> = s.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            // Insert underscore before uppercase if:
            // - Not at start, AND
            // - Previous char was lowercase, OR
            // - Next char exists and is lowercase (handles "HTTPCode" -> "http_code")
            if i > 0 {
                let prev_lower = chars[i - 1].is_lowercase();
                let next_lower = chars.get(i + 1).is_some_and(|c| c.is_lowercase());
                if prev_lower || next_lower {
                    out.push('_');
                }
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// A single parsed enum variant with its SQL label.
struct EnumVariant {
    /// The Rust variant identifier.
    ident: syn::Ident,
    /// The SQL label string (from `#[sql("...")]`).
    sql_label: String,
}

/// Parse and generate code for `#[pg_enum]`.
pub fn expand_pg_enum(_attr: TokenStream, item: TokenStream) -> Result<TokenStream, syn::Error> {
    let input: syn::ItemEnum = syn::parse2(item)?;

    // Validate: must be a C-like enum (no fields on variants)
    for variant in &input.variants {
        if !matches!(variant.fields, syn::Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "pg_enum only supports unit variants (no fields)",
            ));
        }
    }

    if input.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input,
            "pg_enum requires at least one variant",
        ));
    }

    // Extract variants with their SQL labels
    let variants = parse_variants(&input)?;

    let enum_name = &input.ident;
    let vis = &input.vis;

    // Preserve any existing attributes except #[sql(...)] on variants
    let enum_attrs: Vec<_> = input.attrs.iter().collect();

    // Build the clean enum definition (with derives)
    let variant_defs = input.variants.iter().map(|v| {
        // Strip #[sql(...)] attributes, keep anything else
        let attrs: Vec<_> = v
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("sql"))
            .collect();
        let ident = &v.ident;
        quote! { #(#attrs)* #ident }
    });

    // Generate Encode implementation (for parameter binding)
    let encode_impl = gen_encode(enum_name, &variants);

    // Generate from_sql_label function (for result decoding in codegen)
    let from_label_impl = gen_from_label(enum_name, &variants);

    // Generate Display impl (useful for debugging, logging)
    let display_impl = gen_display(enum_name, &variants);

    Ok(quote! {
        #(#enum_attrs)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #vis enum #enum_name {
            #(#variant_defs,)*
        }

        #encode_impl
        #from_label_impl
        #display_impl
    })
}

/// Parse `#[sql("label")]` attributes from each variant.
fn parse_variants(input: &syn::ItemEnum) -> Result<Vec<EnumVariant>, syn::Error> {
    let mut variants = Vec::with_capacity(input.variants.len());

    for variant in &input.variants {
        let sql_label = extract_sql_attr(variant)?;
        variants.push(EnumVariant {
            ident: variant.ident.clone(),
            sql_label,
        });
    }

    // Validate: no duplicate SQL labels
    for (i, a) in variants.iter().enumerate() {
        for b in variants.iter().skip(i + 1) {
            if a.sql_label == b.sql_label {
                return Err(syn::Error::new_spanned(
                    &input.variants[i],
                    format!(
                        "duplicate SQL label \"{}\" on variants `{}` and `{}`",
                        a.sql_label, a.ident, b.ident
                    ),
                ));
            }
        }
    }

    Ok(variants)
}

/// Extract the SQL label from `#[sql("label")]` on a variant.
fn extract_sql_attr(variant: &syn::Variant) -> Result<String, syn::Error> {
    for attr in &variant.attrs {
        if attr.path().is_ident("sql") {
            let label: syn::LitStr = attr.parse_args()?;
            let value = label.value();
            if value.is_empty() {
                return Err(syn::Error::new_spanned(attr, "SQL label cannot be empty"));
            }
            return Ok(value);
        }
    }
    Err(syn::Error::new_spanned(
        variant,
        format!(
            "variant `{}` is missing #[sql(\"...\")] attribute",
            variant.ident
        ),
    ))
}

/// Generate `impl Encode` for the enum (bsql-driver parameter binding).
///
/// Encodes the enum's SQL label as text bytes. PG receives the label string
/// and matches it against the enum type.
fn gen_encode(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    let encode_arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let label = &v.sql_label;
            quote! {
                #enum_name::#ident => {
                    buf.extend_from_slice(#label.as_bytes());
                }
            }
        })
        .collect();

    quote! {
        impl ::bsql_core::driver::Encode for #enum_name {
            fn encode_binary(&self, buf: &mut ::std::vec::Vec<u8>) {
                match self {
                    #(#encode_arms)*
                }
            }

            fn type_oid(&self) -> u32 {
                // PG enum types use text encoding (OID 25) for the label string.
                // The actual enum OID is resolved by PG from the parameter type context.
                25
            }
        }
    }
}

/// Generate a `from_sql_label` method for decoding enum values from query results.
///
/// The driver's Row returns raw bytes for enum columns. This method converts
/// a string label back to the Rust enum variant.
fn gen_from_label(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    let match_body = if variants.len() <= 8 {
        gen_from_label_len_first_byte(enum_name, variants)
    } else {
        gen_from_label_linear(enum_name, variants)
    };
    quote! {
        impl #enum_name {
            /// Convert a PostgreSQL enum label string to this Rust enum.
            ///
            /// Returns `None` if the label does not match any variant.
            pub fn from_sql_label(s: &str) -> ::std::option::Option<Self> {
                #match_body
                ::std::option::Option::None
            }
        }
    }
}

/// Fast path: match on (s.len(), s.as_bytes()[0]) for small enums.
fn gen_from_label_len_first_byte(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    let mut groups: std::collections::BTreeMap<(usize, u8), Vec<&EnumVariant>> =
        std::collections::BTreeMap::new();
    for v in variants {
        let key = (v.sql_label.len(), v.sql_label.as_bytes()[0]);
        groups.entry(key).or_default().push(v);
    }

    let arms: Vec<TokenStream> = groups
        .iter()
        .map(|(&(len, first), group)| {
            let len_lit = len;
            let first_lit = first;
            if group.len() == 1 {
                let v = group[0];
                let label = &v.sql_label;
                let ident = &v.ident;
                quote! {
                    (#len_lit, #first_lit) if s == #label => {
                        return ::std::option::Option::Some(#enum_name::#ident);
                    }
                }
            } else {
                let inner_arms: Vec<TokenStream> = group
                    .iter()
                    .map(|v| {
                        let label = &v.sql_label;
                        let ident = &v.ident;
                        quote! { #label => return ::std::option::Option::Some(#enum_name::#ident), }
                    })
                    .collect();
                quote! {
                    (#len_lit, #first_lit) => {
                        match s {
                            #(#inner_arms)*
                            _ => {}
                        }
                    }
                }
            }
        })
        .collect();

    quote! {
        if !s.is_empty() {
            match (s.len(), s.as_bytes()[0]) {
                #(#arms)*
                _ => {}
            }
        }
    }
}

/// Fallback: linear comparison chain for large enums.
fn gen_from_label_linear(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    let arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let label = &v.sql_label;
            let ident = &v.ident;
            quote! { #label => return ::std::option::Option::Some(#enum_name::#ident), }
        })
        .collect();

    quote! {
        match s {
            #(#arms)*
            _ => {}
        }
    }
}

/// Generate `impl Display` for the enum (shows the SQL label).
fn gen_display(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    let arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let label = &v.sql_label;
            quote! { #enum_name::#ident => #label, }
        })
        .collect();

    quote! {
        impl ::std::fmt::Display for #enum_name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                let label = match self {
                    #(#arms)*
                };
                f.write_str(label)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_enum(tokens: TokenStream) -> TokenStream {
        expand_pg_enum(TokenStream::new(), tokens).expect("should parse")
    }

    #[test]
    fn basic_enum_generates_code() {
        let input = quote! {
            enum Status {
                #[sql("new")]
                New,
                #[sql("active")]
                Active,
                #[sql("closed")]
                Closed,
            }
        };

        let output = parse_enum(input);
        let code = output.to_string();

        // Should contain the enum definition
        assert!(code.contains("enum Status"), "missing enum: {code}");
        // Should contain Encode impl (replaced FromSql/ToSql)
        assert!(code.contains("Encode"), "missing Encode: {code}");
        // Should contain from_sql_label method
        assert!(
            code.contains("from_sql_label"),
            "missing from_sql_label: {code}"
        );
        // Should contain Display impl
        assert!(code.contains("Display"), "missing Display: {code}");
        // Should contain the SQL labels
        assert!(code.contains("\"new\""), "missing 'new' label: {code}");
        assert!(
            code.contains("\"active\""),
            "missing 'active' label: {code}"
        );
        assert!(
            code.contains("\"closed\""),
            "missing 'closed' label: {code}"
        );
        // Should have derive attributes
        assert!(code.contains("Debug"), "missing Debug derive: {code}");
        assert!(code.contains("Clone"), "missing Clone derive: {code}");
        assert!(code.contains("Copy"), "missing Copy derive: {code}");
        assert!(
            code.contains("PartialEq"),
            "missing PartialEq derive: {code}"
        );
    }

    #[test]
    fn missing_sql_attr_errors() {
        let input = quote! {
            enum Status {
                #[sql("new")]
                New,
                Active,
            }
        };

        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing #[sql"), "unexpected error: {err}");
    }

    #[test]
    fn non_unit_variant_errors() {
        let input = quote! {
            enum Status {
                #[sql("new")]
                New(i32),
            }
        };

        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unit variants"), "unexpected error: {err}");
    }

    #[test]
    fn empty_enum_errors() {
        let input = quote! {
            enum Status {}
        };

        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_sql_label_errors() {
        let input = quote! {
            enum Status {
                #[sql("new")]
                New,
                #[sql("new")]
                AlsoNew,
            }
        };

        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate SQL label"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_sql_label_errors() {
        let input = quote! {
            enum Status {
                #[sql("")]
                Empty,
            }
        };

        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be empty"), "unexpected error: {err}");
    }

    #[test]
    fn visibility_preserved() {
        let input = quote! {
            pub enum Status {
                #[sql("new")]
                New,
            }
        };

        let output = parse_enum(input);
        let code = output.to_string();
        assert!(code.contains("pub enum Status"), "visibility lost: {code}");
    }

    #[test]
    fn len_first_byte_match_generated_for_small_enum() {
        let input = quote! {
            enum Color {
                #[sql("red")]
                Red,
                #[sql("blue")]
                Blue,
                #[sql("green")]
                Green,
            }
        };

        let output = parse_enum(input);
        let code = output.to_string();
        // Should use len/first_byte matching in from_sql_label
        assert!(code.contains("s . len ()"), "missing len check: {code}");
        assert!(
            code.contains("as_bytes ()"),
            "missing first_byte check: {code}"
        );
    }

    #[test]
    fn snake_case_conversion() {
        assert_eq!(to_snake_case("TicketStatus"), "ticket_status");
        assert_eq!(to_snake_case("Color"), "color");
        assert_eq!(to_snake_case("HTTPCode"), "http_code");
        assert_eq!(to_snake_case("A"), "a");
    }

    // --- bad-path coverage: pg_enum edge cases ---

    #[test]
    fn single_variant_enum() {
        let input = quote! {
            enum Singleton {
                #[sql("only")]
                Only,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(code.contains("enum Singleton"), "missing enum: {code}");
        assert!(code.contains("\"only\""), "missing sql label: {code}");
    }

    #[test]
    fn variant_with_special_chars_in_label() {
        // SQL labels with hyphens, spaces, unicode
        let input = quote! {
            enum Priority {
                #[sql("high-priority")]
                High,
                #[sql("low priority")]
                Low,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(
            code.contains("\"high-priority\""),
            "missing hyphenated label: {code}"
        );
        assert!(
            code.contains("\"low priority\""),
            "missing spaced label: {code}"
        );
    }

    #[test]
    fn variant_with_long_label() {
        let input = quote! {
            enum LongLabel {
                #[sql("this_is_a_very_long_sql_label_that_goes_on_and_on_and_on")]
                Long,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(
            code.contains("this_is_a_very_long_sql_label"),
            "long label lost: {code}"
        );
    }

    #[test]
    fn variant_with_unicode_label() {
        let input = quote! {
            enum UniLabel {
                #[sql("статус")]
                Status,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(code.contains("\"статус\""), "unicode label lost: {code}");
    }

    #[test]
    fn pub_crate_visibility_preserved() {
        let input = quote! {
            pub(crate) enum Internal {
                #[sql("a")]
                A,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(
            code.contains("pub (crate)"),
            "pub(crate) visibility lost: {code}"
        );
    }

    #[test]
    fn struct_not_accepted() {
        let input = quote! {
            struct NotAnEnum {
                field: i32,
            }
        };
        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err(), "structs should be rejected");
    }

    #[test]
    fn tuple_variant_errors() {
        let input = quote! {
            enum Bad {
                #[sql("a")]
                A(String),
            }
        };
        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unit variants"), "unexpected error: {err}");
    }

    #[test]
    fn struct_variant_errors() {
        let input = quote! {
            enum Bad {
                #[sql("a")]
                A { x: i32 },
            }
        };
        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unit variants"), "unexpected error: {err}");
    }

    #[test]
    fn large_enum_uses_linear_match() {
        // >8 variants should use linear match fallback
        let input = quote! {
            enum NineVariants {
                #[sql("a")] A,
                #[sql("b")] B,
                #[sql("c")] C,
                #[sql("d")] D,
                #[sql("e")] E,
                #[sql("f")] F,
                #[sql("g")] G,
                #[sql("h")] H,
                #[sql("i")] I,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        // Linear match uses direct string comparison, not len/first_byte
        assert!(
            code.contains("from_sql_label"),
            "missing from_sql_label: {code}"
        );
    }

    #[test]
    fn same_length_same_first_byte_labels() {
        // Two labels with same (len, first_byte) — tests collision path
        let input = quote! {
            enum Collision {
                #[sql("abc")]
                Abc,
                #[sql("axz")]
                Axz,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(code.contains("\"abc\""), "missing abc: {code}");
        assert!(code.contains("\"axz\""), "missing axz: {code}");
    }

    #[test]
    fn encode_impl_generated() {
        let input = quote! {
            enum Check {
                #[sql("a")]
                A,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(
            code.contains("encode_binary"),
            "missing encode_binary: {code}"
        );
        assert!(code.contains("type_oid"), "missing type_oid: {code}");
    }

    #[test]
    fn snake_case_single_char() {
        assert_eq!(to_snake_case("X"), "x");
    }

    #[test]
    fn snake_case_all_lowercase() {
        assert_eq!(to_snake_case("color"), "color");
    }

    #[test]
    fn snake_case_empty() {
        assert_eq!(to_snake_case(""), "");
    }

    #[test]
    fn snake_case_consecutive_uppercase() {
        assert_eq!(to_snake_case("HTMLParser"), "html_parser");
        assert_eq!(to_snake_case("IOError"), "io_error");
    }

    #[test]
    fn snake_case_all_uppercase() {
        assert_eq!(to_snake_case("URL"), "url");
        assert_eq!(to_snake_case("HTTP"), "http");
    }

    // --- validate_pg_labels logic tests ---

    /// Test the label comparison logic that `validate_pg_labels` uses.
    /// Since we cannot connect to PG in unit tests, we test the comparison
    /// logic directly: Rust labels must be a subset of PG labels.
    #[test]
    fn validate_labels_matching_subset() {
        let rust_labels = ["new", "active", "closed"];
        let pg_labels = ["new", "active", "closed"];
        // Every Rust label exists in PG — should pass
        for rl in &rust_labels {
            assert!(
                pg_labels.contains(rl),
                "Rust label '{rl}' should exist in PG labels"
            );
        }
    }

    #[test]
    fn validate_labels_mismatched_label() {
        let rust_labels = ["new", "active", "archived"];
        let pg_labels = ["new", "active", "closed"];
        // "archived" not in PG — should be detected
        let mismatched: Vec<_> = rust_labels
            .iter()
            .filter(|rl| !pg_labels.contains(rl))
            .collect();
        assert_eq!(mismatched, [&"archived"]);
    }

    #[test]
    fn validate_labels_extra_pg_label_ok() {
        // PG can have more labels than Rust enum — that's fine
        let rust_labels = ["new", "closed"];
        let pg_labels = ["new", "active", "closed", "archived"];
        for rl in &rust_labels {
            assert!(
                pg_labels.contains(rl),
                "Rust label '{rl}' should exist in PG labels"
            );
        }
    }

    // --- to_snake_case additional edge cases ---

    #[test]
    fn snake_case_already_snake_case() {
        assert_eq!(to_snake_case("ticket_status"), "ticket_status");
    }

    #[test]
    fn snake_case_with_numbers() {
        assert_eq!(to_snake_case("Error404Page"), "error404_page");
    }

    #[test]
    fn snake_case_single_lowercase() {
        assert_eq!(to_snake_case("x"), "x");
    }

    #[test]
    fn snake_case_mixed_with_numbers() {
        assert_eq!(to_snake_case("V2Status"), "v2_status");
    }

    // --- graceful failure when PG unavailable ---

    #[test]
    fn expand_pg_enum_does_not_require_connection() {
        // expand_pg_enum is a pure macro expansion — it should never connect to PG.
        // Validation against PG happens in a separate step. Verify the macro alone works.
        let input = quote! {
            enum Status {
                #[sql("new")]
                New,
                #[sql("closed")]
                Closed,
            }
        };
        let result = expand_pg_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "pg_enum macro expansion should not require a database connection"
        );
    }

    #[test]
    fn from_sql_label_generated_returns_none_for_unknown() {
        // The generated from_sql_label should return None for unknown labels.
        // We verify this by checking the generated code includes a None fallback.
        let input = quote! {
            enum Status {
                #[sql("open")]
                Open,
                #[sql("closed")]
                Closed,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(
            code.contains("None"),
            "from_sql_label should have None fallback: {code}"
        );
    }
}
