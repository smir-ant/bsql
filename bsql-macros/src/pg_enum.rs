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
    let pg_type_name = to_snake_case(&enum_name.to_string());

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

    // Generate FromSql implementation
    let from_sql_impl = gen_from_sql(enum_name, &variants, &pg_type_name);

    // Generate ToSql implementation
    let to_sql_impl = gen_to_sql(enum_name, &variants, &pg_type_name);

    // Generate Display impl (useful for debugging, logging)
    let display_impl = gen_display(enum_name, &variants);

    Ok(quote! {
        #(#enum_attrs)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #vis enum #enum_name {
            #(#variant_defs,)*
        }

        #from_sql_impl
        #to_sql_impl
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

/// Generate `impl<'a> FromSql<'a>` for the enum.
///
/// For enums with <=8 variants, uses a (len, first_byte) match for efficiency.
/// For larger enums, falls back to a simple byte-slice comparison chain.
fn gen_from_sql(
    enum_name: &syn::Ident,
    variants: &[EnumVariant],
    pg_type_name: &str,
) -> TokenStream {
    let match_body = gen_from_sql_match(enum_name, variants);
    let enum_name_str = enum_name.to_string();

    quote! {
        impl<'_pg> ::bsql_core::pg_types::FromSql<'_pg> for #enum_name {
            fn from_sql(
                _ty: &::bsql_core::pg_types::Type,
                raw: &'_pg [u8],
            ) -> ::std::result::Result<Self, ::std::boxed::Box<dyn ::std::error::Error + ::std::marker::Sync + ::std::marker::Send>> {
                let s = ::std::str::from_utf8(raw).map_err(|e| {
                    ::std::boxed::Box::new(e) as ::std::boxed::Box<dyn ::std::error::Error + ::std::marker::Sync + ::std::marker::Send>
                })?;
                #match_body
                Err(::std::boxed::Box::from(::std::format!(
                    "unknown {} variant from PostgreSQL: \"{}\" — this is a schema mismatch. \
                     Update your Rust enum to match the database.",
                    #enum_name_str, s
                )))
            }

            fn accepts(ty: &::bsql_core::pg_types::Type) -> bool {
                // Only accept the specific PG enum type, not any enum
                match ty.kind() {
                    ::bsql_core::pg_types::Kind::Enum(_) => ty.name() == #pg_type_name,
                    _ => false,
                }
            }
        }
    }
}

/// Generate the match body for `from_sql`.
///
/// For small enums (<=8 variants), uses (len, first_byte) discrimination.
/// This exploits the fact that most PostgreSQL enum labels differ in either
/// length or first character.
fn gen_from_sql_match(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    // For small enums, try (len, first_byte) dispatch. If there are collisions
    // in (len, first_byte), fall back to full comparison for those arms.
    if variants.len() <= 8 {
        gen_from_sql_len_first_byte(enum_name, variants)
    } else {
        gen_from_sql_linear(enum_name, variants)
    }
}

/// Fast path: match on (s.len(), s.as_bytes()[0]) then compare full string
/// only when (len, first_byte) collides.
fn gen_from_sql_len_first_byte(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    // Group variants by (len, first_byte)
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
                // Unique (len, first_byte) — no need for inner comparison
                let v = group[0];
                let label = &v.sql_label;
                let ident = &v.ident;
                quote! {
                    (#len_lit, #first_lit) if s == #label => {
                        return Ok(#enum_name::#ident);
                    }
                }
            } else {
                // Collision: need inner match on full string
                let inner_arms: Vec<TokenStream> = group
                    .iter()
                    .map(|v| {
                        let label = &v.sql_label;
                        let ident = &v.ident;
                        quote! { #label => return Ok(#enum_name::#ident), }
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

/// Fallback: linear comparison chain.
fn gen_from_sql_linear(enum_name: &syn::Ident, variants: &[EnumVariant]) -> TokenStream {
    let arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let label = &v.sql_label;
            let ident = &v.ident;
            quote! { #label => return Ok(#enum_name::#ident), }
        })
        .collect();

    quote! {
        match s {
            #(#arms)*
            _ => {}
        }
    }
}

/// Generate `impl ToSql` for the enum.
fn gen_to_sql(enum_name: &syn::Ident, variants: &[EnumVariant], pg_type_name: &str) -> TokenStream {
    let to_sql_arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let label = &v.sql_label;
            quote! {
                #enum_name::#ident => {
                    out.extend_from_slice(#label.as_bytes());
                }
            }
        })
        .collect();

    quote! {
        impl ::bsql_core::pg_types::ToSql for #enum_name {
            fn to_sql(
                &self,
                _ty: &::bsql_core::pg_types::Type,
                out: &mut ::bsql_core::pg_types::private::BytesMut,
            ) -> ::std::result::Result<::bsql_core::pg_types::IsNull, ::std::boxed::Box<dyn ::std::error::Error + ::std::marker::Sync + ::std::marker::Send>> {
                match self {
                    #(#to_sql_arms)*
                }
                Ok(::bsql_core::pg_types::IsNull::No)
            }

            fn accepts(ty: &::bsql_core::pg_types::Type) -> bool {
                match ty.kind() {
                    ::bsql_core::pg_types::Kind::Enum(_) => ty.name() == #pg_type_name,
                    _ => false,
                }
            }

            fn to_sql_checked(
                &self,
                ty: &::bsql_core::pg_types::Type,
                out: &mut ::bsql_core::pg_types::private::BytesMut,
            ) -> ::std::result::Result<::bsql_core::pg_types::IsNull, ::std::boxed::Box<dyn ::std::error::Error + ::std::marker::Sync + ::std::marker::Send>> {
                if !<Self as ::bsql_core::pg_types::ToSql>::accepts(ty) {
                    return Err(::std::format!(
                        "cannot convert {} to PostgreSQL type {:?}",
                        ::std::stringify!(#enum_name), ty
                    ).into());
                }
                self.to_sql(ty, out)
            }
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
        // Should contain FromSql impl
        assert!(code.contains("FromSql"), "missing FromSql: {code}");
        // Should contain ToSql impl
        assert!(code.contains("ToSql"), "missing ToSql: {code}");
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
        // Should use len/first_byte matching
        assert!(code.contains("s . len ()"), "missing len check: {code}");
        assert!(
            code.contains("as_bytes ()"),
            "missing first_byte check: {code}"
        );
    }

    #[test]
    fn accepts_checks_pg_type_name() {
        // The generated `accepts` should check `ty.name() == "snake_case_name"`.
        // For an enum named `TicketStatus`, the PG type name should be `ticket_status`.
        let input = quote! {
            enum TicketStatus {
                #[sql("new")]
                New,
                #[sql("closed")]
                Closed,
            }
        };

        let output = parse_enum(input);
        let code = output.to_string();

        // Both FromSql::accepts and ToSql::accepts should check the type name
        assert!(
            code.contains("\"ticket_status\""),
            "accepts should check for pg type name 'ticket_status': {code}"
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
        assert!(code.contains("FromSql"), "missing FromSql: {code}");
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
    fn to_sql_checked_rejects_wrong_type() {
        // The generated to_sql_checked should reject mismatched PG types.
        // We verify the code contains the check.
        let input = quote! {
            enum Check {
                #[sql("a")]
                A,
            }
        };
        let output = parse_enum(input);
        let code = output.to_string();
        assert!(
            code.contains("to_sql_checked"),
            "missing to_sql_checked: {code}"
        );
        assert!(code.contains("accepts"), "missing accepts check: {code}");
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
}
