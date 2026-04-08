//! Implementation of the `#[bsql::sort]` attribute macro.
//!
//! Generates a sort enum whose variants map to SQL `ORDER BY` fragments.
//! Used with the `$[sort: EnumType]` placeholder syntax in `bsql::query!`.
//!
//! # Usage
//!
//! ```rust,ignore
//! #[bsql::sort]
//! pub enum TicketSort {
//!     #[sql("t.updated_at DESC, t.id DESC")]
//!     UpdatedAt,
//!     #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
//!     Deadline,
//!     #[sql("t.id DESC")]
//!     Id,
//! }
//! ```
//!
//! Each variant must have a `#[sql("...")]` attribute containing the SQL
//! fragment to splice into the `ORDER BY` clause. The macro generates:
//! - The enum with `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]`
//! - A `sql(&self) -> &'static str` method returning the SQL fragment
//!
//! Unlike `#[bsql::pg_enum]`, sort enums do NOT implement `Encode` — they
//! are not parameterized values. The SQL fragment is spliced directly into
//! the query string at compile time, and the runtime selects between
//! pre-validated SQL strings via a `match`.
//!
//! **Note:** Sort SQL fragments are NOT individually validated at compile time.
//! The query structure is validated but individual ORDER BY expressions are
//! verified only at runtime. Ensure your `#[sql("...")]` fragments reference
//! valid columns.

use proc_macro2::TokenStream;
use quote::quote;

/// A single parsed sort variant with its SQL fragment.
struct SortVariant {
    /// The Rust variant identifier.
    ident: syn::Ident,
    /// The SQL ORDER BY fragment (from `#[sql("...")]`).
    sql_fragment: String,
}

/// Parse and generate code for `#[bsql::sort]`.
pub fn expand_sort_enum(_attr: TokenStream, item: TokenStream) -> Result<TokenStream, syn::Error> {
    let input: syn::ItemEnum = syn::parse2(item)?;

    // Validate: must be a C-like enum (no fields on variants)
    for variant in &input.variants {
        if !matches!(variant.fields, syn::Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "sort enum only supports unit variants (no fields)",
            ));
        }
    }

    if input.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input,
            "sort enum requires at least one variant",
        ));
    }

    let variants = parse_sort_variants(&input)?;

    // Write sort fragments to .bsql/sorts/{EnumName}.txt for query! to validate.
    // Each line is one SQL fragment. Errors silently ignored (offline mode etc).
    {
        let cache_dir = std::env::var("CARGO_MANIFEST_DIR")
            .map(|d| std::path::PathBuf::from(d).join(".bsql").join("sorts"))
            .or_else(|_| std::env::current_dir().map(|d| d.join(".bsql").join("sorts")));
        if let Ok(sorts_dir) = cache_dir {
            let _ = std::fs::create_dir_all(&sorts_dir);
            let content: String = variants
                .iter()
                .map(|v| v.sql_fragment.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            let _ = std::fs::write(sorts_dir.join(format!("{}.txt", input.ident)), &content);
        }
    }

    let enum_name = &input.ident;
    let vis = &input.vis;

    // Preserve any existing attributes (doc comments, etc.) except #[sql(...)] on variants
    let enum_attrs: Vec<_> = input.attrs.iter().collect();

    // Build clean enum definition (strip #[sql] attrs from variants)
    let variant_defs = input.variants.iter().map(|v| {
        let attrs: Vec<_> = v
            .attrs
            .iter()
            .filter(|a| !a.path().is_ident("sql"))
            .collect();
        let ident = &v.ident;
        quote! { #(#attrs)* #ident }
    });

    // Generate sql() method arms
    let sql_arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let sql_fragment = &v.sql_fragment;
            quote! { #enum_name::#ident => #sql_fragment }
        })
        .collect();

    // Generate Display impl (shows the SQL fragment)
    let display_arms: Vec<TokenStream> = variants
        .iter()
        .map(|v| {
            let ident = &v.ident;
            let sql_fragment = &v.sql_fragment;
            quote! { #enum_name::#ident => #sql_fragment, }
        })
        .collect();

    Ok(quote! {
        #(#enum_attrs)*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #vis enum #enum_name {
            #(#variant_defs,)*
        }

        impl #enum_name {
            /// Returns the SQL `ORDER BY` fragment for this sort variant.
            pub fn sql(&self) -> &'static str {
                match self {
                    #(#sql_arms,)*
                }
            }
        }

        impl ::std::fmt::Display for #enum_name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                let label = match self {
                    #(#display_arms)*
                };
                f.write_str(label)
            }
        }
    })
}

/// Parse `#[sql("fragment")]` attributes from each variant.
fn parse_sort_variants(input: &syn::ItemEnum) -> Result<Vec<SortVariant>, syn::Error> {
    let mut variants = Vec::with_capacity(input.variants.len());

    for variant in &input.variants {
        let sql_fragment = extract_sql_attr(variant)?;

        // Validate: sort fragments must not contain SQL injection patterns.
        // Since sort SQL is spliced verbatim into ORDER BY, we reject dangerous content.
        validate_sort_fragment(&sql_fragment, variant)?;

        variants.push(SortVariant {
            ident: variant.ident.clone(),
            sql_fragment,
        });
    }

    // Validate: no duplicate SQL fragments
    for (i, a) in variants.iter().enumerate() {
        for b in variants.iter().skip(i + 1) {
            if a.sql_fragment == b.sql_fragment {
                return Err(syn::Error::new_spanned(
                    &input.variants[i],
                    format!(
                        "duplicate SQL fragment \"{}\" on variants `{}` and `{}`",
                        a.sql_fragment, a.ident, b.ident
                    ),
                ));
            }
        }
    }

    Ok(variants)
}

/// Validate that a sort SQL fragment does not contain dangerous patterns.
///
/// Sort fragments are spliced verbatim into the ORDER BY clause. Without
/// compile-time PG validation of individual fragments (which would require
/// the full query context), we defensively reject patterns that could be
/// used for SQL injection: semicolons, comments, and DML/DDL keywords.
fn validate_sort_fragment(fragment: &str, variant: &syn::Variant) -> Result<(), syn::Error> {
    // Check for semicolons
    if fragment.contains(';') {
        return Err(syn::Error::new_spanned(
            variant,
            format!(
                "sort SQL fragment contains a semicolon, which is not allowed: \"{}\"",
                fragment
            ),
        ));
    }

    // Check for SQL comments
    if fragment.contains("--") || fragment.contains("/*") {
        return Err(syn::Error::new_spanned(
            variant,
            format!(
                "sort SQL fragment contains a SQL comment, which is not allowed: \"{}\"",
                fragment
            ),
        ));
    }

    // Check for dangerous keywords (case-insensitive word boundary check)
    const DANGEROUS_KEYWORDS: &[&str] = &[
        "DROP", "DELETE", "INSERT", "UPDATE", "CREATE", "ALTER", "TRUNCATE", "GRANT", "REVOKE",
        "EXECUTE", "COPY",
    ];

    let upper = fragment.to_ascii_uppercase();
    for keyword in DANGEROUS_KEYWORDS {
        // Check for the keyword as a standalone word (not part of a column name)
        for (pos, _) in upper.match_indices(keyword) {
            let before_ok = pos == 0
                || !upper.as_bytes()[pos - 1].is_ascii_alphanumeric()
                    && upper.as_bytes()[pos - 1] != b'_';
            let after_pos = pos + keyword.len();
            let after_ok = after_pos >= upper.len()
                || !upper.as_bytes()[after_pos].is_ascii_alphanumeric()
                    && upper.as_bytes()[after_pos] != b'_';
            if before_ok && after_ok {
                return Err(syn::Error::new_spanned(
                    variant,
                    format!(
                        "sort SQL fragment contains disallowed keyword `{keyword}`: \"{fragment}\". \
                         Sort fragments must contain only ORDER BY expressions (column references, \
                         ASC/DESC, NULLS FIRST/LAST)."
                    ),
                ));
            }
        }
    }

    Ok(())
}

/// Extract the SQL fragment from `#[sql("fragment")]` on a variant.
fn extract_sql_attr(variant: &syn::Variant) -> Result<String, syn::Error> {
    for attr in &variant.attrs {
        if attr.path().is_ident("sql") {
            let label: syn::LitStr = attr.parse_args()?;
            let value = label.value();
            if value.is_empty() {
                return Err(syn::Error::new_spanned(
                    attr,
                    "SQL fragment cannot be empty",
                ));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_sort(tokens: TokenStream) -> TokenStream {
        expand_sort_enum(TokenStream::new(), tokens).expect("should parse")
    }

    #[test]
    fn basic_sort_enum_generates_code() {
        let input = quote! {
            enum TicketSort {
                #[sql("t.updated_at DESC, t.id DESC")]
                UpdatedAt,
                #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
                Deadline,
                #[sql("t.id DESC")]
                Id,
            }
        };

        let output = parse_sort(input);
        let code = output.to_string();

        assert!(code.contains("enum TicketSort"), "missing enum: {code}");
        assert!(code.contains("fn sql"), "missing sql method: {code}");
        assert!(
            code.contains("t.updated_at DESC"),
            "missing sql fragment: {code}"
        );
        assert!(code.contains("Display"), "missing Display: {code}");
        assert!(code.contains("Debug"), "missing Debug derive: {code}");
        assert!(code.contains("Clone"), "missing Clone derive: {code}");
        assert!(code.contains("Copy"), "missing Copy derive: {code}");
    }

    #[test]
    fn missing_sql_attr_errors() {
        let input = quote! {
            enum Sort {
                #[sql("a DESC")]
                A,
                B,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing #[sql"), "unexpected error: {err}");
    }

    #[test]
    fn empty_sql_fragment_errors() {
        let input = quote! {
            enum Sort {
                #[sql("")]
                A,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be empty"), "unexpected error: {err}");
    }

    #[test]
    fn non_unit_variant_errors() {
        let input = quote! {
            enum Sort {
                #[sql("a")]
                A(i32),
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unit variants"), "unexpected error: {err}");
    }

    #[test]
    fn empty_enum_errors() {
        let input = quote! {
            enum Sort {}
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
    }

    #[test]
    fn duplicate_sql_fragment_errors() {
        let input = quote! {
            enum Sort {
                #[sql("a DESC")]
                A,
                #[sql("a DESC")]
                B,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate SQL fragment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn visibility_preserved() {
        let input = quote! {
            pub enum Sort {
                #[sql("a")]
                A,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        assert!(code.contains("pub enum Sort"), "visibility lost: {code}");
    }

    #[test]
    fn no_encode_impl_generated() {
        let input = quote! {
            enum Sort {
                #[sql("a")]
                A,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        // Sort enums must NOT have Encode — they are spliced, not parameterized
        assert!(
            !code.contains("Encode"),
            "sort enum should not have Encode: {code}"
        );
    }

    #[test]
    fn single_variant() {
        let input = quote! {
            enum Sort {
                #[sql("id ASC")]
                Id,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        assert!(code.contains("id ASC"), "missing fragment: {code}");
    }

    #[test]
    fn struct_not_accepted() {
        let input = quote! {
            struct NotAnEnum {
                field: i32,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err(), "structs should be rejected");
    }

    #[test]
    fn sort_fragment_with_semicolon_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("id ASC; DROP TABLE users")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("semicolon"), "should mention semicolon: {err}");
    }

    #[test]
    fn sort_fragment_with_comment_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("id ASC -- sneaky")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("comment"), "should mention comment: {err}");
    }

    #[test]
    fn sort_fragment_with_block_comment_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("id ASC /* hidden */")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("comment"), "should mention comment: {err}");
    }

    #[test]
    fn sort_fragment_with_drop_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("(SELECT DROP FROM evil)")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("DROP"), "should mention DROP: {err}");
    }

    #[test]
    fn sort_fragment_with_delete_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("DELETE FROM users")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("DELETE"), "should mention DELETE: {err}");
    }

    #[test]
    fn sort_fragment_keyword_in_column_name_accepted() {
        // "updated_at" contains "update" as a substring but not as a standalone word
        let input = quote! {
            enum Sort {
                #[sql("updated_at DESC")]
                UpdatedAt,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "column name containing keyword substring should be OK"
        );
    }

    #[test]
    fn sort_fragment_valid_complex_accepted() {
        let input = quote! {
            enum Sort {
                #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
                Deadline,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "valid complex sort fragment should be accepted"
        );
    }

    // --- Audit gap tests ---

    // #109: Valid complex fragment: `created_at DESC NULLS LAST`
    #[test]
    fn sort_fragment_created_at_desc_nulls_last() {
        let input = quote! {
            enum Sort {
                #[sql("created_at DESC NULLS LAST")]
                CreatedAt,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "created_at DESC NULLS LAST should be accepted"
        );
    }

    // #110: Fragment containing keyword substring: `updated_at ASC` accepted
    #[test]
    fn sort_fragment_updated_at_asc_not_confused_with_update() {
        let input = quote! {
            enum Sort {
                #[sql("updated_at ASC")]
                UpdatedAt,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "updated_at should not be confused with UPDATE keyword"
        );
    }

    // Sort fragment with INSERT rejected
    #[test]
    fn sort_fragment_insert_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("INSERT INTO t VALUES (1)")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("INSERT"), "should mention INSERT: {err}");
    }

    // Sort fragment with UPDATE as standalone word rejected
    #[test]
    fn sort_fragment_standalone_update_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("UPDATE t SET x = 1")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("UPDATE"), "should mention UPDATE: {err}");
    }

    // Sort fragment with TRUNCATE rejected
    #[test]
    fn sort_fragment_truncate_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("TRUNCATE users")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
    }

    // Column name containing "execute" substring is accepted
    #[test]
    fn sort_fragment_execute_substring_in_column_accepted() {
        let input = quote! {
            enum Sort {
                #[sql("executed_at DESC")]
                ExecutedAt,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "executed_at should be accepted (EXECUTE is substring, not word)"
        );
    }

    // Column name containing "created" substring is accepted
    #[test]
    fn sort_fragment_created_substring_accepted() {
        let input = quote! {
            enum Sort {
                #[sql("created_by ASC")]
                CreatedBy,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_ok(), "created_by should be accepted");
    }

    // Column name "deleted_at" accepted (contains DELETE substring)
    #[test]
    fn sort_fragment_deleted_at_accepted() {
        let input = quote! {
            enum Sort {
                #[sql("deleted_at DESC NULLS LAST")]
                DeletedAt,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "deleted_at should be accepted (DELETE is substring, not standalone word)"
        );
    }

    // --- Sort registry / resolve tests ---

    #[test]
    fn sort_fragment_special_chars_valid_sql_accepted() {
        // Valid ORDER BY with NULLS LAST — no dangerous keywords
        let input = quote! {
            enum Sort {
                #[sql("created_at DESC NULLS LAST")]
                CreatedAtDesc,
                #[sql("t.priority ASC NULLS FIRST, t.id ASC")]
                Priority,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_ok(), "complex valid fragments should be accepted");
        let code = result.unwrap().to_string();
        assert!(
            code.contains("created_at DESC NULLS LAST"),
            "should contain first fragment: {code}"
        );
        assert!(
            code.contains("t.priority ASC NULLS FIRST"),
            "should contain second fragment: {code}"
        );
    }

    #[test]
    fn sort_enum_generates_all_match_arms() {
        // Verify that generated sql() method contains all arms
        let input = quote! {
            enum Sort {
                #[sql("a ASC")]
                A,
                #[sql("b DESC")]
                B,
                #[sql("c ASC NULLS LAST")]
                C,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        assert!(code.contains("a ASC"), "missing arm A: {code}");
        assert!(code.contains("b DESC"), "missing arm B: {code}");
        assert!(code.contains("c ASC NULLS LAST"), "missing arm C: {code}");
    }

    #[test]
    fn sort_enum_recompile_overwrites_output() {
        // Two successive calls to expand_sort_enum produce fresh output
        let input1 = quote! {
            enum Sort {
                #[sql("a ASC")]
                A,
            }
        };
        let input2 = quote! {
            enum Sort {
                #[sql("b DESC")]
                B,
            }
        };
        let out1 = expand_sort_enum(TokenStream::new(), input1)
            .unwrap()
            .to_string();
        let out2 = expand_sort_enum(TokenStream::new(), input2)
            .unwrap()
            .to_string();
        assert!(out1.contains("a ASC") && !out1.contains("b DESC"));
        assert!(out2.contains("b DESC") && !out2.contains("a ASC"));
    }

    #[test]
    fn sort_enum_multiple_commas_in_fragment() {
        let input = quote! {
            enum Sort {
                #[sql("t.a ASC, t.b DESC, t.c ASC")]
                Multi,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_ok(), "multi-comma fragment should be accepted");
        let code = result.unwrap().to_string();
        assert!(
            code.contains("t.a ASC, t.b DESC, t.c ASC"),
            "fragment should be preserved verbatim: {code}"
        );
    }

    #[test]
    fn sort_enum_doc_comments_preserved() {
        let input = quote! {
            /// This is a doc comment
            enum Sort {
                #[sql("id ASC")]
                Id,
            }
        };
        let output = parse_sort(input);
        let code = output.to_string();
        // Doc comments become #[doc = "..."] attributes
        assert!(
            code.contains("doc"),
            "doc comment should be preserved: {code}"
        );
    }
}
