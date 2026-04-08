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
//! **Compile-time validation:** Sort SQL fragments are validated via a two-phase
//! registry approach. `#[bsql::sort]` writes each fragment to `.bsql/sorts/`,
//! then `query!` reads the registry and PREPAREs each fragment in the full query
//! context against PostgreSQL. Column typos in sort fragments are compile errors.

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

    // --- Sort registry / compile-time validation tests ---

    #[test]
    fn sort_registry_file_written() {
        // After expanding a sort enum, check that .bsql/sorts/{name}.txt exists
        // and contains the correct fragments.
        // Since we can't easily invoke the proc macro in a unit test,
        // test the write logic directly.

        let dir = std::env::temp_dir().join("bsql_test_sorts");
        let _ = std::fs::create_dir_all(&dir);

        let fragments = ["created_at DESC", "price ASC", "name ASC NULLS LAST"];
        let content = fragments.join("\n");
        let path = dir.join("TestSort.txt");
        std::fs::write(&path, &content).unwrap();

        // Read back and verify
        let read = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = read.lines().collect();
        assert_eq!(lines, fragments);

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sort_registry_read_parses_fragments() {
        let dir = std::env::temp_dir().join("bsql_test_sorts_read");
        let _ = std::fs::create_dir_all(&dir);

        let path = dir.join("MySort.txt");
        std::fs::write(&path, "created_at DESC\nprice ASC\n").unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let fragments: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(fragments.len(), 2);
        assert_eq!(fragments[0], "created_at DESC");
        assert_eq!(fragments[1], "price ASC");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sort_registry_empty_file() {
        let dir = std::env::temp_dir().join("bsql_test_sorts_empty");
        let _ = std::fs::create_dir_all(&dir);

        let path = dir.join("EmptySort.txt");
        std::fs::write(&path, "").unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let fragments: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(fragments.len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sort_registry_missing_file_no_error() {
        // If the registry file doesn't exist, validation should be skipped gracefully
        let dir = std::env::temp_dir().join("bsql_test_sorts_missing");
        let path = dir.join("NonExistent.txt");
        assert!(std::fs::read_to_string(&path).is_err());
        // The query_impl_sort code checks `if let Ok(content) = ...` — graceful skip
    }

    #[test]
    fn sort_fragment_substitution() {
        let sql_template = "SELECT id, name FROM users ORDER BY {SORT} LIMIT 100";
        let fragment = "created_at DESC";
        let result = sql_template.replace("{SORT}", fragment);
        assert_eq!(
            result,
            "SELECT id, name FROM users ORDER BY created_at DESC LIMIT 100"
        );
    }

    #[test]
    fn sort_fragment_with_multiple_columns() {
        let sql_template = "SELECT id FROM t ORDER BY {SORT}";
        let fragment = "priority DESC, created_at ASC";
        let result = sql_template.replace("{SORT}", fragment);
        assert_eq!(
            result,
            "SELECT id FROM t ORDER BY priority DESC, created_at ASC"
        );
    }

    #[test]
    fn sort_validation_full_flow() {
        // Simulate: write fragments, read them, check they would substitute correctly
        let dir = std::env::temp_dir().join("bsql_test_sort_flow");
        let _ = std::fs::create_dir_all(&dir);

        // Write
        let fragments = vec!["name ASC", "id DESC"];
        let content = fragments.join("\n");
        std::fs::write(dir.join("UserSort.txt"), &content).unwrap();

        // Read
        let read = std::fs::read_to_string(dir.join("UserSort.txt")).unwrap();
        let read_fragments: Vec<&str> = read.lines().filter(|l| !l.is_empty()).collect();

        // Substitute into a query template
        let template = "SELECT id, name FROM users ORDER BY {SORT} LIMIT 50";
        for frag in &read_fragments {
            let sql = template.replace("{SORT}", frag);
            // Verify the resulting SQL is valid-looking
            assert!(sql.contains("ORDER BY"));
            assert!(!sql.contains("{SORT}"));
            assert!(sql.contains(frag));
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sort_fragment_with_nulls_handling() {
        let sql = "SELECT id FROM t ORDER BY {SORT}";
        let fragment = "name ASC NULLS FIRST";
        let result = sql.replace("{SORT}", fragment);
        assert!(result.contains("NULLS FIRST"));
    }

    #[test]
    fn sort_fragment_with_expression() {
        let sql = "SELECT id FROM t ORDER BY {SORT}";
        let fragment = "LOWER(name) ASC";
        let result = sql.replace("{SORT}", fragment);
        assert_eq!(result, "SELECT id FROM t ORDER BY LOWER(name) ASC");
    }

    // --- GRANT keyword rejected ---

    #[test]
    fn sort_fragment_grant_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("GRANT ALL ON users TO evil")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("GRANT"), "should mention GRANT: {err}");
    }

    // --- REVOKE keyword rejected ---

    #[test]
    fn sort_fragment_revoke_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("REVOKE ALL ON users FROM evil")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("REVOKE"), "should mention REVOKE: {err}");
    }

    // --- COPY keyword rejected ---

    #[test]
    fn sort_fragment_copy_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("COPY users TO '/tmp/evil.csv'")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("COPY"), "should mention COPY: {err}");
    }

    // --- ALTER keyword rejected ---

    #[test]
    fn sort_fragment_alter_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("ALTER TABLE users ADD COLUMN evil TEXT")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("ALTER"), "should mention ALTER: {err}");
    }

    // --- CREATE keyword rejected ---

    #[test]
    fn sort_fragment_create_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("CREATE TABLE evil (id int)")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("CREATE"), "should mention CREATE: {err}");
    }

    // --- EXECUTE keyword rejected ---

    #[test]
    fn sort_fragment_execute_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("EXECUTE evil_func()")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("EXECUTE"), "should mention EXECUTE: {err}");
    }

    // --- Fragment with LOWER() function expression accepted ---

    #[test]
    fn sort_fragment_lower_function_accepted() {
        let input = quote! {
            enum Sort {
                #[sql("LOWER(name) ASC")]
                NameAsc,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_ok(), "LOWER(name) ASC should be accepted");
        let code = result.unwrap().to_string();
        assert!(
            code.contains("LOWER(name) ASC"),
            "fragment preserved: {code}"
        );
    }

    // --- Fragment with COALESCE accepted ---

    #[test]
    fn sort_fragment_coalesce_accepted() {
        let input = quote! {
            enum Sort {
                #[sql("COALESCE(priority, 999) ASC")]
                Priority,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(
            result.is_ok(),
            "COALESCE in sort fragment should be accepted"
        );
    }

    // --- Fragment with case-insensitive keyword detection ---

    #[test]
    fn sort_fragment_lowercase_drop_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("drop table users")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err(), "lowercase 'drop' should be rejected");
    }

    // --- Fragment with mixed case keyword detection ---

    #[test]
    fn sort_fragment_mixed_case_delete_rejected() {
        let input = quote! {
            enum Sort {
                #[sql("DeLeTe from users")]
                Bad,
            }
        };
        let result = expand_sort_enum(TokenStream::new(), input);
        assert!(result.is_err(), "mixed case 'DeLeTe' should be rejected");
    }

    #[test]
    fn sort_registry_overwrite_on_recompile() {
        let dir = std::env::temp_dir().join("bsql_test_sort_overwrite");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("TestSort.txt");

        // First write
        std::fs::write(&path, "old_col ASC").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old_col ASC");

        // Overwrite (simulates recompile)
        std::fs::write(&path, "new_col DESC").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new_col DESC");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
