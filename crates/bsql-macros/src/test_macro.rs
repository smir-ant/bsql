//! Implementation of the `#[bsql::test]` attribute macro.
//!
//! Generates a tokio test wrapper that:
//! 1. Creates an isolated PostgreSQL schema per test
//! 2. Applies SQL fixtures (via `include_str!`)
//! 3. Passes a `Pool` to the test function
//! 4. Drops the schema on cleanup (even on panic)
//!
//! # Usage
//!
//! ```rust,ignore
//! #[bsql::test]
//! async fn test_basic(pool: bsql::Pool) {
//!     pool.raw_execute("SELECT 1").await.unwrap();
//! }
//!
//! #[bsql::test(fixtures("schema", "seed"))]
//! async fn test_with_fixtures(pool: bsql::Pool) {
//!     let user = bsql::query!("SELECT name FROM users WHERE id = $id: i32")
//!         .fetch_one(&pool).await.unwrap();
//!     assert_eq!(user.name, "Alice");
//! }
//! ```
//!
//! Fixture files are resolved from `{CARGO_MANIFEST_DIR}/fixtures/{name}.sql`
//! or `{CARGO_MANIFEST_DIR}/tests/fixtures/{name}.sql`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{ItemFn, LitStr, Token};

/// Parsed arguments from `#[bsql::test(fixtures("a", "b"))]`.
#[derive(Debug)]
struct TestArgs {
    fixtures: Vec<String>,
}

impl Parse for TestArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut fixtures = Vec::new();

        if input.is_empty() {
            return Ok(TestArgs { fixtures });
        }

        // Parse `fixtures("name1", "name2", ...)`
        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            if ident == "fixtures" {
                let content;
                syn::parenthesized!(content in input);
                let names: Punctuated<LitStr, Token![,]> = Punctuated::parse_terminated(&content)?;
                for name in names {
                    fixtures.push(name.value());
                }
            } else {
                return Err(syn::Error::new_spanned(
                    ident,
                    "unknown attribute, expected `fixtures`",
                ));
            }

            // Consume trailing comma between top-level args
            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }

        Ok(TestArgs { fixtures })
    }
}

/// Resolve a fixture name to an absolute path for `include_str!`.
///
/// Checks two locations:
/// 1. `{manifest_dir}/fixtures/{name}.sql`
/// 2. `{manifest_dir}/tests/fixtures/{name}.sql`
///
/// Returns an error if neither exists.
fn resolve_fixture_path(name: &str, manifest_dir: &str) -> Result<String, String> {
    let path1 = format!("{}/fixtures/{}.sql", manifest_dir, name);
    let path2 = format!("{}/tests/fixtures/{}.sql", manifest_dir, name);

    if std::path::Path::new(&path1).exists() {
        Ok(path1)
    } else if std::path::Path::new(&path2).exists() {
        Ok(path2)
    } else {
        Err(format!(
            "fixture '{}' not found at:\n  - {}\n  - {}",
            name, path1, path2
        ))
    }
}

/// Expand `#[bsql::test]` into a `#[tokio::test]` wrapper with schema isolation.
pub fn expand_test(attr: TokenStream, item: TokenStream) -> Result<TokenStream, syn::Error> {
    let args: TestArgs = syn::parse2(attr)?;
    let input_fn: ItemFn = syn::parse2(item)?;

    let fn_name = &input_fn.sig.ident;
    let fn_vis = &input_fn.vis;
    let fn_attrs = &input_fn.attrs;
    let fn_block = &input_fn.block;

    // Validate: must be async
    if input_fn.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &input_fn.sig.fn_token,
            "#[bsql::test] functions must be async",
        ));
    }

    // Validate: must take exactly one argument of type Pool (or bsql::Pool)
    if input_fn.sig.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &input_fn.sig.inputs,
            "#[bsql::test] function must take exactly one argument: pool: bsql::Pool",
        ));
    }

    // Extract the pool parameter name
    let pool_param = match input_fn.sig.inputs.first().unwrap() {
        syn::FnArg::Typed(pat_type) => pat_type,
        syn::FnArg::Receiver(_) => {
            return Err(syn::Error::new_spanned(
                &input_fn.sig.inputs,
                "#[bsql::test] function must not have a `self` parameter",
            ));
        }
    };
    let pool_pat = &pool_param.pat;

    // Resolve fixture paths at macro expansion time (compile time)
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(proc_macro2::Span::call_site(), "CARGO_MANIFEST_DIR not set")
    })?;

    let mut fixture_includes = Vec::new();
    for fixture_name in &args.fixtures {
        let path = resolve_fixture_path(fixture_name, &manifest_dir)
            .map_err(|msg| syn::Error::new(proc_macro2::Span::call_site(), msg))?;
        fixture_includes.push(quote! { include_str!(#path) });
    }

    let fixtures_array = if fixture_includes.is_empty() {
        quote! { &[] }
    } else {
        quote! { &[ #( #fixture_includes ),* ] }
    };

    Ok(quote! {
        #( #fn_attrs )*
        #[::tokio::test]
        #fn_vis async fn #fn_name() {
            let __bsql_ctx = ::bsql::__test_support::setup_test_schema(
                #fixtures_array
            ).await.expect("bsql::test setup failed");

            let #pool_pat = __bsql_ctx.pool.clone();

            // Run the user's test body
            async {
                #fn_block
            }.await;

            // __bsql_ctx drops here, cleaning up the schema
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===============================================================
    // Attribute parsing
    // ===============================================================

    #[test]
    fn parse_empty_args() {
        let tokens: TokenStream = "".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert!(args.fixtures.is_empty());
    }

    #[test]
    fn parse_fixtures_single() {
        let tokens: TokenStream = "fixtures(\"schema\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["schema"]);
    }

    #[test]
    fn parse_fixtures_multiple() {
        let tokens: TokenStream = "fixtures(\"schema\", \"seed\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["schema", "seed"]);
    }

    #[test]
    fn parse_fixtures_three() {
        let tokens: TokenStream = "fixtures(\"a\", \"b\", \"c\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_fixtures_trailing_comma() {
        let tokens: TokenStream = "fixtures(\"schema\", \"seed\",)".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["schema", "seed"]);
    }

    #[test]
    fn parse_fixtures_empty_parens() {
        let tokens: TokenStream = "fixtures()".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert!(args.fixtures.is_empty());
    }

    #[test]
    fn parse_unknown_attr_fails() {
        let tokens: TokenStream = "unknown(\"foo\")".parse().unwrap();
        let result: Result<TestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unknown attribute"), "got: {msg}");
    }

    #[test]
    fn parse_non_string_in_fixtures_fails() {
        let tokens: TokenStream = "fixtures(42)".parse().unwrap();
        let result: Result<TestArgs, _> = syn::parse2(tokens);
        assert!(
            result.is_err(),
            "non-string literal in fixtures should fail"
        );
    }

    #[test]
    fn parse_ident_in_fixtures_fails() {
        let tokens: TokenStream = "fixtures(some_ident)".parse().unwrap();
        let result: Result<TestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err(), "identifier in fixtures should fail");
    }

    #[test]
    fn parse_nested_parentheses_fails() {
        let tokens: TokenStream = "fixtures((\"inner\"))".parse().unwrap();
        let result: Result<TestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err(), "nested parentheses should fail");
    }

    #[test]
    fn parse_fixtures_with_boolean_literal_fails() {
        let tokens: TokenStream = "fixtures(true)".parse().unwrap();
        let result: Result<TestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err(), "boolean in fixtures should fail");
    }

    #[test]
    fn parse_bare_string_without_fixtures_fails() {
        // Just a string literal at top level, not wrapped in fixtures()
        let tokens: TokenStream = "\"schema\"".parse().unwrap();
        let result: Result<TestArgs, _> = syn::parse2(tokens);
        assert!(result.is_err(), "bare string at top level should fail");
    }

    #[test]
    fn parse_duplicate_fixtures_attr() {
        // fixtures("a"), fixtures("b") — two fixtures attrs
        let tokens: TokenStream = "fixtures(\"a\"), fixtures(\"b\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["a", "b"]);
    }

    #[test]
    fn parse_fixtures_preserves_order() {
        let tokens: TokenStream = "fixtures(\"z\", \"a\", \"m\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["z", "a", "m"]);
    }

    // ===============================================================
    // Function signature validation
    // ===============================================================

    #[test]
    fn expand_rejects_sync_fn() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "fn test_sync(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("must be async"), "got: {msg}");
    }

    #[test]
    fn expand_rejects_no_args() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_no_args() {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("exactly one argument"), "got: {msg}");
    }

    #[test]
    fn expand_rejects_two_args() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_two(pool: Pool, extra: i32) {}"
            .parse()
            .unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("exactly one argument"), "got: {msg}");
    }

    #[test]
    fn expand_rejects_three_args() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_three(a: Pool, b: i32, c: String) {}"
            .parse()
            .unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("exactly one argument"), "got: {msg}");
    }

    #[test]
    fn expand_rejects_self_param() {
        let attr: TokenStream = "".parse().unwrap();
        // A free function can't have &self, but we test the parse path
        let item: TokenStream = "async fn test_self(&self) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("self"),
            "error should mention self, got: {msg}"
        );
    }

    #[test]
    fn expand_rejects_mut_self_param() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_self(&mut self) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
    }

    #[test]
    fn expand_accepts_any_parameter_name() {
        // Parameter doesn't have to be named "pool"
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_any_name(db: Pool) { let _ = db; }"
            .parse()
            .unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "should accept any param name, got: {:?}",
            result.unwrap_err()
        );
        let output = result.unwrap().to_string();
        // The generated code should use the user's parameter name
        assert!(
            output.contains("db"),
            "should preserve user's param name 'db'"
        );
    }

    #[test]
    fn expand_accepts_underscore_parameter_name() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_underscore(_pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "should accept _pool param name, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn expand_accepts_unit_return_type() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_unit(pool: Pool) -> () {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "should accept () return type, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn expand_accepts_no_return_type() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_no_ret(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "should accept implicit () return, got: {:?}",
            result.unwrap_err()
        );
    }

    // ===============================================================
    // Generated code verification
    // ===============================================================

    #[test]
    fn expand_generates_tokio_test() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream =
            "async fn test_basic(pool: Pool) { pool.raw_execute(\"SELECT 1\").await.unwrap(); }"
                .parse()
                .unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_ok(), "expand failed: {:?}", result.unwrap_err());
        let output = result.unwrap().to_string();
        assert!(
            output.contains("tokio :: test"),
            "missing tokio::test in: {output}"
        );
        assert!(
            output.contains("setup_test_schema"),
            "missing setup call in: {output}"
        );
        assert!(
            output.contains("__bsql_ctx"),
            "missing context in: {output}"
        );
    }

    #[test]
    fn expand_output_contains_setup_test_schema_call() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_check(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("setup_test_schema"),
            "must call setup_test_schema: {output}"
        );
    }

    #[test]
    fn expand_output_contains_bsql_ctx_for_cleanup() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_cleanup(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        // __bsql_ctx is created and lives until end of fn => Drop runs
        assert!(
            output.contains("__bsql_ctx"),
            "missing __bsql_ctx (cleanup via Drop): {output}"
        );
    }

    #[test]
    fn expand_preserves_function_name() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn my_custom_test_name(pool: Pool) {}"
            .parse()
            .unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("my_custom_test_name"),
            "function name must be preserved: {output}"
        );
    }

    #[test]
    fn expand_preserves_user_body() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_body(pool: Pool) { let x = 42; assert_eq!(x, 42); }"
            .parse()
            .unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("42"),
            "user body must be preserved: {output}"
        );
        assert!(
            output.contains("assert_eq"),
            "user assertions must be preserved: {output}"
        );
    }

    #[test]
    fn expand_with_fixtures_generates_include_str() {
        let attr: TokenStream = "fixtures(\"test_schema\")".parse().unwrap();
        let item: TokenStream = "async fn test_fix(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "expand with existing fixture should succeed: {:?}",
            result.unwrap_err()
        );
        let output = result.unwrap().to_string();
        assert!(
            output.contains("include_str"),
            "must use include_str! for fixture: {output}"
        );
        assert!(
            output.contains("test_schema.sql"),
            "must reference fixture file: {output}"
        );
    }

    #[test]
    fn expand_with_multiple_fixtures_generates_multiple_include_str() {
        let attr: TokenStream = "fixtures(\"test_schema\", \"test_seed\")".parse().unwrap();
        let item: TokenStream = "async fn test_multi(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "expand with multiple fixtures should succeed: {:?}",
            result.unwrap_err()
        );
        let output = result.unwrap().to_string();
        // Should have two include_str! calls
        let include_count = output.matches("include_str").count();
        assert_eq!(
            include_count, 2,
            "expected 2 include_str! calls, got {include_count}: {output}"
        );
    }

    #[test]
    fn expand_without_fixtures_passes_empty_slice() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_no_fix(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        // No include_str should appear
        assert!(
            !output.contains("include_str"),
            "no fixtures means no include_str: {output}"
        );
    }

    #[test]
    fn expand_generates_async_wrapper() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_async(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        // The outer function should be async (for tokio::test)
        assert!(
            output.contains("async fn"),
            "generated function must be async: {output}"
        );
    }

    #[test]
    fn expand_generates_pool_clone_from_context() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_clone(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("clone"),
            "pool should be cloned from context: {output}"
        );
    }

    // ===============================================================
    // Fixture path resolution
    // ===============================================================

    #[test]
    fn resolve_fixture_in_fixtures_dir() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let result = resolve_fixture_path("test_schema", &manifest_dir);
        assert!(
            result.is_ok(),
            "test_schema.sql should be found in fixtures/: {:?}",
            result.unwrap_err()
        );
        let path = result.unwrap();
        assert!(
            path.contains("fixtures/test_schema.sql"),
            "path should reference fixtures dir: {path}"
        );
    }

    #[test]
    fn resolve_fixture_in_tests_fixtures_dir() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let result = resolve_fixture_path("alt_location", &manifest_dir);
        assert!(
            result.is_ok(),
            "alt_location.sql should be found in tests/fixtures/: {:?}",
            result.unwrap_err()
        );
        let path = result.unwrap();
        assert!(
            path.contains("tests/fixtures/alt_location.sql"),
            "path should reference tests/fixtures dir: {path}"
        );
    }

    #[test]
    fn resolve_fixture_prefers_fixtures_over_tests_fixtures() {
        // test_schema.sql exists in fixtures/ — even if it also existed in
        // tests/fixtures/, the fixtures/ path should be returned first.
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let result = resolve_fixture_path("test_schema", &manifest_dir);
        assert!(result.is_ok());
        let path = result.unwrap();
        assert!(
            path.contains("/fixtures/test_schema.sql"),
            "fixtures/ should be preferred: {path}"
        );
        // Verify it's NOT from tests/fixtures/
        assert!(
            !path.contains("tests/fixtures/"),
            "should prefer fixtures/ over tests/fixtures/: {path}"
        );
    }

    #[test]
    fn resolve_nonexistent_fixture_fails_with_both_paths() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let result = resolve_fixture_path("does_not_exist_xyz", &manifest_dir);
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("not found"),
            "error should say 'not found': {msg}"
        );
        assert!(
            msg.contains("fixtures/does_not_exist_xyz.sql"),
            "error should list first path tried: {msg}"
        );
        assert!(
            msg.contains("tests/fixtures/does_not_exist_xyz.sql"),
            "error should list second path tried: {msg}"
        );
    }

    #[test]
    fn resolve_fixture_with_subdirectory() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let result = resolve_fixture_path("subdir/nested", &manifest_dir);
        assert!(
            result.is_ok(),
            "subdir/nested.sql should be found: {:?}",
            result.unwrap_err()
        );
        let path = result.unwrap();
        assert!(
            path.contains("fixtures/subdir/nested.sql"),
            "should resolve to subdir path: {path}"
        );
    }

    #[test]
    fn resolve_fixture_with_sql_extension_in_name() {
        // If user passes "schema.sql", it becomes "schema.sql.sql"
        // which won't exist. This is the expected behavior — document it.
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let result = resolve_fixture_path("test_schema.sql", &manifest_dir);
        assert!(
            result.is_err(),
            "fixture name with .sql extension should not be found (double .sql.sql)"
        );
    }

    #[test]
    fn expand_with_nonexistent_fixture_fails() {
        // This test works because the fixture file won't exist
        let attr: TokenStream = "fixtures(\"nonexistent_fixture_abc123\")".parse().unwrap();
        let item: TokenStream = "async fn test_fix(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not found"),
            "error should mention 'not found', got: {msg}"
        );
    }

    #[test]
    fn expand_with_nonexistent_fixture_lists_paths_tried() {
        let attr: TokenStream = "fixtures(\"nonexistent_xyz\")".parse().unwrap();
        let item: TokenStream = "async fn test_fix(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("fixtures/nonexistent_xyz.sql"),
            "should list fixtures/ path: {msg}"
        );
        assert!(
            msg.contains("tests/fixtures/nonexistent_xyz.sql"),
            "should list tests/fixtures/ path: {msg}"
        );
    }

    // ===============================================================
    // Edge cases
    // ===============================================================

    #[test]
    fn expand_empty_test_body() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_empty(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "empty test body should work: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    fn expand_with_very_long_fixture_name_that_does_not_exist() {
        let long_name = "a".repeat(200);
        let attr: TokenStream = format!("fixtures(\"{}\")", long_name).parse().unwrap();
        let item: TokenStream = "async fn test_long(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        // Should fail because the fixture doesn't exist, not crash
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not found"),
            "long fixture name should give not-found error: {msg}"
        );
    }

    #[test]
    fn parse_fixture_name_with_spaces_is_accepted_by_parser() {
        // The parser accepts any string literal — path resolution will fail
        let tokens: TokenStream = "fixtures(\"name with spaces\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["name with spaces"]);
        // But expansion will fail because the file won't exist
        let attr: TokenStream = "fixtures(\"name with spaces\")".parse().unwrap();
        let item: TokenStream = "async fn test_space(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "fixture with spaces should fail on path resolution"
        );
    }

    #[test]
    fn parse_fixture_name_with_unicode() {
        let tokens: TokenStream = "fixtures(\"schéma_données\")".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["schéma_données"]);
    }

    #[test]
    fn expand_duplicate_fixture_names() {
        let attr: TokenStream = "fixtures(\"test_schema\", \"test_schema\")"
            .parse()
            .unwrap();
        let item: TokenStream = "async fn test_dup(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_ok(),
            "duplicate fixtures should be accepted (applied twice): {:?}",
            result.unwrap_err()
        );
        let output = result.unwrap().to_string();
        let include_count = output.matches("include_str").count();
        assert_eq!(
            include_count, 2,
            "duplicate fixture should produce two include_str: {output}"
        );
    }

    // ===============================================================
    // Bad paths — applying macro to non-function items
    // ===============================================================

    #[test]
    fn expand_rejects_struct() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "struct Foo { x: i32 }".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to a struct should fail"
        );
    }

    #[test]
    fn expand_rejects_enum() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "enum Bar { A, B }".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to an enum should fail"
        );
    }

    #[test]
    fn expand_rejects_const() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "const X: i32 = 42;".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to a const should fail"
        );
    }

    #[test]
    fn expand_rejects_static() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "static X: i32 = 42;".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to a static should fail"
        );
    }

    #[test]
    fn expand_rejects_type_alias() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "type Foo = i32;".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to a type alias should fail"
        );
    }

    #[test]
    fn expand_rejects_impl_block() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "impl Foo { fn bar() {} }".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to an impl block should fail"
        );
    }

    #[test]
    fn expand_rejects_trait_def() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "trait Baz { fn qux(); }".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to a trait should fail"
        );
    }

    #[test]
    fn expand_rejects_use_statement() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "use std::io;".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(
            result.is_err(),
            "applying bsql::test to a use statement should fail"
        );
    }

    // ===============================================================
    // Visibility and attributes preservation
    // ===============================================================

    #[test]
    fn expand_preserves_pub_visibility() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "pub async fn test_pub(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("pub"),
            "pub visibility should be preserved: {output}"
        );
    }

    #[test]
    fn expand_preserves_doc_comments_as_attrs() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "#[doc = \"hello\"] async fn test_doc(pool: Pool) {}"
            .parse()
            .unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("doc"),
            "doc attribute should be preserved: {output}"
        );
    }

    #[test]
    fn expand_preserves_allow_attr() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "#[allow(unused)] async fn test_allow(pool: Pool) { let _x = 1; }"
            .parse()
            .unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        assert!(
            output.contains("allow"),
            "allow attribute should be preserved: {output}"
        );
    }

    // ===============================================================
    // resolve_fixture_path unit tests
    // ===============================================================

    #[test]
    fn resolve_fixture_path_with_empty_manifest_dir() {
        let result = resolve_fixture_path("anything", "");
        // Should fail because /fixtures/anything.sql doesn't exist
        assert!(result.is_err());
    }

    #[test]
    fn resolve_fixture_path_with_nonexistent_manifest_dir() {
        let result = resolve_fixture_path("anything", "/nonexistent/dir/xyz");
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("not found"), "got: {msg}");
    }

    #[test]
    fn resolve_fixture_path_error_includes_fixture_name() {
        let result = resolve_fixture_path("my_missing_fixture", "/nonexistent/dir");
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("my_missing_fixture"),
            "error should include fixture name: {msg}"
        );
    }

    // ===============================================================
    // TestArgs Debug impl
    // ===============================================================

    #[test]
    fn test_args_debug() {
        let args = TestArgs {
            fixtures: vec!["a".to_string(), "b".to_string()],
        };
        let dbg = format!("{:?}", args);
        assert!(dbg.contains("TestArgs"), "Debug output: {dbg}");
        assert!(dbg.contains("fixtures"), "Debug output: {dbg}");
    }

    // ===============================================================
    // Generated output no-fixture vs with-fixture comparison
    // ===============================================================

    #[test]
    fn expand_no_fixtures_vs_with_fixtures_structural_difference() {
        let item_str = "async fn test_cmp(pool: Pool) {}";

        let attr_none: TokenStream = "".parse().unwrap();
        let item_none: TokenStream = item_str.parse().unwrap();
        let out_none = expand_test(attr_none, item_none).unwrap().to_string();

        let attr_fix: TokenStream = "fixtures(\"test_schema\")".parse().unwrap();
        let item_fix: TokenStream = item_str.parse().unwrap();
        let out_fix = expand_test(attr_fix, item_fix).unwrap().to_string();

        // Both should have tokio::test and setup_test_schema
        assert!(out_none.contains("tokio :: test"));
        assert!(out_fix.contains("tokio :: test"));
        assert!(out_none.contains("setup_test_schema"));
        assert!(out_fix.contains("setup_test_schema"));

        // Only the fixture version should have include_str
        assert!(!out_none.contains("include_str"));
        assert!(out_fix.contains("include_str"));
    }

    // ===============================================================
    // Generated function signature is zero-arg async
    // ===============================================================

    #[test]
    fn expand_generated_fn_takes_no_arguments() {
        let attr: TokenStream = "".parse().unwrap();
        let item: TokenStream = "async fn test_sig(pool: Pool) {}".parse().unwrap();
        let result = expand_test(attr, item).unwrap();
        let output = result.to_string();
        // The generated function should be `async fn test_sig()` with no params
        // (the pool param is extracted into the body)
        assert!(
            output.contains("async fn test_sig ()"),
            "generated fn should have no params: {output}"
        );
    }
}
