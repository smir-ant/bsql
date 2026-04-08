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
    fn parse_fixtures_trailing_comma() {
        let tokens: TokenStream = "fixtures(\"schema\", \"seed\",)".parse().unwrap();
        let args: TestArgs = syn::parse2(tokens).unwrap();
        assert_eq!(args.fixtures, vec!["schema", "seed"]);
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
    fn expand_rejects_self_param() {
        let attr: TokenStream = "".parse().unwrap();
        // A free function can't have &self, but we test the parse path
        let item: TokenStream = "async fn test_self(&self) {}".parse().unwrap();
        let result = expand_test(attr, item);
        assert!(result.is_err());
    }

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
}
