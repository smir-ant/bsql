//! The `fragment!` proc-macro front-end — lexes a literal SQL skeleton
//! with `{}` value holes into a `Fragment::__from_chunks(...)` expression.
//!
//! This is the *only* genuinely-new code in slice 2: a thin lexer that
//! splits the author-written format string at each `{}`, interleaving the
//! literal segments (as `&'static str` `.rodata`) with the per-hole bind
//! conversions (`IntoBound::into_bound(arg)`). The `Fragment`
//! representation, the renumbering, and the `IntoBound` wall all live in
//! `bsql-postgres-core`; this crate just builds the chunk vector.
//!
//! # Input grammar
//!
//! ```text
//! fragment!( <string-literal> [ , <expr> ]* [,] )
//! ```
//!
//! The first argument MUST be a string literal (the sanctioned static
//! spine). A non-literal first argument — a runtime `String`, an
//! identifier, an expression — is rejected with a clear diagnostic: there
//! is no runtime-string → SQL-skeleton path.
//!
//! Each `{}` in the literal consumes the next positional argument and
//! becomes a [`Chunk::Hole`]. `{{` / `}}` are `format!`-style escapes for
//! a literal `{` / `}`. The number of `{}` holes must equal the number of
//! arguments, else a compile error.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, LitStr, Token};

/// Parsed `fragment!` input: the literal skeleton plus the positional
/// argument expressions.
struct FragmentInput {
    skeleton: LitStr,
    args: Vec<Expr>,
}

impl Parse for FragmentInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        // The first token MUST be a string literal. `syn::LitStr::parse`
        // gives a precise diagnostic on anything else (a runtime String,
        // an identifier, a call) — but we override the message to name the
        // absolute rule explicitly.
        if !input.peek(LitStr) {
            return Err(syn::Error::new(
                input.span(),
                "fragment!: the SQL skeleton must be a single string \
                 literal (not an identifier, a runtime `String`, or an \
                 expression). There is no runtime-string -> SQL path. Use \
                 `fragment!(\"SELECT ... WHERE id = {}\", value)`.",
            ));
        }
        let skeleton: LitStr = input.parse()?;

        // Zero-or-more `, <expr>` argument expressions, trailing comma OK.
        let mut args: Vec<Expr> = Vec::new();
        while input.peek(Token![,]) {
            let _comma: Token![,] = input.parse()?;
            if input.is_empty() {
                break; // trailing comma
            }
            args.push(input.parse()?);
        }

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "fragment!: unexpected tokens after the argument list — \
                 expected `fragment!(\"...\", arg0, arg1, ...)`.",
            ));
        }

        Ok(FragmentInput { skeleton, args })
    }
}

/// One lexed piece of the skeleton: literal text or a value hole.
enum Segment {
    /// Verbatim literal text (with `{{`/`}}` already unescaped). Becomes a
    /// `Chunk::Rodata` `&'static str`.
    Lit(String),
    /// A `{}` value hole. Becomes a `Chunk::Hole(IntoBound::into_bound(..))`.
    Hole,
}

/// Split a format-style skeleton into literal segments and holes.
///
/// `{{` -> literal `{`, `}}` -> literal `}`, `{}` -> a hole. A bare `{`
/// not followed by `}`/`{`, a `{` with anything other than `}` inside
/// (e.g. `{0}`, `{name}`), or a bare `}` is an error — only positional
/// `{}` holes are supported in this slice.
fn lex_skeleton(skeleton: &str, span: Span) -> syn::Result<Vec<Segment>> {
    let mut segments: Vec<Segment> = Vec::new();
    let mut current = String::new();
    let mut chars = skeleton.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '{' => {
                if chars.peek() == Some(&'{') {
                    // `{{` escape -> literal `{`.
                    let _ = chars.next();
                    current.push('{');
                } else if chars.peek() == Some(&'}') {
                    // `{}` -> a hole. Flush any pending literal first.
                    let _ = chars.next();
                    if !current.is_empty() {
                        segments.push(Segment::Lit(core::mem::take(&mut current)));
                    }
                    segments.push(Segment::Hole);
                } else {
                    return Err(syn::Error::new(
                        span,
                        "fragment!: only positional `{}` holes are \
                         supported. A `{` must be either `{}` (a value \
                         hole) or `{{` (an escaped literal brace); named \
                         or indexed holes (`{name}`, `{0}`) are not \
                         supported.",
                    ));
                }
            }
            '}' => {
                if chars.peek() == Some(&'}') {
                    // `}}` escape -> literal `}`.
                    let _ = chars.next();
                    current.push('}');
                } else {
                    return Err(syn::Error::new(
                        span,
                        "fragment!: a bare `}` is not allowed. Use `}}` for \
                         a literal closing brace.",
                    ));
                }
            }
            other => current.push(other),
        }
    }

    if !current.is_empty() {
        segments.push(Segment::Lit(current));
    }

    Ok(segments)
}

/// Inner implementation: parse, lex, arity-check, emit.
pub(crate) fn fragment_impl(input: TokenStream2) -> syn::Result<TokenStream2> {
    let FragmentInput { skeleton, args } = syn::parse2(input)?;
    let span = skeleton.span();
    let skeleton_str = skeleton.value();

    let segments = lex_skeleton(&skeleton_str, span)?;

    let hole_count = segments
        .iter()
        .filter(|s| matches!(s, Segment::Hole))
        .count();

    if hole_count != args.len() {
        return Err(syn::Error::new(
            span,
            alloc::format!(
                "fragment!: the skeleton has {hole_count} `{{}}` hole(s) but \
                 {} argument(s) were supplied — every hole consumes exactly \
                 one positional argument.",
                args.len()
            ),
        ));
    }

    // Build the `vec![Chunk::Rodata(..), Chunk::Hole(..), ...]` element
    // list, walking segments and pulling the next arg at each hole.
    let mut chunk_exprs: Vec<TokenStream2> = Vec::new();
    let mut arg_iter = args.into_iter();
    for seg in segments {
        match seg {
            Segment::Lit(text) => {
                chunk_exprs.push(quote! {
                    ::bsql_postgres_core::fragment::Chunk::Rodata(#text)
                });
            }
            Segment::Hole => {
                // arity-checked above, so `next()` is always `Some`; the
                // `else` branch is a typed error rather than a panic to
                // honor the derive crate's `forbid(clippy::panic)`.
                let Some(arg) = arg_iter.next() else {
                    return Err(syn::Error::new(
                        span,
                        "fragment!: internal arity desync (more holes than \
                         arguments) — this is a bug in `fragment!`.",
                    ));
                };
                chunk_exprs.push(quote! {
                    ::bsql_postgres_core::fragment::Chunk::Hole(
                        ::bsql_postgres_core::fragment::IntoBound::into_bound(#arg)
                    )
                });
            }
        }
    }

    Ok(quote! {
        ::bsql_postgres_core::fragment::Fragment::__from_chunks(
            ::std::vec![ #( #chunk_exprs ),* ]
        )
    })
}
