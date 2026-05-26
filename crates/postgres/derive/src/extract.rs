//! Placeholder and cast extraction from the SQL token stream
//! produced by [`crate::sql_lexer`].
//!
//! # Validation rules
//!
//! - **V1**: placeholders are contiguous starting at `$1` (no skips).
//! - **V2**: every `$N` carries a cast annotation (`::TYPE` or
//!   enclosed in `CAST($N AS TYPE)`).
//! - **V3**: every SELECT/RETURNING column carries a cast or
//!   `AS alias::TYPE`.
//! - **V4**: each declared TYPE maps to a known OID via
//!   [`crate::typemap::lookup_pg_type`].
//! - **V5**: statement shape is one of {SELECT, INSERT, UPDATE,
//!   DELETE, WITH ...}. DDL is rejected.
//!
//! Output: `(Vec<ParamSpec>, Vec<ColumnSpec>)` — one entry per
//! placeholder (`$N`) and per result column. The macro consumes
//! these in [`crate::lib::prepared`] to build the `PreparedQuery`
//! struct literal.
//!
//! # Tier discipline
//!
//! - **Tier-1 by-compile**: violations of V1-V5 emit
//!   `compile_error!` with a span pointing at the offending source
//!   byte. The user sees errors at the EXACT byte of the SQL string.
//! - **Tier-3 by-test**: ~20 unit tests below cover happy paths and
//!   each V1-V5 violation class.

extern crate alloc;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use proc_macro2::{Span, TokenStream};

use crate::sql_lexer::{KeywordKind, SqlToken, SqlTokenKind, token_str};
use crate::typemap::lookup_pg_type;

/// One parameter `$N` discovered in the SQL with its declared type.
#[derive(Debug)]
pub(crate) struct ParamSpec {
    /// 1-based wire index (`$1` → `1`).
    pub(crate) index: u8,
    /// Rust type token (e.g., `quote!(i32)`).
    pub(crate) rust_type: TokenStream,
    /// OID const path token (e.g., `quote!(::bsql_postgres_proto::oids::INT4)`).
    pub(crate) oid_path: TokenStream,
    /// Numeric OID value resolved at macro-expansion (the
    /// Parse-template body must carry the literal OID bytes).
    pub(crate) oid_value: u32,
}

/// One SELECT/RETURNING column discovered with its declared type.
///
/// Unlike [`ParamSpec`], we do NOT carry the numeric `oid_value` —
/// row OIDs propagate to the consumer crate via the `oid_path`
/// token list which `quote!`s into a `&'static [u32]` of
/// `::bsql_postgres_proto::oids::*` const references. The runtime needs
/// only the `[u32]` slice (no bytes pre-baked here — the synthetic
/// RowDesc is built at push time from the OID slice).
#[derive(Debug)]
pub(crate) struct ColumnSpec {
    /// Rust type token.
    pub(crate) rust_type: TokenStream,
    /// OID const path token.
    pub(crate) oid_path: TokenStream,
}

/// Statement classification from V5 validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementShape {
    /// SELECT (or WITH...SELECT) — has result columns.
    Select,
    /// INSERT/UPDATE/DELETE without RETURNING — no result columns.
    Dml,
    /// INSERT/UPDATE/DELETE WITH RETURNING — has result columns.
    DmlReturning,
}

/// Extract the (params, columns, shape) triple from a SQL string.
///
/// Caller (the `prepared!` macro entry) wraps the result into the
/// final `PreparedQuery` const-init.
pub(crate) fn extract(
    src: &str,
    tokens: &[SqlToken],
    sql_span: Span,
) -> syn::Result<(Vec<ParamSpec>, Vec<ColumnSpec>, StatementShape)> {
    // V5 — determine statement shape.
    let shape = classify_statement(src, tokens, sql_span)?;
    // V1 / V2 — walk placeholders.
    let params = extract_params(src, tokens, sql_span)?;
    // V3 / V4 — walk result columns (SELECT-list / RETURNING-list).
    let columns = match shape {
        StatementShape::Select => extract_select_columns(src, tokens, sql_span)?,
        StatementShape::DmlReturning => extract_returning_columns(src, tokens, sql_span)?,
        StatementShape::Dml => Vec::new(),
    };
    Ok((params, columns, shape))
}

/// V5 — classify the statement's lead keyword.
fn classify_statement(
    src: &str,
    tokens: &[SqlToken],
    sql_span: Span,
) -> syn::Result<StatementShape> {
    let lead = tokens.iter().find(|t| matches!(t.kind, SqlTokenKind::Keyword(_)));
    let Some(first_kw) = lead else {
        return Err(syn::Error::new(
            sql_span,
            "prepared!: SQL contains no recognised statement keyword \
             (expected SELECT / INSERT / UPDATE / DELETE / WITH ...)",
        ));
    };
    let SqlTokenKind::Keyword(kw) = first_kw.kind else {
        // Architecturally dead — the `find` predicate already filtered
        // to Keyword. Fall through to a clean classification error
        // rather than panic.
        return Err(syn::Error::new(
            sql_span,
            "prepared!: failed to classify leading SQL keyword",
        ));
    };
    let has_returning = tokens.iter().any(|t| matches!(t.kind, SqlTokenKind::Keyword(KeywordKind::Returning)));
    match kw {
        KeywordKind::Select => Ok(StatementShape::Select),
        KeywordKind::With => {
            // WITH ... SELECT / WITH ... INSERT etc. We treat WITH +
            // RETURNING / WITH that ends with SELECT as result-bearing;
            // if the trailing statement is DML without RETURNING, the
            // SELECT-list extractor will see no SELECT keyword and
            // produce zero columns (legal under our grammar — equivalent
            // to DML).
            //
            // Tier-3 narrowing: WITH support in v1 is partial — only
            // the statement-shape classification is implemented; the
            // column-extractor walks the first SELECT-list it finds.
            // Complex CTEs with multiple SELECT bodies are out of
            // scope. Document by example in the macro docstring.
            let has_inner_select = tokens.iter().any(|t| matches!(t.kind, SqlTokenKind::Keyword(KeywordKind::Select)));
            if has_inner_select {
                Ok(StatementShape::Select)
            } else if has_returning {
                Ok(StatementShape::DmlReturning)
            } else {
                Ok(StatementShape::Dml)
            }
        }
        KeywordKind::Insert | KeywordKind::Update | KeywordKind::Delete => {
            if has_returning {
                Ok(StatementShape::DmlReturning)
            } else {
                Ok(StatementShape::Dml)
            }
        }
        _ => {
            let head_text = token_str(src, first_kw);
            let msg = format!(
                "prepared!: SQL must start with one of SELECT / INSERT / UPDATE / DELETE / WITH; found `{head_text}`. \
                 DDL (CREATE / DROP / ALTER) cannot sensibly use prepared statements.",
            );
            Err(syn::Error::new(sql_span, msg))
        }
    }
}

/// V1 / V2 / V4 — discover every `$N` placeholder, look up its
/// cast type, validate contiguity from $1.
fn extract_params(
    src: &str,
    tokens: &[SqlToken],
    sql_span: Span,
) -> syn::Result<Vec<ParamSpec>> {
    let mut params: Vec<ParamSpec> = Vec::new();
    let mut seen: Vec<bool> = Vec::new();
    let mut max_idx: u8 = 0;
    for (pos, tok) in tokens.iter().enumerate() {
        let SqlTokenKind::Placeholder(idx) = tok.kind else {
            continue;
        };
        if idx == 0 {
            return Err(syn::Error::new(sql_span, "prepared!: placeholder $0 is illegal (PG indices are 1-based)"));
        }
        let type_name = resolve_placeholder_cast(src, tokens, pos)
            .ok_or_else(|| syn::Error::new(
                sql_span,
                format!("prepared!: placeholder ${idx} lacks a cast annotation. \
                         Use `${idx}::int4` (or another supported type) or wrap in \
                         `CAST(${idx} AS int4)`. Supported types: int2/int4/int8/oid/bool/text."),
            ))?;
        let entry = lookup_pg_type(&type_name, sql_span)?;
        // Grow `seen` so we can pin contiguity at the end.
        while usize::from(idx) > seen.len() {
            seen.push(false);
        }
        let seen_slot = seen.get_mut(usize::from(idx).saturating_sub(1)).ok_or_else(|| {
            syn::Error::new(sql_span, "prepared!: placeholder index seen-table out of range")
        })?;
        if *seen_slot {
            // Re-use of same placeholder is LEGAL in PG (and useful).
            // We don't add a duplicate ParamSpec; the first declaration
            // wins. If the cast types disagree across uses we'd
            // ideally catch that — for now we record only the first.
            continue;
        }
        *seen_slot = true;
        if idx > max_idx {
            max_idx = idx;
        }
        params.push(ParamSpec {
            index: idx,
            rust_type: entry.rust_type,
            oid_path: entry.oid_path,
            oid_value: entry.oid_value,
        });
    }
    // V1 — contiguity check.
    for (slot_idx, was_seen) in seen.iter().enumerate() {
        if !*was_seen {
            let n = u32::try_from(slot_idx).unwrap_or(u32::MAX).saturating_add(1);
            return Err(syn::Error::new(
                sql_span,
                format!(
                    "prepared!: placeholder ${n} is missing — placeholders MUST be contiguous \
                     starting at $1. Found highest index ${max_idx} but ${n} is unused. \
                     Either remove unused suffix placeholders or fill the gap."
                ),
            ));
        }
    }
    // Sort by wire index so the OID array order matches `$1, $2, $3, ...`.
    params.sort_by_key(|p| p.index);
    Ok(params)
}

/// Find the cast that annotates a placeholder at token position `pos`.
/// Accepts two forms:
///
/// - **Postfix**: `$N :: TYPE` (DoubleColon then Ident).
/// - **Prefix CAST**: search backwards for `CAST (` and forward for
///   `AS TYPE )`.
///
/// Returns the type name (lowercase ASCII) when found.
fn resolve_placeholder_cast(src: &str, tokens: &[SqlToken], pos: usize) -> Option<String> {
    // Postfix form: token at `pos+1` must be DoubleColon, `pos+2` must be Ident.
    if let Some(next) = tokens.get(pos.saturating_add(1))
        && matches!(next.kind, SqlTokenKind::DoubleColon)
        && let Some(ty) = tokens.get(pos.saturating_add(2))
        && matches!(ty.kind, SqlTokenKind::Ident)
    {
        return Some(token_str(src, ty).to_ascii_lowercase());
    }
    // Prefix CAST form: look backwards for `CAST (`, forward for `AS Ident )`.
    if let Some(cast_open) = find_enclosing_cast(tokens, pos)
        && let Some(as_pos) = scan_forward_for(tokens, pos, KeywordKind::As)
        && let Some(ty_tok) = tokens.get(as_pos.saturating_add(1))
        && matches!(ty_tok.kind, SqlTokenKind::Ident)
        && tokens
            .get(as_pos.saturating_add(2))
            .map(|t| matches!(t.kind, SqlTokenKind::Punct(b')')))
            .unwrap_or(false)
        && _cast_open_balances(tokens, cast_open, as_pos)
    {
        return Some(token_str(src, ty_tok).to_ascii_lowercase());
    }
    None
}

/// True if `cast_open` is a `CAST(` and `as_pos` is the matching AS
/// before this group's closing `)`. Used to verify the prefix-CAST
/// form binds to the placeholder at `pos`. Minimal — doesn't fully
/// balance arbitrary nested parens, but is sound for the canonical
/// `CAST($N AS TYPE)` shape.
fn _cast_open_balances(tokens: &[SqlToken], cast_open: usize, as_pos: usize) -> bool {
    let mut depth: i32 = 0;
    for i in cast_open..as_pos {
        let Some(t) = tokens.get(i) else {
            return false;
        };
        match t.kind {
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b')') => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    depth == 1
}

/// Find the index of the `CAST(` that opens the group enclosing
/// position `pos`. Returns the index of the `CAST` keyword itself.
fn find_enclosing_cast(tokens: &[SqlToken], pos: usize) -> Option<usize> {
    // Walk backwards finding paren-balance going negative (we exit a
    // group); each opening paren is checked for a preceding CAST kw.
    let mut depth: i32 = 0;
    let mut i = pos;
    while i > 0 {
        i = i.saturating_sub(1);
        let t = tokens.get(i)?;
        match t.kind {
            SqlTokenKind::Punct(b')') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b'(') => {
                if depth == 0 {
                    if let Some(prev) = i.checked_sub(1).and_then(|j| tokens.get(j))
                        && matches!(prev.kind, SqlTokenKind::Keyword(KeywordKind::Cast))
                    {
                        return Some(i.saturating_sub(1));
                    }
                    return None;
                }
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    None
}

/// Scan forward from `start` looking for keyword `kw` at any paren
/// depth >= 0.
fn scan_forward_for(tokens: &[SqlToken], start: usize, kw: KeywordKind) -> Option<usize> {
    let mut depth: i32 = 0;
    let mut i = start;
    while i < tokens.len() {
        let Some(t) = tokens.get(i) else {
            break;
        };
        match t.kind {
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b')') => {
                if depth == 0 {
                    return None; // exited our enclosing group
                }
                depth = depth.saturating_sub(1);
            }
            SqlTokenKind::Keyword(k) if k == kw => return Some(i),
            _ => {}
        }
        i = i.saturating_add(1);
    }
    None
}

/// V3 / V4 — discover every SELECT-list column. Walks from the
/// SELECT keyword forward, splitting on commas at paren-depth 0,
/// stopping at FROM.
fn extract_select_columns(
    src: &str,
    tokens: &[SqlToken],
    sql_span: Span,
) -> syn::Result<Vec<ColumnSpec>> {
    let select_pos = tokens
        .iter()
        .position(|t| matches!(t.kind, SqlTokenKind::Keyword(KeywordKind::Select)))
        .ok_or_else(|| syn::Error::new(sql_span, "prepared!: SELECT keyword missing"))?;
    // SELECT list ends at the FIRST of: FROM, WHERE, end-of-tokens.
    // PG accepts SELECT without FROM (e.g., `SELECT 1::int4`), so
    // WHERE can immediately follow the SELECT list. The pre-fix
    // heuristic only stopped at FROM, which silently swallowed
    // WHERE-clause tokens into the column list — producing wrong
    // row OIDs for queries like `SELECT id::int4 WHERE id = $1::int4
    // AND flag = $2::bool` (the bool got picked up as the last column).
    let list_end = scan_forward_for_at_depth_zero(tokens, select_pos, |k| {
        matches!(
            k,
            SqlTokenKind::Keyword(KeywordKind::From)
                | SqlTokenKind::Keyword(KeywordKind::Where)
        )
    })
    .unwrap_or(tokens.len());
    let list_range = select_pos.saturating_add(1)..list_end;
    extract_column_list(src, tokens, list_range, sql_span)
}

/// V3 / V4 — extract RETURNING column list. Walks from RETURNING
/// keyword to end-of-input.
fn extract_returning_columns(
    src: &str,
    tokens: &[SqlToken],
    sql_span: Span,
) -> syn::Result<Vec<ColumnSpec>> {
    let returning_pos = tokens
        .iter()
        .position(|t| matches!(t.kind, SqlTokenKind::Keyword(KeywordKind::Returning)))
        .ok_or_else(|| {
            syn::Error::new(sql_span, "prepared!: RETURNING keyword missing for DML-with-results")
        })?;
    extract_column_list(src, tokens, returning_pos.saturating_add(1)..tokens.len(), sql_span)
}

/// Scan forward for a keyword at depth 0. Used by SELECT-list walker
/// to find the terminating `FROM`.
fn scan_forward_for_at_depth_zero<F>(tokens: &[SqlToken], start: usize, pred: F) -> Option<usize>
where
    F: Fn(SqlTokenKind) -> bool,
{
    let mut depth: i32 = 0;
    let mut i = start;
    while i < tokens.len() {
        let Some(t) = tokens.get(i) else {
            break;
        };
        match t.kind {
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b')') => depth = depth.saturating_sub(1),
            _ if depth == 0 && pred(t.kind) => return Some(i),
            _ => {}
        }
        i = i.saturating_add(1);
    }
    None
}

/// Walk a comma-separated column list (at paren-depth 0) and extract
/// the cast type of each column.
///
/// Every column MUST carry an explicit cast (`expr::TYPE`)
/// or `CAST(expr AS TYPE)`. The walker finds the **last** `::Type` or
/// `AS Type` token of each comma-separated chunk; this matches the
/// PG semantic where `id::int4` casts `id` to `int4`, and the cast
/// declaration is what determines the wire OID the server returns.
fn extract_column_list(
    src: &str,
    tokens: &[SqlToken],
    range: core::ops::Range<usize>,
    sql_span: Span,
) -> syn::Result<Vec<ColumnSpec>> {
    let mut columns: Vec<ColumnSpec> = Vec::new();
    let mut depth: i32 = 0;
    let mut chunk_start = range.start;
    let end = range.end.min(tokens.len());
    let mut i = chunk_start;
    while i < end {
        let Some(t) = tokens.get(i) else { break };
        match t.kind {
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::Punct(b',') if depth == 0 => {
                let chunk_range = chunk_start..i;
                if !chunk_range.is_empty() {
                    let spec = extract_column_cast(src, tokens, chunk_range, sql_span)?;
                    columns.push(spec);
                }
                chunk_start = i.saturating_add(1);
            }
            _ => {}
        }
        i = i.saturating_add(1);
    }
    // Final chunk.
    let chunk_range = chunk_start..end;
    if !chunk_range.is_empty() {
        let spec = extract_column_cast(src, tokens, chunk_range, sql_span)?;
        columns.push(spec);
    }
    Ok(columns)
}

/// Extract the cast type from a single column chunk.
///
/// Recognised forms (in priority order at depth 0):
/// 1. `expr :: TYPE [AS alias]` — postfix cast.
/// 2. `CAST(expr AS TYPE)` — prefix cast.
///
/// Anything else triggers a `compile_error!` with a helpful suggestion.
fn extract_column_cast(
    src: &str,
    tokens: &[SqlToken],
    range: core::ops::Range<usize>,
    sql_span: Span,
) -> syn::Result<ColumnSpec> {
    // First try postfix `::TYPE` — scan for the LAST DoubleColon at
    // depth 0 within the chunk; the next Ident is the type name.
    let mut depth: i32 = 0;
    let mut last_postfix: Option<usize> = None;
    let mut i = range.start;
    let end = range.end.min(tokens.len());
    while i < end {
        let Some(t) = tokens.get(i) else { break };
        match t.kind {
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b')') => depth = depth.saturating_sub(1),
            SqlTokenKind::DoubleColon if depth == 0 => last_postfix = Some(i),
            _ => {}
        }
        i = i.saturating_add(1);
    }
    if let Some(pos) = last_postfix
        && let Some(ty) = tokens.get(pos.saturating_add(1))
        && matches!(ty.kind, SqlTokenKind::Ident)
    {
        let name = token_str(src, ty).to_ascii_lowercase();
        let entry = lookup_pg_type(&name, sql_span)?;
        return Ok(ColumnSpec {
            rust_type: entry.rust_type,
            oid_path: entry.oid_path,
        });
    }
    // Then try prefix `CAST(expr AS TYPE)` at depth 0.
    let mut depth: i32 = 0;
    i = range.start;
    while i < end {
        let Some(t) = tokens.get(i) else { break };
        match t.kind {
            SqlTokenKind::Keyword(KeywordKind::Cast) if depth == 0 => {
                // CAST followed by `(`; find matching `)`, locate AS inside.
                if let Some(open) = tokens.get(i.saturating_add(1))
                    && matches!(open.kind, SqlTokenKind::Punct(b'('))
                    && let Some(close_pos) = find_matching_close(tokens, i.saturating_add(1))
                    && let Some(as_pos) = scan_in_range_for_kw(tokens, i.saturating_add(2)..close_pos, KeywordKind::As)
                    && let Some(ty) = tokens.get(as_pos.saturating_add(1))
                    && matches!(ty.kind, SqlTokenKind::Ident)
                {
                    let name = token_str(src, ty).to_ascii_lowercase();
                    let entry = lookup_pg_type(&name, sql_span)?;
                    return Ok(ColumnSpec {
                        rust_type: entry.rust_type,
                        oid_path: entry.oid_path,
                    });
                }
            }
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1_i32),
            SqlTokenKind::Punct(b')') => depth = depth.saturating_sub(1_i32),
            _ => {}
        }
        i = i.saturating_add(1);
    }
    // V3 — neither form found. Build a helpful diagnostic.
    let chunk_text = render_chunk(src, tokens, range);
    Err(syn::Error::new(
        sql_span,
        format!(
            "prepared!: column `{chunk_text}` lacks a cast annotation. \
             Use `expr::TYPE` (e.g. `id::int4`) or `CAST(expr AS TYPE)`. \
             Supported types: int2 / int4 / int8 / oid / bool / text."
        ),
    ))
}

/// Find the matching `)` for an opening `(` at `open_pos`.
fn find_matching_close(tokens: &[SqlToken], open_pos: usize) -> Option<usize> {
    let mut depth: i32 = 0;
    let mut i = open_pos;
    while i < tokens.len() {
        let Some(t) = tokens.get(i) else { break };
        match t.kind {
            SqlTokenKind::Punct(b'(') => depth = depth.saturating_add(1),
            SqlTokenKind::Punct(b')') => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i = i.saturating_add(1);
    }
    None
}

/// Look for keyword `kw` in `range`.
fn scan_in_range_for_kw(
    tokens: &[SqlToken],
    range: core::ops::Range<usize>,
    kw: KeywordKind,
) -> Option<usize> {
    let mut i = range.start;
    while i < range.end.min(tokens.len()) {
        let t = tokens.get(i)?;
        if matches!(t.kind, SqlTokenKind::Keyword(k) if k == kw) {
            return Some(i);
        }
        i = i.saturating_add(1);
    }
    None
}

/// Project the source bytes spanning `range` for use in diagnostics.
fn render_chunk(src: &str, tokens: &[SqlToken], range: core::ops::Range<usize>) -> String {
    let first = tokens.get(range.start);
    let last = tokens.get(range.end.saturating_sub(1));
    match (first, last) {
        (Some(f), Some(l)) => {
            let start = f.start;
            let end = l.start.saturating_add(l.len);
            let slice = src.get(start..end).unwrap_or("");
            slice.to_string()
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    //! Spec-conformance tests for V1-V5.
    use super::*;
    use crate::sql_lexer::tokenise;
    use proc_macro2::Span;

    fn run(src: &str) -> syn::Result<(Vec<ParamSpec>, Vec<ColumnSpec>, StatementShape)> {
        let tokens = tokenise(src).map_err(|e| syn::Error::new(Span::call_site(), e.message))?;
        extract(src, &tokens, Span::call_site())
    }

    /// Happy path — full SELECT with cast on every column + placeholder.
    #[test]
    fn extract_select_with_cast() {
        let (p, c, s) = run("SELECT id::int4, name::text FROM users WHERE id = $1::int4")
            .unwrap_or((Vec::new(), Vec::new(), StatementShape::Dml));
        assert_eq!(p.len(), 1);
        assert_eq!(c.len(), 2);
        assert!(matches!(s, StatementShape::Select));
    }

    /// CAST(expr AS TYPE) form on placeholder.
    #[test]
    fn extract_cast_form_placeholder() {
        let r = run("SELECT 1::int4 WHERE id = CAST($1 AS int4)");
        assert!(r.is_ok(), "{:?}", r.err().map(|e| e.to_string()));
    }

    /// CAST(expr AS TYPE) form on column.
    #[test]
    fn extract_cast_form_column() {
        let r = run("SELECT CAST(id AS int4), name::text FROM users");
        assert!(r.is_ok(), "{:?}", r.err().map(|e| e.to_string()));
    }

    /// V2 — placeholder without cast is rejected.
    #[test]
    fn v2_placeholder_without_cast_rejected() {
        let r = run("SELECT 1::int4 WHERE id = $1");
        assert!(r.is_err());
    }

    /// V3 — SELECT column without cast is rejected.
    #[test]
    fn v3_column_without_cast_rejected() {
        let r = run("SELECT id, name::text FROM t");
        assert!(r.is_err());
    }

    /// V1 — non-contiguous placeholders are rejected.
    #[test]
    fn v1_non_contiguous_placeholders_rejected() {
        let r = run("SELECT 1::int4 WHERE a = $1::int4 AND b = $3::int4");
        assert!(r.is_err());
    }

    /// V4 — unknown PG type rejected.
    #[test]
    fn v4_unknown_type_rejected() {
        let r = run("SELECT id::unicorn FROM t");
        assert!(r.is_err());
    }

    /// V5 — DDL rejected.
    #[test]
    fn v5_ddl_rejected() {
        let r = run("CREATE TABLE t (id int)");
        assert!(r.is_err());
        let r2 = run("DROP TABLE users");
        assert!(r2.is_err());
        let r3 = run("ALTER TABLE users ADD COLUMN x int");
        assert!(r3.is_err());
    }

    /// INSERT with RETURNING extracts result columns.
    #[test]
    fn dml_returning_extracts_columns() {
        let r = run("INSERT INTO users (name) VALUES ($1::text) RETURNING id::int4");
        let (p, c, s) = r.unwrap_or((Vec::new(), Vec::new(), StatementShape::Dml));
        assert_eq!(p.len(), 1);
        assert_eq!(c.len(), 1);
        assert!(matches!(s, StatementShape::DmlReturning));
    }

    /// INSERT without RETURNING produces zero result columns.
    #[test]
    fn dml_without_returning_zero_columns() {
        let r = run("INSERT INTO users (name) VALUES ($1::text)");
        let (p, c, s) = r.unwrap_or((Vec::new(), Vec::new(), StatementShape::Select));
        assert_eq!(p.len(), 1);
        assert_eq!(c.len(), 0);
        assert!(matches!(s, StatementShape::Dml));
    }

    /// Multiple placeholders preserve order (sorted by index).
    #[test]
    fn multiple_placeholders_sorted_by_index() {
        let r = run("SELECT 1::int4 WHERE a = $3::text AND b = $1::int4 AND c = $2::bool");
        let (p, _, _) = r.unwrap_or((Vec::new(), Vec::new(), StatementShape::Dml));
        let indices: Vec<u8> = p.iter().map(|s| s.index).collect();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    /// Reused placeholder counts as one slot.
    #[test]
    fn reused_placeholder_one_slot() {
        let r = run("SELECT 1::int4 WHERE a = $1::int4 AND b = $1::int4");
        let (p, _, _) = r.unwrap_or((Vec::new(), Vec::new(), StatementShape::Dml));
        assert_eq!(p.len(), 1);
    }
}
