//! SQL parser for `bsql::query!`.
//!
//! This parser does NOT understand SQL semantics. It extracts:
//! - Parameter bindings (`$name: Type`)
//! - Query kind (SELECT / INSERT / UPDATE / DELETE)
//! - Whether RETURNING is present
//!
//! Everything else is passed through to PostgreSQL verbatim. PG does the real
//! SQL parsing via PREPARE.

use smallvec::SmallVec;

use crate::sql_norm::normalize_sql;
use crate::stmt_name::statement_name;

/// A parsed parameter from the SQL text.
#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    /// Parameter name as written by the user (e.g. `"id"`).
    pub name: String,
    /// Rust type as written by the user (e.g. `"i32"`, `"&str"`).
    pub rust_type: String,
    /// 1-based positional index in the output SQL (`$1`, `$2`, ...).
    pub position: usize,
}

/// An optional clause: a SQL fragment wrapped in `[...]` that is
/// included/excluded at runtime based on `Option` parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionalClause {
    /// The SQL fragment inside `[...]` (with params replaced by `$N`).
    pub sql_fragment: String,
    /// Parameters declared inside this clause (must be `Option<T>`).
    /// Typically 1 param per clause (enforced by parser).
    pub params: SmallVec<[Param; 2]>,
    /// 0-based index among optional clauses.
    pub index: usize,
}

/// The kind of SQL statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    Select,
    Insert,
    Update,
    Delete,
}

/// Result of parsing a `query!` macro invocation.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Normalized SQL with params replaced by `$1`, `$2`, etc.
    /// Whitespace collapsed, keywords lowercased, comments stripped.
    /// For dynamic queries, optional clause placeholders are `{OPT_N}`.
    pub normalized_sql: String,
    /// SQL with params replaced by `$1`, `$2`, etc. but NOT normalized
    /// (preserves original formatting for error messages).
    /// For dynamic queries, optional clause placeholders are `{OPT_N}`.
    pub positional_sql: String,
    /// Extracted parameters in order of appearance (base query only).
    /// Parameters inside optional clauses are in `optional_clauses[i].params`.
    pub params: SmallVec<[Param; 4]>,
    /// What kind of DML this is.
    pub kind: QueryKind,
    /// Prepared statement name: `s_{rapidhash:016x}`.
    pub statement_name: String,
    /// Optional clauses extracted from `[...]` blocks.
    pub optional_clauses: SmallVec<[OptionalClause; 4]>,
}

/// Parse the raw SQL from a `query!` invocation.
///
/// The input is the literal SQL text between the braces of `query! { ... }`.
pub fn parse_query(sql: &str) -> Result<ParsedQuery, String> {
    if sql.trim().is_empty() {
        return Err("empty SQL query".into());
    }

    let comment_stripped = strip_comments(sql);
    let (positional_sql, params, optional_clauses) = extract_params(&comment_stripped)?;
    let normalized_sql = normalize_sql(&positional_sql);
    let kind = detect_query_kind(&normalized_sql)?;
    let stmt_name = statement_name(&normalized_sql);

    Ok(ParsedQuery {
        normalized_sql,
        positional_sql,
        params,
        kind,
        statement_name: stmt_name,
        optional_clauses,
    })
}

/// Extract `$name: Type` parameters from SQL, replacing them with `$1`, `$2`, ...
/// Also extracts `[...]` optional clause blocks.
///
/// Returns the rewritten SQL (with `{OPT_N}` placeholders for optional clauses),
/// the list of base parameters, and the list of optional clauses.
///
/// Uses `char_indices()` for iteration so multi-byte UTF-8 inside string
/// literals is preserved verbatim (we slice the original `&str` by byte
/// offset, never interpreting individual bytes as chars).
fn extract_params(
    sql: &str,
) -> Result<(String, SmallVec<[Param; 4]>, SmallVec<[OptionalClause; 4]>), String> {
    let mut out = String::with_capacity(sql.len());
    let mut params: SmallVec<[Param; 4]> = SmallVec::new();
    let mut optional_clauses: SmallVec<[OptionalClause; 4]> = SmallVec::new();
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0; // byte offset into `sql`

    while i < len {
        let b = bytes[i];

        // String literal: copy verbatim (preserves multi-byte UTF-8)
        if b == b'\'' {
            let start = i;
            i = skip_string_literal(bytes, i);
            out.push_str(&sql[start..i]);
            continue;
        }

        // Dollar-quoted string: copy verbatim
        if b == b'$'
            && i + 1 < len
            && (bytes[i + 1] == b'$' || bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_')
        {
            if let Some(end) = skip_dollar_quote(bytes, i) {
                out.push_str(&sql[i..end]);
                i = end;
                continue;
            }
        }

        // :: cast operator — NOT a param type separator
        if b == b':' && i + 1 < len && bytes[i + 1] == b':' {
            out.push_str("::");
            i += 2;
            continue;
        }

        // Optional clause: [SQL fragment with $param: Option<T>]
        if b == b'[' {
            let clause_idx = optional_clauses.len();
            let (clause, end) = parse_optional_clause(sql, i, &params)?;

            // Check for duplicate param names across optional clauses
            for prev_clause in &optional_clauses {
                for prev_param in &prev_clause.params {
                    for new_param in &clause.params {
                        if prev_param.name == new_param.name {
                            return Err(format!(
                                "parameter `${}` appears in multiple optional clauses \
                                 (clause {} and clause {}). Each optional clause must \
                                 have its own unique parameter.",
                                new_param.name, prev_clause.index, clause_idx
                            ));
                        }
                    }
                }
            }

            optional_clauses.push(OptionalClause {
                sql_fragment: clause.sql_fragment,
                params: clause.params,
                index: clause_idx,
            });
            // Insert a placeholder that dynamic.rs will replace per variant
            out.push_str(&format!("{{OPT_{clause_idx}}}"));
            i = end;
            continue;
        }

        // Unmatched ] outside a clause — error
        if b == b']' {
            return Err("unexpected `]` — not inside an optional clause `[...]`".into());
        }

        // Parameter: $name: Type
        if b == b'$' && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() {
            let (param, end) = parse_one_param(sql, i)?;

            // FIX 7: allow duplicate parameter names if types match
            if let Some(existing) = params.iter().find(|p| p.name == param.name) {
                if existing.rust_type != param.rust_type {
                    return Err(format!(
                        "parameter `${}` declared with conflicting types: `{}` and `{}`",
                        param.name, existing.rust_type, param.rust_type
                    ));
                }
                // Reuse the same positional index
                out.push('$');
                out.push_str(&existing.position.to_string());
            } else {
                params.push(Param {
                    name: param.name,
                    rust_type: param.rust_type,
                    position: params.len() + 1,
                });
                out.push('$');
                out.push_str(&params.len().to_string());
            }
            i = end;
            continue;
        }

        // FIX 3: reject manual positional parameters ($1, $2, ...)
        if b == b'$' && i + 1 < len && bytes[i + 1].is_ascii_digit() {
            return Err(
                "manual positional parameters ($1, $2, ...) are not allowed \
                 in bsql — use $name: Type syntax instead"
                    .into(),
            );
        }

        // Outside of string literals, SQL is ASCII. Copy one byte.
        out.push(b as char);
        i += 1;
    }

    if optional_clauses.len() > 8 {
        return Err(format!(
            "query has {} optional clauses ({} variants) — maximum is 8 (256 variants). \
             Split the query into smaller queries with fewer optional filters.",
            optional_clauses.len(),
            1u32 << optional_clauses.len()
        ));
    }

    Ok((out, params, optional_clauses))
}

/// Parse a `[SQL fragment with $param: Option<T>]` optional clause starting at
/// byte position `start` (which is the `[` character).
///
/// Returns the parsed clause and the byte position after the closing `]`.
fn parse_optional_clause(
    sql: &str,
    start: usize,
    base_params: &[Param],
) -> Result<(OptionalClause, usize), String> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    // Skip opening [
    let mut i = start + 1;

    // Find the matching ] — respecting string literals and nested parens.
    // Nested [] is NOT allowed (no nesting of optional clauses).
    let mut clause_sql = String::new();
    let mut clause_params: SmallVec<[Param; 2]> = SmallVec::new();
    // Position counter for params within the clause (will be renumbered by dynamic.rs)
    let mut clause_param_pos = 0usize;

    while i < len {
        let b = bytes[i];

        // Closing bracket — end of optional clause
        if b == b']' {
            i += 1; // skip ]

            if clause_params.is_empty() {
                return Err(
                    "optional clause `[...]` must contain exactly one `$param: Option<T>` \
                     parameter. If this is not an optional clause, remove the brackets. \
                     For PostgreSQL array subscripts, use parentheses or the ARRAY keyword."
                        .into(),
                );
            }

            // Each optional clause must have exactly ONE unique parameter.
            // The clause is included/excluded based on that single Option.
            // Multiple independent params would require checking all of them,
            // and partial-Some is ambiguous. Use separate clauses instead:
            //   [AND a >= $lo: Option<i32>] [AND a <= $hi: Option<i32>]
            let unique_params: Vec<&str> = clause_params
                .iter()
                .map(|p| p.name.as_str())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            if unique_params.len() > 1 {
                return Err(format!(
                    "optional clause `[...]` must have exactly one parameter, found {}: {}. \
                     Split into separate clauses: [AND a = $p1: Option<T>] [AND b = $p2: Option<T>]",
                    unique_params.len(),
                    unique_params.join(", ")
                ));
            }

            return Ok((
                OptionalClause {
                    sql_fragment: clause_sql,
                    params: clause_params,
                    index: 0, // filled by caller
                },
                i,
            ));
        }

        // Nested [ — error
        if b == b'[' {
            return Err("nested optional clauses `[[...]]` are not supported — \
                 each optional clause must be a flat `[SQL fragment]`"
                .into());
        }

        // String literal inside clause: copy verbatim
        if b == b'\'' {
            let lit_start = i;
            i = skip_string_literal(bytes, i);
            clause_sql.push_str(&sql[lit_start..i]);
            continue;
        }

        // Dollar-quoted string inside clause
        if b == b'$'
            && i + 1 < len
            && (bytes[i + 1] == b'$' || bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_')
        {
            if let Some(end) = skip_dollar_quote(bytes, i) {
                clause_sql.push_str(&sql[i..end]);
                i = end;
                continue;
            }
        }

        // :: cast operator
        if b == b':' && i + 1 < len && bytes[i + 1] == b':' {
            clause_sql.push_str("::");
            i += 2;
            continue;
        }

        // Parameter inside clause: $name: Option<T>
        if b == b'$' && i + 1 < len && bytes[i + 1].is_ascii_alphabetic() {
            let (param, end) = parse_one_param(sql, i)?;

            // Validate: params inside optional clauses MUST be Option<T>
            if !param.rust_type.starts_with("Option<") {
                return Err(format!(
                    "parameter `${}` inside optional clause `[...]` must be \
                     `Option<T>`, found `{}`. Wrap the type: `Option<{}>`",
                    param.name, param.rust_type, param.rust_type
                ));
            }

            // Check for duplicate in base params — not allowed (a param is
            // either base or optional, not both)
            if base_params.iter().any(|p| p.name == param.name) {
                return Err(format!(
                    "parameter `${}` appears both in the base query and in an \
                     optional clause — each parameter must belong to exactly one scope",
                    param.name
                ));
            }

            // Check duplicate within this clause (reuse position)
            if let Some(existing) = clause_params.iter().find(|p| p.name == param.name) {
                if existing.rust_type != param.rust_type {
                    return Err(format!(
                        "parameter `${}` declared with conflicting types in optional \
                         clause: `{}` and `{}`",
                        param.name, existing.rust_type, param.rust_type
                    ));
                }
                clause_sql.push_str(&format!("${{P_{}}}", existing.position));
            } else {
                clause_param_pos += 1;
                clause_params.push(Param {
                    name: param.name,
                    rust_type: param.rust_type,
                    position: clause_param_pos,
                });
                clause_sql.push_str(&format!("${{P_{clause_param_pos}}}"));
            }
            i = end;
            continue;
        }

        // Reject manual positional inside clause
        if b == b'$' && i + 1 < len && bytes[i + 1].is_ascii_digit() {
            return Err(
                "manual positional parameters ($1, $2, ...) are not allowed \
                 in bsql — use $name: Type syntax instead"
                    .into(),
            );
        }

        clause_sql.push(b as char);
        i += 1;
    }

    Err("unclosed optional clause — missing `]`".into())
}

/// Parse a single `$name: Type` parameter starting at byte position `start`.
/// Operates on the `&str` directly, using byte offsets for slicing.
/// Returns (Param, end_byte_position).
fn parse_one_param(sql: &str, start: usize) -> Result<(Param, usize), String> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    // Skip $
    let mut i = start + 1;

    // Parse name: ASCII identifier chars (alphanumeric + _)
    let name_start = i;
    while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    let name = &sql[name_start..i];

    if name.is_empty() {
        return Err(format!(
            "expected parameter name after $ at position {start}"
        ));
    }

    // Skip whitespace before :
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // Expect :
    if i >= len || bytes[i] != b':' {
        return Err(format!(
            "expected `:` after parameter name `${name}` at position {start}"
        ));
    }
    i += 1; // skip :

    // But NOT :: (cast) — we already handle :: before reaching here,
    // so this shouldn't happen, but guard anyway
    if i < len && bytes[i] == b':' {
        return Err(format!(
            "unexpected `::` after `${name}:` — did you mean `${name}: Type`?"
        ));
    }

    // Skip whitespace before type
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    // Parse type: everything until a delimiter
    // Type can include: &str, &[u8], Vec<i32>, Option<i32>, etc.
    let type_start = i;
    let mut angle_depth: u32 = 0;
    let mut bracket_depth: u32 = 0;

    while i < len {
        match bytes[i] {
            b'<' => angle_depth += 1,
            b'>' => {
                if angle_depth == 0 {
                    break;
                }
                angle_depth -= 1;
            }
            b'[' => bracket_depth += 1,
            b']' => {
                if bracket_depth == 0 {
                    break;
                }
                bracket_depth -= 1;
            }
            b',' | b')' | b'\n' if angle_depth == 0 && bracket_depth == 0 => break,
            b' ' | b'\t' if angle_depth == 0 && bracket_depth == 0 => break,
            _ => {}
        }
        i += 1;
    }

    let rust_type = sql[type_start..i].trim();

    if rust_type.is_empty() {
        return Err(format!(
            "expected type after `${name}:` at position {start}"
        ));
    }

    Ok((
        Param {
            name: name.to_owned(),
            rust_type: rust_type.to_owned(),
            position: 0, // filled in by caller
        },
        i,
    ))
}

/// Strip SQL comments (`--` line, `/* */` block) while preserving string literals
/// and dollar-quoted strings. Must run before `extract_params` so that `$name: Type`
/// inside comments is ignored. Uses `&str` slicing to preserve UTF-8.
fn strip_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Single-quoted string: preserve verbatim
        if bytes[i] == b'\'' {
            let start = i;
            i = skip_string_literal(bytes, i);
            out.push_str(&sql[start..i]);
            continue;
        }

        // Dollar-quoted string: preserve verbatim
        if bytes[i] == b'$' {
            if let Some(end) = skip_dollar_quote(bytes, i) {
                out.push_str(&sql[i..end]);
                i = end;
                continue;
            }
        }

        // Line comment: skip to end of line
        if bytes[i] == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            out.push(' ');
            continue;
        }

        // Block comment: skip (with nesting support)
        if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            let mut depth = 1u32;
            while i + 1 < len && depth > 0 {
                if bytes[i] == b'/' && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                    continue;
                }
                i += 1;
            }
            out.push(' ');
            continue;
        }

        // Non-comment content: slice from original str to preserve UTF-8
        // ASCII bytes are single-byte in UTF-8, so this is safe for the
        // control characters above. For multi-byte chars outside quotes,
        // we need to advance by the full char width.
        let ch = sql[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }

    out
}

/// Skip a single-quoted string literal starting at `start` (the `'` byte).
/// Handles `''` escaped quotes. Returns the byte position after the closing `'`.
fn skip_string_literal(bytes: &[u8], start: usize) -> usize {
    let len = bytes.len();
    let mut i = start + 1;
    while i < len {
        if bytes[i] == b'\'' {
            i += 1;
            // Escaped quote '' — continue the literal
            if i < len && bytes[i] == b'\'' {
                i += 1;
                continue;
            }
            break;
        }
        i += 1;
    }
    i
}

/// Skip a dollar-quoted string starting at `start`. Returns end position, or None.
fn skip_dollar_quote(bytes: &[u8], start: usize) -> Option<usize> {
    let len = bytes.len();
    if start >= len || bytes[start] != b'$' {
        return None;
    }

    let tag_start = start + 1;
    let mut tag_end = tag_start;

    while tag_end < len && (bytes[tag_end].is_ascii_alphanumeric() || bytes[tag_end] == b'_') {
        tag_end += 1;
    }

    if tag_end >= len || bytes[tag_end] != b'$' {
        return None;
    }

    let tag_len = tag_end - tag_start + 2;
    let tag = &bytes[start..start + tag_len];
    let body_start = start + tag_len;

    let mut i = body_start;
    while i + tag_len <= len {
        if &bytes[i..i + tag_len] == tag {
            return Some(i + tag_len);
        }
        i += 1;
    }

    None
}

/// Detect the query kind from the first keyword.
fn detect_query_kind(normalized: &str) -> Result<QueryKind, String> {
    let first_word = normalized.split_whitespace().next().unwrap_or("");

    // Handle CTEs: WITH ... SELECT/INSERT/UPDATE/DELETE
    if first_word == "with" {
        // Find the main statement after the CTE
        // Simplified: look for select/insert/update/delete not inside parens
        let mut depth: i32 = 0;
        for word in normalized.split_whitespace() {
            let opens = word.matches('(').count() as i32;
            let closes = word.matches(')').count() as i32;
            depth += opens - closes;
            if depth < 0 {
                depth = 0;
            } // malformed SQL — PG will catch it

            match word {
                "select" if depth == 0 => return Ok(QueryKind::Select),
                "insert" if depth == 0 => return Ok(QueryKind::Insert),
                "update" if depth == 0 => return Ok(QueryKind::Update),
                "delete" if depth == 0 => return Ok(QueryKind::Delete),
                _ => {}
            }
        }
        return Err("CTE (WITH) must be followed by SELECT, INSERT, UPDATE, or DELETE".into());
    }

    match first_word {
        "select" => Ok(QueryKind::Select),
        "insert" => Ok(QueryKind::Insert),
        "update" => Ok(QueryKind::Update),
        "delete" => Ok(QueryKind::Delete),
        other => Err(format!(
            "unsupported statement type: `{other}`. bsql supports SELECT, INSERT, UPDATE, DELETE"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- extract_params ---

    #[test]
    fn simple_select_one_param() {
        let result = parse_query("SELECT id, name FROM users WHERE id = $id: i32").unwrap();
        assert_eq!(result.params.len(), 1);
        assert_eq!(result.params[0].name, "id");
        assert_eq!(result.params[0].rust_type, "i32");
        assert_eq!(result.params[0].position, 1);
        assert!(result.positional_sql.contains("$1"));
        assert!(!result.positional_sql.contains("$id"));
    }

    #[test]
    fn multiple_params() {
        let result =
            parse_query("INSERT INTO users (name, email) VALUES ($name: &str, $email: &str)")
                .unwrap();
        assert_eq!(result.params.len(), 2);
        assert_eq!(result.params[0].name, "name");
        assert_eq!(result.params[0].rust_type, "&str");
        assert_eq!(result.params[0].position, 1);
        assert_eq!(result.params[1].name, "email");
        assert_eq!(result.params[1].rust_type, "&str");
        assert_eq!(result.params[1].position, 2);
    }

    #[test]
    fn generic_type_param() {
        let result = parse_query("SELECT id FROM t WHERE ids = ANY($ids: &[i32])").unwrap();
        assert_eq!(result.params[0].rust_type, "&[i32]");
    }

    #[test]
    fn vec_type_param() {
        let result = parse_query("SELECT id FROM t WHERE id = ANY($ids: Vec<i32>)").unwrap();
        assert_eq!(result.params[0].rust_type, "Vec<i32>");
    }

    #[test]
    fn param_with_spaces_around_colon() {
        let result = parse_query("SELECT id FROM t WHERE id = $id : i32").unwrap();
        assert_eq!(result.params[0].name, "id");
        assert_eq!(result.params[0].rust_type, "i32");
    }

    // --- double colon cast ---

    #[test]
    fn double_colon_cast_not_confused_with_param() {
        let result = parse_query("SELECT status::text FROM t WHERE id = $id: i32").unwrap();
        assert_eq!(result.params.len(), 1);
        assert_eq!(result.params[0].name, "id");
        assert!(result.positional_sql.contains("status::text"));
    }

    // --- string literal passthrough ---

    #[test]
    fn string_literal_dollar_not_parsed_as_param() {
        let result = parse_query("SELECT * FROM t WHERE name = '$not_a_param: i32'").unwrap();
        assert_eq!(result.params.len(), 0);
    }

    // --- query kind ---

    #[test]
    fn detect_select() {
        let r = parse_query("SELECT 1").unwrap();
        assert_eq!(r.kind, QueryKind::Select);
    }

    #[test]
    fn detect_insert() {
        let r = parse_query("INSERT INTO t (a) VALUES ($a: i32)").unwrap();
        assert_eq!(r.kind, QueryKind::Insert);
    }

    #[test]
    fn detect_update() {
        let r = parse_query("UPDATE t SET a = $a: i32 WHERE id = $id: i32").unwrap();
        assert_eq!(r.kind, QueryKind::Update);
    }

    #[test]
    fn detect_delete() {
        let r = parse_query("DELETE FROM t WHERE id = $id: i32").unwrap();
        assert_eq!(r.kind, QueryKind::Delete);
    }

    #[test]
    fn detect_cte_select() {
        let r = parse_query("WITH cte AS (SELECT 1) SELECT * FROM cte").unwrap();
        assert_eq!(r.kind, QueryKind::Select);
    }

    #[test]
    fn detect_cte_insert() {
        let r = parse_query("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte").unwrap();
        assert_eq!(r.kind, QueryKind::Insert);
    }

    #[test]
    fn detect_cte_insert_with_select_tail() {
        // The inner SELECT is at depth > 0 (inside parens).
        // The INSERT is the first depth-0 DML keyword after WITH.
        // The trailing SELECT (after INSERT INTO t) is also depth-0 but
        // INSERT is encountered first — correctly classified as INSERT.
        let r = parse_query(
            "WITH data AS (SELECT id, name FROM source WHERE active = true) \
             INSERT INTO target (id, name) SELECT id, name FROM data",
        )
        .unwrap();
        assert_eq!(r.kind, QueryKind::Insert);
    }

    #[test]
    fn detect_cte_recursive_select() {
        // RECURSIVE CTE: the inner SELECT is at depth > 0.
        // The outer SELECT at depth=0 is the main statement.
        let r = parse_query(
            "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 10) \
             SELECT * FROM t",
        )
        .unwrap();
        assert_eq!(r.kind, QueryKind::Select);
    }

    // --- normalization applied ---

    #[test]
    fn normalized_sql_is_lowercase_collapsed() {
        let r = parse_query("  SELECT   id\n  FROM   users  WHERE  id = $id: i32  ").unwrap();
        assert_eq!(r.normalized_sql, "select id from users where id = $1");
    }

    // --- statement name ---

    #[test]
    fn statement_name_is_deterministic() {
        let r1 = parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        let r2 = parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        assert_eq!(r1.statement_name, r2.statement_name);
    }

    #[test]
    fn formatting_doesnt_change_statement_name() {
        let r1 = parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        let r2 = parse_query("  SELECT  id\n  FROM  users\n  WHERE  id = $id: i32  ").unwrap();
        assert_eq!(r1.statement_name, r2.statement_name);
    }

    #[test]
    fn different_queries_different_statement_names() {
        let r1 = parse_query("SELECT id FROM users WHERE id = $id: i32").unwrap();
        let r2 = parse_query("SELECT id FROM users WHERE login = $login: &str").unwrap();
        assert_ne!(r1.statement_name, r2.statement_name);
    }

    // --- error cases ---

    #[test]
    fn empty_sql_errors() {
        assert!(parse_query("").is_err());
        assert!(parse_query("   ").is_err());
    }

    #[test]
    fn missing_type_after_colon_errors() {
        assert!(parse_query("SELECT id FROM t WHERE id = $id:").is_err());
    }

    #[test]
    fn missing_colon_errors() {
        // $id without : Type — this looks like a positional param, not bsql syntax
        assert!(parse_query("SELECT id FROM t WHERE id = $id").is_err());
    }

    #[test]
    fn unsupported_statement_type_errors() {
        assert!(parse_query("CREATE TABLE t (id int)").is_err());
        assert!(parse_query("DROP TABLE t").is_err());
        assert!(parse_query("ALTER TABLE t ADD COLUMN x int").is_err());
    }

    // --- FIX 1: UTF-8 preservation ---

    #[test]
    fn utf8_cyrillic_in_string_literal() {
        let r = parse_query("SELECT * FROM t WHERE name = 'Москва' AND id = $id: i32").unwrap();
        assert!(
            r.positional_sql.contains("'Москва'"),
            "Cyrillic mangled: {}",
            r.positional_sql
        );
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn utf8_umlaut_in_string_literal() {
        let r = parse_query("SELECT * FROM t WHERE name = 'Müller' AND id = $id: i32").unwrap();
        assert!(
            r.positional_sql.contains("'Müller'"),
            "Umlaut mangled: {}",
            r.positional_sql
        );
    }

    #[test]
    fn utf8_in_dollar_quote() {
        let r = parse_query("SELECT $$Привет$$").unwrap();
        assert!(
            r.positional_sql.contains("$$Привет$$"),
            "Dollar-quote UTF-8 mangled: {}",
            r.positional_sql
        );
    }

    #[test]
    fn normalized_sql_preserves_utf8() {
        let r = parse_query("SELECT * FROM t WHERE name = 'Москва' AND id = $id: i32").unwrap();
        assert!(
            r.normalized_sql.contains("'Москва'"),
            "Normalized Cyrillic mangled: {}",
            r.normalized_sql
        );
    }

    // --- FIX 3: reject manual positional parameters ---

    #[test]
    fn reject_manual_positional_param() {
        let result = parse_query("SELECT id FROM t WHERE id = $1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("manual positional parameters"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_mixed_named_and_positional() {
        let result = parse_query("SELECT id FROM t WHERE a = $x: i32 AND b = $1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("manual positional parameters"),
            "unexpected error: {err}"
        );
    }

    // --- FIX 7: duplicate parameter names ---

    #[test]
    fn duplicate_param_same_type_reuses_position() {
        let r = parse_query("SELECT id FROM t WHERE a = $x: i32 AND b = $x: i32").unwrap();
        assert_eq!(r.params.len(), 1);
        assert_eq!(r.params[0].name, "x");
        assert_eq!(r.params[0].position, 1);
        assert_eq!(r.positional_sql, "SELECT id FROM t WHERE a = $1 AND b = $1");
    }

    #[test]
    fn duplicate_param_conflicting_types_errors() {
        let result = parse_query("SELECT id FROM t WHERE a = $x: i32 AND b = $x: &str");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("conflicting types"), "unexpected error: {err}");
    }

    // --- comment stripping: param inside comment is ignored ---

    #[test]
    fn line_comment_with_param_ignored() {
        let r = parse_query("SELECT id FROM t WHERE id = $id: i32 -- $extra: i32").unwrap();
        assert_eq!(
            r.params.len(),
            1,
            "param inside line comment should be ignored"
        );
        assert_eq!(r.params[0].name, "id");
    }

    #[test]
    fn block_comment_with_param_ignored() {
        let r = parse_query("SELECT id FROM t WHERE id = $id: i32 /* $extra: i32 */").unwrap();
        assert_eq!(
            r.params.len(),
            1,
            "param inside block comment should be ignored"
        );
        assert_eq!(r.params[0].name, "id");
    }

    // --- nested block comments ---

    #[test]
    fn nested_block_comment_stripped() {
        let r = parse_query("SELECT /* outer /* inner */ still comment */ id FROM t").unwrap();
        assert_eq!(r.kind, QueryKind::Select);
        // The nested block comment should be fully stripped
        assert!(
            r.positional_sql.contains("id"),
            "id should remain: {}",
            r.positional_sql
        );
        assert!(
            !r.positional_sql.contains("outer"),
            "comment text should be stripped: {}",
            r.positional_sql
        );
        assert!(
            !r.positional_sql.contains("inner"),
            "nested comment text should be stripped: {}",
            r.positional_sql
        );
        assert!(
            !r.positional_sql.contains("still comment"),
            "text between inner close and outer close should be stripped: {}",
            r.positional_sql
        );
    }

    // --- bad-path coverage: additional edge cases ---

    #[test]
    fn comment_only_sql_errors() {
        // After stripping comments, the remaining text is empty/whitespace
        let r = parse_query("-- just a comment");
        assert!(r.is_err());
    }

    #[test]
    fn block_comment_only_sql_errors() {
        let r = parse_query("/* nothing here */");
        assert!(r.is_err());
    }

    #[test]
    fn truncate_rejected() {
        let r = parse_query("TRUNCATE users");
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("unsupported statement type"));
    }

    #[test]
    fn grant_rejected() {
        let r = parse_query("GRANT SELECT ON users TO public");
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("unsupported statement type"));
    }

    #[test]
    fn revoke_rejected() {
        let r = parse_query("REVOKE SELECT ON users FROM public");
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("unsupported statement type"));
    }

    #[test]
    fn cte_without_dml_errors() {
        let r = parse_query("WITH cte AS (SELECT 1)");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(err.contains("CTE"), "should mention CTE: {err}");
    }

    #[test]
    fn cte_with_update() {
        let r =
            parse_query("WITH cte AS (SELECT 1 as val) UPDATE t SET a = 1 WHERE id = 1").unwrap();
        assert_eq!(r.kind, QueryKind::Update);
    }

    #[test]
    fn cte_with_delete() {
        let r = parse_query("WITH cte AS (SELECT 1) DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(r.kind, QueryKind::Delete);
    }

    #[test]
    fn param_with_underscore_name() {
        let r = parse_query("SELECT id FROM t WHERE id = $my_id: i32").unwrap();
        assert_eq!(r.params[0].name, "my_id");
    }

    #[test]
    fn param_with_digits_in_name() {
        let r = parse_query("SELECT id FROM t WHERE id = $id2: i32").unwrap();
        assert_eq!(r.params[0].name, "id2");
    }

    #[test]
    fn param_with_long_name() {
        let r =
            parse_query("SELECT id FROM t WHERE id = $this_is_a_really_long_parameter_name: i32")
                .unwrap();
        assert_eq!(r.params[0].name, "this_is_a_really_long_parameter_name");
    }

    #[test]
    fn many_params() {
        let sql = "INSERT INTO t (a,b,c,d,e,f,g,h,i,j) VALUES ($a: i32,$b: i32,$c: i32,$d: i32,$e: i32,$f: i32,$g: i32,$h: i32,$i: i32,$j: i32)";
        let r = parse_query(sql).unwrap();
        assert_eq!(r.params.len(), 10);
        assert_eq!(r.params[9].position, 10);
        assert!(r.positional_sql.contains("$10"));
    }

    #[test]
    fn path_type_param() {
        let r = parse_query("SELECT id FROM t WHERE id = $id: time::OffsetDateTime").unwrap();
        assert_eq!(r.params[0].rust_type, "time::OffsetDateTime");
    }

    #[test]
    fn dollar_sign_in_string_literal_not_a_param() {
        let r = parse_query("SELECT * FROM t WHERE price = '$100'").unwrap();
        assert_eq!(r.params.len(), 0);
    }

    #[test]
    fn escaped_single_quote_in_literal() {
        let r = parse_query("SELECT * FROM t WHERE name = 'O''Brien' AND id = $id: i32").unwrap();
        assert_eq!(r.params.len(), 1);
        assert!(r.positional_sql.contains("'O''Brien'"));
    }

    #[test]
    fn dollar_quoted_body_with_param_syntax_ignored() {
        let r = parse_query("SELECT $$has $dollar: signs$$ FROM t").unwrap();
        assert_eq!(
            r.params.len(),
            0,
            "content inside $$ should not be parsed as params"
        );
    }

    #[test]
    fn tagged_dollar_quote_with_param_syntax_ignored() {
        let r = parse_query("SELECT $tag$has $param: i32 inside$tag$ FROM t").unwrap();
        assert_eq!(
            r.params.len(),
            0,
            "content inside $tag$ should not be parsed as params"
        );
    }

    #[test]
    fn no_params_select() {
        let r = parse_query("SELECT 1 + 1 AS val").unwrap();
        assert!(r.params.is_empty());
        assert_eq!(r.kind, QueryKind::Select);
    }

    #[test]
    fn case_insensitive_keywords() {
        let r = parse_query("sElEcT id FrOm t WhErE id = $id: i32").unwrap();
        assert_eq!(r.kind, QueryKind::Select);
        // normalized should be lowercase
        assert!(r.normalized_sql.starts_with("select"));
    }

    #[test]
    fn multiple_positional_params_rejected() {
        assert!(parse_query("SELECT id FROM t WHERE a = $1 AND b = $2").is_err());
    }

    #[test]
    fn triple_duplicate_param_reuses_position() {
        let r = parse_query("SELECT id FROM t WHERE a = $x: i32 AND b = $x: i32 AND c = $x: i32")
            .unwrap();
        assert_eq!(r.params.len(), 1);
        assert_eq!(
            r.positional_sql,
            "SELECT id FROM t WHERE a = $1 AND b = $1 AND c = $1"
        );
    }

    #[test]
    fn param_at_end_of_sql() {
        let r = parse_query("DELETE FROM t WHERE id = $id: i32").unwrap();
        assert_eq!(r.params.len(), 1);
        assert!(r.positional_sql.ends_with("$1"));
    }

    #[test]
    fn double_colon_cast_after_param() {
        // $val: &str followed by ::text should work
        let r = parse_query("SELECT * FROM t WHERE a::text = $val: &str").unwrap();
        assert_eq!(r.params.len(), 1);
        assert!(r.positional_sql.contains("a::text"));
    }

    // --- optional clause parsing ---

    #[test]
    fn optional_clause_extracted() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 [AND a = $a: Option<i32>] ORDER BY id")
            .unwrap();
        assert_eq!(r.optional_clauses.len(), 1);
        assert_eq!(r.optional_clauses[0].params.len(), 1);
        assert_eq!(r.optional_clauses[0].params[0].name, "a");
        assert_eq!(r.optional_clauses[0].params[0].rust_type, "Option<i32>");
        assert_eq!(r.optional_clauses[0].index, 0);
        // Base query should have no params
        assert_eq!(r.params.len(), 0);
        // Positional SQL should have placeholder, not raw bracket
        assert!(
            r.positional_sql.contains("{OPT_0}"),
            "should contain placeholder: {}",
            r.positional_sql
        );
        assert!(
            !r.positional_sql.contains('['),
            "should not contain [: {}",
            r.positional_sql
        );
    }

    #[test]
    fn multiple_optional_clauses() {
        let r = parse_query(
            "SELECT id FROM t WHERE 1 = 1 \
             [AND a = $a: Option<i32>] \
             [AND b = $b: Option<&str>] ORDER BY id",
        )
        .unwrap();
        assert_eq!(r.optional_clauses.len(), 2);
        assert_eq!(r.optional_clauses[0].params[0].name, "a");
        assert_eq!(r.optional_clauses[1].params[0].name, "b");
        assert_eq!(r.optional_clauses[1].params[0].rust_type, "Option<&str>");
    }

    #[test]
    fn optional_clause_with_base_params() {
        let r = parse_query(
            "SELECT id FROM t WHERE status = $s: &str \
             [AND a = $a: Option<i32>]",
        )
        .unwrap();
        assert_eq!(r.params.len(), 1);
        assert_eq!(r.params[0].name, "s");
        assert_eq!(r.optional_clauses.len(), 1);
        assert_eq!(r.optional_clauses[0].params[0].name, "a");
    }

    #[test]
    fn optional_clause_non_option_param_rejected() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 [AND a = $a: i32]");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(err.contains("Option<T>"), "should mention Option<T>: {err}");
    }

    #[test]
    fn nested_brackets_rejected() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 [[AND a = $a: Option<i32>]]");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(err.contains("nested"), "should mention nested: {err}");
    }

    #[test]
    fn unclosed_bracket_rejected() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 [AND a = $a: Option<i32>");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(
            err.contains("unclosed") || err.contains("]"),
            "should mention missing ]: {err}"
        );
    }

    #[test]
    fn unmatched_close_bracket_rejected() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 AND a = $a: i32]");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(err.contains("]"), "should mention ]: {err}");
    }

    #[test]
    fn too_many_optional_clauses_rejected() {
        // 9 optional clauses should be rejected (max 8)
        let clauses: Vec<String> = (0..9)
            .map(|i| format!("[AND c{i} = $c{i}: Option<i32>]"))
            .collect();
        let sql = format!("SELECT id FROM t WHERE 1 = 1 {}", clauses.join(" "));
        let r = parse_query(&sql);
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(
            err.contains("9 optional clauses") && err.contains("maximum is 8"),
            "should mention limit: {err}"
        );
    }

    #[test]
    fn eight_optional_clauses_accepted() {
        let clauses: Vec<String> = (0..8)
            .map(|i| format!("[AND c{i} = $c{i}: Option<i32>]"))
            .collect();
        let sql = format!("SELECT id FROM t WHERE 1 = 1 {}", clauses.join(" "));
        let r = parse_query(&sql).unwrap();
        assert_eq!(r.optional_clauses.len(), 8);
    }

    #[test]
    fn optional_clause_string_literal_preserved() {
        let r = parse_query(
            "SELECT id FROM t WHERE 1 = 1 [AND name ILIKE '%' || $s: Option<&str> || '%']",
        )
        .unwrap();
        assert_eq!(r.optional_clauses.len(), 1);
        assert!(
            r.optional_clauses[0].sql_fragment.contains("'%'"),
            "string literal lost: {}",
            r.optional_clauses[0].sql_fragment
        );
    }

    #[test]
    fn optional_clause_cast_preserved() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 [AND status::text = $s: Option<&str>]")
            .unwrap();
        assert!(
            r.optional_clauses[0].sql_fragment.contains("::text"),
            "cast lost: {}",
            r.optional_clauses[0].sql_fragment
        );
    }

    #[test]
    fn no_optional_clauses_empty_vec() {
        let r = parse_query("SELECT id FROM t WHERE id = $id: i32").unwrap();
        assert!(r.optional_clauses.is_empty());
    }

    #[test]
    fn param_in_both_base_and_clause_rejected() {
        let r = parse_query("SELECT id FROM t WHERE a = $x: i32 [AND b = $x: Option<i32>]");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(
            err.contains("both in the base query and in an optional clause"),
            "should mention scope conflict: {err}"
        );
    }

    #[test]
    fn bracket_without_option_param_rejected() {
        // [1] is array subscript, not an optional clause
        let r = parse_query("SELECT col[1] FROM t");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(
            err.contains("must contain exactly one"),
            "should explain brackets need Option params: {err}"
        );
    }

    #[test]
    fn bracket_with_no_params_rejected() {
        let r = parse_query("SELECT id FROM t WHERE 1 = 1 [AND status = 'active']");
        assert!(r.is_err());
        let err = r.unwrap_err();
        assert!(
            err.contains("must contain exactly one"),
            "should explain brackets need params: {err}"
        );
    }

    #[test]
    fn multi_param_optional_clause_rejected() {
        let result = parse_query(
            "SELECT id FROM t WHERE 1 = 1 [AND a BETWEEN $lo: Option<i32> AND $hi: Option<i32>]",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("exactly one parameter"),
            "should reject multi-param clause: {err}"
        );
    }

    #[test]
    fn same_param_across_clauses_rejected() {
        let result = parse_query(
            "SELECT id FROM t WHERE 1 = 1 [AND a = $x: Option<i32>] [AND b = $x: Option<i32>]",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("multiple optional clauses"),
            "should reject same param in different clauses: {err}"
        );
    }
}
