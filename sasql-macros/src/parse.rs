//! SQL parser for `sasql::query!`.
//!
//! This parser does NOT understand SQL semantics. It extracts:
//! - Parameter bindings (`$name: Type`)
//! - Query kind (SELECT / INSERT / UPDATE / DELETE)
//! - Whether RETURNING is present
//!
//! Everything else is passed through to PostgreSQL verbatim. PG does the real
//! SQL parsing via PREPARE.

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
    pub normalized_sql: String,
    /// SQL with params replaced by `$1`, `$2`, etc. but NOT normalized
    /// (preserves original formatting for error messages).
    pub positional_sql: String,
    /// Extracted parameters in order of appearance.
    pub params: Vec<Param>,
    /// What kind of DML this is.
    pub kind: QueryKind,
    /// Whether the query has a RETURNING clause.
    #[allow(dead_code)] // tested in parse tests; will be consumed by codegen
    pub has_returning: bool,
    /// Prepared statement name: `s_{rapidhash:016x}`.
    pub statement_name: String,
}

/// Parse the raw SQL from a `query!` invocation.
///
/// The input is the literal SQL text between the braces of `query! { ... }`.
pub fn parse_query(sql: &str) -> Result<ParsedQuery, String> {
    if sql.trim().is_empty() {
        return Err("empty SQL query".into());
    }

    let comment_stripped = strip_comments(sql);
    let (positional_sql, params) = extract_params(&comment_stripped)?;
    let normalized_sql = normalize_sql(&positional_sql);
    let kind = detect_query_kind(&normalized_sql)?;
    let has_returning = detect_returning(&normalized_sql);
    let stmt_name = statement_name(&normalized_sql);

    Ok(ParsedQuery {
        normalized_sql,
        positional_sql,
        params,
        kind,
        has_returning,
        statement_name: stmt_name,
    })
}

/// Extract `$name: Type` parameters from SQL, replacing them with `$1`, `$2`, ...
///
/// Returns the rewritten SQL and the list of extracted parameters.
///
/// Uses `char_indices()` for iteration so multi-byte UTF-8 inside string
/// literals is preserved verbatim (we slice the original `&str` by byte
/// offset, never interpreting individual bytes as chars).
fn extract_params(sql: &str) -> Result<(String, Vec<Param>), String> {
    let mut out = String::with_capacity(sql.len());
    let mut params: Vec<Param> = Vec::new();
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0; // byte offset into `sql`

    while i < len {
        let b = bytes[i];

        // String literal: copy verbatim (preserves multi-byte UTF-8)
        if b == b'\'' {
            let start = i;
            i += 1;
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
            out.push_str(&sql[start..i]);
            continue;
        }

        // Dollar-quoted string: copy verbatim
        if b == b'$'
            && i + 1 < len
            && (bytes[i + 1] == b'$'
                || bytes[i + 1].is_ascii_alphabetic()
                || bytes[i + 1] == b'_')
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
                 in sasql — use $name: Type syntax instead"
                    .into(),
            );
        }

        // Outside of string literals, SQL is ASCII. Copy one byte.
        out.push(b as char);
        i += 1;
    }

    Ok((out, params))
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
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' { i += 1; continue; }
                    break;
                }
                i += 1;
            }
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
            while i < len && bytes[i] != b'\n' { i += 1; }
            out.push(' ');
            continue;
        }

        // Block comment: skip (with nesting support)
        if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            let mut depth = 1u32;
            while i + 1 < len && depth > 0 {
                if bytes[i] == b'/' && bytes[i + 1] == b'*' { depth += 1; i += 2; continue; }
                if bytes[i] == b'*' && bytes[i + 1] == b'/' { depth -= 1; i += 2; continue; }
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
    let first_word = normalized
        .split_whitespace()
        .next()
        .unwrap_or("");

    // Handle CTEs: WITH ... SELECT/INSERT/UPDATE/DELETE
    if first_word == "with" {
        // Find the main statement after the CTE
        // Simplified: look for select/insert/update/delete not inside parens
        let mut depth: i32 = 0;
        for word in normalized.split_whitespace() {
            let opens = word.matches('(').count() as i32;
            let closes = word.matches(')').count() as i32;
            depth += opens - closes;
            if depth < 0 { depth = 0; } // malformed SQL — PG will catch it

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
            "unsupported statement type: `{other}`. sasql supports SELECT, INSERT, UPDATE, DELETE"
        )),
    }
}

/// Check if the normalized SQL contains a RETURNING clause (outside string literals).
fn detect_returning(normalized: &str) -> bool {
    // After normalization, RETURNING is lowercase. We look for the word boundary.
    normalized
        .split_whitespace()
        .any(|w| w == "returning")
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
        let result = parse_query(
            "INSERT INTO users (name, email) VALUES ($name: &str, $email: &str)",
        )
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
        let result = parse_query(
            "SELECT id FROM t WHERE ids = ANY($ids: &[i32])",
        )
        .unwrap();
        assert_eq!(result.params[0].rust_type, "&[i32]");
    }

    #[test]
    fn vec_type_param() {
        let result = parse_query(
            "SELECT id FROM t WHERE id = ANY($ids: Vec<i32>)",
        )
        .unwrap();
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

    // --- RETURNING ---

    #[test]
    fn detect_returning_clause() {
        let r = parse_query("INSERT INTO t (a) VALUES ($a: i32) RETURNING id").unwrap();
        assert!(r.has_returning);
    }

    #[test]
    fn no_returning() {
        let r = parse_query("INSERT INTO t (a) VALUES ($a: i32)").unwrap();
        assert!(!r.has_returning);
    }

    #[test]
    fn returning_in_delete() {
        let r = parse_query("DELETE FROM t WHERE id = $id: i32 RETURNING id, name").unwrap();
        assert!(r.has_returning);
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
        // $id without : Type — this looks like a positional param, not sasql syntax
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
        assert!(r.positional_sql.contains("'Москва'"), "Cyrillic mangled: {}", r.positional_sql);
        assert_eq!(r.params.len(), 1);
    }

    #[test]
    fn utf8_umlaut_in_string_literal() {
        let r = parse_query("SELECT * FROM t WHERE name = 'Müller' AND id = $id: i32").unwrap();
        assert!(r.positional_sql.contains("'Müller'"), "Umlaut mangled: {}", r.positional_sql);
    }

    #[test]
    fn utf8_in_dollar_quote() {
        let r = parse_query("SELECT $$Привет$$").unwrap();
        assert!(r.positional_sql.contains("$$Привет$$"), "Dollar-quote UTF-8 mangled: {}", r.positional_sql);
    }

    #[test]
    fn normalized_sql_preserves_utf8() {
        let r = parse_query("SELECT * FROM t WHERE name = 'Москва' AND id = $id: i32").unwrap();
        assert!(r.normalized_sql.contains("'Москва'"), "Normalized Cyrillic mangled: {}", r.normalized_sql);
    }

    // --- FIX 3: reject manual positional parameters ---

    #[test]
    fn reject_manual_positional_param() {
        let result = parse_query("SELECT id FROM t WHERE id = $1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("manual positional parameters"), "unexpected error: {err}");
    }

    #[test]
    fn reject_mixed_named_and_positional() {
        let result = parse_query("SELECT id FROM t WHERE a = $x: i32 AND b = $1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("manual positional parameters"), "unexpected error: {err}");
    }

    // --- FIX 7: duplicate parameter names ---

    #[test]
    fn duplicate_param_same_type_reuses_position() {
        let r = parse_query(
            "SELECT id FROM t WHERE a = $x: i32 AND b = $x: i32",
        )
        .unwrap();
        assert_eq!(r.params.len(), 1);
        assert_eq!(r.params[0].name, "x");
        assert_eq!(r.params[0].position, 1);
        assert_eq!(r.positional_sql, "SELECT id FROM t WHERE a = $1 AND b = $1");
    }

    #[test]
    fn duplicate_param_conflicting_types_errors() {
        let result = parse_query(
            "SELECT id FROM t WHERE a = $x: i32 AND b = $x: &str",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("conflicting types"), "unexpected error: {err}");
    }

    // --- comment stripping: param inside comment is ignored ---

    #[test]
    fn line_comment_with_param_ignored() {
        let r = parse_query(
            "SELECT id FROM t WHERE id = $id: i32 -- $extra: i32",
        )
        .unwrap();
        assert_eq!(r.params.len(), 1, "param inside line comment should be ignored");
        assert_eq!(r.params[0].name, "id");
    }

    #[test]
    fn block_comment_with_param_ignored() {
        let r = parse_query(
            "SELECT id FROM t WHERE id = $id: i32 /* $extra: i32 */",
        )
        .unwrap();
        assert_eq!(r.params.len(), 1, "param inside block comment should be ignored");
        assert_eq!(r.params[0].name, "id");
    }

    // --- nested block comments ---

    #[test]
    fn nested_block_comment_stripped() {
        let r = parse_query(
            "SELECT /* outer /* inner */ still comment */ id FROM t",
        )
        .unwrap();
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
}
