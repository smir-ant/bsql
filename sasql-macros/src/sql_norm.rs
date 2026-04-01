//! SQL normalization: collapse whitespace, lowercase keywords, preserve string literals.
//!
//! Normalized SQL is used for:
//! - Consistent statement naming (different formatting → same hash)
//! - Smaller binary size (.rodata section)

/// Normalize a SQL string for hashing and storage.
///
/// - Collapses runs of whitespace (spaces, tabs, newlines) to a single space.
/// - Lowercases everything OUTSIDE of string literals (single-quoted `'...'`).
/// - Preserves content inside string literals verbatim (including multi-byte UTF-8).
/// - Preserves dollar-quoted strings (`$$...$$`, `$tag$...$tag$`).
/// - Strips leading/trailing whitespace.
/// - Strips SQL comments (`--` line comments, `/* */` block comments).
///
/// Uses byte-offset slicing of the original `&str` for string literal contents,
/// so multi-byte characters (Cyrillic, CJK, etc.) are never misinterpreted.
pub fn normalize_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        let b = bytes[i];

        // Line comment: -- to end of line
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // Block comment: /* ... */ (with nesting support)
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
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
            if !out.is_empty() && !out.ends_with(' ') {
                out.push(' ');
            }
            continue;
        }

        // Single-quoted string literal: slice original &str verbatim
        if b == b'\'' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    i += 1;
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

        // Double-quoted identifier: preserve verbatim (case-sensitive in PG)
        if b == b'"' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'"' {
                    i += 1;
                    // Escaped "" inside identifier — continue
                    if i < len && bytes[i] == b'"' {
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

        // Dollar-quoted string: slice original &str verbatim
        if b == b'$' {
            if let Some((_tag, end)) = find_dollar_quote(bytes, i) {
                out.push_str(&sql[i..end]);
                i = end;
                continue;
            }
        }

        // Whitespace: collapse to single space
        if b.is_ascii_whitespace() {
            if !out.is_empty() && !out.ends_with(' ') {
                out.push(' ');
            }
            i += 1;
            while i < len && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }

        // Outside string literals, SQL is ASCII — lowercase safely
        out.push((b as char).to_ascii_lowercase());
        i += 1;
    }

    // Trim trailing space
    if out.ends_with(' ') {
        out.pop();
    }

    out
}

/// Find a dollar-quoted string starting at position `start`.
/// Returns (tag, end_position) where end_position is one past the closing tag.
fn find_dollar_quote(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    let len = bytes.len();
    if start >= len || bytes[start] != b'$' {
        return None;
    }

    // Find the end of the opening tag: $$ or $identifier$
    let tag_start = start + 1;
    let mut tag_end = tag_start;

    // Tag can be empty ($$) or an identifier
    while tag_end < len && (bytes[tag_end].is_ascii_alphanumeric() || bytes[tag_end] == b'_') {
        tag_end += 1;
    }

    if tag_end >= len || bytes[tag_end] != b'$' {
        return None;
    }

    let tag_len = tag_end - tag_start + 2; // includes both $ delimiters
    let tag = &bytes[start..start + tag_len];
    let body_start = start + tag_len;

    // Find the closing tag
    let mut i = body_start;
    while i + tag_len <= len {
        if &bytes[i..i + tag_len] == tag {
            return Some((tag_len, i + tag_len));
        }
        i += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_whitespace() {
        assert_eq!(
            normalize_sql("SELECT   id,  name\n  FROM   users"),
            "select id, name from users"
        );
    }

    #[test]
    fn lowercase_keywords() {
        assert_eq!(
            normalize_sql("SELECT Id FROM Users WHERE Active = TRUE"),
            "select id from users where active = true"
        );
    }

    #[test]
    fn preserve_string_literal() {
        assert_eq!(
            normalize_sql("SELECT * FROM users WHERE status = 'Active'"),
            "select * from users where status = 'Active'"
        );
    }

    #[test]
    fn preserve_escaped_quote_in_literal() {
        assert_eq!(
            normalize_sql("SELECT * FROM t WHERE name = 'O''Brien'"),
            "select * from t where name = 'O''Brien'"
        );
    }

    #[test]
    fn strip_line_comment() {
        assert_eq!(
            normalize_sql("SELECT id -- primary key\nFROM users"),
            "select id from users"
        );
    }

    #[test]
    fn strip_block_comment() {
        assert_eq!(
            normalize_sql("SELECT /* columns */ id, name FROM users"),
            "select id, name from users"
        );
    }

    #[test]
    fn trim_leading_trailing() {
        assert_eq!(normalize_sql("  SELECT 1  "), "select 1");
    }

    #[test]
    fn tabs_and_newlines() {
        assert_eq!(
            normalize_sql("SELECT\n\tid\n\tFROM\n\tusers"),
            "select id from users"
        );
    }

    #[test]
    fn preserve_dollar_quoted_string() {
        assert_eq!(
            normalize_sql("SELECT $$Hello World$$"),
            "select $$Hello World$$"
        );
    }

    #[test]
    fn preserve_tagged_dollar_quote() {
        assert_eq!(
            normalize_sql("SELECT $fn$Body Text$fn$ FROM t"),
            "select $fn$Body Text$fn$ from t"
        );
    }

    #[test]
    fn empty_string() {
        assert_eq!(normalize_sql(""), "");
    }

    #[test]
    fn only_whitespace() {
        assert_eq!(normalize_sql("   \n\t  "), "");
    }

    #[test]
    fn double_colon_cast_preserved() {
        assert_eq!(
            normalize_sql("SELECT status::TEXT FROM tickets"),
            "select status::text from tickets"
        );
    }

    #[test]
    fn complex_query_normalizes_consistently() {
        let q1 = "  SELECT  id, login,  first_name\n  FROM  users\n  WHERE  id = $1  ";
        let q2 = "select id, login, first_name from users where id = $1";
        assert_eq!(normalize_sql(q1), normalize_sql(q2));
    }

    // --- UTF-8 preservation ---

    #[test]
    fn preserves_cyrillic_in_string_literal() {
        assert_eq!(
            normalize_sql("SELECT * FROM t WHERE name = 'Москва'"),
            "select * from t where name = 'Москва'"
        );
    }

    #[test]
    fn preserves_umlaut_in_string_literal() {
        assert_eq!(
            normalize_sql("SELECT * FROM t WHERE name = 'Müller'"),
            "select * from t where name = 'Müller'"
        );
    }

    #[test]
    fn preserves_cjk_in_string_literal() {
        assert_eq!(
            normalize_sql("SELECT * FROM t WHERE city = '東京'"),
            "select * from t where city = '東京'"
        );
    }

    #[test]
    fn preserves_unicode_in_double_dollar_quote() {
        assert_eq!(
            normalize_sql("SELECT $$Привет мир$$"),
            "select $$Привет мир$$"
        );
    }

    #[test]
    fn preserves_escaped_quote_with_unicode() {
        assert_eq!(
            normalize_sql("SELECT * FROM t WHERE name = 'Д''Артаньян'"),
            "select * from t where name = 'Д''Артаньян'"
        );
    }

    // --- Double-quoted identifier preservation ---

    #[test]
    fn preserves_double_quoted_identifier() {
        assert_eq!(
            normalize_sql(r#"SELECT "MyColumn" FROM "MyTable""#),
            r#"select "MyColumn" from "MyTable""#
        );
    }

    #[test]
    fn preserves_unicode_in_double_quoted_identifier() {
        assert_eq!(
            normalize_sql(r#"SELECT "Ёлка" FROM "Таблица""#),
            r#"select "Ёлка" from "Таблица""#
        );
    }

    #[test]
    fn preserves_escaped_double_quote() {
        assert_eq!(
            normalize_sql(r#"SELECT "col""name" FROM t"#),
            r#"select "col""name" from t"#
        );
    }

    // --- nested block comments ---

    #[test]
    fn nested_block_comment_stripped() {
        assert_eq!(
            normalize_sql("SELECT /* outer /* inner */ still comment */ id FROM t"),
            "select id from t"
        );
    }

    #[test]
    fn deeply_nested_block_comment() {
        assert_eq!(
            normalize_sql("SELECT /* a /* b /* c */ b */ a */ id FROM t"),
            "select id from t"
        );
    }
}
