//! SQL normalization: collapse whitespace, lowercase keywords, preserve string literals.
//!
//! Normalized SQL is used for:
//! - Consistent statement naming (different formatting → same hash)
//! - Smaller binary size (.rodata section)

/// Normalize a SQL string for hashing and storage.
///
/// - Collapses runs of whitespace (spaces, tabs, newlines) to a single space.
/// - Lowercases everything OUTSIDE of string literals (single-quoted `'...'`).
/// - Preserves content inside string literals verbatim.
/// - Preserves dollar-quoted strings (`$$...$$`, `$tag$...$tag$`).
/// - Strips leading/trailing whitespace.
/// - Strips SQL comments (`--` line comments, `/* */` block comments).
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
            // The newline itself becomes whitespace, handled below
            continue;
        }

        // Block comment: /* ... */
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2; // skip */
            }
            // Treat removed comment as whitespace
            if !out.is_empty() && !out.ends_with(' ') {
                out.push(' ');
            }
            continue;
        }

        // Single-quoted string literal: preserve verbatim
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < len {
                if bytes[i] == b'\'' {
                    out.push('\'');
                    i += 1;
                    // Escaped quote '' — continue the literal
                    if i < len && bytes[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                        continue;
                    }
                    break;
                }
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }

        // Dollar-quoted string: $tag$...$tag$
        if b == b'$' {
            if let Some((tag, end)) = find_dollar_quote(bytes, i) {
                // Copy the entire dollar-quoted string verbatim
                for &byte in &bytes[i..end] {
                    out.push(byte as char);
                }
                i = end;
                let _ = tag; // tag used only for matching in find_dollar_quote
                continue;
            }
            // Not a dollar-quote start — fall through to normal processing
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

        // Everything else: lowercase
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
}
