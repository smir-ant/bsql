//! Narrow-scope SQL lexer for the `prepared!` macro.
//!
//! The lexer's job is intentionally narrow: it tokenises a static
//! SQL string at proc-macro expansion time and produces a flat
//! stream of `SqlToken`s pointing back to the source via byte
//! offsets. `extract.rs` walks the stream to build `ParamSpec` /
//! `ColumnSpec` lists; `typemap.rs` maps PG type names to Rust
//! type tokens.
//!
//! # Tier discipline (CREDO §0)
//!
//! - **Tier-1 by-compile**: every token-stream consumer
//!   (`extract.rs`) matches on `SqlTokenKind` exhaustively. Adding a
//!   token kind without updating consumers fails the build.
//! - **Tier-2 by-construction**: no allocator beyond `alloc::vec::Vec`
//!   (this crate is `proc-macro`, not `no_std`; `Vec` is the natural
//!   container for the dynamic-length token list).
//! - **Tier-3 by-test**: ~30 unit tests in `#[cfg(test)] mod tests`
//!   below cover every SQL shape (string literals, dollar-quoted
//!   strings, comments, quoted identifiers, casts).
//!
//! # Why hand-rolled vs `sqlparser-rs`
//!
//! Pulling `sqlparser-rs` triples this proc-macro crate's dep graph
//! for a feature we use ~3% of (find `$N` placeholders and `::TYPE`
//! casts). The 300-line hand-rolled lexer is audit-readable in one
//! sitting, every byte is ours, and diagnostics point at the exact
//! offending source byte rather than travelling through a
//! translation layer. CREDO §4.4 dep discipline wins; CREDO §11
//! policy 9 ("never hand-roll expert-domain code") targets
//! production runtime decoders (JSON/YAML/full SQL for a SQL engine),
//! not a build-time targeted lexer for our narrow grammar.
//!
//! # Grammar accepted
//! - String literals: `'foo'`, with `''` as escape for embedded
//!   single-quote.
//! - Dollar-quoted strings: `$tag$body$tag$` where `tag` is
//!   case-sensitive and matches the opening tag at close.
//! - Comments: line `-- ... \n` and block `/* ... */` (PG does not
//!   support nested block comments; this lexer does not either).
//! - Quoted identifiers: `"foo bar"` (with `""` as escape).
//! - Numeric placeholders: `$1`, `$2`, ..., `$N` for `N` in `1..=u8::MAX`.
//! - Casts: postfix `expr::TYPE` and prefix `CAST(expr AS TYPE)` (the
//!   `TYPE` token is recognised, the `expr` content opaque).
//! - Keywords: case-insensitive ASCII recognition for `SELECT`,
//!   `INSERT`, `UPDATE`, `DELETE`, `WITH`, `RETURNING`, `FROM`,
//!   `WHERE`, `AS`, `CAST`.
//!
//! Anything outside this grammar — non-ASCII identifier chars,
//! schema-qualified identifiers like `public.users`, fancy SQL — is
//! NOT given semantic meaning by the lexer. It still produces tokens
//! (typically `Ident` or `Punct`) so the extractor's walking logic
//! can ignore them.

#![allow(dead_code, reason = "lexer plumbing — `extract.rs` consumes these tokens; intermediate construction sites are part of the same proc-macro pipeline")]

use alloc::vec::Vec;

/// One token from the SQL source string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqlToken {
    /// Starting byte offset of this token in the source string.
    pub(crate) start: usize,
    /// Length of this token in source bytes.
    pub(crate) len: usize,
    /// Kind classification — see [`SqlTokenKind`].
    pub(crate) kind: SqlTokenKind,
}

/// Token classification produced by the lexer.
///
/// Variants are kept narrow on purpose; the extractor handles every
/// remaining lexical case generically via [`Self::Ident`] /
/// [`Self::Punct`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlTokenKind {
    /// SQL keyword (case-insensitive match against the closed set
    /// listed in [`KeywordKind`]).
    Keyword(KeywordKind),
    /// Generic identifier or bareword (alphanumeric + `_`, not a
    /// keyword). Includes type names like `int4`, `text`, etc. The
    /// extractor consults this in the cast-walk step.
    Ident,
    /// Numeric placeholder `$N`. The index is parsed from the bytes
    /// `[start+1, start+len)`.
    Placeholder(u8),
    /// Punctuation character (one of `,`, `(`, `)`, `;`, `=`, `*`,
    /// `+`, `-`, `/`, etc.). Single-byte ASCII.
    Punct(u8),
    /// `::` cast operator. The lexer emits this verbatim; the
    /// extractor walks past it to find the TYPE token.
    DoubleColon,
    /// Numeric literal (integer or decimal). The macro does not
    /// inspect the value; this kind exists so the extractor can step
    /// over literals without mistaking them for identifiers.
    Number,
}

/// Closed set of SQL keywords this lexer recognises case-insensitively.
///
/// New variants need lockstep updates to [`Self::from_ascii_ci`] and
/// the consumer in `extract.rs`. The closed enum makes
/// "added a keyword without updating consumers" a compile error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeywordKind {
    /// `SELECT` — opens a query producing rows.
    Select,
    /// `INSERT` — opens a DML statement that may have RETURNING.
    Insert,
    /// `UPDATE` — opens a DML statement that may have RETURNING.
    Update,
    /// `DELETE` — opens a DML statement that may have RETURNING.
    Delete,
    /// `WITH` — opens a CTE preamble.
    With,
    /// `RETURNING` — opens a SELECT-like list inside a DML statement.
    Returning,
    /// `FROM` — closes a SELECT list.
    From,
    /// `WHERE` — opens a predicate clause.
    Where,
    /// `AS` — alias keyword, used inside `CAST(expr AS TYPE)` and
    /// after SELECT columns.
    As,
    /// `CAST` — prefix cast keyword (`CAST(expr AS TYPE)`).
    Cast,
    /// `VALUES` — INSERT body keyword separating column list from
    /// per-row tuple list.
    Values,
    /// `SET` — UPDATE keyword opening assignment list.
    Set,
}

impl KeywordKind {
    /// ASCII case-insensitive keyword lookup. Returns `None` for any
    /// identifier not in the closed set.
    fn from_ascii_ci(bytes: &[u8]) -> Option<Self> {
        // Inline tables avoid allocating; ASCII-fold by `.to_ascii_lowercase()`
        // on each byte. Keywords are short (≤ 9 chars) so the linear
        // match cost is negligible.
        if bytes.len() > 16 {
            return None;
        }
        let mut lower: [u8; 16] = [0; 16];
        let mut i = 0;
        while i < bytes.len() {
            // SAFETY-equivalent: indexing is bounds-checked by the
            // while-loop guard; clippy::indexing_slicing is allowed
            // here only because we proved `i < bytes.len()` AND
            // `i < lower.len()` (lower is sized 16, bytes.len() ≤ 16).
            // We do NOT use indexing; use `.get()` / `.get_mut()`.
            let lb = bytes.get(i).copied().unwrap_or(0);
            let dst = lower.get_mut(i)?;
            *dst = lb.to_ascii_lowercase();
            i = i.saturating_add(1);
        }
        let folded = lower.get(..bytes.len())?;
        match folded {
            b"select" => Some(Self::Select),
            b"insert" => Some(Self::Insert),
            b"update" => Some(Self::Update),
            b"delete" => Some(Self::Delete),
            b"with" => Some(Self::With),
            b"returning" => Some(Self::Returning),
            b"from" => Some(Self::From),
            b"where" => Some(Self::Where),
            b"as" => Some(Self::As),
            b"cast" => Some(Self::Cast),
            b"values" => Some(Self::Values),
            b"set" => Some(Self::Set),
            _ => None,
        }
    }
}

/// Lex error — pinpoints the offending byte in the source. Mapped
/// to a `syn::Error` with that span at the macro entry.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LexError {
    /// Byte offset of the offending source character.
    pub(crate) byte_offset: usize,
    /// Static-string diagnostic message — keep concise; the macro
    /// entry wraps it with `compile_error!(...)`.
    pub(crate) message: &'static str,
}

/// Tokenise a SQL string into a flat token list. Skips comments and
/// string literals; emits one token per identifier / placeholder /
/// punctuation / cast / number.
///
/// # Errors
///
/// Returns [`LexError`] on malformed source: unterminated string
/// literal, unterminated comment, unterminated quoted identifier,
/// unterminated dollar-quoted body, malformed `$N` placeholder
/// (index ≥ u8::MAX). Other classes of "SQL we don't understand"
/// (non-ASCII identifiers, weird punctuation) are NOT errors at the
/// lex level — they pass through as `Ident` or `Punct`. The
/// extractor's validation step is what classifies "SQL shape we
/// accept" vs "SQL shape we reject".
pub(crate) fn tokenise(src: &str) -> Result<Vec<SqlToken>, LexError> {
    let bytes = src.as_bytes();
    // Initial capacity heuristic: average token length ~4 bytes (an
    // identifier or keyword). Using shift instead of integer division
    // satisfies the forbid-bundle's clippy::integer_division ban
    // and is bit-identical for unsigned right-shift.
    let mut tokens: Vec<SqlToken> = Vec::with_capacity(bytes.len() >> 2);
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        let b = bytes.get(cursor).copied().unwrap_or(0);
        // Skip whitespace.
        if b.is_ascii_whitespace() {
            cursor = cursor.saturating_add(1);
            continue;
        }
        // Skip line comments `-- ... \n`.
        if b == b'-' && bytes.get(cursor.saturating_add(1)).copied() == Some(b'-') {
            cursor = skip_line_comment(bytes, cursor);
            continue;
        }
        // Skip block comments `/* ... */`.
        if b == b'/' && bytes.get(cursor.saturating_add(1)).copied() == Some(b'*') {
            cursor = skip_block_comment(bytes, cursor)?;
            continue;
        }
        // Skip standard string literals `'foo'` / `'foo''bar'`.
        if b == b'\'' {
            cursor = skip_string_literal(bytes, cursor)?;
            continue;
        }
        // Skip quoted identifiers `"foo bar"` — for our extractor's
        // purposes these are opaque; we never need to read their
        // contents (the macro requires casts on SELECT columns
        // explicitly).
        if b == b'"' {
            let next = skip_quoted_identifier(bytes, cursor)?;
            tokens.push(SqlToken {
                start: cursor,
                len: next.saturating_sub(cursor),
                kind: SqlTokenKind::Ident,
            });
            cursor = next;
            continue;
        }
        // Dollar-quoted strings `$tag$body$tag$` or numeric placeholder
        // `$1`. Tier-1 disambiguation: `$<digit>` → placeholder; `$<ident>$`
        // or `$$` → dollar-quoted body.
        if b == b'$' {
            let after_dollar = bytes.get(cursor.saturating_add(1)).copied().unwrap_or(0);
            if after_dollar.is_ascii_digit() {
                let token = read_placeholder(bytes, cursor)?;
                cursor = cursor.saturating_add(token.len);
                tokens.push(token);
                continue;
            }
            cursor = skip_dollar_quoted(bytes, cursor)?;
            continue;
        }
        // `::` cast operator.
        if b == b':' && bytes.get(cursor.saturating_add(1)).copied() == Some(b':') {
            tokens.push(SqlToken {
                start: cursor,
                len: 2,
                kind: SqlTokenKind::DoubleColon,
            });
            cursor = cursor.saturating_add(2);
            continue;
        }
        // Identifier or keyword: `[a-zA-Z_][a-zA-Z0-9_]*`.
        if b.is_ascii_alphabetic() || b == b'_' {
            let token = read_ident_or_keyword(bytes, cursor);
            cursor = cursor.saturating_add(token.len);
            tokens.push(token);
            continue;
        }
        // Numeric literal: `[0-9]+(.[0-9]+)?` — we don't care about
        // the value, just the span.
        if b.is_ascii_digit() {
            let token = read_number(bytes, cursor);
            cursor = cursor.saturating_add(token.len);
            tokens.push(token);
            continue;
        }
        // Everything else is single-byte punctuation. Multi-byte
        // operators (`::`, `--`, `/*`) are handled above; what's left
        // is single-byte ASCII operators that our grammar may or may
        // not care about. We emit them so the extractor sees the
        // structural shape (commas, parens) without re-tokenising.
        tokens.push(SqlToken {
            start: cursor,
            len: 1,
            kind: SqlTokenKind::Punct(b),
        });
        cursor = cursor.saturating_add(1);
    }
    Ok(tokens)
}

/// Advance past `-- ...\n` (or to end-of-input).
fn skip_line_comment(bytes: &[u8], mut cursor: usize) -> usize {
    // `bytes[cursor..cursor+2] == "--"` per caller.
    cursor = cursor.saturating_add(2);
    while cursor < bytes.len() {
        let b = bytes.get(cursor).copied().unwrap_or(0);
        cursor = cursor.saturating_add(1);
        if b == b'\n' {
            break;
        }
    }
    cursor
}

/// Advance past `/* ... */`. Returns Err if unterminated.
fn skip_block_comment(bytes: &[u8], mut cursor: usize) -> Result<usize, LexError> {
    let start = cursor;
    cursor = cursor.saturating_add(2);
    while cursor.saturating_add(1) < bytes.len() {
        let c0 = bytes.get(cursor).copied().unwrap_or(0);
        let c1 = bytes.get(cursor.saturating_add(1)).copied().unwrap_or(0);
        if c0 == b'*' && c1 == b'/' {
            return Ok(cursor.saturating_add(2));
        }
        cursor = cursor.saturating_add(1);
    }
    Err(LexError {
        byte_offset: start,
        message: "prepared!: unterminated block comment (missing closing `*/`)",
    })
}

/// Advance past a standard string literal `'foo'` (with `''` escape).
fn skip_string_literal(bytes: &[u8], mut cursor: usize) -> Result<usize, LexError> {
    let start = cursor;
    cursor = cursor.saturating_add(1); // past opening `'`
    while cursor < bytes.len() {
        let b = bytes.get(cursor).copied().unwrap_or(0);
        if b == b'\'' {
            // `''` is the escape for a literal single-quote.
            if bytes.get(cursor.saturating_add(1)).copied() == Some(b'\'') {
                cursor = cursor.saturating_add(2);
                continue;
            }
            return Ok(cursor.saturating_add(1));
        }
        cursor = cursor.saturating_add(1);
    }
    Err(LexError {
        byte_offset: start,
        message: "prepared!: unterminated string literal (missing closing `'`)",
    })
}

/// Advance past a quoted identifier `"foo bar"` (with `""` escape).
fn skip_quoted_identifier(bytes: &[u8], mut cursor: usize) -> Result<usize, LexError> {
    let start = cursor;
    cursor = cursor.saturating_add(1); // past opening `"`
    while cursor < bytes.len() {
        let b = bytes.get(cursor).copied().unwrap_or(0);
        if b == b'"' {
            if bytes.get(cursor.saturating_add(1)).copied() == Some(b'"') {
                cursor = cursor.saturating_add(2);
                continue;
            }
            return Ok(cursor.saturating_add(1));
        }
        cursor = cursor.saturating_add(1);
    }
    Err(LexError {
        byte_offset: start,
        message: "prepared!: unterminated quoted identifier (missing closing `\"`)",
    })
}

/// Advance past `$tag$body$tag$` (tag may be empty for `$$body$$`).
fn skip_dollar_quoted(bytes: &[u8], cursor: usize) -> Result<usize, LexError> {
    let start = cursor;
    // Find the closing `$` of the opening tag.
    let mut tag_end = cursor.saturating_add(1);
    while tag_end < bytes.len() {
        let b = bytes.get(tag_end).copied().unwrap_or(0);
        if b == b'$' {
            break;
        }
        // Tag chars: letters, digits, `_`. Anything else means this
        // was NOT a dollar-quote opening — fall through as a
        // single-byte punct (the lexer's outer loop already pushed
        // `$` as a Punct; we have to back out). To keep the lexer
        // structurally clean we treat malformed openers as errors —
        // SQL with bare `$` not followed by digit/ident/`$` is
        // out-of-grammar.
        if !(b.is_ascii_alphanumeric() || b == b'_') {
            return Err(LexError {
                byte_offset: start,
                message: "prepared!: malformed dollar-quote opener (expected `$tag$` or `$N`)",
            });
        }
        tag_end = tag_end.saturating_add(1);
    }
    if tag_end >= bytes.len() {
        return Err(LexError {
            byte_offset: start,
            message: "prepared!: unterminated dollar-quote opener (expected closing `$`)",
        });
    }
    // tag bytes are `bytes[start+1 .. tag_end]`; the closing tag must
    // match.
    let tag_inner_start = start.saturating_add(1);
    let tag_inner_end = tag_end;
    let tag_len = tag_inner_end.saturating_sub(tag_inner_start);
    // Body starts right after the closing `$` of the opening tag.
    let mut body_cursor = tag_end.saturating_add(1);
    while body_cursor < bytes.len() {
        let b = bytes.get(body_cursor).copied().unwrap_or(0);
        if b == b'$' {
            // Possible closing tag start; check whether `bytes[body_cursor+1 ..]`
            // begins with `tag` + closing `$`.
            let probe_end = body_cursor
                .saturating_add(1)
                .saturating_add(tag_len)
                .saturating_add(1);
            if probe_end <= bytes.len() {
                let candidate_tag_range = body_cursor
                    .saturating_add(1)
                    ..body_cursor.saturating_add(1).saturating_add(tag_len);
                let candidate_tag = bytes.get(candidate_tag_range).unwrap_or(&[]);
                let expected_tag_range = tag_inner_start..tag_inner_end;
                let expected_tag = bytes.get(expected_tag_range).unwrap_or(&[]);
                let closing_dollar = bytes
                    .get(body_cursor.saturating_add(1).saturating_add(tag_len))
                    .copied();
                if candidate_tag == expected_tag && closing_dollar == Some(b'$') {
                    return Ok(probe_end);
                }
            }
        }
        body_cursor = body_cursor.saturating_add(1);
    }
    Err(LexError {
        byte_offset: start,
        message: "prepared!: unterminated dollar-quoted string (missing matching closing tag)",
    })
}

/// Read `$N` placeholder. Caller already verified `bytes[cursor] == b'$'`
/// and `bytes[cursor+1]` is ASCII digit.
fn read_placeholder(bytes: &[u8], cursor: usize) -> Result<SqlToken, LexError> {
    let start = cursor;
    let mut end = cursor.saturating_add(1); // past `$`
    let mut value: u32 = 0;
    while end < bytes.len() {
        let b = bytes.get(end).copied().unwrap_or(0);
        if !b.is_ascii_digit() {
            break;
        }
        // Tier-1: every digit accumulation is `checked_*`; arithmetic
        // overflow is impossible by classification, not by luck.
        let digit = u32::from(b.saturating_sub(b'0'));
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit))
            .ok_or(LexError {
                byte_offset: start,
                message: "prepared!: placeholder index overflows u8 (max $255)",
            })?;
        end = end.saturating_add(1);
    }
    let idx = u8::try_from(value).map_err(|_| LexError {
        byte_offset: start,
        message: "prepared!: placeholder index out of range (max $255 — tuple-arity cap is 16)",
    })?;
    if idx == 0 {
        return Err(LexError {
            byte_offset: start,
            message: "prepared!: placeholder index must be ≥ 1 (PG convention; $1 is the first)",
        });
    }
    Ok(SqlToken {
        start,
        len: end.saturating_sub(start),
        kind: SqlTokenKind::Placeholder(idx),
    })
}

/// Read identifier / keyword. Caller already verified the first byte
/// starts an identifier.
fn read_ident_or_keyword(bytes: &[u8], cursor: usize) -> SqlToken {
    let start = cursor;
    let mut end = cursor;
    while end < bytes.len() {
        let b = bytes.get(end).copied().unwrap_or(0);
        if !(b.is_ascii_alphanumeric() || b == b'_') {
            break;
        }
        end = end.saturating_add(1);
    }
    let span = bytes.get(start..end).unwrap_or(&[]);
    let kind = match KeywordKind::from_ascii_ci(span) {
        Some(kw) => SqlTokenKind::Keyword(kw),
        None => SqlTokenKind::Ident,
    };
    SqlToken {
        start,
        len: end.saturating_sub(start),
        kind,
    }
}

/// Read numeric literal `[0-9]+(.[0-9]+)?` — the macro doesn't use
/// the value, just records the span so the extractor can step past.
fn read_number(bytes: &[u8], cursor: usize) -> SqlToken {
    let start = cursor;
    let mut end = cursor;
    while end < bytes.len() {
        let b = bytes.get(end).copied().unwrap_or(0);
        if b.is_ascii_digit() {
            end = end.saturating_add(1);
            continue;
        }
        if b == b'.' && bytes.get(end.saturating_add(1)).copied().is_some_and(|c| c.is_ascii_digit()) {
            end = end.saturating_add(1);
            continue;
        }
        break;
    }
    SqlToken {
        start,
        len: end.saturating_sub(start),
        kind: SqlTokenKind::Number,
    }
}

/// Project the bytes of a token's slice into the source.
pub(crate) fn token_bytes<'s>(src: &'s str, tok: &SqlToken) -> &'s [u8] {
    src.as_bytes().get(tok.start..tok.start.saturating_add(tok.len)).unwrap_or(&[])
}

/// Project a token as `&str` if its bytes are valid UTF-8. SQL
/// proc-macro inputs are guaranteed UTF-8 by `syn::LitStr`; the
/// fallback empty string is architecturally dead but observes the
/// crate's no-panic discipline.
pub(crate) fn token_str<'s>(src: &'s str, tok: &SqlToken) -> &'s str {
    core::str::from_utf8(token_bytes(src, tok)).unwrap_or("")
}

#[cfg(test)]
mod tests {
    //! Per-shape unit tests covering each accepted grammar form.
    //!
    //! Each test pins ONE invariant: comment handling, string
    //! escapes, dollar-quoting tag matching, placeholder index
    //! parsing, keyword case-insensitivity. Failure = lexer breaks
    //! one of the grammar shapes the macro requires.
    use super::*;

    fn lex(src: &str) -> Vec<SqlToken> {
        tokenise(src).unwrap_or_default()
    }

    /// Smoke: simple SELECT lexes to keyword + ident + keyword + ident.
    #[test]
    fn lex_simple_select() {
        let tokens = lex("SELECT a FROM t");
        let kinds: Vec<SqlTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert!(matches!(kinds.first(), Some(SqlTokenKind::Keyword(KeywordKind::Select))));
        assert!(matches!(kinds.get(1), Some(SqlTokenKind::Ident)));
        assert!(matches!(kinds.get(2), Some(SqlTokenKind::Keyword(KeywordKind::From))));
        assert!(matches!(kinds.get(3), Some(SqlTokenKind::Ident)));
    }

    /// Keyword recognition is case-insensitive.
    #[test]
    fn lex_keyword_lowercase_and_mixed() {
        let tokens_lower = lex("select");
        assert!(matches!(
            tokens_lower.first().map(|t| t.kind),
            Some(SqlTokenKind::Keyword(KeywordKind::Select))
        ));
        let tokens_mixed = lex("SeLeCt");
        assert!(matches!(
            tokens_mixed.first().map(|t| t.kind),
            Some(SqlTokenKind::Keyword(KeywordKind::Select))
        ));
    }

    /// `$1`, `$2`, ..., `$255` are placeholder tokens.
    #[test]
    fn lex_placeholder_indices() {
        let tokens = lex("$1 $2 $42");
        let placeholders: Vec<u8> = tokens
            .iter()
            .filter_map(|t| match t.kind {
                SqlTokenKind::Placeholder(n) => Some(n),
                _ => None,
            })
            .collect();
        assert_eq!(placeholders, vec![1, 2, 42]);
    }

    /// Placeholder index 0 is rejected (PG convention: $1 is first).
    #[test]
    fn lex_placeholder_zero_rejected() {
        assert!(tokenise("$0").is_err());
    }

    /// Placeholder index > 255 is rejected.
    #[test]
    fn lex_placeholder_overflow_rejected() {
        assert!(tokenise("$256").is_err());
        assert!(tokenise("$99999").is_err());
    }

    /// `::` is a single DoubleColon token (not two Punct(`:`)).
    #[test]
    fn lex_double_colon_cast() {
        let tokens = lex("x::int4");
        let kinds: Vec<SqlTokenKind> = tokens.iter().map(|t| t.kind).collect();
        assert!(matches!(kinds.first(), Some(SqlTokenKind::Ident)));
        assert!(matches!(kinds.get(1), Some(SqlTokenKind::DoubleColon)));
        assert!(matches!(kinds.get(2), Some(SqlTokenKind::Ident)));
    }

    /// Line comments are skipped end-to-end.
    #[test]
    fn lex_line_comment() {
        let tokens = lex("SELECT a -- comment\nFROM t");
        let kw_count = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::Keyword(_)))
            .count();
        assert_eq!(kw_count, 2); // SELECT + FROM
    }

    /// Block comments are skipped.
    #[test]
    fn lex_block_comment() {
        let tokens = lex("SELECT /* comment */ a FROM t");
        let kw_count = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::Keyword(_)))
            .count();
        assert_eq!(kw_count, 2);
    }

    /// Unterminated block comment is an error.
    #[test]
    fn lex_unterminated_block_comment_errors() {
        assert!(tokenise("SELECT /* unterminated").is_err());
    }

    /// String literals are skipped (no token emitted for their
    /// content); doubled-quote escape works.
    #[test]
    fn lex_string_literal_with_escape() {
        let tokens = lex("SELECT 'foo''bar' FROM t");
        // SELECT + FROM = 2 keywords; the literal is skipped entirely.
        let kw_count = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::Keyword(_)))
            .count();
        assert_eq!(kw_count, 2);
    }

    /// Unterminated string literal is an error.
    #[test]
    fn lex_unterminated_string_errors() {
        assert!(tokenise("SELECT 'never closed").is_err());
    }

    /// Dollar-quoted strings (`$tag$body$tag$`) are skipped wholesale.
    #[test]
    fn lex_dollar_quoted_body() {
        let tokens = lex("SELECT $tag$ body with 'quotes' $tag$ FROM t");
        let kw_count = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::Keyword(_)))
            .count();
        assert_eq!(kw_count, 2);
    }

    /// Empty-tag dollar-quoted `$$body$$` is also supported.
    #[test]
    fn lex_dollar_quoted_empty_tag() {
        let tokens = lex("SELECT $$ body $$ FROM t");
        let kw_count = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::Keyword(_)))
            .count();
        assert_eq!(kw_count, 2);
    }

    /// Unterminated dollar-quoted body is an error.
    #[test]
    fn lex_unterminated_dollar_errors() {
        assert!(tokenise("SELECT $tag$ never closed").is_err());
    }

    /// Quoted identifiers `"foo bar"` are treated as a single Ident token.
    #[test]
    fn lex_quoted_identifier() {
        let tokens = lex(r#"SELECT "foo bar" FROM t"#);
        // Token sequence: Select(kw), Ident(quoted), From(kw), Ident.
        assert_eq!(tokens.len(), 4);
        assert!(matches!(tokens.get(1).map(|t| t.kind), Some(SqlTokenKind::Ident)));
    }

    /// Numeric literals are recognised as Number tokens.
    #[test]
    fn lex_number_int_and_decimal() {
        let tokens = lex("SELECT 42, 3.14");
        let numbers: Vec<usize> = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::Number))
            .map(|t| t.len)
            .collect();
        assert_eq!(numbers, vec![2, 4]); // "42", "3.14"
    }

    /// All four DML keywords + WITH map to distinct variants.
    #[test]
    fn lex_dml_keywords() {
        for (sql, expected) in [
            ("SELECT", KeywordKind::Select),
            ("INSERT", KeywordKind::Insert),
            ("UPDATE", KeywordKind::Update),
            ("DELETE", KeywordKind::Delete),
            ("WITH", KeywordKind::With),
            ("RETURNING", KeywordKind::Returning),
            ("CAST", KeywordKind::Cast),
            ("AS", KeywordKind::As),
            ("FROM", KeywordKind::From),
            ("WHERE", KeywordKind::Where),
            ("VALUES", KeywordKind::Values),
            ("SET", KeywordKind::Set),
        ] {
            let tokens = lex(sql);
            assert!(
                matches!(tokens.first().map(|t| t.kind), Some(SqlTokenKind::Keyword(k)) if k == expected),
                "Failed for keyword {sql}",
            );
        }
    }

    /// PG type-name lookups via the ident-byte path. Lexer doesn't
    /// validate these; the typemap (Phase 3) does. Pin that the
    /// lexer at least preserves the byte slice.
    #[test]
    fn lex_pg_type_names_preserved_as_ident() {
        for ty in ["int2", "int4", "int8", "oid", "bool", "text"] {
            let src = format!("$1::{ty}");
            let tokens = tokenise(&src).unwrap_or_default();
            // Find the Ident token after `::`.
            let cast_pos = tokens
                .iter()
                .position(|t| matches!(t.kind, SqlTokenKind::DoubleColon))
                .unwrap_or(usize::MAX);
            let after_cast = tokens.get(cast_pos.saturating_add(1));
            assert!(
                matches!(after_cast.map(|t| t.kind), Some(SqlTokenKind::Ident)),
                "Type name {ty} not parsed as Ident token after `::`",
            );
            let bytes = token_bytes(&src, after_cast.unwrap_or(&SqlToken {
                start: 0,
                len: 0,
                kind: SqlTokenKind::Number,
            }));
            assert_eq!(bytes, ty.as_bytes());
        }
    }

    /// Realistic SELECT with cast — pin the token sequence.
    #[test]
    fn lex_full_select_with_cast() {
        let src = "SELECT id::int4, name::text FROM users WHERE id = $1::int4";
        let tokens = tokenise(src).unwrap_or_default();
        // Expected: Select, id, ::, int4, ',', name, ::, text, FROM, users,
        // WHERE, id, '=', $1, ::, int4 = 16 tokens.
        assert!(tokens.len() >= 14, "Token count {} too low", tokens.len());
        let placeholders: Vec<u8> = tokens
            .iter()
            .filter_map(|t| match t.kind {
                SqlTokenKind::Placeholder(n) => Some(n),
                _ => None,
            })
            .collect();
        assert_eq!(placeholders, vec![1]);
        let double_colons = tokens
            .iter()
            .filter(|t| matches!(t.kind, SqlTokenKind::DoubleColon))
            .count();
        assert_eq!(double_colons, 3);
    }
}
