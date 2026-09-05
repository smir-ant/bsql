//! Injection-safe SQL identifiers spliced directly into statement text.
//!
//! A few driver verbs must interpolate a caller-supplied identifier straight
//! into SQL: `COPY <table> FROM STDIN`, `COPY <table> TO STDOUT`, and
//! `LISTEN`/`UNLISTEN <channel>` have no parameterized form for the identifier
//! itself. An un-validated splice is a SQL-injection vector — a `table` of
//! `users; DROP TABLE x --` would run as a second statement.
//!
//! The safety here is **structural, not by discipline**: the only value the
//! splice sites accept is a [`SafeIdent`] / [`SafeTable`], and the ONLY way to
//! obtain one is its `validate` constructor (the field is private — no other
//! path exists). "Forgot to validate before splicing" is therefore a *compile
//! error*, not a latent injection: a splice helper's signature demands the
//! newtype, and a raw `&str` will not coerce to it.
//!
//! The validators accept ONLY a plain unquoted PostgreSQL identifier
//! (optionally schema-qualified for a table): a leading ASCII letter or `_`,
//! then ASCII letters / digits / `_` / `$`, at most 63 bytes per component.
//! Everything else — spaces, `;`, quotes, comment markers, dots beyond one
//! separator, empty components — is rejected as a classified
//! [`DriverError::Config`], so an injection-shaped string can never reach the
//! interpolated SQL. An identifier that must be double-quoted (special
//! characters) is deliberately rejected rather than quote-escaped: rejection is
//! injection-proof by construction, and no driver path needs quoted names.

use crate::error::DriverError;

/// Maximum bytes in one identifier component (PG `NAMEDATALEN - 1`).
const MAX_COMPONENT_LEN: usize = 63;

/// Whether `s` is a single unquoted PG identifier: non-empty, ≤ 63 bytes, a
/// leading letter or `_`, then letters / digits / `_` / `$`.
fn is_unquoted_identifier(s: &str) -> bool {
    if s.is_empty() || s.len() > MAX_COMPONENT_LEN {
        return false;
    }
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// A single SQL identifier PROVEN safe to splice verbatim into statement text.
///
/// The wrapped `&str` is a plain unquoted PostgreSQL identifier — the ONLY way
/// to construct a `SafeIdent` is [`SafeIdent::validate`], which rejects anything
/// injection-shaped. Because the field is private, a `SafeIdent` cannot be
/// fabricated by any other means; a splice site that takes a `SafeIdent` (not a
/// `&str`) therefore *cannot* be handed an unvalidated name. This is the
/// compile-time half of the "cannot inject via a `LISTEN` / `UNLISTEN` channel"
/// guarantee — the type is the proof.
///
/// It borrows its source `&'a str` (a validated view, zero allocation); build
/// it at the splice boundary and consume it immediately.
#[derive(Debug, Clone, Copy)]
pub struct SafeIdent<'a>(&'a str);

impl<'a> SafeIdent<'a> {
    /// Validate `name` as a single unquoted PG identifier and wrap it. The SOLE
    /// constructor: the private field can be populated no other way, so every
    /// `SafeIdent` in existence has passed this check.
    ///
    /// # Errors
    ///
    /// [`DriverError::Config`] if `name` is not a plain unquoted identifier.
    pub fn validate(name: &'a str) -> Result<Self, DriverError> {
        if is_unquoted_identifier(name) {
            Ok(Self(name))
        } else {
            Err(DriverError::Config(
                "invalid SQL identifier: expected an unquoted name (a letter or '_' \
                 followed by letters, digits, '_', or '$', at most 63 bytes)",
            ))
        }
    }

    /// The validated identifier text, safe to splice into SQL.
    #[must_use]
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

/// Const validator for unquoted ASCII identifiers.
pub const fn is_unquoted_identifier_ascii_const(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > MAX_COMPONENT_LEN {
        return false;
    }
    let Some((&first, mut rest)) = bytes.split_first() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    while let Some((&b, next_rest)) = rest.split_first() {
        if !(b.is_ascii_alphanumeric() || b == b'_' || b == b'$') {
            return false;
        }
        rest = next_rest;
    }
    true
}

/// Const validator for unquoted table identifiers (`table` or `schema.table`).
pub const fn is_unquoted_table_ascii_const(bytes: &[u8]) -> bool {
    const MAX_TABLE_LEN: usize = MAX_COMPONENT_LEN * 2 + 1;
    if bytes.is_empty() || bytes.len() > MAX_TABLE_LEN {
        return false;
    }
    let mut dot_idx = None;
    let mut i: usize = 0;
    let mut cur = bytes;
    while let Some((&b, next)) = cur.split_first() {
        if b == b'.' {
            if dot_idx.is_some() {
                return false;
            }
            dot_idx = Some(i);
        }
        let Some(next_i) = i.checked_add(1) else {
            return false;
        };
        i = next_i;
        cur = next;
    }
    match dot_idx {
        None => is_unquoted_identifier_ascii_const(bytes),
        Some(idx) => {
            let Some((schema, rest)) = bytes.split_at_checked(idx) else {
                return false;
            };
            let table = match rest.split_first() {
                Some((_, t)) => t,
                None => return false,
            };
            is_unquoted_identifier_ascii_const(schema) && is_unquoted_identifier_ascii_const(table)
        }
    }
}

impl SafeIdent<'static> {
    /// Compile-time constructor for static unquoted ASCII identifiers.
    ///
    /// # Panics
    ///
    /// Panics at compile time if `name` is empty, > 63 bytes, or contains
    /// invalid/injection characters.
    #[must_use]
    pub const fn from_static(name: &'static str) -> Self {
        assert!(
            is_unquoted_identifier_ascii_const(name.as_bytes()),
            "invalid SQL identifier: expected an unquoted name (letter or '_' followed by letters, digits, '_', '$', <= 63 bytes)"
        );
        Self(name)
    }
}

/// An optionally schema-qualified table identifier PROVEN safe to splice into a
/// `COPY` statement — `table` or `schema.table`, each component a plain unquoted
/// identifier.
///
/// The peer of [`SafeIdent`] for the two-component `COPY` table rule: the ONLY
/// constructor is [`SafeTable::validate`], the field is private, so a splice
/// site taking a `SafeTable` cannot be handed an unvalidated table name.
#[derive(Debug, Clone, Copy)]
pub struct SafeTable<'a>(&'a str);

impl<'a> SafeTable<'a> {
    /// Validate `table` as an unquoted `table` or `schema.table` and wrap it.
    /// The SOLE constructor.
    ///
    /// # Errors
    ///
    /// [`DriverError::Config`] if `table` is not one or two unquoted identifier
    /// components separated by a single `.`.
    pub fn validate(table: &'a str) -> Result<Self, DriverError> {
        let mut parts = table.split('.');
        let ok = match (parts.next(), parts.next(), parts.next()) {
            (Some(one), None, _) => is_unquoted_identifier(one),
            (Some(schema), Some(name), None) => {
                is_unquoted_identifier(schema) && is_unquoted_identifier(name)
            }
            // Empty input, or more than two dot-separated components.
            _ => false,
        };
        if ok {
            Ok(Self(table))
        } else {
            Err(DriverError::Config(
                "invalid COPY table identifier: expected an unquoted `table` or \
                 `schema.table` (each component a letter or '_' followed by letters, \
                 digits, '_', or '$', at most 63 bytes)",
            ))
        }
    }

    /// The validated table text, safe to splice into SQL.
    #[must_use]
    pub fn as_str(&self) -> &'a str {
        self.0
    }
}

impl SafeTable<'static> {
    /// Compile-time constructor for static unquoted table identifiers.
    ///
    /// # Panics
    ///
    /// Panics at compile time if `table` is not a valid `table` or `schema.table`.
    #[must_use]
    pub const fn from_static(table: &'static str) -> Self {
        assert!(
            is_unquoted_table_ascii_const(table.as_bytes()),
            "invalid SQL table identifier: expected unquoted 'table' or 'schema.table' (<= 63 bytes per component)"
        );
        Self(table)
    }
}

/// Construct a compile-time validated [`SafeIdent`] with zero runtime overhead.
#[macro_export]
macro_rules! safe_ident {
    ($name:literal) => {{
        const __BSQL_SAFE_IDENT: $crate::SafeIdent<'static> = $crate::SafeIdent::from_static($name);
        __BSQL_SAFE_IDENT
    }};
    ($name:ident) => {{
        const __BSQL_SAFE_IDENT: $crate::SafeIdent<'static> = $crate::SafeIdent::from_static(stringify!($name));
        __BSQL_SAFE_IDENT
    }};
}

/// Construct a compile-time validated [`SafeTable`] with zero runtime overhead.
#[macro_export]
macro_rules! safe_table {
    ($name:literal) => {{
        const __BSQL_SAFE_TABLE: $crate::SafeTable<'static> = $crate::SafeTable::from_static($name);
        __BSQL_SAFE_TABLE
    }};
    ($name:ident) => {{
        const __BSQL_SAFE_TABLE: $crate::SafeTable<'static> = $crate::SafeTable::from_static(stringify!($name));
        __BSQL_SAFE_TABLE
    }};
}

// ── Splice seam ──────────────────────────────────────────────────────────────
//
// Every SQL string that interpolates a caller-supplied identifier is assembled
// HERE, and each helper takes a `SafeIdent` / `SafeTable` — never a `&str`. This
// is the seam that grows with every future identifier-splicing verb: a new verb
// that forgets to validate cannot call these builders (the argument will not
// coerce), so the injection guard is enforced by the type system, once, for all
// present and future splice sites.

/// `LISTEN <channel>` — the channel is a validated [`SafeIdent`], so the
/// assembled SQL cannot inject.
pub(crate) fn listen_sql(channel: SafeIdent<'_>) -> String {
    format!("LISTEN {}", channel.as_str())
}

/// `UNLISTEN <channel>` — the channel is a validated [`SafeIdent`].
pub(crate) fn unlisten_sql(channel: SafeIdent<'_>) -> String {
    format!("UNLISTEN {}", channel.as_str())
}

/// `COPY <table> TO STDOUT` — the table is a validated [`SafeTable`].
pub(crate) fn copy_out_sql(table: SafeTable<'_>) -> String {
    format!("COPY {} TO STDOUT", table.as_str())
}

/// `COPY <table> FROM STDIN` — the table is a validated [`SafeTable`].
pub(crate) fn copy_in_sql(table: SafeTable<'_>) -> String {
    format!("COPY {} FROM STDIN", table.as_str())
}

#[cfg(test)]
mod tests {
    use super::{copy_in_sql, copy_out_sql, listen_sql, unlisten_sql, SafeIdent, SafeTable};
    use crate::error::DriverError;

    fn is_config_err<T: core::fmt::Debug>(r: Result<T, DriverError>) -> bool {
        matches!(r, Err(DriverError::Config(_)))
    }

    #[test]
    fn plain_identifiers_are_accepted() {
        for ok in ["users", "_hidden", "t1", "my_table", "col$x", "A", "Table123"] {
            assert!(
                SafeIdent::validate(ok).is_ok(),
                "{ok:?} should be a valid identifier",
            );
            assert!(SafeTable::validate(ok).is_ok(), "{ok:?} should be a valid table");
        }
    }

    #[test]
    fn schema_qualified_tables_are_accepted() {
        assert!(SafeTable::validate("public.my_table").is_ok());
        assert!(SafeTable::validate("s.t").is_ok());
        // …but a schema-qualified name is NOT a single identifier.
        assert!(is_config_err(SafeIdent::validate("public.my_table")));
    }

    #[test]
    fn injection_shaped_identifiers_are_rejected() {
        for bad in [
            "users; DROP TABLE x --",
            "\"; DROP TABLE x; --",
            "users WHERE 1=1",
            "a b",
            "a'b",
            "a\"b",
            "a;b",
            "a--b",
            "a\nb",
            "1abc",  // leading digit
            "$x",    // leading '$'
            "",      // empty
            ".",     // empty components
            "a.",    // trailing empty component
            ".a",    // leading empty component
            "a.b.c", // three components
        ] {
            assert!(is_config_err(SafeTable::validate(bad)), "{bad:?} must be rejected as a table");
            assert!(
                is_config_err(SafeIdent::validate(bad)),
                "{bad:?} must be rejected as an identifier",
            );
        }
    }

    #[test]
    fn over_length_component_is_rejected() {
        let too_long = "a".repeat(64);
        assert!(is_config_err(SafeIdent::validate(&too_long)));
        assert!(is_config_err(SafeTable::validate(&too_long)));
        // Exactly 63 bytes is the boundary and is accepted.
        let at_cap = "a".repeat(63);
        assert!(SafeIdent::validate(&at_cap).is_ok());
    }

    #[test]
    fn splice_helpers_assemble_from_the_validated_newtype() {
        let (Ok(chan), Ok(one), Ok(two)) = (
            SafeIdent::validate("events"),
            SafeTable::validate("my_table"),
            SafeTable::validate("public.my_table"),
        ) else {
            panic!("these are all valid identifiers");
        };
        assert_eq!(listen_sql(chan), "LISTEN events");
        assert_eq!(unlisten_sql(chan), "UNLISTEN events");
        assert_eq!(copy_out_sql(one), "COPY my_table TO STDOUT");
        assert_eq!(copy_in_sql(two), "COPY public.my_table FROM STDIN");
    }

    #[test]
    fn const_ident_and_table_macros_evaluate_compile_time() {
        let id1 = SafeIdent::from_static("events");
        assert_eq!(id1.as_str(), "events");

        let id2 = safe_ident!("orders");
        assert_eq!(id2.as_str(), "orders");

        let id3 = safe_ident!(users);
        assert_eq!(id3.as_str(), "users");

        let t1 = SafeTable::from_static("users");
        assert_eq!(t1.as_str(), "users");

        let t2 = safe_table!("public.orders");
        assert_eq!(t2.as_str(), "public.orders");

        let t3 = safe_table!(accounts);
        assert_eq!(t3.as_str(), "accounts");
    }
}
