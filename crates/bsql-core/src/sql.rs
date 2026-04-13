//! The [`Sql`] type — SQL text with precomputed hash and routing metadata.
//!
//! This is the single type that bridges compile-time macro output and
//! runtime execution. Generated code creates `Sql::precomputed()` with
//! compile-time constants (zero runtime cost). Users pass `&str` which
//! auto-converts via `From` (hash computed once, ~5ns).

/// SQL text bundled with a precomputed statement-cache hash and a
/// read/write routing hint.
///
/// # For generated code (zero-cost)
///
/// ```ignore
/// Sql::precomputed("SELECT id FROM users WHERE id = $1", 0xabc123, true)
/// ```
///
/// All three values are compile-time literals embedded in the binary.
/// No runtime computation.
///
/// # For user code
///
/// ```ignore
/// pool.execute("CREATE TABLE temp (id int)", &[]).await?;
/// // &str auto-converts to Sql via From — hash computed once (~5ns)
/// ```
///
/// # Fields
///
/// - `text` — the SQL string
/// - `hash` — rapidhash of the SQL text, used for prepared statement
///   cache lookup (both PostgreSQL and SQLite)
/// - `readonly` — routing hint: `true` routes to read replica (PG) or
///   reader connection (SQLite); `false` routes to primary/writer
#[derive(Debug, Clone, Copy)]
pub struct Sql<'a> {
    text: &'a str,
    hash: u64,
    readonly: bool,
    /// Pre-built Parse+Describe wire protocol bytes (compile-time generated).
    /// When present, the driver uses these directly on cache miss instead
    /// of building the Parse message at runtime (~200ns saved per first-execution).
    parse_msg: Option<&'a [u8]>,
}

impl<'a> Sql<'a> {
    /// Create with precomputed hash and routing flag.
    ///
    /// Used by generated code — all arguments are compile-time constants.
    /// Zero runtime cost: two integers and a pointer, all in the binary.
    /// No pre-built Parse message — the driver builds it at runtime on cache miss.
    #[inline]
    pub const fn precomputed(text: &'a str, hash: u64, readonly: bool) -> Self {
        Sql {
            text,
            hash,
            readonly,
            parse_msg: None,
        }
    }

    /// Create with precomputed hash, routing flag, AND pre-built Parse+Describe
    /// wire protocol bytes. The driver sends these directly on cache miss — zero
    /// runtime message construction.
    #[inline]
    pub const fn with_parse(text: &'a str, hash: u64, readonly: bool, parse_msg: &'a [u8]) -> Self {
        Sql {
            text,
            hash,
            readonly,
            parse_msg: Some(parse_msg),
        }
    }

    /// Create from a SQL string. Hash computed at construction (~5ns).
    /// Readonly defaults to `false` (safe default — always hits primary/writer).
    #[inline]
    pub fn new(text: &'a str) -> Self {
        Sql {
            text,
            hash: crate::rapid_hash_str(text),
            readonly: false,
            parse_msg: None,
        }
    }

    /// The SQL text.
    #[inline]
    pub fn text(&self) -> &'a str {
        self.text
    }

    /// Precomputed hash for statement cache lookup.
    #[inline]
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Pre-built Parse+Describe wire bytes, if available.
    #[inline]
    pub fn parse_msg(&self) -> Option<&[u8]> {
        self.parse_msg
    }

    /// Whether this query is read-only (determines connection routing).
    #[inline]
    pub fn readonly(&self) -> bool {
        self.readonly
    }
}

/// Auto-convert `&str` to `Sql` for ergonomic user calls.
///
/// ```ignore
/// pool.execute("CREATE TABLE temp (id int)", &[]).await?;
/// // equivalent to:
/// pool.execute(Sql::new("CREATE TABLE temp (id int)"), &[]).await?;
/// ```
impl<'a> From<&'a str> for Sql<'a> {
    #[inline]
    fn from(text: &'a str) -> Self {
        Sql::new(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precomputed_is_const() {
        // Must compile as const — this IS the zero-cost guarantee.
        const SQL: Sql<'static> = Sql::precomputed("SELECT 1", 42, true);
        assert_eq!(SQL.text(), "SELECT 1");
        assert_eq!(SQL.hash(), 42);
        assert!(SQL.readonly());
    }

    #[test]
    fn new_computes_hash() {
        let sql = Sql::new("SELECT 1");
        assert_eq!(sql.text(), "SELECT 1");
        assert_ne!(sql.hash(), 0); // hash should be non-zero for non-empty string
        assert!(!sql.readonly()); // default is false
    }

    #[test]
    fn from_str() {
        let sql: Sql<'_> = "SELECT 1".into();
        assert_eq!(sql.text(), "SELECT 1");
        assert!(!sql.readonly());
    }

    #[test]
    fn hash_deterministic() {
        let a = Sql::new("SELECT id FROM users");
        let b = Sql::new("SELECT id FROM users");
        assert_eq!(a.hash(), b.hash());
    }

    #[test]
    fn different_sql_different_hash() {
        let a = Sql::new("SELECT 1");
        let b = Sql::new("SELECT 2");
        assert_ne!(a.hash(), b.hash());
    }

    #[test]
    fn precomputed_readonly_true() {
        let sql = Sql::precomputed("SELECT 1", 0, true);
        assert!(sql.readonly());
    }

    #[test]
    fn precomputed_readonly_false() {
        let sql = Sql::precomputed("INSERT INTO t VALUES (1)", 0, false);
        assert!(!sql.readonly());
    }

    #[test]
    fn copy_semantics() {
        let sql = Sql::new("SELECT 1");
        let copy = sql; // Copy
        assert_eq!(sql.text(), copy.text()); // both still valid
        assert_eq!(sql.hash(), copy.hash());
    }
}
