//! Prepared statement name generation via rapidhash.

use std::hash::{Hash, Hasher};

/// Generate a prepared statement name from normalized SQL.
///
/// Format: `s_{hash:016x}` — a 64-bit rapidhash of the SQL text, hex-encoded.
/// Deterministic: same SQL always produces the same name.
pub fn statement_name(normalized_sql: &str) -> String {
    let mut hasher = rapidhash::quality::RapidHasher::default();
    normalized_sql.hash(&mut hasher);
    let hash = hasher.finish();
    format!("s_{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let a = statement_name("select id from users where id = $1");
        let b = statement_name("select id from users where id = $1");
        assert_eq!(a, b);
    }

    #[test]
    fn different_sql_different_name() {
        let a = statement_name("select id from users where id = $1");
        let b = statement_name("select id from users where login = $1");
        assert_ne!(a, b);
    }

    #[test]
    fn format_is_s_prefix_16_hex() {
        let name = statement_name("select 1");
        assert!(name.starts_with("s_"), "must start with s_: {name}");
        assert_eq!(name.len(), 2 + 16, "s_ + 16 hex chars: {name}");
        assert!(
            name[2..].chars().all(|c| c.is_ascii_hexdigit()),
            "must be hex after s_: {name}"
        );
    }
}
