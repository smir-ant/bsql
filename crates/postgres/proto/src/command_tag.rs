//! Typed `CommandTag` for `CommandComplete` (`'C'`) frame payloads.
//!
//! # Purpose
//!
//! PostgreSQL's `CommandComplete` frame carries a NUL-terminated
//! command tag string (e.g. `"INSERT 0 5"`, `"UPDATE 12"`,
//! `"SELECT 100"`, `"DELETE 3"`). Pre-, this was stored
//! as a freeform `BoundedStr<32>` (36 B). The string format is
//! well-defined per PG §55.2.6.10:
//!
//! - `INSERT oid rows` — typically `oid = 0` since PG 12+; `rows`
//!   is u64-class affected-row count.
//! - `UPDATE rows`, `DELETE rows`, `SELECT rows`, `FETCH rows`,
//!   `COPY rows`, `MOVE rows` — single u64 row count.
//! - Anything else (DDL, `BEGIN`, `COMMIT`, etc.): freeform tag.
//!
//! Parsing the row count at the protocol layer lets the public API
//! deliver a typed `CommandTag` enum where consumers can match on
//! `CommandTag::Insert { rows: 5 }` instead of substring-parsing the
//! string. Tier-1 by-type: caller code never re-parses the tag.
//!
//! # Layout
//!
//! Dominator is `Other(BoundedStr<32>)` at 36 B + disc + pad ≈ 40 B
//! with default repr aligned to 8 (from `u64 rows`).
//!
//! # `u64` for `rows`
//!
//! PostgreSQL's CommandComplete row count is wire-encoded as a
//! decimal string. The server source (`progress_command_complete()`
//! in `src/backend/tcop/cmdtag.c`) uses `uint64`; a query touching
//! more than 2^32 rows in a single statement can produce a count
//! larger than `u32::MAX`. u64 future-proofs the type at zero
//! footprint cost (`Other` variant dominates anyway).

use crate::ident::BoundedStr;

/// Typed `CommandComplete` (`'C'`) tag.
///
/// `Copy + Clone + Eq + Debug` — no Drop impact on hot path. Public
/// API surfaces `Reply::QueryComplete.command_tag: &'r CommandTag`
/// (borrow into `crate::command_tag_slot::CommandTagSlotCell`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommandTag {
    /// `INSERT oid rows` — `oid = 0` for PG 12+. Only the row count
    /// is preserved.
    Insert {
        /// Number of rows inserted.
        rows: u64,
    },
    /// `UPDATE rows` — affected row count.
    Update {
        /// Number of rows updated.
        rows: u64,
    },
    /// `DELETE rows` — affected row count.
    Delete {
        /// Number of rows deleted.
        rows: u64,
    },
    /// `SELECT rows` — returned row count.
    Select {
        /// Number of rows returned.
        rows: u64,
    },
    /// `FETCH rows` — row count from a cursor's FETCH.
    Fetch {
        /// Number of rows fetched.
        rows: u64,
    },
    /// `MOVE rows` — rows the cursor moved over.
    Move {
        /// Number of rows the cursor moved past.
        rows: u64,
    },
    /// `COPY rows` — rows COPY'd.
    Copy {
        /// Number of rows COPY'd.
        rows: u64,
    },
    /// Freeform fallback (DDL, transaction control, etc.).
    Other(BoundedStr<32>),
}

impl CommandTag {
    /// Empty sentinel — used as the default before any `'C'`
    /// arrives.
    pub const EMPTY: Self = Self::Other(BoundedStr::new());

    /// Returns the row count if this is a known counted-row variant,
    /// or `None` for [`CommandTag::Other`].
    #[inline]
    #[must_use]
    pub const fn rows(&self) -> Option<u64> {
        match self {
            Self::Insert { rows }
            | Self::Update { rows }
            | Self::Delete { rows }
            | Self::Select { rows }
            | Self::Fetch { rows }
            | Self::Move { rows }
            | Self::Copy { rows } => Some(*rows),
            Self::Other(_) => None,
        }
    }

    /// Returns the affected-row count, projecting a tag with no row-count
    /// semantics ([`CommandTag::Other`] — DDL, transaction control) to `0`,
    /// the SQL-standard "rows affected" for such a statement.
    ///
    /// This is an exhaustive projection of the typed tag, not a fallback: every
    /// variant maps to a definite count, and the zero for `Other` is the
    /// correct answer, not a default masking a missing value.
    #[inline]
    #[must_use]
    pub const fn rows_or_zero(&self) -> u64 {
        match self {
            Self::Insert { rows }
            | Self::Update { rows }
            | Self::Delete { rows }
            | Self::Select { rows }
            | Self::Fetch { rows }
            | Self::Move { rows }
            | Self::Copy { rows } => *rows,
            Self::Other(_) => 0,
        }
    }
}

impl Default for CommandTag {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl core::fmt::Display for CommandTag {
    /// Round-trip wire-style formatting — `"INSERT 0 5"`,
    /// `"UPDATE 12"`, etc. For [`CommandTag::Other`], emits the
    /// stored bounded string verbatim.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Insert { rows } => write!(f, "INSERT 0 {rows}"),
            Self::Update { rows } => write!(f, "UPDATE {rows}"),
            Self::Delete { rows } => write!(f, "DELETE {rows}"),
            Self::Select { rows } => write!(f, "SELECT {rows}"),
            Self::Fetch { rows } => write!(f, "FETCH {rows}"),
            Self::Move { rows } => write!(f, "MOVE {rows}"),
            Self::Copy { rows } => write!(f, "COPY {rows}"),
            Self::Other(s) => f.write_str(s.as_str()),
        }
    }
}

/// Parse the `'C'` (CommandComplete) body bytes into a typed
/// [`CommandTag`]. Wire-malformed inputs (missing trailing NUL,
/// embedded NUL, non-UTF-8 content) are classified as
/// [`crate::error::ProtocolError::MalformedCommandComplete`].
/// Well-formed but unrecognised shapes fall to
/// [`CommandTag::Other`].
#[inline]
pub(crate) fn parse_command_tag_bytes(
    body: &[u8],
) -> Result<CommandTag, crate::error::ProtocolError> {
    let Some(bytes) = body.strip_suffix(b"\0") else {
        return Err(crate::error::ProtocolError::MalformedCommandComplete {
            payload_len: body.len(),
        });
    };
    if bytes.contains(&0u8) {
        return Err(crate::error::ProtocolError::MalformedCommandComplete {
            payload_len: body.len(),
        });
    }
    let s = match core::str::from_utf8(bytes) {
        Ok(s) => s,
        // A non-UTF-8 tag is malformed content, not an empty tag. Classify it
        // loudly instead of discarding it into an empty `Other`.
        Err(_) => {
            return Err(crate::error::ProtocolError::MalformedCommandComplete {
                payload_len: body.len(),
            });
        }
    };

    if let Some(suffix) = s.strip_prefix("SELECT ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Select { rows });
    }
    if let Some(suffix) = s.strip_prefix("INSERT 0 ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Insert { rows });
    }
    if let Some(suffix) = s.strip_prefix("UPDATE ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Update { rows });
    }
    if let Some(suffix) = s.strip_prefix("DELETE ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Delete { rows });
    }
    if let Some(suffix) = s.strip_prefix("FETCH ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Fetch { rows });
    }
    if let Some(suffix) = s.strip_prefix("MOVE ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Move { rows });
    }
    if let Some(suffix) = s.strip_prefix("COPY ")
        && let Ok(rows) = suffix.parse::<u64>() {
        return Ok(CommandTag::Copy { rows });
    }

    Ok(CommandTag::Other(BoundedStr::from_str_truncating(s)))
}

#[cfg(test)]
mod tests {
    //! Forbid-bundle compliance: `panic!`, `.unwrap()`, `.expect()`
    //! banned crate-wide. Tests use `assert!(matches!())` idiom;
    //! body-extraction via `if let Ok(x) = result`. Mirror of
    //! `error_arena::tests` shape.
    use super::*;
    use alloc::format;

    #[test]
    fn parses_select_rows() {
        let result = parse_command_tag_bytes(b"SELECT 100\0");
        assert!(matches!(result, Ok(CommandTag::Select { rows: 100 })));
        if let Ok(tag) = result {
            assert_eq!(tag.rows(), Some(100));
        }
    }

    #[test]
    fn parses_insert_with_oid_zero() {
        assert!(matches!(
            parse_command_tag_bytes(b"INSERT 0 5\0"),
            Ok(CommandTag::Insert { rows: 5 })
        ));
    }

    #[test]
    fn parses_update_rows() {
        assert!(matches!(
            parse_command_tag_bytes(b"UPDATE 12\0"),
            Ok(CommandTag::Update { rows: 12 })
        ));
    }

    #[test]
    fn parses_delete_fetch_move_copy() {
        assert!(matches!(parse_command_tag_bytes(b"DELETE 3\0"), Ok(CommandTag::Delete { rows: 3 })));
        assert!(matches!(parse_command_tag_bytes(b"FETCH 7\0"), Ok(CommandTag::Fetch { rows: 7 })));
        assert!(matches!(parse_command_tag_bytes(b"MOVE 4\0"), Ok(CommandTag::Move { rows: 4 })));
        assert!(matches!(parse_command_tag_bytes(b"COPY 200\0"), Ok(CommandTag::Copy { rows: 200 })));
    }

    #[test]
    fn ddl_falls_to_other() {
        let result = parse_command_tag_bytes(b"CREATE TABLE\0");
        assert!(matches!(result, Ok(CommandTag::Other(_))));
        if let Ok(tag) = result {
            assert_eq!(tag.rows(), None);
        }
    }

    #[test]
    fn malformed_numeric_falls_to_other() {
        assert!(matches!(
            parse_command_tag_bytes(b"SELECT abc\0"),
            Ok(CommandTag::Other(_))
        ));
    }

    #[test]
    fn round_trip_via_display() {
        for &expected in &[
            CommandTag::Insert { rows: 5 },
            CommandTag::Update { rows: 12 },
            CommandTag::Select { rows: 100 },
            CommandTag::Delete { rows: 3 },
        ] {
            let mut s = format!("{expected}");
            s.push('\0');
            let parsed = parse_command_tag_bytes(s.as_bytes());
            assert!(parsed.is_ok());
            if let Ok(p) = parsed {
                assert_eq!(expected, p);
            }
        }
    }

    #[test]
    fn missing_nul_terminator_classifies_malformed() {
        assert!(matches!(
            parse_command_tag_bytes(b"SELECT 5"),
            Err(crate::error::ProtocolError::MalformedCommandComplete { .. })
        ));
    }

    #[test]
    fn embedded_nul_classifies_malformed() {
        assert!(matches!(
            parse_command_tag_bytes(b"SEL\0ECT 5\0"),
            Err(crate::error::ProtocolError::MalformedCommandComplete { .. })
        ));
    }

    #[test]
    fn empty_body_classifies_malformed() {
        assert!(matches!(
            parse_command_tag_bytes(b""),
            Err(crate::error::ProtocolError::MalformedCommandComplete { .. })
        ));
    }

    #[test]
    fn nul_only_body_parses_empty_other() {
        assert!(matches!(
            parse_command_tag_bytes(b"\0"),
            Ok(CommandTag::Other(_))
        ));
    }

    #[test]
    fn non_utf8_tag_classifies_malformed() {
        // A NUL-terminated but non-UTF-8 tag is malformed content; it must be
        // classified, never silently collapsed into an empty `Other`.
        assert!(matches!(
            parse_command_tag_bytes(&[0xFF, 0xFE, 0]),
            Err(crate::error::ProtocolError::MalformedCommandComplete { .. })
        ));
    }

    #[test]
    fn rows_or_zero_projects_counted_and_countless() {
        assert_eq!(CommandTag::Insert { rows: 5 }.rows_or_zero(), 5);
        assert_eq!(CommandTag::Select { rows: 12 }.rows_or_zero(), 12);
        // A countless tag projects to zero rows affected.
        assert_eq!(CommandTag::EMPTY.rows_or_zero(), 0);
    }
}
