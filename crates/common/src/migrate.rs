//! The migration PURE LOGIC — the transport-agnostic half of the migration
//! runner, shared by every backend.
//!
//! A backend's runtime migration RUNNER (a driver's `run_migrations`) splits
//! cleanly into two halves:
//!
//! - **Pure, backend-agnostic (here).** The content [checksum](migration_checksum),
//!   the `/`-normalized name ORDERING authority (the [`MigrationSource`] loader
//!   and its duplicate-name pre-flight), the drift classification ([`plan`] over
//!   [`DriftKind`]), and the plain data / error types
//!   ([`AppliedMigration`], [`MigrationReport`], [`MigrationStatus`],
//!   [`MigrationSourceError`]). This is ONE compiled source, so the checksum, the
//!   apply order, and the drift semantics are IDENTICAL on every backend — not a
//!   test-pinned convention.
//! - **Per-backend I/O (in each driver).** The apply loop itself — PostgreSQL's
//!   non-blocking `pg_try_advisory_lock` poll over the transport-generic
//!   `Core<S>`, SQLite's `BEGIN IMMEDIATE` plus in-transaction ledger re-check —
//!   and the backend's own `MigrationError` (whose `Drift` variant carries this
//!   crate's [`DriftKind`], reached through a per-backend
//!   `From<`[`Drift`]`>` / `From<`[`MigrationSourceError`]`>`).
//!
//! The seam between them is [`plan`], which returns `Result<usize, `[`Drift`]`>`:
//! pure classification lives once, and each driver lifts a [`Drift`] into its own
//! error enum. The ledger SQL text (`timestamptz`/`now()`/`$N` on PostgreSQL,
//! `TEXT`/`datetime('now')`/`?` on SQLite) legitimately differs and stays in each
//! driver.
//!
//! # What the checksum / order / drift guarantee
//!
//! - **Exactly once, in order.** Each migration is named by its path relative to
//!   the migrations directory (`/`-normalized), and the set is applied in
//!   lexicographic order by that NAME — the SAME order `bsql-build` replays for
//!   the compile-checked `query!` catalog, so build-validated order == apply
//!   order on every platform.
//! - **Checksum-drift is loud.** An already-applied migration whose file CHANGED
//!   (its content checksum no longer matches the ledger) is a classified
//!   [`Drift`] with [`DriftKind::ChecksumMismatch`].
//! - **Append-only.** A migration inserted before, reordered around, or deleting
//!   an already-applied one is a classified [`Drift`]
//!   ([`DriftKind::Reordered`] / [`DriftKind::MissingFromSource`]).

use std::fmt;
use std::path::{Path, PathBuf};

/// The migration ledger table name. A FIXED compile-time identifier (never user
/// data), spliced only into each driver's constant ledger DDL — so there is no
/// identifier-injection surface at all. Unqualified, so it lands in the first
/// schema of the connection's `search_path` (which respects a connect-time
/// schema isolation, e.g. `#[bsql::test]`).
pub const LEDGER_TABLE: &str = "_bsql_migrations";

/// The comment directive marking a migration that must run OUTSIDE a transaction
/// (see [`is_non_transactional`]).
const NO_TRANSACTION_MARKER: &str = "bsql:no-transaction";

/// The content checksum of a migration's SQL — dependency-free FNV-1a-64.
///
/// Deterministic and toolchain-stable (explicit constants, unlike
/// `DefaultHasher`, whose output may change across releases), so a value stored
/// in the ledger stays comparable forever. Adequate for detecting an ACCIDENTAL
/// edit of an applied migration — the drift footgun this guards against — for
/// which cryptographic strength is unnecessary (an attacker who can rewrite a
/// migration file can rewrite the ledger too; the checksum is a footgun
/// detector, not a security boundary).
#[must_use]
pub fn migration_checksum(sql: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    sql.as_bytes()
        .iter()
        .fold(OFFSET, |hash, &byte| (hash ^ u64::from(byte)).wrapping_mul(PRIME))
}

/// The checksum rendered as a fixed 16-char lowercase hex string (the ledger's
/// stored form). Comparing these strings IS the drift check.
#[must_use]
pub fn checksum_hex(sql: &str) -> String {
    format!("{:016x}", migration_checksum(sql))
}

/// Whether a migration opts out of the wrapping transaction via a
/// `-- bsql:no-transaction` comment line. A dependency-free line scan: a line
/// whose trimmed content, after an optional leading `--`, begins with the
/// marker token. (A false positive would only run a normally-transactional
/// migration non-transactionally; the directive is written deliberately, so the
/// risk is negligible — and documented.)
#[must_use]
pub fn is_non_transactional(sql: &str) -> bool {
    sql.lines().any(|line| {
        let trimmed = line.trim_start();
        let content = match trimmed.strip_prefix("--") {
            Some(after) => after.trim_start(),
            None => trimmed,
        };
        content.starts_with(NO_TRANSACTION_MARKER)
    })
}

/// One migration loaded from its source: its stable NAME and full SQL.
///
/// Yielded (in name order) by [`MigrationSource::load`]; a driver's runner
/// iterates it, applying each `sql` and recording its `name`.
#[derive(Debug, Clone)]
pub struct LoadedMigration {
    /// The migration's stable name (its `/`-normalized path relative to the
    /// migrations root, or the embedded slice's given name).
    pub name: String,
    /// The migration's full SQL text.
    pub sql: String,
}

/// One ledger row (an already-applied migration).
#[derive(Debug, Clone)]
pub struct AppliedMigration {
    /// The migration's stable name (its ledger primary key).
    pub name: String,
    /// The content checksum recorded when it was applied (16-char hex).
    pub checksum: String,
    /// The apply timestamp, as the database rendered it to text.
    pub applied_at: String,
}

/// A detected drift of an already-applied migration from the current source —
/// the pure classification [`plan`] returns.
///
/// It is deliberately backend-neutral: each driver's own `MigrationError` lifts
/// it via `From<Drift>` into its `Drift` variant, so the classification lives in
/// ONE place while the surfaced error type stays per-backend (PostgreSQL carries
/// a `LockTimeout` variant SQLite has no peer for).
#[derive(Debug, Clone)]
pub struct Drift {
    /// The migration that drifted (by name).
    pub migration: String,
    /// How it diverged from the current source.
    pub kind: DriftKind,
}

/// Verify the already-applied migrations against the current source and return
/// the count of migrations already applied (so the pending set is
/// `source[count..]`).
///
/// The applied list (in apply order) must be a PREFIX of the source (in name
/// order): same names at the same positions, matching checksums. Any divergence
/// is a classified [`Drift`].
///
/// # Errors
///
/// [`Drift`] — a checksum mismatch, a reorder / insert-before, or an
/// already-applied migration deleted from the source.
pub fn plan(applied: &[AppliedMigration], source: &[LoadedMigration]) -> Result<usize, Drift> {
    // Every applied migration must still exist in the source (by name); an
    // absence is a deletion of an applied migration. Classify by POSITION: a
    // missing migration whose name sorts AFTER the last source name (or an empty
    // source) is a TAIL extra — the source is a strict prefix of the applied set,
    // which a rolling deploy / rollback also produces — whereas one within the
    // source name range is a MIDDLE gap (an unambiguous deletion). `source` is
    // sorted by name (from `MigrationSource::load`), so `source.last()` is the max.
    for a in applied {
        if !source.iter().any(|s| s.name == a.name) {
            let source_is_strict_prefix = match source.last() {
                None => true,
                Some(last) => a.name > last.name,
            };
            return Err(Drift {
                migration: a.name.clone(),
                kind: DriftKind::MissingFromSource { source_is_strict_prefix },
            });
        }
    }
    // The applied prefix must line up name-for-name and checksum-for-checksum
    // with the source. (Each applied name is in the source and names are unique,
    // so `applied.len() <= source.len()` and `source.get(i)` is always `Some`;
    // the `None` arm stays total.)
    for (i, a) in applied.iter().enumerate() {
        match source.get(i) {
            None => {
                // Unreachable given the loop above (every applied name is in the
                // source, so `applied.len() <= source.len()`); if reached, the
                // source is shorter than the applied set, i.e. a strict prefix.
                return Err(Drift {
                    migration: a.name.clone(),
                    kind: DriftKind::MissingFromSource { source_is_strict_prefix: true },
                });
            }
            Some(s) => {
                if s.name != a.name {
                    return Err(Drift {
                        migration: a.name.clone(),
                        kind: DriftKind::Reordered {
                            applied_ordinal: i,
                            source_name_at_ordinal: s.name.clone(),
                        },
                    });
                }
                let current = checksum_hex(&s.sql);
                if current != a.checksum {
                    return Err(Drift {
                        migration: a.name.clone(),
                        kind: DriftKind::ChecksumMismatch {
                            recorded: a.checksum.clone(),
                            current,
                        },
                    });
                }
            }
        }
    }
    Ok(applied.len())
}

// ─────────────────────────────────────────────────────────────────────────────
// Source loading.
// ─────────────────────────────────────────────────────────────────────────────

/// Where the migration set comes from.
///
/// Build one with [`MigrationSource::embedded`] (the baked
/// `bsql::embed_migrations!()` set — no filesystem at run time) or
/// [`MigrationSource::directory`] (walked at run time — the ops-friendly case).
/// Both feed the SAME runner, in the SAME lexicographic-by-name order.
#[derive(Debug, Clone, Copy)]
pub enum MigrationSource<'a> {
    /// A baked `&[(name, sql)]` set (from `bsql::embed_migrations!()`).
    Embedded(&'a [(&'a str, &'a str)]),
    /// A directory walked at run time (recursing into subdirectories, collecting
    /// `*.sql`). Unlike the embedded path, the directory source parses nothing —
    /// it applies the files as-is (the build-time acknowledgement gate ran on
    /// the EMBEDDED path).
    Directory(&'a Path),
}

impl<'a> MigrationSource<'a> {
    /// The embedded set baked by `bsql::embed_migrations!()`.
    #[must_use]
    pub fn embedded(migrations: &'a [(&'a str, &'a str)]) -> Self {
        Self::Embedded(migrations)
    }

    /// A migrations directory walked at run time.
    #[must_use]
    pub fn directory<P: AsRef<Path> + ?Sized>(path: &'a P) -> Self {
        Self::Directory(path.as_ref())
    }

    /// Load the set into owned `(name, sql)` pairs, sorted by name (the
    /// deterministic replay order shared with the build-time catalog), rejecting
    /// a duplicate name before any apply.
    ///
    /// The driver-composition seam: a backend's runner calls this to obtain the
    /// ordered set, then applies each entry.
    ///
    /// # Errors
    ///
    /// [`MigrationSourceError`] — a directory read error, a non-UTF-8 path, or a
    /// duplicate migration name.
    pub fn load(self) -> Result<Vec<LoadedMigration>, MigrationSourceError> {
        let mut loaded = match self {
            MigrationSource::Embedded(migrations) => migrations
                .iter()
                .map(|&(name, sql)| LoadedMigration {
                    name: name.to_owned(),
                    sql: sql.to_owned(),
                })
                .collect::<Vec<_>>(),
            MigrationSource::Directory(dir) => walk_directory(dir)?,
        };
        loaded.sort_by(|a, b| a.name.cmp(&b.name));
        // Reject a duplicate name BEFORE any apply — a hand-built embedded slice
        // could carry two migrations of the same name (a directory walk yields
        // unique paths). Without this, PostgreSQL fails loud on the ledger PK but
        // SQLite would silently skip the second (its in-transaction re-check sees
        // the name recorded), a parity break + a silent skip. Loud on BOTH now.
        let duplicate = loaded.windows(2).find_map(|pair| match pair {
            [a, b] if a.name == b.name => Some(a.name.clone()),
            _ => None,
        });
        if let Some(name) = duplicate {
            return Err(MigrationSourceError::DuplicateName { name });
        }
        Ok(loaded)
    }
}

impl<'a> From<&'a [(&'a str, &'a str)]> for MigrationSource<'a> {
    fn from(migrations: &'a [(&'a str, &'a str)]) -> Self {
        Self::Embedded(migrations)
    }
}

impl<'a> From<&'a Path> for MigrationSource<'a> {
    fn from(dir: &'a Path) -> Self {
        Self::Directory(dir)
    }
}

/// Recursively collect `*.sql` files under `dir` as `(relative-name, sql)`,
/// with `/`-normalized names so the runtime name matches the build-baked name
/// for the same file on every platform.
fn walk_directory(dir: &Path) -> Result<Vec<LoadedMigration>, MigrationSourceError> {
    let mut out = Vec::new();
    descend(dir, dir, &mut out)?;
    Ok(out)
}

fn descend(
    root: &Path,
    dir: &Path,
    out: &mut Vec<LoadedMigration>,
) -> Result<(), MigrationSourceError> {
    let entries = std::fs::read_dir(dir).map_err(|e| MigrationSourceError::Io {
        path: dir.to_path_buf(),
        message: e.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| MigrationSourceError::Io {
            path: dir.to_path_buf(),
            message: e.to_string(),
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| MigrationSourceError::Io {
            path: path.clone(),
            message: e.to_string(),
        })?;
        if file_type.is_dir() {
            descend(root, &path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "sql") {
            let name = relative_name(root, &path)?;
            let sql = std::fs::read_to_string(&path).map_err(|e| MigrationSourceError::Io {
                path: path.clone(),
                message: e.to_string(),
            })?;
            out.push(LoadedMigration { name, sql });
        }
    }
    Ok(())
}

/// The `/`-normalized name of a file relative to the migrations root.
fn relative_name(root: &Path, file: &Path) -> Result<String, MigrationSourceError> {
    let rel = match file.strip_prefix(root) {
        Ok(r) => r,
        Err(_) => file,
    };
    let text = rel.to_str().ok_or_else(|| MigrationSourceError::NonUtf8Path {
        path: file.to_path_buf(),
    })?;
    Ok(text.replace(std::path::MAIN_SEPARATOR, "/"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Result / status / error types.
// ─────────────────────────────────────────────────────────────────────────────

/// The outcome of a migration run (a driver's `run_migrations`).
#[derive(Debug, Clone)]
pub struct MigrationReport {
    /// The names of the migrations applied by THIS run, in order (empty when the
    /// database was already up to date).
    pub applied: Vec<String>,
    /// How many migrations were already applied before this run.
    pub already_applied: usize,
}

impl MigrationReport {
    /// Whether this run applied at least one migration.
    #[must_use]
    pub fn applied_any(&self) -> bool {
        !self.applied.is_empty()
    }
}

/// A read-only snapshot from a driver's `migration_status`.
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// The already-applied migrations, in apply order (from the ledger).
    pub applied: Vec<AppliedMigration>,
    /// The names of migrations present in the source but not yet applied, in
    /// order.
    pub pending: Vec<String>,
}

/// Why a migration source could not be loaded.
#[derive(Debug)]
#[non_exhaustive]
pub enum MigrationSourceError {
    /// A runtime directory could not be read.
    Io {
        /// The directory or file that failed.
        path: PathBuf,
        /// The underlying I/O error, rendered.
        message: String,
    },
    /// A migration file's path is not valid UTF-8, so no stable name can be
    /// formed for the ledger.
    NonUtf8Path {
        /// The offending path.
        path: PathBuf,
    },
    /// Two migrations in the source share a name (only reachable from a
    /// hand-built embedded slice — a directory walk yields unique paths). A
    /// loud pre-flight error on BOTH backends, before any apply, rather than a
    /// silent skip of the second.
    DuplicateName {
        /// The name that appears more than once.
        name: String,
    },
}

impl fmt::Display for MigrationSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationSourceError::Io { path, message } => {
                write!(f, "cannot read migrations at {}: {message}", path.display())
            }
            MigrationSourceError::NonUtf8Path { path } => {
                write!(f, "migration path is not valid UTF-8: {}", path.display())
            }
            MigrationSourceError::DuplicateName { name } => {
                write!(f, "duplicate migration name `{name}` in the source")
            }
        }
    }
}

impl std::error::Error for MigrationSourceError {}

/// How an already-applied migration diverged from the current source.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DriftKind {
    /// The migration file's content changed after it was applied (its checksum
    /// no longer matches the ledger).
    ChecksumMismatch {
        /// The checksum the ledger recorded at apply time.
        recorded: String,
        /// The checksum of the current file.
        current: String,
    },
    /// The migration is no longer at the ordinal it was applied at — a migration
    /// was inserted before it, or the set was reordered. The set must be
    /// append-only.
    Reordered {
        /// The ordinal (0-based apply position) of the applied migration.
        applied_ordinal: usize,
        /// The name the current source places at that ordinal instead.
        source_name_at_ordinal: String,
    },
    /// An already-applied migration is absent from the current source.
    ///
    /// The `source_is_strict_prefix` flag distinguishes two causes that carry the
    /// SAME data but demand DIFFERENT operator action:
    ///
    /// - `false` (a MIDDLE gap — a LATER applied migration is still present in the
    ///   source): a genuine deletion of an applied migration. A rolling deploy
    ///   cannot drop a middle migration while keeping a later one, so the
    ///   diagnosis is unambiguous: restore it.
    /// - `true` (a TAIL extra — the applied name sorts AFTER the last source name,
    ///   or the source is empty, i.e. the source is a strict prefix of the applied
    ///   set): EITHER a tail deletion OR this instance's migration set is OLDER
    ///   than the database (a rolling deploy / rollback where an older binary
    ///   restarted against a newer DB). The two states are indistinguishable from
    ///   the data alone, so the message names both — it does not over-assert.
    MissingFromSource {
        /// `true` when the source is a strict prefix of the applied set (the
        /// missing migration is a tail extra); `false` for a middle gap.
        source_is_strict_prefix: bool,
    },
}

impl fmt::Display for DriftKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DriftKind::ChecksumMismatch { recorded, current } => write!(
                f,
                "content changed after it was applied (recorded checksum {recorded}, \
                 current {current}) — an applied migration must not be edited; add a \
                 NEW migration instead"
            ),
            DriftKind::Reordered {
                applied_ordinal,
                source_name_at_ordinal,
            } => write!(
                f,
                "was applied at position {applied_ordinal} but the source now has \
                 `{source_name_at_ordinal}` there — a migration was inserted before or \
                 reordered around an applied one; the set must be append-only"
            ),
            // MIDDLE gap: a later applied migration is still in the source, so a
            // rolling deploy cannot explain it — the accurate, unambiguous
            // "deleted, restore it".
            DriftKind::MissingFromSource { source_is_strict_prefix: false } => write!(
                f,
                "is recorded as applied but is absent from the source — it was deleted \
                 after being applied; restore it (an applied migration must not be removed)"
            ),
            // TAIL extra: the source is a strict prefix of the applied set, which
            // is EITHER a tail deletion OR an older instance restarted against a
            // newer DB. Name both causes; do not over-assert either.
            DriftKind::MissingFromSource { source_is_strict_prefix: true } => write!(
                f,
                "is recorded as applied but is absent from the current source — EITHER it \
                 was deleted after being applied, OR this instance's migration set is OLDER \
                 than the database (a rolling deploy / rollback). Verify the app version \
                 before restoring the migration"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loaded(name: &str, sql: &str) -> LoadedMigration {
        LoadedMigration {
            name: name.to_owned(),
            sql: sql.to_owned(),
        }
    }

    fn applied(name: &str, sql: &str) -> AppliedMigration {
        AppliedMigration {
            name: name.to_owned(),
            checksum: checksum_hex(sql),
            applied_at: "2026-01-01 00:00:00+00".to_owned(),
        }
    }

    // The ONE authority now: with a single compiled source there are no two
    // copies to cross-pin, so this test does its GENUINE job — pin the FNV-1a-64
    // algorithm to a fixed on-disk value (so a ledger written today stays
    // comparable forever) AND prove determinism + content-sensitivity. The two
    // former per-backend cross-pin vectors are subsumed by this single source.
    #[test]
    fn checksum_is_deterministic_content_sensitive_and_pins_the_algorithm() {
        // Determinism + content sensitivity.
        assert_eq!(
            migration_checksum("CREATE TABLE t (a int)"),
            migration_checksum("CREATE TABLE t (a int)")
        );
        assert_ne!(
            migration_checksum("CREATE TABLE t (a int)"),
            migration_checksum("CREATE TABLE t (a INT)")
        );
        assert_ne!(migration_checksum(""), migration_checksum(" "));
        // The fixed algorithm vector: FNV-1a-64 of the ASCII bytes of "bsql".
        assert_eq!(migration_checksum("bsql"), 0x3587_bc9c_01e6_f51f);
    }

    #[test]
    fn checksum_hex_is_16_lowercase_hex() {
        let hex = checksum_hex("anything");
        assert_eq!(hex.len(), 16);
        assert!(hex.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    }

    #[test]
    fn non_transactional_marker_detected() {
        assert!(is_non_transactional(
            "-- bsql:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (a)"
        ));
        assert!(is_non_transactional("  --   bsql:no-transaction  \nVACUUM"));
        assert!(!is_non_transactional("CREATE TABLE t (a int) -- not a directive"));
        assert!(!is_non_transactional("-- bsql:ack-destructive\nDROP TABLE t"));
    }

    #[test]
    fn plan_clean_prefix_returns_applied_count() {
        let source = vec![loaded("0001", "a"), loaded("0002", "b"), loaded("0003", "c")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        assert_eq!(plan(&done, &source).unwrap(), 2);
    }

    #[test]
    fn plan_empty_ledger_is_all_pending() {
        let source = vec![loaded("0001", "a"), loaded("0002", "b")];
        assert_eq!(plan(&[], &source).unwrap(), 0);
    }

    #[test]
    fn plan_checksum_drift_is_classified() {
        let source = vec![loaded("0001", "EDITED")];
        let done = vec![applied("0001", "ORIGINAL")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            Drift { kind: DriftKind::ChecksumMismatch { .. }, .. }
        ));
    }

    #[test]
    fn plan_insert_before_applied_is_reorder() {
        // 0002 was applied; source now inserts 0001 before it.
        let source = vec![loaded("0001", "a"), loaded("0002", "b")];
        let done = vec![applied("0002", "b")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            Drift { kind: DriftKind::Reordered { .. }, .. }
        ));
    }

    #[test]
    fn plan_middle_gap_deletion_is_a_strict_deletion() {
        // 0001 is missing but a LATER applied migration (0002) is still in the
        // source — a rolling deploy cannot produce this, so it is an unambiguous
        // deletion (source is NOT a strict prefix).
        let source = vec![loaded("0002", "b")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            Drift {
                migration,
                kind: DriftKind::MissingFromSource { source_is_strict_prefix: false }
            } if migration == "0001"
        ));
    }

    #[test]
    fn plan_tail_extra_is_ambiguous_deletion_or_older_instance() {
        // 0002 is applied but the source stops at 0001 (a strict prefix) — EITHER
        // a tail deletion OR an older instance restarted against a newer DB.
        let source = vec![loaded("0001", "a")];
        let done = vec![applied("0001", "a"), applied("0002", "b")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            Drift {
                migration,
                kind: DriftKind::MissingFromSource { source_is_strict_prefix: true }
            } if migration == "0002"
        ));
    }

    #[test]
    fn plan_empty_source_against_a_populated_ledger_is_a_tail_extra() {
        // The extreme strict prefix: an empty source can only mean the applied set
        // is entirely ahead (a fresh checkout / older instance), never a deletion
        // the operator should "restore".
        let source: Vec<LoadedMigration> = vec![];
        let done = vec![applied("0001", "a")];
        let err = plan(&done, &source).unwrap_err();
        assert!(matches!(
            err,
            Drift { kind: DriftKind::MissingFromSource { source_is_strict_prefix: true }, .. }
        ));
    }

    #[test]
    fn the_two_missing_from_source_messages_diagnose_differently() {
        // The whole point of the split: a MIDDLE gap says "restore it"; a TAIL
        // extra names BOTH causes and does NOT assert one over the other.
        let middle = DriftKind::MissingFromSource { source_is_strict_prefix: false }.to_string();
        let tail = DriftKind::MissingFromSource { source_is_strict_prefix: true }.to_string();
        assert_ne!(middle, tail, "the two causes must not read identically");
        assert!(middle.contains("restore it"), "middle gap keeps the restore directive");
        assert!(!middle.contains("OLDER"), "middle gap must not raise the rolling-deploy reading");
        assert!(
            tail.contains("OLDER than the database"),
            "tail extra must name the older-instance cause"
        );
        assert!(
            tail.contains("Verify the app version"),
            "tail extra must caution before restoring"
        );
    }

    #[test]
    fn embedded_source_loads_and_sorts_by_name() {
        let raw = [("0002_b.sql", "B"), ("0001_a.sql", "A")];
        let loaded = MigrationSource::embedded(&raw).load().unwrap();
        assert_eq!(loaded[0].name, "0001_a.sql");
        assert_eq!(loaded[1].name, "0002_b.sql");
    }

    #[test]
    fn duplicate_name_in_a_slice_is_a_loud_source_error() {
        // A hand-built slice with two same-named migrations is rejected BEFORE
        // any apply (PG fails loud on the ledger PK; SQLite would otherwise
        // silently skip the second). Loud on BOTH now — one source.
        let raw = [("dup.sql", "SELECT 1"), ("dup.sql", "SELECT 2")];
        let err = MigrationSource::embedded(&raw)
            .load()
            .expect_err("duplicate name must be loud");
        assert!(matches!(err, MigrationSourceError::DuplicateName { name } if name == "dup.sql"));
    }
}
