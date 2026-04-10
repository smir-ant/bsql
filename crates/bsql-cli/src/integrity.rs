//! Local cache integrity verification (no database required).
//!
//! Unlike `verify.rs` (which checks cached queries against a live schema),
//! this module checks that the cache directory itself is self-consistent:
//!
//! 1. Every hash listed in `.manifest` / `.manifest.canonical` has a
//!    corresponding `.bitcode` file on disk.
//! 2. Every `.bitcode` file on disk decodes cleanly (correct envelope,
//!    valid inner payload, matching cache format version).
//! 3. Every `.bitcode` file's name matches the hash encoded in the payload.
//!
//! Used by `bsql verify` as a pre-commit / pre-push check — if this fails,
//! the cache is broken and will fail at compile time in offline mode.

use std::collections::HashSet;
use std::path::Path;

use crate::cache;

#[derive(Debug, Default)]
pub struct IntegrityReport {
    /// Hashes listed in manifest(s) but missing bitcode files.
    pub missing_bitcode: Vec<String>,
    /// Bitcode files on disk not listed in any manifest. Not an error on
    /// its own (manifest is authoritative for "what this build uses"), but
    /// reported as a warning so users can clean them up.
    pub orphan_bitcode: Vec<String>,
    /// Bitcode files that failed to decode.
    pub corrupt_files: Vec<(String, String)>,
    /// Bitcode files whose encoded hash does not match their filename.
    pub filename_mismatch: Vec<String>,
    /// Total bitcode files found on disk.
    pub total_bitcode: usize,
    /// Total unique hashes across all manifest files.
    pub total_manifested: usize,
}

impl IntegrityReport {
    /// True when the cache is consistent enough for offline mode to succeed.
    /// Orphan files are ignored (they're not a build-breaker).
    pub fn is_ok(&self) -> bool {
        self.missing_bitcode.is_empty()
            && self.corrupt_files.is_empty()
            && self.filename_mismatch.is_empty()
    }
}

/// Read newline-separated hex hashes from a file into a set.
fn read_hash_set(path: &Path) -> HashSet<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(|l| l.to_owned())
        .collect()
}

/// Run the full integrity check against `cache_dir`.
pub fn check(cache_dir: &Path) -> Result<IntegrityReport, String> {
    if !cache_dir.is_dir() {
        return Err(format!(
            "cache directory does not exist: {}",
            cache_dir.display()
        ));
    }

    let manifest = read_hash_set(&cache_dir.join(".manifest"));
    let canonical = read_hash_set(&cache_dir.join(".manifest.canonical"));
    let manifested: HashSet<String> = manifest.union(&canonical).cloned().collect();

    let mut on_disk: HashSet<String> = HashSet::new();
    let mut report = IntegrityReport {
        total_manifested: manifested.len(),
        ..Default::default()
    };

    // Scan .bitcode files on disk
    let entries = std::fs::read_dir(cache_dir)
        .map_err(|e| format!("cannot read {}: {e}", cache_dir.display()))?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "bitcode") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        on_disk.insert(stem.to_owned());
        report.total_bitcode += 1;

        // Decode each file to catch corruption early
        match cache::read_cache_file(&path) {
            Ok(cached) => {
                let expected = format!("{:016x}", cached.sql_hash);
                if expected != stem {
                    report
                        .filename_mismatch
                        .push(format!("{} (encoded hash: {expected})", path.display()));
                }
            }
            Err(e) => {
                report.corrupt_files.push((path.display().to_string(), e));
            }
        }
    }

    // Manifested but missing from disk — the fatal case
    for hash in &manifested {
        if !on_disk.contains(hash) {
            report.missing_bitcode.push(hash.clone());
        }
    }

    // Orphans — on disk but not in any manifest
    for hash in &on_disk {
        if !manifested.contains(hash) {
            report.orphan_bitcode.push(hash.clone());
        }
    }

    report.missing_bitcode.sort();
    report.orphan_bitcode.sort();
    report.corrupt_files.sort_by(|a, b| a.0.cmp(&b.0));
    report.filename_mismatch.sort();

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcode::Encode;

    #[derive(Encode)]
    struct TestEnvelope {
        version: u8,
        data: Vec<u8>,
    }

    #[derive(Encode)]
    struct TestCachedQuery {
        sql_hash: u64,
        normalized_sql: String,
        columns: Vec<TestCachedColumn>,
        param_pg_oids: Vec<u32>,
        param_is_pg_enum: Vec<bool>,
        bsql_version: String,
        param_rust_types: Vec<String>,
        rewritten_sql: Option<String>,
    }

    #[derive(Encode)]
    struct TestCachedColumn {
        name: String,
        pg_oid: u32,
        pg_type_name: String,
        is_nullable: bool,
        rust_type: String,
    }

    fn write_valid_bitcode(dir: &Path, hash: u64) {
        let query = TestCachedQuery {
            sql_hash: hash,
            normalized_sql: format!("select {hash}"),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: env!("CARGO_PKG_VERSION").to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        };
        let inner = bitcode::encode(&query);
        let envelope = TestEnvelope {
            version: 4,
            data: inner,
        };
        let bytes = bitcode::encode(&envelope);
        let path = dir.join(format!("{hash:016x}.bitcode"));
        std::fs::write(&path, &bytes).unwrap();
    }

    #[test]
    fn check_empty_dir_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let report = check(dir.path()).unwrap();
        assert!(report.is_ok());
        assert_eq!(report.total_bitcode, 0);
        assert_eq!(report.total_manifested, 0);
    }

    #[test]
    fn check_consistent_cache_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        write_valid_bitcode(dir.path(), 0xdead);
        write_valid_bitcode(dir.path(), 0xbeef);
        std::fs::write(
            dir.path().join(".manifest"),
            "000000000000dead\n000000000000beef\n",
        )
        .unwrap();

        let report = check(dir.path()).unwrap();
        assert!(report.is_ok());
        assert_eq!(report.total_bitcode, 2);
        assert_eq!(report.total_manifested, 2);
        assert!(report.missing_bitcode.is_empty());
        assert!(report.orphan_bitcode.is_empty());
    }

    #[test]
    fn check_detects_missing_bitcode() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(".manifest"),
            "000000000000dead\n000000000000beef\n",
        )
        .unwrap();
        // No bitcode files written — this is the exact bug state from prod

        let report = check(dir.path()).unwrap();
        assert!(!report.is_ok());
        assert_eq!(report.missing_bitcode.len(), 2);
        assert!(report
            .missing_bitcode
            .contains(&"000000000000dead".to_owned()));
        assert!(report
            .missing_bitcode
            .contains(&"000000000000beef".to_owned()));
    }

    #[test]
    fn check_detects_orphans_as_warning() {
        let dir = tempfile::tempdir().unwrap();
        write_valid_bitcode(dir.path(), 0xdead);
        // Manifest is empty — the bitcode file is an orphan

        let report = check(dir.path()).unwrap();
        assert!(report.is_ok()); // orphans don't make the report fail
        assert_eq!(report.orphan_bitcode.len(), 1);
    }

    #[test]
    fn check_detects_corrupt_bitcode() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("000000000000dead.bitcode"), b"garbage").unwrap();
        std::fs::write(dir.path().join(".manifest"), "000000000000dead\n").unwrap();

        let report = check(dir.path()).unwrap();
        assert!(!report.is_ok());
        assert_eq!(report.corrupt_files.len(), 1);
    }

    #[test]
    fn check_detects_filename_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        // Write valid bitcode but rename to a different hash
        write_valid_bitcode(dir.path(), 0xdead);
        let wrong_name = dir.path().join("000000000000beef.bitcode");
        std::fs::rename(dir.path().join("000000000000dead.bitcode"), &wrong_name).unwrap();
        std::fs::write(dir.path().join(".manifest"), "000000000000beef\n").unwrap();

        let report = check(dir.path()).unwrap();
        assert!(!report.is_ok());
        assert_eq!(report.filename_mismatch.len(), 1);
    }

    #[test]
    fn check_canonical_also_counted() {
        let dir = tempfile::tempdir().unwrap();
        write_valid_bitcode(dir.path(), 0xdead);
        std::fs::write(dir.path().join(".manifest"), "").unwrap();
        std::fs::write(dir.path().join(".manifest.canonical"), "000000000000dead\n").unwrap();

        let report = check(dir.path()).unwrap();
        assert!(report.is_ok());
        // In canonical, still manifested
        assert_eq!(report.total_manifested, 1);
        assert!(report.orphan_bitcode.is_empty());
    }
}
