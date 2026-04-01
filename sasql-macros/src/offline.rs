//! Offline mode: build without a live PostgreSQL instance.
//!
//! During normal compilation (PG available), each `query!()` invocation
//! writes its validation result to `.sasql/queries/{sql_hash}.bitcode`.
//! When `SASQL_OFFLINE=true`, the proc macro reads from these files
//! instead of connecting to PG.
//!
//! The cache is per-query (one file per SQL hash), so no file locking is
//! needed and incremental compilation works naturally.

use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::LazyLock;

use bitcode::{Decode, Encode};

use crate::parse::ParsedQuery;
use crate::validate::{ColumnInfo, ValidationResult};

// ---------------------------------------------------------------------------
// Cache data structures
// ---------------------------------------------------------------------------

/// A single cached query validation result, persisted as bitcode.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CachedQuery {
    /// rapidhash of the normalized SQL (the filename / lookup key).
    pub sql_hash: u64,
    /// The normalized SQL text (for verification and diagnostics).
    pub normalized_sql: String,
    /// Result columns (empty for non-SELECT / non-RETURNING queries).
    pub columns: Vec<CachedColumn>,
    /// PostgreSQL OIDs of the expected parameter types.
    pub param_pg_oids: Vec<u32>,
    /// Whether each parameter position is a PG enum type.
    pub param_is_pg_enum: Vec<bool>,
}

/// A single result column, cached.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CachedColumn {
    pub name: String,
    pub pg_oid: u32,
    pub pg_type_name: String,
    pub is_nullable: bool,
    pub rust_type: String,
}

// ---------------------------------------------------------------------------
// Offline detection
// ---------------------------------------------------------------------------

/// Whether offline mode is active (`SASQL_OFFLINE=true` or `=1`).
///
/// Evaluated once per compilation via `LazyLock`.
static IS_OFFLINE: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("SASQL_OFFLINE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
});

pub fn is_offline() -> bool {
    *IS_OFFLINE
}

// ---------------------------------------------------------------------------
// Cache directory resolution
// ---------------------------------------------------------------------------

/// Resolve the `.sasql/queries/` directory, walking up from `CARGO_MANIFEST_DIR`
/// to find an existing `.sasql/` (or creating it next to the workspace root).
///
/// Cached once per compilation.
static CACHE_DIR: LazyLock<Result<PathBuf, String>> = LazyLock::new(resolve_cache_dir);

fn resolve_cache_dir() -> Result<PathBuf, String> {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").map_err(|_| "CARGO_MANIFEST_DIR not set".to_owned())?;

    // Walk up from CARGO_MANIFEST_DIR looking for an existing .sasql/ directory
    let mut dir = PathBuf::from(&manifest_dir);
    loop {
        let candidate = dir.join(".sasql");
        if candidate.is_dir() {
            return Ok(candidate.join("queries"));
        }
        // Also check for workspace Cargo.toml (root of workspace)
        let cargo_toml = dir.join("Cargo.toml");
        if cargo_toml.is_file() {
            // Check if this Cargo.toml contains [workspace]
            if let Ok(contents) = std::fs::read_to_string(&cargo_toml) {
                if contents.contains("[workspace]") {
                    // This is the workspace root -- use .sasql/ here
                    return Ok(dir.join(".sasql").join("queries"));
                }
            }
        }
        if !dir.pop() {
            break;
        }
    }

    // Fallback: use CARGO_MANIFEST_DIR
    Ok(PathBuf::from(&manifest_dir).join(".sasql").join("queries"))
}

fn cache_dir() -> Result<&'static PathBuf, String> {
    CACHE_DIR.as_ref().map_err(|e| e.clone())
}

// ---------------------------------------------------------------------------
// SQL hash computation
// ---------------------------------------------------------------------------

/// Compute the rapidhash of normalized SQL, used as the cache key.
pub fn sql_hash(normalized_sql: &str) -> u64 {
    let mut hasher = rapidhash::quality::RapidHasher::default();
    normalized_sql.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Cache reading (offline mode)
// ---------------------------------------------------------------------------

/// Look up a cached validation result for a query.
///
/// Returns the cached `ValidationResult` or a descriptive error.
pub fn lookup_cached_validation(parsed: &ParsedQuery) -> Result<ValidationResult, String> {
    let hash = sql_hash(&parsed.normalized_sql);
    let dir = cache_dir()?;
    let path = dir.join(format!("{hash:016x}.bitcode"));

    if !path.exists() {
        return Err(format!(
            "query not found in offline cache (hash {hash:016x}). \
             The SQL may have changed since the cache was last built. \
             Run `cargo build` with a live PostgreSQL connection to update \
             the cache, then rebuild with SASQL_OFFLINE=true.\n  \
             SQL: {}",
            parsed.normalized_sql
        ));
    }

    let bytes = std::fs::read(&path)
        .map_err(|e| format!("failed to read offline cache file {}: {e}", path.display()))?;

    let cached: CachedQuery = bitcode::decode(&bytes).map_err(|e| {
        format!(
            "failed to decode offline cache file {} (file may be corrupted \
             or from an incompatible sasql version -- rebuild with a live \
             PostgreSQL connection): {e}",
            path.display()
        )
    })?;

    // Verify the normalized SQL matches (guards against hash collisions,
    // which are astronomically unlikely but worth defending against)
    if cached.normalized_sql != parsed.normalized_sql {
        return Err(format!(
            "offline cache hash collision detected (hash {hash:016x}). \
             Cached SQL does not match current SQL. Run `cargo build` \
             with a live PostgreSQL connection to regenerate the cache.\n  \
             cached: {}\n  current: {}",
            cached.normalized_sql, parsed.normalized_sql
        ));
    }

    Ok(cached_to_validation(&cached))
}

/// Convert a `CachedQuery` into a `ValidationResult`.
fn cached_to_validation(cached: &CachedQuery) -> ValidationResult {
    let columns = cached
        .columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            pg_oid: c.pg_oid,
            pg_type_name: c.pg_type_name.clone(),
            is_nullable: c.is_nullable,
            rust_type: c.rust_type.clone(),
        })
        .collect();

    ValidationResult {
        columns,
        param_pg_oids: cached.param_pg_oids.clone(),
        param_is_pg_enum: cached.param_is_pg_enum.clone(),
    }
}

// ---------------------------------------------------------------------------
// Cache writing (online mode side-effect)
// ---------------------------------------------------------------------------

/// Write a validation result to the offline cache.
///
/// Called as a side effect during normal (online) compilation.
/// Errors are logged to stderr but do not fail the build -- the cache
/// is a convenience, not a requirement for online builds.
pub fn write_cache(parsed: &ParsedQuery, validation: &ValidationResult) {
    if let Err(e) = write_cache_inner(parsed, validation) {
        // Log but do not fail the build
        eprintln!("sasql: warning: failed to write offline cache: {e}");
    }
}

fn write_cache_inner(parsed: &ParsedQuery, validation: &ValidationResult) -> Result<(), String> {
    let dir = cache_dir()?;

    // Create the directory if it does not exist
    std::fs::create_dir_all(dir).map_err(|e| {
        format!(
            "failed to create offline cache directory {}: {e}",
            dir.display()
        )
    })?;

    let hash = sql_hash(&parsed.normalized_sql);
    let cached = validation_to_cached(hash, parsed, validation);
    let bytes = bitcode::encode(&cached);

    let path = dir.join(format!("{hash:016x}.bitcode"));

    // Atomic write: write to a temp file then rename.
    // This prevents partial reads if another proc macro invocation reads
    // concurrently (though each query writes to its own file, so this
    // is mostly a precaution).
    let tmp_path = dir.join(format!("{hash:016x}.bitcode.tmp"));

    std::fs::write(&tmp_path, &bytes).map_err(|e| {
        format!(
            "failed to write offline cache file {}: {e}",
            tmp_path.display()
        )
    })?;

    std::fs::rename(&tmp_path, &path).map_err(|e| {
        format!(
            "failed to rename offline cache file {} -> {}: {e}",
            tmp_path.display(),
            path.display()
        )
    })?;

    Ok(())
}

/// Convert a `ValidationResult` into a `CachedQuery` for serialization.
fn validation_to_cached(
    hash: u64,
    parsed: &ParsedQuery,
    validation: &ValidationResult,
) -> CachedQuery {
    let columns = validation
        .columns
        .iter()
        .map(|c| CachedColumn {
            name: c.name.clone(),
            pg_oid: c.pg_oid,
            pg_type_name: c.pg_type_name.clone(),
            is_nullable: c.is_nullable,
            rust_type: c.rust_type.clone(),
        })
        .collect();

    CachedQuery {
        sql_hash: hash,
        normalized_sql: parsed.normalized_sql.clone(),
        columns,
        param_pg_oids: validation.param_pg_oids.clone(),
        param_is_pg_enum: validation.param_is_pg_enum.clone(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    /// Build a minimal CachedQuery for testing.
    fn sample_cached_query() -> CachedQuery {
        CachedQuery {
            sql_hash: 0xDEAD_BEEF_CAFE_1234,
            normalized_sql: "select id, name from users where id = $1".into(),
            columns: vec![
                CachedColumn {
                    name: "id".into(),
                    pg_oid: 23,
                    pg_type_name: "int4".into(),
                    is_nullable: false,
                    rust_type: "i32".into(),
                },
                CachedColumn {
                    name: "name".into(),
                    pg_oid: 25,
                    pg_type_name: "text".into(),
                    is_nullable: true,
                    rust_type: "Option<String>".into(),
                },
            ],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
        }
    }

    #[test]
    fn bitcode_round_trip() {
        let original = sample_cached_query();
        let bytes = bitcode::encode(&original);
        let decoded: CachedQuery = bitcode::decode(&bytes).expect("decode failed");

        assert_eq!(decoded.sql_hash, original.sql_hash);
        assert_eq!(decoded.normalized_sql, original.normalized_sql);
        assert_eq!(decoded.columns.len(), original.columns.len());
        assert_eq!(decoded.param_pg_oids, original.param_pg_oids);
        assert_eq!(decoded.param_is_pg_enum, original.param_is_pg_enum);

        for (d, o) in decoded.columns.iter().zip(&original.columns) {
            assert_eq!(d.name, o.name);
            assert_eq!(d.pg_oid, o.pg_oid);
            assert_eq!(d.pg_type_name, o.pg_type_name);
            assert_eq!(d.is_nullable, o.is_nullable);
            assert_eq!(d.rust_type, o.rust_type);
        }
    }

    #[test]
    fn cached_to_validation_preserves_all_fields() {
        let cached = sample_cached_query();
        let validation = cached_to_validation(&cached);

        assert_eq!(validation.columns.len(), 2);
        assert_eq!(validation.columns[0].name, "id");
        assert_eq!(validation.columns[0].pg_oid, 23);
        assert_eq!(validation.columns[0].is_nullable, false);
        assert_eq!(validation.columns[0].rust_type, "i32");
        assert_eq!(validation.columns[1].name, "name");
        assert_eq!(validation.columns[1].is_nullable, true);
        assert_eq!(validation.columns[1].rust_type, "Option<String>");
        assert_eq!(validation.param_pg_oids, vec![23]);
        assert_eq!(validation.param_is_pg_enum, vec![false]);
    }

    #[test]
    fn validation_to_cached_preserves_all_fields() {
        let validation = ValidationResult {
            columns: vec![ColumnInfo {
                name: "count".into(),
                pg_oid: 20,
                pg_type_name: "int8".into(),
                is_nullable: false,
                rust_type: "i64".into(),
            }],
            param_pg_oids: vec![25, 23],
            param_is_pg_enum: vec![false, false],
        };

        let parsed = crate::parse::parse_query(
            "SELECT COUNT(*) AS count FROM users WHERE name = $name: &str AND id = $id: i32",
        )
        .expect("parse failed");

        let hash = sql_hash(&parsed.normalized_sql);
        let cached = validation_to_cached(hash, &parsed, &validation);

        assert_eq!(cached.sql_hash, hash);
        assert_eq!(cached.normalized_sql, parsed.normalized_sql);
        assert_eq!(cached.columns.len(), 1);
        assert_eq!(cached.columns[0].name, "count");
        assert_eq!(cached.columns[0].pg_oid, 20);
        assert_eq!(cached.columns[0].rust_type, "i64");
        assert_eq!(cached.param_pg_oids, vec![25, 23]);
    }

    #[test]
    fn sql_hash_deterministic() {
        let h1 = sql_hash("select id from users where id = $1");
        let h2 = sql_hash("select id from users where id = $1");
        assert_eq!(h1, h2);
    }

    #[test]
    fn sql_hash_different_for_different_sql() {
        let h1 = sql_hash("select id from users where id = $1");
        let h2 = sql_hash("select name from users where id = $1");
        assert_ne!(h1, h2);
    }

    #[test]
    fn write_and_read_cache_file() {
        let tmp = TempDir::new().expect("tempdir");
        let queries_dir = tmp.path().join("queries");
        std::fs::create_dir_all(&queries_dir).expect("mkdir");

        let cached = sample_cached_query();
        let bytes = bitcode::encode(&cached);
        let path = queries_dir.join(format!("{:016x}.bitcode", cached.sql_hash));
        std::fs::write(&path, &bytes).expect("write");

        let read_bytes = std::fs::read(&path).expect("read");
        let decoded: CachedQuery = bitcode::decode(&read_bytes).expect("decode");
        assert_eq!(decoded.sql_hash, cached.sql_hash);
        assert_eq!(decoded.normalized_sql, cached.normalized_sql);
    }

    #[test]
    fn corrupted_cache_file_returns_error() {
        let tmp = TempDir::new().expect("tempdir");
        let queries_dir = tmp.path().join("queries");
        std::fs::create_dir_all(&queries_dir).expect("mkdir");

        let path = queries_dir.join("deadbeefcafe1234.bitcode");
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(b"this is not bitcode").expect("write");

        let read_bytes = std::fs::read(&path).expect("read");
        let result = bitcode::decode::<CachedQuery>(&read_bytes);
        assert!(result.is_err(), "corrupted file should fail to decode");
    }

    #[test]
    fn empty_cache_file_returns_error() {
        let tmp = TempDir::new().expect("tempdir");
        let queries_dir = tmp.path().join("queries");
        std::fs::create_dir_all(&queries_dir).expect("mkdir");

        let path = queries_dir.join("0000000000000000.bitcode");
        std::fs::write(&path, b"").expect("write");

        let read_bytes = std::fs::read(&path).expect("read");
        let result = bitcode::decode::<CachedQuery>(&read_bytes);
        assert!(result.is_err(), "empty file should fail to decode");
    }

    #[test]
    fn is_offline_default_false() {
        // Unless SASQL_OFFLINE is set in the test environment, should be false.
        // This test is intentionally environment-dependent (like connection.rs tests).
        // We just verify the function does not panic.
        let _ = is_offline();
    }

    #[test]
    fn cached_query_with_no_columns_round_trips() {
        let cached = CachedQuery {
            sql_hash: 123,
            normalized_sql: "delete from users where id = $1".into(),
            columns: vec![],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
        };

        let bytes = bitcode::encode(&cached);
        let decoded: CachedQuery = bitcode::decode(&bytes).expect("decode");
        assert!(decoded.columns.is_empty());
        assert_eq!(decoded.param_pg_oids, vec![23]);
    }

    #[test]
    fn cached_query_with_pg_enum_round_trips() {
        let cached = CachedQuery {
            sql_hash: 456,
            normalized_sql: "select status from tickets where status = $1".into(),
            columns: vec![CachedColumn {
                name: "status".into(),
                pg_oid: 99999, // custom enum OID
                pg_type_name: "ticket_status".into(),
                is_nullable: false,
                rust_type: "::sasql_core::types::EnumString".into(),
            }],
            param_pg_oids: vec![99999],
            param_is_pg_enum: vec![true],
        };

        let bytes = bitcode::encode(&cached);
        let decoded: CachedQuery = bitcode::decode(&bytes).expect("decode");
        assert_eq!(decoded.param_is_pg_enum, vec![true]);
        assert_eq!(decoded.columns[0].pg_type_name, "ticket_status");
    }
}
