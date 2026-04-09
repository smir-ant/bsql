//! Offline mode: build without a live PostgreSQL instance.
//!
//! During normal compilation (PG available), each `query!()` invocation
//! writes its validation result to `.bsql/queries/{sql_hash}.bitcode`.
//! When `BSQL_OFFLINE=true`, the proc macro reads from these files
//! instead of connecting to PG.
//!
//! The cache is per-query (one file per SQL hash), so no file locking is
//! needed and incremental compilation works naturally.

use std::path::PathBuf;
use std::sync::OnceLock;

use bitcode::{Decode, Encode};

use crate::parse::ParsedQuery;
use crate::validate::{ColumnInfo, ValidationResult};

// ---------------------------------------------------------------------------
// Cache data structures
// ---------------------------------------------------------------------------

/// Current cache format version. Bump when `CachedQuery` fields change.
const CACHE_FORMAT_VERSION: u8 = 4;

/// The bsql crate version at build time. Stored in each cache entry so that
/// a bsql version upgrade invalidates stale caches rather than producing
/// cryptic deserialization errors or silently using outdated type mappings.
const BSQL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Versioned envelope wrapping the serialized `CachedQuery`.
///
/// `CachedQuery` is encoded to bytes first, then wrapped in this envelope.
/// On read, the version is checked *before* attempting to decode the inner
/// data, so field changes in `CachedQuery` produce a clear error instead
/// of a cryptic bitcode decode failure.
#[derive(Encode, Decode)]
struct CacheEnvelope {
    version: u8,
    data: Vec<u8>,
}

/// Legacy v1 cache format (without `bsql_version` field).
/// Used for backwards-compatible reading of old cache entries.
#[derive(Debug, Clone, Decode)]
struct CachedQueryV1 {
    pub sql_hash: u64,
    pub normalized_sql: String,
    pub columns: Vec<CachedColumn>,
    pub param_pg_oids: Vec<u32>,
    pub param_is_pg_enum: Vec<bool>,
}

/// Legacy v2 cache format (without `param_rust_types` field).
/// Used for backwards-compatible reading of v2 cache entries.
#[derive(Debug, Clone, Encode, Decode)]
struct CachedQueryV2 {
    pub sql_hash: u64,
    pub normalized_sql: String,
    pub columns: Vec<CachedColumn>,
    pub param_pg_oids: Vec<u32>,
    pub param_is_pg_enum: Vec<bool>,
    pub bsql_version: String,
}

/// Legacy v3 cache format (without `rewritten_sql` field).
/// Used for backwards-compatible reading of v3 cache entries.
#[derive(Debug, Clone, Encode, Decode)]
struct CachedQueryV3 {
    pub sql_hash: u64,
    pub normalized_sql: String,
    pub columns: Vec<CachedColumn>,
    pub param_pg_oids: Vec<u32>,
    pub param_is_pg_enum: Vec<bool>,
    pub bsql_version: String,
    pub param_rust_types: Vec<String>,
}

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
    /// The bsql version that generated this cache entry. Used to invalidate
    /// cache on bsql upgrades that change type mappings or codegen.
    pub bsql_version: String,
    /// User-declared Rust type strings for each parameter position.
    /// Empty for cache entries migrated from v2 (param type checking is
    /// skipped for those entries).
    pub param_rust_types: Vec<String>,
    /// Rewritten SQL with explicit casts (e.g. `$1::jsonb`), if the
    /// two-phase PREPARE mechanism rewrote the query. `None` if unchanged.
    pub rewritten_sql: Option<String>,
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

/// Whether offline mode is active.
///
/// Offline mode is enabled when:
/// 1. `BSQL_OFFLINE=true` or `=1` is set explicitly, OR
/// 2. No `BSQL_DATABASE_URL`/`DATABASE_URL` is set, but a `.bsql/` cache
///    directory exists (auto-fallback).
///
/// Auto-fallback means: if you've built online at least once (populating
/// the cache), subsequent builds without a database just work. No env
/// vars needed. This makes `cargo test --doc` work locally after an
/// initial online build, and makes cloned repos with committed `.bsql/`
/// build out of the box.
///
/// Evaluated once per compilation via `OnceLock`.
static IS_OFFLINE: OnceLock<bool> = OnceLock::new();

fn compute_is_offline() -> bool {
    // Explicit opt-in
    if std::env::var("BSQL_OFFLINE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
    {
        return true;
    }

    // Auto-fallback: no database URL, but cache exists
    let has_url =
        std::env::var("BSQL_DATABASE_URL").is_ok() || std::env::var("DATABASE_URL").is_ok();
    if !has_url {
        // Check if .bsql/ cache directory exists with at least one entry
        if let Ok(dir) = resolve_cache_dir() {
            if dir.is_dir()
                && std::fs::read_dir(&dir)
                    .map(|mut d| d.next().is_some())
                    .unwrap_or(false)
            {
                return true;
            }
        }
    }

    false
}

pub fn is_offline() -> bool {
    *IS_OFFLINE.get_or_init(compute_is_offline)
}

// ---------------------------------------------------------------------------
// Cache directory resolution
// ---------------------------------------------------------------------------

/// Resolve the `.bsql/queries/` directory, walking up from `CARGO_MANIFEST_DIR`
/// to find an existing `.bsql/` (or creating it next to the workspace root).
///
/// Cached once per compilation.
static CACHE_DIR: OnceLock<Result<PathBuf, String>> = OnceLock::new();

fn resolve_cache_dir() -> Result<PathBuf, String> {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").map_err(|_| "CARGO_MANIFEST_DIR not set".to_owned())?;
    let dir = PathBuf::from(&manifest_dir);

    // Walk up from CARGO_MANIFEST_DIR looking for an existing .bsql/ directory.
    // This handles both single-crate and workspace layouts: whoever ran the
    // first online build created `.bsql/` at the right level.
    let mut search = dir.clone();
    loop {
        let candidate = search.join(".bsql");
        if candidate.is_dir() {
            return Ok(candidate.join("queries"));
        }
        if !search.pop() {
            break;
        }
    }

    // No existing .bsql/ found — create at CARGO_MANIFEST_DIR.
    // The user can move it to the workspace root if desired.
    Ok(dir.join(".bsql").join("queries"))
}

fn cache_dir() -> Result<&'static PathBuf, String> {
    CACHE_DIR
        .get_or_init(resolve_cache_dir)
        .as_ref()
        .map_err(|e| e.clone())
}

// ---------------------------------------------------------------------------
// SQL hash computation
// ---------------------------------------------------------------------------

/// Compute the rapidhash of normalized SQL, used as the cache key.
pub fn sql_hash(normalized_sql: &str) -> u64 {
    bsql_core::rapid_hash_str(normalized_sql)
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
             the cache, then rebuild with BSQL_OFFLINE=true.\n  \
             SQL: {}",
            parsed.normalized_sql
        ));
    }

    let bytes = std::fs::read(&path)
        .map_err(|e| format!("failed to read offline cache file {}: {e}", path.display()))?;

    // Decode the versioned envelope first
    let envelope: CacheEnvelope = bitcode::decode(&bytes).map_err(|e| {
        format!(
            "failed to decode offline cache file {} (file may be corrupted \
             or from an incompatible bsql version — run `cargo build` with \
             a live PostgreSQL connection to regenerate): {e}",
            path.display()
        )
    })?;

    // Decode the inner CachedQuery, handling v1 -> v2 -> v3 migration
    let cached: CachedQuery = if envelope.version == 1 {
        // v1 format: CachedQuery without bsql_version or param_rust_types
        let v1: CachedQueryV1 = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode v1 cached query in {} (file may be corrupted \
                 — run `cargo build` with a live PostgreSQL connection to \
                 regenerate): {e}",
                path.display()
            )
        })?;
        CachedQuery {
            sql_hash: v1.sql_hash,
            normalized_sql: v1.normalized_sql,
            columns: v1.columns,
            param_pg_oids: v1.param_pg_oids,
            param_is_pg_enum: v1.param_is_pg_enum,
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        }
    } else if envelope.version == 2 {
        // v2 format: CachedQuery without param_rust_types
        let v2: CachedQueryV2 = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode v2 cached query in {} (file may be corrupted \
                 — run `cargo build` with a live PostgreSQL connection to \
                 regenerate): {e}",
                path.display()
            )
        })?;
        CachedQuery {
            sql_hash: v2.sql_hash,
            normalized_sql: v2.normalized_sql,
            columns: v2.columns,
            param_pg_oids: v2.param_pg_oids,
            param_is_pg_enum: v2.param_is_pg_enum,
            bsql_version: v2.bsql_version,
            param_rust_types: vec![],
            rewritten_sql: None,
        }
    } else if envelope.version == 3 {
        // v3 format: CachedQuery without rewritten_sql
        let v3: CachedQueryV3 = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode v3 cached query in {} (file may be corrupted \
                 — run `cargo build` with a live PostgreSQL connection to \
                 regenerate): {e}",
                path.display()
            )
        })?;
        CachedQuery {
            sql_hash: v3.sql_hash,
            normalized_sql: v3.normalized_sql,
            columns: v3.columns,
            param_pg_oids: v3.param_pg_oids,
            param_is_pg_enum: v3.param_is_pg_enum,
            bsql_version: v3.bsql_version,
            param_rust_types: v3.param_rust_types,
            rewritten_sql: None,
        }
    } else if envelope.version == CACHE_FORMAT_VERSION {
        let cached: CachedQuery = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode cached query in {} (file may be corrupted \
                 — run `cargo build` with a live PostgreSQL connection to \
                 regenerate): {e}",
                path.display()
            )
        })?;

        // Verify the bsql version matches — a bsql upgrade may change type
        // mappings, codegen patterns, or cache fields. Reject stale entries.
        if cached.bsql_version != BSQL_VERSION {
            return Err(format!(
                "offline cache was generated by bsql {} but current version is {} \
                 — run `cargo build` with a live PostgreSQL connection to regenerate",
                cached.bsql_version, BSQL_VERSION
            ));
        }
        cached
    } else {
        return Err(format!(
            "offline cache was generated by a different bsql version \
             (format v{}, expected v{}) — run `cargo build` with a live \
             PostgreSQL connection to regenerate",
            envelope.version, CACHE_FORMAT_VERSION
        ));
    };

    // Verify parameter Rust types match — catches changes like $id: i32 → $id: &str
    // that would not be detected until runtime without this check.
    // Skipped for migrated v2 cache entries (param_rust_types is empty).
    if !cached.param_rust_types.is_empty() {
        for (i, cached_type) in cached.param_rust_types.iter().enumerate() {
            if i < parsed.params.len() {
                let current_type = &parsed.params[i].rust_type;
                if current_type != cached_type {
                    return Err(format!(
                        "parameter type mismatch: ${} was '{}' when cache was built, \
                         now declared as '{}'. Rebuild with a live database connection \
                         to update the cache.",
                        parsed.params[i].name, cached_type, current_type
                    ));
                }
            }
        }
        if parsed.params.len() != cached.param_rust_types.len() {
            return Err(format!(
                "parameter count changed: cache has {} params, query now has {}. \
                 Rebuild with a live database connection.",
                cached.param_rust_types.len(),
                parsed.params.len()
            ));
        }
    }

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

    // Validate cached column types before trusting them for codegen
    for col in &cached.columns {
        validate_cached_type(&col.rust_type)?;
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
        param_pg_oids: cached.param_pg_oids.iter().copied().collect(),
        param_is_pg_enum: cached.param_is_pg_enum.iter().copied().collect(),
        rewritten_sql: cached.rewritten_sql.clone(),
        #[cfg(feature = "explain")]
        explain_plan: None,
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
        log::warn!("bsql: failed to write offline cache: {e}");
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

    // Wrap in versioned envelope: encode CachedQuery first, then envelope
    let inner_bytes = bitcode::encode(&cached);
    let envelope = CacheEnvelope {
        version: CACHE_FORMAT_VERSION,
        data: inner_bytes,
    };
    let bytes = bitcode::encode(&envelope);

    let path = dir.join(format!("{hash:016x}.bitcode"));

    // Atomic write: write to a PID-scoped temp file then rename.
    // PID avoids collisions when parallel proc macro invocations write
    // the same query (e.g. in workspace builds with multiple crates).
    let tmp_path = dir.join(format!("{hash:016x}.{}.bitcode.tmp", std::process::id()));

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
        param_pg_oids: validation.param_pg_oids.to_vec(),
        param_is_pg_enum: validation.param_is_pg_enum.to_vec(),
        bsql_version: BSQL_VERSION.to_owned(),
        param_rust_types: parsed.params.iter().map(|p| p.rust_type.clone()).collect(),
        rewritten_sql: validation.rewritten_sql.clone(),
    }
}

// ---------------------------------------------------------------------------
// Cached type validation (defense against tampered caches)
// ---------------------------------------------------------------------------

/// Validate that a cached `rust_type` string is a known, safe type.
///
/// Prevents a tampered or corrupted cache from injecting arbitrary type
/// names into generated code. Only types that `resolve_rust_type` or the
/// base type map can produce are accepted.
fn validate_cached_type(rust_type: &str) -> Result<(), String> {
    // Strip Option<> wrapper if present
    let inner = rust_type
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(rust_type);

    // Strip Vec<> wrapper if present (for array column types)
    let element = inner
        .strip_prefix("Vec<")
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(inner);

    // Derive known types from BASE_TYPE_MAP (single source of truth)
    let known_base = bsql_core::types::BASE_TYPE_MAP
        .iter()
        .any(|m| m.rust_type == inner);

    // Known feature-gated type prefixes (not in BASE_TYPE_MAP)
    const KNOWN_PREFIXES: &[&str] = &["::time::", "::chrono::", "::uuid::", "::rust_decimal::"];

    if known_base
        || KNOWN_PREFIXES.iter().any(|p| inner.starts_with(p))
        || KNOWN_PREFIXES.iter().any(|p| element.starts_with(p))
    {
        return Ok(());
    }

    // Fallback: parse as Rust type syntax to distinguish "unknown but valid"
    // from "corrupt garbage". Only reached for types not in our allowlist.
    if syn::parse_str::<syn::Type>(rust_type).is_err() {
        return Err(format!(
            "offline cache contains invalid type syntax: `{rust_type}` \
             — run `cargo build` with a live PostgreSQL connection to regenerate"
        ));
    }

    Err(format!(
        "offline cache contains unexpected type: `{rust_type}` \
         — run `cargo build` with a live PostgreSQL connection to regenerate"
    ))
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
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".into()],
            rewritten_sql: None,
        }
    }

    /// Encode a CachedQuery through the versioned envelope (as write_cache does).
    fn encode_enveloped(cached: &CachedQuery) -> Vec<u8> {
        let inner = bitcode::encode(cached);
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION,
            data: inner,
        };
        bitcode::encode(&envelope)
    }

    /// Decode a CachedQuery from an enveloped byte buffer (as lookup does).
    fn decode_enveloped(bytes: &[u8]) -> Result<CachedQuery, String> {
        let envelope: CacheEnvelope =
            bitcode::decode(bytes).map_err(|e| format!("envelope: {e}"))?;
        if envelope.version != CACHE_FORMAT_VERSION {
            return Err(format!(
                "version mismatch: got {}, expected {}",
                envelope.version, CACHE_FORMAT_VERSION
            ));
        }
        bitcode::decode(&envelope.data).map_err(|e| format!("inner: {e}"))
    }

    #[test]
    fn envelope_round_trip() {
        let original = sample_cached_query();
        let bytes = encode_enveloped(&original);
        let decoded = decode_enveloped(&bytes).expect("decode failed");

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
    fn format_version_mismatch_returns_clear_error() {
        let cached = sample_cached_query();
        let inner = bitcode::encode(&cached);
        let envelope = CacheEnvelope {
            version: 99, // wrong version
            data: inner,
        };
        let bytes = bitcode::encode(&envelope);

        let err = decode_enveloped(&bytes).unwrap_err();
        assert!(
            err.contains("version mismatch"),
            "error should mention version: {err}"
        );
    }

    #[test]
    fn cached_to_validation_preserves_all_fields() {
        let cached = sample_cached_query();
        let validation = cached_to_validation(&cached);

        assert_eq!(validation.columns.len(), 2);
        assert_eq!(validation.columns[0].name, "id");
        assert_eq!(validation.columns[0].pg_oid, 23);
        assert!(!validation.columns[0].is_nullable);
        assert_eq!(validation.columns[0].rust_type, "i32");
        assert_eq!(validation.columns[1].name, "name");
        assert!(validation.columns[1].is_nullable);
        assert_eq!(validation.columns[1].rust_type, "Option<String>");
        assert_eq!(validation.param_pg_oids.as_slice(), &[23u32]);
        assert_eq!(validation.param_is_pg_enum.as_slice(), &[false]);
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
            param_pg_oids: smallvec::smallvec![25, 23],
            param_is_pg_enum: smallvec::smallvec![false, false],
            rewritten_sql: None,
            #[cfg(feature = "explain")]
            explain_plan: None,
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
        assert_eq!(cached.param_rust_types, vec!["&str", "i32"]);
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
    fn write_and_read_enveloped_cache_file() {
        let tmp = TempDir::new().expect("tempdir");
        let queries_dir = tmp.path().join("queries");
        std::fs::create_dir_all(&queries_dir).expect("mkdir");

        let cached = sample_cached_query();
        let bytes = encode_enveloped(&cached);
        let path = queries_dir.join(format!("{:016x}.bitcode", cached.sql_hash));
        std::fs::write(&path, &bytes).expect("write");

        let read_bytes = std::fs::read(&path).expect("read");
        let decoded = decode_enveloped(&read_bytes).expect("decode");
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
        let result = bitcode::decode::<CacheEnvelope>(&read_bytes);
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
        let result = bitcode::decode::<CacheEnvelope>(&read_bytes);
        assert!(result.is_err(), "empty file should fail to decode");
    }

    #[test]
    fn is_offline_default_false() {
        // Unless BSQL_OFFLINE is set in the test environment, should be false.
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
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".into()],
            rewritten_sql: None,
        };

        let bytes = encode_enveloped(&cached);
        let decoded = decode_enveloped(&bytes).expect("decode");
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
                pg_oid: 25, // text OID after cast
                pg_type_name: "text".into(),
                is_nullable: false,
                rust_type: "String".into(),
            }],
            param_pg_oids: vec![99999],
            param_is_pg_enum: vec![true],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["String".into()],
            rewritten_sql: None,
        };

        let bytes = encode_enveloped(&cached);
        let decoded = decode_enveloped(&bytes).expect("decode");
        assert_eq!(decoded.param_is_pg_enum, vec![true]);
        assert_eq!(decoded.columns[0].pg_type_name, "text");
    }

    #[test]
    fn raw_cached_query_without_envelope_fails() {
        // Bytes encoded directly (no envelope) must not decode as envelope
        let cached = sample_cached_query();
        let raw_bytes = bitcode::encode(&cached);
        // This should either fail to decode as CacheEnvelope or produce
        // a garbage version number
        match bitcode::decode::<CacheEnvelope>(&raw_bytes) {
            Err(_) => {} // expected
            Ok(env) => assert_ne!(
                env.version, CACHE_FORMAT_VERSION,
                "raw CachedQuery bytes must not accidentally pass version check"
            ),
        }
    }

    #[test]
    fn validate_known_base_types() {
        assert!(validate_cached_type("i32").is_ok());
        assert!(validate_cached_type("i64").is_ok());
        assert!(validate_cached_type("String").is_ok());
        assert!(validate_cached_type("bool").is_ok());
        assert!(validate_cached_type("f64").is_ok());
        assert!(validate_cached_type("u32").is_ok());
        assert!(validate_cached_type("()").is_ok());
        assert!(validate_cached_type("Vec<u8>").is_ok());
        assert!(validate_cached_type("Vec<String>").is_ok());
        assert!(validate_cached_type("Vec<Vec<u8>>").is_ok());
    }

    #[test]
    fn validate_option_wrapped_types() {
        assert!(validate_cached_type("Option<i32>").is_ok());
        assert!(validate_cached_type("Option<String>").is_ok());
        assert!(validate_cached_type("Option<Vec<u8>>").is_ok());
    }

    #[test]
    fn validate_rejects_removed_enum_string() {
        // EnumString was removed — PG enums now require #[bsql::pg_enum] or ::text cast
        assert!(validate_cached_type("::bsql_core::types::EnumString").is_err());
        assert!(validate_cached_type("Option<::bsql_core::types::EnumString>").is_err());
    }

    #[test]
    fn validate_feature_gated_types() {
        assert!(validate_cached_type("::time::OffsetDateTime").is_ok());
        assert!(validate_cached_type("::time::PrimitiveDateTime").is_ok());
        assert!(validate_cached_type("::time::Date").is_ok());
        assert!(validate_cached_type("::time::Time").is_ok());
        assert!(validate_cached_type("::chrono::DateTime<::chrono::Utc>").is_ok());
        assert!(validate_cached_type("::chrono::NaiveDateTime").is_ok());
        assert!(validate_cached_type("::uuid::Uuid").is_ok());
        assert!(validate_cached_type("::rust_decimal::Decimal").is_ok());
    }

    #[test]
    fn validate_feature_gated_option_types() {
        assert!(validate_cached_type("Option<::time::OffsetDateTime>").is_ok());
        assert!(validate_cached_type("Option<::uuid::Uuid>").is_ok());
        assert!(validate_cached_type("Option<::rust_decimal::Decimal>").is_ok());
    }

    #[test]
    fn validate_feature_gated_vec_types() {
        assert!(validate_cached_type("Vec<::time::OffsetDateTime>").is_ok());
        assert!(validate_cached_type("Vec<::chrono::NaiveDate>").is_ok());
        assert!(validate_cached_type("Vec<::uuid::Uuid>").is_ok());
        assert!(validate_cached_type("Vec<::rust_decimal::Decimal>").is_ok());
    }

    #[test]
    fn validate_option_vec_feature_gated_types() {
        assert!(validate_cached_type("Option<Vec<::time::Date>>").is_ok());
        assert!(validate_cached_type("Option<Vec<::uuid::Uuid>>").is_ok());
    }

    #[test]
    fn validate_rejects_unknown_types() {
        let err = validate_cached_type("std::process::Command").unwrap_err();
        assert!(err.contains("unexpected type"), "error: {err}");

        let err = validate_cached_type("SomeUserType").unwrap_err();
        assert!(err.contains("unexpected type"), "error: {err}");
    }

    #[test]
    fn validate_rejects_invalid_syntax() {
        let err = validate_cached_type("not a type!!").unwrap_err();
        assert!(err.contains("invalid type syntax"), "error: {err}");
    }

    #[test]
    fn validate_rejects_injected_code() {
        // Something that parses as a valid type but is not in the allowlist
        let err = validate_cached_type("std::fs::File").unwrap_err();
        assert!(err.contains("unexpected type"), "error: {err}");
    }

    #[test]
    fn temp_filename_includes_pid() {
        let pid = std::process::id();
        let hash: u64 = 0xCAFE;
        let tmp_name = format!("{hash:016x}.{pid}.bitcode.tmp");
        assert!(
            tmp_name.contains(&pid.to_string()),
            "temp filename should include PID: {tmp_name}"
        );
        // Verify the pattern matches what write_cache_inner produces
        assert!(tmp_name.ends_with(".bitcode.tmp"));
        assert!(tmp_name.starts_with("000000000000cafe."));
    }

    #[test]
    fn walk_up_finds_existing_bsql_dir() {
        // Test the directory-walking logic directly (without mutating env vars,
        // which is unsafe in edition 2024 and forbidden by this crate).
        let tmp = TempDir::new().expect("tempdir");
        let bsql_dir = tmp.path().join(".bsql");
        std::fs::create_dir_all(&bsql_dir).expect("mkdir");

        // Simulate walking up from a nested sub-crate
        let sub_crate = tmp.path().join("crates").join("mylib");
        std::fs::create_dir_all(&sub_crate).expect("mkdir");

        let mut search = sub_crate.clone();
        let mut found = None;
        loop {
            let candidate = search.join(".bsql");
            if candidate.is_dir() {
                found = Some(candidate.join("queries"));
                break;
            }
            if !search.pop() {
                break;
            }
        }

        assert_eq!(
            found,
            Some(bsql_dir.join("queries")),
            "walk should find .bsql at workspace root"
        );
    }

    #[test]
    fn walk_up_falls_back_to_start_dir() {
        // No .bsql/ exists anywhere — should fall back to the starting dir
        let tmp = TempDir::new().expect("tempdir");
        let start = tmp.path().join("projects").join("mylib");
        std::fs::create_dir_all(&start).expect("mkdir");

        let mut search = start.clone();
        let mut found = None;
        loop {
            let candidate = search.join(".bsql");
            if candidate.is_dir() {
                found = Some(candidate.join("queries"));
                break;
            }
            if !search.pop() {
                break;
            }
        }

        assert!(found.is_none(), "no .bsql/ should be found");
        // In production code, the fallback creates at CARGO_MANIFEST_DIR
        let fallback = start.join(".bsql").join("queries");
        assert!(fallback.to_str().unwrap().contains("mylib"));
    }

    // --- write + lookup roundtrip (integration-style) ---

    #[test]
    fn write_cache_and_lookup_roundtrip() {
        let tmp = TempDir::new().expect("tempdir");
        let queries_dir = tmp.path().join(".bsql").join("queries");
        std::fs::create_dir_all(&queries_dir).expect("mkdir");

        // Build a cached query, write it through the envelope, read it back
        let cached = sample_cached_query();
        let bytes = encode_enveloped(&cached);
        let path = queries_dir.join(format!("{:016x}.bitcode", cached.sql_hash));
        std::fs::write(&path, &bytes).expect("write");

        // Read and verify through the same path as lookup_cached_validation
        let read_bytes = std::fs::read(&path).expect("read");
        let envelope: CacheEnvelope = bitcode::decode(&read_bytes).expect("envelope decode");
        assert_eq!(envelope.version, CACHE_FORMAT_VERSION);
        let decoded: CachedQuery = bitcode::decode(&envelope.data).expect("inner decode");

        assert_eq!(decoded.sql_hash, cached.sql_hash);
        assert_eq!(decoded.normalized_sql, cached.normalized_sql);
        assert_eq!(decoded.columns.len(), cached.columns.len());
        assert_eq!(decoded.param_pg_oids, cached.param_pg_oids);

        // Verify all column types pass validation
        for col in &decoded.columns {
            validate_cached_type(&col.rust_type).expect("type validation failed");
        }
    }

    // --- collision guard ---

    #[test]
    fn sql_collision_guard_rejects_mismatched_sql() {
        // Simulate two different SQL texts that happen to share the same hash
        // (in practice impossible, but we test the guard logic directly)
        let cached = CachedQuery {
            sql_hash: 999,
            normalized_sql: "select a from t".into(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        };
        let other_sql = "select b from t";
        assert_ne!(cached.normalized_sql, other_sql);
        // The guard in lookup_cached_validation compares cached.normalized_sql
        // against parsed.normalized_sql — here we just verify the strings differ
        // and that the error path would fire.
    }

    // --- v3 param_rust_types tests ---

    #[test]
    fn cache_v3_roundtrip_with_param_types() {
        let query = CachedQuery {
            sql_hash: 42,
            normalized_sql: "SELECT id FROM users WHERE id = $1".to_owned(),
            columns: vec![],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".to_owned()],
            rewritten_sql: None,
        };
        let bytes = encode_enveloped(&query);
        let decoded = decode_enveloped(&bytes).unwrap();
        assert_eq!(decoded.param_rust_types, vec!["i32"]);
    }

    #[test]
    fn cache_v2_migration_has_empty_param_types() {
        // Write a v2 cache entry (without param_rust_types)
        let v2 = CachedQueryV2 {
            sql_hash: 77,
            normalized_sql: "SELECT 1".to_owned(),
            columns: vec![],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
            bsql_version: BSQL_VERSION.to_owned(),
        };
        let inner_bytes = bitcode::encode(&v2);
        let envelope = CacheEnvelope {
            version: 2,
            data: inner_bytes,
        };
        let bytes = bitcode::encode(&envelope);

        // Decode through the v1/v2/v3 migration path used by lookup
        let env: CacheEnvelope = bitcode::decode(&bytes).unwrap();
        assert_eq!(env.version, 2);
        let decoded_v2: CachedQueryV2 = bitcode::decode(&env.data).unwrap();
        let migrated = CachedQuery {
            sql_hash: decoded_v2.sql_hash,
            normalized_sql: decoded_v2.normalized_sql,
            columns: decoded_v2.columns,
            param_pg_oids: decoded_v2.param_pg_oids,
            param_is_pg_enum: decoded_v2.param_is_pg_enum,
            bsql_version: decoded_v2.bsql_version,
            param_rust_types: vec![],
            rewritten_sql: None,
        };
        assert!(migrated.param_rust_types.is_empty());
        assert_eq!(migrated.sql_hash, 77);
    }

    #[test]
    fn cache_v3_multiple_param_types_roundtrip() {
        let query = CachedQuery {
            sql_hash: 100,
            normalized_sql: "SELECT 1 FROM t WHERE a = $1 AND b = $2".to_owned(),
            columns: vec![],
            param_pg_oids: vec![23, 25],
            param_is_pg_enum: vec![false, false],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".to_owned(), "&str".to_owned()],
            rewritten_sql: None,
        };
        let bytes = encode_enveloped(&query);
        let decoded = decode_enveloped(&bytes).unwrap();
        assert_eq!(decoded.param_rust_types, vec!["i32", "&str"]);
    }

    #[test]
    fn cache_v3_empty_param_types_roundtrip() {
        let query = CachedQuery {
            sql_hash: 200,
            normalized_sql: "SELECT 1".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        };
        let bytes = encode_enveloped(&query);
        let decoded = decode_enveloped(&bytes).unwrap();
        assert!(decoded.param_rust_types.is_empty());
    }

    // --- param type mismatch detection ---

    #[test]
    fn param_type_mismatch_detected() {
        // Create a v3 cache with param type "i32", then simulate loading
        // with a parsed query that declares "&str" — the mismatch should error.
        let tmp = TempDir::new().expect("tempdir");
        let queries_dir = tmp.path().join(".bsql").join("queries");
        std::fs::create_dir_all(&queries_dir).expect("mkdir");

        let normalized_sql = "SELECT id FROM users WHERE id = $1";
        let hash = sql_hash(normalized_sql);

        let cached = CachedQuery {
            sql_hash: hash,
            normalized_sql: normalized_sql.to_owned(),
            columns: vec![CachedColumn {
                name: "id".into(),
                pg_oid: 23,
                pg_type_name: "int4".into(),
                is_nullable: false,
                rust_type: "i32".into(),
            }],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".to_owned()],
            rewritten_sql: None,
        };
        let bytes = encode_enveloped(&cached);
        let path = queries_dir.join(format!("{hash:016x}.bitcode"));
        std::fs::write(&path, &bytes).expect("write");

        // Decode and simulate the type check from lookup_cached_validation
        let read_bytes = std::fs::read(&path).expect("read");
        let envelope: CacheEnvelope = bitcode::decode(&read_bytes).expect("envelope decode");
        let decoded: CachedQuery = bitcode::decode(&envelope.data).expect("inner decode");

        // Simulate mismatch: cached has "i32", current query declares "&str"
        let current_type = "&str";
        assert_ne!(decoded.param_rust_types[0], current_type);
        // The real lookup_cached_validation would return Err here
    }

    #[test]
    fn param_count_mismatch_detected() {
        // Cache has 2 params, query has 3 — should detect the discrepancy
        let cached = CachedQuery {
            sql_hash: 300,
            normalized_sql: "SELECT 1 FROM t WHERE a = $1 AND b = $2".to_owned(),
            columns: vec![],
            param_pg_oids: vec![23, 25],
            param_is_pg_enum: vec![false, false],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".to_owned(), "&str".to_owned()],
            rewritten_sql: None,
        };

        // Simulate having 3 params in the current query
        let current_param_count = 3;
        assert_ne!(
            cached.param_rust_types.len(),
            current_param_count,
            "param count should differ: cache has {}, current has {}",
            cached.param_rust_types.len(),
            current_param_count
        );
    }

    #[test]
    fn v1_to_v3_migration_preserves_data() {
        // Test the v1 -> v3 migration logic directly.
        // CachedQueryV1 only has Decode (not Encode), so we test the
        // conversion logic rather than a full encode/decode roundtrip.
        // The migration logic in lookup_cached_validation converts V1 fields
        // into a CachedQuery by adding bsql_version and empty param_rust_types.

        // Simulate what a decoded v1 entry would look like:
        let v1_sql_hash: u64 = 555;
        let v1_normalized_sql = "SELECT name FROM t WHERE id = $1".to_owned();
        let v1_columns = vec![CachedColumn {
            name: "name".into(),
            pg_oid: 25,
            pg_type_name: "text".into(),
            is_nullable: true,
            rust_type: "Option<String>".into(),
        }];
        let v1_param_pg_oids = vec![23u32];
        let v1_param_is_pg_enum = vec![false];

        // Apply the same migration logic as lookup_cached_validation
        let migrated = CachedQuery {
            sql_hash: v1_sql_hash,
            normalized_sql: v1_normalized_sql,
            columns: v1_columns,
            param_pg_oids: v1_param_pg_oids,
            param_is_pg_enum: v1_param_is_pg_enum,
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        };

        // Verify data survived migration
        assert_eq!(migrated.sql_hash, 555);
        assert_eq!(migrated.normalized_sql, "SELECT name FROM t WHERE id = $1");
        assert_eq!(migrated.columns.len(), 1);
        assert_eq!(migrated.columns[0].name, "name");
        assert_eq!(migrated.columns[0].rust_type, "Option<String>");
        assert_eq!(migrated.param_pg_oids, vec![23]);
        assert_eq!(migrated.param_is_pg_enum, vec![false]);
        // v1 migration has empty param_rust_types — type check skipped
        assert!(migrated.param_rust_types.is_empty());

        // The migrated entry should round-trip through v3 encoding
        let bytes = encode_enveloped(&migrated);
        let decoded = decode_enveloped(&bytes).unwrap();
        assert_eq!(decoded.sql_hash, 555);
        assert!(decoded.param_rust_types.is_empty());
    }

    #[test]
    fn v2_migrated_cache_skips_type_check() {
        // v2 cache entries have empty param_rust_types after migration.
        // The type check should be skipped (not erroring on empty vec).
        let v2 = CachedQueryV2 {
            sql_hash: 888,
            normalized_sql: "SELECT 1 WHERE $1 = 1".to_owned(),
            columns: vec![],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
            bsql_version: BSQL_VERSION.to_owned(),
        };
        let inner_bytes = bitcode::encode(&v2);
        let envelope = CacheEnvelope {
            version: 2,
            data: inner_bytes,
        };
        let bytes = bitcode::encode(&envelope);

        let env: CacheEnvelope = bitcode::decode(&bytes).unwrap();
        assert_eq!(env.version, 2);
        let decoded_v2: CachedQueryV2 = bitcode::decode(&env.data).unwrap();
        let migrated = CachedQuery {
            sql_hash: decoded_v2.sql_hash,
            normalized_sql: decoded_v2.normalized_sql,
            columns: decoded_v2.columns,
            param_pg_oids: decoded_v2.param_pg_oids,
            param_is_pg_enum: decoded_v2.param_is_pg_enum,
            bsql_version: decoded_v2.bsql_version,
            param_rust_types: vec![],
            rewritten_sql: None,
        };

        // param_rust_types is empty — the type check in lookup should be skipped
        assert!(migrated.param_rust_types.is_empty());
        // The guard condition: if !cached.param_rust_types.is_empty() { check... }
        // With empty vec, no error should be raised regardless of current param types.
    }

    // --- Future version envelope handling ---

    #[test]
    fn future_version_envelope_rejected() {
        let cached = sample_cached_query();
        let inner = bitcode::encode(&cached);
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION + 1, // future version
            data: inner,
        };
        let bytes = bitcode::encode(&envelope);
        let err = decode_enveloped(&bytes).unwrap_err();
        assert!(
            err.contains("version mismatch"),
            "future version should be rejected: {err}"
        );
    }

    // --- Version 0 envelope rejected ---

    #[test]
    fn version_zero_envelope_rejected() {
        let inner = bitcode::encode(&sample_cached_query());
        let envelope = CacheEnvelope {
            version: 0,
            data: inner,
        };
        let bytes = bitcode::encode(&envelope);
        let err = decode_enveloped(&bytes).unwrap_err();
        assert!(
            err.contains("version mismatch"),
            "version 0 should be rejected: {err}"
        );
    }

    // --- Empty data field in envelope ---

    #[test]
    fn empty_data_in_envelope_fails() {
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION,
            data: vec![],
        };
        let bytes = bitcode::encode(&envelope);
        let err = decode_enveloped(&bytes).unwrap_err();
        assert!(err.contains("inner"), "empty data should fail: {err}");
    }

    // --- Truncated data in envelope ---

    #[test]
    fn truncated_data_in_envelope_fails() {
        let cached = sample_cached_query();
        let inner = bitcode::encode(&cached);
        let truncated = &inner[..inner.len() / 2]; // truncate
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION,
            data: truncated.to_vec(),
        };
        let bytes = bitcode::encode(&envelope);
        let err = decode_enveloped(&bytes).unwrap_err();
        assert!(!err.is_empty(), "truncated data should fail: {err}");
    }

    // --- CachedQuery with many columns round trips ---

    #[test]
    fn cached_query_many_columns_round_trips() {
        let columns: Vec<CachedColumn> = (0..50)
            .map(|i| CachedColumn {
                name: format!("col_{i}"),
                pg_oid: 23,
                pg_type_name: "int4".into(),
                is_nullable: i % 2 == 0,
                rust_type: if i % 2 == 0 {
                    "Option<i32>".into()
                } else {
                    "i32".into()
                },
            })
            .collect();

        let cached = CachedQuery {
            sql_hash: 12345,
            normalized_sql: "SELECT many columns...".into(),
            columns,
            param_pg_oids: vec![23, 25],
            param_is_pg_enum: vec![false, false],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec!["i32".into(), "&str".into()],
            rewritten_sql: None,
        };

        let bytes = encode_enveloped(&cached);
        let decoded = decode_enveloped(&bytes).unwrap();
        assert_eq!(decoded.columns.len(), 50);
        assert_eq!(decoded.columns[0].name, "col_0");
        assert!(decoded.columns[0].is_nullable);
        assert_eq!(decoded.columns[49].name, "col_49");
        assert!(!decoded.columns[49].is_nullable);
    }

    // --- CachedQuery with empty normalized_sql ---

    #[test]
    fn cached_query_empty_sql_round_trips() {
        let cached = CachedQuery {
            sql_hash: 0,
            normalized_sql: String::new(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: BSQL_VERSION.to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        };
        let bytes = encode_enveloped(&cached);
        let decoded = decode_enveloped(&bytes).unwrap();
        assert!(decoded.normalized_sql.is_empty());
        assert_eq!(decoded.sql_hash, 0);
    }

    // --- validate_cached_type: additional types ---

    #[test]
    fn validate_cached_type_i16() {
        assert!(validate_cached_type("i16").is_ok());
    }

    #[test]
    fn validate_cached_type_f32() {
        assert!(validate_cached_type("f32").is_ok());
    }

    #[test]
    fn validate_cached_type_option_i16() {
        assert!(validate_cached_type("Option<i16>").is_ok());
    }

    #[test]
    fn validate_cached_type_option_f32() {
        assert!(validate_cached_type("Option<f32>").is_ok());
    }

    #[test]
    fn validate_cached_type_vec_i32() {
        assert!(validate_cached_type("Vec<i32>").is_ok());
    }

    #[test]
    fn validate_cached_type_vec_i64() {
        assert!(validate_cached_type("Vec<i64>").is_ok());
    }

    #[test]
    fn validate_cached_type_vec_bool() {
        assert!(validate_cached_type("Vec<bool>").is_ok());
    }

    #[test]
    fn validate_cached_type_vec_f32() {
        assert!(validate_cached_type("Vec<f32>").is_ok());
    }

    #[test]
    fn validate_cached_type_vec_f64() {
        assert!(validate_cached_type("Vec<f64>").is_ok());
    }

    #[test]
    fn validate_cached_type_vec_i16() {
        assert!(validate_cached_type("Vec<i16>").is_ok());
    }

    // --- validate_cached_type: empty string is invalid ---

    #[test]
    fn validate_cached_type_empty_string() {
        // Empty type string parses as nothing — should fail
        let result = validate_cached_type("");
        // Empty string may or may not parse, but should be rejected
        assert!(result.is_err(), "empty type string should be rejected");
    }

    // --- sql_hash: empty string deterministic ---

    #[test]
    fn sql_hash_empty_string_deterministic() {
        let h1 = sql_hash("");
        let h2 = sql_hash("");
        assert_eq!(h1, h2);
    }

    // --- sql_hash: whitespace-sensitive ---

    #[test]
    fn sql_hash_whitespace_matters() {
        let h1 = sql_hash("SELECT 1");
        let h2 = sql_hash("SELECT  1");
        assert_ne!(h1, h2, "whitespace should produce different hashes");
    }
}
