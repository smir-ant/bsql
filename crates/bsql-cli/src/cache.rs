use bitcode::{Decode, Encode};
use std::path::Path;

/// Current cache format version. Must match `CACHE_FORMAT_VERSION` in
/// `bsql-macros/src/offline.rs`.
const CACHE_FORMAT_VERSION: u8 = 4;

/// Versioned envelope wrapping the serialized [`CachedQuery`].
///
/// Decoded first so we can check the version before attempting to
/// decode the inner payload.
#[derive(Encode, Decode)]
struct CacheEnvelope {
    version: u8,
    data: Vec<u8>,
}

/// Legacy v1 cache format (without `bsql_version` field).
#[derive(Debug, Clone, Encode, Decode)]
struct CachedQueryV1 {
    pub sql_hash: u64,
    pub normalized_sql: String,
    pub columns: Vec<CachedColumn>,
    pub param_pg_oids: Vec<u32>,
    pub param_is_pg_enum: Vec<bool>,
}

/// Legacy v2 cache format (without `param_rust_types` field).
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
///
/// Field names and order MUST match `bsql-macros/src/offline.rs` exactly —
/// bitcode serialization is positional.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CachedQuery {
    pub sql_hash: u64,
    pub normalized_sql: String,
    pub columns: Vec<CachedColumn>,
    pub param_pg_oids: Vec<u32>,
    pub param_is_pg_enum: Vec<bool>,
    pub bsql_version: String,
    pub param_rust_types: Vec<String>,
    /// SQL with auto-casts added (e.g. `$1::jsonb`). None if no rewrite needed.
    pub rewritten_sql: Option<String>,
}

/// A single result column, cached.
///
/// Field names and order MUST match `bsql-macros/src/offline.rs` exactly.
#[derive(Debug, Clone, Encode, Decode)]
pub struct CachedColumn {
    pub name: String,
    pub pg_oid: u32,
    pub pg_type_name: String,
    pub is_nullable: bool,
    pub rust_type: String,
}

/// Read all `.bitcode` files from a cache directory.
///
/// Returns an error if the directory cannot be read. Individual corrupt
/// files are skipped with a warning on stderr.
pub fn read_cache_dir(path: &Path) -> Result<Vec<CachedQuery>, String> {
    let entries = std::fs::read_dir(path)
        .map_err(|e| format!("cannot read directory {}: {e}", path.display()))?;

    let mut queries = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("directory iteration error: {e}"))?;
        let file_path = entry.path();
        if file_path.extension().is_some_and(|ext| ext == "bitcode") {
            match read_cache_file(&file_path) {
                Ok(q) => queries.push(q),
                Err(e) => {
                    eprintln!("  warning: skipping {}: {e}", file_path.display());
                }
            }
        }
    }
    Ok(queries)
}

/// Read and decode a single `.bitcode` cache file.
pub fn read_cache_file(path: &Path) -> Result<CachedQuery, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;

    let envelope: CacheEnvelope = bitcode::decode(&bytes).map_err(|e| {
        format!(
            "failed to decode envelope in {} (file may be corrupted): {e}",
            path.display()
        )
    })?;

    if envelope.version == 1 {
        let v1: CachedQueryV1 = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode v1 cached query in {}: {e}",
                path.display()
            )
        })?;
        Ok(CachedQuery {
            sql_hash: v1.sql_hash,
            normalized_sql: v1.normalized_sql,
            columns: v1.columns,
            param_pg_oids: v1.param_pg_oids,
            param_is_pg_enum: v1.param_is_pg_enum,
            bsql_version: String::new(),
            param_rust_types: vec![],
            rewritten_sql: None,
        })
    } else if envelope.version == 2 {
        let v2: CachedQueryV2 = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode v2 cached query in {}: {e}",
                path.display()
            )
        })?;
        Ok(CachedQuery {
            sql_hash: v2.sql_hash,
            normalized_sql: v2.normalized_sql,
            columns: v2.columns,
            param_pg_oids: v2.param_pg_oids,
            param_is_pg_enum: v2.param_is_pg_enum,
            bsql_version: v2.bsql_version,
            param_rust_types: vec![],
            rewritten_sql: None,
        })
    } else if envelope.version == 3 {
        let v3: CachedQueryV3 = bitcode::decode(&envelope.data).map_err(|e| {
            format!(
                "failed to decode v3 cached query in {}: {e}",
                path.display()
            )
        })?;
        Ok(CachedQuery {
            sql_hash: v3.sql_hash,
            normalized_sql: v3.normalized_sql,
            columns: v3.columns,
            param_pg_oids: v3.param_pg_oids,
            param_is_pg_enum: v3.param_is_pg_enum,
            bsql_version: v3.bsql_version,
            param_rust_types: v3.param_rust_types,
            rewritten_sql: None,
        })
    } else if envelope.version == CACHE_FORMAT_VERSION {
        bitcode::decode(&envelope.data)
            .map_err(|e| format!("failed to decode cached query in {}: {e}", path.display()))
    } else {
        Err(format!(
            "unsupported cache format version {} in {} (expected {})",
            envelope.version,
            path.display(),
            CACHE_FORMAT_VERSION
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_nonexistent_dir() {
        let result = read_cache_dir(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot read directory"));
    }

    #[test]
    fn read_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = read_cache_dir(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn read_corrupted_file_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("deadbeef.bitcode"), b"not valid bitcode").unwrap();
        // Corrupt files are skipped, not fatal
        let result = read_cache_dir(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn read_non_bitcode_files_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("readme.txt"), b"hello").unwrap();
        std::fs::write(dir.path().join("data.json"), b"{}").unwrap();
        let result = read_cache_dir(dir.path()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn read_file_not_found() {
        let result = read_cache_file(Path::new("/nonexistent/file.bitcode"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot read"));
    }

    #[test]
    fn read_file_corrupt_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.bitcode");
        std::fs::write(&path, b"\x00\x01\x02\x03").unwrap();
        let result = read_cache_file(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to decode envelope"));
    }

    #[test]
    fn read_file_unsupported_version() {
        // Encode an envelope with version 99
        let envelope = CacheEnvelope {
            version: 99,
            data: vec![],
        };
        let bytes = bitcode::encode(&envelope);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.bitcode");
        std::fs::write(&path, &bytes).unwrap();
        let result = read_cache_file(&path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("unsupported cache format version 99"));
    }

    #[test]
    fn read_file_valid_v3_roundtrip() {
        let query = CachedQuery {
            sql_hash: 42,
            normalized_sql: "SELECT 1".to_owned(),
            columns: vec![CachedColumn {
                name: "col".to_owned(),
                pg_oid: 23,
                pg_type_name: "int4".to_owned(),
                is_nullable: false,
                rust_type: "i32".to_owned(),
            }],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.1".to_owned(),
            param_rust_types: vec!["i32".to_owned()],
            rewritten_sql: None,
        };

        let inner_bytes = bitcode::encode(&query);
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION,
            data: inner_bytes,
        };
        let bytes = bitcode::encode(&envelope);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("002a000000000000.bitcode");
        std::fs::write(&path, &bytes).unwrap();

        let decoded = read_cache_file(&path).unwrap();
        assert_eq!(decoded.sql_hash, 42);
        assert_eq!(decoded.normalized_sql, "SELECT 1");
        assert_eq!(decoded.columns.len(), 1);
        assert_eq!(decoded.columns[0].name, "col");
        assert_eq!(decoded.columns[0].pg_oid, 23);
        assert_eq!(decoded.columns[0].pg_type_name, "int4");
        assert!(!decoded.columns[0].is_nullable);
        assert_eq!(decoded.columns[0].rust_type, "i32");
        assert!(decoded.param_pg_oids.is_empty());
        assert!(decoded.param_is_pg_enum.is_empty());
        assert_eq!(decoded.bsql_version, "0.20.1");
        assert_eq!(decoded.param_rust_types, vec!["i32"]);
    }

    #[test]
    fn read_file_valid_v2_migration() {
        let v2 = CachedQueryV2 {
            sql_hash: 42,
            normalized_sql: "SELECT 1".to_owned(),
            columns: vec![CachedColumn {
                name: "col".to_owned(),
                pg_oid: 23,
                pg_type_name: "int4".to_owned(),
                is_nullable: false,
                rust_type: "i32".to_owned(),
            }],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.0".to_owned(),
        };

        let inner_bytes = bitcode::encode(&v2);
        let envelope = CacheEnvelope {
            version: 2,
            data: inner_bytes,
        };
        let bytes = bitcode::encode(&envelope);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2_migration.bitcode");
        std::fs::write(&path, &bytes).unwrap();

        let decoded = read_cache_file(&path).unwrap();
        assert_eq!(decoded.sql_hash, 42);
        assert_eq!(decoded.bsql_version, "0.20.0");
        assert!(decoded.param_rust_types.is_empty());
    }

    #[test]
    fn read_file_valid_v1_roundtrip() {
        let v1 = CachedQueryV1 {
            sql_hash: 99,
            normalized_sql: "SELECT 2".to_owned(),
            columns: vec![],
            param_pg_oids: vec![23],
            param_is_pg_enum: vec![false],
        };

        let inner_bytes = bitcode::encode(&v1);
        let envelope = CacheEnvelope {
            version: 1,
            data: inner_bytes,
        };
        let bytes = bitcode::encode(&envelope);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.bitcode");
        std::fs::write(&path, &bytes).unwrap();

        let decoded = read_cache_file(&path).unwrap();
        assert_eq!(decoded.sql_hash, 99);
        assert_eq!(decoded.normalized_sql, "SELECT 2");
        assert!(decoded.columns.is_empty());
        assert_eq!(decoded.param_pg_oids, vec![23]);
        assert_eq!(decoded.param_is_pg_enum, vec![false]);
        assert_eq!(decoded.bsql_version, ""); // v1 has no version
        assert!(decoded.param_rust_types.is_empty()); // v1 has no param types
    }

    #[test]
    fn read_file_v2_corrupt_inner_data() {
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION,
            data: vec![0xFF, 0xFE, 0xFD],
        };
        let bytes = bitcode::encode(&envelope);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_inner.bitcode");
        std::fs::write(&path, &bytes).unwrap();
        let result = read_cache_file(&path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("failed to decode cached query"));
    }

    #[test]
    fn read_file_v1_corrupt_inner_data() {
        let envelope = CacheEnvelope {
            version: 1,
            data: vec![0xFF, 0xFE, 0xFD],
        };
        let bytes = bitcode::encode(&envelope);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_v1.bitcode");
        std::fs::write(&path, &bytes).unwrap();
        let result = read_cache_file(&path);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("failed to decode v1 cached query"));
    }

    #[test]
    fn read_dir_mixed_valid_and_corrupt() {
        // One valid file, one corrupt file — should get one query back
        let dir = tempfile::tempdir().unwrap();

        // Valid file
        let query = CachedQuery {
            sql_hash: 1,
            normalized_sql: "SELECT 1".to_owned(),
            columns: vec![],
            param_pg_oids: vec![],
            param_is_pg_enum: vec![],
            bsql_version: "0.20.1".to_owned(),
            param_rust_types: vec![],
            rewritten_sql: None,
        };
        let inner_bytes = bitcode::encode(&query);
        let envelope = CacheEnvelope {
            version: CACHE_FORMAT_VERSION,
            data: inner_bytes,
        };
        std::fs::write(dir.path().join("valid.bitcode"), bitcode::encode(&envelope)).unwrap();

        // Corrupt file
        std::fs::write(dir.path().join("corrupt.bitcode"), b"garbage").unwrap();

        let queries = read_cache_dir(dir.path()).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].sql_hash, 1);
    }

    #[test]
    fn read_real_cache_files() {
        // Try reading the actual .bsql/queries/ directory if it exists
        let cache_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join(".bsql")
            .join("queries");
        if cache_dir.exists() {
            let queries = read_cache_dir(&cache_dir).unwrap();
            assert!(
                !queries.is_empty(),
                "expected cached queries in .bsql/queries/"
            );
            for q in &queries {
                assert!(!q.normalized_sql.is_empty());
                assert_ne!(q.sql_hash, 0);
            }
        }
    }

    #[test]
    fn read_dir_empty_bitcode_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("empty.bitcode"), b"").unwrap();
        // Empty file should fail to decode, be skipped
        let queries = read_cache_dir(dir.path()).unwrap();
        assert!(queries.is_empty());
    }
}
