mod cache;
mod integrity;
mod migrate;
mod verify;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("migrate") if args.get(2).map(|s| s.as_str()) == Some("--check") => {
            cmd_migrate_check(&args);
        }
        Some("check") if args.get(2).map(|s| s.as_str()) == Some("--verify-cache") => {
            cmd_verify_cache(&args);
        }
        Some("verify") => {
            cmd_verify_integrity(&args);
        }
        Some("clean") => {
            cmd_clean(&args);
        }
        _ => {
            eprintln!("Usage:");
            eprintln!(
                "  bsql migrate --check <migration.sql> [--database-url URL] [--cache-dir DIR]"
            );
            eprintln!("  bsql check --verify-cache [--database-url URL] [--cache-dir DIR]");
            eprintln!("  bsql verify [--cache-dir DIR]");
            eprintln!("      Check local cache integrity (no database required).");
            eprintln!("      Exits 1 if cache is broken — suitable for pre-commit hooks.");
            eprintln!("  bsql clean [--cache-dir DIR]");
            std::process::exit(2);
        }
    }
}

fn get_database_url(args: &[String]) -> String {
    parse_flag(args, "--database-url")
        .or_else(|| std::env::var("BSQL_DATABASE_URL").ok())
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| {
            eprintln!("error: no database URL. Set BSQL_DATABASE_URL or use --database-url");
            std::process::exit(2);
        })
}

fn get_cache_dir(args: &[String]) -> std::path::PathBuf {
    parse_flag(args, "--cache-dir")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            find_cache_dir().unwrap_or_else(|| {
                eprintln!("error: .bsql/queries/ directory not found");
                std::process::exit(2);
            })
        })
}

fn cmd_migrate_check(args: &[String]) {
    if args.len() < 4 {
        eprintln!(
            "Usage: bsql migrate --check <migration.sql> [--database-url URL] [--cache-dir DIR]"
        );
        std::process::exit(2);
    }

    let migration_path = &args[3];
    let database_url = get_database_url(args);
    let cache_dir = get_cache_dir(args);

    let migration_sql = std::fs::read_to_string(migration_path).unwrap_or_else(|e| {
        eprintln!("error: cannot read {}: {}", migration_path, e);
        std::process::exit(2);
    });

    let queries = cache::read_cache_dir(&cache_dir).unwrap_or_else(|e| {
        eprintln!("error: cannot read cache: {}", e);
        std::process::exit(2);
    });

    if queries.is_empty() {
        println!(
            "No cached queries found in {}. Nothing to check.",
            cache_dir.display()
        );
        std::process::exit(0);
    }

    println!(
        "Checking {} cached queries against migration...",
        queries.len()
    );

    let result =
        migrate::check_migration(&database_url, &migration_sql, &queries).unwrap_or_else(|e| {
            eprintln!("error: {}", e);
            std::process::exit(1);
        });

    if result.failed.is_empty() {
        println!("All {} queries passed.", result.total_queries);
        std::process::exit(0);
    } else {
        println!(
            "\n{} of {} queries FAILED:\n",
            result.failed.len(),
            result.total_queries
        );
        for f in &result.failed {
            println!("  FAIL [{:016x}]: {}", f.sql_hash, f.sql);
            println!("  Error: {}", f.error);
            println!();
        }
        std::process::exit(1);
    }
}

fn cmd_verify_cache(args: &[String]) {
    let database_url = get_database_url(args);
    let cache_dir = get_cache_dir(args);

    let queries = cache::read_cache_dir(&cache_dir).unwrap_or_else(|e| {
        eprintln!("error: cannot read cache: {}", e);
        std::process::exit(2);
    });

    if queries.is_empty() {
        println!(
            "No cached queries found in {}. Nothing to verify.",
            cache_dir.display()
        );
        std::process::exit(0);
    }

    println!(
        "Verifying {} cached queries against live schema...",
        queries.len()
    );

    let result = verify::verify_cache(&database_url, &queries).unwrap_or_else(|e| {
        eprintln!("error: {}", e);
        std::process::exit(1);
    });

    if result.drifted.is_empty() {
        println!("All {} queries match current schema.", result.total_queries);
        std::process::exit(0);
    } else {
        println!(
            "\n{} of {} queries have SCHEMA DRIFT:\n",
            result.drifted.len(),
            result.total_queries
        );
        for d in &result.drifted {
            println!("  DRIFT [{:016x}]: {}", d.sql_hash, d.sql);
            println!("  Reason: {}", d.reason);
            println!();
        }
        eprintln!("Run `cargo build` with a live database connection to regenerate the cache.");
        std::process::exit(1);
    }
}

fn cmd_verify_integrity(args: &[String]) {
    let cache_dir = get_cache_dir(args);

    let report = integrity::check(&cache_dir).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(2);
    });

    println!(
        "Cache at {}: {} bitcode files, {} manifested",
        cache_dir.display(),
        report.total_bitcode,
        report.total_manifested
    );

    if !report.missing_bitcode.is_empty() {
        println!(
            "\nERROR: {} manifest entries have no bitcode file on disk:",
            report.missing_bitcode.len()
        );
        for h in report.missing_bitcode.iter().take(10) {
            println!("  {h}.bitcode");
        }
        if report.missing_bitcode.len() > 10 {
            println!("  ... and {} more", report.missing_bitcode.len() - 10);
        }
        println!(
            "\nThis is the most common cause of 'query not found in offline cache'.\n\
             Usually it means `.bitcode` files were not committed to git.\n\
             Fix: rebuild with a live database, then `git add .bsql/queries/`."
        );
    }

    if !report.corrupt_files.is_empty() {
        println!(
            "\nERROR: {} bitcode files failed to decode:",
            report.corrupt_files.len()
        );
        for (path, err) in report.corrupt_files.iter().take(10) {
            println!("  {path}: {err}");
        }
    }

    if !report.filename_mismatch.is_empty() {
        println!(
            "\nERROR: {} bitcode files have a filename that does not match their hash:",
            report.filename_mismatch.len()
        );
        for f in report.filename_mismatch.iter().take(10) {
            println!("  {f}");
        }
    }

    if !report.orphan_bitcode.is_empty() {
        println!(
            "\nWarning: {} bitcode files are not referenced by any manifest \
             (run `bsql clean` to remove):",
            report.orphan_bitcode.len()
        );
        for h in report.orphan_bitcode.iter().take(5) {
            println!("  {h}.bitcode");
        }
        if report.orphan_bitcode.len() > 5 {
            println!("  ... and {} more", report.orphan_bitcode.len() - 5);
        }
    }

    if report.is_ok() {
        println!("\nCache is consistent.");
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}

fn cmd_clean(args: &[String]) {
    let cache_dir = get_cache_dir(args);
    let mut count = 0u64;
    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            // Clean ALL cache state — bitcode AND metadata — so the next
            // build starts from a known-empty state. Leaving `.manifest` with
            // stale hashes while deleting `.bitcode` is the exact bug that
            // motivated this tool.
            let should_remove = path.extension().is_some_and(|e| e == "bitcode")
                || path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                    matches!(n, ".manifest" | ".manifest.canonical" | ".generation")
                });
            if should_remove && std::fs::remove_file(&path).is_ok() {
                count += 1;
            }
        }
    }
    println!("Removed {count} cache entries from {}", cache_dir.display());
}

/// Parse a `--flag value` pair from the argument list.
pub fn parse_flag(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}

/// Walk up from the current working directory looking for `.bsql/queries/`.
pub fn find_cache_dir() -> Option<std::path::PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let candidate = dir.join(".bsql").join("queries");
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flag_present() {
        let args = vec![
            "bsql".into(),
            "--database-url".into(),
            "postgres://localhost/test".into(),
        ];
        assert_eq!(
            parse_flag(&args, "--database-url"),
            Some("postgres://localhost/test".into())
        );
    }

    #[test]
    fn parse_flag_absent() {
        let args = vec!["bsql".into(), "migrate".into()];
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn parse_flag_at_end_without_value() {
        // --database-url is the last arg with no value following
        let args = vec!["bsql".into(), "--database-url".into()];
        // windows(2) yields ["bsql","--database-url"] which doesn't match
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn parse_flag_multiple_flags() {
        let args = vec![
            "bsql".into(),
            "migrate".into(),
            "--check".into(),
            "file.sql".into(),
            "--database-url".into(),
            "postgres://host/db".into(),
            "--cache-dir".into(),
            "/tmp/cache".into(),
        ];
        assert_eq!(
            parse_flag(&args, "--database-url"),
            Some("postgres://host/db".into())
        );
        assert_eq!(parse_flag(&args, "--cache-dir"), Some("/tmp/cache".into()));
    }

    #[test]
    fn parse_flag_empty_args() {
        let args: Vec<String> = vec![];
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn parse_flag_single_arg() {
        let args = vec!["bsql".into()];
        assert_eq!(parse_flag(&args, "--database-url"), None);
    }

    #[test]
    fn find_cache_dir_walks_up() {
        // If .bsql/queries/ exists somewhere above CWD, find_cache_dir returns it.
        if let Some(dir) = find_cache_dir() {
            assert!(dir.exists());
            assert!(dir.is_dir());
        }
    }

    #[test]
    fn parse_flag_duplicate_takes_first() {
        let args = vec![
            "bsql".into(),
            "--database-url".into(),
            "first".into(),
            "--database-url".into(),
            "second".into(),
        ];
        assert_eq!(parse_flag(&args, "--database-url"), Some("first".into()));
    }

    #[test]
    fn cmd_clean_removes_bitcode_and_metadata() {
        let dir = std::env::temp_dir().join("bsql_clean_full_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("query1.bitcode"), b"data1").unwrap();
        std::fs::write(dir.join("query2.bitcode"), b"data2").unwrap();
        std::fs::write(dir.join(".manifest"), b"deadbeef\n").unwrap();
        std::fs::write(dir.join(".manifest.canonical"), b"deadbeef\n").unwrap();
        std::fs::write(dir.join(".generation"), b"123_456").unwrap();
        std::fs::write(dir.join("keep_me.txt"), b"keep").unwrap();

        let args = vec![
            "bsql".into(),
            "clean".into(),
            "--cache-dir".into(),
            dir.to_str().unwrap().into(),
        ];
        cmd_clean(&args);

        assert!(dir.join("keep_me.txt").exists(), "non-cache file preserved");
        assert!(!dir.join("query1.bitcode").exists());
        assert!(!dir.join("query2.bitcode").exists());
        assert!(
            !dir.join(".manifest").exists(),
            ".manifest must also be cleaned, otherwise a subsequent build \
             in offline mode fails with 'query not in cache'"
        );
        assert!(!dir.join(".manifest.canonical").exists());
        assert!(!dir.join(".generation").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cmd_clean_empty_directory() {
        let dir = std::env::temp_dir().join("bsql_clean_empty_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let args = vec![
            "bsql".into(),
            "clean".into(),
            "--cache-dir".into(),
            dir.to_str().unwrap().into(),
        ];
        let cache_dir = get_cache_dir(&args);
        let mut count = 0u64;
        if let Ok(entries) = std::fs::read_dir(&cache_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().is_some_and(|e| e == "bitcode")
                    && std::fs::remove_file(entry.path()).is_ok()
                {
                    count += 1;
                }
            }
        }

        assert_eq!(count, 0, "should remove 0 files from empty dir");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
