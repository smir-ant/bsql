# Test Coverage Specification

Every item is a scenario that must have a passing test.
Serves as test instruction for any backend (PG, SQLite, future drivers).
Living document — grows with each bug report and edge case discovery.

---

## 1. fetch_all

### Happy path
- 0 rows → empty Vec
- 1 row → Vec with 1 element
- 2+ rows → correct order preserved
- 100+ rows → all returned, no truncation
- Multiple column types in one query (int + text + bool + nullable)
- Same query executed twice → same results (idempotent)

### Bad path
- Nonexistent table → compile error
- Nonexistent column → compile error
- Syntax error in SQL → compile error

### Edge cases
- All rows have NULL in nullable column
- Mix of NULL and non-NULL in same column
- Column name with underscore, digit
- Duplicate column names (SELECT a.id, b.id → id, id_1)
- Very long TEXT value (10KB+)
- Empty string vs NULL in same result set
- Unicode / emoji in text values (кириллица, 中文, 🎉)
- ASCII control characters in text
- Column named with SQL reserved word (type, select, from)

---

## 2. fetch_one

### Happy path
- Exactly 1 row → returns Row
- Row with all field types populated

### Bad path
- 0 rows → Err with "exactly 1 row, got 0" message
- 2+ rows → Err with "exactly 1 row, got N" message
- Error message distinguishable from fetch_optional error

### Edge cases
- fetch_one with NULL in every nullable column
- fetch_one on table with millions of rows + WHERE on PK (index usage)

---

## 3. fetch_optional

### Happy path
- Found → Some(row)
- Not found → None

### Bad path
- 2+ rows → Err (not silently returns first)

### Edge cases
- Empty table → None
- WHERE that sometimes matches, sometimes doesn't (data-dependent)

---

## 4. execute

### Happy path
- INSERT → affected = 1
- UPDATE matching 1 row → affected = 1
- UPDATE matching 0 rows → affected = 0
- UPDATE matching N rows → affected = N
- DELETE matching 1 row → affected = 1
- DELETE matching 0 rows → affected = 0
- DELETE matching N rows → affected = N

### Via different targets
- execute via Pool
- execute via Transaction
- execute via PoolConnection

### Bad path
- INSERT violating UNIQUE → Err + is_unique_violation()
- INSERT violating FK → Err + is_foreign_key_violation()
- INSERT violating NOT NULL → Err
- INSERT violating CHECK constraint → Err
- UPDATE with invalid value → Err

### Edge cases
- execute on SELECT (returns 0 affected? or error?)
- Batch INSERT of 100+ rows via loop
- INSERT ... ON CONFLICT DO NOTHING → affected = 0
- INSERT ... ON CONFLICT DO UPDATE → affected = 1

---

## 5. for_each (zero-alloc iteration)

### Happy path
- Iterates all rows in order
- Borrowed fields (&str) accessible in closure
- Closure can accumulate state (counter, sum)
- for_each_map collects into Vec

### Bad path
- Closure returns Err → iteration stops, error propagated
- Empty result → closure never called

### Edge cases
- for_each on 10,000+ rows (memory stays constant)
- for_each with NULL values in borrowed fields
- Nested query inside for_each closure (acquires second connection)
- for_each inside a transaction (deferred auto-flush before read)

---

## 6. fetch_stream (PG)

### Happy path
- Streams rows one by one via next()
- All rows received in order
- With bind parameters
- Fully consumed → connection returned to pool

### Bad path
- Drop stream mid-iteration → connection discarded (not returned to pool)
- Server error mid-stream → Err on next()
- Statement timeout fires during streaming after N chunks consumed

### Edge cases
- Stream 0 rows → first next() returns None
- Stream 1 row → one Some, then None
- Stream 100,000 rows → constant memory
- Drop after first advance() but before any next_row()
- tokio task cancellation during streaming

---

## 7. query_as!

### Happy path
- Maps to user struct with correct field types
- fetch_one, fetch_all, fetch_optional all work
- Struct with all common types (i32, String, bool, Option<T>)

### Bad path
- Struct field type doesn't match column → clear compile error
- Nullable column but struct field is non-Option → compile error with hint
- Struct missing a field → compile error
- Struct has extra field → compile error

### Edge cases
- Struct with Option<String> for nullable column
- Struct with renamed fields (AS alias in SQL)
- Struct in different module (path resolution)

---

## 8. Parameters — Scalar Types

### Happy path
- i16, i32, i64, f32, f64, bool
- &str, String
- u32 (OID)
- &[u8], Vec<u8> (bytea)

### Bad path
- Wrong type for column → compile error
- i32 for int8 column → compile error
- &str for int4 column → compile error

### Edge cases
- i16::MIN, i16::MAX
- i32::MIN, i32::MAX
- i64::MIN, i64::MAX
- f32::NAN, f32::INFINITY, f32::NEG_INFINITY
- f64::NAN, f64::INFINITY, f64::NEG_INFINITY
- Empty string "" (not NULL)
- Very long string (100KB)
- Empty bytea (zero bytes)
- Large bytea (1MB)
- NUL byte (\0) inside bytea
- bool true and false explicitly
- Same parameter used twice in SQL ($x in WHERE and SET)
- Parameter in subquery context

---

## 9. Parameters — Auto-deref

### Happy path
- String variable → &str param
- Vec<i32> variable → &[i32] param
- Vec<String> variable → &[String] param

### Edge cases
- &String (double reference)
- Cow<str> if applicable

---

## 10. Parameters — Option<T> (nullable)

### Happy path
- Option<i32> = None → SQL NULL
- Option<i32> = Some(42) → 42
- Option<&str> = None → SQL NULL
- Option<&str> = Some("hello") → "hello"
- Option<bool> = None / Some
- Option<f64> = None / Some

### Edge cases
- Option<String> = Some("".to_owned()) → empty string, not NULL
- Repeated None/Some in same query
- Option param used twice in same SQL

---

## 11. Parameters — Arrays (PG)

### Happy path
- Vec<i32> in ANY()
- Vec<String> / &[String] in ANY()
- &[&str] in ANY()
- Vec<i64>, Vec<bool>, Vec<f32>, Vec<f64>

### Edge cases
- Empty array → 0 matches
- Array with 1 element
- Array with 1000 elements
- Array with duplicate values
- Array of empty strings
- Array with NULL elements (Vec<Option<i32>>)

---

## 12. Parameters — JSONB/JSON auto-cast (PG)

### Happy path
- &str → jsonb column (auto-cast)
- &str → json column (auto-cast)
- Valid JSON string round-trips correctly
- JSON with nested objects/arrays

### Bad path
- Invalid JSON string → PG error (not panic)
- Empty string → PG error (not valid JSON)

### Edge cases
- JSON with unicode
- JSON with very long string values
- JSON null literal ("null") vs SQL NULL
- JSONB operators in WHERE (->>, @>, ?)

---

## 13. Parameters — unnest / array functions (PG)

### Happy path
- unnest(Vec<String>) → rows
- unnest(Vec<i32>) → rows
- ANY($array_param)

### Edge cases
- unnest empty array → 0 rows

---

## 14. Nullability — Column inference

### Happy path
- NOT NULL column → T
- Nullable column → Option<T>
- LEFT JOIN right-side → Option<T>
- RIGHT JOIN left-side → Option<T>
- FULL JOIN both sides → Option<T>

### Edge cases
- INNER JOIN → preserves original nullability
- Multiple LEFT JOINs → all right tables nullable
- Self-join (same table twice)
- Column aliased (AS) preserves nullability
- SELECT * expands to correct nullability per column

---

## 15. Nullability — NOT NULL inference for expressions

### Happy path (must be NOT NULL)
- COUNT(*), COUNT(column)
- ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE()
- NOW(), CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_USER
- COALESCE(nullable, literal)
- EXISTS(subquery)
- Literal values (42, 'text', true, false)
- CASE WHEN ... THEN literal ELSE literal END
- LENGTH(), LOWER(), UPPER(), TRIM(), CONCAT()
- ABS(), CEIL(), FLOOR(), ROUND()
- GEN_RANDOM_UUID()
- TO_CHAR(), TO_TIMESTAMP()

### Correctly nullable (must stay Option)
- SUM(), AVG(), MAX(), MIN() on empty group
- COALESCE(nullable, nullable) without literal
- LAG(), LEAD()
- Unknown/custom functions
- Arithmetic on nullable columns

---

## 16. Nullability — Cast inference

### Happy path
- column::text on NOT NULL column → String
- CAST(column AS integer) on NOT NULL column → i32

### Safety
- Cast on nullable column → Option
- Ambiguous column name → stays Option
- Schema-qualified name → not matched, stays Option
- Complex expression cast → not matched, stays Option
- Only whitelisted safe casts: text→jsonb/json/xml

---

## 17. PG enum types

### Happy path
- Enum column → String
- Enum in simple SELECT
- Enum in JOIN
- Enum in subquery
- Enum in CTE

### Edge cases
- Enum with many variants (20+)
- Enum value comparison in WHERE

---

## 18. SQL Constructs

### Basic DML
- SELECT with WHERE
- INSERT single row
- INSERT with RETURNING
- UPDATE with WHERE
- DELETE with WHERE
- INSERT ... ON CONFLICT DO NOTHING
- INSERT ... ON CONFLICT DO UPDATE

### Joins
- INNER JOIN
- LEFT JOIN
- RIGHT JOIN
- CROSS JOIN
- Multiple JOINs in one query
- Self-join

### Subqueries & CTEs
- Subquery in FROM
- Subquery in WHERE (EXISTS)
- Subquery in WHERE (IN)
- CTE (WITH clause)
- Multiple CTEs
- Recursive CTE (PG)

### Set operations
- UNION ALL
- UNION (deduplicated)
- INTERSECT
- EXCEPT

### Aggregation
- GROUP BY single column
- GROUP BY multiple columns
- GROUP BY with HAVING
- COUNT, SUM, AVG, MAX, MIN
- COUNT DISTINCT
- Window function (ROW_NUMBER, RANK)
- Window with PARTITION BY

### Other
- ORDER BY + LIMIT + OFFSET
- LIKE / ILIKE with parameters
- BETWEEN with parameters
- IN list with parameters
- IS NULL / IS NOT NULL in WHERE
- COALESCE in SELECT
- CASE WHEN in SELECT
- String concatenation (||)
- Arithmetic expressions in SELECT
- SQL with comments (-- and /* */)
- SQL with dollar-quoted strings ($$...$$)

### Dynamic queries (PG)
- 1 optional clause (Some / None)
- 2 optional clauses (all 4 combinations)
- 3 optional clauses
- Optional clause with ILIKE
- Optional clause + base required params
- Dynamic query via Transaction
- Dynamic query via PoolConnection
- Dynamic query + sort enum combined

---

## 19. Transactions

### Lifecycle
- Begin + commit → data persists
- Begin + rollback → data discarded
- Drop without commit → auto-rollback (no panic)
- Begin + no operations + commit → noop
- Begin + no operations + rollback → noop
- New transaction after rollback → works normally

### Operations in transaction
- execute → returns affected count
- fetch_one inside tx
- fetch_all inside tx
- fetch_optional inside tx
- Multiple operations in same tx
- Read your own writes within tx

### Error recovery
- Error inside tx → aborted state
- Query after error → "current transaction is aborted"
- Rollback after error → succeeds, connection usable
- Pool.close() while transaction active → connection discarded

### Isolation
- Two concurrent transactions don't see each other's uncommitted data
- Serializable isolation detects conflicts

### Savepoints (PG)
- Savepoint + release
- Savepoint + rollback_to
- Nested savepoints (A inside B)
- Rollback to inner preserves outer changes
- Invalid savepoint name rejected

### Deferred pipeline (PG)
- Defer 1 operation + commit → flushed
- Defer N operations + commit → all flushed in order
- Defer + explicit flush → returns counts in order
- Defer + rollback → discarded
- Deferred count tracking
- Auto-flush before read
- Deferred pipeline with 10,000 operations

### Isolation levels (PG)
- READ COMMITTED (default)
- SERIALIZABLE
- REPEATABLE READ
- Set isolation after first query → Err

---

## 20. Error Handling — Compile-time

- Invalid table name → error with "available tables" hint
- Invalid column name → error
- Type mismatch: &str for int4
- Type mismatch: i32 for int8
- Type mismatch: i32 for text
- Type mismatch: bool for text
- Type mismatch: i64 for int4
- Type mismatch: &str for int column
- Parameter missing type (bare $name)
- Parameter empty type ($name:)
- Parameter conflicting types ($x: i32 and $x: &str)
- DDL: CREATE TABLE → rejected
- DDL: DROP TABLE → rejected
- DDL: ALTER TABLE → rejected
- DDL: TRUNCATE → rejected
- DDL: GRANT → rejected
- DELETE without WHERE → rejected
- UPDATE without WHERE → rejected
- SQL injection (semicolons, multiple statements) → rejected
- Empty SQL string → error
- Whitespace-only SQL → error
- Comment-only SQL → error
- CTE without DML → error
- Manual positional params ($1 instead of $name: type) → error
- Optional clause with non-Option type → error
- Too many optional clauses → error
- Nested brackets in optional clause → error
- Unclosed bracket in optional clause → error
- Missing feature flag for time/uuid/decimal → error with hint
- unnest(unknown) → error with cast suggestion

---

## 21. Error Handling — Runtime

### Constraint violations
- UNIQUE constraint → Err + is_unique_violation() = true
- FK constraint → Err + is_foreign_key_violation() = true
- NOT NULL constraint → Err
- CHECK constraint → Err

### Connection errors
- Wrong host → Err
- Wrong port → Err
- Wrong password → Err
- Wrong database → Err
- Connection refused → Err
- Connection timeout → Err

### Query errors
- Statement timeout → Err with pg_code 57014
- Division by zero → Err
- Invalid JSON → Err
- Server disconnect mid-query → Err
- Query after disconnect → Err

### Error API
- QueryError constructable from user code (pub fields)
- QueryError with source (pub source)
- is_unique_violation() true for 23505, false for others
- is_foreign_key_violation() true for 23503, false for others
- pg_code() returns correct code / None for non-PG errors
- Display format includes message and pg_code
- Debug format includes all fields
- Error messages distinguish pool exhausted / pool closed / timeout

---

## 22. Pool & Connection

### Lifecycle
- Pool connect / open → success
- Pool with invalid URL → Err
- Pool acquire → PoolConnection
- PoolConnection query → works
- PoolConnection drop → returned to pool
- Pool close → all connections dropped

### Limits
- max_size: acquire up to max → success
- max_size: acquire beyond max → blocks/timeout
- max_size=0 → all acquires fail immediately
- max_size=1 + concurrent query and transaction → blocks correctly
- acquire_timeout fires when pool exhausted
- max_lifetime: old connections replaced with new
- stale_timeout: idle connections evicted
- stale_timeout=0 → every connection evicted immediately

### Status
- status.max_size correct
- status.idle + status.active = status.open
- Status under concurrent load maintains invariants

### Recovery
- Backend killed → next query errors
- Pool creates new connection after kill
- Connection usable after statement timeout (outside tx)
- Pool recovers after all connections killed

### Concurrency
- 8+ threads × 50+ queries → no panic, no deadlock
- Concurrent acquire/release cycles → pool stable
- Pool close while connections checked out → graceful
- Pool close then close again (double close) → idempotent
- Pool close then status() → all zeros

### Configuration
- Builder: all options at once
- Builder: minimal options (just URL)
- URL parsing: host, port, user, password, database
- URL parsing: sslmode=disable/prefer/require
- URL parsing: statement_timeout
- URL parsing: statement_cache=disabled (PG)
- URL parsing: sslrootcert, sslcert, sslkey (PG)
- URL parsing: host=/tmp (Unix socket, PG)
- URL with percent-encoded characters
- URL without password
- URL without port (default 5432)

### Statement cache (PG)
- Cache hit → no Parse roundtrip
- Cache miss → Parse+Describe+Bind
- Cache eviction at max_stmt_cache_size
- max_stmt_cache_size=0 → always re-prepare
- Unnamed statements (pgbouncer mode) → cache empty after queries

### Connection states
- Connection returned in transaction state → discarded (not pooled)
- Connection with streaming_active → new query errors or auto-closes

---

## 23. Advanced — PG

### LISTEN/NOTIFY
- Listen on channel → receive notification
- Multiple channels simultaneously
- Notification with payload
- Large payload (8000 bytes)
- Listener drop → cleanup
- Notification burst (1000+ rapid notifications)
- unlisten then re-listen same channel

### Singleflight
- Identical concurrent queries → single roundtrip
- Different queries → separate roundtrips
- Via PoolConnection → not coalesced
- Via Transaction → not coalesced
- Leader panics → followers get None

### Read/write split
- SELECT → routes to replica (when configured)
- INSERT/UPDATE/DELETE → routes to primary
- Transaction → routes to primary

### COPY protocol
- COPY IN (bulk insert)
- COPY OUT (bulk export)
- COPY IN empty → noop
- COPY IN invalid data → error
- COPY inside transaction

### pgbouncer compatibility
- Unnamed statements (statement_cache=disabled)
- Statement cache empty after queries
- Same SQL twice without cache → both succeed
- URL parsing statement_cache param
- Warmup SQLs with pgbouncer mode → skipped or adapted

### TLS
- Custom CA certificate
- Client certificate (mTLS)
- sslmode=prefer → fallback to non-TLS

### TLS — crypto provider (`tls_common` tests)
- `ring_provider()` returns the same `Arc<CryptoProvider>` across calls
  (cached via `OnceLock`, guards the hot-path zero-alloc invariant)
- `default_client_config()` returns the same `Arc<ClientConfig>` across
  calls (shared between sync and async TLS paths)
- `build_client_config()` with webpki roots and no client auth succeeds
  and does not panic under any feature combination
- **Regression for the rustls 0.23 runtime panic** ("Could not
  automatically determine the process-level CryptoProvider from Rustls
  crate features"): under the `feature-unification-repro` dev feature,
  `rustls` is compiled with BOTH `ring` and `aws-lc-rs` enabled —
  reproducing exactly the cargo feature unification scenario that was
  hitting users in the wild.
  - **Positive**: `build_client_config()` via `builder_with_provider`
    survives feature unification and builds a valid `ClientConfig`.
  - **Negative** (`legacy_builder_panics_under_feature_unification`):
    the legacy `rustls::ClientConfig::builder()` entry point is caught
    via `std::panic::catch_unwind` and MUST panic. If it stops
    panicking, the test environment is no longer reproducing the
    conflict and the positive test above would be silently
    false-green — the negative assertion fails loudly in that case.
- `tls_sync` integration: `build_tls_config` routes through
  `tls_common::build_client_config` for both the default (webpki,
  no-auth) path and the custom path (ssl_root_cert / mTLS), so every
  TLS config in the process is constructed with the pinned `ring`
  provider via exactly one codepath.

### Dynamic SQL / runtime escape hatch

**`raw_execute`** (DDL, SET, DML without result rows)
- CREATE TEMP TABLE → succeeds
- SET search_path TO → succeeds (session-level command)
- ALTER TABLE (via schema isolation lifecycle)
- DROP SCHEMA CASCADE (via schema isolation lifecycle)
- Invalid SQL → error

**`raw_query`** (SELECT returning text-encoded rows)
- SELECT returning rows → Vec<RawRow> with values
- SELECT returning empty result → empty Vec
- Invalid SQL → error

**`raw_query_params`** (parameterized runtime SQL via extended protocol)
- SELECT with typed params ($1: i32) → correct rows
- SELECT with multiple params → correct filtering
- SELECT with empty params array → works
- INSERT + DELETE with params → rows affected
- NULL parameter (Option<i32> = None) → PG handles correctly
- Invalid SQL → error

**Schema isolation lifecycle** (the `#[bsql::test]` pattern, exercised end-to-end)
- CREATE SCHEMA → SET search_path → CREATE TABLE inside schema →
  INSERT → SELECT back → DROP SCHEMA CASCADE → verify schema is gone →
  reset search_path. Full round-trip proving the test-infrastructure
  pattern works through `raw_execute` + `raw_query`.

---

## 24. Advanced — SQLite

### Isolation
- In-memory DBs are isolated (separate pools)
- Two pools on same file → WAL handles concurrent access
- Database file deleted during pool operation → error

### Transactions
- BEGIN/COMMIT via raw_execute
- BEGIN/ROLLBACK via raw_execute
- Busy timeout under concurrent write contention

### Schema
- CREATE TABLE + INSERT + SELECT roundtrip
- execute_batch (multiple statements at once)

---

## 25. CLI

### migrate --check
- Valid migration → all queries pass
- Breaking migration → reports failed queries with SQL hash
- Unreachable host → timeout error
- No cache directory → error
- Empty cache → nothing to check
- Semicolon in cached SQL → rejected (tampering defense)

### check --verify-cache
- Fresh cache → all pass
- Stale cache (schema changed) → reports drift
- No database URL → error with env var hint

### bsql clean
- Removes all .bitcode files
- Empty directory → prints 0 removed
- Does NOT remove non-.bitcode files
- Non-existent directory → error

---

## 26. Macros

### #[bsql::pg_enum]
- Derives FromSql/ToSql
- Label validation against pg_catalog
- Display, Debug, Clone, Copy, PartialEq, Eq, Hash

### #[bsql::test]
- PG: creates schema, applies fixtures, cleanup on drop
- SQLite: creates temp file, cleanup on drop
- Unique schema names across parallel tests
- Panic cleanup (Drop guard)

### #[bsql::sort]
- Generates sql() method
- Validates each fragment via PREPARE
- Works in query! with $[sort: EnumType]

---

## 27. Encoding / Decoding Edge Cases

- UTF-8 multibyte characters round-trip (кириллица, 中文, 🎉)
- ASCII control characters in text
- NUL byte (\0) in bytea
- Empty bytea (0 bytes, not NULL)
- Very large row (50+ columns)
- Column with SQL reserved keyword name
- Timestamp at PG epoch (2000-01-01)
- Timestamp at Unix epoch (1970-01-01)
- Timestamp far in future (year 9999)
- Date before Unix epoch
- Numeric with many decimal places (0.000000001)
- Numeric zero, negative zero
- Array with NULL elements
- Empty array (not NULL)
- f64 NaN round-trip
- f64 Infinity round-trip
- i32::MAX, i32::MIN boundary values
- Option<Vec<u8>> Some(vec![]) vs None (empty bytea vs NULL)

---

## 28. Security

- Savepoint name injection attempt → rejected by validation
- COPY table/column name with special characters → properly quoted
- LISTEN channel name with injection attempt → properly quoted
- NOTIFY payload with quotes/backslashes → properly escaped
- Password NOT in error messages
- Password NOT in Debug output of Config/Pool
- Cache tampering (semicolons in cached SQL) → rejected

---

## 29. Concurrency & Race Conditions

- Two threads acquire() when 1 slot remains → one succeeds, one waits
- Rapid acquire/release cycles → pool stable, no leak
- Pool close races with in-progress acquire → no hang
- Singleflight leader panics → followers get clean error
- Listener notification burst exceeding buffer → no crash
- Transaction commit concurrent with pool close → clean error
- PoolGuard Drop on different thread than acquire → buffer returned to correct TL pool
- Statement cache eviction during active streaming query → no crash
- Connection returned while condvar wait times out → no lost wakeup

---

## 30. Resource Exhaustion

- fetch_all on 1M rows → completes (may be slow, but no OOM on 8GB)
- for_each on 1M rows → constant memory
- copy_in with 10M rows → steady memory
- Statement cache at max → eviction works, no unbounded growth
- Thread-local buffer pools → capped, excess dropped
- Deferred pipeline with 10,000 operations → flush works
- Notification buffer full → logged, not crashed, resumes after drain

---

## 31. Partial Failures

- Network drop after Parse sent but before Execute response → connection discarded
- TCP RST mid-DataRow → no panic from bounds check
- Server killed mid-streaming → error on next advance(), connection discarded
- SQLite file becomes read-only mid-write → error
- SQLite disk full during INSERT → error
- Partial cache write (process killed) → next read detects corruption
- TLS renegotiation failure mid-query → error

---

## 32. Backward Compatibility

- Cache v1 file (no bsql_version) → readable with migration
- Cache v2 file (no param_rust_types) → readable
- Cache v3 file (no rewritten_sql) → readable
- Cache v5 (future, unknown) → clear "upgrade bsql" error
- URL param unknown value (sslmode=verify-full) → clear error
