# CREDO

This is the constitution of bsql. Not a spec. Not a wishlist. These are the non-negotiable principles that govern every design decision, every line of code, every dependency choice. If a proposed change violates a principle, the change loses.

---

### 1. If it compiles, the SQL is correct.

This is the load-bearing promise. Everything else in this document exists to serve it.

Every SQL string passed to `bsql::query!` is validated against a real PostgreSQL instance at compile time. Column names, table names, parameter types, nullability, return types --- all verified against `pg_catalog`. The binary that `cargo build` produces contains zero unverified SQL. Not "mostly verified." Not "verified if you used the right function." Zero.

**In practice:**
- Every column typo is a compile error. Every type mismatch is a compile error. Every nullable column is `Option<T>` --- no exceptions, no overrides.
- If the schema changes and a query becomes invalid, the next `cargo build` fails. Not the next deployment. Not the 3 AM page. The build.
- Offline mode validates against a cached schema snapshot. The snapshot is generated from a live database. There is no mode where SQL goes unchecked.

---

### 2. No escape hatch. Period.

There is no `query()`. No `raw_sql()`. No `execute_unchecked()`. No `#[allow(unchecked)]`. No "advanced users only" API. No backdoor hidden in a submodule. The unsafe path does not exist because the function does not exist.

This is not a philosophical position. It is an architectural one. If an unchecked path exists, someone will use it. One unchecked query in a codebase of 500 checked queries breaks the guarantee. The guarantee is binary: 100% or meaningless.

**In practice:**
- The crate exports macros and traits. It does not export any function that accepts `&str` SQL.
- Edge cases (dynamic table names, migration tooling) are solved through compile-time macros or separate crates. Not by weakening the guarantee.
- If you need unchecked SQL, use `tokio-postgres` directly. bsql will not become the thing it replaces.

---

### 3. SQL is the language.

There is no DSL. No `.filter()`. No `.select()`. No query builder. You write PostgreSQL SQL --- CTEs, window functions, `LATERAL` joins, `DISTINCT ON`, `ANY()`, `unnest()` --- and the macro validates it. If you know PostgreSQL, you know bsql.

DSLs inevitably diverge from the SQL they model. They cannot express the full power of PostgreSQL without escape hatches. bsql avoids this problem by not having a DSL at all.

**In practice:**
- The macro is a validator and code generator. Not a query language.
- Complex queries (recursive CTEs, window partitions, subqueries) work on day one. No DSL extensions needed.
- The learning curve is PostgreSQL's, not bsql's. PostgreSQL's documentation is bsql's documentation for query syntax.

---

### 4. Every nanosecond matters.

Not because users notice nanoseconds. Because the mindset that says "nanoseconds don't matter" produces millisecond-level bloat through a thousand "doesn't matter" decisions. bsql fights for every cycle.

**In practice:**
- **Arena allocation** *(planned for v0.3)*: every query execution uses a bump allocator. All row data allocates in a contiguous arena. One deallocation for everything. 300 pointer bumps at ~2ns each vs. 300 malloc/free pairs at ~27ns each. 13x less allocation overhead.
- **Binary protocol** *(planned for v0.3)*: PostgreSQL's binary wire format eliminates parsing entirely for numeric types. `i32` is `i32::from_be_bytes()` --- one instruction. Timestamps are 8-byte memcpy. UUIDs are 16-byte memcpy. 50% less data on the wire for typical results.
- **SIMD** *(planned for v0.4)*: `simdutf` for UTF-8 validation (70 GB/s vs 3 GB/s scalar). `sonic-rs` for JSONB columns. `memchr` for enum string matching.
- **Zero-copy deserialization** *(planned for v0.3)*: `FromRow` reads directly from the wire buffer into struct fields. No intermediate `Row` type. No hash lookups. No string comparisons.
- **Pipelining** *(planned for v0.2)*: N queries sent on one connection in one round-trip. Same wall-clock latency as N parallel connections, 1/N the connection pressure.
- **Pre-computed column offsets** *(planned for v0.3)*: fixed-width columns (`i32`, `i64`, `bool`, `f64`, `uuid`) have their byte offsets computed at compile time as constants. Only variable-width columns need runtime offset calculation.

---

### 5. mimalloc is the recommended global allocator. *(planned for v0.3)*

For multi-threaded async workloads (which is every non-trivial web server), mimalloc outperforms glibc malloc, jemalloc, and the default Rust allocator. Smaller thread-local heaps, better cache locality, faster small-object allocation. The numbers are not close.

bsql does not bundle or force an allocator. But the documentation, examples, and benchmarks will use mimalloc. The `bsql::recommended_allocator!()` macro will set it up in one line. If you have a reason to use something else, you can. But you probably do not.

**In practice** *(not yet implemented --- planned for v0.3)*:
- `#[global_allocator] static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;` in your `main.rs`.
- Or: `bsql::recommended_allocator!();` --- expands to the above.
- Benchmarks show the difference: mimalloc + arena allocation makes bsql's allocation profile essentially invisible in profiling.

---

### 6. rapidhash for all internal hashing.

Prepared statement names, query cache keys, deduplication hashes, schema fingerprints --- every hash computation in bsql uses rapidhash. It is the fastest hash function for short-to-medium strings (which is what SQL text and identifier names are). Better throughput than FNV-1a, wyhash, ahash, and xxhash3 on inputs under 256 bytes. Better distribution than FNV-1a. Zero cryptographic pretensions --- it is a hash for hash maps and identifiers, not for security.

**In practice:**
- Prepared statement naming: `s_{rapidhash(sql_text):016x}`. 64-bit hash, hex-encoded. Collision probability: ~5.4 * 10^-20 per pair. Negligible.
- The `rapidhash` crate is ~200 lines, no dependencies, no unsafe beyond the core hash function. Cost justified.
- FNV-1a (mentioned in early spec drafts) is replaced everywhere. rapidhash is strictly superior for our input profile.

---

### 7. bitcode for all persistence and serialization. *(planned for v0.2)*

The offline schema cache (`.bsql/`), incremental validation state, any data that bsql writes to disk --- all serialized with bitcode. Not JSON. Not CBOR. Not MessagePack. Not bincode.

bitcode is the fastest and most compact binary serialization format in the Rust ecosystem. ~10x faster than serde_json for serialize/deserialize. ~3x more compact than JSON. ~1.5x more compact than bincode. Zero-copy deserialization support. The compile-time cache is read on every `cargo build` --- speed here directly reduces developer iteration time.

**In practice** *(not yet implemented --- v0.1 validates against a live PG instance; offline bitcode cache planned for v0.2)*:
- `.bsql/schema.bitcode` and `.bsql/queries.bitcode` replace the JSON files from the spec.
- `bsql prepare` writes bitcode. The proc macro reads bitcode. No JSON parsing during compilation.
- Schema cache for 50 queries loads in ~100us (bitcode) vs ~5ms (JSON). 50x improvement on every build.

---

### 8. Zero allocations where possible. *(partially implemented; arena planned for v0.3)*

`fetch_one` and `fetch_optional` return stack-allocated structs. No heap allocation for the result container. *(Currently uses owned `String`/`Vec` fields; arena-borrowed strings planned for v0.3.)*

Multi-row results will use the arena. All rows from a single query share one arena. When the result set is dropped, one deallocation frees everything.

Owned `String` only when data escapes the arena's lifetime --- and even then, consider whether the architecture can be restructured so it does not need to escape.

**In practice** *(arena allocation not yet implemented --- planned for v0.3)*:
- A typical web handler (query -> serialize to JSON -> respond) never converts arena strings to owned `String`. The serializer borrows from the arena. The arena lives until the response is sent. Zero string allocations.
- `Vec<T>` for `fetch_all` is the only heap allocation in the common path. The `T` values inside contain arena-borrowed strings.
- The arena is recycled from a thread-local pool. The arena object itself is never heap-allocated.

---

### 9. Dependencies are liabilities.

Every crate in `Cargo.toml` is an attack surface, a compile-time cost, a version conflict, and a maintenance burden. Dependencies are not badges of honor. They are debt.

**In practice:**
- Core runtime has 5 dependencies. Count them. Keep them minimal.
- Every dependency has `default-features = false`. Only the features actually used are enabled.
- Before adding a crate: can the standard library do it? Can we write 50 lines instead of pulling in 50,000? Is the crate maintained? When was the last commit? How many transitive dependencies does it bring?
- Periodic dependency audit. If something can be removed, it is removed.

---

### 10. Dynamic does not mean unchecked.

Optional clauses (`[AND col = $param: Option<T>]`) expand to 2^N SQL variants at compile time. Each variant is a complete, valid SQL statement. Each variant is independently validated against PostgreSQL via `PREPARE`. Each variant gets its own prepared statement at runtime.

The runtime dispatcher is a `match` on a bitflag --- one arm per combination, each arm pointing to a pre-validated query. No string concatenation. No SQL injection surface. No runtime parsing.

**In practice:**
- 4 optional filters = 16 variants. All 16 validated at compile time. All 16 prepared on first use.
- The variant selector is `match (a.is_some(), b.is_some(), c.is_some(), d.is_some())`. Compiles to a jump table. < 5ns dispatch overhead.
- Sort enums (`$[sort: EnumType]`) are spliced and validated per variant. The enum is exhaustive --- no default case, no fallback, no "unknown" sort.

---

### 11. Inline is king.

The query lives where it is used. In the function that calls it. Not in a `.sql` file across the repository. Not in a `queries/` directory. Not in a generated bindings file. Right here, in the Rust code, next to the business logic that needs the data.

**In practice:**
- `bsql::query! { SELECT ... }` in the handler function. The SQL is visible without file-hopping.
- IDE support: rust-analyzer expands the macro, autocompletes field names, shows types. The query is not a second-class citizen hidden in a separate file.
- Code review: the reviewer sees the SQL and the Rust that uses it in the same diff hunk.

---

### 12. Async and parallel by design.

Not bolted on. Not "supports async if you add the right feature flag." The entire architecture assumes tokio, async/await, and concurrent connection usage. Because that is how every production Rust web application works.

**In practice:**
- All execution methods (`.fetch_one()`, `.fetch_all()`, `.execute()`) are async.
- Connection pooling (deadpool-postgres, upgradeable to custom LIFO pool) is built in.
- `bsql::pipeline!` sends N queries on one connection in one round-trip.
- The proc macro shares a connection pool across invocations within a single `cargo build` --- parallel macro expansion validates concurrently.

---

### 13. Errors are first-class citizens.

A confusing error message is a bug. Error quality is a feature, not a nice-to-have.

**In practice:**
- Column typos: `"column 'naem' not found --- did you mean 'name'?"` with Levenshtein suggestions.
- Type mismatches: `"expected i32 (column tickets.id is INTEGER NOT NULL), found &str"` with exact span pointing.
- Missing tables: `"table 'tcikets' not found --- did you mean 'tickets'?"` with available tables listed.
- Optional clause errors: points to the specific clause and explains why the variant is invalid.
- All errors point to the exact token in the source file. Not the macro invocation. The token.
- `BsqlError` at runtime has variants: `Pool`, `Query`, `Decode`, `Connect`. Pattern matching, not string parsing.

---

### 14. Doc-tests are the contract.

Every public API has a doc-test. The doc-test compiles, runs, and demonstrates correct usage. If the doc-test fails, the release does not ship. If the doc-test is wrong, the API is wrong.

**In practice:**
- README examples are extracted from doc-tests. One source of truth.
- The `examples/` directory is generated from doc-tests. Not the other way around.
- CI runs `cargo test --doc` on every commit. A doc-test failure blocks the merge.
- Doc-tests use real PostgreSQL (via testcontainers or the CI database). No mocking.

---

### 15. No blind spots.

Every nullable column is `Option<T>`. Every parameter type is verified against `pg_catalog`. Every column in `SELECT *` is resolved to concrete types. Every cast is checked. Every function return type is looked up. No silent failures. No implicit conversions. No "it'll probably work."

**In practice:**
- `SELECT *` resolves to explicit columns at compile time. If a column is added to the table, the generated struct gains a field on the next build.
- `status::text` --- the cast target `text` is verified. `status::bogus` is a compile error.
- `COUNT(*)` returns `i64`, not "some integer." `SUM(nullable_col)` returns `Option<T>`, because it is nullable even if the column is NOT NULL (empty result set).
- Aggregate functions over nullable columns are `Option<Option<T>>`: outer Option for empty result, inner for NULL values. No, this is not over-engineered. This is correct.

---

### 16. Total query knowledge is a superpower.

bsql has no escape hatch. This means it sees every query the application executes --- at compile time and at runtime. This complete visibility enables optimizations that are fundamentally impossible in libraries with backdoors: singleflight coalescing, automatic cache invalidation, read/write splitting without annotations, cross-query deadlock detection, and N+1 batching. The no-escape-hatch design is not just a safety feature. It is a performance feature.

---

### 17. Fail fast. Never wait and hope.

Timeouts are an admission of helplessness: "I don't know how long this will take, so I'll just cut it off." bsql does not wait and hope.

**Fail-fast, not timeout:**
- Pool exhausted → immediate `PoolExhausted` error. Not "wait 5 seconds and maybe a connection frees up." The caller decides what to do (retry, fallback, 503). A thousand handlers blocked for 5 seconds each is not a timeout strategy --- it is a denial-of-service against yourself.
- Deadlock (transaction holds connection, needs another from the same full pool) → immediate error, not silent hang.
- Dropped transaction without commit/rollback → connection marked dirty, discarded from pool, warning logged. The next pool user gets a clean connection, not a corrupted one.

**Where timeout is unavoidable** (external systems we do not control):
- TCP connect to PostgreSQL --- the network may be down. `connect_timeout` exists because TCP itself will wait forever. This is the only legitimate timeout in bsql.
- PostgreSQL's own `statement_timeout` --- set via `session_init`, enforced by PG, not by bsql. If a query takes too long, PG kills it. bsql reports the error.

Timeouts are not a design pattern. They are a last resort for external boundaries. Inside bsql's own code, every operation either succeeds, fails immediately, or is bounded by a resource it controls.

