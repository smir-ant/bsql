# sasql — Safe SQL for Rust

> **sasql** = "Safe SQL" / "Smirnov Anton SQL"
>
> **Core promise: if it compiles, every SQL query is correct.**
>
> No runtime SQL errors. No escape hatches. No discipline required.

---

## Credo

This library exists because the Rust ecosystem failed to deliver on a simple promise: **if it compiles, it works.** Every existing SQL library has a backdoor. A function that bypasses the type system. An escape hatch that lets you write raw SQL that might crash at 3 AM on a production server with 700,000 tickets and an angry customer on the phone.

sasql has no backdoor. There is no `unsafe_query()`. There is no `raw_sql()`. There is no `trust_me_bro()`. There is `query!` — validated at compile time, every parameter typed, every column checked, every NULL mapped to `Option<T>`. If it compiles, the SQL is correct.

This is not a convenience library. This is a safety guarantee.

The night you lose production data to a typo in a SQL string — a column renamed in a migration but missed in one handler, an `i64` passed where the schema expects `i32`, a nullable column read as `NOT NULL` because the ORM swallowed the distinction — that is the night you understand why sasql exists. Not because SQL is hard. Because the tools that promised to protect you had a side door, and Murphy walked through it at the worst possible moment.

Every alternative says "be disciplined." Don't call the unchecked function. Don't forget to update the query when the schema changes. Don't mix up column indices.

sasql says: **discipline is not a safety mechanism.** The only safety mechanism is a compiler that refuses to produce a binary containing invalid SQL. There is no unchecked function to be disciplined about. There is no escape hatch to avoid. There is one macro. It checks everything. If it passes, the SQL is correct. If it fails, the binary is not produced. There is no third state.

---

## The Problem

Every existing Rust SQL tool has a fatal flaw.

**sqlx** — The `query!` macro validates SQL at compile time. But `query()` — the unchecked
runtime function — is right there, same import path, same module. Nothing prevents using it.
Clippy cannot distinguish a function call from a macro invocation. One `query()` slipped past
review, and your "compile-time safe" codebase has a runtime SQL error waiting in production.

**Diesel** — Strong type system, excellent model. But complex SQL — CTEs, window functions,
`DISTINCT ON`, keyset pagination, `LATERAL` joins — cannot be expressed in the DSL.
`sql_query()` is the escape hatch. It accepts raw strings. It is unchecked. Every Diesel
project of sufficient complexity eventually uses it.

**SeaORM** — Runtime query builder. Zero compile-time SQL checking. Errors surface when the
query hits PostgreSQL, not when the code compiles. This is no better than string concatenation
with extra steps.

**Cornucopia** — 100% compile-time safe. No escape hatch. But SQL lives in separate `.sql`
files, disconnected from the Rust code that uses it. At scale, navigating between dozens of
`.sql` files and their generated bindings is painful. No dynamic query support — every
conditional filter requires a separate `.sql` file.

**tokio-postgres** — Raw driver. Zero safety. Manual row mapping by column index. A renamed
column silently returns wrong data.

The gap is clear:

| Tool | Inline SQL | 100% Safe | Dynamic Queries | No Escape Hatch |
|------|:----------:|:---------:|:---------------:|:---------------:|
| sqlx | yes | no | no | **no** |
| Diesel | no (DSL) | no | partial (DSL) | **no** |
| SeaORM | no (DSL) | no | partial (DSL) | yes (no raw API) |
| Cornucopia | no (.sql files) | yes | no | yes |
| tokio-postgres | yes | no | no | no |
| **sasql** | **yes** | **yes** | **yes** | **yes** |

No existing tool gives you all four. sasql does.

---

## The Solution

sasql is a proc macro library that:

1. **Validates SQL against a real PostgreSQL instance at compile time** — like sqlx, but
   without the escape hatch.
2. **Has no unchecked runtime SQL API** — there is no `sasql::query()` function. Only
   `sasql::query!()` macro. You cannot construct or execute unchecked SQL through sasql.
   The unsafe path does not exist.
3. **Supports dynamic/conditional SQL via compile-time verified fragments** — unlike
   Cornucopia, where every query variant requires a separate file.
4. **Writes SQL inline in Rust** — unlike Cornucopia's `.sql` files. The query lives next to
   the code that uses it.
5. **Generates code equivalent to hand-written tokio-postgres** — zero-cost abstraction.
   No intermediate `Value` types, no runtime reflection, no serde overhead.

---

## Design Principles

1. **No escape hatch. Period.** If it compiles, the SQL is correct. Not "probably correct". Not "correct if you used the right function". Correct. Always. There is no `raw_query()` hidden in a submodule. There is no `#[allow(unchecked)]` annotation. There is no "advanced users only" API that bypasses validation. The attack surface for runtime SQL errors is zero — not small, not minimized, *zero*. This is the load-bearing promise. Everything else in this document serves it.

2. **The fastest safe code is the same speed as unsafe code.** Arena allocation, binary protocol, zero-copy deserialization, SIMD string processing — these are not premature optimization. They are the architecture. When someone asks "what's the performance cost of compile-time safety?", the answer is negative. sasql's generated code is faster than hand-written `query()` calls because the proc macro has information the runtime never will: exact column types, exact parameter counts, exact result shapes. It generates tighter code than a human would write. Performance is not bolted on. It is designed in from the first line.

3. **SQL is the query language. Rust is the host language.** sasql does not invent a DSL. It does not provide `.filter()` methods that approximate `WHERE` clauses. It does not have a query builder that produces SQL as a side effect. You write SQL — real PostgreSQL SQL, with CTEs and window functions and `LATERAL` joins — and the macro validates it against the actual database. The contract is: you bring the SQL knowledge, sasql brings the compile-time guarantee. Neither tries to do the other's job.

4. **Dynamic does not mean unchecked.** Optional clauses are expanded at compile time into every concrete SQL variant. Each variant is independently validated against the running PostgreSQL instance. The runtime dispatcher is a `match` on an enum — one arm per combination, each arm pointing to a pre-validated, pre-prepared statement. No string concatenation. No SQL injection surface. No runtime parsing. The query that runs at 3 AM is the same query that was validated at build time. Every time. Every variant.

5. **Dependencies are liabilities.** Every crate in `Cargo.toml` is an attack surface, a compile-time cost, a version conflict waiting to happen, and a maintenance burden that compounds over years. sasql's core has 5 runtime dependencies. Not 50. Not 15. Five. Each one was chosen because the alternative — implementing it from scratch — would be worse by every measurable metric. If a dependency stops earning its keep, it gets replaced. The dependency list is not a résumé of the Rust ecosystem. It is a liability ledger, and every entry must justify its cost.

6. **Every nanosecond matters.** Not because users notice nanoseconds. Because the mindset that says "nanoseconds don't matter" is the same mindset that produces millisecond-level bloat through a thousand "doesn't matter" decisions. Each allocation that could be a pointer bump. Each text-format integer that could be a 4-byte memcpy. Each round-trip that could be pipelined. Individually invisible. Collectively, they are the difference between a library that benchmarks within 10% of raw C and one that benchmarks within 200% "but that's fine for most use cases." sasql does not build for "most use cases." It builds for the use case where performance is the requirement.

7. **Doc-tests are the contract.** Every public API has a doc-test that compiles, runs, and demonstrates the correct usage. The doc-test is not a suggestion. It is a specification. If the doc-test fails, the release does not ship. If the doc-test demonstrates incorrect usage, the API is broken. The README is generated from doc-tests. The examples directory is generated from doc-tests. There is one source of truth, and it runs in CI.

---

## Requirements

### R1: Extreme Performance

The generated code must be indistinguishable from hand-written tokio-postgres in a profiler.

- **Prepared statements**: every query is prepared once, cached by the connection. Amortized
  parse cost is zero.
- **Direct deserialization**: row data is decoded directly into user structs. No intermediate
  `serde_json::Value`, no `Row::get()` by string name. The proc macro generates positional
  column access (`row.get(0)`, `row.get(1)`, ...) matched to struct fields at compile time.
- **Connection pooling**: `deadpool-postgres` by default. The pool type is opaque — future
  versions may use a custom pool if benchmarks justify it.
- **No serde in the hot path**: `FromRow` is a custom derive, not `Deserialize`. serde is
  optional, feature-gated, and only used if the caller wants `Serialize`/`Deserialize` on
  generated result structs.
- **Minimal allocations**: `String` columns decode directly from the wire buffer where
  possible. Small result sets (`fetch_one`, `fetch_optional`) are stack-allocated structs.
  `fetch_all` returns `Vec<T>` — no arena indirection unless profiling proves it worthwhile.
- **JSONB via sonic-rs**: feature-gated. When enabled, JSONB columns deserialize through
  sonic-rs (SIMD-accelerated JSON) instead of serde_json. The proc macro generates the correct
  deserialization call based on the feature flag.

**Non-goals**: custom allocators (jemalloc, mimalloc) are the caller's decision, not the
library's. sasql does not bundle or configure allocators.

### R2: Compile-Time Guarantee

This is the load-bearing requirement. Everything else serves this.

- **Every SQL string** passed to `sasql::query!` is sent to a real PostgreSQL instance during
  `cargo build`. The proc macro connects to the database specified by `SASQL_DATABASE_URL`
  (or `DATABASE_URL` as fallback) and executes `PREPARE` to validate syntax, column names,
  table names, type compatibility, and parameter binding.
- **No public API accepts raw SQL at runtime.** The `sasql` crate exports macros and traits.
  It does not export any function that takes `&str` SQL. There is no `query()`, no
  `raw_query()`, no `sql()`. The escape hatch does not exist.
- **Type mapping is verified at compile time.** If a column is `INTEGER NOT NULL`, the
  generated struct field is `i32`. If you write `$id: &str` for that parameter, the macro
  emits a compile error. If the column is nullable, the field is `Option<i32>`. This is
  checked against the actual `pg_catalog` schema, not inferred.
- **Parameter count and types are checked.** `$name: Type` syntax declares parameters inline.
  The macro verifies the parameter count matches the SQL, and each Rust type maps to the
  expected PostgreSQL type.
- **Column existence is verified.** `SELECT naem FROM users` produces a compile error with
  a "did you mean `name`?" suggestion.
- **Offline mode**: `sasql prepare` introspects the database and generates a JSON schema
  cache (`.sasql/`). When `SASQL_OFFLINE=true`, the proc macro validates against this cache
  instead of a live database. This enables CI without a running PostgreSQL instance.

### R3: PostgreSQL First

sasql is a PostgreSQL library. Other databases are future considerations, not design drivers.

**Supported PostgreSQL features:**
- Standard DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `UPSERT` (`ON CONFLICT`)
- `RETURNING` clause on all DML
- CTEs: `WITH`, `WITH RECURSIVE`
- Window functions: `ROW_NUMBER()`, `RANK()`, `LAG()`, `LEAD()`, etc.
- `DISTINCT ON`
- `LATERAL` joins
- Subqueries (scalar, `EXISTS`, `IN`, `ANY`, `ALL`)
- Set operations: `UNION`, `INTERSECT`, `EXCEPT`
- Full type system: enums, arrays, JSONB, composite types, ranges, `RECORD`
- `NOTIFY` / `LISTEN`
- Advisory locks
- Full-text search (`tsvector`, `tsquery`, `to_tsvector()`, `@@`)
- `pg_trgm` similarity search
- `COPY` (future — complex wire protocol interaction)

**Target version**: PostgreSQL 15+ (minimum). Optimized for PG 18.

**Future database support** (feature-gated, not in v1.0):
- SQLite via `feature = "sqlite"` — separate type mapping, different validation path.
- MySQL via `feature = "mysql"` — lowest priority. The SQL dialect divergence is significant.

These are listed for architectural awareness. The codebase will not prematurely abstract
for databases that are not yet supported.

### R4: Developer Experience

- **SQL inline in Rust**: the query lives where it is used. No jumping between files.
- **IDE-friendly**: the proc macro generates named structs with typed fields. rust-analyzer
  can autocomplete `result.field_name` after the macro expands.
- **Clear error messages**: the proc macro captures PostgreSQL's error output and maps it
  back to the Rust source span. Column typos get "did you mean?" suggestions via
  Levenshtein distance against the actual schema.
- **Migration support**: `sasql migrate` generates timestamped migration files, tracks
  applied migrations in a `_sasql_migrations` table, and runs pending migrations.
  Migrations are plain `.sql` files — no Rust DSL.
- **Schema introspection**: `sasql schema` dumps the current database schema as a
  human-readable report (tables, columns, types, constraints, indexes).

### R5: Pure SQL, No DSL

Write PostgreSQL SQL. Not a Rust approximation of SQL.

There is no query builder. No method chaining. No `.filter()`, `.select()`, `.join()`.
If you know PostgreSQL SQL, you know sasql. The macro is a validator and code generator,
not a query language.

This is a deliberate constraint. DSLs inevitably diverge from the SQL they model. They
cannot express the full power of PostgreSQL without escape hatches. sasql avoids this
problem by not having a DSL at all.

### R6: Dynamic Queries — The Differentiator

This is what no other Rust SQL library does correctly.

Real-world queries have conditional filters. A ticket list might filter by status, or
department, or assignee, or any combination. In sqlx, you either:
- Write `WHERE ($1::int IS NULL OR department_id = $1)` — which defeats index usage.
- Write separate `query!` invocations for each combination — which is O(2^n) boilerplate.
- Drop to unchecked `query()` with string concatenation — which defeats the purpose of sqlx.

sasql solves this with **optional clauses**: SQL fragments wrapped in `[]` brackets that
are included or excluded at runtime based on `Option` parameters.

```rust
let tickets = sasql::query! {
    SELECT t.id, t.title, t.status::text
    FROM tickets t
    WHERE t.deleted_at IS NULL
    [AND t.department_id = $dept: Option<i32>]
    [AND t.assignee_id = $assignee: Option<i32>]
    [AND t.category_id = $cat: Option<i32>]
    ORDER BY $[sort: TicketSort]
    LIMIT $limit: i64
}.fetch_all(&pool).await?;
```

**How it works:**

1. The proc macro parses the SQL and identifies optional clauses (`[...]`).
2. It generates **every combination** of included/excluded clauses. For 3 optional clauses,
   that is 2^3 = 8 SQL variants.
3. **Each variant is validated** against the live PostgreSQL instance at compile time.
4. At runtime, the generated code inspects which `Option` parameters are `Some` and selects
   the correct pre-validated, pre-prepared SQL variant.
5. The prepared statement for each variant is cached by the connection, so subsequent
   executions skip parsing entirely.

**Combinatorial explosion mitigation**: with N optional clauses, 2^N variants are generated.
This is acceptable for N <= 8 (256 variants). For N > 8, the macro emits a compile error
suggesting the query be split. In practice, queries rarely have more than 5-6 optional
filters.

**Sort enums**: the `$[sort: EnumType]` syntax allows compile-time verified dynamic
`ORDER BY` clauses:

```rust
#[sasql::sort]
enum TicketSort {
    #[sql("t.updated_at DESC, t.id DESC")]
    UpdatedAt,
    #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
    Deadline,
    #[sql("t.id DESC")]
    Id,
}
```

Each variant's SQL is spliced into the query and validated. The enum is exhaustive — there
is no "default" or "unknown" sort. Invalid sort values are a compile error.

**Conditional joins** (future, post-v0.3): the `[]` syntax may extend to `JOIN` clauses,
enabling compile-time verified optional joins. This requires careful handling of column
availability — columns from an optional join are only available in other optional clauses
that are co-dependent.

### R7: Testing

- **Proc macro unit tests**: every parse, validate, and codegen path tested.
- **Integration tests**: against a real PostgreSQL instance. Docker Compose provided.
- **Property-based tests**: type mapping verified with `proptest` — random Rust values
  round-trip through PG and back.
- **Compile-fail tests**: `trybuild` verifies that invalid queries produce the expected
  compile errors with the expected error messages.
- **Fuzz testing**: `cargo-fuzz` on the SQL parser to ensure it does not panic on malformed
  input.
- **Benchmark suite**: `criterion` benchmarks comparing sasql to tokio-postgres, sqlx, and
  Diesel on identical queries.

### R8: Minimal Dependencies

**Core (always required):**

| Crate | Purpose | Justification |
|-------|---------|---------------|
| `tokio-postgres` | PG wire protocol | Cannot be replaced without reimplementing PG protocol |
| `deadpool-postgres` | Connection pooling | Lightest async PG pool available |
| `postgres-types` | PG type system (ToSql, FromSql) | Companion to tokio-postgres |
| `proc-macro2` | Proc macro token handling | Required for proc macros |
| `quote` | Rust code generation | Required for proc macros |
| `syn` | Rust syntax parsing | Required for proc macros |
| `tokio` | Async runtime | Re-exported, not bundled — caller provides |

**Optional (feature-gated):**

| Crate | Feature Flag | Purpose |
|-------|-------------|---------|
| `sonic-rs` | `json` | SIMD-accelerated JSONB deserialization |
| `time` | `time` | Timestamp / date / time types |
| `chrono` | `chrono` | Alternative timestamp types |
| `uuid` | `uuid` | UUID type |
| `rust_decimal` | `decimal` | DECIMAL / NUMERIC type |
| `serde` | `serde` | Serialize/Deserialize on generated structs |
| `ipnetwork` | `net` | INET / CIDR types |
| `bit-vec` | `bit` | BIT / VARBIT types |

**No default features** except `time` (timestamps are ubiquitous in real applications).
Every other feature is opt-in.

---

## Architecture

```
sasql/
├── sasql/                  # Main crate (user-facing, re-exports everything)
│   ├── src/lib.rs          # pub use sasql_macros::query; pub use sasql_core::*;
│   └── Cargo.toml
│
├── sasql-macros/           # Proc macro crate (compile-time only)
│   ├── src/
│   │   ├── lib.rs          # #[proc_macro] query!, #[proc_macro_derive] sort
│   │   ├── parse.rs        # SQL tokenizer + AST (minimal, PG-aware)
│   │   ├── validate.rs     # Connect to PG, PREPARE, introspect types
│   │   ├── codegen.rs      # Generate Rust struct + impl + execute methods
│   │   ├── dynamic.rs      # Expand optional clauses, generate variant dispatcher
│   │   ├── types.rs        # PG OID → Rust type mapping table
│   │   ├── error.rs        # Diagnostic formatting, "did you mean?" suggestions
│   │   └── cache.rs        # Read/write .sasql/ offline validation cache
│   └── Cargo.toml
│
├── sasql-core/             # Runtime support (minimal footprint)
│   ├── src/
│   │   ├── lib.rs
│   │   ├── pool.rs         # Pool<Postgres> — thin wrapper over deadpool
│   │   ├── transaction.rs  # Transaction wrapper with begin/commit/rollback
│   │   ├── row.rs          # FromRow trait (generated by proc macro, not user-implemented)
│   │   ├── error.rs        # SasqlError: Pool | Query | Decode | Connect
│   │   └── types.rs        # Re-exports of postgres-types traits
│   └── Cargo.toml
│
├── sasql-cli/              # CLI binary
│   ├── src/
│   │   ├── main.rs         # Subcommand dispatch
│   │   ├── prepare.rs      # Introspect DB → .sasql/ cache
│   │   ├── schema.rs       # Dump DB schema
│   │   └── migrate.rs      # Migration runner
│   └── Cargo.toml
│
└── tests/                  # Integration tests (require PG)
    ├── basic.rs            # Simple SELECT, INSERT, UPDATE, DELETE
    ├── dynamic.rs          # Optional clauses, sort enums
    ├── types.rs            # Type mapping round-trips
    ├── transactions.rs     # Transaction commit/rollback
    ├── compile_fail/       # trybuild tests for expected compile errors
    └── docker-compose.yml  # PG instance for testing
```

**Crate dependency graph:**
```
sasql (user-facing)
├── sasql-macros (proc macro, compile-time only)
│   ├── syn, quote, proc-macro2
│   └── tokio-postgres (for compile-time validation connection)
└── sasql-core (runtime)
    ├── tokio-postgres
    ├── deadpool-postgres
    └── postgres-types
```

The proc macro crate (`sasql-macros`) has its own `tokio-postgres` dependency for the
compile-time database connection. This is not duplicated at runtime — Cargo deduplicates
identical versions.

---

## Performance Architecture

This is not an optimization pass. This is the architecture. These decisions are baked into the foundation — not sprinkled on top after profiling reveals what everyone already knew: that allocators are slow, text parsing is wasteful, and round-trips compound.

### P1: Arena Allocation Per Request

Every query execution gets a bump allocator. All row data — strings, byte arrays, vectors — is allocated in a contiguous arena. When the response is sent and the result goes out of scope, the arena is dropped. One deallocation for everything. Not one free per string. Not one free per row. One.

**Why this matters:**

A typical 20-row result with 15 TEXT columns produces ~300 individual allocations. Under the system allocator, each `malloc` call costs ~15ns: thread-local cache check, size class lookup, free list traversal, possible page fault. That is 4.5µs of pure allocation overhead on a query that takes 50µs total — 9% of latency spent on memory management.

Arena allocation replaces each `malloc` with a pointer bump: load current offset, add size, store new offset. ~2ns per allocation. 300 pointer bumps = 600ns. The arena itself is pre-allocated at 8KB (covers most result sets without growth), grows in 4KB chunks if needed, and is recycled from a thread-local pool so the arena object itself is never allocated from the heap.

```
System allocator: 300 × malloc(~15ns) + 300 × free(~12ns) = ~8.1µs
Arena allocator:  300 × bump(~2ns)    + 1   × drop(~5ns)  = ~0.6µs
```

The arena is not exposed in the public API. Users receive structs with owned `String` fields (the strings borrow from the arena internally and are promoted to owned on extraction). The lifetime complexity is hidden behind the generated code.

### P2: PostgreSQL Binary Protocol

PostgreSQL supports two wire formats: text (format code 0) and binary (format code 1). Every Rust library defaults to text. This is the wrong default.

**Text format costs:**

| Type | Text wire representation | Bytes | Binary wire representation | Bytes | Parse cost |
|------|-------------------------|-------|---------------------------|-------|------------|
| `i32` | `"12345"` | 5 | `0x00003039` | 4 | integer parse vs. memcpy |
| `i64` | `"1234567890123"` | 13 | 8 bytes big-endian | 8 | integer parse vs. memcpy |
| `bool` | `"t"` or `"f"` | 1 | `0x01` or `0x00` | 1 | char compare vs. zero compare |
| `TIMESTAMPTZ` | `"2026-03-31T12:00:00+00:00"` | 25 | 8-byte i64 (µs since J2000) | 8 | full datetime parse vs. memcpy |
| `UUID` | `"550e8400-e29b-41d4-a716-446655440000"` | 36 | 16 raw bytes | 16 | hex decode + dash strip vs. memcpy |

Binary format eliminates parsing entirely for numeric types. `i32` is `i32::from_be_bytes(bytes[0..4])` — a single instruction on every architecture. Timestamps are 8-byte memcpy + constant offset. UUIDs are 16-byte memcpy.

The bandwidth savings compound: a 100-row result with columns `(i32, i64, timestamptz, uuid, bool)` is ~7,600 bytes in text format vs. ~3,700 bytes in binary. Less data on the wire. Less parsing on arrival. Both directions win.

tokio-postgres already supports binary format — sasql requests it by default for all queries. Text format is available as a per-query opt-in for the rare case where PostgreSQL's binary representation is inconvenient (e.g., `NUMERIC` with arbitrary precision).

### P3: Query Pipelining

PostgreSQL's wire protocol supports pipelining: send N queries before reading any responses, then read N responses in order. This is not batching (one query with multiple value sets). This is true pipelining — independent queries executed with a single round-trip.

**The problem it solves:**

```rust
// Common pattern in web handlers: fetch user, ticket, and unread count
let user    = get_user_by_id(&pool, user_id).await?;       // RTT 1
let ticket  = get_ticket_by_id(&pool, ticket_id).await?;    // RTT 2
let unread  = count_unread(&pool, user_id).await?;           // RTT 3
// Total: 3 × network_latency
```

With `tokio::join!` these run concurrently on separate connections — 1 RTT wall-clock but 3 connections consumed. With pipelining, all three run on a single connection in a single round-trip:

```rust
let (user, ticket, unread) = sasql::pipeline! {
    SELECT id, login, first_name, last_name, role::text
    FROM users WHERE id = $1: i32,

    SELECT id, title, status::text, created_at
    FROM tickets WHERE id = $1: i32,

    SELECT COUNT(*) FROM notifications
    WHERE user_id = $1: i32 AND read_at IS NULL
}.fetch(&pool, (&user_id,), (&ticket_id,), (&user_id,)).await?;
```

**Performance impact:**

| Approach | Connections used | Wall-clock latency |
|----------|:----------------:|:------------------:|
| Sequential | 1 | 3 × RTT |
| `tokio::join!` | 3 | 1 × RTT |
| Pipeline | 1 | 1 × RTT |

At 0.5ms network latency: sequential = 1.5ms, join = 0.5ms but 3 connections, pipeline = 0.5ms on 1 connection. Pipelining is strictly better — same latency as parallel, one-third the connection pressure.

Each query in the pipeline is independently validated at compile time. Each gets its own typed result struct. The pipeline macro generates a single async operation that writes all queries to the socket, then reads all responses in order, deserializing each into its respective type.

### P4: SIMD String Processing

When bytes arrive from PostgreSQL, they must be validated and transformed before they become Rust types. The naive approach — scalar byte-by-byte processing — leaves performance on the table.

**UTF-8 validation** on TEXT/VARCHAR columns: Every string from PostgreSQL must be valid UTF-8 (PostgreSQL guarantees this for `UTF8` encoding, but sasql validates defensively). Scalar UTF-8 validation processes ~3 GB/s. SIMD validation via `simdutf` processes ~70 GB/s — a 23x improvement. On a 100-row result with 10 TEXT columns averaging 50 bytes each, that is 50KB of validation: 17µs scalar vs. 0.7µs SIMD.

**HTML escaping** for template engines consuming sasql results: SIMD-accelerated scanning for `<>&"'` characters. The common case (no special characters) is handled in bulk — 32 bytes per instruction cycle. Only when a special character is found does the code drop to scalar replacement.

**JSONB columns** (with `feature = "json"`): deserialized via `sonic-rs`, which uses SIMD for JSON structural scanning. 2-3x faster than `serde_json` on typical JSONB payloads.

**Enum string matching**: when mapping TEXT columns to Rust enums (e.g., `status::text` → `TicketStatus`), `memchr`-based scanning finds the discriminating byte faster than `str::eq` chains. For a 6-variant enum, the match is 2 comparisons on average instead of 3.

SIMD features are compile-time detected via `#[cfg(target_feature)]` — no runtime dispatch overhead, no fallback check on every call. On targets without SIMD (unlikely in 2026, but possible), the code falls back to scalar automatically.

### P5: Prepared Statement Management

Every query that passes through `sasql::query!` is a prepared statement. But preparation is not free — it requires a PG round-trip, parse, and plan. sasql amortizes this to zero.

**Statement naming**: each statement is named by the FNV-1a hash of its SQL text. FNV-1a is fast (single pass, no multiplication) and collision-resistant for short inputs. A 200-byte SQL string hashes in ~20ns. The name is a hex string of the 64-bit hash — `s_a1b2c3d4e5f67890`.

**Lifecycle**: statements are prepared once per connection on first use, then cached for the lifetime of that connection. When a connection is returned to the pool, its statements persist — the next borrow finds them already prepared. There is no per-borrow setup cost.

**Memory impact**: each prepared statement in the PG backend costs ~1KB (parse tree + plan). For a 50-query application with a 32-connection pool: 50 statements × 1KB = 50KB per connection, 1.6MB total. Negligible.

**Pre-warming**: when `Pool::connect()` is called, sasql can optionally prepare all known statements on each initial connection. This moves the first-use preparation cost from request time to startup time. For a 50-query app, this is ~50 PREPARE round-trips per connection — ~25ms per connection at 0.5ms RTT. Done once at startup, never again.

### P6: Zero-Copy Row Deserialization

The generated `FromRow` implementation does not go through an intermediate `Row` struct. There is no `row.get::<String>(0)` followed by a field assignment. The deserialization reads directly from the wire buffer into the target struct fields.

**How it works with binary protocol:**

```rust
// Generated code (simplified) for: SELECT id, login, active FROM users
impl FromRow for UserRow {
    fn from_row(buf: &[u8], col_offsets: &[u32]) -> Self {
        Self {
            id:     i32::from_be_bytes(buf[offsets[0]..offsets[0]+4]),
            login:  arena.alloc_str(&buf[offsets[1]..offsets[1]+len1]),
            active: buf[offsets[2]] != 0,
        }
    }
}
```

The column offsets are computed once when the row header is parsed (PostgreSQL's binary format prefixes each column value with a 4-byte length). After that, each field read is an indexed slice of the buffer — no hash lookups, no string comparisons, no virtual dispatch.

**For nullable columns**, the length prefix is -1 (0xFFFFFFFF) for NULL. The generated code checks this before reading:

```rust
middle_name: if col_lengths[3] == -1 { None } else {
    Some(arena.alloc_str(&buf[offsets[3]..offsets[3]+len3]))
},
```

The generated code is `#[inline(always)]` — the compiler sees through the function boundary and optimizes the entire deserialization into a linear sequence of loads from the buffer. No branches except NULL checks. No allocations except arena bumps for strings.

### P7: Connection Pool Optimization

deadpool-postgres is the default pool in v0.1. It is adequate. It is not optimal. sasql's architecture allows replacing it without changing user-facing API, and the roadmap includes a custom pool with these properties:

**LIFO ordering**: return the most recently used connection first. PostgreSQL's backend process maintains query plan caches, prepared statement caches, and working memory. A LIFO pool reuses the "warmest" connection — the one most likely to have relevant plans and buffers in memory. FIFO pools (deadpool's default) rotate through all connections equally, keeping none warm.

**Minimal health checks**: check connection liveness only when idle for >30 seconds. Not on every acquire. A `SELECT 1` health check takes ~200µs — negligible once, but at 1,000 requests/second that is 200ms of aggregate time per second wasted asking the database "are you still there?" The database is still there. It was still there 50ms ago when the last query ran.

**Jittered recycling**: connections have a max lifetime (default: 30 minutes) with ±10% random jitter. Without jitter, all connections created at startup expire simultaneously — a thundering herd of reconnections, re-preparations, and brief pool exhaustion. Jitter spreads the recycling over a 6-minute window.

**Pre-warming on creation**: new connections immediately prepare all known statements before entering the pool. The first request to use a new connection pays zero preparation cost.

**Implementation**: built on `crossbeam::channel` (bounded, LIFO via stack discipline) rather than `tokio::sync::Semaphore` (deadpool's approach). crossbeam channels are faster for the acquire/release pattern: ~50ns vs. ~200ns per operation. At 10,000 requests/second, this saves 1.5ms of aggregate lock contention per second.

### P8: Compile-Time Optimizations

The proc macro itself must be fast. Slow compile times are a tax on every developer, every `cargo build`, every CI run. sasql treats macro execution time as a performance metric.

**Connection reuse**: the proc macro maintains a shared tokio runtime and connection pool across macro invocations within a single `cargo build`. The first `sasql::query!` invocation pays the connection cost (~5ms). Subsequent invocations reuse the connection. For a 50-query project: 5ms + 49 × 0.5ms = ~30ms total validation time, vs. 50 × 5ms = 250ms if each invocation connected independently.

**Parallel validation**: when the proc macro encounters multiple queries in the same compilation unit, it validates them concurrently using the pooled connection. tokio-postgres supports pipelining within the proc macro — 50 PREPARE statements pipelined in a single batch complete in ~25ms instead of ~25ms × 50 sequential round-trips.

**Incremental revalidation**: the proc macro tracks a mapping of `(file_path, byte_offset) → query_hash`. On recompilation, only queries whose hash has changed are re-validated. Editing one query in a 50-query project revalidates 1 query, not 50.

**Offline cache format**: when `SASQL_OFFLINE=true`, the proc macro reads validation results from `.sasql/` in a minimal binary format — fixed-size records with pre-computed type information. No JSON parsing during compilation. The binary format loads in ~100µs for 50 queries. sqlx's JSON-based `.sqlx/` cache takes ~5ms for equivalent data.

---

## API Design

### Pool and Connection

```rust
use sasql::Pool;

// Connect with a URL
let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

// Connect with explicit config
let pool = Pool::builder()
    .host("localhost")
    .port(5432)
    .dbname("mydb")
    .user("app")
    .password("secret")
    .max_size(16)          // max connections
    .connect_timeout(5)    // seconds
    .build()
    .await?;
```

### Basic SELECT

```rust
// Fetch one row (error if zero or multiple rows)
let user = sasql::query! {
    SELECT id, login, first_name, last_name, role::text
    FROM users
    WHERE id = $id: i32
}.fetch_one(&pool).await?;

// Generated struct has typed fields:
// user.id: i32
// user.login: String
// user.first_name: String
// user.last_name: String
// user.role: String

// Fetch optional (None if no rows, error if multiple)
let user = sasql::query! {
    SELECT id, login FROM users WHERE login = $login: &str
}.fetch_optional(&pool).await?;
// Returns Option<{id: i32, login: String}>

// Fetch all
let users = sasql::query! {
    SELECT id, login FROM users WHERE active = true ORDER BY login
}.fetch_all(&pool).await?;
// Returns Vec<{id: i32, login: String}>
```

### INSERT with RETURNING

```rust
let ticket = sasql::query! {
    INSERT INTO tickets (title, description, status, created_by_user_id)
    VALUES ($title: &str, $desc: &str, 'new', $creator: i32)
    RETURNING id, created_at
}.fetch_one(&pool).await?;

// ticket.id: i32
// ticket.created_at: OffsetDateTime (with feature = "time")
```

### UPDATE and DELETE

```rust
// execute() returns the number of affected rows
let affected = sasql::query! {
    UPDATE tickets
    SET status = $status: &str, updated_at = NOW()
    WHERE id = $id: i32
}.execute(&pool).await?;
// affected: u64

// DELETE with RETURNING
let deleted = sasql::query! {
    DELETE FROM notifications
    WHERE user_id = $uid: i32 AND created_at < NOW() - INTERVAL '3 days'
    RETURNING id
}.fetch_all(&pool).await?;
```

### Transactions

```rust
let mut tx = pool.begin().await?;

let ticket_id = sasql::query! {
    INSERT INTO tickets (title, status, created_by_user_id)
    VALUES ($title: &str, 'new', $uid: i32)
    RETURNING id
}.fetch_one(&mut tx).await?.id;

sasql::query! {
    INSERT INTO ticket_events (ticket_id, user_id, event_type, comment)
    VALUES ($ticket_id: i32, $uid: i32, 'created', $comment: &str)
}.execute(&mut tx).await?;

tx.commit().await?;
// If tx is dropped without commit(), it rolls back automatically.
```

### CTEs and Complex Queries

```rust
let accessible = sasql::query! {
    WITH user_ticket_access AS (
        SELECT t.id
        FROM tickets t
        WHERE t.created_by_user_id = $uid: i32
        UNION
        SELECT tp.ticket_id
        FROM ticket_participants tp
        WHERE tp.user_id = $uid: i32
        UNION
        SELECT t.id
        FROM tickets t
        JOIN user_departments ud ON ud.department_id = t.department_id
        WHERE ud.user_id = $uid: i32
    )
    SELECT t.id, t.title, t.status::text, t.created_at
    FROM tickets t
    JOIN user_ticket_access ua ON ua.id = t.id
    ORDER BY t.updated_at DESC
    LIMIT $limit: i64
}.fetch_all(&pool).await?;
```

### Window Functions

```rust
let ranked = sasql::query! {
    SELECT
        id,
        title,
        department_id,
        ROW_NUMBER() OVER (
            PARTITION BY department_id
            ORDER BY created_at DESC
        ) as row_num
    FROM tickets
    WHERE status = 'new'
}.fetch_all(&pool).await?;
// row_num: i64
```

### Dynamic Queries (Optional Clauses)

```rust
#[sasql::sort]
enum TicketSort {
    #[sql("t.updated_at DESC, t.id DESC")]
    UpdatedAt,
    #[sql("t.deadline ASC NULLS LAST, t.id ASC")]
    Deadline,
    #[sql("t.id DESC")]
    Id,
}

let tickets = sasql::query! {
    SELECT
        t.id,
        t.title,
        t.status::text,
        d.name AS dept_name,
        u.first_name || ' ' || u.last_name AS creator_name
    FROM tickets t
    LEFT JOIN departments d ON t.department_id = d.id
    LEFT JOIN users u ON t.created_by_user_id = u.id
    WHERE t.deleted_at IS NULL
    AND t.status = ANY($statuses: &[&str])
    [AND t.department_id = ANY($depts: Option<&[i32]>)]
    [AND t.assignee_id = $assignee: Option<i32>]
    [AND t.created_by_user_id = $creator: Option<i32>]
    [AND t.title ILIKE '%' || $search: Option<&str> || '%']
    ORDER BY $[sort: TicketSort]
    LIMIT $limit: i64
}.fetch_all(&pool).await?;
```

The macro generates 2^4 = 16 SQL variants (4 optional clauses). Each variant is a valid,
complete SQL statement validated against PostgreSQL at compile time. At runtime, a match
on the `Option` parameters selects the correct prepared statement.

The generated code is conceptually equivalent to:

```rust
// Auto-generated by sasql (simplified)
match (depts.is_some(), assignee.is_some(), creator.is_some(), search.is_some()) {
    (false, false, false, false) => {
        conn.query(STMT_0, &[&statuses, &sort_sql, &limit]).await
    }
    (true, false, false, false) => {
        conn.query(STMT_1, &[&statuses, &depts.unwrap(), &sort_sql, &limit]).await
    }
    // ... 14 more variants
    (true, true, true, true) => {
        conn.query(STMT_15, &[&statuses, &depts.unwrap(), &assignee.unwrap(),
                              &creator.unwrap(), &search.unwrap(), &sort_sql, &limit]).await
    }
}
```

Each `STMT_N` is a constant SQL string with the correct parameter numbering (`$1`, `$2`, ...)
adjusted for the included parameters.

### Batch Operations

```rust
// Batch insert using unnest (PostgreSQL idiom)
sasql::query! {
    INSERT INTO notifications (user_id, ticket_id, notification_type, message)
    SELECT unnest($user_ids: &[i32]), $ticket_id: i32, $ntype: &str, $msg: &str
}.execute(&pool).await?;

// Batch update
sasql::query! {
    UPDATE users
    SET active = false, deactivated_at = NOW()
    WHERE id = ANY($ids: &[i32])
}.execute(&pool).await?;
```

### LISTEN / NOTIFY

```rust
use sasql::Listener;

let mut listener = Listener::connect("postgres://...").await?;
listener.listen("ticket_updates").await?;

loop {
    let notification = listener.recv().await?;
    // notification.channel(): &str
    // notification.payload(): &str
}
```

LISTEN/NOTIFY uses a dedicated connection, not a pooled one. This is by design — the
listener must hold its connection open to receive notifications.

---

## Error Messages

Error quality is a first-class concern. A confusing error message is a bug.

### Column not found

```
error[sasql]: column "naem" not found in table "users"
  --> src/routes/profile.rs:42:5
   |
42 |     SELECT naem FROM users WHERE id = $id: i32
   |            ^^^^ did you mean "name"?
   |
   = note: available columns: id, login, password_hash, first_name, last_name,
           middle_name, email, phone, role, active, created_at, updated_at
```

### Type mismatch

```
error[sasql]: type mismatch for parameter $id
  --> src/routes/tickets.rs:15:42
   |
15 |     WHERE id = $id: &str
   |                     ^^^^ expected i32 (column "tickets.id" is INTEGER NOT NULL),
   |                          found &str
```

### Invalid optional clause

```
error[sasql]: optional clause produces invalid SQL when included
  --> src/routes/tickets.rs:18:5
   |
18 |     [AND t.department_id = $dept: Option<String>]
   |                                          ^^^^^^ column "department_id" is INTEGER,
   |                                                 parameter type must be Option<i32>
```

### Table not found

```
error[sasql]: table "tcikets" not found
  --> src/routes/list.rs:10:10
   |
10 |     FROM tcikets t
   |          ^^^^^^^ did you mean "tickets"?
   |
   = note: available tables: tickets, ticket_events, ticket_participants,
           users, departments, notifications
```

### Too many optional clauses

```
error[sasql]: query has 12 optional clauses (4096 variants)
  --> src/routes/search.rs:5:1
   |
   = note: maximum is 8 optional clauses (256 variants)
   = help: split this query into smaller queries with fewer optional filters
```

### Nullable column without Option

```
error[sasql]: column "deadline" is nullable but result type is not Optional
  --> src/routes/tickets.rs:8:12
   |
 8 |     SELECT deadline FROM tickets WHERE id = $id: i32
   |            ^^^^^^^^ column "tickets.deadline" is TIMESTAMP WITH TIME ZONE (nullable)
   |
   = help: the generated field will be Option<OffsetDateTime>
   = note: if you expected this column to be NOT NULL, check your schema
```

This is a warning, not an error. The macro generates `Option<T>` regardless. The message
informs the developer that their schema allows NULL, which may be unintentional.

---

## Type Mapping

### Default mappings (no feature flags required)

| PostgreSQL | Rust | Notes |
|-----------|------|-------|
| `BOOLEAN` | `bool` | |
| `SMALLINT` (int2) | `i16` | |
| `INTEGER` (int4) | `i32` | |
| `BIGINT` (int8) | `i64` | |
| `REAL` (float4) | `f32` | |
| `DOUBLE PRECISION` (float8) | `f64` | |
| `TEXT`, `VARCHAR`, `CHAR` | `String` | `&str` for parameters |
| `BYTEA` | `Vec<u8>` | `&[u8]` for parameters |
| `BOOLEAN[]` | `Vec<bool>` | All array types follow this pattern |
| `INTEGER[]` | `Vec<i32>` | |
| `TEXT[]` | `Vec<String>` | |
| `OID` | `u32` | |
| `VOID` | `()` | For functions returning void |

### Feature-gated mappings

| PostgreSQL | Rust | Feature |
|-----------|------|---------|
| `TIMESTAMP` | `time::PrimitiveDateTime` | `time` |
| `TIMESTAMPTZ` | `time::OffsetDateTime` | `time` |
| `DATE` | `time::Date` | `time` |
| `TIME` | `time::Time` | `time` |
| `INTERVAL` | `time::Duration` | `time` |
| `TIMESTAMP` | `chrono::NaiveDateTime` | `chrono` |
| `TIMESTAMPTZ` | `chrono::DateTime<Utc>` | `chrono` |
| `UUID` | `uuid::Uuid` | `uuid` |
| `NUMERIC`, `DECIMAL` | `rust_decimal::Decimal` | `decimal` |
| `JSONB`, `JSON` | `T: DeserializeOwned` | `json` |
| `INET`, `CIDR` | `ipnetwork::IpNetwork` | `net` |
| `BIT`, `VARBIT` | `bit_vec::BitVec` | `bit` |

### Custom enum types

PostgreSQL custom enums are mapped to Rust enums:

```rust
// PostgreSQL: CREATE TYPE ticket_status AS ENUM ('new', 'in_progress', 'resolved', 'closed');
// sasql detects this and generates (or validates) the Rust mapping:

#[sasql::pg_enum]
enum TicketStatus {
    #[sql("new")]
    New,
    #[sql("in_progress")]
    InProgress,
    #[sql("resolved")]
    Resolved,
    #[sql("closed")]
    Closed,
}

// Now usable in queries:
let tickets = sasql::query! {
    SELECT id, title FROM tickets WHERE status = $status: TicketStatus
}.fetch_all(&pool).await?;
```

---

## Offline Mode

For CI environments without a live PostgreSQL instance.

```bash
# Developer runs this locally (requires live PG):
sasql prepare

# This introspects the database and writes:
# .sasql/schema.json    — table schemas, column types, constraints
# .sasql/queries.json   — validated query hashes + type info
# These files are committed to version control.

# In CI:
export SASQL_OFFLINE=true
cargo build  # proc macro reads from .sasql/ instead of connecting to PG
```

The offline cache is a JSON file containing:
- Table schemas (columns, types, nullability, constraints)
- Validated query hashes with their resolved types
- PostgreSQL version the cache was generated against

If the query SQL changes and the hash does not match the cache, the proc macro emits an
error: "query not found in offline cache — run `sasql prepare` to update."

---

## CLI Tool

```bash
# Install
cargo install sasql-cli

# Commands
sasql prepare           # Generate offline validation cache
sasql schema            # Print current database schema
sasql schema --table users  # Print schema for specific table
sasql migrate new "add_deadline_to_tickets"   # Create migration file
sasql migrate run       # Run pending migrations
sasql migrate status    # Show migration status
sasql migrate revert    # Revert last migration
```

### Migrations

```bash
sasql migrate new "add_deadline_to_tickets"
# Creates:
# migrations/20260331_120000_add_deadline_to_tickets/up.sql
# migrations/20260331_120000_add_deadline_to_tickets/down.sql
```

Migrations are plain SQL files. No Rust DSL. The `_sasql_migrations` table tracks which
migrations have been applied:

```sql
CREATE TABLE IF NOT EXISTS _sasql_migrations (
    version     BIGINT PRIMARY KEY,     -- timestamp prefix
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    TEXT NOT NULL            -- SHA-256 of up.sql content
);
```

---

## Benchmark Targets

Measured on a local PostgreSQL 18 instance, single connection, prepared statements.
All values are median latency from 10,000 iterations after 1,000 warmup iterations.

| Operation | sasql Target | sqlx 0.8 | Diesel 2.2 | tokio-postgres |
|-----------|:------------:|:--------:|:----------:|:--------------:|
| `SELECT` 1 row (3 cols) | < 50 us | ~60 us | ~55 us | ~45 us |
| `SELECT` 100 rows (5 cols) | < 200 us | ~250 us | ~230 us | ~180 us |
| `SELECT` 1000 rows (5 cols) | < 1.5 ms | ~2.0 ms | ~1.8 ms | ~1.3 ms |
| `INSERT` 1 row | < 40 us | ~50 us | ~45 us | ~35 us |
| `INSERT` 1000 rows (batch) | < 2 ms | ~3 ms | ~2.5 ms | ~1.8 ms |
| Pool acquire | < 5 us | ~5 us | — | — |
| Proc macro (50 queries) | < 3 s | ~5 s | ~2 s | — |

The target is within 10% of raw tokio-postgres. The generated code is tokio-postgres calls
with zero abstraction overhead — the difference should be negligible and within measurement
noise.

---

## Benchmarks Methodology

Numbers without methodology are marketing. Every performance claim in this document is backed by a repeatable measurement process. If you cannot reproduce our numbers, that is a bug.

### How we measure

**Microbenchmarks**: `criterion` with statistical analysis. Minimum 100 iterations per sample, 10 samples per benchmark. Warm cache (100 warmup iterations before measurement). Results reported as median ± MAD (median absolute deviation) — not mean ± stddev, which is skewed by outliers.

**End-to-end latency**: `wrk` against a real axum server backed by a real PostgreSQL instance. 4 threads, 64 connections, 30-second run. Reports p50, p99, p99.9. The p99.9 is the number that matters — it is what users experience during peak load.

**CPU instruction counts**: `perf stat` (Linux) or Instruments (macOS) measuring retired instructions. This metric is deterministic — no noise from frequency scaling, thermal throttling, or co-tenant interference. When two implementations differ by 10% in instruction count, the faster one is objectively better regardless of what wall-clock measurements say on any particular machine.

**Memory profiling**: `DHAT` (Valgrind's heap profiler) for allocation counts, total bytes allocated, and peak heap usage. Every allocation is a potential fragmentation event, a potential cache miss, and a potential page fault. The allocation count is tracked as a primary metric, not an afterthought.

**Competitors**: every benchmark runs the identical query against `sqlx 0.8`, `diesel 2.3`, `tokio-postgres` raw (hand-written deserialization), and `libpq` C driver (via FFI harness). The C driver is the floor — if sasql is slower than C, something is wrong. The goal is to be within measurement noise of it.

### What we measure per query type

| Query Type | Metrics | Why this query |
|-----------|---------|----------------|
| Simple SELECT (1 row, 5 cols) | latency p50/p99, allocations, bytes transferred | Baseline: pure overhead of the library with minimal data |
| Multi-row SELECT (100 rows, 8 cols) | throughput rows/sec, memory peak, allocation count | Exercises deserialization hot loop, arena vs heap |
| Large result (10,000 rows) | throughput MB/sec, RSS delta, time-to-first-row | Streaming behavior, memory pressure, backpressure |
| INSERT with RETURNING | latency p50/p99, WAL bytes generated | Write path overhead |
| Batch INSERT (1,000 rows via unnest) | throughput rows/sec, pipelining benefit | Bulk write, parameter encoding pressure |
| CTE recursive (depth 20) | latency, PG plan time vs. client overhead | Complex query where client overhead should be invisible next to PG time |
| Dynamic query (3 optional clauses) | dispatch overhead, prepared statement hit rate | The overhead of sasql's variant matching vs. a static query |
| Pipeline (5 parallel queries) | total latency vs. sequential, connections consumed | Validates pipelining claim: same latency, fewer connections |
| Pool acquire + release | operation time p50/p99 under contention | Pool overhead under realistic load (64 concurrent tasks, 16 connections) |

### Target numbers

These are not aspirations. They are pass/fail criteria. If a release does not meet these numbers, it does not ship. Measured on PostgreSQL 18, local Unix socket, prepared statements, binary protocol.

| Operation | sasql target | sqlx 0.8 current | speedup | notes |
|-----------|:-------------|:-----------------|:--------|:------|
| Pool acquire (uncontended) | < 100ns | ~5µs | 50x | crossbeam channel vs tokio semaphore |
| Pool acquire (64 tasks, 16 conns) | < 1µs p99 | ~8µs p99 | 8x | contention test |
| Simple SELECT deserialize (1 row) | < 2µs | ~8µs | 4x | arena + binary protocol eliminates alloc + parse |
| 100-row deserialize (8 cols) | < 50µs | ~200µs | 4x | arena bulk + SIMD utf8 validation |
| 10,000-row throughput | > 500K rows/sec | ~150K rows/sec | 3.3x | streaming + arena + binary |
| 5-query pipeline | 1 RTT | 5 RTT | 5x | network-bound improvement |
| Dynamic query dispatch | < 5ns overhead | n/a | n/a | match on bitflag, not string comparison |
| Proc macro validation (50 queries) | < 2s | ~5s (sqlx) | 2.5x | connection reuse + pipelined PREPARE |
| Offline mode validation (50 queries) | < 200ms | ~1s (sqlx) | 5x | binary cache vs JSON parse |
| Memory per 100-row result | < 8KB arena | ~24KB heap | 3x | contiguous arena vs scattered allocations |

The "sqlx current" column is measured, not estimated. The measurements are in the `benches/` directory and run in CI on every commit to `main`.

---

## What sasql Is Not

- **Not an ORM.** There is no `User::find(42)`, no `user.save()`, no `belongs_to`. If you
  want an ORM, use SeaORM or Diesel.
- **Not a query builder.** There is no `.filter()`, `.select()`, `.join()`. Write SQL.
- **Not a migration-only tool.** The migration feature exists for convenience. If you prefer
  `dbmate` or `sqitch`, use them. sasql validates queries against whatever schema exists.
- **Not database-agnostic.** It is PostgreSQL-first. SQLite and MySQL support may come later,
  but the API will never be constrained by lowest-common-denominator SQL.

---

## Design Decisions and Tradeoffs

### Why tokio-postgres, not a custom wire protocol implementation?

tokio-postgres is battle-tested, maintained, and correct. Reimplementing the PostgreSQL wire
protocol would cost months of development for marginal performance gains. The bottleneck is
network I/O and PostgreSQL's own query execution, not Rust-side serialization.

If profiling reveals that tokio-postgres is a bottleneck, a custom implementation can replace
it behind the same `Pool` abstraction without changing user-facing API.

### Why deadpool-postgres, not bb8 or mobc?

deadpool-postgres is lighter than bb8 (no `async-trait` overhead in its core path) and
simpler than mobc. It is the default choice but not hardcoded — the `Pool` wrapper allows
swapping the backend.

### Why not support `query()` alongside `query!()`?

This is the most important design decision in sasql. The answer is: **because the entire
point of sasql is that you cannot write unchecked SQL.**

If `query()` existed, even as `#[deprecated]` or behind an `unsafe` block, some developer
somewhere would use it. One unchecked query in a codebase of 500 checked queries means the
guarantee is broken. The guarantee is binary — 100% or meaningless.

sqlx made the pragmatic choice to provide `query()` for migration tooling, dynamic table
names, and other edge cases. sasql takes the uncompromising position: those edge cases must
be solved differently (compile-time macros, code generation, or separate crates) rather than
by weakening the guarantee.

### Why limit optional clauses to 8?

2^8 = 256 variants. Each variant is a separate SQL string validated at compile time and
potentially a separate prepared statement at runtime. Beyond 256:
- Compile times grow linearly (each variant requires a `PREPARE` round-trip).
- Connection-level prepared statement caches grow.
- Code size grows (though the runtime dispatcher is a simple match).

8 optional clauses cover the vast majority of real-world filtering needs. Queries requiring
more conditional logic are typically better expressed as multiple simpler queries.

### Why no serde in the hot path?

serde adds a trait object / vtable dispatch layer to deserialization. For database row
mapping, we know the exact types at compile time — there is no need for runtime type
erasure. The generated `FromRow` implementation is monomorphized code that calls
`row.get::<Type>(index)` directly.

serde is available as a feature flag for users who want `Serialize`/`Deserialize` on
their result types (e.g., for JSON API responses). But it is not used for the database
read path.

---

## Roadmap

### v0.1 — Foundation (2-3 weeks)

The minimum viable library. Validates SQL, generates typed code, runs queries.

- `sasql::query!` macro: parse SQL, connect to PG, validate, generate struct + execute
- Type mapping: `i16`, `i32`, `i64`, `f32`, `f64`, `bool`, `String`, `&str`, `Vec<u8>`,
  `&[u8]`
- Execution: `fetch_one`, `fetch_all`, `fetch_optional`, `execute`
- `Pool` wrapper over deadpool-postgres
- `SasqlError` type with `Pool`, `Query`, `Decode`, `Connect` variants
- Basic error messages (column not found, type mismatch, table not found)
- PgBouncer compatibility: detect PgBouncer at pool creation; PgBouncer 1.21+ with
  `prepared_statements=yes` uses named statements normally; older PgBouncer falls back
  to unnamed statements with a warning
- PostgreSQL only

### v0.2 — Full Type System (1 week)

Complete PostgreSQL type coverage.

- Feature-gated types: `time`, `chrono`, `uuid`, `decimal`, `json`, `net`, `bit`
- Array types: `Vec<T>` for all supported `T`
- Custom PG enums via `#[sasql::pg_enum]`
- NULL safety: `Option<T>` generated for nullable columns, `T` for `NOT NULL`
- Cast expressions: `status::text`, `id::bigint`

### v0.3 — Dynamic Queries (2 weeks) — THE DIFFERENTIATOR

The feature that justifies sasql's existence as a separate library.

- `[optional clause]` syntax: parse, expand, validate all combinations
- `$[sort: Enum]` syntax with `#[sasql::sort]` derive
- Combinatorial expansion with 2^N limit (N <= 8)
- Runtime variant dispatcher (match on Option parameters)
- Prepared statement caching per variant per connection
- Compile-time validation of every generated variant

### v0.4 — Developer Experience (1 week)

Polish for daily use.

- `sasql prepare` — offline validation cache
- `sasql schema` — schema introspection
- "Did you mean?" suggestions (Levenshtein distance)
- Improved error spans (point to exact token in SQL)
- rust-analyzer compatibility testing

### v0.5 — Production Readiness (1 week)

- Transaction support: `pool.begin()`, `tx.commit()`, `tx.rollback()`, drop-guard
- Prepared statement management (per-connection cache, no leaks)
- Connection pool tuning guide
- `criterion` benchmark suite
- `trybuild` compile-fail test suite

### v0.6 — Advanced Features + Singleflight (1-2 weeks)

- `LISTEN` / `NOTIFY` support
- Row streaming: `fetch_stream` returning `impl Stream<Item = Result<T>>`
- `COPY IN` / `COPY OUT` support
- Composite type mapping
- Range type mapping
- **Singleflight / request coalescing**: if N concurrent handlers execute the same query with the same parameters, only one query goes to PG — the rest share the result. The proc macro knows the query hash at compile time. The runtime maintains a `DashMap<(query_hash, params_hash), Shared<Future<Result>>>`. When a query arrives, check if it's already in-flight. If yes, subscribe to the existing future. If no, execute and broadcast. Impact: 10-100x PG load reduction for hot-key reads (user profiles, configs, frequently accessed entities). Zero code change for the user.

### v0.7 — Intelligence (2 weeks)

Exploiting total query knowledge — optimizations that require seeing every query.

- **SQL safety gates** — a finite set of compile-time checks for objectively dangerous SQL patterns. sasql is not a SQL linter. It does not optimize query style or suggest JOIN reordering. It catches patterns that are bombs in production — always, unconditionally, regardless of database version. The list is closed, not extensible:

  **Hard errors (will not compile):**
  - `UPDATE` / `DELETE` without `WHERE` — accidental full-table modification

  **Warnings (compiles, but warns):**
  - Implicit cross join (`FROM a, b` without explicit `JOIN`) — likely accidental cartesian product. Use `CROSS JOIN` to silence if intentional.
  - `SELECT *` without `LIMIT` — unbounded result set
  - `LIKE '%...'` with leading wildcard — guaranteed sequential scan, index cannot help
  - `COUNT(*)` without `WHERE` — full table scan in PostgreSQL (no count cache)

  **Runtime detection (feature = "diagnostics"):**
  - N+1 pattern — proc macros cannot see surrounding control flow (loops). Instead, the runtime tracks `(query_hash, thread_id, call_count)`. If the same query executes 10+ times within 100ms on the same thread, emits `tracing::warn!` with a rewrite suggestion. Catches N+1 in development/testing. A separate `sasql-lint` tool (MIR-level analysis) may provide compile-time detection in the future.

  Everything else — CTE vs subquery, window function tuning, index selection, `NOT IN` vs `NOT EXISTS` — is the territory of `EXPLAIN ANALYZE` and human judgment. sasql validates SQL *correctness*. SQL *quality* beyond the six rules above is not sasql's scope. Attempting to lint all SQL patterns is an infinite, database-version-specific maintenance burden that would reject valid code.

- **Compile-time cross-query analysis**: the proc macro sees all queries in the compilation unit. It detects potential deadlocks (query A locks table X then Y, query B locks Y then X) and generates a table dependency graph. Emitted as `#[warn(sasql::cross_query)]` diagnostics, suppressible with `#[allow]`.
- **Compile-time query timing estimates**: the proc macro runs `EXPLAIN ANALYZE` during compilation (on the development database). Estimated cost and plan type are embedded in doc comments on the generated struct: `/// Estimated: 0.1ms (index scan on users_pkey)`. Developers see query performance in their IDE without executing anything. Gated behind `feature = "explain"` — requires a database with representative data.
- **Automatic read/write splitting**: the proc macro knows at compile time whether a query is SELECT (read) or INSERT/UPDATE/DELETE (write). The Pool transparently routes reads to replicas and writes to primary. The user configures `Pool::builder().primary(url).replica(url)` — routing is derived from the SQL, not annotations. Transactions always go to primary. Single-primary setups work unchanged (the replica list is optional).

### v0.8 — Caching & Connection Intelligence (1-2 weeks)

The pool becomes smarter than the developer.

- **Local result cache with automatic invalidation**: since every write goes through sasql, the runtime knows which tables are modified. SELECT results are cached in a local LRU keyed by `(query_hash, params_hash)`. When an INSERT/UPDATE/DELETE touches a table, all cache entries for that table are invalidated. This is impossible in libraries with escape hatches — they cannot know about all writes. Table names are extracted at compile time. Invalidation is table-level for correctness; row-level is a future refinement. Cache is opt-in per Pool: `Pool::builder().result_cache(1024)`.
- **Connection affinity**: route queries to connections that already have the relevant prepared statements cached. The runtime tracks which statements are prepared on each connection (via the compile-time statement index bitmap). When acquiring a connection, prefer one that already has the needed statement. Eliminates PREPARE overhead after warmup.
- **Adaptive statement eviction**: track statement usage frequency per connection. If a connection has 100+ prepared statements but only 20 are used frequently, DEALLOCATE the cold ones to free PG backend memory (~1KB per statement). Re-prepare on demand. The eviction threshold and frequency are configurable.

### v1.0 — Stable Release

- Full documentation with examples
- All tests passing, all benchmarks meeting targets
- API stability guarantee (no breaking changes until v2)
- Published to crates.io
- SQLite feature flag (experimental)

### v1.x — Post-Stable (ongoing)

Optimizations that benefit from production runtime data.

- **Predictive pipelining**: if runtime telemetry shows that queries A, B, C are always called in sequence, suggest `sasql::pipeline!` at compile time. The compiler tells the developer to batch — it does not silently do it for them.
- **Query playground / REPL**: `sasql playground` opens an interactive REPL where you write queries with sasql syntax, see generated Rust types, inspect EXPLAIN output, and test with live data. IDE integration via LSP for autocomplete of table names, column names, and parameter types within `query!` blocks.
- **Automatic migration generation**: if a `sasql::query!` references a column that doesn't exist, `sasql fix` generates the `ALTER TABLE` migration. The proc macro knows the expected type from the parameter annotation. Not a replacement for deliberate schema design — a convenience for rapid prototyping.

---

## Optimization Catalog

Exhaustive list of optimizations beyond what the spec and CREDO already cover (arena, binary protocol, SIMD UTF-8, rapidhash, bitcode caching, pre-computed offsets, pipelining, zero-copy deserialization). Grouped by domain. Status: **confirmed** (will implement), **deferred** (post v1.0), **rejected** (with reason).

### Memory

**Lazy deserialization** — decode columns on access via `#[inline(always)]` getters, not eagerly in `FromRow::from_row()`. Saves ~5us/row when only a subset of a wide row (8+ cols) is accessed. Opt-in per-query (`#[lazy]` or `fetch_lazy`); default remains eager decode.
Status: **confirmed**

**Cow<'arena, str>** — replace the proposed Ref/Owned struct split with a single struct using `Cow<'a, str>` fields. One type, zero-copy serde, `.into_owned()` on individual fields. Eliminates API surface duplication.
Status: **confirmed**

**compact_str::CompactString** — inline storage for owned strings <= 24 bytes (enum variants, short usernames, codes). Used as the owned variant inside `Cow`. Feature-gated: `feature = "compact-str"`. ~500 lines, no transitive deps.
Status: **deferred** — opt-in feature, implement after core stabilizes

**Column-oriented storage** — `fetch_all_columnar()` returning struct-of-arrays layout for analytics/batch/export patterns. Proc macro generates the columnar struct alongside the row struct. Arena strings still borrowed via `Cow`.
Status: **deferred** — niche use case, post v1.0

**Thread-local arena recycling** — pool of <= 4 arenas per thread, LIFO ordering (warmest first), reset bump pointer on return (no memset), shrink back to 8KB if grown beyond 64KB. `RefCell<Vec<Arena>>` for the thread-local pool.
Status: **confirmed**

**MaybeUninit for result structs** — use `MaybeUninit<T>` for partially-initialized result structs during deserialization, avoiding redundant default initialization before field assignment. Safe wrapper that tracks initialization state at compile time.
Status: **deferred** — requires careful unsafe auditing, post v1.0

**Aligned arena allocation for SIMD** — align arena allocations to 32 bytes for SIMD-friendly access. Wastes ~15.5 bytes average per allocation; 300 allocations wastes ~4.6KB of an 8KB arena. `simdutf` already handles unaligned input (2 cycle overhead per string).
Status: **rejected** — cache pressure from wasted arena space outweighs the ~200ns saved

**Self-referential struct solution** — use `yoke` or `self_cell` crate to safely tie arena lifetime to the result struct without `ouroboros` or manual `Pin` + unsafe. Enables `fetch_one` to return a self-contained value that owns its arena.
Status: **confirmed**

**rkyv for result cache** — zero-copy deserialization via `rkyv` for the local LRU result cache (v0.8). Cached results are accessed in-place without deserialization. Significantly faster than bitcode for read-heavy cache access patterns.
Status: **deferred** — v0.8 feature (result caching)

**zerocopy for fixed-width results** — use `zerocopy` crate to reinterpret wire bytes directly as struct fields for rows containing only fixed-width columns (i32, i64, bool, f64, uuid). Zero deserialization cost for these rows.
Status: **deferred** — requires all-fixed-width detection in codegen

**String interning (lasso)** — intern repeated string values (enum columns, status fields, category names) across rows in a result set. One allocation per unique value instead of per row. `lasso` crate provides a concurrent, arena-friendly interner.
Status: **deferred** — high impact for enum-heavy schemas, evaluate post v0.6

### Protocol

**Streaming deserialization with backpressure** — `fetch_stream` uses a rolling arena with 64-row chunks. Connection held until stream is dropped/consumed. Backpressure via TCP flow control (consumer stops `next()` -> tokio stops reading -> PG pauses). Reduces 100K-row memory from ~100MB to ~50KB.
Status: **confirmed**

**Batch parameter encoding** — encode all parameters into a single contiguous buffer via bump allocator. Pre-calculate total size at compile time for fixed types. Eliminates N-1 allocations for N parameters.
Status: **confirmed**

**LIMIT size hints** — when query has literal `LIMIT N`, generate `Vec::with_capacity(N)`. Cap at 1024 for large limits. Eliminates 0-8 Vec reallocations.
Status: **confirmed**

**TCP_NODELAY / Unix sockets** — verify tokio-postgres sets TCP_NODELAY (avoids 40ms Nagle delay). Add SO_KEEPALIVE (15s interval, 3 probes). Auto-detect localhost and prefer Unix domain socket (~20us RTT savings).
Status: **confirmed**

**COPY protocol for bulk reads** — use PostgreSQL's `COPY ... TO STDOUT (FORMAT BINARY)` for large result sets. Eliminates per-row message framing overhead. Returns raw binary column data in a single stream.
Status: **deferred** — complex wire protocol interaction, post v0.6

### CPU

**Branch prediction hints** — `unlikely()` on NULL checks for nullable columns. For NOT NULL columns, eliminate the NULL check entirely (the binary protocol guarantees non-NULL). Saves 1 branch per NOT NULL column per row.
Status: **confirmed**

**SIMD beyond UTF-8** — (a) batch NULL-flag scanning: 8 length-prefixes per SIMD instruction via i32 compare-with-minus-one. (b) column offset prefix-sum for wide rows. (c) SIMD bswap for `&[i32]` array parameter encoding. (d) SIMD enum string matching: load first 16 bytes, compare all variants simultaneously.
Status: **confirmed** — (a) and (d) for v1.0; (b) and (c) deferred

**Read-ahead prefetch** — explicit `_mm_prefetch` / `__prefetch` for row N+1 during `fetch_all` deserialization. Hides ~100-cycle L2 miss latency. Gate behind `cfg(target_arch)`. Only keep if benchmark shows >= 2% improvement on 1000+ rows.
Status: **deferred** — benchmark first, complexity not justified for marginal gains

**Enum matching: len+first_byte / phf** — for mapping TEXT -> Rust enum, dispatch on `(string.len(), string[0])` tuple to avoid full string comparison. For large enums (8+ variants), use compile-time perfect hashing via `phf` crate for O(1) lookup.
Status: **confirmed** — len+first_byte for small enums, phf for 8+ variants

**SWAR (SIMD Within A Register)** — process 8 bytes at a time using u64 arithmetic for short-string operations (enum matching, small-string comparison) without actual SIMD instructions. Works on all architectures, no `cfg(target_feature)` needed.
Status: **deferred** — evaluate after enum matching benchmarks

**Direct DataRow parsing** — bypass tokio-postgres `Row` type entirely, parse PostgreSQL DataRow messages directly into user structs. Eliminates intermediate `Row` allocation and column-index lookups. Requires understanding the wire format at the byte level.
Status: **confirmed** — this is the core zero-copy architecture

**Connection warmup pipeline** — on new connection creation, pipeline all known PREPARE statements in a single batch before the connection enters the pool. First request pays zero preparation cost. Combined with `session_init` SET commands.
Status: **confirmed**

### Code Size

**SQL normalization** — collapse whitespace, strip comments, case-fold keywords at compile time. Reduces .rodata size (~4KB for 50 queries), improves cache utilization, and ensures whitespace-only differences produce identical rapidhash (enabling deduplication).
Status: **confirmed**

**Monomorphization control** — factor shared execute/pool/prepare logic into a non-generic inner function taking `&[&dyn ToSql]`. Per-query code is a thin wrapper (parameter encoding + deserialization only). Saves ~2KB per query (~100KB for 50 queries), costs ~10ns per query (vtable lookup, invisible vs network latency).
Status: **confirmed**

### Compilation

**Query deduplication** — maintain `HashMap<u64, ValidationResult>` in proc-macro shared state. Key: rapidhash of normalized SQL (strip param names, normalize whitespace). Identical queries in multiple modules validated once. Saves ~15ms for a 100-query project with 30% duplication.
Status: **confirmed**

**Pre-computed column offsets** — for all-fixed-width result rows, emit column byte offsets as `const` values. No runtime offset computation. For mixed rows, const prefix up to first variable-width column, runtime suffix after. Saves ~10ns/row on fixed-width results.
Status: **confirmed**

**Aggressive caching** — incremental revalidation via `(file_path, byte_offset) -> query_hash` mapping. Only re-validate queries whose hash changed. Combined with connection pooling across macro invocations and pipelined PREPARE batches.
Status: **confirmed**

### Pool

**Connection session config** — `session_init` callback on pool connection creation (not per borrow). Set `statement_timeout`, `idle_in_transaction_session_timeout`, `work_mem`, `search_path`, `application_name`. Cost amortized to zero over connection lifetime.
Status: **confirmed**

**Runtime PREPARE batching** — on first statement miss, check global registry for other un-prepared statements on this connection. Pipeline all pending PREPAREs in one batch. First request pays 1 RTT for N statements instead of N RTTs over N requests.
Status: **confirmed**

**Bitmap statement cache** — assign each query a compile-time index (0..N). Track preparation status as `u64` bitmap (N <= 64) or `[u64; ceil(N/64)]` per connection. `is_prepared` is a single bitwise AND (~1 cycle) vs HashSet lookup (~20ns). 250x less memory per connection.
Status: **confirmed**

**PgBouncer compatibility** — detect PgBouncer at pool creation. PgBouncer 1.21+ with `prepared_statements=yes` uses named statements normally. Older PgBouncer falls back to unnamed statements with a compile-time warning. Include crate-level salt in statement names to prevent cross-application cache poisoning in session mode.
Status: **confirmed**

**Fail-fast pool (no timeouts)** — pool exhaustion returns immediate `PoolExhausted` error, no waiting. Deadlock (transaction holds connection, needs another from full pool) is an immediate error. Dropped transaction without commit/rollback marks connection dirty, discards from pool. TCP `connect_timeout` is the only legitimate timeout (external boundary).
Status: **confirmed**

**Deterministic statement indexing** — use content-hash-based indices (rapidhash of normalized SQL) instead of monotonic counters for the bitmap statement cache. Deterministic across compilations and crate boundaries. Counters depend on macro expansion order, which is fragile.
Status: **confirmed**

### Diagnostics

**Query complexity warnings** — closed list of compile-time heuristics (the proc macro has no access to table stats or indexes). Not a SQL linter; not extensible. The list:
1. `SELECT *` without `LIMIT` — unbounded result set
2. `LIKE '%...'` leading wildcard — guaranteed sequential scan
3. `SELECT DISTINCT` on likely-unindexed columns — sort cost
4. `NOT IN (subquery)` — quadratic; suggest `NOT EXISTS`
5. `COUNT(*)` without `WHERE` — full table scan (PG has no count cache)
6. Cartesian product (`FROM a, b` without JOIN/WHERE) — hard error
Opt-in: `feature = "lint"` or `#[sasql::lint]`. Silent by default.
Status: **confirmed**

**Runtime N+1 detection** — track `(query_hash, thread_id, call_count)` at runtime. If same query executes 10+ times within 100ms on the same thread, emit `tracing::warn!` with rewrite suggestion. Feature-gated: `feature = "diagnostics"`.
Status: **confirmed**

### Safety

**Cancellation safety** — document and test behavior when a `fetch_*` future is dropped mid-execution. The connection must be in a clean state (no partial read from socket). If tokio-postgres does not guarantee this, wrap with explicit connection reset on drop.
Status: **confirmed**

**Transaction Drop behavior** — dropped transaction without explicit commit/rollback issues ROLLBACK, marks connection dirty, discards from pool with warning. The next pool user gets a clean connection.
Status: **confirmed**

**PREPARE-vs-runtime gap** — compile-time PREPARE validates syntax and types but cannot catch: triggers that reject data, RLS policies that deny access, deferred constraints that fail at COMMIT, domain CHECK constraints evaluated at write time. Document these gaps prominently. They are PostgreSQL limitations, not sasql bugs.
Status: **confirmed** — documentation

**NUMERIC precision** — NUMERIC/DECIMAL has arbitrary precision in PG. Binary protocol sends exact digits. `rust_decimal::Decimal` has 96-bit mantissa (28-29 significant digits). Values exceeding this silently truncate. Detect and error on overflow during decode.
Status: **confirmed**

**Schema drift detection** — embed schema version hash in offline cache. On startup (or optionally at runtime), compare hash against live database. Warn on mismatch: cached schema may be stale. `sasql prepare` regenerates the cache.
Status: **confirmed**

**::text cast enum loophole** — `status::text` bypasses enum type checking (any enum casts to text). Warn when a PG enum column is cast to text if a corresponding `#[sasql::pg_enum]` exists. Suggest using the typed enum directly.
Status: **confirmed** — compile-time warning

---

## SQLite Feasibility Assessment

An honest assessment of adding SQLite support. Conclusion: feasible and worthwhile, but after PostgreSQL v1.0.

### What can be shared

| Component | Shared % | Notes |
|-----------|:--------:|-------|
| SQL parser (`parse.rs`, `dynamic.rs`) | ~60% | Parameter extraction, optional clauses, sort enum splicing are database-agnostic. Extract into `sasql-parse` crate. |
| Codegen templates | ~40% | Struct generation, fetch methods, variant dispatcher, sort enums. Type mapping and deserialization code differ. |
| Error types | ~80% | `SasqlError` variants (`Pool`, `Query`, `Decode`, `Connect`) apply to both. Messages differ. |
| Offline cache format | ~90% | bitcode schema metadata structure is identical. Type names differ semantically. |
| CLI structure | ~70% | `prepare`, `schema`, `migrate` commands apply. Implementations differ (SQLite introspection != `pg_catalog`). |
| Pool abstraction | ~30% | `Pool` trait with `acquire()/release()` works for both. Implementations are radically different. |

### What must be different

**Compile-time validator** — PG: TCP connection, async `PREPARE`, `pg_catalog` introspection. SQLite: local file, synchronous `sqlite3_prepare_v2`, C API column metadata. Shared trait: `CompileTimeValidator::validate(&self, sql: &str) -> Result<QueryMetadata, Error>`.

**Type system** — SQLite has 5 storage classes (INTEGER/i64, REAL/f64, TEXT/String, BLOB/Vec<u8>, NULL) with loose affinity rules. A column declared INTEGER can hold TEXT at runtime. sasql-sqlite validates declared affinity, but type safety is weaker than PG. This limitation must be documented prominently.

**Wire protocol / data access** — No wire protocol; in-process C API via `rusqlite`. Arena still useful (rusqlite Row borrows are invalidated on next `step()`), but binary protocol, SIMD UTF-8, and pipelining are irrelevant. Performance margin vs raw rusqlite: ~20-30% (vs ~3-4x over sqlx for PG).

**Pool model** — PG: N concurrent network connections. SQLite: one write connection (serialized), optionally multiple WAL-mode readers. `Mutex<Connection>` + reader pool, not a true connection pool.

**Migrations** — SQLite supports transactional DDL natively (simpler), but `ALTER TABLE` is restricted (no DROP COLUMN pre-3.35, no ALTER TYPE). Shared CLI interface, separate execution backend.

### Correct architecture

Separate crates, not a feature flag. Feature flags are additive (pulling in rusqlite + SQLite C library for PG-only users is unacceptable), contaminate the API surface, and double the test matrix.

```
sasql-core/             # Shared: Pool trait, error types, arena
sasql-parse/            # Shared: SQL parser, optional clause expansion
sasql-postgres/         # PG-specific: types, binary protocol, pool
sasql-postgres-macros/  # PG-specific: validator, codegen
sasql-sqlite/           # SQLite-specific: types, rusqlite integration
sasql-sqlite-macros/    # SQLite-specific: validator, codegen
sasql/                  # User-facing: re-exports sasql-postgres
sasql-lite/             # User-facing: re-exports sasql-sqlite
```

PG performance is not compromised: the shared `Pool` trait monomorphizes away (each crate uses one concrete type). Arena implementation stays in database-specific crates to avoid constraining PG-specific optimizations.

### Effort estimate

| Component | Shared refactor | SQLite-specific | Total |
|-----------|:---------------:|:---------------:|:-----:|
| Parser refactoring | 3 days | — | 3 days |
| Compile-time validator | — | 5 days | 5 days |
| Type mapping | 1 day | 3 days | 4 days |
| Runtime (pool, execute) | 2 days | 3 days | 5 days |
| Codegen adaptation | 2 days | 3 days | 5 days |
| CLI (prepare, schema, migrate) | 1 day | 3 days | 4 days |
| Tests | — | 5 days | 5 days |
| Documentation | — | 2 days | 2 days |
| **Total** | **9 days** | **24 days** | **33 days** |

Realistic: **6-7 weeks** accounting for SQLite edge cases and PG-code refactoring.

### Verdict

Worth doing, but not before PostgreSQL v1.0. The PG crate must be stable and feature-complete first. Target sasql-lite for v1.1 or v2.0. During v1.0 development, design internal interfaces (parser, validator trait, codegen template) with SQLite in mind to spread the 9-day refactoring cost across normal development. The 24-day SQLite-specific work begins after v1.0 ships.

---

## Open Questions

These are unresolved design decisions that will be settled during implementation.

1. **Proc macro connection pooling.** Should the proc macro maintain a persistent connection
   to PostgreSQL across macro invocations within a single `cargo build`, or connect per
   invocation? A persistent connection is faster but adds complexity (connection lifecycle
   management within a proc macro). sqlx uses a shared connection — investigate their
   approach.

2. **Prepared statement naming.** Should sasql use named prepared statements (stable across
   connections) or unnamed (parsed per connection)? Named statements risk collisions if
   multiple versions of the application share a connection pool. Unnamed statements have no
   amortized parse cost. The answer likely depends on whether deadpool-postgres reuses PG
   backend processes.

3. **Schema change detection.** When the database schema changes, cached offline validation
   becomes stale. Should `sasql prepare` detect schema drift and warn? Should the proc macro
   embed a schema version hash in the generated code and check it at startup?

4. **Custom type registration.** How should users register custom PostgreSQL types (domains,
   composite types) for use in queries? A `#[sasql::pg_type]` derive? A configuration file?

5. **Multi-database.** When a project connects to multiple PostgreSQL databases, how does
   the proc macro know which database to validate against? Per-query annotation
   (`#[database = "analytics"]`)? Separate `Pool` types?

6. **Conditional join elimination.** The `[]` syntax for optional `WHERE` clauses is
   straightforward. Optional `JOIN` clauses are harder — if a join is excluded, columns from
   that table must also be excluded from `SELECT`. Is this worth the complexity?

---

## Appendix A: Comparison with sqlx Internals

sasql's proc macro follows a similar architecture to sqlx's `query!` macro, with key
differences:

| Aspect | sqlx | sasql |
|--------|------|-------|
| SQL parsing | `sqlparser` crate | Custom minimal parser (PG-only) |
| Validation | `PREPARE` via compile-time connection | Same approach |
| Type resolution | `pg_catalog` introspection | Same approach |
| Code generation | Anonymous struct | Named struct (better IDE support) |
| Offline mode | `.sqlx/` directory with JSON | `.sasql/` directory with JSON |
| Dynamic queries | Not supported | Core feature (`[]` clauses) |
| Escape hatch | `query()` function | Does not exist |
| Caching | Per-invocation connection | Shared connection (investigate) |

The custom SQL parser is justified because sasql only needs to:
1. Identify parameter bindings (`$name: Type`)
2. Identify optional clauses (`[...]`)
3. Identify sort placeholders (`$[sort: Enum]`)
4. Pass the rest through to PostgreSQL verbatim

A full SQL parser (like `sqlparser`) is unnecessary overhead. sasql does not need to
understand SQL semantics — PostgreSQL does that during `PREPARE`.

## Appendix B: Dynamic Query Expansion — Detailed Algorithm

Given a query with N optional clauses:

```
SELECT ... FROM ...
WHERE base_condition
[AND clause_1]        -- opt_1: Option<T1>
[AND clause_2]        -- opt_2: Option<T2>
[AND clause_3]        -- opt_3: Option<T3>
ORDER BY $[sort: S]
LIMIT $limit: i64
```

**Step 1: Parse.** Extract the base SQL, optional clauses, and their associated parameters.

**Step 2: Enumerate.** Generate 2^N variants by including/excluding each optional clause.
For N=3:
```
Variant 0: base                          — params: [statuses, sort, limit]
Variant 1: base + clause_1              — params: [statuses, opt_1, sort, limit]
Variant 2: base + clause_2              — params: [statuses, opt_2, sort, limit]
Variant 3: base + clause_1 + clause_2   — params: [statuses, opt_1, opt_2, sort, limit]
Variant 4: base + clause_3              — params: [statuses, opt_3, sort, limit]
Variant 5: base + clause_1 + clause_3   — params: [statuses, opt_1, opt_3, sort, limit]
Variant 6: base + clause_2 + clause_3   — params: [statuses, opt_2, opt_3, sort, limit]
Variant 7: base + clause_1..3           — params: [statuses, opt_1, opt_2, opt_3, sort, limit]
```

**Step 3: Renumber parameters.** Each variant has different parameter positions. `$1`, `$2`,
etc. are renumbered to match the included parameters.

**Step 4: Expand sort.** For each sort enum variant, splice the SQL fragment into the
`ORDER BY` position. If the sort enum has M variants and there are 2^N clause combinations,
validation checks M * 2^N total SQL strings.

However, the runtime dispatcher only branches on clause inclusion — the sort SQL is spliced
as a string parameter, not a separate prepared statement. This reduces runtime variants to
2^N (not M * 2^N).

**Step 5: Validate.** Each of the 2^N SQL variants is sent to PostgreSQL via `PREPARE` at
compile time. If any variant fails validation, the macro reports the error pointing to the
specific optional clause that caused the failure.

**Step 6: Generate code.** The macro generates:
- A `const` SQL string for each variant
- A `match` expression that selects the variant based on `Option::is_some()` checks
- Parameter binding code that unwraps `Option` values for included clauses

---

*sasql: because "it compiles" should mean "it works."*
