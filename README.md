bsql

Compile-time safe SQL for Rust. PostgreSQL and SQLite.

## Why bsql

- **If it compiles, the SQL is correct** -- every query is validated against your real database during `cargo build`. Table names, column names, types, nullability -- all checked before your code can run.
- **Always checked** -- there is no unchecked SQL function. In sqlx, one missing `!` (`query()` vs `query!()`) silently skips compile-time validation. In bsql, there is only one function, and it always checks. You cannot accidentally write unchecked SQL because the unchecked version does not exist.
- **Pure SQL** -- write real SQL. CTEs, JOINs, window functions, subqueries. No DSL, no method chains, no `.filter().select().join()` (hi, diesel). If PostgreSQL or SQLite supports it, bsql validates it.
- **C-level performance** -- on par with raw C (libpq) on single-row queries (within PG server variance; results may flip between runs), 10-20% faster on multi-row fetches, 42% faster on pipelined batch INSERT. See [benchmarks](https://github.com/smir-ant/bsql/blob/main/bench/README.md).
- **Minimal footprint** -- 1.59 MB peak memory — 4.3x less than C (libpq), 4.4x less than sqlx, 10.9x less than Go. See [memory benchmarks](https://github.com/smir-ant/bsql/blob/main/bench/README.md#memory-peak-rss).
- **Async and sync — both first-class** -- same `query!` macro, same performance, same features. Async uses true cooperative scheduling (RPITIT, no `block_in_place` hacks). Sync removes tokio entirely — pure `fn`, zero async runtime overhead. Switch by changing one line in `Cargo.toml`. Most Rust SQL libraries are async-first with sync as an afterthought, or sync-only. bsql is both, equally.
- **PostgreSQL and SQLite** -- same `query!` macro, same compile-time safety, both databases. SQLite is not a second-class citizen.
- **Test isolation in sub-millisecond** -- `#[bsql::test]` creates a schema per test, not a database. Raw schema create+drop cycle is ~300μs on a local PG; a full test with fixtures typically runs 1-3ms total. Orders of magnitude faster than DB-per-test approaches. For DDL with runtime identifiers (custom schema names, ad-hoc setup), `pool.raw_execute()` is the escape hatch — see **Testing** in [Features](#features).
- **Things nobody else does** -- automatic N+1 detection, compile-time query plan analysis, migration safety checking, request coalescing, SQLite parameter type checking, smart NULL inference. See [**One more thing**](#one-more-thing) below.

```rust
let id = 42i32;

// This query is validated at compile time against your real database.
// If the `users` table doesn't exist, or `login` isn't a column,
// or `id` isn't an i32 -- this won't compile.
let users = bsql::query!(
    "SELECT id, login, active FROM users WHERE id = $id: i32"
).fetch_all(&pool).await?;
let user = &users[0];

// user.id: i32, user.login: String, user.active: bool
// Types are inferred from the database schema. Nullable columns become Option<T>.
```

---

## Performance & Memory

[**You need to see this** 🫢](https://github.com/smir-ant/bsql/blob/main/bench/README.md) — bsql vs C vs Go vs diesel vs sqlx, PostgreSQL and SQLite, full methodology and how to reproduce.

<details>
<summary>Squeezing the last 5-15%: PGO and allocator tuning</summary>

bsql is already fast out of the box — compile-time generated wire protocol, arena-based zero-copy decoding, bind templates with in-place parameter patching, thread-local buffer recycling. But two things can push it further:

### Profile-Guided Optimization (PGO)

PGO lets the compiler see which code paths YOUR specific workload actually hits, then optimizes layout, inlining, and branch prediction for those paths. Typical gain: **5-15% on hot paths**, zero runtime cost.

```bash
# 1. Build your app with profiling instrumentation (~5-10% slower)
RUSTFLAGS="-Cprofile-generate=/tmp/pgo-data" cargo build --release

# 2. Run your app with real traffic for a while (hours, overnight — whatever
#    covers your full workload: peak, quiet, simple queries, complex ones)
./target/release/your_app

# 3. After enough data collected — stop the app, merge the profile
llvm-profdata merge -output=/tmp/merged.profdata /tmp/pgo-data/

# 4. Rebuild with the profile (this is the fast binary)
RUSTFLAGS="-Cprofile-use=/tmp/merged.profdata" cargo build --release

# 5. Deploy the optimized binary. Done.
```

The profiling phase captures branch frequencies across your REAL traffic — peak hours, quiet periods, simple and complex queries. The compiler uses this to make bsql's hot paths (wire protocol, decode, statement cache) as fast as your specific workload allows.

**Re-run PGO after major bsql updates** (the profile is tied to code layout). Minor patches: the profile stays mostly valid.

### Custom allocator

bsql uses its own arena allocator for the performance-critical path (response buffers, column data, row decoding). The system allocator is only hit on the "cold path" — `Vec<Row>` results, `String` column values, pool internals.

For most workloads, the default system allocator is fine. But if your application does heavy allocation beyond bsql (web framework, JSON serialization, etc.), switching the **global** allocator can help:

```rust
// In your main.rs (not in bsql — this is YOUR application's choice):
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

```toml
# In your Cargo.toml:
[dependencies]
mimalloc = "0.1"
```

Alternatives: `tikv-jemallocator` (best fragmentation resistance for long-running servers), `snmalloc-rs` (lock-free, latest MSR research). All three are safe, well-maintained, and give 3-8% improvement on allocation-heavy workloads.

This is an application-level decision, not a bsql dependency — bsql works with any global allocator.

</details>

---

## Quick Start

<details><summary>PostgreSQL</summary>

**Cargo.toml:**

```toml
[dependencies]
bsql = { version = "0.27", features = ["time", "uuid"] }
```

**Set the database URL** (used by `query!` at compile time):

```bash
export BSQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
```

**src/main.rs:**

```rust
use bsql::Pool;

#[tokio::main]
async fn main() -> Result<(), bsql::BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    let id = 1i32;
    let users = bsql::query!(
        "SELECT id, login, first_name FROM users WHERE id = $id: i32"
    ).fetch_all(&pool).await?;
    let user = &users[0];

    println!("{} ({})", user.first_name, user.login);
    Ok(())
}
```

</details>

<details><summary>SQLite</summary>

**Cargo.toml:**

```toml
[dependencies]
bsql = { version = "0.27", features = ["sqlite"] }
```

**Set the database URL** (used by `query!` at compile time):

```bash
export BSQL_DATABASE_URL="sqlite:./myapp.db"
```

If you commit the `.bsql/` cache directory to your repo, teammates and CI can compile without a live database -- the cache contains the schema snapshot.

**src/main.rs:**

```rust
use bsql::SqlitePool;

#[tokio::main]
async fn main() -> Result<(), bsql::BsqlError> {
    let pool = SqlitePool::connect("./myapp.db")?;

    let id = 1i64;
    let users = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i64"
    ).fetch_all(&pool).await?;
    let user = &users[0];

    println!("{}: active={}", user.login, user.active);
    Ok(())
}
```

URL formats: `sqlite:./relative/path`, `sqlite:///absolute/path`, `sqlite::memory:`

</details>

See [examples/](examples/) for more complete, runnable programs — including [keyset pagination](examples/pg_keyset_pagination.rs) (works identically in [PostgreSQL](examples/pg_keyset_pagination.rs) and [SQLite](examples/sqlite_keyset_pagination.rs)).

**Writing tests?** See the **Testing** section in [Features](#features) — covers `#[bsql::test]`, schema isolation, runtime SQL escape hatches (`raw_execute`, `raw_query`), and when to use what.

---

## Safety

- **PostgreSQL driver**: `#![forbid(unsafe_code)]` -- zero unsafe
- **SQLite driver**: unsafe confined to FFI boundary calls (`ffi.rs`) -- every other file is safe Rust
- **5 of 6 crates** enforce `#![forbid(unsafe_code)]` at compile time
- **2,300+ tests** (unit, integration, compile-fail, property-based, and stress)

<details>
<summary>Requirements</summary>

**Rust 1.81+** (MSRV). Required for `IoSlice::advance_slices` — vectored I/O that sends Parse+Describe and Bind+Execute+Sync in a single `writev` syscall without copying prebuilt bytes into the write buffer. Also enables `LazyLock` for lock-free lazy initialization and `#[expect(lint)]` for precise lint control. Rust 1.81 was released September 2024.

**PostgreSQL 10+**. bsql uses prepared statements with binary protocol, `pg_catalog` introspection, SCRAM-SHA-256 authentication (PG 10+), and `CREATE SCHEMA` for test isolation. PG 15-18 are tested in CI matrix.

**SQLite 3.37+** (for STRICT tables). bsql uses WAL mode, mmap, foreign keys, and STRICT tables by default. SQLite 3.37 (2021) added STRICT.

</details>

<details>
<summary>Why does the SQLite driver use unsafe?</summary>

SQLite is a C library, not a network protocol. Talking to it means calling C functions from Rust, which requires `unsafe` at the FFI boundary. This is the same constraint every Rust SQLite library faces (including rusqlite, diesel, and sqlx).

In bsql, all unsafe code is confined to one file: `crates/bsql-driver-sqlite/src/ffi.rs`. Every other module in the SQLite driver is safe Rust. The PostgreSQL driver has zero unsafe -- it speaks the PostgreSQL wire protocol in pure Rust.

When a pure-Rust SQLite engine like [Limbo](https://github.com/penberg/limbo) reaches production readiness, this FFI layer can be replaced entirely.

</details>

---

## Compile-Time Checks

| Your mistake                 | What happens                                                            |
| ---------------------------- | ----------------------------------------------------------------------- |
| Table name typo              | `table "tcikets" not found -- did you mean "tickets"?`                |
| Column doesn't exist         | `column "naem" not found in table "users"`                            |
| Wrong parameter type         | `expected i32, found &str for column "users.id"`                      |
| Nullable column              | Automatically becomes `Option<T>` -- you cannot forget to handle NULL |
| `UPDATE` without `WHERE` | Compile error -- flags accidental full-table updates                    |
| `DELETE` without `WHERE` | Compile error -- same protection                                        |
| SQL syntax error             | PostgreSQL's own parser error message, at compile time                  |
| Typo in any identifier       | Levenshtein-based "did you mean?" suggestions                           |

---

## Features

<details>
<summary>Optional type support (timestamps, UUIDs, decimals)</summary>

Out of the box, bsql works with basic types: integers, floats, booleans, strings, byte arrays. Enable features for specialized types:

```toml
bsql = { version = "0.27", features = ["time", "uuid", "decimal"] }
```

| Feature     | PostgreSQL types                   | Rust types                                   |
| ----------- | ---------------------------------- | -------------------------------------------- |
| `time`    | TIMESTAMPTZ, TIMESTAMP, DATE, TIME | `time::OffsetDateTime`, `Date`, `Time` |
| `chrono`  | Same (alternative to `time`)     | `chrono::DateTime<Utc>`, `NaiveDateTime` |
| `uuid`    | UUID                               | `uuid::Uuid`                               |
| `decimal` | NUMERIC, DECIMAL                   | `rust_decimal::Decimal`                    |

If your query touches a column that needs a feature you haven't enabled, you get a compile error naming the exact feature to add.

</details>

<details>
<summary>Dynamic queries (optional WHERE clauses)</summary>

Optional clauses expand to every combination at compile time. Each combination is validated against the database.

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets WHERE deleted_at IS NULL
     [AND department_id = $dept: Option<i64>]
     [AND assignee_id = $assignee: Option<i64>]"
).fetch_all(&pool).await?;
```

No string concatenation. No runtime SQL assembly. 2 optional clauses = 4 variants, all validated at compile time.

**Compile time**: N optional clauses generate 2^N SQL variants, each validated via PREPARE. For 6+ clauses, compile time may increase noticeably. Maximum: 10 clauses (1024 variants).

</details>

<details>
<summary>Execution methods</summary>

| Method            | Returns      | Use                    |
| ----------------- | ------------ | ---------------------- |
| `.fetch_all(&pool).await` | `Vec<Row>` | SELECT queries         |
| `.execute(&pool).await`   | `u64`      | INSERT, UPDATE, DELETE |
| `.defer(&mut tx).await`   | `()`       | Buffer in transaction  |

Power users: `fetch_one`, `fetch_optional`, `fetch_stream`, `for_each` also available.

</details>

<details>
<summary>Transactions and batching</summary>

```rust
let mut tx = pool.begin().await?;

// .defer() buffers writes -- nothing hits the network yet
bsql::query!("INSERT INTO audit_log (msg) VALUES ($msg: &str)")
    .defer(&mut tx).await?;
bsql::query!("UPDATE accounts SET balance = balance - $amt: i32 WHERE id = $id: i32")
    .defer(&mut tx).await?;

// commit() flushes all deferred operations in a single pipeline, then commits
tx.commit().await?;
```

Savepoints are also supported: `tx.savepoint("sp1")`, `tx.rollback_to("sp1")`.

If the transaction is dropped without calling `commit()`, it automatically rolls back.

</details>

<details>
<summary>Streaming large result sets</summary>

```rust
let mut stream = bsql::query!(
    "SELECT id, login FROM users"
).fetch_stream(&pool).await?;

while stream.advance()? {
    let row = stream.next_row().unwrap();
    println!("{}: {}", row.get_i32(0).unwrap(), row.get_str(1).unwrap());
}
```

True PostgreSQL-level streaming. Rows are fetched in batches and yielded one at a time. Memory usage stays constant regardless of result set size.

</details>

<details>
<summary>LISTEN/NOTIFY (PostgreSQL)</summary>

```rust
let mut listener = Listener::connect("postgres://...")?;
listener.listen("events")?;

loop {
    let n = listener.recv()?;
    println!("channel={}, payload={}", n.channel(), n.payload());
}
```

Real-time notifications for cache invalidation, job queues, live updates.

</details>

<details>
<summary>Compile-time EXPLAIN plans</summary>

```toml
bsql = { version = "0.27", features = ["explain"] }
```

Runs `EXPLAIN` on every query during compilation. The plan is embedded as a doc comment (hover in your IDE to see it), and bsql actively warns about sequential scans and missing indexes. See the **Compile-time query plan analysis** section in [One more thing](#one-more-thing) for details. Development-only -- disable in CI and release builds.

</details>

<details>
<summary>PostgreSQL enums</summary>

```rust
#[bsql::pg_enum]
enum TicketStatus {
    #[sql("new")]         New,
    #[sql("in_progress")] InProgress,
    #[sql("resolved")]    Resolved,
    #[sql("closed")]      Closed,
}
```

Type-safe mapping between Rust enums and PostgreSQL enum types.

</details>

<details>
<summary>Sort enums</summary>

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets ORDER BY $[sort: TicketSort] LIMIT $limit: i64"
).fetch_all(&pool).await?;
```

Each sort variant's SQL is validated at compile time. The enum is exhaustive -- no default case, no fallback.

</details>

<details>
<summary>Connection pool</summary>

Full-featured LIFO connection pool with health checks and configurable behavior.

```rust
let pool = Pool::builder()
    .url("postgres://user:pass@localhost/mydb")
    .max_size(20)                                    // max connections (default: 10)
    .acquire_timeout(Some(Duration::from_millis(50))) // wait for free connection (default: None = fail-fast)
    .max_lifetime(Some(Duration::from_secs(1800)))    // recycle connections after 30 min
    .stale_timeout(Duration::from_secs(30))           // discard idle connections after 30s
    .min_idle(2)                                      // keep at least 2 idle connections
    .max_stmt_cache_size(256)                         // prepared statement cache per connection
    .warmup(&["SELECT 1", "SELECT id FROM users WHERE id = $1"])  // pre-PREPARE on new connections
    .build()?;
```

**LIFO ordering** -- most recently returned connection is reused first (warmest PostgreSQL backend caches).

**Fail-fast by default** -- when `acquire_timeout` is `None`, a pool-exhausted condition returns an error immediately. No silent queuing, no unbounded waits. Set `acquire_timeout` when your workload has predictable bursts.

**Health checks** -- connections idle > 5 seconds are health-checked with an empty query before reuse. Stale connections (idle > `stale_timeout`) are silently discarded.

**Statement warmup** -- new connections pre-PREPARE your hot queries. First real execution hits the statement cache instead of doing a Parse+Describe round-trip.

</details>

<details>
<summary>Read/write splitting</summary>

Route SELECT queries to a read replica, writes to the primary -- transparently.

```rust
let pool = Pool::builder()
    .url("postgres://primary/mydb")
    .replica_url("postgres://replica/mydb")  // optional read replica
    .replica_max_size(10)                    // replica pool size (default: same as primary)
    .build()?;

// SELECT queries automatically route to the replica:
let users = bsql::query!("SELECT id, login FROM users").fetch_all(&pool).await?;

// INSERT/UPDATE/DELETE always route to the primary:
bsql::query!("INSERT INTO users (login) VALUES ($login: &str)").execute(&pool).await?;
```

The proc macro knows which queries are read-only (SELECT) at compile time and generates code that routes through `query_raw_readonly`, which the pool sends to the replica. No user code changes needed -- just add `replica_url` to the builder.

</details>

<details>
<summary>Singleflight (request coalescing)</summary>

Opt-in feature — when 100 threads fire the same read-only query at the same time, bsql executes it once and shares the result. See [Singleflight](#one-more-thing) below for the full description.

```rust
let pool = Pool::builder()
    .url("postgres://localhost/mydb")
    .singleflight(true)
    .build()?;
```

</details>

<details>
<summary>Async and sync modes</summary>

Default: async (`#[tokio::main]` + `.await` on all methods).

```toml
# Async (default)
bsql = { version = "0.27" }

# Sync -- removes tokio dependency entirely
bsql = { version = "0.27", default-features = false }
```

Same `query!` macro, same compile-time safety. Sync mode is pure `fn` -- no async runtime, no `.await`, no tokio in your dependency tree.

When async is enabled, TCP connections use true async I/O via tokio — the scheduler can run other tasks while waiting for PostgreSQL. Unix domain socket connections use sync I/O (sub-millisecond, no benefit from async). No `block_in_place`, no `Handle::current().block_on()` — the `QueryTarget` enum dispatch uses genuine cooperative scheduling.

</details>

<details>
<summary>Offline mode (build without a database)</summary>

`.bsql/queries/` holds a bitcode-serialized snapshot of every query's validation result — column types, parameter types, nullability, the whole PostgreSQL view of the query. Commit it to git and your CI and teammates can build without touching a database.

```bash
# Local dev: validate against a live DB, cache everything as a side-effect
cargo build                     # with BSQL_DATABASE_URL set

# CI / prod: use the committed cache, no database needed
BSQL_OFFLINE=true cargo build
```

**How mode selection works**

- `BSQL_OFFLINE=true` → strict offline. Fail-fast on cache misses.
- `BSQL_OFFLINE=false` → strict live. Require `BSQL_DATABASE_URL`.
- Neither set + `DATABASE_URL` present → live mode.
- Neither set + no DB + cache exists → offline mode as a convenience (local dev shortcut).

**How the cache stays consistent**

Each `query!()` invocation appends its hash to `.manifest` under an exclusive file lock ([`fs2::FileExt::lock_exclusive`](https://docs.rs/fs2/latest/fs2/trait.FileExt.html#tymethod.lock_exclusive)) after the bitcode file has been fsynced to disk. Append is atomic even with parallel `rustc` processes from `cargo build --workspace`, so hashes never get lost to races. Cache keys include parameter Rust types, so two `query!()` sites with the same SQL but different declared types don't collide on the same file.

**What the cache does NOT do**

Auto-clean stale entries. If you remove or rename a query, its old `.bitcode` file stays on disk. This is a deliberate tradeoff — detecting "build finished" reliably from inside a proc macro isn't possible (cargo fans out rustc processes per crate), and past attempts at auto-cleanup corrupted the cache on real production builds. The overhead is small: each bitcode file is ~100-500 bytes, and a mature project typically accumulates tens of stale entries per year.

**Housekeeping**

```bash
bsql verify       # check cache integrity (use as a pre-commit hook)
bsql clean        # wipe the cache clean — a fresh cargo build repopulates it
```

`bsql verify` catches two kinds of breakage: manifest entries without a bitcode file (the "I forgot to git-add the bitcode files" mistake), and bitcode files that don't decode (corruption). Exit code 1 on broken state.

Format: [bitcode](https://docs.rs/bitcode/) — 50x faster than JSON for schema cache loading, binary-compact, versioned envelope so upgrading bsql rejects stale entries with a clear message instead of crashing.

</details>

<details>
<summary>Testing</summary>

bsql validates every `query!()` at compile time. That means your tests use the same `query!()` macro as your production code — no mocks, no fakes, same compile-time guarantees. The question is only: how do you isolate test data so parallel tests don't step on each other?

### Schema-per-test: `#[bsql::test]`

```rust
#[bsql::test(fixtures("schema", "seed"))]
async fn get_user_returns_alice(pool: bsql::Pool) {
    let user = bsql::query!(
        "SELECT name FROM users WHERE id = $id: i32"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(user.name, "Alice");
}
```

Each test runs in its own PostgreSQL schema:

1. `CREATE SCHEMA test_<uuid>` — fresh, isolated namespace (~300μs)
2. Apply fixtures (`fixtures/schema.sql`, `fixtures/seed.sql`) — embedded at compile time via `include_str!`, zero file I/O at runtime
3. Run the test body — same `query!()`, same compile-time validation as production
4. `DROP SCHEMA test_<uuid> CASCADE` — cleanup runs even on panic (Drop guard)

Cargo's default parallelism works without changes. Each test has its own schema, so they never see each other's data. No `#[serial]`, no mutexes on the database. The pool is shared — each test acquires a connection, runs, returns it.

> `#[bsql::test]` is the fastest test isolation in Rust that supports **full DDL, real nested transactions, and parallel execution** without caveats. Transaction-wrapping approaches (diesel's `test_transaction`, Go's `go-txdb`) can be ~2x faster but can't test DDL changes, treat nested transactions as savepoints, and serialize tests on one connection. sqlx creates a full database per test — correct but an order of magnitude slower.

### When compile-time validation can't help

`query!()` validates SQL at compile time against a real database. This works for everything where the SQL text is known before the program runs — which is almost all application code: SELECTs, INSERTs, UPDATEs, DELETEs, assertions, fixtures, seed data.

The one case it **can't** cover: SQL where an **identifier** (not a value) is computed at runtime. The canonical example is building your own test harness with dynamic schema names:

```rust
let schema = format!("test_{}", uuid::Uuid::new_v4());
// "CREATE SCHEMA "test_a1b2c3..." — the schema NAME is a runtime string,
// not a parameter. query!() can't validate this because the identifier
// doesn't exist yet when cargo build runs.
```

Note the difference: a **value** (`WHERE id = $id`) can be a `query!()` parameter. An **identifier** (`CREATE SCHEMA "test_xyz"`) cannot — SQL doesn't allow parameterized identifiers. This is a SQL language constraint, not a bsql limitation.

For this narrow case, bsql exposes three methods directly on `PgPool`:

```rust
pool.raw_execute("CREATE SCHEMA \"test_xyz\"").await?;          // DDL, SET — no result rows
pool.raw_query("SELECT id, name FROM users").await?;            // SELECT → Vec<RawRow> (text values)
pool.raw_query_params("SELECT id FROM users WHERE id = $1",
    &[&1i32 as &(dyn Encode + Sync)]).await?;                   // parameterized runtime SQL
```

These bypass compile-time validation entirely. They're the escape hatch for runtime-computed identifiers, `SET` commands (connection-level session config like `SET search_path`, `SET timezone`), and the rare edge case where `query!()` genuinely can't express what you need.

For SQLite, `SqlitePool::raw_execute(sql)` fills the same role.

**If you're using `raw_execute` for a normal SELECT or INSERT** — that's a signal to rewrite it as `query!()`. The escape hatch exists for identifiers and DDL, not for skipping validation on regular queries.

</details>

<details>
<summary>Zero-copy architecture</summary>

The hot path — prepare → bind → execute → decode — allocates nothing on the heap after the second query on a given thread. A few of the mechanics:

- **Binary wire protocol**. Integers come in as `i32::from_be_bytes()`, not parsed from ASCII. Strings are SIMD-validated via `simdutf8` then handed out as `&str` slices pointing into a shared arena.
- **Pipelined messages**. Parse + Bind + Execute + Sync all go out in one `write_all()` syscall, then we read the entire response burst.
- **Bind templates**. When you re-execute the same prepared statement with different parameters, bsql patches the parameter bytes in place inside a cached message template instead of rebuilding the whole Bind message.
- **Thread-local recycling**. Response buffers, column offset vectors, decoding arenas — all pooled per thread. Second query on the same thread hits zero `malloc`.
- **Statement cache** — small Vec with u64 hash keys. For < 30 cached statements this beats `HashMap` because it fits in one cache line per probe and branches are predictable.

</details>

<details>
<summary>What SQLite settings are used</summary>

bsql automatically configures SQLite for optimal performance:

- **WAL mode** -- concurrent readers, non-blocking reads
- **256 MB mmap** -- memory-mapped I/O for fast reads
- **64 MB cache** -- large page cache
- **STRICT tables** -- recommended for type safety
- **`busy_timeout = 0`** -- fail-fast, no silent waiting
- **Foreign keys ON** -- enforced by default

The pool uses a single writer + N reader connections (default 4) behind `Mutex`, fully synchronous.

</details>

<details>
<summary>What bsql is not</summary>

- **Not an ORM.** You write SQL, not method chains.
- **Not a query builder.** No `.filter()`, `.select()`, `.join()`.
- **Not database-agnostic.** PostgreSQL and SQLite only. No MySQL, no MSSQL.
- **Not a migration tool.** Use dbmate, sqitch, refinery, or whatever you prefer. bsql can validate your migrations before you deploy them (see **Migration safety check** in [One more thing](#one-more-thing)), but it does not write or apply them.

</details>

<details>
<summary>Protocol and feature limitations</summary>

bsql implements the PostgreSQL extended query protocol. The following PG features are **not supported**:

- **GSSAPI / SSPI / LDAP / certificate authentication** — only cleartext, MD5, SCRAM-SHA-256, and SCRAM-SHA-256-PLUS
- **Logical replication protocol** — use pg_recvlogical or a dedicated replication tool
- **Large Objects** (lo_read, lo_write) — use BYTEA columns instead
- **SSL_KEY_LOG** for TLS debugging — not exposed

Supported authentication: cleartext password, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS (channel binding).
Supported transports: TCP, Unix domain sockets, TLS (via rustls).

**TLS crypto provider.** bsql hard-pins [`ring`](https://briansmith.org/rustls-docs/rustls/crypto/ring/index.html) as the rustls crypto provider and passes it explicitly to every `ClientConfig::builder_with_provider` call. This bypasses rustls 0.23's process-level `CryptoProvider` auto-selection, which panics at runtime (`"Could not automatically determine the process-level CryptoProvider from Rustls crate features"`) when cargo feature unification pulls in BOTH `ring` and `aws-lc-rs` — for example, if another dependency in your workspace (reqwest, etc.) enables `aws-lc-rs` on rustls. You get panic-free TLS regardless of downstream feature flags. The choice lives in one function in `bsql-driver-postgres/src/tls_common.rs`; if you need `aws-lc-rs` or a custom provider, open an issue.

</details>

---

## One more thing

bsql sees every query at compile time and every query execution at runtime. That end-to-end visibility makes a set of features possible that are architecturally out of reach for libraries that only see one half of the picture.

<details>
<summary>N+1 query detection</summary>

The most common database performance bug: your code fetches a list, then queries once per item. 100 users = 100 queries instead of 1. Frameworks like Rails have third-party gems for this. bsql detects it at the driver level — no middleware, no config, no code changes.

```toml
bsql = { version = "0.27", features = ["detect-n-plus-one"] }
```

When the same query fires more than 10 times in a row on a single connection, bsql logs a warning with the query hash. The threshold is configurable via `Pool::builder().n_plus_one_threshold(5)`. When the feature is disabled, zero code exists in the binary — full compile-time exclusion.

</details>

<details>
<summary>Compile-time query plan analysis</summary>

With the `explain` feature, bsql runs `EXPLAIN` on every query during `cargo build` and analyzes the result. If PostgreSQL would use a sequential scan on a table with more than 1,000 rows, you get a compile-time warning:

```
warning: [bsql] Seq Scan on "orders" (est. 50000 rows) — consider adding an index
```

Catches missing indexes before your code reaches production. Threshold is configurable via `BSQL_EXPLAIN_THRESHOLD`. Development-only — disable in CI and release builds.

</details>

<details>
<summary>Migration safety check</summary>

You write a migration. Will it break any of your existing queries? Find out before deploying:

```bash
bsql migrate --check add_column.sql
```

bsql reads every validated query from its compile-time cache, creates a shadow copy of your schema, applies the migration, and tests each query against the post-migration schema. If any query would break, it tells you which ones and why — before the migration touches production.

This works because `.bsql/queries/` contains every SQL statement your application uses. No other library has this cache, so no other library can offer this check.

</details>

<details>
<summary>Singleflight (request coalescing)</summary>

When 100 requests hit the same endpoint at the same time and each one runs the same query with the same parameters, bsql executes it once and shares the result. The other 99 requests wait (not poll) and receive a shared copy.

```rust
let pool = Pool::builder()
    .url("postgres://localhost/mydb")
    .singleflight(true)
    .build()?;
```

- Only read-only queries (SELECT). Writes never coalesced.
- Key = `rapidhash(sql_hash, encoded parameter bytes)` — same query + same params = same key.
- 30-second timeout on waiting. If the leader panics, followers get an error, not a deadlock.

100 concurrent requests for `SELECT * FROM config WHERE key = 'theme'` become one database round-trip.

</details>

<details>
<summary>SQLite parameter type checking (compile time)</summary>

Every other Rust SQLite library checks parameter types at runtime — pass a string where an integer is expected, and you get a runtime error. bsql checks at compile time.

```rust
// Column "id" is INTEGER in the schema.
// This won't compile — &str is incompatible with INTEGER:
bsql::query!("SELECT name FROM users WHERE id = $id: &str")
// error: parameter $id declared as &str but column "id" is INTEGER (expected i64)
```

bsql parses the SQL, finds which column each parameter is compared against, looks up the column's declared type via `PRAGMA table_info`, and verifies compatibility. Works for `WHERE`, `INSERT VALUES`, `UPDATE SET`, and comparison operators (`=`, `>`, `<`, `LIKE`, `IN`, etc.). No other Rust SQL library does this for SQLite.

</details>

<details>
<summary>Smart NULL inference (50+ SQL patterns)</summary>

Most SQL libraries treat every computed expression as nullable. `SELECT COUNT(*) as cnt` returns `Option<i64>` — even though `COUNT(*)` can never be NULL. You end up writing `.unwrap()` everywhere for values that are guaranteed to exist. bsql analyzes the SQL and infers NOT NULL for expressions the SQL standard already guarantees:

| Expression | Other libraries | bsql |
|---|---|---|
| `COUNT(*)` | `Option<i64>` | `i64` |
| `COALESCE(name, 'unknown')` | `Option<String>` | `String` |
| `EXISTS(subquery)` | `Option<bool>` | `bool` |
| `CURRENT_TIMESTAMP` | `Option<...>` | `OffsetDateTime` |
| `42` (literal) | `Option<i64>` | `i64` |
| `ROW_NUMBER()` | `Option<i64>` | `i64` |
| `NOW()` | `Option<...>` | `OffsetDateTime` |
| `column::text` (NOT NULL source) | `Option<String>` | `String` |
| `CASE WHEN ... THEN 1 ELSE 0 END` | `Option<i32>` | `i32` |
| `LEFT JOIN` columns | varies | `Option<T>` (always) |

50+ patterns recognized. No `!` override syntax, no user hints, no runtime panics. If bsql can prove NOT NULL — you get the bare type. If it can't — you get `Option<T>`.

> **Safety philosophy: when in doubt, `Option<T>`.** A redundant `.unwrap()` is better than a runtime crash. bsql will never mark a column as NOT NULL unless it can prove it at compile time. LEFT/RIGHT/FULL JOIN columns are always `Option<T>` regardless of table constraints, because the join itself can produce NULLs that `pg_attribute` doesn't report.

</details>

<details>
<summary>Schema-per-test isolation</summary>

Every test gets its own PostgreSQL schema. No shared state, no flaky tests, full parallelism.

```rust
#[bsql::test(fixtures("schema", "seed"))]
async fn test_get_user(pool: bsql::Pool) {
    let user = bsql::query!("SELECT name FROM users WHERE id = $id: i32")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(user.name, "Alice");
}
```

Each test: `CREATE SCHEMA` → apply fixtures → run → `DROP SCHEMA CASCADE`. Fixtures are embedded at compile time via `include_str!`. Cleanup runs even on panic (Drop guard).

> `#[bsql::test]` is the fastest test isolation in Rust that supports **full DDL, real nested transactions, and parallel execution** without caveats. Transaction-wrapping approaches (diesel's `test_transaction`, Go's `go-txdb`) can be ~2x faster but can't test DDL changes, treat nested transactions as savepoints, and serialize tests on one connection. sqlx creates a full database per test — correct but an order of magnitude slower.

See the **Testing** section in [Features](#features) above for runtime SQL escape hatches and manual schema isolation.

</details>

---

## About

Built with [Claude Code](https://claude.ai/code). Design first, implementation second, architectural review third. 2,300+ tests across the workspace — unit, integration, compile-fail, property-based, and a handful of stress tests for the pool and the wire protocol. Not just tests that the code works, but tests that broken code is rejected at compile time.

Don't follow the author's name. Don't assume a library that's been around for 2 years is 12 times better than one that's been around for 2 months. Run the benchmarks yourself, read the tests, check the code.

## License

MIT OR Apache-2.0
