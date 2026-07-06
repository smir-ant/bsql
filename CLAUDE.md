# bsql — Multi-backend SQL toolkit in Rust

## Direction (2026 rebuild)

The theoretical-limit rebuild's core has landed: the compile-checked
`query!` flagship, the PostgreSQL async + sync drivers, and the embedded
SQLite backend all ship today, reachable through the single `bsql` umbrella
crate. `reforge.md` (the original blueprint) and `CREDO.md` (its principles)
are now **historical** — this file and the code are the source of truth.
Load-bearing decisions for any new session:

- **Query API = PURE SQL TEXT.** SQL lives as text in the compile-checked
  `query!` macro, validated at build time against the schema replayed from
  migration DDL. There are **NO** method combinators — the diesel-style
  Fragment/Col combinator paradigm was tried and **reverted** (owner rejected
  `.filter().eq()` builders). Do not reintroduce a runtime SQL builder.
- **Wire format = binary-uniform.** The compile-checked `query!` path encodes every param in
  binary; `ParamsWriter` is the **sole** format authority (no per-call
  text/binary drift).
- **NO CI.** There are no GitHub Actions and the owner mandates none. Gates run
  locally via `cargo` + the `tools/devgates` crate (`deps_pin`,
  `runtime_graph_pin`). Treat any `reforge.md` CI prescription (nightly CI,
  cargo-deny CI) as superseded by local devgates.
- **Compile-checked query API.** SQL references are validated at build time
  against the schema replayed from the consumer's migration `*.sql` by
  `bsql-build` (a `[build-dependencies]` helper) into a catalog the
  `bsql-query-macros` proc-macro reads. The whole flagship is reachable
  through the single `bsql` crate with `features = ["macros"]` — a consumer
  hand-wires nothing (see Build & test). **Guarantee boundary:** the catalog
  matches the migration FILES, not the live database. A migration applied
  out-of-band (e.g. by hand in `psql`) without a corresponding file is invisible
  by design — but this is strictly stronger than a live-introspection cache that
  can silently go stale relative to the migrations under version control: the
  source of truth is the committed migration set, and any change to it (add,
  edit, delete, rename — at any directory depth) recompiles and re-validates. A
  DDL form the replay cannot model faithfully is a loud build error, never a
  silently-wrong catalog.
- **Toolchain pinned** to rustc 1.96.0 (`rust-toolchain.toml`); trybuild/clippy
  goldens capture diagnostics verbatim, so the patch version is fixed.
- **Converge, don't drift.** A prescriptive doc that contradicts the current
  direction silently misleads every future session — fix it, don't append.

## Workspace layout

```
crates/
  bsql/              — umbrella facade + query! re-export + #[bsql::test] harness (bsql::pg, ::pg_sync, ::sqlite)  — 914 LoC
  postgres/
    proto/           — sans-IO wire protocol + session engine (no_std + alloc)  — 27886 LoC
    core/            — transport-generic driver engine Core<S> + materializer + types + config + TLS + Rows + notify ledger + N+1 detector + SafeIdent guard  — 9236 LoC
    async/           — tokio async driver (plugs its socket into the shared Core<S>)  — 1743 LoC
    sync/            — std::net blocking driver (plugs its socket into the shared Core<S>)  — 1575 LoC
  sqlite/
    driver/          — embedded SQLite driver (bundled rusqlite)  — 995 LoC
  testkit/           — deterministic in-memory fake PostgreSQL for driver tests (no network)  — 1005 LoC
  build/             — BUILD-DEP: migration DDL → schema catalog (+ SQLite template)  — 34719 LoC
  query-macros/      — PROC-MACRO: query! (types/validates against the catalog) + #[bsql::test] (schema-per-test wrapper)  — 1929 LoC
```

(src LoC measured per crate via `find <crate>/src -name '*.rs' -exec cat {} + | wc -l` — counts inline `#[cfg(test)]` modules, so `build/`'s total is dominated by `src/infer.rs` (29563 lines: the schema/type-inference engine plus a ~13K-line inline `#[cfg(test)]` test module). Publishable package names: `bsql`, `bsql-postgres-{proto,core,async,sync}`, `bsql-sqlite`, `bsql-testkit`, `bsql-build`, `bsql-query-macros`. Non-shipped `publish = false` tools under `tools/`: `bsql-devgates`, `bsql-query-fixture`, `bsql-query-bridge-fixture`, `bsql-query-sqlite-fixture`, `bsql-test-harness-fixture`, `bsql-corpus`.)

## Build & test

```bash
cargo check --workspace              # full build
cargo clippy --workspace --all-targets   # lint wall — must be 0 warnings
cargo test --workspace               # unit + integration (non-ignored)
cargo test --workspace --doc         # doctests
cargo test -p bsql-devgates --test deps_pin            # dependency-frontier gate
cargo test -p bsql-devgates --test runtime_graph_pin   # build-time-only boundary gate
cargo test -p bsql-devgates --test doc_links           # intra-doc-link wall (broken-link deny)
cargo test -p bsql-devgates --test test_count          # README test-count doc-vs-reality gate
cargo test -p bsql-postgres-proto --test engine_hotpath_codegen  # next_event codegen-stability gate (panic-free + instruction ceiling)
cargo test -p bsql-postgres-core --test decoder_fuzz   # decoder total-function gate (dep-free fuzz: no decoder panics on any input)
cargo test -p bsql-sqlite            # SQLite (no PG needed)
cargo test -p bsql-postgres-async --test sq_live -- --ignored    # async PG (needs local PG)
cargo test -p bsql-postgres-sync --test sync_live -- --ignored   # sync PG (needs local PG)
cargo test -p bsql-query-fixture --test query_live_async -- --ignored  # live query! (async, needs PG)
cargo test -p bsql-query-fixture --test query_live_sync  -- --ignored  # live query! (sync, needs PG)
cargo clippy -p bsql --features test-harness --all-targets              # lint the (non-default) #[bsql::test] harness
cargo test  -p bsql --features test-harness --lib                       # harness unit tests (offline)
BSQL_TEST_DSN=postgres://USER@localhost/postgres \
  cargo test -p bsql-test-harness-fixture -- --ignored                  # live #[bsql::test] isolation witness (needs PG)
cargo clippy --workspace --features n1-detect --all-targets             # lint the (non-default) N+1 detector reshape
cargo test  -p bsql-postgres-core --features n1-detect --lib n1::       # N1Tracker offline unit tests
BSQL_TEST_DSN=postgres://USER@localhost/postgres \
  cargo test -p bsql-query-fixture --features n1-detect -- --ignored    # live N+1-detection witness (needs PG)
```

**Consumer wiring — the `query!` flagship.** A consumer reaches the whole
compile-checked query API through the single `bsql` crate: add it with
`features = ["macros", "postgres-async"]` (or `postgres-sync`) to
`[dependencies]`, add `bsql-build` to `[build-dependencies]`, and add a
one-line `build.rs`:

```rust
fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")   // replays migrations/ into the catalog
}
```

Then `bsql::query!(Name, "<SQL>")` types the SQL at build time against that
catalog and emits the `Name` record + the `NameQuery` carrier (which
implements the umbrella's re-exported `bsql::TypedQuery`); an unknown
column is a `compile_error!`. The macro's expansion names only
`::bsql::__rt::…` paths (a `#[doc(hidden)]` internal module), so no other
dependency is needed at compile time — `bsql-query-macros` is a host-only
proc-macro and never enters the runtime binary. `emit` also emits the
SQLite conformance template when the build-dep's `sqlite` feature is on; a
PostgreSQL-only build can call `bsql_build::emit_catalog("migrations")`
instead. `tools/query_fixture` is the end-to-end proof of this shape.

**External-type bridges (optional).** A consumer can make `query!` decode a
column directly into a chosen EXTERNAL crate type (`chrono::DateTime`,
`uuid::Uuid`, `serde_json::Value`, …) with bsql depending on and forcing
NOTHING. The build.rs uses the richer builder — `Catalog::from_migrations(dir)
.bridge(pg_type, target_type_path, converter_fn_path).emit()` (or `.emit_catalog()`
for a PostgreSQL-only build) — keyed on the canonical PG type; the consumer
supplies one INFALLIBLE converter free function per bridged type
(`fn(bsql::Timestamptz) -> chrono::DateTime<Utc>`). The target type and
converter travel as STRINGS, so `bsql-build` / `bsql-query-macros` gain no
external dependency. The free function is the orphan-proof seam: a consumer
cannot `impl bsql::Cell for chrono::DateTime` (E0117 — both foreign), but a free
fn compiles for any foreign target. The bridge reshapes ONLY the record field
value; the row OID list and the const validator ride the native pivot, so the
compile-time OID-drift guarantee (E0080) is untouched.
`tools/query_bridge_fixture` is the end-to-end proof.

**Schema-per-test isolation — `#[bsql::test]` (feature `test-harness`).** A
consumer adds `bsql` with `features = ["test-harness"]` and writes a test taking
a single connection — `async` over the async driver, or a plain `fn` over the
blocking driver:

```rust
#[bsql::test]
async fn creates_a_user(conn: &mut bsql::pg::Connection) {
    conn.query_sql("CREATE TABLE users (id int)").await.unwrap();  // in an ISOLATED schema
}   // schema auto-dropped, even if the test panics

#[bsql::test]
fn creates_a_user_sync(conn: &mut bsql::pg_sync::Connection) {
    conn.query_sql("CREATE TABLE users (id int)").unwrap();  // same isolation, blocking driver
}
```

Each `#[bsql::test]` runs in its own freshly-created PostgreSQL schema, so
tests running in parallel (cargo's default) never interfere — the isolation
rides the connect-time `search_path` (a startup-packet GUC that survives the
pool's `RESET ALL`, so a pooled connection cannot escape its schema). The
attribute wraps the function in a `#[test]` that connects to the server named
by the **`BSQL_TEST_DSN`** environment variable — a test-specific variable,
deliberately distinct from an application's `DATABASE_URL` because the harness
creates and drops schemas — creates a unique injection-safe schema
(`bsql_t_<pid>_<seq>[_<name>]`, identifier-validated before it is spliced into
the `CREATE`/`DROP` DDL), hands the body a connection pinned to it, and drops the
schema on exit inside a `catch_unwind` so a panicking test still cleans up (the
original panic is re-raised afterward, so `#[should_panic]` still works). An
unset `BSQL_TEST_DSN` is a loud panic naming the variable, never a silent skip.
The `async`-ness selects the driver (async fn → async harness, plain fn → sync
harness); the connection argument type must match it, or the harness's own bound
makes it a type-mismatch compile error (never a mis-expansion). The two harnesses
SHARE all driver-agnostic logic — the DSN resolve, the unique schema-name
generator, the schema DDL, the injection guard, and the error type — defined once
and called by both, so a fix to one cannot silently diverge; only connect +
run-the-body differ (a per-test tokio runtime + `block_on` for async, a direct
blocking call for sync). The attribute lives in the same proc-macro crate as
`query!` (both are host-only token transformers); the harness runtime lives
behind the non-default `test-harness` feature (which pulls BOTH drivers), so a
production build pulls neither the runtime nor the harness.
`tools/test_harness_fixture` is the end-to-end proof (parallel isolation over
both drivers, a mixed async+sync file, teardown on success and on panic, the loud
unset-DSN error).

**N+1 query detection — `conn.n1_report()` (feature `n1-detect`).** A consumer
enables `bsql` (or a driver crate) with `features = ["n1-detect"]` to detect the
classic N+1 anti-pattern: the SAME `query!` query executed repeatedly from the
SAME source line (once per row of a prior result). Each typed verb records its
`(sql, call-site)` pair; when a pair repeats past a threshold (25) WITHIN one
logical operation, `conn.n1_report()` returns an `N1Report { sql, file, line,
count }` for it — the source line comes from `#[track_caller]`. The detector is
**diagnostics-only** (it never batches, blocks, errors, or alters a result — a
false positive is at most a spurious report) and **zero-cost when off**: the
feature is default-OFF, so a production build compiles no `Connection` field, no
query-path branch, and no `#[track_caller]` ABI cost — the typed verbs stay
byte-identical `async fn`s (async driver) / blocking `fn`s (sync driver). Because
`#[track_caller]` is a no-op-with-warning on `async fn`, the `n1-detect` build of
the async driver reshapes the four typed verbs to `#[track_caller] fn -> impl
Future + 'a` with a sync prologue that captures `Location::caller()` before the
`async move` block; the prologue keeps `clippy::manual_async_fn` from firing (so
NO `#[allow]`/`#[expect]` is needed in either feature state), and the bare RPIT
return type LEAKS the concrete future's `Send` (a static assertion pins it), so
every existing `.await` call site — and the pool — is unaffected. The recency
window is a fixed inline array (no per-query allocation) reset at each
logical-operation boundary (commit/rollback, `transaction`, `reset_session`), so
repetition ACROSS operations is forgiven while a per-row loop WITHIN one is
caught. Keyed on the `(sql-pointer, call-site-pointer)` composite so two distinct
call sites of the same query are never conflated. No external dependency (a cargo
feature only). The `tools/query_fixture` `n1-detect` feature + its
`tests/n1_detect_live.rs` are the end-to-end proof (N+1 flagged with source +
count, no false positive on a one-shot or across distinct lines, all results
still correct).

The `deps_pin` gate (`tools/devgates/tests/deps_pin.rs`) pins the resolved
dependency set (parsed from `Cargo.lock`) to a committed golden, and bans any
NEW crate resolving to two versions. An accidental dependency addition or a
version drift fails it. A deliberate dependency change is a reviewed golden
diff, regenerated with `BSQL_DEPS_PIN=overwrite cargo test -p bsql-devgates
--test deps_pin` (mirroring `TRYBUILD=overwrite`), and must be justified in the
root `Cargo.toml` `[workspace.dependencies]` policy block. `deps_pin` pins only
the package SET and is dependency-kind-blind — it cannot tell a runtime edge
from a build edge.

The `runtime_graph_pin` gate (`tools/devgates/tests/runtime_graph_pin.rs`)
covers that blind spot: it parses each shipped crate's
`cargo tree --all-features -e normal,no-proc-macro` (runtime) graph and asserts
the build-time-only SQL-parsing libraries — `sqlparser` and `bsql-build` — are
absent. `--all-features` makes the check exhaustive (it activates the umbrella
crate's non-default `macros` feature, which pulls the `bsql-query-macros`
proc-macro); `no-proc-macro` models runtime LINKAGE faithfully (a proc-macro
runs in the compiler and is never linked into the consumer's runtime binary, so
the host-only `bsql -> bsql-query-macros` edge — and `bsql-build` / `sqlparser`
reached only through it — is correctly excluded). Moving `bsql-build` from
`[build-dependencies]` into a shipped crate's `[dependencies]` would leak
`sqlparser` into the runtime/forbid closure via a NORMAL-library edge that
`no-proc-macro` does NOT prune; `deps_pin` stays green on that, but
`runtime_graph_pin` turns red.

The `doc_links` gate (`tools/devgates/tests/doc_links.rs`) is the intra-doc-link
wall: it runs `cargo doc --workspace --no-deps` with
`RUSTDOCFLAGS=-D rustdoc::broken_intra_doc_links` (in a dedicated
`CARGO_TARGET_DIR` to avoid contending for the parent build lock) and asserts
the doc build succeeds. A doc comment that links to a symbol a deletion removed
(`cargo build`/`clippy`/`cargo test` never resolve intra-doc links) fails this
gate the moment it lands. Scope: the PUBLIC documented surface of every member
(a `--document-private-items` tightening is a follow-up — it currently surfaces
unrelated pre-existing private-doc rot in the build-time inference crate + an
engine `super::flush` ambiguity).

The `test_count` gate (`tools/devgates/tests/test_count.rs`) is the
doc-vs-reality wall for the README's advertised test counts. A hard-coded count
in prose ROTS — every test added or removed drifts the doc, and no compile step
reads the README. This gate runs the EXACT two count commands the README cites
(the `#[test]` / `#[tokio::test]` total, and the `#[ignore]` live-suite count),
greps the two numbers back OUT of the README, and asserts they match the live
workspace — so a test added or removed without updating the doc turns it red
inside the standard `cargo test --workspace` flow. A deliberate change
regenerates the two numbers in place with `BSQL_TEST_COUNT_PIN=overwrite cargo
test -p bsql-devgates --test test_count` (mirroring `TRYBUILD=overwrite`). It is
`publish = false` (a devgate) with no dependencies, so `deps_pin` and
`runtime_graph_pin` are untouched.

The `engine_hotpath_codegen` gate
(`crates/postgres/proto/tests/engine_hotpath_codegen.rs`) pins the *compiled
shape* of the inbound hot dispatch `ActiveEngine::next_event` (the pull-cursor
every verb turns socket bytes through). Reusing the asm-dump machinery — emit
release assembly for the proto lib into a dedicated `CARGO_TARGET_DIR`
(no-contention, like `doc_links`), then extract `next_event`'s instruction body
between its definition label and its `.cfi_endproc` — it asserts two robust
properties. (1) **No reachable panic / unwind edge**: the body must contain ZERO
references to the `core::panicking` family (`panic`, `panic_bounds_check`,
`panic_fmt`, …), `rust_begin_unwind`, or `_Unwind_Resume` — a machine-level
proof, strictly stronger than the source `deny(indexing_slicing)` /
`deny(panic)` floor, that no bounds-check or panic survived optimization in the
dispatch's own frame. A regression that reintroduced an un-elidable `arr[i]` or
a fallible `unwrap` on the hot path turns this red. (2) **Instruction-count
ceiling**: the body must compile to no more than a committed golden
(`tests/hotpath_goldens/next_event_insn_ceiling.txt`), which fails only on real
GROWTH (bloat, a cold helper newly inlined into the hot frame, a slipped-in
branch) and is deterministic on the pinned toolchain; a deliberate change is a
reviewed golden diff regenerated with `BSQL_HOTPATH_PIN=overwrite` (mirroring
`TRYBUILD=overwrite`). Deliberately NOT an exact-asm golden (brittle to any
unrelated scheduling shift), and deliberately NOT a whole-body no-alloc claim:
`next_event`'s COLD control-frame branches (RowDescription schema parse, oversize
buffering) legitimately allocate, so the alloc family is documented, not gated —
the HOT DataRow arm's zero-allocation is proven separately by
`engine_query_alloc` (which drives `query_params` through `pump_active`, and
`pump_active` surfaces every row via `next_event`).

The `decoder_fuzz` gate (`crates/postgres/core/tests/decoder_fuzz.rs`) is the
UNIVERSAL-COVERAGE total-function proof for every decoder that turns untrusted
server or text bytes into a Rust value: on ANY input — malformed, truncated,
random, or hostile — the decoder must return `Ok` or a CLASSIFIED `Err`, and
NEVER panic or abort (a panicking decoder is a real vulnerability — a hostile
server byte could crash the driver). A hand-rolled xorshift64 PRNG with a FIXED
seed (no `rand`/`proptest`, no clock — fully reproducible, `deps_pin` unchanged)
feeds a broad length/content sweep (0 bytes up to 64 KiB, off-by-ones around
every fixed decoder width, plus semi-structured `numeric`/array frames crafted to
reach the deep base-10000 digit loop and the per-element length framing) to the
whole surface: `Cell<BinaryFmt/TextFmt>::decode` for every scalar, the array
decoders (`Vec<Option<T>>`), the `FromStr` parsers (`Uuid`/`Date`/`Time`/
`Numeric`), the SWAR fast-paths, and `parse_notification`. Because `cargo test`
runs the test profile (which inherits `dev` — unwind, unlike the `release`
`panic="abort"`), each decode runs under `catch_unwind` with a recording hook
that captures any panic's message + location WITHOUT spamming stderr; a caught
panic fails the gate reporting the exact decoder + input hex + panic. The gate
has teeth: a committed self-check routes a deliberately-planted panic through the
same harness and asserts it is caught + captured (which also confirms the profile
unwinds — under abort it would abort the binary), and a `total >= 150_000`
assertion refuses a vacuous pass. The decoders are byte-untouched (this is a
test); if it ever finds a real panic, the fix is a genuine safety fix (a bounds
check / classified error) in the decoder, never a suppression.

PG tests require: PostgreSQL on localhost:5432, user `smir-ant`, database `postgres`, trust auth.
SCRAM test requires: user `bsql_test_scram` with password `test_password_123` in pg_hba.conf.

## Architecture

- **Sans-IO engine** (`bsql-postgres-proto`, `engine` module): the session engine with zero I/O dependencies (`no_std + alloc`). Its seams: `Transport` (the driver-facing I/O seam, RPITIT + `Send`), `Live` (a branded, non-`Clone`, linear liveness token minted by `engine::session`), and the `Never` carrier for phase-impossible frames. Protocol logic lives here; the driver only supplies bytes.
- **Core** (`bsql-postgres-core`): the shared TRANSPORT-GENERIC driver engine `Core<S: Transport>` — it holds the sans-IO engine over a `Wire<S>` plus the linear liveness token and defines every non-I/O verb ONCE — alongside the result materializer, the dynamic `Row` / `QueryResult` types, `ConnectConfig`, TLS config, and the typed `Rows` / `RowsBuilder` containers. Both drivers build on it.
- **Drivers** are thin I/O adapters that plug their socket into the ONE `Core<S>`: `Core<TokioSocket>` on async, `Core<SyncSocket>` on sync (each socket is a TCP-or-unix enum), MONOMORPHISED per driver — static dispatch, no `dyn`. The verbs live once in `Core<S>`; the drivers differ only in `.await` vs blocking, so async/sync parity is a COMPILER guarantee, not hand-maintained twins.
- **Rows.** The dynamic `Row` (from `query_sql` etc.) is 16 bytes (`'static + Clone + Send + Sync`) over an `Arc`-shared arena: 4 heap allocations per whole `QueryResult`, regardless of row count. The typed `Rows<Q>` (from the `query!` flagship) is 2 allocations per result and 0 per row (borrowed, zero-copy decode).

## Safety invariants

- `#![forbid(unsafe_code)]` on all driver crates
- Every SHIPPED crate is unsafe-free (`#![forbid(unsafe_code)]` at its own root).
  `unsafe` exists only in `publish = false` (never-shipped) places: the
  `tools/devgates` building blocks (the counting allocator + the post-drop memory
  probe), and one justified `std::env::set_var` in each of the two consumer-fixture
  trybuild tests (`tools/query_fixture/tests/compile_fail.rs` and
  `tools/query_sqlite_fixture/tests/sqlite_gate.rs`, which forward the build-emitted
  catalog / SQLite-template rustc-env channels into the spawned trybuild children) —
  `set_var` is `unsafe` in edition 2024, used once, serially, before any trybuild
  child is spawned, with a `SAFETY` comment. None is in a shipped artifact.
- `#![deny(clippy::unwrap_used, clippy::expect_used)]` on all driver crates
- Static assertions: `Connection: Send`, `Row: Send + Sync + 'static`, `Pool: Send + Sync`
- NULL = `Option<NonZeroU32>` (compiler-enforced, no sentinel)
- `PreparedStatement` consumed by `close_statement(stmt)` — use-after-close is compile error
- Transactions via closure scope — no forgotten commits
- Passwords `Zeroizing<String>` — scrubbed on drop, redacted in Debug
- SQL identifiers spliced into DDL / COPY (table + schema names) go through
  `SafeIdent` / `SafeTable` — newtypes with a PRIVATE field and a validate-only
  constructor (`validate`), so an un-validated identifier CANNOT be spliced (no
  other way to build one exists). Identifier-injection safety is STRUCTURAL — a
  construction guarantee, not a runtime escape pass a call site could forget.

## Conventions

- No `expect()` or `unwrap()` in production code
- Error types: `DriverError::Db(DbError)` for server errors with SQLSTATE, `DriverError::Config` for pre-connect validation, `DriverError::NoRows` for empty results
- `ConnectConfig` is `#[non_exhaustive]` — construct via `new()` + builder methods
  (`footprint_pin!`-ed at 152 bytes)
- Runtime parameterized queries (`query_params` / `query_params_one` /
  `execute_params`) execute in a SINGLE round trip: `Parse`(unnamed) + `Bind` +
  `Describe`(portal) + `Execute` + `Sync` are fused into one flush, so a one-shot
  dynamic query costs one network round trip, not three (a separate prepare +
  bind + close).
- `copy_in` batches streamed `CopyData` to a 64 KiB threshold before flushing (a
  single chunk at or above the threshold streams DIRECTLY from the borrowed
  slice, never copied into the buffer), so a megarow bulk load costs about
  `total_bytes / 64 KiB` write syscalls instead of one per row, with the send
  buffer bounded under `2 ×` the threshold — constant memory regardless of COPY
  size.
- Transport (both drivers) is chosen by libpq's rule, centralized once in
  `core::resolve_endpoint`: an ABSOLUTE-PATH host (begins with `/`, e.g.
  `ConnectConfig::new("/tmp", …)`, `PGHOST=/var/run/postgresql`, or the DSN
  `postgresql://u@/db?host=/var/run/postgresql` query parameter — libpq's
  unix-socket URL form, since the path's leading `/` cannot ride the URL
  authority) selects a UNIX-DOMAIN socket at `<host>/.s.PGSQL.<port>`; every
  other host is TCP. A `host=` DSN parameter overrides the authority host (query
  param wins, libpq parity); an empty authority host with no `host=` is a loud
  `from_dsn` error naming the fix. The
  TCP/unix duality lives in a single socket enum ONE level below the concrete
  `Connection` (`transport::Sock` async, `transport::SyncSock` sync) so
  `Connection` and the engine stay monomorphic (no new generic, no `dyn`
  vtable-per-syscall). `TCP_NODELAY` and the `SSLRequest` probe are TCP-only. A
  unix socket is ALWAYS plaintext (`is_encrypted()` == false — TLS is pointless
  on a local kernel socket and PostgreSQL does not offer it there), so
  `SslMode::Require` over a unix host is a fail-loud `DriverError::Config`, and
  `Prefer` over unix is plaintext with NO downgrade warning (nothing was
  downgraded). Measured local win: the unix socket is ~2.4–2.9× faster than
  loopback TCP on the by-PK single-round-trip (`bench/benches/unix_vs_tcp.rs`).
- `SslMode::Prefer` warns on stderr (debug AND release) when the server refuses
  SSL and it falls back to plain TCP — an SSL downgrade is a security event a
  production build must not hide. A consumer can also assert
  `Connection::is_encrypted()` (both drivers) to reject a plaintext/downgraded
  connection. The default `SslMode` is still `Prefer` (ecosystem/libpq parity +
  localhost/dev ergonomics); flipping it to `Require` is a breaking change left
  for the owner / the 1.0 API freeze.
- Custom CA roots: `ConnectConfig::with_ca_roots(pem)` (or the `sslrootcert=<path>`
  DSN key / `PGSSLROOTCERT` env) verify against an internal/private CA, making
  `SslMode::Require` usable there instead of forcing plaintext. Stored raw, parsed
  into a rustls root store at connect; a bad/empty PEM is a fail-closed
  `DriverError::Config`, never a fallback to the baked roots or plaintext.
- The baked Mozilla CA bundle is behind the default-on `webpki-roots` feature
  (core → drivers → umbrella): a custom/pinned-CA-only consumer drops the
  ~55-65 KB blob with `default-features = false, features = ["tls"]`; with no
  roots and no custom CA, TLS fails CLOSED at the handshake.
- The whole rustls-backed encrypted transport is behind the default-on `tls`
  feature (core → drivers → umbrella; `webpki-roots` implies `tls`). With `tls`
  OFF (`default-features = false`) the ENTIRE ring/rustls subtree — ring (the
  single largest/longest-compiling runtime node), rustls, rustls-webpki,
  rustls-pki-types, webpki-roots, untrusted, once_cell (7 crates; measured async
  runtime graph 41 → 34, and a clean release build of the async driver ~11.7 s →
  ~6.8 s) — is dropped and NOT compiled, for the common localhost / unix-socket /
  trust-auth deployment that never negotiates TLS. FAIL-LOUD when off: the
  `SslMode` / `ca_roots_pem` are runtime data (DSN / env / builder), so the guard
  is a uniform runtime check at connect (in the driver's `build_tcp_wire`), not a
  compile gate (a compile gate on the builder would leave the `PGSSLMODE=require`
  DSN/env path an unchecked bypass): `SslMode::Require`, or a custom CA
  (`with_ca_roots` / `sslrootcert` / `PGSSLROOTCERT`), is a classified
  `DriverError::Config` at connect — NEVER a silent plaintext connect the consumer
  believes is encrypted. `SslMode::Prefer` connects plaintext with the SSLRequest
  probe compiled out, and `is_encrypted()` is then always `false`. The `tls`-off
  fail-loud is witnessed by `bsql-postgres-sync`'s `tls_off_fail_loud` test
  (`cargo test -p bsql-postgres-sync --no-default-features --test tls_off_fail_loud`).
- The whole SCRAM-SHA-256 authentication capability is behind the default-on
  `scram` feature (proto → core → drivers → umbrella; proto keeps
  `default = ["scram"]` so the standard `cargo test` runs keep the full SCRAM
  handshake + zeroize/fuzz coverage, and each shipped dependent takes proto with
  `default-features = false` so a consumer can drop it). With `scram` OFF and
  `tls` on, exactly FIVE SCRAM-exclusive crates leave the async runtime graph —
  `sha2`, `hmac`, `pbkdf2`, `base64ct`, `cpufeatures` (41 → 36). `subtle` and
  `getrandom` are NOT SCRAM-exclusive (rustls/ring keep them), so they drop only
  when `tls` is ALSO off; `md-5` stays unconditional (MD5 auth). With BOTH `tls`
  and `scram` off the minimal build is 27 runtime crates (vs 41 default). FAIL-LOUD
  when off: password auth is SCRAM-only, so a driver given a password with `scram`
  off is a classified `DriverError::Config` at connect naming the missing feature
  (Trust auth still works; a Trust client hitting a SCRAM-demanding server already
  fails with `ConnFail::UnsupportedAuthMethod`) — never a silent auth failure or
  panic. `ConnFail` shrinks 8 B → 2 B when the SCRAM leaf class is gated out,
  cascading to `HandshakeProgress` / `HandshakeOutcome` (feature-conditional
  `wire_pin!` pairs; the engine census counts the cfg-blind source text). The
  `scram`-off fail-loud is witnessed by `bsql-postgres-sync`'s
  `scram_off_fail_loud` test.
