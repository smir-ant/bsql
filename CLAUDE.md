# bsql — multi-backend SQL toolkit in Rust

Workspace version **1.0.0-alpha.5** (tag `v1.0.0-alpha.5` = HEAD, published to
crates.io). The API is alpha: breaking changes are allowed.

A compile-checked `query!` macro, PostgreSQL async + sync drivers, and an
embedded SQLite backend, all reachable through the single `bsql` umbrella crate.
This file and the code are the source of truth; the old blueprint/CREDO
documents are no longer in the repo.

## Direction

- **Query API is SQL text.** SQL lives in the `query!` macro, validated at build
  time against the schema replayed from migration DDL. Combinator builders
  (diesel-style `.filter().eq()`) were tried and reverted — do not reintroduce a
  runtime SQL builder.
- **Parameters are always binary.** `build_bind` emits one Binary format code
  for all params; `ParamsWriter::FORMATS` is Binary at every arity, and
  `ParamsWriter` is the only param-encode authority. Result columns are binary
  only on the typed macro path (`build_bind_prepared`); the dynamic path sends
  `n_result_formats = 0` and decodes per the `RowDescription`.
- **Compile-checked query API.** `bsql-build` (a `[build-dependencies]` helper)
  replays the consumer's migration `*.sql` into a catalog the
  `bsql-query-macros` proc-macro reads; reachable through `bsql` with
  `features = ["macros"]`. **Guarantee boundary:** the catalog matches the
  migration *files*, not the live database — DDL applied out-of-band is
  invisible by design. Any change to the migration set at any directory depth
  recompiles and re-validates. A DDL form the replay cannot model faithfully
  (`CREATE TABLE ... AS SELECT`, `SELECT ... INTO`) is a build error, never a
  silently-wrong catalog.
- **Views are modeled relations.** `replay_create_view` infers a
  `CREATE [OR REPLACE] [MATERIALIZED] VIEW` body through the same public
  `infer_query` a `query!` uses, registering columns in `catalog.tables` and the
  name in `catalog.views`. Replay is ordered, so view-over-view works.
  Nullability is the inference engine's own (LEFT JOIN → `Option`, `COALESCE` →
  non-null, aggregate → nullable). `DROP [MATERIALIZED] VIEW` unregisters;
  `CREATE OR REPLACE VIEW` re-infers and replaces, so a dropped column stops
  resolving. Serialized catalog line = 6 tab-separated fields:
  `table \t column \t pg_type \t not_null \t primary_key \t is_view`.
  - *Skip on failure:* a view the engine cannot type — `SELECT *`, an
    unsupported column type, an uninferable expression (a bare `sum()` needs
    `::cast`), a TEMPORARY or `TO`-target view, a non-public schema, a name
    colliding with a base table, a column-list arity mismatch, a duplicate
    output name — is left out of *both* `tables` and `views`, so a `query!`
    against it is a loud unknown-relation error. Skipping never fails the build:
    a view is a leaf, nothing `ALTER`s its columns.
  - *Write rejection:* any INSERT/UPDATE/DELETE targeting a modeled view is
    `InferError::WriteToView` at compile time, via the single
    `reject_write_to_view` choke point. bsql cannot distinguish PostgreSQL's
    auto-updatable views, so it rejects all view writes.
  - *Not modeled:* a bare `ALTER VIEW … RENAME COLUMN` is not re-inferred (fails
    safe — the catalog keeps the last shape; reshape with `CREATE OR REPLACE
    VIEW`). `REFRESH MATERIALIZED VIEW` is not parsed by the pinned sqlparser,
    so it must not appear in a migration file.
- **Toolchain pinned** to rustc 1.96.0 (`rust-toolchain.toml`,
  `components = ["clippy"]`, `profile = "minimal"`); trybuild/clippy goldens
  capture diagnostics verbatim, so the patch version is fixed.
- **Converge, don't drift.** Fix a doc that contradicts current behaviour; do
  not append to it.

### CI

CI exists. This reverses the former "no CI" mandate, which was about not adding
runtime dependencies to the crate graph — a test workflow adds none.

- `.github/workflows/test.yml`, on push / PR / dispatch, concurrency-cancelled
  per ref:
  - `offline` (ubuntu): `rustup component add rust-src` (so const-eval trybuild
    goldens render as on the dev host), build `--all-targets`,
    `clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace -- --skip alloc`, doctests, devgates
    `deps_pin`/`runtime_graph_pin`/`doc_links`, `engine_hotpath_codegen`,
    `decoder_fuzz`, the `--no-default-features` checks + tls/scram/md5 fail-loud
    tests, clippy under `test-harness` and `n1-detect`.
  - `live`: PostgreSQL 15/16/17/18 matrix (fail-fast off). Recreates the
    documented local prerequisites, then runs the `--ignored` suites at
    `--test-threads=1` for both PG drivers, `query_fixture`,
    `query_bridge_fixture`, `test_harness_fixture`, plus `query_fixture` with
    `n1-detect`.
- `.github/workflows/publish.yml` publishes the 10 shippable crates to crates.io
  in topological order on a published Release or manual dispatch; idempotent and
  429-aware; needs the `CRATES_IO_TOKEN` secret.
- **Local gates stay the primary wall.** Three gate classes CI cannot cover, so
  run them on the dev host before landing: the `engine_*_alloc`
  global-allocator gates (environment-pinned, skipped by `--skip alloc`), the
  `next_event` instruction-count ceiling (golden is aarch64 — the panic-freedom
  and cold-helper checks do run on x86_64), and devgates `cross_platform`
  (needs `rustup target add`).

## Workspace layout

LoC are `find <crate>/src -name '*.rs' -exec cat {} + | wc -l` at commit
`3fa99091` (counts inline `#[cfg(test)]` modules). A scale snapshot, not an
invariant — re-measure before quoting.

```
crates/
  bsql/            — umbrella facade: modules pg / pg_sync / sqlite / testkit; re-exports
                     query!, copy!, user_types!, #[bsql::test]; the one cross-backend
                     N1Report; the cross-backend BackendError + SyncBackend / SyncQueries
                     / RunsOn traits (src/backend.rs)                                — 1662
  postgres/
    proto/         — sans-IO wire protocol + session engine (no_std + alloc) + PGCOPY
                     binary framing + TypedCopyIn                                   — 33834
    core/          — transport-generic Core<S> + materializer + types + config + TLS + Rows
                     + notify ledger + SafeIdent + cancel key/redial + copy_in_typed +
                     pipeline / execute_batch / query_batch + connection-identity-bound
                     prepared statements + dynamic statement cache + diagnostics +
                     the PG-side migration I/O runner                               — 18367
    async/         — tokio driver plugging its socket into Core<S> + CancelToken +
                     migration try-lock poll                                         — 4279
    sync/          — std::net blocking twin of the above                             — 3902
  sqlite/
    driver/        — embedded SQLite (bundled rusqlite): typed query! runtime, explicit
                     prepared-statement handles, interrupt CancelToken, the SQLite-side
                     migration I/O runner                                            — 4673
  common/          — zero-dependency leaf (no [dependencies] at all, by design): migration
                     pure logic (checksum / ordering / drift authority + source loader) +
                     the N+1 detector (feature `n1`). One compiled source for both PG core
                     and SQLite, replacing two hand-maintained copies                — 1162
  testkit/         — deterministic in-memory fake PostgreSQL for driver tests (no network) — 1022
  build/           — build-dependency: migration DDL → schema catalog (tables + user types
                     + views), SQLite template, the $N→?N placeholder authority
                     (`sqlite_placeholder_form`), migration embed (`emit_migrations`);
                     entry points emit / emit_catalog / CatalogBuilder / infer_query — 38870
  query-macros/    — proc-macro: query!, copy!, user_types!, #[bsql::test]            — 3275
examples/          — `bsql-examples`, publish = false, workspace member: 16 runnable
                     per-feature programs + migrations + a schema-per-test test       — 1563
```

- `build/` is 78% `src/infer.rs` (30425 lines), of which ~13.7K is an inline
  `#[cfg(test)]` module.
- **Publishable (10, none carries a `publish` key):** `bsql`,
  `bsql-postgres-{proto,core,async,sync}`, `bsql-sqlite`, `bsql-common`,
  `bsql-testkit`, `bsql-build`, `bsql-query-macros`.
- **`publish = false` (8):** under `tools/` — `bsql-devgates`,
  `bsql-query-fixture`, `bsql-query-bridge-fixture`, `bsql-query-sqlite-fixture`,
  `bsql-test-harness-fixture`, `bsql-syncbackend-fixture`, `bsql-corpus`; plus
  `bsql-examples` at `examples/`.
- `bsql-examples` is the only member depending on `chrono` — which is why
  `chrono` + `num-traits` appear in the `deps_pin` golden though no shipped
  crate uses them.

## Build & test

Every `-p` package and `--test` target below resolves; no test target has
`required-features`.

```bash
# whole workspace
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings   # 0-warning wall; -D is what enforces it
cargo test --workspace                                  # offline suite (aarch64-macOS dev host)
cargo test --workspace -- --skip alloc                  # elsewhere: the *_alloc gates are host-pinned
cargo test --workspace --doc

# devgates (tools/devgates)
cargo test -p bsql-devgates --test deps_pin           # Cargo.lock package-set golden (BSQL_DEPS_PIN=overwrite)
cargo test -p bsql-devgates --test runtime_graph_pin  # sqlparser/bsql-build absent from every shipped runtime graph
cargo test -p bsql-devgates --test doc_links          # broken intra-doc links denied
cargo test -p bsql-devgates --test cross_platform     # Windows/Linux cargo check; no-op PASS if target not installed

# offline engine / decode gates
cargo test -p bsql-postgres-proto --test engine_hotpath_codegen         # next_event panic-free + instruction ceiling (BSQL_HOTPATH_PIN=overwrite)
cargo test -p bsql-postgres-proto --test engine_copy_typed_alloc        # typed binary COPY constant memory
cargo test -p bsql-postgres-proto --test engine_query_break_alloc       # dynamic streaming: alloc count row-count-independent
cargo test -p bsql-postgres-proto --test engine_execute_batch_alloc     # execute_batch constant SEND memory
cargo test -p bsql-postgres-proto --test engine_pipeline_windowed_guard # intermediate-window OID-mismatch BAIL
cargo test -p bsql-postgres-core  --test decoder_fuzz                   # no decoder panics on any input
cargo test -p bsql-postgres-core  --test tls_resumption                 # shared ClientConfig resumes; resumed session keeps original peer cert
cargo test -p bsql-testkit        --test wide_overcap_stress            # over-cap RowDescription drains + recovers, 40x loop

# feature-gating fail-loud gates
cargo test -p bsql-postgres-sync  --no-default-features --test tls_off_fail_loud
cargo test -p bsql-postgres-sync  --no-default-features --test scram_off_fail_loud
cargo test -p bsql-postgres-proto --no-default-features --test md5_off_fail_loud

# non-default features
cargo clippy -p bsql --features test-harness --all-targets -- -D warnings
cargo test   -p bsql --features test-harness --lib            # harness unit tests (offline)
cargo clippy --workspace --features n1-detect --all-targets -- -D warnings
cargo test   -p bsql-common                                   # migration pure logic (checksum/order/drift)
cargo test   -p bsql-common --features n1                     # + N1Tracker

# SQLite (in-process, no PG)
cargo test -p bsql-sqlite
cargo test -p bsql-sqlite --test cancel
cargo test -p bsql-sqlite --test migrate
cargo test -p bsql-query-sqlite-fixture --test execute_batch_sqlite
cargo test -p bsql-query-sqlite-fixture --test query_batch_sqlite
cargo test -p bsql-query-sqlite-fixture --test pipeline_sqlite
cargo test -p bsql-query-sqlite-fixture --features n1-detect --test n1_detect_sqlite

# offline fixture gates
cargo test -p bsql-query-fixture --test copy_typed_offline     # copy! expansion + row shape
cargo test -p bsql-query-fixture --test embed_migrations_live  # embed baked-set shape (also has one --ignored live test)

# live PostgreSQL — driver suites (--ignored)
cargo test -p bsql-postgres-async --test sq_live   -- --ignored
cargo test -p bsql-postgres-sync  --test sync_live -- --ignored
cargo test -p bsql-postgres-async --test sq_live   cancel_token_stops -- --ignored
cargo test -p bsql-postgres-sync  --test sync_live cancel_token_stops -- --ignored
cargo test -p bsql-postgres-async --test sq_live   query_each -- --ignored
cargo test -p bsql-postgres-sync  --test sync_live query_each -- --ignored
cargo test -p bsql-postgres-{async,sync} --test pool_liveness    -- --ignored  # get() bounded behind a black-hole relay
cargo test -p bsql-postgres-{async,sync} --test direct_liveness  -- --ignored  # in-flight query bounded; mid-query FIN classified
cargo test -p bsql-postgres-{async,sync} --test midstream_faults -- --ignored  # fault matrix: classified + bounded, never torn
cargo test -p bsql-postgres-{async,sync} --test migrate_live     -- --ignored  # runner (sync adds concurrency + CONCURRENTLY)
cargo test -p bsql-postgres-async --test streaming_scale      -- --ignored  # 5M-row stream, RSS O(1) (48 MiB margin)
cargo test -p bsql-postgres-async --test tls_fragmentation    -- --ignored  # own ephemeral SSL PG behind a 1/3-byte fragmenting relay
cargo test -p bsql-postgres-async --test channel_binding_plus -- --ignored  # own ephemeral SSL+SCRAM PG; -PLUS over TLS, Require over plaintext fails closed
cargo test -p bsql-postgres-async --test md5_auth_live        -- --ignored  # own ephemeral cluster; MD5 + SCRAM + cleartext-over-plaintext refusal

# live query! flagship (--ignored, tools/query_fixture)
cargo test -p bsql-query-fixture --test query_live_{async,sync}      -- --ignored
cargo test -p bsql-query-fixture --test view_live_{async,sync}       -- --ignored  # query! against a VIEW (TEMP-shadowed, parallel-safe)
cargo test -p bsql-query-fixture --test pipeline_live_{async,sync}   -- --ignored
cargo test -p bsql-query-fixture --test query_oid_guard_live         -- --ignored  # typed result-OID guard (run WITHOUT --test-threads=1)
cargo test -p bsql-query-fixture --test pipeline_oid_guard_live      -- --ignored  # per-command guard -> BatchColumnOidMismatch
cargo test -p bsql-query-fixture --test pooled_typed_shadow_live     -- --ignored  # typed cache dropped on checkout (cross-tenant leak regression)
cargo test -p bsql-query-fixture --test copy_typed_live_{async,sync} -- --ignored
cargo test -p bsql-query-fixture --test execute_batch_live_{async,sync} -- --ignored
cargo test -p bsql-query-fixture --test query_batch_live_{async,sync}   -- --ignored
cargo test -p bsql-query-fixture --test embed_migrations_live -- --ignored
cargo test -p bsql-query-fixture --test pipeline_bench        -- --ignored  # K-vs-serial round-trip measurement
cargo test -p bsql-query-fixture --test execute_batch_bench   -- --ignored

# live, DSN-driven
BSQL_TEST_DSN=postgres://USER@localhost/postgres cargo test -p bsql-test-harness-fixture -- --ignored
BSQL_TEST_DSN=postgres://USER@localhost/postgres cargo test -p bsql-query-fixture --features n1-detect -- --ignored
```

Live targets not listed individually (`query_enum/domain/alter_type/composite_live`,
`nullable_param_live`, `copy_wide_cap_live`, `n1_detect_live`, `bridge_live`,
`drop_recovery`, …) run via `cargo test -p <pkg> --tests -- --ignored`, which is
how CI covers them.

**Live PG prerequisites.** Server on localhost:5432; SUPERUSER role `smir-ant`
(suites create/drop schemas); database `postgres`; trust auth;
`unix_socket_directories` including `/tmp` (suites connect to host `"/tmp"`);
`ssl = off` (suites use the defaulted `SslMode::Prefer` against localhost, so a
distro snakeoil cert would be negotiated and then correctly rejected). SCRAM
suites also need role `bsql_test_scram` / password `test_password_123` with
`scram-sha-256` in `pg_hba.conf`. The TLS / channel-binding / MD5 suites start
their *own* ephemeral clusters: they need `initdb`/`openssl`/`psql` on PATH, must
not run as root, and skip cleanly otherwise.

## Consumer wiring — `query!`

Add `bsql` with `features = ["macros", "postgres-async"]` (or `postgres-sync`)
to `[dependencies]`, `bsql-build` to `[build-dependencies]`, and a one-line
`build.rs`:

```rust
fn main() -> Result<(), bsql_build::BuildError> {
    bsql_build::emit("migrations")   // replays migrations/ into the catalog
}
```

- `emit` = `emit_catalog` plus, when the build-dep's `sqlite` feature is on, the
  SQLite conformance template (inseparable — no second call to forget). A
  PostgreSQL-only build can call `bsql_build::emit_catalog("migrations")`.
- `bsql::query!(Name, "<SQL>")` types the SQL against the catalog and emits the
  owned record `Name` (`text` → `String`), which *is* the runnable carrier: it
  implements `TypedQuery`, so `conn.query::<Name>(params)` runs it and
  `query_one`/`query_opt` return the owned `Name`. One user-facing name — do not
  reintroduce a separate `NameQuery` marker: only a lifetime-free type can be a
  turbofish carrier, and making the owned record the carrier removes the old
  "record vs carrier" footgun by construction.
- A borrowing query additionally emits the zero-copy view `NameRef<'q>` (`text`
  → `&'q str`, `bytea` → `&'q [u8]`), which is the `Rows<Name>::iter()` /
  `query_each` item. An all-scalar query has no borrowed twin.
- An unknown column is a `compile_error!`.
- A `$N` parameter's type *and* nullability come from context: a bare `$N` into
  a nullable column (an `INSERT ... VALUES` cell, an `UPDATE` / `ON CONFLICT DO
  UPDATE SET` value) is `Option<T>`; a NOT NULL target is `T`; a `$N` in a
  comparison/`WHERE` position or with an explicit `::cast` stays `T`. A param
  used in any non-null context stays `T` even if it also appears in a nullable
  target. The wire OID is unaffected, so this is purely the Rust surface type.
- A runtime `ORDER BY { ... }` query keeps separate uninhabited `Name…Query`
  carriers, one per ordering (one record cannot carry N orderings' prepared
  plans), selected via the `NameOrderBy` selector. `TypedQuery`'s
  `#[diagnostic::on_unimplemented]` covers the residual misuse: turbofishing
  `NameRef`, or turbofishing a runtime-ORDER-BY record (rustc then lists the
  real carriers). SQLite's `SqliteTypedQuery` has the peer diagnostic.
- The expansion names only `::bsql::__rt::…` and — with `macros` + `sqlite` —
  `::bsql::__rt_sqlite::…`, both `#[doc(hidden)]`, so no other compile-time
  dependency is needed; `bsql-query-macros` is host-only and never enters the
  runtime binary.
- `tools/query_fixture` is the end-to-end fixture (26 migrations, plus
  `runner_migrations/` for the embed chain).

### External-type bridges

`bsql_build::Catalog::from_migrations(dir).bridge(pg_type, target_path,
converter_path).emit()` (`.emit_catalog()` = PG-only) makes `query!` decode a
column into a chosen external type. One infallible `fn(OwnedNative) -> Target`
free function per bridged type — the orphan-proof seam (a foreign↔foreign
`impl bsql::Cell` is E0117). Paths travel as strings over the
`BSQL_TYPE_BRIDGES` rustc-env channel, so bsql gains no dependency; an
unreadable file fails closed.

Matching is on the resolved native pivot `RustType`, not the type string
(`text`/`varchar` share one pivot): no pivot = `BuildError::UnknownBridgeType`,
two bridges per pivot = `ConflictingBridge`, a bridge matching no catalog column
= a `cargo:warning` (it may still fire on a CAST). A bridge reshapes only the
record field *value*; the row-tuple marker keeps the native pivot, so the
compile-time row-OID guarantee and the E0080 param-OID pin stand. A bridged
column is owned in both record twins (gives up the zero-copy `&'q str`); a
bridged 1-D array is `Vec<Option<Target>>` with the converter per element; a
bridged column suppresses that query's SQLite typed bridge; enums and composites
are never bridge targets and composite fields are never bridged. Fixture:
`tools/query_bridge_fixture`.

### Generated user types — `bsql::user_types!()` (feature `macros`)

No arguments; invoke once in a module in scope at the `query!` sites. Rides its
own `BSQL_USER_TYPES` channel, so the schema-catalog format and goldens are
untouched. A non-identifier type name or label, or two labels PascalCasing to
one variant, is a `compile_error!`.

- **ENUM** → `pub enum Mood {..}` in declared order (= PostgreSQL's sort order,
  so the derived `Ord` matches the server); derives `Debug, Clone, Copy,
  PartialEq, Eq, PartialOrd, Ord, Hash`. `ALTER TYPE … ADD VALUE [IF NOT EXISTS]
  [BEFORE|AFTER] / RENAME VALUE / RENAME TO` are fully replayed in place, order
  preserved, never skipped; `ADD VALUE` on a domain is a loud
  `BuildError::Replay`. On the wire an enum is its label text, so decode rides
  the `text` pivot plus `PgEnum::from_wire_label`; an undeclared live label is
  `DecodeError::UnknownEnumLabel`. A parameter binds as `EnumLabel<E>`
  (`v.as_label()`) with OID 0 `unspecified` — a `text`/25 parameter is rejected,
  since a PG enum has no implicit text cast; the phantom `E` separates two enums
  at compile time.
- **DOMAIN** → transparent: types exactly as its base, following
  domain-over-domain/-enum/-composite chains to `MAX_USER_TYPE_DEPTH = 32`
  (cyclic or deeper fails closed as unresolved). No generated type; `CHECK` is
  server-enforced.
- **COMPOSITE** → `struct Addr { street: Option<String>, .. }`; every field is
  `Option<T>` (PostgreSQL forbids `NOT NULL` on a `CREATE TYPE … AS` attribute
  and the frame carries a per-field `-1`). Owned `'static`; derives `Debug,
  Clone, PartialEq` only (a float field forbids `Eq`/`Ord`/`Hash`).
  `CompositeReader` walks the `record_send` frame (`int32` field count, then
  `{uint32 oid, int32 len, byte[len]}` per field), recursing into each field's
  own decoder; field types resolve through the same `resolve_field_type` chain a
  column uses. Validated by **position + arity only** — the wire field OID is
  read and ignored, being server-dynamic: wrong count is
  `DecodeError::CompositeArityMismatch`, malformed or trailing-surplus is
  `CompositeTruncated`. Residual: a same-width live retype (declared `int4`,
  live `float4`) is not caught. A composite `$N` *parameter* is a loud located
  compile error, staged as a whole because an enum/domain/nested field needs
  server-dynamic OIDs — use separate scalar params or `ROW($1,$2)::your_type`.
  Attribute-level `ALTER TYPE (ADD|DROP|ALTER|RENAME) ATTRIBUTE` is a loud
  sqlparser parse error (the pinned grammar models only the three enum ops), so
  drift is a build error; `RENAME TO` re-keys through the generic path.
- **Boundary (enum + composite):** pins the type name and the variant/field set
  from the migration files. There is no compile-time OID pin (a user type's OID
  is server-assigned) and deliberately **no** connect-time OID resolution — do
  not add one. Live drift surfaces as `UnknownEnumLabel` /
  `CompositeArityMismatch`.
- **SQLite:** an enum or composite column (and any bridged column) has no SQLite
  storage class, so no `SqliteTypedQuery` impl is emitted for that query and
  `sqlite_conn.query::<Q>()` is a located compile error.
- Fixture migrations: `0014_moods` / `0015_domains` / `0016_alter_type_evolve` /
  `0017_composites`.

### Schema-per-test — `#[bsql::test]` (feature `test-harness`, non-default)

Pulls both PG drivers + core + tokio. Per test: resolve `BSQL_TEST_DSN`
(`test_harness::DSN_ENV` — deliberately not `DATABASE_URL`, since the harness
creates and drops schemas) → an admin connection → `CREATE SCHEMA` → a second
connection carrying the schema as its connect-time `search_path` (a
startup-packet GUC that survives the pool's `RESET ALL`) → body under
`catch_unwind` → `DROP SCHEMA IF EXISTS … CASCADE` on the admin connection →
`resume_unwind` (so `#[should_panic]` still works). Two connections per test.

Schema name `bsql_t_<pid>_<seq>[_<name>]`, ≤ 63 B (`NAMEDATALEN - 1`), suffix
lowercased with non-alphanumerics → `_` and truncated to budget; unique by pid +
a process-global `AtomicU64` (no randomness); passed through
`SafeIdent::validate` before splicing, so a raw `&str` cannot reach the DDL. An
`async fn` gets a per-test current-thread tokio runtime + `block_on`; a plain
`fn` gets the blocking driver; a connection argument not matching the async-ness
is a compile error. A missing / non-UTF-8 / unparseable DSN is a loud panic
naming the variable. Fixture: `tools/test_harness_fixture`.

## Architecture

- **Sans-IO engine** (`bsql-postgres-proto`, `engine` module): `no_std + alloc`,
  zero I/O. Seams in `engine/seams.rs`: `Transport` (RPITIT, `-> impl Future +
  Send`, `type Error: Send`), `Live<'b>` (a branded ZST liveness token, no
  `Clone`/`Copy`, minted only in-crate by a session scope), and `Never` (an
  uninhabited carrier for phase-impossible frames). Protocol logic lives here;
  drivers only supply bytes.
- **Core** (`bsql-postgres-core`): `Core<S: Transport<Error = io::Error>>` holds
  the engine over a `Wire<S>` plus the liveness token and defines every non-I/O
  verb once. Also: the result materializer, dynamic `Row`/`QueryResult`,
  `ConnectConfig`, TLS config, and typed `Rows` (built via the `#[doc(hidden)]`
  `RowsBuilder` prebuffer).
- **Drivers** are thin I/O adapters: `Core<TokioSocket>` (async),
  `Core<SyncSocket>` (sync). Each socket is a struct wrapping a TCP-or-unix enum
  (`transport::Sock` / `transport::SyncSock`) plus its deadline handle.
  Monomorphised, no `dyn`, so async/sync verb parity is a compiler guarantee.
- **Rows / allocation model.** Dynamic `Row` is 16 B (`footprint_pin!`),
  `'static + Clone + Send + Sync`, an `Arc` into a shared arena
  (`ArenaInner { data: Vec<u8>, slots: Vec<ColSlot>, n_cols }` = 3 allocations);
  `QueryResult` (72 B pin) adds `column_names: Arc<[String]>` and holds one lazy
  `RowSet` (16 B pin), minting `Row` handles on `.get(i)` / `.iter()` — never an
  eager `Vec<Row>`. The gated invariant is allocations O(1) in row count, 0 per
  row; the pinned numbers are `EAGER_QUERY_ALLOC_PIN = 18` and
  `RESET_ALLOC_PIN = 14` in `crates/postgres/core/tests/materialize_alloc.rs`.
  Typed `Rows<Q>` is 2 allocations per result, 0 per row, borrowed zero-copy
  decode.
- **SQLite runs the same lazy arena model:** a `data` byte pool + a `CellSlot`
  table (integer/real inline, text/blob as `(offset, len)`) + `Arc<[String]>`
  names inside the `Arc`-shared `ArenaInner`; `Row` is 16 B and carries its own
  names, so `get_by_name` threads no slice. Text UTF-8 is validated lazily at
  `get::<&str>`, never failing the whole result eagerly. A result overflowing
  the 32-bit slot fields (`> 4 GiB`) is `SqliteError::ResultTooLarge` — stream it
  via the capless `query_each_raw`. Typed `TypedRows<Q>` wraps the same arena;
  borrowed records alias it through an `ArenaRowRef` per-get view.

### SQLite parity

- **Verbs match PG.** Typed: `query` / `query_one` / `query_opt` / `query_each`,
  plus `execute_batch::<Q>`, `query_batch::<Q>`, `pipeline`. Dynamic raw SQL
  carries `_raw`: `query_raw` / `query_one_raw` / `query_opt_raw` /
  `query_each_raw` / `execute_raw` / `execute_batch_raw` (the multi-statement
  script executor). The runtime-bound-parameter family keeps its own names:
  `query_params`, `query_params_one/opt`, `query_each_params`, `execute_params`.
  Cross-backend: `cancel_token()`, `n1_report()`.
- **Typed flagship.** A `query!` carrier for a SQLite-decodable query implements
  `SqliteTypedQuery`, emitted only under `bsql-query-macros/sqlite-runtime`
  (the umbrella `sqlite` feature), so a PG-only expansion is byte-identical.
  Build-time SQLite conformance is the *orthogonal* `macros-sqlite` →
  `bsql-query-macros/sqlite`; enable both — runtime-only is still fail-loud but
  lacks the proof that real SQLite resolves the row shape. Decode verifies each
  value's storage class via `FromColumn`: a mismatch is
  `SqliteError::TypeMismatch`, a NULL in a non-`Option` field is
  `UnexpectedNull` — never a silent coercion. One macro-emitted `decode_row`
  serves both eager and streaming via the `ColumnSource` seam. A PG-only carrier
  gets no `SqliteTypedQuery` impl, so `sqlite_conn.query::<That>()` is a located
  E0277.
- **Row-count contract.** Typed `query_one`/`query_opt` are exactly-one /
  at-most-one (`SqliteError::TooManyRows` on 2+, one extra step, no
  materialization) — same as PG. The dynamic `*_raw` verbs stay first-row.
- **Parameters, two vocabularies.** Typed verbs take `Q::Params<'p>` — the same
  lifetime-GAT tuple as PG's `TypedQuery::Params<'p>`, so a runtime
  `String`/buffer binds — bound by the sealed `SqliteBindParams` over
  `SqliteBindValue`, each element positionally via rusqlite's zero-alloc
  `raw_bind_parameter` in its true storage class. A parameter SQLite cannot bind
  (`u64`, `Uuid`, `Numeric`, temporal, `EnumLabel`) is a compile error *at the
  `query::<Q>` call* — the bound is on the verb, not the associated type, so a
  PG-only-param carrier still gets its `SqliteTypedQuery` impl. Dynamic verbs
  take `&[ValueRef]`.
- **Transparent statement cache.** The eager / execute / typed-single-row verbs
  prepare through a per-connection LRU (rusqlite `prepare_cached`), keyed on SQL
  text, default capacity 16, tunable via
  `Connection::set_prepared_statement_cache_capacity` (`0` disables). A returned
  statement is reset with bindings cleared; a schema change is handled by
  `prepare_v3` auto-reprepare or a classified error. The **streaming** verbs
  deliberately do not cache: `prepare_cached` forces
  `SQLITE_PREPARE_PERSISTENT`, which bypasses lookaside and slows multi-row
  stepping, and streaming has no per-cell copy to mask it.
- **Explicit handles.** `prepare_raw(sql) -> SqliteStatement<'conn>` (verbs
  `execute`/`query`/`query_one`/`query_opt`/`query_each`) and
  `prepare::<Q>() -> SqliteTypedStatement<'conn, Q>` (typed verbs). Each holds
  one plain non-persistent `rusqlite::Statement` borrowing the connection — no
  `unsafe`, no self-referential cache; the borrow checker keeps the connection
  alive. The typed handle keeps every guarantee and checks the `?N`↔tuple arity
  once at prepare. Works inside a `transaction` closure and honors a
  `cancel_token` interrupt. Under `n1-detect` the typed handle's read verbs
  record; the dynamic handle does not.
- `Connection::transaction` hands the closure a borrowing `Transaction` guard
  exposing only data verbs, so a nested/manual-commit desync is E0599. `open`
  sets `DEFAULT_BUSY_TIMEOUT = 5 s`; `set_busy_timeout(Duration::ZERO)` restores
  immediate fail-loud. Affected counts are `u64`. The `$N`→`?N` rewrite has one
  authority, `bsql_build::sqlite_placeholder_form`, shared by the build-time
  conformance oracle and the macro's baked SQLite `const SQL`.

## Errors, config, connections

**Error taxonomy.** `DriverError::Db(Box<DbError>)` carries the 5-byte SQLSTATE
(`DbError` pinned 104 B, boxed so the enum stays 24 B). Pre-connect validation is
one family: `Config(&'static str)` (fixed message) and `ConfigDynamic(Box<str>)`
(runtime-computed, names the offending value, e.g. `invalid port: 99999`);
`is_config()` is true for either. `from_dsn` / `from_env` return
`Result<Self, DriverError>`, not `String`. `NoRows` = a required row was absent.
`footprint_pin!(DriverError, size = 24, align = 8)`.

**`is_disconnect()` — reconnect vs. fix-the-query.** A predicate over the
existing variants; no new variant, no string matching. `true` for `Io`,
`NotReady`, `Timeout`, and for `Db`/`BatchFailed` whose SQLSTATE satisfies
`DbError::is_connection_error()` = `code.starts_with("08") ||
code.starts_with("57P")`. `false` for everything else, exhaustively listed (no
wildcard, so a new variant forces a decision): notably `57014` query_canceled and
`57000` (the `57P` prefix must not sweep the wider `57` class), `SslRefused` (the
connect never established), `NoRows`, `Config`/`ConfigDynamic`, `PoolTimeout`,
decode/column errors, `ParamTypeMismatch`, `ParamCountMismatch`,
`WrongConnection`, `BatchColumnOidMismatch`. SQLite peer: the broken-handle set
`SQLITE_IOERR`/`CORRUPT`/`CANTOPEN`/`NOTADB` (extended codes mask to the
primary), false for `SQLITE_BUSY`, interrupts, and constraint/type errors — one
reconnect/reopen decision across backends.

**`ConnectConfig`** is `#[non_exhaustive]`;
`footprint_pin!(ConnectConfig, size = 152, align = 8)`. Every later feature that
could have added a knob (keepalive, reset liveness bound, reaper bounds,
client-liveness window) deliberately did not, to hold this pin.

**Connect handshake is aggregate-bounded by `connect_timeout` on both drivers.**
Async: one `tokio::time::timeout` around the whole `connect_inner` (dial + TLS +
startup/auth). Sync: the handshake runs inside a single blocking `poll_once`, so
a per-read `SO_RCVTIMEO` alone does *not* bound the total — a server dripping
state-non-advancing frames (`NoticeResponse`/`ParameterStatus`, legal in any
handshaking state) inside each read window pumps forever, and a few such connects
exhaust a blocking pool. So the sync socket carries `transport::ConnectDeadline`
(`Arc<AtomicU64>` nanos-from-epoch, `0` = disarmed), armed with `connect_timeout`
before the handshake and disarmed the instant it completes. Per read it re-arms
`SO_RCVTIMEO` to the remaining budget, floored at `MIN_CONNECT_READ_BUDGET` = 1 ms
(a zero timeout would *disable* the bound), and short-circuits to
`DriverError::Timeout` once spent. Steady state costs one relaxed atomic load. It
also bounds the TLS handshake reads and the throwaway cancel dial. Pins:
`SyncSocket` 16 B, `SyncSock` 8 B / align 4.

**Transport selection.** `core::resolve_endpoint(host, port)` is the one
authority: an absolute-path host (leading `/`) → `Endpoint::Unix(<host>/.s.PGSQL.<port>)`,
every other host → `Endpoint::Tcp`. A DSN `host=` query parameter overrides the
authority host (libpq parity); an empty authority with no `host=` is a loud
`from_dsn` error. The TCP/unix duality lives in a socket enum one level below
`Connection`, so `Connection` and the engine stay monomorphic. `TCP_NODELAY` and
the `SSLRequest` probe are TCP-only. A unix socket is always plaintext
(`is_encrypted()` false): `SslMode::Require` over unix is a fail-loud
`DriverError::Config`; `Prefer` over unix is plaintext with no downgrade warning.

**TCP keepalive is on by default** (both drivers, TCP arm only — a unix socket
has no keepalive). `KEEPALIVE_IDLE` = 60 s, `KEEPALIVE_INTERVAL` = 10 s, set
right after connect via `socket2::SockRef::from(&stream).set_tcp_keepalive` (the
`unsafe` fd handling stays inside socket2, so the drivers keep
`#![forbid(unsafe_code)]`). Matches libpq. No config knob.

## Pool

**`get()` is bounded even on a dead peer.** Every reused connection is
health-gated by `reset_session` at checkout — the exactly-once liveness proof
before the user's (possibly non-idempotent) verb runs. The reset is bounded by
the connection's own `connect_timeout` (no new config knob): async arms the same
`ReadDeadline` absolute deadline via `arm_scoped` (RAII disarm, so a dropped
future cannot strand it); sync arms `SO_RCVTIMEO` + `SO_SNDTIMEO` and restores
the steady contract on every exit path. Unlike the notification wait there is no
would-block quiet arm, so an elapse is fatal: the connection is evicted
(`connections_evicted` + `DiagEvent::PoolConnectionEvicted`) and a fresh one
minted, or the acquire budget classifies out. So `get()` as a whole is bounded,
not merely its permit wait.

**The 1-RTT-at-checkout tax is fundamental — do not re-attempt.** A pooled
checkout must confirm the peer is alive before the user's verb runs;
`pool_reset_session` is both the isolation reset and the liveness probe. Two
alternatives were refuted live and must not be relitigated: probing at check-in
(it confirms liveness only early in idle, so a later black-hole death escapes
onto the user's verb) and fusing the probe into the verb (sends the verb to an
unconfirmed connection — the two-generals ambiguity). A pooled op is therefore
~2 RTT vs a direct op's 1 RTT; measured pooled ≈ 2.3–2.4× the direct qps.
Competitors' faster pools reach the higher number by skipping the liveness check.

**`Pool::close(self)`** sends a protocol `Terminate` (`close_graceful`) to every
currently-idle connection, then closes each socket — replacing the bare RST/FIN
that makes PostgreSQL log "unexpected EOF on client connection" per connection.
Consumes `self` (use-after-close is a compile error, pinned by a `compile_fail`
doctest), best-effort, and bounded by `connect_timeout`. Idle connections are
moved out from under the lock before any I/O. Drains only connections idle at the
call. This is the intended home for `Terminate` — do not restore an unbounded
blocking `Terminate` to the hot `Drop` path.

**`max_lifetime` + `idle_timeout` — a lazy reaper.** Both on `PoolBuilder` as
`Option<Duration>`, default `None`. Each idle slot is `Idle { conn, created,
returned }`; `PooledConnection` carries `created` so `Drop` preserves the
original birth time (`max_lifetime` measures true age, not age-since-last-checkout).
At checkout, *before* the liveness reset, `is_stale(...)` reaps an
over-age/over-idle connection: bounded `close_graceful`, `PoolConnectionEvicted`
+ `connections_evicted`, then a fresh connection — routed through the same
eviction machinery as a failed reset so the two paths cannot drift. Reaping is
lazy at checkout by decision: a background timer task was rejected (runtime
dependency + lifecycle, and cannot spawn from a pool built outside a runtime) and
reap-on-return was rejected (`Drop` cannot `.await` a graceful close and cannot
catch a connection aging while idle). No background task exists. Zero-cost when
off: the `is_stale` call is short-circuited behind `max_lifetime.is_some() ||
idle_timeout.is_some()`, and `Drop` restamps `returned` only when
`idle_timeout.is_some()`. Note: `is_stale` is a **per-driver private copy** (two
byte-identical definitions, each with its own `reaper_tests`), not a shared
source — a hand-maintained twin worth collapsing.

## Parameters and statement caches

**Dynamic-param type fidelity (no silent coercion).** `query_params` /
`query_params_one` / `query_params_opt` / `execute_params` / `query_each_params`
declare `<P as ParamsWriter>::OIDS` in the `Parse` frame — the same OIDs the
compile-checked path bakes in. PostgreSQL then decodes each binary parameter as
the declared type and applies its own coercion rules; a genuinely incompatible
bind is a loud server error (`42883` / `22P02`). The retired `n_param_types = 0`
form silently matched `WHERE id = $1` bound with `&str "AAAA"` against
`id = 1094795585` — do not reintroduce it. An `EnumLabel`'s `unspecified` OID 0 is
still left to per-parameter server inference.

**Explicit prepared handles are stricter (client-side verify).** A prepared
plan's parameter types are fixed at `Parse`, so the server cannot coerce a
differently-typed `Bind` — a same-width wrong type would be a silent reinterpret.
`prepare` retains the server-inferred parameter OIDs (from
`ParameterDescription`) on the `PreparedStatement`, and `query_prepared` /
`execute_prepared` check the caller's `P::OIDS` against them *before* the `Bind`:
arity, then per-parameter strict equality (int8-into-int4 is rejected here,
unlike the dynamic path). Failures are `DriverError::ParamTypeMismatch { index,
expected, found }` / `ParamCountMismatch { expected, found }`, returned with no
wire I/O — the connection is untouched and neither is a disconnect. An OID 0 on
either side is unverifiable and passes through.

*Intended strictness:* binding a bare `&str` (OID `text` 25) where the server
wants a type with no implicit `text` cast — enum, uuid, date, timestamptz — is a
loud error (dynamic: server `42883`; prepared: client `ParamTypeMismatch`). Bind
the typed value (`EnumLabel<E>`, `bsql::Uuid`, a temporal type) or add an
explicit `$1::type` cast. A `bpchar`/`char(n)` column still blank-pads a shorter
bound `&str` — ordinary fixed-width CHAR semantics.

**Dynamic prepared-statement cache (`DynStmtCache`).** Per connection, invisible
(the verb still takes SQL text), keyed on the (SQL text, `P::OIDS`) pair — the
same SQL with a different parameter-type tuple is a distinct entry, so a reuse
never crosses parameter types. The first sighting runs the fused
one-round-trip path (`Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` +
`Sync` in one flush) and is noted PENDING, so a genuinely one-shot query is never
regressed to two round trips. The second sighting prepares a named statement (one
extra round trip, once); every later call is `Bind`+`Execute`+`Sync` with no
server re-parse. Bounded at `DYN_STMT_CACHE_CAP` = 32: a first sighting evicts
the oldest PENDING slot (which holds no server-side statement, so eviction is
free); a READY statement is never evicted, and if every slot is READY the
overflow SQL simply stays on the fused path. Self-healing: a `0A000` / `26000` on
the reuse path evicts + `Close`s the stale statement, emits
`DiagEvent::PreparedCacheSelfHeal`, and transparently re-runs the query fused.
`query_each_params` deliberately bypasses the cache (a streaming bulk read is
one-shot).

**The one rule: a statement cache lives within a single connection lease, never
across a pool checkout.** `Core::reset_session` (used by a direct consumer and by
the pool at checkout) drops both the dynamic and the compile-checked (typed)
cache. This is correctness, not hygiene: a prepared plan resolves its relation
names once, at `Parse`, so a plan a prior logical user promoted (an unqualified
`… FROM orders` bound to `public.orders`) surviving into the next user's checkout
lets that user's shadowing `CREATE TEMP TABLE orders` read the *prior user's*
rows on a cache hit — a silent cross-user wrong result, verified live for both
caches. The typed cache had the same hole and was formerly kept warm: neither the
result-schema OID guard nor PostgreSQL's `0A000` rescues it (both catch a
result-*type* divergence, while a matching-column temp shadow has the same result
type, and a typed hit sends no `Describe` so the guard never runs).
`DISCARD PLANS` does not fix it either — PostgreSQL re-validates the invalidated
plan at its next use, and any trailing reset statement re-resolves it back to
`public` (verified live). Dropping the statement is the only fix; the cost is the
cross-checkout plan-reuse micro-optimization. **Do not re-attempt keeping either
cache warm across a checkout.**

Server-side hygiene at zero extra round trips: `RESET`/`DISCARD` run no
`DEALLOCATE`, so the server-side statements survive the reset SQL. When the
dynamic cache is non-empty its statements are `Close`d in one batched round trip
and the typed cache's names are folded into the same batch
(`close_statements_bytes` takes raw name bytes; dynamic `_bsql_<n>` and typed
content-addressed `bsql_q_<24hex>` prefixes are disjoint). When the dynamic cache
is empty no batch is forced (preserving the typed path's zero-RTT checkout): the
typed client cache is cleared and its server-side statements are reclaimed lazily
by the next typed MISS's leading `Close`.

Full `reset_session` contract: the RESET/DISCARD simple query (`ROLLBACK`-prefixed
when the transaction status is not `Idle`), drop both statement caches, clear the
notification ledger (so a prior user's notifications never reach the next), reset
the N+1 recency window, reclaim an oversized send buffer, and restore the
client-liveness window to the connect baseline. A direct (non-pooled) connection
never resets on its own, so both its caches persist for the connection's life.

## Typed result-schema OID guard

Typed decode is positional / const-offset, so a runtime result column whose type
diverged from the migration schema (out-of-band `ALTER COLUMN TYPE`, a
`CREATE TEMP TABLE` shadow) would decode wrongly rather than error. Closed as
follows.

- A typed statement-cache **MISS** (a fresh `Parse`) appends a `Describe`(portal);
  the engine compares each runtime `RowDescription` OID against the carrier's
  `PreparedQuery::row_oids` in `apply_fused_row_stream` (cold, off the `DataRow`
  arm).
- Divergence → `DecodeError::ColumnOidMismatch { index, expected, found }`
  surfaced as `DriverError::Decode`. Not a disconnect: the result drains via the
  over-cap `DrainOvercapToRfq` path, so no row reaches a streaming verb's
  `on_row`, and the connection stays reusable.
- A mismatched MISS is **not** recorded in the cache, so a repeat is a fresh MISS
  that re-`Describe`s. A cache **HIT** sends no `Describe` (hot path unchanged)
  and cannot mis-decode: PostgreSQL refuses to change a reused plan's result type
  (`0A000`).
- Compatibility rule (`decode::result_oid_compatible`): accept if the runtime OID
  `>= oids::FIRST_NORMAL_OID` (16384 — every user-defined type is server-assigned
  above it; their runtime safety is `UnknownEnumLabel` /
  `CompositeArityMismatch`); otherwise compare `wire_decode_class`, which
  collapses `varchar`(1043)/`bpchar`(1042) → `text`(25) and
  `varchar[]`(1015)/`bpchar[]`(1014) → `text[]`(1009); otherwise exact equality.
- The array-element cross-check consults raw `wire_decode_class` and deliberately
  does **not** inherit the `>= 16384` skip — otherwise a hostile array frame
  declaring a user-type element OID would feed arbitrary bytes to a scalar
  decoder.
- Residual (accepted, same boundary as user types): a native column shadowed at a
  MISS by a user-defined type is not caught; a wire-compatible `varchar`↔`text`
  swap is accepted, being an identical decode.

**Stale-plan self-heal.** A typed HIT that hits `0A000` (plan result-type change)
or `26000` (statement vanished — out-of-band `DEALLOCATE`/`DISCARD ALL`, or a
pgbouncer transaction-pooling backend reassignment) re-runs **once** on the
forced MISS path via `Core::typed_plan_went_stale`, emitting
`DiagEvent::PreparedCacheSelfHeal`. Both SQLSTATEs are raised before any
`DataRow`, so no partial row reached the caller. Covers `execute::<Q>`, `query`,
`query_one`/`query_opt`, `query_each`. Exactly one retry (a MISS cannot itself be
stale). `Engine::query_params` / `query_params_break` take `args` by value and
hand it back, so the retry needs no `Clone` bound and the verb future stays
`Send` without requiring `Params: Sync`.

**Batch verbs are out of scope for the self-heal** — their param source is
consumed by the first attempt; they evict every referenced statement on a
mid-batch failure, so the caller-visible `BatchFailed` is retryable.

**Coverage.** The guard covers the single-statement typed verbs, every
heterogeneous `pipeline` command, and `query_batch`'s command 0. `execute_batch`
is not guarded and not vulnerable (it reads counts off the command tag and
discards RETURNING rows — no decode into `Q`, no `Describe`).

**Footprints.** `DecodeError` 12 B; `ActiveEngine` 432 B; `DriverError` 24 B;
`ConnectConfig` 152 B. The guard's `result_mismatch: Option<ResultOidMismatch>` is
12 B (`index: u16`, `found: NonZeroU32`, `expected: u32` — one source for the
checked triple, read by both the single-query and the pipeline settles) and
`pipeline_guard_oids: Vec<&'static [u32]>` is 24 B; both are cold and appended
after the hot fields, so the `DataRow` dispatch is unaffected (pinned by
`engine_hotpath_codegen`).

## Streaming, COPY, batch verbs

### Dynamic streaming — `query_each_raw` / `query_each_params`

Both PG drivers + their transaction guards; SQLite has identically named verbs.

- Each row is lent to `on_row` as a zero-copy `BorrowedRow<'r>` over the wire
  buffer (no `Arc`). Cell offsets are parsed once per row into a slot table
  reused across rows, so a stream allocates nothing per row.
- Reads are **positional only**: result column names arrive on the wire after
  every row, so by-name resolution is impossible on the streaming path (by-name
  lives on the eager `QueryResult::row` → `RowRef`).
- `on_row` returns `ControlFlow`. `Break(e)` rides back as `Ok(Some(e))` and the
  remaining rows are drained to a clean idle (O(remaining)) so a pooled
  connection stays reusable. A per-row decode failure or mid-stream server error
  is loud + drained, never swallowed.
- `query_each_raw` rides the simple-query wire; `query_each_params` rides the
  fused one-round-trip dynamic path and does not touch the statement cache.
- All three streaming verbs (typed `query_each` + these two) share
  `Core::finish_stream`, so they cannot drift in how they reclaim the connection.

### `copy_in` batching

`COPY_IN_FLUSH_THRESHOLD` = 64 KiB. Streamed `CopyData` accumulates in the send
buffer and flushes only when pending bytes reach it (~`total_bytes / 64 KiB`
write syscalls instead of one per row); a single chunk at or above the threshold
streams directly from the borrowed slice and is never copied. Because one
sub-threshold chunk is appended before the length check, the buffer stays
strictly under `2 ×` the threshold — constant memory regardless of COPY size.

### Typed binary COPY — `copy_in_typed::<Q>` + `copy!`

- `copy!(Name, "table", (cols))` validates target table, columns and types
  against the same build catalog `query!` reads, and emits an uninhabited
  `Name: TypedCopyIn` with a GAT `Row<'q>: ParamsWriter` (a `NOT NULL` column is
  `T`, a nullable one `Option<T>`; `text`/`bytea` borrow) and
  `const SQL = "COPY <table> (<cols>) FROM STDIN WITH (FORMAT binary)"` baked
  from validated catalog identifiers (the compile-time peer of the raw path's
  runtime `SafeTable`).
- `copy_in_typed::<Q>(rows)` streams each item as one PGCOPY binary row —
  `int16` field count then per field `{len, bytes}` or `-1` — byte-identical to a
  Bind parameter block, so it reuses the same `ParamsWriter` binary leaves as the
  `query!` param path (no format drift) and rides the 64 KiB batcher in constant
  memory.
- Injection-safe by construction: there is no text to mis-escape, so an embedded
  tab / newline / quote rides the binary field verbatim. A mid-stream server
  rejection is a classified `DriverError::Db` and the connection recovers.
- Caps: a carrier names at most 32 columns (`MAX_COPY_COLUMNS`); an unknown,
  duplicate, over-32 or array column is a `copy!` `compile_error!` (use raw
  `copy_in` for arrays). `ParamsWriter` tuple impls cover arity `0..=32`;
  `query!` accepts ≤ 32 `$N` params and ≤ 16 projected result columns.
- Raw `copy_in` / `copy_in_with` remain the escape hatch for pre-formatted or
  text COPY data.

### Heterogeneous pipelining — `conn.pipeline((Q0::bind(p), Q1::bind(p), …))`

- Both PG drivers + the `Transaction` guard; SQLite sequential twin. Returns the
  typed tuple `(Rows<Q0>, Rows<Q1>, …)`, each command decoded against its own
  carrier's compile-time OIDs. `Bound<Q>` comes from `BindExt::bind` — a bound
  carrier, not a runtime SQL fragment. `Pipeline` is sealed with hand-written
  tuple impls arity `1..=16`.
- **Atomic all-or-nothing**, forced by PostgreSQL: N extended-query commands with
  one trailing `Sync` are a single implicit transaction (a mid-batch error rolls
  back the commands before it, errors the failing one, skips the rest). Either
  every result, or `Err(DriverError::BatchFailed { index, source })` and zero
  results. Returning the pre-failure results as `Ok` is forbidden — those writes
  were rolled back. Structurally enforced: the only path that builds the `Ok`
  tuple is the settle's no-failure arm, driven by the *parked* failure state, not
  by the final boundary (which is `Idle` even after a mid-batch failure's
  recovery drain).
- **No auto-rollback:** a mid-batch failure inside an explicit transaction leaves
  it aborted (`'E'`) for its owner, exactly like any other failed verb. A later
  in-guard verb then gets a loud `25P02`, never a silent autocommit.
- A **commit-time** failure (a `DEFERRABLE INITIALLY DEFERRED` constraint) is
  `DriverError::Db` with `batch_failed_index()` == `None` — never an out-of-range
  `index == arity`.
- Engine: one dispatch state `PipelineAwaitingNextOrRfq`; per-command staging
  `[Close+Parse if MISS] + Bind + [Describe if guarded MISS] + Execute` with the
  `Sync` hoisted to the batch end, driven by the `Pipeline::stage_nth` cursor.
- **Windowed drive (constant send memory, deadlock-free).** Staging one command
  at a time, when `pending_send_len() >= BATCH_WINDOW_THRESHOLD` (64 KiB) the
  driver `Flush`es — which forces the window's responses out without ending the
  implicit transaction, only the trailing `Sync` does that — and drains the
  window before staging the next, so the client always reads before it
  write-blocks. A single command whose own `Bind` alone crosses the threshold on
  a non-empty prefix is isolated (`Core::isolate_prefix`: flush + drain the
  prefix alone, then re-stage the lifted bytes into a fresh window), closing the
  unbounded single-oversize-Bind class. A batch fitting one window sends no
  intermediate `Flush` and is ~1 round trip; a huge-param batch pays ~`N/window`.
- **Bounded residual (documented limit).** A window of multiple sub-threshold
  commands cumulatively up to ~2× the threshold, co-windowed with an early
  large-result command, can still deadlock only when the combined client-send +
  server-recv socket buffers are under ~128 KiB (never default-autotuned Linux or
  loopback). Closing it fully costs 1 RTT per command.
- **Per-command result-OID guard.** Per MISS command a `Describe`(portal) is
  appended (a HIT sends none). Command 0's expected OIDs are seated at staging;
  each subsequent MISS command's are popped front off `pipeline_guard_oids` at its
  leading `CloseComplete` (`'3'`) — a HIT leads with `BindComplete` (`'2'`) and
  never pushes, so pop count equals MISS count. A divergence is
  `DriverError::BatchColumnOidMismatch { command, source }`; the client drains to
  a clean idle and returns zero results; `is_disconnect()` is false.
- The guard **survives the windowed drive**: a mismatch parking in an intermediate
  window (a `Flush`, no `Sync`) has no RFQ to drain to, so
  `run_pipeline_break_guarded` bails with `Boundary::Failed` the moment it parks;
  the driver then stages the trailing `Sync`, drains to the recovering RFQ and
  returns the classified error. The bail is a const-generic parameter of
  `pump_active_to_boundary`, folded away for Sync-terminated callers;
  `execute_batch` keeps the unguarded `run_pipeline_break`.
- Honest post-detection state: a mismatch is a **client** rejection *after* the
  server processed — and, for an implicit-tx batch, committed — the transaction,
  so the batch's writes may have persisted. A mismatch in an intermediate window
  also stops staging later windows, so only the flushed windows committed. Both
  cases return zero results plus the classified drift; fail-loud beats decoding a
  drifted result.
- Cancellation (`57014`, connection recovers) and mid-batch transport death
  (classified disconnect, bounded) are honored.
- **SQLite twin** runs the commands sequentially inside the standard
  `transaction` guard's plain `BEGIN … COMMIT` — a *deferred* begin,
  deliberately not the migration runner's `BEGIN IMMEDIATE` — with the identical
  tuple and contract, no RTT win. Read-only under a conformance build.
- Measured on loopback (`pipeline_bench`, `--ignored`), K heterogeneous SELECTs
  fitting one window vs K serial `query_one`: K=2 ~3.5×, K=8 ~5×, K=16 ~7.3×.
  N=1 equals a fused single query.

### Homogeneous bulk write — `conn.execute_batch::<Q>(params_iter)`

- Both PG drivers + the `Transaction` guard; SQLite twin. One `query!` write
  carrier `Q` (`UPDATE` / `DELETE` / `INSERT … RETURNING`) against N runtime
  parameter sets, `Parse`d once and re-bound per set; returns `Vec<u64>`
  per-command affected counts. Params are the same lifetime-GAT `Q::Params<'p>`
  tuple.
- `Q` is a row-shaped carrier (the macro rejects a bare non-returning write); the
  count rides the `CommandComplete` tag and RETURNING rows are read-and-ignored,
  symmetric with `execute::<Q>`.
- Same atomic all-or-nothing contract and settle as `pipeline`: mid-batch →
  `BatchFailed { index }`; commit-time → `Db` with `batch_failed_index()` `None`;
  no auto-rollback.
- Abort preserves a deferred `BEGIN`: a first-window `FrameTooLong` routes
  through `abort_pipeline_staging`, and the pending prelude is *peeked*, not
  consumed, so the `BEGIN` survives and the connection stays healthy.
- `N == 0` does no wire I/O (`Ok(vec![])`); `N == 1` equals a single `execute`.
- Staging: command 0 via `stage_pipeline_command`, every subsequent set a bare
  `Bind`+`Execute` (`stage_execute_batch_command`); the receive multiplexer is
  reused verbatim (a bare `Bind` leads with the `BindComplete` the FSM already
  handles). The drive is **unguarded**.
- Same 64 KiB windowed batcher. Unlike COPY — where the server is silent while
  the client streams, so write-ahead cannot deadlock — an extended-protocol
  command emits a per-command response, so streaming N `Bind`s without reading
  would fill both the server's output and the client's send buffer.
- **SQLite twin** runs sequentially inside `transaction`, same `Vec<u64>` and
  contract. The typed batch is `execute_batch` on **both** backends; SQLite's raw
  multi-statement script executor is the disambiguated `execute_batch_raw`.
  Read-only under a conformance build — a typed *write* batch is PostgreSQL-only.

### Homogeneous bulk query — `conn.query_batch::<Q>(params_iter)`

- Same shape as `execute_batch` but **keeps** each command's typed rows: returns
  a grouped `Vec<Rows<Q>>`, one result per command, in order.
- It is the only verb returning N grouped typed results for a **runtime** N:
  `pipeline` is a fixed-arity compile-time tuple, `execute_batch` returns only
  counts, `copy_in_typed` is INSERT-only with no RETURNING.
- Return-shape decisions (do not relitigate): a flattened single `Rows<Q>` was
  rejected (it loses which rows came from which parameter set); `Vec<Vec<Q::Record>>`
  was rejected as redundant with `Rows<Q>`. Memory is O(total rows) by nature —
  the eager peer of `query`; a constant-memory streaming batch is a separate
  non-goal verb.
- Composition, adding **no** new engine method or dispatch state: command 0 via
  `stage_pipeline_command` (guard on); every subsequent command is
  `execute_batch`'s bare `Bind`+`Execute`; the window drive is the **guarded**
  batcher (command 0's mismatch can park in an intermediate window, so the bail
  is required); the sink routes each surface into its command's `RowsBuilder`;
  the settle is `pipeline`'s verbatim.
- **Guard verified once.** All N commands run the same `Q` `Parse`d once → one
  server-side plan → one result descriptor, so verifying command 0's runtime OIDs
  proves the schema for the whole batch; subsequent bare `Bind`+`Execute` reuse
  that verified descriptor. Divergence is `BatchColumnOidMismatch { command: 0 }`.
  A HIT command 0 sends no `Describe` and is `0A000`-safe.
- **SQLite twin** returns `Vec<TypedRows<Q>>` with the identical contract, and
  routes each command through the non-recording typed collect so a deliberate
  N-command batch is never flagged as N+1 under `n1-detect`.

## Cancellation, timeouts, liveness

### Cancellation

- `conn.cancel_token()` mints a detached `CancelToken` (`Send + Sync + 'static`,
  borrows nothing), obtainable before a long query and movable to another task —
  no `&mut` aliasing with the in-flight future. Pinned at 56 B (`CancelKey` 8 B +
  `Redial` 48 B).
- The secret is captured only at connect and kept in `Sensitive<i32>`. A PG
  cancel must travel on a second socket, so `cancel()` dials a throwaway
  connection from the credential-free `Redial` snapshot (host / port / raw
  `Option<SslMode>` / CA roots, no password), re-running the `SSLRequest` probe —
  a cancel to a TLS-required server negotiates TLS.
- Best-effort by PG spec §55.4: it *requests* cancellation. A honored cancel
  yields `57014` and leaves the connection drained + reusable
  (`is_disconnect()` false); a late cancel is a server no-op, a double cancel two
  harmless packets.
- SQLite twin `SqliteCancelToken` wraps `rusqlite::InterruptHandle`: same
  surface, but in-process, so `cancel()` returns `()` and an interrupted step is
  `SqliteError::Interrupted`.

### Server-side `statement_timeout`

- `ConnectConfig::with_statement_timeout(Duration)` inserts
  `("statement_timeout", <ms>)` into the existing startup-parameter map — no new
  field, so the 152 B pin is untouched. Chosen over a per-query `SET` (extra RTT,
  not session-wide).
- As a startup-packet GUC it applies before the first query and becomes the
  session reset value, so it survives the pool's `RESET ALL` on checkout.
- `Duration` → ms: `ZERO` → `"0"` (PG's "disabled"); non-zero sub-ms → `1` (never
  down to 0); whole ms above `i32::MAX` (~24.8 days) is clamped there. The map
  appends without dedup, so a repeat is last-wins.
- Caveat (not a bsql defect, no knob planned): under pgbouncer **transaction**
  pooling the client `StartupMessage` never reaches the per-transaction backend,
  so a connect-time `statement_timeout` / `search_path` / `timezone` does not
  persist. Qualify names, set the GUC per statement, or use session-level pooling.

### Client-side read-liveness window

- A live black-hole peer (kernel still ACKs, app forwards nothing) defeats TCP
  keepalive and black-holes the server's own `57014`, so `statement_timeout`
  alone bounds the server, not the client's read. The only safe client deadline is
  one derived from the server's budget.
- `ConnectConfig::client_liveness_window()` = `statement_timeout +
  connect_timeout` (saturating); `None` when the GUC is unset, `0`, or in a form
  the parser cannot model. Async arms it as `ReadBound::Within` (relative,
  re-armed per read; an absolute reset/notification deadline wins); sync as the
  steady `SO_RCVTIMEO` the socket rests at between verbs. An elapse is a
  classified `DriverError::Timeout` (`is_disconnect()` true).
- It never cuts a query the server allows, because the server would have aborted
  anything past `statement_timeout` first; per-read inactivity semantics keep a
  slow stream alive.
- Re-derived, never left stale: (1) the migration runner suppresses it for its
  whole run (async `ReadDeadline::suppress_scoped` RAII, sync save/restore);
  (2) every dynamic runtime-SQL verb — raw-text, `_params`, streaming — on the
  connection *and* inside a `transaction` guard routes its SQL through
  `window_after_statement` / `statement_timeout_effect`: a top-level
  `SET`/`RESET` re-derives (units `us`/`ms`/`s`/`min`/`h`/`d` parse), an ambiguous
  `SET` (`SET LOCAL`, `= DEFAULT`, unparseable) disarms, and text containing
  *both* `set_config` and `statement_timeout` disarms unconditionally (a
  `set_config` value cannot be pinned; requiring both tokens keeps a query that
  merely names the GUC as data from disarming); (3) `reset_session` restores the
  connect baseline.
- Residual (theoretical floor, not a gap): a `statement_timeout` change with no
  contiguous textual mention of the GUC name — a function body, an `EXECUTE` of a
  prepared plan, `'statement' || '_timeout'` — can leave the window stale-low.
  PostgreSQL does not report `statement_timeout` via `ParameterStatus`, so
  observing it would need a per-query round trip. Every observable form fails safe.
- Zero-cost when off: no `statement_timeout` → `None` window → the steady read is
  the historical unbounded one. Without a declared query budget no finite client
  deadline is safe.

## Migration runner — `conn.run_migrations(source)`

Applies a consumer's migration set to a live database on all three drivers
(async PG, sync PG, SQLite); adds no dependency and needs no `query!` catalog.
Peers: `migration_status` (a plain snapshot — it does **not** verify checksums)
and `dry_run_migrations` (the same drift check as a real run, applying nothing).
All three take `impl Into<MigrationSource>` (`directory(path)`, `embedded`, or a
`&[(&str, &str)]` slice).

The pure logic lives once in the zero-dependency `bsql-common` leaf: the
FNV-1a-64 checksum (stored as 16-char hex — dep-free, not `scram`-gated `sha2`),
the `/`-normalized name ordering + source loader + duplicate pre-flight, the
drift classifier `plan() -> Result<usize, Drift>`, and the data/error types
(`MigrationSource`, `AppliedMigration`, `MigrationReport`, `MigrationStatus`,
`MigrationSourceError`, `DriftKind`). One compiled source, so checksum, apply
order and drift semantics are identical on both backends. Each driver keeps its
own I/O half and its own `MigrationError` (PostgreSQL adds a `LockTimeout`
SQLite has no peer for), bridged by `From<Drift>` / `From<MigrationSourceError>`;
the ledger SQL text is legitimately per-backend.

- **Exactly once, in order.** Lexicographic by the `/`-normalized relative name —
  the same *string* key `bsql-build`'s `scan_sql_tree` sorts by, not the raw
  `PathBuf`, whose component-wise `Ord` disagrees with a byte compare at the
  `.`/`/` boundary (`[a/b.sql, a.sql]` vs `[a.sql, a/b.sql]`). Build-validated
  order equals apply order in every layout. A duplicate name (only reachable from
  a hand-built embedded slice) is a pre-flight
  `MigrationError::Source(DuplicateName)` on both backends, because the rejection
  lives in the shared loader.
- **Atomic per migration.** The DDL and its ledger row are one transaction
  (PostgreSQL DDL is transactional; SQLite too). A failure rolls back and the
  runner stops with `MigrationError::MigrationFailed` naming it; later migrations
  do not run.
- **Drift is loud.** `MigrationError::Drift` over `DriftKind::{ChecksumMismatch
  { recorded, current }, Reordered { applied_ordinal, source_name_at_ordinal },
  MissingFromSource { source_is_strict_prefix }}` — never silently re-run. The
  missing case is classified by position: a middle gap (`false`) is an
  unambiguous deletion ("restore it"), while a tail extra (`true` — the source is
  a strict prefix of the applied set, including an empty source) names both
  causes without asserting one, since a tail deletion and an older instance
  restarted against a newer database are identical data. The checksum is
  exact-bytes, so a CRLF (`autocrlf`) checkout re-checked against an LF-recorded
  ledger drifts spuriously — the safe direction, but pin line endings in
  `.gitattributes` (the repo has none today).
- **Concurrency.** PostgreSQL polls the non-blocking `pg_try_advisory_lock` with
  client-side backoff (`LOCK_POLL_INITIAL` 10 ms doubling to `LOCK_POLL_MAX` 1 s
  through the shared `next_backoff`; the acquire loop itself is per-driver —
  `tokio::time::sleep` vs `thread::sleep` — over shared constants and the shared
  lock/apply verbs). Deliberately **not** a blocking `pg_advisory_lock`: a
  blocked waiter's implicit-transaction vxid is exactly what a `CREATE INDEX
  CONCURRENTLY` in the lock holder waits on, a real 40P01. A holder past
  `LOCK_ACQUIRE_TIMEOUT` (60 s) is `MigrationError::LockTimeout`. The key is
  derived from the ledger name, so the lock is database-global, not
  schema-scoped: parallel `#[bsql::test]` schemas serialize on the one lock
  (correct, just serialized). SQLite serializes via `BEGIN IMMEDIATE` (a
  concurrent runner waits on `busy_timeout`) plus an in-transaction ledger
  re-check that skips a migration a concurrent runner already applied.

**Ledger.** `_bsql_migrations` (`LEDGER_TABLE`), unqualified so it follows the
connection's `search_path`. A fixed compile-time literal in each driver's
constant DDL — no identifier-injection surface — with ordinal, name and checksum
on Bind parameters, never spliced. Columns `ordinal, name (PK), checksum,
applied_at`, read `ORDER BY ordinal`; a not-yet-created ledger reads as empty for
status/dry-run.

**Non-transactional migrations.** A `-- bsql:no-transaction` comment line applies
the migration outside a transaction (`CREATE INDEX CONCURRENTLY`, `VACUUM`), with
a weaker guarantee: a crash between the DDL commit and the ledger insert leaves
it applied-but-unrecorded and a re-run re-applies it, so write it idempotently
(`CREATE INDEX CONCURRENTLY IF NOT EXISTS`). The same statement without the
marker runs inside the runner's `BEGIN` and PostgreSQL rejects it loudly —
atomicity is never silently weakened.

**Embed vs directory.** Either baked into the binary (`bsql::embed_migrations!()`,
generated by `bsql_build::emit_migrations(dir)` as `include_str!` pairs over the
`BSQL_EMBEDDED_MIGRATIONS` rustc-env channel — no filesystem at run time) or read
at run time from `MigrationSource::directory(path)`. The embed path parses each
file (bounded at depth 512; a form `sqlparser` cannot parse is a loud
`BuildError::Parse` — apply it via the directory source, which parses nothing)
and in that one AST pass reruns the destructive-acknowledgement gate plus the
transaction-control reject: a top-level
`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE SAVEPOINT` is
`BuildError::TransactionControlInMigration`, and a `-- bsql:no-transaction`
migration is **not** exempt. `enforce_no_transaction_control` is the one
authority for that reject, run on *both* build paths — the `emit_migrations`
embed (`parse_and_enforce_acks`) and the `query!` catalog replay (`replay_file`)
— so a runner-only and a `query!` consumer get the same refuse-before-apply
guarantee.

**Runtime boundary backstop.** The directory source parses nothing, and a
`COMMIT` inside a `DO`/procedure body is invisible to any parser, so after
applying each migration the runner verifies the native transaction status and
fails with `MigrationError::TransactionBoundaryBroken { migration }`: PostgreSQL
reads the RFQ tx-status (`Core::tx_status` — a transactional migration must leave
the runner's `BEGIN` open, a `-- bsql:no-transaction` one must be back at `Idle`),
SQLite reads `sqlite3_get_autocommit` (`Connection::is_autocommit`). Either way
it rolls back best-effort so the connection stays reusable. **Accepted
trade-off:** this is fail-loud *after* the boundary-breaking migration ran (its
statements already committed piecemeal), not refuse-before-apply — correct for a
path that trusts the operator and parses nothing, and for an in-procedure
`COMMIT` no parser can see. On PostgreSQL it is the only catch, since the
runner's own trailing `COMMIT` after a stray commit is a silent no-op warning;
the build-time gate above still gives refuse-before-apply for the embed and
catalog paths.

PostgreSQL emits `DiagEvent::MigrationLockWaiting` per poll and
`MigrationApplying` / `MigrationApplied` per migration (SQLite has no
diagnostics sink), and suppresses the client-liveness window for the whole run.

Test coverage is asymmetric: `crates/postgres/sync/tests/migrate_live.rs` carries
the full matrix (CONCURRENTLY, its fail-loud counterpart, advisory-lock
concurrency, the boundary backstop, duplicate names, progress events), while the
async twin carries only apply+idempotent, progress events and the backstop.
`crates/sqlite/driver/tests/migrate.rs` is the in-process superset.

## N+1 detection — `conn.n1_report()` (feature `n1-detect`, default off)

The tracker lives once in the zero-dep `bsql-common` behind its default-off `n1`;
each driver forwards and re-exports, so `N1Report` is **one** nominal type on
async PG, sync PG and SQLite (the umbrella holds a `const _`
fn-pointer-coercion proof that re-forking it turns the build red). The container
differs: PG returns `&[N1Report]`, SQLite `Vec<N1Report>`.

`N1Report { sql, file, line, count }`, site from `#[track_caller]`. Key =
`(sql-pointer, call-site-pointer)`, so two sites of one SQL never conflate;
`DEFAULT_THRESHOLD = 25` repeats within one logical operation produces a report.
The window is an inline `[WindowSlot; 16]` = 384 B, `const _`-pinned (slot 24 B:
`count == 0` is the vacant sentinel — no `Option` — plus a `u32` tick); a 17th
distinct key evicts LRU, so detection spans ~16 concurrent queries. Reset at
every logical-operation boundary — `commit`, `rollback`, `transaction` closure
exit, `reset_session` (pool checkout) — so repetition across operations is
forgiven and a per-row loop within one is caught.

Diagnostics-only: it never batches, blocks, errors or alters a result; a false
positive is at most a spurious report. Zero-cost off: the `Connection` field, the
query-path branch, the caller argument and `#[track_caller]` are all cfg-gated
out. Verb shape: the **five** typed verbs (`execute` / `query` / `query_one` /
`query_opt` / `query_each`, on `Connection` and on the `Transaction` guard) are
`fn(..) -> impl Future + 'a` in *both* feature states on the async driver — they
forward the `Core` verb's future directly with no wrapping `async` block, so
`clippy::manual_async_fn` never fires and no `#[allow]` is needed; `n1-detect`
adds only `#[track_caller]` + the `Location::caller()` argument. The bare RPIT
leaks the future's `Send`, pinned by the `_is_send` `const _`. Sync PG and SQLite
verbs are plain blocking `fn`, so `#[track_caller]` applies directly.

## Structured diagnostics

Dep-free observability seam (no `tracing`/`log`/`metrics` in the runtime graph).
`pub type DiagSink = Arc<dyn Fn(&DiagEvent<'_>) + Send + Sync>`; a `Diagnostics`
handle holds `Option<DiagSink>` + an optional slow-query threshold. Installed via
`Connection::connect_with(&config, &diag)` / `set_diagnostics`, or
`Pool::builder(cfg, max).on_diagnostic(..).slow_query_threshold(..)`. Not a
`ConnectConfig` field.

- **Zero-cost off:** an unset sink is one never-taken branch at each cold
  lifecycle boundary — no event built, no clock read, no alloc. Nothing fires
  from the `DataRow` hot arm (the deliberate distinction from the deleted per-row
  `Observer`).
- **No PII:** an event never carries a bound parameter value; `SlowQuery` carries
  only the SQL text.
- **Sink isolation:** every invocation goes through `diag::dispatch`, which
  (a) wraps the callback in `catch_unwind(AssertUnwindSafe(..))` and drops a
  caught panic, noting it to stderr once per process — a buggy sink can never
  strand a `Live` token or double-panic from a `Drop`; and (b) suppresses
  re-entry via a per-thread `IN_DISPATCH` flag cleared by an RAII guard, so a
  self-emitting sink fires once and cannot recurse. The sync pool drops its state
  lock before emitting, so a sink calling `pool.stats()` cannot deadlock.
  Consequence a consumer owns: a diagnostic emitted from inside a sink does not
  reach a sink.
- **`DiagEvent<'a>`** (the *enum* is `#[non_exhaustive]`; the variants are not):
  `ServerNotice { severity, code, message }` (a `RAISE NOTICE`/`WARNING`, from
  the shared `capture_notify` adapter via the fuzz-proven `error_response_fields`
  walk — steady-state query streams only; a pre-auth handshake notice is not
  surfaced), `SslDowngrade { host }`, `PoolAcquireTimeout { waited }`,
  `PoolConnectionEvicted`, `SlowQuery { sql, elapsed }`,
  `ParameterStatus { name, value }`, `MigrationLockWaiting { elapsed }`,
  `MigrationApplying { name }`, `MigrationApplied { name }`,
  `PreparedCacheSelfHeal { sql }`.
- **`SlowQuery`** is gated behind `Diagnostics::slow_query_armed()` (a threshold
  *and* a sink), so the off path reads no clock. A `SlowQueryGuard` fires on drop
  only for a verb that completed successfully (it also returns early if the
  thread is panicking). Covered: `simple_query`, `execute_raw`, `query_raw`,
  `query_params`, `execute_params`, the typed `execute`/`query`/`query_one`/`query_opt`,
  `execute_batch`, `query_batch`. Deliberately excluded: `query_each*` (a
  stream's duration is consumer iteration time), `pipeline`, `copy_in_typed`, and
  the low-level `query_prepared`/`execute_prepared`.
- **`Pool::stats() -> PoolStats`** (`#[non_exhaustive]`): `idle`, `max_size`, and
  the monotonic relaxed-atomic `acquire_timeouts`, `connections_evicted`,
  `waiters_high_water` (counts only truly blocked waiters — the async
  `try_acquire_owned` fast path leaves it `0` when uncontended).
- Follow-ups: a `Connected`/`CancelRedial` lifecycle event, cancel issue/no-op
  events, an optional default-off `tracing` adapter.

## TLS and authentication

### SSL mode + CA roots

- The default `SslMode` is **threat-scoped**, resolved at connect by
  `ConnectConfig::resolve_ssl_mode(&endpoint)`: a unix or loopback host
  (`localhost` case-insensitive, `127.0.0.0/8`, `::1`) → `Prefer`; every other
  host, including private ranges → `Require`. An explicit mode always wins. The
  classification is syntactic on the configured host (no DNS — slow and a TOCTOU
  hole). `SslMode` is deliberately not `Default`; the config stores
  `Option<SslMode>` (`None` = defaulted), niche-packed to 1 byte.
- A refusing server: `DriverError::SslRefused` when the mode was explicit, or a
  `DriverError::Config` naming the `Prefer`/`Disable` opt-out when bsql defaulted
  to `Require`.
- A `Prefer` downgrade emits `DiagEvent::SslDowngrade { host }` through the sink
  when one is installed, else keeps the stderr warning in debug *and* release.
  `Connection::is_encrypted()` lets a consumer reject a downgraded connection.
- Custom CA roots: `with_ca_roots(pem)` / DSN `sslrootcert=<path>` /
  `PGSSLROOTCERT`. They **replace** the default anchors (libpq parity), are
  stored raw and parsed into a rustls root store at connect; a bad or empty PEM
  is a fail-closed `DriverError::Config`, never a fallback to baked roots or
  plaintext.

### TLS session resumption

- rustls's resumption store lives inside the `ClientConfig`, so sharing the
  `Arc<ClientConfig>` makes reconnects resume an abbreviated handshake. Default
  roots share via the `tls::shared_client_config` `OnceLock`; custom CA via
  `tls::shared_client_config_with_ca_roots(pem)` — a bounded
  (`MAX_CACHED_CA_CONFIGS = 8`), `Mutex`-guarded, poison-recovering process-wide
  table both drivers resolve through. Past 8 distinct PEMs a fresh unshared
  config is handed out (no resumption, never an eviction of a live store).
- Keyed on the exact PEM bytes: necessary (rustls only shares a store between
  configs with the same verifier) and sufficient (a custom-CA config is a pure
  function of its roots; rustls additionally keys its store by server name).
- **Channel binding survives resumption:** rustls restores the original
  full-handshake peer certificate (TLS 1.2 session-id and TLS 1.3 PSK), so
  `Wire::peer_end_entity_cert` — and the SCRAM `-PLUS` hash — is unchanged. No
  disable-on-resume is needed.

### SCRAM-SHA-256-PLUS channel binding

- `core::resolve_channel_binding(encrypted, peer_cert_der, mode)` is the one
  authority both drivers thread through, like `resolve_endpoint` /
  `resolve_ssl_mode`.
- `tls_server_end_point(cert_der)` hashes the whole certificate DER; the hash is
  picked from the certificate's own `signatureAlgorithm` OID via a bounded, total
  DER walk: SHA-384/512 for the ECDSA/RSA variants naming them, SHA-256 for
  everything else — including the MD5/SHA-1 upgrade RFC 5929 §4.1 mandates and
  any unrecognised or unreadable OID, which fails safe into a loud SCRAM
  signature mismatch, never a silent downgrade. Direct `sha2`; no hand-rolled
  crypto, no X.509 trust parsing.
- `decide_sasl_choice(offer, binding)` picks: `SCRAM-SHA-256-PLUS`
  (`p=tls-server-end-point,,`) when offered over a bound channel; plain SCRAM
  with the RFC 5802 §6 anti-downgrade `y,,` over TLS without `-PLUS`; `n,,` when
  unbound; `ScramError::ChannelBindingRequired` under require;
  `NoSupportedMechanism` when the server offers neither. The cbind input is
  base64'd into the client-final `c=`, so a MITM's different cert breaks the
  proof.
- Policy: `ChannelBindingMode::{Disable, Prefer, Require}` on `ConnectConfig`
  (pinned 1 B), default `Prefer` (libpq parity — the threat-scoped `SslMode`
  default already encrypts remote endpoints, so strictness is opt-in and does not
  break legacy PG or poolers without `-PLUS`). Settable via the builder
  `channel_binding(..)`, the DSN `channel_binding=` key, or `PGCHANNELBINDING`;
  an unknown value is a classified error. `Require` over a plaintext channel is a
  fail-closed `DriverError::Config`. Entirely `scram`-gated.

### Server-driven password auth

- `pg_hba.conf` picks the mechanism and the client learns it only mid-handshake,
  so `core::build_password_credentials` builds a mechanism-agnostic
  `Credentials::Password(Box<PasswordAuth>)` carrying **both** forms — the raw
  bytes (MD5 / cleartext, never SASLprepped) and `SASLprep(password)` (SCRAM) —
  plus the resolved channel binding and the encrypted flag. The engine's
  `StartupPassword` state answers whichever `Authentication*` frame arrives.
- Two forms are load-bearing: SASLprep must **not** touch the MD5/cleartext
  bytes. A non-ASCII password whose prepped form differs (a non-breaking space
  folding to a plain space) would otherwise pass SCRAM and silently fail
  MD5/cleartext.
- Cleartext is answered **only** over an encrypted channel; a cleartext challenge
  over plaintext is `ConnFail::CleartextOverPlaintext` → `DriverError::Config`.
  An `AuthenticationOk` (server chose trust despite a password) proceeds like
  Trust (libpq parity).
- `PasswordAuth` is deliberately not `Drop` (the dispatch moves the chosen form
  out; each un-moved `Sensitive<Password>` scrubs via field drop-glue). The
  per-mechanism `Credentials::{ScramPassword, CleartextPassword, Md5Password}`
  primitives remain and share the same state transitions and message builders.
- MD5: `AuthSubCode::Md5Password` (sub-code 5) stays an unconditional wire
  classification; only the dispatch arms are `md5-auth`-gated, so an
  MD5-demanding server with the feature off gets
  `ConnFail::UnsupportedAuthMethod`, never a panic or silent failure.

### Feature gates (all default-on in core / async / sync / umbrella `bsql`)

`tls = [dep:rustls]`, `webpki-roots = ["tls", dep:webpki-roots]`, `scram`,
`md5-auth`. `bsql-postgres-proto`'s own default is `["scram", "md5-auth"]`; every
shipped dependent takes proto with `default-features = false` so a consumer can
drop them. `core`'s `scram` also pulls `dep:stringprep` (SASLprep).

Runtime crate counts, `cargo tree -p bsql-postgres-async -e normal,no-proc-macro`
(macOS aarch64, current `Cargo.lock`):

| build | crates | leaving |
|---|---|---|
| default | **40** | — |
| `tls` off | **33** | ring, rustls, rustls-webpki, rustls-pki-types, webpki-roots, untrusted, once_cell |
| `scram` off (tls on) | **29** | sha2, hmac, pbkdf2, base64ct, cpufeatures, stringprep, tinyvec, tinyvec_macros, unicode-bidi, unicode-normalization, unicode-properties |
| `tls` + `scram` off | **20** | both sets above |
| + `md5-auth` off | **13** | md-5, digest, block-buffer, generic-array, typenum, crypto-common, cfg-if |

`subtle` / `getrandom` are not SCRAM-exclusive (rustls/ring keep them); they drop
only when `tls` is also off.

- `tls` off is fail-loud **at connect**, as a runtime check in `build_tcp_wire`
  (not a compile gate — `SslMode` and CA roots arrive from DSN/env at runtime):
  `SslMode::Require`, or any custom CA, is a classified `DriverError::Config`.
  `Prefer` connects plaintext with the probe compiled out and `is_encrypted()`
  always false.
- No password mechanism at all (`scram` **and** `md5-auth` off) plus a configured
  password = `DriverError::Config` at connect naming the missing features. With
  `scram` off but `md5-auth` on a password still works for MD5 /
  cleartext-over-TLS; a SASL challenge then fails loud with
  `ConnFail::UnsupportedAuthMethod`. Trust auth always works.
- Footprints: `ConnFail` is `wire_pin!`-ed 8 B / align 4 with `scram`, 2 B /
  align 1 without. `HandshakeProgress` and `HandshakeOutcome` are pinned
  **unconditionally** at 24 B / align 8 in both feature states — their
  `ServerError(Box<[u8]>)` variant dominates `ConnFail`, so the shrink does not
  cascade.

### Per-connection memory (pool sizing)

- Plaintext: a fixed 4 KiB engine read buffer (`READ_BUF_CAP`, compile-pinned by
  a `const _: ()` in `frame.rs` tying `READ_BUF_CAP == 4096` to
  `MAX_FRAME_LEN_FIELD == 4095`).
- TLS adds, boxed in `Wire::Tls`: `STAGING_CAP = MAX_CIPHERTEXT_RECORD
  (5+16384+256) + RECV_CHUNK (16384)` ≈ 32 KiB allocated once, plus rustls's
  connection state and two transient vecs each bounded near one 16 KiB record.
  There is no resident encrypt scratch — each record is encrypted directly into
  `out_buf`'s reserved tail (`stage_into`, sized `chunk + TLS_RECORD_OVERHEAD`,
  then truncated). Roughly ~48 KiB TLS vs ~4 KiB plaintext; a 100-connection TLS
  pool ≈ 4.8 MiB.

## Safety invariants

- `#![forbid(unsafe_code)]` at the root of every workspace crate.
- **`unsafe` lives in exactly three places, none in shipped library source.**
  (1) `tools/devgates/src/lib.rs` — the `CountingAllocator` `GlobalAlloc` impl
  and the post-drop `probe_bytes`; `publish = false`. (2) `publish = false`
  `tools/` test code — the two `std::env::set_var` sites
  (`tools/query_fixture/tests/compile_fail.rs`,
  `tools/query_sqlite_fixture/tests/sqlite_gate.rs`; `set_var` is `unsafe` in
  edition 2024, called once serially before any trybuild child spawns, with a
  SAFETY comment) plus four `compile_fail/` trybuild fixture inputs. (3) Two
  **`bsql-postgres-proto` integration tests** (`tests/scram_zeroize_miri_spec.rs`,
  `tests/secret_bounded_str_spec.rs`) that call the audited
  `bsql_devgates::probe_bytes` under a file-level `#![allow(unsafe_code, reason =
  …)]` — a test target is a separate crate, so the lib root's `forbid` does not
  reach it, and proto *is* a published crate. Post-drop memory probing has no
  sound safe wrapper.
- `#![deny(clippy::unwrap_used, clippy::expect_used)]` on core, both PG drivers,
  the SQLite driver and `bsql-common`. `bsql-postgres-proto` uses the stricter
  `#![forbid(...)]` bundle (also panic / todo / unimplemented / unreachable /
  indexing_slicing / mem_forget / as_conversions / arithmetic_side_effects /
  float_arithmetic / integer_division).
- Static assertions (a `const _` block in each PG driver's `lib.rs`):
  `Connection: Send`; `Row: Send + Sync + 'static`; `Pool: Send + Sync`;
  `PooledConnection: Send + 'static`; `CancelToken: Send + Sync + 'static`.
- NULL = `Option<NonZeroU32>` (`ColSlot.len_plus_one`) — compiler-enforced, no
  sentinel.
- `PreparedStatement` is consumed by `close_statement(stmt)` — use-after-close is
  a compile error.
- **A PG `PreparedStatement` cannot run on a foreign connection.** It is a free
  owned `'static` handle naming a server-side `_bsql_<n>` whose plan lives only
  on the minting connection, and the name comes from a per-connection counter, so
  `_bsql_0` exists on every connection with a different plan — a cross-connection
  use would be a silent wrong result. Each `Core` mints a process-unique `u64`
  identity at connect and stamps it onto every statement it prepares;
  `check_stmt_origin` runs **first** in `query_prepared` / `execute_prepared` /
  `close_statement` — before `verify_params`, before any wire I/O — and a
  mismatch is `DriverError::WrongConnection` (fieldless, so the 24 B pin holds).
  The connection is untouched, so `is_disconnect()` is false. This is a *runtime*
  guard because a PG prepared verb needs `&mut conn` at call time, so the handle
  cannot hold the connection borrow the way SQLite's `SqliteStatement<'conn>`
  does; **do not re-propose** a generative lifetime brand on every
  `Connection`/verb signature — it was rejected as pervasive complexity. The
  internal `DynStmtCache` has no such hole (never handed out).
- Transactions via closure scope. Passwords are `Zeroizing<String>` — scrubbed on
  drop, redacted in `Debug`.
- SQL identifiers spliced into DDL/COPY go through `SafeIdent<'a>` /
  `SafeTable<'a>` — private tuple field, sole constructor `validate`
  (`SafeIdent`: one unquoted identifier; `SafeTable`: `table` or `schema.table`),
  so an unvalidated identifier cannot be spliced. Injection safety is structural,
  not a runtime escape pass a call site could forget.

## Platform support

- **64-bit required.** `#[cfg(not(target_pointer_width = "64"))] compile_error!`
  is present in six crates: `bsql`, `bsql-postgres-proto`, core, both PG drivers,
  and the SQLite driver. It tests pointer width only, so "Linux/macOS/Windows on
  x86_64 + aarch64" is a support statement, not an arch restriction.
- **TCP everywhere; unix sockets are unix-only.** The `UnixStream` arm is
  `#[cfg(unix)]`-gated in each driver's `transport.rs`. A unix-socket host on a
  non-unix target is `DriverError::Config(UNIX_SOCKET_UNSUPPORTED)` — one
  definition in `core::config`, consumed by both drivers — never a silent TCP
  fallback.
- **TLS cross-compilation needs a C toolchain.** The default-on `tls` feature
  pulls `ring`, which compiles C. Native builds are fine; cross-compiling needs
  the *target's* C cross-toolchain, which `rustup target add` does not install.
  `--no-default-features` is pure Rust — which is why the `cross_platform` gate
  scopes there.
- Measured on the `bench` branch (`benches/unix_vs_tcp.rs`): a unix socket is
  ~2.4–2.9× faster than loopback TCP on the by-PK round trip.

## House rules

- No `unwrap()` / `expect()` in production code; no fallbacks, no silent
  defaults, no `let _ = …`.
- **Never hand-roll expert-domain code — cryptography above all.** Use the
  audited crate (RustCrypto / rustls) directly, never a facade over a
  reimplementation.
- Stable Rust only; cross-platform; no `unsafe` in shipped library source; no
  platform-specific I/O hacks.
- Comments state constraints, never provenance — no references to plan items,
  slices, agents, tasks or review rounds in code or comments.
- Zero-cost: clone/copy/alloc to the minimum, cold paths included. Prefer
  deleting a mechanism over patching it. Breaking the API is never an objection.
- Measurements, not faith: deterministic gates (compile, clippy, alloc counts,
  `size_of`, asm) rather than timing under parallel load.
- Perf-relevant changes must keep `engine_hotpath_codegen` green; footprint
  changes must move a `footprint_pin!` / `wire_pin!` deliberately, never
  incidentally.

## Open items

Carried forward from the retired root `TODO` (never-implemented ideas, verified
absent from the code) and from this cleanup pass:

- **Query corpus + CLI.** A content-addressed query corpus and a `cargo bsql`
  subcommand with `migrate --check`. Never built; the stale `.bsql/` stanzas in
  `.gitignore` are its only residue.
- **Pool singleflight.** In-flight deduplication of identical concurrent queries
  at the pool layer. Never built.
- **Build-time PostgreSQL validation gate.** The SQLite compile-time conformance
  oracle shipped; the opt-in PostgreSQL peer (validate the catalog against a live
  server at build time) did not.
- **Consumer binary-size ceiling.** A minimal-consumer harness gating shipped
  binary bloat. Never built.
- **Read-window measurement.** `READ_BUF_CAP` is still 4096 and compile-pinned;
  whether 64 KiB−1 is better was never measured, only deferred.
- **`is_stale` is duplicated** per pool driver (two byte-identical private
  copies) — collapse into one source, like `bsql-common` did for the migration
  logic.
- **The live-test role `smir-ant` is hardcoded** in ~30 test files and in a doc
  example in shipped source (`crates/postgres/sync/src/pool.rs`). Parametrize or
  rename it to a neutral role.
- **Three live suites are covered by neither a documented command nor CI:**
  `bsql-syncbackend-fixture` (`live_pg.rs`), `bsql-examples`
  (`schema_per_test.rs`), and `bsql`'s `backend_error.rs`.
- **`publish.yml`'s header comment** still claims there is no test CI, and points
  at a CLAUDE.md section that does not exist.
- **`rust-version = "1.96"`** in the workspace manifest is aligned with the
  pinned 1.96.0 toolchain (`rust-toolchain.toml`).
- **Stale module doc in shipped code:** `crates/postgres/core/src/migrate.rs`
  still claims the run holds a session-level `pg_advisory_lock`, contradicting
  the non-blocking `pg_try_advisory_lock` poll the same file implements.
- **The async migration suite is much thinner than the sync one** (3 tests vs the
  full matrix) — bring it to parity or state the split deliberately.
- **`crates/postgres/async/README.md`** is stale on every axis (wrong crate name,
  a link to the nonexistent `bsql-pg-proto`, pre-rename `_sql` verbs) and cargo
  auto-publishes it to crates.io; `crates/postgres/async/tests/ping_live.rs` is a
  strict subset of `sq_live`'s `connect_and_ping`.
- **`.gitignore` lines 13–29** describe a `.bsql/` cache architecture that no
  longer exists (`#[bsql::sort]` was never shipped, `BSQL_DATABASE_URL` appears
  nowhere, no `.bsql/` directory is created or read).
