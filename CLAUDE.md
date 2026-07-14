# bsql — Multi-backend SQL toolkit in Rust

## Direction (2026 rebuild)

The theoretical-limit rebuild's core has landed: the compile-checked
`query!` flagship, the PostgreSQL async + sync drivers, and the embedded
SQLite backend all ship today, reachable through the single `bsql` umbrella
crate. This file and the code are the source of truth (the original design
blueprint and principles are historical and no longer tracked in-repo).
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
  `runtime_graph_pin`). Treat any historical CI prescription (nightly CI,
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
  bsql/              — umbrella facade + query! re-export + #[bsql::test] harness (bsql::pg, ::pg_sync, ::sqlite) + the ONE cross-backend bsql::N1Report  — 1650 LoC
  postgres/
    proto/           — sans-IO wire protocol + session engine (no_std + alloc) + PGCOPY binary framing + TypedCopyIn  — 29678 LoC
    core/            — transport-generic driver engine Core<S> + materializer + types + config + TLS + Rows + notify ledger + SafeIdent guard + cancel key/redial + copy_in_typed + dynamic prepared-statement cache + migration RUNNER (PG I/O; pure logic in bsql-common) + N+1 re-export  — 13928 LoC
    async/           — tokio async driver (plugs its socket into the shared Core<S>) + CancelToken + migration-runner try-lock poll  — 2651 LoC
    sync/            — std::net blocking driver (plugs its socket into the shared Core<S>) + CancelToken + migration-runner try-lock poll  — 2558 LoC
  sqlite/
    driver/          — embedded SQLite driver (bundled rusqlite) + typed query! runtime + explicit prepared-statement handles + interrupt CancelToken + migration RUNNER (SQLite I/O; pure logic in bsql-common) + N+1 re-export  — 4059 LoC
  common/            — ZERO-DEP leaf: migration PURE logic (checksum/ordering/drift authority + source loader) + N+1 detector (feature `n1`) — ONE compiled source both PG core + SQLite depend on (was two hand-maintained copies)  — 1065 LoC
  testkit/           — deterministic in-memory fake PostgreSQL for driver tests (no network)  — 1005 LoC
  build/             — BUILD-DEP: migration DDL → schema catalog (+ SQLite template) + shared $N→?N placeholder authority + migration embed (emit_migrations)  — 36279 LoC
  query-macros/      — PROC-MACRO: query! + copy! (types/validates against the catalog; emits the PostgreSQL + SQLite typed bridges) + #[bsql::test] (schema-per-test wrapper)  — 2507 LoC
```

(src LoC measured per crate via `find <crate>/src -name '*.rs' -exec cat {} + | wc -l` — counts inline `#[cfg(test)]` modules, so `build/`'s total is dominated by `src/infer.rs` (29563 lines: the schema/type-inference engine plus a ~13K-line inline `#[cfg(test)]` test module). Publishable package names: `bsql`, `bsql-postgres-{proto,core,async,sync}`, `bsql-sqlite`, `bsql-common`, `bsql-testkit`, `bsql-build`, `bsql-query-macros`. Non-shipped `publish = false` tools under `tools/`: `bsql-devgates`, `bsql-query-fixture`, `bsql-query-bridge-fixture`, `bsql-query-sqlite-fixture`, `bsql-test-harness-fixture`, `bsql-corpus`.)

## Build & test

```bash
cargo check --workspace              # full build
cargo clippy --workspace --all-targets   # lint wall — must be 0 warnings
cargo test --workspace               # unit + integration (non-ignored)
cargo test --workspace --doc         # doctests
cargo test -p bsql-devgates --test deps_pin            # dependency-frontier gate
cargo test -p bsql-devgates --test runtime_graph_pin   # build-time-only boundary gate
cargo test -p bsql-devgates --test doc_links           # intra-doc-link wall (broken-link deny)
cargo test -p bsql-devgates --test cross_platform      # Windows/Linux cross-target regression gate (cargo check --no-default-features; NO-OP-PASS when the target isn't `rustup target add`-ed)
cargo test -p bsql-postgres-proto --test engine_hotpath_codegen  # next_event codegen-stability gate (panic-free + instruction ceiling)
cargo test -p bsql-postgres-core --test decoder_fuzz   # decoder total-function gate (dep-free fuzz: no decoder panics on any input)
cargo test -p bsql-postgres-core --test tls_resumption # TLS session-resumption gate (hermetic, no PG): a SHARED ClientConfig resumes an abbreviated handshake AND the resumed session exposes the ORIGINAL peer cert (so the SCRAM tls-server-end-point channel-binding hash is unchanged); + a negative control that SEPARATE configs never resume
cargo test -p bsql-testkit --test wide_overcap_stress  # wide over-cap teardown-under-load gate (offline testkit: a too-wide RowDescription up to i16::MAX drains + recovers the connection every iteration of a 40× loop, both drivers)
cargo test -p bsql-sqlite            # SQLite (no PG needed)
cargo test -p bsql-postgres-async --test sq_live -- --ignored    # async PG (needs local PG)
cargo test -p bsql-postgres-sync --test sync_live -- --ignored   # sync PG (needs local PG)
cargo test -p bsql-postgres-async --test sq_live cancel_token_stops -- --ignored   # async cancel witness (needs PG)
cargo test -p bsql-postgres-sync  --test sync_live cancel_token_stops -- --ignored # sync cancel witness (needs PG)
cargo test -p bsql-postgres-async --test pool_liveness -- --ignored   # async pool dead-peer liveness (get() bounded, not a hang; PG behind a black-hole relay)
cargo test -p bsql-postgres-sync  --test pool_liveness -- --ignored   # sync  pool dead-peer liveness (get() bounded, not a hang; PG behind a black-hole relay)
cargo test -p bsql-postgres-async --test direct_liveness -- --ignored   # async DIRECT/in-flight query liveness (a black-holed query with a statement_timeout is a bounded classified Timeout, not a hang; mid-query FIN classified in ms; PG behind a black-hole relay)
cargo test -p bsql-postgres-sync  --test direct_liveness -- --ignored   # sync  DIRECT/in-flight query liveness (same bound over the blocking driver; PG behind a black-hole relay)
cargo test -p bsql-postgres-async --test tls_fragmentation -- --ignored  # TLS byte-fragmentation reassembly gate (the owner's-burn regression net): a self-contained ephemeral SSL PG behind a 1/3-byte fragmenting relay; handshake + multi-record result + 300 KB record-spanning value + stream reassemble byte-exact, is_encrypted, zero panics/hangs. Skips cleanly if no initdb/openssl or run as root
cargo test -p bsql-postgres-async --test channel_binding_plus -- --ignored  # live SCRAM-SHA-256-PLUS witness: a self-contained ephemeral SSL + SCRAM PG (initdb + openssl self-signed cert + password_encryption=scram-sha-256 + a hostssl scram login role) it starts itself; channel_binding=Require/Prefer over TLS AUTHENTICATE (real PG only accepts the tls-server-end-point cert hash a correct -PLUS proof carries), and channel_binding=Require over plaintext FAILS CLOSED (DriverError::Config). Skips cleanly if no initdb/openssl/psql or run as root
cargo test -p bsql-postgres-async --test midstream_faults -- --ignored  # async mid-stream fault matrix (server error / cancel-57014 / transport-death FIN mid-result / pg_terminate_backend / dropped future): each is a classified Err in bounded time, connection recovers OR evicts as NotReady, never a torn "success" and never a hang; no leak over a repeat loop
cargo test -p bsql-postgres-sync  --test midstream_faults -- --ignored  # sync mid-stream fault matrix (same, minus the dropped-future class the blocking driver has no peer for)
cargo test -p bsql-postgres-async --test streaming_scale -- --ignored  # live constant-memory scale gate: query_each_sql streams 5M rows with RSS O(1) in row count (coarse ps -o rss= delta vs a 10k stream, generous 48 MiB margin) + exact count/Gauss-sum/order — the live peer of the offline engine_query_break_alloc gate
cargo test -p bsql-sqlite --test cancel              # SQLite interrupt witness (in-process, no PG)
cargo test -p bsql-query-sqlite-fixture --features n1-detect --test n1_detect_sqlite  # SQLite N+1 witness (in-process)
cargo test -p bsql-query-fixture --test query_live_async -- --ignored  # live query! (async, needs PG)
cargo test -p bsql-query-fixture --test query_live_sync  -- --ignored  # live query! (sync, needs PG)
cargo test -p bsql-query-fixture --test query_oid_guard_live -- --ignored  # live typed RESULT-schema OID guard (both drivers): a TEMP shadow of a DIFFERENT type is a classified ColumnOidMismatch on every typed verb (never a silent "AAAA"); a matching-typed shadow + varchar/bpchar columns decode correctly (no false positive). Per-connection TEMP shadows over the 0020_oidguard.sql table, so parallel-safe (run WITHOUT --test-threads=1)
cargo test -p bsql-query-fixture --test pipeline_oid_guard_live -- --ignored  # live PER-COMMAND result-schema OID guard on the PIPELINE (both drivers): a drifted pipeline command is a classified DriverError::BatchColumnOidMismatch naming the command (batch_failed_index()), never a silent value, connection recovers; a matching shadow + varchar/bpchar + a user-type (domain) column decode correctly (no false positive). Per-connection TEMP shadows / per-test schema, so parallel-safe (run WITHOUT --test-threads=1)
cargo test -p bsql-sqlite --test migrate                # migration runner (in-process, no PG)
cargo test -p bsql-postgres-sync  --test migrate_live -- --ignored  # migration runner (sync PG, incl. concurrency + CONCURRENTLY)
cargo test -p bsql-postgres-async --test migrate_live -- --ignored  # migration runner (async PG try-lock poll)
cargo test -p bsql-query-fixture  --test embed_migrations_live      # embed baked-set shape (offline)
cargo test -p bsql-query-fixture  --test embed_migrations_live -- --ignored  # embedded set applies live (needs PG)
cargo test -p bsql-query-fixture --test copy_typed_offline             # copy! macro expansion + row shape (offline)
cargo test -p bsql-query-fixture --test copy_typed_live_async -- --ignored  # live copy_in_typed (async, needs PG)
cargo test -p bsql-query-fixture --test copy_typed_live_sync  -- --ignored  # live copy_in_typed (sync, needs PG)
cargo test -p bsql-postgres-proto --test engine_copy_typed_alloc       # typed binary-COPY constant-memory gate
cargo test -p bsql-postgres-proto --test engine_query_break_alloc      # dynamic streaming constant-memory gate (alloc count row-count-independent)
cargo test -p bsql-postgres-proto --test engine_execute_batch_alloc    # execute_batch constant-SEND-memory gate (staged-bytes high-water independent of N: identical for a small-N and a 100×-larger windowed batch)
cargo test -p bsql-query-sqlite-fixture --test execute_batch_sqlite    # SQLite execute_batch_typed twin (in-process, read-only-scoped)
cargo test -p bsql-query-fixture --test execute_batch_live_async -- --ignored  # live execute_batch (async: N counts + all-or-nothing bulk-write rollback + commit-time-None + no-auto-rollback + 20k-command windowed deadlock-free run; needs PG)
cargo test -p bsql-query-fixture --test execute_batch_live_sync  -- --ignored  # live execute_batch (sync twin; needs PG)
cargo test -p bsql-postgres-async --test sq_live query_each -- --ignored    # dynamic streaming witnesses (async, needs PG)
cargo test -p bsql-postgres-sync  --test sync_live query_each -- --ignored  # dynamic streaming witnesses (sync, needs PG)
cargo clippy -p bsql --features test-harness --all-targets              # lint the (non-default) #[bsql::test] harness
cargo test  -p bsql --features test-harness --lib                       # harness unit tests (offline)
BSQL_TEST_DSN=postgres://USER@localhost/postgres \
  cargo test -p bsql-test-harness-fixture -- --ignored                  # live #[bsql::test] isolation witness (needs PG)
cargo clippy --workspace --features n1-detect --all-targets             # lint the (non-default) N+1 detector reshape
cargo test  -p bsql-common                                             # migration pure-logic offline unit tests (checksum / order / drift)
cargo test  -p bsql-common --features n1                                # + N1Tracker offline unit tests (the ONE shared source)
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
column is a `compile_error!`. Passing the RECORD type (`conn.query::<Name>()`)
where the runnable CARRIER (`NameQuery`) is required — the single most common
`query!` mistake — is a `TypedQuery` `#[diagnostic::on_unimplemented]`: the error
reads `` `Name` is not a runnable `query!` carrier `` and names the fix (use the
`…Query` carrier; the bare record holds a decoded row and is not runnable),
the PostgreSQL peer of the SQLite driver's `SqliteTypedQuery` diagnostic. The macro's expansion names only
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
value; the row-tuple marker (whose type SOURCES the row OID list) rides the
native pivot, so the compile-time OID guarantee is untouched — a wrong row type
is still a compile error, and the surviving param-wire OID pin (the pre-baked
`Parse` template cross-checked against the parameter tuple's `ParamsWriter::OIDS`)
stays `E0080`. `tools/query_bridge_fixture` is the end-to-end proof.

**User-defined types generated from the migration DDL — `bsql::user_types!()`
(feature `macros`).** A consumer who declares a PostgreSQL type in a migration
gets a generated Rust type for it with ZERO derives and no hand-maintained type
name — a capability no other library offers, because only bsql parses the
migration set at build time. A `CREATE TYPE mood AS ENUM ('happy', 'sad')`
migration, plus `bsql::user_types!()` invoked ONCE (in a module in scope at the
`query!` call sites), generates `pub enum Mood { Happy, Sad }` (variants in
declared order — PostgreSQL's enum sort order, so the derived `Ord` matches the
server). A `query!` selecting a `mood` column decodes into `Mood`; a variant
renamed or deleted in a later migration regenerates the type and any code that
named the old variant stops compiling — DRIFT IS A BUILD ERROR. Enum EVOLUTION
reaches the catalog too: `ALTER TYPE … ADD VALUE [BEFORE|AFTER]` /
`RENAME VALUE … TO …` / `RENAME TO …` are FULLY replayed (label-set mutation in
place, preserving declared order — which the derived `Ord` mirrors — and a type
re-key), so the generated enum always matches the migration FILES; a silent skip
of an `ALTER TYPE` would leave the enum missing an added label (a runtime
`UnknownEnumLabel`) or mapping a variant to a label the live server rejects, so
it is never a no-op. On the wire a PG
enum is its LABEL TEXT (`enum_send`/`recv`), so decode rides the `text` pivot
(positional, no runtime OID check — as for every column) plus a label→variant
reshape (`PgEnum::from_wire_label`); an unknown label (a value ALTERed into the
live enum out-of-band, absent from the migration) is a classified
`DecodeError::UnknownEnumLabel`, never a panic. An enum PARAMETER binds through
`EnumLabel<E>` (from `Mood::Happy.as_label()`) as an `unspecified`-typed (OID 0)
label the server infers from context — a PG enum has NO implicit `text` cast, so
a `text` (25) parameter is rejected; the phantom `E` makes `EnumLabel<Mood>` ≠
`EnumLabel<Status>`, so a query expecting one enum rejects another's label at
compile time. A `CREATE DOMAIN age AS int CHECK (VALUE >= 0)` is TRANSPARENT: a
domain column types EXACTLY as its base (`age` -> `i32`, following a
domain-over-domain / domain-over-enum chain), and the `CHECK` is SERVER-enforced
(a violation is a classified server error, never a client check) — so a domain
emits no generated type. **Guarantee boundary (honest):** the compile-time layer
pins the type NAME + variant SET from the migration catalog; there is NO
compile-time OID pin (a user type's OID is server-assigned/dynamic) and —
deliberately — NO connect-time OID resolution (PG's unknown-parameter inference
+ positional label decode make it unnecessary), so a live-DB label the migration
did not declare surfaces as a LOUD classified error, never silent corruption.
This is exactly the existing "catalog matches migration FILES" boundary, and the
runtime label check is strictly STRONGER than a native column's (which has no
runtime OID check at all). The user types ride their own build channel
(`BSQL_USER_TYPES`), so the schema catalog format and its goldens are untouched;
`RustType::UserEnum(UserEnumId)` carries a `Copy` index into the catalog. A
`CREATE TYPE addr AS (street text, zip int4)` COMPOSITE generates a Rust `struct
Addr { street: Option<String>, zip: Option<i32> }` — one `Option<T>` field per
attribute, because a composite attribute is ALWAYS nullable on the wire
(PostgreSQL forbids `NOT NULL` on a `CREATE TYPE ... AS` attribute, and the
row-type binary frame carries a per-field length that may be `-1`). A `query!`
selecting an `addr` column DECODES it by walking the row-type binary frame — an
`int32` field count, then per field a `{uint32 type_oid, int32 len (-1 = NULL),
byte[len] value}` triple (the exact `record_send` form) — via the
`CompositeReader` cursor, RECURSING into each field's own existing decoder (a
native `Cell<BinaryFmt>` scalar/array, a NESTED composite `PgComposite::decode_row`,
or an ENUM label reshape), never a second copy of the scalar decoders. The
generated struct is OWNED and `'static` (its `text`/`bytea` fields copy), so it is
a valid record field in both record twins; it derives `Debug`/`Clone`/`PartialEq`
(deliberately not `Eq`/`Ord`/`Hash` — a composite may carry a float field, so a
record with a composite column is `PartialEq` but not `Eq`). **Guarantee boundary
(same as the enum's).** The composite's OID — and every field's wire OID — is
server-assigned/DYNAMIC (a domain or enum field carries its own dynamic OID), so
there is NO static field-OID pin: the wire field OID is READ and IGNORED, and the
decode is validated by field POSITION + ARITY (a field count differing from the
migration's is a classified `DecodeError::CompositeArityMismatch`, e.g. a field
ADDed/DROPped on the LIVE type out-of-band) + each field's own decode succeeding
(a malformed frame is `DecodeError::CompositeTruncated`). A composite `struct`'s
field SET is the migration's, so a renamed/dropped/retyped attribute (an
`ALTER TYPE ... {DROP|RENAME|ALTER} ATTRIBUTE`) breaks the build at the field use
site — the exact peer of the enum variant-set drift guarantee. Composite decode
resolves a field of another user type (an enum, a domain, a NESTED composite)
through the SAME `resolve_field_type` chain a column uses; a composite `$N`
PARAMETER (the row-type binary ENCODE) is a follow-up (decode is the high-value
half) — a loud, located rejection today, not a half-correct encode. It stages as
a WHOLE (not the encodable subset) precisely because an all-native composite's
field OIDs are stable but a composite with an enum / domain / nested-composite
field needs server-dynamic OIDs — the composite's own type OID (the `$N` param
OID) AND each field's OID inside the `record_recv` frame — and bsql does NO
connect-time OID resolution, so shipping only the all-native subset would be a
non-universal partial. Composite
ATTRIBUTE-level `ALTER TYPE` (`ADD`/`DROP`/`ALTER`/`RENAME ATTRIBUTE`) is a LOUD
`sqlparser` parse error (the pinned `sqlparser` grammar models only enum
`ALTER TYPE` ops), i.e. drift is a build error, never a silently-stale struct;
`ALTER TYPE ... RENAME TO` re-keys the composite via the generic path. A
composite column is PostgreSQL-only (its row-type frame has no SQLite storage
class), so `sqlite_conn.query::<Q>()` over it is a located compile error. The
`0014_moods.sql` / `0015_domains.sql` / `0016_alter_type_evolve.sql` /
`0017_composites.sql` migrations in `tools/query_fixture` and its
`query_enum_live` / `query_domain_live` / `query_alter_type_live` /
`query_composite_live` / `query_enum_offline` / `query_composite_offline` tests
are the end-to-end proof (decode both twins + nullable + actual NULL; a param
round-trip; an unknown label classified; a renamed-variant / removed-composite-field
compile-error golden; the domain base decode + server-enforced CHECK; ADD VALUE /
RENAME VALUE / RENAME TO evolution; and the composite decode of a plain / NESTED /
enum-bearing composite over both drivers + the classified arity drift).

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
still correct). The SAME detector is a CROSS-BACKEND feature: the tracker
(`N1Tracker` / `N1Report` / the recency window + its 384-byte footprint pin)
lives ONCE in the dependency-free `bsql-common` leaf crate, behind its default-off
`n1` feature. Each driver's `n1-detect` forwards to `bsql-common/n1` and
RE-EXPORTS the type, so `conn.n1_report()` returns the SAME
`bsql_common::N1Report` on EVERY backend — the async PG driver, the sync PG
driver, AND SQLite — and `bsql::N1Report` is that ONE canonical type (a consumer
can write a single `fn(&bsql::N1Report)` over both backends; the umbrella carries
a compile-time `const _` fn-pointer-coercion proof that the type is single, so a
regression re-forking it turns the build red). This REPLACES the former
hand-maintained COPY in each driver — the copies had already drifted (the SQLite
copy dropped `window_evicts_lru_beyond_capacity` and abbreviated two tests); the
single source heals that. The embedded SQLite crate keeps its
zero-`bsql-postgres-core` boundary precisely because `bsql-common` has NO
dependencies at all (it drags in no PG / rustls tree). SQLite's `n1-detect` path
is SIMPLER than the async PG driver's RPIT reshape — its verbs are plain blocking
`fn`, so `#[track_caller]` works directly (no future reshape) — but both now share
the ONE `bsql-common` tracker. Witnessed by `tools/query_sqlite_fixture`'s
`n1-detect` feature + `tests/n1_detect_sqlite.rs`, the `bsql-common` offline unit
tests, and the umbrella's single-type proof.

**Migration runner — `conn.run_migrations(source)`.** The one product gap
between build-time schema validation and a live database: a runtime capability
on all three drivers (async PG, sync PG, SQLite) that APPLIES a consumer's
migration set. Always available (adds NO dependency — the checksum is dep-free
FNV-1a-64, not `scram`-gated `sha2`). The runner's transport-agnostic PURE logic
— the FNV-1a-64 checksum, the `/`-normalized name ORDERING authority + source
loader + duplicate-name pre-flight, the drift classification (`plan()`), and the
plain data / error types (`MigrationSource` / `AppliedMigration` /
`MigrationReport` / `MigrationStatus` / `MigrationSourceError` / `DriftKind`) —
lives ONCE in the dependency-free `bsql-common` leaf crate, so the checksum,
apply order, and drift semantics are a COMPILER FACT identical on both backends,
not a test-pinned convention (this REPLACES the former hand-maintained COPY in
each driver + its known-answer cross-pin vector). The per-backend I/O RUNNER
stays in each driver: the PostgreSQL half lives ONCE over the transport-generic
`Core<S>` (so async/sync are a compiler guarantee) with its non-blocking
advisory-lock poll and its own `MigrationError` (carrying a `LockTimeout` variant
SQLite has no peer for); the SQLite half is the `BEGIN IMMEDIATE` +
in-transaction re-check twin, over the SAME shared `MigrationSource` / ledger /
`run_migrations` / `migration_status` / `dry_run_migrations` verbs. Both bridge to
the shared classifier through `bsql_common::migrate::plan() -> Result<usize,
Drift>` plus a per-backend `From<Drift>` / `From<MigrationSourceError>`; the ledger
SQL text (`timestamptz`/`now()`/`$N` vs `TEXT`/`datetime('now')`/`?`) legitimately
differs and stays per-driver. **Correctness:**
(a) EXACTLY ONCE, in the SAME lexicographic-by-name order the build-time catalog
replay uses — ONE genuine ordering authority: the build's `scan_sql_tree` sorts
by the SAME `/`-normalized relative-name STRING key the runner sorts by (NOT the
raw `PathBuf`, whose component-wise `Ord` disagrees with a byte-wise name compare
at the `.`/`/` boundary for nested prefix collisions — `[a/b.sql, a.sql]` vs
`[a.sql, a/b.sql]`), so build-validated order == apply order in every layout. A
duplicate migration name (only reachable from a hand-built embedded slice — a
directory walk yields unique paths) is a loud pre-flight
`MigrationError::Source(DuplicateName)` on BOTH backends BEFORE any apply, never a
silent skip of the second (SQLite would otherwise skip it via its ledger
re-check; PG would fail on the ledger PK — now both fail loud identically);
(b) each migration's DDL + its `_bsql_migrations` ledger row are ONE transaction
(PostgreSQL DDL is transactional; SQLite too) — a migration that fails ROLLS BACK
and the runner STOPS with a classified `MigrationError::MigrationFailed` naming
it, later migrations untouched; (c) checksum DRIFT (an edited applied migration),
a reorder / insert-before, and a deleted-from-source applied migration are each a
classified `MigrationError::Drift` — never silently re-run or ignored. The
checksum is EXACT-BYTES, so a git `autocrlf` checkout (CRLF line endings)
re-checked against a ledger recorded from an LF apply spuriously drifts (the SAFE
direction — a false drift error, never a silent mis-apply — but pin line endings
in `.gitattributes` to avoid the surprise); (d)
CONCURRENCY: two instances booting together SERIALIZE. PostgreSQL uses a
NON-BLOCKING `pg_try_advisory_lock` POLL with client-side backoff (the sleep is
the one inherently per-driver piece — `tokio::time::sleep` async, `thread::sleep`
sync — sharing one `next_backoff` policy); this is DELIBERATE over a blocking
`pg_advisory_lock`, which deadlocks against a `CREATE INDEX CONCURRENTLY`
migration in the lock-holder (the blocked waiter's implicit-transaction vxid is
exactly what the concurrent index build waits on — a real 40P01 the parallel live
witness surfaced). SQLite serializes via `BEGIN IMMEDIATE` (write lock up front,
so a concurrent runner's `BEGIN IMMEDIATE` waits on `busy_timeout`) plus an
in-transaction ledger RE-CHECK that skips a migration a concurrent runner already
applied. A stuck holder past `LOCK_ACQUIRE_TIMEOUT` (60 s) is the classified
`MigrationError::LockTimeout`. The advisory lock is DB-GLOBAL (keyed on the
ledger name), NOT schema-scoped — so parallel `#[bsql::test]` schemas each
running migrations serialize on the one lock (correctness-preserving, just
serialized; migrations are a rare boot-time op). **Non-transactional
migrations:** a `-- bsql:no-transaction` comment line makes the runner apply a
migration OUTSIDE a transaction (for `CREATE INDEX CONCURRENTLY` etc.), with a
documented WEAKER guarantee (a crash between the DDL commit and the ledger insert
leaves it applied-but-unrecorded → a re-run RE-APPLIES it, so write such a
migration idempotently: `CREATE INDEX CONCURRENTLY IF NOT EXISTS`); the SAME
statement WITHOUT the marker is wrapped in `BEGIN`, so PostgreSQL rejects it
LOUDLY — the runner never silently breaks atomicity. **Embed vs directory:** the
set is either baked into the binary (`bsql::embed_migrations!()`, generated by
`bsql_build::emit_migrations(dir)` via `include_str!` + the
`BSQL_EMBEDDED_MIGRATIONS` rustc-env channel — no filesystem at run time) or read
from a runtime `MigrationSource::directory(path)` (the ops-friendly case). The
EMBED path re-runs, per file in the SAME sqlparser AST pass, the S42
destructive-acknowledgement gate (an unacknowledged `DROP TABLE` cannot ship
baked into a binary — it fails the build there too, `reuse, never bypass`) AND a
transaction-control reject (a top-level `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`
is a loud `BuildError::TransactionControlInMigration` — the runner owns the
transaction boundary, so an embedded `COMMIT` that would break atomicity is a
BUILD error; a `-- bsql:no-transaction` migration is NOT exempt, it runs as
autocommit statements). The runtime directory source parses nothing, so both
build-time gates are authorship guarantees honestly scoped to the embed. The
`_bsql_migrations` identifier is a fixed compile-time literal (no injection
surface); the migration NAME + checksum ride Bind PARAMETERS, never spliced. Runtime emission and the runner are ORTHOGONAL to `query!` — a
runner-only consumer needs no catalog. Witnessed by
`crates/sqlite/driver/tests/migrate.rs` (in-process: order, idempotent re-run,
drift, fail-stop, status/dry-run, directory source, no-transaction, two
concurrent runners over one FILE), `crates/postgres/{sync,async}/tests/migrate_live.rs`
(`--ignored`, per-test isolated schemas: the same set PLUS `CREATE INDEX
CONCURRENTLY` via the marker AND its fail-loud counterpart AND the advisory-lock
concurrency, all green in PARALLEL — the deadlock repro), and
`tools/query_fixture`'s `runner_migrations/` + `tests/embed_migrations_live.rs`
(the build.rs `emit_migrations` embed chain, including an acked-destructive
migration that applies live).

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

The `cross_platform` gate (`tools/devgates/tests/cross_platform.rs`) is the
NO-CI regression wall for the Windows/Linux cross-target fix (the `#[cfg(unix)]`
gating of the unix-domain-socket transport arm in each driver's `transport.rs`
plus the 64-bit `compile_error!`). Without CI, an unconditional
`use std::os::unix::…` would silently re-break Windows and no routine check on
the dev's macOS/Linux host would notice. This gate runs `cargo check -p
<driver> --target <triple> --no-default-features` for the two shipped drivers
(`bsql-postgres-async`, `bsql-postgres-sync`) against `x86_64-pc-windows-msvc`
and `x86_64-unknown-linux-gnu`, in a dedicated `CARGO_TARGET_DIR`
(`target/devgate-cross`, no-contention like `doc_links`). An ungated
`use std::os::unix::…` fails the Windows check with `E0433` (the module does not
exist there) — the exact regression the gate catches (proven RED->GREEN by
temporarily adding `use std::os::unix::net::UnixListener;` to a driver's
`transport.rs`). Two deliberate scoping choices: (1) `cargo check`, not `build`
— `check` emits only metadata and NEVER LINKS, so it needs no target linker (no
MSVC `link.exe` on macOS), only the target's prebuilt `std` from `rustup target
add`; (2) `--no-default-features` — the default `tls` feature pulls `ring`,
whose C compile needs a C CROSS-toolchain a bare target add does not provide, so
the gate scopes to the pure-Rust surface (the transport gating is on the TARGET,
not a feature, so `--no-default-features` still compiles `transport.rs`); TLS-on
cross-compilation needs the target's C toolchain and is documented, not gated.
NO-OP-PASS when absent: it probes `rustup target list --installed` and SKIPS
(passes, with an `eprintln!` note) any target not installed — or the whole gate
if `rustup` is unavailable — so a developer without the Windows target added
never gets a false red, mirroring how the `--ignored` live suites skip without a
database. It FAILS only when a target IS installed and its check fails (a real,
reproducible regression). It is `publish = false` with no dependencies, so
`deps_pin` and `runtime_graph_pin` are untouched.

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
- **Core** (`bsql-postgres-core`): the shared TRANSPORT-GENERIC driver engine `Core<S: Transport>` — it holds the sans-IO engine over a `Wire<S>` plus the linear liveness token and defines every non-I/O verb ONCE — alongside the result materializer, the dynamic `Row` / `QueryResult` types, `ConnectConfig`, TLS config, and the typed `Rows` container (built from an internal `#[doc(hidden)]` `RowsBuilder` prebuffer). Both drivers build on it.
- **Drivers** are thin I/O adapters that plug their socket into the ONE `Core<S>`: `Core<TokioSocket>` on async, `Core<SyncSocket>` on sync (each socket is a TCP-or-unix enum), MONOMORPHISED per driver — static dispatch, no `dyn`. The verbs live once in `Core<S>`; the drivers differ only in `.await` vs blocking, so async/sync parity is a COMPILER guarantee, not hand-maintained twins.
- **Rows.** The dynamic `Row` (from `query_sql` etc.) is 16 bytes (`'static + Clone + Send + Sync`) over an `Arc`-shared arena: 3 heap allocations per whole `QueryResult` (the arena's `data` + `slots` vectors + the shared `Arc`), regardless of row count, and 0 per row — a `QueryResult` holds ONE lazy `RowSet` and mints each `Row` handle on demand (`.get(i)` / `.iter()`), never eagerly building a `Vec<Row>`, so a single-row read (`query_one_sql`) clones the `Arc` once, not N times. This mirrors the typed `Rows<Q>` (from the `query!` flagship), which is 2 allocations per result and 0 per row (borrowed, zero-copy decode). The **SQLite** backend runs the SAME lazy model: its `QueryResult` is one lazy `RowSet` over a shared arena (a `data` byte pool + a `CellSlot` table carrying integer/real inline and text/blob as `(offset, len)`, with the column names shared by `Arc`), so an eager `query_sql()` costs a constant number of allocations, 0 per row (a minted 16-byte `Row` handle carries its own names, so `get_by_name` threads no slice), and text UTF-8 is validated lazily at `get::<&str>` (never eagerly failing the whole result on one bad byte). A `> 4 GiB` eager result is the loud `SqliteError::ResultTooLarge` (stream it via `query_each_sql`, which is capless/constant-memory). The SQLite **typed** flagship result `TypedRows<Q>` wraps that same lazy arena: a constant number of allocations, 0 per row, borrowed records aliasing the arena zero-copy via an `ArenaRowRef` per-get view.
- **SQLite parity.** The SQLite driver is a full peer of the PG path, not a text-only wrapper. **Verb naming matches PG:** `query` / `query_one` / `query_opt` / `query_each` are the compile-checked TYPED flagship (over a `query!` carrier); the dynamic raw-SQL verbs carry the `_sql` suffix (`query_sql` / `query_one_sql` / `query_opt_sql` / `query_each_sql`), and the parameterized dynamic verbs keep their names (`query_params`, `query_params_one/opt`, `query_each_params`). **Typed flagship:** a `query!` carrier for a SQLite-decodable query (every column a SQLite storage class, unbridged, no PG-only dynamic sugar) implements `SqliteTypedQuery` (emitted by the macro under the umbrella `sqlite` feature), so `Connection::query::<Q>()` and its peers (+ the transaction guard) execute it and decode into the SAME typed records the PG path produces. SQLite is dynamically typed, so decoding VERIFIES each value's actual storage class against the record's declared field type via `FromColumn` — a mismatch is a classified `TypeMismatch`, a `NULL` in a non-`Option` field is `UnexpectedNull`, never a silent coercion (the runtime peer of the PG wire-OID pinning). The borrowed record `Q::Record<'q>` aliases the shared arena zero-copy through an `ArenaRowRef` per-get VIEW that lends cells for the CONTAINER lifetime (the memory-proven per-get view, scoped to SQLite); one macro-emitted `decode_row` serves BOTH the eager (`TypedRows<Q>`) and the streaming (`query_each`) paths via the `ColumnSource` seam that `ArenaRowRef` and `BorrowedRow` both implement. A carrier for a PG-only query does NOT implement `SqliteTypedQuery`, so `sqlite_conn.query::<That>()` is a LOCATED E0277 (`#[diagnostic::on_unimplemented]`), never a silent mis-run. The macro emits the SQLite bridge ONLY under `sqlite-runtime` (the umbrella `sqlite` feature), so a PG-only build's expansion is byte-identical and the PG codegen/alloc gates and trybuild goldens are untouched. Runtime emission (`sqlite`) and build-time conformance (`macros-sqlite`) are ORTHOGONAL — a SQLite-targeting consumer should enable BOTH: runtime-only is still fail-loud (a storage-class mismatch is a classified runtime error) but lacks the build-time proof that real SQLite resolves the inferred row shape. Typed `query_one`/`query_opt` are exactly-one/at-most-one (`SqliteError::TooManyRows` on 2+, one extra `sqlite3_step`, no materialization) — the SAME contract as the PG typed verbs, so the flagship reads identically on both backends; the dynamic `*_sql` verbs stay first-row. **Parameters** come in two vocabularies, matching PG. The TYPED flagship verbs take the compile-checked `Q::Params<'p>` tuple — a LIFETIME-GAT (a `text`/`bytea` param is `&'p str`/`&'p [u8]`), so a RUNTIME `String`/buffer binds on the typed path (no `'static` wall; the `'static` instantiation feeds only the const validator/OID pins) — the SAME tuple type the PG `TypedQuery::Params<'p>` uses (the macro emits both from one source), bound onto the statement by the SQLite twin of `ParamsWriter`: the sealed `SqliteBindParams` (tuple) over `SqliteBindValue` (per-leaf → `ValueRef`), which binds each element positionally via rusqlite's zero-alloc `raw_bind_parameter` in its true storage class (`&str`→`TEXT`, `&[u8]`→`BLOB`, `None`→`NULL`). So a `query!` binds the SAME typed parameters on both backends, and a parameter type SQLite cannot bind (a `u64`, or a PG-only `Uuid`/`Numeric`/temporal/`EnumLabel`) is a LOCATED compile error at the `query::<Q>` call (the `SqliteBindParams` bound lives on the verb, not the associated type, so a PG-only-param carrier still gets its `SqliteTypedQuery` impl — only running it on SQLite is refused). The DYNAMIC verbs keep the untyped `&[ValueRef]` slice (the ONE value vocabulary for read AND dynamic bind) as the escape hatch — every value in its TRUE storage class, so `NULL`/`BLOB` are bindable and integers escape the affinity trap. The PG `ParamsWriter` binary-encode path is byte-untouched (the SQLite twin is a SEPARATE trait). **Explicit prepared-statement handles** close the last SQLite parity cell (a real surface gap — the PG driver has `prepare`/`query_prepared`, SQLite had none). `conn.prepare_sql(sql)` returns a DYNAMIC `SqliteStatement<'conn>`, `conn.prepare::<Q>()` a TYPED `SqliteTypedStatement<'conn, Q>` — each holds one PLAIN (non-persistent) `rusqlite::Statement<'conn>` borrowing the connection, so the CONSUMER holds it on the stack beside the connection and calls its verbs (`query`/`query_one`/`query_opt`/`query_each`/`execute` dynamic; `query`/`query_one`/`query_opt`/`query_each` typed) repeatedly. The SQL is compiled ONCE and reused every call, so a hot loop pays NO per-call `sqlite3_prepare_v2` recompile — the fast reuse shape a hand-rolled FFI layer achieves, with NO `unsafe` and NO self-referential hidden cache (the borrow checker keeps the connection alive for the handle, tier-1). This is the THIRD path the transparent verb-level cache cannot take: rusqlite's `prepare_cached` forces `SQLITE_PREPARE_PERSISTENT` (which bypasses lookaside and slows multi-row stepping), and a hidden cache of plain `Statement`s would be self-referential — an explicit handle is neither. The typed handle keeps every guarantee (storage-class verification, exactly-one/at-most-one, the `?N`↔tuple arity guard checked ONCE at prepare); a statement prepared on the connection runs correctly INSIDE a `transaction` closure (same db handle) and honors a `cancel_token` interrupt. Under `n1-detect` the TYPED handle's read verbs record (a typed read repeated 25+ times from one call site is the anti-pattern regardless of pre-preparation); the DYNAMIC handle does not (deliberate reuse, matching the dynamic connection verbs and the PG `query_prepared`). Measured: the reused handle hits the plain-prepare pilot ideal — `by_pk_prepared` ~1.7 µs, `10row_prepared` ~5.2 µs, both BELOW the C reference (2.28 / 5.66 µs), where the per-call-prepare streaming verbs sit at ~4.7 / ~7.4 µs. `Connection::transaction` hands the closure a borrowing `Transaction` guard exposing only the data verbs, so a nested/manual-`commit` desync is E0599 (same design as the PG transaction guard). `open` sets a 5 s default `busy_timeout` (a contended write waits, bounded, then classifies as busy — never a hang; `set_busy_timeout(Duration::ZERO)` restores immediate fail-loud). Affected counts are `u64` (PG parity). The `$N`→`?N` SQLite placeholder rewrite has ONE authority (`bsql_build::sqlite_placeholder_form`), shared by the build-time conformance oracle AND the macro's baked SQLite `const SQL`, so the runtime string is byte-identical to the one build-time validation proved SQLite prepares. **Cross-backend capabilities:** `conn.cancel_token()` (interrupt-based `SqliteCancelToken`) and `conn.n1_report()` (feature `n1-detect`) read the SAME as the PostgreSQL drivers, so cancellation and N+1 detection are one mental model across backends (see the Conventions cancellation bullet and the N+1 paragraph).

## Platform support

- **64-bit Linux, macOS, and Windows** (`x86_64` + `aarch64`). A 64-bit target
  is REQUIRED: the footprint pins assert exact `size_of`/`align_of` for 64-bit
  pointers, so `#[cfg(not(target_pointer_width = "64"))] compile_error!` fails a
  non-64-bit build (i686 / wasm32 / 32-bit ARM) loudly in `bsql`, core, and both
  drivers — never a silently-wrong layout.
- **TCP everywhere; unix-domain-socket transport is unix-only.** The unix-socket
  arm (`std::os::unix::net::UnixStream`) is gated behind `#[cfg(unix)]` in each
  driver's `transport.rs` (`std::os::unix` does not exist on Windows). A
  unix-socket host requested on a non-unix target is the classified
  `DriverError::Config` message `UNIX_SOCKET_UNSUPPORTED` (defined once in
  `core::config`), never a silent TCP fallback or a panic. So Windows uses TCP; a
  unix-socket deployment is Linux / macOS.
- **TLS (`ring`) on Windows / cross needs a C toolchain.** The default-on `tls`
  feature pulls `ring`, which compiles C. A NATIVE build on each platform (with
  its own C compiler) builds it normally; CROSS-compiling it (e.g. a Windows
  target from a macOS host) needs the TARGET's C cross-toolchain, which a bare
  `rustup target add <triple>` does NOT install. A `default-features = false`
  (TLS-off) build is pure Rust and cross-compiles with only the target's prebuilt
  `std`. This is why the `cross_platform` gate scopes to `--no-default-features`.

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
- Error types: `DriverError::Db(DbError)` for server errors with SQLSTATE, `DriverError::Config`/`DriverError::ConfigDynamic` for pre-connect validation, `DriverError::NoRows` for empty results. Pre-connect config validation is ONE classified family: `Config(&'static str)` carries a fixed message; `ConfigDynamic(Box<str>)` carries a runtime-computed one that names its offending value (an `invalid port: 99999` DSN/env parse failure). `ConnectConfig::from_dsn` / `from_env` therefore return `Result<Self, DriverError>` (NOT a bare `String` — a consumer can `match` the classified error, or use `DriverError::is_config()` which is `true` for either carrier). `ConfigDynamic`'s `Box<str>` is 16 B, the same width as the enum's existing widest payload, so `DriverError` stays pinned at 24 B.
- **Reconnect vs. retry — `DriverError::is_disconnect()`** (both PG drivers; a
  cross-backend peer `SqliteError::is_disconnect()` / `BackendError::is_disconnect()`).
  Draws the EXACT line a resilient consumer needs — "the connection DIED
  mid-operation, RECONNECT" vs. "the server rejected my query but the connection
  is fine, fix the query" — by construction, never a string-match heuristic. It is
  a PREDICATE over the existing classified variant set (no new variant): `true`
  for `Io` (a dropped socket / EOF / reset — the transport is dead), `NotReady`
  (the linear liveness token was already taken by a prior fatal error), `Timeout`
  (a FATAL mid-command read deadline — the pool's dead-peer liveness bound, a
  silently vanished peer), and `Db` whose SQLSTATE is connection-broken (the whole
  `08` connection-exception class, or the whole `57P` operator-intervention
  termination/refusal subclass — `57P01`/`57P02` admin/crash shutdown, `57P03`
  cannot-connect, `57P04` database-dropped, `57P05` idle-session-timeout, matched
  by class prefix like `08`, via the new `DbError::is_connection_error()`
  predicate). It is `false`
  for every per-query error the connection survives — CRUCIALLY `57014`
  `query_canceled` (a `statement_timeout` abort or a `CancelToken` cancel leaves
  the connection drained + reusable, so a cancel is never a disconnect), a syntax
  error, a constraint violation, `NoRows`, `Config`, `PoolTimeout`, a decode /
  column error. The `DriverError` match is EXHAUSTIVE (no wildcard), so a future
  variant forces a classification decision. Cross-backend honesty: SQLite is
  IN-PROCESS (never network-disconnects), so its `is_disconnect()` maps to the
  broken-HANDLE/FILE codes (`SQLITE_IOERR` / `SQLITE_CORRUPT` / `SQLITE_CANTOPEN`
  / `SQLITE_NOTADB`, whose recovery is a fresh handle), and is `false` for a
  `SQLITE_BUSY` retry, an interrupt, and every constraint/type error — so a
  generic consumer's reconnect/reopen logic is ONE decision on both backends.
  Witnessed live on both PG drivers (`is_disconnect_true_on_terminated_backend...`:
  a `pg_terminate_backend` mid-flight classifies `true`, a syntax error `false`,
  the connection surviving its own syntax error) + offline unit tests for every
  variant's classification (PG core + SQLite) + the cross-backend
  `BackendError::is_disconnect` witness.
- `ConnectConfig` is `#[non_exhaustive]` — construct via `new()` + builder methods
  (`footprint_pin!`-ed at 152 bytes)
- **Pool liveness — `get()` is BOUNDED even on a dead peer.** The pool
  health-gates every REUSED connection with a `reset_session` on checkout (the
  exactly-once liveness proof before the user's verb runs). That reset is a
  round-trip, so a pooled connection whose peer VANISHED SILENTLY (a half-open
  socket — a NAT idle-drop, a cable pull, an AZ partition — where no FIN/RST
  arrives) must not block the checkout for the kernel's `tcp_retries2` budget
  (~15 min). The reset therefore arms a liveness deadline = the connection's own
  `connect_timeout` (a reset is a mini handshake, so it earns the handshake's
  budget — no separate knob, so the `ConnectConfig` 152-byte footprint is
  untouched): the async driver arms the same `ReadDeadline` `recv_notification`
  uses (proven token-safe), the sync driver arms the socket read+write timeout
  `connect` uses. UNLIKE the notification wait, the reset's pump has no
  would-block QUIET arm, so an elapsed deadline is a FATAL transport error, not a
  quiet-alive one — the token drops, the reset returns classified, and the pool's
  existing eviction arm drops the dead connection and hands out a FRESH one (or a
  classified acquire-timeout if the whole budget is spent). So `get()` as a WHOLE
  is bounded (its permit/slot wait AND its post-acquire reset), not merely its
  permit wait. The happy path is UNCHANGED: a healthy reset completes in
  microseconds far inside the budget, so the deadline never fires — only an
  arm/disarm bracket is added, never a round trip. Witnessed by the `--ignored`
  `pool_liveness` suites (both drivers: a black-hole TCP relay freezes a pooled
  connection mid-stream, then `get()` recovers a fresh connection / returns a
  classified error, both bounded — never the `tcp_retries2` hang).
- **Pool graceful shutdown — `Pool::close(self)`** (both drivers). Dropping a pool
  drops each pooled connection's socket BARE — an RST/FIN with no protocol
  `Terminate` — and PostgreSQL logs an "unexpected EOF on client connection" per
  connection (an error-log flood at shutdown for a large pool). `close` instead
  sends a protocol `Terminate` (`Connection::close_graceful`, a `pub(crate)` verb)
  to every currently-IDLE connection so the server sees a CLEAN disconnect, then
  closes each socket. It CONSUMES `self` (use-after-close is a compile error) and
  is BEST-EFFORT: a per-connection failure is swallowed and the drain continues.
  BOUNDED on a dead peer — the `Terminate` write is bounded by the connection's
  own `connect_timeout` (no new `ConnectConfig` knob): the async driver wraps the
  whole close in a `tokio::time::timeout` (SAFE here — unlike `reset_session` /
  `recv_notification`, there is no live token to strand, since `close` consumes it
  and the connection is discarded next), the sync driver arms the same
  `SO_RCVTIMEO`/`SO_SNDTIMEO` ceiling `reset_session` arms (no restore — the
  connection is dropped). This is the INTENTIONAL, opt-in, batched home for a
  `Terminate` (a prior slice deliberately deleted the unbounded blocking
  `Terminate` from the hot `Drop` path — `close` is the correct place for it).
  Drains only connections idle at the call; a checked-out connection returns to the
  detached idle set on its own `Drop`. Witnessed by the `--ignored` `pool_liveness`
  suites (`pool_close_gracefully_terminates_idle_backends` — the backends EXIT,
  seen via `pg_stat_activity`; `pool_close_is_bounded_when_a_pooled_peer_is_black_holed`
  — bounded against the black-hole relay) plus a `compile_fail` doctest on
  `Pool::close` pinning use-after-close.
- **TCP keepalive is ON by default** (both drivers, TCP-only). Every TCP
  connection enables `SO_KEEPALIVE` (idle 60 s, interval 10 s) right after connect
  via `socket2`'s SAFE borrowed-fd API (`SockRef::from(&stream).set_tcp_keepalive`
  — the `unsafe` fd handling lives inside `socket2`, so the driver crates stay
  `#![forbid(unsafe_code)]`), so a silently-vanished peer on an IDLE connection is
  eventually detected by the kernel. This matches libpq (keepalives on by default)
  and complements the checkout-time reset bound above: the reset catches a dead
  peer at checkout, keepalive lets the kernel notice one while the connection just
  sits idle. NO config knob — a dead-peer probe is near-free on a healthy
  connection and near-universally wanted, so it is not a `ConnectConfig` field
  (the 152 B footprint is untouched). A unix socket has no keepalive concept, so
  it is gated on the transport enum (TCP arm only). `socket2` is ALREADY in the
  resolved graph (via tokio's `net` feature), so `deps_pin` is unchanged.
  Constants: `KEEPALIVE_IDLE` / `KEEPALIVE_INTERVAL` in each driver's
  `connection.rs`. Witnessed offline by each driver's `keepalive_tests`
  (`set_tcp_keepalive_enables_so_keepalive`: a fresh socket has it off, the helper
  turns it on — read back via `socket2`, no PG).
- **Pool `max_lifetime` + `idle_timeout` — a LAZY reaper** (both drivers, on the
  same `PoolBuilder`: `.max_lifetime(Option<Duration>)` / `.idle_timeout(Option<Duration>)`,
  default `None` = disabled, no behaviour change). Each idle slot is stamped with
  its connection's `created` (established) and `returned` (last check-in) `Instant`
  (a private `Idle { conn, created, returned }` in each pool; `PooledConnection`
  carries `created` so its `Drop` preserves the ORIGINAL birth time — `max_lifetime`
  measures TRUE age, not age-since-last-checkout). At CHECKOUT, BEFORE the liveness
  reset, a shared `is_stale(created, returned, now, max_lifetime, idle_timeout)`
  gate reaps a connection that outlived `max_lifetime` (age) or `idle_timeout`
  (idle): it is gracefully closed via C2's bounded `close_graceful`, emits
  `DiagEvent::PoolConnectionEvicted` (+ the `connections_evicted` counter), and the
  loop mints a FRESH one instead (async retains the permit + `continue`s; sync
  `release_slot()`s then loops — routing through the EXISTING eviction machinery so
  the stale + failed-reset paths cannot drift). **Reaping is LAZY (at checkout),
  chosen over ≥2 alternatives:** a background timer thread/task (rejected — adds a
  runtime dependency + lifecycle, and cannot spawn from a pool built outside a
  runtime; sqlx's simplest mode is lazy too) and reap-on-return (rejected — Drop
  cannot `.await` a graceful close, and it cannot catch a connection that ages past
  a bound WHILE idle). Pool-builder-side config, NOT `ConnectConfig` (the 152 B pin
  is fixed). **ZERO-COST WHEN OFF:** the checkout `is_stale` call is gated behind
  `max_lifetime.is_some() || idle_timeout.is_some()` (Rust `&&` short-circuit → a
  default pool never evaluates `Instant::now()` on the reuse path), and the `Drop`
  `returned` restamp reads the clock ONLY when `idle_timeout.is_some()` (else it
  reuses the clock-free `created` stamp, which the disabled `is_stale` never reads)
  — matching the `slow_query_armed` / `n1-detect` zero-cost-off discipline. The one
  remaining `Instant::now()` in a disabled pool is the birth stamp at fresh-connect
  MINT (per-connection, syscall-dominated — NOT the per-checkout reaper overhead;
  gating it out would need a non-const `Instant` sentinel or a stored base
  `Instant`, a restructure). NO background task exists (grep the diff: no
  `thread::spawn` / `tokio::spawn`). Witnessed by the offline `reaper_tests`
  (`disabled_bounds_are_never_stale` pins the invariant the short-circuit relies on)
  and the `--ignored` `pool_liveness` suites (both
  drivers): `pool_reaps_a_connection_past_max_lifetime` /
  `pool_reaps_a_connection_past_idle_timeout` (a NEW backend pid + eviction count)
  and the negative `pool_reuses_a_connection_within_limits` (SAME pid, nothing
  reaped).
- **Dynamic-param TYPE FIDELITY (no silent coercion).** The dynamic
  `query_params` / `query_params_one` / `query_params_opt` / `execute_params` /
  `query_each_params` path declares each parameter's ENCODED type OID
  (`<P as ParamsWriter>::OIDS`) in its `Parse` frame — the SAME OIDs the
  compile-checked `query!` path bakes into its `Parse` template. So PostgreSQL
  decodes each binary parameter AS the client's declared type and applies normal
  SQL coercion: a correctly-typed param round-trips, a coercible one (an `int8`
  value into an `int4` comparison) is coerced by PG's own rules, and a genuinely
  incompatible one (a `&str` where an `int4` is required, no implicit cast) is a
  LOUD classified server error (`42883` / `22P02`), NEVER a silent byte-for-byte
  reinterpretation (the old `n_param_types = 0` hole silently matched `WHERE id =
  $1` bound with `&str "AAAA"` against `id = 1094795585` — the four ASCII bytes
  read as `int4`). This is the runtime peer of the wire-OID pinning the typed path
  and the SQLite twin already enforce — the "never a silent coercion" invariant now
  holds on ALL of bsql's parameter-binding surfaces (witnessed by the
  `dynamic_param_type_fidelity_{sync,async}` live tests, the
  `param_type_fidelity_three_path_parity` oracle, and the offline
  `dynamic_parse_param_oids` wire-shape gate). An `EnumLabel`'s `unspecified` OID
  `0` still leaves that one parameter to server inference, per-parameter. The
  `Parse` frame is streamed onto the growable send buffer via one generic
  `build_parse` (like `Bind`), so its SQL + OID list are uncapped.

  The EXPLICIT prepared-statement handle (`conn.prepare(sql)` →
  `query_prepared::<P>` / `execute_prepared::<P>`) closes the SAME hole from the
  OTHER direction, and is STRICTER. A prepared statement's plan is FIXED at
  `Parse` (the explicit handle declares NO OIDs — the server infers each `$N`),
  so the server CANNOT coerce a differently-typed binary `Bind` against it: a
  same-width wrong-typed value is read AS the pinned type, a silent reinterpret.
  `prepare` therefore RETAINS the server-inferred parameter types (from the
  prepare's `ParameterDescription`, parsed by `parse_param_description` and
  surfaced via `Engine::current_param_oids`) on the `PreparedStatement`, and
  `query_prepared` / `execute_prepared` (generic over `P`) VERIFY the caller's
  `<P as ParamsWriter>::OIDS` against them BEFORE the `Bind` — a per-parameter
  STRICT-EQUALITY check (a fixed plan admits no coercion, so int8-into-int4 is
  rejected here, UNLIKE the dynamic path where PG coerces) plus an arity check.
  A mismatch is a classified `DriverError::ParamTypeMismatch { index, expected,
  found }` / `ParamCountMismatch { expected, found }` returned with NO wire round
  trip, so the connection is untouched (neither is a disconnect — fix the
  parameter and retry on the SAME connection). An `unspecified` OID `0` on EITHER
  side (an `EnumLabel` param, or a `$N` the server could not infer — e.g. a bare
  `SELECT $1`) is not verifiable and passed through (best-effort), never falsely
  rejected. This makes the "never a silent coercion" invariant TRUE on every
  parameter-binding surface — typed (compile), dynamic (server), prepared
  (client), and SQLite (storage class) — witnessed by
  `prepared_param_type_fidelity_{sync,async}`, the extended
  `param_type_fidelity_three_path_parity` oracle, the offline
  `parse_param_description` unit + `decoder_fuzz` total-function gate, and the
  `Describe`-parse `#[cold]`-kept `next_event` codegen gate (byte-stable hot arm).

  INTENDED STRICTNESS (behaviour note): because a parameter's ENCODED type is now
  declared, binding a bare `&str` (OID `text` 25) where the server expects a
  PostgreSQL type with NO implicit `text` cast — an `enum`, `uuid`, `date`,
  `timestamptz`, etc. — is now a LOUD classified error (dynamic: server `42883`;
  prepared: client `ParamTypeMismatch`), where the OLD `n_param_types = 0` path
  silently reinterpreted the text bytes. This is strictly BETTER (loud, not
  silent-garbage): bind the proper typed value instead (the blessed
  `EnumLabel<E>` for an enum, `bsql::Uuid` / a temporal type for the rest — all
  unaffected), or add an explicit `$1::type` cast in the SQL where a text→type
  cast is valid. `char(n)`/`bpchar` note: a `bpchar` column right-pads to `n`, so a
  bound `&str` shorter than `n` compares/stores blank-padded per SQL semantics —
  unchanged by type fidelity (the OID is still `text`-compatible via the implicit
  cast), just the usual fixed-width-CHAR trailing-space behaviour.
- Runtime parameterized queries (`query_params` / `query_params_one` /
  `execute_params`) cost a SINGLE round trip whether run once or repeatedly, via a
  transparent per-connection dynamic prepared-statement cache keyed on the (SQL
  TEXT, parameter-type OIDs) pair (`DynStmtCache` in `bsql-postgres-core`). The key
  includes `P::OIDS`, not just the SQL text, so the SAME SQL string bound with a
  DIFFERENT parameter-type tuple is a DISTINCT cache entry with its own plan — a
  reuse NEVER crosses parameter types (which would reinterpret the new value's
  binary bytes as the cached plan's type — the reuse-path peer of the declared-OID
  `Parse`). The FIRST sighting of a SQL runs the
  fused unnamed path (`Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` +
  `Sync` in one flush — a one-shot query costs one round trip, not three), so a
  query run once pays nothing. A query run AGAIN is prepared to a named
  server-side statement (one one-time extra round trip on its SECOND sighting) and
  every later call reuses that plan in ONE round trip (`Bind`+`Execute`+`Sync`, NO
  server-side re-parse / re-plan) — strictly better than re-parsing on every call
  (the original bsql's behavior, and the ~14% the fused-only path lost on a
  repeated dynamic query; measured `pg_dynamic_4clauses` ~165 µs fused-only →
  ~153 µs cached, matching the original and the C libpq reference). The cache is
  INVISIBLE (the verb still takes SQL text), preparing on the SECOND sighting
  specifically so a genuinely one-shot query is never regressed from one round
  trip to two. It is BOUNDED (32 hot SQLs; a first sighting evicts the oldest
  PENDING slot — which holds no server-side statement, so eviction is free — and a
  READY statement is never evicted, so nothing leaks), SELF-HEALING (a schema
  change or out-of-band `DEALLOCATE` surfaces the classified `0A000` / `26000`
  once while the cache reclaims the stale statement and re-warms against the
  current schema — never a silently-stale result; the reuse path re-runs the
  fused query TRANSPARENTLY on stale detection, so a schema change costs one
  re-parse, never a user-visible spurious error). **DROPPED on a pool checkout — a
  CORRECTNESS requirement, not hygiene.** The single `Core::reset_session` (used by
  both a direct consumer and the pool at checkout) CLEARS the dynamic cache in ONE
  batched round trip (all cached statements `Close`d + a single `Sync`), so a
  pooled connection behaves EXACTLY like a fresh one for the next logical user.
  Keeping the dynamic cache WARM across a checkout (an earlier
  optimization) was REVERTED because it is not airtight: a dynamic runtime-SQL plan
  can resolve an UNQUALIFIED name (against the search path) or reference a SESSION
  object, so a plan a prior logical user promoted (e.g. `SELECT … FROM orders …`
  bound to `public.orders`, driven to a GENERIC plan) would survive into the next
  user's checkout — and a next user who creates a shadowing `CREATE TEMP TABLE
  orders` (with `pg_temp` already active, so the search-path OID list is unchanged)
  would receive the PRIOR user's `public.orders` rows: a SILENT CROSS-USER WRONG
  RESULT (a tenant-boundary leak, verified live). `DISCARD PLANS` cannot fix this
  robustly — it invalidates a kept plan only ONCE at checkout, and PostgreSQL
  re-validates it against the pre-shadow schema (any trailing reset statement, or a
  user query before the shadow exists, re-resolves it back to `public`; verified
  live), and a shadow created afterward is not seen — so DROPPING the statements
  (a fresh `Parse` on next use, resolving against the current user's schema) is the
  only airtight fix, at the cost of the pooled-reuse micro-optimization. The engine's
  compile-checked (TYPED) cache is KEPT (the typed flagship's server-side plan reuse
  is preserved) — safe NOT because a typed plan cannot bind a session object (it CAN:
  a fresh typed `Parse` under a `CREATE TEMP TABLE` shadow DOES resolve to the temp),
  but because a silent mis-decode is closed independently by the TYPED RESULT-SCHEMA
  OID GUARD (see the typed-path guarantee-boundary bullet below) plus PostgreSQL's own
  `0A000` refusal to change a reused plan's result type. A DIRECT (non-pooled)
  connection never resets on its own, so its dynamic cache persists for the
  connection's life. Witnessed by the `--ignored`
  `pooled_dynamic_plan_re_resolves_a_temp_shadow_across_users` live test (both drivers).

- **Typed `query!` RESULT-schema OID guard — the last silent-wrong-result surface,
  closed.** The typed decode is POSITIONAL / const-offset, so a runtime result column
  whose type DIVERGED from the migration schema the carrier was typed against — an
  out-of-band `ALTER TABLE ... ALTER COLUMN ... TYPE`, or a `CREATE TEMP TABLE`
  shadowing a migration table with a different column type — would silently mis-decode
  (a `text` decoder reading 4 `int4` bytes yields a plausible-but-wrong `"AAAA"`, not
  an error). This was the ONE bsql surface without runtime type verification (the
  dynamic path declares `P::OIDS` so PG rejects a mismatch `42883`; SQLite verifies
  each storage class; the typed ARRAY decoder already checks the element OID). It is
  now closed WITHOUT touching the flagship hot path: a typed cache MISS (a FRESH
  `Parse` — exactly where the resolved type can diverge) appends a `Describe`(portal)
  so the server returns a `RowDescription`, and the engine's guard (in the cold
  `apply_fused_row_stream`, off the `DataRow` hot arm — `next_event` is byte-identical)
  verifies each runtime column OID against the seated compile-time schema
  (`q.row_oids`). A divergence is a classified `DecodeError::ColumnOidMismatch { index,
  expected, found }` (surfaced as `DriverError::Decode`, NOT a disconnect — the guard
  drains the result to a clean idle so the connection is reusable), never a silent
  value; the drain reuses the over-cap `DrainOvercapToRfq` path so NO row reaches a
  streaming verb's `on_row`. A MISS that mismatched is NOT recorded in the statement
  cache, so a REPEAT re-`Describe`s + re-guards (never a cache HIT that skips the
  guard). A cache HIT sends no `Describe` (hot path unchanged, perf-neutral in steady
  state — a warm connection is all HITs): a HIT cannot silently mis-decode because
  PostgreSQL itself refuses to change a reused plan's result type
  (`ERROR: cached plan must not change result type`, SQLSTATE `0A000` — verified
  live), so a result-type-changing DDL is fail-loud, and a temp shadow created after
  the plan was recorded leaves it OID-pinned to the original table (correct rows) or
  forces the `0A000`. **The dynamic-OID skip needs NO build/macro change:** `q.row_oids`
  (already on `PreparedQuery`) equals the per-column marker OID (25/`text` for
  enum+composite, the base OID for a domain, the real native/bridged/array OID
  otherwise), and the guard SKIPS any column whose RUNTIME OID `>= FIRST_NORMAL_OID`
  (16384) — the PostgreSQL boundary above which every user-defined type (enum, domain,
  composite) is assigned, so a domain column (which reports its OWN dynamic OID, not
  its base's) / enum / composite is skipped exactly as the existing "no compile-time
  OID pin for user types" boundary requires (their runtime safety is
  `UnknownEnumLabel` / `CompositeArityMismatch`). The `text` family
  (`text`/`varchar`/`bpchar`, and their arrays) is one compatibility class (they share
  ONE wire decode — raw UTF-8 — so a `varchar` column reported as OID 1043 is NOT
  falsely rejected against the `text` marker 25), else EXACT equality. **Honest
  residual** (same dynamic-OID boundary as enum/domain/composite): a native migration
  column shadowed AT A MISS by a USER-DEFINED type with a DIFFERENT base (runtime OID
  `>= 16384` → skipped) is not caught — extremely exotic (a temp/DDL swap to a
  user-defined type), and within the deliberate "bsql does no connect-time OID
  resolution" boundary; a wire-COMPATIBLE swap (`varchar`↔`text`) is ACCEPTED, being an
  identical decode. Footprint: `DecodeError` stays 12 B (the new variant is the same
  shape as `TruncatedColumnData`); `ActiveEngine` 400 → 432 B — the guard's mismatch
  slot `Option<ResultOidMismatch>` grew 8 → 12 B (it now carries the checked
  `index` + `found` + `expected` triple, so the engine is the SINGLE source of the
  pair — the driver no longer recovers `expected` from the carrier, and the
  batch-generic PIPELINE settle reads the same triple) plus the pipeline guard's
  per-command expected-OID FIFO `pipeline_guard_oids: Vec<&'static [u32]>` (24 B, a
  bare Vec of `'static` fat pointers, empty in steady state). Both are COLD, appended
  AFTER the hot fields, so every hot-field offset is unchanged and `next_event` is
  byte-identical. **The guard now covers EVERY typed-decode surface** — the
  single-statement verbs AND the heterogeneous `pipeline((...))` (each command decodes
  into its own `Rows<Qi>`, so a MISS command whose runtime result columns diverged
  would silently mis-decode; see the pipeline bullet below). `execute_batch` is
  deliberately NOT guarded and NOT vulnerable: it reads affected COUNTS from the
  command tag and DISCARDS its RETURNING rows (it never decodes into `Q`), so it adds
  no `Describe` and stays byte-identical. HITs on every path are `0A000`-safe.
  Witnessed by the `--ignored`
  `query_oid_guard_live.rs` (both drivers: the mismatch caught on every typed verb
  `query`/`query_one`/`query_each` with the connection recovering, a matching-typed
  shadow decoding correctly, and `varchar`/`bpchar` columns NOT falsely rejected) over
  the dedicated `0020_oidguard.sql` migration table (per-connection TEMP shadows, so
  parallel-safe), the `--ignored` `pipeline_oid_guard_live.rs` (both drivers: a
  drifted pipeline command is a classified `BatchColumnOidMismatch` naming the command
  via `batch_failed_index()`, the connection recovering; a matching shadow, a
  `varchar`/`bpchar` column, and a user-type (domain) column NOT falsely rejected),
  plus the offline `engine_pipeline_spec` guard tests (command 0 + FIFO-seated
  subsequent command, match + drift + no-false-positive) and `engine_verbs_spec` /
  `engine_query_alloc` / corpus MISS-Describe wire updates and the byte-identical
  `engine_hotpath_codegen`.

  **Airtight pooled liveness is a fundamental 1-RTT-at-checkout tax (NOT removable).**
  A pooled checkout MUST confirm the peer is still alive BEFORE the user's
  (possibly non-idempotent) verb runs, so a peer that DIED WHILE IDLE is
  transparently swapped at checkout, never surfacing as an error on the verb. That
  confirmation is a fresh round trip completing AT CHECKOUT (the `pool_reset_session`
  reset serves as both the isolation reset AND the liveness probe): it CANNOT be
  overlapped with idle time (a probe sent at check-in confirms liveness only when it
  REPLIES — early idle — so a black-hole death LATER in idle escapes and hits the
  user's verb; empirically demonstrated) and CANNOT be fused into the verb (that
  sends the verb to an unconfirmed connection — the two-generals ambiguity). So a
  pooled op is ~2 RTT (reset + verb) vs a direct op's 1 RTT — measured pooled ≈
  2.3–2.4× the direct control's qps. This tax is the price of exactly-once liveness
  and is deliberately NOT traded away for throughput.
- **Dynamic streaming — `conn.query_each_sql(sql, on_row)` /
  `conn.query_each_params(sql, params, on_row)`** (both drivers + their
  transaction guards). The DYNAMIC (runtime-untyped) constant-memory streaming
  peer of the eager `query_sql` / `query_params`, and the PostgreSQL peer of the
  SQLite driver's identically-named verbs — so a dynamic stream over millions of
  runtime-assembled rows now reads the SAME on both backends (the last
  cross-backend asymmetry: SQLite had dynamic streaming, PG had only the TYPED
  `query_each`). Each row is lent to `on_row` as a zero-copy `BorrowedRow<'r>` as
  it arrives, accumulating NOTHING — a colossal runtime SELECT streams without
  growing memory (the escape from `query_sql`, which materialises the whole
  result). `BorrowedRow` borrows the transient wire buffer directly (no `Arc`,
  ZERO per-row allocation — the dynamic peer of the typed `query_each`'s borrowed
  record); its cell offsets are parsed ONCE per row into a REUSED slot table (via
  proto's `DataRowRef` walker, no copy), so column access is O(1) and the whole
  stream allocates nothing per row. Reads are POSITIONAL only (matching the
  SQLite streaming view): the result's column NAMES arrive on the wire only after
  every row, so per-row by-name resolution is impossible on the streaming path
  (by-name lives on the eager `QueryResult::row` → `RowRef`). `on_row` returns
  `ControlFlow`: `Continue` keeps streaming, `Break(e)` stops early — the payload
  rides `Ok(Some(e))` and the remaining rows are drained back to a clean idle so
  the (pooled) connection stays reusable (O(remaining rows), like the typed
  `query_each`); a per-row decode failure or a mid-stream server error is LOUD +
  drained, never swallowed. `query_each_sql` rides the SIMPLE-query wire (like
  `query_sql`); `query_each_params` rides the FUSED one-round-trip dynamic path
  (like the one-shot `query_params`) and deliberately does NOT touch the dynamic
  prepared-statement cache (a streaming bulk read is one-shot by nature). The
  three streaming verbs (typed + these two dynamic) share ONE post-pump settle
  (`Core::finish_stream`) so they cannot drift in how they reclaim the
  connection; the engine's two breakable dynamic verbs (`query_break` /
  `query_params_fused_break`) reuse the existing `pump_active_to_boundary`, so
  `next_event` is byte-identical (the codegen gate is green). Constant memory is
  gated by `engine_query_break_alloc` (a warm stream's allocation count is
  INDEPENDENT of the row count — an 8-row and a 512-row stream allocate the same
  fixed handful, proving zero per-row alloc). Witnessed live on both drivers by
  the `--ignored` `query_each_sql_*` / `query_each_params_*` tests (a 20 000-row
  in-order stream with a correctness check, a runtime `$1` filter, an early break
  leaving the connection reusable, and streaming inside a transaction).
- `copy_in` batches streamed `CopyData` to a 64 KiB threshold before flushing (a
  single chunk at or above the threshold streams DIRECTLY from the borrowed
  slice, never copied into the buffer), so a megarow bulk load costs about
  `total_bytes / 64 KiB` write syscalls instead of one per row, with the send
  buffer bounded under `2 ×` the threshold — constant memory regardless of COPY
  size.
- **Typed binary COPY — `conn.copy_in_typed::<Q>(rows)` + the `copy!` macro.**
  The SAFE-BY-CONSTRUCTION bulk-insert flagship, both drivers (+ their transaction
  guards). The raw `copy_in` / `copy_in_with` takes `&[u8]`: the caller
  hand-formats COPY *text* with correct escaping / NULL sentinels, and a
  mis-escaped tab or newline SILENTLY corrupts a row (the classic COPY footgun).
  `copy!(Name, "table", (cols))` validates the target table + columns + their
  types against the SAME build catalog `query!` reads and emits an uninhabited
  `Name` carrier implementing `TypedCopyIn` — a GAT `Row<'q>` tuple pinning the
  column encode types (a `NOT NULL` column is `T`, a nullable column `Option<T>`;
  `text` / `bytea` borrow `&'q str` / `&'q [u8]`) and a const `SQL` =
  `COPY <table> (<cols>) FROM STDIN WITH (FORMAT binary)`. `copy_in_typed::<Name>`
  streams each `rows` item (an `IntoIterator<Item = Q::Row<'q>>`) as one PGCOPY
  *binary* row — an `int16` field-count + each field's `{len, bytes}` / `-1`,
  which is byte-identical to a Bind parameter block, so it REUSES the same
  `ParamsWriter` binary leaves the `query!` param path uses (no format drift) and
  streams through the EXISTING 64 KiB batcher in constant memory (no per-row
  scratch; one copy per field). This is a genuine tier-elevation over the raw
  path: FASTER (no text parse/format on either side) AND injection-safe by
  CONSTRUCTION — a typed value cannot carry an escaping bug (there is no text to
  mis-escape; an embedded tab / newline / quote rides the binary field verbatim),
  and the target identifiers are a compile-time constant baked from validated
  catalog names (stronger than the raw path's runtime `SafeTable`). A wrong-typed
  or wrong-arity row is a compile error at the `copy_in_typed` call (the tuple
  does not match `Row<'q>`); an unknown / duplicate / over-32 / unsupported
  (array — use raw `copy_in`) column is a `copy!` `compile_error!`. A carrier
  names at most 32 columns — the row tuple is a `ParamsWriter`, whose tuple impls
  cover arity `0..=32` (raised from 16 so a wide bulk-load target is not capped;
  a `query!`'s `$N` params ride the same raised cap, while its result-column
  decode stays 16). The whole orchestration
  (begin + header + rows + trailer + `CopyDone`) lives ONCE in `Core::copy_in_typed`;
  a mid-stream server rejection is a classified `DriverError::Db` and the
  connection RECOVERS (pooled). The raw `copy_in` / `copy_in_with` STAYS as the
  advanced escape hatch for pre-formatted / text COPY data. Witnessed by
  `tools/query_fixture`'s `copy_typed_offline` (macro expansion + row shape) +
  `--ignored` `copy_typed_live_{sync,async}` (hostile-string / NULL / large
  multi-flush round-trip + rejected-row recovery, both drivers) + the
  `copy_wrong_*` / `copy_unknown_column` trybuild goldens, and the
  `engine_copy_typed_alloc` constant-memory gate.
- **Atomic query pipelining — `conn.pipeline((Q0::bind(p), Q1::bind(p), …))`**
  (both PG drivers + the `Transaction` guard; a SQLite sequential twin). Sends N
  compile-checked `query!` commands in ONE round trip instead of N, and returns
  the typed tuple `(Rows<Q0>, Rows<Q1>, …)` — each command decoded against ITS OWN
  carrier's compile-time OIDs (no erasure), each guarded against RESULT-schema drift
  (below). Pure `query!` carriers, NO builder —
  `Bound<Q>` (`Q::bind(params)`) is a bound carrier, not a runtime SQL fragment;
  the `Pipeline` sealed trait has hand-written tuple impls arity `1..=16`. **The
  contract is ATOMIC all-or-nothing** — this is the ONLY airtight semantic, forced
  by the verified PostgreSQL fact that N extended-query commands with ONE trailing
  `Sync` form a SINGLE implicit transaction (a mid-batch error ROLLS BACK the
  commands before it, errors the failing one, and SKIPS the rest). So the whole
  batch commits and returns every result, or it returns `Err(DriverError::BatchFailed
  { index, source })` (naming the zero-based failing command; `batch_failed_index()`
  accessor) and **ZERO** results. Returning "the results before the failure as `Ok`"
  is FORBIDDEN — those writes were rolled back. Airtight BY CONSTRUCTION, not
  discipline: the ONLY code path that builds the `Ok((Rows<Q0>, …))` tuple is the
  `CommandStatus::Completed` arm, reached ONLY after the pump's clean trailing
  `ReadyForQuery` (`Boundary::Idle`), which the server emits ONLY if the whole
  implicit transaction COMMITTED; a rolled-back / failing / skipped command has no
  `Ok` value to read. **Consistent with every other verb on error:** `pipeline`
  does NOT auto-rollback — a mid-batch failure inside an explicit transaction
  (a `Transaction` guard, or a caller `BEGIN` in the batch) leaves it ABORTED
  (`'E'`), untouched, exactly as a normal failed verb does; the transaction's OWNER
  rolls it back (the guard at closure exit, a pooled `reset_session` on checkin, or
  the direct caller). This closes a silent-autocommit-escape: a caller who IGNORES
  a pipeline error inside a guard and issues another verb gets a LOUD `25P02`
  (aborted transaction), never a silent autocommit (witnessed by
  `ignored_in_guard_pipeline_error_does_not_autocommit_later_verbs`). The COMMON
  implicit-tx batch (no explicit `BEGIN`) needs no rollback — the single `Sync`
  closes it server-side → `Idle`, connection clean. **Engine:** the receive side is
  the extended-protocol twin of the existing simple-query `AwaitingRfq` multiplexer
  — ONE new dispatch state `PipelineAwaitingNextOrRfq` reusing every existing
  sub-chain, staged by `stage_pipeline` (per command `[Close+Parse if MISS] + Bind +
  Execute`, the `Sync` hoisted to the batch end) with a 1-byte `pipelining` flag on
  `ActiveEngine`. `next_event` stays BYTE-IDENTICAL (the continuation is
  `#[cold]`/out-of-line like `step_fused`; the DataRow hot arm is untouched — the
  codegen gate is green). `DriverError::BatchFailed`'s payload is 16 B
  (`usize` + `Box<DbError>`), so the 24 B `DriverError` pin holds; `deps_pin` is
  unchanged (no new dependency). **RESULT-schema OID guard (the last silent-mis-decode
  surface, closed on the pipeline too).** Because each command decodes into its own
  `Rows<Qi>` POSITIONALLY, a cache-MISS command whose runtime result columns diverged
  from its carrier's migration schema (an out-of-band `ALTER COLUMN TYPE`, a `TEMP`
  shadow) would silently mis-decode — the SAME class the single-statement verbs close.
  It is now closed on the pipeline WITHOUT touching the hot path: per MISS command a
  `Describe`(portal) is appended (a HIT sends none — byte-identical, `0A000`-safe), and
  the `PipelineAwaitingNextOrRfq` multiplexer runs the SAME
  `apply_fused_row_stream` / `result_oid_compatible` / 16384-user-type-skip check the
  single-query MISS uses, against that command's expected `Qi::row_oids` (command 0's
  seated at staging; a subsequent command's popped off a per-command FIFO
  `pipeline_guard_oids: Vec<&'static [u32]>` at its `CloseComplete`). A divergence is a
  classified `DriverError::BatchColumnOidMismatch { command, source: DecodeError::ColumnOidMismatch }`
  — the reused decode error plus the failing COMMAND index (read via the same
  `batch_failed_index()` as `BatchFailed`); its 16 B payload holds the 24 B `DriverError`
  pin. The client drains to a clean idle (reusable) and returns ZERO results, so no
  garbage row ever decodes. HONEST NOTE: unlike a mid-batch SERVER error (rollback), a
  mismatch is a CLIENT decode rejection AFTER the server processed — and, for the
  implicit-tx batch, COMMITTED — the transaction; the client returns the drift instead of
  decoding (fail-loud beats silent-wrong: the caller learns the SCHEMA drifted, not a
  transient error to blind-retry — the SAME already-broken-schema situation the
  single-query guard has). `ActiveEngine` grows 400 → 432 B (the guard's mismatch slot 8
  → 12 B carrying the checked `index`/`found`/`expected` triple from ONE source, plus the
  24 B FIFO), both COLD and appended after the hot fields, so `next_event` is
  byte-identical. `execute_batch` is NOT guarded and NOT vulnerable (it discards its
  RETURNING rows — no decode into `Q` — so it adds no `Describe` and stays byte-identical).
  Cancellation (`57014`, connection recovers) and a
  mid-batch transport death (classified disconnect, bounded, never a torn success)
  are honored. **SQLite twin:** `sqlite_conn.pipeline((…))` runs the commands
  SEQUENTIALLY inside the standard `transaction` guard's plain `BEGIN … COMMIT`
  (a deferred begin — the typed pipeline never upgrades a lock, so `BEGIN
  IMMEDIATE` would needlessly cut read concurrency; that write-lock-up-front form
  is the MIGRATION runner's, not the pipeline's) — the IDENTICAL typed tuple + the
  IDENTICAL all-or-nothing contract (no RTT win, in-process; read-only in a
  conformance build since the SQLite authorizer permits only SELECTs for typed
  writes, so the atomicity is read-consistency across the batch). Measured PG (loopback, K heterogeneous SELECTs, `pipeline` vs K serial
  `query_one`): K=2 ~3.5×, K=8 ~5×, K=16 ~7.3× — the RTT term collapses from K× to
  1×; a single-statement batch (N=1) equals today's fused query (no regression, the
  path is opt-in). Witnessed by `tools/query_fixture`'s `pipeline_live_{async,sync}`
  (all-or-nothing rollback proof, the ignored-in-guard blind-zone regression, the
  index accessor, cancel, transport death, in-guard commit), the `--ignored`
  `pipeline_oid_guard_live` (both drivers: a drifted command → classified
  `BatchColumnOidMismatch` naming the command + connection recovering; a matching
  shadow, `varchar`/`bpchar`, and a user-type (domain) column NOT falsely rejected),
  the offline `engine_pipeline_spec` (the multiplexer + mid-batch drain, PLUS the
  per-command guard: command 0 + FIFO-seated subsequent command, match + drift + the
  varchar/bpchar/user-type no-false-positive), the SQLite `pipeline_sqlite`, and the
  `pipeline_wrong_param` trybuild golden.
- **Homogeneous atomic bulk write — `conn.execute_batch::<Q>(params_iter)`**
  (both PG drivers + the `Transaction` guard; a SQLite sequential twin). Runs ONE
  compile-checked `query!` write carrier `Q` (an `UPDATE`/`DELETE`/`INSERT ...
  RETURNING`) against N runtime parameter sets — `Parse`d ONCE and re-bound for every
  set (the bulk win) — returning a `Vec<u64>` of per-command affected counts. The
  batch peer of the typed `execute::<Q>(params)` and the homogeneous sibling of
  `pipeline((…))`: it closes the gap those two leave (N varying-parameter writes fit
  neither `copy_in_typed` [INSERT-only] nor `pipeline` [fixed-arity tuple]). NO new
  vocabulary — one method symmetric with `copy_in_typed::<Q>(rows)` / `execute::<Q>(params)`,
  its params the SAME lifetime-GAT tuple (`Q::Params<'p>`) the typed path uses (a
  runtime `String`/buffer binds). Like `execute::<Q>`, `Q` is a row-shaped carrier
  (SELECT or `… RETURNING` — the macro rejects a bare non-returning write); the
  affected count rides the `CommandComplete` tag and any RETURNING rows are
  read-and-ignored (symmetric with `execute::<Q>`). **The contract is ATOMIC
  all-or-nothing, INHERITED from the pipeline core** — the N commands ride ONE trailing
  `Sync` (a single implicit transaction), so the whole batch commits and returns every
  count or it errors and returns ZERO. It reuses `pipeline`'s staging (`stage_pipeline_frames`
  for command 0, then a Parse-once bare `Bind`+`Execute` per set — `stage_batch_frames`),
  its receive multiplexer (`PipelineAwaitingNextOrRfq`, VERBATIM — a subsequent bare
  `Bind` leads with the `BindComplete` the FSM already handles), and its SETTLE (the
  same `failed_index`/`arity` classifier): a mid-batch failure is `DriverError::BatchFailed
  { index }` (`batch_failed_index()`), a COMMIT-TIME failure (a `DEFERRABLE INITIALLY
  DEFERRED` constraint) is `DriverError::Db` with `batch_failed_index()` `None` (the
  inherited commit-time-`None` fix — a failure at the implicit COMMIT belongs to no
  single command, never an out-of-range `index == arity`). NO auto-rollback (a mid-batch
  failure inside an explicit transaction leaves it aborted `'E'` for its owner; a next
  in-guard verb is a loud `25P02`, never a silent autocommit) and the abort-preserves-BEGIN
  fix (a FIRST-window `FrameTooLong` reuses `abort_pipeline_staging`'s peek path — the
  consumed deferred `BEGIN` survives). `N == 0` does NO wire I/O (`Ok(vec![])`); `N == 1`
  equals a single `execute` (no regression). **Constant SEND memory, deadlock-free (the
  windowed batcher).** A large N must NOT buffer all N `Bind`s: the commands stream and
  flush at a 64 KiB threshold (`BATCH_WINDOW_THRESHOLD`, like `copy_in`), so the
  staged-bytes high-water is bounded regardless of N. UNLIKE COPY — where the server is
  silent while the client streams, so a write-ahead cannot deadlock — an extended-protocol
  command emits a per-command response, so streaming N `Bind`s without reading would fill
  BOTH the server's output buffer AND the client's send buffer and DEADLOCK. So each
  window ends with a `Flush` (which forces the window's responses out WITHOUT ending the
  implicit transaction — only the single trailing `Sync` does that, so all N stay ONE
  atomic transaction) and the window's responses are DRAINED before the next window is
  staged (a breakable `run_pipeline_break` sink breaks at the window's delivery count →
  `Boundary::Stopped`, then resumes). A batch whose commands fit one window (the common
  case) is exactly ~1 round trip with ONE `Sync` and no intermediate `Flush`; only a
  genuinely huge N pays ~`N / window` round trips — the honest floor for a deadlock-free
  constant-memory bulk over a bidirectional protocol (a single unbounded flush is either
  O(N) send memory or a hang). Cancellation (`57014`, connection recovers) and a mid-batch
  transport death (classified disconnect, bounded) are honored. `next_event` stays
  BYTE-IDENTICAL (the receive FSM is reused — no new dispatch state); `DriverError` (24 B)
  / `ConnectConfig` (152 B) / `ActiveEngine` (392 B) footprints are unchanged (no new
  variant or field — the `pipelining` flag and `BatchFailed` variant are the pipeline's);
  `deps_pin` is unchanged. **SQLite twin:** `sqlite_conn.execute_batch_typed::<Q>(…)` runs
  the commands SEQUENTIALLY inside the standard `transaction` guard's `BEGIN … COMMIT`,
  returning the SAME `Vec<u64>` + the IDENTICAL all-or-nothing contract (no RTT win,
  in-process). It is `execute_batch_TYPED` (not `execute_batch`) because SQLite already
  has a dynamic `execute_batch(sql)` — rusqlite's multi-statement executor, a semantically
  DISTINCT operation — so the name is disambiguated exactly as `copy_in` / `copy_in_typed`
  are; a consumer doing cross-backend bulk writes uses the PG `execute_batch` and the
  SQLite `execute_batch_typed`. READ-ONLY under a conformance build (with `macros-sqlite`
  a typed WRITE `query!` is rejected at its definition site, so every SQLite carrier is a
  SELECT — the atomicity is read-consistency and the counts are 0; a typed WRITE batch is
  a PostgreSQL-only capability, NOT advertised on SQLite). Witnessed by `tools/query_fixture`'s
  `execute_batch_live_{async,sync}` (N correct counts + all applied; the all-or-nothing
  bulk-write rollback — a mid-batch failure applies NOTHING, zero rows persisted, at both
  small and LARGE N; the commit-time-`None` inheritance; the no-auto-rollback `25P02`; N=0
  no-I/O + N=1; the LARGE-N 20 000-command windowed run — deadlock-free where a single
  flush would hang), the offline `engine_execute_batch_alloc` constant-send-memory gate
  (the staged-bytes high-water is IDENTICAL for a small-N and a 100×-larger batch, and
  bounded under 2× the threshold — driving 400 000 windowed commands), the SQLite
  `execute_batch_sqlite`, and the `execute_batch_wrong_param` trybuild golden.
- **Query cancellation** — `conn.cancel_token()` mints a DETACHED
  `CancelToken` (`Send + Sync + 'static`, borrowing NOTHING from the connection)
  that can be obtained BEFORE a long query and moved to another task/thread that
  calls `token.cancel()` while the query is in flight — no `&mut` aliasing with
  the in-flight future. The token is UNFORGEABLE (the `BackendKeyData` secret is
  captured only at connect, kept in a `Sensitive<i32>` end-to-end; `Core::cancel_key()`
  clones it into the detached `CancelKey`) and pinned tier-1 (footprint 56 B).
  Because a PostgreSQL cancel MUST travel on a SECOND socket (the owning
  connection is blocked server-side), `cancel()` opens a THROWAWAY socket to the
  same endpoint by driving each driver's OWN wire-builder over a rebuilt
  credential-free `ConnectConfig` (the `Redial` snapshot — host/port/raw-ssl-mode/
  ca-roots, NO password), so it re-runs the `SSLRequest` probe and honors the
  original `SslMode` / custom CA roots — a cancel to a TLS-required server
  negotiates TLS — with ONE wire authority and no drift. **HONEST FRAMING:** PG
  cancel is BEST-EFFORT by spec (§55.4) — a CAPABILITY, not a guarantee: `cancel()`
  REQUESTS cancellation (the canceled query returns SQLSTATE `57014`
  `query_canceled` and the connection is left drained + reusable), it does not
  promise the query stops (a late cancel is a server no-op; a double cancel is two
  harmless packets). The SQLite twin `SqliteCancelToken` (over rusqlite's
  `InterruptHandle`) reads the SAME — `conn.cancel_token()` / `token.cancel()` —
  for one cross-backend mental model, but cancels IN-PROCESS via
  `sqlite3_interrupt` (so `cancel()` is infallible → `()`, and an interrupted step
  is the classified `SqliteError::Interrupted`; the connection stays reusable).
  Both PG drivers' cancel is witnessed by the `--ignored` `cancel_token_stops_an_inflight_query`
  live tests; SQLite's by `crates/sqlite/driver/tests/cancel.rs`.
- **Server-side `statement_timeout` — `ConnectConfig::with_statement_timeout(Duration)`.**
  The SERVER-side complement to the client `CancelToken`, and the standard
  production guardrail against a runaway query: PostgreSQL aborts any statement
  running longer than the budget with SQLSTATE `57014` `query_canceled`, and the
  connection is left drained + REUSABLE (a `statement_timeout` abort is NOT a
  disconnect — `is_disconnect()` is false, so the guardrail sheds a slow query
  without killing the pooled connection). Implemented as a footprint-neutral
  convenience over the EXISTING startup-parameter map (it inserts
  `("statement_timeout", <ms>)` via `with_startup_param`, adding NO `ConnectConfig`
  field — the 152 B pin is untouched), CHOSEN over a per-query `SET
  statement_timeout` round trip (which costs an extra RTT and is not session-wide)
  and over a new typed field (which would break the footprint pin and format into
  the same map anyway). As a startup-packet GUC it applies from before the first
  query and becomes the session's reset value, so it SURVIVES a pooled
  connection's `RESET ALL` on checkout — the guardrail persists across checkouts.
  The `Duration` maps to PostgreSQL's integer-millisecond GUC: `Duration::ZERO` →
  `"0"` (PG's own convention: DISABLED — the explicit opt-out); a non-zero
  sub-millisecond duration rounds UP to `1` ms (never DOWN to `0`, which would
  silently weaken the guardrail into "disabled"); and a duration whose whole
  milliseconds exceed PG's 32-bit GUC ceiling (`i32::MAX` ms ≈ 24.8 days) is
  capped there so an enormous `Duration` never produces a value the server
  rejects. Witnessed live on both drivers
  (`statement_timeout_aborts_a_runaway_query_and_the_connection_recovers`: a
  `with_statement_timeout(200ms)` connection aborts `SELECT pg_sleep(2)` with a
  classified `57014` and then reuses the connection, while a connection WITHOUT
  the timeout runs the same sleep to completion) plus offline unit tests for the
  ms formatting / zero / sub-ms / cap.
- **Client-side read-liveness window (a black-holed in-flight query cannot hang) —
  derived from `statement_timeout`.** A DIRECT (non-pooled) in-flight query's
  socket read has no client deadline by default, so a peer that VANISHED while
  still ALIVE (a black-hole proxy whose kernel keeps ACKing — TCP keepalive is
  answered, so keepalive canNOT detect it — but whose app forwards nothing) leaves
  the read blocked indefinitely, and the server's `statement_timeout` `57014` abort
  is ITSELF black-holed, so `statement_timeout` alone bounds the SERVER, not the
  client (measured: a black-holed query hangs past a generous outer timeout even
  with `statement_timeout` set). The catch: a silent black-hole is
  INDISTINGUISHABLE at the socket layer from a server legitimately taking a long
  time to produce the first byte, so NO fixed client deadline can catch it without
  cutting a legitimate slow query — EXCEPT one derived from the server's own query
  budget. When a `statement_timeout` is configured, both drivers therefore arm a
  per-read INACTIVITY window = `statement_timeout` + `connect_timeout`
  (`ConnectConfig::client_liveness_window`, the ONE authority both drivers resolve
  through, like `resolve_endpoint`): the async driver as a third `ReadDeadline`
  mode (`ReadBound::Within` — a relative `timeout` re-armed per read, the absolute
  reset/notification deadline still taking priority), the sync driver as the steady
  `SO_RCVTIMEO` the socket rests at between verbs (per-read by nature). A
  black-holed read then ELAPSES at the window into a classified `DriverError::Timeout`
  (`is_disconnect()` true → a resilient consumer reconnects) instead of the kernel's
  `tcp_retries2` hang — and it NEVER cuts a query the server allows, because the
  server would already have aborted any query past `statement_timeout` (the `+
  connect_timeout` margin covers the abort's network round-trip), and the per-read
  inactivity semantics never trip a legitimately slow STREAM whose bytes keep the
  window fresh. This bounds BOTH a DIRECT query and a POOLED connection's IN-FLIGHT
  query (the pool bounds a CHECKOUT via `reset_session`; this bounds the query
  running ON the checked-out connection — the two together leave no unbounded
  window), and it makes the dead-KERNEL case snappier than keepalive's ~2.5 min
  too. **NEVER falsely cuts a query the server ALLOWS (runtime-drift tracked):** the
  connect-time window would go STALE if a runtime `SET statement_timeout` RAISED the
  server budget (a query the server now permits would be client-cut — a real bug).
  So the window is RE-DERIVED, never left stale: (1) the migration runner SUPPRESSES
  the window for its whole run (a trusted bsql-controlled long op — a `CREATE INDEX
  CONCURRENTLY` behind a `SET statement_timeout = 0` — is governed by its own
  server-side budget, so bsql never client-cuts its own migration; async via
  `ReadDeadline::suppress_scoped` RAII, sync via a socket read-timeout save/restore);
  (2) EVERY dynamic runtime-SQL verb — the raw-text (`simple_query`/`execute_sql`/
  `query_sql`/`query_one_sql`/`query_opt_sql`), param (`query_params*`/
  `execute_params`) and streaming (`query_each*`) families, on the CONNECTION AND
  inside a `transaction` GUARD (the guard borrows the connection's window cell and
  re-derives through the SAME `window_after_statement` authority — closing the
  former tx-guard observation gap) — OBSERVES its executed SQL's effect on
  `statement_timeout` (the shared, pure `statement_timeout_effect` classifier: a
  precise top-level `SET`/`RESET` re-derives to the new budget — `Unchanged` after a
  few-byte check for a non-`SET`/`RESET` statement, the value parser handles PG's
  duration forms `us`/`ms`/`s`/`min`/`h`/`d`; an AMBIGUOUS `SET` — `SET LOCAL`,
  `= DEFAULT`, an unparseable value — DISARMS; and DISARM-ON-SUSPICION drops the
  window whenever the text mentions BOTH `set_config` AND `statement_timeout`, since
  a `set_config('statement_timeout', …)` — the only in-session GUC-setter function,
  reachable from a `SELECT`/`DO` block — cannot be pinned to a value, so dropping the
  client bound is the only fail-safe choice; requiring `set_config` too keeps a query
  that merely MENTIONS the GUC name as data from disarming); (3) the pool
  `reset_session` restores the window to the connect baseline (a runtime change on
  one checkout never leaks its window to the next). The window is a mutable atomic
  (async `AtomicU64` ms + sentinel, one relaxed load per read) / a re-`setsockopt`'d
  `SO_RCVTIMEO` (sync). The remaining false-cut residual is the THEORETICAL FLOOR:
  ONLY a `statement_timeout` change whose executed SQL has NO contiguous textual
  mention of the GUC name — a `SELECT my_func()` whose body calls `set_config`, an
  `EXECUTE prepared_stmt` whose prepared body does, an adversarial
  `set_config('statement' || '_timeout', …)` — can still leave the window stale-low.
  PostgreSQL does NOT report `statement_timeout` via `ParameterStatus` (it is absent
  from the hard-wired GUC_REPORT set), so a client CANNOT observe such a change
  without a forbidden per-query round trip — this is a genuine limit, not a
  discipline gap, and every OBSERVABLE form fails safe (disarm or re-derive-up).
  **ZERO-COST + honest residual:** no `statement_timeout` → `client_liveness_window`
  is `None` → the steady read is byte-identical to the historical unbounded one (a
  fresh `None`-window `ReadDeadline` / a `None` `SO_RCVTIMEO`), because without a
  declared query budget NO finite client deadline is safe (it would cut legitimate
  long queries) — so that residual is fundamental, not a gap (a dead KERNEL is
  still caught by keepalive; a live black-hole on a budget-less connection is the
  same case libpq leaves to `statement_timeout` + cancel). Footprint-neutral: no
  new `ConnectConfig` field (derived from the existing startup map + `connect_timeout`;
  the 152 B pin is untouched); the async `Arc<ReadDeadline>` and the DataRow hot
  path (`next_event`) are byte-identical (the window lives in the driver's socket
  read, below the engine). Witnessed by the `--ignored` `direct_liveness` suites
  (both drivers: a black-holed direct AND pooled-in-flight query is a bounded
  classified `Timeout`; a mid-query FIN is a classified disconnect in ms; AND the
  never-false-cut half — an under-budget query returns, an over-budget query on a
  HEALTHY peer gets the SERVER's recoverable `57014` not the client `Timeout`, a
  runtime `SET` RAISING the budget is not falsely cut, a `SET` inside a `transaction`
  guard is not falsely cut (`tx_guard_set_raising_the_budget_is_not_falsely_cut`), a
  `set_config`-raised budget is not falsely cut
  (`set_config_raising_the_budget_is_not_falsely_cut`), and a migration long op is
  not client-cut) plus the offline
  `client_liveness_window_is_derived_from_statement_timeout` /
  `steady_window_bounds_a_read_without_an_armed_deadline` /
  `observe_statement_timeout_re_derives_the_steady_window` /
  `window_after_statement_maps_every_effect` /
  `statement_timeout_value_parsing` / `statement_timeout_effect_classification`
  unit tests.
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
  loopback TCP on the by-PK single-round-trip (`benches/unix_vs_tcp.rs` on the
  `bench` branch — the competitor/RSS harness lives there, off the code branch).
- `SslMode::Prefer` surfaces an SSL downgrade (the server refused TLS and it fell
  back to plain TCP — a security event a production build must not hide): it
  routes through the structured-diagnostics sink as `DiagEvent::SslDowngrade`
  when one is installed (see the diagnostics bullet below), and keeps the
  historical stderr warning (debug AND release) when no sink is set, so a
  consumer who installs no sink sees no behaviour change. A consumer can also
  assert `Connection::is_encrypted()` (both drivers) to reject a
  plaintext/downgraded connection.
- **Structured diagnostics — `DiagEvent` + the `Diagnostics` sink.** The dep-free
  observability seam (no `tracing`/`log`/`metrics` in the runtime graph — a
  consumer picks their own stack; an optional `tracing` adapter can wrap the
  callback later). A `Diagnostics` handle carries an
  `Option<Arc<dyn Fn(&DiagEvent<'_>) + Send + Sync>>` sink plus a slow-query
  threshold; a consumer installs it on a standalone connection
  (`Connection::connect_with(config, &diag)` / `set_diagnostics`) or on a pool
  (`Pool::builder(config, max).on_diagnostic(..).slow_query_threshold(..).build()`,
  which rides every minted connection). It is NOT a `ConnectConfig` field — the
  152-byte config footprint is untouched. **Zero-cost when off:** an unset sink
  is a single never-taken `if let Some` branch at each COLD lifecycle boundary —
  no event built, no wire parsed, no `Instant::now`, no alloc; the per-row hot
  path is untouched (the `next_event` codegen ceiling is byte-identical — this is
  the deliberate distinction from the deleted per-row `Observer`, which fired per
  row on the hot path). **No PII:** a `DiagEvent` never carries a bound parameter
  VALUE — a slow-query event carries the SQL TEXT (or a digest a consumer
  computes), never the values. **A panicking sink cannot hurt the driver:** the
  sink is arbitrary consumer code, so EVERY invocation routes through ONE
  `diag::dispatch` that wraps it in `catch_unwind(AssertUnwindSafe(..))` (SAFE — no
  `unsafe`) and DROPS a caught panic (noted once to stderr); diagnostics are
  strictly non-correctness, so a buggy callback can never poison a connection
  (unwind a verb before it restores its `Live` token → `NotReady`) or abort the
  process (a double-panic from a `Drop` mid-unwind). **A sink can do ANYTHING —
  the driver absorbs it structurally:** a pool event's sink runs OUTSIDE the
  pool's state lock (the sync pool `drop`s the guard before the emit), so a sink
  that inspects the pool (`pool.stats()`) cannot deadlock; and a per-thread
  `IN_DISPATCH` flag in the single `dispatch` chokepoint SUPPRESSES any diagnostic
  emitted from WITHIN a sink (a self-slow query, a `pool.get()` that times out, a
  direct `emit`), so a self-emitting sink fires exactly once and can never recurse
  into a stack-overflow abort (the flag resets via an RAII guard, so it clears
  even if the sink panics). So the sink contract is the strongest form — a sink
  may do anything, and the only consequence a consumer owns is that a diagnostic
  emitted from inside a sink does not itself reach a sink. The events (all
  `#[non_exhaustive]`): `ServerNotice
  { severity, code, message }` (a `RAISE NOTICE`/`WARNING` — the primary PL/pgSQL
  log channel, formerly dropped `=> {}` by both materializers; surfaced by the
  shared `capture_notify` adapter, borrowed `Cow` fields, total parse via the
  fuzz-proven `error_response_fields` walk; SCOPE: STEADY-STATE query streams — a
  pre-auth NoticeResponse during the connect HANDSHAKE rides the connecting-phase
  dispatch and is not surfaced); `SslDowngrade { host }`; `SlowQuery { sql, elapsed
  }` (gated behind `Diagnostics::slow_query_armed()` = a threshold AND a sink, so
  the off path reads no clock — reported by a `SlowQueryGuard` that fires on drop,
  ONLY for a verb that COMPLETED successfully — `commit_slow` marks the `Ok` path;
  an errored/cancelled/panicked verb reports nothing — and covers the compile-
  checked FLAGSHIP (`query`/`query_one`/`query_opt`/typed `execute`) AND the
  dynamic verbs (`query_sql`/`execute_sql`/`query_params`/`execute_params`, timing
  the whole cache-promotion), DELIBERATELY excluding the streaming verbs
  (`query_each*` — a stream's duration is consumer iteration time, not query
  latency) and the low-level `query_prepared`/`execute_prepared` primitive
  (already timed via `query_params`); `PoolAcquireTimeout { waited }` /
  `PoolConnectionEvicted` (plus monotonic relaxed-atomic counters —
  acquire-timeouts, evictions, and waiter high-water which counts only truly
  BLOCKED waiters via the async `try_acquire_owned` fast-path, `0` when
  uncontended — via `Pool::stats() -> PoolStats`); `ParameterStatus { name, value }`
  (a GUC change); `MigrationLockWaiting { elapsed }` / `MigrationApplying { name }`
  / `MigrationApplied { name }` (a serialized deploy is no longer a silent
  freeze); and `PreparedCacheSelfHeal { sql }` (the transparent stale-plan
  re-prepare, an otherwise-invisible fallback). All fire ONLY at cold boundaries,
  never from the DataRow hot arm. Witnessed live on both drivers
  (`raise_notice_surfaces...`, `ssl_prefer_downgrade_routes...`,
  `pool_acquire_timeout_emits_and_counts`, `slow_query_emits_with_the_threshold_set`,
  the typed `typed_slow_query_emits_slow_query`, the panic-safety
  `panicking_sink_neither_aborts_nor_poisons_the_connection`, the no-deadlock
  `sync_pool_stats_sink_on_acquire_timeout_does_not_deadlock`, the
  `uncontended_checkout_leaves_waiters_high_water_zero`,
  `{async,sync}_runner_emits_progress_events`, all `--ignored`) + offline (the seam
  plumbing, the notice/param-status/arm-gate parses). Follow-ups: a
  `Connected`/`CancelRedial` lifecycle event, cancel issue/no-op events, and the
  optional default-off `tracing` adapter.
- The default `SslMode` is THREAT-SCOPED, not a fixed value: when the consumer
  sets none (no builder `ssl_mode` / DSN `sslmode=` / `PGSSLMODE`), the effective
  mode is resolved at connect against the endpoint by
  `ConnectConfig::resolve_ssl_mode`, scoped to where an interception threat can
  actually exist — a network path. A LOCAL endpoint (a unix socket, or a loopback
  TCP host — `localhost` case-insensitive, `127.0.0.0/8`, `::1`) resolves to
  `Prefer`: no network to intercept, and PG offers no TLS on a unix socket. A
  REMOTE endpoint (any other host, INCLUDING private ranges like `10.0.0.0/8` /
  `192.168.0.0/16` — still a network path) resolves to `Require`: a remote server
  that refuses TLS is a LOUD classified error (`DriverError::SslRefused` for an
  explicit require, or a `DriverError::Config` naming the `Prefer`/`Disable`
  opt-out for the defaulted-remote case), never a silent plaintext connect an
  on-path attacker could have forced. This closes the last silent-downgrade class
  (the former blanket-`Prefer` default). An EXPLICIT `SslMode` always wins,
  unchanged; the classification is SYNTACTIC on the configured host (no DNS — a
  resolver round trip would be slow and a TOCTOU hole). The rule lives in one
  method both drivers resolve through, exactly as `resolve_endpoint` centralizes
  the unix-vs-TCP rule, so it cannot drift between them. `SslMode` is
  deliberately NOT `Default` (there is no single default to return), and the
  config stores the mode as a private `Option<SslMode>` (`None` = defaulted),
  niche-packed to the same 1 byte — the `ConnectConfig` footprint is unchanged.
- Custom CA roots: `ConnectConfig::with_ca_roots(pem)` (or the `sslrootcert=<path>`
  DSN key / `PGSSLROOTCERT` env) verify against an internal/private CA, making
  `SslMode::Require` usable there instead of forcing plaintext. Stored raw, parsed
  into a rustls root store at connect; a bad/empty PEM is a fail-closed
  `DriverError::Config`, never a fallback to the baked roots or plaintext.
- **TLS session resumption is TRANSPARENT and ON — a faster reconnect handshake
  at ZERO API cost, ZERO channel-binding compromise.** rustls's session-resumption
  store lives INSIDE the `ClientConfig`, so sharing the `Arc<ClientConfig>` across
  connections accumulates session tickets and a RECONNECT resumes an abbreviated
  handshake (a pool warmup / reaped-connection replacement / connection-churn win).
  The DEFAULT-roots config was ALREADY shared (the `tls::shared_client_config`
  `OnceLock`), so it already resumed; the CUSTOM-CA path used to build a FRESH
  config per connection (empty store ⇒ never resumed). It now shares too:
  `tls::shared_client_config_with_ca_roots(pem)` caches one `Arc<ClientConfig>` per
  DISTINCT CA-roots PEM in a bounded (≤8), `Mutex`-guarded, poison-recovering
  process-wide table both drivers' `build_tcp_wire` resolve through (the custom-CA
  peer of the default-roots `OnceLock`). Keyed on the EXACT PEM bytes, which is
  both NECESSARY and SUFFICIENT: rustls permits sharing a resumption store only
  between configs with the SAME certificate verifier (a ticket from server A's CA
  must never resume under server B's), and a custom-CA config is a pure function of
  its roots (provider / versions / no-client-auth constant; `SslMode` and
  `webpki-roots` do not enter it), so different PEM ⇒ different key ⇒ different,
  isolated store, and rustls additionally keys its own store by server name.
  **Channel binding is UNTOUCHED on a resumed session** (the load-bearing safety
  property): on an abbreviated handshake the server sends NO Certificate message,
  but RFC 5929 §4 requires the `tls-server-end-point` binding to use the ORIGINAL
  full-handshake cert — and rustls RESTORES that certificate on resumption (both
  TLS 1.2 session-id and TLS 1.3 PSK paths set `peer_certificates` from the stored
  session's `server_cert_chain`), so `Wire::peer_end_entity_cert` — and hence the
  SCRAM `-PLUS` binding hash — resolves to the SAME cert a full handshake would.
  So a resumed `channel_binding=Require` `-PLUS` auth still binds to the right
  certificate (case 1: rustls exposes the original cert; there is NO fail-safe
  disable-on-resume because none is needed). No new `ConnectConfig` field (the 152 B
  pin is untouched — the cache is internal, not config); no new public API (the
  cache is transparent); `deps_pin` unchanged (rustls already resolved); the
  `next_event` hot path is byte-identical (TLS is the connect phase). Sharing the
  config REDUCES per-connection memory (the config is shared, not rebuilt).
  Witnessed hermetically by `bsql-postgres-core`'s `tls_resumption` test (a shared
  config RESUMES — read off the loopback server's `handshake_kind` — and the resumed
  session exposes the ORIGINAL cert so the `tls-server-end-point` hash is identical;
  plus a negative control proving SEPARATE configs do NOT resume) and live against
  real PG by the `--ignored` `channel_binding_plus`
  `scram_sha_256_plus_survives_tls_session_resumption` (a resumed
  `channel_binding=Require` `-PLUS` reconnect STILL authenticates as the SCRAM role
  with the correct cert hash — a broken binding on resume would fail closed — plus
  a full-vs-resumed connect-latency measurement).
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
  PER-CONNECTION MEMORY (pool sizing): a plaintext connection holds a fixed 4 KiB
  engine read buffer (`bsql_postgres_proto::READ_BUF_CAP`); a TLS connection adds
  the rustls record buffers boxed in `Wire::Tls` — a fixed ~32 KiB inbound
  ciphertext staging buffer (`STAGING_CAP = MAX_CIPHERTEXT_RECORD + RECV_CHUNK` in
  `core::tls`) plus a fixed ~16 KiB encrypt scratch (`TLS_RECORD_SCRATCH`), both
  allocated ONCE per connection, plus rustls's own connection state and two
  transient plaintext/ciphertext vecs each bounded near one 16 KiB TLS record. So
  a TLS connection costs on the order of ~64 KiB of driver-owned buffers vs ~4 KiB
  plaintext — a 100-connection TLS pool ≈ ~6 MiB. Dropping `tls` removes it
  entirely. (The plaintext 4 KiB is pinned by `footprint_baseline`'s
  `per_connection_resident_estimate`.)
- The whole SCRAM-SHA-256 authentication capability is behind the default-on
  `scram` feature (proto → core → drivers → umbrella; proto keeps
  `default = ["scram"]` so the standard `cargo test` runs keep the full SCRAM
  handshake + zeroize/fuzz coverage, and each shipped dependent takes proto with
  `default-features = false` so a consumer can drop it). With `scram` OFF and
  `tls` on, exactly FIVE SCRAM-exclusive crates leave the async runtime graph —
  `sha2`, `hmac`, `pbkdf2`, `base64ct`, `cpufeatures` (41 → 36). `subtle` and
  `getrandom` are NOT SCRAM-exclusive (rustls/ring keep them), so they drop only
  when `tls` is ALSO off; `md-5` rides its own default-on `md5-auth` feature
  (below). With BOTH `tls` and `scram` off the minimal build is 27 runtime crates
  (vs 41 default); dropping `md5-auth` too removes 7 more → 20. FAIL-LOUD
  when off: password auth is SCRAM-only, so a driver given a password with `scram`
  off is a classified `DriverError::Config` at connect naming the missing feature
  (Trust auth still works; a Trust client hitting a SCRAM-demanding server already
  fails with `ConnFail::UnsupportedAuthMethod`) — never a silent auth failure or
  panic. `ConnFail` shrinks 8 B → 2 B when the SCRAM leaf class is gated out,
  cascading to `HandshakeProgress` / `HandshakeOutcome` (feature-conditional
  `wire_pin!` pairs; the engine census counts the cfg-blind source text). The
  `scram`-off fail-loud is witnessed by `bsql-postgres-sync`'s
  `scram_off_fail_loud` test.
- **SCRAM-SHA-256-PLUS channel binding** closes the valid-cert relay/MITM
  residual that full cert+hostname verification alone leaves (a compromised proxy
  or a mis-issued cert for a DIFFERENT name relaying the exchange). Over TLS the
  driver hashes the server's END-ENTITY certificate into the RFC 5929
  `tls-server-end-point` binding data — `resolve_channel_binding` in
  `bsql-postgres-core` centralizes the rule ONE place both drivers thread
  through, exactly like `resolve_endpoint` / `resolve_ssl_mode`. The cert hash is
  chosen from the certificate's OWN `signatureAlgorithm` (a bounded, TOTAL
  ASN.1 walk reading only that OID): SHA-384/512 for the ECDSA/RSA variants
  naming them, and SHA-256 for everything else — the SHA-256 variants, the
  MD5/SHA-1 upgrade RFC 5929 §4.1 mandates, and any unrecognised/unreadable OID
  (which fails SAFE — a loud SCRAM signature mismatch, never a silent downgrade).
  The hash is a direct `sha2` call (no hand-rolled crypto, no X.509 trust
  parsing). The sans-IO engine then combines the binding with the server's
  `AuthenticationSASL` mechanism offer (`channel_binding::decide_sasl_choice`)
  to pick the mechanism + gs2 flag: `SCRAM-SHA-256-PLUS` (`p=tls-server-end-point,,`)
  when the server offers it over a bound channel; plain SCRAM-SHA-256 with `y,,`
  (the RFC 5802 §6 anti-downgrade flag) over TLS WITHOUT `-PLUS`; `n,,` when
  unbound (plaintext / disabled); or a fail-closed `ChannelBindingRequired`
  refusal under `channel_binding=require`. The chosen cbind-input is base64'd
  into the client-final `c=` value, so the binding is cryptographically anchored
  into the SCRAM proof (a MITM's different cert breaks the proof — a classified
  `SignatureMismatch`). The policy is a `ChannelBindingMode` on `ConnectConfig`
  (`Disable`/`Prefer`/`Require`, default `Prefer` — libpq parity; the
  threat-scoped `SslMode` default already encrypts remote endpoints, so channel
  binding is opt-in-strict to avoid breaking legacy PG / poolers without `-PLUS`),
  settable via the builder `channel_binding(..)`, the DSN `channel_binding=` key,
  or `PGCHANNELBINDING`. `Require` is the strict opt-in — a plaintext channel, or
  a server without `-PLUS` (including a downgrade attacker who stripped it), is a
  fail-closed `DriverError::Config` / `ChannelBindingRequired`, never a fallback.
  Both the cert hash and the mechanism/cbind parse are TOTAL-function fuzzed
  (`channel_binding_parse_paths_are_total`). Witnessed OFFLINE by the
  `ScramServer` fake extended to offer `-PLUS` + verify the cbind data
  (`bsql-postgres-proto`'s `engine_connect_spec` `scram_plus_*` /
  `scram_require_*` / `scram_prefer_sends_y_flag_without_plus` — `-PLUS` actually
  SELECTED not plain SCRAM, a binding MISMATCH fails, `require` refuses, `y,,`
  anti-downgrade). The `-PLUS` LIVE witness needs a TLS-enabled SCRAM server (the
  standard local PG has `ssl=off`, so it is not in the DEFAULT offline suite); it
  now lives IN-REPO as the `--ignored` `channel_binding_plus` test
  (`crates/postgres/async/tests/channel_binding_plus.rs`), which stands up its OWN
  ephemeral SSL + SCRAM PostgreSQL (a temp `initdb` cluster + a self-signed
  CA -> leaf chain + `ssl=on` + `password_encryption=scram-sha-256` + a
  `hostssl ... scram-sha-256` login role, torn down on drop like
  `tls_fragmentation`; it SKIPS cleanly if `initdb`/`openssl`/`psql` are absent or
  it runs as root) and asserts, against real PG: `channel_binding=Require`/`Prefer`
  over TLS AUTHENTICATE (`-PLUS` used and the `tls-server-end-point` hash accepted
  by real PostgreSQL — a `Require` client can send only `p=tls-server-end-point,,`,
  and a `-PLUS`-offering server rejects a `y,,` downgrade, so a green auth is a
  proof the `-PLUS` proof carried the right cert hash), and `channel_binding=Require`
  over a plaintext channel fails closed (`DriverError::Config`). This CLOSES the
  gap the note formerly flagged (a prior OUT-OF-BAND ephemeral verification is now
  a committed in-repo witness). Channel binding is entirely `scram`-gated; the
  `next_event` hotpath is unaffected (SCRAM is the connecting phase).
- The whole MD5-password authentication capability
  (`AuthenticationMD5Password`, sub-code 5) is behind the default-on `md5-auth`
  feature (proto → core → drivers → umbrella; each shipped dependent takes proto
  with `default-features = false` so a consumer can drop it). With `md5-auth` OFF,
  exactly SEVEN crates leave a `--no-default-features` runtime graph — `md-5` and
  its private `digest` / `block-buffer` / `generic-array` / `typenum` /
  `crypto-common` / `cfg-if` stack (none shared with the rest once SCRAM is also
  off). FAIL-LOUD when off: `Credentials::Md5Password` is feature-gated (a client
  cannot opt into MD5), and a server that DEMANDS MD5 is answered by the
  always-present dispatch arms with `ConnFail::UnsupportedAuthMethod` — never a
  panic or a silent auth failure. The unconditional `AuthSubCode::Md5Password`
  wire classification stays (it is only a decode of a server sub-code; the
  fail-loud rides the dispatch, not the wire enum). Witnessed by
  `bsql-postgres-proto`'s `md5_off_fail_loud` test
  (`cargo test -p bsql-postgres-proto --no-default-features --test md5_off_fail_loud`).
  The drivers themselves only ever build `Credentials::ScramPassword` / `Trust`,
  so `md5-auth` gating leaves their code path untouched — MD5 is a proto-engine
  capability, exercised by proto's own connect specs.
