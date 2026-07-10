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
  bsql/              — umbrella facade + query! re-export + #[bsql::test] harness (bsql::pg, ::pg_sync, ::sqlite)  — 947 LoC
  postgres/
    proto/           — sans-IO wire protocol + session engine (no_std + alloc) + PGCOPY binary framing + TypedCopyIn  — 29678 LoC
    core/            — transport-generic driver engine Core<S> + materializer + types + config + TLS + Rows + notify ledger + N+1 detector + SafeIdent guard + cancel key/redial + copy_in_typed + dynamic prepared-statement cache  — 11256 LoC
    async/           — tokio async driver (plugs its socket into the shared Core<S>) + CancelToken  — 2549 LoC
    sync/            — std::net blocking driver (plugs its socket into the shared Core<S>) + CancelToken  — 2480 LoC
  sqlite/
    driver/          — embedded SQLite driver (bundled rusqlite) + typed query! runtime + explicit prepared-statement handles + interrupt CancelToken + N+1 detector  — 3914 LoC
  testkit/           — deterministic in-memory fake PostgreSQL for driver tests (no network)  — 1005 LoC
  build/             — BUILD-DEP: migration DDL → schema catalog (+ SQLite template) + shared $N→?N placeholder authority  — 35036 LoC
  query-macros/      — PROC-MACRO: query! + copy! (types/validates against the catalog; emits the PostgreSQL + SQLite typed bridges) + #[bsql::test] (schema-per-test wrapper)  — 2507 LoC
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
cargo test -p bsql-postgres-async --test sq_live cancel_token_stops -- --ignored   # async cancel witness (needs PG)
cargo test -p bsql-postgres-sync  --test sync_live cancel_token_stops -- --ignored # sync cancel witness (needs PG)
cargo test -p bsql-sqlite --test cancel              # SQLite interrupt witness (in-process, no PG)
cargo test -p bsql-query-sqlite-fixture --features n1-detect --test n1_detect_sqlite  # SQLite N+1 witness (in-process)
cargo test -p bsql-query-fixture --test query_live_async -- --ignored  # live query! (async, needs PG)
cargo test -p bsql-query-fixture --test query_live_sync  -- --ignored  # live query! (sync, needs PG)
cargo test -p bsql-query-fixture --test copy_typed_offline             # copy! macro expansion + row shape (offline)
cargo test -p bsql-query-fixture --test copy_typed_live_async -- --ignored  # live copy_in_typed (async, needs PG)
cargo test -p bsql-query-fixture --test copy_typed_live_sync  -- --ignored  # live copy_in_typed (sync, needs PG)
cargo test -p bsql-postgres-proto --test engine_copy_typed_alloc       # typed binary-COPY constant-memory gate
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
`RustType::UserEnum(UserEnumId)` carries a `Copy` index into the catalog. Composite
types (`CREATE TYPE addr AS (...)`) are a STAGED follow-up (their wire form is the
PG row-type binary format, a new decode path with no native pivot); a composite
column is a loud `UnsupportedPgType` until then, never silently wrong. The
`0014_moods.sql` / `0015_domains.sql` / `0016_alter_type_evolve.sql` migrations in
`tools/query_fixture` and its `query_enum_live` / `query_domain_live` /
`query_alter_type_live` / `query_enum_offline` tests are the end-to-end proof
(decode both twins + nullable + actual NULL; a param round-trip; an unknown label
classified; a renamed-variant compile-error golden; the domain base decode +
server-enforced CHECK; and ADD VALUE / RENAME VALUE / RENAME TO evolution
round-tripping the added / renamed labels).

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
still correct). The SAME detector is a CROSS-BACKEND feature: the SQLite driver
carries its own `n1-detect` feature over the SAME `N1Report` shape / threshold /
window-reset-at-transaction semantics, so `conn.n1_report()` reads identically on
both backends and a consumer relying on the net in tests does not lose it when the
backend is SQLite. SQLite's is SIMPLER than the async PG driver's RPIT reshape —
its verbs are plain blocking `fn`, so `#[track_caller]` works directly (no future
reshape); the tracker is a self-contained COPY (a `bsql-postgres-core` dependency
would drag the whole PG + rustls tree into the embedded crate), so it adds no
external dependency. Witnessed by `tools/query_sqlite_fixture`'s `n1-detect`
feature + `tests/n1_detect_sqlite.rs`.

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
- **Core** (`bsql-postgres-core`): the shared TRANSPORT-GENERIC driver engine `Core<S: Transport>` — it holds the sans-IO engine over a `Wire<S>` plus the linear liveness token and defines every non-I/O verb ONCE — alongside the result materializer, the dynamic `Row` / `QueryResult` types, `ConnectConfig`, TLS config, and the typed `Rows` container (built from an internal `#[doc(hidden)]` `RowsBuilder` prebuffer). Both drivers build on it.
- **Drivers** are thin I/O adapters that plug their socket into the ONE `Core<S>`: `Core<TokioSocket>` on async, `Core<SyncSocket>` on sync (each socket is a TCP-or-unix enum), MONOMORPHISED per driver — static dispatch, no `dyn`. The verbs live once in `Core<S>`; the drivers differ only in `.await` vs blocking, so async/sync parity is a COMPILER guarantee, not hand-maintained twins.
- **Rows.** The dynamic `Row` (from `query_sql` etc.) is 16 bytes (`'static + Clone + Send + Sync`) over an `Arc`-shared arena: 3 heap allocations per whole `QueryResult` (the arena's `data` + `slots` vectors + the shared `Arc`), regardless of row count, and 0 per row — a `QueryResult` holds ONE lazy `RowSet` and mints each `Row` handle on demand (`.get(i)` / `.iter()`), never eagerly building a `Vec<Row>`, so a single-row read (`query_one_sql`) clones the `Arc` once, not N times. This mirrors the typed `Rows<Q>` (from the `query!` flagship), which is 2 allocations per result and 0 per row (borrowed, zero-copy decode). The **SQLite** backend runs the SAME lazy model: its `QueryResult` is one lazy `RowSet` over a shared arena (a `data` byte pool + a `CellSlot` table carrying integer/real inline and text/blob as `(offset, len)`, with the column names shared by `Arc`), so an eager `query_sql()` costs a constant number of allocations, 0 per row (a minted 16-byte `Row` handle carries its own names, so `get_by_name` threads no slice), and text UTF-8 is validated lazily at `get::<&str>` (never eagerly failing the whole result on one bad byte). A `> 4 GiB` eager result is the loud `SqliteError::ResultTooLarge` (stream it via `query_each_sql`, which is capless/constant-memory). The SQLite **typed** flagship result `TypedRows<Q>` wraps that same lazy arena: a constant number of allocations, 0 per row, borrowed records aliasing the arena zero-copy via an `ArenaRowRef` per-get view.
- **SQLite parity.** The SQLite driver is a full peer of the PG path, not a text-only wrapper. **Verb naming matches PG:** `query` / `query_one` / `query_opt` / `query_each` are the compile-checked TYPED flagship (over a `query!` carrier); the dynamic raw-SQL verbs carry the `_sql` suffix (`query_sql` / `query_one_sql` / `query_opt_sql` / `query_each_sql`), and the parameterized dynamic verbs keep their names (`query_params`, `query_params_one/opt`, `query_each_params`). **Typed flagship:** a `query!` carrier for a SQLite-decodable query (every column a SQLite storage class, unbridged, no PG-only dynamic sugar) implements `SqliteTypedQuery` (emitted by the macro under the umbrella `sqlite` feature), so `Connection::query::<Q>()` and its peers (+ the transaction guard) execute it and decode into the SAME typed records the PG path produces. SQLite is dynamically typed, so decoding VERIFIES each value's actual storage class against the record's declared field type via `FromColumn` — a mismatch is a classified `TypeMismatch`, a `NULL` in a non-`Option` field is `UnexpectedNull`, never a silent coercion (the runtime peer of the PG wire-OID pinning). The borrowed record `Q::Record<'q>` aliases the shared arena zero-copy through an `ArenaRowRef` per-get VIEW that lends cells for the CONTAINER lifetime (the memory-proven per-get view, scoped to SQLite); one macro-emitted `decode_row` serves BOTH the eager (`TypedRows<Q>`) and the streaming (`query_each`) paths via the `ColumnSource` seam that `ArenaRowRef` and `BorrowedRow` both implement. A carrier for a PG-only query does NOT implement `SqliteTypedQuery`, so `sqlite_conn.query::<That>()` is a LOCATED E0277 (`#[diagnostic::on_unimplemented]`), never a silent mis-run. The macro emits the SQLite bridge ONLY under `sqlite-runtime` (the umbrella `sqlite` feature), so a PG-only build's expansion is byte-identical and the PG codegen/alloc gates and trybuild goldens are untouched. Runtime emission (`sqlite`) and build-time conformance (`macros-sqlite`) are ORTHOGONAL — a SQLite-targeting consumer should enable BOTH: runtime-only is still fail-loud (a storage-class mismatch is a classified runtime error) but lacks the build-time proof that real SQLite resolves the inferred row shape. Typed `query_one`/`query_opt` are exactly-one/at-most-one (`SqliteError::TooManyRows` on 2+, one extra `sqlite3_step`, no materialization) — the SAME contract as the PG typed verbs, so the flagship reads identically on both backends; the dynamic `*_sql` verbs stay first-row. **Parameters** come in two vocabularies, matching PG. The TYPED flagship verbs take the compile-checked `Q::Params<'p>` tuple — a LIFETIME-GAT (a `text`/`bytea` param is `&'p str`/`&'p [u8]`), so a RUNTIME `String`/buffer binds on the typed path (no `'static` wall; the `'static` instantiation feeds only the const validator/OID pins) — the SAME tuple type the PG `TypedQuery::Params<'p>` uses (the macro emits both from one source), bound onto the statement by the SQLite twin of `ParamsWriter`: the sealed `SqliteBindParams` (tuple) over `SqliteBindValue` (per-leaf → `ValueRef`), which binds each element positionally via rusqlite's zero-alloc `raw_bind_parameter` in its true storage class (`&str`→`TEXT`, `&[u8]`→`BLOB`, `None`→`NULL`). So a `query!` binds the SAME typed parameters on both backends, and a parameter type SQLite cannot bind (a `u64`, or a PG-only `Uuid`/`Numeric`/temporal/`EnumLabel`) is a LOCATED compile error at the `query::<Q>` call (the `SqliteBindParams` bound lives on the verb, not the associated type, so a PG-only-param carrier still gets its `SqliteTypedQuery` impl — only running it on SQLite is refused). The DYNAMIC verbs keep the untyped `&[ValueRef]` slice (the ONE value vocabulary for read AND dynamic bind) as the escape hatch — every value in its TRUE storage class, so `NULL`/`BLOB` are bindable and integers escape the affinity trap. The PG `ParamsWriter` binary-encode path is byte-untouched (the SQLite twin is a SEPARATE trait). **Explicit prepared-statement handles** close the last SQLite parity cell (a real surface gap — the PG driver has `prepare`/`query_prepared`, SQLite had none). `conn.prepare_sql(sql)` returns a DYNAMIC `SqliteStatement<'conn>`, `conn.prepare::<Q>()` a TYPED `SqliteTypedStatement<'conn, Q>` — each holds one PLAIN (non-persistent) `rusqlite::Statement<'conn>` borrowing the connection, so the CONSUMER holds it on the stack beside the connection and calls its verbs (`query`/`query_one`/`query_opt`/`query_each`/`execute` dynamic; `query`/`query_one`/`query_opt`/`query_each` typed) repeatedly. The SQL is compiled ONCE and reused every call, so a hot loop pays NO per-call `sqlite3_prepare_v2` recompile — the fast reuse shape a hand-rolled FFI layer achieves, with NO `unsafe` and NO self-referential hidden cache (the borrow checker keeps the connection alive for the handle, tier-1). This is the THIRD path the transparent verb-level cache cannot take: rusqlite's `prepare_cached` forces `SQLITE_PREPARE_PERSISTENT` (which bypasses lookaside and slows multi-row stepping), and a hidden cache of plain `Statement`s would be self-referential — an explicit handle is neither. The typed handle keeps every guarantee (storage-class verification, exactly-one/at-most-one, the `?N`↔tuple arity guard checked ONCE at prepare); a statement prepared on the connection runs correctly INSIDE a `transaction` closure (same db handle) and honors a `cancel_token` interrupt. Under `n1-detect` the TYPED handle's read verbs record (a typed read repeated 25+ times from one call site is the anti-pattern regardless of pre-preparation); the DYNAMIC handle does not (deliberate reuse, matching the dynamic connection verbs and the PG `query_prepared`). Measured: the reused handle hits the plain-prepare pilot ideal — `by_pk_prepared` ~1.7 µs, `10row_prepared` ~5.2 µs, both BELOW the C reference (2.28 / 5.66 µs), where the per-call-prepare streaming verbs sit at ~4.7 / ~7.4 µs. `Connection::transaction` hands the closure a borrowing `Transaction` guard exposing only the data verbs, so a nested/manual-`commit` desync is E0599 (same design as the PG transaction guard). `open` sets a 5 s default `busy_timeout` (a contended write waits, bounded, then classifies as busy — never a hang; `set_busy_timeout(Duration::ZERO)` restores immediate fail-loud). Affected counts are `u64` (PG parity). The `$N`→`?N` SQLite placeholder rewrite has ONE authority (`bsql_build::sqlite_placeholder_form`), shared by the build-time conformance oracle AND the macro's baked SQLite `const SQL`, so the runtime string is byte-identical to the one build-time validation proved SQLite prepares. **Cross-backend capabilities:** `conn.cancel_token()` (interrupt-based `SqliteCancelToken`) and `conn.n1_report()` (feature `n1-detect`) read the SAME as the PostgreSQL drivers, so cancellation and N+1 detection are one mental model across backends (see the Conventions cancellation bullet and the N+1 paragraph).

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
  `execute_params`) cost a SINGLE round trip whether run once or repeatedly, via a
  transparent per-connection dynamic prepared-statement cache keyed on SQL TEXT
  (`DynStmtCache` in `bsql-postgres-core`). The FIRST sighting of a SQL runs the
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
  re-parse, never a user-visible spurious error), and CLEARED by `reset_session`
  in ONE batched round trip (all cached statements `Close`d + a single `Sync`).
  The clear is primarily HYGIENE, not strict correctness: PostgreSQL re-resolves
  a cached plan's objects when `search_path` changes, so a `search_path`-shifted
  reuse re-plans rather than returning the wrong table. The distinguishing reason
  the DYNAMIC cache clears while the engine's compile-checked (TYPED) cache is
  KEPT is object LIFETIME — a TYPED `query!` is build-validated against PERMANENT
  migration objects that cannot dangle, whereas a DYNAMIC runtime-SQL plan can
  reference a SESSION-scoped object (a `CREATE TEMP TABLE`) that `reset_session`'s
  `DISCARD TEMP` tears down, so keeping it across a checkout would leave the plan
  referencing a dropped object (`DISCARD` excludes `DEALLOCATE ALL` / `DISCARD
  PLANS`, so the typed statements survive). A DIRECT (non-pooled) connection never
  calls `reset_session`, so its dynamic cache persists for the connection's life.
  The typed `query!` path caches in the ENGINE (a content-addressed named
  statement with a Close-before-Parse idempotent MISS path); this is its
  driver-level DYNAMIC peer.
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
  connection.
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
