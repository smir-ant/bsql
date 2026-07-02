# Performance & Codegen Measurement Methodology

Playbook for verifying that performance-relevant changes do (or do
not) affect generated code, allocation traffic, and runtime
behaviour. Default `cargo bench` runs under typical developer-
machine load produce sign-flipping noise that swamps the signal
perf work generates; the stable-bench workflow below holds the
measurement variance under the change variance.

The evidence layer has two halves: **DETERMINISTIC gates** (run
under `cargo test`, pin exact baselines as asserted consts, go RED
automatically on drift) and a **STATISTICAL ns layer** (criterion
benches + the asm/bench scripts). The deterministic gates are the
load-bearing witnesses for a later perf slice's RED→GREEN; the ns
layer quantifies the "by how many ns" and feeds `asm-diff`.

## Bench targets

The criterion benches drive the sans-IO engine's hot paths over an
in-process scripted transport (no socket, no thread):

| Target | Package | Covers |
|---|---|---|
| `hot_paths` | `bsql-postgres-proto` | `query_params` cache HIT, per-cell `DataRow` decode, flush drain loop, ingest framing loop |
| `typed_decode` | `bsql-query-fixture` | the `query!` carrier's const-offset all-fixed decode vs per-cell (borrowed / owned) |

```bash
cargo bench -p bsql-postgres-proto --bench hot_paths
cargo bench -p bsql-query-fixture  --bench typed_decode
```

`typed_decode` lives in the fixture crate because that decode is
EMITTED by the `query!` macro against the crate's build catalog, which
the proto crate has no access to.

## Deterministic gates (`cargo test`)

These PIN the current baselines. A later perf slice that changes one
must update its pinned const — a visible, reviewed number, and the
witness for the win. A counting `#[global_allocator]` (from
`bsql-devgates`) measures allocation traffic exactly; source-scans pin
memset locations a counting allocator is blind to; a read-counting
transport pins syscall counts.

| Gate (test binary) | Crate | Pins |
|---|---|---|
| `engine_query_alloc` | proto | warm `query_params` cache HIT = **0** allocs; handshake budget = **1** alloc (one-time connecting-ingest heap escape for a >128 B reply) |
| `engine_read_ramp_spec` | proto | a ~4 KB single-row response drains in **5** reads (the 128→doubling per-command ramp) |
| `engine_flush_alloc` / `engine_ingest_alloc` | proto | steady-state flush / ingest are **0**-alloc; heap escape is one-time |
| `engine_ingest_memset_guard` | proto | the ingest hot path is memset-free (source-scan) |
| `tls_recv_memset_pin` | core | the ONE TLS per-read memset is pinned: `RECV_CHUNK = 16384` in `recv_more`, `read_impl` memset-free |
| `tls_alloc_gate` | core | TLS inbound/outbound per-record alloc is bounded, does not grow per record |
| `materialize_alloc` | core | warm eager `query()` prebuffer = **20** allocs; pool reset round-trip = **7** allocs |

```bash
cargo test --workspace          # runs every gate above
```

## TL;DR (the ns / codegen scripts)

| Question | Tool | Determinism |
|---|---|---|
| Did the codegen change? | `scripts/asm-diff.sh` / `scripts/asm-linked-diff.sh` | 100% — same compiler+flags+source = same ASM |
| Did alloc traffic / reads / memset change? | the deterministic gates above (`cargo test`) | 100% — counted exactly, pinned as consts |
| By how many ns? | `scripts/bench-stable.sh` | Statistical, ±5% noise floor on quiet machine |
| Was the bench machine quiet? | `scripts/bench-cpu-time.sh` | POSIX `getrusage` ratio — not noise-affected |

**Mandatory rule** (the owner's no-vapor rule): any change presented
as performance-relevant MUST be verified through `asm-diff` +
`bench-stable`, in order:

1. **`asm-diff` first** — if codegen is unchanged, no perf claim
   needed. If codegen IS different, proceed to step 2.
2. **`bench-stable` second** — quantify the runtime delta. Reject
   any change that regresses an existing bench beyond the 5% noise
   threshold without explicit justification.

ASM-diff catches codegen drift; bench-stable catches runtime
regressions; together they're the minimum viable pair. If the change
is supposed to remove/add an allocation, a read, or a memset, the
relevant DETERMINISTIC gate above is the proof — its pinned const
moves, and that diff is the reviewed evidence.

**Recommended for substantive perf work** (situational):

3. **`bench-cpu-time`** — if `bench-stable` results look noisier
   than usual, wrap the bench command and check the `cpu/wall`
   ratio. Ratio < 0.8 means the OS was preempting; bench numbers
   are unreliable until the machine quiets.

## Tool 1: `scripts/asm-diff.sh`

Deterministic codegen comparison between current working tree and
a git reference (default `HEAD`).

### Usage

```bash
# Compare current WIP vs HEAD.
scripts/asm-diff.sh <symbol-pattern>

# Compare current WIP vs a specific commit.
scripts/asm-diff.sh <symbol-pattern> <git-ref>
```

`<symbol-pattern>` is a substring of the demangled function name
(e.g. `materialise`, `compute_response_body`, `parse_header`).
Matching is case-sensitive.

### What it does

1. Builds the crate with `--release --emit=asm`.
2. Locates the most-recent `.s` file in `target/release/deps/`.
3. Extracts every function whose mangled name contains the pattern.
4. Strips per-build hashes (`17h<16-hex>E` → `E`,
   `l_anon.<hash>.<n>` → `l_anon.<HASH>.<n>`,
   `Lloh<num>` → `Lloh<N>`) so two snapshots from different
   builds diff cleanly.
5. Repeats the dump at the comparison ref via `git stash` + `git
   checkout` (recovers cleanly on failure — uncommitted changes
   stay safe in the stash).
6. Prints a unified diff of the two normalized dumps.

### Examples

```bash
# Verify your inline annotation actually moved code.
scripts/asm-diff.sh parse_header

# Compare against an older commit to see cumulative codegen
# change since branch point.
scripts/asm-diff.sh query_params main

# Sanity-check a refactor against an earlier commit.
scripts/asm-diff.sh next_event HEAD~3
```

Note: under fat LTO the hot sans-IO functions are fully INLINED into
their callers and do not survive as standalone symbols, so `asm-diff`
(which isolates by symbol) may report an empty/asymmetric match for
them. For those, use `asm-linked-diff.sh` (Tool 3), which diffs the
whole optimized bench binary.

### Reading the output

- **No output / "no codegen change"** — the change is either
  ASM-neutral or hits no symbols matching the pattern. For
  tier-2-by-construct changes (e.g. type-level refactors that
  produce identical machine code), this is the expected signal
  and means "no perf concern".
- **Diff with `+`/`-` instructions** — codegen changed. Read
  carefully:
  - More `bl` / `call` instructions → fewer inlines.
  - More `stp`/`mov` setup at function start → larger stack
    frame (register spills).
  - Branch-table changes (`tbl`, `csel`) → match-arm shape
    changed.
  - Whole new function symbol appearing → previously inlined,
    now standalone.

### When it lies (limitations)

- **Cache effects, branch predictor warmup, frequency scaling**
  are invisible at ASM level — two identical .s files can
  produce different ns/op under different runtime conditions.
  That's why bench-stable is the second layer.
- **Fat LTO is enabled** (`profile.release`: `lto = "fat"`,
  `codegen-units = 1`), so the per-crate `--emit=asm` dump this tool
  reads is PRE-LTO: a hot function inlined into its caller at link
  time has no standalone symbol here. When `asm-diff` finds no symbol,
  reach for `asm-linked-diff.sh` (Tool 3), which disassembles the
  fully-optimized linked bench binary. The release build is
  deterministic (codegen-units=1 + fat LTO), so a no-change rebuild
  diffs to nothing — any non-empty linked diff is a real codegen change.
- **Symbol patterns must be unique enough** — `parse` matches
  `parse_header`, `parse_command_tag`, etc. Use specific
  substrings.

## Tool 2: `scripts/bench-stable.sh`

Runtime-perf measurement with stability-improving conditions.

### Usage

```bash
# Save a baseline before changes.
scripts/bench-stable.sh save <baseline-name> [bench-filter]

# Compare current state against saved baseline.
scripts/bench-stable.sh compare <baseline-name> [bench-filter]

# List saved baselines.
scripts/bench-stable.sh list
```

`<bench-filter>` is a criterion regex (defaults to `""` = all).

### Stability mechanisms

1. **Lower process priority** — `taskpolicy -c utility` on macOS,
   `nice -n 19` on Linux. The bench process becomes background
   workload; the OS scheduler preempts it less aggressively when
   the user is interacting with foreground apps. Not a guarantee
   under high system load — close other heavy apps for best
   results.
2. **Extended measurement** — `--measurement-time 30
   --warm-up-time 10` overrides criterion's 5s/3s defaults. With
   ~6× more samples, the confidence interval tightens
   substantially. Empirically: noise floor drops from ±10% to
   ±2-5% on a quiet developer machine.
3. **Noise threshold** — `--noise-threshold 0.05` tells criterion
   to suppress reports of changes < 5% (those are below the
   reliable detection floor on consumer hardware).
4. **Persistent baselines** — `--save-baseline NAME` stores in
   `target/criterion/<bench>/<name>/`, surviving `cargo clean
   --release` and persisting across commits.

### Workflow examples

```bash
# Before refactor: snapshot current perf.
scripts/bench-stable.sh save before-md5-refactor

# Apply your changes...

# After refactor: compare.
scripts/bench-stable.sh compare before-md5-refactor
# → 1 PASS / N FAIL output. Exit 1 on any regression beyond noise.

# Targeted: only a specific bench group (a criterion regex over the
# bench names, e.g. the cache-HIT round-trip or the decode group).
scripts/bench-stable.sh save before-cleanup "query_params"
scripts/bench-stable.sh compare before-cleanup "query_params"
```

### Reading the output

After bench completion, the summary classifies each bench:

```
============================================================
Bench summary vs baseline 'audit-test-baseline'
============================================================
  unchanged:    N
  improvements: N
  regressions:  N
```

- **All unchanged** — code change is perf-neutral (no
  measurable delta beyond noise). Exit 0.
- **Improvements** — bench got faster. Numbers are below
  baseline's confidence interval. Welcome news.
- **Regressions** — bench got slower beyond noise. **Script
  exits 1.** Investigate before merging.

Per-bench detail is in the criterion output above the summary:

```
parse_header/rfq_header time:   [2.52 ns 2.53 ns 2.54 ns]
                 change: time:   [-0.5% +1.2% +4.0%] (p = 0.50 > 0.05)
                        No change in performance detected.
```

### When it lies (limitations)

- **Bench-noise still exists**, just at a lower level. Sub-5%
  changes are below the detection floor; if you suspect a real
  effect smaller than that, you need a quiet bench environment
  (no GUI, fixed CPU clock, dedicated machine).
- **Cache-line-sensitive changes** — moving a hot field across
  a cache line boundary can produce a 5-15% change that's real
  but only detectable on workloads that exercise the affected
  code path. Single-bench-group filters miss these.
- **First runs** after long idle have warmer caches than
  subsequent runs. The 10s warmup mitigates but doesn't
  eliminate.
- **Apple Silicon CPU power management** — under low load
  cores boost; under high load they throttle. Long benches see
  both regimes. Use a power-connected machine, avoid running on
  battery.

## Tool 3: `scripts/asm-linked-diff.sh`

Post-LTO codegen diff via the fully-optimized LINKED bench binary —
the tool for hot functions that `asm-diff` cannot isolate because fat
LTO inlined them away (see Tool 1's limitations).

### Usage

```bash
# Default: the proto crate's `hot_paths` bench, vs HEAD.
scripts/asm-linked-diff.sh

# A specific bench vs a ref.
scripts/asm-linked-diff.sh hot_paths main

# Target the fixture crate's `typed_decode` bench.
PKG=bsql-query-fixture scripts/asm-linked-diff.sh typed_decode
```

### Mechanism

It builds `cargo bench -p "$PKG" --bench "$BENCH" --no-run` (the
`[[bench]]` binary carries the release/LTO profile), disassembles the
whole binary with `otool -tV`, and diffs a NORMALIZED instruction
stream — the leading address column, `adrp` page immediates, absolute
hex operands, and 6+ hex runs (branch targets / mangled hashes) are
blanked, so only real instruction/register structure remains. Because
the release build is deterministic (`codegen-units = 1` + `lto =
"fat"`), a no-change rebuild diffs to nothing, so any non-empty diff
is a REAL codegen change.

It compares the current tree against a ref by `git stash` + `git
checkout` (recovers on failure — uncommitted changes stay in the
stash), then prints IDENTICAL or the differing normalized lines.

### Reading the output

- **IDENTICAL post-LTO codegen** — the change is ASM-neutral on the
  paths this bench exercises.
- **CODEGEN CHANGED: N differing normalized lines** — followed by the
  differing `+`/`-` instructions. Read them the same way as Tool 1's
  diff (more `bl`/`call` = fewer inlines, etc.).

### Note on allocation / read / memset evidence

There is no `bench-allocs` script: allocation traffic, read-syscall
counts, and memset locations are proven by the DETERMINISTIC gates
(run under `cargo test`; see the "Deterministic gates" section near
the top). Those pin exact baselines as asserted consts and go RED
automatically — strictly stronger than a manual save/compare against a
baseline file, so the old bench-allocs workflow was retired in their
favour.

## Tool 4: `scripts/bench-cpu-time.sh`

Wall-clock-vs-CPU-time confidence indicator via POSIX
`getrusage(2)` (exposed by `/usr/bin/time -p`). Doesn't replace
`bench-stable.sh` — it answers a different question: "**was
the bench machine quiet enough for bench-stable's numbers to be
reliable?**".

### Usage

```bash
# Wrap any command (including cargo bench).
scripts/bench-cpu-time.sh -- cargo bench -p bsql-postgres-proto --bench hot_paths

# Wrap bench-stable.sh save in one call.
scripts/bench-cpu-time.sh stable-wrap before-md5

# Sanity check the wrapper.
scripts/bench-cpu-time.sh check
```

### The signal

```
============================================================
CPU-time stats for wrapped command
============================================================
  real (wall-clock):   2.81 s
  user (on-CPU):       2.78 s
  sys  (on-CPU kern):  0.01 s
  ratio (cpu / wall):  0.993
  verdict:             OK (machine quiet, bench numbers reliable)
```

Verdict tiers (single-threaded bench expectations):

| ratio        | verdict | meaning                                                  |
|--------------|---------|----------------------------------------------------------|
| ≥ 0.95       | OK      | Machine quiet; bench-stable numbers reliable.            |
| 0.80 – 0.95  | WARN    | Minor preemption; numbers usable with elevated noise.    |
| < 0.80       | FAIL    | Heavy interference; rerun on quieter machine.            |

If ratio < 0.95 *during* a save, you've discovered that the
machine state was contaminating measurements. The
`bench-stable.sh` numbers are still recorded, but you now know
the noise floor was higher than nominal — a 5% delta in the
numbers might be 10% in actual signal, or 0%.

### When it lies (limitations)

- **Multi-threaded benches** can exceed ratio 1.0 (sum across
  cores) — verdict tiers are calibrated for single-threaded.
  All current `bsql-postgres-proto` benches are single-threaded.
- **Background-flush amortisation** — a build that triggers
  filesystem flushes near end can spike `sys` time after the
  bench window closed. We measure the whole `cargo bench`
  invocation including build + report generation; the bench
  body's actual ratio may be tighter.

## Combined workflow

### Daily flow (Tool 1 + Tool 2 — minimum viable)

```bash
# Step 1: snapshot perf BEFORE your changes (commit, then save).
git commit -am "WIP: about to refactor"
scripts/bench-stable.sh save before-X

# Step 2: apply changes.
# ... edit ...

# Step 3: deterministic check — did codegen actually change?
scripts/asm-diff.sh <relevant-fn>

# Step 4: if codegen changed, statistical check.
scripts/bench-stable.sh compare before-X
# → exit 0 = no regression, ship it
# → exit 1 = regression, investigate or revert

# Step 5: cleanup (optional — baselines are small).
# Baselines persist in target/criterion/<bench>/before-X/
# until next `cargo clean --release` or manual rm -rf.
```

### Substantive perf work (full stack)

When the change is load-bearing — the headline claim of a
session, a tier-elevation bundle, or anything where "did this
actually win?" is the user-facing question — add the
situational layers:

```bash
# Step 1: snapshot ns baselines + confirm the deterministic gates are
# green BEFORE changes.
git commit -am "WIP: about to refactor"
cargo test --workspace                       # deterministic gates: current baselines
scripts/bench-stable.sh save before-X

# Step 2: apply changes.
# ... edit ...

# Step 3: codegen check.
scripts/asm-diff.sh <relevant-fn>            # or asm-linked-diff.sh for inlined fns

# Step 4: alloc / read / memset check — the deterministic gate that
# owns the claim goes RED, and you update its pinned const.
cargo test --workspace
# → a pin that moved is the reviewed evidence of the win (or regression)

# Step 5: ns/op check, optionally CPU-time-wrapped.
scripts/bench-cpu-time.sh -- scripts/bench-stable.sh compare before-X
# → ratio ≥ 0.95 means bench numbers reliable; exit 1 on regression
```

The substantive flow produces three orthogonal guarantees:

1. **Codegen drift detected** (asm-diff / asm-linked-diff).
2. **Alloc / read / memset accounted for** (the deterministic gate's
   pinned const — moved on purpose, or held).
3. **No regression beyond noise** (bench-stable exit code) on a
   verified-quiet machine (cpu-time ratio).

### Picking the right subset

| Situation                                         | Run                                             |
|---------------------------------------------------|--------------------------------------------------|
| Routine inline-annotation change                  | asm-diff (alone if empty diff)                   |
| Type tier elevation (compile-time only)           | asm-diff (alone if empty diff)                   |
| Non-trivial refactor on existing hot path         | asm-diff + bench-stable                          |
| Refactor claiming an alloc / read / memset change | asm-diff + the owning deterministic gate + bench-stable |
| Headline perf claim (e.g., "−15% on push")        | full stack (asm + gates + bench-stable + cpu-time) |
| Suspect bench numbers are noisy                   | bench-cpu-time wrap                              |

## When to skip these tools

Tools 1+2 (asm-diff + bench-stable) are **mandatory** for
changes that claim a perf impact (positive or negative). They
are **not required** for:

- **Pure docs** changes (no source-code edit).
- **Test-only** changes (`#[cfg(test)]` blocks; tests don't
  ship in release).
- **API-surface** changes that don't alter codegen of existing
  paths (e.g. adding a new `pub fn` that no existing code
  calls).

If unsure, run `asm-diff` — it's deterministic and fast (~5s
build + parse). If the diff is empty, the change is
codegen-neutral and you're done.

The DETERMINISTIC gates run as part of `cargo test --workspace` and
are never optional: they are the always-on floor that catches an
allocation, read, or memset drift automatically. `bench-cpu-time` is
optional and situational — reach for it when bench numbers look
noisier than the noise floor would predict.

## Failure-recovery contract

`asm-diff` uses `git stash` + `git checkout` to compare against
a ref. If the script is killed mid-run (Ctrl+C, SIGTERM,
machine crash), the working tree may be left at the comparison
ref with your changes safely in `git stash list`. Recovery:

```bash
git checkout <your-branch>     # back to original branch
git stash pop                  # restore working-tree changes
```

The script's normal trap-handler does this automatically on
any internal failure — manual recovery is only needed if the
script process itself was killed externally.

## Dirty-tree-on-save warning (bench-stable)

`bench-stable.sh save` prints a loud warning + 5-second pre-bench
delay if the working tree differs from HEAD when invoked. The race it
protects against:

```
user:   bench-stable.sh save baseline-X    # cargo bench takes ~3 min
user:   edits source code in parallel
cargo bench:  rebuilds — picks up the in-progress edits
result: "baseline" reflects edited code, not HEAD
```

A real incident: someone started a background `save` and edited
the macro in parallel; the saved "before" baseline was actually a
"with-changes" snapshot. Manual recovery via `git stash` + delete
polluted baseline + re-save + `git stash pop`.

The warning lets the user catch the mismatch immediately:

```
[bench-stable] ⚠  WARNING: dirty working tree on save
  HEAD:   8493113
  STATE:  working tree differs from HEAD
  ...
  RECOMMENDED if you want a HEAD baseline:
    Ctrl+C now → git stash → re-run → git stash pop
  Continuing in 5 seconds (Ctrl+C to abort)...
```

Press Ctrl+C in the 5-second window if the working tree state
isn't what you wanted to save. If you intentionally want a
working-tree baseline (e.g., snapshot post-change state for
future comparisons against further edits), let it proceed — the
warning is informational, not blocking.

When the working tree IS clean, both scripts print one
confirming line:

```
[bench-stable] working tree CLEAN at 8493113 — baseline will reflect HEAD
```

so the expected state is positively confirmed rather than
silently assumed.
