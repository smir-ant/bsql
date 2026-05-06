# Performance & Codegen Measurement Methodology

Audit-canonical playbook for verifying that performance-relevant
changes do (or do not) affect generated code and runtime
behaviour. Established 2026-05-07 after DEF-236 demonstrated
that default `cargo bench` runs under typical developer-machine
load produce sign-flipping noise that swamps the kind of signal
tier-elevation work generates.

## TL;DR

| Question | Tool | Determinism |
|---|---|---|
| Did the codegen change? | `scripts/asm-diff.sh` | 100% — same compiler+flags+source = same ASM |
| By how many ns? | `scripts/bench-stable.sh` | Statistical, ±5% noise floor on quiet machine |

**Mandatory rule** (also pinned in `reforge.md` measurement section):
any change presented as performance-relevant MUST be verified
through BOTH layers in order:

1. **`asm-diff` first** — if codegen is unchanged, no perf claim
   needed. If codegen IS different, proceed to step 2.
2. **`bench-stable` second** — quantify the runtime delta. Reject
   any change that regresses an existing bench beyond the 5% noise
   threshold without explicit justification.

ASM-diff alone catches drift; bench-stable alone catches
runtime regressions; together they're the complete pair.

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
scripts/asm-diff.sh materialise

# Compare against an older commit to see cumulative codegen
# change since branch point.
scripts/asm-diff.sh feed_bytes main

# Sanity-check a tier-elevation refactor.
scripts/asm-diff.sh compute_push_idle_only HEAD~3
```

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
- **Whole-program optimization (LTO)** is not enabled in the
  default release build; cross-crate inlining at the binary
  level may differ from per-crate ASM dumps. For our sans-IO
  crate this is fine — it's tested in isolation.
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

# Targeted: only a specific bench group.
scripts/bench-stable.sh save before-cleanup "iter_rows"
scripts/bench-stable.sh compare before-cleanup "iter_rows"
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

## Combined workflow

A typical perf-relevant audit follows this template:

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

## When to skip these tools

These tools are **mandatory** for changes that claim a perf
impact (positive or negative). They are **not required** for:

- **Pure docs** changes (no source-code edit).
- **Test-only** changes (`#[cfg(test)]` blocks; tests don't
  ship in release).
- **API-surface** changes that don't alter codegen of existing
  paths (e.g. adding a new `pub fn` that no existing code
  calls).

If unsure, run `asm-diff` — it's deterministic and fast (~5s
build + parse). If the diff is empty, the change is
codegen-neutral and you're done.

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
