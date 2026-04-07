# Test Philosophy

bsql has 1,800+ tests across 6 crates. Not all tests are equal — they serve different purposes.

## Test categories

### Behavioral tests (~70%)
Core functionality: query execution, result parsing, transaction semantics, connection lifecycle, streaming, COPY protocol, LISTEN/NOTIFY, singleflight coalescing, read/write split routing.

### Edge case and bad-path tests (~15%)
Malformed input, boundary values, NULL handling, empty results, very large data, concurrent access, connection errors, authentication failures, timeout behavior.

### Property-based tests (proptest, ~3%)
Wire protocol parser, codec roundtrips, URL parser, auth parser — tested with random input to ensure no panics on arbitrary data.

### Trait and format tests (~12%)
Debug/Display formatting, Send/Sync bounds, Clone behavior, Default implementations. These catch regressions that break downstream code — a missing `Send` impl prevents use in `tokio::spawn`, a broken `Display` breaks error logging.

## Why trait tests matter

A test like `assert!(std::mem::size_of::<Pool>() > 0)` or `fn assert_send<T: Send>() {}` looks trivial. But these are compile-time contracts:

- **Send** — required for `tokio::spawn`, `std::thread::spawn`, `crossbeam::scope`
- **Sync** — required for `&Pool` across threads (Arc<Pool>)
- **Debug** — required for `#[derive(Debug)]` on user structs containing bsql types
- **Display** — required for error reporting (`eprintln!("{}", err)`)

If any of these break, user code stops compiling. The tests catch this before release.

## Running tests

```bash
# All unit tests (no database needed)
cargo test --workspace --lib

# Integration tests (requires PostgreSQL)
BSQL_DATABASE_URL="postgres://user:pass@localhost/test_db" cargo test --workspace

# Property-based tests with more iterations
PROPTEST_CASES=10000 cargo test -p bsql-driver-postgres --lib
```
