//! Allocation gates for the eager result-materialisation path: the per-call
//! `query_sql()` prebuffer and the pool `reset_session()` round-trip.
//!
//! Each window reproduces the WHOLE literal driver path, so the pinned number is
//! the real per-call allocation cost a later unification / collector-pooling
//! slice will drive down (a valid RED→GREEN witness), not a synthetic subset:
//!
//! - **eager `query_sql`** — the driver runs `engine.query(sql, feed)` +
//!   `settle` + `build_query_result(collector, None)`. `build_query_result` is
//!   `collector.finish()` PLUS `Arc::from(column_names.into_boxed_slice())` and
//!   the `QueryResult` construction. The gate reproduces all of it (the
//!   `QueryResult` fields are public), so the Arc-for-column-names allocation the
//!   earlier finish-only measurement omitted is now counted.
//! - **`reset_session`** — the driver runs the exact 7-statement idle `RESET`
//!   batch (`SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; UNLISTEN
//!   *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES`) via
//!   `simple_query`, then returns `into_command_tag()` (rendering the `Copy`
//!   command tag to a `String` ONCE, at the return — the collector stores no tag
//!   `String` per statement). The gate scripts the real reply — six tag-only completions
//!   PLUS the row-returning `pg_advisory_unlock_all` (a `RowDescription` +
//!   `DataRow`, surfacing as a `Deliver` with non-empty oids/names AND a
//!   `Surface::Row`) — and ends with the same trailing `into_command_tag()`.
//!
//! Driven at the CORE level over an in-process scripted transport (no socket,
//! no thread — deterministic). SCOPE HONESTY: this reproduces the whole
//! `Connection`-level round-trip and its materialisation exactly; it does NOT
//! model the `Pool` wrapper around it (the mutex + a pre-sized `Vec` pop/push on
//! acquire), which is socket-bound and not part of the reset round-trip's
//! allocation cost. `build_query_result` is a driver-private fn, so the gate
//! reproduces its body (finish + `Arc::from` + `QueryResult`), not a call to it.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global and counts every thread. `cargo
//! test` runs `#[test]` fns in parallel, so all measured windows live in a
//! SINGLE `#[test]` fn run sequentially.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "alloc-gate harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::Arc;

use bsql_devgates::CountingAllocator;
use bsql_postgres_core::{QueryResult, ResultCollector};
use bsql_postgres_proto::engine::{open_owned, poll_once, Outcome, Transport};
use bsql_postgres_proto::{Credentials, Ident};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// ─────────────────────────── scripted transport ───────────────────────────

struct Script {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Transport for Script {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        let end = self.cursor + n;
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A two-`text`-column `RowDescription`.
fn row_description_2text() -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for name in ["a", "b"] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0_i16.to_be_bytes()); // column attr
        body.extend_from_slice(&25_i32.to_be_bytes()); // text
        body.extend_from_slice(&(-1_i16).to_be_bytes()); // varlena
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
        body.extend_from_slice(&0_i16.to_be_bytes()); // text format
    }
    frame(b'T', &body)
}

/// A two-`text`-column `DataRow`.
fn two_text_row(a: &str, b: &str) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for v in [a, b] {
        let len = i32::try_from(v.len()).expect("value fits i32");
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(v.as_bytes());
    }
    frame(b'D', &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut cc = tag.as_bytes().to_vec();
    cc.push(0);
    frame(b'C', &cc)
}

fn ready_for_query() -> Vec<u8> {
    frame(b'Z', b"I")
}

/// A one-column `RowDescription` for `SELECT pg_advisory_unlock_all()` (a `void`
/// result). The column name and OID drive the `Deliver`'s `oids`/`names`.
fn row_description_void() -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    body.extend_from_slice(b"pg_advisory_unlock_all");
    body.push(0);
    body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
    body.extend_from_slice(&0_i16.to_be_bytes()); // column attr
    body.extend_from_slice(&2278_i32.to_be_bytes()); // void OID
    body.extend_from_slice(&4_i16.to_be_bytes()); // type length
    body.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
    body.extend_from_slice(&0_i16.to_be_bytes()); // text format
    frame(b'T', &body)
}

/// The single `void` `DataRow` `pg_advisory_unlock_all()` returns (an empty,
/// non-null value).
fn void_row() -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    body.extend_from_slice(&0_i32.to_be_bytes()); // length 0 = empty, non-null
    frame(b'D', &body)
}

/// A three-row `SELECT` reply: RowDescription, 3 DataRows, CommandComplete, RFQ.
const QUERY_ROWS: usize = 3;
fn query_reply() -> Vec<u8> {
    let mut out = row_description_2text();
    for i in 0..QUERY_ROWS {
        out.extend_from_slice(&two_text_row("col-a-value", &format!("row{i}")));
    }
    out.extend_from_slice(&command_complete("SELECT 3"));
    out.extend_from_slice(&ready_for_query());
    out
}

/// The LITERAL reply to the drivers' 7-statement idle `reset_session` batch:
/// `SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; UNLISTEN *; SELECT
/// pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES`. Six statements
/// complete with a tag only; the fifth (`SELECT pg_advisory_unlock_all()`)
/// returns a row, so it carries a `RowDescription` + `DataRow` — surfacing as a
/// `Deliver` with non-empty oids/names AND a `Surface::Row` — before its
/// CommandComplete. One trailing RFQ.
fn reset_reply() -> Vec<u8> {
    let mut out = command_complete("SET");
    out.extend_from_slice(&command_complete("RESET"));
    out.extend_from_slice(&command_complete("CLOSE CURSOR"));
    out.extend_from_slice(&command_complete("UNLISTEN"));
    // SELECT pg_advisory_unlock_all() — the row-returning statement.
    out.extend_from_slice(&row_description_void());
    out.extend_from_slice(&void_row());
    out.extend_from_slice(&command_complete("SELECT 1"));
    out.extend_from_slice(&command_complete("DISCARD TEMP"));
    out.extend_from_slice(&command_complete("DISCARD SEQUENCES"));
    out.extend_from_slice(&ready_for_query());
    out
}

fn no_op_sink<'s>(
    collector: &'s mut ResultCollector,
) -> impl FnMut(bsql_postgres_proto::engine::Surface<'_>) -> ControlFlow<bsql_postgres_proto::engine::Never>
       + 's {
    move |s| {
        collector.feed(s);
        ControlFlow::Continue(())
    }
}

// ─────────────────────────── the gate ───────────────────────────

/// PINNED baseline: allocations charged to the WHOLE warm eager `query_sql()`
/// path — fresh `ResultCollector`, `feed` over a 3-row result, then the full
/// `build_query_result` finalization (`finish` into the lazy [`RowSet`] PLUS the
/// `Arc::from(column_names.into_boxed_slice())` and `QueryResult` construction).
/// Currently **19**: the collector's per-call vectors + the arena `finish` (the
/// ONE shared `Arc`) + the column-names `Arc` allocation. The command tag now
/// rides the result as a `Copy` `CommandTag` (no `String`), so this path no
/// longer allocates a tag string a dynamic caller usually never reads.
///
/// History: **21 → 20** when `finish` stopped eagerly building a `Vec<Row>` of N
/// handles and now seals into a [`RowSet`] (one `Arc`, handles minted lazily on
/// access) — the `16·N`-byte handle `Vec` is no longer allocated at all, and a
/// single-row read clones the `Arc` once instead of N times. **20 → 19** when
/// `QueryResult.command_tag` became the `Copy` `CommandTag` instead of a heap
/// `String`, deleting the per-result `t.to_string()` (and adding an `affected()`
/// accessor). **19 → 18** when the `ResultCollector` stopped storing the
/// result-column OID `Vec<u32>`: those OIDs are read at EXACTLY ONE cold site
/// (`prepare_with_oids`, which now captures them into its own Vec in its pump
/// closure), so every dynamic row-returning verb no longer pays a heap
/// `Vec<u32>` per `Deliver` for a value it never read — the last per-`Deliver`
/// metadata allocation the hot dynamic SELECT path charged. The remaining
/// per-call prebuffer cost a later slice (a pooled/reused collector) would trim.
const EAGER_QUERY_ALLOC_PIN: usize = 18;

/// PINNED baseline: allocations charged to the WHOLE warm `reset_session()`
/// round-trip — a fresh `ResultCollector` over the LITERAL 7-statement idle
/// `RESET` reply, ending with the trailing `into_command_tag()` `simple_query`
/// returns. Currently **14**: the collector stores the `Copy` `CommandTag`
/// (no per-statement tag `String`) and no longer stores the result-column OIDs
/// at all, so the ONLY tag allocation is the SINGLE `into_command_tag()` at the
/// end that renders the last tag to a `String` for `simple_query`'s return —
/// plus the row-returning `SELECT pg_advisory_unlock_all()`'s `Surface::Row`
/// `ArenaBuilder`/value push and the ONE `names` allocation its delivery needs.
/// The pool pays this per re-acquire. History: 24 -> 23 when `simple_query`
/// moved the tag out instead of cloning it; 23 -> 21 when the collector began
/// REUSING the `oids` Vec spine (`clear` + `extend_from_slice`) instead of a
/// fresh `to_vec()` per `Deliver` (-2). **21 -> 15** when the command tag became
/// a `Copy` `CommandTag`: the 7 per-statement `t.to_string()` allocations vanish
/// from the feed, leaving one lazy render at the return (-7 feed, +1 render =
/// -6). **15 -> 14** when the collector stopped storing the result-column OIDs
/// entirely (read at ONE cold site — `prepare_with_oids` — not this path): the
/// single `pg_advisory_unlock_all` OID allocation this reset used to charge is
/// gone. (The in-transaction `ROLLBACK`-prefixed variant is one statement longer
/// — the idle path modelled here is the pooled steady state.)
const RESET_ALLOC_PIN: usize = 14;

#[test]
fn eager_query_and_reset_prebuffer_allocs_are_pinned() {
    let user = Ident::try_from_str("mat").expect("valid ident");

    let mut inbound = handshake();
    inbound.extend_from_slice(&query_reply()); // warm-up query
    inbound.extend_from_slice(&query_reply()); // measured query
    inbound.extend_from_slice(&reset_reply()); // warm-up reset
    inbound.extend_from_slice(&reset_reply()); // measured reset

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // ---- warm-up query (UNTIMED): grows engine buffers + first arena. ----
    let live = run_query(&mut engine, live);

    // ---- (1) MEASURED eager query. ----
    let before = ALLOC.snapshot();
    let live = run_query(&mut engine, live);
    let eager_query_allocs = ALLOC.snapshot().delta(before).allocs;

    // ---- warm-up reset (UNTIMED). ----
    let live = run_reset(&mut engine, live);

    // ---- (2) MEASURED reset round-trip. ----
    let before = ALLOC.snapshot();
    let live = run_reset(&mut engine, live);
    let reset_allocs = ALLOC.snapshot().delta(before).allocs;
    let _ = live;

    assert_eq!(
        eager_query_allocs, EAGER_QUERY_ALLOC_PIN,
        "eager query() prebuffer alloc drifted from its pin ({EAGER_QUERY_ALLOC_PIN}): \
         got {eager_query_allocs}. The ResultCollector is rebuilt from empty each call; \
         this is the honest per-call cost. If a change alters it (e.g. a pooled \
         collector), update EAGER_QUERY_ALLOC_PIN with the new reviewed number."
    );
    assert_eq!(
        reset_allocs, RESET_ALLOC_PIN,
        "reset_session round-trip alloc drifted from its pin ({RESET_ALLOC_PIN}): \
         got {reset_allocs}. This is the literal per-re-acquire reset cost (7 \
         command-tag strings — the last MOVED out, not cloned — plus the \
         pg_advisory_unlock_all row's Deliver + Row allocations). Update \
         RESET_ALLOC_PIN if a change alters it."
    );
}

/// Drive one eager `query` exactly as the drivers' `query_sql` does: fresh
/// collector, `feed` every surface, then the WHOLE `build_query_result`
/// finalization — `finish` PLUS the `Arc::from(column_names.into_boxed_slice())`
/// and the `QueryResult` construction. (`build_query_result` is a driver-private
/// fn; its body is reproduced here field-for-field, since the `QueryResult`
/// fields are public.)
fn run_query<'b>(
    engine: &mut bsql_postgres_proto::engine::Engine<'b, Script>,
    live: bsql_postgres_proto::engine::Live<'b>,
) -> bsql_postgres_proto::engine::Live<'b> {
    let mut collector = ResultCollector::new();
    let live = match poll_once(engine.query(live, "SELECT a, b FROM t", no_op_sink(&mut collector))) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("query must complete, got {other:?}"),
    };
    // ── the literal body of Connection::build_query_result(collector, None) ──
    let (rows, command_tag, names) = collector.finish().expect("materialise owned rows");
    assert_eq!(rows.len(), QUERY_ROWS, "all rows sealed into the RowSet");
    let column_names: Arc<[String]> = Arc::from(names.into_boxed_slice());
    let result = QueryResult::new(rows, command_tag, column_names);
    // The result is lazy: no eager `Vec<Row>` was built, yet every row is still
    // reachable (and identical) through the on-demand accessors.
    assert_eq!(result.len(), QUERY_ROWS);
    core::hint::black_box(&result);
    live
}

/// Drive one reset round-trip exactly as the drivers' `reset_session` does: the
/// real 7-statement idle `RESET` batch via `simple_query`, surfaces fed to a
/// fresh collector, ending with the same trailing `into_command_tag()`
/// `simple_query` returns (moved out, even though `reset_session` discards it).
fn run_reset<'b>(
    engine: &mut bsql_postgres_proto::engine::Engine<'b, Script>,
    live: bsql_postgres_proto::engine::Live<'b>,
) -> bsql_postgres_proto::engine::Live<'b> {
    const RESET: &str = "SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; \
         UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
    let mut collector = ResultCollector::new();
    let live = match poll_once(engine.simple_query(live, RESET, no_op_sink(&mut collector))) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("reset must complete, got {other:?}"),
    };
    // simple_query returns `Ok(collector.into_command_tag())` — it renders the
    // `Copy` command tag to a `String` here (the collector stored no tag
    // `String`). reset_session discards it via `?`.
    core::hint::black_box(collector.into_command_tag());
    live
}
