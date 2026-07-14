//! The representative seed corpus — a set of [`Transcript`] data values
//! covering the engine's request flows. Adding a fixture is adding a value to
//! [`seed`]; no adapter code changes for the data-driven request kinds.
//!
//! Each transcript's `expect` is the observable result PINNED from the real
//! engine: captured by replaying through the engine and recording exactly what
//! it produced, never assumed. The regression asserts `adapter.run(t) ==
//! t.expect` (the pin, which any engine under test must satisfy) and replays
//! each fixture under several transport chunk schedules (the
//! fragmentation-invariance that generalises across engines).
//!
//! The client-wire bytes in `expect` are committed verbatim so a change to the
//! engine's outbound encoding (statement-name scheme, frame layout) is a loud
//! pin failure, not a silent drift. Re-bake only as a reviewed change, reading
//! the new value off the failing regression's assert diff against the engine
//! under test.

use crate::frames;
use crate::observed::{
    ObservedErr, ObservedNotice, ObservedNotify, ObservedOk, ObservedResultSet, ObservedRun,
    ObservedStatus, ObservedTxStatus, ProtocolFailureKind, TerminalErrorKind,
};
use crate::transcript::{ChunkSchedule, ClientRequest, ParamSpec, Setup, Step, Transcript};

/// The backend PID pinned by the canonical trust handshake (`backend_key_data(4321, …)`),
/// surfaced for every `ActiveViaTrustHandshake` transcript.
const TRUST_BACKEND_PID: i32 = 4321;

/// A `Ready` observed run reached via the canonical trust handshake, with the
/// given client bytes and `Ok` body. Defaults: no notices/notifications/params,
/// the trust-handshake backend PID, idle transaction.
fn ready_ok(client_bytes: Vec<u8>, ok: ObservedOk) -> ObservedRun {
    ObservedRun {
        client_bytes,
        outcome: Ok(ok),
        notices: Vec::new(),
        parameter_statuses: Vec::new(),        notifications: Vec::new(),
        backend_pid: Some(TRUST_BACKEND_PID),
        tx_status: ObservedTxStatus::Idle,
        terminal: ObservedStatus::Ready,
    }
}

/// One result set (a single SQL statement's observable result), not suspended.
fn rs(
    command_tag: &str,
    column_names: &[&str],
    type_oids: &[u32],
    rows: Vec<Vec<Option<Vec<u8>>>>,
    affected_rows: Option<u64>,
) -> ObservedResultSet {
    ObservedResultSet {
        command_tag: command_tag.to_string(),
        column_names: column_names.iter().map(|s| (*s).to_string()).collect(),
        type_oids: type_oids.to_vec(),
        rows,
        affected_rows,
        portal_suspended: false,
    }
}

/// An `ObservedOk` with a single result set and no COPY output.
fn ok_one(result_set: ObservedResultSet) -> ObservedOk {
    ObservedOk { result_sets: vec![result_set], copy_out: Vec::new() }
}

/// An `ObservedOk` with an explicit result-set sequence (multi-statement).
fn ok_sets(result_sets: Vec<ObservedResultSet>) -> ObservedOk {
    ObservedOk { result_sets, copy_out: Vec::new() }
}

/// The expected client wire for a `SimpleQuery` (`Q` + len + sql + NUL) — the
/// requests whose client bytes are content-independent enough to assert
/// directly from the wire vocabulary.
fn simple_query_wire(sql: &str) -> Vec<u8> {
    let mut body = Vec::from(sql.as_bytes());
    body.push(0);
    frames::frame(b'Q', &body)
}

/// Owned cell helper for raw row bytes.
fn cell(bytes: &[u8]) -> Option<Vec<u8>> {
    Some(bytes.to_vec())
}

/// A `CommandComplete` whose tag body exceeds the engine read-buffer cap,
/// forcing the oversize stream-and-truncate (Sub-B) path. The tag is a long
/// non-verb string ending in the wire `\0`; it fits within the 8 KiB oversize
/// prefix, so the tag parses (truncated to `CommandTag::Other`'s 32-byte cap)
/// and the command boundary transitions. Body length (4201) is above
/// READ_BUF_CAP (4096) and below the 8 KiB prefix.
fn oversize_command_complete() -> Vec<u8> {
    let mut body = vec![b'X'; 4200];
    body.push(0);
    frames::frame(b'C', &body)
}

/// A simple-query SQL text larger than the engine's bounded outbound frame
/// builder (`MAX_OWNED_SEND_LEN ≈ 2176`), forcing the path that streams the SQL
/// body onto the growable send buffer rather than copying it into the bounded
/// scratch frame. A valid `SELECT 1 AS n` with a trailing line comment padding
/// it well past the cap (~3 KiB) — the outbound bytes must be byte-identical to
/// the whole-frame builder's output. There was previously NO offline test for
/// large SQL; this is the regression's guard for it.
fn large_simple_sql() -> String {
    let mut sql = String::from("SELECT 1 AS n -- ");
    // 380 * 8 = 3040 bytes of comment → frame body well over the ~2176 cap.
    sql.push_str(&"padding ".repeat(380));
    sql
}

/// The column count for the oversize-wide-RowDescription fixture: 300 `int4`
/// columns ≈ 7.2 KiB of RowDescription, comfortably over `READ_BUF_CAP` (4096),
/// so the inbound RowDescription cannot whole-buffer — the new engine gathers it
/// via its Sub-C accumulator and the old engine via its streaming row_desc
/// parser, and they must decode it identically.
const WIDE_COLUMNS: usize = 300;

/// The command tag both engines yield for [`oversize_command_complete`]: the
/// long `X` tag, parsed from the oversize prefix and truncated to
/// `CommandTag::Other`'s 32-byte `BoundedStr` with the `…` overflow marker —
/// 29 bytes of `X` + the 3-byte UTF-8 `…` = 32 bytes. Both engines share the
/// tag parser and the bounded string, so they agree on this byte-for-byte.
fn oversize_cc_tag() -> String {
    let mut tag = "X".repeat(29);
    tag.push('…');
    tag
}

/// The representative seed corpus. Expected values are the pinned current-
/// engine observations (see module docs).
#[must_use]
#[allow(
    clippy::too_many_lines,
    reason = "a corpus is a flat list of data fixtures; splitting it would scatter related transcripts without aiding clarity"
)]
#[allow(
    clippy::vec_init_then_push,
    reason = "each fixture is a large multi-line block under a numbered section comment; sequential `out.push(..)` keeps each transcript with its heading far more readable than one giant `vec![..]` literal"
)]
pub fn seed() -> Vec<Transcript> {
    let mut out = Vec::new();

    // ── 1. simple_query SELECT returning rows (also: row_stream iter_rows) ──
    out.push(Transcript {
        name: "simple_query_select_rows",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT n, v FROM t".to_string()),
            frames::concat(&[
                frames::row_description(&[("n", frames::OID_INT4), ("v", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"1"), Some(b"alpha")]),
                frames::data_row(&[Some(b"2"), None]),
                frames::command_complete("SELECT 2"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("SELECT n, v FROM t"),
            ok_one(rs(
                "SELECT 2",
                &["n", "v"],
                &[23, 25],
                vec![vec![cell(b"1"), cell(b"alpha")], vec![cell(b"2"), None]],
                Some(2),
            )),
        ),
    });

    // ── 2. simple_query SELECT with zero rows ──
    out.push(Transcript {
        name: "simple_query_zero_rows",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT n FROM t WHERE false".to_string()),
            frames::concat(&[
                frames::row_description(&[("n", frames::OID_INT4)]),
                frames::command_complete("SELECT 0"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("SELECT n FROM t WHERE false"),
            ok_one(rs("SELECT 0", &["n"], &[23], Vec::new(), Some(0))),
        ),
    });

    // ── 3. simple_query command (DDL) — no rows, command tag ──
    out.push(Transcript {
        name: "simple_query_command_tag",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("CREATE TABLE t (n int)".to_string()),
            frames::concat(&[
                frames::command_complete("CREATE TABLE"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("CREATE TABLE t (n int)"),
            ok_one(rs("CREATE TABLE", &[], &[], Vec::new(), None)),
        ),
    });

    // ── 4. parse + describe + bind_execute (SELECT, one row) ──
    out.push(Transcript {
        name: "prepare_describe_bind_select",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::Prepare("SELECT id, name FROM users WHERE id = $1".to_string()),
                frames::concat(&[frames::parse_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
            Step::new(
                ClientRequest::DescribeStatement,
                frames::concat(&[
                    frames::parameter_description(&[frames::OID_INT4]),
                    frames::row_description(&[("id", frames::OID_INT4), ("name", frames::OID_TEXT)]),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::BindExecute(ParamSpec::I32(7)),
                frames::concat(&[
                    frames::bind_complete(),
                    frames::data_row(&[Some(b"7"), Some(b"alice")]),
                    frames::command_complete("SELECT 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            vec![
                80, 0, 0, 0, 55, 95, 98, 115, 113, 108, 95, 48, 0, 83, 69, 76, 69, 67, 84, 32, 105,
                100, 44, 32, 110, 97, 109, 101, 32, 70, 82, 79, 77, 32, 117, 115, 101, 114, 115, 32,
                87, 72, 69, 82, 69, 32, 105, 100, 32, 61, 32, 36, 49, 0, 0, 0, 83, 0, 0, 0, 4, 68,
                0, 0, 0, 13, 83, 95, 98, 115, 113, 108, 95, 48, 0, 83, 0, 0, 0, 4, 66, 0, 0, 0, 29,
                0, 95, 98, 115, 113, 108, 95, 48, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0,
                69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83, 0, 0, 0, 4,
            ],
            ok_one(rs(
                "SELECT 1",
                &[],
                &[23, 25],
                vec![vec![cell(b"7"), cell(b"alice")]],
                Some(1),
            )),
        ),
    });

    // ── 5. parse + describe with NoData (statement returns no rows) ──
    out.push(Transcript {
        name: "parse_describe_nodata",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::Prepare("INSERT INTO users (name) VALUES ($1)".to_string()),
                frames::concat(&[frames::parse_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
            Step::new(
                ClientRequest::DescribeStatement,
                frames::concat(&[
                    frames::parameter_description(&[frames::OID_TEXT]),
                    frames::no_data(),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            vec![
                80, 0, 0, 0, 51, 95, 98, 115, 113, 108, 95, 48, 0, 73, 78, 83, 69, 82, 84, 32, 73,
                78, 84, 79, 32, 117, 115, 101, 114, 115, 32, 40, 110, 97, 109, 101, 41, 32, 86, 65,
                76, 85, 69, 83, 32, 40, 36, 49, 41, 0, 0, 0, 83, 0, 0, 0, 4, 68, 0, 0, 0, 13, 83,
                95, 98, 115, 113, 108, 95, 48, 0, 83, 0, 0, 0, 4,
            ],
            ok_one(rs("", &[], &[], Vec::new(), None)),
        ),
    });

    // ── 6. prepare + bind_execute DML (no rows, affected count) ──
    out.push(Transcript {
        name: "prepare_bind_dml",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::Prepare("INSERT INTO users (name) VALUES ($1)".to_string()),
                frames::concat(&[frames::parse_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
            Step::new(
                ClientRequest::DescribeStatement,
                frames::concat(&[
                    frames::parameter_description(&[frames::OID_TEXT]),
                    frames::no_data(),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::BindExecute(ParamSpec::Text("bob".to_string())),
                frames::concat(&[
                    frames::bind_complete(),
                    frames::command_complete("INSERT 0 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            vec![
                80, 0, 0, 0, 51, 95, 98, 115, 113, 108, 95, 48, 0, 73, 78, 83, 69, 82, 84, 32, 73,
                78, 84, 79, 32, 117, 115, 101, 114, 115, 32, 40, 110, 97, 109, 101, 41, 32, 86, 65,
                76, 85, 69, 83, 32, 40, 36, 49, 41, 0, 0, 0, 83, 0, 0, 0, 4, 68, 0, 0, 0, 13, 83,
                95, 98, 115, 113, 108, 95, 48, 0, 83, 0, 0, 0, 4, 66, 0, 0, 0, 28, 0, 95, 98, 115,
                113, 108, 95, 48, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 3, 98, 111, 98, 0, 0, 69, 0, 0, 0,
                9, 0, 0, 0, 0, 0, 83, 0, 0, 0, 4,
            ],
            ok_one(rs("INSERT 0 1", &[], &[], Vec::new(), Some(1))),
        ),
    });

    // ── 7. prepare + describe + close statement ──
    out.push(Transcript {
        name: "prepare_close",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::Prepare("SELECT 1".to_string()),
                frames::concat(&[frames::parse_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
            Step::new(
                ClientRequest::DescribeStatement,
                frames::concat(&[
                    frames::parameter_description(&[]),
                    frames::row_description(&[("?column?", frames::OID_INT4)]),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::CloseStatement,
                frames::concat(&[frames::close_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            vec![
                80, 0, 0, 0, 23, 95, 98, 115, 113, 108, 95, 48, 0, 83, 69, 76, 69, 67, 84, 32, 49,
                0, 0, 0, 83, 0, 0, 0, 4, 68, 0, 0, 0, 13, 83, 95, 98, 115, 113, 108, 95, 48, 0, 83,
                0, 0, 0, 4, 67, 0, 0, 0, 13, 83, 95, 98, 115, 113, 108, 95, 48, 0, 83, 0, 0, 0, 4,
            ],
            ok_one(rs("", &[], &[], Vec::new(), None)),
        ),
    });

    // ── 8. compile-checked query path (binary params, synthetic row desc) ──
    //        First use of the statement on this connection is a cache MISS, so the
    //        client wire leads with a Close(statement) before the Parse — the
    //        Close makes the re-Parse idempotent (Close of a nonexistent statement
    //        is a wire no-op), eliminating the duplicate-statement error for a
    //        name the server may still hold. The server answers CloseComplete
    //        before ParseComplete accordingly.
    out.push(Transcript {
        name: "prepared_macro",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::ExecutePreparedDemo(42),
            frames::concat(&[
                frames::close_complete(),
                frames::parse_complete(),
                frames::bind_complete(),
                // A MISS now appends a Describe(portal), so the server returns a
                // RowDescription; the typed result-schema guard VERIFIES its OIDs
                // ([23, 25] == the seated compile-time schema) then DISCARDS it (the
                // typed path keeps its seated schema + surfaces no runtime names).
                frames::row_description(&[("id", frames::OID_INT4), ("name", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"42"), Some(b"neo")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            // MISS wire: Close(statement) frame (tag 'C'=67, name `bsql_p_…`)
            // FIRST, then the baked Parse (tag 'P'=80), Bind, Describe(portal)
            // (tag 'D'=68, `[68,0,0,0,6,80,0]`), Execute, Sync.
            vec![
                67, 0, 0, 0, 37, 83, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100,
                50, 100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 80, 0, 0,
                0, 100, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100, 50, 100, 57,
                52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 83, 69, 76, 69, 67, 84,
                32, 105, 100, 58, 58, 105, 110, 116, 52, 44, 32, 110, 97, 109, 101, 58, 58, 116,
                101, 120, 116, 32, 70, 82, 79, 77, 32, 100, 101, 109, 111, 32, 87, 72, 69, 82, 69,
                32, 105, 100, 32, 61, 32, 36, 49, 58, 58, 105, 110, 116, 52, 0, 0, 1, 0, 0, 0, 23,
                66, 0, 0, 0, 55, 0, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100,
                50, 100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 0, 1, 0, 1,
                0, 1, 0, 0, 0, 4, 0, 0, 0, 42, 0, 1, 0, 1, 68, 0, 0, 0, 6, 80, 0, 69, 0, 0, 0, 9, 0,
                0, 0, 0, 0, 83, 0, 0, 0, 4,
            ],
            ok_one(rs("SELECT 1", &[], &[23, 25], vec![vec![cell(b"42"), cell(b"neo")]], Some(1))),
        ),
    });

    // ── 8b. compile-checked query path REUSE (second call is a cache HIT) ──
    //        Two `ExecutePreparedDemo` steps on ONE connection: the first is a
    //        cache MISS (Close+Parse+Bind+Execute+Sync), the second a HIT — a bare
    //        Bind+Execute+Sync with NO Close and NO Parse (the server-side plan is
    //        reused). This pins the reuse wire so a regression on the reuse branch
    //        is caught by the differential (and, via the client-bytes mutations,
    //        by the falsifier). The run's observable is the corpus's last-step
    //        projection (the HIT's result set); `client_bytes` accumulates both
    //        steps' wire (miss ++ hit).
    out.push(Transcript {
        name: "prepared_macro_reuse",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            // Step 1 (MISS): CloseComplete, ParseComplete, BindComplete,
            // RowDescription (for the appended Describe(portal) the guard verifies +
            // discards), row, CC, RFQ.
            Step::new(
                ClientRequest::ExecutePreparedDemo(42),
                frames::concat(&[
                    frames::close_complete(),
                    frames::parse_complete(),
                    frames::bind_complete(),
                    frames::row_description(&[("id", frames::OID_INT4), ("name", frames::OID_TEXT)]),
                    frames::data_row(&[Some(b"42"), Some(b"neo")]),
                    frames::command_complete("SELECT 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            // Step 2 (HIT): BindComplete, row, CC, RFQ (no CloseComplete/ParseComplete,
            // and NO Describe/RowDescription — a HIT reuses the plan without a guard).
            Step::new(
                ClientRequest::ExecutePreparedDemo(42),
                frames::concat(&[
                    frames::bind_complete(),
                    frames::data_row(&[Some(b"42"), Some(b"neo")]),
                    frames::command_complete("SELECT 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            // Accumulated wire across both steps: step 1 MISS (Close+Parse+Bind+
            // Execute+Sync) then step 2 HIT (bare Bind+Execute+Sync — the trailing
            // `66 ('B') …` frame carries NO Close 'C' and NO Parse 'P').
            vec![
                67, 0, 0, 0, 37, 83, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100,
                50, 100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 80, 0, 0,
                0, 100, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100, 50, 100, 57,
                52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 83, 69, 76, 69, 67, 84,
                32, 105, 100, 58, 58, 105, 110, 116, 52, 44, 32, 110, 97, 109, 101, 58, 58, 116,
                101, 120, 116, 32, 70, 82, 79, 77, 32, 100, 101, 109, 111, 32, 87, 72, 69, 82, 69,
                32, 105, 100, 32, 61, 32, 36, 49, 58, 58, 105, 110, 116, 52, 0, 0, 1, 0, 0, 0, 23,
                66, 0, 0, 0, 55, 0, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100,
                50, 100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 0, 1, 0, 1,
                0, 1, 0, 0, 0, 4, 0, 0, 0, 42, 0, 1, 0, 1, 68, 0, 0, 0, 6, 80, 0, 69, 0, 0, 0, 9,
                0, 0, 0, 0, 0, 83, 0, 0,
                0, 4, 66, 0, 0, 0, 55, 0, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48,
                100, 50, 100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 0, 1,
                0, 1, 0, 1, 0, 0, 0, 4, 0, 0, 0, 42, 0, 1, 0, 1, 69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83,
                0, 0, 0, 4,
            ],
            ok_one(rs("SELECT 1", &[], &[23, 25], vec![vec![cell(b"42"), cell(b"neo")]], Some(1))),
        ),
    });

    // ── 9. ping ──
    out.push(Transcript {
        name: "ping",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::Ping,
            frames::ready_for_query(frames::TX_IDLE),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(vec![83, 0, 0, 0, 4], ok_one(rs("", &[], &[], Vec::new(), None))),
    });

    // ── 10. multi_statement row-FIRST (SELECT 1; SELECT 2): the current engine
    //        FLATTENS a row-first batch into one result set via the row-stream
    //        pull (the intermediate boundary is swallowed inside iter_rows). The
    //        pin documents that flattening; fixture 18 shows the boundaries the
    //        engine DOES delineate (when the row-bearing statement is last). ──
    out.push(Transcript {
        name: "multi_statement_select",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT 1; SELECT 2".to_string()),
            frames::concat(&[
                frames::row_description(&[("a", frames::OID_INT4)]),
                frames::data_row(&[Some(b"1")]),
                frames::command_complete("SELECT 1"),
                frames::row_description(&[("b", frames::OID_INT4)]),
                frames::data_row(&[Some(b"2")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("SELECT 1; SELECT 2"),
            ok_one(rs(
                "SELECT 1",
                &["b"],
                &[23],
                vec![vec![cell(b"1")], vec![cell(b"2")]],
                Some(1),
            )),
        ),
    });

    // ── 11. NoticeResponse during a query reply (steady state — surfaced) ──
    out.push(Transcript {
        name: "notice_during_query",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("DROP TABLE IF EXISTS ghost".to_string()),
            frames::concat(&[
                frames::notice_response("NOTICE", "00000", "table \"ghost\" does not exist, skipping"),
                frames::command_complete("DROP TABLE"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("DROP TABLE IF EXISTS ghost"),
            outcome: Ok(ok_one(rs("DROP TABLE", &[], &[], Vec::new(), None))),
            notices: vec![ObservedNotice {
                severity: "NOTICE".to_string(),
                sqlstate: "00000".to_string(),
                message: "table \"ghost\" does not exist, skipping".to_string(),
            }],
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 12. NotificationResponse during a query reply ──
    out.push(Transcript {
        name: "notification_during_query",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT 1".to_string()),
            frames::concat(&[
                frames::notification_response(99, "chan", "hello"),
                frames::row_description(&[("a", frames::OID_INT4)]),
                frames::data_row(&[Some(b"1")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SELECT 1"),
            outcome: Ok(ok_one(rs("SELECT 1", &["a"], &[23], vec![vec![cell(b"1")]], Some(1)))),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: vec![ObservedNotify {
                pid: 99,
                channel: "chan".to_string(),
                payload: b"hello".to_vec(),
            }],
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 13. server ErrorResponse (recoverable) on a simple query ──
    out.push(Transcript {
        name: "server_error_recovers",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELCT 1".to_string()),
            frames::concat(&[
                frames::error_response("ERROR", "42601", "syntax error at or near \"SELCT\""),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SELCT 1"),
            outcome: Err(ObservedErr::Server {
                sqlstate: "42601".to_string(),
                severity: Some("ERROR".to_string()),
                message: "syntax error at or near \"SELCT\"".to_string(),
                detail: None,
                hint: None,
                position: None,
                schema: None,
                table: None,
                column: None,
                constraint: None,
            }),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 14. terminate (no reply; socket closed) ──
    out.push(Transcript {
        name: "terminate",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(ClientRequest::Terminate, Vec::new())],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: vec![88, 0, 0, 0, 4],
            outcome: Ok(ObservedOk::default()),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Closed,
        },
    });

    // ── 15. trust handshake with ParameterStatus frames (StartupScript) ──
    out.push(Transcript {
        name: "startup_with_params",
        setup: Setup::StartupScript {
            server_bytes: frames::concat(&[
                frames::auth_ok(),
                frames::parameter_status("server_version", "17.2"),
                frames::parameter_status("application_name", "corpus_app"),
                frames::backend_key_data(1, 2),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        },
        steps: Vec::new(),
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: vec![
                0, 0, 0, 42, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0,
                99, 108, 105, 101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0, 85, 84,
                70, 56, 0, 0,
            ],
            outcome: Ok(ObservedOk::default()),
            notices: Vec::new(),
            parameter_statuses: vec![
                ("server_version".to_string(), "17.2".to_string()),
                ("application_name".to_string(), "corpus_app".to_string()),
            ],            notifications: Vec::new(),
            backend_pid: Some(1),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 15b. trust handshake ABSORBING NoticeResponses (login-trigger case) ──
    // A NoticeResponse may arrive in ANY handshaking state (PG protocol §55.2.7);
    // a PG17 `login` event trigger's RAISE NOTICE fires on every connection,
    // interleaved through the post-auth batch AND (here) before AuthenticationOk.
    // The engine ABSORBS each notice without advancing the state machine, so the
    // handshake still reaches Ready — and, since the connect phase surfaces no
    // notice sink, the notices are NOT observed (byte-for-byte the same startup
    // wire + captured ParameterStatus as `startup_with_params`).
    out.push(Transcript {
        name: "startup_absorbs_notice",
        setup: Setup::StartupScript {
            server_bytes: frames::concat(&[
                frames::notice_response("NOTICE", "00000", "notice before auth"),
                frames::auth_ok(),
                frames::notice_response("NOTICE", "01000", "login event trigger fired"),
                frames::parameter_status("server_version", "17.2"),
                frames::backend_key_data(1, 2),
                frames::notice_response("NOTICE", "00000", "notice just before ready"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        },
        steps: Vec::new(),
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: vec![
                0, 0, 0, 42, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0,
                99, 108, 105, 101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0, 85, 84,
                70, 56, 0, 0,
            ],
            outcome: Ok(ObservedOk::default()),
            notices: Vec::new(),
            parameter_statuses: vec![("server_version".to_string(), "17.2".to_string())],
            notifications: Vec::new(),
            backend_pid: Some(1),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 16. partial-frame assembly: chunked SELECT under OneBytePerRead ──
    out.push(Transcript {
        name: "partial_one_byte_per_read",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT v FROM t".to_string()),
            frames::concat(&[
                frames::row_description(&[("v", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"chunky")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::OneBytePerRead,
        expect: ready_ok(
            simple_query_wire("SELECT v FROM t"),
            ok_one(rs("SELECT 1", &["v"], &[25], vec![vec![cell(b"chunky")]], Some(1))),
        ),
    });

    // ── 17. same partial-frame fixture under the header/body split schedule ──
    out.push(Transcript {
        name: "partial_split_headers",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT v FROM t".to_string()),
            frames::concat(&[
                frames::row_description(&[("v", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"chunky")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::SplitHeaders,
        expect: ready_ok(
            simple_query_wire("SELECT v FROM t"),
            ok_one(rs("SELECT 1", &["v"], &[25], vec![vec![cell(b"chunky")]], Some(1))),
        ),
    });

    // ── 18. multi_statement DELINEATED (UPDATE; INSERT; SELECT): the row-bearing
    //        statement is LAST, so the engine surfaces each prior statement's tag
    //        as its own intermediate result set (UPDATE 3, INSERT 0 1) before the
    //        final SELECT's rows + tag — the per-statement boundaries are pinned,
    //        so flattening / dropping / reordering a statement is caught. ──
    out.push(Transcript {
        name: "multi_statement_delineated",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery(
                "UPDATE t SET v=1; INSERT INTO t DEFAULT VALUES; SELECT id FROM t".to_string(),
            ),
            frames::concat(&[
                frames::command_complete("UPDATE 3"),
                frames::command_complete("INSERT 0 1"),
                frames::row_description(&[("id", frames::OID_INT4)]),
                frames::data_row(&[Some(b"10")]),
                frames::data_row(&[Some(b"11")]),
                frames::command_complete("SELECT 2"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("UPDATE t SET v=1; INSERT INTO t DEFAULT VALUES; SELECT id FROM t"),
            ok_sets(vec![
                rs("UPDATE 3", &[], &[], Vec::new(), Some(3)),
                rs("INSERT 0 1", &[], &[], Vec::new(), Some(1)),
                rs(
                    "SELECT 2",
                    &["id"],
                    &[23],
                    vec![vec![cell(b"10")], vec![cell(b"11")]],
                    Some(2),
                ),
            ]),
        ),
    });

    // ── 19. empty-string cell vs NULL cell (also: a 2nd NULL fixture). A row
    //        with an empty-but-not-NULL `Some(b"")` cell, a NULL cell, and a
    //        normal cell — `Some(Vec::new())` is DISTINCT from `None`. ──
    out.push(Transcript {
        name: "empty_string_vs_null",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT e, n, v FROM t".to_string()),
            frames::concat(&[
                frames::row_description(&[
                    ("e", frames::OID_TEXT),
                    ("n", frames::OID_TEXT),
                    ("v", frames::OID_TEXT),
                ]),
                frames::data_row(&[Some(b""), None, Some(b"x")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("SELECT e, n, v FROM t"),
            ok_one(rs(
                "SELECT 1",
                &["e", "n", "v"],
                &[25, 25, 25],
                vec![vec![Some(Vec::new()), None, cell(b"x")]],
                Some(1),
            )),
        ),
    });

    // ── 20. RFQ transaction status 'T' (in a transaction block) ──
    out.push(Transcript {
        name: "tx_status_in_transaction",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("BEGIN".to_string()),
            frames::concat(&[
                frames::command_complete("BEGIN"),
                frames::ready_for_query(frames::TX_IN_TX),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("BEGIN"),
            outcome: Ok(ok_one(rs("BEGIN", &[], &[], Vec::new(), None))),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::InTransaction,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 21. RFQ transaction status 'E' (failed transaction). A wire-legal
    //        CommandComplete + RFQ('E') exercises the engine's parking of the
    //        failed-transaction status (the only path that parks tx_status is a
    //        successful command's terminal RFQ; a mid-batch error drains without
    //        parking, so the 'E' parse path is reached with this synthetic but
    //        wire-legal sequence). ──
    out.push(Transcript {
        name: "tx_status_failed",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SAVEPOINT s".to_string()),
            frames::concat(&[
                frames::command_complete("SAVEPOINT"),
                frames::ready_for_query(frames::TX_FAILED),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SAVEPOINT s"),
            outcome: Ok(ok_one(rs("SAVEPOINT", &[], &[], Vec::new(), None))),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Failed,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 22. server error with the full diagnostic field set on the wire. The
    //        engine surfaces detail + hint (and sqlstate/severity/message); the
    //        position/schema/table/column/constraint fields are dropped by the
    //        current engine — pinned as `None` (their absence is the observable). ──
    out.push(Transcript {
        name: "server_error_full_fields",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("INSERT INTO users (id) VALUES (1)".to_string()),
            frames::concat(&[
                frames::error_response_fields(&[
                    (b'S', "ERROR"),
                    (b'C', "23505"),
                    (b'M', "duplicate key value violates unique constraint \"users_pkey\""),
                    (b'D', "Key (id)=(1) already exists."),
                    (b'H', "Use a different id."),
                    (b'P', "13"),
                    (b's', "public"),
                    (b't', "users"),
                    (b'c', "id"),
                    (b'n', "users_pkey"),
                ]),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("INSERT INTO users (id) VALUES (1)"),
            outcome: Err(ObservedErr::Server {
                sqlstate: "23505".to_string(),
                severity: Some("ERROR".to_string()),
                message: "duplicate key value violates unique constraint \"users_pkey\"".to_string(),
                detail: Some("Key (id)=(1) already exists.".to_string()),
                hint: Some("Use a different id.".to_string()),
                position: None,
                schema: None,
                table: None,
                column: None,
                constraint: None,
            }),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 23. ParameterStatus keys beyond the commonly-modeled set: the engine
    //        lends every `ParameterStatus` frame raw, so both keys are surfaced
    //        in `parameter_statuses` in arrival order (no known-key projection). ──
    out.push(Transcript {
        name: "unknown_parameter_status",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SET extra_float_digits = 3".to_string()),
            frames::concat(&[
                frames::parameter_status("standard_conforming_strings", "on"),
                frames::parameter_status("IntervalStyle", "postgres"),
                frames::command_complete("SET"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SET extra_float_digits = 3"),
            outcome: Ok(ok_one(rs("SET", &[], &[], Vec::new(), None))),
            notices: Vec::new(),
            parameter_statuses: vec![
                ("standard_conforming_strings".to_string(), "on".to_string()),
                ("IntervalStyle".to_string(), "postgres".to_string()),
            ],
            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 24. COPY OUT sub-protocol: CopyOutResponse + CopyData* + CopyDone +
    //        CommandComplete. The per-frame copy chunks are surfaced verbatim. ──
    out.push(Transcript {
        name: "copy_out",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("COPY t TO STDOUT".to_string()),
            frames::concat(&[
                frames::copy_out_response(1),
                frames::copy_data(b"row1\n"),
                frames::copy_data(b"row2\n"),
                frames::copy_done(),
                frames::command_complete("COPY 2"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire("COPY t TO STDOUT"),
            ObservedOk {
                result_sets: vec![rs("COPY 2", &[], &[], Vec::new(), Some(2))],
                copy_out: vec![b"row1\n".to_vec(), b"row2\n".to_vec()],
            },
        ),
    });

    // ── 25. EmptyQueryResponse: an empty SQL statement. The engine accepts it
    //        and reports an empty command tag (no error). ──
    out.push(Transcript {
        name: "empty_query",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery(String::new()),
            frames::concat(&[
                frames::empty_query_response(),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire(""),
            ok_one(rs("", &[], &[], Vec::new(), None)),
        ),
    });

    // ── 26. row-limited Execute (max_rows) → PortalSuspended (PG §55.2.7). The
    //        portal pauses at the cap with the rows fetched so far; the final
    //        result set is flagged `portal_suspended`. ──
    out.push(Transcript {
        name: "portal_suspend_row_limited",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::Prepare("SELECT id FROM t".to_string()),
                frames::concat(&[frames::parse_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
            Step::new(
                ClientRequest::DescribeStatement,
                frames::concat(&[
                    frames::parameter_description(&[]),
                    frames::row_description(&[("id", frames::OID_INT4)]),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::BindExecuteRowLimited { params: ParamSpec::None, max_rows: 2 },
                frames::concat(&[
                    frames::bind_complete(),
                    frames::data_row(&[Some(b"10")]),
                    frames::data_row(&[Some(b"11")]),
                    frames::portal_suspended(),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            // Baked from the real engine (recorded as the pinned golden):
            // Parse+Sync, Describe+Sync, Bind+Execute(max_rows=2)+Sync.
            vec![
                80, 0, 0, 0, 31, 95, 98, 115, 113, 108, 95, 48, 0, 83, 69, 76, 69, 67, 84, 32, 105,
                100, 32, 70, 82, 79, 77, 32, 116, 0, 0, 0, 83, 0, 0, 0, 4, 68, 0, 0, 0, 13, 83, 95,
                98, 115, 113, 108, 95, 48, 0, 83, 0, 0, 0, 4, 66, 0, 0, 0, 19, 0, 95, 98, 115, 113,
                108, 95, 48, 0, 0, 0, 0, 0, 0, 0, 69, 0, 0, 0, 9, 0, 0, 0, 0, 2, 83, 0, 0, 0, 4,
            ],
            ObservedOk {
                result_sets: vec![ObservedResultSet {
                    command_tag: String::new(),
                    column_names: Vec::new(),
                    type_oids: vec![23],
                    rows: vec![vec![cell(b"10")], vec![cell(b"11")]],
                    affected_rows: None,
                    portal_suspended: true,
                }],
                copy_out: Vec::new(),
            },
        ),
    });

    // ── 27. >=2 NoticeResponse in one reply (also: a 2nd notices fixture). Two
    //        notices with DISTINCT fields, so dropping/reordering the 2nd is
    //        caught. ──
    out.push(Transcript {
        name: "notices_two",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("VACUUM t".to_string()),
            frames::concat(&[
                frames::notice_response("WARNING", "01000", "first warning"),
                frames::notice_response("NOTICE", "00000", "second notice"),
                frames::command_complete("VACUUM"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("VACUUM t"),
            outcome: Ok(ok_one(rs("VACUUM", &[], &[], Vec::new(), None))),
            notices: vec![
                ObservedNotice {
                    severity: "WARNING".to_string(),
                    sqlstate: "01000".to_string(),
                    message: "first warning".to_string(),
                },
                ObservedNotice {
                    severity: "NOTICE".to_string(),
                    sqlstate: "00000".to_string(),
                    message: "second notice".to_string(),
                },
            ],
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 28. >=2 NotificationResponse in one reply (also: a 2nd notifications
    //        fixture). Two notifications with DISTINCT pid/channel/payload. ──
    out.push(Transcript {
        name: "notifications_two",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT 1".to_string()),
            frames::concat(&[
                frames::notification_response(11, "alpha", "first"),
                frames::notification_response(22, "beta", "second"),
                frames::row_description(&[("a", frames::OID_INT4)]),
                frames::data_row(&[Some(b"1")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SELECT 1"),
            outcome: Ok(ok_one(rs("SELECT 1", &["a"], &[23], vec![vec![cell(b"1")]], Some(1)))),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: vec![
                ObservedNotify { pid: 11, channel: "alpha".to_string(), payload: b"first".to_vec() },
                ObservedNotify { pid: 22, channel: "beta".to_string(), payload: b"second".to_vec() },
            ],
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 29. portal Describe + bare-Execute RESUME after a suspend. A row-limited
    //        Execute suspends (portal open); the open portal is then Described
    //        (RowDescription, NO ParameterDescription) and resumed with a bare
    //        Execute (no Bind → no BindComplete) that fetches the rest to
    //        completion. Exercises the portal-Describe and resume seams. ──
    out.push(Transcript {
        name: "portal_resume_after_suspend",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::Prepare("SELECT id FROM t".to_string()),
                frames::concat(&[frames::parse_complete(), frames::ready_for_query(frames::TX_IDLE)]),
            ),
            Step::new(
                ClientRequest::DescribeStatement,
                frames::concat(&[
                    frames::parameter_description(&[]),
                    frames::row_description(&[("id", frames::OID_INT4)]),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::BindExecuteRowLimited { params: ParamSpec::None, max_rows: 2 },
                frames::concat(&[
                    frames::bind_complete(),
                    frames::data_row(&[Some(b"10")]),
                    frames::data_row(&[Some(b"11")]),
                    frames::portal_suspended(),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::DescribePortal,
                frames::concat(&[
                    frames::row_description(&[("id", frames::OID_INT4)]),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::ResumeExecute,
                frames::concat(&[
                    frames::data_row(&[Some(b"12")]),
                    frames::data_row(&[Some(b"13")]),
                    frames::command_complete("SELECT 2"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            // Baked from the real engine (recorded as the pinned golden):
            // Parse+Sync, Describe+Sync, Bind+Execute(max_rows=2)+Sync,
            // Describe(P)+Sync, Execute(All)+Sync.
            vec![
                80, 0, 0, 0, 31, 95, 98, 115, 113, 108, 95, 48, 0, 83, 69, 76, 69, 67, 84, 32, 105,
                100, 32, 70, 82, 79, 77, 32, 116, 0, 0, 0, 83, 0, 0, 0, 4, 68, 0, 0, 0, 13, 83, 95,
                98, 115, 113, 108, 95, 48, 0, 83, 0, 0, 0, 4, 66, 0, 0, 0, 19, 0, 95, 98, 115, 113,
                108, 95, 48, 0, 0, 0, 0, 0, 0, 0, 69, 0, 0, 0, 9, 0, 0, 0, 0, 2, 83, 0, 0, 0, 4, 68,
                0, 0, 0, 6, 80, 0, 83, 0, 0, 0, 4, 69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83, 0, 0, 0, 4,
            ],
            ok_one(rs(
                "SELECT 2",
                &[],
                &[23],
                vec![vec![cell(b"12")], vec![cell(b"13")]],
                Some(2),
            )),
        ),
    });

    // ── 30. oversize CommandComplete (forces the Sub-B stream-and-truncate
    //        path). The tag is parsed from the frame's OWN 8 KiB prefix and the
    //        command boundary transitions — not a stale tag, not a teardown.
    //        Pinned to OneBytePerRead: the bounded ingest buffer drains between
    //        reads, so the >buffer frame streams. Under AllAtOnce/SplitHeaders
    //        the adapter feeds the whole >buffer chunk before it can drain and
    //        both engines report TransportExhausted — a buffer-feed-model
    //        artifact, not a protocol property, so this fixture is excluded from
    //        the seed schedule-invariance check (see tests/seed.rs). ──
    out.push(Transcript {
        name: "oversize_command_complete",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("VACUUM big".to_string()),
            frames::concat(&[
                oversize_command_complete(),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::OneBytePerRead,
        // Baked from the real engine (recorded as the pinned golden): the
        // oversize tag parses + truncates to the 32-byte `…`-marked tag, the
        // command boundary transitions, and the trailing RFQ recovers to Ready.
        expect: ready_ok(
            simple_query_wire("VACUUM big"),
            ok_one(rs(&oversize_cc_tag(), &[], &[], Vec::new(), None)),
        ),
    });

    // ── 32. large simple-query SQL (> the bounded outbound frame builder) ──
    // The SQL body exceeds the ~2176-byte scratch-frame cap, so the engine must
    // stream it onto the growable send buffer; the regression pins the
    // outbound Q frame byte-for-byte, proving large SQL streams identically.
    out.push(Transcript {
        name: "large_simple_query_sql",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery(large_simple_sql()),
            frames::concat(&[
                frames::row_description(&[("n", frames::OID_INT4)]),
                frames::data_row(&[Some(b"1")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            simple_query_wire(&large_simple_sql()),
            ok_one(rs("SELECT 1", &["n"], &[23], vec![vec![cell(b"1")]], Some(1))),
        ),
    });

    // ── 33. oversize WIDE RowDescription (> READ_BUF_CAP) under chunked reads ──
    // 300 int4 columns (~7.2 KiB RowDescription) driven OneBytePerRead exercises
    // the new engine's Sub-C accumulate AND the partial-read path the async
    // cutover's Pending logic will hit; the old engine's streaming row_desc
    // parser must decode it byte-identically. Pinned to OneBytePerRead and exempt
    // from the schedule-invariance check (an oversize inbound frame is not
    // fragmentation-invariant under the feed-whole-chunk model — see
    // oversize_command_complete).
    {
        let wide_cols: Vec<(String, i32)> = (0..WIDE_COLUMNS)
            .map(|i| (format!("col_{i}"), frames::OID_INT4))
            .collect();
        let wide_col_refs: Vec<(&str, i32)> =
            wide_cols.iter().map(|(name, oid)| (name.as_str(), *oid)).collect();
        let wide_names: Vec<&str> = wide_cols.iter().map(|(name, _)| name.as_str()).collect();
        let wide_oids: Vec<u32> = vec![23u32; WIDE_COLUMNS];
        let wide_cell_vals: Vec<Vec<u8>> =
            (0..WIDE_COLUMNS).map(|i| i.to_string().into_bytes()).collect();
        let wide_cells: Vec<Option<&[u8]>> =
            wide_cell_vals.iter().map(|c| Some(c.as_slice())).collect();
        let wide_row: Vec<Option<Vec<u8>>> =
            wide_cell_vals.iter().map(|c| Some(c.clone())).collect();
        out.push(Transcript {
            name: "oversize_wide_row_description",
            setup: Setup::ActiveViaTrustHandshake,
            steps: vec![Step::new(
                ClientRequest::SimpleQuery("SELECT wide".to_string()),
                frames::concat(&[
                    frames::row_description(&wide_col_refs),
                    frames::data_row(&wide_cells),
                    frames::command_complete("SELECT 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            )],
            chunk_schedule: ChunkSchedule::OneBytePerRead,
            expect: ready_ok(
                simple_query_wire("SELECT wide"),
                ok_one(rs("SELECT 1", &wide_names, &wide_oids, vec![wide_row], Some(1))),
            ),
        });
    }

    // ── 34b. recovery-WINDOW frames: notice + param-status BETWEEN the error
    // and the recovering RFQ must still surface ──
    // The error step's reply interleaves a NoticeResponse + ParameterStatus AFTER
    // the ErrorResponse but BEFORE the recovering ReadyForQuery. The engine must
    // surface them on the recovered run. TEETH: the new engine's verb path drains
    // the recovery window with the CALLER's sink (not a noop), so a verb path that
    // dropped these would diverge from the pinned golden.
    // This is the wire-legal recovery-window interleaving the async cutover's
    // notice/GUC-tracking sink would otherwise miss.
    out.push(Transcript {
        name: "recovery_window_notice_and_param",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::SimpleQuery("SELCT 1".to_string()),
                frames::concat(&[
                    frames::error_response("ERROR", "42601", "syntax error at or near \"SELCT\""),
                    frames::notice_response("WARNING", "01000", "recovery-window notice"),
                    frames::parameter_status("application_name", "recovered_app"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::SimpleQuery("SELECT 1 AS n".to_string()),
                frames::concat(&[
                    frames::row_description(&[("n", frames::OID_INT4)]),
                    frames::data_row(&[Some(b"1")]),
                    frames::command_complete("SELECT 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: frames::concat(&[
                simple_query_wire("SELCT 1"),
                simple_query_wire("SELECT 1 AS n"),
            ]),
            outcome: Ok(ok_one(rs("SELECT 1", &["n"], &[23], vec![vec![cell(b"1")]], Some(1)))),
            // The recovery-window NoticeResponse surfaced (captured during the drain).
            notices: vec![ObservedNotice {
                severity: "WARNING".to_string(),
                sqlstate: "01000".to_string(),
                message: "recovery-window notice".to_string(),
            }],
            // The recovery-window ParameterStatus surfaced + tracked.
            parameter_statuses: vec![("application_name".to_string(), "recovered_app".to_string())],            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // ── 34. error → recover → success on the SAME connection (observable) ──
    // A query that server-errors, THEN a follow-up query that succeeds on the
    // same connection — the model-agnostic observable that the connection
    // recovered. The engine runs BOTH steps (the run's outcome is the last
    // step's success; reaching it requires threading the recovered token past
    // the error), so a regression that consumed the token on a recoverable error
    // would fail to reach step 2 and diverge. The verb surface drives this
    // through the real recovery (Ok(ServerErrored) → continue), so the
    // regression covers the recover path the old `recover`-verb hole hid.
    out.push(Transcript {
        name: "error_then_success_same_connection",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![
            Step::new(
                ClientRequest::SimpleQuery("SELCT 1".to_string()),
                frames::concat(&[
                    frames::error_response("ERROR", "42601", "syntax error at or near \"SELCT\""),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
            Step::new(
                ClientRequest::SimpleQuery("SELECT 1 AS n".to_string()),
                frames::concat(&[
                    frames::row_description(&[("n", frames::OID_INT4)]),
                    frames::data_row(&[Some(b"1")]),
                    frames::command_complete("SELECT 1"),
                    frames::ready_for_query(frames::TX_IDLE),
                ]),
            ),
        ],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            // The accumulated outbound wire: both statements' Q frames in order.
            client_bytes: frames::concat(&[
                simple_query_wire("SELCT 1"),
                simple_query_wire("SELECT 1 AS n"),
            ]),
            // The run's outcome is the LAST step's — the follow-up's success,
            // reachable only because the connection recovered from the error.
            outcome: Ok(ok_one(rs("SELECT 1", &["n"], &[23], vec![vec![cell(b"1")]], Some(1)))),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    out
}

/// The 3 mandatory adversarial fixtures. Each PINS the CURRENT engine's
/// observed behaviour for a wire-legal but unusual sequence — captured by
/// replay, not assumed. A future engine that handles any of these differently
/// fails the pin loudly, surfacing the behavioural change for review.
#[must_use]
#[allow(
    clippy::vec_init_then_push,
    reason = "each adversarial fixture is a large multi-line block under a numbered comment; sequential `out.push(..)` keeps each transcript with its heading more readable than one `vec![..]` literal"
)]
pub fn adversarial() -> Vec<Transcript> {
    let mut out = Vec::new();

    // (1) Duplicate ParameterStatus for one key in a single reply. PINNED: the
    // engine lends BOTH frames raw (it retains no map), so both appear in arrival
    // order ("first" then "second"); the command still completes Ready.
    out.push(Transcript {
        name: "adversarial_dup_parameter_status",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SET application_name = 'x'".to_string()),
            frames::concat(&[
                frames::parameter_status("application_name", "first"),
                frames::parameter_status("application_name", "second"),
                frames::command_complete("SET"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SET application_name = 'x'"),
            outcome: Ok(ok_one(rs("SET", &[], &[], Vec::new(), None))),
            notices: Vec::new(),
            parameter_statuses: vec![
                ("application_name".to_string(), "first".to_string()),
                ("application_name".to_string(), "second".to_string()),
            ],
            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // (2) A SECOND RowDescription before CommandComplete in a SELECT flow.
    // PINNED: the engine treats the re-described stream as a protocol
    // violation — the command fails and the connection goes terminally errored.
    out.push(Transcript {
        name: "adversarial_second_row_description",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::SimpleQuery("SELECT v FROM t".to_string()),
            frames::concat(&[
                frames::row_description(&[("v", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"row1")]),
                frames::row_description(&[("w", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"row2")]),
                frames::command_complete("SELECT 2"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: simple_query_wire("SELECT v FROM t"),
            outcome: Err(ObservedErr::Protocol(ProtocolFailureKind::Unclassified)),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(TRUST_BACKEND_PID),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Errored(TerminalErrorKind::Protocol),
        },
    });

    // (3) NoticeResponse during the authentication/connecting phase. PINNED:
    // per PG protocol §55.2.7 an unsolicited notice may arrive in ANY handshaking
    // state; the connecting engine ABSORBS it without advancing the state machine
    // (never surfaced during connect), so the handshake still reaches Ready and
    // the connection becomes active — a mid-handshake notice cannot tear it down.
    out.push(Transcript {
        name: "adversarial_notice_during_auth",
        setup: Setup::StartupScript {
            server_bytes: frames::concat(&[
                frames::notice_response("WARNING", "01000", "a notice before auth completes"),
                frames::auth_ok(),
                frames::backend_key_data(1, 2),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        },
        steps: Vec::new(),
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: vec![
                0, 0, 0, 42, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0,
                99, 108, 105, 101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0, 85, 84,
                70, 56, 0, 0,
            ],
            outcome: Ok(ObservedOk::default()),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: Some(1),
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Ready,
        },
    });

    // (3b) NegotiateProtocolVersion before Authentication. PINNED: bsql requests
    // exactly protocol 3.0 with NO `_pq_.` options, so a compliant server never
    // sends 'v'; the engine classifies it (ConnFail::ProtocolNegotiationRejected)
    // as a LOUD handshake failure rather than swallowing it — swallowing would
    // mask a real negotiation rejection if bsql ever grows protocol options. This
    // is the genuine HandshakeFailed observable (the corrected notice fixture
    // above now reaches Ready, so a real connect failure is witnessed here).
    out.push(Transcript {
        name: "adversarial_negotiate_protocol_version",
        setup: Setup::StartupScript {
            // 'v' body: Int32(newest supported minor) + Int32(count of options).
            server_bytes: frames::frame(b'v', &[0, 0, 0, 0, 0, 0, 0, 0]),
        },
        steps: Vec::new(),
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ObservedRun {
            client_bytes: vec![
                0, 0, 0, 42, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0,
                99, 108, 105, 101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0, 85, 84,
                70, 56, 0, 0,
            ],
            outcome: Err(ObservedErr::Protocol(ProtocolFailureKind::HandshakeFailed)),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),            notifications: Vec::new(),
            backend_pid: None,
            tx_status: ObservedTxStatus::Idle,
            terminal: ObservedStatus::Errored(TerminalErrorKind::Handshake),
        },
    });

    out
}
