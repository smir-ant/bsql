//! The representative seed corpus — a set of [`Transcript`] data values
//! covering the engine's request flows. Adding a fixture is adding a value to
//! [`seed`]; no adapter code changes for the data-driven request kinds.
//!
//! Each transcript's `expect` is the observable result PINNED from the current
//! engine: captured by replaying through [`crate::SansIoAdapter`] and recording
//! exactly what it produced (the `capture` dev test prints these), never
//! assumed. The corpus test asserts both `adapter.run(t) == t.expect` (the pin,
//! which a future engine must also satisfy) and `sync.run(t) == async.run(t)`
//! (the cross-twin equivalence that generalises to that future engine).
//!
//! The client-wire bytes in `expect` are committed verbatim so a change to the
//! engine's outbound encoding (statement-name scheme, frame layout) is a loud
//! pin failure, not a silent drift. Re-bake with the `capture` dev test only as
//! a reviewed change.

use crate::frames;
use crate::observed::{
    ObservedErr, ObservedNotice, ObservedNotify, ObservedOk, ObservedRun, ObservedStatus,
    ProtocolFailureKind, TerminalErrorKind,
};
use crate::transcript::{ChunkSchedule, ClientRequest, ParamSpec, Setup, Step, Transcript};

/// A `Ready`/`Ok`-terminal observed run with the given client bytes and ok body.
fn ready_ok(client_bytes: Vec<u8>, ok: ObservedOk) -> ObservedRun {
    ObservedRun {
        client_bytes,
        outcome: Ok(ok),
        notices: Vec::new(),
        parameter_statuses: Vec::new(),
        notifications: Vec::new(),
        terminal: ObservedStatus::Ready,
    }
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
            ObservedOk {
                command_tag: "SELECT 2".to_string(),
                column_names: vec!["n".to_string(), "v".to_string()],
                rows: vec![
                    vec![cell(b"1"), cell(b"alpha")],
                    vec![cell(b"2"), None],
                ],
                affected_rows: Some(2),
            },
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
            ObservedOk {
                command_tag: "SELECT 0".to_string(),
                column_names: vec!["n".to_string()],
                rows: Vec::new(),
                affected_rows: Some(0),
            },
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
            ObservedOk {
                command_tag: "CREATE TABLE".to_string(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
            },
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
            ObservedOk {
                command_tag: "SELECT 1".to_string(),
                column_names: Vec::new(),
                rows: vec![vec![cell(b"7"), cell(b"alice")]],
                affected_rows: Some(1),
            },
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
            ObservedOk {
                command_tag: String::new(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
            },
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
            ObservedOk {
                command_tag: "INSERT 0 1".to_string(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: Some(1),
            },
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
            ObservedOk {
                command_tag: String::new(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
            },
        ),
    });

    // ── 8. prepared! macro path (binary params, synthetic row desc) ──
    out.push(Transcript {
        name: "prepared_macro",
        setup: Setup::ActiveViaTrustHandshake,
        steps: vec![Step::new(
            ClientRequest::ExecutePreparedDemo(42),
            frames::concat(&[
                frames::parse_complete(),
                frames::bind_complete(),
                frames::data_row(&[Some(b"42"), Some(b"neo")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        chunk_schedule: ChunkSchedule::AllAtOnce,
        expect: ready_ok(
            vec![
                80, 0, 0, 0, 100, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48, 100, 50,
                100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 83, 69, 76, 69,
                67, 84, 32, 105, 100, 58, 58, 105, 110, 116, 52, 44, 32, 110, 97, 109, 101, 58, 58,
                116, 101, 120, 116, 32, 70, 82, 79, 77, 32, 100, 101, 109, 111, 32, 87, 72, 69, 82,
                69, 32, 105, 100, 32, 61, 32, 36, 49, 58, 58, 105, 110, 116, 52, 0, 0, 1, 0, 0, 0,
                23, 66, 0, 0, 0, 55, 0, 98, 115, 113, 108, 95, 112, 95, 97, 54, 102, 102, 55, 48,
                100, 50, 100, 57, 52, 98, 99, 51, 52, 55, 55, 50, 100, 52, 97, 52, 98, 97, 0, 0, 1,
                0, 1, 0, 1, 0, 0, 0, 4, 0, 0, 0, 42, 0, 1, 0, 1, 69, 0, 0, 0, 9, 0, 0, 0, 0, 0, 83,
                0, 0, 0, 4,
            ],
            ObservedOk {
                command_tag: "SELECT 1".to_string(),
                column_names: Vec::new(),
                rows: vec![vec![cell(b"42"), cell(b"neo")]],
                affected_rows: Some(1),
            },
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
        expect: ready_ok(
            vec![83, 0, 0, 0, 4],
            ObservedOk {
                command_tag: String::new(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
            },
        ),
    });

    // ── 10. multi_statement (row-bearing): SELECT 1; SELECT 2 ──
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
            ObservedOk {
                command_tag: "SELECT 1".to_string(),
                column_names: vec!["b".to_string()],
                rows: vec![vec![cell(b"1")], vec![cell(b"2")]],
                affected_rows: Some(1),
            },
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
            outcome: Ok(ObservedOk {
                command_tag: "DROP TABLE".to_string(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
            }),
            notices: vec![ObservedNotice {
                severity: "NOTICE".to_string(),
                sqlstate: "00000".to_string(),
                message: "table \"ghost\" does not exist, skipping".to_string(),
            }],
            parameter_statuses: Vec::new(),
            notifications: Vec::new(),
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
            outcome: Ok(ObservedOk {
                command_tag: "SELECT 1".to_string(),
                column_names: vec!["a".to_string()],
                rows: vec![vec![cell(b"1")]],
                affected_rows: Some(1),
            }),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),
            notifications: vec![ObservedNotify {
                pid: 99,
                channel: "chan".to_string(),
                payload: b"hello".to_vec(),
            }],
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
            }),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),
            notifications: Vec::new(),
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
            parameter_statuses: Vec::new(),
            notifications: Vec::new(),
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
                0, 0, 0, 21, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0, 0,
            ],
            outcome: Ok(ObservedOk::default()),
            notices: Vec::new(),
            parameter_statuses: vec![
                ("server_version".to_string(), "17.2".to_string()),
                ("application_name".to_string(), "corpus_app".to_string()),
            ],
            notifications: Vec::new(),
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
            ObservedOk {
                command_tag: "SELECT 1".to_string(),
                column_names: vec!["v".to_string()],
                rows: vec![vec![cell(b"chunky")]],
                affected_rows: Some(1),
            },
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
            ObservedOk {
                command_tag: "SELECT 1".to_string(),
                column_names: vec!["v".to_string()],
                rows: vec![vec![cell(b"chunky")]],
                affected_rows: Some(1),
            },
        ),
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

    // (1) Duplicate ParameterStatus for one key in a single reply. PINNED:
    // latest value wins ("second"); the command still completes Ready.
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
            outcome: Ok(ObservedOk {
                command_tag: "SET".to_string(),
                column_names: Vec::new(),
                rows: Vec::new(),
                affected_rows: None,
            }),
            notices: Vec::new(),
            parameter_statuses: vec![("application_name".to_string(), "second".to_string())],
            notifications: Vec::new(),
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
            parameter_statuses: Vec::new(),
            notifications: Vec::new(),
            terminal: ObservedStatus::Errored(TerminalErrorKind::Protocol),
        },
    });

    // (3) NoticeResponse during the authentication/connecting phase. PINNED:
    // the connecting state rejects an unsolicited notice (it is NOT surfaced),
    // so the handshake fails — the connection never becomes active.
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
                0, 0, 0, 21, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0, 0,
            ],
            outcome: Err(ObservedErr::Protocol(ProtocolFailureKind::HandshakeFailed)),
            notices: Vec::new(),
            parameter_statuses: Vec::new(),
            notifications: Vec::new(),
            terminal: ObservedStatus::Errored(TerminalErrorKind::Handshake),
        },
    });

    out
}
