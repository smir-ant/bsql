//! Dev-only capture harness: prints the current engine's observed result for
//! each flow so the pinned `expect` values in `corpus.rs` can be baked from
//! REAL observations (never assumed). Run with:
//!   cargo test -p bsql-corpus --test capture -- --ignored --nocapture

#![allow(clippy::panic, reason = "dev capture harness — prints actuals; not production")]

use bsql_corpus::frames;
use bsql_corpus::{
    Adapter, ChunkSchedule, ClientRequest, ParamSpec, SansIoAdapter, Setup, Step, Transcript,
};
use bsql_corpus::observed::ObservedRun;

fn dummy_expect() -> ObservedRun {
    ObservedRun {
        client_bytes: Vec::new(),
        outcome: Ok(Default::default()),
        notices: Vec::new(),
        parameter_statuses: Vec::new(),
        notifications: Vec::new(),
        terminal: bsql_corpus::ObservedStatus::Ready,
    }
}

fn show(name: &'static str, setup: Setup, steps: Vec<Step>, schedule: ChunkSchedule) {
    let t = Transcript { name, setup, steps, chunk_schedule: schedule, expect: dummy_expect() };
    let a = SansIoAdapter::sync().run(&t);
    let b = SansIoAdapter::async_twin().run(&t);
    println!("=== {name} ===");
    println!("twins_agree: {}", a == b);
    println!("client_bytes: {:?}", a.client_bytes);
    println!("outcome: {:?}", a.outcome);
    println!("notices: {:?}", a.notices);
    println!("parameter_statuses: {:?}", a.parameter_statuses);
    println!("notifications: {:?}", a.notifications);
    println!("terminal: {:?}", a.terminal);
}

#[test]
#[ignore = "dev capture harness"]
fn capture_all() {
    // prepare + describe + bind_execute (SELECT)
    show(
        "prepare_describe_bind_select",
        Setup::ActiveViaTrustHandshake,
        vec![
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
        ChunkSchedule::AllAtOnce,
    );

    // parse + describe with NoData (statement returns no rows)
    show(
        "parse_describe_nodata",
        Setup::ActiveViaTrustHandshake,
        vec![
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
        ChunkSchedule::AllAtOnce,
    );

    // bind_execute DML (no rows)
    show(
        "prepare_bind_dml",
        Setup::ActiveViaTrustHandshake,
        vec![
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
        ChunkSchedule::AllAtOnce,
    );

    // close statement
    show(
        "prepare_close",
        Setup::ActiveViaTrustHandshake,
        vec![
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
        ChunkSchedule::AllAtOnce,
    );

    // prepared! macro path
    show(
        "prepared_macro",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::ExecutePreparedDemo(42),
            frames::concat(&[
                frames::parse_complete(),
                frames::bind_complete(),
                frames::data_row(&[Some(b"42"), Some(b"neo")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        ChunkSchedule::AllAtOnce,
    );

    // ping
    show(
        "ping",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::Ping,
            frames::concat(&[frames::ready_for_query(frames::TX_IDLE)]),
        )],
        ChunkSchedule::AllAtOnce,
    );

    // multi-statement (row-bearing): SELECT 1; SELECT 2
    show(
        "multi_statement_select",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
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
        ChunkSchedule::AllAtOnce,
    );

    // NoticeResponse during a query reply (steady state — surfaced)
    show(
        "notice_during_query",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::SimpleQuery("DROP TABLE IF EXISTS ghost".to_string()),
            frames::concat(&[
                frames::notice_response("NOTICE", "00000", "table \"ghost\" does not exist, skipping"),
                frames::command_complete("DROP TABLE"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        ChunkSchedule::AllAtOnce,
    );

    // NotificationResponse delivered during a query reply
    show(
        "notification_during_query",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::SimpleQuery("SELECT 1".to_string()),
            frames::concat(&[
                frames::notification_response(99, "chan", "hello"),
                frames::row_description(&[("a", frames::OID_INT4)]),
                frames::data_row(&[Some(b"1")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        ChunkSchedule::AllAtOnce,
    );

    // server error (recoverable) during a simple query
    show(
        "server_error_recovers",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::SimpleQuery("SELCT 1".to_string()),
            frames::concat(&[
                frames::error_response("ERROR", "42601", "syntax error at or near \"SELCT\""),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        ChunkSchedule::AllAtOnce,
    );

    // terminate
    show(
        "terminate",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(ClientRequest::Terminate, Vec::new())],
        ChunkSchedule::AllAtOnce,
    );

    // handshake with parameter statuses (StartupScript)
    show(
        "startup_with_params",
        Setup::StartupScript {
            server_bytes: frames::concat(&[
                frames::auth_ok(),
                frames::parameter_status("server_version", "17.2"),
                frames::parameter_status("application_name", "corpus_app"),
                frames::backend_key_data(1, 2),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        },
        Vec::new(),
        ChunkSchedule::AllAtOnce,
    );

    // partial-assembly: chunked select under OneBytePerRead
    show(
        "partial_one_byte",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::SimpleQuery("SELECT v FROM t".to_string()),
            frames::concat(&[
                frames::row_description(&[("v", frames::OID_TEXT)]),
                frames::data_row(&[Some(b"chunky")]),
                frames::command_complete("SELECT 1"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        ChunkSchedule::OneBytePerRead,
    );
}
