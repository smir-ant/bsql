//! Dev-only capture for the 3 mandatory adversarial fixtures. Run with:
//!   cargo test -p bsql-corpus --test capture_adversarial -- --ignored --nocapture

#![allow(clippy::panic, reason = "dev capture harness — prints actuals; not production")]

use bsql_corpus::frames;
use bsql_corpus::observed::ObservedRun;
use bsql_corpus::{
    Adapter, ChunkSchedule, ClientRequest, SansIoAdapter, Setup, Step, Transcript,
};

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
fn capture_adversarial() {
    // (1) duplicate ParameterStatus for one key during a query reply.
    show(
        "adversarial_dup_parameter_status",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
            ClientRequest::SimpleQuery("SET application_name = 'x'".to_string()),
            frames::concat(&[
                frames::parameter_status("application_name", "first"),
                frames::parameter_status("application_name", "second"),
                frames::command_complete("SET"),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        )],
        ChunkSchedule::AllAtOnce,
    );

    // (2) a SECOND RowDescription before CommandComplete in a SELECT flow.
    show(
        "adversarial_second_row_description",
        Setup::ActiveViaTrustHandshake,
        vec![Step::new(
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
        ChunkSchedule::AllAtOnce,
    );

    // (3) NoticeResponse during the authentication/connecting phase.
    show(
        "adversarial_notice_during_auth",
        Setup::StartupScript {
            server_bytes: frames::concat(&[
                frames::notice_response("WARNING", "01000", "a notice before auth completes"),
                frames::auth_ok(),
                frames::backend_key_data(1, 2),
                frames::ready_for_query(frames::TX_IDLE),
            ]),
        },
        Vec::new(),
        ChunkSchedule::AllAtOnce,
    );
}
