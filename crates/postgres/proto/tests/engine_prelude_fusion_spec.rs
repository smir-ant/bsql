//! Fused-prelude (deferred `BEGIN`) round-trip-fusion witness.
//!
//! A transaction's `BEGIN` is DEFERRED and pipelined with the first statement the
//! body issues: the driver arms it via
//! [`Engine::defer_command_prelude`](bsql_postgres_proto::engine::Engine::defer_command_prelude)
//! and the engine prepends the `BEGIN` `'Q'` frame ahead of that statement, so a
//! single flush carries BOTH and the pump drains `BEGIN`'s reply BEFORE the
//! statement's. This spec pins the load-bearing properties directly at the wire:
//!
//! 1. **One flush carries both** — after the prelude is armed, running one verb
//!    performs EXACTLY ONE socket write, and that write's bytes are the `BEGIN`
//!    `'Q'` frame IMMEDIATELY FOLLOWED by the statement's frame. This is the
//!    round-trip saving made machine-checkable (the `CountingServer` records each
//!    write's bytes).
//! 2. **The prelude's own results are SWALLOWED** — the caller's sink sees the
//!    STATEMENT's row + delivery, never `BEGIN`'s `CommandComplete` delivery, so
//!    the fused prelude cannot corrupt the statement's materialised result.
//! 3. **The transaction is really open** — `tx_status` after the fused verb is
//!    `InTransaction` (the `BEGIN`'s `ReadyForQuery` was consumed and recorded),
//!    not `Idle`.
//! 4. **An empty body still opens the transaction** — arming the prelude and then
//!    running the terminating `COMMIT` fuses `BEGIN`+`COMMIT` into one flush.
//!
//! The deterministic tests run always (a scripted `CountingServer`, no network);
//! the `#[ignore]` `live_*` test proves the SAME single-write fusion against a real
//! PostgreSQL over a counting blocking socket (localhost:5432, user `smir-ant`, db
//! `postgres`, trust auth — the project's live-test triple).

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test harness — a fixture/verb failure is a loud assertion, the sanctioned test-failure signal"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use bsql_postgres_proto::action::TxStatus;
use bsql_postgres_proto::engine::{poll_once, session, Surface, Transport};
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_COMMAND_COMPLETE, TAG_DATA_ROW,
    TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use bsql_postgres_proto::{Credentials, Ident};

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn concat(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(p);
    }
    out
}

/// A PostgreSQL simple-query (`'Q'`) frame — the exact wire the engine builds for
/// a prelude / a runtime simple query: tag + length(incl self) + SQL + NUL.
fn simple_query_frame(sql: &str) -> Vec<u8> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    frame(b'Q', &body)
}

fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    concat(&[
        frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()),
        frame(TAG_BACKEND_KEY_DATA.byte(), &key),
        frame(TAG_READY_FOR_QUERY.byte(), b"I"),
    ])
}

fn rfq(status: u8) -> Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), &[status])
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = tag.as_bytes().to_vec();
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

fn row_description_one(name: &str, oid: i32) -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i32.to_be_bytes()); // table oid
    body.extend_from_slice(&1_i16.to_be_bytes()); // column idx
    body.extend_from_slice(&oid.to_be_bytes());
    body.extend_from_slice(&(-1_i16).to_be_bytes());
    body.extend_from_slice(&(-1_i32).to_be_bytes());
    body.extend_from_slice(&0_i16.to_be_bytes()); // text format
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

fn data_row_one(cell: &[u8]) -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    body.extend_from_slice(&i32::try_from(cell.len()).expect("len").to_be_bytes());
    body.extend_from_slice(cell);
    frame(TAG_DATA_ROW.byte(), &body)
}

// ─────────────────────────── counting scripted server ───────────────────

/// A write-recording, read-scripted transport: `read` drains a fixed reply;
/// `write` accepts every byte AND records the written slice (so a test can count
/// flushes and inspect the exact wire). `Send` via `Arc<Mutex<…>>`, so the engine
/// verbs' `Send`-bounded futures compose over it. Always-ready (one-poll drive).
struct CountingServer {
    inbound: Vec<u8>,
    cursor: usize,
    /// Each socket write's bytes, in order — shared with the test so it can
    /// inspect them after the (owning) session closure returns.
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Transport for CountingServer {
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
        self.writes.lock().expect("writes lock").push(buf.to_vec());
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Build a scripted counting server sharing its write log with the returned
/// handle. The session's `for<'b>` brand ties `Engine<'b>` to `Live<'b>`, so each
/// test INLINES the `session` call (a helper taking the borrowed pair would break
/// the shared brand — the two `'b`s must be one).
fn counting_server(inbound: Vec<u8>) -> (CountingServer, Arc<Mutex<Vec<Vec<u8>>>>) {
    let writes = Arc::new(Mutex::new(Vec::new()));
    let server = CountingServer {
        inbound,
        cursor: 0,
        writes: Arc::clone(&writes),
    };
    (server, writes)
}

/// A collecting sink recording the rows + delivery command-tags surfaced, so a
/// test can assert the fused prelude's OWN results are swallowed (never surfaced).
#[derive(Default)]
struct Collected {
    rows: Vec<Vec<u8>>,
    tags: Vec<String>,
}

// ─────────────────────────── deterministic witnesses ────────────────────

/// The load-bearing witness: a deferred `BEGIN` fuses into the first statement's
/// flush. One socket write carries `BEGIN` + the statement, the prelude's own
/// delivery is swallowed, and the transaction is really open.
#[test]
fn deferred_begin_fuses_into_first_statement_one_flush() {
    // Handshake, then BEGIN's reply, then SELECT's reply — the order the server
    // produces for the fused `BEGIN` + `SELECT 1` batch.
    let inbound = concat(&[
        handshake(),
        // BEGIN: CommandComplete + ReadyForQuery(InTransaction).
        command_complete("BEGIN"),
        rfq(b'T'),
        // SELECT 1: RowDescription + DataRow + CommandComplete + ReadyForQuery.
        row_description_one("?column?", 23),
        data_row_one(b"1"),
        command_complete("SELECT 1"),
        rfq(b'T'),
    ]);

    let (server, writes) = counting_server(inbound);
    let user = Ident::try_from_str("bsql_fusion").expect("user");
    session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let live = poll_once(engine.connect(live))
            .expect("single poll")
            .expect("handshake");
        // Discard the handshake's startup-packet write: only the body's flushes are
        // the subject of the flush-count assertion.
        writes.lock().expect("writes lock").clear();

        // Arm the deferred BEGIN, then run the FIRST statement.
        engine.defer_command_prelude("BEGIN");
        let mut collected = Collected::default();
        drop(poll_once(engine.query(live, "SELECT 1", |s: Surface<'_>| {
            match s {
                Surface::Row(body) => collected.rows.push(body.to_vec()),
                Surface::Deliver { tag, .. } => {
                    let label = match tag {
                        Some(t) => format!("{t:?}"),
                        None => "<none>".to_owned(),
                    };
                    collected.tags.push(label);
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }))
        .expect("single poll")
        .expect("fused query"));

        // (1) EXACTLY ONE flush carried both frames.
        let log = writes.lock().expect("writes lock");
        assert_eq!(
            log.len(),
            1,
            "expected ONE flush carrying BEGIN + the statement; got {} writes",
            log.len()
        );
        let expected = concat(&[simple_query_frame("BEGIN"), simple_query_frame("SELECT 1")]);
        assert_eq!(
            &log[0], &expected,
            "the single flush must be the BEGIN 'Q' frame immediately followed by the statement's 'Q' frame"
        );
        drop(log);

        // (2) The prelude's OWN delivery is swallowed: the sink saw the SELECT's
        // row + delivery, never BEGIN's CommandComplete delivery (BEGIN produces no
        // rows). The lent `Surface::Row` body is the whole `DataRow` payload
        // (field-count + the framed cell), so it ends with the `1` cell byte.
        assert_eq!(collected.rows.len(), 1, "exactly the SELECT row surfaced");
        assert!(
            collected.rows[0].ends_with(b"1"),
            "the surfaced row is the SELECT's, carrying the `1` cell: {:?}",
            collected.rows[0]
        );
        assert!(
            !collected.tags.iter().any(|t| t.contains("BEGIN") || t.contains("Begin")),
            "BEGIN's delivery must NOT surface to the statement's sink: {:?}",
            collected.tags
        );

        // (3) The transaction is really open (BEGIN's RFQ recorded InTransaction).
        assert_eq!(
            engine.tx_status().expect("active"),
            TxStatus::InTransaction,
            "the fused BEGIN must leave the session in a transaction"
        );
    })
    .expect("session");
}

/// An EMPTY transaction body still opens the transaction: arming BEGIN and then
/// running the terminating `COMMIT` fuses `BEGIN`+`COMMIT` into ONE flush (so no
/// stale prelude survives, and the empty transaction is a real BEGIN;COMMIT).
#[test]
fn empty_body_fuses_begin_into_the_commit() {
    let inbound = concat(&[
        handshake(),
        // BEGIN reply.
        command_complete("BEGIN"),
        rfq(b'T'),
        // COMMIT reply (back to Idle).
        command_complete("COMMIT"),
        rfq(b'I'),
    ]);

    let (server, writes) = counting_server(inbound);
    let user = Ident::try_from_str("bsql_fusion").expect("user");
    session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let live = poll_once(engine.connect(live))
            .expect("single poll")
            .expect("handshake");
        writes.lock().expect("writes lock").clear();

        engine.defer_command_prelude("BEGIN");
        // No statement issued — the terminating COMMIT flushes the pending BEGIN.
        drop(poll_once(engine.query(live, "COMMIT", |_s: Surface<'_>| ControlFlow::Continue(())))
            .expect("single poll")
            .expect("fused commit"));

        let log = writes.lock().expect("writes lock");
        assert_eq!(log.len(), 1, "BEGIN+COMMIT ride ONE flush");
        assert_eq!(
            &log[0],
            &concat(&[simple_query_frame("BEGIN"), simple_query_frame("COMMIT")]),
            "the empty-transaction flush is BEGIN then COMMIT"
        );
        drop(log);
        // COMMIT's RFQ returned the session to Idle.
        assert_eq!(engine.tx_status().expect("active"), TxStatus::Idle);
    })
    .expect("session");
}

/// Without arming a prelude, a statement flushes ONLY itself (the control: the
/// fusion is opt-in, and a normal verb's single-flush wire is unchanged).
#[test]
fn without_a_prelude_a_statement_flushes_only_itself() {
    let inbound = concat(&[
        handshake(),
        row_description_one("?column?", 23),
        data_row_one(b"1"),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);

    let (server, writes) = counting_server(inbound);
    let user = Ident::try_from_str("bsql_fusion").expect("user");
    session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let live = poll_once(engine.connect(live))
            .expect("single poll")
            .expect("handshake");
        writes.lock().expect("writes lock").clear();

        drop(poll_once(engine.query(live, "SELECT 1", |_s: Surface<'_>| ControlFlow::Continue(())))
            .expect("single poll")
            .expect("query"));
        let log = writes.lock().expect("writes lock");
        assert_eq!(log.len(), 1, "one statement, one flush");
        assert_eq!(&log[0], &simple_query_frame("SELECT 1"), "just the statement");
        drop(log);
        assert_eq!(engine.tx_status().expect("active"), TxStatus::Idle);
    })
    .expect("session");
}

/// A stranded prelude (armed but never consumed — the fingerprint of a
/// transaction body that PANICKED before its first statement) is DISCARDED by
/// `clear_command_prelude`, so it cannot fuse into the next verb. Guards the pool
/// checkout path: `reset_session` clears it, so a stranded `BEGIN` never corrupts
/// the reset or the next user's first statement.
#[test]
fn clear_command_prelude_discards_a_stranded_prelude() {
    let inbound = concat(&[
        handshake(),
        // Only the statement's own reply — NO BEGIN reply, because after the clear
        // the statement flushes ONLY itself.
        row_description_one("?column?", 23),
        data_row_one(b"1"),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);

    let (server, writes) = counting_server(inbound);
    let user = Ident::try_from_str("bsql_fusion").expect("user");
    session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let live = poll_once(engine.connect(live))
            .expect("single poll")
            .expect("handshake");
        writes.lock().expect("writes lock").clear();

        // Arm a prelude (as a transaction would), then DISCARD it (as the pool's
        // reset_session does when a panicked body stranded it).
        engine.defer_command_prelude("BEGIN");
        engine.clear_command_prelude();

        // The next verb flushes ONLY itself — the stranded BEGIN did not fuse.
        drop(poll_once(engine.query(live, "SELECT 1", |_s: Surface<'_>| ControlFlow::Continue(())))
            .expect("single poll")
            .expect("query"));
        let log = writes.lock().expect("writes lock");
        assert_eq!(log.len(), 1, "one flush");
        assert_eq!(
            &log[0],
            &simple_query_frame("SELECT 1"),
            "the stranded BEGIN was discarded — only the statement flushed"
        );
        drop(log);
        assert_eq!(
            engine.tx_status().expect("active"),
            TxStatus::Idle,
            "no transaction was opened (the stranded BEGIN never ran)"
        );
    })
    .expect("session");
}

// ─────────────────────────── LIVE flush-count witness ───────────────────

/// A blocking-`TcpStream` transport that RECORDS each write's bytes — the live
/// counterpart of [`CountingServer`], so the same one-flush fusion can be proven
/// against a real PostgreSQL. Every op blocks and resolves on the first poll, so
/// `poll_once` drives the engine over it.
#[cfg(test)]
mod live {
    use super::{simple_query_frame, Collected};
    use bsql_postgres_proto::action::TxStatus;
    use bsql_postgres_proto::engine::{open_owned, poll_once, Surface, Transport};
    use bsql_postgres_proto::{Credentials, Ident};
    use core::future::{ready, Future};
    use core::ops::ControlFlow;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::{Arc, Mutex};

    struct LiveCountingSocket {
        stream: TcpStream,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl Transport for LiveCountingSocket {
        type Error = std::io::Error;
        fn is_would_block(err: &Self::Error) -> bool {
            matches!(
                err.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            )
        }
        fn read<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
            ready(self.stream.read(buf))
        }
        fn write<'a>(
            &'a mut self,
            buf: &'a [u8],
        ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
            let n = self.stream.write(buf);
            if let Ok(written) = n {
                let recorded = match buf.get(..written) {
                    Some(bytes) => bytes.to_vec(),
                    None => Vec::new(),
                };
                self.writes.lock().expect("writes lock").push(recorded);
            }
            ready(n)
        }
        fn flush<'a>(
            &'a mut self,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
            ready(self.stream.flush())
        }
        fn shutdown<'a>(
            &'a mut self,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
            ready(self.stream.shutdown(std::net::Shutdown::Both))
        }
    }

    /// LIVE: a deferred `BEGIN` fuses into the first statement's flush against a
    /// real PostgreSQL — proven by the socket write count (ONE write carries
    /// `BEGIN` + the statement) and a correct query result inside the transaction.
    ///
    /// Run with: `cargo test -p bsql-postgres-proto --test engine_prelude_fusion_spec -- --ignored`
    /// (needs PostgreSQL on localhost:5432, user `smir-ant`, db `postgres`, trust).
    #[test]
    #[ignore = "needs a local PostgreSQL (localhost:5432, user smir-ant, db postgres, trust auth)"]
    fn live_deferred_begin_fuses_into_first_statement_one_flush() {
        let stream = TcpStream::connect("127.0.0.1:5432").expect("connect PG");
        stream.set_nodelay(true).expect("nodelay");
        let writes = Arc::new(Mutex::new(Vec::new()));
        let socket = LiveCountingSocket {
            stream,
            writes: Arc::clone(&writes),
        };
        let user = Ident::try_from_str("smir-ant").expect("user");
        let db = bsql_postgres_proto::DatabaseName::try_from_str("postgres").expect("db");
        let (mut engine, live) =
            open_owned(socket, &user, Some(&db), &[], Credentials::Trust).expect("open");
        let live = poll_once(engine.connect(live))
            .expect("single poll")
            .expect("handshake");

        // Only the body's writes are the subject of the count.
        writes.lock().expect("writes lock").clear();
        engine.defer_command_prelude("BEGIN");
        let mut collected = Collected::default();
        drop(poll_once(engine.query(live, "SELECT 1", |s: Surface<'_>| {
            match s {
                Surface::Row(body) => collected.rows.push(body.to_vec()),
                Surface::Deliver { tag, .. } => {
                    let label = match tag {
                        Some(t) => format!("{t:?}"),
                        None => "<none>".to_owned(),
                    };
                    collected.tags.push(label);
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }))
        .expect("single poll")
        .expect("fused query"));

        // ONE flush carried BEGIN + the statement.
        let log = writes.lock().expect("writes lock");
        assert_eq!(
            log.len(),
            1,
            "LIVE: expected ONE flush carrying BEGIN + the statement; got {} writes: {:?}",
            log.len(),
            log
        );
        assert_eq!(
            &log[0],
            &super::concat(&[simple_query_frame("BEGIN"), simple_query_frame("SELECT 1")]),
            "LIVE: the single flush is BEGIN 'Q' then the statement 'Q'"
        );
        // The statement's own row surfaced; BEGIN's delivery was swallowed; the
        // transaction is really open on the server.
        assert_eq!(collected.rows.len(), 1, "the SELECT row surfaced");
        assert!(collected.rows[0].ends_with(b"1"), "carries the `1` cell");
        assert!(!collected.tags.iter().any(|t| t.contains("BEGIN")));
        assert_eq!(engine.tx_status().expect("active"), TxStatus::InTransaction);
    }
}
