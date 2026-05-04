//! Phase 1b — startup handshake + SCRAM-SHA-256 end-to-end tests.
//!
//! Every test here names the invariant it defends. Per architect.txt
//! Part III, a test exists only for spec conformance (A), tier-3
//! invariants (B), or compile-time invariant docs (C).
//!
//! These tests drive the state machine with synthetic wire bytes —
//! the same way the async wrapper does, without any runtime.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    Action, ConnectionStatus, Credentials, Ident, PgCommand, PgProtocol, Password, ProtoState,
    ProtocolError, Reply, ReplyId, ReplyKind, Sensitive,
};
use core::num::NonZeroU64;

mod common;
use common::PushOrPanic;

fn raw(value: u64) -> NonZeroU64 {
    // DEF-145: raw(0) is a test bug; assert fires loud.
    assert!(value > 0, "raw(0) is a test bug — use raw(1..) for non-zero test correlators");
    NonZeroU64::new(value).unwrap_or(NonZeroU64::MIN)
}

/// Generic over `K: ReplyKind` — call-site infers the kind from the
/// command being constructed (e.g. `PgCommand::Startup { reply:
/// id(...) }` selects `StartupKind`; `PgCommand::Ping { reply:
/// id(...) }` selects `PingKind`).
fn id<K: ReplyKind>(value: NonZeroU64) -> ReplyId<K> {
    ReplyId::from_raw(value)
}

/// Build an AuthenticationOk frame: tag 'R', length 8, sub-code 0.
fn auth_ok_frame() -> [u8; 9] {
    // tag R, length=8 (4 length + 4 subcode), subcode=0
    [b'R', 0, 0, 0, 8, 0, 0, 0, 0]
}

/// Build an AuthenticationSASL frame offering SCRAM-SHA-256.
fn auth_sasl_frame() -> Vec<u8> {
    // tag R, length = 4 + 4(subcode) + mechanism_list_bytes
    // subcode = 10
    // mechanism list: "SCRAM-SHA-256\0\0"
    let mechanism = b"SCRAM-SHA-256\0\0";
    let subcode: u32 = 10;
    let payload_len = 4u32.saturating_add(u32::try_from(mechanism.len()).unwrap_or(0));
    let declared = payload_len.saturating_add(4); // length includes self
    let mut frame = Vec::new();
    frame.push(b'R');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&subcode.to_be_bytes());
    frame.extend_from_slice(mechanism);
    frame
}

/// Build an AuthenticationSASLContinue frame with server-first-message body.
fn auth_sasl_continue_frame(server_first: &[u8]) -> Vec<u8> {
    // tag R, length = 4 + 4(subcode) + body, subcode = 11
    let subcode: u32 = 11;
    let payload_len = 4u32.saturating_add(u32::try_from(server_first.len()).unwrap_or(0));
    let declared = payload_len.saturating_add(4);
    let mut frame = Vec::new();
    frame.push(b'R');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&subcode.to_be_bytes());
    frame.extend_from_slice(server_first);
    frame
}

/// Build an AuthenticationSASLFinal frame with server-final-message body.
fn auth_sasl_final_frame(server_final: &[u8]) -> Vec<u8> {
    // tag R, length = 4 + 4(subcode) + body, subcode = 12
    let subcode: u32 = 12;
    let payload_len = 4u32.saturating_add(u32::try_from(server_final.len()).unwrap_or(0));
    let declared = payload_len.saturating_add(4);
    let mut frame = Vec::new();
    frame.push(b'R');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&subcode.to_be_bytes());
    frame.extend_from_slice(server_final);
    frame
}

/// Build a ParameterStatus frame: tag 'S', key\0value\0.
fn param_status_frame(key: &str, value: &str) -> Vec<u8> {
    let body_len = key.len().saturating_add(1).saturating_add(value.len()).saturating_add(1);
    let declared = u32::try_from(body_len).unwrap_or(0).saturating_add(4);
    let mut frame = Vec::new();
    frame.push(b'S');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(key.as_bytes());
    frame.push(0);
    frame.extend_from_slice(value.as_bytes());
    frame.push(0);
    frame
}

/// Build a BackendKeyData frame: tag 'K', 8-byte payload (pid + secret_key).
fn backend_key_data_frame(pid: i32, secret_key: i32) -> [u8; 13] {
    let pid_bytes = pid.to_be_bytes();
    let key_bytes = secret_key.to_be_bytes();
    [
        b'K', 0, 0, 0, 12,
        pid_bytes[0], pid_bytes[1], pid_bytes[2], pid_bytes[3],
        key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3],
    ]
}

/// Build a ReadyForQuery frame.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [b'Z', 0, 0, 0, 5, tx_status]
}

/// Build an ErrorResponse frame with severity, code, message fields.
fn error_response_frame(severity: &str, code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    // Severity field
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    // Code field
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    // Message field
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    // Terminator
    body.push(0);

    let declared = u32::try_from(body.len()).unwrap_or(0).saturating_add(4);
    let mut frame = Vec::new();
    frame.push(b'E');
    frame.extend_from_slice(&declared.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Build a NegotiateProtocolVersion frame (tag 'v').
fn negotiate_proto_version_frame() -> Vec<u8> {
    // Minimal: tag v, length 12, newest_minor_version=0, num_options=0.
    let mut frame = Vec::new();
    frame.push(b'v');
    frame.extend_from_slice(&12u32.to_be_bytes()); // declared len
    frame.extend_from_slice(&0u32.to_be_bytes()); // newest_minor
    frame.extend_from_slice(&0u32.to_be_bytes()); // num_options
    frame
}

/// Push a Startup command (trust auth). DEF-212: bytes go to wb.
fn startup_trust(
    proto: &mut PgProtocol,
    wb: &mut bsql_pg_proto::WriteBuf,
    user: &str,
    db: Option<&str>,
    reply_raw: NonZeroU64,
) {
    let user_ident = Ident::try_from_str(user).unwrap_or_else(|e| panic!("bad user: {e}"));
    let database = db.map(|d| {
        bsql_pg_proto::DatabaseName::try_from_str(d).unwrap_or_else(|e| panic!("bad db: {e}"))
    });
    // DEF-212 (Alt Y'): push_or_panic returns (); bytes live in wb.
    // Caller drains via `wb.as_bytes()` for wire-layout assertions.
    proto.push_or_panic(PgCommand::Startup {
        user: user_ident,
        database,
        app_name: None,
        credentials: Credentials::Trust,
        reply: id(reply_raw),
    }, wb);
}

// ------------------------------------------------------------------
// (A) Spec conformance: trust-auth startup handshake end-to-end
// ------------------------------------------------------------------

/// Invariant (spec): trust-auth handshake: StartupMessage → AuthOk →
/// ParameterStatus × N → BackendKeyData → ReadyForQuery → Idle +
/// Reply::StartupComplete.
#[test]
fn trust_auth_handshake_end_to_end() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(1);

    // Push Startup (trust). DEF-212: bytes live in wb post-Ok.
    startup_trust(&mut proto, &mut wb, "testuser", Some("testdb"), startup_raw);
    {
        // Scope the `&wb` borrow so subsequent feed_bytes calls can
        // re-borrow `&mut wb` after the inspection.
        let bytes = wb.as_bytes();
        // Verify the StartupMessage wire format.
        // First 4 bytes: length (includes self).
        // Next 4 bytes: protocol version 196608.
        assert!(bytes.len() >= 8, "StartupMessage must be at least 8 bytes");
        let version_bytes = bytes.get(4..8);
        assert_eq!(
            version_bytes,
            Some([0, 3, 0, 0].as_slice()),
            "protocol version must be 3.0 (196608)",
        );
    }
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingStartupTrust { .. }
    ));

    // Feed AuthenticationOk.
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk produces no actions (state transition only)");
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingPostAuthAwaitingKey(_)
    ));

    // Feed ParameterStatus messages.
    let out = proto.feed_bytes(&param_status_frame("server_version", "17.2"), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "UTC"), &mut wb);
    assert_eq!(out.len(), 0);

    // Feed BackendKeyData.
    let out = proto.feed_bytes(&backend_key_data_frame(12345, 67890), &mut wb);
    assert_eq!(out.len(), 0);
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingPostAuthHaveKey { .. }
    ));

    // Feed more ParameterStatus after BackendKeyData (allowed).
    let out = proto.feed_bytes(&param_status_frame("client_encoding", "UTF8"), &mut wb);
    assert_eq!(out.len(), 0);

    // Feed ReadyForQuery — completes the handshake.
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1, "RFQ completes handshake with DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(delivered_id, &startup_raw);
            match value {
                Reply::StartupComplete(p) => {
                    assert_eq!(p.pid, 12345);
                    assert_eq!(p.secret_key, 67890);
                    assert_eq!(p.tx_status, bsql_pg_proto::TxStatus::Idle);
                }
                other => panic!("expected StartupComplete, got {other:?}"),
            }
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));

    // Verify session params were recorded.
    assert_eq!(
        proto.session_params().server_version.as_ref().map(|s| s.as_str()),
        Some("17.2"),
    );
    assert_eq!(proto.session_params().time_zone.as_ref().map(|s| s.as_str()), Some("UTC"));
    // DEF-114: client_encoding is now a typed Encoding enum.
    assert_eq!(
        proto.session_params().client_encoding,
        Some(bsql_pg_proto::Encoding::Utf8),
    );
}

/// Invariant (spec): ErrorResponse during startup → classified
/// ServerErrorResponse with severity/code/message fields.
#[test]
fn error_response_during_startup_is_classified() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(2);
    startup_trust(&mut proto, &mut wb, "baduser", None, startup_raw);

    let err_frame = error_response_frame("FATAL", "28P01", "password authentication failed");
    let out = proto.feed_bytes(&err_frame, &mut wb);

    assert_eq!(out.len(), 2, "ErrorResponse → FailReply + CloseSocket");
    // DEF-184 (A1+A13): ServerErrorResponse now carries
    // `details_ref: ErrorRef`. Extract the ref (Copy), drop `out`
    // to release the &mut proto borrow, then resolve via
    // `proto.get_server_error(r)` for message/detail/hint.
    let details_ref = match out.as_slice() {
        [Action::FailReply { id: failed_id, cause }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
            match cause {
                ProtocolError::ServerErrorResponse {
                    severity,
                    code,
                    details_ref,
                } => {
                    assert_eq!(severity.as_str(), "FATAL");
                    assert_eq!(code.as_str(), "28P01");
                    *details_ref
                }
                other => panic!("expected ServerErrorResponse, got {other:?}"),
            }
        }
        other => panic!("unexpected action sequence: {other:?}"),
    };
    // `out` is Copy-like (ManuallyDrop<heapless::Vec>); NLL
    // releases the &mut proto borrow at `out.as_slice()`'s last
    // use above. No explicit drop needed.
    // DEF-184 (audit #3 A-06): Result-returning get_server_error.
    // Err branch panics with the classified ArenaError for debuggability
    // — architecturally unreachable here (parse allocated into arena,
    // no intervening clear before this resolve).
    let payload = match proto.get_server_error(details_ref) {
        Ok(payload) => payload,
        Err(e) => panic!("server error payload must resolve via arena, got ArenaError::{e:?}"),
    };
    assert_eq!(
        payload.message.as_str(),
        "password authentication failed",
    );
}

/// Invariant (spec): NegotiateProtocolVersion during startup →
/// classified UnsupportedProtocolOption. DEF-044.
#[test]
fn negotiate_protocol_version_during_startup() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(3);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);

    let out = proto.feed_bytes(&negotiate_proto_version_frame(), &mut wb);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id: failed_id, cause }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
            assert!(
                matches!(cause, ProtocolError::UnsupportedProtocolOption),
                "expected UnsupportedProtocolOption, got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// Invariant (spec): unknown Authentication sub-code → classified
/// UnsupportedAuthMethod.
#[test]
fn unknown_auth_subcode_is_rejected() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(4);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);

    // Build an Authentication frame with sub-code 99 (unknown).
    let frame = [b'R', 0, 0, 0, 8, 0, 0, 0, 99];
    let out = proto.feed_bytes(&frame, &mut wb);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            // DEF-184 (B9): Unknown carries NonZeroU32 (type-level
            // proof that server sent ≠ 0; AUTH_OK = 0 is classified
            // as KnownButWrong(AuthSubCode::Ok)).
            let expected_99 = match core::num::NonZeroU32::new(99) {
                Some(nz) => nz,
                None => panic!("99 is non-zero, NonZeroU32::new infallible"),
            };
            assert!(
                matches!(
                    cause,
                    ProtocolError::UnsupportedAuthMethod {
                        sub_code: bsql_pg_proto::error::AuthSubCodeClass::Unknown(nz),
                    } if *nz == expected_99,
                ),
                "expected UnsupportedAuthMethod(Unknown(99)), got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// DEF-198 invariant: pipelined Startup while one is in flight is
/// structurally blocked at the public API. `ConnectionStatus::Handshaking`
/// classifies the in-flight startup state for caller-side recovery.
#[test]
fn def198_pipelined_startup_blocked_at_compile_time() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let first_raw = raw(10);

    startup_trust(&mut proto, &mut wb, "testuser", None, first_raw);

    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready must return None during in-flight Startup",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Handshaking,
        "in-flight Startup classifies as ConnectionStatus::Handshaking",
    );

    // Drain the first startup to avoid Drop-guard panic.
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&backend_key_data_frame(1, 2), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1);
}

/// DEF-198 invariant: Startup on Errored state is structurally blocked
/// at the public API. `ConnectionStatus::Errored(kind)` exposes the
/// stored cause (here: ServerError from the fatal auth-failure).
#[test]
fn def198_startup_on_errored_blocked_at_compile_time() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let first_raw = raw(20);
    startup_trust(&mut proto, &mut wb, "testuser", None, first_raw);

    // Drive into Errored via ErrorResponse.
    let err = error_response_frame("FATAL", "28000", "auth failed");
    let out = proto.feed_bytes(&err, &mut wb);
    assert_eq!(out.len(), 2);

    use bsql_pg_proto::error::ErrorKind;

    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready must return None on Errored",
    );
    match proto.connection_status() {
        ConnectionStatus::Errored(state_err_kind) => {
            assert_eq!(
                state_err_kind.as_kind(),
                ErrorKind::ServerError,
                "stored cause must be ServerError",
            );
        }
        other => panic!("expected ConnectionStatus::Errored(ServerError), got {other:?}"),
    }
}

/// Invariant (spec): Ident::try_from_str rejects NUL bytes, rejects
/// over-length, accepts valid.
#[test]
fn ident_validation() {
    use bsql_pg_proto::IdentError;

    // Valid
    assert!(Ident::try_from_str("testuser").is_ok());

    // Empty
    assert!(matches!(
        Ident::try_from_str(""),
        Err(IdentError::Empty),
    ));

    // Contains NUL
    assert!(matches!(
        Ident::try_from_str("test\0user"),
        Err(IdentError::ContainsNul),
    ));

    // Over-length (> 63)
    let long = "a".repeat(64);
    assert!(matches!(
        Ident::try_from_str(&long),
        Err(IdentError::TooLong { .. }),
    ));

    // Exactly at limit
    let exact = "a".repeat(63);
    assert!(Ident::try_from_str(&exact).is_ok());
}

/// Invariant (spec): Password::try_from_str rejects empty, rejects
/// over-length, accepts valid.
#[test]
fn password_validation() {
    use bsql_pg_proto::PasswordError;

    // Valid
    assert!(Password::try_from_str("pencil").is_ok());

    // Empty (DEF-051)
    assert!(matches!(
        Password::try_from_str(""),
        Err(PasswordError::Empty),
    ));

    // DEF-185 P2-E (audit 2026-04-24): symbolic + 1-over-cap boundary.
    // Pre-fix literal `1025` happened to exceed the true cap (512
    // post-DEF-154 O) by a generous margin, but did not pin the
    // exact boundary. Post-fix uses `MAX_PASSWORD_LEN + 1` so any
    // future cap bump keeps the boundary test honest without manual
    // sync.
    let over_cap = "a".repeat(bsql_pg_proto::password::MAX_PASSWORD_LEN.saturating_add(1));
    assert!(matches!(
        Password::try_from_str(&over_cap),
        Err(PasswordError::TooLong { .. }),
    ));

    // Exact-cap boundary: MAX_PASSWORD_LEN bytes should succeed.
    let at_cap = "a".repeat(bsql_pg_proto::password::MAX_PASSWORD_LEN);
    assert!(Password::try_from_str(&at_cap).is_ok());
}

/// Invariant (spec): StartupMessage serialised byte-for-byte correctly.
#[test]
fn startup_message_wire_format() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(100);
    startup_trust(&mut proto, &mut wb, "alice", Some("mydb"), startup_raw);

    // DEF-212: bytes live in wb. Scope the borrow so subsequent
    // feed_bytes calls reacquire &mut wb cleanly.
    {
        let bytes = wb.as_bytes();
        // Parse the length prefix.
        let len_bytes = bytes.get(..4).unwrap_or(&[]);
        let declared = u32::from_be_bytes([
            *len_bytes.first().unwrap_or(&0),
            *len_bytes.get(1).unwrap_or(&0),
            *len_bytes.get(2).unwrap_or(&0),
            *len_bytes.get(3).unwrap_or(&0),
        ]);
        assert_eq!(
            usize::try_from(declared).unwrap_or(0),
            bytes.len(),
            "length prefix must equal total frame length",
        );

        // Protocol version at offset 4..8.
        let version = bytes.get(4..8);
        assert_eq!(version, Some([0, 3, 0, 0].as_slice()));

        // Check that "user" and "alice" appear in the payload.
        let payload = bytes.get(8..).unwrap_or(&[]);
        assert!(
            contains_nul_terminated_pair(payload, b"user", b"alice"),
            "StartupMessage must contain user=alice",
        );
        assert!(
            contains_nul_terminated_pair(payload, b"database", b"mydb"),
            "StartupMessage must contain database=mydb",
        );
    }

    // Drain.
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&backend_key_data_frame(1, 1), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1);
}

/// Check if a byte slice contains a NUL-terminated key-value pair.
fn contains_nul_terminated_pair(data: &[u8], key: &[u8], value: &[u8]) -> bool {
    // Look for: key \0 value \0
    let mut needle = Vec::new();
    needle.extend_from_slice(key);
    needle.push(0);
    needle.extend_from_slice(value);
    needle.push(0);

    // Search for needle in data.
    if needle.len() > data.len() {
        return false;
    }
    let limit = data.len().saturating_sub(needle.len());
    let mut i: usize = 0;
    while i <= limit {
        if data.get(i..i.saturating_add(needle.len())) == Some(needle.as_slice()) {
            return true;
        }
        i = match i.checked_add(1) {
            Some(n) => n,
            None => break,
        };
    }
    false
}

/// Invariant (spec): post-terminal frame behaviour for new connecting
/// states — extends existing errored_state_is_terminal pattern.
#[test]
fn connecting_states_become_errored_on_bad_frame() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(200);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);

    // Feed a completely unexpected frame tag during ConnectingStartup.
    let garbage_frame = [b'X', 0, 0, 0, 4]; // tag X, minimal legal length
    let out = proto.feed_bytes(&garbage_frame, &mut wb);

    assert_eq!(out.len(), 2, "unexpected frame → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            assert!(
                matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'X'),
                "expected UnexpectedFrame(X), got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Errored(_)));

    // Post-terminal frames are dropped silently.
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 0, "post-terminal frame must emit zero actions");
}

// ------------------------------------------------------------------
// (A) SCRAM-SHA-256 handshake end-to-end
// ------------------------------------------------------------------

/// Helper: push Startup with SCRAM password credentials. DEF-212: bytes go to wb.
fn startup_scram(
    proto: &mut PgProtocol,
    wb: &mut bsql_pg_proto::WriteBuf,
    user: &str,
    password: &str,
    reply_raw: NonZeroU64,
) {
    let user_ident = Ident::try_from_str(user).unwrap_or_else(|e| panic!("bad user: {e}"));
    let pw = Password::try_from_str(password).unwrap_or_else(|e| panic!("bad pw: {e}"));
    // DEF-212 (Alt Y'): push_or_panic returns (); bytes live in wb.
    proto.push_or_panic(PgCommand::Startup {
        user: user_ident,
        database: None,
        app_name: None,
        credentials: Credentials::ScramPassword(Sensitive::new(pw)),
        reply: id(reply_raw),
    }, wb);
}

/// Extract the client-first-message from a SASLInitialResponse frame.
/// Returns (client_nonce, client_first_bare, full_client_first).
fn extract_client_first_from_sasl_initial(frame_bytes: &[u8]) -> (&[u8], Vec<u8>) {
    // SASLInitialResponse frame:
    // tag 'p' [0], length [1..5], body:
    //   mechanism NUL, i32 body_len, client-first-message
    //
    // We need to find the client-first-message body.
    // Skip tag(1) + length(4) = 5 bytes.
    let body = frame_bytes.get(5..).unwrap_or(&[]);

    // Find the mechanism NUL terminator.
    let mech_end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    // Skip mechanism + NUL + i32 body_length.
    let msg_start = mech_end.saturating_add(1).saturating_add(4);
    let client_first = body.get(msg_start..).unwrap_or(&[]);

    // client_first starts with GS2 header "n,,". Strip it to get bare.
    let bare = if client_first.starts_with(b"n,,") {
        client_first.get(3..).unwrap_or(&[])
    } else {
        client_first
    };

    (client_first, bare.to_vec())
}

/// Extract the client nonce from a client-first-message-bare.
/// Format: "n=<user>,r=<nonce>" — we want the nonce part.
fn extract_client_nonce_from_bare(bare: &[u8]) -> Vec<u8> {
    let bare_str = std::str::from_utf8(bare).unwrap_or("");
    for part in bare_str.split(',') {
        if let Some(nonce) = part.strip_prefix("r=") {
            return nonce.as_bytes().to_vec();
        }
    }
    Vec::new()
}

/// Invariant (spec): SCRAM-SHA-256 handshake end-to-end.
///
/// Simulates the server side by computing correct responses for the
/// client's nonce (extracted from the SASLInitialResponse).
#[test]
fn scram_sha256_handshake_end_to_end() {
    use base64ct::{Base64, Encoding};
    use bsql_pg_proto::scram::crypto::compute_client_proof;

    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(300);
    let password = "pencil";

    // Step 1: Push Startup with SCRAM password.
    startup_scram(&mut proto, &mut wb, "user", password, startup_raw);
    // DEF-212: bytes live in wb. StartupMessage has no tag byte;
    // protocol version 3.0 (196608 = 0x00030000) occupies bytes [4..8]
    // after the 4-byte length prefix.
    {
        let bytes = wb.as_bytes();
        assert!(bytes.len() >= 8, "StartupMessage must be >= 8 bytes");
        assert_eq!(
            bytes.get(4..8),
            Some([0, 3, 0, 0].as_slice()),
            "StartupMessage protocol version must be 3.0 (196608)",
        );
    }
    assert!(matches!(proto.state(), ProtoState::ConnectingStartupScram { .. }));

    // Step 2: Server sends AuthenticationSASL with SCRAM-SHA-256.
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    // This should produce a SASLInitialResponse.
    assert_eq!(out.len(), 1, "AuthSASL → SendBytes(SASLInitialResponse)");
    let sasl_initial_bytes: Vec<u8> = match out.as_slice() {
        [Action::SendBytes(send_buf)] => send_buf.to_vec(),
        other => panic!("expected SendBytes(Owned), got {other:?}"),
    };
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingScramAwaitingServerFirst { .. }
    ));

    // Step 3: Extract client nonce from SASLInitialResponse.
    let (_client_first, client_first_bare) =
        extract_client_first_from_sasl_initial(&sasl_initial_bytes);
    let client_nonce = extract_client_nonce_from_bare(&client_first_bare);
    assert!(!client_nonce.is_empty(), "client nonce must not be empty");

    // Step 4: Build server-first-message.
    // We use a known salt and iteration count.
    let salt_raw: [u8; 16] = [
        0x5B, 0x6D, 0x99, 0x68, 0x9D, 0x12, 0x35, 0x8E,
        0xEC, 0xA0, 0x4B, 0x14, 0x12, 0x36, 0xFA, 0x81,
    ];
    let iterations = 4096u32;

    // Base64-encode the salt via dev-dep `base64ct` — tests don't
    // depend on crate-internal `base64_encode_to_buf` (which is
    // `pub(crate)` post-visibility audit).
    let mut salt_b64_buf = [0u8; 64];
    let salt_b64 = Base64::encode(&salt_raw, &mut salt_b64_buf).unwrap_or("");

    // Server nonce = client_nonce + server_suffix.
    let server_suffix = b"ServerSuffix1234";
    let mut server_nonce = client_nonce.clone();
    server_nonce.extend_from_slice(server_suffix);
    let server_nonce_str = std::str::from_utf8(&server_nonce).unwrap_or("");

    let server_first = format!("r={server_nonce_str},s={salt_b64},i={iterations}");

    // Step 5: Feed AuthenticationSASLContinue with server-first.
    let out = proto.feed_bytes(&auth_sasl_continue_frame(server_first.as_bytes()), &mut wb);
    // This should produce a SASLResponse (client-final-message).
    assert_eq!(out.len(), 1, "SASLContinue → SendBytes(SASLResponse)");
    match out.as_slice() {
        [Action::SendBytes(_)] => {}
        other => panic!("expected SendBytes(SASLResponse), got {other:?}"),
    }
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingScramAwaitingServerFinal { .. }
    ));

    // Step 6: Compute the expected server signature.
    // Pass AuthMessage components separately — compute_client_proof
    // feeds them incrementally into HMAC (no staging buffer).
    let client_first_bare_str = std::str::from_utf8(&client_first_bare).unwrap_or("");
    let client_final_without_proof = format!("c=biws,r={server_nonce_str}");

    // F54: compute_client_proof returns Result (fail-closed on the
    // architecturally-dead HMAC-key-reject path). RFC 7677 params
    // produce Ok; assert before destructuring.
    let proof_result = compute_client_proof(
        password.as_bytes(),
        &salt_raw,
        iterations,
        client_first_bare_str.as_bytes(),
        server_first.as_bytes(),
        client_final_without_proof.as_bytes(),
    );
    assert!(proof_result.is_ok(), "compute_client_proof must succeed on well-formed inputs");
    let expected_server_sig = match proof_result {
        Ok((_, sig)) => sig,
        Err(_) => return, // dead after assert above
    };

    // Base64-encode the server signature via dev-dep `base64ct`.
    let mut sig_b64_buf = [0u8; 64];
    let sig_b64 = Base64::encode(expected_server_sig.as_bytes(), &mut sig_b64_buf).unwrap_or("");

    let server_final = format!("v={sig_b64}");

    // Step 7: Feed AuthenticationSASLFinal with server-final.
    let out = proto.feed_bytes(&auth_sasl_final_frame(server_final.as_bytes()), &mut wb);
    assert_eq!(out.len(), 0, "SASLFinal → state transition only (awaiting AuthOk)");
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingScramAwaitingAuthOk(_)
    ));

    // Step 8: Feed AuthenticationOk.
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk after SCRAM → post-auth chain");
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingPostAuthAwaitingKey(_)
    ));

    // Step 9: Complete post-auth chain.
    let out = proto.feed_bytes(&param_status_frame("server_version", "17.2"), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&backend_key_data_frame(42, 99), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(delivered_id, &startup_raw);
            assert!(matches!(
                value,
                Reply::StartupComplete(bsql_pg_proto::StartupCompletePayload {
                    pid: 42,
                    secret_key: 99,
                    tx_status: bsql_pg_proto::TxStatus::Idle,
                })
            ));
        }
        other => panic!("expected DeliverReply(StartupComplete), got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): SCRAM server signature mismatch → classified error.
#[test]
fn scram_signature_mismatch_is_rejected() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(400);

    // Start SCRAM handshake.
    startup_scram(&mut proto, &mut wb, "user", "pencil", startup_raw);
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    let sasl_initial_bytes: Vec<u8> = match out.as_slice() {
        [Action::SendBytes(send_buf)] => send_buf.to_vec(),
        other => panic!("expected SendBytes, got {other:?}"),
    };

    let (_client_first, client_first_bare) =
        extract_client_first_from_sasl_initial(&sasl_initial_bytes);
    let client_nonce = extract_client_nonce_from_bare(&client_first_bare);

    // Build valid server-first.
    let salt_raw: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let mut salt_b64_buf = [0u8; 64];
    let salt_b64 = {
        use base64ct::{Base64, Encoding};
        Base64::encode(&salt_raw, &mut salt_b64_buf).unwrap_or("")
    };

    let mut server_nonce = client_nonce.clone();
    server_nonce.extend_from_slice(b"SRV");
    let server_nonce_str = std::str::from_utf8(&server_nonce).unwrap_or("");

    let server_first = format!("r={server_nonce_str},s={salt_b64},i=4096");
    let out = proto.feed_bytes(&auth_sasl_continue_frame(server_first.as_bytes()), &mut wb);
    assert_eq!(out.len(), 1, "SASLContinue → SendBytes(SASLResponse)");

    // Send a WRONG server signature.
    let wrong_sig = "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let out = proto.feed_bytes(&auth_sasl_final_frame(wrong_sig.as_bytes()), &mut wb);

    assert_eq!(out.len(), 2, "sig mismatch → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            assert!(
                matches!(cause, ProtocolError::Scram(_)),
                "expected ScramError, got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// Invariant (spec): SCRAM iterations < 4096 → classified error.
#[test]
fn scram_iterations_too_low_is_rejected() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(500);

    startup_scram(&mut proto, &mut wb, "user", "pencil", startup_raw);
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    let sasl_initial_bytes: Vec<u8> = match out.as_slice() {
        [Action::SendBytes(send_buf)] => send_buf.to_vec(),
        other => panic!("expected SendBytes, got {other:?}"),
    };

    let (_client_first, client_first_bare) =
        extract_client_first_from_sasl_initial(&sasl_initial_bytes);
    let client_nonce = extract_client_nonce_from_bare(&client_first_bare);

    let mut server_nonce = client_nonce;
    server_nonce.extend_from_slice(b"SRV");
    let server_nonce_str = std::str::from_utf8(&server_nonce).unwrap_or("");

    // iterations = 100 (below minimum 4096)
    let server_first = format!("r={server_nonce_str},s=QSXCR+Q6sek8bf92,i=100");
    let out = proto.feed_bytes(&auth_sasl_continue_frame(server_first.as_bytes()), &mut wb);

    assert_eq!(out.len(), 2, "low iterations → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            assert!(
                matches!(cause, ProtocolError::Scram(_)),
                "expected ScramError for low iterations, got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// BS8 regression (pass #6 audit): SCRAM iteration count above the
/// client's sanity cap classifies as `ScramError::IterationsTooHigh`.
/// Closes the client-side DoS surface where a malicious or
/// mis-configured server could send `iterations = u32::MAX` to stall
/// PBKDF2 for minutes per connection.
#[test]
fn scram_iterations_above_cap_is_rejected() {
    use bsql_pg_proto::scram::wire::{MAX_SCRAM_ITERATIONS, ScramError};

    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(550);

    startup_scram(&mut proto, &mut wb, "user", "pencil", startup_raw);
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    let sasl_initial_bytes: Vec<u8> = match out.as_slice() {
        [Action::SendBytes(send_buf)] => send_buf.to_vec(),
        other => panic!("expected SendBytes, got {other:?}"),
    };

    let (_client_first, client_first_bare) =
        extract_client_first_from_sasl_initial(&sasl_initial_bytes);
    let client_nonce = extract_client_nonce_from_bare(&client_first_bare);

    let mut server_nonce = client_nonce;
    server_nonce.extend_from_slice(b"SRV");
    let server_nonce_str = std::str::from_utf8(&server_nonce).unwrap_or("");

    // iterations above sanity cap (= MAX + 1)
    let too_high = MAX_SCRAM_ITERATIONS.saturating_add(1);
    let server_first = format!("r={server_nonce_str},s=QSXCR+Q6sek8bf92,i={too_high}");
    let out = proto.feed_bytes(&auth_sasl_continue_frame(server_first.as_bytes()), &mut wb);

    assert_eq!(out.len(), 2, "high iterations → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            match cause {
                ProtocolError::Scram(ScramError::IterationsTooHigh { iterations }) => {
                    assert_eq!(*iterations, too_high);
                }
                other => panic!("expected IterationsTooHigh, got {other:?}"),
            }
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// F30 regression: SCRAM server-final-message with `e=<text>`
/// produces `ScramError::ServerScramError { message }` carrying the
/// RFC-defined error token, not an opaque unit variant. Before F30
/// the wrapper crate could only log "server reported authentication
/// error" with no forensic clue to which failure mode fired.
#[test]
fn scram_server_error_preserves_diagnostic_message() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(700);

    startup_scram(&mut proto, &mut wb, "user", "pencil", startup_raw);
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    let sasl_initial_bytes: Vec<u8> = match out.as_slice() {
        [Action::SendBytes(send_buf)] => send_buf.to_vec(),
        other => panic!("expected SendBytes, got {other:?}"),
    };
    let (_client_first, client_first_bare) =
        extract_client_first_from_sasl_initial(&sasl_initial_bytes);
    let client_nonce = extract_client_nonce_from_bare(&client_first_bare);

    let salt_raw: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let mut salt_b64_buf = [0u8; 64];
    let salt_b64 = {
        use base64ct::{Base64, Encoding};
        Base64::encode(&salt_raw, &mut salt_b64_buf).unwrap_or("")
    };

    let mut server_nonce = client_nonce.clone();
    server_nonce.extend_from_slice(b"SRV");
    let server_nonce_str = std::str::from_utf8(&server_nonce).unwrap_or("");

    let server_first = format!("r={server_nonce_str},s={salt_b64},i=4096");
    let _ = proto.feed_bytes(&auth_sasl_continue_frame(server_first.as_bytes()), &mut wb);

    // Server responds with `e=invalid-proof` instead of `v=<verifier>`.
    let server_error_msg = b"e=invalid-proof";
    let out = proto.feed_bytes(&auth_sasl_final_frame(server_error_msg), &mut wb);

    assert_eq!(out.len(), 2, "server e= → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            match cause {
                ProtocolError::Scram(bsql_pg_proto::scram::wire::ScramError::ServerScramError { message }) => {
                    assert_eq!(
                        message.as_str(),
                        "invalid-proof",
                        "F30: server-error-value must be preserved, not silent-empty",
                    );
                }
                other => panic!("expected ServerScramError{{message}}, got {other:?}"),
            }
        }
        other => panic!("unexpected: {other:?}"),
    }
}

/// Invariant (spec): SCRAM nonce prefix mismatch → classified error.
#[test]
fn scram_nonce_prefix_mismatch_is_rejected() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(600);

    startup_scram(&mut proto, &mut wb, "user", "pencil", startup_raw);
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    assert_eq!(out.len(), 1);

    // Server-first with a nonce that does NOT start with client nonce.
    let server_first = b"r=COMPLETELY_DIFFERENT_NONCE,s=QSXCR+Q6sek8bf92,i=4096";
    let out = proto.feed_bytes(&auth_sasl_continue_frame(server_first), &mut wb);

    assert_eq!(out.len(), 2, "nonce mismatch → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { cause, .. }, Action::CloseSocket] => {
            assert!(
                matches!(cause, ProtocolError::Scram(_)),
                "expected ScramError for nonce mismatch, got {cause:?}",
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// =================================================================
// Sensitive<T> Debug redaction — seam class §4.11.1 / 3
// =================================================================

/// Category (1) spec-conformance — `Sensitive<T>` Debug pin.
///
/// Invariant: `Sensitive<T>` Debug prints `"<REDACTED>"` and does NOT
/// leak the inner value. A one-line impl drift (`f.write_str("<REDACTED>")`
/// → `f.debug_struct("Sensitive").field("inner", &self.inner).finish()`)
/// compiles and would silently expose secrets in logs / error messages.
/// This test catches such a drift.
#[test]
fn sensitive_debug_does_not_leak_inner_value() {
    let secret = bsql_pg_proto::Sensitive::new([42u8; 4]);
    let debug_output = format!("{secret:?}");
    assert!(
        debug_output.contains("REDACTED"),
        "Sensitive Debug must contain REDACTED, got: {debug_output}",
    );
    assert!(
        !debug_output.contains("42"),
        "Sensitive Debug must NOT leak inner value, got: {debug_output}",
    );
}

/// Category (1) spec-conformance — `Password` Debug pin.
///
/// Same seam as `Sensitive`: a `Password` Debug must never reveal
/// the password bytes.
#[test]
fn password_debug_does_not_leak_bytes() {
    let pw = bsql_pg_proto::Password::try_from_bytes(b"hunter2").unwrap_or_else(|_| {
        panic!("valid password must construct")
    });
    let debug_output = format!("{pw:?}");
    assert!(
        debug_output.contains("REDACTED"),
        "Password Debug must contain REDACTED, got: {debug_output}",
    );
    assert!(
        !debug_output.contains("hunter2"),
        "Password Debug must NOT leak password text, got: {debug_output}",
    );
}

/// Category (1) spec-conformance — `Credentials::ScramPassword` Debug pin.
#[test]
fn credentials_debug_does_not_leak_password() {
    let pw = bsql_pg_proto::Password::try_from_bytes(b"s3cret").unwrap_or_else(|_| {
        panic!("valid password must construct")
    });
    let cred = bsql_pg_proto::Credentials::ScramPassword(bsql_pg_proto::Sensitive::new(pw));
    let debug_output = format!("{cred:?}");
    assert!(
        debug_output.contains("REDACTED"),
        "Credentials Debug must contain REDACTED, got: {debug_output}",
    );
    assert!(
        !debug_output.contains("s3cret"),
        "Credentials Debug must NOT leak password, got: {debug_output}",
    );
}

// =================================================================
// (A) Spec conformance — unsolicited ParameterStatus after startup.
// DEF-054 regression guard.
// =================================================================

/// Invariant (spec): once the handshake completes and the state is
/// `Idle`, the server may emit `ParameterStatus` at any time (commonly
/// after a session `SET` command or `ALTER SYSTEM`). The protocol must
/// accept such frames silently — recording the new value in
/// [`PgProtocol::session_params`] and returning no actions — rather
/// than classifying as `UnexpectedFrame` and closing the connection.
///
/// This pins the `allows_unsolicited_param_status` exhaustive match in
/// `protocol.rs`: adding a new state variant without explicitly
/// deciding how it handles unsolicited PS fails the build there, and
/// flipping `Idle` from `true` to `false` in that match would compile
/// but would break the invariant this test asserts.
#[test]
fn unsolicited_param_status_in_idle_is_recorded_and_skipped() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(100);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);

    // Complete startup: AuthOk → BackendKeyData → RFQ → Idle.
    // Setup frames' actions are discarded explicitly (`drop`) rather
    // than via `let _ = ...` — the latter is banned by user feedback
    // memory (`feedback_no_underscore_vars`). Post-auth shape is
    // verified below via state and out assertions on the real test
    // body (PS frame).
    _ = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    _ = proto.feed_bytes(&backend_key_data_frame(1, 1), &mut wb);
    _ = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "handshake should land in Idle, got {:?}",
        proto.state(),
    );

    // Now simulate PG sending ParameterStatus in idle (e.g. after a
    // SET command finishes). Before DEF-054 this would trigger
    // UnexpectedFrame → CloseSocket. After DEF-054 it is recorded
    // silently.
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "America/New_York"), &mut wb);
    assert_eq!(out.len(), 0, "unsolicited PS in Idle emits no actions");
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "state must remain Idle after unsolicited PS, got {:?}",
        proto.state(),
    );
    assert_eq!(
        proto.session_params().time_zone.as_ref().map(|s| s.as_str()),
        Some("America/New_York"),
        "PS must update session_params",
    );
}

/// Invariant (spec): ParameterStatus arriving while the protocol is
/// in `PingAwaitingRfq` (or any other post-auth flight state) is
/// similarly recorded without disturbing the state. PG can emit PS
/// during query execution if `ALTER SYSTEM` runs server-side.
///
/// Before DEF-054 this would corrupt the in-flight ping (the PS would
/// be misclassified as an unexpected frame, failing the Ping reply).
/// After DEF-054 the PS is recorded pre-dispatch and the Ping remains
/// in flight.
#[test]
fn unsolicited_param_status_in_awaiting_ping_reply_is_recorded() {
    use bsql_pg_proto::PgCommand;

    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(200);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);
    // Explicit `drop` (see preceding test for rationale).
    _ = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    _ = proto.feed_bytes(&backend_key_data_frame(1, 1), &mut wb);
    _ = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);

    // Send a ping. State → PingAwaitingRfq.
    // DEF-212: feed_bytes calls above wrote the post-handshake bytes
    // into wb; clear before pushing the new Ping so wb.as_bytes()
    // contains ONLY the Sync. Production push_command_internal does
    // wb.clear() at entry; the helper preserves that — but if a
    // future caller drains feed_bytes between handshake and push
    // (which we do above), they see leftover handshake bytes in
    // wb until the next push wipes it. The helper test below
    // verifies the post-Ping wb contents are exactly Sync.
    let ping_raw = raw(201);
    proto.push_or_panic(PgCommand::Ping { reply: id(ping_raw) }, &mut wb);
    {
        // F33: literal PG Sync wire layout — avoids tautology with
        // internal SYNC_WIRE_BYTES const (both sourced from same symbol
        // would mirror any const-drift).
        assert_eq!(
            wb.as_bytes(),
            &[b'S', 0u8, 0u8, 0u8, 4u8],
            "Ping must emit PG Sync wire bytes: tag 'S' + BE u32 length=4",
        );
    }
    assert!(matches!(proto.state(), ProtoState::PingAwaitingRfq(_)));

    // Server emits ParameterStatus before RFQ (e.g. an ALTER SYSTEM
    // committed during our ping's round-trip).
    let out = proto.feed_bytes(&param_status_frame("client_encoding", "LATIN1"), &mut wb);
    assert_eq!(out.len(), 0, "PS during flight emits no actions");
    assert!(
        matches!(proto.state(), ProtoState::PingAwaitingRfq(_)),
        "PS must not disturb the PingAwaitingRfq state",
    );
    // DEF-114: typed Encoding.
    assert_eq!(
        proto.session_params().client_encoding,
        Some(bsql_pg_proto::Encoding::Latin1),
    );

    // Now feed RFQ — ping completes normally.
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1, "RFQ completes ping with DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered, .. }] => {
            assert_eq!(delivered, &ping_raw);
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): ParameterStatus arriving during the SCRAM
/// handshake (ConnectingScramAwaitingServerFirst) is out-of-spec — PG
/// does not emit PS between SASLInitialResponse and
/// AuthenticationSASLContinue. Policy (`allows_unsolicited_param_status`)
/// returns false for all ConnectingScram* states; dispatcher fallback
/// classifies any tag-in-state pair not explicitly handled as
/// UnexpectedFrame. This E2E test pins the wiring: unit-level
/// `policy_per_variant` verifies policy correctness; this test
/// verifies filter + dispatch composition produces the expected
/// FailReply + CloseSocket sequence.
///
/// The other two ConnectingScram* states (ServerFinal, AuthOk) use
/// the same policy function and the same dispatcher fallback — a
/// single E2E test here is sufficient because the code path is
/// structurally identical. Flipping any ConnectingScram* variant to
/// the `true` branch in `allows_unsolicited_param_status` would
/// compile and silently accept server PS frames mid-handshake;
/// `policy_per_variant` would catch that. Flipping the dispatcher's
/// catch-all arm would be caught by `connecting_states_become_errored_on_bad_frame`.
/// This test is the integration-level guard.
#[test]
fn unsolicited_ps_during_scram_await_server_first_is_unexpected() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(901);
    startup_scram(&mut proto, &mut wb, "user", "pencil", startup_raw);
    // Server offers SASL → we emit SASLInitialResponse → state is now
    // ConnectingScramAwaitingServerFirst. Setup frame's actions discarded
    // explicitly — `let _ = ...` is banned.
    _ = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    assert!(matches!(
        proto.state(),
        ProtoState::ConnectingScramAwaitingServerFirst { .. },
    ));

    // Unsolicited ParameterStatus during SCRAM — must be classified.
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "UTC"), &mut wb);
    assert_eq!(out.len(), 2, "PS during SCRAM → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id, cause }, Action::CloseSocket] => {
            assert_eq!(id, &startup_raw);
            assert!(
                matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'S'),
                "expected UnexpectedFrame('S'), got {cause:?}",
            );
        }
        other => panic!("unexpected action sequence: {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

/// Invariant (spec): ParameterStatus arriving in a pre-auth state
/// (before AuthOk) is out-of-spec and must be classified as
/// UnexpectedFrame — `allows_unsolicited_param_status` returns false
/// for Connecting* pre-auth states.
///
/// This pins the other side of the DEF-054 boundary: flipping a
/// Connecting* state from `false` to `true` in the exhaustive match
/// would compile and silently accept server frames that contravene
/// the auth handshake.
#[test]
fn param_status_during_pre_auth_is_unexpected() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(300);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);
    // State is now ConnectingStartup. Server should send
    // AuthenticationOk / SASL / ErrorResponse — not ParameterStatus.
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "UTC"), &mut wb);
    assert_eq!(out.len(), 2, "pre-auth PS → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id: failed, cause }, Action::CloseSocket] => {
            assert_eq!(failed, &startup_raw);
            assert!(
                matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'S'),
                "expected UnexpectedFrame('S'), got {cause:?}",
            );
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

/// DEF-184 (B17 fallback-hygiene catch): ParameterStatus with
/// missing trailing NUL (wire-spec §55.7 violation) must be
/// classified as MalformedPayload and silently dropped — NOT
/// silently absorbed with the wrong value. Pre-(184)
/// `strip_suffix(b"\0").unwrap_or(value_region)` silently accepted
/// malformed payload; post-(184) explicit Option match routes
/// missing-NUL to MalformedPayload.
///
/// Observable surface: the key is NOT recorded in SessionParams
/// (server_version stays None). Additionally the connection stays
/// in a normal post-auth state — ParameterStatus classification
/// failures don't tear down the connection (they're forward-compat
/// tolerant per PG §55.7 unsolicited-message semantics).
#[test]
fn param_status_missing_trailing_nul_classified_as_malformed() {
    let mut proto = PgProtocol::new();
    let mut wb = bsql_pg_proto::WriteBuf::new();
    let startup_raw = raw(400);
    startup_trust(&mut proto, &mut wb, "testuser", None, startup_raw);

    // Complete handshake: AuthOk → BackendKeyData → RFQ → Idle.
    let _ = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    let _ = proto.feed_bytes(&backend_key_data_frame(12345, 67890), &mut wb);
    let _ = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "handshake must complete to Idle — got {:?}",
        proto.state(),
    );

    // Build a malformed ParameterStatus: key\0value (NO trailing
    // NUL on value). Wire: 'S' + len + "server_version\017.2".
    // Body: "server_version" (14) + NUL (1) + "17.2" (4) = 19.
    // Length field: 19 + 4 = 23.
    let mut frame = Vec::new();
    frame.push(b'S');
    frame.extend_from_slice(&23u32.to_be_bytes());
    frame.extend_from_slice(b"server_version");
    frame.push(0);
    frame.extend_from_slice(b"17.2");
    // NO trailing NUL.

    let out = proto.feed_bytes(&frame, &mut wb);
    // Classified silent-skip — no action emitted, state unchanged.
    assert_eq!(out.len(), 0, "malformed PS is silently dropped, not an action");
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "state preserved through malformed PS",
    );
    // Critical: server_version must remain None. Pre-(184) the
    // fallback `unwrap_or(value_region)` would have set it to
    // some garbage byte slice.
    assert!(
        proto.session_params().server_version.is_none(),
        "malformed PS must NOT set server_version — {:?}",
        proto.session_params().server_version,
    );
}
