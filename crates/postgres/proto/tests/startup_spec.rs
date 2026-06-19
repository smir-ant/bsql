//! Startup handshake + SCRAM-SHA-256 end-to-end tests.
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
// Fixture-builder helper fns below panic on malformed synthetic input.
// Integration-test helpers run WITHOUT `cfg(test)`, so the floor's
// `allow-panic-in-tests` carve-out (keyed on `#[test]` context) cannot
// reach them; the panic is the loud test-failure signal, not a silent
// production fallback.
#![allow(clippy::panic, reason = "test harness — fixture builders panic on malformed synthetic input as the loud test-failure signal, not as a silent production fallback; integration-test helper fns are not in `#[test]` context so the in-tests carve-out cannot reach them")]

#![allow(clippy::disallowed_methods, reason = "test/bench harness — fixtures use the sanctioned try_from(..).unwrap_or(SAT) / slice.get(..).unwrap_or(&[]) dead-arm shape, not production data fallbacks")]
use bsql_postgres_proto::{
    Action, ActiveState, ConnectingPhase, ConnectingState, ConnectionStatus, Credentials, DisconnectedPhase,
    Ident, PgProtocol, Password, PingKind, ProtocolError, Reply, Sensitive,
    StartupKind,
};
use core::num::NonZeroU64;

mod common;
use common::{PushOrPanic, mint_reply, mint_reply_disconnected};

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

/// Consume-self Startup push for tests.
///
/// Pre-Phase-2 shape: `startup_trust(&mut proto, ...)` over a default
/// `<ActivePhase>` protocol via `push_command(Startup { ... })`.
/// Post-Phase-2 the Startup push is a consume-self transition from
/// `<DisconnectedPhase>` to `<ConnectingPhase>`; the helper mirrors.
///
/// Returns `(NonZeroU64, PgProtocol<ConnectingPhase>)` — the minted raw
/// ID for round-trip assertions and the typed Connecting wrapper for
/// the subsequent `feed_inbound` / `feed_bytes` drives. The `wb`
/// argument is repurposed: pre-Phase-2 the helper rebuilt `wb` with
/// the staged bytes for `wb.as_bytes()`-based wire assertions; post-
/// Phase-2 the consumed `OutActions` is collapsed to a single
/// `Action::SendBytes` chunk in `wb.as_bytes()` (this helper drains
/// it into the same `wb` scratch via the materialised slice).
fn startup_trust_consume(
    proto: PgProtocol<DisconnectedPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
    user: &str,
    db: Option<&str>,
) -> (NonZeroU64, PgProtocol<ConnectingPhase>) {
    let user_ident = Ident::try_from_str(user).unwrap_or_else(|e| panic!("bad user: {e}"));
    let database = db.map(|d| {
        bsql_postgres_proto::DatabaseName::try_from_str(d).unwrap_or_else(|e| panic!("bad db: {e}"))
    });
    let mut proto = proto;
    let (reply, reply_raw) = mint_reply_disconnected::<StartupKind>(&mut proto);
    let (actions, proto_connecting) = match proto.push_startup(
        user_ident,
        database,
        None,
        Credentials::Trust,
        reply,
        wb,
    ) {
        Ok(pair) => pair,
        Err(f) => panic!("test fixture: push_startup must succeed for Trust auth, got {:?}", f.cause),
    };
    // Pre-Phase-2 tests inspected `wb.as_bytes()` for the StartupMessage
    // wire layout. Post-Phase-2 the bytes are in `OutActions` slices.
    // Drain into a scratch and rebuild `wb` so the legacy
    // `wb.as_bytes()` invariant survives the migration.
    let mut scratch: std::vec::Vec<u8> = std::vec::Vec::with_capacity(512);
    for action in actions {
        if let Action::SendBytes(b) = action {
            scratch.extend_from_slice(b);
        }
    }
    wb.clear();
    if wb.push_bytes(&scratch).is_err() {
        panic!("test fixture: rebuilt StartupMessage ({} B) overflowed WriteBuf", scratch.len());
    }
    (reply_raw, proto_connecting)
}

// ------------------------------------------------------------------
// (A) Spec conformance: trust-auth startup handshake end-to-end
// ------------------------------------------------------------------

/// Invariant (spec): trust-auth handshake: StartupMessage → AuthOk →
/// ParameterStatus × N → BackendKeyData → ReadyForQuery → Idle +
/// Reply::StartupComplete.
#[test]
fn trust_auth_handshake_end_to_end() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();

    // Push Startup (trust). Bytes live in `wb` post-Ok.
    let (startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", Some("testdb"));
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
        ConnectingState::StartupTrust { .. }
    ));

    // Feed AuthenticationOk.
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk produces no actions (state transition only)");
    assert!(matches!(
        proto.state(),
        ConnectingState::PostAuthAwaitingKey(_)
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
        ConnectingState::PostAuthHaveKey { .. }
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
                    // DEF-286 Φ-E exception: StartupCompletePayload
                    // keeps inline tx_status (Connecting phase has
                    // no persistent slot).
                    assert_eq!(p.tx_status, bsql_postgres_proto::TxStatus::Idle);
                }
                other => panic!("expected StartupComplete, got {other:?}"),
            }
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ConnectingState::HandshakeReady { .. }));

    // Verify session params were recorded.
    assert_eq!(
        proto.session_params().server_version.as_ref().map(|s| s.as_str()),
        Some("17.2"),
    );
    assert_eq!(proto.session_params().time_zone.as_ref().map(|s| s.as_str()), Some("UTC"));
    // `client_encoding` is a typed Encoding enum.
    assert_eq!(
        proto.session_params().client_encoding,
        Some(bsql_postgres_proto::Encoding::Utf8),
    );
}

/// Invariant (spec): ErrorResponse during startup → classified
/// ServerErrorResponse with severity/code/message fields.
#[test]
fn error_response_during_startup_is_classified() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"baduser", None);

    let err_frame = error_response_frame("FATAL", "28P01", "password authentication failed");
    let out = proto.feed_bytes(&err_frame, &mut wb);

    assert_eq!(out.len(), 2, "ErrorResponse → FailReply + CloseSocket");
    // DEF-286 Φ-I.b: cause externalised onto the protocol's
    // `fail_cause_slot`. Action::FailReply carries only `id`; the
    // cause is queried via `proto.fail_cause()` AFTER out's borrow
    // releases. Pre-extract the FailReply id (Copy) here.
    match out.as_slice() {
        [Action::FailReply { id: failed_id }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
        }
        other => panic!("unexpected action sequence: {other:?}"),
    }
    // `out` is Copy-like (ManuallyDrop<heapless::Vec>); NLL
    // releases the &mut proto borrow at `out.as_slice()`'s last
    // use above. Query the cause now.
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated post-FailReply event"); };
    let details_ref = match cause {
        ProtocolError::ServerErrorResponse {
            severity,
            code,
            details_ref,
        } => {
            // `severity` is `Option<Severity>`. `Some(_)` is
            // required here (PG sent `SFATAL` field per
            // fixture below); `None` would indicate a
            // non-conformant peer (no S/V field at all).
            match severity {
                Some(s) => assert_eq!(s.as_str(), "FATAL"),
                None => panic!(
                    "expected Some(Severity::Fatal), got None — \
                     fixture sent SFATAL on the wire so the parser \
                     should have captured it",
                ),
            }
            assert_eq!(code.as_str(), "28P01");
            details_ref
        }
        other => panic!("expected ServerErrorResponse, got {other:?}"),
    };
    // `out` is Copy-like (ManuallyDrop<heapless::Vec>); NLL
    // releases the &mut proto borrow at `out.as_slice()`'s last
    // use above. No explicit drop needed.
    // Result-returning `get_server_error`. Err branch panics with
    // the classified `ArenaError` for debuggability —
    // architecturally unreachable here (parse allocated into arena,
    // no intervening clear before this resolve).
    let payload = match proto.get_server_error(details_ref) {
        Ok(payload) => payload,
        Err(e) => panic!("server error payload must resolve via arena, got ArenaError::{e:?}"),
    };
    match payload {
        bsql_postgres_proto::ErrorPayload::ServerError { message, .. } => assert_eq!(
            message.as_str(),
            "password authentication failed",
        ),
        other => panic!("expected ServerError variant, got {other:?}"),
    }
}

/// Invariant (spec): NegotiateProtocolVersion during startup →
/// classified `UnsupportedProtocolOption`.
#[test]
fn negotiate_protocol_version_during_startup() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

    let out = proto.feed_bytes(&negotiate_proto_version_frame(), &mut wb);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id: failed_id }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
        }
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnsupportedProtocolOption),
        "expected UnsupportedProtocolOption, got {cause:?}",
    );
}

/// Invariant (spec): unknown Authentication sub-code → classified
/// UnsupportedAuthMethod.
#[test]
fn unknown_auth_subcode_is_rejected() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

    // Build an Authentication frame with sub-code 99 (unknown).
    let frame = [b'R', 0, 0, 0, 8, 0, 0, 0, 99];
    let out = proto.feed_bytes(&frame, &mut wb);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let expected_99 = match core::num::NonZeroU32::new(99) {
        Some(nz) => nz,
        None => panic!("99 is non-zero, NonZeroU32::new infallible"),
    };
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(
            cause,
            ProtocolError::UnsupportedAuthMethod {
                sub_code: bsql_postgres_proto::error::AuthSubCodeClass::Unknown(nz),
            } if nz == expected_99,
        ),
        "expected UnsupportedAuthMethod(Unknown(99)), got {cause:?}",
    );
}

/// Invariant: pipelined Startup while one is in flight is
/// structurally blocked at the public API.
/// `ConnectionStatus::Handshaking` classifies the in-flight startup
/// state for caller-side recovery.
#[test]
fn pipelined_startup_blocked_at_compile_time() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();

    let (_first_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight Startup",
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

/// Invariant: Startup on Errored state is structurally blocked at
/// the public API. `ConnectionStatus::Errored(kind)` exposes the
/// stored cause (here: ServerError from the fatal auth-failure).
#[test]
fn startup_on_errored_blocked_at_compile_time() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_first_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

    // Drive into Errored via ErrorResponse.
    let err = error_response_frame("FATAL", "28000", "auth failed");
    let out = proto.feed_bytes(&err, &mut wb);
    assert_eq!(out.len(), 2);

    use bsql_postgres_proto::error::ErrorKind;

    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None on Errored",
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
    use bsql_postgres_proto::IdentError;

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
    use bsql_postgres_proto::PasswordError;

    // Valid
    assert!(Password::try_from_str("pencil").is_ok());

    // Empty
    assert!(matches!(
        Password::try_from_str(""),
        Err(PasswordError::Empty),
    ));

    // Symbolic + 1-over-cap boundary. A naive literal (e.g. `1025`)
    // would happen to exceed the true cap by some margin but not
    // pin the exact boundary; `MAX_PASSWORD_LEN + 1` keeps the
    // boundary test honest under any future cap bump without manual
    // sync.
    let over_cap = "a".repeat(bsql_postgres_proto::password::MAX_PASSWORD_LEN.saturating_add(1));
    assert!(matches!(
        Password::try_from_str(&over_cap),
        Err(PasswordError::TooLong { .. }),
    ));

    // Exact-cap boundary: MAX_PASSWORD_LEN bytes should succeed.
    let at_cap = "a".repeat(bsql_postgres_proto::password::MAX_PASSWORD_LEN);
    assert!(Password::try_from_str(&at_cap).is_ok());
}

/// Invariant (spec): StartupMessage serialised byte-for-byte correctly.
#[test]
fn startup_message_wire_format() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"alice", Some("mydb"));

    // Bytes live in `wb`. Scope the borrow so subsequent
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
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

    // Feed a completely unexpected frame tag during ConnectingStartup.
    let garbage_frame = [b'X', 0, 0, 0, 4]; // tag X, minimal legal length
    let out = proto.feed_bytes(&garbage_frame, &mut wb);

    assert_eq!(out.len(), 2, "unexpected frame → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'X'),
        "expected UnexpectedFrame(X), got {cause:?}",
    );
    assert!(matches!(proto.state(), ConnectingState::Errored(_)));

    // Post-terminal frames are dropped silently.
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 0, "post-terminal frame must emit zero actions");
}

// ------------------------------------------------------------------
// (A) SCRAM-SHA-256 handshake end-to-end
// ------------------------------------------------------------------

/// Consume-self Startup push for SCRAM
/// tests (mirror of `startup_trust_consume`).
fn startup_scram_consume(
    proto: PgProtocol<DisconnectedPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
    user: &str,
    password: &str,
) -> (NonZeroU64, PgProtocol<ConnectingPhase>) {
    let user_ident = Ident::try_from_str(user).unwrap_or_else(|e| panic!("bad user: {e}"));
    let pw = Password::try_from_str(password).unwrap_or_else(|e| panic!("bad pw: {e}"));
    let mut proto = proto;
    let (reply, reply_raw) = mint_reply_disconnected::<StartupKind>(&mut proto);
    let (actions, proto_connecting) = match proto.push_startup(
        user_ident,
        None,
        None,
        Credentials::ScramPassword(Sensitive::new(pw)),
        reply,
        wb,
    ) {
        Ok(pair) => pair,
        Err(f) => panic!("test fixture: push_startup must succeed for SCRAM auth, got {:?}", f.cause),
    };
    let mut scratch: std::vec::Vec<u8> = std::vec::Vec::with_capacity(512);
    for action in actions {
        if let Action::SendBytes(b) = action {
            scratch.extend_from_slice(b);
        }
    }
    wb.clear();
    if wb.push_bytes(&scratch).is_err() {
        panic!("test fixture: rebuilt StartupMessage ({} B) overflowed WriteBuf", scratch.len());
    }
    (reply_raw, proto_connecting)
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
    use bsql_postgres_proto::scram::crypto::compute_client_proof;

    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
        let password = "pencil";

    // Step 1: Push Startup with SCRAM password.
    let (startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", password);
    // Bytes live in `wb`. StartupMessage has no tag byte;
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
    assert!(matches!(proto.state(), ConnectingState::StartupScram { .. }));

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
        ConnectingState::ScramAwaitingServerFirst { .. }
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
        ConnectingState::ScramAwaitingServerFinal { .. }
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
        ConnectingState::ScramAwaitingAuthOk(_)
    ));

    // Step 8: Feed AuthenticationOk.
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk after SCRAM → post-auth chain");
    assert!(matches!(
        proto.state(),
        ConnectingState::PostAuthAwaitingKey(_)
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
                Reply::StartupComplete(bsql_postgres_proto::StartupCompletePayload {
                    pid: 42,
                    secret_key: 99,
                    tx_status: bsql_postgres_proto::TxStatus::Idle,
                })
            ));
        }
        other => panic!("expected DeliverReply(StartupComplete), got {other:?}"),
    }
    assert!(matches!(proto.state(), ConnectingState::HandshakeReady { .. }));
}

/// Invariant (spec): SCRAM server signature mismatch → classified error.
#[test]
fn scram_signature_mismatch_is_rejected() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    
    // Start SCRAM handshake.
    let (_startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", "pencil");
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
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ScramHandshakeFailure { .. }),
        "expected ScramError, got {cause:?}",
    );
}

/// Invariant (spec): SCRAM iterations < 4096 → classified error.
#[test]
fn scram_iterations_too_low_is_rejected() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();

    let (_startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", "pencil");
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
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ScramHandshakeFailure { .. }),
        "expected ScramError for low iterations, got {cause:?}",
    );
}

/// BS8 regression (pass #6 audit): SCRAM iteration count above the
/// client's sanity cap classifies as `ScramError::IterationsTooHigh`.
/// Closes the client-side DoS surface where a malicious or
/// mis-configured server could send `iterations = u32::MAX` to stall
/// PBKDF2 for minutes per connection.
#[test]
fn scram_iterations_above_cap_is_rejected() {
    use bsql_postgres_proto::scram::wire::{MAX_SCRAM_ITERATIONS, ScramFailureClass};

    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    
    let (_startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", "pencil");
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
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::ScramHandshakeFailure {
            class: ScramFailureClass::IterationsTooHigh { iterations },
            detail: _,
        } => {
            assert_eq!(iterations, too_high);
        }
        other => panic!("expected IterationsTooHigh, got {other:?}"),
    }
}

/// F30 regression: SCRAM server-final-message with `e=<text>`
/// produces `ScramError::ServerScramError { message }` carrying the
/// RFC-defined error token, not an opaque unit variant. Before F30
/// the wrapper crate could only log "server reported authentication
/// error" with no forensic clue to which failure mode fired.
#[test]
fn scram_server_error_preserves_diagnostic_message() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();

    let (_startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", "pencil");
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
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    let detail_ref = match cause {
        ProtocolError::ScramHandshakeFailure {
            class: bsql_postgres_proto::scram::wire::ScramFailureClass::ServerScramError,
            detail: Some(r),
        } => r,
        other => panic!("expected ScramHandshakeFailure{{ServerScramError, Some(_)}}, got {other:?}"),
    };
    // `out` is `OutActions` (`ManuallyDrop<heapless::Vec>`) — not a
    // Drop type. NLL releases the &mut proto borrow after the match
    // above; no explicit drop call needed.
    // F30 regression: server-error-value text must survive the
    // arena externalisation. Resolve via the same accessor used
    // for `ServerErrorResponse`.
    let payload = match proto.get_server_error(detail_ref) {
        Ok(p) => p,
        Err(e) => panic!("scram detail must resolve via arena, got ArenaError::{e:?}"),
    };
    match payload {
        bsql_postgres_proto::ErrorPayload::Scram { text } => assert_eq!(
            text.as_str(),
            "invalid-proof",
            "F30: server-error-value must be preserved, not silent-empty",
        ),
        other => panic!("expected ErrorPayload::Scram, got {other:?}"),
    }
}

/// Invariant (spec): SCRAM nonce prefix mismatch → classified error.
#[test]
fn scram_nonce_prefix_mismatch_is_rejected() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();

    let (_startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", "pencil");
    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    assert_eq!(out.len(), 1);

    // Server-first with a nonce that does NOT start with client nonce.
    let server_first = b"r=COMPLETELY_DIFFERENT_NONCE,s=QSXCR+Q6sek8bf92,i=4096";
    let out = proto.feed_bytes(&auth_sasl_continue_frame(server_first), &mut wb);

    assert_eq!(out.len(), 2, "nonce mismatch → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ScramHandshakeFailure { .. }),
        "expected ScramError for nonce mismatch, got {cause:?}",
    );
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
    let secret = bsql_postgres_proto::Sensitive::new([42u8; 4]);
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
    let pw = bsql_postgres_proto::Password::try_from_bytes(b"hunter2").unwrap_or_else(|_| {
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
    let pw = bsql_postgres_proto::Password::try_from_bytes(b"s3cret").unwrap_or_else(|_| {
        panic!("valid password must construct")
    });
    let cred = bsql_postgres_proto::Credentials::ScramPassword(bsql_postgres_proto::Sensitive::new(pw));
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
// Out-of-band ParameterStatus regression guard.
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
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

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
        matches!(proto.state(), ConnectingState::HandshakeReady { .. }),
        "handshake should land in HandshakeReady, got {:?}",
        proto.state(),
    );

    // Now simulate PG sending `ParameterStatus` in idle (e.g. after
    // a `SET` command finishes). The frame is recorded silently —
    // a naive shape without the unsolicited-PS filter would
    // misclassify as `UnexpectedFrame` → CloseSocket.
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "America/New_York"), &mut wb);
    assert_eq!(out.len(), 0, "unsolicited PS in Idle emits no actions");
    assert!(
        matches!(proto.state(), ConnectingState::HandshakeReady { .. }),
        "state must remain HandshakeReady after unsolicited PS, got {:?}",
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
/// The PS is recorded pre-dispatch and the Ping remains in flight
/// — a naive shape without the pre-dispatch filter would corrupt
/// the in-flight ping (PS misclassified as an unexpected frame,
/// failing the Ping reply).
#[test]
fn unsolicited_param_status_in_awaiting_ping_reply_is_recorded() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);
    // Explicit `drop` (see preceding test for rationale).
    _ = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    _ = proto.feed_bytes(&backend_key_data_frame(1, 1), &mut wb);
    _ = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);

    // The handshake completed; transition to `<ActivePhase>` before
    // pushing post-handshake commands.
    let mut proto = match proto.into_active() {
        Ok(p) => p,
        Err(_) => panic!("test fixture: handshake did not complete"),
    };

    // Send a ping. State → PingAwaitingRfq.
    let (ping_reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    proto.push_or_panic(bsql_postgres_proto::push_command::Ping { reply: ping_reply }, &mut wb);
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
    assert!(matches!(proto.state(), ActiveState::PingAwaitingRfq(_)));

    // Server emits ParameterStatus before RFQ (e.g. an ALTER SYSTEM
    // committed during our ping's round-trip).
    let out = proto.feed_bytes(&param_status_frame("client_encoding", "LATIN1"), &mut wb);
    assert_eq!(out.len(), 0, "PS during flight emits no actions");
    assert!(
        matches!(proto.state(), ActiveState::PingAwaitingRfq(_)),
        "PS must not disturb the PingAwaitingRfq state",
    );
    // Typed Encoding.
    assert_eq!(
        proto.session_params().client_encoding,
        Some(bsql_postgres_proto::Encoding::Latin1),
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
    assert!(matches!(proto.state(), ActiveState::Idle));
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
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_scram_consume(proto, &mut wb,"user", "pencil");
    // Server offers SASL → we emit SASLInitialResponse → state is now
    // ConnectingScramAwaitingServerFirst. Setup frame's actions discarded
    // explicitly — `let _ = ...` is banned.
    _ = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    assert!(matches!(
        proto.state(),
        ConnectingState::ScramAwaitingServerFirst { .. },
    ));

    // Unsolicited ParameterStatus during SCRAM — must be classified.
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "UTC"), &mut wb);
    assert_eq!(out.len(), 2, "PS during SCRAM → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &startup_raw);
        }
        other => panic!("unexpected action sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'S'),
        "expected UnexpectedFrame('S'), got {cause:?}",
    );
    assert!(matches!(proto.state(), ConnectingState::Errored(_)));
}

/// Invariant (spec): ParameterStatus arriving in a pre-auth state
/// (before AuthOk) is out-of-spec and must be classified as
/// UnexpectedFrame — `allows_unsolicited_param_status` returns false
/// for Connecting* pre-auth states.
///
/// This pins the other side of the unsolicited-PS filter: flipping
/// a Connecting* state from `false` to `true` in the exhaustive
/// match would compile and silently accept server frames that
/// contravene the auth handshake.
#[test]
fn param_status_during_pre_auth_is_unexpected() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);
    // State is now ConnectingStartup. Server should send
    // AuthenticationOk / SASL / ErrorResponse — not ParameterStatus.
    let out = proto.feed_bytes(&param_status_frame("TimeZone", "UTC"), &mut wb);
    assert_eq!(out.len(), 2, "pre-auth PS → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id: failed }, Action::CloseSocket] => {
            assert_eq!(failed, &startup_raw);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'S'),
        "expected UnexpectedFrame('S'), got {cause:?}",
    );
    assert!(matches!(proto.state(), ConnectingState::Errored(_)));
}

/// ParameterStatus with a missing trailing NUL (wire-spec §55.7
/// violation) is classified as `MalformedPayload` and silently
/// dropped — explicit `Option` match routes missing-NUL there.
/// A naive `strip_suffix(b"\0").unwrap_or(value_region)` shape
/// would silently absorb the malformed payload with the wrong
/// value.
///
/// Observable surface: the key is NOT recorded in SessionParams
/// (server_version stays None). Additionally the connection stays
/// in a normal post-auth state — ParameterStatus classification
/// failures don't tear down the connection (they're forward-compat
/// tolerant per PG §55.7 unsolicited-message semantics).
#[test]
fn param_status_missing_trailing_nul_classified_as_malformed() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_trust_consume(proto, &mut wb,"testuser", None);

    // Complete handshake: AuthOk → BackendKeyData → RFQ → Idle.
    let _ = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    let _ = proto.feed_bytes(&backend_key_data_frame(12345, 67890), &mut wb);
    let _ = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert!(
        matches!(proto.state(), ConnectingState::HandshakeReady { .. }),
        "handshake must complete to HandshakeReady — got {:?}",
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
        matches!(proto.state(), ConnectingState::HandshakeReady { .. }),
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

// ------------------------------------------------------------------
// (A) Cleartext-password auth handshake
// ------------------------------------------------------------------

/// Build an `AuthenticationCleartextPassword` frame: tag 'R',
/// length 8, sub-code 3.
fn auth_cleartext_password_frame() -> [u8; 9] {
    [b'R', 0, 0, 0, 8, 0, 0, 0, 3]
}

/// Consume-self Startup push for
/// cleartext-auth tests.
fn startup_cleartext_consume(
    proto: PgProtocol<DisconnectedPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
    user: &str,
    password: &str,
) -> (NonZeroU64, PgProtocol<ConnectingPhase>) {
    let user_ident = Ident::try_from_str(user).unwrap_or_else(|e| panic!("bad user: {e}"));
    let pw = Password::try_from_str(password).unwrap_or_else(|e| panic!("bad pw: {e}"));
    let mut proto = proto;
    let (reply, reply_raw) = mint_reply_disconnected::<StartupKind>(&mut proto);
    let (actions, proto_connecting) = match proto.push_startup(
        user_ident,
        None,
        None,
        Credentials::CleartextPassword(Sensitive::new(pw)),
        reply,
        wb,
    ) {
        Ok(pair) => pair,
        Err(f) => panic!("test fixture: push_startup must succeed for Cleartext auth, got {:?}", f.cause),
    };
    let mut scratch: std::vec::Vec<u8> = std::vec::Vec::with_capacity(512);
    for action in actions {
        if let Action::SendBytes(b) = action {
            scratch.extend_from_slice(b);
        }
    }
    wb.clear();
    if wb.push_bytes(&scratch).is_err() {
        panic!("test fixture: rebuilt StartupMessage ({} B) overflowed WriteBuf", scratch.len());
    }
    (reply_raw, proto_connecting)
}

/// Spec conformance: full cleartext-auth handshake. StartupMessage
/// → AuthCleartextPassword → PasswordMessage emitted with the
/// password bytes + NUL terminator → AuthOk → BackendKeyData → RFQ
/// → Idle + Reply::StartupComplete.
#[test]
fn cleartext_auth_handshake_end_to_end() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
        let password = "secret123";

    // Step 1: push StartupMessage with cleartext credentials.
    let (startup_raw, mut proto) = startup_cleartext_consume(proto, &mut wb,"alice", password);
    assert!(matches!(
        proto.state(),
        ConnectingState::StartupCleartext { .. }
    ));

    // Drain the StartupMessage bytes from wb (don't need them
    // for this test) before next-write.
    wb.clear();

    // Step 2: server replies with AuthenticationCleartextPassword.
    // Client must respond with PasswordMessage.
    let out = proto.feed_bytes(&auth_cleartext_password_frame(), &mut wb);
    assert_eq!(
        out.len(),
        1,
        "AuthCleartextPassword must produce exactly one SendBytes \
         action (the PasswordMessage frame)",
    );
    match out.as_slice() {
        [Action::SendBytes(bytes)] => {
            // Frame shape: tag 'p' + BE u32 length + password + NUL.
            let expected_body_len = password.len().saturating_add(1); // password + NUL
            let expected_total = 5usize.saturating_add(expected_body_len);
            assert_eq!(
                bytes.len(),
                expected_total,
                "PasswordMessage frame size: tag(1) + length(4) + body({expected_body_len})",
            );
            assert_eq!(
                bytes.first().copied(),
                Some(b'p'),
                "PasswordMessage tag must be 'p'",
            );
            // Length field is BE u32 of the 4-byte length itself + body.
            let len_bytes: [u8; 4] = bytes
                .get(1..5)
                .and_then(|s| <[u8; 4]>::try_from(s).ok())
                .unwrap_or_else(|| panic!("frame too short for length field"));
            let declared_len = u32::from_be_bytes(len_bytes);
            let declared_len_usize = usize::try_from(declared_len)
                .unwrap_or_else(|_| panic!("declared_len overflows usize"));
            assert_eq!(
                declared_len_usize,
                4usize.saturating_add(expected_body_len),
                "length-field includes itself + body",
            );
            // Body: password bytes + NUL.
            let body = bytes.get(5..).unwrap_or_default();
            let pw_slice = body.get(..password.len()).unwrap_or_default();
            assert_eq!(
                pw_slice,
                password.as_bytes(),
                "password bytes copied verbatim",
            );
            assert_eq!(
                body.get(password.len()).copied(),
                Some(0u8),
                "PasswordMessage body must be NUL-terminated",
            );
        }
        other => panic!("expected single SendBytes, got {other:?}"),
    }
    assert!(
        matches!(proto.state(), ConnectingState::CleartextAwaitingAuthOk(_)),
        "after PasswordMessage emission, state must transition to AwaitingAuthOk",
    );

    // Step 3: server replies with AuthenticationOk.
    wb.clear();
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk produces silent state transition");
    assert!(matches!(
        proto.state(),
        ConnectingState::PostAuthAwaitingKey(_)
    ));

    // Step 4: BackendKeyData.
    let out = proto.feed_bytes(&backend_key_data_frame(54321, 98765), &mut wb);
    assert_eq!(out.len(), 0);
    assert!(matches!(
        proto.state(),
        ConnectingState::PostAuthHaveKey { .. }
    ));

    // Step 5: ReadyForQuery — completes the handshake.
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1, "RFQ completes handshake with DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(delivered_id, &startup_raw);
            match value {
                Reply::StartupComplete(p) => {
                    assert_eq!(p.pid, 54321);
                    assert_eq!(p.secret_key, 98765);
                }
                other => panic!("expected StartupComplete, got {other:?}"),
            }
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ConnectingState::HandshakeReady { .. }));
}

/// Spec conformance: ErrorResponse mid-cleartext-handshake → tier-3
/// classification, FailReply + CloseSocket, terminal Errored state.
#[test]
fn error_response_during_cleartext_startup() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_cleartext_consume(proto, &mut wb,"baduser", "wrong");
    wb.clear();

    let err_frame = error_response_frame("FATAL", "28P01", "password authentication failed");
    let out = proto.feed_bytes(&err_frame, &mut wb);

    assert_eq!(out.len(), 2, "ErrorResponse → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id: failed_id }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
        }
        other => panic!("unexpected action sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
        "expected ServerErrorResponse, got {cause:?}",
    );
}

/// Spec conformance: server sends wrong auth code (e.g. SASL) while
/// client is in cleartext-startup state → classified
/// `UnsupportedAuthMethod` (tier-1: cleartext client refuses to
/// engage with SASL even if it has credentials — security: prevent
/// downgrade-by-server attacks).
#[test]
fn cleartext_startup_rejects_sasl_offer() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_cleartext_consume(proto, &mut wb,"alice", "password");
    wb.clear();

    let out = proto.feed_bytes(&auth_sasl_frame(), &mut wb);
    assert_eq!(out.len(), 2, "wrong auth code → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id: failed_id }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
        }
        other => panic!("unexpected action sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::UnsupportedAuthMethod { sub_code } => {
            use bsql_postgres_proto::error::AuthSubCodeClass;
            use bsql_postgres_proto::wire::AuthSubCode;
            assert!(
                matches!(
                    sub_code,
                    AuthSubCodeClass::KnownButWrong(AuthSubCode::Sasl),
                ),
                "expected KnownButWrong(Sasl), got {sub_code:?}",
            );
        }
        other => panic!("expected UnsupportedAuthMethod, got {other:?}"),
    }
}

/// Debug-redaction pin: a `Credentials::CleartextPassword`
/// instance must NOT print the password text in its Debug output.
#[test]
fn credentials_cleartext_debug_does_not_leak_password() {
    let pw = Password::try_from_str("super-secret-cleartext").unwrap_or_else(|e| panic!("{e}"));
    let cred = bsql_postgres_proto::Credentials::CleartextPassword(bsql_postgres_proto::Sensitive::new(pw));
    let dbg = format!("{cred:?}");
    assert!(
        dbg.contains("REDACTED"),
        "Debug must contain REDACTED marker; got: {dbg}",
    );
    assert!(
        !dbg.contains("super-secret-cleartext"),
        "Debug must NOT contain raw password text; got: {dbg}",
    );
}

// ------------------------------------------------------------------
// (A) MD5-password auth handshake
// ------------------------------------------------------------------

/// Build an `AuthenticationMD5Password` frame: tag 'R',
/// length 12 (4 length + 4 sub-code + 4 salt), sub-code 5,
/// 4-byte salt body.
fn auth_md5_password_frame(salt: [u8; 4]) -> [u8; 13] {
    [
        b'R', 0, 0, 0, 12, // header: tag + length=12
        0, 0, 0, 5,        // sub-code 5
        salt[0], salt[1], salt[2], salt[3],
    ]
}

/// Consume-self Startup push for
/// MD5-auth tests.
fn startup_md5_consume(
    proto: PgProtocol<DisconnectedPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
    user: &str,
    password: &str,
) -> (NonZeroU64, PgProtocol<ConnectingPhase>) {
    let user_ident = Ident::try_from_str(user).unwrap_or_else(|e| panic!("bad user: {e}"));
    let pw = Password::try_from_str(password).unwrap_or_else(|e| panic!("bad pw: {e}"));
    let mut proto = proto;
    let (reply, reply_raw) = mint_reply_disconnected::<StartupKind>(&mut proto);
    let (actions, proto_connecting) = match proto.push_startup(
        user_ident,
        None,
        None,
        Credentials::Md5Password(Sensitive::new(pw)),
        reply,
        wb,
    ) {
        Ok(pair) => pair,
        Err(f) => panic!("test fixture: push_startup must succeed for MD5 auth, got {:?}", f.cause),
    };
    let mut scratch: std::vec::Vec<u8> = std::vec::Vec::with_capacity(512);
    for action in actions {
        if let Action::SendBytes(b) = action {
            scratch.extend_from_slice(b);
        }
    }
    wb.clear();
    if wb.push_bytes(&scratch).is_err() {
        panic!("test fixture: rebuilt StartupMessage ({} B) overflowed WriteBuf", scratch.len());
    }
    (reply_raw, proto_connecting)
}

/// Compute the expected MD5 password response body using the
/// `md-5` library directly. The integration test asserts that the
/// dispatcher's emitted bytes match this independent computation
/// — a regression in input ordering, salt placement, or hex
/// encoding produces a divergent response.
fn expected_md5_response_body(password: &[u8], username: &[u8], salt: [u8; 4]) -> [u8; 35] {
    use md5::{Digest, Md5};
    let mut inner = Md5::new();
    inner.update(password);
    inner.update(username);
    let inner_digest: [u8; 16] = inner.finalize().into();

    let mut inner_hex = [0u8; 32];
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (i, byte) in inner_digest.iter().enumerate() {
        let hi = usize::from(byte >> 4);
        let lo = usize::from(byte & 0x0f);
        if let (Some(h), Some(l)) = (HEX.get(hi).copied(), HEX.get(lo).copied()) {
            if let Some(slot) = inner_hex.get_mut(i.saturating_mul(2)) {
                *slot = h;
            }
            if let Some(slot) = inner_hex.get_mut(i.saturating_mul(2).saturating_add(1)) {
                *slot = l;
            }
        }
    }

    let mut outer = Md5::new();
    outer.update(inner_hex.as_slice());
    outer.update(salt);
    let outer_digest: [u8; 16] = outer.finalize().into();

    let mut outer_hex = [0u8; 32];
    for (i, byte) in outer_digest.iter().enumerate() {
        let hi = usize::from(byte >> 4);
        let lo = usize::from(byte & 0x0f);
        if let (Some(h), Some(l)) = (HEX.get(hi).copied(), HEX.get(lo).copied()) {
            if let Some(slot) = outer_hex.get_mut(i.saturating_mul(2)) {
                *slot = h;
            }
            if let Some(slot) = outer_hex.get_mut(i.saturating_mul(2).saturating_add(1)) {
                *slot = l;
            }
        }
    }

    let mut response = [0u8; 35];
    if let Some(prefix) = response.get_mut(..3) {
        prefix.copy_from_slice(b"md5");
    }
    if let Some(tail) = response.get_mut(3..) {
        tail.copy_from_slice(&outer_hex);
    }
    response
}

/// **Known Answer Test**: hardcoded expected
/// digest computed externally via Python `hashlib.md5`. Catches
/// drift between our `compute_response_body` implementation and
/// the canonical PG/RFC 1321 algorithm. Crucially, this test
/// would FAIL if `Ident::as_bytes()` returned padded bytes
/// instead of the populated prefix — the digest would diverge
/// from the externally-computed value. The earlier
/// `algorithm_shape_pw_then_user_not_user_then_pw` test uses an
/// inline mirror with the SAME `md-5` library and SAME `as_bytes`
/// call, so it's self-referential and would NOT catch padding
/// bugs. This file's KAT closes that gap.
///
/// Reference computation (Python):
/// ```python
/// import hashlib
/// inner = hashlib.md5(b'secretalice').hexdigest()
/// # = '4a0a68b43b6cd5cf266fa02f196e2371'
/// outer = hashlib.md5(inner.encode() + bytes([0xde,0xad,0xbe,0xef])).hexdigest()
/// # = '3e1d73ba00a55e8805aa0277d29996c5'
/// response = 'md5' + outer
/// # = 'md53e1d73ba00a55e8805aa0277d29996c5'
/// ```
#[test]
fn md5_auth_kat_secret_alice_deadbeef() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_md5_consume(proto, &mut wb,"alice", "secret");
    wb.clear();

    let salt: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];
    let out = proto.feed_bytes(&auth_md5_password_frame(salt), &mut wb);
    let expected_response: &[u8] = b"md53e1d73ba00a55e8805aa0277d29996c5";
    match out.as_slice() {
        [Action::SendBytes(bytes)] => {
            // Wire frame: 'p' + len(4) + body(35) + NUL.
            let body = bytes.get(5..40).unwrap_or_default();
            assert_eq!(
                body, expected_response,
                "MD5 response body must match externally-computed reference; \
                 a divergence here indicates either a library drift, an \
                 algorithm-shape regression, or a username/password padding \
                 bug (Ident::as_bytes returning padded bytes)",
            );
        }
        other => panic!("expected single SendBytes, got {other:?}"),
    }
}

/// KAT for empty username — PG accepts empty SASL user (the
/// real username travels in StartupMessage's `user=` field). The
/// MD5 inner hash incorporates whatever username we pass, even
/// if empty. Reference: `md5(md5("p" || "") || 0x00000000) = ...`.
///
/// Note: we cannot construct an `Ident` with empty content via
/// `try_from_str("")` (Ident requires non-empty). This KAT is
/// covered at the `compute_response_body` level (lib unit
/// `empty_username_does_not_panic`); here we pin the FULL
/// startup-flow path with the smallest legal Ident: a single
/// non-empty character.
///
/// Reference computation (Python):
/// ```python
/// inner = hashlib.md5(b'mypasswordalice').hexdigest()
/// outer = hashlib.md5(inner.encode() + bytes([0x12,0x34,0x56,0x78])).hexdigest()
/// # = '6570e2ae51c521b1f1ef46c78c104163'
/// ```
#[test]
fn md5_auth_kat_mypassword_alice_seq_salt() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_startup_raw, mut proto) = startup_md5_consume(proto, &mut wb,"alice", "mypassword");
    wb.clear();

    let salt: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
    let out = proto.feed_bytes(&auth_md5_password_frame(salt), &mut wb);
    let expected_response: &[u8] = b"md56570e2ae51c521b1f1ef46c78c104163";
    match out.as_slice() {
        [Action::SendBytes(bytes)] => {
            let body = bytes.get(5..40).unwrap_or_default();
            assert_eq!(
                body, expected_response,
                "MD5 response must match externally-computed Python reference",
            );
        }
        other => panic!("expected single SendBytes, got {other:?}"),
    }
}

/// Spec conformance: full MD5-password handshake. StartupMessage
/// → AuthMD5Password (with 4-byte salt) → PasswordMessage with
/// `"md5" + 32 hex chars + NUL` → AuthOk → BackendKeyData → RFQ
/// → Idle + Reply::StartupComplete.
#[test]
fn md5_auth_handshake_end_to_end() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
        let password = "secret";
    let user = "alice";
    let salt: [u8; 4] = [0xde, 0xad, 0xbe, 0xef];

    let (startup_raw, mut proto) = startup_md5_consume(proto, &mut wb,user, password);
    assert!(matches!(
        proto.state(),
        ConnectingState::StartupMd5 { .. }
    ));
    wb.clear();

    // Server replies with AuthMD5Password + 4-byte salt.
    let out = proto.feed_bytes(&auth_md5_password_frame(salt), &mut wb);
    assert_eq!(
        out.len(),
        1,
        "AuthMD5Password must produce one SendBytes (PasswordMessage)",
    );
    let expected_body = expected_md5_response_body(password.as_bytes(), user.as_bytes(), salt);
    match out.as_slice() {
        [Action::SendBytes(bytes)] => {
            // Frame: tag 'p' + BE u32 length + 35-byte body + NUL.
            assert_eq!(bytes.len(), 1 + 4 + 35 + 1, "MD5 PasswordMessage frame size");
            assert_eq!(
                bytes.first().copied(),
                Some(b'p'),
                "MD5 PasswordMessage tag must be 'p'",
            );
            let len_bytes: [u8; 4] = bytes
                .get(1..5)
                .and_then(|s| <[u8; 4]>::try_from(s).ok())
                .unwrap_or_else(|| panic!("frame too short"));
            let declared_len = u32::from_be_bytes(len_bytes);
            let declared_len_usize = usize::try_from(declared_len)
                .unwrap_or_else(|_| panic!("len overflow"));
            assert_eq!(
                declared_len_usize,
                4 + 35 + 1,
                "length-field includes itself + body + NUL",
            );
            // Body must equal the independently-computed expected
            // response.
            let body = bytes.get(5..40).unwrap_or_default();
            assert_eq!(
                body, &expected_body,
                "MD5 response body byte-mismatch",
            );
            // Trailing NUL.
            assert_eq!(
                bytes.get(40).copied(),
                Some(0u8),
                "MD5 PasswordMessage body must be NUL-terminated",
            );
        }
        other => panic!("expected single SendBytes, got {other:?}"),
    }
    assert!(matches!(
        proto.state(),
        ConnectingState::Md5AwaitingAuthOk(_)
    ));

    // Server replies AuthOk; transition to PostAuthAwaitingKey.
    wb.clear();
    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk produces silent state transition");
    assert!(matches!(
        proto.state(),
        ConnectingState::PostAuthAwaitingKey(_)
    ));

    // BackendKeyData + RFQ complete the handshake.
    let out = proto.feed_bytes(&backend_key_data_frame(11111, 22222), &mut wb);
    assert_eq!(out.len(), 0);
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(delivered_id, &startup_raw);
            match value {
                Reply::StartupComplete(p) => {
                    assert_eq!(p.pid, 11111);
                    assert_eq!(p.secret_key, 22222);
                }
                other => panic!("expected StartupComplete, got {other:?}"),
            }
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ConnectingState::HandshakeReady { .. }));
}

/// Wrong salt length (e.g. 3 bytes instead of 4) → tier-3
/// `MalformedAuthentication` rejection. Server-side framing bug
/// or active interference.
#[test]
fn md5_auth_rejects_wrong_salt_length() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_md5_consume(proto, &mut wb,"alice", "secret");
    wb.clear();

    // Build a malformed AuthMD5Password with only 3 salt bytes
    // (length=11 instead of 12).
    let frame: [u8; 12] = [
        b'R', 0, 0, 0, 11, // length=11 (4 + 4 sub-code + 3 salt)
        0, 0, 0, 5,        // sub-code 5
        0xaa, 0xbb, 0xcc,  // 3-byte salt — protocol violation
    ];
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(
        out.len(),
        2,
        "malformed MD5 salt → FailReply + CloseSocket",
    );
    match out.as_slice() {
        [Action::FailReply { id: failed_id }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::MalformedAuthentication { .. }),
        "expected MalformedAuthentication, got {cause:?}",
    );
}

/// Server tries to coerce MD5 client into cleartext (downgrade) →
/// rejected as `UnsupportedAuthMethod::KnownButWrong(CleartextPassword)`.
/// Symmetric with cleartext + SCRAM downgrade-rejection pins.
#[test]
fn md5_startup_rejects_cleartext_offer() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (startup_raw, mut proto) = startup_md5_consume(proto, &mut wb,"alice", "password");
    wb.clear();

    // AuthCleartextPassword frame: tag 'R', length 8, sub-code 3.
    let cleartext_frame: [u8; 9] = [b'R', 0, 0, 0, 8, 0, 0, 0, 3];
    let out = proto.feed_bytes(&cleartext_frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id: failed_id }, Action::CloseSocket] => {
            assert_eq!(failed_id, &startup_raw);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::UnsupportedAuthMethod { sub_code } => {
            use bsql_postgres_proto::error::AuthSubCodeClass;
            use bsql_postgres_proto::wire::AuthSubCode;
            assert!(
                matches!(
                    sub_code,
                    AuthSubCodeClass::KnownButWrong(AuthSubCode::CleartextPassword),
                ),
                "expected KnownButWrong(CleartextPassword), got {sub_code:?}",
            );
        }
        other => panic!("expected UnsupportedAuthMethod, got {other:?}"),
    }
}

/// Debug-redaction pin: `Credentials::Md5Password` Debug must NOT
/// print the password.
#[test]
fn credentials_md5_debug_does_not_leak_password() {
    let pw = Password::try_from_str("super-secret-md5").unwrap_or_else(|e| panic!("{e}"));
    let cred = bsql_postgres_proto::Credentials::Md5Password(bsql_postgres_proto::Sensitive::new(pw));
    let dbg = format!("{cred:?}");
    assert!(
        dbg.contains("REDACTED"),
        "Debug must contain REDACTED; got: {dbg}",
    );
    assert!(
        !dbg.contains("super-secret-md5"),
        "Debug must NOT contain raw password; got: {dbg}",
    );
}

// ==================================================================
// (B) Negative-path & symmetric coverage for cleartext + MD5 auth:
// comprehensive dispatcher and downgrade-rejection test suite.
// Crypto + auth cannot be "good enough" covered; every transition
// × every input combination is exercised to pin tier-1 invariants
// against future drift.
// ==================================================================

/// Build an Authentication frame whose body is exactly the 4-byte
/// sub-code (no trailing data): tag 'R', length 8, sub-code as BE u32.
/// Suitable for sub-codes that carry no body (Ok=0, Cleartext=3,
/// arbitrary Unknown).
fn auth_subcode_only_frame(subcode: u32) -> [u8; 9] {
    let bytes = subcode.to_be_bytes();
    [b'R', 0, 0, 0, 8, bytes[0], bytes[1], bytes[2], bytes[3]]
}

/// Helper: assert the FailReply path with `UnsupportedAuthMethod
/// { sub_code: KnownButWrong(expected) }`. Typed for
/// `<ConnectingPhase>` — all callers in this file are
/// mid-handshake.
fn assert_known_but_wrong<const N: usize>(
    proto: &mut PgProtocol<ConnectingPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
    frame: [u8; N],
    expected_reply_raw: NonZeroU64,
    expected_subcode: bsql_postgres_proto::wire::AuthSubCode,
) {
    let out = proto.feed_bytes(&frame, wb);
    assert_eq!(out.len(), 2, "wrong sub-code → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &expected_reply_raw);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::UnsupportedAuthMethod { sub_code } => {
            use bsql_postgres_proto::error::AuthSubCodeClass;
            assert!(
                matches!(sub_code, AuthSubCodeClass::KnownButWrong(c) if c == expected_subcode),
                "expected KnownButWrong({expected_subcode:?}), got {sub_code:?}",
            );
        }
        other => panic!("expected UnsupportedAuthMethod, got {other:?}"),
    }
}

/// Helper: assert UnexpectedFrame classification with the given
/// frame tag. Typed for `<ConnectingPhase>`.
fn assert_unexpected_frame<const N: usize>(
    proto: &mut PgProtocol<ConnectingPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
    frame: [u8; N],
    expected_reply_raw: NonZeroU64,
    expected_tag_byte: u8,
) {
    let out = proto.feed_bytes(&frame, wb);
    assert_eq!(out.len(), 2, "unexpected frame → FailReply + CloseSocket");
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &expected_reply_raw);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::UnexpectedFrame { tag } => {
            assert_eq!(tag.byte(), expected_tag_byte, "tag byte mismatch");
        }
        other => panic!("expected UnexpectedFrame, got {other:?}"),
    }
}

// ----- MD5 startup state — negative AuthSubCode paths -----

#[test]
fn md5_startup_rejects_auth_ok_subcode() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(&mut proto, &mut wb, auth_subcode_only_frame(0), raw_id, AuthSubCode::Ok);
}

#[test]
fn md5_startup_rejects_auth_sasl_continue_subcode() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(&mut proto, &mut wb, auth_subcode_only_frame(11), raw_id, AuthSubCode::SaslContinue);
}

#[test]
fn md5_startup_rejects_auth_sasl_final_subcode() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(&mut proto, &mut wb, auth_subcode_only_frame(12), raw_id, AuthSubCode::SaslFinal);
}

#[test]
fn md5_startup_rejects_unknown_subcode() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    wb.clear();

    let frame = auth_subcode_only_frame(99);
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::UnsupportedAuthMethod { sub_code } => {
            use bsql_postgres_proto::error::AuthSubCodeClass;
            let expected = match core::num::NonZeroU32::new(99) {
                Some(n) => n,
                None => panic!("99 is non-zero"),
            };
            assert!(
                matches!(sub_code, AuthSubCodeClass::Unknown(n) if n == expected),
                "expected Unknown(99), got {sub_code:?}",
            );
        }
        other => panic!("expected UnsupportedAuthMethod, got {other:?}"),
    }
}

// ----- MD5AwaitingAuthOk — comprehensive negative paths -----

/// Helper: drive proto to MD5AwaitingAuthOk state with valid
/// MD5 challenge + response.
/// Returns the minted raw ID so callers can assert on round-trip.
///
/// Consume-self shape — takes
/// `<DisconnectedPhase>`, returns `(NonZeroU64, <ConnectingPhase>)`.
fn drive_to_md5_awaiting_authok(
    proto: PgProtocol<DisconnectedPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
) -> (NonZeroU64, PgProtocol<ConnectingPhase>) {
    let (raw_id, mut proto) = startup_md5_consume(proto, wb, "user", "password");
    wb.clear();
    let _ = proto.feed_bytes(&auth_md5_password_frame([1, 2, 3, 4]), wb);
    assert!(
        matches!(proto.state(), ConnectingState::Md5AwaitingAuthOk(_)),
        "test setup invariant",
    );
    wb.clear();
    (raw_id, proto)
}

#[test]
fn md5_awaiting_authok_accepts_auth_ok() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_raw_id, mut proto) = drive_to_md5_awaiting_authok(proto, &mut wb);

    let out = proto.feed_bytes(&auth_ok_frame(), &mut wb);
    assert_eq!(out.len(), 0, "AuthOk → silent state transition");
    assert!(matches!(
        proto.state(),
        ConnectingState::PostAuthAwaitingKey(_),
    ));
}

#[test]
fn md5_awaiting_authok_rejects_cleartext_subcode() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_md5_awaiting_authok(proto, &mut wb);
    // After password sent, server replying with another auth
    // request (cleartext sub-code) is a protocol violation.
    assert_unexpected_frame(&mut proto, &mut wb, auth_subcode_only_frame(3), raw_id, b'R');
}

#[test]
fn md5_awaiting_authok_rejects_md5_subcode() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_md5_awaiting_authok(proto, &mut wb);
    assert_unexpected_frame(&mut proto, &mut wb, auth_md5_password_frame([0; 4]), raw_id, b'R');
}

#[test]
fn md5_awaiting_authok_rejects_sasl_subcode() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_md5_awaiting_authok(proto, &mut wb);
    let frame = auth_sasl_frame();
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'R'),
        "expected UnexpectedFrame{{R}}, got {cause:?}",
    );
}

#[test]
fn md5_awaiting_authok_handles_error_response() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_md5_awaiting_authok(proto, &mut wb);

    let err_frame = error_response_frame("FATAL", "28P01", "auth failed");
    let out = proto.feed_bytes(&err_frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
        "expected ServerErrorResponse, got {cause:?}",
    );
}

#[test]
fn md5_awaiting_authok_rejects_random_tag() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_md5_awaiting_authok(proto, &mut wb);
    // 'Z' is the ReadyForQuery tag; arriving here pre-auth is
    // out of order. Build a synthetic 6-byte RFQ frame.
    let frame = [b'Z', 0, 0, 0, 5, b'I'];
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnexpectedFrame { tag } if tag.byte() == b'Z'),
        "expected UnexpectedFrame{{Z}}, got {cause:?}",
    );
}

// ----- Cleartext startup — extended negative paths -----

#[test]
fn cleartext_startup_rejects_auth_ok_subcode() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_cleartext_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(&mut proto, &mut wb, auth_subcode_only_frame(0), raw_id, AuthSubCode::Ok);
}

#[test]
fn cleartext_startup_rejects_md5_password_offer() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_cleartext_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(
        &mut proto,
        &mut wb,
        auth_md5_password_frame([0xaa, 0xbb, 0xcc, 0xdd]),
        raw_id,
        AuthSubCode::Md5Password,
    );
}

#[test]
fn cleartext_startup_rejects_unknown_subcode() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_raw_id, mut proto) = startup_cleartext_consume(proto, &mut wb,"u", "p");
    wb.clear();
    let out = proto.feed_bytes(&auth_subcode_only_frame(77), &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { .. }, Action::CloseSocket] => {}
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::UnsupportedAuthMethod { .. }),
        "expected UnsupportedAuthMethod for unknown sub-code; got {cause:?}",
    );
}

// ----- CleartextAwaitingAuthOk — negative paths -----

/// Returns the minted raw ID so callers can assert on round-trip.
/// Consume-self shape.
fn drive_to_cleartext_awaiting_authok(
    proto: PgProtocol<DisconnectedPhase>,
    wb: &mut bsql_postgres_proto::WriteBuf,
) -> (NonZeroU64, PgProtocol<ConnectingPhase>) {
    let (raw_id, mut proto) = startup_cleartext_consume(proto, wb, "user", "password");
    wb.clear();
    let _ = proto.feed_bytes(&auth_subcode_only_frame(3), wb);
    assert!(
        matches!(proto.state(), ConnectingState::CleartextAwaitingAuthOk(_)),
        "test setup invariant",
    );
    wb.clear();
    (raw_id, proto)
}

#[test]
fn cleartext_awaiting_authok_rejects_md5_subcode() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_cleartext_awaiting_authok(proto, &mut wb);
    assert_unexpected_frame(
        &mut proto,
        &mut wb,
        auth_md5_password_frame([0; 4]),
        raw_id,
        b'R',
    );
}

#[test]
fn cleartext_awaiting_authok_handles_error_response() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = drive_to_cleartext_awaiting_authok(proto, &mut wb);

    let err_frame = error_response_frame("FATAL", "28P01", "auth failed");
    let out = proto.feed_bytes(&err_frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
        "expected ServerErrorResponse, got {cause:?}",
    );
}

// ----- Trust + SCRAM — symmetric downgrade rejection of new codes -----

#[test]
fn trust_startup_rejects_cleartext_password_offer() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_trust_consume(proto, &mut wb,"u", None);
    wb.clear();
    assert_known_but_wrong(
        &mut proto,
        &mut wb,
        auth_subcode_only_frame(3),
        raw_id,
        AuthSubCode::CleartextPassword,
    );
}

#[test]
fn trust_startup_rejects_md5_password_offer() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_trust_consume(proto, &mut wb,"u", None);
    wb.clear();
    assert_known_but_wrong(
        &mut proto,
        &mut wb,
        auth_md5_password_frame([0; 4]),
        raw_id,
        AuthSubCode::Md5Password,
    );
}

#[test]
fn scram_startup_rejects_cleartext_password_offer() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_scram_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(
        &mut proto,
        &mut wb,
        auth_subcode_only_frame(3),
        raw_id,
        AuthSubCode::CleartextPassword,
    );
}

#[test]
fn scram_startup_rejects_md5_password_offer() {
    use bsql_postgres_proto::wire::AuthSubCode;
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_scram_consume(proto, &mut wb,"u", "p");
    wb.clear();
    assert_known_but_wrong(
        &mut proto,
        &mut wb,
        auth_md5_password_frame([0; 4]),
        raw_id,
        AuthSubCode::Md5Password,
    );
}

// ----- ErrorResponse path coverage -----

#[test]
fn md5_startup_handles_error_response() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    wb.clear();

    let frame = error_response_frame("FATAL", "28000", "no pg_hba.conf entry");
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(matches!(cause, ProtocolError::ServerErrorResponse { .. }));
}

#[test]
fn md5_startup_handles_negotiate_protocol_version() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    wb.clear();
    let frame = negotiate_proto_version_frame();
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(matches!(cause, ProtocolError::UnsupportedProtocolOption));
}

#[test]
fn cleartext_startup_handles_negotiate_protocol_version() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (raw_id, mut proto) = startup_cleartext_consume(proto, &mut wb,"u", "p");
    wb.clear();
    let frame = negotiate_proto_version_frame();
    let out = proto.feed_bytes(&frame, &mut wb);
    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [Action::FailReply { id }, Action::CloseSocket] => {
            assert_eq!(id, &raw_id);
        }
        other => panic!("unexpected sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(matches!(cause, ProtocolError::UnsupportedProtocolOption));
}

// ----- State preservation under password handling -----

/// After a successful MD5 password emission, the password should
/// no longer be reachable through the state. We can verify
/// indirectly: the variant changes from
/// `ConnectingStartupMd5 { handshake: Box<...> }` (carries pw) to
/// `ConnectingMd5AwaitingAuthOk(reply)` (no pw field). Failing this
/// transition would mean the password lingers in the state-machine
/// envelope past the point it's needed.
#[test]
fn md5_state_post_dispatch_no_longer_carries_handshake() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_raw_id, mut proto) = startup_md5_consume(proto, &mut wb,"u", "p");
    // Pre-dispatch: state must be ConnectingStartupMd5.
    assert!(matches!(
        proto.state(),
        ConnectingState::StartupMd5 { .. },
    ));
    wb.clear();

    let _ = proto.feed_bytes(&auth_md5_password_frame([0; 4]), &mut wb);
    // Post-dispatch: state is the password-less variant.
    assert!(
        matches!(proto.state(), ConnectingState::Md5AwaitingAuthOk(_)),
        "post-PasswordMessage state must be password-less variant",
    );
}

#[test]
fn cleartext_state_post_dispatch_no_longer_carries_password() {
    let proto = PgProtocol::new();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (_raw_id, mut proto) = startup_cleartext_consume(proto, &mut wb,"u", "p");
    assert!(matches!(
        proto.state(),
        ConnectingState::StartupCleartext { .. },
    ));
    wb.clear();

    let _ = proto.feed_bytes(&auth_subcode_only_frame(3), &mut wb);
    assert!(
        matches!(proto.state(), ConnectingState::CleartextAwaitingAuthOk(_)),
        "post-PasswordMessage state must be password-less variant",
    );
}
