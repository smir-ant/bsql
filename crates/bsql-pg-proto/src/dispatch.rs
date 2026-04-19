//! `(prev_state, frame_tag) → outcome` matcher.
//!
//! The dispatcher is the **single** place the protocol decides what to
//! do with a freshly-parsed frame given the current state. The match is
//! exhaustive over `(state, tag)` pairs the Phase 1b flows can encounter;
//! adding a new state or tag is a build error until it is wired into
//! the matcher.
//!
//! # Payload contract — tier-1 via slice patterns
//!
//! The caller has already parsed the header (5 bytes: tag + 4-byte BE
//! length) and verified that the full frame is buffered. It passes the
//! dispatcher the **payload** — the bytes *after* the header, of length
//! `total_len - 5`. Every arm that needs to inspect bytes uses a slice
//! pattern (`[b0]`, `[b0, b1, ..]`, etc.) so the compiler enforces the
//! length / presence check.

use crate::action::{Action, Reply, SendBuf};
use crate::error::ProtocolError;
use crate::password::Credentials;
use crate::reply_id::ReplyId;
use crate::state::ProtoState;
use crate::wire::{
    AUTH_OK, AUTH_SASL, AUTH_SASL_CONTINUE, AUTH_SASL_FINAL, SCRAM_SHA_256_MECHANISM,
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_ERROR_RESPONSE, TAG_NEGOTIATE_PROTOCOL_VERSION,
    TAG_READY_FOR_QUERY,
};

/// What to do after dispatching a single frame.
#[derive(Debug)]
#[expect(clippy::large_enum_variant, reason = "no_alloc: Box unavailable; DispatchOutcome is a one-shot return, not stored")]
pub(crate) enum DispatchOutcome {
    /// Frame consumed; transition to `new_state`. Caller advances the
    /// read buffer and pushes `action` if present.
    Advanced {
        new_state: ProtoState,
        action: Option<Action>,
    },
    /// Frame rejected; connection irrecoverable. Caller tears down.
    Errored {
        reply_id: Option<ReplyId>,
        cause: ProtocolError,
    },
}

/// Dispatch a single frame.
pub(crate) fn dispatch(prev: ProtoState, tag: u8, payload: &[u8]) -> DispatchOutcome {
    match (prev, tag) {
        // =============================================================
        // Ping flow (Phase 1a, carried forward)
        // =============================================================
        (ProtoState::AwaitingPingReply(id), TAG_READY_FOR_QUERY) => match payload {
            [tx_status] => DispatchOutcome::Advanced {
                new_state: ProtoState::Idle,
                action: Some(Action::DeliverReply {
                    id: id.consume(),
                    value: Reply::Pong {
                        tx_status: *tx_status,
                    },
                }),
            },
            other => DispatchOutcome::Errored {
                reply_id: Some(id),
                cause: ProtocolError::MalformedReadyForQuery {
                    payload_len: other.len(),
                },
            },
        },
        (ProtoState::AwaitingPingReply(id), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(id),
                cause,
            }
        }
        (ProtoState::AwaitingPingReply(id), other) => DispatchOutcome::Errored {
            reply_id: Some(id),
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },

        // =============================================================
        // ConnectingStartup — awaiting initial auth response
        // =============================================================
        (ProtoState::ConnectingStartup { reply, credentials }, TAG_AUTHENTICATION) => {
            dispatch_auth_in_startup(reply, credentials, payload)
        }
        (ProtoState::ConnectingStartup { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
        (ProtoState::ConnectingStartup { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: ProtocolError::UnsupportedProtocolOption,
            }
        }
        (ProtoState::ConnectingStartup { reply, .. }, other) => DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },

        // =============================================================
        // SCRAM: awaiting server-first-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitServerFirst {
                reply,
                scram,
                client_first_bare,
                client_nonce_b64,
            },
            TAG_AUTHENTICATION,
        ) => {
            dispatch_auth_sasl_continue(reply, scram, client_first_bare, client_nonce_b64, payload)
        }
        (ProtoState::ConnectingScramAwaitServerFirst { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
        (ProtoState::ConnectingScramAwaitServerFirst { reply, .. }, other) => {
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: ProtocolError::UnexpectedFrame { tag: other },
            }
        }

        // =============================================================
        // SCRAM: awaiting server-final-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitServerFinal {
                reply,
                expected_server_sig,
            },
            TAG_AUTHENTICATION,
        ) => dispatch_auth_sasl_final(reply, expected_server_sig, payload),
        (ProtoState::ConnectingScramAwaitServerFinal { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
        (ProtoState::ConnectingScramAwaitServerFinal { reply, .. }, other) => {
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: ProtocolError::UnexpectedFrame { tag: other },
            }
        }

        // =============================================================
        // SCRAM: awaiting AuthenticationOk after server sig verified
        // =============================================================
        (ProtoState::ConnectingScramAwaitAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_scram(reply, payload)
        }
        (ProtoState::ConnectingScramAwaitAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
        (ProtoState::ConnectingScramAwaitAuthOk(reply), other) => DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },

        // =============================================================
        // Post-auth: waiting for BackendKeyData
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (ProtoState::ConnectingPostAuthWaitKey(reply), TAG_BACKEND_KEY_DATA) => {
            match parse_backend_key_data(payload) {
                Ok((pid, secret_key)) => DispatchOutcome::Advanced {
                    new_state: ProtoState::ConnectingPostAuthHaveKey {
                        reply,
                        pid,
                        secret_key,
                    },
                    action: None,
                },
                Err(cause) => DispatchOutcome::Errored {
                    reply_id: Some(reply),
                    cause,
                },
            }
        }
        (ProtoState::ConnectingPostAuthWaitKey(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
        (ProtoState::ConnectingPostAuthWaitKey(reply), other) => DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },

        // =============================================================
        // Post-auth: have BackendKeyData, waiting for ReadyForQuery
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (
            ProtoState::ConnectingPostAuthHaveKey {
                reply,
                pid,
                secret_key,
            },
            TAG_READY_FOR_QUERY,
        ) => match payload {
            [tx_status] => DispatchOutcome::Advanced {
                new_state: ProtoState::Idle,
                action: Some(Action::DeliverReply {
                    id: reply.consume(),
                    value: Reply::StartupComplete {
                        pid,
                        secret_key,
                        tx_status: *tx_status,
                    },
                }),
            },
            other => DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: ProtocolError::MalformedReadyForQuery {
                    payload_len: other.len(),
                },
            },
        },
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, other) => {
            DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: ProtocolError::UnexpectedFrame { tag: other },
            }
        }

        // =============================================================
        // Idle — unsolicited frames are out-of-spec
        // =============================================================
        (ProtoState::Idle, other) => DispatchOutcome::Errored {
            reply_id: None,
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },

        // =============================================================
        // Errored — terminal sink (Phase 1a pattern carried forward)
        // =============================================================
        (ProtoState::Errored(original), _) => DispatchOutcome::Advanced {
            new_state: ProtoState::Errored(original),
            action: None,
        },
    }
}

// -----------------------------------------------------------------
// Helper: parse Authentication sub-code from payload
// -----------------------------------------------------------------

/// Extract the 4-byte BE auth sub-code from an `'R'` payload.
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
fn auth_sub_code(payload: &[u8]) -> Result<(u32, &[u8]), ProtocolError> {
    match payload {
        [a, b, c, d, rest @ ..] => {
            let code = u32::from_be_bytes([*a, *b, *c, *d]);
            Ok((code, rest))
        }
        _ => Err(ProtocolError::MalformedAuthentication {
            payload_len: payload.len(),
        }),
    }
}

/// Dispatch an Authentication message while in ConnectingStartup.
fn dispatch_auth_in_startup(
    reply: ReplyId,
    credentials: Credentials,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
    };

    match code {
        AUTH_OK => {
            // Trust auth succeeded. Move to post-auth chain.
            DispatchOutcome::Advanced {
                new_state: ProtoState::ConnectingPostAuthWaitKey(reply),
                action: None,
            }
        }
        AUTH_SASL => {
            // Server wants SASL. Tier-1 boundary (audit A2): the
            // `Trust`-vs-`ScramPassword` discrimination happens here,
            // exactly once. Every downstream site takes `ScramSession`
            // by value or reference; the `Trust` variant cannot drift
            // into them.
            let scram = match crate::scram::session::ScramSession::try_from_credentials(
                credentials,
            ) {
                Ok(s) => s,
                Err(()) => {
                    return DispatchOutcome::Errored {
                        reply_id: Some(reply),
                        cause: ProtocolError::UnsupportedAuthMethod { sub_code: code },
                    };
                }
            };

            if !mechanism_list_contains_scram(rest) {
                return DispatchOutcome::Errored {
                    reply_id: Some(reply),
                    cause: ProtocolError::ScramError {
                        detail: heapless::String::try_from("no supported mechanism")
                            .unwrap_or_default(),
                    },
                };
            }

            // Build client-first-message and SASLInitialResponse.
            match build_sasl_initial_response(&scram) {
                Ok((send_buf, client_first_bare, client_nonce_b64)) => {
                    DispatchOutcome::Advanced {
                        new_state: ProtoState::ConnectingScramAwaitServerFirst {
                            reply,
                            scram,
                            client_first_bare,
                            client_nonce_b64,
                        },
                        action: Some(Action::SendBytes(send_buf)),
                    }
                }
                Err(cause) => DispatchOutcome::Errored {
                    reply_id: Some(reply),
                    cause,
                },
            }
        }
        _ => DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnsupportedAuthMethod { sub_code: code },
        },
    }
}

/// Check if the SASL mechanism list contains SCRAM-SHA-256.
fn mechanism_list_contains_scram(data: &[u8]) -> bool {
    // Mechanism names are NUL-separated, with an extra NUL terminator.
    // e.g.: b"SCRAM-SHA-256\0\0"
    for name in data.split(|b| *b == 0) {
        if name == SCRAM_SHA_256_MECHANISM {
            return true;
        }
    }
    false
}

/// Build the SASLInitialResponse frame for SCRAM-SHA-256.
///
/// Takes a [`ScramSession`] by shared reference. The parameter is
/// not dereferenced — the password is not needed until the
/// SASL-continue step — but the signature makes the call-site
/// typestate explicit: calling this function without constructing a
/// `ScramSession` first is a compile error. The
/// `Credentials`-vs-`ScramPassword` split happens exactly once at
/// [`ScramSession::try_from_credentials`] (audit A2). The `_` in
/// `_: &ScramSession` uses Rust's anonymous-parameter syntax — the
/// parameter shape is load-bearing, its binding is not.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// [`ScramSession::try_from_credentials`]: crate::scram::session::ScramSession::try_from_credentials
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
fn build_sasl_initial_response(
    _: &crate::scram::session::ScramSession,
) -> Result<
    (
        SendBuf,
        heapless::Vec<u8, 128>,
        heapless::Vec<u8, 48>,
    ),
    ProtocolError,
> {
    use crate::scram::wire;
    use crate::write_buf::WriteBuf;

    // SCRAM client-first uses the username in "n=<user>".
    // PostgreSQL already has the username from the StartupMessage;
    // an empty "n=" field is accepted per RFC 5802.
    let user_bytes: &[u8] = b"";

    let client_nonce_b64 = wire::generate_client_nonce().map_err(scram_err_from)?;

    let client_first_bare =
        wire::build_client_first_bare(user_bytes, &client_nonce_b64).map_err(scram_err_from)?;

    let client_first_msg =
        wire::build_client_first_message(user_bytes, &client_nonce_b64).map_err(scram_err_from)?;

    // Build SASLInitialResponse frame:
    // tag 'p', length-prefix, mechanism-name NUL, i32 body-length, body
    let mut wb = WriteBuf::new();
    wb.push_u8(crate::wire::TAG_SASL_RESPONSE)
        .map_err(|_| scram_buf_err())?;
    wb.with_length_prefix(|w| {
        // Mechanism name NUL-terminated
        w.push_bytes(SCRAM_SHA_256_MECHANISM)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_u8(0).map_err(|_| crate::write_buf::WriteBufFull)?;
        // Body length as i32
        let body_len =
            i32::try_from(client_first_msg.len()).map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_i32_be(body_len)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        // Body = client-first-message
        w.push_bytes(&client_first_msg)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        Ok(())
    })
    .map_err(|_| scram_buf_err())?;

    Ok((
        SendBuf::from_owned(wb.into_inner()),
        client_first_bare,
        client_nonce_b64,
    ))
}

/// Dispatch AuthenticationSASLContinue (server-first-message).
///
/// Takes a [`ScramSession`] by value — the `Trust`-vs-`ScramPassword`
/// discrimination was consumed at
/// [`ScramSession::try_from_credentials`] in the parent dispatch
/// call; this function cannot be reached with `Trust` credentials
/// because the state variant it destructures from
/// ([`ProtoState::ConnectingScramAwaitServerFirst`]) carries
/// `ScramSession`, not `Credentials`. Audit A2.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// [`ScramSession::try_from_credentials`]: crate::scram::session::ScramSession::try_from_credentials
fn dispatch_auth_sasl_continue(
    reply: ReplyId,
    scram: crate::scram::session::ScramSession,
    client_first_bare: heapless::Vec<u8, 128>,
    client_nonce_b64: heapless::Vec<u8, 48>,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
    };

    if code != AUTH_SASL_CONTINUE {
        return DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        };
    }

    // `rest` is the server-first-message body.
    let server_first =
        match crate::scram::wire::parse_server_first(rest, &client_nonce_b64) {
            Ok(sf) => sf,
            Err(e) => {
                return DispatchOutcome::Errored {
                    reply_id: Some(reply),
                    cause: scram_err_from(e),
                };
            }
        };

    // Password bytes come from the typed SCRAM session — no
    // Trust-vs-ScramPassword discrimination here (audit A2).
    let password_bytes = scram.password_bytes();

    // Build client-final-without-proof.
    let client_final_without_proof =
        match crate::scram::wire::build_client_final_without_proof(
            server_first.server_nonce.as_bytes(),
        ) {
            Ok(v) => v,
            Err(e) => {
                return DispatchOutcome::Errored {
                    reply_id: Some(reply),
                    cause: scram_err_from(e),
                };
            }
        };

    // Compute proof and expected server signature.
    //
    // AuthMessage = client-first-bare + "," + server-first + "," +
    // client-final-without-proof. The three components are passed
    // separately — compute_client_proof feeds them incrementally into
    // HMAC::update(), with zero intermediate buffer. No staging
    // buffer → no silent-truncation class → tier-1 by construction.
    let (proof, expected_server_sig) = crate::scram::crypto::compute_client_proof(
        password_bytes,
        &server_first.salt,
        server_first.iterations,
        &client_first_bare,
        rest,
        &client_final_without_proof,
    );

    // Base64-encode proof.
    let mut proof_b64_buf = [0u8; 64];
    let proof_b64_len =
        match crate::scram::wire::base64_encode_to_buf(proof.as_ref(), &mut proof_b64_buf) {
            Ok(n) => n,
            Err(_) => {
                return DispatchOutcome::Errored {
                    reply_id: Some(reply),
                    cause: scram_buf_err(),
                }
            }
        };
    let proof_b64 = match proof_b64_buf.get(..proof_b64_len) {
        Some(s) => s,
        None => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: scram_buf_err(),
            }
        }
    };

    // Build client-final-message.
    let client_final_msg = match crate::scram::wire::build_client_final_message(
        server_first.server_nonce.as_bytes(),
        proof_b64,
    ) {
        Ok(v) => v,
        Err(e) => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: scram_err_from(e),
            };
        }
    };

    // Build SASLResponse frame.
    let mut wb = crate::write_buf::WriteBuf::new();
    if wb.push_u8(crate::wire::TAG_SASL_RESPONSE).is_err()
        || wb
            .with_length_prefix(|w| w.push_bytes(&client_final_msg))
            .is_err()
    {
        return DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: scram_buf_err(),
        };
    }

    DispatchOutcome::Advanced {
        new_state: ProtoState::ConnectingScramAwaitServerFinal {
            reply,
            expected_server_sig,
        },
        action: Some(Action::SendBytes(SendBuf::from_owned(wb.into_inner()))),
    }
}

/// Dispatch AuthenticationSASLFinal (server-final-message).
fn dispatch_auth_sasl_final(
    reply: ReplyId,
    expected_server_sig: crate::scram::types::SecretDigest,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
    };

    if code != AUTH_SASL_FINAL {
        return DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        };
    }

    // Parse server-final-message.
    let received_sig = match crate::scram::wire::parse_server_final(rest) {
        Ok(sig) => sig,
        Err(e) => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause: scram_err_from(e),
            };
        }
    };

    // Constant-time comparison (DEF-039).
    if !bool::from(expected_server_sig.ct_eq(&received_sig)) {
        return DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::ScramError {
                detail: heapless::String::try_from("server signature mismatch")
                    .unwrap_or_default(),
            },
        };
    }

    // Signature matches. Await AuthenticationOk.
    DispatchOutcome::Advanced {
        new_state: ProtoState::ConnectingScramAwaitAuthOk(reply),
        action: None,
    }
}

/// Dispatch AuthenticationOk after SCRAM verification.
fn dispatch_auth_ok_after_scram(reply: ReplyId, payload: &[u8]) -> DispatchOutcome {
    let (code, _rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return DispatchOutcome::Errored {
                reply_id: Some(reply),
                cause,
            }
        }
    };

    if code != AUTH_OK {
        return DispatchOutcome::Errored {
            reply_id: Some(reply),
            cause: ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        };
    }

    DispatchOutcome::Advanced {
        new_state: ProtoState::ConnectingPostAuthWaitKey(reply),
        action: None,
    }
}

// -----------------------------------------------------------------
// Helper: parse ErrorResponse typed fields
// -----------------------------------------------------------------

/// Parse an ErrorResponse payload into a classified error.
///
/// PG ErrorResponse body: series of typed fields, each = type-byte +
/// NUL-terminated string. Terminated by a bare NUL (0x00). We extract
/// 'S' (severity), 'C' (code), 'M' (message), 'D' (detail), 'H' (hint).
///
/// Cold path: called only when the server emits an `ErrorResponse`
/// frame (`'E'` tag). The `#[cold]` attribute tells LLVM to keep the
/// body out of hot-path inlining scope.
#[cold]
fn parse_error_response(payload: &[u8]) -> ProtocolError {
    let mut severity = heapless::String::new();
    let mut code = heapless::String::new();
    let mut message = heapless::String::new();
    let mut detail = heapless::String::new();
    let mut hint = heapless::String::new();

    let mut pos: usize = 0;
    loop {
        let field_type = match payload.get(pos) {
            Some(0) | None => break, // Terminator or end of payload.
            Some(b) => *b,
        };
        pos = match pos.checked_add(1) {
            Some(p) => p,
            None => break,
        };

        // Find NUL terminator for this field's value.
        let start = pos;
        while let Some(b) = payload.get(pos) {
            if *b == 0 {
                break;
            }
            pos = match pos.checked_add(1) {
                Some(p) => p,
                None => break,
            };
        }
        let value_bytes = payload.get(start..pos).unwrap_or(&[]);
        let value_str = core::str::from_utf8(value_bytes).unwrap_or("");

        // Skip past the NUL terminator.
        pos = match pos.checked_add(1) {
            Some(p) => p,
            None => break,
        };

        match field_type {
            b'S' | b'V' if severity.is_empty() => {
                severity = heapless::String::try_from(value_str).unwrap_or_default();
            }
            b'C' => {
                code = heapless::String::try_from(value_str).unwrap_or_default();
            }
            b'M' => {
                message = heapless::String::try_from(value_str).unwrap_or_default();
            }
            b'D' => {
                detail = heapless::String::try_from(value_str).unwrap_or_default();
            }
            b'H' => {
                hint = heapless::String::try_from(value_str).unwrap_or_default();
            }
            _ => {} // Unknown field type — skip.
        }
    }

    ProtocolError::ServerErrorResponse {
        severity,
        code,
        message,
        detail,
        hint,
    }
}

// -----------------------------------------------------------------
// Helper: parse BackendKeyData
// -----------------------------------------------------------------

/// Parse BackendKeyData payload: 8 bytes = pid(i32 BE) + secret_key(i32 BE).
///
/// Cold path: called once per connection at end of startup handshake.
/// Not on any per-frame or per-query hot path.
#[cold]
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
fn parse_backend_key_data(payload: &[u8]) -> Result<(i32, i32), ProtocolError> {
    match payload {
        [a, b, c, d, e, f, g, h] => {
            let pid = i32::from_be_bytes([*a, *b, *c, *d]);
            let secret_key = i32::from_be_bytes([*e, *f, *g, *h]);
            Ok((pid, secret_key))
        }
        other => Err(ProtocolError::MalformedBackendKeyData {
            payload_len: other.len(),
        }),
    }
}

/// Convenience: SCRAM buffer overflow error.
fn scram_buf_err() -> ProtocolError {
    ProtocolError::ScramError {
        detail: heapless::String::try_from("buffer overflow").unwrap_or_default(),
    }
}

/// Build a [`ProtocolError::ScramError`] from any [`Display`] source,
/// handling the `heapless::String` cap-exhaustion case explicitly.
///
/// Call sites previously wrote this inline as
/// `let _ = Write::write_fmt(...)` — banned pattern (memory
/// `feedback_no_underscore_vars`) *and* tier-3 silent-truncation
/// class: `write_fmt` returns `Err(fmt::Error)` iff the formatter
/// exhausted capacity, after possibly writing partial content. The
/// inline form discarded that signal, so oversized detail messages
/// silently surfaced as partial strings to the caller.
///
/// This helper replaces the inline pattern with an explicit
/// `is_err()` branch: on cap exhaustion the partial content is
/// cleared and a static sentinel (`"scram error (detail
/// truncated)"`, 30 bytes — fits 128-byte cap) takes its place.
/// The fallback's `try_from` Err branch is architecturally dead (the
/// sentinel fits) but surfaced honestly via `if let Ok` — if a
/// future refactor tightens the cap below 30 bytes, `detail` stays
/// empty rather than carrying garbage.
///
/// Cold path: every SCRAM error classification routes through here;
/// the happy path never constructs one. `#[cold]` moves the
/// monomorphised code out of the hot instruction-cache neighbourhood.
///
/// Superseded by DEF-060 (typed SCRAM error sub-enums eliminate the
/// `heapless::String<128>` detail field entirely); until DEF-060
/// ships, this helper is the canonical construction path.
///
/// [`Display`]: core::fmt::Display
#[cold]
fn scram_err_from<E: core::fmt::Display>(source: E) -> ProtocolError {
    use core::fmt::Write;
    let mut detail = heapless::String::<128>::new();
    if detail.write_fmt(format_args!("{source}")).is_err() {
        // Partial content may have been appended before the capacity
        // hit. Drop it — the caller would otherwise receive a
        // truncated message with no indication of the truncation.
        detail.clear();
        if let Ok(sentinel) = heapless::String::try_from("scram error (detail truncated)") {
            detail = sentinel;
        }
        // Dead `else`: sentinel (30 bytes) fits the 128-byte cap —
        // a future cap reduction would surface as empty `detail`,
        // which is still non-silent.
    }
    ProtocolError::ScramError { detail }
}

#[cfg(test)]
mod parse_error_response_tests {
    //! Seam-closing tests for `parse_error_response` (S4 / B1 from the
    //! 2026-04-18 second-pass audit).
    //!
    //! The function has nine field-type arms (`b'S'`, `b'V'`, `b'C'`,
    //! `b'M'`, `b'D'`, `b'H'`, unknown) mapping to five
    //! `ServerErrorResponse` fields. Swapping any two arms compiles
    //! cleanly — the lint-level type checker cannot see that `b'M'`
    //! should land in `message` and `b'D'` should land in `detail`.
    //! Tests below set each field explicitly and assert the full
    //! `ProtocolError` value for byte-exact mapping.
    //!
    //! Also covers pathological inputs:
    //! - Empty payload (just the terminator).
    //! - Field type with no NUL-terminated value (unterminated).
    //! - Duplicate severity (S / V) — first wins (`if severity.is_empty()`).
    //!
    //! Category (1) per reforge.md §4.11. Uses `assert_eq!` on the
    //! full `ProtocolError` variant because `panic!` is crate-root
    //! forbidden even in unit tests.

    extern crate alloc;

    use super::*;

    /// Build a well-formed ErrorResponse body: a sequence of
    /// `type_byte + NUL-terminated-value` entries, followed by a
    /// single trailing `\0` terminator.
    fn build_error_body(fields: &[(u8, &[u8])]) -> alloc::vec::Vec<u8> {
        let mut body = alloc::vec::Vec::new();
        for (type_byte, value) in fields {
            body.push(*type_byte);
            body.extend_from_slice(value);
            body.push(0);
        }
        body.push(0); // Terminator.
        body
    }

    fn mk_err(
        severity: &str,
        code: &str,
        message: &str,
        detail: &str,
        hint: &str,
    ) -> ProtocolError {
        ProtocolError::ServerErrorResponse {
            severity: heapless::String::try_from(severity).unwrap_or_default(),
            code: heapless::String::try_from(code).unwrap_or_default(),
            message: heapless::String::try_from(message).unwrap_or_default(),
            detail: heapless::String::try_from(detail).unwrap_or_default(),
            hint: heapless::String::try_from(hint).unwrap_or_default(),
        }
    }

    /// Invariant (spec): each known field type routes to its dedicated
    /// `ServerErrorResponse` field. A one-arm swap in `parse_error_response`
    /// compiles silently; this table catches it via full-value equality.
    #[test]
    fn field_type_routes_to_correct_output_field() {
        let body = build_error_body(&[
            (b'S', b"FATAL"),
            (b'C', b"28P01"),
            (b'M', b"authentication failed"),
            (b'D', b"user does not exist"),
            (b'H', b"check pg_hba.conf"),
        ]);
        let actual = parse_error_response(&body);
        let expected = mk_err(
            "FATAL",
            "28P01",
            "authentication failed",
            "user does not exist",
            "check pg_hba.conf",
        );
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): `b'V'` is an alternate for `b'S'` (non-localised
    /// severity). When only `V` arrives, it populates `severity`.
    #[test]
    fn severity_v_used_when_s_absent() {
        let body = build_error_body(&[(b'V', b"ERROR")]);
        let actual = parse_error_response(&body);
        let expected = mk_err("ERROR", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): first severity (`S` or `V`) wins — the
    /// `if severity.is_empty()` guard in the S/V arms blocks overwrite.
    #[test]
    fn severity_s_wins_over_later_v() {
        let body = build_error_body(&[(b'S', b"FATAL"), (b'V', b"ERROR")]);
        let actual = parse_error_response(&body);
        let expected = mk_err("FATAL", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): unknown field types are silently dropped;
    /// other fields still parse.
    #[test]
    fn unknown_field_types_are_silently_skipped() {
        let body = build_error_body(&[
            (b'Z', b"irrelevant"),
            (b'M', b"real message"),
            (b'Q', b"also irrelevant"),
        ]);
        let actual = parse_error_response(&body);
        let expected = mk_err("", "", "real message", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): empty payload (just the terminator NUL)
    /// yields an all-empty `ServerErrorResponse`, not a parse failure
    /// or panic.
    #[test]
    fn empty_payload_yields_empty_fields() {
        let body: alloc::vec::Vec<u8> = alloc::vec![0];
        let actual = parse_error_response(&body);
        let expected = mk_err("", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): a field-type byte with no NUL-terminated
    /// value (adversarial input: payload ends mid-field) does not
    /// loop forever and does not panic. Pins that the
    /// `while let Some(b) = payload.get(pos)` inner loop terminates
    /// on end-of-buffer. A regression using unchecked indexing would
    /// loop / panic; this test verifies graceful termination.
    #[test]
    fn unterminated_final_field_does_not_panic() {
        // [S, 'A', 'B'] — no NUL after 'B', no terminator.
        let body: alloc::vec::Vec<u8> = alloc::vec![b'S', b'A', b'B'];
        let actual = parse_error_response(&body);
        // Whatever the parser recovered: must be a bounded string and
        // must not panic. Exact value of severity is an implementation
        // detail (parser reads to EOF as the value).
        let expected = mk_err("AB", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): duplicate non-severity fields — second
    /// overwrites first. Pins that the unguarded assignments for
    /// `C`, `M`, `D`, `H` arms hold. A regression that added `if
    /// .is_empty()` guards would silently change the semantics
    /// (first-wins vs last-wins).
    #[test]
    fn duplicate_code_second_wins() {
        let body = build_error_body(&[(b'C', b"FIRST"), (b'C', b"SECOND")]);
        let actual = parse_error_response(&body);
        let expected = mk_err("", "SECOND", "", "", "");
        assert_eq!(actual, expected);
    }
}
