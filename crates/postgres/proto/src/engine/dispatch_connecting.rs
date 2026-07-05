//! Connecting-phase dispatch for the session engine.
//!
//! This is the strangler engine's handshake brain. It dispatches **directly**
//! on the existing per-phase [`ConnectingState`] enum — there is no wide
//! `ProtoState` mirror and no `From`/`TryFrom` lift/lower: a `(phase, frame)`
//! pair that the protocol cannot reach has no arm by *omission* (so it is a
//! compile-time impossibility, not a runtime guard), and a wire-illegal frame
//! within a phase is a *classified* [`ConnFail`] surfaced as
//! [`AuthEvent::Fail`], never a silent skip. The module scopes
//! `#![deny(clippy::wildcard_enum_match_arm)]` so a future contributor cannot
//! paper over a new state or auth sub-code with a `_` arm.
//!
//! # Reuse, not reimplementation
//!
//! The cryptographic and message-builder leaves are reused verbatim — the
//! SCRAM nonce/proof primitives ([`crate::scram::wire`],
//! [`crate::scram::crypto`]), the MD5 digest ([`crate::md5`]), the password
//! containers ([`crate::password`]), and the typed auth sub-code classifier
//! ([`crate::wire::AuthSubCode`]). This dispatch only threads them through the
//! connecting state machine and assembles each outbound frame into a transient
//! [`WriteBuf`] frame assembler via the public `push_*` surface, then queues
//! the assembled bytes on the caller's persistent [`SendBuf`]. No crypto is
//! hand-rolled here.
//!
//! # Single outbound residence
//!
//! The connecting engine owns no persistent outbound buffer. Every frame —
//! the startup packet and each auth response — is assembled into a short-lived
//! [`WriteBuf`] (scrub-on-drop, so a password-correlated assembly never
//! outlives the build) and immediately copied onto the caller's [`SendBuf`],
//! which is the sole outbound queue threaded through the flush loop. The SCRAM
//! client proof therefore lands only in the transient assembler (scrubbed when
//! it drops / is cleared for the next frame) and in [`SendBuf`] (scrubbed over
//! its full backing capacity on teardown).
//!
//! # Pull surface
//!
//! [`ConnectingEngine::next_auth_event`] is the borrowing pull: it locates one
//! inbound frame in the [`IngestBuf`], runs the consuming dispatch, queues any
//! outbound auth response onto the caller's [`SendBuf`], and returns the next
//! [`AuthEvent`] borrowing the read buffer. Silent intermediate frames
//! (`AuthenticationOk`,
//! `BackendKeyData`, the client-initiated SASL exchange) loop internally; the
//! caller sees only the seven connecting events. Completion is a dispatch
//! return value — [`ConnPhase::Ready`] reached through `ConnEvent::Ready` —
//! consumed by the [`ConnectingEngine::into_active`] move; there is no
//! synthetic placeholder state variant.

#![deny(
    clippy::wildcard_enum_match_arm,
    reason = "the connecting dispatch must enumerate every state and auth sub-code; a `_` arm would silently swallow a new phase/frame instead of classifying it"
)]

use alloc::boxed::Box;
use alloc::string::String;

use super::{ActiveEngine, AuthEvent, IngestBuf, IngestCommitOverflow, IngestFull, SendBuf};
use crate::action::TxStatus;
use crate::ident::{DatabaseName, Ident, PodBytes};
use crate::md5::Md5HandshakeState;
use crate::password::{Credentials, Password};
use crate::startup::StartupParam;
use crate::scram::session::ScramSession;
use crate::scram::types::SecretDigest;
use crate::scram::wire::{ScramError, ScramFailureClass};
use crate::sensitive::Sensitive;
use crate::state::ConnectingState;
use crate::wire::{
    AuthSubCode, InboundTag, PROTOCOL_VERSION_3_0, SCRAM_SHA_256_MECHANISM, TAG_AUTHENTICATION,
    TAG_BACKEND_KEY_DATA, TAG_ERROR_RESPONSE, TAG_PARAMETER_STATUS, TAG_READY_FOR_QUERY,
    TAG_SASL_RESPONSE,
};
use crate::write_buf::{WriteBuf, WriteBufFull};

// ===========================================================================
// Classified connecting failure
// ===========================================================================

/// Why the connecting handshake terminated unsuccessfully.
///
/// Every failure path in the dispatch produces one of these — there is no
/// silent drop. A server `ErrorResponse` is [`Self::ServerError`]; a
/// wire-illegal frame for the current phase is [`Self::UnexpectedFrame`]; the
/// auth/SCRAM/MD5 sub-classes carry the specific cause. Observable consumers
/// (the differential corpus) collapse every variant to "handshake failed",
/// but the variant preserves the diagnostic for richer surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnFail {
    /// The server sent an `ErrorResponse` during the handshake.
    ServerError,
    /// A frame whose tag is not legal for the current connecting phase.
    UnexpectedFrame {
        /// The offending wire tag byte.
        tag: u8,
    },
    /// The server requested an authentication method the configured
    /// credentials cannot satisfy (e.g. SASL offered to a Trust client, or a
    /// downgrade a password client refuses).
    UnsupportedAuthMethod,
    /// An `Authentication` frame whose sub-code field was truncated.
    MalformedAuthentication,
    /// A `BackendKeyData` frame whose payload was not the expected 8 bytes.
    MalformedBackendKeyData,
    /// A `ReadyForQuery` frame whose payload was not a single legal
    /// transaction-status byte.
    MalformedReadyForQuery,
    /// A SCRAM-SHA-256 exchange failure, carrying the leaf classification.
    Scram(ScramFailureClass),
    /// The outbound auth response did not fit the bounded write buffer.
    BufferOverflow,
}

impl ConnFail {
    /// Collapse a leaf [`ScramError`] into the classified SCRAM failure,
    /// discarding the optional inline text (the connecting surface keeps only
    /// the class; richer error surfaces resolve the text elsewhere).
    #[inline]
    fn scram(error: ScramError) -> Self {
        ConnFail::Scram(error.split_into_class_and_text().0)
    }
}

// The widest variant is `Scram(ScramFailureClass)`; the `UnexpectedFrame { tag }`
// u8 and the unit causes ride the discriminant → 8 B, align 4.
crate::wire_pin!(ConnFail, size = 8, align = 4);

// ===========================================================================
// Engine phase + dispatch outcome
// ===========================================================================

/// The backend cancel-key material extracted at handshake completion.
#[derive(Debug)]
struct BackendKeyParts {
    pid: i32,
    secret_key: Sensitive<i32>,
    tx_status: TxStatus,
}

/// The connecting engine's current phase.
///
/// `Transient` is the `mem::replace` placeholder used while the consuming
/// dispatch borrows the in-flight [`ConnectingState`] out; it is never
/// observed by a caller.
#[derive(Debug)]
enum ConnPhase {
    Handshaking(ConnectingState),
    Ready(BackendKeyParts),
    Failed(ConnFail),
    Transient,
}

/// The per-frame dispatch outcome: the successor phase plus the (non-borrowing)
/// event classification. The payload-lending [`AuthEvent`]s re-borrow the read
/// buffer in [`ConnectingEngine::next_auth_event`] after this returns.
#[derive(Debug)]
struct ConnDispatch {
    next: ConnPhase,
    event: ConnEvent,
}

/// Non-borrowing classification of one dispatched frame. Maps to an
/// [`AuthEvent`] in [`ConnectingEngine::next_auth_event`]; the lending
/// variants name the frame body re-borrowed there.
#[derive(Debug, Clone, Copy)]
enum ConnEvent {
    /// An expected, handled intermediate frame with no surfaceable event —
    /// keep pulling (`AuthenticationOk`, `BackendKeyData`, the
    /// client-initiated SASL exchange).
    Silent,
    /// Server requested cleartext-password auth; the response is in the buffer.
    Cleartext,
    /// Server requested MD5 auth; the response is in the buffer.
    Md5 {
        /// The server-chosen 4-byte salt.
        salt: [u8; 4],
    },
    /// SASL continuation — lend the server's challenge frame body.
    SaslContinue,
    /// A `ParameterStatus` report — lend the raw key/value payload.
    ParamStatus,
    /// Handshake complete.
    Ready,
    /// Server `ErrorResponse` — lend the raw body.
    ServerFail,
    /// A protocol-level violation with no server body to lend, carrying its
    /// classified cause.
    ProtoFail(ConnFail),
}

/// Non-borrowing result of the silent-frame drive loop. The payload-lending
/// variants carry the active-buffer offset range of the frame to re-borrow;
/// [`ProtoFail`](Self::ProtoFail) carries the classified cause directly.
#[derive(Debug, Clone, Copy)]
enum DriveOutcome {
    NeedMore,
    Ready,
    Cleartext,
    Md5 { salt: [u8; 4] },
    ProtoFail(ConnFail),
    SaslContinue { start: usize, end: usize },
    ParamStatus { start: usize, end: usize },
    ServerFail { start: usize, end: usize },
}

/// Non-borrowing handshake progress for the connecting pump.
///
/// The pump classifies one connecting step but never needs the lent frame
/// bodies the public [`AuthEvent`] carries — only whether to flush a queued
/// response, read more, keep pulling, or stop. This drops every borrow, so the
/// pump can act across an `.await` with no borrow to end first, and carries the
/// classified [`ConnFail`] directly on failure (no unreachable `Option`).
#[derive(Debug, Clone, Copy)]
pub(crate) enum HandshakeProgress {
    /// The framing buffer is drained — read one chunk.
    NeedMore,
    /// An auth-challenge response was queued — flush it to the wire.
    AuthResponse,
    /// A non-response intermediate (e.g. `ParameterStatus`) — keep pulling.
    ParamStatus,
    /// Handshake complete — ready to transition to the active phase.
    Ready,
    /// The handshake failed, carrying the classified cause.
    Failed(ConnFail),
}

// `pub(crate)` pump-facing surface. Widest variant is `Failed(ConnFail)` (8/4);
// the four unit steps ride the discriminant.
crate::wire_pin!(HandshakeProgress, size = 8, align = 4);

#[inline]
fn advance(state: ConnectingState) -> ConnDispatch {
    ConnDispatch {
        next: ConnPhase::Handshaking(state),
        event: ConnEvent::Silent,
    }
}

#[inline]
fn fail(reason: ConnFail) -> ConnDispatch {
    ConnDispatch {
        next: ConnPhase::Failed(reason),
        event: ConnEvent::ProtoFail(reason),
    }
}

#[inline]
fn server_fail() -> ConnDispatch {
    ConnDispatch {
        next: ConnPhase::Failed(ConnFail::ServerError),
        event: ConnEvent::ServerFail,
    }
}

/// Extract one `ParameterStatus` GUC value by key, decoded as UTF-8.
///
/// A `ParameterStatus` body is `key\0value\0` — two NUL-terminated C strings
/// (PG §55.7). Returns the owned value when `key` matches and the value is
/// valid UTF-8. A key mismatch, a missing NUL separator/terminator (malformed
/// frame), or a non-UTF-8 value all yield `None` — honest absence, never a
/// lossy substitution. `server_version` is always ASCII, so the UTF-8 arm
/// never rejects a real report; the check exists so a corrupt/injected frame
/// degrades to absence rather than a mangled string.
fn param_status_value(body: &[u8], key: &[u8]) -> Option<String> {
    let key_end = body.iter().position(|&b| b == 0)?;
    if body.get(..key_end)? != key {
        return None;
    }
    let value_field = body.get(key_end.checked_add(1)?..)?;
    let value_end = value_field.iter().position(|&b| b == 0)?;
    let value = value_field.get(..value_end)?;
    core::str::from_utf8(value).ok().map(String::from)
}

// ===========================================================================
// The connecting engine
// ===========================================================================

/// The connecting-phase session engine.
///
/// Holds the single-residence inbound [`IngestBuf`] and the current
/// [`ConnPhase`]. It owns no outbound buffer: the startup packet and every auth
/// response are queued onto the caller's [`SendBuf`] (passed to
/// [`start`](Self::start) and [`next_auth_event`](Self::next_auth_event)), the
/// sole outbound residence. Feed scripted/socket bytes through
/// [`read_slot`](Self::read_slot) + [`commit`](Self::commit), drive the
/// handshake with [`next_auth_event`](Self::next_auth_event), and transition to
/// the active phase with [`into_active`](Self::into_active) once
/// [`AuthEvent::Ready`] is observed.
#[derive(Debug)]
pub struct ConnectingEngine {
    ingest: IngestBuf,
    phase: ConnPhase,
    /// The `server_version` GUC captured from the startup `ParameterStatus`
    /// reports the server sends during the handshake, before the first
    /// `ReadyForQuery`. Owned (not a borrow into `ingest`, which is refilled by
    /// active-phase reads after `into_active`) so it outlives the handshake and
    /// rides into the [`ActiveEngine`], where the driver reads it via
    /// [`super::Engine::server_version`] — deleting the post-connect
    /// `SHOW server_version` round-trip. `None` if the server sent no
    /// `server_version` report (spec-guaranteed for protocol 3.0, so honest
    /// absence, never a fabricated fallback).
    server_version: Option<String>,
}

// Stack footprint: the single-residence `IngestBuf` (144) + the `ConnPhase`
// enum (16 B — its widest variant is `ConnectingState`, no id correlator
// threaded) + the captured `server_version` (`Option<String>`, 24 B).
// `ConnPhase` is sized by its widest variant — the password/SCRAM handshake
// state is `Box`-externalised inside `ConnectingState`, so the live handshake
// material lives off-stack and the engine's own footprint stays bounded. A
// field reshape lands here as a reviewed drift.
crate::wire_pin!(ConnectingEngine, size = 184, align = 8);

impl ConnectingEngine {
    /// Begin a handshake: queue the `StartupMessage` onto `send_buf` and seat
    /// the initial connecting state for the chosen credentials.
    ///
    /// The startup packet is assembled into a transient [`WriteBuf`] (scrubbed
    /// on drop) and copied onto `send_buf` — the caller's persistent outbound
    /// queue, drained by the flush loop. It is byte-identical to the live
    /// protocol's `build_startup_message` (PG §55.2.1): a 4-byte length prefix
    /// wrapping the 3.0 protocol version, the fixed `user` / pinned
    /// `client_encoding=UTF8` / optional `database` parameters, each validated
    /// consumer [`StartupParam`] appended after them, and the trailing
    /// empty-key NUL.
    ///
    /// Returns [`ConnFail::BufferOverflow`] if the startup packet exceeds the
    /// bounded assembler. For the fixed parameters this is structurally
    /// unreachable (the `MAX_OWNED_SEND_LEN >= max_startup_message_size()`
    /// const-assert in `write_buf` proves the fixed prefix always fits); a large
    /// enough set of consumer `params` can genuinely overflow the bounded frame,
    /// which is surfaced honestly here rather than discharged with a panic-able
    /// unwrap.
    pub fn start(
        send_buf: &mut SendBuf,
        user: &Ident,
        database: Option<&DatabaseName>,
        params: &[StartupParam],
        credentials: Credentials,
    ) -> Result<Self, ConnFail> {
        let mut scratch = WriteBuf::new();
        build_startup_message(&mut scratch, user, database, params)
            .map_err(|_| ConnFail::BufferOverflow)?;
        send_buf.enqueue(scratch.as_bytes());
        // Exactly one in-flight startup — the handshake is strictly serial and
        // never multiplexed, so the current variant is itself the correlation
        // and no reply id is threaded.
        let state = match credentials {
            Credentials::Trust => ConnectingState::StartupTrust,
            Credentials::ScramPassword(password) => ConnectingState::StartupScram {
                scram: Box::new(ScramSession::from_password(password)),
            },
            Credentials::CleartextPassword(password) => ConnectingState::StartupCleartext {
                password: Box::new(password),
            },
            Credentials::Md5Password(password) => ConnectingState::StartupMd5 {
                handshake: Box::new(Md5HandshakeState {
                    password,
                    user: *user,
                }),
            },
        };
        Ok(Self {
            ingest: IngestBuf::new(),
            phase: ConnPhase::Handshaking(state),
            server_version: None,
        })
    }

    /// Lend a writable tail slice the socket/script reads inbound bytes into.
    ///
    /// Pair with [`commit`](Self::commit) of the count actually written.
    #[inline]
    pub fn read_slot(&mut self, want: usize) -> Result<&mut [u8], IngestFull> {
        self.ingest.read_slot(want)
    }

    /// Publish `n` inbound bytes written into the most recent
    /// [`read_slot`](Self::read_slot).
    #[inline]
    pub fn commit(&mut self, n: usize) -> Result<(), IngestCommitOverflow> {
        self.ingest.commit(n)
    }

    /// Pull the next non-borrowing handshake step — the connecting pump's pull.
    ///
    /// Runs the same dispatch as [`next_auth_event`](Self::next_auth_event) but
    /// projects the outcome to a [`HandshakeProgress`] that drops every frame
    /// borrow (the pump needs only the classification) and carries the
    /// classified [`ConnFail`] directly on failure — so the pump never holds a
    /// borrow across its follow-on `flush`/`read` and the handshake outcome
    /// needs no unreachable `Option`.
    pub(crate) fn next_handshake_step(&mut self, send_buf: &mut SendBuf) -> HandshakeProgress {
        match self.drive_to_event(send_buf) {
            DriveOutcome::NeedMore => HandshakeProgress::NeedMore,
            DriveOutcome::Ready => HandshakeProgress::Ready,
            DriveOutcome::Cleartext
            | DriveOutcome::Md5 { .. }
            | DriveOutcome::SaslContinue { .. } => HandshakeProgress::AuthResponse,
            DriveOutcome::ParamStatus { .. } => HandshakeProgress::ParamStatus,
            DriveOutcome::ProtoFail(reason) => HandshakeProgress::Failed(reason),
            // A server `ErrorResponse` during connect — the cause is fixed.
            DriveOutcome::ServerFail { .. } => HandshakeProgress::Failed(ConnFail::ServerError),
        }
    }

    /// Pull the next connecting event, borrowing the read buffer.
    ///
    /// Locates one complete frame, runs the consuming dispatch (queuing any
    /// outbound auth response onto `send_buf`), and returns the classified
    /// [`AuthEvent`]. Expected intermediate frames loop internally; the caller
    /// sees only [`AuthEvent::NeedMore`] (buffer drained),
    /// [`AuthEvent::AuthCleartext`] / [`AuthEvent::AuthMd5`] /
    /// [`AuthEvent::AuthSaslContinue`] (auth steps), [`AuthEvent::ParamStatus`],
    /// [`AuthEvent::Ready`], or [`AuthEvent::Fail`].
    ///
    /// Two-phase to keep the borrow checker honest: the silent-frame loop in
    /// [`drive_to_event`](Self::drive_to_event) consumes frames, queues outbound
    /// bytes onto the disjoint `send_buf`, and mutates `phase` without holding a
    /// read borrow; the `send_buf`/ingest borrows end before the payload-lending
    /// events re-borrow the just-consumed frame body here.
    pub fn next_auth_event(&mut self, send_buf: &mut SendBuf) -> AuthEvent<'_> {
        match self.drive_to_event(send_buf) {
            DriveOutcome::NeedMore => AuthEvent::NeedMore,
            DriveOutcome::Ready => AuthEvent::Ready,
            DriveOutcome::Cleartext => AuthEvent::AuthCleartext,
            DriveOutcome::Md5 { salt } => AuthEvent::AuthMd5 { salt },
            DriveOutcome::ProtoFail(_) => AuthEvent::Fail(&[]),
            DriveOutcome::SaslContinue { start, end } => {
                AuthEvent::AuthSaslContinue(self.ingest.frame_body(start, end))
            }
            DriveOutcome::ParamStatus { start, end } => {
                AuthEvent::ParamStatus(self.ingest.frame_body(start, end))
            }
            DriveOutcome::ServerFail { start, end } => {
                AuthEvent::Fail(self.ingest.frame_body(start, end))
            }
        }
    }

    /// Drive the silent-frame loop to the next surfaceable outcome.
    ///
    /// Returns a non-borrowing [`DriveOutcome`] (the payload-lending variants
    /// carry the frame's active-buffer offset range, re-borrowed by the
    /// caller). No read borrow is held across a loop iteration, so the
    /// per-iteration `take_frame` + dispatch mutation compiles without the
    /// returns-a-loop-borrow E0499.
    ///
    /// Each dispatched frame's outbound response (if any) is assembled into one
    /// reusable transient [`WriteBuf`] and copied onto `send_buf`. The assembler
    /// is [`clear`](crate::write_buf::WriteBuf::clear)ed before each frame —
    /// which scrubs the previous frame's bytes (the SCRAM proof among them)
    /// before the slot is reused — and scrubs its final contents on drop; the
    /// queued copies live on `send_buf` (scrubbed over full capacity on
    /// teardown).
    fn drive_to_event(&mut self, send_buf: &mut SendBuf) -> DriveOutcome {
        let mut scratch = WriteBuf::new();
        loop {
            // Terminal phases are idempotent and never consume a frame.
            match &self.phase {
                ConnPhase::Ready(_) => return DriveOutcome::Ready,
                ConnPhase::Failed(reason) => return DriveOutcome::ProtoFail(*reason),
                ConnPhase::Handshaking(_) | ConnPhase::Transient => {}
            }

            let (tag, start, end) = match self.ingest.take_frame() {
                Some(frame) => frame,
                None => return DriveOutcome::NeedMore,
            };

            // Move the in-flight state out for the consuming dispatch. The
            // terminal arms are unreachable (the short-circuit above returned)
            // but are classified rather than wildcarded.
            let state = match core::mem::replace(&mut self.phase, ConnPhase::Transient) {
                ConnPhase::Handshaking(state) => state,
                ConnPhase::Ready(parts) => {
                    self.phase = ConnPhase::Ready(parts);
                    return DriveOutcome::Ready;
                }
                ConnPhase::Failed(reason) => {
                    self.phase = ConnPhase::Failed(reason);
                    return DriveOutcome::ProtoFail(reason);
                }
                // A frame arriving while the phase is the move-out placeholder is
                // an inconsistent internal state; classify it with the real
                // offending tag (no fabricated cause).
                ConnPhase::Transient => {
                    return DriveOutcome::ProtoFail(ConnFail::UnexpectedFrame { tag })
                }
            };

            let payload = self.ingest.frame_body(start, end);
            // Empty (and scrub) the assembler, build this frame's response into
            // it, then queue the bytes before the next iteration reuses the slot.
            scratch.clear();
            let dispatch =
                dispatch_connecting(state, InboundTag::from_byte(tag), payload, &mut scratch);
            let response = scratch.as_bytes();
            if !response.is_empty() {
                send_buf.enqueue(response);
            }
            self.phase = dispatch.next;
            match dispatch.event {
                ConnEvent::Silent => continue,
                ConnEvent::Cleartext => return DriveOutcome::Cleartext,
                ConnEvent::Md5 { salt } => return DriveOutcome::Md5 { salt },
                ConnEvent::SaslContinue => return DriveOutcome::SaslContinue { start, end },
                ConnEvent::ParamStatus => {
                    // Capture `server_version` from the GUC key/value before the
                    // pump drops the report — the driver reads it via the active
                    // accessor instead of a post-connect `SHOW`. Parsed here, the
                    // single choke point both `next_handshake_step` (pump) and
                    // `next_auth_event` (corpus) funnel through, so neither's
                    // signature grows. First writer wins: `server_version` is sent
                    // once, but the guard keeps a hostile duplicate from
                    // reallocating.
                    if self.server_version.is_none() {
                        let body = self.ingest.frame_body(start, end);
                        if let Some(value) = param_status_value(body, b"server_version") {
                            self.server_version = Some(value);
                        }
                    }
                    return DriveOutcome::ParamStatus { start, end };
                }
                ConnEvent::Ready => return DriveOutcome::Ready,
                ConnEvent::ServerFail => return DriveOutcome::ServerFail { start, end },
                ConnEvent::ProtoFail(reason) => return DriveOutcome::ProtoFail(reason),
            }
        }
    }

    /// Consume the connecting engine and produce the active-phase engine.
    ///
    /// Form-1 consuming move: succeeds only once the handshake reached
    /// [`AuthEvent::Ready`]; otherwise returns the connecting engine unchanged
    /// (`Err(self)`) so the caller can keep driving it. There is no synthetic
    /// "ready" state variant — completion is the [`ConnPhase::Ready`] carried
    /// by the dispatch return.
    #[expect(
        clippy::result_large_err,
        reason = "form-1 consuming move: the Err arm hands the connecting engine back by value so the caller can keep driving the handshake (the analogue of the live IntoActiveError::StillConnecting). Boxing the Err would force a heap allocation on the still-connecting path and change the consume-self return shape for no benefit on this cold transition."
    )]
    pub fn into_active(mut self) -> Result<ActiveEngine, Self> {
        let parts = match core::mem::replace(&mut self.phase, ConnPhase::Transient) {
            ConnPhase::Ready(parts) => parts,
            ConnPhase::Handshaking(state) => {
                self.phase = ConnPhase::Handshaking(state);
                return Err(self);
            }
            ConnPhase::Failed(reason) => {
                self.phase = ConnPhase::Failed(reason);
                return Err(self);
            }
            ConnPhase::Transient => return Err(self),
        };
        // Carry the single-residence ingest buffer forward: any active-phase
        // frames the server pipelined after the handshake terminal are already
        // resident, so the active engine resumes framing without a re-read. The
        // captured `server_version` moves with it (a plain field move out of the
        // consumed `self` — `ConnectingEngine` has no `Drop`).
        Ok(ActiveEngine::from_handshake(
            parts.pid,
            parts.secret_key,
            parts.tx_status,
            self.ingest,
            self.server_version,
        ))
    }
}

// ===========================================================================
// Outbound startup / auth message builders (reuse the leaf crypto)
// ===========================================================================

/// Build the `StartupMessage` into `write`. Byte-identical to the live
/// protocol builder.
///
/// Emits the fixed parameters first (`user`, the pinned `client_encoding=UTF8`,
/// and the optional `database`), then each consumer [`StartupParam`] in order,
/// then the trailing empty-key NUL. Each `StartupParam` is already validated —
/// its name/value carry no NUL and no reserved key — so appending them cannot
/// corrupt the packet or displace a fixed parameter.
///
/// With no consumer `params`, the emitted bytes are identical to the
/// fixed-only packet, so a default connection's wire is unchanged.
fn build_startup_message(
    write: &mut WriteBuf,
    user: &Ident,
    database: Option<&DatabaseName>,
    params: &[StartupParam],
) -> Result<(), WriteBufFull> {
    write.with_length_prefix(|w| {
        w.push_u32_be(PROTOCOL_VERSION_3_0)?;
        w.push_nul_terminated(b"user")?;
        w.push_nul_terminated(user.as_bytes())?;
        // Pin the session to UTF-8 regardless of the server's default
        // client_encoding. The driver decodes every TEXT value as UTF-8
        // (`str::from_utf8`); on a LATIN1 / SQL_ASCII server that assumption
        // would otherwise be silently violated. Forcing UTF8 here makes the
        // decode correct by construction. A consumer cannot override it — the
        // `client_encoding` key is reserved against `StartupParam`.
        w.push_nul_terminated(b"client_encoding")?;
        w.push_nul_terminated(b"UTF8")?;
        if let Some(db) = database {
            w.push_nul_terminated(b"database")?;
            w.push_nul_terminated(db.as_bytes())?;
        }
        for param in params {
            w.push_nul_terminated(param.name_bytes())?;
            w.push_nul_terminated(param.value_bytes())?;
        }
        w.push_u8(0)
    })
}

/// Build the cleartext `PasswordMessage` (`'p'` frame, NUL-terminated body).
fn build_cleartext_password_message(
    write: &mut WriteBuf,
    password: &Sensitive<Password>,
) -> Result<(), ConnFail> {
    write
        .push_u8(TAG_SASL_RESPONSE.byte())
        .map_err(|_| ConnFail::BufferOverflow)?;
    write
        .with_length_prefix(|w| {
            password.with_inner(|pwd| w.push_bytes(pwd.as_bytes()))?;
            w.push_u8(0)
        })
        .map_err(|_| ConnFail::BufferOverflow)
}

/// Build the MD5 `PasswordMessage`, reusing [`crate::md5::compute_response_body`].
fn build_md5_password_message(
    write: &mut WriteBuf,
    handshake: &Md5HandshakeState,
    salt: [u8; 4],
) -> Result<(), ConnFail> {
    let user = handshake.user.as_bytes();
    let body = handshake
        .password
        .with_inner(|pwd| crate::md5::compute_response_body(pwd, user, salt));
    write
        .push_u8(TAG_SASL_RESPONSE.byte())
        .map_err(|_| ConnFail::BufferOverflow)?;
    write
        .with_length_prefix(|w| {
            w.push_bytes(body.as_slice())?;
            w.push_u8(0)
        })
        .map_err(|_| ConnFail::BufferOverflow)
}

/// Build the `SASLInitialResponse`, reusing the SCRAM nonce/message leaves and
/// populating the session's `client_first_bare` / `client_nonce_b64` in place
/// (the single `Box<ScramSession>` is reused across the next transition).
fn build_sasl_initial_response(
    write: &mut WriteBuf,
    scram: &mut ScramSession,
) -> Result<(), ConnFail> {
    use crate::scram::wire;
    // PG binds the SCRAM identity to the StartupMessage `user`; the SASL-level
    // name is empty (mirrors the live builder).
    let user_bytes: &[u8] = b"";
    let client_nonce = wire::generate_client_nonce().map_err(ConnFail::scram)?;
    let client_first_bare =
        wire::build_client_first_bare(user_bytes, &client_nonce).map_err(ConnFail::scram)?;
    let client_first_msg =
        wire::build_client_first_message(user_bytes, &client_nonce).map_err(ConnFail::scram)?;

    write
        .push_u8(TAG_SASL_RESPONSE.byte())
        .map_err(|_| ConnFail::BufferOverflow)?;
    write
        .with_length_prefix(|w| {
            w.push_bytes(SCRAM_SHA_256_MECHANISM)?;
            w.push_u8(0)?;
            let body_len = i32::try_from(client_first_msg.len()).map_err(|_| WriteBufFull)?;
            w.push_i32_be(body_len)?;
            w.push_bytes(&client_first_msg)
        })
        .map_err(|_| ConnFail::BufferOverflow)?;

    scram.client_first_bare =
        PodBytes::try_from_slice(&client_first_bare).map_err(|_| ConnFail::BufferOverflow)?;
    scram.client_nonce_b64 =
        PodBytes::try_from_slice(&client_nonce).map_err(|_| ConnFail::BufferOverflow)?;
    Ok(())
}

/// Build the `SASLResponse` (client-final-with-proof), reusing the SCRAM proof
/// computation, and return the expected server signature for the final
/// verification step. `server_first` is the server-first-message body (the
/// auth frame payload after the 4-byte sub-code).
fn build_sasl_response(
    write: &mut WriteBuf,
    scram: &ScramSession,
    server_first: &[u8],
) -> Result<SecretDigest, ConnFail> {
    use crate::scram::{crypto, wire};
    let parsed = wire::parse_server_first(server_first, scram.client_nonce_b64.as_slice())
        .map_err(ConnFail::scram)?;
    let client_final_without_proof =
        wire::build_client_final_without_proof(parsed.server_nonce.as_bytes())
            .map_err(ConnFail::scram)?;
    let client_first_bare = scram.client_first_bare.as_slice();
    // Closure-scoped password access: the borrow dies at the call boundary;
    // only the `(proof, expected_sig)` tuple survives.
    let proof_result = scram.with_password_bytes(|password_bytes| {
        crypto::compute_client_proof(
            password_bytes,
            &parsed.salt,
            parsed.iterations,
            client_first_bare,
            server_first,
            &client_final_without_proof,
        )
    });
    let (proof, expected_server_sig) = proof_result.map_err(ConnFail::scram)?;

    // The base64 proof is password-correlated; scrub the stack buffer on drop.
    let mut proof_b64: zeroize::Zeroizing<[u8; 64]> = zeroize::Zeroizing::new([0u8; 64]);
    let proof_b64_len =
        wire::base64_encode_to_buf(proof.as_ref(), proof_b64.as_mut()).map_err(ConnFail::scram)?;
    let proof_b64_slice = match proof_b64.get(..proof_b64_len) {
        Some(slice) => slice,
        None => return Err(ConnFail::BufferOverflow),
    };
    let mut client_final_msg =
        wire::build_client_final_message(parsed.server_nonce.as_bytes(), proof_b64_slice)
            .map_err(ConnFail::scram)?;

    write
        .push_u8(TAG_SASL_RESPONSE.byte())
        .map_err(|_| ConnFail::BufferOverflow)?;
    let push_result = write.with_length_prefix(|w| w.push_bytes(&client_final_msg));
    // Scrub the password-correlated client-final bytes now they are copied
    // into the write buffer (the heapless::Vec has no ZeroizeOnDrop).
    use zeroize::Zeroize;
    client_final_msg.as_mut_slice().zeroize();
    push_result.map_err(|_| ConnFail::BufferOverflow)?;
    Ok(expected_server_sig)
}

// ===========================================================================
// Per-frame dispatch — DIRECTLY on ConnectingState
// ===========================================================================

/// Classification of a parsed `Authentication` frame sub-code field.
enum AuthCode<'a> {
    /// A recognised PG auth sub-code plus the bytes after it.
    Known(AuthSubCode, &'a [u8]),
    /// A 4-byte-aligned sub-code outside the PG-defined set.
    Unknown,
    /// The payload was shorter than the 4-byte sub-code field.
    Malformed,
}

#[inline]
fn parse_auth_sub_code(payload: &[u8]) -> AuthCode<'_> {
    match payload {
        [a, b, c, d, rest @ ..] => {
            let raw = u32::from_be_bytes([*a, *b, *c, *d]);
            match AuthSubCode::try_from_u32(raw) {
                Ok(code) => AuthCode::Known(code, rest),
                Err(_) => AuthCode::Unknown,
            }
        }
        _ => AuthCode::Malformed,
    }
}

#[inline]
fn parse_backend_key_data(payload: &[u8]) -> Option<(i32, i32)> {
    match payload {
        [a, b, c, d, e, f, g, h] => Some((
            i32::from_be_bytes([*a, *b, *c, *d]),
            i32::from_be_bytes([*e, *f, *g, *h]),
        )),
        _ => None,
    }
}

#[inline]
fn parse_rfq_tx_status(payload: &[u8]) -> Option<TxStatus> {
    match payload {
        [byte] => TxStatus::try_from_byte(*byte).ok(),
        _ => None,
    }
}

/// Does the SASL mechanism list advertise `SCRAM-SHA-256`? Mirror of the live
/// dispatcher's fast-path + NUL-split fallback.
fn mechanism_list_contains_scram(data: &[u8]) -> bool {
    if let Some(rest) = data.strip_prefix(SCRAM_SHA_256_MECHANISM)
        && let Some(&0) = rest.first()
    {
        return true;
    }
    for name in data.split(|byte| *byte == 0) {
        if name == SCRAM_SHA_256_MECHANISM {
            return true;
        }
    }
    false
}

/// The connecting-phase dispatch: consume one [`ConnectingState`] and one
/// inbound frame, return the successor phase + event. Matches directly on the
/// per-phase enum — every variant is enumerated (the `#![deny(...)]` at the
/// module head forbids a wildcard arm).
fn dispatch_connecting(
    state: ConnectingState,
    tag: InboundTag,
    payload: &[u8],
    write: &mut WriteBuf,
) -> ConnDispatch {
    match state {
        ConnectingState::StartupTrust => dispatch_startup_trust(tag, payload),
        ConnectingState::StartupCleartext { password } => {
            dispatch_startup_cleartext(password, tag, payload, write)
        }
        ConnectingState::CleartextAwaitingAuthOk => dispatch_await_auth_ok(tag, payload),
        ConnectingState::StartupMd5 { handshake } => {
            dispatch_startup_md5(handshake, tag, payload, write)
        }
        ConnectingState::Md5AwaitingAuthOk => dispatch_await_auth_ok(tag, payload),
        ConnectingState::StartupScram { scram } => {
            dispatch_startup_scram(scram, tag, payload, write)
        }
        ConnectingState::ScramAwaitingServerFirst { scram } => {
            dispatch_scram_server_first(scram, tag, payload, write)
        }
        ConnectingState::ScramAwaitingServerFinal {
            expected_server_sig,
        } => dispatch_scram_server_final(*expected_server_sig, tag, payload),
        ConnectingState::ScramAwaitingAuthOk => dispatch_await_auth_ok(tag, payload),
        ConnectingState::PostAuthAwaitingKey => dispatch_post_auth_awaiting_key(tag, payload),
        ConnectingState::PostAuthHaveKey { pid, secret_key } => {
            dispatch_post_auth_have_key(pid, secret_key, tag, payload)
        }
        // The new engine never parks `HandshakeReady` (completion is the
        // dispatch return + `into_active`), so a frame arriving here is a
        // classified protocol violation. The matched value drops at arm end,
        // firing the carried `Sensitive` scrub.
        ConnectingState::HandshakeReady { .. } => fail(ConnFail::UnexpectedFrame { tag: tag.byte() }),
        ConnectingState::Errored(_) => fail(ConnFail::UnexpectedFrame { tag: tag.byte() }),
    }
}

fn dispatch_startup_trust(tag: InboundTag, payload: &[u8]) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::Ok, _) => {
                advance(ConnectingState::PostAuthAwaitingKey)
            }
            // A Trust client (no password) cannot satisfy any challenge.
            AuthCode::Known(
                AuthSubCode::CleartextPassword
                | AuthSubCode::Md5Password
                | AuthSubCode::Sasl
                | AuthSubCode::SaslContinue
                | AuthSubCode::SaslFinal,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_startup_cleartext(
    password: Box<Sensitive<Password>>,
    tag: InboundTag,
    payload: &[u8],
    write: &mut WriteBuf,
) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::CleartextPassword, _) => {
                match build_cleartext_password_message(write, &password) {
                    Ok(()) => ConnDispatch {
                        next: ConnPhase::Handshaking(
                            ConnectingState::CleartextAwaitingAuthOk,
                        ),
                        event: ConnEvent::Cleartext,
                    },
                    Err(reason) => fail(reason),
                }
            }
            AuthCode::Known(
                AuthSubCode::Ok
                | AuthSubCode::Md5Password
                | AuthSubCode::Sasl
                | AuthSubCode::SaslContinue
                | AuthSubCode::SaslFinal,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_startup_md5(
    handshake: Box<Md5HandshakeState>,
    tag: InboundTag,
    payload: &[u8],
    write: &mut WriteBuf,
) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::Md5Password, rest) => {
                let salt: [u8; 4] = match <[u8; 4]>::try_from(rest) {
                    Ok(bytes) => bytes,
                    Err(_) => return fail(ConnFail::MalformedAuthentication),
                };
                match build_md5_password_message(write, &handshake, salt) {
                    Ok(()) => ConnDispatch {
                        next: ConnPhase::Handshaking(ConnectingState::Md5AwaitingAuthOk),
                        event: ConnEvent::Md5 { salt },
                    },
                    Err(reason) => fail(reason),
                }
            }
            AuthCode::Known(
                AuthSubCode::Ok
                | AuthSubCode::CleartextPassword
                | AuthSubCode::Sasl
                | AuthSubCode::SaslContinue
                | AuthSubCode::SaslFinal,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_startup_scram(
    mut scram: Box<ScramSession>,
    tag: InboundTag,
    payload: &[u8],
    write: &mut WriteBuf,
) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::Sasl, rest) => {
                if !mechanism_list_contains_scram(rest) {
                    return fail(ConnFail::Scram(ScramFailureClass::NoSupportedMechanism));
                }
                match build_sasl_initial_response(write, &mut scram) {
                    // Client-initiated SASL: the response is in the buffer;
                    // the surfaceable challenge is the server-first that
                    // follows, so this step is a silent intermediate.
                    Ok(()) => advance(ConnectingState::ScramAwaitingServerFirst { scram }),
                    Err(reason) => fail(reason),
                }
            }
            AuthCode::Known(
                AuthSubCode::Ok
                | AuthSubCode::CleartextPassword
                | AuthSubCode::Md5Password
                | AuthSubCode::SaslContinue
                | AuthSubCode::SaslFinal,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_scram_server_first(
    scram: Box<ScramSession>,
    tag: InboundTag,
    payload: &[u8],
    write: &mut WriteBuf,
) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::SaslContinue, rest) => {
                match build_sasl_response(write, &scram, rest) {
                    Ok(expected_server_sig) => ConnDispatch {
                        next: ConnPhase::Handshaking(ConnectingState::ScramAwaitingServerFinal {
                            expected_server_sig: Box::new(expected_server_sig),
                        }),
                        event: ConnEvent::SaslContinue,
                    },
                    Err(reason) => fail(reason),
                }
            }
            AuthCode::Known(
                AuthSubCode::Ok
                | AuthSubCode::CleartextPassword
                | AuthSubCode::Md5Password
                | AuthSubCode::Sasl
                | AuthSubCode::SaslFinal,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_scram_server_final(
    expected_server_sig: SecretDigest,
    tag: InboundTag,
    payload: &[u8],
) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::SaslFinal, rest) => {
                match crate::scram::wire::parse_server_final(rest) {
                    Ok(received_sig) => {
                        if bool::from(expected_server_sig.ct_eq(&received_sig)) {
                            advance(ConnectingState::ScramAwaitingAuthOk)
                        } else {
                            fail(ConnFail::Scram(ScramFailureClass::SignatureMismatch))
                        }
                    }
                    Err(error) => fail(ConnFail::scram(error)),
                }
            }
            AuthCode::Known(
                AuthSubCode::Ok
                | AuthSubCode::CleartextPassword
                | AuthSubCode::Md5Password
                | AuthSubCode::Sasl
                | AuthSubCode::SaslContinue,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

/// Shared `AuthenticationOk`-awaiting arm for the cleartext / MD5 / SCRAM
/// post-response states.
fn dispatch_await_auth_ok(tag: InboundTag, payload: &[u8]) -> ConnDispatch {
    if tag == TAG_AUTHENTICATION {
        match parse_auth_sub_code(payload) {
            AuthCode::Known(AuthSubCode::Ok, _) => {
                advance(ConnectingState::PostAuthAwaitingKey)
            }
            AuthCode::Known(
                AuthSubCode::CleartextPassword
                | AuthSubCode::Md5Password
                | AuthSubCode::Sasl
                | AuthSubCode::SaslContinue
                | AuthSubCode::SaslFinal,
                _,
            ) => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Unknown => fail(ConnFail::UnsupportedAuthMethod),
            AuthCode::Malformed => fail(ConnFail::MalformedAuthentication),
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_post_auth_awaiting_key(tag: InboundTag, payload: &[u8]) -> ConnDispatch {
    if tag == TAG_BACKEND_KEY_DATA {
        match parse_backend_key_data(payload) {
            Some((pid, secret)) => advance(ConnectingState::PostAuthHaveKey {
                pid,
                secret_key: Sensitive::new(secret),
            }),
            None => fail(ConnFail::MalformedBackendKeyData),
        }
    } else if tag == TAG_PARAMETER_STATUS {
        ConnDispatch {
            next: ConnPhase::Handshaking(ConnectingState::PostAuthAwaitingKey),
            event: ConnEvent::ParamStatus,
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}

fn dispatch_post_auth_have_key(
    pid: i32,
    secret_key: Sensitive<i32>,
    tag: InboundTag,
    payload: &[u8],
) -> ConnDispatch {
    if tag == TAG_READY_FOR_QUERY {
        match parse_rfq_tx_status(payload) {
            Some(tx_status) => ConnDispatch {
                next: ConnPhase::Ready(BackendKeyParts {
                    pid,
                    secret_key,
                    tx_status,
                }),
                event: ConnEvent::Ready,
            },
            None => fail(ConnFail::MalformedReadyForQuery),
        }
    } else if tag == TAG_PARAMETER_STATUS {
        ConnDispatch {
            next: ConnPhase::Handshaking(ConnectingState::PostAuthHaveKey { pid, secret_key }),
            event: ConnEvent::ParamStatus,
        }
    } else if tag == TAG_ERROR_RESPONSE {
        server_fail()
    } else {
        fail(ConnFail::UnexpectedFrame { tag: tag.byte() })
    }
}
