//! Sans-I/O TLS transport over `rustls::unbuffered`.
//!
//! [`TlsTransport`] wraps an inner byte transport (a TCP socket) and a
//! [`rustls::client::UnbufferedClientConnection`], presenting the SAME
//! [`Transport`] quartet the engine drives every socket through
//! (`read` / `write` / `flush` / `shutdown`). The engine is unaware whether
//! its bytes travel in cleartext or inside TLS records: a `TlsTransport`
//! is just another `Transport`.
//!
//! # Why unbuffered
//!
//! `rustls::unbuffered` separates the TLS state machine from I/O. The state
//! machine tells us *what bytes to send* and *what bytes it needs*; we own
//! every buffer and every socket call. That is exactly the sans-I/O contract
//! the engine is built on, so the TLS layer composes with it without a second
//! buffering or async-runtime layer.
//!
//! # Buffer / cursor model
//!
//! Four owned buffers, each with a caller-owned cursor so a dropped (cancelled)
//! future never re-sends or re-decrypts:
//!
//! - **outbound ciphertext queue** (`out_buf` + `out_sent`). `write` encrypts
//!   plaintext into records appended here and performs **no socket I/O**.
//!   `flush` drains the queue to the socket, advancing `out_sent`; the cursor
//!   survives a dropped `flush` future, so a resumed flush continues from the
//!   first unsent byte and never re-sends a committed prefix.
//! - **inbound ciphertext staging** (`staging` + `staging_start`). `read` pulls
//!   socket bytes here; `rustls` consumes whole records from here and yields
//!   each record's plaintext as an **owned chunk** (the measured +1 allocation,
//!   not an in-place borrow); the consumed prefix is front-drained by advancing
//!   `staging_start`, compacted lazily before the next socket read.
//! - **inbound plaintext** (`plaintext` + `plaintext_start`). Decrypted
//!   payloads accumulate here and are copied out to the caller incrementally,
//!   so a caller buffer smaller than a record loses no bytes.
//! - **fixed encrypt/encode scratch** (`scratch`). One TLS record's worth,
//!   reused across calls; `write` chunks plaintext into <=record pieces so the
//!   scratch never needs to grow (0-alloc per `write`).
//!
//! # Cryptographic authority
//!
//! `rustls` (ring provider) is the sole TLS authority. No handshake, record
//! framing, key schedule, or AEAD is reimplemented here. The configuration is
//! built with [`rustls::client::ClientConfig::builder_with_provider`] passing
//! the ring provider **explicitly**: the workspace pins `rustls` to ring only
//! (no aws-lc-rs, no auto-installed process-default provider), so the
//! provider-less `ClientConfig::builder()` would have no provider to resolve.

use std::boxed::Box;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::vec::Vec;

use rustls::client::{ClientConfig, UnbufferedClientConnection};
use rustls::pki_types::ServerName;
use rustls::unbuffered::{
    ConnectionState, EncodeError, EncryptError, InsufficientSizeError,
};
use zeroize::Zeroize;

use bsql_postgres_proto::engine::Transport;

/// Maximum plaintext bytes in a single TLS record (RFC 8446 / RFC 5246).
/// `write` chunks its plaintext to this size so each `rustls` `encrypt`
/// produces exactly one record that fits in the fixed [`TLS_RECORD_SCRATCH`].
const MAX_PLAINTEXT_PER_RECORD: usize = 16384;

/// Fixed scratch capacity: one max-size plaintext record plus TLS framing
/// overhead (content-type byte, AEAD tag, record header, and headroom).
/// A `<=16 KiB` plaintext chunk always encrypts within this bound, so the
/// scratch never grows and `write` allocates nothing per call.
const TLS_RECORD_SCRATCH: usize = 16384 + 256;

/// Bytes requested from the socket per inbound read. One max record, so a
/// whole record is typically pulled in a single `read` syscall.
const RECV_CHUNK: usize = 16384;

// ===========================================================================
// Errors
// ===========================================================================

/// A failure in the TLS transport, generic over the inner transport's own
/// error so the core never bakes in a concrete socket error type.
///
/// `Send` whenever `E: Send` (and `rustls::Error` / `usize` are `Send`), which
/// the [`Transport::Error`] bound requires — every variant is built from
/// `Send` data, so the union travels across task boundaries with the async
/// driver.
#[derive(Debug)]
pub enum TlsError<E> {
    /// The inner socket transport failed (connection reset, broken pipe, …).
    Socket(E),
    /// `rustls` reported a TLS-protocol error: a handshake failure, a decrypt
    /// failure, a fatal alert from the peer, or a malformed record. The cause
    /// is carried verbatim; nothing is reclassified or swallowed.
    Tls(rustls::Error),
    /// A record's ciphertext did not fit the fixed encrypt scratch and
    /// `rustls` requested a larger buffer than the bound permits. Structurally
    /// unreachable for the `<=16 KiB` chunks `write` produces, but surfaced as
    /// a classified error rather than an `unwrap`: a future `rustls` overhead
    /// change is a loud failure, never silent corruption.
    RecordOversize {
        /// The output size `rustls` asked for, in bytes.
        required: usize,
    },
    /// `rustls` reported the encrypter is exhausted: the connection's
    /// sequence-number space or key schedule can encrypt no further records.
    /// A terminal state, surfaced rather than dropping the write.
    EncryptExhausted,
    /// The inner socket accepted zero bytes for a non-empty ciphertext queue —
    /// a stalled or broken write side. Classified rather than spun on (an
    /// `Ok(0)` loop would never make progress).
    WriteZero,
    /// The peer closed the connection before the TLS handshake completed
    /// (socket EOF mid-handshake). Distinct from a clean post-handshake close,
    /// which `read` reports as `Ok(0)`.
    ClosedDuringHandshake,
    /// `rustls` surfaced a `ConnectionState` this client does not model.
    /// `ConnectionState` is `#[non_exhaustive]`; rather than a silent wildcard
    /// that drops the state, an unmodelled state is a loud classified error.
    UnexpectedState,
}

impl<E: fmt::Display> fmt::Display for TlsError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(e) => write!(f, "TLS transport socket error: {e}"),
            Self::Tls(e) => write!(f, "TLS protocol error: {e}"),
            Self::RecordOversize { required } => write!(
                f,
                "TLS record exceeds the fixed encrypt scratch (required {required} bytes)"
            ),
            Self::EncryptExhausted => {
                write!(f, "TLS encrypter exhausted; no further records can be sent")
            }
            Self::WriteZero => {
                write!(f, "TLS socket accepted zero bytes for a non-empty ciphertext queue")
            }
            Self::ClosedDuringHandshake => {
                write!(f, "TLS peer closed the connection before the handshake completed")
            }
            Self::UnexpectedState => {
                write!(f, "TLS connection reached a state this client does not model")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for TlsError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Socket(e) => Some(e),
            Self::Tls(e) => Some(e),
            Self::RecordOversize { .. }
            | Self::EncryptExhausted
            | Self::WriteZero
            | Self::ClosedDuringHandshake
            | Self::UnexpectedState => None,
        }
    }
}

/// The sans-I/O subset of [`TlsError`]: classifications the synchronous TLS
/// state-machine steps can produce, before any socket error is possible. Kept
/// separate from the I/O-bearing variants so the sync core need not be generic
/// over the inner error type; the async layer lifts it via [`From`].
#[derive(Debug)]
enum TlsStepError {
    Tls(rustls::Error),
    RecordOversize { required: usize },
    EncryptExhausted,
    UnexpectedState,
}

impl<E> From<TlsStepError> for TlsError<E> {
    fn from(e: TlsStepError) -> Self {
        match e {
            TlsStepError::Tls(e) => Self::Tls(e),
            TlsStepError::RecordOversize { required } => Self::RecordOversize { required },
            TlsStepError::EncryptExhausted => Self::EncryptExhausted,
            TlsStepError::UnexpectedState => Self::UnexpectedState,
        }
    }
}

// ===========================================================================
// Configuration
// ===========================================================================

/// The process-wide client TLS configuration, built once and shared by every
/// connection.
///
/// Built with [`ClientConfig::builder_with_provider`] passing the ring
/// provider **explicitly** — the workspace pins `rustls` to ring only, so the
/// provider-less `ClientConfig::builder()` has no process-default provider to
/// resolve and would fail the moment provider resolution matters. The Mozilla
/// CA roots are loaded for real server-certificate verification; both TLS 1.2
/// and 1.3 are offered (the workspace enables `tls12` for legacy-PG reach).
///
/// Cached in a [`OnceLock`]: the first caller builds the config, every later
/// caller shares the same `Arc`. A build error is not cached — a transient
/// failure does not poison the slot.
///
/// # Errors
///
/// Returns the `rustls::Error` from protocol-version selection if the ring
/// provider somehow advertises no usable versions (it always does, so this is
/// surfaced rather than `unwrap`ped).
pub fn shared_client_config() -> Result<Arc<ClientConfig>, rustls::Error> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    if let Some(cfg) = CONFIG.get() {
        return Ok(Arc::clone(cfg));
    }
    let built = build_client_config()?;
    // A concurrent caller may win the race; `get_or_init` returns the single
    // installed value either way (the loser's `built` is dropped).
    Ok(Arc::clone(CONFIG.get_or_init(|| built)))
}

fn build_client_config() -> Result<Arc<ClientConfig>, rustls::Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

// ===========================================================================
// Transport
// ===========================================================================

/// What one synchronous inbound state-machine step needs the async layer to do
/// next. The byte movement (encode handshake output, decrypt records) has
/// already happened in the owned buffers; this only signals the next I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pumped {
    /// `WriteTraffic` reached: the handshake is complete and no inbound
    /// ciphertext remains to process. For `read`, more socket bytes are needed.
    Idle,
    /// `BlockedHandshake`, or a partial record with no full record available:
    /// more inbound ciphertext is needed from the socket.
    NeedRead,
    /// The peer (or both sides) sent `close_notify`.
    Closed,
}

/// A sans-I/O TLS client transport over `rustls::unbuffered`, wrapping an
/// inner byte [`Transport`].
///
/// Construct with [`TlsTransport::connect`], which drives the handshake to
/// completion before returning a ready transport. Then drive it through the
/// [`Transport`] quartet exactly like a plaintext socket.
pub struct TlsTransport<Inner: Transport> {
    conn: UnbufferedClientConnection,
    inner: Inner,
    /// Outbound ciphertext awaiting the socket; `out_sent` bytes from the
    /// front are already written (the cancel-safe send cursor).
    out_buf: Vec<u8>,
    out_sent: usize,
    /// Inbound ciphertext pulled from the socket; `rustls` decrypts it in
    /// place. `staging_start` bytes from the front are already consumed.
    staging: Vec<u8>,
    staging_start: usize,
    /// Decrypted inbound plaintext awaiting copy-out; `plaintext_start` bytes
    /// from the front are already delivered to the caller.
    plaintext: Vec<u8>,
    plaintext_start: usize,
    /// Fixed one-record encrypt/encode scratch, reused across calls.
    scratch: Box<[u8; TLS_RECORD_SCRATCH]>,
}

impl<Inner: Transport> fmt::Debug for TlsTransport<Inner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Buffer *contents* (plaintext + ciphertext) are deliberately omitted.
        f.debug_struct("TlsTransport")
            .field("out_pending", &(self.out_buf.len() - self.out_sent))
            .field("staging_pending", &(self.staging.len() - self.staging_start))
            .field("plaintext_pending", &(self.plaintext.len() - self.plaintext_start))
            .finish_non_exhaustive()
    }
}

impl<Inner: Transport> TlsTransport<Inner> {
    /// Establish a TLS session over an already-connected `inner` transport.
    ///
    /// Drives the rustls handshake to completion: encodes handshake records to
    /// the socket and reads the peer's flights until the connection reaches
    /// the data phase, then returns a ready transport. This is a deliberate
    /// driver-side connect step rather than handshake-on-first-write, so
    /// [`write`](Transport::write) keeps its one-attempt contract.
    ///
    /// The caller is responsible for the plaintext PostgreSQL `SSLRequest`
    /// negotiation (the 8-byte request and the `'S'`/`'N'` reply) on the raw
    /// socket *before* calling this: `connect` assumes the server has agreed to
    /// TLS and the socket is positioned for the ClientHello.
    ///
    /// A blocking (sync) driver drives this future with a single-poll executor
    /// over a blocking `inner` (the same `poll_once`/`Waker::noop` pattern the
    /// engine verbs use): every inner `read`/`write` resolves synchronously, so
    /// the whole handshake future completes in one poll.
    ///
    /// # Errors
    ///
    /// [`TlsError::Tls`] for a handshake/certificate failure,
    /// [`TlsError::Socket`] for an inner I/O failure, and
    /// [`TlsError::ClosedDuringHandshake`] if the peer closes before the
    /// handshake completes.
    pub async fn connect(
        inner: Inner,
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Result<Self, TlsError<Inner::Error>> {
        let conn =
            UnbufferedClientConnection::new(config, server_name).map_err(TlsError::Tls)?;
        let mut transport = Self::with_conn(conn, inner);
        transport.drive_handshake().await?;
        Ok(transport)
    }

    fn with_conn(conn: UnbufferedClientConnection, inner: Inner) -> Self {
        Self {
            conn,
            inner,
            out_buf: Vec::new(),
            out_sent: 0,
            staging: Vec::new(),
            staging_start: 0,
            plaintext: Vec::new(),
            plaintext_start: 0,
            scratch: Box::new([0u8; TLS_RECORD_SCRATCH]),
        }
    }

    /// Reclaim the already-sent prefix of the outbound queue without a
    /// per-record `Vec::drain`: front-drained by copying the unsent tail down.
    fn reclaim_out(&mut self) {
        if self.out_sent == 0 {
            return;
        }
        if self.out_sent >= self.out_buf.len() {
            self.out_buf.clear();
        } else {
            self.out_buf.copy_within(self.out_sent.., 0);
            self.out_buf.truncate(self.out_buf.len() - self.out_sent);
        }
        self.out_sent = 0;
    }

    /// Drop the consumed prefix of the inbound staging buffer (same front-drain
    /// as [`reclaim_out`](Self::reclaim_out)), preserving any partial record.
    fn compact_staging(&mut self) {
        if self.staging_start == 0 {
            return;
        }
        if self.staging_start >= self.staging.len() {
            self.staging.clear();
        } else {
            self.staging.copy_within(self.staging_start.., 0);
            self.staging.truncate(self.staging.len() - self.staging_start);
        }
        self.staging_start = 0;
    }

    /// Overwrite every owned buffer that has held plaintext (or ciphertext)
    /// with zeros. Plaintext residue — the staging bytes (a record is decrypted
    /// out of here into an owned chunk) and the inbound plaintext buffer — is
    /// the post-handshake secret; the ciphertext buffers are scrubbed too
    /// (cheap, same pass). `Vec::zeroize` clears the full CURRENT allocation
    /// (length and spare capacity), then empties the buffer. A reallocation
    /// during growth frees the prior, smaller block unscrubbed; steady-state
    /// operation does not reallocate, so the live allocation at drop is cleared.
    fn scrub(&mut self) {
        self.plaintext.zeroize();
        self.staging.zeroize();
        self.out_buf.zeroize();
        self.scratch[..].zeroize();
    }

    /// Drain the outbound ciphertext queue to the socket, advancing the
    /// cancel-safe send cursor. No `inner.flush` — see [`flush_impl`].
    async fn flush_to_socket(&mut self) -> Result<(), TlsError<Inner::Error>> {
        while self.out_sent < self.out_buf.len() {
            let n = self
                .inner
                .write(&self.out_buf[self.out_sent..])
                .await
                .map_err(TlsError::Socket)?;
            if n == 0 {
                return Err(TlsError::WriteZero);
            }
            self.out_sent += n;
        }
        self.reclaim_out();
        Ok(())
    }

    async fn flush_impl(&mut self) -> Result<(), TlsError<Inner::Error>> {
        self.flush_to_socket().await?;
        self.inner.flush().await.map_err(TlsError::Socket)?;
        Ok(())
    }

    /// Pull more ciphertext from the socket into the staging buffer. Returns
    /// the byte count (0 = clean EOF).
    async fn recv_more(&mut self) -> Result<usize, TlsError<Inner::Error>> {
        self.compact_staging();
        let base = self.staging.len();
        // Zero-fill the read window (no `unsafe` uninit read); the socket
        // overwrites the bytes it returns and the tail is truncated away.
        self.staging.resize(base + RECV_CHUNK, 0);
        let n = self
            .inner
            .read(&mut self.staging[base..])
            .await
            .map_err(TlsError::Socket)?;
        self.staging.truncate(base + n);
        Ok(n)
    }

    async fn drive_handshake(&mut self) -> Result<(), TlsError<Inner::Error>> {
        loop {
            let signal = pump_inbound(
                &mut self.conn,
                &mut self.staging[..],
                &mut self.staging_start,
                &mut self.out_buf,
                &mut self.plaintext,
                &mut self.scratch[..],
            )?;
            // Emit any handshake records the step produced before blocking on
            // the peer's reply, and flush so they actually reach the socket.
            self.flush_impl().await?;
            match signal {
                Pumped::Idle => return Ok(()),
                Pumped::Closed => return Err(TlsError::ClosedDuringHandshake),
                Pumped::NeedRead => {
                    if self.recv_more().await? == 0 {
                        return Err(TlsError::ClosedDuringHandshake);
                    }
                }
            }
        }
    }

    async fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize, TlsError<Inner::Error>> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            // 1. Serve buffered plaintext first (a record may span several
            //    reads when the caller buffer is smaller than the record).
            if self.plaintext_start < self.plaintext.len() {
                let avail = &self.plaintext[self.plaintext_start..];
                let n = avail.len().min(buf.len());
                buf[..n].copy_from_slice(&avail[..n]);
                self.plaintext_start += n;
                if self.plaintext_start >= self.plaintext.len() {
                    self.plaintext.clear();
                    self.plaintext_start = 0;
                }
                return Ok(n);
            }
            // 2. Decrypt staged ciphertext into the plaintext buffer.
            //
            //    Temporary inbound floor: a record travels socket -> staging
            //    (copy), is decrypted by rustls into an owned chunk (the +1
            //    allocation), whose payload is copied into this owned plaintext
            //    buffer (a second residence) before the final copy-out to the
            //    caller. The return path is an in-place decrypt that lends
            //    rustls' decrypted region directly
            //    to the engine's single-residence ingest buffer, removing the
            //    owned plaintext buffer and one copy. The cost is bounded per
            //    record and does not grow; only the extra residence remains.
            let signal = pump_inbound(
                &mut self.conn,
                &mut self.staging[..],
                &mut self.staging_start,
                &mut self.out_buf,
                &mut self.plaintext,
                &mut self.scratch[..],
            )?;
            // 3. Drive any client output the step produced (a post-handshake
            //    key update or new-session-ticket acknowledgement) to the wire.
            if self.out_sent < self.out_buf.len() {
                self.flush_impl().await?;
            }
            // 4. If plaintext became available, loop to serve it.
            if self.plaintext_start < self.plaintext.len() {
                continue;
            }
            match signal {
                Pumped::Closed => return Ok(0),
                Pumped::Idle | Pumped::NeedRead => {
                    if self.recv_more().await? == 0 {
                        return Ok(0);
                    }
                }
            }
        }
    }

    async fn write_impl(&mut self, plaintext: &[u8]) -> Result<usize, TlsError<Inner::Error>> {
        if plaintext.is_empty() {
            return Ok(0);
        }
        // Reclaim any sent prefix so the queue does not accumulate already-sent
        // ciphertext across a cancelled flush.
        self.reclaim_out();
        // Encrypt the whole plaintext into records appended to the queue. Once
        // encrypted, the plaintext is irrevocably committed to the TLS record
        // stream, so the consumed count is the full length regardless of how
        // much the socket later accepts. No socket I/O happens here — a
        // cancelled `write` cannot half-send a record, because the bytes only
        // reach the socket in `flush`, behind the cancel-safe send cursor.
        encrypt_app_data(
            &mut self.conn,
            plaintext,
            &mut self.out_buf,
            &mut self.scratch[..],
        )?;
        Ok(plaintext.len())
    }

    async fn shutdown_impl(&mut self) -> Result<(), TlsError<Inner::Error>> {
        // Queue close_notify so the peer can tell a clean close from a
        // truncation attack, drain it to the socket, then shut the write half.
        self.reclaim_out();
        queue_close_notify(&mut self.conn, &mut self.out_buf, &mut self.scratch[..])?;
        self.flush_impl().await?;
        self.inner.shutdown().await.map_err(TlsError::Socket)?;
        Ok(())
    }
}

impl<Inner: Transport> Drop for TlsTransport<Inner> {
    fn drop(&mut self) {
        self.scrub();
    }
}

impl<Inner: Transport> Transport for TlsTransport<Inner> {
    type Error = TlsError<Inner::Error>;

    fn is_would_block(err: &Self::Error) -> bool {
        match err {
            // Delegate a socket-level deadline to the inner transport; a
            // TLS-protocol error is a genuine failure, never a read deadline.
            TlsError::Socket(inner) => Inner::is_would_block(inner),
            TlsError::Tls(_)
            | TlsError::RecordOversize { .. }
            | TlsError::EncryptExhausted
            | TlsError::WriteZero
            | TlsError::ClosedDuringHandshake
            | TlsError::UnexpectedState => false,
        }
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        self.read_impl(buf)
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        self.write_impl(buf)
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.flush_impl()
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.shutdown_impl()
    }
}

// ===========================================================================
// Synchronous TLS state-machine steps (sans-I/O)
// ===========================================================================
//
// These free functions take the owned buffers by reference (not `&mut self`)
// so the borrow checker can prove `conn` and `staging` are disjoint from the
// output buffers within a single `process_tls_records` borrow. They never
// touch the socket: the async methods above own all I/O. No `rustls` borrow is
// ever held across an `await`, which is what keeps the quartet futures `Send`.

/// Drive the connection over the staged ciphertext: encode handshake output
/// into `out_buf`, decrypt application records into `plaintext`, and advance
/// the staging cursor by every discarded byte. Returns when the connection
/// reaches a state needing external action.
fn pump_inbound(
    conn: &mut UnbufferedClientConnection,
    staging: &mut [u8],
    staging_start: &mut usize,
    out_buf: &mut Vec<u8>,
    plaintext: &mut Vec<u8>,
    scratch: &mut [u8],
) -> Result<Pumped, TlsStepError> {
    loop {
        let status = conn.process_tls_records(&mut staging[*staging_start..]);
        let mut discard = status.discard;
        let signal: Option<Pumped> = match status.state {
            Err(e) => return Err(TlsStepError::Tls(e)),
            Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                match enc.encode(scratch) {
                    Ok(n) => out_buf.extend_from_slice(&scratch[..n]),
                    Err(EncodeError::InsufficientSize(InsufficientSizeError {
                        required_size,
                    })) => {
                        return Err(TlsStepError::RecordOversize {
                            required: required_size,
                        });
                    }
                    Err(EncodeError::AlreadyEncoded) => {}
                }
                None
            }
            Ok(ConnectionState::TransmitTlsData(t)) => {
                t.done();
                None
            }
            Ok(ConnectionState::ReadTraffic(mut rt)) => {
                let mut got = false;
                loop {
                    match rt.next_record() {
                        Some(Ok(rec)) => {
                            plaintext.extend_from_slice(rec.payload);
                            discard += rec.discard;
                            got = true;
                        }
                        Some(Err(e)) => return Err(TlsStepError::Tls(e)),
                        None => break,
                    }
                }
                // Records drained: loop to reach the next state. None available:
                // the staged bytes are a partial record — need more from the
                // socket.
                if got {
                    None
                } else {
                    Some(Pumped::NeedRead)
                }
            }
            Ok(ConnectionState::BlockedHandshake) => Some(Pumped::NeedRead),
            Ok(ConnectionState::WriteTraffic(_)) => Some(Pumped::Idle),
            Ok(ConnectionState::PeerClosed | ConnectionState::Closed) => Some(Pumped::Closed),
            // ConnectionState is #[non_exhaustive]: an unmodelled state is a
            // loud error, never a silent wildcard that drops it.
            Ok(_) => return Err(TlsStepError::UnexpectedState),
        };
        if discard > 0 {
            *staging_start += discard;
        }
        if let Some(sig) = signal {
            return Ok(sig);
        }
    }
}

/// Encrypt `plaintext` into `<=`one-record chunks, appending each ciphertext
/// record to `out_buf`. The fixed `scratch` holds exactly one record, so it
/// never grows.
fn encrypt_app_data(
    conn: &mut UnbufferedClientConnection,
    plaintext: &[u8],
    out_buf: &mut Vec<u8>,
    scratch: &mut [u8],
) -> Result<(), TlsStepError> {
    let mut off = 0;
    while off < plaintext.len() {
        let end = (off + MAX_PLAINTEXT_PER_RECORD).min(plaintext.len());
        let chunk = &plaintext[off..end];
        let mut empty: [u8; 0] = [];
        loop {
            let status = conn.process_tls_records(&mut empty);
            match status.state {
                Err(e) => return Err(TlsStepError::Tls(e)),
                Ok(ConnectionState::WriteTraffic(mut wt)) => match wt.encrypt(chunk, scratch) {
                    Ok(n) => {
                        out_buf.extend_from_slice(&scratch[..n]);
                        break;
                    }
                    Err(EncryptError::InsufficientSize(InsufficientSizeError {
                        required_size,
                    })) => {
                        return Err(TlsStepError::RecordOversize {
                            required: required_size,
                        });
                    }
                    Err(EncryptError::EncryptExhausted) => {
                        return Err(TlsStepError::EncryptExhausted);
                    }
                },
                Ok(ConnectionState::EncodeTlsData(mut enc)) => match enc.encode(scratch) {
                    Ok(n) => out_buf.extend_from_slice(&scratch[..n]),
                    Err(EncodeError::InsufficientSize(InsufficientSizeError {
                        required_size,
                    })) => {
                        return Err(TlsStepError::RecordOversize {
                            required: required_size,
                        });
                    }
                    Err(EncodeError::AlreadyEncoded) => {}
                },
                Ok(ConnectionState::TransmitTlsData(t)) => t.done(),
                // Post-handshake the write path reaches only the states above;
                // anything else (a peer close, a blocked read) is unexpected.
                Ok(_) => return Err(TlsStepError::UnexpectedState),
            }
        }
        off = end;
    }
    Ok(())
}

/// Queue a `close_notify` alert record into `out_buf`, driving past any
/// residual handshake/key-update output first.
fn queue_close_notify(
    conn: &mut UnbufferedClientConnection,
    out_buf: &mut Vec<u8>,
    scratch: &mut [u8],
) -> Result<(), TlsStepError> {
    let mut empty: [u8; 0] = [];
    loop {
        let status = conn.process_tls_records(&mut empty);
        match status.state {
            Err(e) => return Err(TlsStepError::Tls(e)),
            Ok(ConnectionState::WriteTraffic(mut wt)) => {
                match wt.queue_close_notify(scratch) {
                    Ok(n) => {
                        out_buf.extend_from_slice(&scratch[..n]);
                        return Ok(());
                    }
                    Err(EncryptError::InsufficientSize(InsufficientSizeError {
                        required_size,
                    })) => {
                        return Err(TlsStepError::RecordOversize {
                            required: required_size,
                        });
                    }
                    Err(EncryptError::EncryptExhausted) => {
                        return Err(TlsStepError::EncryptExhausted);
                    }
                }
            }
            Ok(ConnectionState::EncodeTlsData(mut enc)) => match enc.encode(scratch) {
                Ok(n) => out_buf.extend_from_slice(&scratch[..n]),
                Err(EncodeError::InsufficientSize(InsufficientSizeError { required_size })) => {
                    return Err(TlsStepError::RecordOversize {
                        required: required_size,
                    });
                }
                Err(EncodeError::AlreadyEncoded) => {}
            },
            Ok(ConnectionState::TransmitTlsData(t)) => t.done(),
            Ok(_) => return Err(TlsStepError::UnexpectedState),
        }
    }
}
