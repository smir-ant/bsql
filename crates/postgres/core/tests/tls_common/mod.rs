//! Shared TLS test harness: a deterministic in-memory client<->server
//! loopback over a real `rustls::unbuffered` server, plus the static
//! self-signed test certificate and the test-only configs.
//!
//! The genuine rustls key exchange + AEAD run on both sides; only the client's
//! certificate-chain/name *check* is stubbed (a [`NoVerify`] verifier), which
//! is irrelevant to transport mechanics. No network, no threads: the server is
//! embedded in the mock socket and produces its flights on demand, so every
//! transport future resolves in a single poll.

use std::future::Future;
use std::sync::{Arc, Mutex};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::ClientConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
};
use rustls::server::{ServerConfig, UnbufferedServerConnection};
use rustls::unbuffered::{ConnectionState, EncodeError, EncryptError, InsufficientSizeError};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};

use bsql_postgres_proto::engine::Transport;

/// Static self-signed certificate (DER), CN=localhost. Generated once and
/// committed so the test adds no certificate-generator dependency.
pub const CERT_DER: &[u8] = include_bytes!("cert.der");
/// Static PKCS#8 private key (DER) for [`CERT_DER`] — ECDSA P-256.
pub const KEY_DER: &[u8] = include_bytes!("key.pk8.der");

// ===========================================================================
// Test-only certificate verifier (transport probe, not a security artifact).
// ===========================================================================

/// Accepts any server certificate without checking the chain or name. The real
/// key exchange + signature + AEAD still run; only the verification *decision*
/// is stubbed, which is irrelevant to the transport mechanics under test.
#[derive(Debug)]
pub struct NoVerify {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ===========================================================================
// Configs (explicit ring provider — the production form).
// ===========================================================================

/// Build a client config that exercises the production provider-explicit path
/// (`builder_with_provider(ring)`) but swaps in [`NoVerify`] so the static
/// self-signed cert is accepted.
pub fn test_client_config() -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("client protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify { provider }))
        .with_no_client_auth();
    Arc::new(config)
}

/// Build a server config from the static test cert + key, ring provider.
pub fn test_server_config() -> Arc<ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cert = CertificateDer::from(CERT_DER);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(KEY_DER));
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("server protocol versions")
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .expect("server single cert");
    Arc::new(config)
}

/// The server name the client connects to (matches the cert's CN/SAN).
pub fn test_server_name() -> ServerName<'static> {
    ServerName::try_from("localhost")
        .expect("valid server name")
        .to_owned()
}

// ===========================================================================
// In-memory loopback socket with an embedded rustls server.
// ===========================================================================

/// The shared loopback state: the embedded server connection, the two
/// ciphertext wires, and the back-pressure / split knobs.
pub struct LoopbackInner {
    server: UnbufferedServerConnection,
    /// Client -> server ciphertext (the client writes here).
    c2s: Vec<u8>,
    /// Server -> client ciphertext (the client reads here).
    s2c: Vec<u8>,
    /// Client read cursor into `s2c`.
    s2c_pos: usize,
    /// Decrypted application data the server has received.
    pub server_recv: Vec<u8>,
    /// Set once the server observes the peer's `close_notify`.
    pub server_closed: bool,
    /// Max ciphertext bytes the socket accepts per client write (0 = all).
    pub send_cap: usize,
    /// Max ciphertext bytes delivered per client read (0 = all available).
    pub recv_cap: usize,
    /// Reusable server-side encode/encrypt staging.
    scratch: Vec<u8>,
}

impl LoopbackInner {
    fn new() -> Self {
        let server = UnbufferedServerConnection::new(test_server_config())
            .expect("server connection");
        Self {
            server,
            c2s: Vec::new(),
            s2c: Vec::new(),
            s2c_pos: 0,
            server_recv: Vec::new(),
            server_closed: false,
            send_cap: 0,
            recv_cap: 0,
            scratch: vec![0u8; 4096],
        }
    }

    /// One client socket write attempt: accept up to `send_cap` ciphertext
    /// bytes onto the `c2s` wire (partial accept under back-pressure).
    fn client_write(&mut self, buf: &[u8]) -> usize {
        let n = if self.send_cap == 0 {
            buf.len()
        } else {
            self.send_cap.min(buf.len())
        };
        self.c2s.extend_from_slice(&buf[..n]);
        n
    }

    /// One client socket read: deliver up to `recv_cap` ciphertext bytes from
    /// the `s2c` wire. If the wire is empty, pump the server once to generate
    /// its next flight; a still-empty wire is a clean EOF (returns 0).
    fn client_read(&mut self, buf: &mut [u8]) -> usize {
        if self.s2c_pos >= self.s2c.len() {
            self.pump_server();
        }
        let avail = self.s2c.len() - self.s2c_pos;
        if avail == 0 {
            return 0;
        }
        let cap = if self.recv_cap == 0 { avail } else { self.recv_cap.min(avail) };
        let n = cap.min(buf.len());
        buf[..n].copy_from_slice(&self.s2c[self.s2c_pos..self.s2c_pos + n]);
        self.s2c_pos += n;
        if self.s2c_pos >= self.s2c.len() {
            self.s2c.clear();
            self.s2c_pos = 0;
        }
        n
    }

    /// Drive the embedded server over the queued `c2s` ciphertext: emit its
    /// handshake flights into `s2c`, deposit decrypted app-data into
    /// `server_recv`, and observe a peer close.
    pub fn pump_server(&mut self) {
        let Self {
            server,
            c2s,
            s2c,
            server_recv,
            server_closed,
            scratch,
            ..
        } = self;
        let mut start = 0usize;
        loop {
            let status = server.process_tls_records(&mut c2s[start..]);
            let mut discard = status.discard;
            let stop = match status.state {
                Err(e) => panic!("server TLS error: {e}"),
                Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                    loop {
                        match enc.encode(scratch.as_mut_slice()) {
                            Ok(n) => {
                                s2c.extend_from_slice(&scratch[..n]);
                                break;
                            }
                            Err(EncodeError::InsufficientSize(InsufficientSizeError {
                                required_size,
                            })) => {
                                if scratch.len() < required_size {
                                    scratch.resize(required_size, 0);
                                }
                            }
                            Err(EncodeError::AlreadyEncoded) => break,
                        }
                    }
                    false
                }
                Ok(ConnectionState::TransmitTlsData(t)) => {
                    t.done();
                    false
                }
                Ok(ConnectionState::ReadTraffic(mut rt)) => {
                    while let Some(rec) = rt.next_record() {
                        match rec {
                            Ok(r) => {
                                server_recv.extend_from_slice(r.payload);
                                discard += r.discard;
                            }
                            Err(e) => panic!("server record error: {e}"),
                        }
                    }
                    false
                }
                Ok(ConnectionState::BlockedHandshake | ConnectionState::WriteTraffic(_)) => true,
                Ok(ConnectionState::PeerClosed | ConnectionState::Closed) => {
                    *server_closed = true;
                    true
                }
                Ok(_) => true,
            };
            if discard > 0 {
                start += discard;
            }
            if stop {
                break;
            }
        }
        if start > 0 {
            if start >= c2s.len() {
                c2s.clear();
            } else {
                c2s.copy_within(start.., 0);
                c2s.truncate(c2s.len() - start);
            }
        }
    }

    /// Encrypt `data` as one or more app-data records onto the `s2c` wire.
    /// Requires the server handshake to be complete (pump it first).
    pub fn server_send_app(&mut self, data: &[u8]) {
        let Self {
            server, s2c, scratch, ..
        } = self;
        let mut empty: [u8; 0] = [];
        loop {
            let status = server.process_tls_records(&mut empty);
            match status.state {
                Err(e) => panic!("server TLS error: {e}"),
                Ok(ConnectionState::WriteTraffic(mut wt)) => loop {
                    match wt.encrypt(data, scratch.as_mut_slice()) {
                        Ok(n) => {
                            s2c.extend_from_slice(&scratch[..n]);
                            return;
                        }
                        Err(EncryptError::InsufficientSize(InsufficientSizeError {
                            required_size,
                        })) => {
                            if scratch.len() < required_size {
                                scratch.resize(required_size, 0);
                            }
                        }
                        Err(EncryptError::EncryptExhausted) => panic!("server encrypt exhausted"),
                    }
                },
                Ok(ConnectionState::EncodeTlsData(mut enc)) => loop {
                    match enc.encode(scratch.as_mut_slice()) {
                        Ok(n) => {
                            s2c.extend_from_slice(&scratch[..n]);
                            break;
                        }
                        Err(EncodeError::InsufficientSize(InsufficientSizeError {
                            required_size,
                        })) => {
                            if scratch.len() < required_size {
                                scratch.resize(required_size, 0);
                            }
                        }
                        Err(EncodeError::AlreadyEncoded) => break,
                    }
                },
                Ok(ConnectionState::TransmitTlsData(t)) => t.done(),
                Ok(_) => panic!("server: unexpected state before app-data write"),
            }
        }
    }

    /// Bytes of client ciphertext currently queued on the `c2s` wire.
    pub fn c2s_len(&self) -> usize {
        self.c2s.len()
    }
}

/// A `Transport` mock backed by an in-memory [`LoopbackInner`]. Cloning shares
/// the same state, so a test can inspect the server side after the client
/// transport has consumed its `MockInner`.
#[derive(Clone)]
pub struct MockInner {
    pub state: Arc<Mutex<LoopbackInner>>,
}

impl MockInner {
    /// Construct a fresh loopback pair; returns the mock and a shared handle to
    /// the server-side state.
    pub fn new() -> (Self, Arc<Mutex<LoopbackInner>>) {
        let state = Arc::new(Mutex::new(LoopbackInner::new()));
        (
            Self {
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl Transport for MockInner {
    type Error = std::convert::Infallible;

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        let state = Arc::clone(&self.state);
        async move {
            let mut g = state.lock().expect("loopback mutex");
            Ok(g.client_read(buf))
        }
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        let state = Arc::clone(&self.state);
        async move {
            let mut g = state.lock().expect("loopback mutex");
            Ok(g.client_write(buf))
        }
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        std::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        std::future::ready(Ok(()))
    }
}

// ===========================================================================
// Single-poll executor (the loopback never blocks).
// ===========================================================================

/// Drive `fut` to completion. The in-memory loopback is always ready, so one
/// poll suffices; a `Pending` means a harness bug and panics loudly.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        std::task::Poll::Pending => {
            panic!("loopback future returned Pending; the in-memory transport never blocks")
        }
    }
}
