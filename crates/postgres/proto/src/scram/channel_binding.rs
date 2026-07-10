//! SCRAM-SHA-256-PLUS channel binding (RFC 5802 §6 + RFC 5929 `tls-server-end-point`).
//!
//! Channel binding cryptographically ties a SCRAM exchange to the specific TLS
//! channel it runs over, so a relay/MITM holding a *valid cert for a different
//! name* (a compromised proxy terminating TLS, a mis-issued cert) cannot forward
//! the exchange: the binding data it would have to reproduce is a hash of the
//! server's own certificate, which it does not control. Without channel binding,
//! full cert+hostname verification blocks the bogus-cert case at the handshake,
//! but the valid-cert-relay residual remains — this module closes it, matching
//! libpq's `channel_binding=require` capability.
//!
//! # `tls-server-end-point` (PostgreSQL's binding type)
//!
//! PostgreSQL binds to `tls-server-end-point` (RFC 5929 §4.1): the binding data
//! is a hash of the server's DER-encoded end-entity certificate. The hash
//! function is the one named in the certificate's own `signatureAlgorithm`,
//! **except** that MD5 and SHA-1 are upgraded to SHA-256 (RFC 5929 §4.1). This
//! module reads the `signatureAlgorithm` OID from the certificate DER with a
//! bounded, total ASN.1 walk (never a panic on hostile bytes) and selects the
//! matching SHA-2 hash; any OID it does not recognise defaults to SHA-256 (see
//! [`tls_server_end_point`] for the exact coverage). The DER-encoded certificate
//! itself is supplied by the TLS layer (`rustls::peer_certificates`); this crate
//! never parses X.509 for trust — only reads the one OID field needed to pick
//! the hash, delegating every cryptographic operation to `sha2`.
//!
//! # What this crate decides vs what the driver supplies
//!
//! The driver (which owns the TLS transport) computes the [`ChannelBindingData`]
//! from the negotiated server certificate and resolves the consumer's policy
//! into a [`ChannelBinding`]; the engine then combines that with the server's
//! advertised SASL mechanism list (via [`decide_sasl_choice`]) to pick the
//! [`SaslChoice`] — the gs2 flag and mechanism name actually put on the wire.

use crate::scram::wire::ScramError;
use crate::wire::{SCRAM_SHA_256_MECHANISM, SCRAM_SHA_256_PLUS_MECHANISM};

/// Maximum `tls-server-end-point` binding-data length: a SHA-512 digest is
/// 64 bytes, the widest hash this module produces.
pub const MAX_CBIND_DATA_LEN: usize = 64;

/// Longest gs2 channel-binding header this client emits
/// (`p=tls-server-end-point,,`).
pub const MAX_GS2_HEADER_LEN: usize = GS2_HEADER_SERVER_END_POINT.len();

/// Longest cbind-input — the `gs2-header || cbind-data` string that RFC 5802 §6
/// base64-encodes into the client-final `c=` value. Widest for
/// `ServerEndPoint`: the `p=…` header plus a SHA-512 cert hash.
pub const MAX_CBIND_INPUT_LEN: usize = MAX_GS2_HEADER_LEN + MAX_CBIND_DATA_LEN;

/// Base64-encoded length (RFC 4648 padded) of the longest cbind-input — the
/// worst-case width of the client-final `c=` value.
pub const MAX_CBIND_B64_LEN: usize = MAX_CBIND_INPUT_LEN.div_ceil(3).saturating_mul(4);

/// gs2 channel-binding header for `p=tls-server-end-point` (RFC 5802 §7
/// `gs2-cbind-flag` = `p`, no authzid). Prepended to the client-first-message
/// and echoed (with the binding data appended) in the client-final `c=` value.
pub const GS2_HEADER_SERVER_END_POINT: &[u8] = b"p=tls-server-end-point,,";

/// gs2 header for a client that does **not** support channel binding (`n,,`).
/// Used on a plaintext connection, or when the consumer set
/// `channel_binding=disable`.
pub const GS2_HEADER_NONE: &[u8] = b"n,,";

/// gs2 header for a client that **supports** channel binding but is not using it
/// because the server did not offer a `-PLUS` mechanism (`y,,`). This is the
/// RFC 5802 §6 anti-downgrade signal: a server that *does* support channel
/// binding, seeing `y`, must fail — so a MITM that stripped `-PLUS` from the
/// mechanism list is detected.
pub const GS2_HEADER_SUPPORTED_UNUSED: &[u8] = b"y,,";

/// The `tls-server-end-point` channel-binding data: a hash of the server's
/// DER-encoded end-entity certificate (RFC 5929 §4.1).
///
/// Wire-public — it is derived entirely from the server's *public* certificate
/// and travels base64-encoded in the SCRAM client-final `c=` field — so it is
/// not a secret and is neither redacted in `Debug` nor zeroized on drop.
/// Minted only by [`tls_server_end_point`]; the internal buffer is private, so a
/// consumer cannot fabricate binding data out of thin air.
#[derive(Clone, Copy, Debug)]
pub struct ChannelBindingData {
    /// The digest bytes, followed by zero padding up to [`MAX_CBIND_DATA_LEN`].
    buf: [u8; MAX_CBIND_DATA_LEN],
    /// The number of valid leading bytes in `buf` (32 / 48 / 64 for
    /// SHA-256 / 384 / 512).
    len: usize,
}

impl ChannelBindingData {
    /// The binding-data bytes (the certificate hash).
    #[must_use]
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.buf.get(..self.len).unwrap_or(&[])
    }

    /// Build from a raw digest slice, clamped to [`MAX_CBIND_DATA_LEN`]. The
    /// clamp is defensive: every hash this module produces is `<= 64` bytes, so
    /// no truncation ever occurs in practice.
    fn from_digest(digest: &[u8]) -> Self {
        let mut buf = [0u8; MAX_CBIND_DATA_LEN];
        let len = digest.len().min(MAX_CBIND_DATA_LEN);
        if let (Some(dst), Some(src)) = (buf.get_mut(..len), digest.get(..len)) {
            dst.copy_from_slice(src);
        }
        Self { buf, len }
    }
}

/// The resolved channel-binding context for a SCRAM exchange, produced by the
/// driver from the transport and the consumer's `channel_binding` policy, then
/// carried in the SCRAM credential into the engine.
///
/// `#[non_exhaustive]`: a future binding type (RFC 9266 `tls-exporter`) would be
/// a new variant. Constructed by the driver, matched only inside this crate.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ChannelBinding {
    /// No channel binding: the connection is plaintext, or the consumer set
    /// `channel_binding=disable`. The engine sends gs2 `n,,` and plain
    /// SCRAM-SHA-256.
    Unbound,
    /// The connection is TLS and `tls-server-end-point` binding data is
    /// available. The engine selects SCRAM-SHA-256-PLUS (gs2 `p=…`) when the
    /// server offers it; otherwise gs2 `y,,` (when `require` is false) or a
    /// classified [`ScramError::ChannelBindingRequired`] refusal (when `true`).
    Available {
        /// The `tls-server-end-point` binding data (server certificate hash).
        data: ChannelBindingData,
        /// When `true` (the consumer set `channel_binding=require`), a server
        /// that does not offer `-PLUS` is a fail-closed refusal, never a
        /// fallback to plain SCRAM.
        require: bool,
    },
}

impl ChannelBinding {
    /// The binding data, present only for [`Self::Available`]. Used to build the
    /// `ServerEndPoint` cbind-input.
    #[must_use]
    #[inline]
    pub fn data(&self) -> Option<&ChannelBindingData> {
        match self {
            Self::Available { data, .. } => Some(data),
            Self::Unbound => None,
        }
    }
}

/// The concrete SCRAM mechanism + gs2 channel-binding flag the engine will put
/// on the wire, resolved by [`decide_sasl_choice`] from the server's offer and
/// the [`ChannelBinding`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaslChoice {
    /// Plain SCRAM-SHA-256, gs2 `n,,` — the client is not channel-binding-capable
    /// (plaintext / disabled).
    NoBinding,
    /// Plain SCRAM-SHA-256, gs2 `y,,` — the client is channel-binding-capable
    /// (TLS) but the server offered no `-PLUS` mechanism (anti-downgrade signal).
    SupportedButUnused,
    /// SCRAM-SHA-256-PLUS, gs2 `p=tls-server-end-point,,` — channel binding in
    /// use, bound to the server certificate hash.
    ServerEndPoint,
}

impl SaslChoice {
    /// The gs2 channel-binding header prepended to the client-first-message for
    /// this choice.
    #[must_use]
    #[inline]
    pub fn gs2_header(self) -> &'static [u8] {
        match self {
            Self::NoBinding => GS2_HEADER_NONE,
            Self::SupportedButUnused => GS2_HEADER_SUPPORTED_UNUSED,
            Self::ServerEndPoint => GS2_HEADER_SERVER_END_POINT,
        }
    }

    /// The SASL mechanism name advertised in the `SASLInitialResponse` for this
    /// choice: `SCRAM-SHA-256-PLUS` for [`Self::ServerEndPoint`], otherwise
    /// `SCRAM-SHA-256`.
    #[must_use]
    #[inline]
    pub fn mechanism(self) -> &'static [u8] {
        match self {
            Self::ServerEndPoint => SCRAM_SHA_256_PLUS_MECHANISM,
            Self::NoBinding | Self::SupportedButUnused => SCRAM_SHA_256_MECHANISM,
        }
    }

    /// Whether this choice uses channel binding (only [`Self::ServerEndPoint`]).
    #[must_use]
    #[inline]
    pub fn uses_binding(self) -> bool {
        matches!(self, Self::ServerEndPoint)
    }

    /// Write the raw cbind-input (`gs2-header || cbind-data`) for this choice
    /// into `out`, returning the number of bytes written. The binding data is
    /// appended only for [`Self::ServerEndPoint`] (`n`/`y` bind no data). The
    /// caller base64-encodes the result into the client-final `c=` value.
    ///
    /// Returns `None` if `out` is smaller than the cbind-input — structurally
    /// unreachable for an [`MAX_CBIND_INPUT_LEN`]-sized buffer, surfaced as a
    /// classified overflow at the call site rather than a panic.
    #[must_use]
    pub fn write_cbind_input(self, data: Option<&ChannelBindingData>, out: &mut [u8]) -> Option<usize> {
        let header = self.gs2_header();
        let cbind_data: &[u8] = if self.uses_binding() {
            data.map_or(&[], ChannelBindingData::as_slice)
        } else {
            &[]
        };
        let total = header.len().checked_add(cbind_data.len())?;
        let dst = out.get_mut(..total)?;
        let (header_slot, data_slot) = dst.split_at_mut(header.len());
        header_slot.copy_from_slice(header);
        data_slot.copy_from_slice(cbind_data);
        Some(total)
    }
}

/// The SCRAM mechanisms a server advertised in its `AuthenticationSASL` frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MechanismOffer {
    /// The server offered `SCRAM-SHA-256-PLUS` (channel binding).
    pub plus: bool,
    /// The server offered plain `SCRAM-SHA-256`.
    pub plain: bool,
}

impl MechanismOffer {
    /// Parse the NUL-separated SASL mechanism list from an `AuthenticationSASL`
    /// frame body. Recognises `SCRAM-SHA-256` and `SCRAM-SHA-256-PLUS`; every
    /// other advertised mechanism is ignored. Total on any bytes.
    #[must_use]
    pub fn parse(list: &[u8]) -> Self {
        let mut plus = false;
        let mut plain = false;
        for name in list.split(|&byte| byte == 0) {
            if name == SCRAM_SHA_256_PLUS_MECHANISM {
                plus = true;
            } else if name == SCRAM_SHA_256_MECHANISM {
                plain = true;
            }
        }
        Self { plus, plain }
    }
}

/// Decide the SASL mechanism + gs2 flag from the server's mechanism offer and
/// the resolved [`ChannelBinding`].
///
/// - [`ChannelBinding::Unbound`] → plain SCRAM-SHA-256 (`n,,`) if the server
///   offers it; else [`ScramError::NoSupportedMechanism`].
/// - [`ChannelBinding::Available`] → SCRAM-SHA-256-PLUS (`p=…`) if the server
///   offers `-PLUS`; else plain with `y,,` (anti-downgrade) when `require` is
///   false, or [`ScramError::ChannelBindingRequired`] when `require` is true;
///   or [`ScramError::NoSupportedMechanism`] if the server offers neither.
///
/// # Errors
///
/// [`ScramError::NoSupportedMechanism`] when the server offers no mechanism this
/// client can use, and [`ScramError::ChannelBindingRequired`] when
/// `channel_binding=require` is set but the server offered no `-PLUS` mechanism.
pub fn decide_sasl_choice(
    offer: MechanismOffer,
    binding: &ChannelBinding,
) -> Result<SaslChoice, ScramError> {
    match binding {
        ChannelBinding::Unbound => {
            if offer.plain {
                Ok(SaslChoice::NoBinding)
            } else {
                Err(ScramError::NoSupportedMechanism)
            }
        }
        ChannelBinding::Available { require, .. } => {
            if offer.plus {
                Ok(SaslChoice::ServerEndPoint)
            } else if offer.plain {
                if *require {
                    Err(ScramError::ChannelBindingRequired)
                } else {
                    Ok(SaslChoice::SupportedButUnused)
                }
            } else {
                Err(ScramError::NoSupportedMechanism)
            }
        }
    }
}

/// Which SHA-2 hash the `tls-server-end-point` binding uses for a given
/// certificate, per RFC 5929 §4.1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HashAlg {
    Sha256,
    Sha384,
    Sha512,
}

impl HashAlg {
    /// Select the hash for a certificate's `signatureAlgorithm` OID content
    /// bytes (RFC 5929 §4.1): SHA-384/512 for the ECDSA/RSA variants that name
    /// them; SHA-256 for everything else — the SHA-256 signature variants, the
    /// MD5/SHA-1 upgrade the RFC mandates, and any OID this map does not list.
    fn from_signature_oid(oid: &[u8]) -> Self {
        match oid {
            OID_SHA384_WITH_RSA | OID_ECDSA_WITH_SHA384 => Self::Sha384,
            OID_SHA512_WITH_RSA | OID_ECDSA_WITH_SHA512 => Self::Sha512,
            _ => Self::Sha256,
        }
    }

    /// Hash `data` with this algorithm into a [`ChannelBindingData`].
    fn hash(self, data: &[u8]) -> ChannelBindingData {
        use sha2::{Digest, Sha256, Sha384, Sha512};
        match self {
            Self::Sha256 => ChannelBindingData::from_digest(&Sha256::digest(data)),
            Self::Sha384 => ChannelBindingData::from_digest(&Sha384::digest(data)),
            Self::Sha512 => ChannelBindingData::from_digest(&Sha512::digest(data)),
        }
    }
}

// DER-encoded OBJECT IDENTIFIER *content* bytes (tag/length stripped) for the
// certificate `signatureAlgorithm`s whose hash is NOT SHA-256. Everything else
// (SHA-256 variants, MD5/SHA-1 → SHA-256, and unknown OIDs) folds to the
// SHA-256 default, so only the SHA-384 / SHA-512 OIDs need enumerating here.

/// `sha384WithRSAEncryption` — 1.2.840.113549.1.1.12.
const OID_SHA384_WITH_RSA: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0C];
/// `sha512WithRSAEncryption` — 1.2.840.113549.1.1.13.
const OID_SHA512_WITH_RSA: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0D];
/// `ecdsa-with-SHA384` — 1.2.840.10045.4.3.3.
const OID_ECDSA_WITH_SHA384: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x03];
/// `ecdsa-with-SHA512` — 1.2.840.10045.4.3.4.
const OID_ECDSA_WITH_SHA512: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x04];

/// DER `SEQUENCE` tag.
const DER_TAG_SEQUENCE: u8 = 0x30;
/// DER `OBJECT IDENTIFIER` tag.
const DER_TAG_OID: u8 = 0x06;

/// Compute the `tls-server-end-point` channel-binding data for a server
/// certificate (RFC 5929 §4.1): the hash of the DER-encoded end-entity
/// certificate, where the hash is chosen from the certificate's own
/// `signatureAlgorithm`.
///
/// # Hash selection and coverage
///
/// The signatureAlgorithm OID is read with a bounded, total ASN.1 walk. The
/// `ecdsa-with-SHA384` / `sha384WithRSAEncryption` OIDs select SHA-384, the
/// `-SHA512` variants select SHA-512, and **every other OID — including all the
/// SHA-256 variants (`ecdsa-with-SHA256`, `sha256WithRSAEncryption`), the
/// MD5/SHA-1 signature OIDs (upgraded to SHA-256 per RFC 5929 §4.1), and any
/// unrecognised or unreadable algorithm — selects SHA-256.** This exactly covers
/// the RSA + ECDSA certificates real PostgreSQL servers present (the
/// overwhelming majority use SHA-256, and SHA-384/512 are handled precisely). An
/// exotic signature algorithm this map does not name (e.g. RSASSA-PSS, whose
/// hash lives in the algorithm *parameters* rather than the OID, or Ed25519)
/// falls to the SHA-256 default; if the server used a different hash, the
/// binding data will not match and authentication *fails safely* (a loud SCRAM
/// signature mismatch), never a silent security downgrade. This mirrors the
/// safe-failure posture of PostgreSQL's own server-side binding, which errors
/// rather than guesses on an unknown digest.
///
/// Infallible: an unreadable/malformed certificate (structurally unreachable
/// after a verified TLS handshake) simply falls to the SHA-256 default.
#[must_use]
pub fn tls_server_end_point(cert_der: &[u8]) -> ChannelBindingData {
    let hash = match read_signature_algorithm_oid(cert_der) {
        Some(oid) => HashAlg::from_signature_oid(oid),
        None => HashAlg::Sha256,
    };
    hash.hash(cert_der)
}

/// Read one DER TLV at the front of `data`: returns `(tag, value, rest)` where
/// `value` is the content octets and `rest` is everything after them. Total —
/// any truncation, an indefinite length (`0x80`, illegal in DER), or an
/// over-wide length field yields `None`, never a panic.
fn read_der_tlv(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, after_tag) = data.split_first()?;
    let (&first_len, after_first_len) = after_tag.split_first()?;
    let (value_len, after_len) = if first_len < 0x80 {
        (usize::from(first_len), after_first_len)
    } else {
        let num_len_octets = usize::from(first_len & 0x7F);
        // `0x80` (indefinite form) is not permitted in DER; a length field wider
        // than a `usize`'s worth of octets cannot address a real certificate.
        if num_len_octets == 0 || num_len_octets > 4 {
            return None;
        }
        let len_octets = after_first_len.get(..num_len_octets)?;
        let mut value_len: usize = 0;
        for &octet in len_octets {
            value_len = value_len.checked_mul(256)?.checked_add(usize::from(octet))?;
        }
        (value_len, after_first_len.get(num_len_octets..)?)
    };
    let value = after_len.get(..value_len)?;
    let rest = after_len.get(value_len..)?;
    Some((tag, value, rest))
}

/// Read the `signatureAlgorithm` OID content bytes from a DER-encoded X.509
/// certificate. Total on any input.
///
/// ```text
/// Certificate ::= SEQUENCE {
///     tbsCertificate       SEQUENCE { … },          -- skipped
///     signatureAlgorithm   SEQUENCE { algorithm OID, parameters … },
///     signatureValue       BIT STRING }
/// ```
fn read_signature_algorithm_oid(cert_der: &[u8]) -> Option<&[u8]> {
    let (cert_tag, cert_body, _) = read_der_tlv(cert_der)?;
    if cert_tag != DER_TAG_SEQUENCE {
        return None;
    }
    // First element: tbsCertificate — advance past it without inspecting it.
    let (_tbs_tag, _tbs_body, after_tbs) = read_der_tlv(cert_body)?;
    // Second element: signatureAlgorithm (an AlgorithmIdentifier SEQUENCE).
    let (sig_alg_tag, sig_alg_body, _) = read_der_tlv(after_tbs)?;
    if sig_alg_tag != DER_TAG_SEQUENCE {
        return None;
    }
    // First element of the AlgorithmIdentifier: the algorithm OID.
    let (oid_tag, oid, _) = read_der_tlv(sig_alg_body)?;
    if oid_tag != DER_TAG_OID {
        return None;
    }
    Some(oid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;

    /// A helper: wrap `content` in a DER TLV with `tag` (short-form length only,
    /// content `< 128` bytes — sufficient for the OIDs under test).
    fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
        assert!(content.len() < 128, "test TLV uses short-form length");
        let len = u8::try_from(content.len()).unwrap_or(0);
        let mut out = vec![tag, len];
        out.extend_from_slice(content);
        out
    }

    /// Build a minimal DER certificate whose `signatureAlgorithm` OID is `oid`:
    /// `SEQUENCE { SEQUENCE {} , SEQUENCE { OID }, BIT STRING {} }`. Only the
    /// second element's OID is read by [`read_signature_algorithm_oid`].
    fn cert_with_sig_oid(oid: &[u8]) -> Vec<u8> {
        let tbs = tlv(DER_TAG_SEQUENCE, &[]);
        let sig_alg = tlv(DER_TAG_SEQUENCE, &tlv(DER_TAG_OID, oid));
        let sig_value = tlv(0x03, &[0x00]);
        let mut body = Vec::new();
        body.extend_from_slice(&tbs);
        body.extend_from_slice(&sig_alg);
        body.extend_from_slice(&sig_value);
        tlv(DER_TAG_SEQUENCE, &body)
    }

    #[test]
    fn reads_sha256_rsa_signature_oid() {
        // 1.2.840.113549.1.1.11 = sha256WithRSAEncryption.
        let oid = [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0B];
        let cert = cert_with_sig_oid(&oid);
        assert_eq!(read_signature_algorithm_oid(&cert), Some(oid.as_slice()));
        assert_eq!(HashAlg::from_signature_oid(&oid), HashAlg::Sha256);
    }

    #[test]
    fn selects_sha384_and_sha512_for_their_oids() {
        assert_eq!(HashAlg::from_signature_oid(OID_SHA384_WITH_RSA), HashAlg::Sha384);
        assert_eq!(HashAlg::from_signature_oid(OID_SHA512_WITH_RSA), HashAlg::Sha512);
        assert_eq!(HashAlg::from_signature_oid(OID_ECDSA_WITH_SHA384), HashAlg::Sha384);
        assert_eq!(HashAlg::from_signature_oid(OID_ECDSA_WITH_SHA512), HashAlg::Sha512);
    }

    #[test]
    fn md5_sha1_and_unknown_oids_upgrade_to_sha256() {
        // 1.2.840.113549.1.1.5 = sha1WithRSAEncryption → SHA-256 (RFC 5929 §4.1).
        let sha1_rsa = [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x05];
        assert_eq!(HashAlg::from_signature_oid(&sha1_rsa), HashAlg::Sha256);
        // 1.2.840.113549.1.1.4 = md5WithRSAEncryption → SHA-256.
        let md5_rsa = [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x04];
        assert_eq!(HashAlg::from_signature_oid(&md5_rsa), HashAlg::Sha256);
        // An OID this map does not list → SHA-256 default.
        assert_eq!(HashAlg::from_signature_oid(&[0x2A, 0x03]), HashAlg::Sha256);
    }

    #[test]
    fn hash_lengths_match_the_algorithm() {
        assert_eq!(HashAlg::Sha256.hash(b"cert").as_slice().len(), 32);
        assert_eq!(HashAlg::Sha384.hash(b"cert").as_slice().len(), 48);
        assert_eq!(HashAlg::Sha512.hash(b"cert").as_slice().len(), 64);
    }

    #[test]
    fn tls_server_end_point_hashes_the_whole_cert_der() {
        use sha2::{Digest, Sha256};
        // A cert with an unrecognised sig OID hashes with SHA-256 over the WHOLE
        // DER (RFC 5929 §4.1 hashes the certificate, not the tbsCertificate).
        let cert = cert_with_sig_oid(&[0x2A, 0x03]);
        let expected = Sha256::digest(&cert);
        assert_eq!(tls_server_end_point(&cert).as_slice(), expected.as_slice());
    }

    #[test]
    fn tls_server_end_point_uses_sha384_when_the_cert_names_it() {
        use sha2::{Digest, Sha384};
        let cert = cert_with_sig_oid(OID_ECDSA_WITH_SHA384);
        let expected = Sha384::digest(&cert);
        assert_eq!(tls_server_end_point(&cert).as_slice(), expected.as_slice());
    }

    #[test]
    fn malformed_cert_der_falls_to_sha256_never_panics() {
        use sha2::{Digest, Sha256};
        for bad in [
            [].as_slice(),
            &[0x30],
            &[0x30, 0x82],
            &[0x30, 0x80, 0x00, 0x00], // indefinite length — illegal in DER
            &[0x06, 0x03, 0x2A, 0x03], // an OID at top level, not a cert SEQUENCE
            &[0xFF; 8],
        ] {
            // Never panics, and unreadable DER falls to the SHA-256 default.
            let got = tls_server_end_point(bad);
            assert_eq!(got.as_slice(), Sha256::digest(bad).as_slice());
        }
    }

    #[test]
    fn mechanism_offer_parse_distinguishes_plus_from_plain() {
        assert_eq!(
            MechanismOffer::parse(b"SCRAM-SHA-256\0\0"),
            MechanismOffer { plus: false, plain: true },
        );
        assert_eq!(
            MechanismOffer::parse(b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0"),
            MechanismOffer { plus: true, plain: true },
        );
        assert_eq!(
            MechanismOffer::parse(b"SCRAM-SHA-256-PLUS\0\0"),
            MechanismOffer { plus: true, plain: false },
        );
        // An unrelated mechanism is ignored; empty list offers nothing.
        assert_eq!(
            MechanismOffer::parse(b"PLAIN\0\0"),
            MechanismOffer { plus: false, plain: false },
        );
        assert_eq!(
            MechanismOffer::parse(b""),
            MechanismOffer { plus: false, plain: false },
        );
    }

    fn available(require: bool) -> ChannelBinding {
        ChannelBinding::Available {
            data: tls_server_end_point(b"cert"),
            require,
        }
    }

    #[test]
    fn decide_prefers_plus_over_tls_when_offered() {
        let offer = MechanismOffer { plus: true, plain: true };
        assert_eq!(
            decide_sasl_choice(offer, &available(false)),
            Ok(SaslChoice::ServerEndPoint),
        );
        // require does not change the -PLUS selection.
        assert_eq!(
            decide_sasl_choice(offer, &available(true)),
            Ok(SaslChoice::ServerEndPoint),
        );
    }

    #[test]
    fn decide_falls_back_to_y_flag_over_tls_without_plus() {
        let offer = MechanismOffer { plus: false, plain: true };
        assert_eq!(
            decide_sasl_choice(offer, &available(false)),
            Ok(SaslChoice::SupportedButUnused),
        );
    }

    #[test]
    fn decide_require_refuses_when_plus_absent() {
        let offer = MechanismOffer { plus: false, plain: true };
        assert_eq!(
            decide_sasl_choice(offer, &available(true)),
            Err(ScramError::ChannelBindingRequired),
        );
    }

    #[test]
    fn decide_unbound_uses_plain_n_flag() {
        let offer = MechanismOffer { plus: true, plain: true };
        assert_eq!(
            decide_sasl_choice(offer, &ChannelBinding::Unbound),
            Ok(SaslChoice::NoBinding),
        );
    }

    #[test]
    fn decide_errors_when_no_usable_mechanism_offered() {
        let none = MechanismOffer { plus: false, plain: false };
        assert_eq!(
            decide_sasl_choice(none, &ChannelBinding::Unbound),
            Err(ScramError::NoSupportedMechanism),
        );
        // Unbound cannot use a -PLUS-only offer (it has no binding data).
        let plus_only = MechanismOffer { plus: true, plain: false };
        assert_eq!(
            decide_sasl_choice(plus_only, &ChannelBinding::Unbound),
            Err(ScramError::NoSupportedMechanism),
        );
        assert_eq!(
            decide_sasl_choice(none, &available(false)),
            Err(ScramError::NoSupportedMechanism),
        );
    }

    #[test]
    fn gs2_headers_and_mechanisms_match_the_choice() {
        assert_eq!(SaslChoice::NoBinding.gs2_header(), b"n,,");
        assert_eq!(SaslChoice::SupportedButUnused.gs2_header(), b"y,,");
        assert_eq!(SaslChoice::ServerEndPoint.gs2_header(), b"p=tls-server-end-point,,");
        assert_eq!(SaslChoice::NoBinding.mechanism(), SCRAM_SHA_256_MECHANISM);
        assert_eq!(SaslChoice::ServerEndPoint.mechanism(), SCRAM_SHA_256_PLUS_MECHANISM);
        assert!(SaslChoice::ServerEndPoint.uses_binding());
        assert!(!SaslChoice::NoBinding.uses_binding());
    }
}
