//! Validated startup-parameter (GUC) pairs for the `StartupMessage`.
//!
//! A consumer may set PostgreSQL session parameters at connect time —
//! `search_path`, `application_name`, `statement_timeout`, or any GUC — by
//! naming a `(name, value)` pair. Each pair is funnelled through
//! [`StartupParam::new`] before it can reach the wire assembler, which enforces
//! three invariants that make a corrupt or subverted `StartupMessage`
//! structurally impossible:
//!
//! 1. **No NUL** in the name or value. The wire encodes each parameter as a
//!    NUL-terminated `name\0value\0` pair; a NUL inside either field would
//!    terminate it early and desynchronise the whole packet. The bounded
//!    [`GucName`] / [`GucValue`] constructors reject NUL, so a `StartupParam`
//!    can never hold one — the assembler's [`push_nul_terminated`] input is
//!    NUL-free by construction, not by trust.
//! 2. **Bounded length.** Names cap at [`MAX_GUC_NAME_LEN`], values at
//!    [`MAX_GUC_VALUE_LEN`]; over-length input is a loud classified rejection,
//!    never a silent truncation.
//! 3. **No reserved name.** The parameters that establish session identity or
//!    are pinned by the connection ([`RESERVED_NAMES`]) cannot be set through
//!    this path. A consumer cannot displace `user` / `database` (auth identity,
//!    set from the connection config), re-encode the session away from the
//!    pinned `client_encoding=UTF8` (which the text decoders rely on), switch
//!    the connection into `replication` protocol mode, or smuggle any of those
//!    through the catch-all `options` meta-parameter.
//!
//! Because the reserved check lives here — at the wire authority — the
//! `client_encoding=UTF8` pin the assembler emits is protected structurally: a
//! `StartupParam` that names `client_encoding` cannot be constructed, so it can
//! never reach [`crate::engine`]'s startup builder to override the pin.
//!
//! [`GucName`]: crate::ident::GucName
//! [`GucValue`]: crate::ident::GucValue
//! [`MAX_GUC_NAME_LEN`]: crate::ident::MAX_GUC_NAME_LEN
//! [`MAX_GUC_VALUE_LEN`]: crate::ident::MAX_GUC_VALUE_LEN
//! [`push_nul_terminated`]: crate::write_buf::WriteBuf::push_nul_terminated

use core::fmt;

use crate::ident::{GucName, GucValue, IdentError};

/// Startup-parameter names a consumer may **not** set through a
/// [`StartupParam`] — they are managed by the connection.
///
/// Matched case-insensitively (PostgreSQL folds GUC names to lower case).
///
/// - `user`, `database` — session identity; set from the connection config so
///   the wire value always matches the credentials the handshake authenticates.
/// - `client_encoding` — pinned to `UTF8` by the startup assembler; the text
///   decoders assume UTF-8, so an override (`LATIN1`, `SQL_ASCII`, …) would
///   silently corrupt every decoded string.
/// - `replication` — switches the connection into the streaming-replication
///   sub-protocol, which this driver does not speak.
/// - `options` — a meta-parameter that carries `-c name=value` command-line
///   settings; it can smuggle any of the above (e.g.
///   `options=-c client_encoding=LATIN1`), so it is rejected wholesale. Set
///   individual parameters directly instead.
pub const RESERVED_NAMES: [&str; 5] =
    ["user", "database", "client_encoding", "replication", "options"];

/// Why a `(name, value)` pair was rejected as a startup parameter.
///
/// # `#[non_exhaustive]`
///
/// New rejection classes may land as the startup-parameter surface grows;
/// sealing forces downstream `match` callers to keep a catch-all arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartupParamError {
    /// The parameter NAME failed validation (empty, NUL, or over-length).
    Name(IdentError),
    /// The parameter VALUE failed validation (NUL or over-length).
    Value(IdentError),
    /// The name is one of [`RESERVED_NAMES`] — managed by the connection and
    /// not settable through a startup parameter.
    Reserved,
}

impl core::error::Error for StartupParamError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Name(e) | Self::Value(e) => Some(e),
            Self::Reserved => None,
        }
    }
}

impl fmt::Display for StartupParamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(e) => write!(f, "startup parameter name invalid: {e}"),
            Self::Value(e) => write!(f, "startup parameter value invalid: {e}"),
            Self::Reserved => f.write_str(
                "startup parameter name is reserved and managed by the connection \
                 (user, database, client_encoding, replication, options)",
            ),
        }
    }
}

/// A validated startup-parameter (GUC) pair, ready to append to a
/// `StartupMessage`.
///
/// Constructed only via [`StartupParam::new`], which enforces the no-NUL,
/// bounded-length, and non-reserved invariants documented at the
/// [module level](self). A constructed value is therefore safe to hand to the
/// wire assembler with no further checks — it cannot corrupt the packet or
/// override a reserved parameter.
///
/// `Copy` (both fields are POD bounded strings), so a `&[StartupParam]` slice
/// threads through the engine with no allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupParam {
    name: GucName,
    value: GucValue,
}

// Size anchor: GucValue (260, align 2) + GucName (65) = 326 (padded to the
// 2-byte alignment GucValue's BoundedU16 length forces). Building a startup
// param is a once-per-connection cold path, so this is a hygiene pin (a
// silently-added field lands on the review surface), not a hot-path budget.
crate::wire_pin!(StartupParam, size = 326, align = 2);

impl StartupParam {
    /// Validate a `(name, value)` pair into a `StartupParam`.
    ///
    /// # Errors
    ///
    /// - [`StartupParamError::Name`] — the name is empty, contains a NUL, or
    ///   exceeds [`MAX_GUC_NAME_LEN`](crate::ident::MAX_GUC_NAME_LEN).
    /// - [`StartupParamError::Value`] — the value contains a NUL or exceeds
    ///   [`MAX_GUC_VALUE_LEN`](crate::ident::MAX_GUC_VALUE_LEN).
    /// - [`StartupParamError::Reserved`] — the name is one of
    ///   [`RESERVED_NAMES`] (case-insensitive).
    pub fn new(name: &str, value: &str) -> Result<Self, StartupParamError> {
        let name = GucName::try_from_str(name).map_err(StartupParamError::Name)?;
        if is_reserved(name.as_str()) {
            return Err(StartupParamError::Reserved);
        }
        let value = GucValue::try_from_str(value).map_err(StartupParamError::Value)?;
        Ok(Self { name, value })
    }

    /// The validated NUL-free parameter-name bytes (no terminator).
    #[inline]
    #[must_use]
    pub fn name_bytes(&self) -> &[u8] {
        self.name.as_bytes()
    }

    /// The validated NUL-free parameter-value bytes (no terminator).
    #[inline]
    #[must_use]
    pub fn value_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }
}

/// Whether `name` is a [reserved](RESERVED_NAMES) startup-parameter name,
/// compared case-insensitively (PostgreSQL folds GUC names to lower case).
#[inline]
#[must_use]
fn is_reserved(name: &str) -> bool {
    RESERVED_NAMES
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString as _;

    #[test]
    fn accepts_a_plain_parameter() {
        let p = match StartupParam::new("search_path", "myschema, public") {
            Ok(p) => p,
            Err(e) => panic!("valid startup param must construct: {e}"),
        };
        assert_eq!(p.name_bytes(), b"search_path");
        assert_eq!(p.value_bytes(), b"myschema, public");
    }

    #[test]
    fn accepts_an_empty_value() {
        // A GUC value may be empty (e.g. `application_name=''`).
        assert!(StartupParam::new("application_name", "").is_ok());
    }

    #[test]
    fn rejects_empty_name() {
        assert_eq!(
            StartupParam::new("", "x"),
            Err(StartupParamError::Name(IdentError::Empty)),
        );
    }

    #[test]
    fn rejects_nul_in_name() {
        assert_eq!(
            StartupParam::new("bad\0name", "x"),
            Err(StartupParamError::Name(IdentError::ContainsNul)),
        );
    }

    #[test]
    fn rejects_nul_in_value() {
        assert_eq!(
            StartupParam::new("search_path", "a\0b"),
            Err(StartupParamError::Value(IdentError::ContainsNul)),
        );
    }

    #[test]
    fn rejects_over_length_value() {
        let big = "x".repeat(crate::ident::MAX_GUC_VALUE_LEN + 1);
        assert!(matches!(
            StartupParam::new("search_path", &big),
            Err(StartupParamError::Value(IdentError::TooLong { .. })),
        ));
    }

    #[test]
    fn rejects_each_reserved_name() {
        for reserved in RESERVED_NAMES {
            assert_eq!(
                StartupParam::new(reserved, "x"),
                Err(StartupParamError::Reserved),
                "'{reserved}' must be rejected as reserved",
            );
        }
    }

    #[test]
    fn reserved_check_is_case_insensitive() {
        // PostgreSQL folds GUC names, so a mixed-case reserved name must not
        // slip past the guard and override the pinned client_encoding.
        assert_eq!(
            StartupParam::new("Client_Encoding", "LATIN1"),
            Err(StartupParamError::Reserved),
        );
        assert_eq!(
            StartupParam::new("USER", "postgres"),
            Err(StartupParamError::Reserved),
        );
    }

    #[test]
    fn reserved_error_message_names_the_managed_set() {
        let msg = StartupParamError::Reserved.to_string();
        assert_eq!(
            msg,
            "startup parameter name is reserved and managed by the connection \
             (user, database, client_encoding, replication, options)",
        );
    }

    #[test]
    fn nul_in_value_error_message_is_specific() {
        let err = match StartupParam::new("search_path", "a\0b") {
            Err(e) => e,
            Ok(_) => panic!("a NUL in the value must be rejected"),
        };
        assert_eq!(
            err.to_string(),
            "startup parameter value invalid: identifier must not contain NUL bytes",
        );
    }
}
