//! Fixed-struct session parameters from `ParameterStatus` messages.
//!
//! [`SessionParams`] stores the known-useful parameters PostgreSQL sends
//! during the post-authentication handshake. Each field is an
//! `Option<heapless::String<N>>`: `None` until the server sends a
//! matching `ParameterStatus`.
//!
//! Unknown keys are parsed and dropped — there is no growable map, so
//! the "overflow" error class does not exist. DEF-042: tier-1 by
//! absence of a growable container.
//!
//! # Capacity
//!
//! Each value is bounded at 128 bytes, which accommodates every known
//! PG parameter value (`TimeZone` can be long: `America/Argentina/Buenos_Aires`
//! is 33 bytes; 128 is generous). An over-length value from the server
//! is silently dropped (the key is known but the value does not fit) —
//! the parameter is treated as if the server never sent it.

use core::fmt;

/// Maximum byte length for a single session parameter value.
const MAX_PARAM_VALUE_LEN: usize = 128;

/// Bounded string type for parameter values.
type ParamValue = heapless::String<MAX_PARAM_VALUE_LEN>;

/// Known session parameters received from the PostgreSQL server.
///
/// Populated during the post-authentication handshake from
/// `ParameterStatus` messages. Read-only after handshake completes.
/// Accessible via [`crate::PgProtocol::session_params`].
///
/// Per DEF-042: fixed struct, no map, no overflow class.
#[derive(Default)]
pub struct SessionParams {
    /// PostgreSQL server version string (e.g. `"17.2"`).
    pub server_version: Option<ParamValue>,
    /// Server-side encoding (e.g. `"UTF8"`).
    pub server_encoding: Option<ParamValue>,
    /// Client-side encoding (e.g. `"UTF8"`).
    pub client_encoding: Option<ParamValue>,
    /// Application name echoed back by the server.
    pub application_name: Option<ParamValue>,
    /// Whether the connected role is a superuser (`"on"` / `"off"`).
    pub is_superuser: Option<ParamValue>,
    /// The authorised session user.
    pub session_authorization: Option<ParamValue>,
    /// DateStyle setting (e.g. `"ISO, MDY"`).
    pub date_style: Option<ParamValue>,
    /// Whether integer datetimes are used (`"on"` / `"off"`).
    pub integer_datetimes: Option<ParamValue>,
    /// Server timezone (e.g. `"UTC"`, `"America/New_York"`).
    pub time_zone: Option<ParamValue>,
}

impl SessionParams {
    /// Create empty params — all fields `None`.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            server_version: None,
            server_encoding: None,
            client_encoding: None,
            application_name: None,
            is_superuser: None,
            session_authorization: None,
            date_style: None,
            integer_datetimes: None,
            time_zone: None,
        }
    }

    /// Record a parameter from the server.
    ///
    /// If `key` matches a known parameter, the value is stored (replacing
    /// any previous value). If the value exceeds the capacity bound, the
    /// parameter is silently dropped. Unknown keys are ignored.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        // Convert value to str for heapless::String. If the server
        // sends non-UTF8 values (should not happen for these params),
        // we silently skip.
        let value_str = match core::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => return,
        };
        let field = match key {
            b"server_version" => &mut self.server_version,
            b"server_encoding" => &mut self.server_encoding,
            b"client_encoding" => &mut self.client_encoding,
            b"application_name" => &mut self.application_name,
            b"is_superuser" => &mut self.is_superuser,
            b"session_authorization" => &mut self.session_authorization,
            b"DateStyle" => &mut self.date_style,
            b"integer_datetimes" => &mut self.integer_datetimes,
            b"TimeZone" => &mut self.time_zone,
            _ => return, // Unknown key — silently dropped.
        };
        // heapless::String::try_from returns Err if over capacity.
        // On Err we drop the value — no silent truncation.
        if let Ok(s) = ParamValue::try_from(value_str) {
            *field = Some(s);
        }
    }
}

impl fmt::Debug for SessionParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionParams")
            .field("server_version", &self.server_version)
            .field("server_encoding", &self.server_encoding)
            .field("client_encoding", &self.client_encoding)
            .field("application_name", &self.application_name)
            .field("is_superuser", &self.is_superuser)
            .field("session_authorization", &self.session_authorization)
            .field("date_style", &self.date_style)
            .field("integer_datetimes", &self.integer_datetimes)
            .field("time_zone", &self.time_zone)
            .finish()
    }
}
