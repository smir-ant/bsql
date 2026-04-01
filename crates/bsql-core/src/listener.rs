//! LISTEN/NOTIFY support via a dedicated PostgreSQL connection.
//!
//! [`Listener`] opens a standalone connection (not from the pool) and
//! subscribes to named channels. Notifications arrive asynchronously and
//! are read via [`recv()`](Listener::recv).
//!
//! # Design
//!
//! The listener uses a dedicated connection because LISTEN requires a
//! persistent session — the subscription is tied to the backend process.
//! Pooled connections cycle between callers, so LISTEN on a pooled
//! connection would silently lose the subscription on return-to-pool.
//!
//! Internally, the `Connection` future is spawned on a tokio task that
//! polls `poll_message()` and forwards `AsyncMessage::Notification`
//! values over an mpsc channel. The `recv()` method reads from this
//! channel.

use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use crate::error::{BsqlError, BsqlResult, ConnectError};

/// A notification received from PostgreSQL via LISTEN/NOTIFY.
///
/// Zero-copy wrapper around `tokio_postgres::Notification` — avoids
/// allocating two `String`s per notification.
#[derive(Debug, Clone)]
pub struct Notification(tokio_postgres::Notification);

impl Notification {
    /// The channel name this notification was raised on.
    pub fn channel(&self) -> &str {
        self.0.channel()
    }

    /// The payload string attached to the notification (may be empty).
    pub fn payload(&self) -> &str {
        self.0.payload()
    }
}

/// A dedicated LISTEN/NOTIFY connection to PostgreSQL.
///
/// Created via [`Listener::connect`]. This is NOT a pooled connection —
/// it opens a fresh TCP connection that persists for the listener's lifetime.
///
/// # Example
///
/// ```rust,ignore
/// use bsql::Listener;
///
/// let mut listener = Listener::connect("postgres://user:pass@localhost/mydb").await?;
/// listener.listen("order_updates").await?;
///
/// loop {
///     let notif = listener.recv().await?;
///     println!("{}: {}", notif.channel(), notif.payload());
/// }
/// ```
pub struct Listener {
    client: tokio_postgres::Client,
    rx: mpsc::Receiver<tokio_postgres::Notification>,
    _conn_handle: tokio::task::JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self._conn_handle.abort();
    }
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener")
            .field("active", &!self._conn_handle.is_finished())
            .finish()
    }
}

impl Listener {
    /// Connect to PostgreSQL and start listening for notifications.
    ///
    /// Opens a dedicated connection (not from any pool). The connection
    /// is driven by a background tokio task.
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| ConnectError::create(format!("listener connect failed: {e}")))?;

        let (tx, rx) = mpsc::channel(10_000);

        let handle = tokio::spawn(async move {
            drive_connection(connection, tx).await;
        });

        Ok(Listener {
            client,
            rx,
            _conn_handle: handle,
        })
    }

    /// Subscribe to a named notification channel.
    ///
    /// The channel name is properly quoted as a PostgreSQL identifier to
    /// prevent SQL injection. Rejects empty names and names containing null bytes.
    pub async fn listen(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "LISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel)?;
        self.client
            .batch_execute(&format!("LISTEN {quoted}"))
            .await
            .map_err(BsqlError::from)
    }

    /// Unsubscribe from a named notification channel.
    ///
    /// The channel name is properly quoted as a PostgreSQL identifier.
    /// Rejects empty names and names containing null bytes.
    pub async fn unlisten(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "UNLISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel)?;
        self.client
            .batch_execute(&format!("UNLISTEN {quoted}"))
            .await
            .map_err(BsqlError::from)
    }

    /// Unsubscribe from all channels.
    pub async fn unlisten_all(&self) -> BsqlResult<()> {
        self.client
            .batch_execute("UNLISTEN *")
            .await
            .map_err(BsqlError::from)
    }

    /// Receive the next notification.
    ///
    /// Blocks until a notification arrives, or returns an error if the
    /// connection has been closed.
    pub async fn recv(&mut self) -> BsqlResult<Notification> {
        self.rx
            .recv()
            .await
            .map(Notification)
            .ok_or_else(|| ConnectError::create("listener connection closed"))
    }

    /// Send a NOTIFY on a channel with a payload.
    ///
    /// Convenience method — in production, NOTIFY is typically sent from
    /// a pooled connection or trigger, not the listener connection.
    /// Rejects empty channel names and null bytes in channel or payload.
    pub async fn notify(&self, channel: &str, payload: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "NOTIFY channel name must not be empty",
            ));
        }
        if payload.contains('\0') {
            return Err(ConnectError::create(
                "NOTIFY payload must not contain null bytes",
            ));
        }
        let quoted_channel = quote_ident(channel)?;
        let escaped_payload = payload.replace('\'', "''");
        self.client
            .batch_execute(&format!("NOTIFY {quoted_channel}, '{escaped_payload}'"))
            .await
            .map_err(BsqlError::from)
    }
}

/// Quote a PostgreSQL identifier: wrap in double quotes, double any internal quotes.
///
/// This is the standard PG identifier quoting rule. It prevents SQL injection
/// in LISTEN/UNLISTEN/NOTIFY commands, where the channel name is an identifier
/// (not a parameter — `$1` binding does not work with LISTEN).
///
/// Returns an error if `name` contains null bytes (PostgreSQL rejects them).
fn quote_ident(name: &str) -> BsqlResult<String> {
    if name.contains('\0') {
        return Err(ConnectError::create(
            "identifier must not contain null bytes",
        ));
    }
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for c in name.chars() {
        if c == '"' {
            quoted.push('"');
        }
        quoted.push(c);
    }
    quoted.push('"');
    Ok(quoted)
}

/// Drive the connection future, forwarding notifications to the channel.
///
/// Runs until the connection closes or encounters an unrecoverable error.
/// When the channel buffer is full, notifications are dropped with a warning.
/// When the receiver is dropped, the loop exits.
async fn drive_connection<S, T>(
    mut connection: tokio_postgres::Connection<S, T>,
    tx: mpsc::Sender<tokio_postgres::Notification>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Poll the connection message-by-message, forwarding notifications.
    // We cannot just `.await` the connection because that discards
    // notification messages (the default Future impl only logs notices).
    loop {
        let message =
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut connection).poll_message(cx)).await;

        match message {
            Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => match tx.try_send(n) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    eprintln!(
                        "bsql: listener notification dropped \
                             — channel buffer full (10000)"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            },
            Some(Ok(_)) => {
                // Notices and other async messages — ignore
            }
            Some(Err(e)) => {
                eprintln!("bsql: listener connection error: {e}");
                return;
            }
            None => {
                // Connection closed normally
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("my_channel").unwrap(), "\"my_channel\"");
    }

    #[test]
    fn quote_ident_with_double_quotes() {
        assert_eq!(quote_ident("my\"channel").unwrap(), "\"my\"\"channel\"");
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident("").unwrap(), "\"\"");
    }

    #[test]
    fn quote_ident_with_spaces() {
        assert_eq!(quote_ident("my channel").unwrap(), "\"my channel\"");
    }

    #[test]
    fn quote_ident_with_semicolon() {
        // SQL injection attempt: semicolons are harmless inside quoted identifier
        assert_eq!(
            quote_ident("foo; DROP TABLE users").unwrap(),
            "\"foo; DROP TABLE users\""
        );
    }

    #[test]
    fn quote_ident_multiple_quotes() {
        assert_eq!(quote_ident("a\"b\"c").unwrap(), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn quote_ident_rejects_null_bytes() {
        let result = quote_ident("chan\0nel");
        assert!(result.is_err());
    }

    // Notification accessor and clone tests are exercised by the integration
    // tests (listener.rs) via a real PostgreSQL LISTEN/NOTIFY round-trip.
    // Unit-level construction is not possible because tokio_postgres::Notification
    // has private fields with no public constructor.
}
