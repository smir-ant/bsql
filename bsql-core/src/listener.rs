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

use crate::error::{ConnectError, BsqlError, BsqlResult};

/// A notification received from PostgreSQL via LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct Notification {
    channel: String,
    payload: String,
}

impl Notification {
    /// The channel name this notification was raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The payload string attached to the notification (may be empty).
    pub fn payload(&self) -> &str {
        &self.payload
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
    rx: mpsc::UnboundedReceiver<tokio_postgres::Notification>,
    _conn_handle: tokio::task::JoinHandle<()>,
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

        let (tx, rx) = mpsc::unbounded_channel();

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
    /// prevent SQL injection.
    pub async fn listen(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "LISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel);
        self.client
            .batch_execute(&format!("LISTEN {quoted}"))
            .await
            .map_err(BsqlError::from)
    }

    /// Unsubscribe from a named notification channel.
    ///
    /// The channel name is properly quoted as a PostgreSQL identifier.
    pub async fn unlisten(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "UNLISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel);
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
            .map(|n| Notification {
                channel: n.channel().to_owned(),
                payload: n.payload().to_owned(),
            })
            .ok_or_else(|| ConnectError::create("listener connection closed"))
    }

    /// Send a NOTIFY on a channel with a payload.
    ///
    /// Convenience method — in production, NOTIFY is typically sent from
    /// a pooled connection or trigger, not the listener connection.
    pub async fn notify(&self, channel: &str, payload: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "NOTIFY channel name must not be empty",
            ));
        }
        let quoted_channel = quote_ident(channel);
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
fn quote_ident(name: &str) -> String {
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for c in name.chars() {
        if c == '"' {
            quoted.push('"');
        }
        quoted.push(c);
    }
    quoted.push('"');
    quoted
}

/// Drive the connection future, forwarding notifications to the channel.
///
/// Runs until the connection closes or encounters an unrecoverable error.
/// When the mpsc sender is dropped (receiver side dropped), send errors
/// are silently ignored — the listener is shutting down.
async fn drive_connection<S, T>(
    mut connection: tokio_postgres::Connection<S, T>,
    tx: mpsc::UnboundedSender<tokio_postgres::Notification>,
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
            Some(Ok(tokio_postgres::AsyncMessage::Notification(n))) => {
                // If the receiver is dropped, stop driving the connection
                if tx.send(n).is_err() {
                    return;
                }
            }
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
        assert_eq!(quote_ident("my_channel"), "\"my_channel\"");
    }

    #[test]
    fn quote_ident_with_double_quotes() {
        assert_eq!(quote_ident("my\"channel"), "\"my\"\"channel\"");
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn quote_ident_with_spaces() {
        assert_eq!(quote_ident("my channel"), "\"my channel\"");
    }

    #[test]
    fn quote_ident_with_semicolon() {
        // SQL injection attempt: semicolons are harmless inside quoted identifier
        assert_eq!(
            quote_ident("foo; DROP TABLE users"),
            "\"foo; DROP TABLE users\""
        );
    }

    #[test]
    fn quote_ident_multiple_quotes() {
        assert_eq!(quote_ident("a\"b\"c"), "\"a\"\"b\"\"c\"");
    }

    #[test]
    fn notification_accessors() {
        let n = Notification {
            channel: "test".into(),
            payload: "hello".into(),
        };
        assert_eq!(n.channel(), "test");
        assert_eq!(n.payload(), "hello");
    }

    #[test]
    fn notification_clone() {
        let n = Notification {
            channel: "ch".into(),
            payload: "data".into(),
        };
        let cloned = n.clone();
        assert_eq!(cloned.channel(), "ch");
        assert_eq!(cloned.payload(), "data");
    }
}
