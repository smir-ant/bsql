//! LISTEN/NOTIFY support via a dedicated PostgreSQL connection.
//!
//! [`Listener`] opens a standalone connection (not from the pool) and
//! subscribes to named channels. Notifications arrive asynchronously and
//! are read via [`recv()`](Listener::recv).
//!
//! # Design
//!
//! The listener uses a dedicated connection because LISTEN requires a
//! persistent session -- the subscription is tied to the backend process.
//! Pooled connections cycle between callers, so LISTEN on a pooled
//! connection would silently lose the subscription on return-to-pool.
//!
//! A background task owns the `Connection` exclusively. It uses
//! `tokio::select!` to multiplex between:
//!
//! 1. **Commands** from the caller (listen/unlisten/notify) delivered via
//!    a bounded mpsc channel (capacity 64 — commands are rare).
//! 2. **Notifications** from PostgreSQL read via `Connection::wait_for_notification()`,
//!    forwarded to the caller through a bounded mpsc channel.
//!
//! # Reconnection
//!
//! When the background task detects a connection loss, it attempts to
//! reconnect with exponential backoff (100ms, 200ms, 400ms, ... up to 5s).
//! On successful reconnect, all previously subscribed channels are
//! re-subscribed automatically. A special notification with channel
//! `"_bsql_reconnected"` is sent so the application knows some notifications
//! may have been lost during the outage.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::error::{BsqlError, BsqlResult, ConnectError};

/// Buffer capacity for the notification channel.
const NOTIFICATION_BUFFER_SIZE: usize = 1024;

/// Buffer capacity for the command channel. Commands (listen/unlisten/notify)
/// are rare relative to notifications, so 64 slots is ample.
const COMMAND_BUFFER_SIZE: usize = 64;

/// PostgreSQL's maximum NOTIFY payload size in bytes.
const PG_MAX_PAYLOAD_SIZE: usize = 7999;

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

/// Commands sent from the `Listener` API to the background task.
enum Command {
    Listen(
        String,
        tokio::sync::oneshot::Sender<Result<(), bsql_driver_postgres::DriverError>>,
    ),
    Unlisten(
        String,
        tokio::sync::oneshot::Sender<Result<(), bsql_driver_postgres::DriverError>>,
    ),
    UnlistenAll(tokio::sync::oneshot::Sender<Result<(), bsql_driver_postgres::DriverError>>),
}

/// A dedicated LISTEN/NOTIFY connection to PostgreSQL.
///
/// Created via [`Listener::connect`]. This is NOT a pooled connection --
/// it opens a fresh TCP connection that persists for the listener's lifetime.
///
/// # Reconnection
///
/// On connection loss, the listener automatically reconnects with exponential
/// backoff and re-subscribes to all channels. A notification on channel
/// `"_bsql_reconnected"` is emitted after successful reconnection.
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
    cmd_tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Notification>,
    _task_handle: tokio::task::JoinHandle<()>,
    /// Tracked subscribed channels for reconnection.
    channels: Arc<Mutex<HashSet<String>>>,
    /// Connection config for creating notify connections.
    config: bsql_driver_postgres::Config,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self._task_handle.abort();
    }
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let channel_count = self
            .channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        f.debug_struct("Listener")
            .field("active", &!self._task_handle.is_finished())
            .field("channels", &channel_count)
            .finish()
    }
}

impl Listener {
    /// Connect to PostgreSQL and start the background notification reader.
    ///
    /// Opens a dedicated connection (not from any pool).
    pub async fn connect(url: &str) -> BsqlResult<Self> {
        let config = bsql_driver_postgres::Config::from_url(url)
            .map_err(|e| ConnectError::create(format!("listener connect failed: {e}")))?;

        // Disable statement_timeout on the listener connection -- it only runs
        // LISTEN/UNLISTEN/NOTIFY, and the notification wait is unbounded by design.
        let mut listener_config = config;
        listener_config.statement_timeout_secs = 0;

        let conn = bsql_driver_postgres::Connection::connect(&listener_config)
            .await
            .map_err(|e| ConnectError::create(format!("listener connect failed: {e}")))?;

        let (notif_tx, rx) = mpsc::channel(NOTIFICATION_BUFFER_SIZE);
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_BUFFER_SIZE);
        let channels: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let config_for_notify = listener_config.clone();

        let handle = tokio::spawn(drive_listener(
            conn,
            listener_config,
            cmd_rx,
            notif_tx,
            Arc::clone(&channels),
        ));

        Ok(Listener {
            cmd_tx,
            rx,
            _task_handle: handle,
            channels,
            config: config_for_notify,
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
        let quoted = quote_ident(channel)?;
        let sql = format!("LISTEN {quoted}");
        self.send_command_listen(channel.to_owned(), sql).await
    }

    /// Unsubscribe from a named notification channel.
    pub async fn unlisten(&self, channel: &str) -> BsqlResult<()> {
        if channel.is_empty() {
            return Err(ConnectError::create(
                "UNLISTEN channel name must not be empty",
            ));
        }
        let quoted = quote_ident(channel)?;
        let sql = format!("UNLISTEN {quoted}");
        self.send_command_unlisten(channel.to_owned(), sql).await
    }

    /// Unsubscribe from all channels.
    pub async fn unlisten_all(&self) -> BsqlResult<()> {
        // Clear the tracked set
        self.channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::UnlistenAll(resp_tx))
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?;
        resp_rx
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?
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
            .ok_or_else(|| ConnectError::create("listener connection closed"))
    }

    /// Non-blocking receive. Returns `Ok(None)` if no notification is
    /// available right now (as opposed to `recv()` which awaits).
    ///
    /// Returns `Err` only if the listener connection has been closed.
    pub fn try_recv(&mut self) -> BsqlResult<Option<Notification>> {
        match self.rx.try_recv() {
            Ok(notif) => Ok(Some(notif)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                Err(ConnectError::create("listener connection closed"))
            }
        }
    }

    /// Send a NOTIFY on a channel with a payload.
    ///
    /// The payload must not exceed 7999 bytes (PostgreSQL's limit).
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
        if payload.len() > PG_MAX_PAYLOAD_SIZE {
            return Err(ConnectError::create(format!(
                "NOTIFY payload exceeds PostgreSQL's {PG_MAX_PAYLOAD_SIZE}-byte limit \
                 (got {} bytes)",
                payload.len()
            )));
        }
        let quoted_channel = quote_ident(channel)?;
        let escaped_payload = payload.replace('\'', "''");
        let sql = format!("NOTIFY {quoted_channel}, '{escaped_payload}'");

        // Send NOTIFY on a SEPARATE short-lived connection to avoid
        // self-notification race on the listener connection. When NOTIFY
        // is sent on the same connection that LISTENs, the notification
        // arrives during simple_query's response read, creating duplicates.
        let mut conn = bsql_driver_postgres::Connection::connect(&self.config)
            .await
            .map_err(|e| ConnectError::create(format!("notify connection failed: {e}")))?;
        conn.simple_query(&sql)
            .await
            .map_err(BsqlError::from_driver_query)?;
        Ok(())
    }

    /// The set of currently subscribed channels.
    pub fn subscribed_channels(&self) -> Vec<String> {
        self.channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Send a LISTEN command to the background task and wait for the response.
    async fn send_command_listen(&self, channel: String, sql: String) -> BsqlResult<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::Listen(sql, resp_tx))
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?;
        let result = resp_rx
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?
            .map_err(BsqlError::from);

        if result.is_ok() {
            self.channels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(channel);
        }
        result
    }

    /// Send an UNLISTEN command to the background task and wait for the response.
    async fn send_command_unlisten(&self, channel: String, sql: String) -> BsqlResult<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(Command::Unlisten(sql, resp_tx))
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?;
        let result = resp_rx
            .await
            .map_err(|_| ConnectError::create("listener background task exited"))?
            .map_err(BsqlError::from);

        if result.is_ok() {
            self.channels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&channel);
        }
        result
    }
}

/// Quote a PostgreSQL identifier: wrap in double quotes, double any internal quotes.
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

/// Background task that owns the Connection and multiplexes between
/// reading commands from the API and waiting for PostgreSQL notifications.
///
/// On connection loss, attempts reconnection with exponential backoff and
/// re-subscribes to all tracked channels.
async fn drive_listener(
    mut conn: bsql_driver_postgres::Connection,
    config: bsql_driver_postgres::Config,
    mut cmd_rx: mpsc::Receiver<Command>,
    notif_tx: mpsc::Sender<Notification>,
    channels: Arc<Mutex<HashSet<String>>>,
) {
    /// Counter for dropped notifications (buffer full).
    /// Intentionally process-wide: gives a single global view of notification
    /// back-pressure across all Listener instances in the process.
    static DROPPED_NOTIFICATIONS: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    loop {
        tokio::select! {
            // Branch 1: Commands from the Listener API
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::Listen(sql, resp)) => {
                        let result = conn.simple_query(&sql).await;
                        drain_pending(&mut conn, &notif_tx);
                        let _ = resp.send(result);
                    }
                    Some(Command::Unlisten(sql, resp)) => {
                        let result = conn.simple_query(&sql).await;
                        drain_pending(&mut conn, &notif_tx);
                        let _ = resp.send(result);
                    }
                    Some(Command::UnlistenAll(resp)) => {
                        let result = conn.simple_query("UNLISTEN *").await;
                        drain_pending(&mut conn, &notif_tx);
                        let _ = resp.send(result);
                    }
                    None => break, // Listener dropped, all senders gone
                }
            }
            // Branch 2: Notifications from PostgreSQL
            notif = conn.wait_for_notification() => {
                match notif {
                    Ok((channel, payload)) => {
                        if notif_tx.try_send(Notification { channel: channel.clone(), payload }).is_err() {
                            let count = DROPPED_NOTIFICATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                            eprintln!(
                                "bsql: notification buffer full, dropped notification on channel \
                                 \"{channel}\" (total dropped: {count})"
                            );
                        }
                    }
                    Err(_) => {
                        // Connection lost — attempt reconnection
                        eprintln!("bsql: listener connection lost, attempting reconnect...");
                        match reconnect_with_backoff(&config, &channels).await {
                            Some(new_conn) => {
                                conn = new_conn;
                                // Notify application that reconnection occurred
                                let _ = notif_tx.try_send(Notification {
                                    channel: "_bsql_reconnected".to_owned(),
                                    payload: "connection was lost and re-established; \
                                              some notifications may have been missed"
                                        .to_owned(),
                                });
                                eprintln!("bsql: listener reconnected successfully");
                            }
                            None => {
                                eprintln!("bsql: listener reconnection failed, task exiting");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Drain any notifications that were buffered during `simple_query()` and
/// forward them to the notification channel. This is necessary because
/// `read_one_message` auto-buffers NotificationResponse messages in
/// `pending_notifications` instead of returning them — so after any command
/// that calls `simple_query`, self-notifications would be silently lost.
fn drain_pending(
    conn: &mut bsql_driver_postgres::Connection,
    notif_tx: &mpsc::Sender<Notification>,
) {
    for notif in conn.drain_notifications() {
        let _ = notif_tx.try_send(Notification {
            channel: notif.channel,
            payload: notif.payload,
        });
    }
}

/// Attempt to reconnect with exponential backoff: 100ms, 200ms, 400ms, ... up to 5s.
/// Gives up after 10 consecutive failures.
async fn reconnect_with_backoff(
    config: &bsql_driver_postgres::Config,
    channels: &Arc<Mutex<HashSet<String>>>,
) -> Option<bsql_driver_postgres::Connection> {
    let mut delay = std::time::Duration::from_millis(100);
    let max_delay = std::time::Duration::from_secs(5);
    let max_attempts = 10;

    for attempt in 1..=max_attempts {
        tokio::time::sleep(delay).await;

        match bsql_driver_postgres::Connection::connect(config).await {
            Ok(mut new_conn) => {
                // Re-subscribe to all tracked channels
                let channel_set = channels.lock().unwrap_or_else(|e| e.into_inner()).clone();

                for channel in &channel_set {
                    match quote_ident(channel) {
                        Ok(quoted) => {
                            let sql = format!("LISTEN {quoted}");
                            if let Err(e) = new_conn.simple_query(&sql).await {
                                eprintln!(
                                    "bsql: failed to re-subscribe to channel \"{channel}\" \
                                     after reconnect: {e}"
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "bsql: failed to quote channel \"{channel}\" for \
                                 re-subscribe: {e}"
                            );
                        }
                    }
                }

                return Some(new_conn);
            }
            Err(e) => {
                eprintln!("bsql: listener reconnect attempt {attempt}/{max_attempts} failed: {e}");
                delay = std::cmp::min(delay * 2, max_delay);
            }
        }
    }

    None
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

    #[test]
    fn pg_max_payload_constant() {
        assert_eq!(PG_MAX_PAYLOAD_SIZE, 7999);
    }

    // --- Audit gap tests ---

    // #94: Notification payload > 7999 validation
    // (This is validated in Listener::notify, which requires async + connection.
    //  We test the constant and the quote_ident utility instead.)

    // #95: quote_ident with various special characters
    #[test]
    fn quote_ident_with_backslash() {
        assert_eq!(quote_ident("chan\\nel").unwrap(), "\"chan\\nel\"");
    }

    #[test]
    fn quote_ident_with_single_quote() {
        assert_eq!(quote_ident("it's").unwrap(), "\"it's\"");
    }

    #[test]
    fn quote_ident_with_unicode() {
        assert_eq!(
            quote_ident("channel_\u{00e9}").unwrap(),
            "\"channel_\u{00e9}\""
        );
    }

    #[test]
    fn quote_ident_with_newline() {
        assert_eq!(quote_ident("chan\nnel").unwrap(), "\"chan\nnel\"");
    }

    #[test]
    fn quote_ident_with_tab() {
        assert_eq!(quote_ident("chan\tnel").unwrap(), "\"chan\tnel\"");
    }

    #[test]
    fn quote_ident_with_mixed_special() {
        let result = quote_ident("a\"b'c;d").unwrap();
        // Double quotes are doubled
        assert_eq!(result, "\"a\"\"b'c;d\"");
    }

    // --- Notification accessors ---

    #[test]
    fn notification_channel_and_payload() {
        let notif = Notification {
            channel: "orders".to_owned(),
            payload: "{\"id\": 42}".to_owned(),
        };
        assert_eq!(notif.channel(), "orders");
        assert_eq!(notif.payload(), "{\"id\": 42}");
    }

    #[test]
    fn notification_empty_payload() {
        let notif = Notification {
            channel: "pings".to_owned(),
            payload: String::new(),
        };
        assert_eq!(notif.channel(), "pings");
        assert_eq!(notif.payload(), "");
    }

    #[test]
    fn notification_clone() {
        let notif = Notification {
            channel: "ch".to_owned(),
            payload: "data".to_owned(),
        };
        let cloned = notif.clone();
        assert_eq!(cloned.channel(), notif.channel());
        assert_eq!(cloned.payload(), notif.payload());
    }

    #[test]
    fn notification_debug() {
        let notif = Notification {
            channel: "ch".to_owned(),
            payload: "data".to_owned(),
        };
        let dbg = format!("{notif:?}");
        assert!(dbg.contains("ch"), "Debug should show channel: {dbg}");
        assert!(dbg.contains("data"), "Debug should show payload: {dbg}");
    }

    // --- Listener Debug (compile-time only, can't construct without DB) ---

    fn _assert_debug<T: std::fmt::Debug>() {}

    #[test]
    fn listener_debug_impl_exists() {
        _assert_debug::<Listener>();
    }

    // --- Send assertions ---

    fn _assert_send<T: Send>() {}

    #[test]
    fn notification_is_send() {
        _assert_send::<Notification>();
    }

    #[test]
    fn listener_is_send() {
        _assert_send::<Listener>();
    }
}
