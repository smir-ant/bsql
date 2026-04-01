//! Singleflight: coalesce identical concurrent queries into one PG round-trip.
//!
//! When N handlers execute the same `SELECT` with the same SQL simultaneously,
//! only one query is sent to PostgreSQL. The result (`Arc<Vec<Row>>`) is shared
//! to all waiters via `broadcast`. Each consumer decodes from `&Row`
//! independently -- zero cloning of row data.
//!
//! Singleflight only applies to `Pool` (not `Transaction` or `PoolConnection`):
//! - Transactions have isolated snapshots -- sharing across them is incorrect.
//! - `PoolConnection` is an explicit opt-out of pool-level optimizations.
//!
//! Errors are NOT coalesced. If the executing query fails, waiters retry
//! independently on their next call.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio_postgres::Row;

/// Coalesces identical concurrent queries into a single PG round-trip.
pub(crate) struct Singleflight {
    /// In-flight queries: key = hash(sql), value = broadcast sender.
    ///
    /// The Mutex is held only for HashMap insert/remove (nanoseconds),
    /// never across await points.
    in_flight: Mutex<HashMap<u64, broadcast::Sender<Arc<Vec<Row>>>>>,
}

/// Result of checking the singleflight map.
pub(crate) enum FlightStatus {
    /// We are the first caller -- execute the query and broadcast.
    Leader,
    /// Another caller is already executing -- wait for their result.
    Follower(broadcast::Receiver<Arc<Vec<Row>>>),
}

impl Singleflight {
    pub(crate) fn new() -> Self {
        Self {
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Check if a query is already in-flight. Returns `Leader` if we should
    /// execute, or `Follower` with a receiver to wait on.
    ///
    /// If `Leader`, the caller MUST call `complete` or `abandon` afterwards.
    pub(crate) fn try_join(&self, key: u64) -> FlightStatus {
        let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(tx) = map.get(&key) {
            FlightStatus::Follower(tx.subscribe())
        } else {
            // 16 is the broadcast channel capacity. Receivers that lag will
            // get `RecvError::Lagged` -- but since we only ever send ONE
            // message per flight, capacity=1 would suffice. We use 2 for
            // safety against edge-case timing.
            let (tx, _rx) = broadcast::channel(2);
            map.insert(key, tx);
            FlightStatus::Leader
        }
    }

    /// Broadcast a successful result to all waiters and remove the entry.
    pub(crate) fn complete(&self, key: u64, rows: Arc<Vec<Row>>) {
        let tx = {
            let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            map.remove(&key)
        };
        if let Some(tx) = tx {
            // Ignore send error -- means no receivers (all dropped).
            let _ = tx.send(rows);
        }
    }

    /// Remove the entry without broadcasting (on error). Waiters' receivers
    /// will get `RecvError::Closed`, which callers handle by retrying.
    pub(crate) fn abandon(&self, key: u64) {
        let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(&key);
        // Sender drops here -> all receivers get RecvError::Closed
    }
}

/// Hash the SQL string to produce a singleflight key.
///
/// Singleflight only applies to parameterless queries (params.is_empty()).
/// Parameterized queries bypass singleflight entirely because different param
/// values produce the same SQL text but different results.
/// string, so singleflight key-generation is done at the codegen level where
/// concrete param types (which implement `Hash`) are available.
pub(crate) fn sql_key(sql: &str) -> u64 {
    let mut hasher = rapidhash::quality::RapidHasher::default();
    sql.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_empty_map() {
        let sf = Singleflight::new();
        let map = sf.in_flight.lock().unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn first_caller_is_leader() {
        let sf = Singleflight::new();
        assert!(matches!(sf.try_join(42), FlightStatus::Leader));
    }

    #[test]
    fn second_caller_is_follower() {
        let sf = Singleflight::new();
        let _ = sf.try_join(42); // leader
        assert!(matches!(sf.try_join(42), FlightStatus::Follower(_)));
    }

    #[test]
    fn different_keys_are_independent() {
        let sf = Singleflight::new();
        let _ = sf.try_join(42);
        assert!(matches!(sf.try_join(99), FlightStatus::Leader));
    }

    #[test]
    fn complete_removes_entry() {
        let sf = Singleflight::new();
        let _ = sf.try_join(42);
        sf.complete(42, Arc::new(Vec::new()));
        // After complete, next caller should be leader again
        assert!(matches!(sf.try_join(42), FlightStatus::Leader));
    }

    #[test]
    fn abandon_removes_entry() {
        let sf = Singleflight::new();
        let _ = sf.try_join(42);
        sf.abandon(42);
        assert!(matches!(sf.try_join(42), FlightStatus::Leader));
    }

    #[test]
    fn sql_key_deterministic() {
        let a = sql_key("SELECT id FROM users");
        let b = sql_key("SELECT id FROM users");
        assert_eq!(a, b);
    }

    #[test]
    fn sql_key_different_sql_different_key() {
        let a = sql_key("SELECT id FROM users");
        let b = sql_key("SELECT name FROM users");
        assert_ne!(a, b);
    }
}
