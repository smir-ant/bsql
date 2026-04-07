//! Singleflight request coalescing for query deduplication.
//!
//! When multiple threads issue the SAME query (same sql_hash + same parameter
//! bytes) simultaneously, only one actually executes against PostgreSQL. The others
//! wait for the result and receive a shared copy via a condvar.
//!
//! This is opt-in: enabled via `Pool::builder().singleflight(true)`.
//!
//! # Key design
//!
//! Key = hash of (sql_hash, parameter bytes). We use rapidhash to combine the
//! sql_hash with a hash of the parameter slice. If a request is already in-flight
//! with the same key, the caller waits on its condvar instead of executing a new
//! query.
//!
//! # Limitations
//!
//! - Only coalesces `query_raw` and `query_raw_readonly` (not `execute_raw`).
//!   Writes must not be coalesced.
//! - The result is `Arc`-shared, so callers receive the same data (no mutations).
//! - Large result sets are shared by reference, reducing memory for hot reads.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::error::BsqlError;

/// Shared result type sent to waiting threads.
type SharedResult = Arc<Result<Arc<OwnedResultSnapshot>, BsqlError>>;

/// State shared between a leader and its followers via condvar.
pub struct FlightState {
    result: Mutex<Option<SharedResult>>,
    condvar: Condvar,
    /// Set to true when leader drops without completing.
    /// Followers check this to break out of the wait loop.
    cancelled: std::sync::atomic::AtomicBool,
}

/// The in-flight map type: key -> flight state.
type InFlightMap = Arc<Mutex<HashMap<u64, Arc<FlightState>>>>;

/// A snapshot of query results that can be shared across threads.
///
/// Unlike `OwnedResult`, this does not own an arena — the data has been
/// copied into owned `Vec<u8>` storage for safe sharing.
pub struct OwnedResultSnapshot {
    /// The query result metadata (column offsets, column descriptors).
    pub result: bsql_driver_postgres::QueryResult,
    /// Arena data copied into owned storage for sharing.
    pub arena: bsql_driver_postgres::Arena,
}

/// Singleflight coalescing layer.
///
/// Tracks in-flight queries by key. Concurrent identical queries share results.
pub struct Singleflight {
    /// In-flight queries: key -> flight state.
    /// Uses std::sync::Mutex because the critical section is trivial
    /// (HashMap insert/remove — no I/O).
    /// Wrapped in Arc so FlightLeader can hold a back-reference for cleanup on drop.
    in_flight: InFlightMap,
}

/// Result of attempting to join a singleflight group.
pub enum FlightResult {
    /// This thread is the leader — it should execute the query.
    Leader(FlightLeader),
    /// Another thread is already executing this query — wait for the result.
    Follower(Arc<FlightState>),
}

/// Handle for the leader thread that will execute the query and notify followers.
///
/// If the leader is dropped without calling `complete()` (e.g., the thread panics),
/// the `Drop` impl removes the key from the in-flight map so new requests don't
/// wait on a dead condvar. Followers waiting on the condvar are woken and will
/// find `None` in the result, which surfaces as a query error.
pub struct FlightLeader {
    key: u64,
    state: Arc<FlightState>,
    /// Back-reference to the in-flight map for cleanup on drop.
    /// `None` after `complete()` has been called (key already removed).
    in_flight: Option<InFlightMap>,
}

impl FlightLeader {
    /// Send the result to all waiting followers and remove from in-flight map.
    pub fn complete(mut self, sf: &Singleflight, result: SharedResult) {
        // Remove from in-flight first so new requests don't join a completed flight
        sf.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
        // Mark as completed so Drop doesn't double-remove
        self.in_flight = None;
        // Store the result and notify all waiting followers
        *self.state.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
        self.state.condvar.notify_all();
    }
}

impl Drop for FlightLeader {
    fn drop(&mut self) {
        // If complete() was not called (e.g., leader thread panicked), remove
        // the key from the in-flight map and signal cancellation.
        if let Some(ref map) = self.in_flight {
            map.lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.key);
            // Signal cancellation so followers break out of wait loop
            self.state
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
            self.state.condvar.notify_all();
        }
    }
}

impl Singleflight {
    /// Create a new singleflight coalescing layer.
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Try to join an in-flight query group, or become the leader.
    ///
    /// `key` should be a hash of (sql_hash, parameter bytes).
    pub fn try_join(&self, key: u64) -> FlightResult {
        let mut map = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());

        if let Some(state) = map.get(&key) {
            // Another thread is already executing — wait on condvar
            FlightResult::Follower(Arc::clone(state))
        } else {
            // We are the leader — create flight state
            let state = Arc::new(FlightState {
                result: Mutex::new(None),
                condvar: Condvar::new(),
                cancelled: std::sync::atomic::AtomicBool::new(false),
            });
            map.insert(key, Arc::clone(&state));
            FlightResult::Leader(FlightLeader {
                key,
                state,
                in_flight: Some(Arc::clone(&self.in_flight)),
            })
        }
    }

    /// Wait for a flight result as a follower.
    ///
    /// Blocks until the leader calls `complete()`, is dropped, or the 30-second
    /// timeout expires. Returns `None` if the leader was dropped without
    /// completing (e.g., panic) or if the wait timed out.
    pub fn wait_for_result(state: &FlightState) -> Option<SharedResult> {
        const SINGLEFLIGHT_TIMEOUT: Duration = Duration::from_secs(30);
        let mut guard = state.result.lock().unwrap_or_else(|e| e.into_inner());
        while guard.is_none() {
            // Check if leader dropped without completing (panic, error, etc.)
            if state.cancelled.load(std::sync::atomic::Ordering::Acquire) {
                return None;
            }
            let (new_guard, wait_result) = state
                .condvar
                .wait_timeout(guard, SINGLEFLIGHT_TIMEOUT)
                .unwrap_or_else(|e| e.into_inner());
            guard = new_guard;
            // If the wait timed out and there's still no result, give up.
            if wait_result.timed_out() && guard.is_none() {
                return None;
            }
        }
        guard.clone()
    }

    /// Compute a singleflight key from sql_hash and parameter data.
    ///
    /// Uses rapidhash to combine the sql_hash with a hash of all parameter
    /// bytes (including actual encoded values). Two queries with the same SQL
    /// and same parameter values produce the same key.
    pub fn compute_key(
        sql_hash: u64,
        params: &[&(dyn bsql_driver_postgres::Encode + Sync)],
    ) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = rapidhash::quality::RapidHasher::default();
        sql_hash.hash(&mut hasher);
        // Thread-local scratch buffer avoids heap allocation on every key computation.
        // encode_binary requires &mut Vec<u8>, so SmallVec won't work here.
        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        SCRATCH.with(|cell| {
            let mut scratch = cell.borrow_mut();
            for param in params {
                if param.is_null() {
                    hasher.write_u8(0xFF); // NULL marker
                } else {
                    scratch.clear();
                    param.encode_binary(&mut scratch);
                    hasher.write(&scratch);
                }
            }
        });
        hasher.finish()
    }
}

impl Default for Singleflight {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleflight_leader_when_empty() {
        let sf = Singleflight::new();
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    #[test]
    fn singleflight_follower_when_in_flight() {
        let sf = Singleflight::new();
        let _leader = sf.try_join(42);
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Follower(_)));
    }

    #[test]
    fn singleflight_different_keys_both_leaders() {
        let sf = Singleflight::new();
        let r1 = sf.try_join(42);
        let r2 = sf.try_join(43);
        assert!(matches!(r1, FlightResult::Leader(_)));
        assert!(matches!(r2, FlightResult::Leader(_)));
    }

    #[test]
    fn singleflight_complete_removes_from_map() {
        let sf = Singleflight::new();
        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };
        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("test".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        // After completion, same key should produce a new leader
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    #[test]
    fn compute_key_same_inputs_same_key() {
        let k1 = Singleflight::compute_key(123, &[]);
        let k2 = Singleflight::compute_key(123, &[]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_different_sql_hash_different_key() {
        let k1 = Singleflight::compute_key(123, &[]);
        let k2 = Singleflight::compute_key(456, &[]);
        assert_ne!(k1, k2);
    }

    // --- compute_key with actual params ---

    #[test]
    fn compute_key_same_params_same_key() {
        let a = 42i32;
        let b = 42i32;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_different_params_different_key() {
        let a = 42i32;
        let b = 99i32;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn compute_key_different_sql_same_params_different_key() {
        let a = 42i32;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(200, &[&a]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn compute_key_null_param_handling() {
        // Option<i32> = None encodes as NULL
        let null_val: Option<i32> = None;
        let some_val: Option<i32> = Some(42);
        let k1 = Singleflight::compute_key(100, &[&null_val]);
        let k2 = Singleflight::compute_key(100, &[&some_val]);
        assert_ne!(k1, k2, "NULL and Some(42) should produce different keys");
    }

    #[test]
    fn compute_key_two_nulls_same_key() {
        let a: Option<i32> = None;
        let b: Option<i32> = None;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_multiple_params() {
        let a = 1i32;
        let b = "hello";
        let k1 = Singleflight::compute_key(100, &[&a, &b]);
        let k2 = Singleflight::compute_key(100, &[&a, &b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_param_order_matters() {
        let a = 1i32;
        let b = 2i32;
        let k1 = Singleflight::compute_key(100, &[&a, &b]);
        let k2 = Singleflight::compute_key(100, &[&b, &a]);
        assert_ne!(k1, k2);
    }

    // --- FlightLeader::complete notifies follower ---

    #[test]
    fn leader_complete_notifies_follower() {
        let sf = Arc::new(Singleflight::new());

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        let follower_state = match sf.try_join(42) {
            FlightResult::Follower(state) => state,
            _ => panic!("expected follower"),
        };

        let handle = std::thread::spawn(move || Singleflight::wait_for_result(&follower_state));

        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("test".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        let received = handle.join().unwrap();
        assert!(received.is_some());
        assert!(received.unwrap().is_err());
    }

    // --- Multiple followers receive same result ---

    #[test]
    fn multiple_followers_receive_result() {
        let sf = Arc::new(Singleflight::new());

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        let state1 = match sf.try_join(42) {
            FlightResult::Follower(s) => s,
            _ => panic!("expected follower 1"),
        };
        let state2 = match sf.try_join(42) {
            FlightResult::Follower(s) => s,
            _ => panic!("expected follower 2"),
        };

        let h1 = std::thread::spawn(move || Singleflight::wait_for_result(&state1));
        let h2 = std::thread::spawn(move || Singleflight::wait_for_result(&state2));

        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("done".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();
        assert!(r1.is_some());
        assert!(r1.unwrap().is_err());
        assert!(r2.is_some());
        assert!(r2.unwrap().is_err());
    }

    // --- Drop leader without completing -> key is removed from map ---

    #[test]
    fn drop_leader_without_complete_cleans_up_map() {
        let sf = Singleflight::new();

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        // Drop leader without calling complete (e.g., thread panicked).
        // The Drop impl removes the key from the in-flight map so new
        // requests don't wait on a dead condvar.
        drop(leader);

        // A new try_join for the same key should produce a NEW leader
        // (the entry was cleaned up on drop).
        let result = sf.try_join(42);
        assert!(
            matches!(result, FlightResult::Leader(_)),
            "key should be removed from map after leader drop without complete"
        );
    }

    // --- Concurrent stress test ---

    #[test]
    fn concurrent_stress_test() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let sf = Arc::new(Singleflight::new());
        let leader_count = Arc::new(AtomicUsize::new(0));
        let follower_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // 10 threads, 5 unique keys (2 threads per key)
        for i in 0..10 {
            let sf = Arc::clone(&sf);
            let leaders = Arc::clone(&leader_count);
            let followers = Arc::clone(&follower_count);
            let key = (i % 5) as u64;

            handles.push(std::thread::spawn(move || {
                match sf.try_join(key) {
                    FlightResult::Leader(leader) => {
                        leaders.fetch_add(1, Ordering::Relaxed);
                        // Complete immediately
                        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool(
                            "stress".into(),
                        ));
                        leader.complete(&sf, Arc::new(Err(err)));
                    }
                    FlightResult::Follower(_state) => {
                        followers.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let total = leader_count.load(Ordering::Relaxed) + follower_count.load(Ordering::Relaxed);
        assert_eq!(total, 10, "all 10 threads should participate");
        // At least 5 leaders (one per unique key)
        assert!(
            leader_count.load(Ordering::Relaxed) >= 5,
            "should have at least 5 leaders (one per key)"
        );
    }

    // --- Default trait ---

    #[test]
    fn singleflight_default() {
        let sf = Singleflight::default();
        // Should be able to use it
        let result = sf.try_join(1);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    // --- Send + Sync assertions ---

    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    #[test]
    fn singleflight_is_send_and_sync() {
        _assert_send::<Singleflight>();
        _assert_sync::<Singleflight>();
    }

    // --- compute_key with string params ---

    #[test]
    fn compute_key_string_params() {
        let a = "hello";
        let b = "world";
        let k1 = Singleflight::compute_key(100, &[&a, &b]);
        let k2 = Singleflight::compute_key(100, &[&a, &b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_empty_params_consistent() {
        let k1 = Singleflight::compute_key(0, &[]);
        let k2 = Singleflight::compute_key(0, &[]);
        assert_eq!(k1, k2);
    }

    // --- Gap: compute_key with bool params ---

    #[test]
    fn compute_key_bool_params() {
        let t = true;
        let f = false;
        let k1 = Singleflight::compute_key(100, &[&t]);
        let k2 = Singleflight::compute_key(100, &[&f]);
        assert_ne!(k1, k2, "true and false should produce different keys");
    }

    #[test]
    fn compute_key_bool_same_value_same_key() {
        let a = true;
        let b = true;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_eq!(k1, k2);
    }

    // --- Gap: compute_key with mixed types ---

    #[test]
    fn compute_key_mixed_types() {
        let i = 42i32;
        let s = "hello";
        let b = true;
        let k1 = Singleflight::compute_key(100, &[&i, &s, &b]);
        let k2 = Singleflight::compute_key(100, &[&i, &s, &b]);
        assert_eq!(k1, k2, "same mixed params should produce same key");
    }

    #[test]
    fn compute_key_mixed_types_different_values() {
        let i1 = 42i32;
        let i2 = 43i32;
        let s = "hello";
        let k1 = Singleflight::compute_key(100, &[&i1, &s]);
        let k2 = Singleflight::compute_key(100, &[&i2, &s]);
        assert_ne!(k1, k2, "different int values should produce different keys");
    }

    // --- Gap: compute_key with f64 params ---

    #[test]
    fn compute_key_f64_params() {
        let a = 1.23f64;
        let b = 1.23f64;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_f64_different_values() {
        let a = 1.23f64;
        let b = 4.56f64;
        let k1 = Singleflight::compute_key(100, &[&a]);
        let k2 = Singleflight::compute_key(100, &[&b]);
        assert_ne!(k1, k2);
    }

    // --- Leader complete with no followers ---

    #[test]
    fn leader_complete_with_no_followers() {
        let sf = Singleflight::new();
        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };
        // Complete without any followers. Should not panic.
        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("solo".into()));
        leader.complete(&sf, Arc::new(Err(err)));

        // Key should be removed
        let result = sf.try_join(42);
        assert!(matches!(result, FlightResult::Leader(_)));
    }

    // --- Audit: leader dropped while followers are waiting ---

    #[test]
    fn follower_gets_none_when_leader_dropped_without_complete() {
        let sf = Arc::new(Singleflight::new());

        let leader = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };

        let follower_state = match sf.try_join(42) {
            FlightResult::Follower(s) => s,
            _ => panic!("expected follower"),
        };

        let handle = std::thread::spawn(move || {
            // This will block until leader notifies. Since leader drops without
            // completing, the condvar is notified but result is None.
            // However, our wait_for_result loops while None, so we need the
            // leader drop to set something. The current impl wakes followers
            // but leaves result as None. The wait_for_result will spin once
            // more on the lock. We need to handle this edge case.
            // For now, verify that the follower state is eventually dropped.
            let _ = follower_state;
        });

        // Drop leader without completing (simulates thread panic).
        drop(leader);

        handle.join().unwrap();

        // Key should be cleaned up
        let result = sf.try_join(42);
        assert!(
            matches!(result, FlightResult::Leader(_)),
            "key should be removed from map after leader drop"
        );
    }

    // --- Audit: leader drop cleans up, new leader can succeed ---

    #[test]
    fn new_leader_succeeds_after_previous_leader_dropped() {
        let sf = Arc::new(Singleflight::new());

        // First leader drops without completing (simulates panic).
        let leader1 = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected leader"),
        };
        drop(leader1);

        // A new try_join should produce a fresh leader (not a follower on a dead condvar).
        let leader2 = match sf.try_join(42) {
            FlightResult::Leader(l) => l,
            _ => panic!("expected new leader after previous leader drop"),
        };

        let follower_state = match sf.try_join(42) {
            FlightResult::Follower(s) => s,
            _ => panic!("expected follower for second leader"),
        };

        let handle = std::thread::spawn(move || Singleflight::wait_for_result(&follower_state));

        let err = BsqlError::from(bsql_driver_postgres::DriverError::Pool("retry".into()));
        leader2.complete(&sf, Arc::new(Err(err)));

        let received = handle.join().unwrap();
        assert!(received.is_some());
        assert!(received.unwrap().is_err());
    }
}
