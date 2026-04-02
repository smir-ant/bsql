//! Singleflight placeholder.
//!
//! With bsql-driver, connection pooling and query execution are handled by
//! the driver layer. Singleflight coalescing at the bsql-core level is no
//! longer used — the driver's pool and connection architecture handles
//! concurrency differently (per-connection arenas, LIFO pool).
//!
//! This module is retained as a no-op to avoid breaking the module structure.
//! It may be re-introduced as a driver-level optimization in a future version.
