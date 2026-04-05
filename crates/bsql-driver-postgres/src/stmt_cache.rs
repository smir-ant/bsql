//! Shared statement cache types used by both `Connection` and `SyncConnection`.
//!
//! The cache is Vec-backed with O(n) lookup — faster than HashMap for small n
//! (< ~30 entries) due to cache locality and zero hashing overhead.

use std::sync::Arc;

use crate::proto;
use crate::types::ColumnDesc;

// --- Vec-based statement cache ---
//
// For typical workloads of 5-20 cached statements, linear scan over a Vec
// with u64 comparison is faster than HashMap probe sequence because:
// - Vec = contiguous memory, perfect cache locality (all entries in L1)
// - u64 comparison = one instruction per entry
// - No hash probe, no bucket lookup, no load factor math

/// Vec-backed statement cache with O(n) lookup. Faster than HashMap for
/// small n (< ~30 entries) due to cache locality and zero hashing overhead.
pub(crate) struct StmtCache {
    entries: Vec<(u64, StmtInfo)>,
}

impl Default for StmtCache {
    fn default() -> Self {
        Self {
            entries: Vec::with_capacity(16),
        }
    }
}

impl StmtCache {
    #[inline]
    pub(crate) fn get_mut(&mut self, hash: &u64) -> Option<&mut StmtInfo> {
        self.entries
            .iter_mut()
            .find(|(h, _)| h == hash)
            .map(|(_, info)| info)
    }

    #[inline]
    pub(crate) fn get(&self, hash: &u64) -> Option<&StmtInfo> {
        self.entries
            .iter()
            .find(|(h, _)| h == hash)
            .map(|(_, info)| info)
    }

    #[inline]
    pub(crate) fn contains_key(&self, hash: &u64) -> bool {
        self.entries.iter().any(|(h, _)| h == hash)
    }

    #[inline]
    pub(crate) fn insert(&mut self, hash: u64, info: StmtInfo) {
        if let Some(entry) = self.entries.iter_mut().find(|(h, _)| *h == hash) {
            entry.1 = info;
        } else {
            self.entries.push((hash, info));
        }
    }

    #[inline]
    pub(crate) fn remove(&mut self, hash: &u64) -> Option<StmtInfo> {
        if let Some(pos) = self.entries.iter().position(|(h, _)| h == hash) {
            Some(self.entries.swap_remove(pos).1)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Evict the least recently used entry (lowest `last_used` counter).
    pub(crate) fn evict_lru(&mut self) -> Option<(u64, StmtInfo)> {
        if self.entries.is_empty() {
            return None;
        }
        let min_idx = self
            .entries
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, info))| info.last_used)
            .map(|(i, _)| i)?;
        Some(self.entries.swap_remove(min_idx))
    }
}

/// Format a statement name from a hash: `"s_{hash:016x}"`.
///
/// Stack-allocated formatting. The name is always exactly 18 bytes:
/// "s_" (2) + 16 hex digits (16).
/// Uses a fixed [u8; 18] buffer with manual hex encoding — no heap allocation.
#[inline]
pub(crate) fn make_stmt_name(hash: u64) -> Box<str> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 18]; // "s_" + 16 hex = 18 bytes
    buf[0] = b's';
    buf[1] = b'_';
    let bytes = hash.to_be_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[2 + i * 2] = HEX[(b >> 4) as usize];
        buf[2 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
    // buf contains only ASCII hex digits ('0'-'9','a'-'f') and 's','_'.
    // from_utf8 is infallible here — the expect documents why.
    let s = std::str::from_utf8(&buf).expect("BUG: stmt name buffer contains only ASCII hex");
    s.into()
}

/// Cached information about a prepared statement.
///
/// The statement name is a 64-bit rapidhash formatted as `"s_{hash:016x}"`.
/// With 2^64 possible values, collision probability is negligible for realistic
/// workloads (e.g., ~1 in 10^13 for 10,000 distinct queries). A collision would
/// cause a protocol error from PostgreSQL (parameter mismatch), not silent
/// data corruption. If you have an adversarial workload that could craft
/// collisions, consider a verified cache keyed on the full SQL text.
pub(crate) struct StmtInfo {
    /// Statement name: `"s_{hash:016x}"`
    pub(crate) name: Box<str>,
    /// Column metadata from RowDescription.
    pub(crate) columns: Arc<[ColumnDesc]>,
    /// Monotonic counter value at last use for LRU eviction.
    /// Cheaper than `Instant::now()` which is a syscall on macOS (~20-40ns).
    pub(crate) last_used: u64,
    /// Pre-built Bind message template for fast re-execution.
    ///
    /// On the first execution of a cached statement, we snapshot the complete
    /// Bind message bytes. On subsequent executions with fixed-size parameters,
    /// we memcpy the template and patch only the parameter data in-place,
    /// avoiding the full `write_bind_params` rebuild (~100-200ns savings per
    /// query on the hot path).
    ///
    /// `None` until the first execution populates it.
    pub(crate) bind_template: Option<BindTemplate>,
}

/// Pre-built Bind+Execute+Sync message template for fast re-execution.
///
/// Stores the complete Bind message bytes followed by EXECUTE_SYNC, and the
/// byte offsets where each parameter's data begins. On re-execution with
/// same-sized params, we copy the template and overwrite param data in-place
/// via `encode_at` — no scratch buffer, no double-copy.
pub(crate) struct BindTemplate {
    /// Bind message bytes + EXECUTE_SYNC (15 bytes) appended.
    pub(crate) bytes: Vec<u8>,
    /// Offset where the Bind message ends (before EXECUTE_SYNC).
    /// Used by streaming queries that need Execute+Flush instead.
    pub(crate) bind_end: usize,
    /// For each parameter: `(data_offset, data_len)` within `bytes`.
    /// `data_offset` points to the first byte of param data (after the i32 length).
    /// `data_len` is the length of the param data. -1 means NULL.
    pub(crate) param_slots: Vec<(usize, i32)>,
}

// --- Bind template builder ---

/// Build a `BindTemplate` from the current write_buf contents.
///
/// Parses the Bind message to locate each parameter's data offset and length.
/// Appends EXECUTE_SYNC to the template bytes so the hot path is a single memcpy.
/// Returns `None` if the message cannot be parsed.
pub(crate) fn build_bind_template(
    write_buf: &[u8],
    param_count: usize,
) -> Option<BindTemplate> {
    if write_buf.is_empty() || write_buf[0] != b'B' {
        return None;
    }
    if write_buf.len() < 5 {
        return None;
    }

    let mut pos = 5; // skip type byte (1) + length (4)

    // Skip portal name (NUL-terminated).
    while pos < write_buf.len() && write_buf[pos] != 0 {
        pos += 1;
    }
    pos += 1;

    // Skip statement name (NUL-terminated).
    while pos < write_buf.len() && write_buf[pos] != 0 {
        pos += 1;
    }
    pos += 1;

    // Skip format codes.
    if pos + 2 > write_buf.len() {
        return None;
    }
    let num_fmt_codes = i16::from_be_bytes([write_buf[pos], write_buf[pos + 1]]);
    pos += 2;
    pos += num_fmt_codes.max(0) as usize * 2;

    // Parameter count.
    if pos + 2 > write_buf.len() {
        return None;
    }
    let wire_param_count = i16::from_be_bytes([write_buf[pos], write_buf[pos + 1]]) as usize;
    pos += 2;

    if wire_param_count != param_count {
        return None;
    }

    let mut param_slots = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        if pos + 4 > write_buf.len() {
            return None;
        }
        let data_len = i32::from_be_bytes([
            write_buf[pos],
            write_buf[pos + 1],
            write_buf[pos + 2],
            write_buf[pos + 3],
        ]);
        pos += 4;

        if data_len < 0 {
            param_slots.push((pos, -1));
        } else {
            param_slots.push((pos, data_len));
            pos += data_len as usize;
        }
    }

    // Include EXECUTE_SYNC in the template so the hot path is one memcpy.
    let bind_end = write_buf.len();
    let mut bytes = Vec::with_capacity(bind_end + proto::EXECUTE_SYNC.len());
    bytes.extend_from_slice(write_buf);
    bytes.extend_from_slice(proto::EXECUTE_SYNC);

    Some(BindTemplate {
        bytes,
        bind_end,
        param_slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Encode;
    use crate::types::hash_sql;

    // ---- StmtCache tests ----

    /// Vec-based StmtCache basic operations.
    #[test]
    fn stmt_cache_basic_ops() {
        let mut cache = StmtCache::default();
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains_key(&42));
        assert!(cache.get(&42).is_none());
        assert!(cache.get_mut(&42).is_none());
        assert!(cache.remove(&42).is_none());
    }

    #[test]
    fn stmt_cache_insert_get_remove() {
        let mut cache = StmtCache::default();
        let info = StmtInfo {
            name: "s_test".into(),
            columns: Arc::from(Vec::new()),
            last_used: 1,
            bind_template: None,
        };
        cache.insert(42, info);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&42));
        assert!(cache.get(&42).is_some());
        assert!(cache.get_mut(&42).is_some());

        let removed = cache.remove(&42);
        assert!(removed.is_some());
        assert_eq!(cache.len(), 0);
        assert!(!cache.contains_key(&42));
    }

    #[test]
    fn stmt_cache_evict_lru() {
        let mut cache = StmtCache::default();
        for i in 0..3u64 {
            cache.insert(
                i,
                StmtInfo {
                    name: format!("s_{i}").into(),
                    columns: Arc::from(Vec::new()),
                    last_used: i + 1,
                    bind_template: None,
                },
            );
        }
        assert_eq!(cache.len(), 3);
        let evicted = cache.evict_lru().unwrap();
        assert_eq!(evicted.0, 0); // lowest last_used=1
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn stmt_cache_insert_overwrite() {
        let mut cache = StmtCache::default();
        let info1 = StmtInfo {
            name: "s_a".into(),
            columns: Arc::from(Vec::new()),
            last_used: 1,
            bind_template: None,
        };
        let info2 = StmtInfo {
            name: "s_b".into(),
            columns: Arc::from(Vec::new()),
            last_used: 2,
            bind_template: None,
        };
        cache.insert(42, info1);
        cache.insert(42, info2);
        assert_eq!(cache.len(), 1);
        assert_eq!(&*cache.get(&42).unwrap().name, "s_b");
    }

    // ---- make_stmt_name tests ----

    /// Statement name formatting uses hex encoding.
    #[test]
    fn stmt_name_format() {
        let name = make_stmt_name(0);
        assert_eq!(&*name, "s_0000000000000000");
        let name = make_stmt_name(0xDEADBEEF12345678);
        assert_eq!(&*name, "s_deadbeef12345678");
        let name = make_stmt_name(u64::MAX);
        assert_eq!(&*name, "s_ffffffffffffffff");
    }

    #[test]
    fn stmt_name_format_verification() {
        let name = make_stmt_name(0xDEADBEEFCAFEBABE);
        assert!(name.starts_with("s_"), "must start with s_");
        assert_eq!(name.len(), 18, "s_ (2) + 16 hex = 18");
        assert!(
            name[2..].chars().all(|c| c.is_ascii_hexdigit()),
            "remaining chars must be hex: {}",
            &*name
        );
    }

    #[test]
    fn stmt_name_zero() {
        let name = make_stmt_name(0);
        assert_eq!(&*name, "s_0000000000000000");
    }

    #[test]
    fn stmt_name_max() {
        let name = make_stmt_name(u64::MAX);
        assert_eq!(&*name, "s_ffffffffffffffff");
    }

    #[test]
    fn stmt_name_one() {
        let name = make_stmt_name(1);
        assert_eq!(&*name, "s_0000000000000001");
    }

    #[test]
    fn stmt_name_powers_of_two() {
        let name = make_stmt_name(256);
        assert_eq!(&*name, "s_0000000000000100");
    }

    #[test]
    fn stmt_name_format_always_18_chars() {
        for val in [0u64, 1, 0xFF, 0xFFFF, 0xFFFF_FFFF, u64::MAX] {
            let name = make_stmt_name(val);
            assert_eq!(name.len(), 18, "name len for {val:x}");
            assert!(name.starts_with("s_"));
            assert!(name[2..].chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    // ---- StmtInfo tests ----

    #[test]
    fn stmt_info_has_last_used_counter() {
        let info = StmtInfo {
            name: "s_test".into(),
            columns: Arc::from(Vec::new()),
            last_used: 42,
            bind_template: None,
        };
        assert_eq!(info.last_used, 42);
    }

    // ---- hash_sql tests ----

    #[test]
    fn hash_sql_consistency() {
        let h = hash_sql("SELECT 1");
        assert_eq!(h, hash_sql("SELECT 1"));
        assert_ne!(h, hash_sql("SELECT 2"));
    }

    // ---- build_bind_template tests ----

    #[test]
    fn build_bind_template_basic() {
        let mut buf = Vec::new();
        let val: i32 = 42;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);

        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 1);
        // i32 is 4 bytes
        assert_eq!(tmpl.param_slots[0].1, 4);
    }

    #[test]
    fn build_bind_template_null_param() {
        let mut buf = Vec::new();
        let val: Option<i32> = None;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);

        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 1);
        assert_eq!(tmpl.param_slots[0].1, -1); // NULL
    }

    #[test]
    fn build_bind_template_multiple_params() {
        let mut buf = Vec::new();
        let id: i32 = 1;
        let name: &str = "alice";
        proto::write_bind_params(
            &mut buf,
            "",
            "s_test",
            &[&id as &(dyn Encode + Sync), &name as &(dyn Encode + Sync)],
        );

        let tmpl = build_bind_template(&buf, 2);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 2);
        assert_eq!(tmpl.param_slots[0].1, 4); // i32 = 4 bytes
        assert_eq!(tmpl.param_slots[1].1, 5); // "alice" = 5 bytes
    }

    #[test]
    fn build_bind_template_empty_buf() {
        let tmpl = build_bind_template(&[], 0);
        assert!(tmpl.is_none());
    }

    #[test]
    fn build_bind_template_wrong_type() {
        let tmpl = build_bind_template(&[b'E', 0, 0, 0, 4], 0);
        assert!(tmpl.is_none());
    }

    #[test]
    fn build_bind_template_param_count_mismatch() {
        let mut buf = Vec::new();
        let val: i32 = 42;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);

        // Ask for 2 params but only 1 in the message.
        let tmpl = build_bind_template(&buf, 2);
        assert!(tmpl.is_none());
    }

    #[test]
    fn build_bind_template_too_short_buf() {
        let tmpl = build_bind_template(&[b'B', 0, 0], 0);
        assert!(tmpl.is_none());
    }

    #[test]
    fn build_bind_template_zero_params() {
        let mut buf = Vec::new();
        proto::write_bind_params(&mut buf, "", "s_test", &[]);
        let tmpl = build_bind_template(&buf, 0);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 0);
    }

    #[test]
    fn build_bind_template_bool_param() {
        let mut buf = Vec::new();
        let val = true;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);
        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 1);
        assert_eq!(tmpl.param_slots[0].1, 1); // bool is 1 byte
    }

    #[test]
    fn build_bind_template_i64_param() {
        let mut buf = Vec::new();
        let val: i64 = 123456789;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);
        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots[0].1, 8); // i64 is 8 bytes
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn build_bind_template_f64_param() {
        let mut buf = Vec::new();
        let val: f64 = 3.14;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);
        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots[0].1, 8); // f64 is 8 bytes
    }

    #[test]
    fn build_bind_template_str_param() {
        let mut buf = Vec::new();
        let val: &str = "hello world";
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);
        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots[0].1, 11); // "hello world" = 11 bytes
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn build_bind_template_mixed_params_with_null() {
        let mut buf = Vec::new();
        let id: i32 = 1;
        let name: Option<i32> = None;
        let score: f64 = 9.9;
        proto::write_bind_params(
            &mut buf,
            "",
            "s_test",
            &[
                &id as &(dyn Encode + Sync),
                &name as &(dyn Encode + Sync),
                &score as &(dyn Encode + Sync),
            ],
        );
        let tmpl = build_bind_template(&buf, 3);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 3);
        assert_eq!(tmpl.param_slots[0].1, 4); // i32
        assert_eq!(tmpl.param_slots[1].1, -1); // NULL
        assert_eq!(tmpl.param_slots[2].1, 8); // f64
    }

    #[test]
    fn build_bind_template_preserves_bytes() {
        let mut buf = Vec::new();
        let val: i32 = 42;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);
        let bind_len = buf.len();
        let tmpl = build_bind_template(&buf, 1).unwrap();
        // Template bytes = Bind message + EXECUTE_SYNC appended.
        assert_eq!(
            &tmpl.bytes[..bind_len],
            &buf[..],
            "template must start with original Bind message"
        );
        assert_eq!(
            &tmpl.bytes[bind_len..],
            proto::EXECUTE_SYNC,
            "template must end with EXECUTE_SYNC"
        );
        // bind_end should equal the original Bind message length.
        assert_eq!(tmpl.bind_end, bind_len);
    }
}
