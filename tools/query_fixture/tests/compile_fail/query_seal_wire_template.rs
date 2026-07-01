//! Seal probe — the baked `parse_template` bytes cannot be harvested or
//! mutated from outside the crate: the field is `pub(crate)`
//! (`error[E0616]`) and the `unsafe` raw-pointer write is barred by this
//! file's `#![forbid(unsafe_code)]`. The macro emits `parse_template` as a
//! `&'static [u8]` into read-only `.rodata`; the field privacy plus the
//! file-scope forbid close the language-level half of the OS-boundary
//! closure. Built by `query!`.

#![forbid(unsafe_code)]

bsql_query_macros::query!(SealWireTemplate, "SELECT id FROM users");

fn main() {
    let _hostile_bytes: &[u8] = SealWireTemplateQuery::PREPARED.parse_template;
    let _ = _hostile_bytes;
    unsafe {
        let ptr = &SealWireTemplateQuery::PREPARED.parse_template as *const &[u8] as *mut &[u8];
        ptr.write(b"hostile bytes");
    }
}
