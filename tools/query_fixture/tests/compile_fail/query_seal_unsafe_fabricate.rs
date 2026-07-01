//! Seal probe — fabricating a `PreparedQuery` by transmuting a look-alike
//! `#[repr(C)]` mirror is barred by this file's `#![forbid(unsafe_code)]`.
//! This is the language-level half of the OS-boundary closure: even without
//! forbid, the resulting reference would point into stack / `.rodata` the OS
//! protects, so a subsequent mutation would segfault rather than corrupt the
//! prepared-query template.

#![forbid(unsafe_code)]

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;
use core::marker::PhantomData;

fn main() {
    #[repr(C)]
    struct Mirror {
        sql: &'static str,
        stmt_name: &'static str,
        param_oids: &'static [u32],
        row_oids: &'static [u32],
        parse_template: &'static [u8],
        bind_execute_prefix: &'static [u8],
        _phantom: PhantomData<fn(()) -> ()>,
    }
    let mirror = Mirror {
        sql: "DROP TABLE users; --",
        stmt_name: "x",
        param_oids: &[],
        row_oids: &[],
        parse_template: &[],
        bind_execute_prefix: &[],
        _phantom: PhantomData,
    };
    let _hostile: &PreparedQuery<(), ()> = unsafe {
        &*(&mirror as *const Mirror as *const PreparedQuery<(), ()>)
    };
}
