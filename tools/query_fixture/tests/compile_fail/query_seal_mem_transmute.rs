//! Seal probe — `core::mem::transmute` of a raw byte array into a
//! `PreparedQuery` is barred by this file's `#![forbid(unsafe_code)]`.
//! `mem::transmute` is `unsafe`-only by definition; the forbid rejects the
//! `unsafe` block at file scope. Same OS-boundary class as the raw-pointer
//! fabrication probe, for a distinct UB pattern.

#![forbid(unsafe_code)]

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;

fn main() {
    let bytes: [u8; 96] = [0; 96];
    let _hostile: PreparedQuery<(), ()> = unsafe { core::mem::transmute(bytes) };
}
