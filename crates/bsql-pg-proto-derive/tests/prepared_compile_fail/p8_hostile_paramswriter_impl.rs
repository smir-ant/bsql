//! Hostile-bypass probe **P8** — implement `ParamsWriter`
//! on a hostile user type to write injection bytes.
//!
//! # Tier
//!
//! Tier-1 by-construction. `ParamsWriter` is sealed via the
//! `params::sealed::ParamsWriterSealed` super-trait — external
//! crates cannot satisfy the bound because the sealed marker is
//! `pub` in a `pub(crate) mod sealed` module, unreachable from
//! outside.
//!
//! # Expected diagnostic
//!
//! `error[E0277]: the trait bound 'EvilParams: ParamsWriterSealed'
//! is not satisfied` (the sealed-trait super-bound diagnostic).
//!
//! # Why this probe matters
//!
//! `ParamsWriter::write_params` receives the WriteBuf and emits the
//! raw parameter bytes. A hostile impl could write arbitrary bytes
//! (designed to confuse the server parser, smuggle a query
//! delimiter, etc.) bypassing the macro's type-OID enforcement.
//! The sealed super-bound makes this architecturally impossible.
//!
//! # Memo cross-reference
//!
//! Memo §7 Probe P8.

extern crate bsql_pg_proto;

use bsql_pg_proto::params::ParamsWriter;
use bsql_pg_proto::decode::FormatCode;
use bsql_pg_proto::WriteBuf;
use bsql_pg_proto::write_buf::WriteBufFull;

struct EvilParams;

// P8 attack: hostile impl. Should fail because `ParamsWriter` has a
// sealed super-trait `ParamsWriterSealed` that external crates can't
// implement.
impl ParamsWriter for EvilParams {
    const COUNT: u16 = 1;
    const FORMATS: &'static [FormatCode] = &[FormatCode::Binary];
    const OIDS: &'static [u32] = &[23]; // INT4

    fn write_params(&self, _dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
        // Would write hostile injection bytes here in a real attack.
        Ok(())
    }
}

fn main() {}
