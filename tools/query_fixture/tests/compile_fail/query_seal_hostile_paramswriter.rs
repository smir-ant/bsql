//! Seal probe — a hostile `impl ParamsWriter for EvilParams` cannot compile:
//! `ParamsWriter` has a module-private sealed super-trait
//! (`params::sealed::ParamsWriterSealed`) that external crates cannot
//! satisfy (`error[E0277]`). Without the seal a hostile impl could emit
//! arbitrary parameter bytes — crafted to confuse the server parser or
//! smuggle a delimiter — bypassing the type-OID enforcement of the query
//! pipeline. The sealed super-bound makes that architecturally impossible.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::decode::FormatCode;
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::write_buf::WriteBufFull;
use bsql_postgres_proto::WriteBuf;

struct EvilParams;

impl ParamsWriter for EvilParams {
    const COUNT: u16 = 1;
    const FORMATS: &'static [FormatCode] = &[FormatCode::Binary];
    const OIDS: &'static [u32] = &[23]; // INT4

    fn write_params(&self, _dst: &mut WriteBuf) -> Result<(), WriteBufFull> {
        // A real attack would write hostile injection bytes here.
        Ok(())
    }
}

fn main() {}
