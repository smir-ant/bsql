// EXPECT: "lifetime may not live long enough" — the generative brand `'b`
// cannot escape its `session()` scope, so the closure cannot return the
// branded liveness token.
use bsql_postgres_proto::engine::{session, Transport};
use core::convert::Infallible;
use core::future::{ready, Future};

struct T0;

impl Transport for T0 {
    type Error = Infallible;
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn write_all<'a>(
        &'a mut self,
        _buf: &'a [u8],
    ) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

fn main() {
    // Try to smuggle the branded liveness token OUT of the for<'b> scope:
    let _escaped = session(T0, |_e, live| live);
}
