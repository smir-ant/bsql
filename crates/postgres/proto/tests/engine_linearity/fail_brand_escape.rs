// EXPECT: "lifetime may not live long enough" — the generative brand `'b`
// cannot escape its `session()` scope, so the closure cannot return the
// branded liveness token.
use bsql_postgres_proto::engine::{session, Transport};
use bsql_postgres_proto::{Credentials, Ident};
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
    fn write<'a>(
        &'a mut self,
        _buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn flush<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

fn main() {
    let user = Ident::try_from_str("brand").unwrap();
    // Try to smuggle the branded liveness token OUT of the for<'b> scope:
    let _escaped = session(T0, &user, None, None, Credentials::Trust, |_e, live| live);
}
