// EXPECT: brand mismatch — session-1's token cannot drive session-2's
// engine. The two `session(..)` scopes mint distinct, invariant brands, so
// passing one scope's `Live` to the other's engine is a lifetime error.
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

fn block_on<F: Future>(f: F) -> F::Output {
    use core::task::{Context, Poll};
    let mut f = core::pin::pin!(f);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

fn main() {
    let user = Ident::try_from_str("brand").unwrap();
    let _ = session(T0, &user, None, None, Credentials::Trust, |mut _e1, live1| {
        let _ = session(T0, &user, None, None, Credentials::Trust, |mut e2, _live2| {
            // Drive session-2's engine with session-1's branded token:
            let _ = block_on(e2.begin(live1));
        });
    });
}
