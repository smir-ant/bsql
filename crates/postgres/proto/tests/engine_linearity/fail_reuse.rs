// EXPECT: E0382 — `Live<'b>` is a linear token; reusing it after a verb
// consumes it is a use-of-moved-value error.
use bsql_postgres_proto::engine::{session, Transport};
use bsql_postgres_proto::{Credentials, Ident};
use core::convert::Infallible;
use core::future::{ready, Future};

struct T0;

impl Transport for T0 {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
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
    let _ = session(T0, &user, None, &[], Credentials::Trust, |mut e, live| {
        let token = block_on(e.connect(live)).unwrap();
        let _next = block_on(e.connect(token)).unwrap();
        // Reuse the ALREADY-CONSUMED token — use of moved value:
        let _bad = block_on(e.connect(token)).unwrap();
    });
}
