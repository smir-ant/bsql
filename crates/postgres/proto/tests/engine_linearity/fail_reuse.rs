// EXPECT: E0382 — `Live<'b>` is a linear token; reusing it after a verb
// consumes it is a use-of-moved-value error.
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
    session(T0, |mut e, live| {
        let token = block_on(e.begin(live)).unwrap();
        let _next = block_on(e.commit(token)).unwrap();
        // Reuse the ALREADY-CONSUMED token — use of moved value:
        let _bad = block_on(e.begin(token)).unwrap();
    });
}
