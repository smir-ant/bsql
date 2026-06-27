// EXPECT: compiles — the sanctioned path. A single linear token threads
// through two SEQUENTIAL async verbs inside one `async` scope that holds
// `&mut engine` across both `await`s (the R4 self-referential-async shape),
// and is consumed at the end without escaping the scope.
use bsql_postgres_proto::engine::{session, EngineError, Transport};
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
    let ok = session(T0, |mut e, live| {
        block_on(async move {
            let live = e.begin(live).await?;
            let live = e.commit(live).await?;
            let _consumed = live;
            Ok::<(), EngineError<Infallible>>(())
        })
        .is_ok()
    });
    assert!(ok);
}
