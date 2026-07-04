// EXPECT: E0382 — a `PreparedStatement` is consumed by `close_statement`, so
// using it again (here, closing it a second time) is a use-of-moved-value error.
// The compile-time half of the use-after-close safety invariant.
use bsql_postgres_proto::engine::{
    session, Never, PreparedStatement, Surface, Transport,
};
use bsql_postgres_proto::{Credentials, Ident, StmtName};
use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

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

fn sink(_surface: Surface<'_>) -> ControlFlow<Never> {
    ControlFlow::Continue(())
}

fn main() {
    let user = Ident::try_from_str("verbs").unwrap();
    let _ = session(T0, &user, None, &[], Credentials::Trust, |mut e, live| {
        let live = block_on(e.connect(live)).unwrap();
        let stmt = PreparedStatement::new(StmtName::try_from_str("s").unwrap(), Vec::new());
        // First close consumes the statement:
        let live = block_on(e.close_statement(live, stmt, sink)).unwrap().live;
        // Reuse the CLOSED (moved) statement — use of moved value:
        let _ = block_on(e.close_statement(live, stmt, sink));
    });
}
