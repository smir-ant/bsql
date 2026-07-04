//! Per-call arg-shape probe — binding arguments of the wrong type to a
//! `PreparedQuery` fails to type-check at the `query_params` boundary. The
//! engine's `query_params<P, R, S>(.., q: &PreparedQuery<P, R>, args: P, ..)`
//! ties the arg-tuple type `P` to the query's pinned `P`; Rust's nominal
//! tuple types make `(i64,)` and `(&str,)` distinct, so a mismatched tuple
//! is `error[E0308]`. The query is built by the compile-checked `query!`
//! macro (`$1` binds the `int8` PK, so `P = (i64,)`); binding `("hostile",)`
//! is rejected. Without this the wrong value would reach the encoder path.

use bsql_postgres_proto::engine::{session, Never, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};
use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

bsql::query!(SealArgMismatch, "SELECT id FROM users WHERE id = $1");

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
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
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
        // Bind a `(&str,)` to a query expecting `(i64,)`: E0308, tuple types
        // are nominally distinct.
        let _ = block_on(e.query_params(live, &SealArgMismatchQuery::PREPARED, ("hostile",), sink));
    });
}
