//! Hostile-bypass probe **P7** — pass arguments of the wrong type to a
//! `PreparedQuery`.
//!
//! # Tier
//!
//! Tier-1 by-construction. The sans-IO engine's
//! `query_params<P, R, S>(.., q: &PreparedQuery<P, R>, args: P, ..)` ties the
//! arg-tuple type `P` to the query's pinned `P`. Mismatched tuple types fail
//! to type-check at the call boundary — Rust's nominal tuple types make
//! `(i32,)` and `(&str,)` distinct.
//!
//! # Expected diagnostic
//!
//! `error[E0308]: mismatched types: expected 'i32', found '&str'` (the golden
//! pins the current form).
//!
//! # Why this probe matters
//!
//! A query expecting `(i32,)` would, without this tier-1 enforcement, silently
//! accept `("hostile",)` and try to write an `&str` body through the int4
//! encoder path — at best a wire-protocol error. Pinning the type-equality at
//! the call boundary closes the per-call arg-shape mismatch class.

use bsql_postgres_proto::engine::{session, Never, Surface, Transport};
use bsql_postgres_proto::{prepared, Credentials, Ident, PreparedQuery};
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

const Q_INT4: PreparedQuery<(i32,), ()> = prepared!("DELETE FROM users WHERE id = $1::int4");

fn main() {
    let user = Ident::try_from_str("verbs").unwrap();
    let _ = session(T0, &user, None, None, Credentials::Trust, |mut e, live| {
        let live = block_on(e.connect(live)).unwrap();
        // P7 attack: bind a `(&str,)` to a query expecting `(i32,)`.
        // Should fail with E0308 — tuple types are nominally distinct.
        let _ = block_on(e.query_params(live, &Q_INT4, ("hostile",), sink));
    });
}
