// EXPECT: compiles + runs — the sanctioned path. A single linear token threads
// through two SEQUENTIAL async verbs inside one `async` scope that holds
// `&mut engine` across both `await`s (the R4 self-referential-async shape), and
// is consumed at the end without escaping the scope. The verbs are real
// (`connect` then `ping`) driven over a scripted trust-handshake server.
use bsql_postgres_proto::engine::{session, EngineError, Never, Surface, Transport};
use bsql_postgres_proto::wire::{TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_READY_FOR_QUERY};
use bsql_postgres_proto::{Credentials, Ident};
use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

/// Static scripted server: replies a trust handshake then one extra
/// `ReadyForQuery` for the ping's `Sync`.
struct ScriptServer {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Transport for ScriptServer {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        let end = self.cursor + n;
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
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

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&u32::try_from(body.len() + 4).unwrap().to_be_bytes());
    out.extend_from_slice(body);
    out
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

fn drop_sink(_surface: Surface<'_>) -> ControlFlow<Never> {
    ControlFlow::Continue(())
}

fn main() {
    let user = Ident::try_from_str("brand").unwrap();
    let mut key_body = 4321_i32.to_be_bytes().to_vec();
    key_body.extend_from_slice(&8765_i32.to_be_bytes());
    let mut inbound = Vec::new();
    inbound.extend_from_slice(&frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()));
    inbound.extend_from_slice(&frame(TAG_BACKEND_KEY_DATA.byte(), &key_body));
    inbound.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    inbound.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    let server = ScriptServer { inbound, cursor: 0 };

    let ok = session(server, &user, None, &[], Credentials::Trust, |mut e, live| {
        block_on(async move {
            let live = e.connect(live).await?;
            let live = e.ping(live, drop_sink).await?.live;
            let _ = live;
            Ok::<(), EngineError<Infallible>>(())
        })
        .is_ok()
    })
    .unwrap();
    assert!(ok);
}
