use bsql_postgres_proto::{PgProtocol, WriteBuf, FeedEvent};

#[test]
fn ping_offline() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply = proto.next_reply_id::<bsql_postgres_proto::reply_id::StartupKind>();
    let user = bsql_postgres_proto::Ident::try_from_str("test").unwrap();

    let (_actions, mut connecting) = proto
        .push_startup(user, None, None,
            bsql_postgres_proto::password::Credentials::Trust,
            reply, &mut wb)
        .unwrap();

    let mut resp = Vec::new();
    resp.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    resp.extend_from_slice(&[b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]);
    resp.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

    connecting.feed_inbound(&resp).unwrap();
    connecting.feed_bytes(&[], &mut wb);
    let mut active = match connecting.into_active() { Ok(a) => a, Err(_) => panic!("into_active failed") };

    // Now ping
    let ping_reply = active.next_reply_id::<bsql_postgres_proto::reply_id::PingKind>();
    let guard = active.as_ready().unwrap();
    guard.push_command(
        bsql_postgres_proto::push_command::Ping { reply: ping_reply },
        &mut wb,
    ).unwrap();

    eprintln!("wb after push: {} bytes", wb.as_bytes().len());
    eprintln!("state after push: {:?}", active.state());

    // Feed RFQ (Pong response)
    let rfq = [b'Z', 0, 0, 0, 5, b'I'];
    active.feed_inbound(&rfq).unwrap();

    let event = active.advance_one_frame(&mut wb);
    eprintln!("event: {:?}", event);
    assert!(matches!(event, FeedEvent::Deliver(_, _)));

    let event2 = active.advance_one_frame(&mut wb);
    eprintln!("event2: {:?}", event2);
    assert!(matches!(event2, FeedEvent::Idle));
}
