use bsql_postgres_proto::{PgProtocol, WriteBuf, FeedEvent};

#[test]
fn parse_then_bind_execute_offline() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Handshake
    let reply = proto.next_reply_id::<bsql_postgres_proto::reply_id::StartupKind>();
    let user = bsql_postgres_proto::Ident::try_from_str("test").unwrap();
    let (_a, mut c) = proto.push_startup(user, None, None,
        bsql_postgres_proto::password::Credentials::Trust, reply, &mut wb).unwrap();
    let mut resp = Vec::new();
    resp.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    resp.extend_from_slice(&[b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]);
    resp.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    c.feed_inbound(&resp).unwrap();
    c.feed_bytes(&[], &mut wb);
    let mut active = match c.into_active() { Ok(a) => a, Err(_) => panic!("fail") };

    // Parse
    let pr = active.next_reply_id::<bsql_postgres_proto::reply_id::ParseKind>();
    let guard = active.as_ready().unwrap();
    let actions = guard.push_command(
        bsql_postgres_proto::push_command::Parse {
            stmt_name: bsql_postgres_proto::StmtName::default(),
            sql: "INSERT INTO t VALUES ($1)",
            reply: pr,
        }, &mut wb).unwrap();
    eprintln!("parse actions: {}", actions.len());

    // Simulate ParseComplete + RFQ
    let mut srv = Vec::new();
    srv.extend_from_slice(&[b'1', 0, 0, 0, 4]); // ParseComplete
    srv.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']); // RFQ
    active.feed_inbound(&srv).unwrap();
    wb.clear();

    for step in 0..10 {
        let ev = active.advance_one_frame(&mut wb);
        eprintln!("parse step {step}: {ev:?}, state: {:?}", active.state());
        if matches!(ev, FeedEvent::Idle) { break; }
    }
    eprintln!("state after parse drain: {:?}", active.state());

    // Bind + Execute
    let qr = active.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
    let guard = active.as_ready().unwrap();
    let portal = bsql_postgres_proto::PortalName::default();
    let stmt = bsql_postgres_proto::StmtName::default();
    let actions = guard.push_bind_execute(
        &portal, &stmt, &(42i32,), None,
        bsql_postgres_proto::FetchRows::All, qr, &mut wb).unwrap();
    eprintln!("bind_execute actions: {}", actions.len());

    // Simulate BindComplete + CommandComplete + RFQ
    let mut srv2 = Vec::new();
    srv2.extend_from_slice(&[b'2', 0, 0, 0, 4]); // BindComplete
    let cc = b"INSERT 0 1\0";
    let cc_len = (cc.len() as u32).saturating_add(4);
    srv2.push(b'C');
    srv2.extend_from_slice(&cc_len.to_be_bytes());
    srv2.extend_from_slice(cc);
    srv2.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    active.feed_inbound(&srv2).unwrap();
    wb.clear();

    for step in 0..10 {
        let ev = active.advance_one_frame(&mut wb);
        eprintln!("be step {step}: {ev:?}, state: {:?}", active.state());
        if matches!(ev, FeedEvent::Idle) {
            if let Some(tag) = active.current_command_tag() {
                eprintln!("tag: {tag}");
            }
            return;
        }
    }
    panic!("never idle");
}
