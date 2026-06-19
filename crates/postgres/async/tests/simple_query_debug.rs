//! Debug probe for simple_query hang.

use bsql_postgres_proto::{Action, FeedEvent, PgProtocol, Reply, WriteBuf};

#[test]
fn simple_query_offline_create_table() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Handshake
    let reply = proto.next_reply_id::<bsql_postgres_proto::reply_id::StartupKind>();
    let user = bsql_postgres_proto::Ident::try_from_str("test").unwrap();
    let (_actions, mut connecting) = proto
        .push_startup(user, None, None,
            bsql_postgres_proto::password::Credentials::Trust, reply, &mut wb)
        .unwrap();

    let mut resp = Vec::new();
    resp.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]); // AuthOk
    resp.extend_from_slice(&[b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]); // BackendKeyData
    resp.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']); // RFQ
    connecting.feed_inbound(&resp).unwrap();
    // The empty-input drain completes the handshake: it emits EXACTLY ONE
    // action, the StartupComplete delivery (the precondition `into_active`
    // checks). The single-element slice pattern enforces both at once —
    // length 1 AND the variant — so a drain that regressed to emit any
    // spurious extra action would fail here. This also honours
    // `feed_bytes`'s must-use contract and proves the terminal transition
    // actually fired.
    let drain = connecting.feed_bytes(&[], &mut wb);
    assert!(
        matches!(
            drain.as_slice(),
            [Action::DeliverReply { value: Reply::StartupComplete(_), .. }]
        ),
        "handshake drain must emit exactly one action: StartupComplete delivery"
    );
    let mut active = match connecting.into_active() {
        Ok(a) => a, Err(_) => panic!("into_active failed")
    };

    // Push SimpleQuery
    let q_reply = active.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
    let guard = active.as_ready().unwrap();
    let actions = guard.push_command(
        bsql_postgres_proto::push_command::SimpleQuery::new("CREATE TEMP TABLE x(i int)", q_reply),
        &mut wb,
    ).unwrap();

    eprintln!("push actions: {}", actions.len());
    for (i, a) in actions.as_slice().iter().enumerate() {
        eprintln!("  action[{i}]: {a:?}");
    }

    // Simulate server response: CommandComplete + RFQ
    let mut server = Vec::new();
    // CommandComplete: 'C' + len + "CREATE TABLE\0"
    let tag = b"CREATE TABLE\0";
    let cc_len = (tag.len() as u32).saturating_add(4);
    server.push(b'C');
    server.extend_from_slice(&cc_len.to_be_bytes());
    server.extend_from_slice(tag);
    // RFQ
    server.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

    eprintln!("server response: {} bytes", server.len());
    active.feed_inbound(&server).unwrap();

    // Drive advance_one_frame loop
    for step in 0..20 {
        let event = active.advance_one_frame(&mut wb);
        eprintln!("step {step}: event={event:?}, state={:?}", active.state());
        match event {
            FeedEvent::Idle => {
                eprintln!("IDLE reached at step {step}");
                return;
            }
            FeedEvent::Deliver(id, reply) => {
                eprintln!("  Deliver: id={id}, reply={reply:?}");
            }
            FeedEvent::NeedMoreBytes => {
                eprintln!("  NeedMoreBytes — buffer should have data!");
            }
            _ => {}
        }
    }
    panic!("never reached Idle after 20 steps");
}
