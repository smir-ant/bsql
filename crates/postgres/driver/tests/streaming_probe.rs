use bsql_postgres_proto::{PgProtocol, WriteBuf, FeedEvent};

#[test]
fn select_1_offline_via_advance_then_iter_rows() {
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
    resp.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    resp.extend_from_slice(&[b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]);
    resp.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    connecting.feed_inbound(&resp).unwrap();
    connecting.feed_bytes(&[], &mut wb);
    let mut active = match connecting.into_active() {
        Ok(a) => a, Err(_) => panic!("into_active failed")
    };

    // Push SimpleQuery SELECT 1
    let q_reply = active.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
    let guard = active.as_ready().unwrap();
    let _actions = guard.push_command(
        bsql_postgres_proto::push_command::SimpleQuery { sql: "SELECT 1", reply: q_reply },
        &mut wb,
    ).unwrap();

    // Server response: RowDescription + DataRow + CommandComplete + RFQ
    let mut srv = Vec::new();
    // RowDescription: 1 column "?column?" int4
    let mut rd_body = Vec::new();
    rd_body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    rd_body.extend_from_slice(b"?column?\0"); // name
    rd_body.extend_from_slice(&0i32.to_be_bytes()); // table_oid
    rd_body.extend_from_slice(&0i16.to_be_bytes()); // attr_num
    rd_body.extend_from_slice(&23i32.to_be_bytes()); // int4 oid
    rd_body.extend_from_slice(&4i16.to_be_bytes()); // type_size
    rd_body.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
    rd_body.extend_from_slice(&0i16.to_be_bytes()); // format = text
    let rd_len = (rd_body.len() as u32).saturating_add(4);
    srv.push(b'T');
    srv.extend_from_slice(&rd_len.to_be_bytes());
    srv.extend_from_slice(&rd_body);

    // DataRow: 1 column, value "1"
    let mut dr_body = Vec::new();
    dr_body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    dr_body.extend_from_slice(&1i32.to_be_bytes()); // col len = 1
    dr_body.push(b'1'); // data
    let dr_len = (dr_body.len() as u32).saturating_add(4);
    srv.push(b'D');
    srv.extend_from_slice(&dr_len.to_be_bytes());
    srv.extend_from_slice(&dr_body);

    // CommandComplete: SELECT 1
    let cc_tag = b"SELECT 1\0";
    let cc_len = (cc_tag.len() as u32).saturating_add(4);
    srv.push(b'C');
    srv.extend_from_slice(&cc_len.to_be_bytes());
    srv.extend_from_slice(cc_tag);

    // RFQ
    srv.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

    eprintln!("total server bytes: {}", srv.len());
    active.feed_inbound(&srv).unwrap();
    wb.clear();

    // Now drive advance_one_frame loop
    for step in 0..30 {
        let event = active.advance_one_frame(&mut wb);
        eprintln!("step {step}: {event:?}, state: {:?}", active.state());
        match event {
            FeedEvent::Idle => {
                eprintln!("IDLE at step {step}");
                if let Some(tag) = active.current_command_tag() {
                    eprintln!("tag: {tag}");
                }
                return;
            }
            FeedEvent::StreamingRows => {
                eprintln!("STREAMING — entering iter_rows");
                active.iter_rows(&mut wb, |stream| {
                    for i in 0..10 {
                        let ev = stream.col_next();
                        eprintln!("  col_next[{i}]: {ev:?}");
                        match ev {
                            bsql_postgres_proto::ColEvent::EndQuery { .. } => return,
                            bsql_postgres_proto::ColEvent::NeedMore => { continue;
                                eprintln!("  NeedMore inside iter_rows!");
                                return;
                            }
                            _ => {}
                        }
                    }
                });
                eprintln!("  post-iter_rows state: {:?}", active.state());
            }
            FeedEvent::NeedMoreBytes => {
                // Check streaming
                if matches!(active.state(),
                    bsql_postgres_proto::ActiveState::SimpleQueryStreamingRows { .. })
                {
                    eprintln!("  NeedMoreBytes but STREAMING — entering iter_rows");
                    active.iter_rows(&mut wb, |stream| {
                        for i in 0..10 {
                            let ev = stream.col_next();
                            eprintln!("    col_next[{i}]: {ev:?}");
                            match ev {
                                bsql_postgres_proto::ColEvent::EndQuery { .. } => return,
                                bsql_postgres_proto::ColEvent::NeedMore => { continue;
                                    eprintln!("    NeedMore!");
                                    return;
                                }
                                _ => {}
                            }
                        }
                    });
                    eprintln!("  post-iter_rows state: {:?}", active.state());
                }
            }
            _ => {}
        }
    }
    panic!("never reached Idle");
}
