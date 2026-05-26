use bsql_pg_proto::{PgProtocol, WriteBuf};

#[test]
fn trust_handshake_offline() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply = proto.next_reply_id::<bsql_pg_proto::reply_id::StartupKind>();
    let user = bsql_pg_proto::Ident::try_from_str("test").unwrap();

    let (_actions, mut connecting) = proto
        .push_startup(
            user,
            None,
            None,
            bsql_pg_proto::password::Credentials::Trust,
            reply,
            &mut wb,
        )
        .unwrap();

    let mut server_response = Vec::new();
    // AuthenticationOk
    server_response.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
    // BackendKeyData: pid=1, secret=2
    server_response.extend_from_slice(&[b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]);
    // ReadyForQuery: 'I'
    server_response.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

    connecting.feed_inbound(&server_response).unwrap();
    let out = connecting.feed_bytes(&[], &mut wb);
    eprintln!("actions: {}", out.len());

    match connecting.into_active() {
        Ok(active) => {
            eprintln!("into_active OK! state: {:?}", active.state());
        }
        Err(bsql_pg_proto::IntoActiveError::StillConnecting(c)) => {
            eprintln!("StillConnecting, state: {:?}", c.state());
            panic!("expected Active, got StillConnecting");
        }
        Err(bsql_pg_proto::IntoActiveError::Closed(_)) => {
            panic!("unexpected Closed");
        }
    }
}
