//! DEF-214 SSL negotiation state-machine tests.

#![forbid(unsafe_code)]

use bsql_pg_proto::{PgProtocol, SslClassified};

#[test]
fn push_ssl_request_produces_8_bytes() {
    let proto = PgProtocol::new();
    let (bytes, _ssl_proto) = proto.push_ssl_request();
    assert_eq!(bytes.len(), 8);
    assert_eq!(bytes, &bsql_pg_proto::wire::SSL_REQUEST_WIRE_BYTES);
}

#[test]
fn classify_accepted() {
    let proto = PgProtocol::new();
    let (_bytes, ssl_proto) = proto.push_ssl_request();
    let result = ssl_proto.classify_ssl_response(b'S');
    assert!(matches!(result, SslClassified::Accepted(_)));
}

#[test]
fn classify_refused() {
    let proto = PgProtocol::new();
    let (_bytes, ssl_proto) = proto.push_ssl_request();
    let result = ssl_proto.classify_ssl_response(b'N');
    assert!(matches!(result, SslClassified::Refused(_)));
}

#[test]
fn classify_error_incoming() {
    let proto = PgProtocol::new();
    let (_bytes, ssl_proto) = proto.push_ssl_request();
    let result = ssl_proto.classify_ssl_response(b'E');
    assert!(matches!(result, SslClassified::ErrorIncoming(_)));
}

#[test]
fn classify_invalid_byte() {
    let proto = PgProtocol::new();
    let (_bytes, ssl_proto) = proto.push_ssl_request();
    let result = ssl_proto.classify_ssl_response(0xFF);
    assert!(matches!(result, SslClassified::InvalidByte { byte: 0xFF }));
}

#[test]
fn accepted_then_push_startup_compiles() {
    let proto = PgProtocol::new();
    let (_bytes, ssl_proto) = proto.push_ssl_request();
    let result = ssl_proto.classify_ssl_response(b'S');
    if let SslClassified::Accepted(disconnected) = result {
        let _has_push_startup = disconnected;
    }
}
