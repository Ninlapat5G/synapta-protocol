//! Mutual authentication handshake. Runs on every new connection before any other frame.
//!
//! ```text
//! dialer                                   acceptor
//!   Hello{version, dialer_id, nonce_d}  -->
//!                                       <--  HelloAck{version, acceptor_id, nonce_a, sig_a}
//!   HelloFinish{sig_d}                  -->
//!                                       <--  Welcome
//! ```
//!
//! Each side signs a transcript containing a role label, both node ids and both fresh nonces.
//! The role label stops a signature from one direction being replayed in the other; the nonces
//! stop replay across connections; the ids bind the proof to this exact pair. A node id that is
//! not in the trust store is refused no matter what it claims.
//!
//! Limitation (documented in docs/protocol.md): this authenticates the peers at connection
//! start but does not encrypt or MAC later frames. On loopback that is acceptable; over a real
//! network it relies on the transport (Tailscale/WireGuard) until v0.0.2 adds a secure channel.

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{ErrorBody, ErrorCode};
use crate::identity::{from_hex, random_bytes, to_hex, NodeIdentity, TrustStore};
use crate::protocol::message::{Frame, Hello, HelloAck, HelloFinish};
use crate::protocol::{is_compatible_version, NodeId, PROTOCOL_VERSION};
use crate::transport::framing::{read_frame, write_frame, FrameError};

pub struct HandshakeParams<'a> {
    pub identity: &'a NodeIdentity,
    pub trust: &'a TrustStore,
    pub max_frame_bytes: usize,
}

/// Dialing side. `expected` is the node id we meant to reach, when known (learned from an
/// advert); a different — even trusted — node answering on that address is refused.
pub async fn dial<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
    expected: Option<&NodeId>,
) -> Result<NodeId, ErrorBody> {
    let own_id = &params.identity.node_id;
    let dialer_nonce = to_hex(&random_bytes::<32>());
    send(
        stream,
        params,
        Frame::Hello(Hello {
            protocol_version: PROTOCOL_VERSION.into(),
            node_id: own_id.clone(),
            nonce: dialer_nonce.clone(),
        }),
    )
    .await?;

    let ack = match receive(stream, params).await? {
        Frame::HelloAck(ack) => ack,
        Frame::Error(error) => return Err(error),
        other => {
            return fail(
                stream,
                params,
                ErrorCode::HandshakeFailed,
                unexpected("HelloAck", &other),
            )
            .await
        }
    };
    if !is_compatible_version(&ack.protocol_version) {
        let msg = format!("peer speaks {}, this node {PROTOCOL_VERSION}", ack.protocol_version);
        return fail(stream, params, ErrorCode::VersionMismatch, msg).await;
    }
    if &ack.node_id == own_id {
        return fail(stream, params, ErrorCode::HandshakeFailed, "connected to self".into()).await;
    }
    if let Some(expected) = expected {
        if &ack.node_id != expected {
            let msg = format!("expected {expected} at this address, found {}", ack.node_id);
            return fail(stream, params, ErrorCode::HandshakeFailed, msg).await;
        }
    }
    if !params.trust.is_trusted(&ack.node_id) {
        let msg = format!("{} is not in the trust configuration", ack.node_id);
        return fail(stream, params, ErrorCode::UntrustedPeer, msg).await;
    }
    if !is_valid_nonce(&ack.nonce) {
        return fail(stream, params, ErrorCode::HandshakeFailed, "invalid nonce".into()).await;
    }
    let acceptor_transcript = transcript("acceptor", own_id, &ack.node_id, &dialer_nonce, &ack.nonce);
    if !params.trust.verify(&ack.node_id, &acceptor_transcript, &ack.signature) {
        let msg = format!("{} failed to prove its identity", ack.node_id);
        return fail(stream, params, ErrorCode::HandshakeFailed, msg).await;
    }

    let dialer_transcript = transcript("dialer", own_id, &ack.node_id, &dialer_nonce, &ack.nonce);
    send(
        stream,
        params,
        Frame::HelloFinish(HelloFinish {
            signature: params.identity.sign(&dialer_transcript),
        }),
    )
    .await?;

    match receive(stream, params).await? {
        Frame::Welcome => Ok(ack.node_id),
        Frame::Error(error) => Err(error),
        other => {
            fail(
                stream,
                params,
                ErrorCode::HandshakeFailed,
                unexpected("Welcome", &other),
            )
            .await
        }
    }
}

/// Accepting side. Returns the authenticated id of the dialer.
pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
) -> Result<NodeId, ErrorBody> {
    let own_id = &params.identity.node_id;
    let hello = match receive(stream, params).await? {
        Frame::Hello(hello) => hello,
        // Anything else — including a perfectly valid request — is refused until authenticated.
        other => {
            let msg = format!("handshake required before {}", frame_kind(&other));
            return fail(stream, params, ErrorCode::NotReady, msg).await;
        }
    };
    if !is_compatible_version(&hello.protocol_version) {
        let msg = format!("peer speaks {}, this node {PROTOCOL_VERSION}", hello.protocol_version);
        return fail(stream, params, ErrorCode::VersionMismatch, msg).await;
    }
    if &hello.node_id == own_id {
        return fail(stream, params, ErrorCode::HandshakeFailed, "connected to self".into()).await;
    }
    if !params.trust.is_trusted(&hello.node_id) {
        let msg = format!("{} is not in the trust configuration", hello.node_id);
        return fail(stream, params, ErrorCode::UntrustedPeer, msg).await;
    }
    if !is_valid_nonce(&hello.nonce) {
        return fail(stream, params, ErrorCode::HandshakeFailed, "invalid nonce".into()).await;
    }

    let acceptor_nonce = to_hex(&random_bytes::<32>());
    let acceptor_transcript = transcript("acceptor", &hello.node_id, own_id, &hello.nonce, &acceptor_nonce);
    send(
        stream,
        params,
        Frame::HelloAck(HelloAck {
            protocol_version: PROTOCOL_VERSION.into(),
            node_id: own_id.clone(),
            nonce: acceptor_nonce.clone(),
            signature: params.identity.sign(&acceptor_transcript),
        }),
    )
    .await?;

    let finish = match receive(stream, params).await? {
        Frame::HelloFinish(finish) => finish,
        other => {
            return fail(
                stream,
                params,
                ErrorCode::HandshakeFailed,
                unexpected("HelloFinish", &other),
            )
            .await
        }
    };
    let dialer_transcript = transcript("dialer", &hello.node_id, own_id, &hello.nonce, &acceptor_nonce);
    if !params
        .trust
        .verify(&hello.node_id, &dialer_transcript, &finish.signature)
    {
        let msg = format!("{} failed to prove its identity", hello.node_id);
        return fail(stream, params, ErrorCode::HandshakeFailed, msg).await;
    }
    send(stream, params, Frame::Welcome).await?;
    Ok(hello.node_id)
}

fn transcript(role: &str, dialer: &NodeId, acceptor: &NodeId, dialer_nonce: &str, acceptor_nonce: &str) -> Vec<u8> {
    format!("synapta-handshake/{PROTOCOL_VERSION}/{role}\n{dialer}\n{acceptor}\n{dialer_nonce}\n{acceptor_nonce}")
        .into_bytes()
}

fn is_valid_nonce(nonce: &str) -> bool {
    nonce.len() == 64 && from_hex(nonce).is_ok()
}

async fn send<S: AsyncWrite + Unpin>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
    frame: Frame,
) -> Result<(), ErrorBody> {
    write_frame(stream, &frame, params.max_frame_bytes)
        .await
        .map_err(|e| ErrorBody::new(ErrorCode::HandshakeFailed, format!("send failed: {e}")))
}

async fn receive<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
) -> Result<Frame, ErrorBody> {
    match read_frame(stream, params.max_frame_bytes).await {
        Ok(frame) => Ok(frame),
        Err(e) => {
            if let Some(body) = e.to_error_body() {
                let _ = write_frame(stream, &Frame::Error(body.clone()), params.max_frame_bytes).await;
                return Err(body);
            }
            let code = if matches!(e, FrameError::Closed) {
                ErrorCode::ConnectionLost
            } else {
                ErrorCode::HandshakeFailed
            };
            Err(ErrorBody::new(code, format!("handshake: {e}")))
        }
    }
}

/// Tells the peer why (best effort) and returns the error.
async fn fail<S: AsyncWrite + Unpin, T>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
    code: ErrorCode,
    message: String,
) -> Result<T, ErrorBody> {
    let body = ErrorBody::new(code, message);
    let _ = write_frame(stream, &Frame::Error(body.clone()), params.max_frame_bytes).await;
    Err(body)
}

fn unexpected(wanted: &str, got: &Frame) -> String {
    format!("expected {wanted}, got {}", frame_kind(got))
}

fn frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::Hello(_) => "hello",
        Frame::HelloAck(_) => "hello_ack",
        Frame::HelloFinish(_) => "hello_finish",
        Frame::Welcome => "welcome",
        Frame::Request(_) => "request",
        Frame::Response(_) => "response",
        Frame::Event(_) => "event",
        Frame::Notify(_) => "notify",
        Frame::Error(_) => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{generate_keypair, parse_signing_key, parse_verifying_key};
    use crate::protocol::message::{Request, RequestFrame};

    struct Peer {
        identity: NodeIdentity,
        public: String,
    }

    fn peer(id: &str) -> Peer {
        let (private, public) = generate_keypair();
        Peer {
            identity: NodeIdentity::new(NodeId::new(id).unwrap(), parse_signing_key(&private).unwrap()),
            public,
        }
    }

    fn trusting(peers: &[&Peer]) -> TrustStore {
        let mut store = TrustStore::default();
        for p in peers {
            store.insert(p.identity.node_id.clone(), parse_verifying_key(&p.public).unwrap());
        }
        store
    }

    async fn run(
        dialer: &Peer,
        dialer_trust: &TrustStore,
        acceptor: &Peer,
        acceptor_trust: &TrustStore,
        expected: Option<&NodeId>,
    ) -> (Result<NodeId, ErrorBody>, Result<NodeId, ErrorBody>) {
        let (mut d, mut a) = tokio::io::duplex(8192);
        let dp = HandshakeParams {
            identity: &dialer.identity,
            trust: dialer_trust,
            max_frame_bytes: 4096,
        };
        let ap = HandshakeParams {
            identity: &acceptor.identity,
            trust: acceptor_trust,
            max_frame_bytes: 4096,
        };
        tokio::join!(dial(&mut d, &dp, expected), accept(&mut a, &ap))
    }

    #[tokio::test]
    async fn mutual_trust_succeeds_and_returns_peer_ids() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (dialed, accepted) = run(&a, &trusting(&[&b]), &b, &trusting(&[&a]), Some(&b.identity.node_id)).await;
        assert_eq!(dialed.unwrap(), b.identity.node_id);
        assert_eq!(accepted.unwrap(), a.identity.node_id);
    }

    #[tokio::test]
    async fn untrusted_dialer_is_refused() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (dialed, accepted) = run(&a, &trusting(&[&b]), &b, &trusting(&[]), None).await;
        assert_eq!(accepted.unwrap_err().code, ErrorCode::UntrustedPeer);
        assert_eq!(dialed.unwrap_err().code, ErrorCode::UntrustedPeer);
    }

    #[tokio::test]
    async fn impostor_with_trusted_name_but_wrong_key_is_refused() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let impostor = Peer {
            identity: NodeIdentity::new(
                a.identity.node_id.clone(),
                parse_signing_key(&generate_keypair().0).unwrap(),
            ),
            public: String::new(),
        };
        let (_, accepted) = run(&impostor, &trusting(&[&b]), &b, &trusting(&[&a]), None).await;
        assert_eq!(accepted.unwrap_err().code, ErrorCode::HandshakeFailed);
    }

    #[tokio::test]
    async fn wrong_node_at_expected_address_is_refused() {
        let (a, b, c) = (peer("node-a"), peer("node-b"), peer("node-c"));
        let (dialed, _) = run(
            &a,
            &trusting(&[&b, &c]),
            &b,
            &trusting(&[&a]),
            Some(&c.identity.node_id),
        )
        .await;
        assert_eq!(dialed.unwrap_err().code, ErrorCode::HandshakeFailed);
    }

    #[tokio::test]
    async fn request_before_handshake_is_refused_with_not_ready() {
        let b = peer("node-b");
        let trust = trusting(&[]);
        let (mut d, mut a) = tokio::io::duplex(8192);
        let params = HandshakeParams {
            identity: &b.identity,
            trust: &trust,
            max_frame_bytes: 4096,
        };
        let early = Frame::Request(RequestFrame {
            id: 1,
            request: Request::Invoke {
                capability: "math.add".into(),
                input: serde_json::json!({}),
            },
        });
        write_frame(&mut d, &early, 4096).await.unwrap();
        assert_eq!(accept(&mut a, &params).await.unwrap_err().code, ErrorCode::NotReady);
        assert!(matches!(read_frame(&mut d, 4096).await.unwrap(), Frame::Error(e) if e.code == ErrorCode::NotReady));
    }
}
