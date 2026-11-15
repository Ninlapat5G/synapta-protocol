//! Mutual authentication and key agreement. Runs on every new connection before any other frame.
//!
//! ```text
//! dialer                                      acceptor
//!   Hello{version, dialer_id, noise: e}    -->                       in the clear
//!                                          <--  HelloAck{version, acceptor_id, noise: e, ee}
//!   ─── Noise NN done: both hold the session keys and the handshake hash h ───
//!                                          <--  HelloProof{sig_a}    encrypted from here on
//!   HelloFinish{sig_d}                     -->
//!                                          <--  Welcome
//! ```
//!
//! Noise NN (`Noise_NN_25519_ChaChaPoly_SHA256`, via the `snow` crate) gives fresh X25519 keys
//! per connection, so recorded traffic stays unreadable even if a node key leaks later. NN by
//! itself proves nothing about who is on the other end; that comes from each side signing the
//! handshake hash h with its Ed25519 node key (the same keys and trust list as v0.0.1). h covers
//! both ephemeral keys, so a man in the middle — who must run a separate Noise session with each
//! side — ends up with two different h values and cannot pass either side's signature on.
//!
//! The signed transcript also names the role (a signature from one direction cannot be replayed
//! in the other) and both node ids (the ids travel in the clear, outside h). A node id that is
//! not in the trust store is refused no matter what it claims.
//!
//! In the clear: protocol version, both node ids and the ephemeral public keys. Everything after
//! `HelloAck`, including errors, is encrypted by `transport::secure`.

use snow::HandshakeState;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{ErrorBody, ErrorCode};
use crate::identity::{from_hex, to_hex, NodeIdentity, TrustStore};
use crate::protocol::message::{Frame, Hello, HelloAck, HelloFinish, HelloProof};
use crate::protocol::{is_compatible_version, NodeId, PROTOCOL_VERSION};
use crate::transport::framing::{read_frame, write_frame, FrameError};
use crate::transport::secure::{self, Opener, Sealer};

const NOISE_PARAMS: &str = "Noise_NN_25519_ChaChaPoly_SHA256";
/// Each NN handshake message is one 32-byte key plus an empty payload (and a tag on the second).
const NOISE_MESSAGE_MAX: usize = 128;

pub struct HandshakeParams<'a> {
    pub identity: &'a NodeIdentity,
    pub trust: &'a TrustStore,
    pub max_frame_bytes: usize,
}

/// An authenticated, encrypted connection: who the peer is, and the keys to talk to it.
pub struct Session {
    pub peer: NodeId,
    pub sealer: Sealer,
    pub opener: Opener,
}

/// Dialing side. `expected` is the node id we meant to reach, when known (learned from an
/// advert); a different — even trusted — node answering on that address is refused.
pub async fn dial<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
    expected: Option<&NodeId>,
) -> Result<Session, ErrorBody> {
    let own_id = &params.identity.node_id;
    let mut wire = Wire::new(stream, params.max_frame_bytes);
    let mut noise = noise_builder().build_initiator().map_err(noise_error)?;
    let mut buf = [0u8; NOISE_MESSAGE_MAX];
    let len = noise.write_message(&[], &mut buf).map_err(noise_error)?;
    wire.send(Frame::Hello(Hello {
        protocol_version: PROTOCOL_VERSION.into(),
        node_id: own_id.clone(),
        noise: to_hex(&buf[..len]),
    }))
    .await?;

    let ack = match wire.receive().await? {
        Frame::HelloAck(ack) => ack,
        Frame::Error(error) => return Err(error),
        other => {
            return wire
                .fail(ErrorCode::HandshakeFailed, unexpected("HelloAck", &other))
                .await
        }
    };
    if !is_compatible_version(&ack.protocol_version) {
        let msg = format!("peer speaks {}, this node {PROTOCOL_VERSION}", ack.protocol_version);
        return wire.fail(ErrorCode::VersionMismatch, msg).await;
    }
    if &ack.node_id == own_id {
        return wire.fail(ErrorCode::HandshakeFailed, "connected to self".into()).await;
    }
    if let Some(expected) = expected {
        if &ack.node_id != expected {
            let msg = format!("expected {expected} at this address, found {}", ack.node_id);
            return wire.fail(ErrorCode::HandshakeFailed, msg).await;
        }
    }
    if !params.trust.is_trusted(&ack.node_id) {
        let msg = format!("{} is not in the trust configuration", ack.node_id);
        return wire.fail(ErrorCode::UntrustedPeer, msg).await;
    }
    if read_noise(&mut noise, &ack.noise).is_err() {
        return wire
            .fail(ErrorCode::HandshakeFailed, "invalid key exchange message".into())
            .await;
    }
    let hash = noise.get_handshake_hash().to_vec();
    wire.encrypt(noise)?;

    let proof = match wire.receive().await? {
        Frame::HelloProof(proof) => proof,
        Frame::Error(error) => return Err(error),
        other => {
            return wire
                .fail(ErrorCode::HandshakeFailed, unexpected("HelloProof", &other))
                .await
        }
    };
    let acceptor_transcript = transcript("acceptor", own_id, &ack.node_id, &hash);
    if !params
        .trust
        .verify(&ack.node_id, &acceptor_transcript, &proof.signature)
    {
        let msg = format!("{} failed to prove its identity", ack.node_id);
        return wire.fail(ErrorCode::HandshakeFailed, msg).await;
    }

    let dialer_transcript = transcript("dialer", own_id, &ack.node_id, &hash);
    wire.send(Frame::HelloFinish(HelloFinish {
        signature: params.identity.sign(&dialer_transcript),
    }))
    .await?;

    match wire.receive().await? {
        Frame::Welcome => wire.into_session(ack.node_id),
        Frame::Error(error) => Err(error),
        other => {
            wire.fail(ErrorCode::HandshakeFailed, unexpected("Welcome", &other))
                .await
        }
    }
}

/// Accepting side. Returns the authenticated dialer.
pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    params: &HandshakeParams<'_>,
) -> Result<Session, ErrorBody> {
    let own_id = &params.identity.node_id;
    let mut wire = Wire::new(stream, params.max_frame_bytes);
    let hello = match wire.receive().await? {
        Frame::Hello(hello) => hello,
        // Anything else — including a perfectly valid request — is refused until authenticated.
        other => {
            let msg = format!("handshake required before {}", frame_kind(&other));
            return wire.fail(ErrorCode::NotReady, msg).await;
        }
    };
    if !is_compatible_version(&hello.protocol_version) {
        let msg = format!("peer speaks {}, this node {PROTOCOL_VERSION}", hello.protocol_version);
        return wire.fail(ErrorCode::VersionMismatch, msg).await;
    }
    if &hello.node_id == own_id {
        return wire.fail(ErrorCode::HandshakeFailed, "connected to self".into()).await;
    }
    if !params.trust.is_trusted(&hello.node_id) {
        let msg = format!("{} is not in the trust configuration", hello.node_id);
        return wire.fail(ErrorCode::UntrustedPeer, msg).await;
    }
    let mut noise = noise_builder().build_responder().map_err(noise_error)?;
    if read_noise(&mut noise, &hello.noise).is_err() {
        return wire
            .fail(ErrorCode::HandshakeFailed, "invalid key exchange message".into())
            .await;
    }
    let mut buf = [0u8; NOISE_MESSAGE_MAX];
    let len = noise.write_message(&[], &mut buf).map_err(noise_error)?;
    wire.send(Frame::HelloAck(HelloAck {
        protocol_version: PROTOCOL_VERSION.into(),
        node_id: own_id.clone(),
        noise: to_hex(&buf[..len]),
    }))
    .await?;
    let hash = noise.get_handshake_hash().to_vec();
    wire.encrypt(noise)?;

    let acceptor_transcript = transcript("acceptor", &hello.node_id, own_id, &hash);
    wire.send(Frame::HelloProof(HelloProof {
        signature: params.identity.sign(&acceptor_transcript),
    }))
    .await?;

    let finish = match wire.receive().await? {
        Frame::HelloFinish(finish) => finish,
        other => {
            return wire
                .fail(ErrorCode::HandshakeFailed, unexpected("HelloFinish", &other))
                .await
        }
    };
    let dialer_transcript = transcript("dialer", &hello.node_id, own_id, &hash);
    if !params
        .trust
        .verify(&hello.node_id, &dialer_transcript, &finish.signature)
    {
        let msg = format!("{} failed to prove its identity", hello.node_id);
        return wire.fail(ErrorCode::HandshakeFailed, msg).await;
    }
    wire.send(Frame::Welcome).await?;
    wire.into_session(hello.node_id)
}

fn noise_builder() -> snow::Builder<'static> {
    // The prologue is mixed into h: both sides must agree on it, or every signature fails.
    const PROLOGUE: &[u8] = b"synapta-protocol";
    snow::Builder::new(NOISE_PARAMS.parse().expect("valid noise parameters"))
        .prologue(PROLOGUE)
        .expect("prologue set once")
}

fn read_noise(noise: &mut HandshakeState, message_hex: &str) -> Result<(), ()> {
    let message = from_hex(message_hex).map_err(|_| ())?;
    if message.len() > NOISE_MESSAGE_MAX {
        return Err(());
    }
    let mut payload = [0u8; NOISE_MESSAGE_MAX];
    noise.read_message(&message, &mut payload).map_err(|_| ())?;
    Ok(())
}

fn noise_error(e: snow::Error) -> ErrorBody {
    ErrorBody::new(ErrorCode::HandshakeFailed, format!("key exchange: {e}"))
}

fn transcript(role: &str, dialer: &NodeId, acceptor: &NodeId, handshake_hash: &[u8]) -> Vec<u8> {
    let hash = to_hex(handshake_hash);
    format!("synapta-handshake/{PROTOCOL_VERSION}/{role}\n{dialer}\n{acceptor}\n{hash}").into_bytes()
}

/// The connection during the handshake: frames in the clear until [`Wire::encrypt`], then
/// sealed. Errors go out the same way as everything else at that point.
struct Wire<'a, S> {
    stream: &'a mut S,
    max_frame_bytes: usize,
    keys: Option<(Sealer, Opener)>,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> Wire<'a, S> {
    fn new(stream: &'a mut S, max_frame_bytes: usize) -> Self {
        Self {
            stream,
            max_frame_bytes,
            keys: None,
        }
    }

    fn encrypt(&mut self, noise: HandshakeState) -> Result<(), ErrorBody> {
        let transport = noise.into_stateless_transport_mode().map_err(noise_error)?;
        self.keys = Some(secure::channel(transport, self.max_frame_bytes));
        Ok(())
    }

    fn into_session(self, peer: NodeId) -> Result<Session, ErrorBody> {
        let (sealer, opener) = self.keys.expect("handshake finishes encrypted");
        Ok(Session { peer, sealer, opener })
    }

    async fn write(&mut self, frame: &Frame) -> Result<(), FrameError> {
        match &mut self.keys {
            Some((sealer, _)) => sealer.write_frame(self.stream, frame).await,
            None => write_frame(self.stream, frame, self.max_frame_bytes).await,
        }
    }

    async fn send(&mut self, frame: Frame) -> Result<(), ErrorBody> {
        self.write(&frame)
            .await
            .map_err(|e| ErrorBody::new(ErrorCode::HandshakeFailed, format!("send failed: {e}")))
    }

    async fn receive(&mut self) -> Result<Frame, ErrorBody> {
        let read = match &mut self.keys {
            Some((_, opener)) => opener.read_frame(self.stream).await,
            None => read_frame(self.stream, self.max_frame_bytes).await,
        };
        match read {
            Ok(frame) => Ok(frame),
            Err(e) => {
                if let Some(body) = e.to_error_body() {
                    let _ = self.write(&Frame::Error(body.clone())).await;
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
    async fn fail<T>(&mut self, code: ErrorCode, message: String) -> Result<T, ErrorBody> {
        let body = ErrorBody::new(code, message);
        let _ = self.write(&Frame::Error(body.clone())).await;
        Err(body)
    }
}

fn unexpected(wanted: &str, got: &Frame) -> String {
    format!("expected {wanted}, got {}", frame_kind(got))
}

fn frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::Hello(_) => "hello",
        Frame::HelloAck(_) => "hello_ack",
        Frame::HelloProof(_) => "hello_proof",
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
    use tokio::io::AsyncWriteExt;

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

    fn params<'a>(peer: &'a Peer, trust: &'a TrustStore) -> HandshakeParams<'a> {
        HandshakeParams {
            identity: &peer.identity,
            trust,
            max_frame_bytes: 4096,
        }
    }

    async fn run(
        dialer: &Peer,
        dialer_trust: &TrustStore,
        acceptor: &Peer,
        acceptor_trust: &TrustStore,
        expected: Option<&NodeId>,
    ) -> (Result<Session, ErrorBody>, Result<Session, ErrorBody>) {
        let (mut d, mut a) = tokio::io::duplex(8192);
        let (dp, ap) = (params(dialer, dialer_trust), params(acceptor, acceptor_trust));
        tokio::join!(dial(&mut d, &dp, expected), accept(&mut a, &ap))
    }

    fn ping(id: u64) -> Frame {
        Frame::Request(RequestFrame {
            id,
            request: Request::Ping,
        })
    }

    #[tokio::test]
    async fn mutual_trust_succeeds_and_both_sides_share_keys() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (at, bt) = (trusting(&[&b]), trusting(&[&a]));
        let (ap, bp) = (params(&a, &at), params(&b, &bt));
        let (mut d, mut s) = tokio::io::duplex(8192);
        let (dialed, accepted) = tokio::join!(dial(&mut d, &ap, Some(&b.identity.node_id)), accept(&mut s, &bp));
        let (mut dialed, mut accepted) = (dialed.unwrap(), accepted.unwrap());
        assert_eq!(dialed.peer, b.identity.node_id);
        assert_eq!(accepted.peer, a.identity.node_id);
        // Frames flow both ways over the keys the handshake agreed on.
        dialed.sealer.write_frame(&mut d, &ping(1)).await.unwrap();
        assert_eq!(accepted.opener.read_frame(&mut s).await.unwrap(), ping(1));
        accepted.sealer.write_frame(&mut s, &ping(2)).await.unwrap();
        assert_eq!(dialed.opener.read_frame(&mut d).await.unwrap(), ping(2));
    }

    #[tokio::test]
    async fn untrusted_dialer_is_refused() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (dialed, accepted) = run(&a, &trusting(&[&b]), &b, &trusting(&[]), None).await;
        assert_eq!(accepted.err().unwrap().code, ErrorCode::UntrustedPeer);
        assert_eq!(dialed.err().unwrap().code, ErrorCode::UntrustedPeer);
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
        assert_eq!(accepted.err().unwrap().code, ErrorCode::HandshakeFailed);
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
        assert_eq!(dialed.err().unwrap().code, ErrorCode::HandshakeFailed);
    }

    #[tokio::test]
    async fn request_before_handshake_is_refused_with_not_ready() {
        let b = peer("node-b");
        let trust = trusting(&[]);
        let (mut d, mut a) = tokio::io::duplex(8192);
        let early = Frame::Request(RequestFrame {
            id: 1,
            request: Request::Invoke {
                capability: "math.add".into(),
                input: serde_json::json!({}),
            },
        });
        write_frame(&mut d, &early, 4096).await.unwrap();
        let bp = params(&b, &trust);
        assert_eq!(accept(&mut a, &bp).await.err().unwrap().code, ErrorCode::NotReady);
        assert!(matches!(read_frame(&mut d, 4096).await.unwrap(), Frame::Error(e) if e.code == ErrorCode::NotReady));
    }

    /// A v0.0.1 node's hello (nonce instead of a Noise message) is told the versions differ.
    #[tokio::test]
    async fn v001_hello_gets_version_mismatch() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let trust = trusting(&[&a]);
        let (mut d, mut s) = tokio::io::duplex(8192);
        let old_hello = format!(
            r#"{{"type":"hello","protocol_version":"0.0.1","node_id":"node-a","nonce":"{}"}}"#,
            "ab".repeat(32)
        );
        d.write_all(&(old_hello.len() as u32).to_be_bytes()).await.unwrap();
        d.write_all(old_hello.as_bytes()).await.unwrap();
        let bp = params(&b, &trust);
        let refused = accept(&mut s, &bp).await.err().unwrap();
        assert_eq!(refused.code, ErrorCode::VersionMismatch);
        assert!(
            matches!(read_frame(&mut d, 4096).await.unwrap(), Frame::Error(e) if e.code == ErrorCode::VersionMismatch)
        );
    }

    /// Someone between a and b who holds no trusted key but relays everything, running its own
    /// Noise session with each side (the only way to read the traffic). Both node keys still
    /// sign, but each over its own session's h, so the relayed proof does not verify.
    #[tokio::test]
    async fn relaying_man_in_the_middle_is_refused() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (at, bt) = (trusting(&[&b]), trusting(&[&a]));
        let (ap, bp) = (params(&a, &at), params(&b, &bt));
        let (mut to_a, mut a_end) = tokio::io::duplex(8192);
        let (mut to_b, mut b_end) = tokio::io::duplex(8192);

        let mitm = async {
            // Toward a: pretend to be the acceptor.
            let Frame::Hello(hello) = read_frame(&mut to_a, 4096).await.unwrap() else {
                panic!()
            };
            let mut toward_a = noise_builder().build_responder().unwrap();
            read_noise(&mut toward_a, &hello.noise).unwrap();
            // Toward b: pretend to be the dialer, under a's name.
            let mut toward_b = noise_builder().build_initiator().unwrap();
            let mut buf = [0u8; NOISE_MESSAGE_MAX];
            let len = toward_b.write_message(&[], &mut buf).unwrap();
            let forged = Hello {
                noise: to_hex(&buf[..len]),
                ..hello
            };
            write_frame(&mut to_b, &Frame::Hello(forged), 4096).await.unwrap();
            let Frame::HelloAck(ack) = read_frame(&mut to_b, 4096).await.unwrap() else {
                panic!()
            };
            read_noise(&mut toward_b, &ack.noise).unwrap();
            let len = toward_a.write_message(&[], &mut buf).unwrap();
            let relayed_ack = HelloAck {
                noise: to_hex(&buf[..len]),
                ..ack
            };
            write_frame(&mut to_a, &Frame::HelloAck(relayed_ack), 4096)
                .await
                .unwrap();

            // Both sessions are up; pass b's proof on to a.
            let (_, mut from_b) = secure::channel(toward_b.into_stateless_transport_mode().unwrap(), 4096);
            let (mut to_a_sealer, _) = secure::channel(toward_a.into_stateless_transport_mode().unwrap(), 4096);
            let proof = from_b.read_frame(&mut to_b).await.unwrap();
            assert!(
                matches!(proof, Frame::HelloProof(_)),
                "the MITM can read b's proof: {proof:?}"
            );
            to_a_sealer.write_frame(&mut to_a, &proof).await.unwrap();
        };
        let (dialed, accepted, ()) = tokio::join!(
            dial(&mut a_end, &ap, Some(&b.identity.node_id)),
            async {
                // b waits for a HelloFinish that never comes; give up once a has refused.
                let r = tokio::time::timeout(std::time::Duration::from_millis(500), accept(&mut b_end, &bp)).await;
                r.ok()
            },
            mitm
        );
        let refused = dialed.err().unwrap();
        assert_eq!(refused.code, ErrorCode::HandshakeFailed);
        assert!(refused.message.contains("failed to prove"), "{}", refused.message);
        assert!(
            !matches!(accepted, Some(Ok(_))),
            "b must not accept the relayed session"
        );
    }
}
