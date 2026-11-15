//! Mutual authentication and key agreement. Runs on every new connection before any other frame.
//!
//! ```text
//! dialer                                      acceptor
//!   Hello{wire, dialer_id, noise: e}       -->                       in the clear
//!                                          <--  HelloAck{wire, wire_version, acceptor_id, noise: e, ee}
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
//! in the other) and everything else that travels in the clear, outside h: both node ids, the
//! wire versions each side offered and the one chosen. Someone who edits an offer in transit —
//! to push both sides onto an older version — makes the two transcripts differ, and the
//! signatures fail. A node id that is neither listed in the trust store nor shows a valid
//! membership card (`Hello::card`, `HelloAck::card`) is refused no matter what it claims. The
//! card need not be signed again: it binds the id to a key, and the transcript signature proves
//! the peer holds that key.
//!
//! In the clear: wire versions, both node ids and the ephemeral public keys. Everything after
//! `HelloAck`, including errors, is encrypted by `transport::secure`.
//!
//! This exchange is the one part of the protocol every future version must keep as it is:
//! it is how two nodes find out which version to speak.

use snow::HandshakeState;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{ErrorBody, ErrorCode};
use crate::identity::{from_hex, to_hex, NodeIdentity, TrustStore};
use crate::protocol::message::{Frame, Hello, HelloAck, HelloFinish, HelloProof, MemberCard};
use crate::protocol::{NodeId, WireRange};
use crate::transport::framing::{read_frame, write_frame, FrameError};
use crate::transport::secure::{self, Opener, Sealer};

const NOISE_PARAMS: &str = "Noise_NN_25519_ChaChaPoly_SHA256";
/// Each NN handshake message is one 32-byte key plus an empty payload (and a tag on the second).
const NOISE_MESSAGE_MAX: usize = 128;
/// Size limit for every frame until the peer has proven who it is (real ones are under 1 KiB).
/// The node-wide `max_frame_bytes` applies only afterwards: before this limit existed, anyone
/// who could open a connection could make the node buffer a full-size frame per connection.
const HANDSHAKE_FRAME_MAX: usize = 4096;

pub struct HandshakeParams<'a> {
    pub identity: &'a NodeIdentity,
    pub trust: &'a TrustStore,
    /// This node's membership card, shown to the peer (None: rely on being listed by name).
    pub card: Option<&'a MemberCard>,
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
        wire: WireRange::SUPPORTED,
        node_id: own_id.clone(),
        noise: to_hex(&buf[..len]),
        card: params.card.cloned(),
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
    let Some(wire_version) = WireRange::SUPPORTED.highest_common(ack.wire) else {
        return wire.fail(ErrorCode::VersionMismatch, no_common_version(ack.wire)).await;
    };
    if ack.wire_version != wire_version {
        let msg = format!("peer chose wire version {}, expected {wire_version}", ack.wire_version);
        return wire.fail(ErrorCode::HandshakeFailed, msg).await;
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
    if let Err(msg) = params.trust.admit(&ack.node_id, ack.card.as_ref()) {
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
    let transcript = Transcript {
        wire_version,
        dialer: (own_id, WireRange::SUPPORTED),
        acceptor: (&ack.node_id, ack.wire),
        handshake_hash: &hash,
    };
    if !params
        .trust
        .verify(&ack.node_id, &transcript.signed_by("acceptor"), &proof.signature)
    {
        let msg = format!("{} failed to prove its identity", ack.node_id);
        return wire.fail(ErrorCode::HandshakeFailed, msg).await;
    }

    wire.send(Frame::HelloFinish(HelloFinish {
        signature: params.identity.sign(&transcript.signed_by("dialer")),
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
    let Some(wire_version) = WireRange::SUPPORTED.highest_common(hello.wire) else {
        return wire
            .fail(ErrorCode::VersionMismatch, no_common_version(hello.wire))
            .await;
    };
    if &hello.node_id == own_id {
        return wire.fail(ErrorCode::HandshakeFailed, "connected to self".into()).await;
    }
    if let Err(msg) = params.trust.admit(&hello.node_id, hello.card.as_ref()) {
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
        wire: WireRange::SUPPORTED,
        wire_version,
        node_id: own_id.clone(),
        noise: to_hex(&buf[..len]),
        card: params.card.cloned(),
    }))
    .await?;
    let hash = noise.get_handshake_hash().to_vec();
    wire.encrypt(noise)?;

    let transcript = Transcript {
        wire_version,
        dialer: (&hello.node_id, hello.wire),
        acceptor: (own_id, WireRange::SUPPORTED),
        handshake_hash: &hash,
    };
    wire.send(Frame::HelloProof(HelloProof {
        signature: params.identity.sign(&transcript.signed_by("acceptor")),
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
    if !params
        .trust
        .verify(&hello.node_id, &transcript.signed_by("dialer"), &finish.signature)
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

fn no_common_version(peer: WireRange) -> String {
    format!("peer speaks {peer}, this node {}", WireRange::SUPPORTED)
}

/// What both sides sign: everything the handshake agreed on, as each side saw it.
struct Transcript<'a> {
    wire_version: u32,
    dialer: (&'a NodeId, WireRange),
    acceptor: (&'a NodeId, WireRange),
    handshake_hash: &'a [u8],
}

impl Transcript<'_> {
    fn signed_by(&self, role: &str) -> Vec<u8> {
        let (dialer, dialer_wire) = self.dialer;
        let (acceptor, acceptor_wire) = self.acceptor;
        format!(
            "synapta-handshake\n{role}\nwire {}\ndialer {dialer} {} {}\nacceptor {acceptor} {} {}\n{}",
            self.wire_version,
            dialer_wire.min,
            dialer_wire.max,
            acceptor_wire.min,
            acceptor_wire.max,
            to_hex(self.handshake_hash)
        )
        .into_bytes()
    }
}

/// The connection during the handshake: frames in the clear until [`Wire::encrypt`], then
/// sealed. Errors go out the same way as everything else at that point. Frames are held to
/// `HANDSHAKE_FRAME_MAX` throughout; the finished session gets the node's real limit.
struct Wire<'a, S> {
    stream: &'a mut S,
    /// Limit for the session once the handshake has succeeded.
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
        self.keys = Some(secure::channel(transport, HANDSHAKE_FRAME_MAX));
        Ok(())
    }

    fn into_session(self, peer: NodeId) -> Result<Session, ErrorBody> {
        let (mut sealer, mut opener) = self.keys.expect("handshake finishes encrypted");
        sealer.set_max_frame_bytes(self.max_frame_bytes);
        opener.set_max_frame_bytes(self.max_frame_bytes);
        Ok(Session { peer, sealer, opener })
    }

    async fn write(&mut self, frame: &Frame) -> Result<(), FrameError> {
        match &mut self.keys {
            Some((sealer, _)) => sealer.write_frame(self.stream, frame).await,
            None => write_frame(self.stream, frame, HANDSHAKE_FRAME_MAX).await,
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
            None => read_frame(self.stream, HANDSHAKE_FRAME_MAX).await,
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
        Frame::Unknown => "an unknown frame type",
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
            card: None,
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

    /// Hellos as v0.0.1 (a nonce) and v0.0.2 (a Noise message) sent them, with a version string
    /// and no wire range: both are told the versions differ, not that the frame is malformed.
    #[tokio::test]
    async fn hellos_from_older_releases_get_version_mismatch() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let trust = trusting(&[&a]);
        let old_hellos = [
            format!(
                r#"{{"type":"hello","protocol_version":"0.0.1","node_id":"node-a","nonce":"{}"}}"#,
                "ab".repeat(32)
            ),
            format!(
                r#"{{"type":"hello","protocol_version":"0.0.2","node_id":"node-a","noise":"{}"}}"#,
                "ab".repeat(32)
            ),
        ];
        for old_hello in old_hellos {
            let (mut d, mut s) = tokio::io::duplex(8192);
            d.write_all(&(old_hello.len() as u32).to_be_bytes()).await.unwrap();
            d.write_all(old_hello.as_bytes()).await.unwrap();
            let bp = params(&b, &trust);
            let refused = accept(&mut s, &bp).await.err().unwrap();
            assert_eq!(refused.code, ErrorCode::VersionMismatch, "{old_hello}");
            let Frame::Error(told) = read_frame(&mut d, 4096).await.unwrap() else {
                panic!("no error frame for {old_hello}")
            };
            assert_eq!(told.code, ErrorCode::VersionMismatch);
            assert!(told.message.contains("older than v0.0.3"), "{}", told.message);
        }
    }

    /// Someone in the middle edits the dialer's offer, then relays every byte untouched. The edit
    /// changes nothing either side computes (only one version is on offer today), yet it must
    /// fail: the day a range spans two versions, the same edit could force the older one.
    #[tokio::test]
    async fn an_edited_version_offer_is_refused() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (at, bt) = (trusting(&[&b]), trusting(&[&a]));
        let (ap, bp) = (params(&a, &at), params(&b, &bt));
        let (mut to_a, mut a_end) = tokio::io::duplex(8192);
        let (mut to_b, mut b_end) = tokio::io::duplex(8192);
        let mitm = async {
            let Frame::Hello(mut hello) = read_frame(&mut to_a, 4096).await.unwrap() else {
                panic!()
            };
            hello.wire.max += 1;
            write_frame(&mut to_b, &Frame::Hello(hello), 4096).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut to_a, &mut to_b).await;
        };
        let (dialed, accepted) = tokio::select! {
            outcome = async { tokio::join!(dial(&mut a_end, &ap, None), accept(&mut b_end, &bp)) } => outcome,
            _ = mitm => panic!("relay ended first"),
        };
        let refused = dialed.err().unwrap();
        assert_eq!(refused.code, ErrorCode::HandshakeFailed);
        assert!(refused.message.contains("failed to prove"), "{}", refused.message);
        assert!(accepted.is_err());
    }

    /// The acceptor names the version for the connection; naming any other than the highest
    /// both offered is refused.
    #[tokio::test]
    async fn acceptor_choosing_another_version_is_refused() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let at = trusting(&[&b]);
        let ap = params(&a, &at);
        let (mut d, mut s) = tokio::io::duplex(8192);
        let fake_acceptor = async {
            let Frame::Hello(hello) = read_frame(&mut s, 4096).await.unwrap() else {
                panic!()
            };
            let mut noise = noise_builder().build_responder().unwrap();
            read_noise(&mut noise, &hello.noise).unwrap();
            let mut buf = [0u8; NOISE_MESSAGE_MAX];
            let len = noise.write_message(&[], &mut buf).unwrap();
            let ack = HelloAck {
                wire: WireRange { min: 2, max: 3 },
                wire_version: 2,
                node_id: b.identity.node_id.clone(),
                noise: to_hex(&buf[..len]),
                card: None,
            };
            write_frame(&mut s, &Frame::HelloAck(ack), 4096).await.unwrap();
        };
        let (dialed, ()) = tokio::join!(dial(&mut d, &ap, None), fake_acceptor);
        let refused = dialed.err().unwrap();
        assert_eq!(refused.code, ErrorCode::HandshakeFailed);
        assert!(refused.message.contains("chose wire version 2"), "{}", refused.message);
    }

    /// Before the peer is authenticated, the node-wide frame limit (1 MiB here) does not apply.
    #[tokio::test]
    async fn oversized_hello_is_refused_before_it_is_read() {
        let b = peer("node-b");
        let trust = trusting(&[]);
        let (mut d, mut s) = tokio::io::duplex(8192);
        d.write_all(&(HANDSHAKE_FRAME_MAX as u32 + 1).to_be_bytes())
            .await
            .unwrap();
        let bp = HandshakeParams {
            identity: &b.identity,
            trust: &trust,
            card: None,
            max_frame_bytes: 1 << 20,
        };
        assert_eq!(accept(&mut s, &bp).await.err().unwrap().code, ErrorCode::FrameTooLarge);
        assert!(
            matches!(read_frame(&mut d, 4096).await.unwrap(), Frame::Error(e) if e.code == ErrorCode::FrameTooLarge)
        );
    }

    /// The handshake limit is lifted once the peer is authenticated.
    #[tokio::test]
    async fn the_session_gets_the_full_frame_limit() {
        let (a, b) = (peer("node-a"), peer("node-b"));
        let (at, bt) = (trusting(&[&b]), trusting(&[&a]));
        let ap = HandshakeParams {
            identity: &a.identity,
            trust: &at,
            card: None,
            max_frame_bytes: 1 << 20,
        };
        let bp = HandshakeParams {
            identity: &b.identity,
            trust: &bt,
            card: None,
            max_frame_bytes: 1 << 20,
        };
        let (mut d, mut s) = tokio::io::duplex(1 << 16);
        let (dialed, accepted) = tokio::join!(dial(&mut d, &ap, None), accept(&mut s, &bp));
        let (mut dialed, mut accepted) = (dialed.unwrap(), accepted.unwrap());
        let big = Frame::Request(RequestFrame {
            id: 1,
            request: Request::Invoke {
                capability: "system.echo".into(),
                input: serde_json::json!({ "text": "x".repeat(100_000) }),
            },
        });
        let (sent, received) = tokio::join!(
            dialed.sealer.write_frame(&mut d, &big),
            accepted.opener.read_frame(&mut s)
        );
        sent.unwrap();
        assert_eq!(received.unwrap(), big);
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
