//! Encrypted framing: every frame after the handshake's first two messages.
//!
//! On the wire a frame is one or more records. A record is a `u16` big-endian length, then that
//! many bytes of Noise transport ciphertext (ChaCha20-Poly1305, 16-byte tag). It decrypts to a
//! flag byte (`1` = last record of this frame, `0` = more follow) and a piece of the frame's JSON.
//! Noise caps a message at 65 535 bytes, so a frame of up to `max_frame_bytes` spans several.
//!
//! Each direction numbers its records 0, 1, 2… and that number is the nonce. A record that was
//! altered, replayed, dropped, reordered or forged fails to decrypt: [`FrameError::Unauthenticated`],
//! and the caller closes the connection. Nothing is skipped or retransmitted.
//!
//! Not cancel-safe: if a `read_frame` future is dropped half way, the connection must be closed
//! (every caller already does — a cancelled read means shutdown or idle timeout).

use std::io;
use std::sync::Arc;

use snow::StatelessTransportState;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::framing::{self, FrameError};
use crate::protocol::message::Frame;

/// Largest Noise message (snow's `MAXMSGLEN`).
const MAX_RECORD: usize = 65_535;
const TAG: usize = 16;
const MAX_CHUNK: usize = MAX_RECORD - TAG - 1;
const MORE: u8 = 0;
const LAST: u8 = 1;

/// The two directions of one encrypted connection, so reading and writing can run in
/// separate tasks.
pub fn channel(transport: StatelessTransportState, max_frame_bytes: usize) -> (Sealer, Opener) {
    let cipher = Arc::new(transport);
    let sealer = Sealer {
        cipher: cipher.clone(),
        next_nonce: 0,
        max_frame_bytes,
    };
    let opener = Opener {
        cipher,
        next_nonce: 0,
        max_frame_bytes,
        sealed: vec![0; MAX_RECORD],
        plain: vec![0; MAX_RECORD],
    };
    (sealer, opener)
}

/// Write side.
pub struct Sealer {
    cipher: Arc<StatelessTransportState>,
    next_nonce: u64,
    max_frame_bytes: usize,
}

impl Sealer {
    pub async fn write_frame<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        frame: &Frame,
    ) -> Result<(), FrameError> {
        let body = framing::encode(frame, self.max_frame_bytes)?;
        // All records of the frame go out in one write.
        let mut out = Vec::with_capacity(body.len() + (body.len() / MAX_CHUNK + 1) * (2 + TAG + 1));
        let mut plain = Vec::with_capacity(MAX_CHUNK + 1);
        let mut chunks = body.chunks(MAX_CHUNK).peekable();
        while let Some(chunk) = chunks.next() {
            plain.clear();
            plain.push(if chunks.peek().is_some() { MORE } else { LAST });
            plain.extend_from_slice(chunk);
            let start = out.len();
            out.resize(start + 2 + plain.len() + TAG, 0);
            let len = self
                .cipher
                .write_message(self.next_nonce, &plain, &mut out[start + 2..])
                .map_err(|e| FrameError::Io(io::Error::other(format!("encrypt: {e}"))))?;
            self.next_nonce += 1;
            out[start..start + 2].copy_from_slice(&(len as u16).to_be_bytes());
            out.truncate(start + 2 + len);
        }
        writer.write_all(&out).await?;
        writer.flush().await?;
        Ok(())
    }
}

/// Read side.
pub struct Opener {
    cipher: Arc<StatelessTransportState>,
    next_nonce: u64,
    max_frame_bytes: usize,
    sealed: Vec<u8>,
    plain: Vec<u8>,
}

impl Opener {
    pub async fn read_frame<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> Result<Frame, FrameError> {
        let mut body = Vec::new();
        loop {
            let mut len = [0u8; 2];
            match reader.read_exact(&mut len).await {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && body.is_empty() => {
                    return Err(FrameError::Closed)
                }
                Err(e) => return Err(FrameError::Io(e)),
            }
            let len = usize::from(u16::from_be_bytes(len));
            if len <= TAG {
                // Too short to hold a tag and the flag byte: cannot be a record we sent.
                return Err(FrameError::Unauthenticated);
            }
            reader.read_exact(&mut self.sealed[..len]).await?;
            let n = self
                .cipher
                .read_message(self.next_nonce, &self.sealed[..len], &mut self.plain)
                .map_err(|_| FrameError::Unauthenticated)?;
            self.next_nonce += 1;

            let (&flag, chunk) = self.plain[..n].split_first().ok_or(FrameError::Unauthenticated)?;
            let size = body.len() + chunk.len();
            if size > self.max_frame_bytes {
                return Err(FrameError::TooLarge {
                    size,
                    limit: self.max_frame_bytes,
                });
            }
            body.extend_from_slice(chunk);
            match flag {
                LAST => return framing::decode(&body),
                MORE if !chunk.is_empty() => {}
                _ => return Err(FrameError::Malformed("invalid record".into())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::{Request, RequestFrame};

    /// Both ends of a finished Noise NN handshake, without any of the Synapta handshake.
    fn pair(max_frame_bytes: usize) -> ((Sealer, Opener), (Sealer, Opener)) {
        pair_with_limits(max_frame_bytes, max_frame_bytes)
    }

    fn pair_with_limits(initiator_max: usize, responder_max: usize) -> ((Sealer, Opener), (Sealer, Opener)) {
        let params: snow::params::NoiseParams = "Noise_NN_25519_ChaChaPoly_SHA256".parse().unwrap();
        let mut initiator = snow::Builder::new(params.clone()).build_initiator().unwrap();
        let mut responder = snow::Builder::new(params).build_responder().unwrap();
        let mut buf = [0u8; 256];
        let mut payload = [0u8; 256];
        let n = initiator.write_message(&[], &mut buf).unwrap();
        responder.read_message(&buf[..n], &mut payload).unwrap();
        let n = responder.write_message(&[], &mut buf).unwrap();
        initiator.read_message(&buf[..n], &mut payload).unwrap();
        (
            channel(initiator.into_stateless_transport_mode().unwrap(), initiator_max),
            channel(responder.into_stateless_transport_mode().unwrap(), responder_max),
        )
    }

    fn echo(text: String) -> Frame {
        Frame::Request(RequestFrame {
            id: 7,
            request: Request::Invoke {
                capability: "system.echo".into(),
                input: serde_json::json!({ "text": text }),
            },
        })
    }

    async fn sealed_bytes(sealer: &mut Sealer, frame: &Frame) -> Vec<u8> {
        let mut wire = Vec::new();
        sealer.write_frame(&mut wire, frame).await.unwrap();
        wire
    }

    #[tokio::test]
    async fn frames_round_trip_including_ones_spanning_many_records() {
        let ((mut sealer, _), (_, mut opener)) = pair(1 << 20);
        let small = echo("hi".into());
        let large = echo("x".repeat(200_000));
        let mut wire = sealed_bytes(&mut sealer, &small).await;
        wire.extend(sealed_bytes(&mut sealer, &large).await);
        let mut reader = wire.as_slice();
        assert_eq!(opener.read_frame(&mut reader).await.unwrap(), small);
        assert_eq!(opener.read_frame(&mut reader).await.unwrap(), large);
        assert!(matches!(opener.read_frame(&mut reader).await, Err(FrameError::Closed)));
    }

    #[tokio::test]
    async fn plaintext_never_appears_on_the_wire() {
        let ((mut sealer, _), _) = pair(1 << 20);
        let wire = sealed_bytes(&mut sealer, &echo("very-secret-marker".into())).await;
        for needle in [&b"very-secret-marker"[..], b"system.echo", b"invoke"] {
            assert!(!wire.windows(needle.len()).any(|w| w == needle));
        }
    }

    #[tokio::test]
    async fn any_flipped_byte_is_rejected() {
        let ((mut sealer, _), (_, mut opener)) = pair(1 << 20);
        let frame = echo("hello".into());
        let wire = sealed_bytes(&mut sealer, &frame).await;
        // Every byte after the length prefix. A failed record does not advance the nonce, so the
        // same opener is reused, and the untouched record still opens at the end.
        for i in 2..wire.len() {
            let mut tampered = wire.clone();
            tampered[i] ^= 0x01;
            let result = opener.read_frame(&mut tampered.as_slice()).await;
            assert!(
                matches!(result, Err(FrameError::Unauthenticated)),
                "byte {i}: {result:?}"
            );
        }
        assert_eq!(opener.read_frame(&mut wire.as_slice()).await.unwrap(), frame);
    }

    #[tokio::test]
    async fn replayed_and_reordered_records_are_rejected() {
        let ((mut sealer, _), (_, mut opener)) = pair(1 << 20);
        let (first, second) = (echo("one".into()), echo("two".into()));
        let first_wire = sealed_bytes(&mut sealer, &first).await;
        let second_wire = sealed_bytes(&mut sealer, &second).await;

        let reordered = opener.read_frame(&mut second_wire.as_slice()).await;
        assert!(matches!(reordered, Err(FrameError::Unauthenticated)));
        assert_eq!(opener.read_frame(&mut first_wire.as_slice()).await.unwrap(), first);
        let replayed = opener.read_frame(&mut first_wire.as_slice()).await;
        assert!(matches!(replayed, Err(FrameError::Unauthenticated)));
    }

    #[tokio::test]
    async fn record_from_another_session_is_rejected() {
        let ((mut sealer, _), _) = pair(1 << 20);
        let (_, (_, mut stranger)) = pair(1 << 20);
        let wire = sealed_bytes(&mut sealer, &echo("hello".into())).await;
        let result = stranger.read_frame(&mut wire.as_slice()).await;
        assert!(matches!(result, Err(FrameError::Unauthenticated)));
    }

    #[tokio::test]
    async fn size_limit_holds_on_both_sides() {
        let big = echo("x".repeat(5_000));
        let ((mut sealer, _), (_, mut opener)) = pair_with_limits(1 << 20, 1024);
        let wire = sealed_bytes(&mut sealer, &big).await;
        let result = opener.read_frame(&mut wire.as_slice()).await;
        assert!(matches!(result, Err(FrameError::TooLarge { .. })), "{result:?}");

        let ((mut limited, _), _) = pair(1024);
        let result = limited.write_frame(&mut Vec::new(), &big).await;
        assert!(matches!(result, Err(FrameError::TooLarge { .. })), "{result:?}");
    }
}
