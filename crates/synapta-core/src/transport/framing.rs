//! Length-prefixed JSON framing: `u32` big-endian byte length, then the JSON bytes.
//!
//! Used as is only for the first two handshake messages; after that every frame goes through
//! [`super::secure`], which encrypts the same JSON bytes ([`encode`] / [`decode`]).
//!
//! The length is checked against the limit *before* allocating, so a peer announcing a 4 GB
//! frame costs us four bytes of reading, not 4 GB of memory.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{ErrorBody, ErrorCode};
use crate::protocol::message::Frame;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// Peer closed the connection cleanly between frames.
    #[error("connection closed")]
    Closed,
    #[error("frame of {size} bytes exceeds limit of {limit}")]
    TooLarge { size: usize, limit: usize },
    #[error("malformed frame: {0}")]
    Malformed(String),
    /// An encrypted record failed authentication: tampered, replayed, reordered or forged.
    #[error("frame failed authentication")]
    Unauthenticated,
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl FrameError {
    /// The error to send to the peer before closing, if the connection is still writable.
    pub fn to_error_body(&self) -> Option<ErrorBody> {
        match self {
            FrameError::TooLarge { .. } => Some(ErrorBody::new(ErrorCode::FrameTooLarge, self.to_string())),
            FrameError::Malformed(_) => Some(ErrorBody::new(ErrorCode::MalformedFrame, self.to_string())),
            // Nothing is sent back: the channel itself can no longer be trusted.
            FrameError::Closed | FrameError::Io(_) | FrameError::Unauthenticated => None,
        }
    }
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R, max_bytes: usize) -> Result<Frame, FrameError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(FrameError::Closed),
        Err(e) => return Err(FrameError::Io(e)),
    }
    let size = u32::from_be_bytes(len_buf) as usize;
    if size == 0 {
        return Err(FrameError::Malformed("empty frame".into()));
    }
    if size > max_bytes {
        return Err(FrameError::TooLarge { size, limit: max_bytes });
    }
    let mut body = vec![0u8; size];
    reader.read_exact(&mut body).await?;
    decode(&body)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    max_bytes: usize,
) -> Result<(), FrameError> {
    let body = encode(frame, max_bytes)?;
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

/// The JSON bytes of one frame, refused if over the limit.
pub fn encode(frame: &Frame, max_bytes: usize) -> Result<Vec<u8>, FrameError> {
    let body = serde_json::to_vec(frame).map_err(|e| FrameError::Malformed(e.to_string()))?;
    if body.len() > max_bytes {
        return Err(FrameError::TooLarge {
            size: body.len(),
            limit: max_bytes,
        });
    }
    Ok(body)
}

pub fn decode(body: &[u8]) -> Result<Frame, FrameError> {
    serde_json::from_slice(body).map_err(|e| FrameError::Malformed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::{Request, RequestFrame};

    fn ping(id: u64) -> Frame {
        Frame::Request(RequestFrame {
            id,
            request: Request::Ping,
        })
    }

    #[tokio::test]
    async fn several_frames_on_one_stream() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        for id in 1..=3 {
            write_frame(&mut a, &ping(id), 1024).await.unwrap();
        }
        for id in 1..=3 {
            assert_eq!(read_frame(&mut b, 1024).await.unwrap(), ping(id));
        }
        drop(a);
        assert!(matches!(read_frame(&mut b, 1024).await, Err(FrameError::Closed)));
    }

    #[tokio::test]
    async fn oversized_length_is_rejected_without_reading_body() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        assert!(matches!(
            read_frame(&mut b, 1024).await,
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn malformed_json_and_empty_frame_are_errors() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&5u32.to_be_bytes()).await.unwrap();
        a.write_all(b"{oops").await.unwrap();
        assert!(matches!(read_frame(&mut b, 1024).await, Err(FrameError::Malformed(_))));
        a.write_all(&0u32.to_be_bytes()).await.unwrap();
        assert!(matches!(read_frame(&mut b, 1024).await, Err(FrameError::Malformed(_))));
    }

    #[tokio::test]
    async fn writer_refuses_frames_over_limit() {
        let (mut a, _b) = tokio::io::duplex(64);
        let err = write_frame(&mut a, &ping(1), 4).await.unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { .. }));
    }
}
