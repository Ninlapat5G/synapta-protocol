//! Transport abstraction: how bytes get between two nodes.
//!
//! Everything above this module (handshake, links, routing) only sees a [`Connection`]
//! (an async byte stream) and never a socket type, so a Tailscale transport can be added
//! by implementing [`Transport`] plus an [`AddressPolicy`] variant for the tailnet range —
//! without touching routing or discovery.

pub mod framing;
pub mod tcp;

use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::BoxFuture;

pub trait Connection: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Connection for T {}

pub type BoxedConnection = Box<dyn Connection>;

pub trait Listener: Send {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<(BoxedConnection, SocketAddr)>>;
    fn local_addr(&self) -> SocketAddr;
}

pub trait Transport: Send + Sync + 'static {
    fn dial(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<BoxedConnection>>;
    fn bind(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<Box<dyn Listener>>>;
}

/// Which addresses a node may listen on and dial. Applied to configuration, to every dial,
/// and to addresses learned from peers' adverts — a trusted-but-buggy peer must not be able to
/// make this node connect anywhere outside the policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressPolicy {
    /// v0.0.1 sandbox rule: 127.0.0.0/8 and ::1 only. `0.0.0.0` is always rejected.
    LoopbackOnly,
}

impl AddressPolicy {
    pub fn check(self, addr: &SocketAddr) -> Result<(), String> {
        match self {
            AddressPolicy::LoopbackOnly if addr.ip().is_loopback() => Ok(()),
            AddressPolicy::LoopbackOnly => Err(format!("{addr} is not a loopback address (sandbox is loopback-only)")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_policy() {
        let policy = AddressPolicy::LoopbackOnly;
        for ok in ["127.0.0.1:1", "127.10.0.1:1", "[::1]:1"] {
            assert!(policy.check(&ok.parse().unwrap()).is_ok(), "{ok}");
        }
        for bad in ["0.0.0.0:1", "[::]:1", "192.168.1.5:1", "100.64.0.1:1", "8.8.8.8:53"] {
            assert!(policy.check(&bad.parse().unwrap()).is_err(), "{bad}");
        }
    }
}
