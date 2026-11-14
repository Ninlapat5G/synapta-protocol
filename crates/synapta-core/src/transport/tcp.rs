//! Plain TCP transport. Every dial and bind passes through the [`AddressPolicy`] first.

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use super::{AddressPolicy, BoxedConnection, Listener, Transport};
use crate::BoxFuture;

pub struct TcpTransport {
    policy: AddressPolicy,
    connect_timeout: Duration,
}

impl TcpTransport {
    pub fn new(policy: AddressPolicy, connect_timeout: Duration) -> Self {
        Self {
            policy,
            connect_timeout,
        }
    }

    fn check(&self, addr: &SocketAddr) -> io::Result<()> {
        self.policy
            .check(addr)
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))
    }
}

impl Transport for TcpTransport {
    fn dial(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<BoxedConnection>> {
        Box::pin(async move {
            self.check(&addr)?;
            let stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, format!("connect to {addr} timed out")))??;
            stream.set_nodelay(true)?;
            Ok(Box::new(stream) as BoxedConnection)
        })
    }

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<Box<dyn Listener>>> {
        Box::pin(async move {
            self.check(&addr)?;
            let listener = TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            // Defence in depth: re-check what the OS actually bound to.
            self.check(&local_addr)?;
            Ok(Box::new(TcpListenerHandle { listener, local_addr }) as Box<dyn Listener>)
        })
    }
}

struct TcpListenerHandle {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl Listener for TcpListenerHandle {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<(BoxedConnection, SocketAddr)>> {
        Box::pin(async move {
            let (stream, peer) = self.listener.accept().await?;
            stream.set_nodelay(true)?;
            Ok((Box::new(stream) as BoxedConnection, peer))
        })
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refuses_to_bind_or_dial_outside_policy() {
        let transport = TcpTransport::new(AddressPolicy::LoopbackOnly, Duration::from_secs(1));
        let err = transport.bind("0.0.0.0:0".parse().unwrap()).await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        let err = transport.dial("10.0.0.1:47322".parse().unwrap()).await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn binds_loopback_with_ephemeral_port() {
        let transport = TcpTransport::new(AddressPolicy::LoopbackOnly, Duration::from_secs(1));
        let listener = transport.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(listener.local_addr().ip().is_loopback());
        assert_ne!(listener.local_addr().port(), 0);
    }
}
