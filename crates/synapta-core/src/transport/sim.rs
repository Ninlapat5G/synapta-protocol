//! Simulated network for running many nodes in one process (synapta-lab, tests): real TCP on
//! loopback, real handshakes and encryption — only *who can reach whom* is simulated.
//!
//! Nodes join named rings. Two nodes can connect only if they share a ring, the way two
//! machines can only talk when they are on the same network. A node in several rings bridges
//! them. Changing a node's rings takes effect at once on the connections already open: those
//! between nodes that no longer share a ring go silent (writes are dropped, reads never return),
//! the way a pulled cable behaves, so the nodes find out through their own heartbeats and
//! timeouts rather than from a clean close.
//!
//! Each node should listen on its own loopback address (127.x.y.z). Its dials leave from that
//! address too: with every node on 127.0.0.1, a big ring would exhaust the ephemeral ports.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

use super::{AddressPolicy, BoxedConnection, Listener, Transport};
use crate::protocol::NodeId;
use crate::BoxFuture;

/// Shared map of rings, nodes and open connections. Hand each node its own
/// [`SimNetwork::transport`].
#[derive(Default)]
pub struct SimNetwork {
    state: Mutex<NetState>,
    next_conn: AtomicU64,
}

#[derive(Default)]
struct NetState {
    rings: HashMap<NodeId, BTreeSet<String>>,
    /// Which node listens where (filled when a node binds).
    listeners: HashMap<SocketAddr, NodeId>,
    conns: HashMap<u64, OpenConn>,
}

struct OpenConn {
    from: NodeId,
    to: NodeId,
    cut: Arc<AtomicBool>,
}

impl NetState {
    fn share_ring(&self, a: &NodeId, b: &NodeId) -> bool {
        match (self.rings.get(a), self.rings.get(b)) {
            (Some(a), Some(b)) => !a.is_disjoint(b),
            _ => false,
        }
    }

    /// Silences every open connection whose ends no longer share a ring.
    fn cut_unreachable(&mut self) {
        let cut: Vec<&Arc<AtomicBool>> = self
            .conns
            .values()
            .filter(|c| !self.share_ring(&c.from, &c.to))
            .map(|c| &c.cut)
            .collect();
        for flag in cut {
            flag.store(true, Ordering::Relaxed);
        }
    }
}

impl SimNetwork {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The transport for `node`. It listens on, and dials from, `ip` (a loopback address).
    /// Dials to a node it shares no ring with fail after `connect_timeout`, like an unreachable
    /// host.
    pub fn transport(self: &Arc<Self>, node: NodeId, ip: IpAddr, connect_timeout: Duration) -> Arc<dyn Transport> {
        Arc::new(SimTransport {
            network: self.clone(),
            node,
            ip,
            connect_timeout,
        })
    }

    /// Puts `node` in exactly these rings (none = cut off from everyone).
    pub fn set_rings<S: Into<String>>(&self, node: &NodeId, rings: impl IntoIterator<Item = S>) {
        let mut state = self.state.lock().unwrap();
        state
            .rings
            .insert(node.clone(), rings.into_iter().map(Into::into).collect());
        state.cut_unreachable();
    }

    /// Forgets `node` (it was removed): it reaches nobody and its listen address is freed.
    pub fn remove(&self, node: &NodeId) {
        let mut state = self.state.lock().unwrap();
        state.rings.remove(node);
        state.listeners.retain(|_, owner| owner != node);
        state.cut_unreachable();
    }

    pub fn rings_of(&self, node: &NodeId) -> BTreeSet<String> {
        self.state.lock().unwrap().rings.get(node).cloned().unwrap_or_default()
    }

    pub fn can_reach(&self, a: &NodeId, b: &NodeId) -> bool {
        self.state.lock().unwrap().share_ring(a, b)
    }

    /// Connections currently open from one node to another (silenced ones included until the
    /// node notices and closes them).
    pub fn open_connections(&self) -> Vec<(NodeId, NodeId)> {
        let state = self.state.lock().unwrap();
        state.conns.values().map(|c| (c.from.clone(), c.to.clone())).collect()
    }

    fn open(self: &Arc<Self>, from: &NodeId, to: &NodeId) -> (u64, Arc<AtomicBool>) {
        let id = self.next_conn.fetch_add(1, Ordering::Relaxed);
        let cut = Arc::new(AtomicBool::new(false));
        self.state.lock().unwrap().conns.insert(
            id,
            OpenConn {
                from: from.clone(),
                to: to.clone(),
                cut: cut.clone(),
            },
        );
        (id, cut)
    }
}

struct SimTransport {
    network: Arc<SimNetwork>,
    node: NodeId,
    ip: IpAddr,
    connect_timeout: Duration,
}

impl SimTransport {
    fn check(&self, addr: &SocketAddr) -> io::Result<()> {
        AddressPolicy::LoopbackOnly
            .check(addr)
            .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))
    }

    async fn unreachable(&self, addr: SocketAddr) -> io::Error {
        tokio::time::sleep(self.connect_timeout).await;
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("connect to {addr} timed out (no shared ring)"),
        )
    }
}

impl Transport for SimTransport {
    fn dial(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<BoxedConnection>> {
        Box::pin(async move {
            self.check(&addr)?;
            let target = {
                let state = self.network.state.lock().unwrap();
                state
                    .listeners
                    .get(&addr)
                    .filter(|target| state.share_ring(&self.node, target))
                    .cloned()
            };
            let Some(target) = target else {
                return Err(self.unreachable(addr).await);
            };
            let socket = match addr {
                SocketAddr::V4(_) => TcpSocket::new_v4()?,
                SocketAddr::V6(_) => TcpSocket::new_v6()?,
            };
            socket.bind(SocketAddr::new(self.ip, 0))?;
            let stream = tokio::time::timeout(self.connect_timeout, socket.connect(addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, format!("connect to {addr} timed out")))??;
            stream.set_nodelay(true)?;
            let (id, cut) = self.network.open(&self.node, &target);
            Ok(Box::new(SimStream {
                inner: stream,
                cut,
                id,
                network: Arc::downgrade(&self.network),
            }) as BoxedConnection)
        })
    }

    fn bind(&self, addr: SocketAddr) -> BoxFuture<'_, io::Result<Box<dyn Listener>>> {
        Box::pin(async move {
            self.check(&addr)?;
            let listener = TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            self.check(&local_addr)?;
            self.network
                .state
                .lock()
                .unwrap()
                .listeners
                .insert(local_addr, self.node.clone());
            Ok(Box::new(SimListener { listener, local_addr }) as Box<dyn Listener>)
        })
    }
}

struct SimListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl Listener for SimListener {
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

/// The dialling end of a connection. Once cut it swallows writes and never completes a read,
/// so neither end receives anything more. Every connection is dialled through some node's
/// transport, so silencing this end silences the pair.
struct SimStream {
    inner: TcpStream,
    cut: Arc<AtomicBool>,
    id: u64,
    network: Weak<SimNetwork>,
}

impl SimStream {
    fn is_cut(&self) -> bool {
        self.cut.load(Ordering::Relaxed)
    }
}

impl AsyncRead for SimStream {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.is_cut() {
            // No waker is registered: this read stays pending for as long as the stream lives.
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for SimStream {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if self.is_cut() {
            return Poll::Ready(Ok(buf.len()));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.is_cut() {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.is_cut() {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Drop for SimStream {
    fn drop(&mut self) {
        if let Some(network) = self.network.upgrade() {
            network.state.lock().unwrap().conns.remove(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn id(s: &str) -> NodeId {
        NodeId::new(s).unwrap()
    }

    const LOCALHOST: &str = "127.0.0.1";

    async fn listening(network: &Arc<SimNetwork>, node: &str) -> (Arc<dyn Transport>, Box<dyn Listener>) {
        let transport = network.transport(id(node), LOCALHOST.parse().unwrap(), Duration::from_millis(200));
        let listener = transport.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        (transport, listener)
    }

    #[tokio::test]
    async fn only_nodes_sharing_a_ring_connect() {
        let network = SimNetwork::new();
        network.set_rings(&id("a"), ["one"]);
        network.set_rings(&id("b"), ["one", "two"]);
        network.set_rings(&id("c"), ["two"]);
        let (a, _) = listening(&network, "a").await;
        let (_, mut b_listener) = listening(&network, "b").await;
        let (_, c_listener) = listening(&network, "c").await;

        let b_addr = b_listener.local_addr();
        let accept = tokio::spawn(async move { b_listener.accept().await.map(|_| ()) });
        assert!(a.dial(b_addr).await.is_ok());
        accept.await.unwrap().unwrap();
        let err = a.dial(c_listener.local_addr()).await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(!network.can_reach(&id("a"), &id("c")));
    }

    #[tokio::test]
    async fn leaving_the_ring_silences_open_connections() {
        let network = SimNetwork::new();
        network.set_rings(&id("a"), ["one"]);
        network.set_rings(&id("b"), ["one"]);
        let (a, _) = listening(&network, "a").await;
        let (_, mut b_listener) = listening(&network, "b").await;
        let addr = b_listener.local_addr();
        let accepted = tokio::spawn(async move { b_listener.accept().await.unwrap().0 });
        let mut dialled = a.dial(addr).await.unwrap();
        let mut served = accepted.await.unwrap();

        dialled.write_all(b"hi").await.unwrap();
        let mut buf = [0u8; 2];
        served.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hi");
        assert_eq!(network.open_connections(), vec![(id("a"), id("b"))]);

        network.set_rings(&id("a"), ["elsewhere"]);
        // Nothing gets through in either direction, and nobody is told.
        dialled.write_all(b"lost").await.unwrap();
        served.write_all(b"lost").await.unwrap();
        let silent = Duration::from_millis(200);
        assert!(tokio::time::timeout(silent, served.read(&mut buf)).await.is_err());
        assert!(tokio::time::timeout(silent, dialled.read(&mut buf)).await.is_err());

        drop(dialled);
        assert!(network.open_connections().is_empty());
    }

    #[tokio::test]
    async fn dials_leave_from_the_nodes_own_address() {
        let network = SimNetwork::new();
        network.set_rings(&id("a"), ["one"]);
        network.set_rings(&id("b"), ["one"]);
        let a = network.transport(id("a"), "127.0.0.7".parse().unwrap(), Duration::from_millis(200));
        let (_, mut b_listener) = listening(&network, "b").await;
        let addr = b_listener.local_addr();
        let accepted = tokio::spawn(async move { b_listener.accept().await.unwrap().1 });
        let _conn = a.dial(addr).await.unwrap();
        assert_eq!(accepted.await.unwrap().ip(), "127.0.0.7".parse::<IpAddr>().unwrap());
    }
}
