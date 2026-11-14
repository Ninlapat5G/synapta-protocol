//! Minimal client for a node's local API, used by the `synapta` CLI, the sandbox launcher and
//! end-to-end tests. One request per connection, matching the server.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

/// Where a running node's local API is, as written by `synapta-node` into its data dir.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeInfo {
    pub node_id: String,
    pub listen: SocketAddr,
    pub api: Option<SocketAddr>,
    pub pid: u32,
}

pub const RUNTIME_FILE: &str = "runtime.json";
pub const TOKEN_FILE: &str = "api-token";

#[derive(Clone)]
pub struct ApiClient {
    addr: SocketAddr,
    token: String,
    timeout: Duration,
}

impl ApiClient {
    pub fn new(addr: SocketAddr, token: impl Into<String>) -> Self {
        Self {
            addr,
            token: token.into(),
            timeout: Duration::from_secs(30),
        }
    }

    /// Reads the API address and token a node wrote into its data dir.
    pub fn from_data_dir(dir: &Path) -> Result<Self, String> {
        let runtime = std::fs::read_to_string(dir.join(RUNTIME_FILE)).map_err(|e| {
            format!(
                "cannot read {}: {e} (is the node running?)",
                dir.join(RUNTIME_FILE).display()
            )
        })?;
        let runtime: RuntimeInfo = serde_json::from_str(&runtime).map_err(|e| format!("invalid runtime.json: {e}"))?;
        let api = runtime.api.ok_or("this node has no [api] section enabled")?;
        let token = std::fs::read_to_string(dir.join(TOKEN_FILE)).map_err(|e| format!("cannot read api token: {e}"))?;
        Ok(Self::new(api, token.trim()))
    }

    pub async fn get(&self, path: &str) -> Result<(u16, Value), String> {
        self.send("GET", path, None).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<(u16, Value), String> {
        self.send("POST", path, Some(body)).await
    }

    async fn send(&self, method: &str, path: &str, body: Option<&Value>) -> Result<(u16, Value), String> {
        tokio::time::timeout(self.timeout, self.exchange(method, path, body))
            .await
            .map_err(|_| format!("{method} {path}: timed out"))?
    }

    async fn exchange(&self, method: &str, path: &str, body: Option<&Value>) -> Result<(u16, Value), String> {
        let body = body.map(|b| b.to_string()).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            self.token,
            body.len()
        );
        let mut stream = TcpStream::connect(self.addr)
            .await
            .map_err(|e| format!("connect {}: {e}", self.addr))?;
        stream.write_all(request.as_bytes()).await.map_err(|e| e.to_string())?;
        let mut raw = Vec::new();
        stream
            .take(MAX_RESPONSE_BYTES)
            .read_to_end(&mut raw)
            .await
            .map_err(|e| e.to_string())?;

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut headers);
        let head_len = match response.parse(&raw).map_err(|e| format!("bad response: {e}"))? {
            httparse::Status::Complete(len) => len,
            httparse::Status::Partial => return Err("incomplete response".into()),
        };
        let status = response.code.unwrap_or(0);
        let value = serde_json::from_slice(&raw[head_len..]).map_err(|e| format!("response is not JSON: {e}"))?;
        Ok((status, value))
    }
}
