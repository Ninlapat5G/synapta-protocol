//! Minimal HTTP/1.1 server for the node's local API. One request per connection
//! (`Connection: close`), `Content-Length` bodies only, strict size and time limits.
//!
//! Guards, applied to every request before any route runs:
//! - bound to loopback only;
//! - `Host` must be this listener's loopback address — blocks DNS-rebinding, where a web page
//!   in the user's browser resolves its own hostname to 127.0.0.1 and calls this API;
//! - `Authorization: Bearer <token>` with the token from the node's data dir.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::BoxFuture;

pub const MAX_HEAD_BYTES: usize = 16 * 1024;
pub const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_HEADERS: usize = 32;
const MAX_CONNECTIONS: usize = 64;
const READ_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Request {
    pub method: String,
    /// Path without the query string, e.g. `/tools/math.add`.
    pub path: String,
    pub query: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    pub fn segments(&self) -> Vec<&str> {
        self.path.split('/').filter(|s| !s.is_empty()).collect()
    }
}

pub struct Response {
    pub status: u16,
    pub body: Value,
}

impl Response {
    pub fn json(status: u16, body: Value) -> Self {
        Self { status, body }
    }
}

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: Request) -> BoxFuture<'_, Response>;
}

/// Accepts connections until `shutdown` resolves.
pub async fn serve(listener: TcpListener, token: String, handler: Arc<dyn Handler>, shutdown: BoxFuture<'static, ()>) {
    let local = match listener.local_addr() {
        Ok(addr) => addr,
        Err(e) => {
            tracing::error!(error = %e, "gateway listener has no address");
            return;
        }
    };
    let token: Arc<str> = token.into();
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    tokio::pin!(shutdown);
    loop {
        let accepted = tokio::select! {
            _ = &mut shutdown => return,
            accepted = listener.accept() => accepted,
        };
        let (stream, _) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "gateway accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue; // over the connection limit: drop it
        };
        let (token, handler) = (token.clone(), handler.clone());
        tokio::spawn(async move {
            serve_one(stream, local, &token, handler.as_ref()).await;
            drop(permit);
        });
    }
}

async fn serve_one(mut stream: TcpStream, local: SocketAddr, token: &str, handler: &dyn Handler) {
    let response = match tokio::time::timeout(READ_TIMEOUT, read_request(&mut stream, local, token)).await {
        Err(_) => error(408, "request timeout"),
        Ok(Err(response)) => response,
        Ok(Ok(request)) => handler.handle(request).await,
    };
    let _ = write_response(&mut stream, &response).await;
}

/// Reads and checks one request. `Err` carries the response to send instead.
async fn read_request(stream: &mut TcpStream, local: SocketAddr, token: &str) -> Result<Request, Response> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let (head_len, method, target, headers) = loop {
        let n = stream.read(&mut chunk).await.map_err(|_| error(400, "read failed"))?;
        if n == 0 {
            return Err(error(400, "incomplete request"));
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut header_slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Request::new(&mut header_slots);
        match parsed.parse(&buf) {
            Ok(httparse::Status::Complete(len)) => {
                let headers: Vec<(String, String)> = parsed
                    .headers
                    .iter()
                    .map(|h| {
                        (
                            h.name.to_ascii_lowercase(),
                            String::from_utf8_lossy(h.value).into_owned(),
                        )
                    })
                    .collect();
                let method = parsed.method.unwrap_or_default().to_string();
                let target = parsed.path.unwrap_or_default().to_string();
                break (len, method, target, headers);
            }
            Ok(httparse::Status::Partial) if buf.len() < MAX_HEAD_BYTES => continue,
            Ok(httparse::Status::Partial) => return Err(error(431, "request head too large")),
            Err(_) => return Err(error(400, "malformed request")),
        }
    };
    let header = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());

    if !host_is_allowed(header("host"), local) {
        return Err(error(403, "Host header must be this node's loopback address"));
    }
    let presented = header("authorization")
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !constant_time_eq(presented.as_bytes(), token.as_bytes()) {
        return Err(error(401, "missing or invalid bearer token"));
    }
    if header("transfer-encoding").is_some() {
        return Err(error(501, "chunked bodies are not supported; send Content-Length"));
    }
    let content_length: usize = match header("content-length") {
        None => 0,
        Some(v) => v.trim().parse().map_err(|_| error(400, "invalid Content-Length"))?,
    };
    if content_length > MAX_BODY_BYTES {
        return Err(error(413, "body too large"));
    }
    let mut body = buf[head_len..].to_vec();
    if body.len() > content_length {
        return Err(error(400, "body longer than Content-Length"));
    }
    if body.len() < content_length {
        let start = body.len();
        body.resize(content_length, 0);
        stream
            .read_exact(&mut body[start..])
            .await
            .map_err(|_| error(400, "incomplete body"))?;
    }

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), parse_query(query)),
        None => (target, Vec::new()),
    };
    Ok(Request {
        method,
        path,
        query,
        body,
    })
}

fn host_is_allowed(host: Option<&str>, local: SocketAddr) -> bool {
    let Some(host) = host else { return false };
    let port = local.port();
    [
        format!("127.0.0.1:{port}"),
        format!("localhost:{port}"),
        format!("[::1]:{port}"),
    ]
    .iter()
    .any(|allowed| allowed.eq_ignore_ascii_case(host.trim()))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%')
            .then(|| bytes.get(i + 1..i + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match (bytes[i], escaped) {
            (_, Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (b'+', None) => {
                out.push(b' ');
                i += 1;
            }
            (byte, None) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn error(status: u16, message: &str) -> Response {
    Response::json(
        status,
        serde_json::json!({ "error": { "code": "http", "message": message } }),
    )
}

async fn write_response(stream: &mut TcpStream, response: &Response) -> std::io::Result<()> {
    let body = serde_json::to_vec(&response.body).unwrap_or_else(|_| b"{}".to_vec());
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        response.status,
        reason(response.status),
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Internal Server Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_check_accepts_only_this_loopback_listener() {
        let local: SocketAddr = "127.0.0.1:48321".parse().unwrap();
        assert!(host_is_allowed(Some("127.0.0.1:48321"), local));
        assert!(host_is_allowed(Some("localhost:48321"), local));
        assert!(!host_is_allowed(Some("evil.example:48321"), local));
        assert!(!host_is_allowed(Some("127.0.0.1:9999"), local));
        assert!(!host_is_allowed(None, local));
    }

    #[test]
    fn query_parsing_and_decoding() {
        assert_eq!(
            parse_query("target=node-c&x=a%2Fb+c"),
            vec![("target".into(), "node-c".into()), ("x".into(), "a/b c".into())]
        );
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }

    #[test]
    fn token_comparison() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
