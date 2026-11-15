//! A small HTTP/1.1 server for the lab page: one request per connection, `Content-Length`
//! bodies only.
//!
//! Separate from the node gateway's server on purpose: that one guards a real node (token,
//! loopback-only Host). The lab is a simulator the owner opens over loopback or Tailscale, so it
//! has no login (decided in docs/web-sandbox-plan.md). One guard stays because it costs nothing:
//! every request that changes something must be `Content-Type: application/json`. A browser
//! will not send that cross-site without asking first (CORS preflight, which this server never
//! answers), so some other web page open in the same browser cannot drive the lab.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const MAX_HEAD_BYTES: usize = 16 * 1024;
/// A saved topology of a few thousand nodes fits.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_HEADERS: usize = 48;
const MAX_CONNECTIONS: usize = 256;
const READ_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Request {
    pub method: String,
    /// Without the query string.
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
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(status: u16, body: &Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec()),
        }
    }

    pub fn error(status: u16, message: impl Into<String>) -> Self {
        Self::json(status, &serde_json::json!({ "error": message.into() }))
    }

    pub fn file(content_type: &'static str, body: &'static [u8]) -> Self {
        Self {
            status: 200,
            content_type,
            body: body.to_vec(),
        }
    }
}

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: Request) -> impl std::future::Future<Output = Response> + Send;
}

/// Accepts connections on `listener` until `shutdown` is cancelled.
pub async fn serve<H: Handler>(listener: TcpListener, handler: Arc<H>, shutdown: CancellationToken) {
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => return,
            accepted = listener.accept() => accepted,
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::warn!(error = %e, "lab accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue; // over the connection limit: drop it
        };
        let handler = handler.clone();
        tokio::spawn(async move {
            serve_one(stream, handler.as_ref()).await;
            drop(permit);
        });
    }
}

async fn serve_one<H: Handler>(mut stream: TcpStream, handler: &H) {
    let response = match tokio::time::timeout(READ_TIMEOUT, read_request(&mut stream)).await {
        Err(_) => Response::error(408, "request timeout"),
        Ok(Err(response)) => response,
        Ok(Ok(request)) => handler.handle(request).await,
    };
    let _ = write_response(&mut stream, &response).await;
}

/// Reads one request. `Err` carries the response to send instead.
async fn read_request(stream: &mut TcpStream) -> Result<Request, Response> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let (head_len, method, target, headers) = loop {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|_| Response::error(400, "read failed"))?;
        if n == 0 {
            return Err(Response::error(400, "incomplete request"));
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Request::new(&mut slots);
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
            Ok(httparse::Status::Partial) => return Err(Response::error(431, "request head too large")),
            Err(_) => return Err(Response::error(400, "malformed request")),
        }
    };
    let header = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());

    let changes_something = !matches!(method.as_str(), "GET" | "HEAD");
    let is_json = header("content-type").is_some_and(|v| v.trim_start().starts_with("application/json"));
    if changes_something && !is_json {
        return Err(Response::error(415, "send Content-Type: application/json"));
    }
    if header("transfer-encoding").is_some() {
        return Err(Response::error(
            501,
            "chunked bodies are not supported; send Content-Length",
        ));
    }
    let content_length: usize = match header("content-length") {
        None => 0,
        Some(v) => v
            .trim()
            .parse()
            .map_err(|_| Response::error(400, "invalid Content-Length"))?,
    };
    if content_length > MAX_BODY_BYTES {
        return Err(Response::error(413, "body too large"));
    }
    let mut body = buf[head_len..].to_vec();
    if body.len() > content_length {
        return Err(Response::error(400, "body longer than Content-Length"));
    }
    if body.len() < content_length {
        let start = body.len();
        body.resize(content_length, 0);
        stream
            .read_exact(&mut body[start..])
            .await
            .map_err(|_| Response::error(400, "incomplete body"))?;
    }

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (percent_decode(path), parse_query(query)),
        None => (percent_decode(&target), Vec::new()),
    };
    Ok(Request {
        method,
        path,
        query,
        body,
    })
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

async fn write_response(stream: &mut TcpStream, response: &Response) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        response.status,
        reason(response.status),
        response.content_type,
        response.body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    stream.shutdown().await
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        409 => "Conflict",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        _ => "Internal Server Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_parsing_and_decoding() {
        assert_eq!(
            parse_query("after=12&capability=math.add&x=a%2Fb+c"),
            vec![
                ("after".into(), "12".into()),
                ("capability".into(), "math.add".into()),
                ("x".into(), "a/b c".into())
            ]
        );
        assert_eq!(percent_decode("100%"), "100%");
    }
}
