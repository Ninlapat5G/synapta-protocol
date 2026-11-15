//! The lab's HTTP API end to end on 127.0.0.1, the way the page uses it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use synapta_lab::api::Api;
use synapta_lab::{http, Lab};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const WAIT: Duration = Duration::from_secs(15);

struct Server {
    addr: SocketAddr,
    lab: Arc<Lab>,
    stop: CancellationToken,
}

impl Server {
    async fn start() -> Self {
        let lab = Arc::new(Lab::new().unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        tokio::spawn(http::serve(listener, Api::new(lab.clone()), stop.clone()));
        Self { addr, lab, stop }
    }

    /// Sends one request; `body` goes as JSON. Returns the status and the parsed body.
    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        let body = body.map(|b| b.to_string()).unwrap_or_default();
        let content_type = if method == "GET" {
            ""
        } else {
            "Content-Type: application/json\r\n"
        };
        self.raw(&format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\n{content_type}Content-Length: {}\r\n\r\n{body}",
            self.addr,
            body.len()
        ))
        .await
    }

    async fn raw(&self, request: &str) -> (u16, Value) {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let text = String::from_utf8(response).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, serde_json::from_str(body).unwrap_or(Value::String(body.into())))
    }

    /// Polls `path` until `check` holds on its body.
    async fn wait_for(&self, path: &str, check: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let (_, body) = self.call("GET", path, None).await;
            if check(&body) || Instant::now() > deadline {
                return body;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn stop(self) {
        self.stop.cancel();
        self.lab.shutdown().await;
    }
}

fn links(nodes: &Value, id: &str) -> Vec<String> {
    nodes
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["setup"]["id"] == id)
        .map(|n| serde_json::from_value(n["links"].clone()).unwrap())
        .unwrap_or_default()
}

#[tokio::test]
async fn the_page_can_build_a_network_and_send_work_across_it() {
    let server = Server::start().await;
    let (status, page) = server.call("GET", "/", None).await;
    assert_eq!(status, 200);
    assert!(page.as_str().unwrap().contains("<html"), "the page is served");

    let nodes = json!([
        { "id": "node-a", "rings": ["home"] },
        { "id": "node-b", "rings": ["home", "office"] },
        { "id": "node-c", "rings": ["office"], "capabilities": [{ "name": "math.add" }] },
    ]);
    let (status, added) = server.call("POST", "/api/nodes", Some(nodes)).await;
    assert_eq!((status, added["failed"].as_array().unwrap().len()), (201, 0));
    let (status, _) = server.call("POST", "/api/nodes", Some(json!({ "id": "node-a" }))).await;
    assert_eq!(status, 409, "ids are unique");

    let nodes = server.wait_for("/api/nodes", |n| links(n, "node-b").len() == 2).await;
    assert_eq!(links(&nodes, "node-a"), vec!["node-b"], "a and c share no ring");
    server
        .wait_for("/api/nodes/node-a/explain?capability=math.add", |d| {
            d["selected"] == "node-c"
        })
        .await;

    let (_, sum) = server
        .call(
            "POST",
            "/api/nodes/node-a/invoke",
            Some(json!({ "capability": "math.add", "input": { "a": 2, "b": 3 } })),
        )
        .await;
    assert_eq!(sum["ok"]["output"]["sum"], 5, "{sum}");
    assert_eq!(sum["ok"]["routing"]["candidates"][0]["via"], "node-b");
    let (_, routes) = server.call("GET", "/api/nodes/node-a/routes", None).await;
    assert_eq!(routes["node-c"], json!({ "next_hop": "node-b", "hops": 2 }));

    // The trace pages on from where the page left off.
    let (_, first) = server.call("GET", "/api/trace?after=0&limit=5", None).await;
    assert_eq!(first["events"].as_array().unwrap().len(), 5);
    let last = first["last"].as_u64().unwrap();
    let (_, next) = server
        .call("GET", &format!("/api/trace?after={last}&limit=1"), None)
        .await;
    assert_eq!(next["events"][0]["seq"].as_u64(), Some(last + 1));

    // Unplug the bridge from "office": node-c drops out of node-a's reach.
    let (status, _) = server
        .call("PUT", "/api/nodes/node-b/rings", Some(json!(["home"])))
        .await;
    assert_eq!(status, 200);
    server
        .wait_for("/api/nodes/node-a/explain?capability=math.add", |d| {
            d["selected"].is_null()
        })
        .await;
    server.stop().await;
}

#[tokio::test]
async fn a_saved_topology_loads_back_and_a_burst_spreads() {
    let server = Server::start().await;
    let (status, report) = server
        .call(
            "POST",
            "/api/scenarios/rings",
            Some(json!({ "rings": 2, "per_ring": 3, "capabilities": ["math.add"] })),
        )
        .await;
    assert_eq!((status, report["nodes"].as_u64()), (200, Some(6)), "{report}");

    // r1-n3 is the bridge: in both rings.
    let (_, saved) = server.call("GET", "/api/topology", None).await;
    assert_eq!(saved["nodes"].as_array().unwrap().len(), 6);
    let mut saved = saved;
    saved["layout"] = json!({ "r1-n1": { "x": 10, "y": 20 } });
    let (status, _) = server.call("PUT", "/api/topology", Some(saved.clone())).await;
    assert_eq!(status, 200);
    let (_, loaded) = server.call("GET", "/api/topology", None).await;
    assert_eq!(loaded, saved, "nodes and layout come back as saved");

    server
        .wait_for("/api/nodes/r1-n1/explain?capability=math.add", |d| {
            d["candidates"]
                .as_array()
                .is_some_and(|c| c.iter().filter(|c| c["eligible"] == true).count() == 6)
        })
        .await;
    let (_, burst) = server
        .call(
            "POST",
            "/api/scenarios/burst",
            Some(json!({ "from": "r1-n1", "count": 12 })),
        )
        .await;
    assert_eq!(burst["errors"], json!({}), "{burst}");
    let spread = burst["ran_on"].as_object().unwrap();
    assert!(spread.len() > 1, "12 calls at once spread over nodes: {burst}");

    let (_, cut) = server.call("POST", "/api/scenarios/cut_bridge", Some(json!({}))).await;
    assert_eq!(cut["bridge"], "r1-n3", "{cut}");
    server.stop().await;
}

#[tokio::test]
async fn changes_must_be_json_so_other_sites_cannot_drive_the_lab() {
    let server = Server::start().await;
    // What a cross-site HTML form would send.
    let form = "POST /api/reset HTTP/1.1\r\nHost: x\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n";
    let (status, _) = server.raw(form).await;
    assert_eq!(status, 415);
    let (status, _) = server.call("POST", "/api/reset", None).await;
    assert_eq!(status, 200);
    let (status, _) = server.call("GET", "/api/nodes/nobody/routes", None).await;
    assert_eq!(status, 409);
    server.stop().await;
}
