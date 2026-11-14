//! TEST-05: UTCP cross-node invocation through node-a's local API.
//!
//! `test_05_utcp_contract` replays, in Rust, the exact HTTP exchange the official UTCP HTTP
//! protocol performs (verified from utcp-http 1.1.15 source). `test_05_official_utcp_sdk` runs the
//! official Python SDK itself; it is `#[ignore]`d unless that SDK is installed — see README.

#[path = "../../synapta-core/tests/common/mod.rs"]
mod common;

use std::net::SocketAddr;

use common::Cluster;
use serde_json::{json, Value};
use synapta_gateway::client::ApiClient;
use synapta_gateway::{utcp, Gateway};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Setup {
    cluster: Cluster,
    gateway: Gateway,
    token: String,
}

async fn setup() -> Setup {
    let cluster = Cluster::start(common::sandbox_specs()).await;
    cluster.wait_full_mesh().await;
    let token = synapta_gateway::generate_token();
    let a = cluster.node("node-a").clone();
    let gateway = synapta_gateway::start(a, "127.0.0.1:0".parse().unwrap(), token.clone())
        .await
        .unwrap();
    Setup {
        cluster,
        gateway,
        token,
    }
}

/// Splits a tool's call template URL into (path) and checks it targets this gateway.
fn tool_path(tool: &Value, api: SocketAddr) -> String {
    let url = tool["tool_call_template"]["url"].as_str().unwrap();
    let prefix = format!("http://{api}");
    assert!(url.starts_with(&prefix), "{url} must point at the loopback gateway");
    url[prefix.len()..].to_string()
}

#[tokio::test]
async fn test_05_utcp_contract() {
    let s = setup().await;
    let api = ApiClient::new(s.gateway.addr, &s.token);

    // Discovery: the manual describes every capability in the cluster.
    let (status, manual) = api.get("/utcp").await.unwrap();
    assert_eq!(status, 200);
    assert_eq!(manual["utcp_version"], utcp::UTCP_VERSION);
    let tools = manual["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["math.add", "system.echo", "system.info", "test.execute"]);
    assert!(
        !manual.to_string().contains(&s.token),
        "the token is never written into the manual"
    );

    let add = tools.iter().find(|t| t["name"] == "math.add").unwrap();
    let template = &add["tool_call_template"];
    assert_eq!(template["call_template_type"], "http");
    assert_eq!(template["http_method"], "POST");
    assert_eq!(template["body_field"], "body");
    assert_eq!(template["auth"]["api_key"], "Bearer ${SYNAPTA_API_TOKEN}");
    assert_eq!(add["inputs"]["required"], json!(["body"]));

    // Invocation exactly as utcp-http does it: `body` → JSON body, other args → query string.
    let path = tool_path(add, s.gateway.addr);
    let (status, reply) = api.post(&path, &json!({"a": 2, "b": 3})).await.unwrap();
    assert_eq!(status, 200, "{reply}");
    assert_eq!(reply["result"], json!({"sum": 5}));
    assert_eq!(reply["executed_by"], "node-b");
    assert_eq!(reply["routing"]["selected"], "node-b");

    // node-b executed it; node-a (which hosts the UTCP endpoint) did not.
    assert_eq!(s.cluster.node("node-b").status().executions.get("math.add"), Some(&1));
    assert!(!s.cluster.node("node-a").status().executions.contains_key("math.add"));

    // `target` travels as a query parameter and pins the node.
    let test_tool = tools.iter().find(|t| t["name"] == "test.execute").unwrap();
    let path = format!("{}?target=node-c", tool_path(test_tool, s.gateway.addr));
    let (status, reply) = api
        .post(&path, &json!({"suite": "smoke", "steps": 1, "step_ms": 0}))
        .await
        .unwrap();
    assert_eq!((status, reply["executed_by"].as_str()), (200, Some("node-c")));

    // Structured errors with HTTP status codes the SDK surfaces as exceptions.
    let (status, reply) = api
        .post("/tools/math.add?target=node-c", &json!({"a": 1, "b": 1}))
        .await
        .unwrap();
    assert_eq!(status, 404);
    assert_eq!(reply["error"]["code"], "capability_not_supported");
    let (status, reply) = api
        .post("/tools/math.add?target=node-x", &json!({"a": 1, "b": 1}))
        .await
        .unwrap();
    assert_eq!(
        (status, reply["error"]["code"].as_str()),
        (404, Some("target_not_found"))
    );
    let (status, reply) = api.post("/tools/math.add", &json!({"a": "x", "b": 1})).await.unwrap();
    assert_eq!((status, reply["error"]["code"].as_str()), (400, Some("bad_request")));

    s.cluster.shutdown().await;
}

#[tokio::test]
async fn local_api_rejects_missing_token_wrong_host_and_bad_bodies() {
    let s = setup().await;
    let addr = s.gateway.addr;

    let (status, _) = ApiClient::new(addr, "wrong-token").get("/status").await.unwrap();
    assert_eq!(status, 401);

    let raw = |request: String| async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        reply
    };
    let rebinding = raw(format!(
        "GET /status HTTP/1.1\r\nHost: evil.example:{}\r\nAuthorization: Bearer {}\r\n\r\n",
        addr.port(),
        s.token
    ))
    .await;
    assert!(rebinding.starts_with("HTTP/1.1 403"), "{rebinding}");
    let huge = raw(format!(
        "POST /tools/math.add HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\nContent-Length: 99999999\r\n\r\n",
        s.token
    ))
    .await;
    assert!(huge.starts_with("HTTP/1.1 413"), "{huge}");
    let garbage = raw("\u{1}\u{2}\u{3} nonsense\r\n\r\n".into()).await;
    assert!(garbage.starts_with("HTTP/1.1 400"), "{garbage}");

    let api = ApiClient::new(addr, &s.token);
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = "{not json";
    stream
        .write_all(format!("POST /tools/math.add HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\n\r\n{body}", s.token, body.len()).as_bytes())
        .await
        .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).await.unwrap();
    assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");

    // Still serving afterwards.
    let (status, _) = api.get("/status").await.unwrap();
    assert_eq!(status, 200);
    s.cluster.shutdown().await;
}

#[tokio::test]
async fn job_endpoints_cover_the_async_lifecycle() {
    let s = setup().await;
    let api = ApiClient::new(s.gateway.addr, &s.token);
    let (status, submitted) = api
        .post("/jobs", &json!({"capability": "test.execute", "input": {"suite": "unit", "steps": 3, "step_ms": 20}, "target": "node-b"}))
        .await
        .unwrap();
    assert_eq!(status, 202, "{submitted}");
    let job = submitted["job_id"].as_str().unwrap();
    let (status, info) = api.get(&format!("/jobs/node-b/{job}?wait_ms=5000")).await.unwrap();
    assert_eq!((status, info["state"].as_str()), (200, Some("succeeded")));
    let (_, events) = api.get(&format!("/jobs/node-b/{job}/events")).await.unwrap();
    assert!(!events["events"].as_array().unwrap().is_empty());
    // Equal load now; node-a last sent to node-b, so node-c is next in turn.
    let (status, explain) = api.get("/routing/explain?capability=test.execute").await.unwrap();
    assert_eq!((status, explain["selected"].as_str()), (200, Some("node-c")));
    assert_eq!(
        s.cluster.node("node-b").status().executions.get("test.execute"),
        Some(&1)
    );
    s.cluster.shutdown().await;
}

#[tokio::test]
#[ignore = "needs the official UTCP Python SDK: set SYNAPTA_UTCP_PYTHON (see scripts/utcp-official)"]
async fn test_05_official_utcp_sdk() {
    let python =
        std::env::var("SYNAPTA_UTCP_PYTHON").expect("SYNAPTA_UTCP_PYTHON must point to a python with utcp installed");
    let s = setup().await;
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/utcp-official/official_client_check.py");
    let output = tokio::process::Command::new(python)
        .arg("-I")
        .arg(&script)
        .env(
            "SYNAPTA_MANUAL_TEMPLATE",
            utcp::manual_call_template(s.gateway.addr).to_string(),
        )
        .env("SYNAPTA_API_TOKEN", &s.token)
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "SDK script failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
    println!("official UTCP SDK result: {result}");

    assert_eq!(
        result["tools"],
        json!([
            "synapta.math.add",
            "synapta.system.echo",
            "synapta.system.info",
            "synapta.test.execute"
        ])
    );
    assert_eq!(result["auto"]["result"], json!({"sum": 5}));
    assert_eq!(result["auto"]["executed_by"], "node-b");
    assert_eq!(result["explicit"]["executed_by"], "node-c");
    assert!(result["refused"].as_str().unwrap().contains("capability_not_supported"));
    assert_eq!(s.cluster.node("node-b").status().executions.get("math.add"), Some(&1));
    assert!(!s.cluster.node("node-a").status().executions.contains_key("math.add"));
    s.cluster.shutdown().await;
}
