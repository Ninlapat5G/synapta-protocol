//! The lab's HTTP routes: the page's files and a JSON API over [`Lab`].
//!
//! | route                                          | does                                    |
//! |------------------------------------------------|-----------------------------------------|
//! | `GET /`, `/app.js`, `/style.css`               | the page (embedded in the binary)       |
//! | `GET /api/info`                                | version, network, capability catalog    |
//! | `GET /api/nodes`                               | every node, light view (for the map)    |
//! | `POST /api/nodes`                              | add one node (object) or many (array)   |
//! | `GET/PUT/DELETE /api/nodes/{id}`               | full status / change setup / remove     |
//! | `POST /api/nodes/{id}/power`                   | `{"on":true}` or `{"off":"graceful"|"crash"}` |
//! | `PUT /api/nodes/{id}/rings`                    | move its cables: `["ring-a", ...]`      |
//! | `GET /api/nodes/{id}/routes`                   | its route table                         |
//! | `GET /api/nodes/{id}/explain?capability=&target=` | routing dry run                      |
//! | `GET /api/nodes/{id}/decisions`                | its recent routing decisions            |
//! | `POST /api/nodes/{id}/invoke`, `/jobs`         | call / submit `{capability,input,target?}` |
//! | `GET /api/nodes/{id}/jobs/{executor}/{job}`    | job status (`?wait_ms=` waits for the end) |
//! | `POST /api/nodes/{id}/jobs/{executor}/{job}/cancel` | cancel                             |
//! | `GET /api/trace?after=&limit=`, `DELETE /api/trace` | recorded frames / forget them      |
//! | `PUT /api/trace/heartbeats`                    | `true`/`false`: record pings too        |
//! | `GET/PUT /api/topology`                        | save / load `{nodes, layout}`           |
//! | `PUT /api/layout`                              | node positions on the page (opaque)     |
//! | `POST /api/reset`                              | remove every node                       |
//! | `POST /api/scenarios/{name}`                   | run a preset (`scenarios.rs`)           |

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use synapta_core::execution::builtin;
use synapta_core::protocol::SOFTWARE_VERSION;
use synapta_core::{NodeHandle, NodeId};

use crate::http::{Handler, Request, Response};
use crate::lab::{Lab, NodeSetup, PowerOff, NETWORK};
use crate::scenarios;

/// Most trace events one page carries; the page asks again with the new `after`.
const MAX_TRACE_PAGE: usize = 5_000;
/// Longest a job-status request may wait for the job to end.
const MAX_JOB_WAIT: Duration = Duration::from_secs(60);

pub struct Api {
    lab: Arc<Lab>,
    /// Where the page drew each node. The server only keeps it, so a reload or a second
    /// viewer sees the same map.
    layout: Mutex<Value>,
}

type Reply = Result<Response, Response>;

impl Handler for Api {
    async fn handle(&self, request: Request) -> Response {
        self.route(request).await.unwrap_or_else(|e| e)
    }
}

impl Api {
    pub fn new(lab: Arc<Lab>) -> Arc<Self> {
        Arc::new(Self {
            lab,
            layout: Mutex::new(json!({})),
        })
    }

    async fn route(&self, request: Request) -> Reply {
        let method = request.method.clone();
        let segments: Vec<String> = request.segments().iter().map(|s| s.to_string()).collect();
        let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
        match (method.as_str(), segments.as_slice()) {
            ("GET", []) => Ok(Response::file("text/html; charset=utf-8", PAGE)),
            ("GET", ["app.js"]) => Ok(Response::file("text/javascript; charset=utf-8", SCRIPT)),
            ("GET", ["style.css"]) => Ok(Response::file("text/css; charset=utf-8", STYLE)),
            ("GET", ["api", "info"]) => {
                let mut info = info();
                info["heartbeats"] = json!(self.lab.recorder().heartbeats());
                Ok(ok(&info))
            }
            ("GET", ["api", "nodes"]) => Ok(ok(&self.lab.nodes().await)),
            ("POST", ["api", "nodes"]) => self.add(&request).await,
            ("POST", ["api", "reset"]) => {
                self.lab.reset().await;
                Ok(ok(&json!({})))
            }
            ("GET", ["api", "trace"]) => self.trace(&request),
            ("PUT", ["api", "trace", "heartbeats"]) => {
                self.lab.recorder().set_heartbeats(body(&request)?);
                Ok(ok(&json!({})))
            }
            ("DELETE", ["api", "trace"]) => {
                self.lab.recorder().clear();
                Ok(ok(&json!({})))
            }
            ("GET", ["api", "topology"]) => Ok(ok(&json!({
                "nodes": self.lab.setups().await,
                "layout": *self.layout.lock().unwrap(),
            }))),
            ("PUT", ["api", "topology"]) => self.load(&request).await,
            ("PUT", ["api", "layout"]) => {
                *self.layout.lock().unwrap() = body(&request)?;
                Ok(ok(&json!({})))
            }
            ("POST", ["api", "scenarios", name]) => scenarios::run(&self.lab, name, body(&request)?)
                .await
                .map(|report| ok(&report))
                .map_err(|e| Response::error(400, e)),
            (_, ["api", "nodes", id, rest @ ..]) => self.node_route(&method, &node_id(id)?, rest, &request).await,
            _ => Err(Response::error(404, format!("no route {method} {}", request.path))),
        }
    }

    async fn node_route(&self, method: &str, id: &NodeId, rest: &[&str], request: &Request) -> Reply {
        match (method, rest) {
            ("GET", []) => Ok(ok(&self.running(id).await?.status())),
            ("PUT", []) => {
                let setup: NodeSetup = body(request)?;
                if &setup.id != id {
                    return Err(Response::error(400, "the id in the body must match the URL"));
                }
                self.lab.update_node(setup).await.map_err(conflict)?;
                Ok(ok(&json!({})))
            }
            ("DELETE", []) => {
                self.lab.remove_node(id).await.map_err(not_found)?;
                Ok(ok(&json!({})))
            }
            ("POST", ["power"]) => self.power(id, body(request)?).await,
            ("PUT", ["rings"]) => {
                self.lab.set_rings(id, body(request)?).await.map_err(not_found)?;
                Ok(ok(&json!({})))
            }
            ("GET", ["routes"]) => Ok(ok(&self.running(id).await?.routes())),
            ("GET", ["decisions"]) => Ok(ok(&self.running(id).await?.recent_decisions())),
            ("GET", ["explain"]) => {
                let capability = request
                    .query_param("capability")
                    .ok_or_else(|| Response::error(400, "capability is required"))?;
                let target = request.query_param("target").map(node_id).transpose()?;
                Ok(ok(&self.running(id).await?.explain(capability, target.as_ref())))
            }
            ("POST", ["invoke"]) => {
                let call: Call = body(request)?;
                let node = self.running(id).await?;
                let result = node.invoke(&call.capability, call.input, call.target.as_ref()).await;
                Ok(routed(result))
            }
            ("POST", ["jobs"]) => {
                let call: Call = body(request)?;
                let node = self.running(id).await?;
                let result = node
                    .submit_job(&call.capability, call.input, call.target.as_ref())
                    .await;
                Ok(routed(result))
            }
            ("GET", ["jobs", executor, job]) => {
                let node = self.running(id).await?;
                let executor = node_id(executor)?;
                let wait = request
                    .query_param("wait_ms")
                    .and_then(|v| v.parse().ok())
                    .map_or(Duration::ZERO, Duration::from_millis)
                    .min(MAX_JOB_WAIT);
                let info = if wait.is_zero() {
                    node.job_status(&executor, job).await
                } else {
                    node.job_result(&executor, job, wait).await
                };
                let events = node.job_events(&executor, job);
                Ok(match info {
                    Ok(info) => ok(&json!({ "job": info, "events": events })),
                    Err(error) => Response::json(502, &json!({ "error": error, "events": events })),
                })
            }
            ("POST", ["jobs", executor, job, "cancel"]) => {
                let node = self.running(id).await?;
                Ok(match node.cancel_job(&node_id(executor)?, job).await {
                    Ok(info) => ok(&json!({ "job": info })),
                    Err(error) => Response::json(502, &json!({ "error": error })),
                })
            }
            _ => Err(Response::error(404, format!("no route {method} {}", request.path))),
        }
    }

    /// One node (object) or many (array). Many are added in order; the reply lists the ones
    /// that failed instead of stopping at the first.
    async fn add(&self, request: &Request) -> Reply {
        let value: Value = body(request)?;
        let many = value.is_array();
        let setups: Vec<NodeSetup> = if many { parse(value)? } else { vec![parse(value)?] };
        let mut failed = Vec::new();
        for setup in setups {
            let id = setup.id.clone();
            if let Err(e) = self.lab.add_node(setup).await {
                failed.push(json!({ "id": id, "error": e }));
            }
        }
        if !many && !failed.is_empty() {
            return Err(Response::error(409, failed[0]["error"].as_str().unwrap_or_default()));
        }
        Ok(Response::json(201, &json!({ "failed": failed })))
    }

    async fn load(&self, request: &Request) -> Reply {
        #[derive(Deserialize)]
        struct Topology {
            nodes: Vec<NodeSetup>,
            #[serde(default)]
            layout: Value,
        }
        let topology: Topology = body(request)?;
        *self.layout.lock().unwrap() = topology.layout;
        self.lab.load(topology.nodes).await.map_err(conflict)?;
        Ok(ok(&json!({})))
    }

    fn trace(&self, request: &Request) -> Reply {
        let after = request.query_param("after").and_then(|v| v.parse().ok()).unwrap_or(0);
        let limit = request
            .query_param("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(MAX_TRACE_PAGE)
            .min(MAX_TRACE_PAGE);
        Ok(ok(&self.lab.recorder().after(after, limit)))
    }

    async fn power(&self, id: &NodeId, change: Power) -> Reply {
        let result = match change {
            Power { on: Some(true), .. } => self.lab.power_on(id).await,
            Power { off: Some(how), .. } => self.lab.power_off(id, how).await,
            _ => {
                return Err(Response::error(
                    400,
                    r#"send {"on":true} or {"off":"graceful"|"crash"}"#,
                ))
            }
        };
        result.map_err(not_found)?;
        Ok(ok(&json!({})))
    }

    async fn running(&self, id: &NodeId) -> Result<NodeHandle, Response> {
        self.lab
            .node(id)
            .await
            .ok_or_else(|| Response::error(409, format!("{id} does not exist or is powered off")))
    }
}

#[derive(Deserialize)]
struct Call {
    capability: String,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    target: Option<NodeId>,
}

#[derive(Deserialize)]
struct Power {
    on: Option<bool>,
    off: Option<PowerOff>,
}

fn info() -> Value {
    let catalog: Vec<Value> = builtin::catalog(Value::Null)
        .iter()
        .map(|executor| {
            let spec = executor.spec();
            json!({ "name": spec.name, "description": spec.description, "input_schema": spec.input_schema })
        })
        .collect();
    json!({ "version": SOFTWARE_VERSION, "network": NETWORK, "catalog": catalog, "scenarios": scenarios::NAMES })
}

/// A routed call's outcome. A failed call is still 200: the routing report is the useful part.
fn routed<T: serde::Serialize, E: serde::Serialize>(result: Result<T, E>) -> Response {
    match result {
        Ok(value) => ok(&json!({ "ok": value })),
        Err(failure) => ok(&json!({ "failed": failure })),
    }
}

fn ok<T: serde::Serialize + ?Sized>(value: &T) -> Response {
    Response::json(200, &serde_json::to_value(value).unwrap_or(Value::Null))
}

fn body<T: DeserializeOwned>(request: &Request) -> Result<T, Response> {
    let body = if request.body.is_empty() {
        b"null".as_slice()
    } else {
        &request.body
    };
    serde_json::from_slice(body).map_err(|e| Response::error(400, format!("bad JSON body: {e}")))
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T, Response> {
    serde_json::from_value(value).map_err(|e| Response::error(400, format!("bad JSON body: {e}")))
}

fn node_id(raw: &str) -> Result<NodeId, Response> {
    NodeId::new(raw).map_err(|e| Response::error(400, e))
}

fn not_found(message: String) -> Response {
    Response::error(404, message)
}

fn conflict(message: String) -> Response {
    Response::error(409, message)
}

static PAGE: &[u8] = include_bytes!("web/index.html");
static SCRIPT: &[u8] = include_bytes!("web/app.js");
static STYLE: &[u8] = include_bytes!("web/style.css");
