//! Local API routes. The caller of these routes acts *as this node* (it holds the node's
//! API token), so routing decisions use this node as the caller.
//!
//! | method | path                                   | does                                   |
//! |--------|----------------------------------------|----------------------------------------|
//! | GET    | /utcp                                  | UTCP manual                            |
//! | POST   | /tools/{capability}?target=<node>      | invoke (UTCP tool endpoint)            |
//! | GET    | /status                                | node, peers, registry, executions      |
//! | GET    | /routing/explain?capability=&target=   | dry-run routing decision               |
//! | GET    | /routing/decisions                     | recent decisions (bounded log)         |
//! | POST   | /jobs                                  | submit `{capability, input, target?}`  |
//! | GET    | /jobs/{node}/{job}?wait_ms=            | job status (optionally wait)           |
//! | GET    | /jobs/{node}/{job}/events              | progress / finished events             |
//! | POST   | /jobs/{node}/{job}/cancel              | cancel                                 |
//! | POST   | /admin/shutdown                        | graceful shutdown                      |

use std::net::SocketAddr;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use synapta_core::routing::dispatch::RouteFailure;
use synapta_core::{ErrorBody, ErrorCode, NodeHandle, NodeId};

use crate::http::{error, Handler, Request, Response};
use crate::utcp;
use crate::BoxFuture;

/// Upper bound for `wait_ms` on job status, below the HTTP client's usual 30 s timeout.
const MAX_WAIT: Duration = Duration::from_secs(20);

pub struct Routes {
    pub node: NodeHandle,
    pub api_addr: SocketAddr,
}

impl Handler for Routes {
    fn handle(&self, request: Request) -> BoxFuture<'_, Response> {
        Box::pin(self.route(request))
    }
}

impl Routes {
    async fn route(&self, request: Request) -> Response {
        let method = request.method.clone();
        let segments: Vec<String> = request.segments().into_iter().map(String::from).collect();
        let segs: Vec<&str> = segments.iter().map(String::as_str).collect();
        match (method.as_str(), segs.as_slice()) {
            ("GET", ["utcp"]) => Response::json(200, utcp::manual(&self.node.status(), self.api_addr)),
            ("POST", ["tools", capability]) => self.invoke(capability, &request).await,
            ("GET", ["status"]) => to_response(200, &self.node.status()),
            ("GET", ["routing", "explain"]) => self.explain(&request),
            ("GET", ["routing", "decisions"]) => {
                to_response(200, &json!({ "decisions": self.node.recent_decisions() }))
            }
            ("POST", ["jobs"]) => self.submit(&request).await,
            ("GET", ["jobs", node, job]) => self.job_status(node, job, &request).await,
            ("GET", ["jobs", node, job, "events"]) => match parse_node(node) {
                Ok(node) => to_response(200, &json!({ "events": self.node.job_events(&node, job) })),
                Err(response) => response,
            },
            ("POST", ["jobs", node, job, "cancel"]) => match parse_node(node) {
                Ok(node) => job_response(self.node.cancel_job(&node, job).await),
                Err(response) => response,
            },
            ("POST", ["admin", "shutdown"]) => {
                self.node.request_shutdown();
                Response::json(202, json!({ "shutting_down": true }))
            }
            _ => error(404, "no such route"),
        }
    }

    async fn invoke(&self, capability: &str, request: &Request) -> Response {
        let input: Value = match parse_body(request) {
            Ok(v) => v,
            Err(response) => return response,
        };
        let target = match optional_target(request.query_param("target")) {
            Ok(t) => t,
            Err(response) => return response,
        };
        match self.node.invoke(capability, input, target.as_ref()).await {
            Ok(routed) => to_response(
                200,
                &json!({
                    "result": routed.value.output,
                    "executed_by": routed.value.executed_by,
                    "job_id": routed.value.job_id,
                    "routing": routed.routing,
                }),
            ),
            Err(failure) => route_failure(failure),
        }
    }

    fn explain(&self, request: &Request) -> Response {
        let Some(capability) = request.query_param("capability") else {
            return error(400, "query parameter 'capability' is required");
        };
        match optional_target(request.query_param("target")) {
            Ok(target) => to_response(200, &self.node.explain(capability, target.as_ref())),
            Err(response) => response,
        }
    }

    async fn submit(&self, request: &Request) -> Response {
        #[derive(Deserialize)]
        struct Submit {
            capability: String,
            #[serde(default)]
            input: Value,
            #[serde(default)]
            target: Option<String>,
        }
        let submit: Submit = match parse_body(request) {
            Ok(v) => v,
            Err(response) => return response,
        };
        let target = match optional_target(submit.target.as_deref()) {
            Ok(t) => t,
            Err(response) => return response,
        };
        match self
            .node
            .submit_job(&submit.capability, submit.input, target.as_ref())
            .await
        {
            Ok(routed) => to_response(202, &routed),
            Err(failure) => route_failure(failure),
        }
    }

    async fn job_status(&self, node: &str, job: &str, request: &Request) -> Response {
        let node = match parse_node(node) {
            Ok(node) => node,
            Err(response) => return response,
        };
        match request.query_param("wait_ms").map(str::parse::<u64>) {
            None => job_response(self.node.job_status(&node, job).await),
            Some(Ok(wait)) => job_response(
                self.node
                    .job_result(&node, job, Duration::from_millis(wait).min(MAX_WAIT))
                    .await,
            ),
            Some(Err(_)) => error(400, "wait_ms must be an integer"),
        }
    }
}

fn parse_body<T: serde::de::DeserializeOwned>(request: &Request) -> Result<T, Response> {
    let body: &[u8] = if request.body.is_empty() { b"{}" } else { &request.body };
    serde_json::from_slice(body).map_err(|e| error(400, &format!("invalid JSON body: {e}")))
}

fn parse_node(node: &str) -> Result<NodeId, Response> {
    NodeId::new(node).map_err(|e| error(400, &e))
}

fn optional_target(target: Option<&str>) -> Result<Option<NodeId>, Response> {
    target.filter(|t| !t.is_empty()).map(parse_node).transpose()
}

fn to_response<T: serde::Serialize>(status: u16, value: &T) -> Response {
    match serde_json::to_value(value) {
        Ok(body) => Response::json(status, body),
        Err(e) => error(500, &e.to_string()),
    }
}

fn job_response(result: Result<synapta_core::protocol::message::JobInfo, ErrorBody>) -> Response {
    match result {
        Ok(info) => to_response(200, &info),
        Err(e) => Response::json(status_for(e.code), json!({ "error": e })),
    }
}

fn route_failure(failure: RouteFailure) -> Response {
    Response::json(
        status_for(failure.error.code),
        json!({ "error": failure.error, "routing": failure.routing }),
    )
}

/// HTTP status for each protocol error code (documented in docs/utcp-integration.md).
pub fn status_for(code: ErrorCode) -> u16 {
    match code {
        ErrorCode::BadRequest | ErrorCode::MalformedFrame => 400,
        ErrorCode::Unauthorized => 403,
        ErrorCode::TargetNotFound | ErrorCode::CapabilityNotSupported | ErrorCode::JobNotFound => 404,
        ErrorCode::Busy => 429,
        ErrorCode::TargetUnavailable
        | ErrorCode::NoEligibleNode
        | ErrorCode::ConnectionLost
        | ErrorCode::ShuttingDown => 503,
        ErrorCode::Timeout => 504,
        _ => 500,
    }
}
