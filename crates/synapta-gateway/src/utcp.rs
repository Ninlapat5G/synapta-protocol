//! UTCP adapter: publishes the cluster's capabilities as a UTCP manual whose tools call back
//! into this node's `/tools/{capability}` endpoint, where Synapta routing picks the node.
//!
//! Checked against the official Python SDK (`utcp` 1.2.0 + `utcp-http` 1.1.15), see
//! docs/utcp-integration.md. The parts of that SDK this file depends on:
//! - a manual is `{utcp_version, manual_version, tools}`; tool calls use an `http` call template;
//! - the argument named by `body_field` becomes the JSON body, every other argument becomes a
//!   query parameter — so tools take `{"body": <capability input>, "target"?: "<node id>"}`;
//! - plain `http://` is accepted only for loopback hosts written literally in the template;
//! - `${VAR}` in a template is resolved from the client's variables, namespaced by manual name
//!   (`synapta_SYNAPTA_API_TOKEN` for a manual registered as `synapta`). The token itself is
//!   never written into the manual.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde_json::{json, Value};
use synapta_core::protocol::message::CapabilityAdvert;
use synapta_core::protocol::PROTOCOL_VERSION;
use synapta_core::NodeStatus;

/// UTCP specification version this manual follows (the SDK does not gate on it).
pub const UTCP_VERSION: &str = "1.0.1";
/// Name clients should register the manual under; also the variable namespace.
pub const MANUAL_NAME: &str = "synapta";
pub const TOKEN_VARIABLE: &str = "SYNAPTA_API_TOKEN";

pub fn manual(status: &NodeStatus, api_addr: SocketAddr) -> Value {
    // One tool per capability, whichever nodes offer it. Iterating own advert first, then
    // peers in node-id order, keeps the output deterministic.
    let mut tools: BTreeMap<&str, (&CapabilityAdvert, Vec<&str>)> = BTreeMap::new();
    let adverts = std::iter::once(&status.node).chain(status.peers.iter().filter_map(|p| p.advert.as_ref()));
    for advert in adverts {
        for cap in &advert.capabilities {
            tools
                .entry(cap.name.as_str())
                .or_insert((cap, Vec::new()))
                .1
                .push(advert.node_id.as_str());
        }
    }
    let tools: Vec<Value> = tools
        .into_values()
        .map(|(cap, providers)| tool(cap, &providers, api_addr))
        .collect();
    json!({
        "utcp_version": UTCP_VERSION,
        "manual_version": PROTOCOL_VERSION,
        "tools": tools,
    })
}

fn tool(cap: &CapabilityAdvert, providers: &[&str], api_addr: SocketAddr) -> Value {
    json!({
        "name": cap.name,
        "description": format!("{} (Synapta capability; currently offered by: {})", cap.description, providers.join(", ")),
        "inputs": {
            "type": "object",
            "properties": {
                "body": cap.input_schema,
                "target": {
                    "type": "string",
                    "description": "Optional node id. When set, only that node may run the call (no fallback)."
                }
            },
            "required": ["body"]
        },
        "outputs": {
            "type": "object",
            "properties": {
                "result": cap.output_schema,
                "executed_by": {"type": "string", "description": "Node that ran the call."},
                "job_id": {"type": "string"},
                "routing": {"type": "object", "description": "Synapta routing decision."}
            }
        },
        "tags": ["synapta", format!("capability:{}", cap.name)],
        "tool_call_template": {
            "name": MANUAL_NAME,
            "call_template_type": "http",
            "http_method": "POST",
            "url": format!("http://{api_addr}/tools/{}", cap.name),
            "content_type": "application/json",
            "body_field": "body",
            "auth": auth()
        }
    })
}

/// Auth block shared by the manual's tools and by the template clients use to fetch the manual.
pub fn auth() -> Value {
    json!({
        "auth_type": "api_key",
        "api_key": format!("Bearer ${{{TOKEN_VARIABLE}}}"),
        "var_name": "Authorization",
        "location": "header"
    })
}

/// The call template a UTCP client registers to discover this node's tools.
pub fn manual_call_template(api_addr: SocketAddr) -> Value {
    json!({
        "name": MANUAL_NAME,
        "call_template_type": "http",
        "http_method": "GET",
        "url": format!("http://{api_addr}/utcp"),
        "auth": auth()
    })
}
