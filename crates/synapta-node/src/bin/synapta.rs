//! `synapta` — command-line client for a running node's local API.
//!
//! ```text
//! synapta --node <data_dir> <command>             # reads runtime.json + api-token from data_dir
//! synapta --api <host:port> --token <t> <command>
//!
//! commands:
//!   status                                       node, peers, registry, executions
//!   manual                                       UTCP manual
//!   invoke <capability> [json] [--target <node>]
//!   explain <capability> [--target <node>]       routing decision without executing
//!   decisions                                    recent routing decisions
//!   job submit <capability> [json] [--target <node>]
//!   job status <node> <job_id> [--wait <ms>]
//!   job events <node> <job_id>
//!   job cancel <node> <job_id>
//!   shutdown
//! ```
//! Prints the JSON response. Exit code 1 when the node answers with an error status.

use std::path::Path;
use std::process::ExitCode;

use serde_json::{json, Value};
use synapta_gateway::client::ApiClient;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => return fail(&e.to_string()),
    };
    match runtime.block_on(run(args)) {
        Ok((status, body)) => {
            println!("{}", serde_json::to_string_pretty(&body).unwrap_or_default());
            if status < 400 {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(message) => fail(&message),
    }
}

fn fail(message: &str) -> ExitCode {
    eprintln!("synapta: {message}");
    ExitCode::FAILURE
}

async fn run(mut args: Vec<String>) -> Result<(u16, Value), String> {
    let client = connect(&mut args)?;
    let target = take_flag(&mut args, "--target")?;
    let wait = take_flag(&mut args, "--wait")?;
    let words: Vec<&str> = args.iter().map(String::as_str).collect();
    let target_query = target.as_ref().map(|t| format!("?target={t}")).unwrap_or_default();

    match words.as_slice() {
        ["status"] => client.get("/status").await,
        ["manual"] => client.get("/utcp").await,
        ["decisions"] => client.get("/routing/decisions").await,
        ["shutdown"] => client.post("/admin/shutdown", &json!({})).await,
        ["invoke", capability, rest @ ..] => {
            let input = parse_json(rest.first())?;
            client.post(&format!("/tools/{capability}{target_query}"), &input).await
        }
        ["explain", capability] => {
            let target = target.map(|t| format!("&target={t}")).unwrap_or_default();
            client
                .get(&format!("/routing/explain?capability={capability}{target}"))
                .await
        }
        ["job", "submit", capability, rest @ ..] => {
            let body = json!({"capability": capability, "input": parse_json(rest.first())?, "target": target});
            client.post("/jobs", &body).await
        }
        ["job", "status", node, job] => {
            let wait = wait.map(|w| format!("?wait_ms={w}")).unwrap_or_default();
            client.get(&format!("/jobs/{node}/{job}{wait}")).await
        }
        ["job", "events", node, job] => client.get(&format!("/jobs/{node}/{job}/events")).await,
        ["job", "cancel", node, job] => client.post(&format!("/jobs/{node}/{job}/cancel"), &json!({})).await,
        _ => Err(usage()),
    }
}

fn connect(args: &mut Vec<String>) -> Result<ApiClient, String> {
    if let Some(dir) = take_flag(args, "--node")? {
        return ApiClient::from_data_dir(Path::new(&dir));
    }
    match (take_flag(args, "--api")?, take_flag(args, "--token")?) {
        (Some(api), Some(token)) => Ok(ApiClient::new(api.parse().map_err(|e| format!("--api: {e}"))?, token)),
        _ => Err(usage()),
    }
}

/// Removes `--name value` from `args` and returns the value.
fn take_flag(args: &mut Vec<String>, name: &str) -> Result<Option<String>, String> {
    let Some(index) = args.iter().position(|a| a == name) else {
        return Ok(None);
    };
    if index + 1 >= args.len() {
        return Err(format!("{name} needs a value"));
    }
    let value = args.remove(index + 1);
    args.remove(index);
    Ok(Some(value))
}

fn parse_json(text: Option<&&str>) -> Result<Value, String> {
    match text {
        None => Ok(json!({})),
        Some(text) => serde_json::from_str(text).map_err(|e| format!("invalid JSON input: {e}")),
    }
}

fn usage() -> String {
    "usage: synapta (--node <data_dir> | --api <host:port> --token <token>) <command>\n\
     commands: status | manual | decisions | shutdown | invoke <cap> [json] [--target n] | explain <cap> [--target n]\n\
     \u{20}         job submit <cap> [json] [--target n] | job status <node> <id> [--wait ms] | job events <node> <id> | job cancel <node> <id>"
        .to_string()
}
