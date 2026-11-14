//! Built-in, side-effect-free executors. None of them touches the host: no processes,
//! no file system, no network. `test.execute` *simulates* an allow-listed test suite.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::{CapabilitySpec, Executor, JobContext};
use crate::error::{ErrorBody, ErrorCode};
use crate::BoxFuture;

/// Every capability a node can be configured to offer in v0.0.1.
pub fn catalog(node_info: Value) -> Vec<Arc<dyn Executor>> {
    vec![
        Arc::new(Echo),
        Arc::new(SystemInfo { node_info }),
        Arc::new(MathAdd),
        Arc::new(TestExecute),
        Arc::new(AgentExecuteMock),
    ]
}

fn bad_request(message: impl Into<String>) -> ErrorBody {
    ErrorBody::new(ErrorCode::BadRequest, message)
}

struct Echo;

impl Executor for Echo {
    fn spec(&self) -> CapabilitySpec {
        CapabilitySpec {
            name: "system.echo",
            description: "Returns the input unchanged.",
            input_schema: json!({"type": "object", "description": "Any JSON object."}),
            output_schema: json!({"type": "object", "properties": {"echo": {"type": "object"}}}),
        }
    }

    fn execute(&self, _ctx: JobContext, input: Value) -> BoxFuture<'static, Result<Value, ErrorBody>> {
        Box::pin(async move { Ok(json!({ "echo": input })) })
    }
}

/// Reports the node's protocol-level description. Deliberately no host details
/// (hostname, OS, users, paths) — peers do not need them.
struct SystemInfo {
    node_info: Value,
}

impl Executor for SystemInfo {
    fn spec(&self) -> CapabilitySpec {
        CapabilitySpec {
            name: "system.info",
            description: "Describes the node: id, name, type, protocol version, capabilities, configured resources.",
            input_schema: json!({"type": "object", "properties": {}}),
            output_schema: json!({"type": "object"}),
        }
    }

    fn execute(&self, _ctx: JobContext, _input: Value) -> BoxFuture<'static, Result<Value, ErrorBody>> {
        let info = self.node_info.clone();
        Box::pin(async move { Ok(info) })
    }
}

struct MathAdd;

impl Executor for MathAdd {
    fn spec(&self) -> CapabilitySpec {
        CapabilitySpec {
            name: "math.add",
            description: "Adds two numbers.",
            input_schema: json!({
                "type": "object",
                "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                "required": ["a", "b"]
            }),
            output_schema: json!({"type": "object", "properties": {"sum": {"type": "number"}}}),
        }
    }

    fn execute(&self, _ctx: JobContext, input: Value) -> BoxFuture<'static, Result<Value, ErrorBody>> {
        Box::pin(async move {
            let (a, b) = (&input["a"], &input["b"]);
            if !a.is_number() || !b.is_number() {
                return Err(bad_request("math.add needs numeric fields 'a' and 'b'"));
            }
            // Integers stay exact; mixing in a float switches to float arithmetic.
            if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                return x
                    .checked_add(y)
                    .map(|sum| json!({ "sum": sum }))
                    .ok_or_else(|| bad_request("integer overflow"));
            }
            let sum = a.as_f64().unwrap_or(f64::NAN) + b.as_f64().unwrap_or(f64::NAN);
            if !sum.is_finite() {
                return Err(bad_request("result is not a finite number"));
            }
            Ok(json!({ "sum": sum }))
        })
    }
}

/// Simulated test runner. Only suites in `SUITES` exist — this stands in for the
/// "allow-listed commands" rule: a peer picks a name from a fixed list, never a command line.
struct TestExecute;

const SUITES: &[&str] = &["smoke", "unit", "integration"];
const MAX_STEPS: u64 = 1_000;
const MAX_STEP_MS: u64 = 10_000;

impl Executor for TestExecute {
    fn spec(&self) -> CapabilitySpec {
        CapabilitySpec {
            name: "test.execute",
            description:
                "Runs a simulated, allow-listed test suite step by step, reporting progress. No host commands.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "suite": {"type": "string", "enum": SUITES},
                    "steps": {"type": "integer", "minimum": 1, "maximum": MAX_STEPS, "default": 5},
                    "step_ms": {"type": "integer", "minimum": 0, "maximum": MAX_STEP_MS, "default": 100},
                    "fail_at_step": {"type": "integer", "minimum": 1, "description": "Simulate a failing step."}
                },
                "required": ["suite"]
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "suite": {"type": "string"},
                    "steps_run": {"type": "integer"},
                    "passed": {"type": "boolean"},
                    "node": {"type": "string"}
                }
            }),
        }
    }

    fn execute(&self, ctx: JobContext, input: Value) -> BoxFuture<'static, Result<Value, ErrorBody>> {
        Box::pin(async move {
            let suite = input["suite"].as_str().unwrap_or_default().to_string();
            if !SUITES.contains(&suite.as_str()) {
                return Err(bad_request(format!("unknown suite {suite:?}; allowed: {SUITES:?}")));
            }
            let steps = optional_u64(&input, "steps", 5, 1, MAX_STEPS)?;
            let step_ms = optional_u64(&input, "step_ms", 100, 0, MAX_STEP_MS)?;
            let fail_at = input.get("fail_at_step").and_then(Value::as_u64);

            for step in 1..=steps {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ErrorBody::new(ErrorCode::Cancelled, "cancelled")),
                    _ = tokio::time::sleep(Duration::from_millis(step_ms)) => {}
                }
                if fail_at == Some(step) {
                    return Ok(json!({"suite": suite, "steps_run": step, "passed": false, "node": ctx.node_id}));
                }
                ctx.progress((step * 100 / steps) as u8, format!("{suite}: step {step}/{steps}"));
            }
            Ok(json!({"suite": suite, "steps_run": steps, "passed": true, "node": ctx.node_id}))
        })
    }
}

fn optional_u64(input: &Value, field: &str, default: u64, min: u64, max: u64) -> Result<u64, ErrorBody> {
    match input.get(field) {
        None | Some(Value::Null) => Ok(default),
        Some(v) => v
            .as_u64()
            .filter(|n| (min..=max).contains(n))
            .ok_or_else(|| bad_request(format!("'{field}' must be an integer in {min}..={max}"))),
    }
}

/// MOCK of a future agent runtime. Returns a canned reply; no model is called.
struct AgentExecuteMock;

const MAX_PROMPT_CHARS: usize = 8_192;

impl Executor for AgentExecuteMock {
    fn spec(&self) -> CapabilitySpec {
        CapabilitySpec {
            name: "agent.execute",
            description: "MOCK agent: acknowledges the prompt with a canned reply. No AI model is called in v0.0.1.",
            input_schema: json!({
                "type": "object",
                "properties": {"prompt": {"type": "string", "maxLength": MAX_PROMPT_CHARS}},
                "required": ["prompt"]
            }),
            output_schema: json!({"type": "object", "properties": {"reply": {"type": "string"}, "mock": {"type": "boolean"}}}),
        }
    }

    fn execute(&self, ctx: JobContext, input: Value) -> BoxFuture<'static, Result<Value, ErrorBody>> {
        Box::pin(async move {
            let prompt = input["prompt"]
                .as_str()
                .ok_or_else(|| bad_request("'prompt' must be a string"))?;
            let chars = prompt.chars().count();
            if chars > MAX_PROMPT_CHARS {
                return Err(bad_request(format!("prompt longer than {MAX_PROMPT_CHARS} characters")));
            }
            Ok(json!({"reply": format!("[mock agent on {}] received {chars} characters", ctx.node_id), "mock": true}))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::jobs::ProgressReporter;
    use crate::protocol::NodeId;
    use tokio_util::sync::CancellationToken;

    fn ctx() -> JobContext {
        JobContext {
            job_id: "j1".into(),
            caller: NodeId::new("node-a").unwrap(),
            node_id: NodeId::new("node-b").unwrap(),
            cancel: CancellationToken::new(),
            progress: ProgressReporter::detached(),
        }
    }

    async fn run(executor: &dyn Executor, input: Value) -> Result<Value, ErrorBody> {
        executor.execute(ctx(), input).await
    }

    #[tokio::test]
    async fn math_add_integers_floats_and_errors() {
        assert_eq!(run(&MathAdd, json!({"a": 2, "b": 3})).await.unwrap(), json!({"sum": 5}));
        assert_eq!(
            run(&MathAdd, json!({"a": 0.5, "b": 2})).await.unwrap(),
            json!({"sum": 2.5})
        );
        assert_eq!(
            run(&MathAdd, json!({"a": i64::MAX, "b": 1})).await.unwrap_err().code,
            ErrorCode::BadRequest
        );
        assert_eq!(
            run(&MathAdd, json!({"a": "2", "b": 3})).await.unwrap_err().code,
            ErrorCode::BadRequest
        );
    }

    #[tokio::test]
    async fn test_execute_rejects_unlisted_suite_and_bad_ranges() {
        let err = run(&TestExecute, json!({"suite": "rm -rf /"})).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        let err = run(&TestExecute, json!({"suite": "unit", "steps": 0}))
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn test_execute_runs_and_can_fail_on_purpose() {
        let ok = run(&TestExecute, json!({"suite": "smoke", "steps": 2, "step_ms": 0}))
            .await
            .unwrap();
        assert_eq!(ok["passed"], true);
        assert_eq!(ok["node"], "node-b");
        let failed = run(
            &TestExecute,
            json!({"suite": "smoke", "steps": 3, "step_ms": 0, "fail_at_step": 2}),
        )
        .await
        .unwrap();
        assert_eq!(failed["passed"], false);
        assert_eq!(failed["steps_run"], 2);
    }

    #[tokio::test]
    async fn agent_mock_is_labelled_mock() {
        let out = run(&AgentExecuteMock, json!({"prompt": "hello"})).await.unwrap();
        assert_eq!(out["mock"], true);
        assert!(out["reply"].as_str().unwrap().contains("node-b"));
    }
}
