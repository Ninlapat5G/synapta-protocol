//! Carries out a routing decision: run locally or send over the READY link, and record the
//! decision (with every attempt) in the log.
//!
//! Delivery is at-most-once. Automatic routing moves on to the next ranked candidate only when
//! the chosen node provably did not run anything: its link was not READY at dispatch time, or
//! it answered `Busy`. After any other failure (timeout, lost connection) the request may have
//! run, so it is reported, not retried elsewhere. Explicit targets are never substituted.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use super::{decide, Attempt, ClusterView, RoutingDecision, RoutingMode};
use crate::config::ms;
use crate::discovery::link::LinkHandle;
use crate::error::{ErrorBody, ErrorCode};
use crate::execution::EventSink;
use crate::node::state::NodeState;
use crate::protocol::message::{InvokeResult, JobSubmitted, Request};
use crate::protocol::NodeId;

/// Extra time a caller waits beyond the executing node's own request timeout, so the remote
/// side's structured `Timeout` error arrives before the caller gives up on its own.
const REMOTE_CALL_GRACE: Duration = Duration::from_millis(1_000);

#[derive(Debug, Clone, Serialize)]
pub struct Routed<T> {
    #[serde(flatten)]
    pub value: T,
    pub routing: RoutingDecision,
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[error("{error}")]
pub struct RouteFailure {
    pub error: ErrorBody,
    /// Boxed: a decision is large and errors are moved through several layers.
    pub routing: Box<RoutingDecision>,
}

pub(crate) fn current_decision(state: &NodeState, capability: &str, target: Option<&NodeId>) -> RoutingDecision {
    let view = ClusterView {
        local: state.advert_now(0),
        peers: state.registry.read().unwrap().snapshots(),
    };
    decide(&view, state.node_id(), capability, target)
}

pub(crate) async fn invoke(
    state: &Arc<NodeState>,
    capability: &str,
    input: Value,
    target: Option<&NodeId>,
) -> Result<Routed<InvokeResult>, RouteFailure> {
    dispatch(state, capability, target, |destination| {
        let input = input.clone();
        async move {
            match destination {
                Destination::Local => {
                    let executor = state.authorize(state.node_id(), capability)?;
                    let timeout = ms(state.config.timeouts.request_ms);
                    let (job_id, output) = state
                        .jobs
                        .run(state.node_id().clone(), executor, input, timeout)
                        .await?;
                    Ok(InvokeResult {
                        executed_by: state.node_id().clone(),
                        job_id,
                        output,
                    })
                }
                Destination::Remote(link) => {
                    let request = Request::Invoke {
                        capability: capability.to_string(),
                        input,
                    };
                    let value = link.call(request, remote_timeout(state)).await?;
                    let result: InvokeResult = decode(value)?;
                    check_executor(&link, &result.executed_by)?;
                    Ok(result)
                }
            }
        }
    })
    .await
}

pub(crate) async fn submit(
    state: &Arc<NodeState>,
    capability: &str,
    input: Value,
    target: Option<&NodeId>,
) -> Result<Routed<JobSubmitted>, RouteFailure> {
    dispatch(state, capability, target, |destination| {
        let input = input.clone();
        async move {
            match destination {
                Destination::Local => {
                    let executor = state.authorize(state.node_id(), capability)?;
                    let log_state = state.clone();
                    let sink: EventSink =
                        Arc::new(move |event| log_state.job_events.record(log_state.node_id(), event));
                    let job_id = state
                        .jobs
                        .submit(state.node_id().clone(), executor, input, Some(sink))?;
                    Ok(JobSubmitted {
                        executed_by: state.node_id().clone(),
                        job_id,
                    })
                }
                Destination::Remote(link) => {
                    let request = Request::JobSubmit {
                        capability: capability.to_string(),
                        input,
                    };
                    let submitted: JobSubmitted = decode(link.call(request, remote_timeout(state)).await?)?;
                    check_executor(&link, &submitted.executed_by)?;
                    Ok(submitted)
                }
            }
        }
    })
    .await
}

enum Destination {
    Local,
    Remote(LinkHandle),
}

async fn dispatch<T, F, Fut>(
    state: &Arc<NodeState>,
    capability: &str,
    target: Option<&NodeId>,
    perform: F,
) -> Result<Routed<T>, RouteFailure>
where
    F: Fn(Destination) -> Fut,
    Fut: std::future::Future<Output = Result<T, ErrorBody>>,
{
    let mut decision = current_decision(state, capability, target);
    if let Some(error) = decision.error.clone() {
        state.record_decision(&decision);
        return Err(RouteFailure {
            error,
            routing: Box::new(decision),
        });
    }

    for node in decision.ranked() {
        let destination = if &node == state.node_id() {
            Destination::Local
        } else {
            match state.registry.read().unwrap().ready_link(&node) {
                Some(link) => Destination::Remote(link),
                None => {
                    // Became unavailable between deciding and sending: nothing was sent.
                    decision
                        .attempts
                        .push(attempt(&node, "link not READY at dispatch; not sent"));
                    if decision.mode == RoutingMode::Explicit {
                        let error = ErrorBody::new(ErrorCode::TargetUnavailable, format!("{node} became unavailable"));
                        return Err(fail(state, decision, error));
                    }
                    continue;
                }
            }
        };
        match perform(destination).await {
            Ok(value) => {
                decision.attempts.push(attempt(&node, "ok"));
                decision.selected = Some(node);
                state.record_decision(&decision);
                return Ok(Routed {
                    value,
                    routing: decision,
                });
            }
            Err(error) if decision.mode == RoutingMode::Automatic && error.code.is_pre_execution_rejection() => {
                decision
                    .attempts
                    .push(attempt(&node, &format!("refused before execution: {:?}", error.code)));
            }
            Err(error) => {
                decision
                    .attempts
                    .push(attempt(&node, &format!("failed: {:?}: {}", error.code, error.message)));
                decision.selected = Some(node);
                return Err(fail(state, decision, error));
            }
        }
    }
    let error = ErrorBody::new(
        ErrorCode::NoEligibleNode,
        format!("every candidate for {capability} refused"),
    );
    decision.selected = None;
    Err(fail(state, decision, error))
}

fn fail(state: &NodeState, mut decision: RoutingDecision, error: ErrorBody) -> RouteFailure {
    decision.error = Some(error.clone());
    state.record_decision(&decision);
    RouteFailure {
        error,
        routing: Box::new(decision),
    }
}

fn attempt(node: &NodeId, outcome: &str) -> Attempt {
    Attempt {
        node_id: node.clone(),
        outcome: outcome.to_string(),
    }
}

fn remote_timeout(state: &NodeState) -> Duration {
    ms(state.config.timeouts.request_ms) + REMOTE_CALL_GRACE
}

/// The executing node reports who ran the job; it must be the node we are linked to.
fn check_executor(link: &LinkHandle, executed_by: &NodeId) -> Result<(), ErrorBody> {
    if executed_by == link.peer() {
        Ok(())
    } else {
        Err(ErrorBody::new(
            ErrorCode::Internal,
            format!("{} reported the job as executed by {executed_by}", link.peer()),
        ))
    }
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, ErrorBody> {
    serde_json::from_value(value)
        .map_err(|e| ErrorBody::new(ErrorCode::MalformedFrame, format!("unexpected response: {e}")))
}
