//! Handles requests arriving from an authenticated peer. `caller` is the id proven in the
//! handshake — never a value taken from the request body.
//!
//! The receiving node never forwards work to a third node: it executes locally or refuses.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::config::ms;
use crate::discovery::flooding;
use crate::discovery::link::SyncAdvertsResult;
use crate::error::ErrorBody;
use crate::execution::EventSink;
use crate::node::state::NodeState;
use crate::protocol::message::{InvokeResult, JobSubmitted, Request};
use crate::protocol::NodeId;

pub(crate) async fn handle_request(
    state: &Arc<NodeState>,
    caller: &NodeId,
    request: Request,
    sink: EventSink,
) -> Result<Value, ErrorBody> {
    let request_timeout = ms(state.config.timeouts.request_ms);
    match request {
        Request::Ping => Ok(json!({})),
        Request::SyncAdverts { adverts } => {
            for signed in adverts {
                if let Err(e) = flooding::accept_advert(state, signed, Some(caller)) {
                    tracing::warn!(peer = %caller, error = %e, "advert from sync rejected");
                }
            }
            to_value(SyncAdvertsResult {
                adverts: flooding::all_known_adverts(state),
            })
        }
        Request::Invoke { capability, input } => {
            let executor = state.authorize(caller, &capability)?;
            let (job_id, output) = state.jobs.run(caller.clone(), executor, input, request_timeout).await?;
            to_value(InvokeResult {
                executed_by: state.node_id().clone(),
                job_id,
                output,
            })
        }
        Request::JobSubmit { capability, input } => {
            let executor = state.authorize(caller, &capability)?;
            let job_id = state.jobs.submit(caller.clone(), executor, input, Some(sink))?;
            to_value(JobSubmitted {
                executed_by: state.node_id().clone(),
                job_id,
            })
        }
        Request::JobStatus { job_id } => to_value(state.jobs.status(caller, &job_id)?),
        Request::JobResult { job_id, wait_ms } => {
            // Answer well before the caller's own request timeout fires.
            let wait = ms(wait_ms).min(request_timeout / 2);
            to_value(state.jobs.wait(caller, &job_id, wait).await?)
        }
        Request::JobCancel { job_id } => to_value(state.jobs.cancel(caller, &job_id).await?),
    }
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, ErrorBody> {
    serde_json::to_value(value).map_err(|e| ErrorBody::new(crate::error::ErrorCode::Internal, e.to_string()))
}
