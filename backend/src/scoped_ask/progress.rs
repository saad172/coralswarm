//! Progress of one scoped Ask, and the SSE frames `POST /v1/queries/stream`
//! sends for it. Progress carries stage names and counts only: the answer
//! leaves the server once, in `final`, after the citation check and the
//! emission fence.
use std::sync::Arc;

use axum::response::{sse::Event, Response};
use futures::Stream;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::Prepared;
use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant, AppState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum Stage {
    Searching,
    Retrieved { sources: usize },
    Writing,
    Checking,
}

/// `attempt` starts at 1 and grows when a transient failure reruns retrieval,
/// so a client resets its stage line when it sees a higher one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Progress {
    #[serde(flatten)]
    pub stage: Stage,
    pub attempt: u32,
}

pub struct Sink(Option<UnboundedSender<Progress>>);

impl Sink {
    pub fn off() -> Self {
        Self(None)
    }

    pub fn channel() -> (Self, UnboundedReceiver<Progress>) {
        let (tx, rx) = unbounded_channel();
        (Self(Some(tx)), rx)
    }

    pub(crate) fn send(&self, stage: Stage, attempt: u32) {
        if let Some(tx) = &self.0 {
            let _ = tx.send(Progress { stage, attempt });
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Progress(Progress),
    Meta(Value),
    Final(Value),
    Error(Value),
}

impl Frame {
    pub fn name(&self) -> &'static str {
        match self {
            Frame::Progress(_) => "progress",
            Frame::Meta(_) => "meta",
            Frame::Final(_) => "final",
            Frame::Error(_) => "error",
        }
    }

    pub fn data(&self) -> Value {
        match self {
            Frame::Progress(p) => json!(p),
            Frame::Meta(v) | Frame::Final(v) | Frame::Error(v) => v.clone(),
        }
    }

    pub fn into_event(self) -> Event {
        Event::default().event(self.name()).data(self.data().to_string())
    }
}

/// The frames of one prepared Ask: `progress`* as the phases run, then `meta`
/// and one `final`, or one `error`.
pub fn frames(
    state: Arc<AppState>,
    tenant: Tenant,
    user: UserContext,
    prepared: Prepared,
    request_id: String,
) -> impl Stream<Item = Frame> + Send + 'static {
    async_stream::stream! {
        let question = prepared.request.text.clone();
        let (sink, mut rx) = Sink::channel();
        // Spawned so a client that disconnects mid-Ask still gets its answer
        // or failure record saved.
        let mut run = tokio::spawn({
            let (state, tenant, user) = (state.clone(), tenant.clone(), user.clone());
            async move { super::answer(&state, &tenant, &user, &prepared, sink).await }
        });
        let joined = loop {
            let step = tokio::select! {
                biased;
                Some(progress) = rx.recv() => Ok(progress),
                joined = &mut run => Err(joined),
            };
            match step {
                Ok(progress) => yield Frame::Progress(progress),
                Err(joined) => break joined,
            }
        };
        while let Ok(progress) = rx.try_recv() {
            yield Frame::Progress(progress);
        }
        match joined {
            Err(_) => yield Frame::Error(json!({"status":500,"error":"scoped ask failed","code":"internal","retryable":true,"failed_message_id":null})),
            Ok(Err(response)) => yield Frame::Error(error_data(response).await),
            Ok(Ok(mut answer)) => {
                crate::services::find_results::attach_to_answer(&tenant, &user, &question, &mut answer.payload).await;
                yield Frame::Meta(json!({"conversation_id":answer.payload["conversation_id"],"new_conversation":answer.payload["new_conversation"],"request_id":request_id}));
                if super::emission_allowed(&state, &tenant, &user, &answer).await {
                    yield Frame::Final(answer.payload);
                } else {
                    yield Frame::Error(json!({"status":409,"error":"access changed; answer withheld","code":"access_changed","retryable":false,"failed_message_id":null}));
                }
            }
        }
    }
}

async fn error_data(response: Response) -> Value {
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap_or_default();
    let mut data: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({"error":"scoped ask failed"}));
    if let Some(object) = data.as_object_mut() {
        let code = object.get("reason").cloned().unwrap_or_else(|| json!("internal"));
        object.insert("status".into(), json!(status));
        object.insert("code".into(), code);
        object.entry("retryable").or_insert(json!(false));
        object.entry("failed_message_id").or_insert(Value::Null);
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_stage_serializes_to_one_flat_event() {
        let p = |stage| json!(Progress { stage, attempt: 1 });
        assert_eq!(p(Stage::Searching), json!({"stage":"searching","attempt":1}));
        assert_eq!(p(Stage::Retrieved { sources: 4 }), json!({"stage":"retrieved","sources":4,"attempt":1}));
        assert_eq!(p(Stage::Writing), json!({"stage":"writing","attempt":1}));
        assert_eq!(p(Stage::Checking), json!({"stage":"checking","attempt":1}));
    }

    #[test]
    fn a_sink_without_a_receiver_drops_progress() {
        Sink::off().send(Stage::Searching, 1);
        let (sink, rx) = Sink::channel();
        drop(rx);
        sink.send(Stage::Writing, 2);
    }
}
