//! Test support: a gated transport around a scripted mock provider.
//!
//! Managed calls refuse any provider without a [`GatedLlm`] transport, so
//! managed-path tests wrap their mock in [`GatedScript`]. Each call takes one
//! permit from the gate, runs the mock once, and settles: `Billed` at the
//! mock's reported cost on `Ok`, `NotGenerated` on `Err`.

use super::{
    send_gate, CompletionRequest, CompletionResponse, GatedLlm, LlmError, LlmProvider,
    SendGate, StreamEvent,
};
use coralswarm_core::send_gate::SendOutcome;

pub struct GatedScript<'a, M: ?Sized>(pub &'a M);

impl<M: LlmProvider + ?Sized> super::private::Sealed for GatedScript<'_, M> {}

#[async_trait::async_trait]
impl<M: LlmProvider + ?Sized> GatedLlm for GatedScript<'_, M> {
    async fn complete_gated(
        &self,
        req: CompletionRequest,
        gate: &dyn SendGate,
    ) -> Result<CompletionResponse, LlmError> {
        let permit = send_gate::acquire(gate, 1).await.map_err(LlmError::Managed)?;
        let sent = permit.into_sent();
        let result = self.0.complete(req).await;
        let outcome = match &result {
            Ok(r) => SendOutcome::Billed {
                reported_cost_usd: r.cost_usd,
                input_tokens: u64::from(r.input_tokens),
                output_tokens: u64::from(r.output_tokens),
                served_model: r.model.clone(),
                upstream: None,
                generation_id: None,
            },
            Err(_) => SendOutcome::NotGenerated,
        };
        send_gate::settle(gate, sent, outcome).await;
        result
    }
}

#[async_trait::async_trait]
impl<M: LlmProvider + ?Sized> LlmProvider for GatedScript<'_, M> {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.0.complete(req).await
    }

    async fn stream_complete(
        &self,
        req: CompletionRequest,
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent, LlmError>> + Send>>,
        LlmError,
    > {
        self.0.stream_complete(req).await
    }

    fn name(&self) -> &'static str {
        self.0.name()
    }

    fn gated(&self) -> Option<&dyn GatedLlm> {
        Some(self)
    }
}
