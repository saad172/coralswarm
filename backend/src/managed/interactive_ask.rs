//! `interactive_ask`: Jev's calls inside a user-initiated Ask (catalog scan,
//! pack, cite, the capture Noul and contradiction pairs), on their own managed
//! purpose with a per-account daily cap
//! (`MANAGED_DAILY_CAP_USD_INTERACTIVE_ASK`, default $1).
//!
//! An Ask is interactive, not automatic, so it does not share
//! `managed_enrichment`'s background cap: a busy enrichment day must never
//! hold a user's Ask, and a burst of Asks must never starve enrichment.
//!
//! Every Jev question set is one logical call. Each provider attempt reserves
//! the daily cap and the asker's credits before it is sent and settles once
//! after, through the same funded gate as the other purposes. The receipts are
//! the bill: the call's `llm_usage` row carries no `source_query_id`, so the
//! query axis never charges it a second time.
//!
//! A hold or refusal never fails the Ask. `decision::openrouter` returns it as
//! a fail-open `DecisionError::Budget`, and each Ask phase keeps its non-Jev
//! path (rerank and cap, unverified citations, the LLM capture classifier).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use coralswarm_core::send_gate::{Deny, HoldCode};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{embedding, ManagedPurpose, PlanePermit};
use crate::tenancy::TenantId;

/// Provider attempts one logical call may reserve (the receipt CHECK allows
/// 1..=8).
pub(crate) const MAX_ATTEMPTS: u32 = 8;

const PURPOSE: ManagedPurpose = ManagedPurpose::InteractiveAsk;

fn unavailable(error: impl std::fmt::Display) -> Deny {
    tracing::warn!(%error, "managed interactive ask unavailable");
    Deny::Unavailable
}

/// One Jev question set inside one Ask. The asker is the principal: the
/// receipt, the credit reservation and the membership fence all name them.
#[derive(Clone, Debug)]
pub(crate) struct AskWork {
    account_id: Uuid,
    asker: Uuid,
    graph_id: Uuid,
    query_id: Uuid,
    phase: Option<&'static str>,
    logical_call: Uuid,
    /// Set when the Ask stops waiting for this call. The fence then holds
    /// every further attempt, so nothing new is sent for an Ask that already
    /// failed open. An attempt already in flight still settles.
    stopped: Arc<AtomicBool>,
}

impl AskWork {
    pub(crate) fn new(
        account_id: Uuid,
        asker: Uuid,
        graph_id: Uuid,
        query_id: Uuid,
        phase: Option<&'static str>,
        stopped: Arc<AtomicBool>,
    ) -> Self {
        Self {
            account_id,
            asker,
            graph_id,
            query_id,
            phase,
            logical_call: Uuid::new_v4(),
            stopped,
        }
    }

    pub(crate) fn graph_id(&self) -> Uuid {
        self.graph_id
    }

    /// One logical call per work value; every attempt's receipt carries it.
    pub(crate) fn logical_call(&self) -> Uuid {
        self.logical_call
    }

    /// The call must name this work's account and charge exactly the asker.
    pub(in crate::managed) fn check_call(&self, account: Uuid, principal: Uuid) -> Result<(), Deny> {
        if account != self.account_id || principal != self.asker {
            return Err(Deny::Unavailable);
        }
        Ok(())
    }

    /// The request-hash material: purpose, plane, account, asker, graph,
    /// query, phase and logical call.
    pub(in crate::managed) fn binding(&self, tenant: &TenantId) -> Value {
        let plane = match tenant {
            TenantId::Consumer => "consumer".to_owned(),
            TenantId::Org(org) => format!("org:{org}"),
        };
        json!({
            "purpose": PURPOSE.as_str(),
            "plane": plane,
            "account": self.account_id,
            "asker": self.asker,
            "graph": self.graph_id,
            "query": self.query_id,
            "phase": self.phase,
            "logical_call": self.logical_call,
        })
    }

    /// The locked fence for one attempt: the Ask is still waiting, the global
    /// kill switch is off, the account allows cloud egress, the asker is still
    /// a current unrevoked member of this plane's account, and the graph is
    /// live and reachable by them.
    pub(in crate::managed) async fn fence_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
        mut financial: Option<&mut Transaction<'_, Postgres>>,
        permit: &PlanePermit,
        principal: Uuid,
    ) -> Result<(), Deny> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Deny::Hold(HoldCode::Fenced));
        }
        if principal != self.asker || permit.account() != self.account_id {
            return Err(Deny::Unavailable);
        }
        embedding::policy(content, financial.as_deref_mut(), self.account_id).await?;
        let graph: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM graphs WHERE id=$1 AND archived_at IS NULL FOR SHARE")
                .bind(self.graph_id)
                .fetch_optional(&mut **content)
                .await
                .map_err(unavailable)?;
        if graph.is_none()
            || !embedding::member_reaches_locked(content, financial, permit, self.graph_id, self.asker).await?
        {
            return Err(Deny::Hold(HoldCode::RequesterAccessLost));
        }
        Ok(())
    }
}
