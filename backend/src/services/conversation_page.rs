//! Pure receipt-checked conversation message page. No charging or writing handler.
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use uuid::Uuid;
pub async fn messages(
    tx: &mut Transaction<'static, Postgres>,
    actor: Uuid,
    id: Uuid,
    before: Option<i32>,
) -> Result<(Vec<Value>, bool, Option<i32>), crate::read_models::Error> {
    let mut messages = crate::store::page_sources::list_messages_scoped(
        tx,
        id,
        before,
        crate::store::CONVERSATION_MESSAGE_PAGE + 1,
    )
    .await?;
    let messages_truncated = messages.len() as i64 > crate::store::CONVERSATION_MESSAGE_PAGE;
    if messages_truncated {
        messages.remove(0);
    }
    let older_before_seq = if messages_truncated {
        messages.first().map(|m| m.seq)
    } else {
        None
    };
    let mut support_statuses = HashMap::new();
    let mut demo_statuses = HashMap::new();
    {
        let ids: Vec<Uuid> = messages
            .iter()
            .filter(|m| m.role == "assistant")
            .map(|m| m.id)
            .collect();
        match sqlx::query_as::<_,(Uuid,bool)>("SELECT r.receipt_id,EXISTS(SELECT 1 FROM jsonb_array_elements(r.support) e JOIN source_versions v ON v.graph_id=(e->>'graph_id')::uuid AND v.version_id=(e->>'source_version_id')::uuid WHERE e->>'kind'='source_passage' AND (COALESCE(v.origin->>'transport'='synthetic',false) OR COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false))) FROM answer_support_receipts r WHERE r.receipt_id=ANY($1) /* history_floor: a demo flag per receipt, no content */").bind(&ids).fetch_all(&mut **tx).await {
            Ok(rows)=>{
                let receipts:HashMap<Uuid,bool>=rows.into_iter().collect();
                for m in messages.iter().filter(|m|m.role=="assistant") {
                    let Some(&demo)=receipts.get(&m.id) else {continue};
                    let allowed=m.support_verified==Some(true);
                    // A recorded failed ask carries no answer and no evidence: it is
                    // never "complete", whatever its receipt (the question) verifies.
                    let failed=allowed && m.mode.as_deref()==Some("failed");
                    support_statuses.insert(m.id,if failed {"failed"} else if allowed {"complete"} else {"unavailable"});
                    if allowed {demo_statuses.insert(m.id,demo);}
                }
            },
            Err(_)=>return Err("conversation source read unavailable".into()),
        }
        if crate::routes::conversations::recheck_headline_message_scopes(
            tx,
            &messages,
            &mut support_statuses,
        )
        .await
        .is_err()
        {
            return Err("conversation source read unavailable".into());
        }
        for m in &mut messages {
            if m.role == "assistant"
                && !matches!(
                    support_statuses.get(&m.id).copied(),
                    Some("complete" | "failed")
                )
            {
                m.text = "[Evidence unavailable: retained support cannot be verified]".into();
                m.citations = json!([]);
                m.sources = json!([]);
                m.timing = json!({});
                m.scope_graph_ids.clear();
            }
        }
        // Answers saved before citation titling carry null titles.
        let mut roots: Vec<&mut Value> = messages
            .iter_mut()
            .filter(|m| support_statuses.get(&m.id).copied() == Some("complete"))
            .map(|m| &mut m.timing)
            .collect();
        if crate::scoped_ask::fill_citation_titles(&mut **tx, &mut roots)
            .await
            .is_err()
        {
            return Err("conversation source read unavailable".into());
        }
    }
    let feedback = if crate::services::message_feedback::enabled() {
        match crate::services::message_feedback::for_conversation(
            tx,
            actor,
            &messages.iter().map(|m| m.id).collect::<Vec<_>>(),
        )
        .await
        {
            Ok(f) => Some(f),
            Err(_) => return Err("conversation source read unavailable".into()),
        }
    } else {
        None
    };
    let msgs: Vec<Value> = messages
        .into_iter()
        .map(|m| {
            let mut msg = json!({
                "id": m.id,
                "seq": m.seq,
                "support_status": support_statuses.get(&m.id).copied().unwrap_or("unknown"),
                "failure_reason": if m.mode.as_deref()==Some("failed") {m.timing.get("failure_reason").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "question_message_id": if m.mode.as_deref()==Some("failed") {m.timing.get("question_message_id").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "contains_synthetic_demo_evidence": demo_statuses.get(&m.id).copied(),
                "role": m.role,
                "text": m.text,
                "citations": m.citations,
                "sources": m.sources,
                "mode": m.mode,
                "library_scope": if m.role=="assistant" { m.timing.get("library_scope").cloned().unwrap_or(Value::Null) } else {Value::Null},
                "retained_citations": if m.role=="assistant" {m.timing.get("retained_citations").cloned().unwrap_or(json!([]))} else {json!([])},
                "outcomes": if m.role=="assistant" {m.timing.get("outcomes").cloned().unwrap_or(json!([]))} else {json!([])},
                "legacy_evidence": if m.role=="assistant" {m.timing.get("legacy_evidence").cloned().unwrap_or(json!([]))} else {json!([])},
                "legacy_headlines": if m.role=="assistant" {m.timing.get("legacy_headlines").cloned().unwrap_or(json!([]))} else {json!([])},
                "event_time_window": if m.role=="assistant" {m.timing.get("event_time_window").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "next_recap_cursor": if m.role=="assistant" {m.timing.get("next_recap_cursor").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "untitled_legacy_count": if m.role=="assistant" {m.timing.get("untitled_legacy_count").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "temporal": if m.role=="assistant" {m.timing.get("temporal").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "temporal_state": if m.role=="assistant" {m.timing.get("temporal_state").cloned().unwrap_or(Value::Null)} else {Value::Null},
                "timing": m.timing,
                // Per-turn scope snapshot (ADR-0011 C11) so a reopened thread
                // re-renders the ocean(s) each turn was actually asked over.
                "scope_graph_ids": m.scope_graph_ids.iter().map(|g| g.to_string()).collect::<Vec<_>>(),
                "created_at": m.created_at,
            });
            if let Some(feedback) = &feedback {
                msg["feedback"] = json!(feedback.get(&m.id));
            }
            msg
        })
        .collect();

    Ok((msgs, messages_truncated, older_before_seq))
}

/// Certify optional Find cards only for this already selected message page.
/// The adjacent question can sit just outside the page: fetch exact indexed
/// sequence keys in one read, never scan or enrich the remaining history.
pub(crate) async fn attach_find_results(
    tx: &mut Transaction<'static, Postgres>,
    id: Uuid,
    graphs: &[crate::services::find_results::GraphRef],
    messages: &mut [Value],
) -> Result<(), crate::read_models::Error> {
    use crate::services::find_results;
    if !find_results::enabled() {
        return Ok(());
    }
    let eligible: Vec<(usize, i32)> = messages
        .iter()
        .enumerate()
        .filter_map(|(i, m)| {
            if m["role"] != "assistant"
                || m["support_status"] != "complete"
                || !matches!(m["mode"].as_str(), Some("synthesized" | "retrieval_only"))
            {
                return None;
            }
            let seq = i32::try_from(m["seq"].as_i64()?).ok()?.checked_sub(1)?;
            Some((i, seq))
        })
        .collect();
    if eligible.is_empty() {
        return Ok(());
    }
    let seqs: Vec<i32> = eligible.iter().map(|(_, seq)| *seq).collect();
    let questions: HashMap<i32, String> = sqlx::query_as::<_, (i32, String)>(
        "SELECT seq,text FROM conversation_messages WHERE conversation_id=$1 AND seq=ANY($2) AND role='user'"
    ).bind(id).bind(&seqs).fetch_all(&mut **tx).await?.into_iter().collect();
    let indices: Vec<usize> = eligible
        .into_iter()
        .filter_map(|(i, seq)| {
            questions
                .get(&seq)
                .filter(|q| find_results::is_find_question(q))
                .map(|_| i)
        })
        .collect();
    let payloads: Vec<&Value> = indices.iter().map(|i| &messages[*i]).collect();
    let mut tiles = find_results::answer_tiles_batch(tx, graphs, &payloads).await?;
    find_results::certify_saved_quotes(tx, graphs, &mut tiles).await?;
    for (i, tiles) in indices.into_iter().zip(tiles) {
        messages[i]["source_results"] = serde_json::to_value(tiles)?;
    }
    Ok(())
}
