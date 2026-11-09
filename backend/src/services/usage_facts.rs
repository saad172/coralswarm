//! The sealed capability grants only the existing whole-org metering DTO.
//! It never raises content RLS or supplies a system content transaction.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub(crate) struct MeteringProof {
    account: Uuid,
    actor: Uuid,
    org: String,
}
impl MeteringProof {
    pub async fn verify(
        control: &mut sqlx::PgConnection,
        account: Uuid,
        actor: Uuid,
        expected_org: &str,
    ) -> Result<Self, crate::read_models::Error> {
        let org: Option<String> =
            sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
                .bind(account)
                .fetch_optional(&mut *control)
                .await?
                .flatten();
        let org = org
            .filter(|org| org == expected_org)
            .ok_or("metering plane/account mismatch")?;
        crate::read_models::access::AccessPolicy::Usage
            .compute(control, account, actor, Some(&org))
            .await?;
        Ok(Self {
            account,
            actor,
            org,
        })
    }
    pub async fn facts(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        period: DateTime<Utc>,
    ) -> Result<UsageFacts, crate::read_models::Error> {
        // Bound proof; SQL independently checks transaction actor/account and
        // tenant_meta.org_id. Only the aggregate definer reads these GUCs.
        sqlx::query("SELECT set_config('app.metering_account',$1,true),set_config('app.metering_actor',$2,true),set_config('app.metering_org',$3,true)")
            .bind(self.account.to_string()).bind(self.actor.to_string()).bind(&self.org).execute(&mut **tx).await?;
        let v: serde_json::Value = sqlx::query_scalar("SELECT read_model_org_usage_facts($1,$2)")
            .bind(self.account)
            .bind(period)
            .fetch_one(&mut **tx)
            .await?;
        Ok(serde_json::from_value(v)?)
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct UsageFacts {
    pub buckets: Vec<Bucket>,
    pub askers: Vec<Asker>,
    pub atoms_ingested: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Bucket {
    pub graph_id: Option<Uuid>,
    pub name: Option<String>,
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_creation_tokens: i64,
    pub cache_read_tokens: i64,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Asker {
    pub user_id: Option<Uuid>,
    pub asks: i64,
}
impl UsageFacts {
    pub async fn personal(
        tx: &mut Transaction<'_, Postgres>,
        account: Uuid,
        period: DateTime<Utc>,
    ) -> Result<Self, crate::read_models::Error> {
        let v: serde_json::Value = sqlx::query_scalar(PERSONAL_FACTS)
            .bind(account)
            .bind(period)
            .fetch_one(&mut **tx)
            .await?;
        Ok(serde_json::from_value(v)?)
    }
}
// history_floor: financial ingestion counts intentionally use created_at and
// account governance scope, matching the existing usage summary. These aggregate
// facts disclose no content and are not a member's evidence-admission query.
const PERSONAL_FACTS: &str = r#"
/* history_floor: financial aggregate facts intentionally retain account-wide ingestion-time semantics; no evidence content is returned. */
WITH buckets AS (SELECT u.graph_id,g.name,u.model,sum(u.input_tokens)::bigint input_tokens,sum(u.output_tokens)::bigint output_tokens,
 sum(u.cache_creation_input_tokens)::bigint cache_creation_tokens,sum(u.cache_read_input_tokens)::bigint cache_read_tokens
 FROM llm_usage u LEFT JOIN graphs g ON g.id=u.graph_id AND g.account_id=$1 WHERE u.account_id=$1 AND u.created_at >= $2 GROUP BY u.graph_id,g.name,u.model),
askers AS (SELECT user_id,count(*) asks FROM query_history WHERE account_id=$1 AND created_at >= $2 GROUP BY user_id ORDER BY asks DESC LIMIT 10)
SELECT jsonb_build_object('buckets',COALESCE((SELECT jsonb_agg(to_jsonb(b)) FROM buckets b),'[]'::jsonb),
 'askers',COALESCE((SELECT jsonb_agg(to_jsonb(a)) FROM askers a),'[]'::jsonb),
 'atoms_ingested',(SELECT count(*) FROM atoms a JOIN graphs g ON g.id=a.graph_id WHERE g.account_id=$1 AND a.created_at >= $2))
"#;
