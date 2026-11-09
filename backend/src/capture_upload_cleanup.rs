//! Cleanup of successful, newly generated native PUTs after a capture failure.
//! This journal is constructed inside the admitted capture transaction; neither
//! object identities nor transaction proofs come from a client cleanup request.
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;
use crate::{services::blob_store::PromotionBlobStore, tenancy::{extractor::Tenant, TenantId}};

pub(crate) struct UploadJournal {
    database: String,
    xid: String,
    account: Uuid,
    graph: Uuid,
    org: Option<String>,
    pub(crate) keys: Vec<String>,
}
impl UploadJournal {
    pub(crate) async fn begin(tx: &mut Transaction<'_,Postgres>, tenant:&Tenant, account:Uuid, graph:Uuid)->Result<Self,sqlx::Error> {
        let (database,xid):(String,String)=sqlx::query_as("SELECT current_database()::text,pg_current_xact_id()::text").fetch_one(&mut **tx).await?;
        Ok(Self { database,xid,account,graph,org:tenant.org_context().map(|v|v.org_id.clone()),keys:vec![] })
    }
    pub(crate) async fn rollback_and_retain(self,tx:Transaction<'_,Postgres>,tenant:&Tenant,blobs:Option<&dyn PromotionBlobStore>)->&'static str {
        // An unsuccessful ROLLBACK remains ambiguous until the database itself
        // reports aborted. The receipt does not trust this transport result.
        let _=tx.rollback().await;
        self.retain_and_attempt(tenant,blobs).await
    }
    /// Must run only after the capture transaction has been released. The SQL
    /// service independently verifies outcome; network ambiguity never means
    /// rollback. Durable queue insertion precedes the optional transport retry.
    pub(crate) async fn retain_and_attempt(self,tenant:&Tenant,blobs:Option<&dyn PromotionBlobStore>)->&'static str {
        if self.keys.is_empty() { return "completed"; }
        let Some(pool)=&tenant.pool else{return "incomplete"};
        let plane=tenant.org_context().map(|v|TenantId::Org(v.org_id.clone())).unwrap_or(TenantId::Consumer);
        let Ok(mut tx)=scoped(pool,&plane,self.account).await else{return "incomplete"};
        let mut ids=Vec::new();
        for key in &self.keys {
            let Some(obj)=key.rsplit('/').next().and_then(|v|Uuid::parse_str(v).ok()) else{return "incomplete"};
            let result=sqlx::query_scalar::<_,Uuid>("SELECT library_record_unattached_upload($1,$2,$3,$4,$5,$6,$7)")
                .bind(self.account).bind(self.graph).bind(obj).bind(&self.org).bind(key).bind(&self.database).bind(&self.xid).fetch_one(&mut *tx).await;
            match result {Ok(id)=>ids.push(id),Err(_)=>return "incomplete"}
        }
        if tx.commit().await.is_err(){return "incomplete";}
        // A durable receipt now exists even if transport is absent or fails.
        match drain_account(pool,&plane,self.account,blobs,Some(&ids)).await {
            Ok(_) => {
                let Ok(mut tx)=scoped(pool,&plane,self.account).await else{return "queued"};
                let states=sqlx::query_as::<_,(String,bool)>("SELECT state,abort_verified FROM unattached_capture_uploads WHERE cleanup_id=ANY($1)").bind(&ids).fetch_all(&mut *tx).await;
                let _=tx.rollback().await;
                match states {Ok(v) if v.len()==ids.len() && v.iter().all(|(s,_)|s=="deleted")=>"completed",Ok(v) if v.iter().any(|(s,_)|s=="committed" || s=="retained_reference")=>"retained",Ok(v) if v.iter().all(|(_,verified)|*verified)=>"queued",_=>"pending_ambiguity"}
            }
            Err(_)=>"queued",
        }
    }
}
async fn scoped(pool:&PgPool,plane:&TenantId,account:Uuid)->Result<Transaction<'static,Postgres>,String>{
    match plane {TenantId::Consumer=>crate::tenancy::scope::begin_account_scoped(pool,account).await,TenantId::Org(_)=>crate::tenancy::scope::begin_tenant_system_scoped(pool,account).await}.map_err(|_|"capture cleanup scope failed".into())
}
/// Uses one existing scoped executor, including across DELETE. No second pool
/// acquisition occurs while that transaction or its exact-object lock is held.
pub async fn drain_account(pool:&PgPool,plane:&TenantId,account:Uuid,blobs:Option<&dyn PromotionBlobStore>,exact_ids:Option<&[Uuid]>)->Result<usize,String>{
    let mut tx=scoped(pool,plane,account).await?;
    let ids:Vec<Uuid>=sqlx::query_scalar("SELECT cleanup_id FROM unattached_capture_uploads WHERE account_id=$1 AND state IN('pending','retained_reference') AND ($2::uuid[] IS NULL OR cleanup_id=ANY($2)) AND ($2::uuid[] IS NOT NULL OR attempted_at IS NULL OR attempted_at<now()-interval '30 seconds') ORDER BY attempted_at NULLS FIRST,created_at LIMIT 20")
        .bind(account).bind(exact_ids).fetch_all(&mut *tx).await.map_err(|_|"capture cleanup pending lookup failed".to_string())?;
    let mut deleted=0;
    for id in ids {
        let ready:bool=sqlx::query_scalar("SELECT library_stage_unattached_upload($1)").bind(id).fetch_one(&mut *tx).await.map_err(|_|"capture cleanup proof failed".to_string())?;
        if !ready {continue;}
        let key:String=sqlx::query_scalar("SELECT blob_key FROM unattached_capture_uploads WHERE cleanup_id=$1").bind(id).fetch_one(&mut *tx).await.map_err(|_|"capture cleanup locator lookup failed".to_string())?;
        let success=match blobs {Some(b)=>matches!(b.delete_blob(&key).await,Ok(())|Err(coralswarm_core::r2::R2Error::NotFound(_))),None=>false};
        sqlx::query("SELECT library_finish_unattached_upload($1,$2)").bind(id).bind(success).execute(&mut *tx).await.map_err(|_|"capture cleanup delivery save failed".to_string())?;
        if success {deleted+=1;}
    }
    tx.commit().await.map_err(|_|"capture cleanup delivery commit failed".to_string())?;
    Ok(deleted)
}
