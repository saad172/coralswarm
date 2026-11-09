//! Immutable blob reads for model workers. No pool or write capability enters
//! compute. The row and wrapped key come from its authorized READ ONLY snapshot.
use coralswarm_core::r2::R2Client;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;
pub struct BlobReader<'a> {
    pub(crate) r2: Option<&'a R2Client>,
    pub(crate) tenant: &'a crate::tenancy::TenantId,
}
impl BlobReader<'_> {
    pub(crate) fn org_id(&self) -> Option<&str> {
        match self.tenant {
            crate::tenancy::TenantId::Org(org) => Some(org.as_str()),
            _ => None,
        }
    }

    pub async fn text(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        account: Uuid,
        graphs: &[Uuid],
        graph: Uuid,
        inline: Option<&str>,
        key: Option<&str>,
    ) -> Result<Option<String>, super::Error> {
        if !graphs.contains(&graph) {
            return Err("blob graph outside verified model scope".into());
        }
        if let Some(inline) = inline {
            return Ok(Some(inline.to_owned()));
        }
        let (Some(r2), Some(key)) = (self.r2, key) else {
            return Ok(None);
        };
        let org = match self.tenant {
            crate::tenancy::TenantId::Org(org) => Some(org.as_str()),
            _ => None,
        };
        if !crate::source_evidence::artifact_locator_admitted(graph, key, org) {
            return Err("blob locator outside verified graph/plane".into());
        }
        let wrapped: Option<Vec<u8>> = if org.is_some() {
            sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id=$1")
                .bind(account)
                .fetch_optional(&mut **tx)
                .await?
        } else {
            sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id=$1")
                .bind(account)
                .fetch_optional(&mut **tx)
                .await?
                .flatten()
        };
        let Some(wrapped) = wrapped else {
            return Ok(None);
        };
        let dek = crate::crypto::unwrap_snapshot_dek(&wrapped)?;
        let bytes = r2.get_atom_blob(key, &dek).await?;
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    }
}
