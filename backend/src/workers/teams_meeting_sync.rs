//! Durable selected-meeting retry, independent of calendar capture and its off switch.
use crate::{
    connectors::{MicrosoftConnector, TokenOwner},
    tenancy::{
        scope::{begin_account_scoped, begin_tenant_scoped},
        TenantId,
    },
    workers::tenant_harness::{Tick, WorkerTenant},
    AppState,
};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

pub const POLL_INTERVAL: Duration = Duration::from_secs(5 * 60);

async fn claim_due_meetings(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Vec<(String, Uuid)>> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let rows=sqlx::query_as("UPDATE teams_meeting_captures c SET next_poll_at=now()+interval '5 minutes' FROM graphs g WHERE g.id=c.graph_id AND c.next_poll_at<=now() AND c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.aad_id=$4 AND c.revoked_at IS NULL AND c.occurrence_start<=now() AND c.occurrence_end+interval '24 hours'>=now() AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) AND (c.source<>'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active)) RETURNING c.meeting_id,c.graph_id")
 .bind(account).bind(member).bind(tenant).bind(aad).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(rows)
}

// A shared enqueue may wake us early. Preserve the durable due instant rather
// than escalating idle backoff or restarting a fresh five-minute timer.
async fn next_poll_delay(
    pool: &PgPool,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
) -> sqlx::Result<Option<Duration>> {
    let mut tx = begin_tenant_scoped(pool, account, member)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let due:Option<chrono::DateTime<chrono::Utc>>=sqlx::query_scalar("SELECT min(greatest(c.next_poll_at,c.occurrence_start)) FROM teams_meeting_captures c JOIN graphs g ON g.id=c.graph_id WHERE c.account_id=$1 AND c.user_id=$2 AND c.tenant_id=$3 AND c.aad_id=$4 AND c.revoked_at IS NULL AND c.occurrence_end+interval '24 hours'>=now() AND g.account_id=$1 AND g.archived_at IS NULL AND ((g.kind='member' AND g.owner_user_id=$2) OR (g.kind='personal' AND g.owner_user_id IS NULL)) AND (c.source<>'meeting_bot' OR EXISTS(SELECT 1 FROM teams_meeting_installations i WHERE i.account_id=c.account_id AND i.tenant_id=c.tenant_id AND i.installation_id=c.installation_id AND i.active))").bind(account).bind(member).bind(tenant).bind(aad).fetch_one(&mut *tx).await?;
    tx.rollback().await?;
    Ok(due.map(|at| {
        Duration::from_millis((at - chrono::Utc::now()).num_milliseconds().max(1) as u64)
    }))
}

// Shared execution seam: the durable member-scoped selection is the only
// producer of provider work. Calendar settings and cached health are absent.
async fn poll_selected<F, Fut>(selected: Vec<(String, Uuid)>, mut pull: F) -> Result<usize, String>
where
    F: FnMut(String, Uuid) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let attempted = selected.len();
    for (meeting, graph) in selected {
        if let Err(error) = pull(meeting, graph).await {
            tracing::warn!(%error,"selected meeting poll failed");
        }
    }
    Ok(attempted)
}

async fn provider_result<T>(
    result: Result<T, crate::connectors::teams_targeted_transcripts::Error>,
    control: &PgPool,
    account: Uuid,
    health_user: Option<Uuid>,
) -> Result<T, String> {
    if matches!(&result,Err(crate::connectors::teams_targeted_transcripts::Error::Graph{status:403,code:Some(code)}) if code=="GraphAccessToTranscriptsDisabled")
    {
        let _ = crate::services::connector_health::note_transcript_access_disabled(
            control,
            account,
            health_user,
        )
        .await;
    }
    result.map_err(|e| e.to_string())
}

/// Authority that must still hold immediately before every provider read and write.
enum Lifecycle<'a> {
    Org(&'a str),
    Personal,
}

impl Lifecycle<'_> {
    /// Transcript-access health is per member under an org and account-wide for a
    /// personal account (matching `skip_user` in the push and calendar paths).
    fn health_user(&self, member: Uuid) -> Option<Uuid> {
        match self {
            Lifecycle::Org(_) => Some(member),
            Lifecycle::Personal => None,
        }
    }
}

/// The consent's own plane, rechecked from authoritative state: org membership,
/// org flag and member grant; or the personal account's live owner and current
/// work/school account-wide grant. Both must still name this tenant/object.
async fn lifecycle_current(
    state: &Arc<AppState>,
    control: &PgPool,
    lifecycle: &Lifecycle<'_>,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
) -> bool {
    use crate::routes::teams_onboarding::{capture_identity, identity, ExpectedPlane};
    match lifecycle {
        Lifecycle::Org(org) => {
            matches!(crate::middleware::auth::live_org_membership_for_capture(control,org,member).await,Ok(Some((live,_))) if live==account)
                && crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, org).await
                && identity(control, account, member).await.ok().flatten()
                    == Some((tenant.to_string(), aad.to_string()))
        }
        Lifecycle::Personal => matches!(
            capture_identity(control, account, member, ExpectedPlane::Personal).await,
            Ok(found) if found.tenant == tenant && found.aad == aad
        ),
    }
}

/// Pull the already-claimed selections for one member with `access_token`.
async fn pull_member(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
    lifecycle: &Lifecycle<'_>,
    account: Uuid,
    member: Uuid,
    tenant: &str,
    aad: &str,
    access_token: &str,
    selected: Vec<(String, Uuid)>,
) -> Result<usize, String> {
    let (tenant, aad) = (tenant.to_string(), aad.to_string());
        crate::graph_rate_limit::scoped(crate::graph_rate_limit::rate_key(account,&tenant),poll_selected(selected,|meeting,graph| {
   let token=access_token.to_string();let tenant=tenant.clone();let aad=aad.clone();
   async move {
    use crate::connectors::teams_targeted_transcripts::{self as provider,Target};
    use crate::services::teams_meeting_capture as capture;
    let result=async {
     let Some(selected_capture)=capture::status(&t.pool,account,member,&tenant,&aad,None,Some(&meeting)).await.map_err(|e|e.to_string())? else {return Ok(());};
     let Some(lease)=selected_capture.lease().filter(|_|selected_capture.graph_id==graph) else {return Ok(());};

     // Recheck lifecycle before any metadata/content request, and again at ingest.
     if !lifecycle_current(state,control,lifecycle,account,member,&tenant,&aad).await {return Ok(());}
     if crate::services::teams_meeting_capture::active_graph(&t.pool,account,member,&tenant,&aad,&meeting).await.map_err(|e|e.to_string())?!=Some(graph){return Ok(());}
     if !capture::lease_active(&t.pool,account,member,&tenant,&aad,&meeting,lease).await.map_err(|e|e.to_string())? {return Ok(());}
     let resolved=provider::resolve(&state.http_client,&token,Target::MeetingId(meeting.clone())).await;
     let verified=match resolved {
      Ok(m)=>m,
      Err(error)=>{
       let failed=Err(error);
       let _=capture::observe_fetch(&t.pool,account,member,&tenant,&aad,&meeting,lease,&failed).await.map_err(|e|e.to_string())?;
       return provider_result(failed,control,account,lifecycle.health_user(member)).await.map(|_|());
      }
     };
     // The authorized occurrence is the STORED window, verified when the member
     // consented. The onlineMeeting's own bounds are not used: a recurring series
     // shares one meeting ID, so only this occurrence's window may be fetched.
     let (start,end)=(selected_capture.occurrence_start,selected_capture.occurrence_end);
     let mut verified=verified;
     verified.start=Some(start.to_rfc3339());
     verified.end=Some(end.to_rfc3339());
     verified.transcript_end=selected_capture.transcript_end;
     if crate::services::teams_meeting_capture::matches_occurrence(&t.pool,account,member,&tenant,&aad,&meeting,start,end).await.map_err(|e|e.to_string())?!=Some(graph){return Ok(());}
     if !lifecycle_current(state,control,lifecycle,account,member,&tenant,&aad).await {return Ok(());}
     if !capture::lease_active(&t.pool,account,member,&tenant,&aad,&meeting,lease).await.map_err(|e|e.to_string())? {return Ok(());}
     let fetched_result=provider::fetch_verified(&state.http_client,&token,verified).await;
     if !capture::observe_fetch(&t.pool,account,member,&tenant,&aad,&meeting,lease,&fetched_result).await.map_err(|e|e.to_string())? {return Ok(());}
     let fetched=provider_result(fetched_result,control,account,lifecycle.health_user(member)).await?;
     let _=crate::services::connector_health::note_transcript_access_recovered(control,account,lifecycle.health_user(member)).await;
     for item in fetched.items {if !crate::routes::graph_webhook::ingest_message_stored(state,account,member,graph,item).await{return Err("Selected meeting ingest was not accepted".into());}}
     Ok(())
    }.await;

    result
   }
  })).await
}

/// Personal accounts evaluated per consumer tick, soonest-due first.
const PERSONAL_ACCOUNTS_PER_TICK: i32 = 100;

/// Consumer plane: only unrevoked, personal-account, explicitly approved captures
/// are enumerated (`resolve_teams_personal_capture_due`, ids and a due instant
/// only). No calendar discovery, no org-membership shortcut. An account with nothing
/// due is deferred without ever touching its credential; a due one is authorized
/// again under its real owner's scoped transaction before the grant is decrypted.
async fn tick_personal(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    let candidates: Vec<(Uuid, Uuid, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT account_id, user_id, due_at FROM resolve_teams_personal_capture_due($1)",
    )
    .bind(PERSONAL_ACCOUNTS_PER_TICK)
    .fetch_all(control)
    .await
    .map_err(|e| e.to_string())?;
    let mut attempted = 0;
    let mut deferred: Option<Duration> = None;
    let now = chrono::Utc::now();
    let defer_until = |deferred: &mut Option<Duration>, at: chrono::DateTime<chrono::Utc>| {
        let wait = Duration::from_millis((at - chrono::Utc::now()).num_milliseconds().max(1) as u64);
        *deferred = Some(deferred.map_or(wait, |existing| existing.min(wait)));
    };
    for (account, member, due_at) in candidates {
        if due_at > now {
            defer_until(&mut deferred, due_at);
            continue;
        }
        let identity = match crate::routes::teams_onboarding::capture_identity(
            control,
            account,
            member,
            crate::routes::teams_onboarding::ExpectedPlane::Personal,
        )
        .await
        {
            Ok(identity) => identity,
            Err(_) => continue,
        };
        let (tenant, aad) = identity.pair();
        // Never decrypt/refresh the grant for an owner with no due consent.
        let selected = claim_due_meetings(&t.pool, account, member, &tenant, &aad)
            .await
            .map_err(|e| e.to_string())?;
        if selected.is_empty() {
            if let Some(wait) = next_poll_delay(&t.pool, account, member, &tenant, &aad)
                .await
                .map_err(|e| e.to_string())?
            {
                deferred = Some(deferred.map_or(wait, |existing| existing.min(wait)));
            }
            continue;
        }
        let result = async {
            let dek = crate::crypto::DekPlane::resolve_for_account(state, control, account)
                .await
                .map_err(|e| e.to_string())?;
            let Some(mut creds) = crate::routes::connectors::load_creds_bg(
                control,
                &dek,
                account,
                "microsoft",
                TokenOwner::AccountWide,
            )
            .await
            .map_err(|e| e.to_string())?
            else {
                return Ok::<usize, String>(0);
            };
            // The decrypted row must be the exact directory the verified identity named.
            if !creds.provider_workspace_id.eq_ignore_ascii_case(&tenant) {
                return Ok(0);
            }
            MicrosoftConnector
                .refresh_if_needed(&state.http_client, &mut creds)
                .await
                .map_err(|e| e.to_string())?;
            if let Some(bundle) = creds.refreshed.as_ref() {
                crate::routes::oauth::store_token_bundle(
                    control,
                    &dek,
                    account,
                    "microsoft",
                    TokenOwner::AccountWide,
                    bundle,
                )
                .await
                .map_err(|response| {
                    format!("Microsoft credential persistence failed ({})", response.status())
                })?;
            }
            pull_member(
                t,
                control,
                state,
                &Lifecycle::Personal,
                account,
                member,
                &tenant,
                &aad,
                &creds.access_token,
                selected,
            )
            .await
        }
        .await;
        match result {
            Ok(count) => attempted += count,
            Err(error) => {
                attempted += 1;
                tracing::warn!(%account,%member,%error,"selected personal meeting refresh failed");
            }
        }
    }
    Ok(match (attempted, deferred) {
        (0, Some(wait)) => Tick::until_next_work(0, wait),
        (0, None) => Tick::idle(),
        (processed, Some(wait)) => Tick::until_next_work(processed, wait.min(POLL_INTERVAL)),
        (processed, None) => Tick::worked(processed),
    })
}

pub async fn tick(
    t: &WorkerTenant,
    control: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    let org = match &t.tenant {
        TenantId::Org(org) => org,
        TenantId::Consumer => return tick_personal(t, control, state).await,
    };
    let Some(account): Option<Uuid> =
        sqlx::query_scalar("SELECT account_id FROM tenant_databases WHERE clerk_org_id=$1")
            .bind(org)
            .fetch_optional(control)
            .await
            .map_err(|e| e.to_string())?
    else {
        return Ok(Tick::idle());
    };
    if !crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, org).await {
        return Ok(Tick::idle());
    }
    let mut tx = begin_account_scoped(control, account)
        .await
        .map_err(|e| e.to_string())?;
    let owners:Vec<Uuid>=sqlx::query_scalar("SELECT DISTINCT ot.owner_user_id FROM oauth_tokens ot WHERE ot.account_id=$1 AND ot.provider='microsoft' AND ot.owner_user_id IS NOT NULL AND EXISTS(SELECT 1 FROM account_members am WHERE am.account_id=ot.account_id AND am.user_id=ot.owner_user_id)").bind(account).fetch_all(&mut *tx).await.map_err(|e|e.to_string())?;
    tx.rollback().await.map_err(|e| e.to_string())?;
    // Outlook compose capture v2: find/follow the meetings members are scheduling. Bounded
    // and non-fatal so it can never hold up polling of already-authorized captures.
    let scheduling_wake = tokio::time::timeout(
        Duration::from_secs(60),
        crate::workers::outlook_schedule::run_tick(state, control, &t.pool, account, org, &owners),
    )
    .await
    .unwrap_or_else(|_| {
        tracing::warn!("Outlook scheduling pass timed out");
        Some(Duration::from_secs(30))
    });
    let mut attempted = 0;
    let mut deferred: Option<Duration> = scheduling_wake;
    for member in owners {
        if !matches!(crate::middleware::auth::live_org_membership_for_capture(control,org,member).await,Ok(Some((live,_))) if live==account)
        {
            continue;
        }
        let (tenant, aad) =
            match crate::routes::teams_onboarding::identity(control, account, member).await {
                Ok(Some(i)) => i,
                _ => continue,
            };
        // Never decrypt/refresh an OAuth token for a member with no due consent.
        let selected = claim_due_meetings(&t.pool, account, member, &tenant, &aad)
            .await
            .map_err(|e| e.to_string())?;
        if selected.is_empty() {
            if let Some(wait) = next_poll_delay(&t.pool, account, member, &tenant, &aad)
                .await
                .map_err(|e| e.to_string())?
            {
                deferred = Some(deferred.map_or(wait, |existing| existing.min(wait)));
            }
            continue;
        }
        let member_result = async {
        let dek = crate::crypto::DekPlane::resolve_for_org(state, control, account, Some(org))
            .await
            .map_err(|e| e.to_string())?;
        let Some(mut creds) = crate::routes::connectors::load_creds_bg(
            control,
            &dek,
            account,
            "microsoft",
            TokenOwner::Member(member),
        )
        .await
        .map_err(|e| e.to_string())?
        else {
            return Ok::<usize,String>(0);
        };
        MicrosoftConnector
            .refresh_if_needed(&state.http_client, &mut creds)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(bundle) = creds.refreshed.as_ref() {
            crate::routes::oauth::store_token_bundle(
                control,
                &dek,
                account,
                "microsoft",
                TokenOwner::Member(member),
                bundle,
            )
            .await
            .map_err(|response| format!("Microsoft credential persistence failed ({})", response.status()))?;
        }
        let count=pull_member(t,control,state,&Lifecycle::Org(org),account,member,&tenant,&aad,&creds.access_token,selected).await?;
        Ok::<usize,String>(count)
        }.await;
        match member_result {
            Ok(count) => attempted += count,
            Err(error) => {
                attempted += 1;
                tracing::warn!(%account,%member,%error,"selected meeting member refresh failed");
            }
        }
    }
    Ok(match (attempted, deferred) {
        (0, Some(wait)) => Tick::until_next_work(0, wait),
        (0, None) => Tick::idle(),
        (processed, Some(wait)) => Tick::until_next_work(processed, wait.min(POLL_INTERVAL)),
        (processed, None) => Tick::worked(processed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Executor;
    use std::sync::Mutex;

    fn meeting_fixture_port(value: Result<String, std::env::VarError>) -> u16 {
        let value = match value {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return 15444,
            Err(std::env::VarError::NotUnicode(_)) => panic!("meeting fixture port must be Unicode"),
        };
        assert!(
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
            "meeting fixture port must be nonempty ASCII decimal"
        );
        let port: u16 = value.parse().expect("meeting fixture port must fit a TCP port");
        assert!(port > 0, "meeting fixture port must be nonzero");
        port
    }

    #[test]
    fn meeting_fixture_port_preserves_hosted_default_and_accepts_owned_lane() {
        assert_eq!(meeting_fixture_port(Err(std::env::VarError::NotPresent)), 15444);
        assert_eq!(meeting_fixture_port(Ok("5442".into())), 5442);
        assert_eq!(meeting_fixture_port(Ok("65535".into())), 65535);
    }

    #[test]
    fn meeting_fixture_port_rejects_invalid_explicit_values() {
        for value in ["", " ", "+5442", "-1", "0", "65536", "５４４２", "5442\n"] {
            assert!(std::panic::catch_unwind(|| meeting_fixture_port(Ok(value.into()))).is_err());
        }
        assert!(std::panic::catch_unwind(|| {
            meeting_fixture_port(Err(std::env::VarError::NotUnicode(std::ffi::OsString::new())))
        }).is_err());
    }

    /// Production polling selection runs as a real NOBYPASSRLS role while
    /// calendar capture is disabled and transcript health is cached disabled.
    /// The injected provider transport is the same execution seam used by tick.
    #[tokio::test]
    async fn selected_poll_ignores_calendar_off_and_cached_health_but_confines_provider_reads_pg() {
        let base = std::env::var("TEAMS_MEETING_TEST_BASE_URL")
            .expect("isolated meeting PostgreSQL fixture required");
        let mut url = reqwest::Url::parse(&base).unwrap();
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(
            url.port(),
            Some(meeting_fixture_port(std::env::var("TEAMS_MEETING_TEST_OWNED_PORT")))
        );
        url.set_path("/postgres");
        let admin = PgPool::connect(url.as_str()).await.unwrap();
        let db = crate::test_db::TestDatabase::from_url(url.as_str(), "meeting_poll");
        let role = db.role("meeting_poll_app");
        // A distinct UUID password supports SCRAM fixtures without elevated roles.
        let role_password = Uuid::new_v4().simple().to_string();
        admin
            .execute(format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{role_password}'").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        url.set_path(&format!("/{db}"));
        let fixture = PgPool::connect(url.as_str()).await.unwrap();
        fixture.execute("CREATE TABLE graphs(id uuid PRIMARY KEY,account_id uuid NOT NULL,owner_user_id uuid,kind text,archived_at timestamptz); ALTER TABLE graphs ENABLE ROW LEVEL SECURITY; ALTER TABLE graphs FORCE ROW LEVEL SECURITY; CREATE POLICY own_graph ON graphs USING(account_id::text=current_setting('app.account_id',true) AND owner_user_id::text=current_setting('app.user_id',true)); CREATE TABLE microsoft_calendar_sync_state(account_id uuid,owner_user_id uuid,sync_enabled boolean); CREATE TABLE connector_health(account_id uuid,owner_user_id uuid,disabled boolean);").await.unwrap();
        fixture
            .execute(include_str!(
                "../../migrations_control/0260_teams_meeting_captures.sql"
            ))
            .await
            .unwrap();
        fixture
            .execute(include_str!(
                "../../migrations_tenant/0106_teams_meeting_capture_status.sql"
            ))
            .await
            .unwrap();
        fixture
            .execute(include_str!(
                "../../migrations_tenant/0108_teams_meeting_capture_transcript_end.sql"
            ))
            .await
            .unwrap();
        // Minimal audit fixture: consent/Stop still commit their required audit row.
        fixture.execute("CREATE TABLE connector_audit_log(account_id uuid,actor_user_id uuid,actor_kind text,action text,provider text,subject text,detail jsonb)").await.unwrap();
        fixture.execute(format!("GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON graphs,teams_meeting_captures,teams_meeting_installations TO {role}; GRANT UPDATE,INSERT ON teams_meeting_captures TO {role}; GRANT INSERT ON connector_audit_log TO {role}").as_str()).await.unwrap();
        let account = Uuid::new_v4();
        let member = Uuid::new_v4();
        let other = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let other_graph = Uuid::new_v4();
        let tenant = "fb2a6e39-6614-4c56-ab1e-15d6d5069ad6";
        let aad = "d2c12dfd-3d6c-445b-91e4-fd4b88c2de50";
        for (id, owner) in [(graph, member), (other_graph, other)] {
            sqlx::query("INSERT INTO graphs VALUES($1,$2,$3,'member',NULL)")
                .bind(id)
                .bind(account)
                .bind(owner)
                .execute(&fixture)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO microsoft_calendar_sync_state VALUES($1,$2,false); ")
            .bind(account)
            .bind(member)
            .execute(&fixture)
            .await
            .unwrap();
        sqlx::query("INSERT INTO connector_health VALUES($1,$2,true)")
            .bind(account)
            .bind(member)
            .execute(&fixture)
            .await
            .unwrap();
        for (meeting, owner, destination, age, revoked) in [
            ("selected", member, graph, 0, false),
            ("other-member", other, other_graph, 0, false),
            ("expired", member, graph, 48, false),
            ("revoked", member, graph, 0, true),
        ] {
            sqlx::query("INSERT INTO teams_meeting_captures(account_id,user_id,tenant_id,aad_id,meeting_id,graph_id,source,occurrence_start,occurrence_end,revoked_at) VALUES($1,$2,$3,$4,$5,$6,'explicit_meeting',now()-interval '2 hours'-make_interval(hours=>$7),now()-interval '1 hour'-make_interval(hours=>$7),CASE WHEN $8 THEN now() ELSE NULL END)").bind(account).bind(owner).bind(tenant).bind(aad).bind(meeting).bind(destination).bind(age).bind(revoked).execute(&fixture).await.unwrap();
        }
        url.set_username(&role).unwrap();
        url.set_password(Some(&role_password)).unwrap();
        let app = PgPool::connect(url.as_str()).await.unwrap();
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname=current_user"
        )
        .fetch_one(&app)
        .await
        .unwrap());
        // The application cannot even read calendar/health tables: polling has
        // no hidden dependency on either disabled flag.
        use crate::connectors::teams_targeted_transcripts::{Error, Fetched};
        use crate::services::teams_meeting_capture as capture;
        let initial = capture::status(&app, account, member, tenant, aad, None, Some("selected"))
            .await
            .unwrap()
            .unwrap();
        let lease = initial.lease().unwrap();
        assert!(initial.last_checked_at.is_none());
        assert_eq!(initial.status, "active");
        let serialized = serde_json::to_value(&initial).unwrap();
        assert!(serialized.get("capture_generation").is_none());
        assert!(serialized.get("graph_id").is_none());
        // Known/guessed capture IDs do not bypass member, account or grant binding.
        for (who, account_id, identity, id) in [
            (other, account, aad, initial.capture_id),
            (member, Uuid::new_v4(), aad, initial.capture_id),
            (member, account, "other-aad", initial.capture_id),
            (member, account, aad, Uuid::new_v4()),
        ] {
            assert!(
                capture::status(&app, account_id, who, tenant, identity, Some(id), None)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert!(
            capture::status(&app, account, member, tenant, aad, None, None)
                .await
                .unwrap()
                .is_none()
        );
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let received = requests.clone();
        let server = axum::Router::new().route(
            "/selected",
            axum::routing::get(move || {
                let received = received.clone();
                async move {
                    received.lock().unwrap().push("selected".into());
                    axum::Json(serde_json::json!({"value":[]}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, server).await.unwrap();
        });
        let client = reqwest::Client::new();
        let count = poll_selected(
            claim_due_meetings(&app, account, member, tenant, aad)
                .await
                .unwrap(),
            |meeting, destination| {
                let client = client.clone();
                let app = app.clone();
                async move {
                    assert!(
                        capture::status(
                            &app,
                            account,
                            member,
                            tenant,
                            aad,
                            Some(lease.capture_id),
                            None
                        )
                        .await
                        .unwrap()
                        .unwrap()
                        .last_checked_at
                        .is_none(),
                        "claiming due consent is not a provider attempt"
                    );
                    assert_eq!(destination, graph);
                    client
                        .get(format!("http://{addr}/{meeting}"))
                        .send()
                        .await
                        .map_err(|e| e.to_string())?
                        .error_for_status()
                        .map_err(|e| e.to_string())?;
                    assert!(capture::observe_fetch(
                        &app,
                        account,
                        member,
                        tenant,
                        aad,
                        &meeting,
                        lease,
                        &Ok(Fetched {
                            meeting_id: meeting.clone(),
                            items: vec![]
                        })
                    )
                    .await
                    .unwrap());
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(
            count, 1,
            "pending selected poll counts as work even before an artifact exists"
        );
        assert_eq!(*requests.lock().unwrap(), vec!["selected"]);
        let checked = capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(checked.last_checked_at.is_some());
        assert_eq!(checked.last_retrieved_at, checked.last_checked_at);
        assert_eq!(checked.retrieved_transcript_count, 0);
        let item = crate::connectors::NormalizedItem {
            source_type: "teams_transcript".into(),
            source_id: "selected:fixture".into(),
            content: "Harmless local regression transcript.".into(),
            participants: vec![],
            timestamp: initial.occurrence_start,
            scope: None,
            thread_id: Some("selected".into()),
            channel_id: None,
            author_id: None,
            author_name: None,
        };
        assert!(capture::observe_fetch(
            &app,
            account,
            member,
            tenant,
            aad,
            "selected",
            lease,
            &Ok(Fetched {
                meeting_id: "selected".into(),
                items: vec![item.clone()]
            })
        )
        .await
        .unwrap());
        let retrieved = capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(retrieved.retrieved_transcript_count, 1);
        assert!(retrieved.last_retrieved_at >= checked.last_retrieved_at);
        assert!(
            !capture::observe_fetch(
                &app,
                account,
                member,
                tenant,
                aad,
                "selected",
                lease,
                &Ok(Fetched {
                    meeting_id: "different-private-meeting".into(),
                    items: vec![item]
                })
            )
            .await
            .unwrap(),
            "other provider identity must not stamp selected status"
        );
        let provider_error = Err(Error::Graph {
            status: 403,
            code: Some("PRIVATE_PROVIDER_CANARY".into()),
        });
        assert!(capture::observe_fetch(
            &app,
            account,
            member,
            tenant,
            aad,
            "selected",
            lease,
            &provider_error
        )
        .await
        .unwrap());
        let failed = capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(failed.last_error_code.as_deref(), Some("permission_denied"));
        assert_eq!(failed.last_retrieved_at, retrieved.last_retrieved_at);
        assert_eq!(failed.retrieved_transcript_count, 1);
        assert!(failed.last_checked_at >= checked.last_checked_at);
        assert!(!serde_json::to_string(&failed)
            .unwrap()
            .contains("PRIVATE_PROVIDER_CANARY"));
        // Active idempotent Enable preserves its existing generation/progress.
        capture::authorize(
            &app,
            account,
            member,
            tenant,
            aad,
            "selected",
            graph,
            initial.occurrence_start,
            initial.occurrence_end,
            "meeting_addin",
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let unchanged = capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(unchanged.capture_generation, lease.capture_generation);
        assert_eq!(unchanged.last_checked_at, failed.last_checked_at);

        let first_wait = next_poll_delay(&app, account, member, tenant, aad)
            .await
            .unwrap()
            .unwrap();
        assert!(first_wait > Duration::ZERO && first_wait <= POLL_INTERVAL);
        let repeated_wait = next_poll_delay(&app, account, member, tenant, aad)
            .await
            .unwrap()
            .unwrap();
        assert!(
            repeated_wait <= first_wait,
            "shared wake preserves original durable due instant"
        );
        assert_eq!(
            poll_selected(
                claim_due_meetings(&app, account, member, tenant, aad)
                    .await
                    .unwrap(),
                |_, _| async {
                    panic!("shared self-wake bypassed cooldown");
                    #[allow(unreachable_code)]
                    Ok(())
                }
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(requests.lock().unwrap().len(), 1);
        sqlx::query(
            "UPDATE teams_meeting_captures SET next_poll_at=now() WHERE meeting_id='selected'",
        )
        .execute(&fixture)
        .await
        .unwrap();
        let next = poll_selected(
            claim_due_meetings(&app, account, member, tenant, aad)
                .await
                .unwrap(),
            |meeting, destination| {
                let client = client.clone();
                async move {
                    assert_eq!(destination, graph);
                    client
                        .get(format!("http://{addr}/{meeting}"))
                        .send()
                        .await
                        .map_err(|e| e.to_string())?
                        .error_for_status()
                        .map_err(|e| e.to_string())?;
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(next, 1);
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert!(claim_due_meetings(
            &app,
            account,
            member,
            tenant,
            "bd7811d4-6141-4eb6-ad7e-482cddbb2228"
        )
        .await
        .unwrap()
        .is_empty());
        assert!(
            claim_due_meetings(&app, Uuid::new_v4(), member, tenant, aad)
                .await
                .unwrap()
                .is_empty()
        );
        sqlx::query(
            "UPDATE teams_meeting_captures SET revoked_at=now(),next_poll_at=now() WHERE meeting_id='selected'",
        )
        .execute(&fixture)
        .await
        .unwrap();
        assert_eq!(
            poll_selected(
                claim_due_meetings(&app, account, member, tenant, aad)
                    .await
                    .unwrap(),
                |_, _| async {
                    panic!("revoked selection reached provider");
                    #[allow(unreachable_code)]
                    Ok(())
                }
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert!(next_poll_delay(&app, account, member, tenant, aad)
            .await
            .unwrap()
            .is_none());
        let stopped = capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(stopped.status, "stopped");
        assert!(stopped.lease().is_none());
        assert_eq!(stopped.last_checked_at, failed.last_checked_at);
        assert!(
            !capture::observe_fetch(
                &app,
                account,
                member,
                tenant,
                aad,
                "selected",
                lease,
                &Ok(Fetched {
                    meeting_id: "selected".into(),
                    items: vec![]
                })
            )
            .await
            .unwrap(),
            "Stop rejects an in-flight observation"
        );
        capture::authorize(
            &app,
            account,
            member,
            tenant,
            aad,
            "selected",
            graph,
            initial.occurrence_start,
            initial.occurrence_end,
            "meeting_addin",
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let reopened = capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(reopened.status, "active");
        assert_ne!(reopened.capture_generation, lease.capture_generation);
        assert!(reopened.last_checked_at.is_none());
        assert!(reopened.last_retrieved_at.is_none());
        assert_eq!(reopened.retrieved_transcript_count, 0);
        assert!(reopened.last_error_code.is_none());
        assert!(
            !capture::lease_active(&app, account, member, tenant, aad, "selected", lease)
                .await
                .unwrap()
        );
        assert!(
            !capture::observe_fetch(
                &app,
                account,
                member,
                tenant,
                aad,
                "selected",
                lease,
                &provider_error
            )
            .await
            .unwrap(),
            "old generation cannot stamp a new Enable"
        );
        assert!(capture::status(
            &app,
            account,
            member,
            tenant,
            aad,
            Some(lease.capture_id),
            None
        )
        .await
        .unwrap()
        .unwrap()
        .last_checked_at
        .is_none());
        // Microsoft credentials are not an authority prerequisite for owner-only
        // ledger reads or negative-authority Stop. These helpers use no grant.
        assert!(
            capture::owned_status(&app, account, member, lease.capture_id)
                .await
                .unwrap()
                .is_some()
        );
        for (account_id, who) in [(account, other), (Uuid::new_v4(), member)] {
            assert!(
                capture::owned_status(&app, account_id, who, lease.capture_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                !capture::revoke_owned(&app, account_id, who, lease.capture_id)
                    .await
                    .unwrap()
            );
        }
        assert!(
            capture::revoke_owned(&app, account, member, lease.capture_id)
                .await
                .unwrap()
        );
        assert!(
            capture::revoke_owned(&app, account, member, lease.capture_id)
                .await
                .unwrap(),
            "own repeated Stop is idempotent"
        );
        let disconnected_stopped = capture::owned_status(&app, account, member, lease.capture_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(disconnected_stopped.status, "stopped");
        assert!(!capture::observe_fetch(
            &app,
            account,
            member,
            tenant,
            aad,
            "selected",
            reopened.lease().unwrap(),
            &provider_error
        )
        .await
        .unwrap());
        task.abort();
        app.close().await;
        fixture.close().await;
        admin
            .execute(format!("DROP DATABASE {db}").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("DROP ROLE {role}").as_str())
            .await
            .unwrap();
        admin.close().await;
    }
}
