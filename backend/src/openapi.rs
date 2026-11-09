//! OpenAPI document definition for CoralSwarm's v1 API.
//!
//! Served at `GET /openapi.json` (and Swagger UI at `/swagger-ui/`) in
//! non-production environments only.  Gate: `APP_ENV != "production"`,
//! resolved once at boot into `AppState.env.is_production` (see
//! `AppEnv::resolve` in `lib.rs`) — `app()` reads that field, not `APP_ENV`
//! itself.

use utoipa::{
    openapi::security::{Http, HttpAuthScheme, SecurityScheme},
    Modify, OpenApi,
};

use crate::routes::{
    activity, billing_me, billing_seats, bootstrap, connector_audit, connector_catalog, connector_health, embed, find_results, graphs, ingest, jobs,
    home_launchpad, home_metrics, journal, journal_gist, message_feedback, nodes, org_auto_add, org_domains, org_invites, org_join_requests, org_members, org_slots, queries,
    query_audit, retractions,
    teams, transcript, workspace_activity,
};

/// Security scheme modifier — injects the `bearer_token` scheme into the spec.
struct BearerAuth;

impl Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_token",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    }
}

/// The top-level OpenAPI document.
///
/// Paths are collected from each route module's `#[utoipa::path]` annotations.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "CoralSwarm API",
        version = "0.1.0",
        description = "CoralSwarm knowledge-graph backend",
    ),
    paths(
        bootstrap::get_bootstrap,
        graphs::list_graphs,
        graphs::create_graph,
        graphs::get_graph,
        graphs::list_atoms,
        graphs::get_ocean_stats,
        ingest::ingest_atom,
        embed::embed_texts,
        transcript::cleanup_transcript,
        nodes::list_nodes,
        nodes::get_node_by_id,
        nodes::patch_node,
        nodes::get_node_atoms,
        queries::list_queries,
        queries::create_query,
        queries::create_query_stream,
        query_audit::list_query_audit,
        retractions::list_retractions,
        retractions::get_retraction,
        connector_audit::list_connector_audit,
        activity::list_activity,
        home_metrics::get_home_metrics,
        home_launchpad::get_continue,
        home_launchpad::get_whats_new,
        home_launchpad::get_attention,
        workspace_activity::get_workspace_activity,
        journal::list_journal,
        find_results::title_search,
        journal::journal_hour_items,
        journal_gist::get_gist,
        message_feedback::put_feedback,
        message_feedback::delete_feedback,
        teams::list_teams,
        teams::list_members,
        teams::invite_member,
        teams::invite_member_legacy,
        teams::remove_member,
        teams::remove_member_legacy,
        teams::create_team,
        teams::rename_team,
        teams::archive_team,
        teams::restore_team,
        teams::list_archived_teams,
        teams::list_team_members,
        teams::add_member,
        teams::assign_team_member_role,
        teams::remove_team_member,
        teams::list_team_events,
        teams::list_org_members,
        org_invites::create_invite,
        org_invites::resend_invite,
        org_invites::revoke_invite,
        org_members::remove_org_member,
        org_members::change_org_member_role,
        nodes::get_node_candidates,
        nodes::promote_node,
        billing_me::get_billing_me,
        billing_seats::change_seat_tier,
        billing_seats::list_seat_tiers,
        org_slots::get_slots,
        org_slots::purchase_slots,
        org_slots::reduce_slots,
        org_join_requests::create,
        org_join_requests::list_mine,
        org_join_requests::list,
        org_join_requests::approve,
        org_join_requests::reject,
        org_domains::claim_domain,
        org_domains::list_domains,
        org_domains::remove_domain,
        org_domains::verify_dns,
        org_domains::request_confirmation,
        org_auto_add::get_settings,
        org_auto_add::put_settings,
        org_auto_add::queued_preview,
        org_auto_add::admit_queued,
        connector_health::list,
        connector_catalog::list,
        connector_catalog::detail,
        connector_catalog::captures,
        connector_catalog::connect_start,
        connector_catalog::connect_complete,
        connector_catalog::recover,
        connector_catalog::disconnect,
        connector_catalog::scope_items,
        connector_catalog::put_scope,
        connector_catalog::patch_source,
        connector_catalog::backfill,
        jobs::get_job,
    ),
    tags(
        (name = "bootstrap", description = "One-request app-shell boot read"),
        (name = "graphs",   description = "Knowledge-graph CRUD"),
        (name = "ingest",   description = "Atom ingestion pipeline"),
        (name = "transcript", description = "Meeting transcript cleanup"),
        (name = "nodes",    description = "Node CRUD"),
        (name = "queries",  description = "Saved query management"),
        (name = "query_audit", description = "Query-audit disclosure log (org-admin only)"),
        (name = "retractions", description = "D6 UI's read of the retraction audit log (author + admin-of-ocean, per D8)"),
        (name = "connector_audit", description = "Connector compliance audit log (org-admin only)"),
        (name = "activity", description = "Activity feed"),
        (name = "home",     description = "Home dashboard"),
        (name = "journal",  description = "Hourly work journal (/today)"),
        (name = "find",     description = "Chat find: title-only source search (Cmd-K fallback)"),
        (name = "teams",    description = "Team management"),
        (name = "billing",  description = "Self-serve billing (seat lifecycle)"),
        (name = "join_requests", description = "Join-request queue: a non-member asks to join an org, an admin approves/rejects (CS-437)"),
        (name = "connector_health", description = "Connector-health degradations an admin can act on — today, D16's rung-1 directory lost to a departing member (CS-446, item 16)"),
        (name = "connectors", description = "The generic connector catalog: every connector's status, sources and problems, one shape for all of them"),
        (name = "org_domains", description = "Org domain verification: claim a domain, prove it by DNS TXT or an emailed confirmation, and record which proved it (CS-443, D11)"),
        (name = "jobs",     description = "Background job status/progress polling"),
    ),
    modifiers(&BearerAuth),
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::*;

    /// CodeRabbit finding on PR #537: `teams::rename_team`'s request body
    /// type (`RenameTeamRequest`) was listed as a path but never registered
    /// as a schema, so the generated spec's `#/components/schemas` said
    /// nothing about the shape of a rename request. Fixed by giving
    /// `RenameTeamRequest` a `#[derive(utoipa::ToSchema)]` and pointing
    /// `rename_team`'s `#[utoipa_path(request_body = RenameTeamRequest, ...)]`
    /// at it (`routes::teams`) — the same pairing `IngestAtomRequest`/
    /// `CleanupTranscriptRequest`/`CreateQueryRequest` already use. This
    /// pins the outcome at the spec-generation level so a future refactor
    /// that drops either half of that pairing fails a test instead of only
    /// being visible by diffing `/openapi.json` by hand.
    ///
    /// `archive_team`/`restore_team` (this PR's other two new lifecycle
    /// operations) take no request body at all — no `Json<T>` extractor,
    /// nothing to register — so they have no equivalent gap to fix.
    #[test]
    fn rename_team_request_body_is_registered_as_a_schema() {
        let doc = ApiDoc::openapi();
        let components = doc.components.expect("ApiDoc must have components");
        assert!(
            components.schemas.contains_key("RenameTeamRequest"),
            "RenameTeamRequest must be registered under #/components/schemas \
             (schemas present: {:?})",
            components.schemas.keys().collect::<Vec<_>>()
        );
    }
}
