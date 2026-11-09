use super::*;
use chrono::{Duration, Timelike, Utc};
use sqlx::Executor;

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

/// Executes the production migrations and service through a real non-owner,
/// NOBYPASSRLS role. This deliberately never connects to a remote database.
#[tokio::test]
async fn meeting_consent_member_occurrence_revocation_and_audit_boundaries_pg() {
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
    let database = crate::test_db::TestDatabase::from_url(url.as_str(), "meeting");
    let role = database.role("meeting_app");
    // A distinct UUID password supports SCRAM fixtures without elevated roles.
    let role_password = Uuid::new_v4().simple().to_string();
    admin
        .execute(format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{role_password}'").as_str())
        .await
        .unwrap();
    admin
        .execute(format!("CREATE DATABASE {database}").as_str())
        .await
        .unwrap();
    url.set_path(&format!("/{database}"));
    let fixture = PgPool::connect(url.as_str()).await.unwrap();
    fixture.execute("CREATE TABLE graphs(id uuid PRIMARY KEY,account_id uuid NOT NULL,owner_user_id uuid,kind text,archived_at timestamptz);
      ALTER TABLE graphs ENABLE ROW LEVEL SECURITY; ALTER TABLE graphs FORCE ROW LEVEL SECURITY;
      CREATE POLICY own_graph ON graphs USING(account_id::text=current_setting('app.account_id',true) AND owner_user_id::text=current_setting('app.user_id',true));
      CREATE FUNCTION tenant_rls_is_system() RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT current_setting('app.tenant_actor',true)='system' $$;
      CREATE FUNCTION tenant_rls_actor_user_id() RETURNS uuid LANGUAGE sql STABLE AS $$ SELECT nullif(current_setting('app.user_id',true),'')::uuid $$;")
        .await.unwrap();
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
    fixture
        .execute(include_str!(
            "../../migrations_tenant/0071_connector_audit_log.sql"
        ))
        .await
        .unwrap();
    fixture
        .execute("ALTER TABLE connector_audit_log FORCE ROW LEVEL SECURITY")
        .await
        .unwrap();
    fixture.execute(format!("GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {role};").as_str()).await.unwrap();
    let account = Uuid::new_v4();
    let other_account = Uuid::new_v4();
    let member = Uuid::new_v4();
    let other_member = Uuid::new_v4();
    let graph = Uuid::new_v4();
    let other_graph = Uuid::new_v4();
    for (id, owner) in [(graph, member), (other_graph, other_member)] {
        sqlx::query(
            "INSERT INTO graphs(id,account_id,owner_user_id,kind) VALUES($1,$2,$3,'member')",
        )
        .bind(id)
        .bind(account)
        .bind(owner)
        .execute(&fixture)
        .await
        .unwrap();
    }
    url.set_username(&role).unwrap();
    url.set_password(Some(&role_password)).unwrap();
    let app = PgPool::connect(url.as_str()).await.unwrap();
    let bypass: bool = sqlx::query_scalar(
        "SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname=current_user",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    assert!(!bypass);
    let tenant = "fb2a6e39-6614-4c56-ab1e-15d6d5069ad6";
    let aad = "d2c12dfd-3d6c-445b-91e4-fd4b88c2de50";
    let other_aad = "bd7811d4-6141-4eb6-ad7e-482cddbb2228";
    let start = Utc::now() - Duration::hours(2);
    let end = start + Duration::minutes(30);
    let event_at = start - Duration::minutes(1);
    installation_status(&app, account, tenant, "generation-1", "context", true)
        .await
        .unwrap();
    let id = authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "meeting",
        graph,
        start,
        end,
        "meeting_bot",
        Some("generation-1"),
        Some("add-1"),
        Some(event_at),
    )
    .await
    .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "meeting", start)
            .await
            .unwrap(),
        Some(graph)
    );
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "meeting", end)
            .await
            .unwrap(),
        Some(graph)
    );
    for at in [
        start - Duration::seconds(1),
        end + Duration::hours(24) + Duration::seconds(1),
    ] {
        assert_eq!(
            active_graph_at(&app, account, member, tenant, aad, "meeting", at)
                .await
                .unwrap(),
            None
        );
    }
    assert_eq!(
        active_graph_at(
            &app,
            account,
            member,
            tenant,
            aad,
            "meeting",
            end + Duration::hours(1)
        )
        .await
        .unwrap(),
        Some(graph)
    );
    assert_eq!(
        matches_occurrence(&app, account, member, tenant, aad, "meeting", start, end)
            .await
            .unwrap(),
        Some(graph)
    );
    assert_eq!(
        matches_occurrence(
            &app,
            account,
            member,
            tenant,
            aad,
            "meeting",
            start + Duration::minutes(1),
            end
        )
        .await
        .unwrap(),
        None
    );
    assert!(authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "missing-source",
        graph,
        start,
        end,
        "meeting_bot",
        None,
        None,
        None
    )
    .await
    .is_err());
    for (a, m, t) in [
        (account, other_member, tenant),
        (other_account, member, tenant),
        (account, member, "wrong-tenant"),
    ] {
        assert!(active_meetings(&app, a, m, t, aad)
            .await
            .unwrap()
            .is_empty());
    }
    assert!(active_meetings(&app, account, member, tenant, other_aad)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        active_graph(&app, account, member, tenant, other_aad, "meeting")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        active_graph_at(&app, account, member, tenant, other_aad, "meeting", start)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        matches_occurrence(&app, account, member, tenant, other_aad, "meeting", start, end)
            .await
            .unwrap(),
        None
    );
    assert!(authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "wrong-graph",
        other_graph,
        start,
        end,
        "meeting_addin",
        None,
        None,
        None
    )
    .await
    .is_err());
    let mut own = begin_tenant_scoped(&app, account, member).await.unwrap();
    let hidden: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log")
        .fetch_one(&mut *own)
        .await
        .unwrap();
    assert_eq!(
        hidden, 0,
        "members may insert audit rows but cannot read them"
    );
    own.rollback().await.unwrap();
    let audit_count: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_audit_log")
        .fetch_one(&fixture)
        .await
        .unwrap();
    assert_eq!(audit_count, 1);

    // Repeating an active authorization keeps the original consent boundary.
    assert_eq!(
        authorize(
            &app,
            account,
            member,
            tenant,
            aad,
            "meeting",
            graph,
            start,
            end,
            "meeting_bot",
            Some("generation-1"),
            Some("add-1"),
            Some(event_at)
        )
        .await
        .unwrap(),
        id
    );
    assert!(authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "meeting",
        graph,
        start + Duration::days(1),
        end + Duration::days(1),
        "meeting_bot",
        Some("generation-1"),
        Some("add-1"),
        Some(event_at)
    )
    .await
    .is_err());
    assert_eq!(
        matches_occurrence(
            &app,
            account,
            member,
            tenant,
            aad,
            "meeting",
            start + Duration::days(1),
            end + Duration::days(1)
        )
        .await
        .unwrap(),
        None
    );
    revoke(&app, account, member, tenant, "meeting")
        .await
        .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "meeting", start)
            .await
            .unwrap(),
        None
    );
    for event in ["add-1", "different-old-add"] {
        assert!(
            authorize(
                &app,
                account,
                member,
                tenant,
                aad,
                "meeting",
                graph,
                start,
                end,
                "meeting_bot",
                Some("generation-1"),
                Some(event),
                Some(event_at)
            )
            .await
            .is_err(),
            "old provider delivery must never undo an explicit stop"
        );
    }
    // A fresh explicit member action can intentionally enable the same meeting.
    authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "meeting",
        graph,
        start,
        end,
        "meeting_addin",
        None,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "meeting", start)
            .await
            .unwrap(),
        Some(graph)
    );

    // Switching the connected Microsoft actor never inherits the old actor's
    // consent; a new explicit verified action binds that actor independently.
    authorize(
        &app,
        account,
        member,
        tenant,
        other_aad,
        "meeting",
        graph,
        start,
        end,
        "meeting_addin",
        None,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "meeting", start)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        active_graph_at(&app, account, member, tenant, other_aad, "meeting", start)
            .await
            .unwrap(),
        Some(graph)
    );
    // Failed compliance writes roll back the consent insert in the same tx.
    fixture
        .execute(format!("REVOKE INSERT ON connector_audit_log FROM {role}").as_str())
        .await
        .unwrap();
    assert!(authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "audit-denied",
        graph,
        start,
        end,
        "meeting_addin",
        None,
        None,
        None
    )
    .await
    .is_err());
    let denied: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM teams_meeting_captures WHERE meeting_id='audit-denied'",
    )
    .fetch_one(&fixture)
    .await
    .unwrap();
    assert_eq!(denied, 0);
    fixture
        .execute(format!("GRANT INSERT ON connector_audit_log TO {role}").as_str())
        .await
        .unwrap();
    // Removing the installation prevents both pending and queued body capture;
    // the same generation cannot be reactivated by a retry.
    authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "bot-pending",
        graph,
        start,
        end,
        "meeting_bot",
        Some("generation-1"),
        Some("add-2"),
        Some(event_at),
    )
    .await
    .unwrap();
    authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "bot-promoted",
        graph,
        start,
        end,
        "meeting_bot",
        Some("generation-1"),
        Some("promote-add"),
        Some(event_at),
    )
    .await
    .unwrap();
    authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "bot-promoted",
        graph,
        start,
        end,
        "meeting_addin",
        None,
        None,
        None,
    )
    .await
    .unwrap();
    remove_installations(&app, account, tenant, "context")
        .await
        .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "bot-pending", start)
            .await
            .unwrap(),
        None
    );
    assert!(
        installation_status(&app, account, tenant, "generation-1", "context", true)
            .await
            .is_err()
    );
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "bot-promoted", start)
            .await
            .unwrap(),
        Some(graph),
        "explicit user confirmation survives removal of the bot"
    );
    installation_status(&app, account, tenant, "generation-2", "context", true)
        .await
        .unwrap();
    authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "bot-pending",
        graph,
        start,
        end,
        "meeting_bot",
        Some("generation-2"),
        Some("fresh-readd"),
        Some(event_at + Duration::seconds(1)),
    )
    .await
    .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "bot-pending", start)
            .await
            .unwrap(),
        Some(graph),
        "a fresh verified re-add binds the new installation generation"
    );
    remove_installations(&app, account, tenant, "context")
        .await
        .unwrap();
    authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "bot-pending",
        graph,
        start,
        end,
        "meeting_addin",
        None,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, aad, "bot-pending", start)
            .await
            .unwrap(),
        Some(graph)
    );
    revoke(&app, account, member, tenant, "bot-promoted")
        .await
        .unwrap();
    assert!(authorize(
        &app,
        account,
        member,
        tenant,
        aad,
        "bot-promoted",
        graph,
        start,
        end,
        "meeting_bot",
        Some("generation-2"),
        Some("late-alternate-event"),
        Some(event_at)
    )
    .await
    .is_err());
    assert_eq!(
        active_graph_at(&app, account, member, tenant, other_aad, "meeting", start)
            .await
            .unwrap(),
        Some(graph)
    );
    sqlx::query("UPDATE graphs SET archived_at=now() WHERE id=$1")
        .bind(graph)
        .execute(&fixture)
        .await
        .unwrap();
    assert_eq!(
        active_graph_at(&app, account, member, tenant, other_aad, "meeting", start)
            .await
            .unwrap(),
        None
    );

    // Real system-only context RLS, tenant/chat binding and deletion tombstones.
    let removed_at = event_at + Duration::minutes(1);
    assert!(store_context(
        &app,
        tenant,
        "ctx",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        "aad",
        event_at,
        "ctx-add"
    )
    .await
    .unwrap());
    assert!(context(&app, "wrong-tenant", "ctx", Some("chat"))
        .await
        .unwrap()
        .is_none());
    assert!(context(&app, tenant, "ctx", Some("wrong-chat"))
        .await
        .unwrap()
        .is_none());
    assert!(context(&app, tenant, "ctx", Some("chat"))
        .await
        .unwrap()
        .is_some());
    assert!(
        remove_context(&app, tenant, "ctx", removed_at, "ctx-remove")
            .await
            .unwrap()
    );
    assert!(!store_context(
        &app,
        tenant,
        "ctx",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        "aad",
        event_at,
        "ctx-add"
    )
    .await
    .unwrap());
    assert!(context(&app, tenant, "ctx", None).await.unwrap().is_none());
    assert!(remove_context(
        &app,
        tenant,
        "remove-before-add",
        removed_at,
        "remove-first"
    )
    .await
    .unwrap());
    assert!(!store_context(
        &app,
        tenant,
        "remove-before-add",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        "aad",
        event_at,
        "old-add"
    )
    .await
    .unwrap());

    // Out-of-order metadata is not an installation authority. It cannot
    // prevent the real Add, replace an Add generation, or undo a Remove.
    store_context_metadata(
        &app,
        tenant,
        "metadata-first",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        removed_at,
        "membership-later",
    )
    .await
    .unwrap();
    assert!(context(&app, tenant, "metadata-first", None)
        .await
        .unwrap()
        .is_none());
    assert!(store_context(
        &app,
        tenant,
        "metadata-first",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        event_at,
        "authoritative-add"
    )
    .await
    .unwrap());
    store_context_metadata(
        &app,
        tenant,
        "metadata-first",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        removed_at,
        "another-membership",
    )
    .await
    .unwrap();
    assert_eq!(
        context(&app, tenant, "metadata-first", Some("chat"))
            .await
            .unwrap()
            .unwrap()
            .2,
        "authoritative-add"
    );
    assert!(remove_context(
        &app,
        tenant,
        "metadata-first",
        removed_at,
        "authoritative-remove"
    )
    .await
    .unwrap());
    store_context_metadata(
        &app,
        tenant,
        "metadata-first",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        removed_at + Duration::minutes(1),
        "late-membership",
    )
    .await
    .unwrap();
    assert!(!store_context(
        &app,
        tenant,
        "metadata-first",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        event_at,
        "authoritative-add"
    )
    .await
    .unwrap());
    assert!(context(&app, tenant, "metadata-first", None)
        .await
        .unwrap()
        .is_none());
    store_context_metadata(
        &app,
        tenant,
        "provisional-remove",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        removed_at + Duration::minutes(1),
        "metadata-first",
    )
    .await
    .unwrap();
    assert!(remove_context(
        &app,
        tenant,
        "provisional-remove",
        removed_at,
        "earlier-remove"
    )
    .await
    .unwrap());
    assert!(!store_context(
        &app,
        tenant,
        "provisional-remove",
        "chat",
        "graph-meeting",
        "https://smba.trafficmanager.net/amer/",
        aad,
        event_at,
        "old-add"
    )
    .await
    .unwrap());

    // Recurring series: one meeting ID, a new occurrence each day. Explicit
    // consent for the NEXT occurrence replaces a finished one; the bot cannot
    // (asserted above), and a still-running occurrence is never replaced.
    let series_graph = Uuid::new_v4();
    sqlx::query("INSERT INTO graphs(id,account_id,owner_user_id,kind) VALUES($1,$2,$3,'member')")
        .bind(series_graph)
        .bind(account)
        .bind(member)
        .execute(&fixture)
        .await
        .unwrap();
    let series_start = Utc::now() - Duration::days(3);
    let series = |s: chrono::DateTime<Utc>, source: &'static str| {
        authorize(
            &app, account, member, tenant, aad, "series", series_graph, s, s + Duration::hours(1),
            source, None, None, None,
        )
    };
    let first = series(series_start, "meeting_addin").await.unwrap();
    let today = Utc::now() - Duration::minutes(10);
    assert_eq!(series(today, "meeting_addin").await.unwrap(), first);
    let stored: (chrono::DateTime<Utc>,) = sqlx::query_as(
        "SELECT occurrence_start FROM teams_meeting_captures WHERE meeting_id='series'",
    )
    .fetch_one(&fixture)
    .await
    .unwrap();
    assert_eq!(stored.0.timestamp_micros(), today.timestamp_micros());
    // The recurring occurrence's transcript bound is persisted and read back.
    let bound = today + Duration::hours(3);
    authorize_with_window(
        &app, account, member, tenant, aad, "series2", series_graph, today,
        today + Duration::hours(1), Some(bound), "meeting_addin", None, None, None,
    )
    .await
    .unwrap();
    let got = status(&app, account, member, tenant, aad, None, Some("series2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.transcript_end.unwrap().timestamp_micros(), bound.timestamp_micros());
    assert!(status(&app, account, member, tenant, aad, None, Some("series"))
        .await
        .unwrap()
        .unwrap()
        .transcript_end
        .is_none());
    // An ended occurrence whose fetch window is still open (end + 24h) and has
    // retrieved nothing is NOT replaced: its transcript may still arrive.
    let open_start = Utc::now() - Duration::hours(3);
    let open_series = |meeting: &'static str, s: chrono::DateTime<Utc>, bound| {
        authorize_with_window(
            &app, account, member, tenant, aad, meeting, series_graph, s,
            s + Duration::hours(1), bound, "meeting_addin", None, None, None,
        )
    };
    open_series("series3", open_start, None).await.unwrap();
    assert!(open_series("series3", today, None).await.is_err());
    // ...until it has retrieved a transcript...
    sqlx::query("UPDATE teams_meeting_captures SET retrieved_transcript_count=1 WHERE meeting_id='series3'")
        .execute(&fixture)
        .await
        .unwrap();
    open_series("series3", today, None).await.unwrap();
    // ...or a recurring occurrence's own (earlier) transcript bound has passed.
    open_series("series4", open_start, Some(Utc::now() - Duration::hours(1))).await.unwrap();
    open_series("series4", today, None).await.unwrap();
    // A stored year-1 (Graph placeholder) capture is replaceable by any source.
    let year_one = chrono::DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z").unwrap().with_timezone(&Utc);
    authorize_with_window(
        &app, account, member, tenant, aad, "meetnow", series_graph, year_one,
        year_one + Duration::hours(3), None, "meeting_addin", None, None, None,
    )
    .await
    .unwrap();
    open_series("meetnow", today, None).await.unwrap();
    // Authority never downgrades: a BOT event cannot replace an explicit member's
    // pre-2000 capture (removing the bot would revoke that consent) ...
    let bot = |meeting: &'static str, s: chrono::DateTime<Utc>, event: &'static str| {
        authorize_with_window(
            &app, account, member, tenant, aad, meeting, series_graph, s,
            s + Duration::hours(3), None, "meeting_bot", Some("generation-1"),
            Some(event), Some(Utc::now()),
        )
    };
    authorize_with_window(
        &app, account, member, tenant, aad, "explicit-y1", series_graph, year_one,
        year_one + Duration::hours(3), None, "meeting_addin", None, None, None,
    )
    .await
    .unwrap();
    assert!(bot("explicit-y1", today, "ev-1").await.is_err());
    let kept: (String,) = sqlx::query_as("SELECT source FROM teams_meeting_captures WHERE meeting_id='explicit-y1'")
        .fetch_one(&fixture).await.unwrap();
    assert_eq!(kept.0, "meeting_addin");
    // ... while a bot-owned pre-2000 row may be replaced by the bot, and by explicit consent.
    bot("bot-y1", year_one, "ev-2").await.unwrap();
    bot("bot-y1", today, "ev-3").await.unwrap();
    bot("bot-y2", year_one, "ev-4").await.unwrap();
    open_series("bot-y2", today, None).await.unwrap();
    let up: (String,) = sqlx::query_as("SELECT source FROM teams_meeting_captures WHERE meeting_id='bot-y2'")
        .fetch_one(&fixture).await.unwrap();
    assert_eq!(up.0, "meeting_addin");
    let fixed = status(&app, account, member, tenant, aad, None, Some("meetnow"))
        .await.unwrap().unwrap();
    assert_eq!(fixed.occurrence_start.timestamp_micros(), today.timestamp_micros());
    assert!(
        series(today + Duration::days(1), "meeting_addin").await.is_err(),
        "a still-running occurrence cannot be silently replaced"
    );

    // Graph carries 100ns ticks but timestamptz stores microseconds: an exact repeat
    // with sub-microsecond bounds must still be recognised as the same occurrence,
    // while a change of one full microsecond stays a different (rejected) target.
    // The nanoseconds are explicit so this never depends on the platform clock.
    let precise_start = (Utc::now() - Duration::hours(2))
        .with_nanosecond(123_456_789)
        .unwrap();
    let precise_end = precise_start + Duration::minutes(30);
    let precise_event_at = precise_start - Duration::minutes(1);
    for (meeting, source, installation, event) in [
        (
            "subus-bot",
            "meeting_bot",
            Some("generation-1"),
            Some("add-subus"),
        ),
        ("subus-addin", "meeting_addin", None, None),
    ] {
        let consent = |s: chrono::DateTime<Utc>, e: chrono::DateTime<Utc>| {
            authorize(
                &app,
                account,
                member,
                tenant,
                aad,
                meeting,
                series_graph,
                s,
                e,
                source,
                installation,
                event,
                Some(precise_event_at),
            )
        };
        let first = consent(precise_start, precise_end).await.unwrap();
        assert_eq!(
            consent(precise_start, precise_end).await.unwrap(),
            first,
            "{meeting}: exact repeat with sub-microsecond bounds"
        );
        // Same stored microsecond, only the nanoseconds differ: still the same target.
        assert_eq!(
            consent(precise_start + Duration::nanoseconds(1), precise_end)
                .await
                .unwrap(),
            first,
            "{meeting}: sub-microsecond jitter is not a different occurrence"
        );
        let micro = Duration::microseconds(1);
        assert!(
            consent(precise_start + micro, precise_end).await.is_err(),
            "{meeting}: start moved by one microsecond"
        );
        assert!(
            consent(precise_start, precise_end + micro).await.is_err(),
            "{meeting}: end moved by one microsecond"
        );
        let stored: (chrono::DateTime<Utc>, chrono::DateTime<Utc>) = sqlx::query_as(
            "SELECT occurrence_start,occurrence_end FROM teams_meeting_captures WHERE meeting_id=$1",
        )
        .bind(meeting)
        .fetch_one(&fixture)
        .await
        .unwrap();
        assert_eq!(
            stored.0.timestamp_micros(),
            precise_start.timestamp_micros()
        );
        assert_eq!(stored.1.timestamp_micros(), precise_end.timestamp_micros());
        assert_eq!(stored.0.nanosecond() % 1_000, 0);
    }

    app.close().await;
    fixture.close().await;
    admin
        .execute(format!("DROP DATABASE {database} WITH (FORCE)").as_str())
        .await
        .unwrap();
    admin
        .execute(format!("DROP ROLE {role}").as_str())
        .await
        .unwrap();
    admin.close().await;
}

#[test]
fn database_precision_keeps_whole_microseconds_only() {
    let whole = Utc::now().with_nanosecond(123_456_000).unwrap();
    assert_eq!(to_db_precision(whole), whole);
    assert_eq!(
        to_db_precision(whole.with_nanosecond(123_456_789).unwrap()),
        whole
    );
    let year_one = chrono::DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(to_db_precision(year_one), year_one);
}

#[test]
fn omitted_occurrence_bounds_map_to_invalid_metadata() {
    assert_eq!(
        provider_error_code(
            &crate::connectors::teams_targeted_transcripts::Error::InvalidMetadata
        ),
        "invalid_metadata"
    );
}

#[test]
fn meeting_intent_round_trips_and_is_bound_to_its_own_derived_key() {
    let secret = "example_4";
    let tenant = uuid::Uuid::new_v4().to_string();
    let aad = uuid::Uuid::new_v4().to_string();
    let ticket = mint_intent(secret, &tenant, &aad, "m1", "inst", "evt", Utc::now()).unwrap();
    let intent = verify_intent(secret, &ticket).unwrap();
    assert_eq!(intent.meeting_id, "m1");
    assert!(verify_intent("another-secret-0123456789abcdef", &ticket).is_err());
    // Not an OAuth state token, and an OAuth state token is not an intent.
    assert!(crate::oauth::state::verify_state(secret, &ticket).is_err());
    let state = crate::oauth::state::sign_state(
        secret,
        uuid::Uuid::new_v4(),
        Utc::now().timestamp(),
        "n",
        None,
        None,
    )
    .unwrap();
    assert!(verify_intent(secret, &state).is_err());
}
