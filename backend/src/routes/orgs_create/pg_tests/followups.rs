//! #1540: the self-serve A2 follow-ups that must land before
//! `ORG_SELFSERVE_SIGNUP_ENABLED` is turned on. Same harness as the parent
//! suite: real Postgres, every handler and the sweep as `coralswarm_app`,
//! local Clerk and Stripe doubles. Each test fails with its fix removed.

use super::*;

fn marked_org(id: &str) -> Value {
    json!({"id": id, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}})
}

async fn trial_at_backstop(owner: &PgPool, account: Uuid, creator: Uuid, sub: &str, tag: &str) {
    fin(
        owner,
        account,
        sqlx::query(
            "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, checkout_session_id, \
                 checkout_opened_at, stripe_subscription_id, pool_baseline, trial_started_at, trial_ends_at) \
             VALUES ($1, 'trialing', $2, 2, true, $3, now() - interval '91 days', $4, 3000, now() - interval '91 days', \
                     now() - interval '1 day')",
        )
        .bind(account)
        .bind(creator)
        .bind(format!("cs_t{tag}"))
        .bind(sub),
    )
    .await;
}

async fn trial_state(owner: &PgPool, account: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT state FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_optional(owner)
        .await
        .unwrap()
}

async fn paused_at(owner: &PgPool, org: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar("SELECT collection_paused_at FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(org)
        .fetch_one(owner)
        .await
        .unwrap()
}

/// SF1: Stripe's trial end falls inside a held org's pause, so the
/// conversion invoice is voided and the subscription goes `active`. Once
/// the org is built, the trial worker must not read that void invoice as
/// "not paid" and cancel (it waits for the sweep's resume), and the resume
/// re-anchors the subscription so the first period is charged afresh.
#[tokio::test]
async fn sf1_a_held_org_whose_trial_ended_in_the_pause_is_reanchored_on_resume_not_canceled() {
    const NAME: &str = "sf1_a_held_org_whose_trial_ended_in_the_pause_is_reanchored_on_resume_not_canceled";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let org = format!("org_sf1{tag}");
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 48).await;
    cmock.add_org(marked_org(&org));
    failed_job(&p.owner, &org, account, 2).await;
    sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup_requeues = 2 WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    let (sub, cus) = (format!("sub_sf1{tag}"), format!("cus_sf1{tag}"));
    mirror(&p.owner, account, &cus, Some(&sub), "active").await;
    smock.set_sub(&cus, &sub, "trialing");
    trial_at_backstop(&p.owner, account, u, &sub, &tag).await;
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };

    let r1 = sweeper.sweep_once(chrono::Utc::now()).await;
    let paused = paused_at(&p.owner, &org).await;
    // Stripe ends the trial inside the pause: its invoice is voided, the
    // subscription reports active.
    let void_invoice = format!("in_void{tag}");
    smock.set_sub_sequence(&sub, &["active"]);
    smock.set_invoice(&sub, &void_invoice, "void", paused.map(|t| t.timestamp() + 60).unwrap_or(0));
    // An operator gets it built.
    sqlx::query("UPDATE tenant_provisioning_jobs SET status = 'ready', step = 'done', clerk_org_cleanup = 'none' WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    // The trial worker runs before the sweep has resumed collection.
    let early =
        crate::billing::org_trial::lifecycle::process_account(&p.app, Some(stripe.as_ref()), account, chrono::Utc::now()).await;
    let early_state = trial_state(&p.owner, account).await;
    let early_cancels = smock.calls("DELETE", &format!("/v1/subscriptions/{sub}")).len();
    let r2 = sweeper.sweep_once(chrono::Utc::now()).await;
    let resumed = paused_at(&p.owner, &org).await;
    let posts = smock.posts(&format!("/v1/subscriptions/{sub}"));
    sqlx::query("DELETE FROM org_trials WHERE account_id = $1").bind(account).execute(&p.owner).await.ok();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(r1.collection_paused.contains(&sub), "{r1:?}");
    assert!(paused.is_some());
    assert!(early.is_ok(), "{early:?}");
    assert_eq!(early_state.as_deref(), Some("trialing"), "no conversion (and so no cancel) while collection is still paused");
    assert_eq!(early_cancels, 0, "the subscription was not canceled");
    assert!(r2.collection_resumed.contains(&sub), "{r2:?}");
    assert!(r2.collection_reanchored.contains(&sub), "the voided period is charged afresh: {r2:?}");
    assert!(resumed.is_none(), "the pause is cleared only after the re-anchor");
    let reanchor = posts.last().expect("posts");
    assert!(reanchor.1.contains("billing_cycle_anchor=now") && reanchor.1.contains("proration_behavior=none"), "{posts:?}");
    assert!(
        reanchor.2.as_deref().is_some_and(|k| k.starts_with(&format!("org-selfserve-held-reanchor:{account}:{sub}:"))),
        "keyed per pause: {posts:?}"
    );
}

/// SF5: a held org whose routing row is `active` (a `held_routed` org) is
/// served, so its collection is never paused, a pause from before is
/// resumed, and the trial worker no longer treats it as held.
#[tokio::test]
async fn sf5_a_held_org_with_an_active_routing_row_keeps_paying() {
    const NAME: &str = "sf5_a_held_org_with_an_active_routing_row_keeps_paying";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let mut fixtures = Vec::new();
    for which in ["fresh", "paused"] {
        let org = format!("org_sf5{which}{tag}");
        let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 48).await;
        cmock.add_org(marked_org(&org));
        failed_job(&p.owner, &org, account, 2).await;
        sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup = 'held' WHERE clerk_org_id = $1")
            .bind(&org)
            .execute(&p.owner)
            .await
            .unwrap();
        let slug = crate::tenancy::provisioning::opaque_slug_for_account(account);
        fin(
            &p.owner,
            account,
            sqlx::query(
                "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
                 VALUES ($1, $2, $3, $4, 'active')",
            )
            .bind(&org)
            .bind(account)
            .bind(&slug)
            .bind(crate::tenancy::provisioning::secret_ref_for_slug(&slug)),
        )
        .await;
        let (sub, cus) = (format!("sub_sf5{which}{tag}"), format!("cus_sf5{which}{tag}"));
        mirror(&p.owner, account, &cus, Some(&sub), "active").await;
        smock.set_sub(&cus, &sub, "active");
        if which == "paused" {
            sqlx::query(
                "UPDATE org_selfserve_creations SET collection_paused_at = now() - interval '1 hour' WHERE clerk_org_id = $1",
            )
            .bind(&org)
            .execute(&p.owner)
            .await
            .unwrap();
        }
        fixtures.push((org, account, sub));
    }
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let mut after = Vec::new();
    for (org, account, _) in &fixtures {
        let mut conn = p.app.acquire().await.unwrap();
        let held = crate::billing::org_trial::lifecycle::provisioning_held(&mut conn, *account).await.unwrap();
        drop(conn);
        after.push((paused_at(&p.owner, org).await, held));
    }
    for (org, account, _) in &fixtures {
        fin(&p.owner, *account, sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id = $1").bind(org)).await;
        drop_org(&p.owner, org).await;
    }
    drop_users(&p.owner, &[u]).await;

    let (fresh_sub, paused_sub) = (&fixtures[0].2, &fixtures[1].2);
    assert!(!r.collection_paused.contains(fresh_sub), "a served held org is never paused: {r:?}");
    assert!(r.collection_resumed.contains(paused_sub), "a served org paused before is resumed: {r:?}");
    assert_eq!(after[0], (None, false), "not paused, not held for the trial worker");
    assert_eq!(after[1], (None, false), "resumed, not held for the trial worker");
}

/// SF4: Clerk created the org but the response was lost (a timeout), and
/// Clerk's org list does not show it yet. The retry's create is refused
/// ("slug taken"); the org is found by its slug and marker and adopted,
/// never marked `refused` and orphaned.
#[tokio::test]
async fn sf4_a_retry_after_a_lost_create_response_adopts_the_org_by_its_slug() {
    const NAME: &str = "sf4_a_retry_after_a_lost_create_response_adopts_the_org_by_its_slug";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);
    cmock.lose_next_create_response();
    cmock.hide_from_list(true);
    let key = Uuid::new_v4().to_string();

    let (s1, b1) = create(&state, &clerk, u, &key, req("Lost Response", 2), &CAPS).await;
    let (s2, b2) = create(&state, &clerk, u, &key, req("Lost Response", 2), &CAPS).await;
    let made: Vec<String> = cmock.0.lock().unwrap().orgs.keys().cloned().collect();
    let intent: (String, Option<String>) =
        sqlx::query_as("SELECT status, clerk_org_id FROM org_selfserve_creations WHERE user_id = $1 AND idempotency_key = $2")
            .bind(u)
            .bind(&key)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    for o in &made {
        drop_org(&p.owner, o).await;
    }
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = $1").bind(u).execute(&p.owner).await.ok();
    drop_users(&p.owner, &[u]).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((s1, b1["code"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("identity_unavailable")), "{b1}");
    assert_eq!(s2, StatusCode::CREATED, "the retry adopts the org: {b2}");
    assert_eq!(made.len(), 1, "one Clerk org, never a second");
    assert_eq!(b2["org_id"].as_str(), made.first().map(String::as_str), "{b2}");
    assert_eq!(intent, ("created".to_string(), made.first().cloned()), "recorded, not refused");
    assert_eq!(cmock.creates().len(), 2, "the retry did ask Clerk (and was refused on the slug)");
}

/// SF6: subdomains of one registrable domain share its bucket, and one
/// person's free-mail aliases (`+tags`, Gmail dots, googlemail.com) are one
/// bucket, while a different free-mail user is not blocked by them.
#[tokio::test]
async fn sf6_subdomains_share_a_bucket_and_freemail_aliases_are_one_person() {
    const NAME: &str = "sf6_subdomains_share_a_bucket_and_freemail_aliases_are_one_person";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let add = |email: String| {
        let owner = p.owner.clone();
        let cmock = cmock.clone();
        async move {
            let (u, clerk_id) = new_user(&owner).await;
            cmock.add_user_email(&clerk_id, &email);
            u
        }
    };
    let dom = format!("evil-{}.test", Uuid::new_v4().simple());
    let base = format!("jdoe{}", &Uuid::new_v4().simple().to_string()[..12]);
    let dotted = format!("{}.{}", &base[..2], &base[2..]);
    let users = vec![
        add(format!("a@x.{dom}")).await,
        add(format!("b@y.{dom}")).await,
        add(format!("c@z.{dom}")).await,
        add(format!("{base}person@example.com")).await,
        add(format!("{dotted}@googlemail.com")).await,
        add(format!("{base}person@example.com")).await,
        add(format!("other{}@gmail.com", Uuid::new_v4().simple())).await,
    ];
    let caps = Caps { max_per_user_per_day: 2, max_per_domain_per_hour: 2, ..CAPS };
    let mut out = Vec::new();
    for (i, u) in users.iter().enumerate() {
        out.push(create(&state, &clerk, *u, &Uuid::new_v4().to_string(), req(&format!("Bucket {i}"), 2), &caps).await);
    }
    for (_, b) in &out {
        if let Some(o) = b["org_id"].as_str() {
            drop_org(&p.owner, o).await;
        }
    }
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = ANY($1)").bind(&users).execute(&p.owner).await.ok();
    drop_users(&p.owner, &users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    let limited = (StatusCode::TOO_MANY_REQUESTS, Some("org_create_rate_limited"));
    let code = |i: usize| (out[i].0, out[i].1["code"].as_str());
    assert_eq!(out[0].0, StatusCode::CREATED, "{}", out[0].1);
    assert_eq!(out[1].0, StatusCode::CREATED, "{}", out[1].1);
    assert_eq!(code(2), limited, "a third subdomain of one registrable domain: {}", out[2].1);
    assert_eq!(out[3].0, StatusCode::CREATED, "{}", out[3].1);
    assert_eq!(out[4].0, StatusCode::CREATED, "{}", out[4].1);
    assert_eq!(code(5), limited, "a third alias of one Gmail mailbox: {}", out[5].1);
    assert_eq!(out[6].0, StatusCode::CREATED, "another free-mail user is not blocked: {}", out[6].1);
}

/// N2: a webhook org object with no `private_metadata` (Clerk's membership
/// events) is looked up in Clerk, never read as "not self-serve"; a Clerk
/// failure is an error, and an org Clerk no longer has is treated as
/// self-serve (no account row minted for it).
#[tokio::test]
async fn n2_the_selfserve_marker_is_read_from_clerk_when_the_payload_lacks_it() {
    const NAME: &str = "n2_the_selfserve_marker_is_read_from_clerk_when_the_payload_lacks_it";
    let Some(_p) = pools(NAME).await else { return };
    let (clerk, cmock) = spawn_clerk().await;
    let tag = Uuid::new_v4().simple().to_string();
    let (marked, plain) = (format!("org_n2m{tag}"), format!("org_n2p{tag}"));
    cmock.add_org(marked_org(&marked));
    cmock.add_org(json!({"id": plain, "name": "plain", "created_by": "user_x"}));
    let dead = ClerkAdminClient::new(reqwest::Client::new(), "sk").with_base_url("http://127.0.0.1:9");

    let gone_id = format!("org_gone{tag}");
    let from_clerk = org_is_selfserve(&marked, &json!({"id": marked, "name": "M"}), Some(&clerk)).await;
    let plain_org = org_is_selfserve(&plain, &json!({"id": plain, "name": "P"}), Some(&clerk)).await;
    let gone = org_is_selfserve(&gone_id, &json!({"id": gone_id}), Some(&clerk)).await;
    let unreachable = org_is_selfserve(&marked, &json!({"id": marked}), Some(&dead)).await;
    let no_org_object = org_is_selfserve(&marked, &Value::Null, Some(&clerk)).await;
    let in_payload =
        org_is_selfserve(&plain, &json!({"id": plain, "private_metadata": {SELFSERVE_ORG_MARKER: true}}), Some(&dead)).await;

    assert_eq!(from_clerk, Ok(true), "the payload lacked private_metadata; Clerk says self-serve");
    assert_eq!(plain_org, Ok(false));
    assert_eq!(gone, Ok(true), "fail closed for an org Clerk no longer has");
    assert!(unreachable.is_err(), "a Clerk failure is an error (Svix retries): {unreachable:?}");
    assert_eq!(in_payload, Ok(true), "a payload that carries private_metadata is read directly");
    assert_eq!(no_org_object, Ok(true), "the extracted id is looked up even with no org object in the payload");
}

/// SF2 + N3 (the shared helpers): a purchase-mode admission gets one place
/// of Clerk cap headroom first and gives it back if it fails; after a seat
/// purchase the cap is synced BEFORE the drain runs.
#[tokio::test]
async fn sf2_n3_cap_headroom_is_given_back_on_failure_and_synced_before_the_drain() {
    const NAME: &str = "sf2_n3_cap_headroom_is_given_back_on_failure_and_synced_before_the_drain";
    let Some(p) = pools(NAME).await else { return };
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let org = format!("org_cap{}", Uuid::new_v4().simple());
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 1).await;
    let caps = |c: &ClerkMock| -> Vec<i64> {
        c.patches().iter().filter(|(o, _)| *o == org).filter_map(|(_, b)| b["max_allowed_memberships"].as_i64()).collect()
    };

    let failed =
        with_member_cap_headroom(&p.app, &clerk, account, async { refuse(StatusCode::CONFLICT, "already_a_member", "x") }).await;
    let after_failure = caps(&cmock);
    let ok = with_member_cap_headroom(&p.app, &clerk, account, async { StatusCode::CREATED.into_response() }).await;
    let after_success = caps(&cmock);
    let seen_by_drain = sync_cap_then(&p.app, Some(&clerk), account, async { caps(&cmock).len() }).await;
    let after_drain = caps(&cmock);
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert_eq!(failed.status(), StatusCode::CONFLICT);
    // Seats being bought: 2 (the seeded intent), one member: cap 2.
    assert_eq!(after_failure, vec![3, 2], "N3: +1 for the admission, given back when it failed");
    assert_eq!(ok.status(), StatusCode::CREATED);
    assert_eq!(after_success, vec![3, 2, 3], "a successful admission keeps it (the seat purchase re-syncs)");
    assert_eq!(seen_by_drain, 4, "SF2: the cap PATCH landed before the drain ran");
    assert_eq!(after_drain.last(), Some(&2));
}

/// N4: the abandoned delete refuses while the mirror names any subscription
/// it does not say is `canceled`.
#[tokio::test]
async fn n4_the_abandoned_delete_refuses_while_the_mirror_names_any_uncanceled_subscription() {
    const NAME: &str = "n4_the_abandoned_delete_refuses_while_the_mirror_names_any_uncanceled_subscription";
    let Some(p) = pools(NAME).await else { return };
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let org = format!("org_n4{tag}");
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 48).await;
    mirror(&p.owner, account, &format!("cus_n4{tag}"), Some(&format!("sub_n4{tag}")), "pending").await;
    let claim: (Uuid, chrono::DateTime<chrono::Utc>) = sqlx::query_as(
        "UPDATE org_selfserve_creations SET sweep_state = 'deleting', sweep_claimed_at = now() \
          WHERE clerk_org_id = $1 RETURNING id, sweep_claimed_at",
    )
    .bind(&org)
    .fetch_one(&p.owner)
    .await
    .unwrap();
    use crate::tenancy::org_selfserve_cleanup::delete_abandoned_account;
    let refused = delete_abandoned_account(&p.app, claim.0, Some(account), claim.1).await;
    // Asserted here: with the fix removed the account is already gone and
    // the fixture write below could not run.
    assert_eq!(refused.as_ref().ok(), Some(&false), "a mirror row that is not canceled keeps the org");
    fin(&p.owner, account, sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(account))
        .await;
    let deleted = delete_abandoned_account(&p.app, claim.0, Some(account), claim.1).await;
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM accounts WHERE id = $1").bind(account).fetch_one(&p.owner).await.unwrap();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert_eq!(deleted.ok(), Some(true), "once canceled it is deleted");
    assert_eq!(left, 0);
}

/// N5: a request dropped mid-flight (the client went away) gives its lease
/// back instead of leaving the sign-up `create_in_progress` for 120 s.
#[tokio::test]
async fn n5_a_dropped_create_request_gives_its_lease_back() {
    const NAME: &str = "n5_a_dropped_create_request_gives_its_lease_back";
    let Some(p) = pools(NAME).await else { return };
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);
    cmock.set_create_delay(3000);
    let key = Uuid::new_v4().to_string();

    let dropped =
        tokio::time::timeout(std::time::Duration::from_millis(700), create(&state, &clerk, u, &key, req("Gone Away", 2), &CAPS))
            .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let lease: Option<(Option<chrono::DateTime<chrono::Utc>>, String)> =
        sqlx::query_as("SELECT lease_until, status FROM org_selfserve_creations WHERE user_id = $1 AND idempotency_key = $2")
            .bind(u)
            .bind(&key)
            .fetch_optional(&p.owner)
            .await
            .unwrap();
    // Let the delayed Clerk create land before tearing down.
    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = $1").bind(u).execute(&p.owner).await.ok();
    drop_users(&p.owner, &[u]).await;

    assert!(dropped.is_err(), "the request was still in flight when it was dropped");
    let (until, status) = lease.expect("the intent was written before Clerk was asked");
    assert_eq!(status, "creating");
    assert_eq!(until, None, "the lease was released when the request was dropped");
}

/// N6: no global lock. A bucket lock held elsewhere stalls only that
/// bucket (answered `409 create_in_progress` after the lock timeout), never
/// another bucket; and a creator's verified email is read from Clerk once.
#[tokio::test]
async fn n6_a_busy_bucket_stalls_only_itself_and_the_creator_lookup_is_cached() {
    const NAME: &str = "n6_a_busy_bucket_stalls_only_itself_and_the_creator_lookup_is_cached";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let busy_dom = format!("busy-{}.test", Uuid::new_v4().simple());
    let free_dom = format!("free-{}.test", Uuid::new_v4().simple());
    let (busy_user, busy_clerk) = new_user(&p.owner).await;
    cmock.add_user_email(&busy_clerk, &format!("a@{busy_dom}"));
    let (free_user, free_clerk) = new_user(&p.owner).await;
    cmock.add_user_email(&free_clerk, &format!("b@{free_dom}"));

    // Another transaction holds the busy bucket's lock, and the old global
    // key every create used to queue on.
    let mut holder = p.owner.begin().await.unwrap();
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0)), \
                pg_advisory_xact_lock(hashtextextended('org_selfserve_create_caps', 0))",
    )
    .bind(format!("org_selfserve_create_bucket:{busy_dom}"))
    .execute(&mut *holder)
    .await
    .unwrap();
    let free = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        create(&state, &clerk, free_user, &Uuid::new_v4().to_string(), req("Free Bucket", 2), &CAPS),
    )
    .await;
    let busy = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        create(&state, &clerk, busy_user, &Uuid::new_v4().to_string(), req("Busy Bucket", 2), &CAPS),
    )
    .await;
    holder.rollback().await.unwrap();
    // A second sign-up by the same creator: Clerk is not asked again.
    let again = create(&state, &clerk, free_user, &Uuid::new_v4().to_string(), req("Free Again", 2), &CAPS).await;
    let reads = cmock.user_reads(&free_clerk);
    let mut orgs: Vec<String> = Vec::new();
    if let Ok((_, b)) = &free {
        orgs.extend(b["org_id"].as_str().map(str::to_string));
    }
    orgs.extend(again.1["org_id"].as_str().map(str::to_string));
    for o in &orgs {
        drop_org(&p.owner, o).await;
    }
    let users = vec![busy_user, free_user];
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = ANY($1)").bind(&users).execute(&p.owner).await.ok();
    drop_users(&p.owner, &users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    let free = free.expect("another bucket's create is not queued behind a held lock");
    assert_eq!(free.0, StatusCode::CREATED, "{}", free.1);
    let busy = busy.expect("the busy bucket answers within the lock timeout");
    assert_eq!((busy.0, busy.1["code"].as_str()), (StatusCode::CONFLICT, Some("create_in_progress")), "{}", busy.1);
    assert_eq!(again.0, StatusCode::CREATED, "{}", again.1);
    assert_eq!(reads, 1, "the verified email was cached");
}

/// N7: the failed-job sweep never cancels or deletes a Clerk org without
/// the self-serve marker; it marks that org's cleanup done and alerts.
#[tokio::test]
async fn n7_the_failed_sweep_never_deletes_an_unmarked_clerk_org() {
    const NAME: &str = "n7_the_failed_sweep_never_deletes_an_unmarked_clerk_org";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, _smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let (plain, marked) = (format!("org_n7p{tag}"), format!("org_n7m{tag}"));
    let plain_account = seed_org(&p.owner, &plain, u, &[(u, "admin")], 48).await;
    let marked_account = seed_org(&p.owner, &marked, u, &[(u, "admin")], 48).await;
    cmock.add_org(json!({"id": plain, "name": "concierge", "created_by": "user_x"}));
    cmock.add_org(marked_org(&marked));
    failed_job(&p.owner, &plain, plain_account, 2).await;
    failed_job(&p.owner, &marked, marked_account, 2).await;
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let plain_job = job_row(&p.owner, &plain).await;
    let deletes = cmock.deletes();
    drop_org(&p.owner, &plain).await;
    drop_org(&p.owner, &marked).await;
    drop_users(&p.owner, &[u]).await;

    assert!(r.unmarked_skipped.contains(&plain), "{r:?}");
    assert!(!deletes.contains(&plain), "an unmarked Clerk org is never deleted: {deletes:?}");
    assert_eq!(plain_job.1, "done", "its cleanup is settled, not retried every pass");
    assert!(deletes.contains(&marked), "a marked org is still cleaned up: {deletes:?}");
    assert!(r.failed_deleted.contains(&marked), "{r:?}");
}

/// The gap from web PR B: `GET /v1/bootstrap`'s `pending_orgs` lists the
/// caller's self-serve orgs that are not ready, for their creator and their
/// members, and nothing that is ready, concierge, or someone else's.
#[tokio::test]
async fn bootstrap_pending_orgs_lists_only_the_callers_not_ready_selfserve_orgs() {
    const NAME: &str = "bootstrap_pending_orgs_lists_only_the_callers_not_ready_selfserve_orgs";
    let Some(p) = pools(NAME).await else { return };
    let (creator, _) = new_user(&p.owner).await;
    let (member, _) = new_user(&p.owner).await;
    let (stranger, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let (pending, ready) = (format!("org_bp{tag}"), format!("org_br{tag}"));
    seed_org(&p.owner, &pending, creator, &[(creator, "admin"), (member, "member")], 1).await;
    let ready_account = seed_org(&p.owner, &ready, creator, &[(creator, "admin")], 1).await;
    failed_job(&p.owner, &ready, ready_account, 0).await;
    sqlx::query(
        "UPDATE tenant_provisioning_jobs SET status = 'ready', step = 'done', clerk_org_cleanup = 'none', failed_at = NULL \
          WHERE clerk_org_id = $1",
    )
    .bind(&ready)
    .execute(&p.owner)
    .await
    .unwrap();
    let concierge = format!("org_bc{tag}");
    let concierge_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(creator)
            .bind(&concierge)
            .fetch_one(&p.owner)
            .await
            .unwrap();

    let for_creator = pending_orgs_for(&p.app, creator).await;
    let for_member = pending_orgs_for(&p.app, member).await;
    let for_stranger = pending_orgs_for(&p.app, stranger).await;
    drop_org(&p.owner, &pending).await;
    drop_org(&p.owner, &ready).await;
    drop_org(&p.owner, &concierge).await;
    let _ = concierge_account;
    drop_users(&p.owner, &[creator, member, stranger]).await;

    let want = vec![json!({"org_id": pending, "status": "awaiting_payment"})];
    assert_eq!(for_creator, Ok(want.clone()), "the creator sees the unpaid org, not the ready one");
    assert_eq!(for_member, Ok(want), "a member sees it too");
    assert_eq!(for_stranger, Ok(Vec::new()));
}

// ── #1540 review fixes ───────────────────────────────────────────────────

/// A held org whose collection the sweep paused, with its Stripe trial
/// ended inside the pause (subscription `active`), then built by an
/// operator. Returns `(org, account, sub, paused_at)`; the caller sets up
/// the invoice Stripe voided and runs the resume pass.
async fn paused_held_org(
    p: &Pools,
    sweeper: &Sweeper<'_>,
    smock: &StripeMock,
    cmock: &ClerkMock,
    u: Uuid,
    tag: &str,
) -> (String, Uuid, String, chrono::DateTime<chrono::Utc>) {
    let org = format!("org_rv{tag}");
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 48).await;
    cmock.add_org(marked_org(&org));
    failed_job(&p.owner, &org, account, 2).await;
    sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup_requeues = 2 WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    let (sub, cus) = (format!("sub_rv{tag}"), format!("cus_rv{tag}"));
    mirror(&p.owner, account, &cus, Some(&sub), "active").await;
    smock.set_sub(&cus, &sub, "trialing");
    trial_at_backstop(&p.owner, account, u, &sub, tag).await;
    let r1 = sweeper.sweep_once(chrono::Utc::now()).await;
    assert!(r1.collection_paused.contains(&sub), "{r1:?}");
    let paused = paused_at(&p.owner, &org).await.expect("paused");
    smock.set_sub_sequence(&sub, &["active"]);
    sqlx::query("UPDATE tenant_provisioning_jobs SET status = 'ready', step = 'done', clerk_org_cleanup = 'none' WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    (org, account, sub, paused)
}

/// SF-b: the conversion invoice was DRAFTED before the pause and voided
/// during it (Stripe drafts a renewal about an hour ahead). It is judged by
/// `status_transitions.voided_at`, not `created`, so the resume re-anchors.
#[tokio::test]
async fn sf_b_a_draft_made_before_the_pause_and_voided_during_it_is_reanchored() {
    const NAME: &str = "sf_b_a_draft_made_before_the_pause_and_voided_during_it_is_reanchored";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let (org, account, sub, paused) = paused_held_org(&p, &sweeper, &smock, &cmock, u, &tag).await;
    let invoice = format!("in_draft{tag}");
    smock.set_invoice(&sub, &invoice, "void", paused.timestamp() - 3600);
    smock.set_invoice_voided_at(&invoice, paused.timestamp() + 60);
    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let posts = smock.posts(&format!("/v1/subscriptions/{sub}"));
    sqlx::query("DELETE FROM org_trials WHERE account_id = $1").bind(account).execute(&p.owner).await.ok();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(r.collection_resumed.contains(&sub), "{r:?}");
    assert!(r.collection_reanchored.contains(&sub), "voided during the pause, though drafted before it: {r:?}");
    assert!(posts.iter().any(|(_, body, _)| body.contains("billing_cycle_anchor=now")), "{posts:?}");
}

/// SF-c: a resumed subscription the customer scheduled to cancel
/// (`cancel_at_period_end`) is not re-anchored: no fresh period is charged,
/// and the trial worker then cancels it.
#[tokio::test]
async fn sf_c_a_subscription_scheduled_to_cancel_is_not_reanchored_and_is_canceled() {
    const NAME: &str = "sf_c_a_subscription_scheduled_to_cancel_is_not_reanchored_and_is_canceled";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let (org, account, sub, paused) = paused_held_org(&p, &sweeper, &smock, &cmock, u, &tag).await;
    smock.set_invoice(&sub, &format!("in_void{tag}"), "void", paused.timestamp() + 60);
    smock.set_sub_extra(&sub, json!({"cancel_at_period_end": true}));
    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let posts = smock.posts(&format!("/v1/subscriptions/{sub}"));
    let worker =
        crate::billing::org_trial::lifecycle::process_account(&p.app, Some(stripe.as_ref()), account, chrono::Utc::now()).await;
    let cancels = smock.calls("DELETE", &format!("/v1/subscriptions/{sub}")).len();
    let state = trial_state(&p.owner, account).await;
    sqlx::query("DELETE FROM org_trials WHERE account_id = $1").bind(account).execute(&p.owner).await.ok();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(r.collection_resumed.contains(&sub), "{r:?}");
    assert!(!r.collection_reanchored.contains(&sub), "scheduled to cancel: no fresh period: {r:?}");
    assert!(!posts.iter().any(|(_, body, _)| body.contains("billing_cycle_anchor")), "{posts:?}");
    assert!(worker.is_ok(), "{worker:?}");
    assert_eq!(cancels, 1, "the trial worker cancels it (state {state:?})");
}

/// N7 (review follow-up): a failed job whose org has no self-serve creation
/// record is never cancelled or deleted, even when Clerk answers 404 for
/// the org (which used to let the sweep go ahead).
#[tokio::test]
async fn n7_a_clerk_404_without_a_creation_record_never_cancels() {
    const NAME: &str = "n7_a_clerk_404_without_a_creation_record_never_cancels";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let org = format!("org_n7c{tag}");
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(u)
        .bind(&org)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    failed_job(&p.owner, &org, account, 2).await;
    let (sub, cus) = (format!("sub_n7c{tag}"), format!("cus_n7c{tag}"));
    mirror(&p.owner, account, &cus, Some(&sub), "past_due").await;
    smock.set_sub(&cus, &sub, "past_due");
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let job = job_row(&p.owner, &org).await;
    let cancels = smock.calls("DELETE", &format!("/v1/subscriptions/{sub}")).len();
    let deletes = cmock.deletes();
    sqlx::query("DELETE FROM tenant_provisioning_jobs WHERE clerk_org_id = $1").bind(&org).execute(&p.owner).await.ok();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert_eq!(cancels, 0, "no creation record: the subscription is never canceled");
    assert!(!deletes.contains(&org), "{deletes:?}");
    assert!(r.unmarked_skipped.contains(&org), "{r:?}");
    assert_eq!(job.1, "done");
}

/// SF-e: an org with an `active` routing row is served, so it is never
/// listed as pending, whatever its job says (a `held_routed` org).
#[tokio::test]
async fn sf_e_pending_orgs_skip_an_org_with_an_active_routing_row() {
    const NAME: &str = "sf_e_pending_orgs_skip_an_org_with_an_active_routing_row";
    let Some(p) = pools(NAME).await else { return };
    let (creator, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let (routed, pending) = (format!("org_sfer{tag}"), format!("org_sfep{tag}"));
    let routed_account = seed_org(&p.owner, &routed, creator, &[(creator, "admin")], 1).await;
    seed_org(&p.owner, &pending, creator, &[(creator, "admin")], 1).await;
    failed_job(&p.owner, &routed, routed_account, 0).await;
    sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup = 'held' WHERE clerk_org_id = $1")
        .bind(&routed)
        .execute(&p.owner)
        .await
        .unwrap();
    let slug = crate::tenancy::provisioning::opaque_slug_for_account(routed_account);
    fin(
        &p.owner,
        routed_account,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(&routed)
        .bind(routed_account)
        .bind(&slug)
        .bind(crate::tenancy::provisioning::secret_ref_for_slug(&slug)),
    )
    .await;
    let listed = pending_orgs_for(&p.app, creator).await;
    fin(&p.owner, routed_account, sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id = $1").bind(&routed)).await;
    drop_org(&p.owner, &routed).await;
    drop_org(&p.owner, &pending).await;
    drop_users(&p.owner, &[creator]).await;

    let listed = listed.expect("read");
    let ids: Vec<&str> = listed.iter().filter_map(|o| o["org_id"].as_str()).collect();
    assert_eq!(ids, vec![pending.as_str()], "the routed org is served, not pending: {listed:?}");
}

/// SF-a: `GET /v1/bootstrap`'s `selfserve_org` is true only for an account
/// `POST /v1/orgs` made (a `created` intent row), never for a concierge org
/// or a personal account.
#[tokio::test]
async fn sf_a_selfserve_org_is_true_only_for_an_account_post_v1_orgs_made() {
    const NAME: &str = "sf_a_selfserve_org_is_true_only_for_an_account_post_v1_orgs_made";
    let Some(p) = pools(NAME).await else { return };
    let (creator, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let (selfserve, concierge) = (format!("org_ssa{tag}"), format!("org_ssc{tag}"));
    let selfserve_account = seed_org(&p.owner, &selfserve, creator, &[(creator, "admin")], 1).await;
    let concierge_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(creator)
            .bind(&concierge)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let a = selfserve_org_for(&p.app, selfserve_account).await;
    let b = selfserve_org_for(&p.app, concierge_account).await;
    let c = selfserve_org_for(&p.app, Uuid::new_v4()).await;
    drop_org(&p.owner, &selfserve).await;
    drop_org(&p.owner, &concierge).await;
    drop_users(&p.owner, &[creator]).await;

    assert_eq!(a, Ok(true));
    assert_eq!(b, Ok(false), "a concierge org keeps Clerk's org profile");
    assert_eq!(c, Ok(false), "a personal account");
}

/// SF-d + retention: the sweep clears `cap_bucket` on intent rows older
/// than 24 h (never on a newer one), and deletes card-fingerprint trial
/// claims older than 24 months (never a newer card claim, never a user
/// claim).
#[tokio::test]
async fn sf_d_the_sweep_clears_day_old_cap_buckets_and_expires_old_card_claims() {
    const NAME: &str = "sf_d_the_sweep_clears_day_old_cap_buckets_and_expires_old_card_claims";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, _smock) = spawn_stripe().await;
    let (clerk, _cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let mut intents = Vec::new();
    for hours in [25, 2] {
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status, cap_bucket, created_at) \
             VALUES ($1, $2, 'Old', 2, 'refused', $3, now() - make_interval(hours => $4)) RETURNING id",
        )
        .bind(u)
        .bind(format!("ret-{hours}-{tag}"))
        .bind(format!("*freemail:{tag}"))
        .bind(hours)
        .fetch_one(&p.owner)
        .await
        .unwrap();
        intents.push(id);
    }
    let claim_account = Uuid::new_v4();
    let (old_card, new_card, old_user) = (format!("fp_old{tag}"), format!("fp_new{tag}"), Uuid::new_v4().to_string());
    for (kind, value, months) in [("card", &old_card, 25), ("card", &new_card, 23), ("user", &old_user, 30)] {
        sqlx::query(
            "INSERT INTO org_trial_claims (claim_kind, claim_value, account_id, claimed_at) \
             VALUES ($1, $2, $3, now() - make_interval(months => $4))",
        )
        .bind(kind)
        .bind(value)
        .bind(claim_account)
        .bind(months)
        .execute(&p.owner)
        .await
        .unwrap();
    }
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let mut buckets = Vec::new();
    for id in &intents {
        let b: Option<String> = sqlx::query_scalar("SELECT cap_bucket FROM org_selfserve_creations WHERE id = $1")
            .bind(id)
            .fetch_one(&p.owner)
            .await
            .unwrap();
        buckets.push(b);
    }
    let claims: Vec<String> =
        sqlx::query_scalar("SELECT claim_value FROM org_trial_claims WHERE account_id = $1 ORDER BY claim_value")
            .bind(claim_account)
            .fetch_all(&p.owner)
            .await
            .unwrap();
    sqlx::query("DELETE FROM org_trial_claims WHERE account_id = $1").bind(claim_account).execute(&p.owner).await.ok();
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = $1").bind(u).execute(&p.owner).await.ok();
    drop_users(&p.owner, &[u]).await;

    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(buckets[0], None, "a row older than a day holds no bucket");
    assert_eq!(buckets[1], Some(format!("*freemail:{tag}")), "a recent row still counts against its cap");
    let mut want = vec![new_card.clone(), old_user.clone()];
    want.sort();
    assert_eq!(claims, want, "only the card claim older than 24 months is gone");
    assert!(r.card_claims_expired >= 1, "{r:?}");
}

/// Freemail regional variants (#1540 review): `yahoo.co.uk` is free mail, so
/// two aliases of one mailbox there share the per-mailbox daily cap instead
/// of counting as a company domain.
#[tokio::test]
async fn a_regional_freemail_domain_is_bucketed_per_mailbox() {
    const NAME: &str = "a_regional_freemail_domain_is_bucketed_per_mailbox";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let base = format!("jb{}", &Uuid::new_v4().simple().to_string()[..12]);
    let mut users = Vec::new();
    for email in [format!("{base}+a@yahoo.co.uk"), format!("{base}+b@Yahoo.co.uk")] {
        let (u, clerk_id) = new_user(&p.owner).await;
        cmock.add_user_email(&clerk_id, &email);
        users.push(u);
    }
    let caps = Caps { max_per_user_per_day: 1, ..CAPS };
    let mut out = Vec::new();
    for (i, u) in users.iter().enumerate() {
        out.push(create(&state, &clerk, *u, &Uuid::new_v4().to_string(), req(&format!("Regional {i}"), 2), &caps).await);
    }
    let bucket: Option<String> =
        sqlx::query_scalar("SELECT cap_bucket FROM org_selfserve_creations WHERE user_id = $1 ORDER BY created_at LIMIT 1")
            .bind(users[0])
            .fetch_optional(&p.owner)
            .await
            .unwrap()
            .flatten();
    for (_, b) in &out {
        if let Some(o) = b["org_id"].as_str() {
            drop_org(&p.owner, o).await;
        }
    }
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = ANY($1)").bind(&users).execute(&p.owner).await.ok();
    drop_users(&p.owner, &users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(out[0].0, StatusCode::CREATED, "{}", out[0].1);
    assert!(is_freemail_bucket(bucket.as_deref()), "{bucket:?}");
    assert!(!bucket.as_deref().unwrap_or("").contains(&base), "no address in the row: {bucket:?}");
    assert_eq!(
        (out[1].0, out[1].1["code"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("org_create_rate_limited")),
        "a second alias of the same yahoo.co.uk mailbox: {}",
        out[1].1
    );
}
