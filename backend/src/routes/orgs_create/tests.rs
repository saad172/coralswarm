//! Pure rules of `POST /v1/orgs` and the status route (self-serve signup A2).

use super::*;
use chrono::TimeZone;

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 27, 12, 0, 0).unwrap()
}

#[test]
fn names_are_trimmed_bounded_and_never_personal_shaped() {
    assert_eq!(validate_name("  Acme Corp  ").as_deref(), Some("Acme Corp"));
    assert!(validate_name("a").is_none());
    assert!(validate_name("---").is_none());
    assert!(validate_name("Ada's Organization").is_none());
    assert!(validate_name("bad\u{0}name").is_none());
    assert!(validate_name(&"x".repeat(121)).is_none());
    assert!(validate_name(&"x".repeat(120)).is_some());
    assert!(validate_name("Ünïcode Co").is_some());
}

#[test]
fn the_idempotency_key_is_required_and_bounded() {
    assert_eq!(validate_idempotency_key(None), None);
    assert_eq!(validate_idempotency_key(Some("short")), None);
    assert_eq!(validate_idempotency_key(Some("has space in it")), None);
    assert_eq!(validate_idempotency_key(Some(&"k".repeat(201))), None);
    let uuid = Uuid::new_v4().to_string();
    assert_eq!(validate_idempotency_key(Some(&format!(" {uuid} "))), Some(uuid));
    assert_eq!(validate_idempotency_key(Some("signup:2026.10.27_a")), Some("signup:2026.10.27_a".into()));
}

/// No per-org subdomain (founder 2026-10-27): Clerk's slug is a hidden key,
/// unique per intent, and never the routing key shape A1 reserves.
#[test]
fn the_clerk_slug_is_opaque_and_never_a_routing_key() {
    let id = Uuid::new_v4();
    let slug = clerk_slug_for_intent(id);
    assert_eq!(slug, format!("cs-{}", id.simple()));
    assert!(slug.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
    assert!(!crate::tenancy::routing::is_selfserve_slug_shape(&slug));
    assert_ne!(clerk_slug_for_intent(Uuid::new_v4()), slug);
}

#[test]
fn the_caps_refuse_pending_daily_and_per_domain_and_only_throttle_globally() {
    let caps = Caps {
        max_pending_per_user: 2,
        max_per_user_per_day: 5,
        max_per_hour: 30,
        max_per_domain_per_hour: 10,
        max_freemail_per_hour: 20,
    };
    let ok = CreationCounts::default();
    assert_eq!(decide_caps(&ok, &caps, false, now()), CapDecision::Allow { throttled: false });

    let pending = CreationCounts { pending_for_user: 2, ..ok };
    assert!(matches!(decide_caps(&pending, &caps, false, now()), CapDecision::Refuse { code: "too_many_pending_orgs", .. }));

    let oldest = now() - chrono::Duration::hours(20);
    let daily = CreationCounts { user_last_day: 5, user_oldest_in_day: Some(oldest), ..ok };
    assert_eq!(
        decide_caps(&daily, &caps, false, now()),
        CapDecision::Refuse { code: "org_create_rate_limited", retry_after_s: 4 * 3600, throttled: false },
        "retry once the oldest of the day leaves the window"
    );

    // S8: the per-domain bucket refuses; free mail has its own (larger) cap.
    let bucket = CreationCounts {
        bucket_last_hour: 10,
        bucket_oldest_in_hour: Some(now() - chrono::Duration::minutes(50)),
        ..ok
    };
    assert_eq!(
        decide_caps(&bucket, &caps, false, now()),
        CapDecision::Refuse { code: "org_create_rate_limited", retry_after_s: 600, throttled: false }
    );
    assert_eq!(decide_caps(&bucket, &caps, true, now()), CapDecision::Allow { throttled: false }, "free-mail cap is 20");

    // S8: past the global threshold nothing is refused for that alone; the
    // per-creator pending cap drops to 1 and the bucket caps halve.
    let global = CreationCounts { global_last_hour: 30, ..ok };
    assert_eq!(decide_caps(&global, &caps, false, now()), CapDecision::Allow { throttled: true });
    let global_one_pending = CreationCounts { pending_for_user: 1, ..global };
    assert!(matches!(
        decide_caps(&global_one_pending, &caps, false, now()),
        CapDecision::Refuse { code: "too_many_pending_orgs", throttled: true, .. }
    ));
    let global_half_bucket = CreationCounts { bucket_last_hour: 5, ..global };
    assert!(matches!(decide_caps(&global_half_bucket, &caps, false, now()), CapDecision::Refuse { throttled: true, .. }));

    // Zero per-creator caps refuse everything (an operator's kill switch).
    let off = Caps { max_pending_per_user: 0, max_per_user_per_day: 0, ..caps };
    assert!(matches!(decide_caps(&ok, &off, false, now()), CapDecision::Refuse { .. }));
}

#[test]
fn the_cap_bucket_is_the_registrable_domain_or_the_freemail_mailbox() {
    assert_eq!(cap_bucket_for("a@Acme.COM").as_deref(), Some("acme.com"));
    assert_eq!(cap_bucket_for("a@acme.com.").as_deref(), Some("acme.com"));
    // #1540 SF6: subdomains are their registrable domain (eTLD+1).
    assert_eq!(cap_bucket_for("a@x.evil.com").as_deref(), Some("evil.com"));
    assert_eq!(cap_bucket_for("a@deep.x.evil.com").as_deref(), Some("evil.com"));
    assert_eq!(cap_bucket_for("a@mail.acme.co.uk").as_deref(), Some("acme.co.uk"), "multi-label public suffix");
    assert_eq!(cap_bucket_for("a@x.acme-1.test").as_deref(), Some("acme-1.test"), "unknown TLD: the default rule");
    // Free mail: one bucket per canonical mailbox, never the address itself.
    let jdoe = cap_bucket_for("person@example.com").unwrap();
    assert!(jdoe.starts_with(&format!("{FREEMAIL_BUCKET}:")) && !jdoe.contains("jdoe"), "{jdoe}");
    for alias in ["person@example.com", "person@example.com", "j.d.o.e+promo@GoogleMail.com", "jdoe@mail.gmail.com"] {
        assert_eq!(cap_bucket_for(alias).as_deref(), Some(jdoe.as_str()), "{alias} is person@example.com");
    }
    assert_ne!(cap_bucket_for("person@example.com").as_deref(), Some(jdoe.as_str()));
    // Dots are only folded for Gmail; `+tags` everywhere.
    assert_eq!(cap_bucket_for("a.b+x@outlook.com"), cap_bucket_for("a.b@outlook.com"));
    assert_ne!(cap_bucket_for("a.b@outlook.com"), cap_bucket_for("ab@outlook.com"));
    assert!(is_freemail_bucket(cap_bucket_for("a@Outlook.com").as_deref()));
    assert!(!is_freemail_bucket(cap_bucket_for("a@acme.com").as_deref()));
    assert_eq!(cap_bucket_for("no-at-sign"), None);
    assert_eq!(cap_bucket_for("a@"), None);
}

/// #1540 review SF-d: a free-mail bucket is an HMAC under a server key, not
/// a bare hash anyone could recompute for a guessed address; and regional
/// free-mail domains are free mail.
#[test]
fn the_freemail_bucket_is_keyed_and_regional_domains_are_freemail() {
    use sha2::{Digest, Sha256};
    let key_a = crate::crypto::kdf::derive_key(&[7u8; 32], crate::crypto::kdf::purpose::SELFSERVE_CAP_BUCKET);
    let key_b = crate::crypto::kdf::derive_key(&[8u8; 32], crate::crypto::kdf::purpose::SELFSERVE_CAP_BUCKET);
    let keyed = cap_bucket_with_key("person@example.com", Some(&key_a)).unwrap();
    let plain = hex::encode(&Sha256::digest(b"person@example.com")[..16]);
    assert_eq!(keyed, cap_bucket_with_key("jdoe@googlemail.com", Some(&key_a)).unwrap(), "aliases still share it");
    assert!(keyed.starts_with(&format!("{FREEMAIL_BUCKET}:")) && is_freemail_bucket(Some(&keyed)), "{keyed}");
    assert!(!keyed.contains(&plain), "not the bare SHA-256 of the mailbox: {keyed}");
    assert_ne!(keyed, cap_bucket_with_key("person@example.com", Some(&key_b)).unwrap(), "the key matters");
    let unkeyed = cap_bucket_with_key("person@example.com", None).unwrap();
    assert!(unkeyed.starts_with(&format!("{FREEMAIL_BUCKET}:u:")), "an unkeyed digest is marked: {unkeyed}");
    assert!(keyed.len() <= 255 && unkeyed.len() <= 255);
    assert_eq!(cap_bucket_with_key("a@acme.com", Some(&key_a)).as_deref(), Some("acme.com"), "company mail is unchanged");
    // Regional variants.
    for d in ["yahoo.co.uk", "yahoo.fr", "hotmail.co.uk", "hotmail.fr", "outlook.fr", "live.co.uk"] {
        assert!(FREEMAIL_DOMAINS.contains(&d), "{d}");
    }
    assert!(is_freemail_bucket(cap_bucket_with_key("a@Mail.Yahoo.co.uk", Some(&key_a)).as_deref()));
    assert_eq!(
        cap_bucket_with_key("x+1@hotmail.fr", Some(&key_a)),
        cap_bucket_with_key("x+2@hotmail.fr", Some(&key_a)),
        "one hotmail.fr mailbox"
    );
}

/// #1540 review SF-b: "voided during the pause" is read from
/// `status_transitions.voided_at`, falling back to `created` only when it is
/// missing.
#[test]
fn voided_during_the_pause_is_judged_by_voided_at() {
    use crate::tenancy::org_selfserve_cleanup::voided_during_pause;
    let paused = now();
    let t = paused.timestamp();
    let inv = |status: &str, created: i64, voided_at: Option<i64>| {
        json!({"status": status, "created": created, "status_transitions": {"voided_at": voided_at}})
    };
    assert!(voided_during_pause(&inv("void", t - 3600, Some(t + 60)), paused), "drafted before, voided during");
    assert!(!voided_during_pause(&inv("void", t - 3600, Some(t - 60)), paused), "voided before the pause");
    assert!(!voided_during_pause(&inv("void", t + 60, Some(t - 1)), paused), "voided_at wins over created");
    assert!(voided_during_pause(&inv("void", t + 60, None), paused), "no voided_at: created decides");
    assert!(voided_during_pause(&json!({"status": "void", "created": t}), paused), "no status_transitions at all");
    assert!(!voided_during_pause(&inv("void", t - 60, None), paused));
    assert!(!voided_during_pause(&inv("paid", t + 60, Some(t + 60)), paused), "not void");
    assert!(!voided_during_pause(&json!({"status": "void"}), paused), "no time at all");
}

#[test]
fn freemail_caps_count_the_person_and_the_aggregate() {
    let caps = Caps {
        max_pending_per_user: 2,
        max_per_user_per_day: 5,
        max_per_hour: 1000,
        max_per_domain_per_hour: 10,
        max_freemail_per_hour: 200,
    };
    let ok = CreationCounts::default();
    // One person's aliases: the daily per-person cap applies to the bucket.
    let person = CreationCounts { bucket_last_day: 5, bucket_oldest_in_day: Some(now() - chrono::Duration::hours(23)), ..ok };
    assert_eq!(
        decide_caps(&person, &caps, true, now()),
        CapDecision::Refuse { code: "org_create_rate_limited", retry_after_s: 3600, throttled: false }
    );
    // The free-mail aggregate is a backstop, not the hourly domain cap.
    let busy_hour = CreationCounts { freemail_last_hour: 199, bucket_last_hour: 50, ..ok };
    assert_eq!(decide_caps(&busy_hour, &caps, true, now()), CapDecision::Allow { throttled: false });
    let full = CreationCounts { freemail_last_hour: 200, ..ok };
    assert!(matches!(decide_caps(&full, &caps, true, now()), CapDecision::Refuse { .. }));
    // A company domain is not limited by the free-mail counts.
    assert_eq!(decide_caps(&full, &caps, false, now()), CapDecision::Allow { throttled: false });
}

/// N6: NFKC folds look-alikes, and format (`Cf`) characters are refused.
#[test]
fn names_are_nfkc_normalised_and_refuse_format_characters() {
    assert_eq!(validate_name("Ａｃｍｅ").as_deref(), Some("Acme"), "full-width folds to ASCII");
    assert_eq!(validate_name("ﬁnance").as_deref(), Some("finance"), "ligature folds");
    for c in ['\u{200B}', '\u{202E}', '\u{FEFF}', '\u{2066}', '\u{00AD}', '\u{E0041}', '\u{061C}', '\u{180E}'] {
        assert!(is_format_char(c), "{:04X}", c as u32);
        assert!(validate_name(&format!("Acme{c}Corp")).is_none(), "{:04X} is refused", c as u32);
    }
    for c in ['a', ' ', 'é', '中', '-', '\u{00A0}'] {
        assert!(!is_format_char(c), "{:04X}", c as u32);
    }
}

fn job(status: JobStatus, cleanup: ClerkOrgCleanup) -> JobFacts {
    JobFacts {
        status: Some(status),
        step: "migrate".into(),
        waiting_for_payment: false,
        waiting_for_build: false,
        cleanup: Some(cleanup),
    }
}

#[test]
fn the_status_follows_the_trial_then_the_job() {
    let opened = now() - chrono::Duration::hours(2);
    // Before payment.
    let s = derive_status(None, None, now());
    assert_eq!((s.status, s.resume_payment_until), (SetupStatus::AwaitingPayment, None));
    let s = derive_status(Some((TrialState::CheckoutOpen, opened)), None, now());
    assert_eq!(s.status, SetupStatus::AwaitingPayment);
    assert_eq!(s.resume_payment_until, Some(opened + chrono::Duration::hours(24)));
    let s = derive_status(Some((TrialState::CheckoutOpen, now() - chrono::Duration::hours(25))), None, now());
    assert_eq!(s.status, SetupStatus::PaymentAbandoned, "past the window even before the worker marks it");
    assert_eq!(derive_status(Some((TrialState::Abandoned, opened)), None, now()).status, SetupStatus::PaymentAbandoned);
    let s = derive_status(Some((TrialState::Converting, opened)), None, now());
    assert_eq!((s.status, s.waiting_for), (SetupStatus::AwaitingPayment, Some("payment")));
    assert_eq!(derive_status(Some((TrialState::Canceled, opened)), None, now()).status, SetupStatus::Canceled);
    let s = derive_status(Some((TrialState::Trialing, opened)), None, now());
    assert_eq!((s.status, s.step.as_deref()), (SetupStatus::Provisioning, Some("queued")));

    // The job decides once it exists, whatever the trial says.
    let trialing = Some((TrialState::Trialing, opened));
    let mut j = job(JobStatus::Provisioning, ClerkOrgCleanup::None);
    let s = derive_status(trialing, Some(&j), now());
    assert_eq!((s.status, s.step.as_deref(), s.waiting_for), (SetupStatus::Provisioning, Some("migrate"), None));
    j.waiting_for_payment = true;
    assert_eq!(derive_status(trialing, Some(&j), now()).waiting_for, Some("payment"));
    j.waiting_for_payment = false;
    j.waiting_for_build = true;
    assert_eq!(derive_status(trialing, Some(&j), now()).waiting_for, Some("update"));
    assert_eq!(derive_status(trialing, Some(&job(JobStatus::Ready, ClerkOrgCleanup::None)), now()).status, SetupStatus::Ready);

    // Failed: retryable exactly when the owner's retry would be accepted.
    for (cleanup, retryable) in [
        (ClerkOrgCleanup::Pending, true),
        (ClerkOrgCleanup::Held, true),
        (ClerkOrgCleanup::Deleting, false),
        (ClerkOrgCleanup::Done, false),
    ] {
        let s = derive_status(trialing, Some(&job(JobStatus::Failed, cleanup)), now());
        assert_eq!((s.status, s.retryable), (SetupStatus::Failed, retryable), "{cleanup:?}");
    }
    // An unreadable job status is never reported as ready.
    let unknown = JobFacts { status: None, ..job(JobStatus::Ready, ClerkOrgCleanup::None) };
    assert_eq!(derive_status(trialing, Some(&unknown), now()).status, SetupStatus::Failed);
}

/// The view is what the browser sees: a fixed vocabulary and fixed copy,
/// nothing taken from the job's operator-facing text.
#[test]
fn the_status_view_serialises_to_a_fixed_vocabulary() {
    let s = derive_status(Some((TrialState::CheckoutOpen, now())), None, now());
    let v = serde_json::to_value(&s).unwrap();
    assert_eq!(v["status"], "awaiting_payment");
    let keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, ["message", "resume_payment_until", "retryable", "status", "step", "waiting_for"]);
    let failed = derive_status(None, Some(&job(JobStatus::Failed, ClerkOrgCleanup::Pending)), now());
    assert_eq!(serde_json::to_value(&failed).unwrap()["status"], "failed");
}

/// The gate's 503 while an org's routing row is being built carries
/// `"status":"provisioning"`; `error` is unchanged for old clients.
#[tokio::test]
async fn the_gate_503_while_provisioning_is_readable() {
    let resp = crate::middleware::auth::provisioning_unavailable();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body, json!({"error": "service unavailable", "status": "provisioning"}));
}

/// #1556: only a Clerk 422 about the name/slug says "try a different name".
#[test]
fn only_a_422_name_or_slug_code_is_a_name_refusal() {
    for code in ["form_identifier_exists", "form_param_value_invalid", "form_param_format_invalid", "form_param_exceeds_allowed_size"] {
        assert!(is_name_refusal(422, Some(code)), "{code}");
        // The same code on another status is not a name refusal.
        assert!(!is_name_refusal(403, Some(code)), "{code} on 403");
    }
    // The staging repro, plan/quota limits, and a code-less 4xx.
    for (status, code) in [
        (403, Some("organization_slugs_disabled")),
        (422, Some("organization_slugs_disabled")),
        (403, Some("organization_not_enabled_in_instance")),
        (402, Some("plan_limit_exceeded")),
        (422, Some("quota_exceeded")),
        // A nil or missing param is our own malformed request, not the name.
        (422, Some("form_param_nil")),
        (422, Some("form_param_missing")),
        (401, Some("authentication_invalid")),
        (403, None),
        (422, None),
        (400, None),
    ] {
        assert!(!is_name_refusal(status, code), "{status} {code:?}");
    }
}
