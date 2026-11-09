//! The words and the wire for billing notices: one template per
//! [`NoticeKind`] and audience, and the Resend call that sends it.
//!
//! # Plain words
//!
//! Settings launch plan decision 5: no "ocean", "reef" or "coral" in anything
//! a customer reads about their account. The copy talks about "extra usage",
//! "your plan credits", "Settings -> Billing" and "your organization". A test
//! pins the word list across every kind and audience.
//!
//! # Dollars
//!
//! Admin and owner emails state the limit or the reload amount in dollars:
//! they set those numbers. The member email (a member hit their own cap)
//! carries no money at all, the same rule as the "Your usage" card
//! (decision 15).
//!
//! # The shell
//!
//! Same cream-document shell as `workers::org_join_request_digest` and
//! `routes::org_invites` (table layout, inline styles, no webfont loads), kept
//! as this module's own copy like every other email module in the crate.

use std::time::Duration;

use chrono::{DateTime, Utc};

use super::NoticeKind;

const FONT_SANS: &str = "Geist,-apple-system,'Segoe UI',system-ui,sans-serif";
const FONT_SERIF: &str = "'Instrument Serif',Georgia,'Times New Roman',serif";
const FONT_MONO: &str = "'JetBrains Mono',ui-monospace,Consolas,monospace";

/// Same sender as every other transactional email in the crate.
const FROM: &str = "coralswarm <person@example.com>";

/// Who a notice is written to. The copy differs: an admin can change the
/// limit, a member can only ask the admin to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Audience {
    /// An admin or owner of a team organization.
    OrgAdmin,
    /// A member of a team organization (not acting as its admin).
    OrgMember,
    /// The owner of a personal (Pro) account.
    PersonalOwner,
}

/// Everything a template can say. All optional facts degrade to a sentence
/// that is still true without them.
#[derive(Clone, Debug, Default)]
pub struct NoticeFacts {
    /// The organization's display name, when known.
    pub org_name: Option<String>,
    /// When the current extra-usage window ends and limits start over.
    pub resets_at: Option<DateTime<Utc>>,
    /// The monthly extra-usage limit, in micro-USD.
    pub limit_micro_usd: Option<i64>,
    /// An auto-reload's amount, in micro-USD.
    pub reload_amount_micro_usd: Option<i64>,
    /// The failed reload this email is about was the one that turned
    /// auto-reload off (too many failures in a row).
    pub auto_reload_turned_off: bool,
    /// Trial emails: the seats the card is charged for at conversion.
    pub trial_seats: Option<i32>,
    /// Trial emails: when the 90-day backstop converts the trial.
    pub trial_ends_at: Option<DateTime<Utc>>,
    /// Absolute Settings -> Billing URL (or, for a member, their own Account
    /// page: members cannot open Billing).
    pub link: String,
}

/// A rendered email.
#[derive(Clone, Debug)]
pub struct NoticeEmail {
    pub subject: String,
    pub html: String,
    pub text: String,
}

fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// `$100.00`. Micro-USD is exact to the cent for every amount the settings
/// accept; sub-cent amounts round down.
pub fn dollars(micro: i64) -> String {
    let cents = micro / 10_000;
    format!("${}.{:02}", cents / 100, cents % 100)
}

fn date_label(at: DateTime<Utc>) -> String {
    at.format("%B %-d, %Y").to_string()
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

/// "2 seats", or "your seats" when the count is unknown.
fn seats_phrase(seats: Option<i32>) -> String {
    match seats {
        Some(1) => "1 seat".to_string(),
        Some(n) if n > 1 => format!("{n} seats"),
        _ => "your seats".to_string(),
    }
}

/// How the email names the organization: its name, else "your organization".
fn org_ref(facts: &NoticeFacts) -> String {
    match facts.org_name.as_deref().map(str::trim) {
        Some(n) if !n.is_empty() => n.chars().take(80).collect(),
        _ => "your organization".to_string(),
    }
}

struct Words {
    subject: String,
    heading: String,
    paragraphs: Vec<String>,
    cta: &'static str,
    why: String,
}

fn copy_for(kind: NoticeKind, audience: Audience, facts: &NoticeFacts) -> Words {
    let org = org_ref(facts);
    let reset = facts
        .resets_at
        .map(|at| format!(" on {}", date_label(at)))
        .unwrap_or_default();
    let limit = facts.limit_micro_usd.map(dollars);
    let admin_why = format!("You're getting this because you're an admin of {org} on coralswarm.");
    let owner_why = "You're getting this because it's your coralswarm account.".to_string();
    match (kind, audience) {
        (NoticeKind::ExtraUsageLimitReached, Audience::OrgAdmin) => Words {
            subject: format!("{} has reached its monthly extra usage limit", capitalize(&org)),
            heading: "Extra usage hit its monthly limit".into(),
            paragraphs: vec![
                format!(
                    "{} has used all of the extra usage you allowed this month{}.",
                    capitalize(&org),
                    limit.map(|l| format!(" ({l})")).unwrap_or_default()
                ),
                format!(
                    "Members who have used their plan credits can't run new asks until it resets{reset}. \
                     To keep going sooner, raise the monthly limit in Settings \u{2192} Billing."
                ),
            ],
            cta: "Open billing settings",
            why: admin_why,
        },
        (NoticeKind::ExtraUsageLimitReached, _) => Words {
            subject: "You've reached your monthly extra usage limit".into(),
            heading: "Extra usage hit its monthly limit".into(),
            paragraphs: vec![
                format!(
                    "You've used all of the extra usage you allowed this month{}.",
                    limit.map(|l| format!(" ({l})")).unwrap_or_default()
                ),
                format!(
                    "New asks are paused once your plan credits run out, until it resets{reset}. \
                     To keep going sooner, raise the monthly limit in Settings \u{2192} Billing."
                ),
            ],
            cta: "Open billing settings",
            why: owner_why,
        },
        (NoticeKind::BalanceDepleted, Audience::OrgAdmin) => Words {
            subject: format!("{}'s extra usage balance is empty", capitalize(&org)),
            heading: "Your extra usage balance is empty".into(),
            paragraphs: vec![
                format!("The extra usage balance for {org} has run out."),
                "Members who have used their plan credits can't run new asks until you add funds in \
                 Settings \u{2192} Billing."
                    .into(),
            ],
            cta: "Add funds",
            why: admin_why,
        },
        (NoticeKind::BalanceDepleted, _) => Words {
            subject: "Your extra usage balance is empty".into(),
            heading: "Your extra usage balance is empty".into(),
            paragraphs: vec![
                "Your extra usage balance has run out.".into(),
                "New asks are paused once your plan credits run out, until you add funds in \
                 Settings \u{2192} Billing."
                    .into(),
            ],
            cta: "Add funds",
            why: owner_why,
        },
        (NoticeKind::MemberCapReached, _) => Words {
            subject: "You've reached your extra usage limit for this month".into(),
            heading: "You've used your share of extra usage".into(),
            paragraphs: vec![
                format!("You've reached the limit on extra usage that {org} allows each member this month."),
                format!(
                    "You can run new asks again when it resets{reset}. If you need more sooner, \
                     ask an admin of your organization."
                ),
            ],
            cta: "See your usage",
            why: format!("You're getting this because you're a member of {org} on coralswarm."),
        },
        (NoticeKind::TrialCredits80, _) => Words {
            subject: format!("{} has used 80% of its free trial credits", capitalize(&org)),
            heading: "80% of your trial credits are used".into(),
            paragraphs: vec![
                format!("{} has used 80% of the free credits that came with its trial.", capitalize(&org)),
                format!(
                    "When the rest run out, your subscription starts automatically: we charge the card you \
                     added at sign-up for {}, and your team gets its full monthly credits right away. You \
                     don't need to do anything.",
                    seats_phrase(facts.trial_seats)
                ),
                "To change seats or your card before then, open Settings \u{2192} Billing.".into(),
            ],
            cta: "Open billing settings",
            why: format!("You're getting this because you own {org} on coralswarm."),
        },
        (NoticeKind::TrialCreditsUsed, _) => Words {
            subject: format!("{}'s free trial credits are used up", capitalize(&org)),
            heading: "Your subscription is starting".into(),
            paragraphs: vec![
                format!("{} has used all of its free trial credits, so its subscription is starting now.", capitalize(&org)),
                format!(
                    "We're charging the card you added at sign-up for {}. Your team's monthly credits are \
                     available as soon as the payment goes through.",
                    seats_phrase(facts.trial_seats)
                ),
                "If the payment doesn't go through, update your card in Settings \u{2192} Billing so your \
                 team can keep working."
                    .into(),
            ],
            cta: "Open billing settings",
            why: format!("You're getting this because you own {org} on coralswarm."),
        },
        (NoticeKind::TrialEndingSoon, _) => {
            let when = facts
                .trial_ends_at
                .map(|at| format!("on {}", date_label(at)))
                .unwrap_or_else(|| "in 7 days".to_string());
            Words {
                subject: format!("{}'s free trial ends {when}", capitalize(&org)),
                heading: "Your free trial ends soon".into(),
                paragraphs: vec![
                    format!(
                        "Trials last 90 days. {}'s trial ends {when}, even though it still has free credits left.",
                        capitalize(&org)
                    ),
                    format!(
                        "Then your subscription starts automatically: we charge the card you added at sign-up \
                         for {}, and your team gets its full monthly credits right away.",
                        seats_phrase(facts.trial_seats)
                    ),
                    "To change seats or your card, or to cancel, open Settings \u{2192} Billing.".into(),
                ],
                cta: "Open billing settings",
                why: format!("You're getting this because you own {org} on coralswarm."),
            }
        }
        (NoticeKind::AutoReloadSucceeded, aud) => Words {
            subject: "Extra usage was reloaded".into(),
            heading: "Extra usage was reloaded".into(),
            paragraphs: vec![format!(
                "We added {} of extra usage{} so asks keep working.",
                facts.reload_amount_micro_usd.map(dollars).unwrap_or_else(|| "more".into()),
                if aud == Audience::OrgAdmin { format!(" to {org}") } else { String::new() }
            )],
            cta: "Open billing settings",
            why: if aud == Audience::OrgAdmin { admin_why } else { owner_why },
        },
        (NoticeKind::AutoReloadFailed, aud) => {
            let of = if aud == Audience::OrgAdmin { format!(" for {org}") } else { String::new() };
            let to = if aud == Audience::OrgAdmin { format!(" to {org}") } else { String::new() };
            let mut paragraphs = vec![match facts.reload_amount_micro_usd.map(dollars) {
                Some(amt) => format!("We tried to add {amt} of extra usage{to}, but the payment didn't go through."),
                None => format!(
                    "We couldn't reload extra usage{of} because there's no payment method on file to charge."
                ),
            }];
            if facts.auto_reload_turned_off {
                paragraphs.push(
                    "Because this has failed several times in a row, we turned off auto-reload. Your balance \
                     won't be topped up automatically until you turn it back on."
                        .into(),
                );
            } else if facts.reload_amount_micro_usd.is_some() {
                paragraphs.push("We'll try again later, and turn auto-reload off if it keeps failing.".into());
            }
            paragraphs.push("Update your payment method in Settings \u{2192} Billing so asks don't stop.".into());
            Words {
                subject: if facts.auto_reload_turned_off {
                    "Auto-reload is turned off".into()
                } else {
                    "We couldn't reload your extra usage".into()
                },
                heading: if facts.auto_reload_turned_off {
                    "Auto-reload is turned off".into()
                } else {
                    "We couldn't reload your extra usage".into()
                },
                paragraphs,
                cta: "Open billing settings",
                why: if aud == Audience::OrgAdmin { admin_why } else { owner_why },
            }
        }
    }
}

/// Render one notice for one audience.
pub fn render(kind: NoticeKind, audience: Audience, facts: &NoticeFacts) -> NoticeEmail {
    let c = copy_for(kind, audience, facts);
    let url = escape_html(&facts.link);
    let paras: String = c
        .paragraphs
        .iter()
        .map(|p| {
            format!(
                "<p style=\"margin:0 0 16px 0;font-size:14px;line-height:1.6;color:#4A3F36\">{}</p>",
                escape_html(p)
            )
        })
        .collect();
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<div style=\"display:none;max-height:0;overflow:hidden;mso-hide:all\">{pre}</div>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:{FONT_SANS};color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coralswarm</div>\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:12px\">billing</div>\
<h1 style=\"margin:0 0 14px 0;font-family:{FONT_SERIF};font-size:30px;font-weight:400;line-height:1.2;color:#1A1410\">{heading}</h1>\
{paras}\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin:8px 0 22px 0\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url}\" style=\"display:inline-block;padding:11px 22px;font-family:{FONT_SANS};font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">{cta}</a>\
</td></tr></table>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>\
<td style=\"border-top:1px solid #D9CCB8;padding-top:16px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7;color:#8A7B6E\">{why}</p>\
</td></tr></table>\
</td></tr></table>\
</td></tr></table></body></html>",
        pre = escape_html(&c.subject),
        heading = escape_html(&c.heading),
        cta = escape_html(c.cta),
        why = escape_html(&c.why),
    );
    let text = format!("{}\n\n{}\n\n{}: {}\n\n{}\n", c.heading, c.paragraphs.join("\n\n"), c.cta, facts.link, c.why);
    NoticeEmail { subject: c.subject, html, text }
}

/// Why a send did not go out.
#[derive(Debug)]
pub struct SendError(pub String);

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The Resend client for notices. The base URL is a field, not an env read
/// per call, so tests point it at a local mock without touching process env.
#[derive(Clone)]
pub struct Mailer {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl Mailer {
    pub fn new(http: reqwest::Client, api_key: String, base_url: String) -> Self {
        Self { http, api_key, base_url }
    }

    /// The production sender: the app's HTTP client and `RESEND_API_KEY`, and
    /// `RESEND_API_BASE_URL` when set (the override every email module in the
    /// crate honours). `None` when email is not configured.
    pub fn from_state(state: &crate::AppState) -> Option<Self> {
        let key = state.resend_api_key.clone().filter(|k| !k.trim().is_empty())?;
        let base = std::env::var("RESEND_API_BASE_URL").unwrap_or_else(|_| "https://api.resend.com".to_string());
        Some(Self::new(state.http_client.clone(), key, base))
    }

    /// Send one email. `idempotency_key` is Resend's own `Idempotency-Key`: if
    /// a worker dies after Resend accepts the email but before the database
    /// records it, the retry carries the same key and Resend does not send a
    /// second copy. Returns the provider's message id when it gave one.
    pub async fn send(
        &self,
        to: &str,
        email: &NoticeEmail,
        idempotency_key: &str,
    ) -> Result<Option<String>, SendError> {
        let payload = serde_json::json!({
            "from": FROM,
            "to": [to],
            "subject": email.subject,
            "html": email.html,
            "text": email.text,
        });
        let url = format!("{}/emails", self.base_url);
        let send = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .header("Idempotency-Key", idempotency_key)
            .json(&payload)
            .send();
        match tokio::time::timeout(Duration::from_secs(10), send).await {
            Ok(Ok(resp)) if resp.status().is_success() => {
                let id = resp
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|v| v.get("id").and_then(|i| i.as_str()).map(str::to_string));
                Ok(id)
            }
            Ok(Ok(resp)) => Err(SendError(format!("resend returned {}", resp.status()))),
            Ok(Err(e)) => Err(SendError(format!("resend request failed: {e}"))),
            Err(_) => Err(SendError("resend request timed out".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [NoticeKind; 8] = [
        NoticeKind::ExtraUsageLimitReached,
        NoticeKind::MemberCapReached,
        NoticeKind::BalanceDepleted,
        NoticeKind::AutoReloadSucceeded,
        NoticeKind::AutoReloadFailed,
        NoticeKind::TrialCredits80,
        NoticeKind::TrialCreditsUsed,
        NoticeKind::TrialEndingSoon,
    ];

    fn facts() -> NoticeFacts {
        NoticeFacts {
            org_name: Some("Acme <b>Co</b>".into()),
            resets_at: DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z").ok().map(|d| d.with_timezone(&Utc)),
            limit_micro_usd: Some(100_000_000),
            reload_amount_micro_usd: Some(25_000_000),
            auto_reload_turned_off: false,
            trial_seats: Some(3),
            trial_ends_at: DateTime::parse_from_rfc3339("2027-01-25T00:00:00Z").ok().map(|d| d.with_timezone(&Utc)),
            link: "https://app.coralswarm.example/settings/organization/billing".into(),
        }
    }

    /// The trial emails are the Checkout page's promise kept: they say the
    /// subscription starts by itself and what the card is charged for.
    #[test]
    fn the_trial_emails_say_the_card_is_charged_for_the_seats_automatically() {
        for kind in [NoticeKind::TrialCredits80, NoticeKind::TrialCreditsUsed, NoticeKind::TrialEndingSoon] {
            let e = render(kind, Audience::OrgAdmin, &facts());
            assert!(e.text.contains("3 seats"), "{kind:?}: {}", e.text);
            assert!(e.text.contains("card you added at sign-up"), "{kind:?}");
            assert!(e.text.contains("Settings"), "{kind:?}");
            assert!(e.subject.contains("Acme"), "{kind:?}: {}", e.subject);
        }
        let soon = render(NoticeKind::TrialEndingSoon, Audience::OrgAdmin, &facts());
        assert!(soon.text.contains("January 25, 2027"), "{}", soon.text);
        assert!(soon.text.contains("90 days"));
        let eighty = render(NoticeKind::TrialCredits80, Audience::OrgAdmin, &facts());
        assert!(eighty.text.contains("80%"));
        assert!(eighty.text.contains("automatically"));
    }

    #[test]
    fn a_trial_email_with_no_facts_still_reads_true() {
        let bare = NoticeFacts { link: "https://x/settings".into(), ..Default::default() };
        let e = render(NoticeKind::TrialEndingSoon, Audience::OrgAdmin, &bare);
        assert!(e.text.contains("in 7 days"), "{}", e.text);
        assert!(e.text.contains("your seats"));
    }

    #[test]
    fn dollars_formats_whole_and_fractional_amounts() {
        assert_eq!(dollars(100_000_000), "$100.00");
        assert_eq!(dollars(22_000), "$0.02");
        assert_eq!(dollars(1_234_560_000), "$1234.56");
        assert_eq!(dollars(0), "$0.00");
    }

    #[test]
    fn no_kind_or_audience_uses_the_internal_vocabulary() {
        for kind in ALL {
            for aud in [Audience::OrgAdmin, Audience::OrgMember, Audience::PersonalOwner] {
                let e = render(kind, aud, &facts());
                let all = format!("{} {} {}", e.subject, e.html, e.text).to_lowercase();
                for banned in ["ocean", "reef"] {
                    assert!(!all.contains(banned), "{kind:?}/{aud:?} says {banned}");
                }
                // "coral" is only allowed as the brand name in "coralswarm".
                assert!(
                    !all.replace("coralswarm", "").contains("coral"),
                    "{kind:?}/{aud:?} says coral"
                );
                assert!(e.html.contains("href=\"https://app.coralswarm.example/settings/organization/billing\""));
                assert!(e.text.contains("https://app.coralswarm.example/settings/organization/billing"));
            }
        }
    }

    #[test]
    fn the_member_email_carries_no_money() {
        let e = render(NoticeKind::MemberCapReached, Audience::OrgMember, &facts());
        let all = format!("{} {} {}", e.subject, e.html, e.text);
        assert!(!all.contains('$'), "member email must not show dollars: {all}");
    }

    #[test]
    fn the_admin_limit_email_states_the_limit_and_the_reset_date() {
        let e = render(NoticeKind::ExtraUsageLimitReached, Audience::OrgAdmin, &facts());
        assert!(e.text.contains("$100.00"));
        assert!(e.text.contains("November 1, 2026"));
    }

    #[test]
    fn organization_names_are_escaped_in_html() {
        let e = render(NoticeKind::BalanceDepleted, Audience::OrgAdmin, &facts());
        assert!(!e.html.contains("<b>Co</b>"));
        assert!(e.html.contains("&lt;b&gt;Co&lt;/b&gt;"));
    }

    #[test]
    fn missing_facts_still_read_as_true_sentences() {
        let bare = NoticeFacts { link: "https://x/settings".into(), ..Default::default() };
        let e = render(NoticeKind::ExtraUsageLimitReached, Audience::OrgAdmin, &bare);
        assert!(e.subject.starts_with("Your organization has reached"));
        assert!(!e.text.contains("()"));
    }

    #[test]
    fn the_failure_that_turns_auto_reload_off_says_so() {
        let mut f = facts();
        let plain = render(NoticeKind::AutoReloadFailed, Audience::PersonalOwner, &f);
        assert!(!plain.text.contains("turned off"));
        assert!(plain.text.contains("try again"));
        f.auto_reload_turned_off = true;
        for aud in [Audience::OrgAdmin, Audience::PersonalOwner] {
            let e = render(NoticeKind::AutoReloadFailed, aud, &f);
            assert!(e.subject.contains("turned off"), "{}", e.subject);
            assert!(e.text.contains("turned off auto-reload"), "{}", e.text);
            assert!(e.text.contains("Settings"));
        }
    }

    #[test]
    fn a_missing_card_is_named_as_the_reason() {
        let mut f = facts();
        f.reload_amount_micro_usd = None;
        let e = render(NoticeKind::AutoReloadFailed, Audience::OrgAdmin, &f);
        assert!(e.text.contains("no payment method on file"), "{}", e.text);
        assert!(!e.text.contains("We tried to add"));
    }
}
